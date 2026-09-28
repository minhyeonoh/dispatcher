"""Per-host cap autotune: track each trial container's peak RSS,
estimate per-trial cost (P75 over a ring of completed peaks),
and lower `max_concurrent` so estimated cost × cap fits in
available memory minus a reserve.

Operator authority: autotune never raises above the operator's
ceiling. The ceiling re-anchors whenever the current cap differs
from what autotune itself last advised — i.e. any external PATCH
wins immediately.

Completion detection is disappearance-based (a tracked container
missing from this tick's sample is finished); peaks persist to a
jsonl ring so restarts keep their history."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import (
  BaseModel,
  ConfigDict,
  Field,
  PositiveFloat,
  PositiveInt,
)

from dispatcher.host_metrics import HostSample, sample_hosts

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.scheduler import Scheduler

logger = logging.getLogger(__name__)

HOST_METRICS_FILENAME = "host-metrics.jsonl"


class HostAutotuneConfig(BaseModel):
  enabled: bool = True
  seconds_between_ticks: PositiveFloat = 60.0
  ring_buffer_size: PositiveInt = 50
  bootstrap_min_samples: PositiveInt = 8
  """Below this many peaks per host, fall back to the global
  pool; below it globally too, leave the host alone."""

  peak_floor_bytes: PositiveInt = 1024 * 1024 * 1024
  """Lower bound on the estimate — a P75 dragged down by
  short-lived trials would over-cap into OOM."""

  reserve_fraction: float = Field(0.15, gt=0.0, lt=1.0)


class HostAutotunePatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  enabled: bool | None = None
  seconds_between_ticks: PositiveFloat | None = None
  ring_buffer_size: PositiveInt | None = None
  bootstrap_min_samples: PositiveInt | None = None
  peak_floor_bytes: PositiveInt | None = None
  reserve_fraction: float | None = Field(default=None, gt=0.0, lt=1.0)


@dataclass
class _TrackedTrial:
  trial_name: str
  max_rss: int
  first_seen: datetime
  last_seen: datetime
  sample_count: int


@dataclass
class TrialPeak:
  host: str
  trial_name: str
  peak_rss: int
  sample_count: int
  first_seen: datetime
  last_seen: datetime

  def to_json(self) -> dict[str, object]:
    return {
      "host": self.host,
      "trial_name": self.trial_name,
      "peak_rss": self.peak_rss,
      "sample_count": self.sample_count,
      "first_seen": self.first_seen.isoformat(),
      "last_seen": self.last_seen.isoformat(),
    }

  @classmethod
  def from_json(cls, obj: dict[str, object]) -> TrialPeak:
    return cls(
      host=str(obj["host"]),
      trial_name=str(obj["trial_name"]),
      peak_rss=int(obj["peak_rss"]),  # type: ignore[arg-type]
      sample_count=int(obj["sample_count"]),  # type: ignore[arg-type]
      first_seen=datetime.fromisoformat(str(obj["first_seen"])),
      last_seen=datetime.fromisoformat(str(obj["last_seen"])),
    )


@dataclass
class HostAutotuneState:
  metrics_file: Path
  tracked: dict[str, dict[str, _TrackedTrial]] = field(
    default_factory=dict
  )
  ring: dict[str, list[TrialPeak]] = field(default_factory=dict)
  operator_ceiling: dict[str, int] = field(default_factory=dict)
  last_advised: dict[str, int] = field(default_factory=dict)


# ── ring persistence ─────────────────────────────────────────────


def load_ring(
  metrics_file: Path, ring_size: int
) -> dict[str, list[TrialPeak]]:
  """Last `ring_size` peaks per host; malformed lines are just
  one less sample (unlike the event log, nothing depends on a
  complete record here)."""
  ring: dict[str, list[TrialPeak]] = {}
  if not metrics_file.exists():
    return ring
  with metrics_file.open("r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      try:
        peak = TrialPeak.from_json(json.loads(line))
      except (
        json.JSONDecodeError,
        KeyError,
        ValueError,
        TypeError,
      ) as exc:
        logger.warning("host_autotune: bad metrics line: %s", exc)
        continue
      ring.setdefault(peak.host, []).append(peak)
  for host in ring:
    if len(ring[host]) > ring_size:
      ring[host] = ring[host][-ring_size:]
  return ring


def truncate_ring_file(
  metrics_file: Path, ring: dict[str, list[TrialPeak]]
) -> None:
  """Rewrite with just the in-memory ring (bounded growth).
  Atomic; failure leaves the previous file, which re-trims on
  next load."""
  metrics_file.parent.mkdir(parents=True, exist_ok=True)
  tmp = metrics_file.with_suffix(metrics_file.suffix + ".tmp")
  with tmp.open("w", encoding="utf-8") as f:
    for host_peaks in ring.values():
      for peak in host_peaks:
        f.write(json.dumps(peak.to_json()) + "\n")
  tmp.replace(metrics_file)


def append_peak(metrics_file: Path, peak: TrialPeak) -> None:
  metrics_file.parent.mkdir(parents=True, exist_ok=True)
  with metrics_file.open("a", encoding="utf-8") as f:
    f.write(json.dumps(peak.to_json()) + "\n")


# ── pure computation ─────────────────────────────────────────────


def p75(values: list[int]) -> int:
  """Linear-interpolated 75th percentile (numpy 'linear')."""
  if not values:
    raise ValueError("p75 of empty sequence")
  s = sorted(values)
  n = len(s)
  pos = 0.75 * (n - 1)
  lo = int(pos)
  hi = min(lo + 1, n - 1)
  frac = pos - lo
  return int(s[lo] * (1 - frac) + s[hi] * frac)


def peak_estimate(
  host_ring: list[TrialPeak],
  global_ring: list[TrialPeak],
  bootstrap_min_samples: int,
  peak_floor_bytes: int,
) -> int | None:
  """None = not enough data anywhere; don't tune this host."""
  if len(host_ring) >= bootstrap_min_samples:
    peaks = [p.peak_rss for p in host_ring]
  elif len(global_ring) >= bootstrap_min_samples:
    peaks = [p.peak_rss for p in global_ring]
  else:
    return None
  return max(peak_floor_bytes, p75(peaks))


def advised_cap(
  *,
  mem_total_bytes: int,
  mem_avail_bytes: int,
  running_count: int,
  peak_estimate_bytes: int,
  reserve_fraction: float,
  operator_ceiling: int,
) -> int:
  """Never below 1 — a zero cap is an operator decision, not
  something autotune manufactures."""
  reserve = int(mem_total_bytes * reserve_fraction)
  free_for_us = max(0, mem_avail_bytes - reserve)
  headroom = free_for_us // peak_estimate_bytes
  advised = max(1, running_count + headroom)
  return min(operator_ceiling, advised)


def update_tracker(
  state: HostAutotuneState,
  host: str,
  sample: HostSample,
  now: datetime,
  ring_size: int,
) -> list[TrialPeak]:
  """Fold one sample; returns freshly-completed peaks for the
  caller to persist."""
  tracked_here = state.tracked.setdefault(host, {})
  current_names = {t.name for t in sample.trials}
  freshly_completed: list[TrialPeak] = []

  vanished = set(tracked_here) - current_names
  for name in vanished:
    tt = tracked_here.pop(name)
    peak = TrialPeak(
      host=host,
      trial_name=tt.trial_name,
      peak_rss=tt.max_rss,
      sample_count=tt.sample_count,
      first_seen=tt.first_seen,
      last_seen=tt.last_seen,
    )
    freshly_completed.append(peak)
    ring = state.ring.setdefault(host, [])
    ring.append(peak)
    if len(ring) > ring_size:
      state.ring[host] = ring[-ring_size:]

  for tstat in sample.trials:
    prev = tracked_here.get(tstat.name)
    if prev is None:
      tracked_here[tstat.name] = _TrackedTrial(
        trial_name=tstat.name,
        max_rss=tstat.rss_bytes,
        first_seen=now,
        last_seen=now,
        sample_count=1,
      )
    else:
      prev.max_rss = max(prev.max_rss, tstat.rss_bytes)
      prev.last_seen = now
      prev.sample_count += 1

  return freshly_completed


# ── the tick ─────────────────────────────────────────────────────


async def autotune_tick(
  *,
  state: HostAutotuneState,
  scheduler: Scheduler,
  config: HostAutotuneConfig,
  self_host: str,
  apply_cap: object | None = None,
  now: datetime | None = None,
) -> dict[str, int]:
  """Sample every host, fold trackers, persist completions, apply
  advised caps. Per-host failures leave that host untouched.
  `apply_cap(host, cap)` lets the server mirror the change into
  its config; defaults to `scheduler.set_host_settings`."""
  now = now or datetime.now(UTC)
  host_settings = scheduler.all_host_settings()
  hosts = list(host_settings.keys())
  if not hosts:
    return {}

  samples = await sample_hosts(hosts, self_host)
  applied: dict[str, int] = {}

  for host in hosts:
    result = samples.get(host)
    if isinstance(result, Exception):
      logger.warning("host_autotune: sample %s failed: %s", host, result)
      continue
    if result is None:
      continue
    completions = update_tracker(
      state, host, result, now, config.ring_buffer_size
    )
    for peak in completions:
      try:
        append_peak(state.metrics_file, peak)
      except OSError as exc:
        logger.warning(
          "host_autotune: append peak (%s/%s) failed: %s",
          host,
          peak.trial_name,
          exc,
        )

  running_per_host = scheduler.running_per_host()
  global_ring: list[TrialPeak] = [
    peak for host_peaks in state.ring.values() for peak in host_peaks
  ]

  for host in hosts:
    result = samples.get(host)
    if isinstance(result, Exception) or result is None:
      continue
    current_cap = host_settings[host].max_concurrent
    prev_advised = state.last_advised.get(host)
    if prev_advised is None or current_cap != prev_advised:
      # Operator (or startup) set this value — re-anchor.
      state.operator_ceiling[host] = current_cap
    ceiling = state.operator_ceiling[host]
    est = peak_estimate(
      state.ring.get(host, []),
      global_ring,
      config.bootstrap_min_samples,
      config.peak_floor_bytes,
    )
    if est is None:
      continue
    new_cap = advised_cap(
      mem_total_bytes=result.mem_total_bytes,
      mem_avail_bytes=result.mem_avail_bytes,
      running_count=running_per_host.get(host, 0),
      peak_estimate_bytes=est,
      reserve_fraction=config.reserve_fraction,
      operator_ceiling=ceiling,
    )
    if new_cap != current_cap:
      if callable(apply_cap):
        apply_cap(host, new_cap)
      else:
        scheduler.set_host_settings(host, max_concurrent=new_cap)
      applied[host] = new_cap
    state.last_advised[host] = new_cap

  return applied
