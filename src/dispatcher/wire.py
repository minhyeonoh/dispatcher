"""Wire shapes: request/response DTOs, snapshot builders, SSE
framing. Pure projections over scheduler/metrics state — no
mutations, no side effects."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from dispatcher.config import (
  ArchiveConfig,
  DispatcherConfig,
  OrphanGCConfig,
  StateReconciliationConfig,
)
from dispatcher.host_autotune import HostAutotuneConfig
from dispatcher.metrics import AttemptMetrics, MetricsCache
from dispatcher.models import HostSettings, TrialView
from dispatcher.notify import NotifyConfig

if TYPE_CHECKING:
  from dispatcher.scheduler import Scheduler


class RetryDoneErrRequest(BaseModel):
  """Empty body = retry every done_err trial. `trial_names`
  overrides the host/since filters."""

  model_config = ConfigDict(extra="forbid")

  host: str | None = None
  since_iso: datetime | None = None
  trial_names: list[str] | None = None


class AttemptCountsOut(BaseModel):
  pending: int
  running: int
  done_ok: int
  done_err: int
  ghosted: int
  unknown: int
  total: int


class AttemptSummaryOut(BaseModel):
  attempt_id: str
  label: str
  weight: int
  max_concurrent: int | None
  pause_on_error: bool | None
  paused: bool
  counts: AttemptCountsOut
  home_root: str
  alias: str
  scope: str = ""
  tags: list[str] = Field(default_factory=list)
  pool: str = "default"
  archived_at: datetime | None = None
  archive_kind: str = ""


class AttemptSummaryWithMetricsOut(AttemptSummaryOut):
  metrics: AttemptMetrics


class TrialViewOut(BaseModel):
  trial_name: str
  host: str
  dispatched_at: datetime
  # Outcome `values` for terminal trials whose envelope is cached;
  # None before then.
  values: dict[str, float] | None = None


class FullAttemptOut(AttemptSummaryOut):
  pending: list[str] = Field(default_factory=list)
  running: dict[str, TrialViewOut] = Field(default_factory=dict)
  done_ok: dict[str, TrialViewOut] = Field(default_factory=dict)
  done_err: dict[str, TrialViewOut] = Field(default_factory=dict)
  ghosted: dict[str, TrialViewOut] = Field(default_factory=dict)
  unknown: dict[str, TrialViewOut] = Field(default_factory=dict)


class ClusterConfigOut(BaseModel):
  max_concurrent: int
  hosts: dict[str, HostSettings]
  self_host: str
  data_dir: Path
  state_reconciliation: StateReconciliationConfig
  orphan_gc: OrphanGCConfig
  host_autotune: HostAutotuneConfig
  notify: NotifyConfig
  archive: ArchiveConfig = Field(default_factory=ArchiveConfig)
  pool_caps: dict[str, int] = Field(default_factory=dict)


class ClusterSnapshotOut(BaseModel):
  config: ClusterConfigOut
  running_total: int
  running_per_host: dict[str, int]
  running_per_pool: dict[str, int] = Field(default_factory=dict)


class StateOut(ClusterSnapshotOut):
  attempts: list[AttemptSummaryOut]


class MonitorOut(ClusterSnapshotOut):
  attempts: list[AttemptSummaryWithMetricsOut]


class HealthOut(BaseModel):
  status: str


# ── snapshot builders ────────────────────────────────────────────


def attempt_counts(
  scheduler: Scheduler, attempt_id: str
) -> AttemptCountsOut:
  state = scheduler.attempt_state(attempt_id)
  view = scheduler.attempt_view(attempt_id)
  return AttemptCountsOut(
    pending=len(view.pending),
    running=len(view.running),
    done_ok=len(view.done_ok),
    done_err=len(view.done_err),
    ghosted=len(view.ghosted),
    unknown=len(view.unknown),
    total=len(state.task_list),
  )


def snapshot_attempt(
  scheduler: Scheduler, attempt_id: str
) -> AttemptSummaryOut:
  state = scheduler.attempt_state(attempt_id)
  return AttemptSummaryOut(
    attempt_id=attempt_id,
    label=state.label,
    weight=state.weight,
    max_concurrent=state.max_concurrent,
    pause_on_error=state.pause_on_error,
    paused=state.paused,
    counts=attempt_counts(scheduler, attempt_id),
    home_root=str(state.home_root),
    alias=state.alias,
    scope=state.scope,
    tags=list(state.tags),
    pool=state.pool or "default",
    archived_at=state.archived_at,
    archive_kind=state.archive_kind or "",
  )


def snapshot_attempt_with_metrics(
  scheduler: Scheduler, metrics: MetricsCache, attempt_id: str
) -> AttemptSummaryWithMetricsOut:
  base = snapshot_attempt(scheduler, attempt_id)
  return AttemptSummaryWithMetricsOut(
    **base.model_dump(), metrics=metrics.get(attempt_id)
  )


def cluster_snapshot(
  config: DispatcherConfig, scheduler: Scheduler
) -> ClusterSnapshotOut:
  return ClusterSnapshotOut(
    config=ClusterConfigOut.model_validate(config, from_attributes=True),
    running_total=scheduler.running_total,
    running_per_host=scheduler.running_per_host(),
    running_per_pool=scheduler.pool_running_snapshot(),
  )


def full_attempt_view(
  scheduler: Scheduler, attempt_id: str
) -> FullAttemptOut:
  view = scheduler.attempt_view(attempt_id)
  base = snapshot_attempt(scheduler, attempt_id)

  def _values_for(task_name: str) -> dict[str, float] | None:
    outcome = scheduler.outcome_of(attempt_id, task_name)
    if outcome is None:
      return None
    return {
      k: float(v)
      for k, v in outcome.values.items()
      if isinstance(v, (int, float)) and not isinstance(v, bool)
    }

  def _bucket(
    d: dict[str, TrialView],
  ) -> dict[str, TrialViewOut]:
    return {
      tn: TrialViewOut(
        trial_name=tv.trial_name,
        host=tv.host,
        dispatched_at=tv.dispatched_at,
        values=_values_for(tn),
      )
      for tn, tv in d.items()
    }

  return FullAttemptOut(
    **base.model_dump(),
    pending=list(view.pending),
    running=_bucket(view.running),
    done_ok=_bucket(view.done_ok),
    done_err=_bucket(view.done_err),
    ghosted=_bucket(view.ghosted),
    unknown=_bucket(view.unknown),
  )


def full_attempt_bytes(scheduler: Scheduler, attempt_id: str) -> bytes:
  if scheduler.is_archived(attempt_id):
    return scheduler.archived_bytes(attempt_id)
  return (
    full_attempt_view(scheduler, attempt_id)
    .model_dump_json()
    .encode("utf-8")
  )


def build_full_attempts_body(
  scheduler: Scheduler, aids: list[str]
) -> bytes:
  """Concatenated JSON array; archived attempts splice their
  cached bytes verbatim so the hot path never rebuilds them."""
  parts: list[bytes] = [b"["]
  for i, aid in enumerate(aids):
    if i > 0:
      parts.append(b",")
    parts.append(full_attempt_bytes(scheduler, aid))
  parts.append(b"]")
  return b"".join(parts)


def sse(event: str, payload: BaseModel | dict[str, Any]) -> str:
  """One SSE frame: `event:` + JSON `data:` + blank line."""
  if isinstance(payload, BaseModel):
    data = payload.model_dump_json()
  else:
    data = json.dumps(payload, default=str)
  return f"event: {event}\ndata: {data}\n\n"
