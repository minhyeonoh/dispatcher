"""Operations on the dispatcher's state — the bodies behind the
HTTP endpoints, HTTP-free so loops and future non-HTTP callers
use the same code path. Failures raise OpError subclasses; the
server maps them to status codes at the edge."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dispatcher.event_log import (
  append_event,
  append_index_entry,
  event_log_path_for,
  replay_events,
)
from dispatcher.scheduler import (
  AliasCollisionError,
  AliasFormatError,
  NotArchivableError,
  NotArchivedError,
)
from dispatcher.wire import (
  RetryDoneErrRequest,
  attempt_counts,
  full_attempt_view,
  snapshot_attempt,
  snapshot_attempt_with_metrics,
)

if TYPE_CHECKING:
  import asyncio
  from collections.abc import Callable

  from dispatcher.config import DispatcherConfig, SettingsPatch
  from dispatcher.event_bus import EventBus
  from dispatcher.metrics import MetricsCache
  from dispatcher.models import AttemptState, TrialView
  from dispatcher.notify import TelegramSender
  from dispatcher.runtime import DispatcherRuntime
  from dispatcher.scheduler import Scheduler
  from dispatcher.wire import AttemptSummaryOut

logger = logging.getLogger(__name__)


class OpError(Exception):
  """Base for operation failures; `status` is the HTTP mapping."""

  status = 500


class Invalid(OpError):
  status = 400


class NotFound(OpError):
  status = 404


class Conflict(OpError):
  status = 409


class Internal(OpError):
  status = 500


@dataclass
class ServerState:
  config: DispatcherConfig
  scheduler: Scheduler
  runtime: DispatcherRuntime
  metrics: MetricsCache
  event_bus: EventBus
  tasks: list[asyncio.Task[None]] = field(default_factory=list)
  notify_sender: TelegramSender | None = None
  _seq: int = field(default=0)

  def next_trial_name(self, task_name: str) -> str:
    """`<task[:32]>__<7-digit seq>` — deterministic, monotonic."""
    self._seq += 1
    truncated = task_name[:32].rstrip("_-")
    return f"{truncated}__{self._seq:07d}"

  def advance_seq_to(self, seq: int) -> None:
    """The counter lives in memory only; restore pushes it past
    every name on disk — re-minting a used name would read the
    OLD trial dir's outcome as the new trial's before it runs."""
    self._seq = max(self._seq, seq)


def filter_presets_path(config: DispatcherConfig) -> Path:
  return config.data_dir / "filter-presets.json"


def pool_caps_path(config: DispatcherConfig) -> Path:
  return config.data_dir / "pool-caps.json"


def load_pool_caps(config: DispatcherConfig) -> dict[str, int]:
  """Corrupt/missing blob → {} (never blocks startup)."""
  path = pool_caps_path(config)
  if not path.is_file():
    return {}
  try:
    raw = json.loads(path.read_text())
  except (OSError, ValueError):
    logger.warning("pool_caps blob at %s unreadable; ignoring", path)
    return {}
  if not isinstance(raw, dict):
    return {}
  return {
    k: int(v)
    for k, v in raw.items()
    if isinstance(k, str) and isinstance(v, (int, float)) and v >= 0
  }


def save_pool_caps(config: DispatcherConfig, caps: dict[str, int]) -> None:
  path = pool_caps_path(config)
  path.parent.mkdir(parents=True, exist_ok=True)
  tmp = path.with_suffix(path.suffix + ".tmp")
  try:
    tmp.write_text(json.dumps(caps, indent=2, sort_keys=True))
    tmp.replace(path)
  except OSError as exc:
    logger.warning("pool_caps blob write failed path=%s err=%s", path, exc)


def apply_settings(st: ServerState, patch: SettingsPatch) -> None:
  if patch.hosts is not None:
    for host, host_patch in patch.hosts.items():
      new = st.scheduler.set_host_settings(
        host,
        max_concurrent=host_patch.max_concurrent,
        active=host_patch.active,
        alive=host_patch.alive,
      )
      # Mirror into config so /state readback stays truthful.
      st.config.hosts[host] = new.model_copy()
      docker_events = st.runtime._docker_events
      if docker_events is not None:
        # Streams stay up for inactive hosts too: trials already
        # dispatched there still emit die events we must catch.
        docker_events.ensure_host(host)
  if patch.max_concurrent is not None:
    st.scheduler.set_max_concurrent(patch.max_concurrent)
    st.config.max_concurrent = patch.max_concurrent
  if patch.state_reconciliation is not None:
    recon = patch.state_reconciliation
    if recon.max_concurrent_probes is not None:
      st.config.state_reconciliation.max_concurrent_probes = (
        recon.max_concurrent_probes
      )
    if recon.seconds_between_probes is not None:
      st.config.state_reconciliation.seconds_between_probes = (
        recon.seconds_between_probes
      )
  if patch.orphan_gc is not None:
    gc = patch.orphan_gc
    if gc.enabled is not None:
      st.config.orphan_gc.enabled = gc.enabled
    if gc.seconds_between_sweeps is not None:
      st.config.orphan_gc.seconds_between_sweeps = (
        gc.seconds_between_sweeps
      )
    if gc.min_container_age_s is not None:
      st.config.orphan_gc.min_container_age_s = gc.min_container_age_s
  if patch.host_autotune is not None:
    at = patch.host_autotune
    if at.enabled is not None:
      st.config.host_autotune.enabled = at.enabled
    if at.seconds_between_ticks is not None:
      st.config.host_autotune.seconds_between_ticks = (
        at.seconds_between_ticks
      )
    if at.ring_buffer_size is not None:
      st.config.host_autotune.ring_buffer_size = at.ring_buffer_size
    if at.bootstrap_min_samples is not None:
      st.config.host_autotune.bootstrap_min_samples = (
        at.bootstrap_min_samples
      )
    if at.peak_floor_bytes is not None:
      st.config.host_autotune.peak_floor_bytes = at.peak_floor_bytes
    if at.reserve_fraction is not None:
      st.config.host_autotune.reserve_fraction = at.reserve_fraction
  if patch.pool_caps is not None:
    normalised = {name: int(cap) for name, cap in patch.pool_caps.items()}
    st.scheduler.set_pool_caps(normalised)
    st.config.pool_caps = dict(normalised)
    save_pool_caps(st.config, normalised)
  if patch.notify is not None:
    nf = patch.notify
    if nf.enabled is not None:
      st.config.notify.enabled = nf.enabled
    if nf.thresholds is not None:
      st.config.notify.thresholds = [float(t) for t in nf.thresholds]
    if nf.telegram_chat_id is not None:
      st.config.notify.telegram_chat_id = nf.telegram_chat_id
  if patch.archive is not None:
    av = patch.archive
    if av.auto_after_days is not None:
      st.config.archive.auto_after_days = av.auto_after_days
    if av.scan_interval_seconds is not None:
      st.config.archive.scan_interval_seconds = av.scan_interval_seconds


def _prepare_submit(payload: dict[str, Any]) -> dict[str, Any]:
  out = dict(payload)
  out.setdefault(
    "attempt_id", _mk_attempt_id(payload.get("label", "attempt"))
  )
  out.setdefault("submitted_at", datetime.now(UTC).isoformat())
  return out


def _validate_submit(st: ServerState, payload: dict[str, Any]) -> None:
  home_root = payload.get("home_root")
  if not isinstance(home_root, str) or not home_root:
    raise Invalid("home_root must be a non-empty string")
  if not Path(home_root).is_absolute():
    raise Invalid(
      f"home_root must be absolute (got {home_root!r})",
    )
  # One home_root per attempt, ever: two attempts sharing one
  # would interleave trial dirs and merge their event logs — the
  # kind of silent cross-contamination no readout would catch.
  resolved = str(Path(home_root))
  for aid in st.scheduler.iter_attempt_ids():
    other = st.scheduler.attempt_state(aid)
    if str(other.home_root) == resolved:
      raise Conflict(
        (f"home_root {home_root!r} already belongs to attempt {aid!r}"),
      )
  task_list = payload.get("task_list")
  if not isinstance(task_list, list) or not task_list:
    raise Invalid("task_list must be a non-empty list")
  if len(set(task_list)) != len(task_list):
    raise Invalid("task_list contains duplicates")
  payloads = payload.get("payloads") or {}
  if not isinstance(payloads, dict):
    raise Invalid("payloads must be an object")
  stray = set(payloads) - set(task_list)
  if stray:
    raise Invalid(
      (
        f"payloads for unknown tasks: {sorted(stray)[:5]} — "
        f"likely a typo; every payload key must be in task_list"
      ),
    )


def submit_attempt(
  st: ServerState,
  payload: dict[str, Any],
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  _validate_submit(st, payload)
  prepared = _prepare_submit(payload)
  # Alias minted BEFORE the submit event so the handle the
  # operator uses lives on disk from birth.
  if not prepared.get("alias"):
    prepared["alias"] = st.scheduler.mint_alias(prepared["attempt_id"])
  events: list[dict[str, Any]] = [{"type": "submit", **prepared}]
  replay_out = replay_events(events)
  if replay_out is None:  # pragma: no cover — no cancel possible
    raise Internal("internal")
  attempt, _ = replay_out
  try:
    st.scheduler.submit(attempt)
  except AliasCollisionError as exc:
    raise Conflict(f"alias {exc.alias!r} already in use") from exc
  except ValueError as exc:
    raise Conflict(str(exc)) from exc
  # Log written AFTER scheduler accepted, so a 409 leaves no
  # orphan log; index after the log so a crash between the two is
  # recoverable from the log side.
  log_path = event_log_path_for(attempt)
  for ev in events:
    append_event(log_path, ev)
  append_index_entry(
    st.config.data_dir,
    {
      "event": "submit",
      "attempt_id": attempt.attempt_id,
      "log_path": str(log_path),
      "at": clock_fn().isoformat(),
    },
  )
  st.event_bus.publish(
    "attempt_submitted",
    snapshot_attempt_with_metrics(
      st.scheduler, st.metrics, attempt.attempt_id
    ).model_dump(mode="json"),
  )
  return {
    "attempt_id": attempt.attempt_id,
    "alias": attempt.alias,
    "status": "submitted",
  }


_ALLOWED_PATCH_KNOBS = frozenset(
  {
    "paused",
    "weight",
    "max_concurrent",
    "pause_on_error",
    "alias",
    "tags",
    "pool",
  }
)


def patch_attempt(
  st: ServerState,
  attempt_id: str,
  payload: dict[str, Any],
  clock_fn: Callable[[], datetime],
) -> AttemptSummaryOut:
  if not isinstance(payload, dict) or not payload:
    raise Invalid(
      "body must be a non-empty {knob: value} object",
    )
  unknown = set(payload) - _ALLOWED_PATCH_KNOBS
  if unknown:
    raise Invalid(
      (
        f"unknown knob(s): {sorted(unknown)}; "
        f"allowed: {sorted(_ALLOWED_PATCH_KNOBS)}"
      ),
    )
  if not st.scheduler.has_attempt(attempt_id):
    raise NotFound(f"attempt {attempt_id!r} not found")
  if st.scheduler.is_archived(attempt_id):
    raise Conflict(
      (
        f"attempt {attempt_id!r} is archived — POST "
        f"/attempts/{attempt_id}/unarchive first"
      ),
    )
  # Alias goes through the uniqueness-enforcing entry point.
  alias_value = payload.pop("alias", None)
  if alias_value is not None:
    if not isinstance(alias_value, str):
      raise Invalid("alias must be a string")
    try:
      st.scheduler.set_alias(attempt_id, alias_value)
    except AliasFormatError as exc:
      raise Invalid(
        (f"alias {exc.alias!r} invalid: non-empty, ≤120 chars"),
      ) from exc
    except AliasCollisionError as exc:
      other = st.scheduler.attempt_id_of_alias(exc.alias)
      raise Conflict(
        (f"alias {exc.alias!r} already used by attempt {other!r}"),
      ) from exc
  if "pool" in payload:
    raw_pool = payload["pool"]
    if not isinstance(raw_pool, str):
      raise Invalid("pool must be a string")
    payload["pool"] = raw_pool.strip() or "default"
  if "tags" in payload:
    raw = payload["tags"]
    if not isinstance(raw, list) or not all(
      isinstance(t, str) for t in raw
    ):
      raise Invalid("tags must be a list of strings")
    seen: set[str] = set()
    cleaned: list[str] = []
    for t in raw:
      s = t.strip()
      if not s or s in seen:
        continue
      seen.add(s)
      cleaned.append(s)
    payload["tags"] = cleaned
  try:
    if payload:
      st.scheduler.patch(attempt_id, **payload)
  except ValueError as exc:
    raise Invalid(str(exc)) from exc
  if alias_value is not None:
    payload["alias"] = alias_value
  state = st.scheduler.attempt_state(attempt_id)
  log_path = event_log_path_for(state)
  at = clock_fn().isoformat()
  for knob, value in payload.items():
    append_event(
      log_path,
      {
        "type": "patch",
        "attempt_id": attempt_id,
        "at": at,
        knob: value,
      },
    )
  st.event_bus.publish(
    "attempt_patched",
    snapshot_attempt_with_metrics(
      st.scheduler, st.metrics, attempt_id
    ).model_dump(mode="json"),
  )
  return snapshot_attempt(st.scheduler, attempt_id)


def cancel_attempt(
  st: ServerState,
  attempt_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  if not st.scheduler.has_attempt(attempt_id):
    raise NotFound(f"attempt {attempt_id!r} not found")
  final_snapshot = snapshot_attempt(st.scheduler, attempt_id)
  state = st.scheduler.attempt_state(attempt_id)
  runtime_state = st.scheduler.cancel(attempt_id)
  # Fire-and-forget remote kill; the GC catches stragglers.
  st.runtime.fire_kill_trials(runtime_state.running)
  log_path = event_log_path_for(state)
  append_event(
    log_path,
    {
      "type": "cancel",
      "attempt_id": attempt_id,
      "at": clock_fn().isoformat(),
    },
  )
  append_index_entry(
    st.config.data_dir,
    {
      "event": "cancel",
      "attempt_id": attempt_id,
      "at": clock_fn().isoformat(),
    },
  )
  final_json = final_snapshot.model_dump(mode="json")
  st.event_bus.publish(
    "attempt_cancelled",
    {"attempt_id": attempt_id, "final": final_json},
  )
  return {
    "attempt_id": attempt_id,
    "status": "cancelled",
    "final": final_json,
    "killed": len(runtime_state.running),
  }


def _require_paused_live(
  st: ServerState, attempt_id: str, verb: str
) -> AttemptState:
  try:
    state = st.scheduler.attempt_state(attempt_id)
  except KeyError as exc:
    raise NotFound(f"attempt {attempt_id!r} not found") from exc
  if st.scheduler.is_archived(attempt_id):
    raise Conflict(
      (
        f"attempt {attempt_id!r} is archived; POST "
        f"/attempts/{attempt_id}/unarchive first"
      ),
    )
  if not state.paused:
    raise Conflict(
      (
        f'attempt {attempt_id!r} is not paused; PATCH {{"paused": '
        f"true}} first so {verb} doesn't race the dispatch loop"
      ),
    )
  return state


def reclaim_attempt(
  st: ServerState,
  attempt_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  state = _require_paused_live(st, attempt_id, "reclaim")
  # Snapshot before any mutation so the remote kill and the
  # scheduler bookkeeping see the same running set.
  view = st.scheduler.attempt_view(attempt_id)
  running_snapshot: dict[str, TrialView] = dict(view.running)
  st.runtime.fire_kill_trials(running_snapshot)
  log_path = event_log_path_for(state)
  at = clock_fn().isoformat()
  reclaimed: list[str] = []
  skipped: list[str] = []
  for task_name, tv in running_snapshot.items():
    if st.scheduler.reclaim_from_running(attempt_id, task_name):
      reclaimed.append(task_name)
      append_event(
        log_path,
        {
          "type": "reclaim",
          "attempt_id": attempt_id,
          "task_name": task_name,
          "trial_name": tv.trial_name,
          "at": at,
        },
      )
    else:
      skipped.append(task_name)
  st.event_bus.publish(
    "attempt_reclaimed",
    {
      "attempt_id": attempt_id,
      "reclaimed": reclaimed,
      "skipped_completed": skipped,
    },
  )
  return {
    "attempt_id": attempt_id,
    "reclaimed": reclaimed,
    "skipped_completed": skipped,
    "total": len(running_snapshot),
  }


def retry_done_err(
  st: ServerState,
  attempt_id: str,
  payload: RetryDoneErrRequest | None,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  state = _require_paused_live(st, attempt_id, "retry")
  payload = payload or RetryDoneErrRequest()
  view = st.scheduler.attempt_view(attempt_id)
  done_err_snapshot: dict[str, TrialView] = dict(view.done_err)

  targets: list[tuple[str, TrialView]] = []
  if payload.trial_names:
    wanted = set(payload.trial_names)
    for task_name, tv in done_err_snapshot.items():
      if tv.trial_name in wanted:
        targets.append((task_name, tv))
  else:
    since_epoch = (
      payload.since_iso.timestamp()
      if payload.since_iso is not None
      else None
    )
    for task_name, tv in done_err_snapshot.items():
      if payload.host is not None and tv.host != payload.host:
        continue
      if since_epoch is not None:
        outcome_path = state.home_root / tv.trial_name / "outcome.json"
        try:
          if outcome_path.stat().st_mtime < since_epoch:
            continue
        except OSError:
          # Nothing to compare against — do NOT retry, so the
          # operator's filter stays predictable.
          continue
      targets.append((task_name, tv))

  log_path = event_log_path_for(state)
  at = clock_fn().isoformat()
  retried: list[str] = []
  skipped: list[str] = []
  for task_name, tv in targets:
    if st.scheduler.retry_from_done_err(attempt_id, task_name):
      retried.append(task_name)
      st.metrics.undo_done_err(attempt_id)
      append_event(
        log_path,
        {
          "type": "retry",
          "attempt_id": attempt_id,
          "task_name": task_name,
          "trial_name": tv.trial_name,
          "at": at,
        },
      )
    else:
      skipped.append(task_name)
  st.event_bus.publish(
    "attempt_retried",
    {
      "attempt_id": attempt_id,
      "retried": retried,
      "skipped": skipped,
    },
  )
  return {
    "attempt_id": attempt_id,
    "retried": retried,
    "skipped": skipped,
    "total_targets": len(targets),
  }


def archive_attempt(
  st: ServerState,
  attempt_id: str,
  clock_fn: Callable[[], datetime],
  *,
  kind: str = "manual",
) -> dict[str, Any]:
  try:
    view = full_attempt_view(st.scheduler, attempt_id)
  except KeyError as exc:
    raise NotFound(f"attempt {attempt_id!r} not found") from exc
  payload_bytes = view.model_dump_json().encode("utf-8")
  now = clock_fn()
  try:
    st.scheduler.archive_attempt(
      attempt_id, at=now, kind=kind, payload_bytes=payload_bytes
    )
  except NotArchivableError as exc:
    raise Conflict(exc.reason) from exc
  state = st.scheduler.attempt_state(attempt_id)
  append_event(
    event_log_path_for(state),
    {
      "type": "archive",
      "attempt_id": attempt_id,
      "kind": kind,
      "at": clock_fn().isoformat(),
    },
  )
  counts = attempt_counts(st.scheduler, attempt_id)
  warnings: list[str] = []
  if counts.done_err > 0:
    warnings.append(
      f"{counts.done_err} trial(s) ended in done_err — archived "
      f"anyway (unarchive at any time to inspect / retry)"
    )
  snapshot = snapshot_attempt(st.scheduler, attempt_id)
  st.event_bus.publish(
    "attempt_archived", {"attempt_id": attempt_id, "kind": kind}
  )
  return {
    "attempt_id": attempt_id,
    "status": "archived",
    "kind": kind,
    "archived_at": (
      snapshot.archived_at.isoformat() if snapshot.archived_at else None
    ),
    "warnings": warnings,
    "final": snapshot.model_dump(mode="json"),
  }


def unarchive_attempt(
  st: ServerState,
  attempt_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  try:
    st.scheduler.unarchive_attempt(attempt_id)
  except KeyError as exc:
    raise NotFound(f"attempt {attempt_id!r} not found") from exc
  except NotArchivedError as exc:
    raise Conflict(
      f"attempt {exc.attempt_id!r} is not archived",
    ) from exc
  state = st.scheduler.attempt_state(attempt_id)
  append_event(
    event_log_path_for(state),
    {
      "type": "unarchive",
      "attempt_id": attempt_id,
      "at": clock_fn().isoformat(),
    },
  )
  snapshot = snapshot_attempt(st.scheduler, attempt_id)
  st.event_bus.publish("attempt_unarchived", {"attempt_id": attempt_id})
  return {
    "attempt_id": attempt_id,
    "status": "unarchived",
    "final": snapshot.model_dump(mode="json"),
  }


def _mk_attempt_id(label: str) -> str:
  return datetime.now(UTC).strftime(
    "att-%Y%m%dT%H%M%S%fZ-"
  ) + label.replace("/", "-").replace(" ", "-")
