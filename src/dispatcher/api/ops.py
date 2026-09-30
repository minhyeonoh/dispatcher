"""Operations on the dispatcher's state — the bodies behind the
HTTP endpoints, HTTP-free so loops and future non-HTTP callers
use the same code path. Failures raise OpError subclasses; the
server maps them to status codes at the edge."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dispatcher.api.settings import (
  Settings,
  SettingsPatch,
  apply_patch_pure,
  save_settings,
)
from dispatcher.api.wire import (
  RetryDoneErrRequest,
  full_job_view,
  job_counts,
  snapshot_job,
  snapshot_job_with_metrics,
)
from dispatcher.core import clock
from dispatcher.core.event_log import (
  append_event_async,
  append_index_entry_async,
  event_log_path_for,
  replay_events,
)
from dispatcher.core.scheduler import (
  AliasCollisionError,
  AliasFormatError,
  NotArchivableError,
  NotArchivedError,
)

if TYPE_CHECKING:
  from collections.abc import Callable
  from datetime import datetime

  from dispatcher.api.config import Config
  from dispatcher.api.wire import JobSummaryOut
  from dispatcher.core.event_bus import EventBus
  from dispatcher.core.metrics import MetricsCache
  from dispatcher.core.models import InstanceView, JobState
  from dispatcher.core.runtime import DispatcherRuntime
  from dispatcher.core.scheduler import Scheduler
  from dispatcher.services.notify import TelegramSender

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
  config: Config
  settings: Settings
  scheduler: Scheduler
  runtime: DispatcherRuntime
  metrics: MetricsCache
  event_bus: EventBus
  tasks: list[asyncio.Task[None]] = field(default_factory=list)
  notify_sender: TelegramSender | None = None
  # Resolves an image ref to its immutable ID on the launcher.
  # None in fake-dispatch (test) mode — then nothing is pinned.
  resolve_image: Callable[[str], str] | None = None
  _seq: int = field(default=0)

  def next_instance_id(self, task_id: str) -> str:
    """`<task[:32]>__<7-digit seq>` — deterministic, monotonic."""
    self._seq += 1
    truncated = task_id[:32].rstrip("_-")
    return f"{truncated}__{self._seq:07d}"

  def advance_seq_to(self, seq: int) -> None:
    """The counter lives in memory only; restore pushes it past
    every name on disk — re-minting a used name would read the
    OLD instance dir's outcome as the new instance's before it runs."""
    self._seq = max(self._seq, seq)


def filter_presets_path(config: Config) -> Path:
  return config.data_dir / "filter-presets.json"


def apply_settings(st: ServerState, patch: SettingsPatch) -> None:
  """Scheduler side effects + pure settings merge + persist.
  This is the ONLY path that saves settings to disk — autotune's
  in-memory cap mirroring must never persist (ratchet; see
  api/settings.py)."""
  if patch.hosts is not None:
    for host, host_patch in patch.hosts.items():
      new = st.scheduler.set_host_settings(
        host,
        max_concurrent=host_patch.max_concurrent,
        active=host_patch.active,
        alive=host_patch.alive,
      )
      # Mirror into settings so /state readback stays truthful.
      st.settings.hosts[host] = new.model_copy()
      docker_events = st.runtime._docker_events
      if docker_events is not None:
        # Streams stay up for inactive hosts too: instances already
        # dispatched there still emit die events we must catch.
        docker_events.ensure_host(host)
  if patch.max_concurrent is not None:
    st.scheduler.set_max_concurrent(patch.max_concurrent)
    st.settings.max_concurrent = patch.max_concurrent
  if patch.pool_caps is not None:
    normalised = {name: int(cap) for name, cap in patch.pool_caps.items()}
    st.scheduler.set_pool_caps(normalised)
    st.settings.pool_caps = dict(normalised)
  apply_patch_pure(st.settings, patch)
  save_settings(st.config.data_dir, st.settings)


def _prepare_submit(payload: dict[str, Any]) -> dict[str, Any]:
  out = dict(payload)
  out.setdefault("job_id", _mk_job_id(payload.get("label", "job")))
  out.setdefault("submitted_at", clock.now().isoformat())
  return out


def _validate_submit(st: ServerState, payload: dict[str, Any]) -> None:
  home_root = payload.get("home_root")
  if not isinstance(home_root, str) or not home_root:
    raise Invalid("home_root must be a non-empty string")
  if not Path(home_root).is_absolute():
    raise Invalid(
      f"home_root must be absolute (got {home_root!r})",
    )
  # One home_root per job, ever: two jobs sharing one
  # would interleave instance dirs and merge their event logs — the
  # kind of silent cross-contamination no readout would catch.
  resolved = str(Path(home_root))
  for aid in st.scheduler.iter_job_ids():
    other = st.scheduler.job_state(aid)
    if str(other.home_root) == resolved:
      raise Conflict(
        (f"home_root {home_root!r} already belongs to job {aid!r}"),
      )
  task_ids = payload.get("task_ids")
  if not isinstance(task_ids, list) or not task_ids:
    raise Invalid("task_ids must be a non-empty list")
  if len(set(task_ids)) != len(task_ids):
    raise Invalid("task_ids contains duplicates")
  payloads = payload.get("payloads") or {}
  if not isinstance(payloads, dict):
    raise Invalid("payloads must be an object")
  stray = set(payloads) - set(task_ids)
  if stray:
    raise Invalid(
      (
        f"payloads for unknown tasks: {sorted(stray)[:5]} — "
        f"likely a typo; every payload key must be in task_ids"
      ),
    )


SOURCE_TAR_FILENAME = ".source.tar"
_SOURCE_TAR_MAX_BYTES = 256 * 1024 * 1024


def _decode_source_tar(payload: dict[str, Any]) -> bytes | None:
  """Pop + decode `source_tar_b64`. The archive is both the code
  DELIVERY (mounted ro into every instance) and the arm RECORD (it
  outlives any docker prune on plain NFS)."""
  raw = payload.pop("source_tar_b64", None)
  if raw is None:
    return None
  if not isinstance(raw, str):
    raise Invalid("source_tar_b64 must be a base64 string")
  try:
    blob = base64.b64decode(raw, validate=True)
  except (binascii.Error, ValueError) as exc:
    raise Invalid(f"source_tar_b64 is not valid base64: {exc}") from exc
  if not blob:
    raise Invalid("source_tar_b64 decodes to zero bytes")
  if len(blob) > _SOURCE_TAR_MAX_BYTES:
    raise Invalid(
      f"source archive too large ({len(blob)} bytes > "
      f"{_SOURCE_TAR_MAX_BYTES}); ship deps in the image, not "
      f"in the source archive"
    )
  return blob


async def submit_job(
  st: ServerState,
  payload: dict[str, Any],
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  # INVARIANT: no await between validation and scheduler.submit —
  # the home_root-uniqueness check reads the scheduler, and an
  # interleaved concurrent submit could otherwise double-claim
  # one home. Everything before scheduler.submit stays sync.
  source_blob = _decode_source_tar(payload)
  if source_blob is None and st.settings.require_source:
    raise Invalid(
      "this dispatcher requires source_tar_b64 (settings."
      "require_source) — the frozen archive is the experiment's "
      "code record"
    )
  _validate_submit(st, payload)
  prepared = _prepare_submit(payload)
  if source_blob is not None:
    prepared["source_sha256"] = hashlib.sha256(source_blob).hexdigest()
  # Pin the image to its immutable ID so a tag re-pushed
  # mid-sweep can't change what runs. Loud 400 when the image
  # isn't on the launcher — better than instances dying host by
  # host later.
  if st.resolve_image is not None and not prepared.get("image_id"):
    container = prepared.get("container") or {}
    ref = container.get("image") if isinstance(container, dict) else None
    if ref:
      try:
        prepared["image_id"] = st.resolve_image(ref)
      except RuntimeError as exc:
        raise Invalid(str(exc)) from exc
  # Alias minted BEFORE the submit event so the handle the
  # operator uses lives on disk from birth.
  if not prepared.get("alias"):
    prepared["alias"] = st.scheduler.mint_alias(prepared["job_id"])
  events: list[dict[str, Any]] = [{"type": "submit", **prepared}]
  replay_out = replay_events(events)
  if replay_out is None:  # pragma: no cover — no cancel possible
    raise Internal("internal")
  job, _ = replay_out
  try:
    st.scheduler.submit(job)
  except AliasCollisionError as exc:
    raise Conflict(f"alias {exc.alias!r} already in use") from exc
  except ValueError as exc:
    raise Conflict(str(exc)) from exc
  # Source archive written AFTER scheduler accepted (a 409 must
  # leave nothing on disk) and BEFORE the event log (a job
  # whose log exists must have its recorded archive). A write
  # failure unwinds the submit — accepting a job without
  # the record it promised would be a silent contract break.
  if source_blob is not None:
    source_path = job.home_root / SOURCE_TAR_FILENAME

    def _write_source() -> None:
      source_path.parent.mkdir(parents=True, exist_ok=True)
      tmp = source_path.with_suffix(".tar.tmp")
      tmp.write_bytes(source_blob)
      tmp.replace(source_path)

    try:
      # Off the loop (R5): this can be hundreds of MB onto NFS.
      await asyncio.to_thread(_write_source)
    except OSError as exc:
      st.scheduler.cancel(job.job_id)
      raise Internal(f"source archive write failed: {exc}") from exc
  # Log written AFTER scheduler accepted, so a 409 leaves no
  # orphan log; index after the log so a crash between the two is
  # recoverable from the log side.
  log_path = event_log_path_for(job)
  for ev in events:
    await append_event_async(log_path, ev)
  await append_index_entry_async(
    st.config.data_dir,
    {
      "event": "submit",
      "job_id": job.job_id,
      "log_path": str(log_path),
      "at": clock_fn().isoformat(),
    },
  )
  st.event_bus.publish(
    "job_submitted",
    snapshot_job_with_metrics(
      st.scheduler, st.metrics, job.job_id
    ).model_dump(mode="json"),
  )
  return {
    "job_id": job.job_id,
    "alias": job.alias,
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


async def patch_job(
  st: ServerState,
  job_id: str,
  payload: dict[str, Any],
  clock_fn: Callable[[], datetime],
) -> JobSummaryOut:
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
  if not st.scheduler.has_job(job_id):
    raise NotFound(f"job {job_id!r} not found")
  if st.scheduler.is_archived(job_id):
    raise Conflict(
      (
        f"job {job_id!r} is archived — POST /jobs/{job_id}/unarchive first"
      ),
    )
  # Alias goes through the uniqueness-enforcing entry point.
  alias_value = payload.pop("alias", None)
  if alias_value is not None:
    if not isinstance(alias_value, str):
      raise Invalid("alias must be a string")
    try:
      st.scheduler.set_alias(job_id, alias_value)
    except AliasFormatError as exc:
      raise Invalid(
        (f"alias {exc.alias!r} invalid: non-empty, ≤120 chars"),
      ) from exc
    except AliasCollisionError as exc:
      other = st.scheduler.job_id_of_alias(exc.alias)
      raise Conflict(
        (f"alias {exc.alias!r} already used by job {other!r}"),
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
      st.scheduler.patch(job_id, **payload)
  except ValueError as exc:
    raise Invalid(str(exc)) from exc
  if alias_value is not None:
    payload["alias"] = alias_value
  state = st.scheduler.job_state(job_id)
  log_path = event_log_path_for(state)
  at = clock_fn().isoformat()
  for knob, value in payload.items():
    await append_event_async(
      log_path,
      {
        "type": "patch",
        "job_id": job_id,
        "at": at,
        knob: value,
      },
    )
  st.event_bus.publish(
    "job_patched",
    snapshot_job_with_metrics(st.scheduler, st.metrics, job_id).model_dump(
      mode="json"
    ),
  )
  return snapshot_job(st.scheduler, job_id)


async def cancel_job(
  st: ServerState,
  job_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  if not st.scheduler.has_job(job_id):
    raise NotFound(f"job {job_id!r} not found")
  final_snapshot = snapshot_job(st.scheduler, job_id)
  state = st.scheduler.job_state(job_id)
  runtime_state = st.scheduler.cancel(job_id)
  # Fire-and-forget remote kill; the GC catches stragglers.
  st.runtime.fire_kill_instances(runtime_state.running)
  log_path = event_log_path_for(state)
  await append_event_async(
    log_path,
    {
      "type": "cancel",
      "job_id": job_id,
      "at": clock_fn().isoformat(),
    },
  )
  await append_index_entry_async(
    st.config.data_dir,
    {
      "event": "cancel",
      "job_id": job_id,
      "at": clock_fn().isoformat(),
    },
  )
  final_json = final_snapshot.model_dump(mode="json")
  st.event_bus.publish(
    "job_cancelled",
    {"job_id": job_id, "final": final_json},
  )
  return {
    "job_id": job_id,
    "status": "cancelled",
    "final": final_json,
    "killed": len(runtime_state.running),
  }


def _require_paused_live(
  st: ServerState, job_id: str, verb: str
) -> JobState:
  try:
    state = st.scheduler.job_state(job_id)
  except KeyError as exc:
    raise NotFound(f"job {job_id!r} not found") from exc
  if st.scheduler.is_archived(job_id):
    raise Conflict(
      (f"job {job_id!r} is archived; POST /jobs/{job_id}/unarchive first"),
    )
  if not state.paused:
    raise Conflict(
      (
        f'job {job_id!r} is not paused; PATCH {{"paused": '
        f"true}} first so {verb} doesn't race the dispatch loop"
      ),
    )
  return state


async def reclaim_instance(
  st: ServerState,
  job_id: str,
  instance_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  """Kill ONE running instance and put its task back on pending —
  the surgical version of /reclaim, for a zombie sitting on a
  slot without taking the job's healthy instances down with it.

  No pause required: the freed task may re-dispatch on the next
  tick with a fresh instance name and home, which is usually the
  point ("kill it and run it again"). The old container's late
  die event / outcome cannot touch the successor — the
  stale-observation guard drops signals whose instance_id no
  longer occupies the bucket."""
  try:
    state = st.scheduler.job_state(job_id)
  except KeyError as exc:
    raise NotFound(f"job {job_id!r} not found") from exc
  if st.scheduler.is_archived(job_id):
    raise Conflict(
      f"job {job_id!r} is archived; POST /jobs/{job_id}/unarchive first"
    )
  view = st.scheduler.job_view(job_id)
  match: tuple[str, InstanceView] | None = None
  for task_id, tv in view.running.items():
    if tv.instance_id == instance_id:
      match = (task_id, tv)
      break
  if match is None:
    # Say where it actually is — "already finished" and "typo"
    # need different operator reactions.
    for bucket in ("done_ok", "done_err", "unknown", "ghosted"):
      if any(
        tv.instance_id == instance_id
        for tv in getattr(view, bucket).values()
      ):
        raise Conflict(
          f"instance {instance_id!r} is not running (state={bucket})"
        )
    raise NotFound(f"instance {instance_id!r} not found in job {job_id!r}")
  task_id, tv = match
  # Kill first, then reclaim — same order as job-level
  # reclaim, so the container set is already being torn down
  # when the task becomes dispatchable again.
  st.runtime.fire_kill_instances({task_id: tv})
  reclaimed = st.scheduler.reclaim_from_running(job_id, task_id)
  if not reclaimed:
    # Natural completion won the race between snapshot and now.
    raise Conflict(
      f"instance {instance_id!r} completed before it could be reclaimed"
    )
  await append_event_async(
    event_log_path_for(state),
    {
      "type": "reclaim",
      "job_id": job_id,
      "task_id": task_id,
      "instance_id": instance_id,
      "at": clock_fn().isoformat(),
    },
  )
  st.event_bus.publish(
    "job_reclaimed",
    {
      "job_id": job_id,
      "reclaimed": [task_id],
      "skipped_completed": [],
    },
  )
  return {
    "job_id": job_id,
    "task_id": task_id,
    "instance_id": instance_id,
    "status": "reclaimed",
  }


async def reclaim_job(
  st: ServerState,
  job_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  state = _require_paused_live(st, job_id, "reclaim")
  # Snapshot before any mutation so the remote kill and the
  # scheduler bookkeeping see the same running set.
  view = st.scheduler.job_view(job_id)
  running_snapshot: dict[str, InstanceView] = dict(view.running)
  st.runtime.fire_kill_instances(running_snapshot)
  log_path = event_log_path_for(state)
  at = clock_fn().isoformat()
  reclaimed: list[str] = []
  skipped: list[str] = []
  for task_id, tv in running_snapshot.items():
    if st.scheduler.reclaim_from_running(job_id, task_id):
      reclaimed.append(task_id)
      await append_event_async(
        log_path,
        {
          "type": "reclaim",
          "job_id": job_id,
          "task_id": task_id,
          "instance_id": tv.instance_id,
          "at": at,
        },
      )
    else:
      skipped.append(task_id)
  st.event_bus.publish(
    "job_reclaimed",
    {
      "job_id": job_id,
      "reclaimed": reclaimed,
      "skipped_completed": skipped,
    },
  )
  return {
    "job_id": job_id,
    "reclaimed": reclaimed,
    "skipped_completed": skipped,
    "total": len(running_snapshot),
  }


async def retry_done_err(
  st: ServerState,
  job_id: str,
  payload: RetryDoneErrRequest | None,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  state = _require_paused_live(st, job_id, "retry")
  payload = payload or RetryDoneErrRequest()
  view = st.scheduler.job_view(job_id)
  done_err_snapshot: dict[str, InstanceView] = dict(view.done_err)

  targets: list[tuple[str, InstanceView]] = []
  if payload.instance_ids:
    wanted = set(payload.instance_ids)
    for task_id, tv in done_err_snapshot.items():
      if tv.instance_id in wanted:
        targets.append((task_id, tv))
  else:
    since_epoch = (
      payload.since_iso.timestamp()
      if payload.since_iso is not None
      else None
    )
    for task_id, tv in done_err_snapshot.items():
      if payload.host is not None and tv.host != payload.host:
        continue
      if since_epoch is not None:
        outcome_path = state.home_root / tv.instance_id / "outcome.json"
        try:
          mtime = await asyncio.to_thread(
            lambda p=outcome_path: p.stat().st_mtime
          )
          if mtime < since_epoch:
            continue
        except OSError:
          # Nothing to compare against — do NOT retry, so the
          # operator's filter stays predictable.
          continue
      targets.append((task_id, tv))

  log_path = event_log_path_for(state)
  at = clock_fn().isoformat()
  retried: list[str] = []
  skipped: list[str] = []
  for task_id, tv in targets:
    if st.scheduler.retry_from_done_err(job_id, task_id):
      retried.append(task_id)
      st.metrics.undo_done_err(job_id)
      await append_event_async(
        log_path,
        {
          "type": "retry",
          "job_id": job_id,
          "task_id": task_id,
          "instance_id": tv.instance_id,
          "at": at,
        },
      )
    else:
      skipped.append(task_id)
  st.event_bus.publish(
    "job_retried",
    {
      "job_id": job_id,
      "retried": retried,
      "skipped": skipped,
    },
  )
  return {
    "job_id": job_id,
    "retried": retried,
    "skipped": skipped,
    "total_targets": len(targets),
  }


async def archive_job(
  st: ServerState,
  job_id: str,
  clock_fn: Callable[[], datetime],
  *,
  kind: str = "manual",
) -> dict[str, Any]:
  try:
    view = full_job_view(st.scheduler, job_id)
  except KeyError as exc:
    raise NotFound(f"job {job_id!r} not found") from exc
  payload_bytes = view.model_dump_json().encode("utf-8")
  now = clock_fn()
  try:
    st.scheduler.archive_job(
      job_id, at=now, kind=kind, payload_bytes=payload_bytes
    )
  except NotArchivableError as exc:
    raise Conflict(exc.reason) from exc
  state = st.scheduler.job_state(job_id)
  await append_event_async(
    event_log_path_for(state),
    {
      "type": "archive",
      "job_id": job_id,
      "kind": kind,
      "at": clock_fn().isoformat(),
    },
  )
  counts = job_counts(st.scheduler, job_id)
  warnings: list[str] = []
  if counts.done_err > 0:
    warnings.append(
      f"{counts.done_err} instance(s) ended in done_err — archived "
      f"anyway (unarchive at any time to inspect / retry)"
    )
  snapshot = snapshot_job(st.scheduler, job_id)
  st.event_bus.publish("job_archived", {"job_id": job_id, "kind": kind})
  return {
    "job_id": job_id,
    "status": "archived",
    "kind": kind,
    "archived_at": (
      snapshot.archived_at.isoformat() if snapshot.archived_at else None
    ),
    "warnings": warnings,
    "final": snapshot.model_dump(mode="json"),
  }


async def unarchive_job(
  st: ServerState,
  job_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  try:
    st.scheduler.unarchive_job(job_id)
  except KeyError as exc:
    raise NotFound(f"job {job_id!r} not found") from exc
  except NotArchivedError as exc:
    raise Conflict(
      f"job {exc.job_id!r} is not archived",
    ) from exc
  state = st.scheduler.job_state(job_id)
  await append_event_async(
    event_log_path_for(state),
    {
      "type": "unarchive",
      "job_id": job_id,
      "at": clock_fn().isoformat(),
    },
  )
  snapshot = snapshot_job(st.scheduler, job_id)
  st.event_bus.publish("job_unarchived", {"job_id": job_id})
  return {
    "job_id": job_id,
    "status": "unarchived",
    "final": snapshot.model_dump(mode="json"),
  }


def _mk_job_id(label: str) -> str:
  # KST timestamp; no Z suffix — these are +09:00 times.
  return clock.now().strftime("job-%Y%m%dT%H%M%S%f-") + label.replace(
    "/", "-"
  ).replace(" ", "-")
