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
  archive_payload_bytes,
  arena_members,
  full_job_view,
  job_counts,
  snapshot_job,
)
from dispatcher.core import clock
from dispatcher.core.event_log import (
  append_event_async,
  append_index_entry_async,
  event_log_path_for,
  replay_events,
)
from dispatcher.core.outcome import (
  instance_home_for,
  read_completion,
)
from dispatcher.core.readout import BadReadout, ReadoutSpec
from dispatcher.core.readout_service import ReadoutConflict
from dispatcher.core.scheduler import (
  AliasCollisionError,
  AliasFormatError,
  NotArchivableError,
  NotArchivedError,
)

if TYPE_CHECKING:
  from collections.abc import AsyncIterator, Callable
  from datetime import datetime

  from dispatcher.api.config import Config
  from dispatcher.api.wire import JobSummaryOut, ReadoutSummaryFn
  from dispatcher.core.event_bus import EventBus
  from dispatcher.core.models import InstanceView, JobState
  from dispatcher.core.readout_service import ReadoutService
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
  event_bus: EventBus
  tasks: list[asyncio.Task[None]] = field(default_factory=list)
  notify_sender: TelegramSender | None = None
  # Resolves an image ref to its immutable ID on the launcher.
  # None in fake-dispatch (test) mode — then nothing is pinned.
  resolve_image: Callable[[str], str] | None = None
  readouts: ReadoutService | None = None
  _seq: int = field(default=0)

  @property
  def readout_fn(self) -> ReadoutSummaryFn | None:
    """The projection wire.py needs, or None when no readout
    service is attached (tests) — then job rows simply carry no
    readout columns."""
    return None if self.readouts is None else self.readouts.summary

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


def normalize_arena(raw: Any) -> str:
  """Arena names are paths (`bench/v7/front5`); the tree is a
  naming convention, not server structure — depth and segment
  meaning are the operator's. Normalisation is deliberately
  minimal: trim, drop outer slashes, refuse empty segments (the
  `a//b` typo would silently mint a sibling tree)."""
  if not isinstance(raw, str):
    raise Invalid("arena must be a string")
  name = raw.strip().strip("/")
  if not name:
    return ""
  segments = [s.strip() for s in name.split("/")]
  if any(not s for s in segments):
    raise Invalid(f"arena path {raw!r} has an empty segment")
  return "/".join(segments)


def _validate_submit(st: ServerState, payload: dict[str, Any]) -> None:
  arena = payload.get("arena")
  if arena is not None:
    payload["arena"] = normalize_arena(arena)
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
    snapshot_job(st.scheduler, job.job_id, st.readout_fn).model_dump(
      mode="json"
    ),
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
    "arena",
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
  if "arena" in payload:
    # Empty (after normalisation) = leave the arena.
    payload["arena"] = normalize_arena(payload["arena"])
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
    snapshot_job(st.scheduler, job_id, st.readout_fn).model_dump(
      mode="json"
    ),
  )
  return snapshot_job(st.scheduler, job_id, st.readout_fn)


async def cancel_job(
  st: ServerState,
  job_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  if not st.scheduler.has_job(job_id):
    raise NotFound(f"job {job_id!r} not found")
  final_snapshot = snapshot_job(st.scheduler, job_id, st.readout_fn)
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


def _task_of_instance(
  st: ServerState, job_id: str, instance_id: str
) -> str | None:
  """Which task an instance belongs to, or None once a later
  instance has superseded it — the buckets hold one instance per
  task, so a requeued predecessor is simply not in them."""
  view = st.scheduler.job_view(job_id)
  for bucket in (
    "running",
    "done_ok",
    "done_err",
    "unknown",
    "ghosted",
  ):
    for task_id, tv in getattr(view, bucket).items():
      if tv.instance_id == instance_id:
        return task_id
  return None


async def get_instance_outcome(
  st: ServerState, job_id: str, instance_id: str
) -> dict[str, Any]:
  """The result envelope one instance wrote.

  Read from the instance's own home rather than the scheduler's
  cache: the cache holds the LATEST outcome per task, so a
  superseded instance — exactly the one a requeue investigation is
  about — would come back as its successor's result. The file is
  the per-instance record.

  Envelopes are immutable once written, which is what makes this
  worth caching hard on the client."""
  try:
    state = st.scheduler.job_state(job_id)
  except KeyError as exc:
    raise NotFound(f"job {job_id!r} not found") from exc
  home = instance_home_for(state.home_root, instance_id)
  snapshot = await asyncio.to_thread(read_completion, home)
  if snapshot is None or snapshot.outcome is None:
    raise NotFound(
      f"no readable outcome for instance {instance_id!r} "
      f"(never written, or not yet visible on this client)"
    )
  return {
    "job_id": job_id,
    "instance_id": instance_id,
    "task_id": _task_of_instance(st, job_id, instance_id),
    "home": str(home),
    "outcome": snapshot.outcome.model_dump(mode="json"),
  }


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
    view = full_job_view(st.scheduler, job_id, st.readout_fn)
  except KeyError as exc:
    raise NotFound(f"job {job_id!r} not found") from exc
  payload_bytes = archive_payload_bytes(view)
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
  snapshot = snapshot_job(st.scheduler, job_id, st.readout_fn)
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
  snapshot = snapshot_job(st.scheduler, job_id, st.readout_fn)
  st.event_bus.publish("job_unarchived", {"job_id": job_id})
  return {
    "job_id": job_id,
    "status": "unarchived",
    "final": snapshot.model_dump(mode="json"),
  }


# ── arena group ops: fan-outs over per-job ops ──────────────────
# An arena has no state of its own — every group op is a loop of
# the per-job op it names, so the arena layer can never desync
# from the mechanism layer.


def _arena_member_ids(st: ServerState, arena: str) -> list[str]:
  name = normalize_arena(arena)
  if not name:
    raise Invalid("arena name must be non-empty")
  members = arena_members(st.scheduler, name)
  if not members:
    raise NotFound(f"arena {name!r} has no jobs")
  return members


async def arena_set_paused(
  st: ServerState,
  arena: str,
  paused: bool,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  name = normalize_arena(arena)
  members = _arena_member_ids(st, name)
  changed: list[str] = []
  skipped: list[dict[str, str]] = []
  for aid in members:
    try:
      await patch_job(st, aid, {"paused": paused}, clock_fn)
      changed.append(aid)
    except OpError as exc:
      # Archived members etc. — the rest of the arena still moves.
      skipped.append({"job_id": aid, "reason": str(exc)})
  return {
    "arena": name,
    "paused": paused,
    "changed": changed,
    "skipped": skipped,
  }


async def arena_reclaim(
  st: ServerState,
  arena: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  name = normalize_arena(arena)
  members = _arena_member_ids(st, name)
  reclaimed: dict[str, list[str]] = {}
  skipped: list[dict[str, str]] = []
  for aid in members:
    try:
      result = await reclaim_job(st, aid, clock_fn)
      reclaimed[aid] = result["reclaimed"]
    except OpError as exc:
      # Same per-job precondition as /jobs/{id}/reclaim: not
      # paused → skipped, stated, never silently forced.
      skipped.append({"job_id": aid, "reason": str(exc)})
  return {
    "arena": name,
    "reclaimed": reclaimed,
    "skipped": skipped,
  }


async def arena_cancel(
  st: ServerState,
  arena: str,
  confirm: bool,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  name = normalize_arena(arena)
  members = _arena_member_ids(st, name)
  if not confirm:
    # Destructive fan-out keeps friction: name what would die,
    # do nothing.
    raise Conflict(
      f"arena {name!r} cancel would drop "
      f"{len(members)} job(s): {members} — resend with "
      f'{{"confirm": true}}'
    )
  cancelled: list[str] = []
  for aid in members:
    await cancel_job(st, aid, clock_fn)
    cancelled.append(aid)
  return {"arena": name, "cancelled": cancelled}


# ── readouts ────────────────────────────────────────────────────


def _require_readouts(st: ServerState) -> ReadoutService:
  if st.readouts is None:
    raise Internal("no readout service on this dispatcher")
  return st.readouts


def list_readouts(st: ServerState) -> dict[str, Any]:
  """Per arena: the per-instance readouts, and the `columns`
  function that turns their values into the job's columns."""
  service = _require_readouts(st)
  return {"by_arena": service.registry.snapshot()}


async def set_columns(
  st: ServerState, payload: dict[str, Any]
) -> dict[str, Any]:
  """Register an arena's `columns` function (empty source removes
  it).

  Unlike a readout, this takes effect on the next READ — the
  function runs in a resident process over values already on disk,
  so there is nothing to backfill and no instance has to re-run."""
  service = _require_readouts(st)
  if not isinstance(payload, dict):
    raise Invalid("body must be an object")
  arena = normalize_arena(payload.get("arena", ""))
  if not arena:
    raise Invalid("arena is required")
  source = str(payload.get("source") or "")
  try:
    service.registry.set_columns(arena, source)
  except BadReadout as exc:
    raise Invalid(str(exc)) from exc
  except OSError as exc:
    raise Internal(f"readout registry write failed: {exc}") from exc
  members = arena_members(st.scheduler, arena)
  service.invalidate(members)
  await service.refresh(members)
  return {
    "arena": arena,
    "columns_sha256": service.registry.snapshot()
    .get(arena, {})
    .get("columns_sha256", ""),
    "members": members,
    "errors": {
      aid: err
      for aid in members
      if (err := service.summary(aid).columns_error)
    },
  }


async def register_readout(
  st: ServerState, payload: dict[str, Any]
) -> dict[str, Any]:
  """Register a readout on an arena subtree.

  Takes effect for every instance dispatched from now on — they
  score themselves on the way out. Instances that already finished
  are NOT computed here: that starts containers, which is an
  operator's decision to make and watch, not a side effect of a
  POST. What it does do is READ the arena's existing values, so the
  response can say exactly which jobs need the retroactive command
  rather than leaving the operator to guess."""
  service = _require_readouts(st)
  if not isinstance(payload, dict):
    raise Invalid("body must be an object")
  arena = normalize_arena(payload.get("arena", ""))
  if not arena:
    raise Invalid(
      "arena is required — a readout belongs to a comparison unit, "
      "and jobs with no arena get none"
    )
  try:
    spec = ReadoutSpec.model_validate(
      {k: v for k, v in payload.items() if k != "arena"}
    )
  except BadReadout as exc:
    raise Invalid(str(exc)) from exc
  except Exception as exc:
    raise Invalid(f"bad readout spec: {exc}") from exc
  try:
    service.registry.register(arena, spec)
  except BadReadout as exc:
    raise Invalid(str(exc)) from exc
  except ReadoutConflict as exc:
    raise Conflict(str(exc)) from exc
  except OSError as exc:
    raise Internal(f"readout registry write failed: {exc}") from exc
  members = arena_members(st.scheduler, arena)
  await service.load_many(members)
  needs = service.jobs_with_lag(members)
  return {
    "arena": arena,
    "readout": spec.model_dump(mode="json"),
    "members": members,
    "needs_backfill": needs,
    "hint": (
      f"dispatcher readout {arena} --name {spec.name}" if needs else ""
    ),
  }


def resolve_compute_targets(
  st: ServerState, *, arena: str = "", job_id: str = ""
) -> list[str]:
  """Which jobs a retroactive request names.

  Separate from the streaming body on purpose: once a streaming
  response has started, an error can no longer become a status code —
  it would arrive as a truncated body with a 200 already on the
  wire. So every rejection happens here, before the first byte."""
  _require_readouts(st)
  if job_id:
    if not st.scheduler.has_job(job_id):
      raise NotFound(f"job {job_id!r} not found")
    return [job_id]
  node = normalize_arena(arena)
  if not node:
    raise Invalid("name an arena or a job_id")
  members = arena_members(st.scheduler, node)
  if not members:
    raise NotFound(f"arena {node!r} has no jobs")
  return members


async def compute_readouts(
  st: ServerState,
  job_ids: list[str],
  *,
  names: list[str] | None = None,
) -> AsyncIterator[dict[str, Any]]:
  """The retroactive pass, yielding one record per container pass.

  Streamed rather than returned: a backfill over an arena can take
  minutes, and the operator who asked for it should watch it rather
  than wait on a silent request. Driven entirely by this call — if
  the client disconnects the work stops, which is fine because it is
  idempotent (only missing pairs are ever computed)."""
  service = _require_readouts(st)
  async for report in service.compute(job_ids, names=names):
    yield {
      "job_id": report.job_id,
      "instances": report.instances,
      "written": report.written,
      "unreported": report.unreported,
      "remaining": report.remaining,
      "error": report.error,
    }


def unregister_readout(
  st: ServerState, arena: str, name: str
) -> dict[str, Any]:
  service = _require_readouts(st)
  node = normalize_arena(arena)
  if not node:
    raise Invalid("arena is required")
  try:
    removed = service.registry.unregister(node, name)
  except OSError as exc:
    raise Internal(f"readout registry write failed: {exc}") from exc
  if not removed:
    raise NotFound(f"readout {name!r} is not registered at {node!r}")
  # Values stay on disk: they are the record of finished work, not a
  # cache of a registration.
  return {"arena": node, "name": name, "status": "unregistered"}


async def get_job_readouts(st: ServerState, job_id: str) -> dict[str, Any]:
  """Every value this job carries, per readout, keyed by instance."""
  service = _require_readouts(st)
  if not st.scheduler.has_job(job_id):
    raise NotFound(f"job {job_id!r} not found")
  loaded = await service.load(job_id)
  return {
    "job_id": job_id,
    "values": {
      name: [v.model_dump(mode="json") for v in values.values()]
      for name, values in loaded.items()
    },
    "summary": service.summary(job_id).model_dump(mode="json"),
  }


def _mk_job_id(label: str) -> str:
  # KST timestamp; no Z suffix — these are +09:00 times.
  return clock.now().strftime("job-%Y%m%dT%H%M%S%f-") + label.replace(
    "/", "-"
  ).replace(" ", "-")
