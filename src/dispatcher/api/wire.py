"""Wire shapes: request/response DTOs, snapshot builders, SSE
framing. Pure projections over scheduler state — no mutations,
no side effects."""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from dispatcher.api.settings import Settings
from dispatcher.core.models import BlockReason
from dispatcher.core.readout import ReadoutAggregate, ReadoutJobSummary

if TYPE_CHECKING:
  from collections.abc import Callable

  from dispatcher.core.models import InstanceView
  from dispatcher.core.scheduler import Scheduler

  ReadoutSummaryFn = Callable[[str], ReadoutJobSummary]
  """`job_id → its readout cell`. Passed in rather than imported so
  wire.py stays a projection over scheduler state: None (tests,
  restore) simply means no readout columns."""


class RetryDoneErrRequest(BaseModel):
  """Empty body = retry every done_err instance. `instance_ids`
  overrides the host/since filters."""

  model_config = ConfigDict(extra="forbid")

  host: str | None = None
  since_iso: datetime | None = None
  instance_ids: list[str] | None = None


class JobCountsOut(BaseModel):
  pending: int
  running: int
  done_ok: int
  done_err: int
  ghosted: int
  unknown: int
  total: int


class ReadoutCellOut(BaseModel):
  """The readout half of a job row — the ONLY part that can change
  after a job is archived.

  Split out so both sides of the archive cache derive from one
  definition: the frozen blob excludes exactly these fields, and the
  serve path splices exactly these back on. A hand-kept list on each
  side would eventually disagree, and the symptom is a row whose
  numbers change when you click it."""

  # Derived columns: `{readout name: roll-up}` over this job's
  # finished instances. Empty when the job's arena registers none.
  readouts: dict[str, ReadoutAggregate] = Field(default_factory=dict)
  # Finished (instance, readout) pairs with no value yet. 0 = caught
  # up; null = this job's values aren't loaded, so the honest answer
  # is unknown.
  readout_lag: int | None = None
  # Operator-defined columns (an arena's `columns` function). The
  # table shows these when present; `readouts` above stays as the
  # facts (n / errors / nulls) either way.
  columns: dict[str, Any] = Field(default_factory=dict)
  columns_stale: bool = False
  columns_error: str = ""
  # Which `columns` function produced them. Rows sharing this are
  # comparable column-by-column; rows that differ are not, and the
  # table has to say so rather than line them up under one header.
  columns_source_sha256: str = ""

  @classmethod
  def of(cls, cell: ReadoutJobSummary) -> ReadoutCellOut:
    return cls(
      readouts=cell.aggregates,
      readout_lag=cell.lag,
      columns=cell.columns,
      columns_stale=cell.columns_stale,
      columns_error=cell.columns_error,
      columns_source_sha256=cell.columns_source_sha256,
    )


READOUT_FIELDS: set[str] = set(ReadoutCellOut.model_fields)


class JobSummaryOut(ReadoutCellOut):
  job_id: str
  label: str
  weight: int
  max_concurrent: int | None
  pause_on_error: bool | None
  paused: bool
  counts: JobCountsOut
  submitted_at: datetime
  home_root: str
  alias: str
  arena: str = ""
  pool: str = "default"
  # What holds this job's pending work back right now; null when it
  # is simply waiting its turn (or has nothing pending). Straight
  # from the scheduler's own decision — see models.BlockReason.
  blocked: BlockReason | None = None
  image_id: str = ""
  source_sha256: str = ""
  archived_at: datetime | None = None
  archive_kind: str = ""


class InstanceViewOut(BaseModel):
  instance_id: str
  host: str
  dispatched_at: datetime
  # Null while running, or when nothing on disk dates the finish
  # (an instance parked in unknown after a restart).
  finished_at: datetime | None = None


class FullJobOut(JobSummaryOut):
  pending: list[str] = Field(default_factory=list)
  running: dict[str, InstanceViewOut] = Field(default_factory=dict)
  done_ok: dict[str, InstanceViewOut] = Field(default_factory=dict)
  done_err: dict[str, InstanceViewOut] = Field(default_factory=dict)
  ghosted: dict[str, InstanceViewOut] = Field(default_factory=dict)
  unknown: dict[str, InstanceViewOut] = Field(default_factory=dict)


class ClusterSnapshotOut(BaseModel):
  """Boot identity + the live Settings document + running
  counters. `settings` is the operator-tunable state verbatim —
  what you read here is exactly what PATCH /settings edits."""

  self_host: str
  settings: Settings
  running_total: int
  running_per_host: dict[str, int]
  running_per_pool: dict[str, int] = Field(default_factory=dict)


class StateOut(ClusterSnapshotOut):
  jobs: list[JobSummaryOut]


class ArenaSummaryOut(BaseModel):
  """One derived group row: an arena exists iff a job names it,
  so there is nothing to create or delete — only to read."""

  arena: str
  jobs: int
  paused_jobs: int
  archived_jobs: int
  counts: JobCountsOut


class ArenaDetailOut(ArenaSummaryOut):
  members: list[JobSummaryOut]


class HealthOut(BaseModel):
  status: str


# ── snapshot builders ────────────────────────────────────────────


def job_counts(scheduler: Scheduler, job_id: str) -> JobCountsOut:
  state = scheduler.job_state(job_id)
  view = scheduler.job_view(job_id)
  return JobCountsOut(
    pending=len(view.pending),
    running=len(view.running),
    done_ok=len(view.done_ok),
    done_err=len(view.done_err),
    ghosted=len(view.ghosted),
    unknown=len(view.unknown),
    total=len(state.task_ids),
  )


def snapshot_job(
  scheduler: Scheduler,
  job_id: str,
  readouts: ReadoutSummaryFn | None = None,
) -> JobSummaryOut:
  state = scheduler.job_state(job_id)
  cell = readouts(job_id) if readouts is not None else ReadoutJobSummary()
  return JobSummaryOut(
    job_id=job_id,
    label=state.label,
    weight=state.weight,
    max_concurrent=state.max_concurrent,
    pause_on_error=state.pause_on_error,
    paused=state.paused,
    counts=job_counts(scheduler, job_id),
    submitted_at=state.submitted_at,
    home_root=str(state.home_root),
    alias=state.alias,
    arena=state.arena,
    pool=state.pool or "default",
    blocked=scheduler.block_reason(job_id),
    image_id=state.image_id,
    source_sha256=state.source_sha256,
    archived_at=state.archived_at,
    archive_kind=state.archive_kind or "",
    **ReadoutCellOut.of(cell).model_dump(),
  )


def arena_members(scheduler: Scheduler, arena: str) -> list[str]:
  """Membership is derived per read — submission order, live and
  archived alike (an arena spans weeks; hiding archived members
  would silently shrink the comparison set).

  Arena names are paths, and a path names its SUBTREE: members
  of `bench/v7` are jobs at `bench/v7` and below. The prefix
  check is segment-aware — `bench/v7` must not match
  `bench/v70`."""
  prefix = arena + "/"
  return [
    aid
    for aid in scheduler.all_job_ids()
    if (a := scheduler.job_state(aid).arena) == arena
    or a.startswith(prefix)
  ]


def _arena_summary(
  scheduler: Scheduler, arena: str, member_ids: list[str]
) -> ArenaSummaryOut:
  agg = dict.fromkeys(
    (
      "pending",
      "running",
      "done_ok",
      "done_err",
      "ghosted",
      "unknown",
      "total",
    ),
    0,
  )
  paused = archived = 0
  for aid in member_ids:
    c = job_counts(scheduler, aid)
    for key in agg:
      agg[key] += getattr(c, key)
    if scheduler.is_archived(aid):
      archived += 1
    elif scheduler.job_paused(aid):
      paused += 1
  return ArenaSummaryOut(
    arena=arena,
    jobs=len(member_ids),
    paused_jobs=paused,
    archived_jobs=archived,
    counts=JobCountsOut(**agg),
  )


def snapshot_arenas(scheduler: Scheduler) -> list[ArenaSummaryOut]:
  grouped: dict[str, list[str]] = {}
  for aid in scheduler.all_job_ids():
    arena = scheduler.job_state(aid).arena
    if arena:
      grouped.setdefault(arena, []).append(aid)
  return [
    _arena_summary(scheduler, arena, ids) for arena, ids in grouped.items()
  ]


def snapshot_arena(
  scheduler: Scheduler,
  arena: str,
  member_ids: list[str],
  readouts: ReadoutSummaryFn | None = None,
) -> ArenaDetailOut:
  base = _arena_summary(scheduler, arena, member_ids)
  return ArenaDetailOut(
    **base.model_dump(),
    members=[snapshot_job(scheduler, aid, readouts) for aid in member_ids],
  )


def cluster_snapshot(
  self_host: str, settings: Settings, scheduler: Scheduler
) -> ClusterSnapshotOut:
  return ClusterSnapshotOut(
    self_host=self_host,
    settings=settings,
    running_total=scheduler.running_total,
    running_per_host=scheduler.running_per_host(),
    running_per_pool=scheduler.pool_running_snapshot(),
  )


def full_job_view(
  scheduler: Scheduler,
  job_id: str,
  readouts: ReadoutSummaryFn | None = None,
) -> FullJobOut:
  view = scheduler.job_view(job_id)
  base = snapshot_job(scheduler, job_id, readouts)

  def _bucket(
    d: dict[str, InstanceView],
  ) -> dict[str, InstanceViewOut]:
    return {
      tn: InstanceViewOut(
        instance_id=tv.instance_id,
        host=tv.host,
        dispatched_at=tv.dispatched_at,
        finished_at=tv.finished_at,
      )
      for tn, tv in d.items()
    }

  return FullJobOut(
    **base.model_dump(),
    pending=list(view.pending),
    running=_bucket(view.running),
    done_ok=_bucket(view.done_ok),
    done_err=_bucket(view.done_err),
    ghosted=_bucket(view.ghosted),
    unknown=_bucket(view.unknown),
  )


def archive_payload_bytes(view: FullJobOut) -> bytes:
  """What `archive_job` freezes: everything EXCEPT the readout cell.

  Those fields are the only part of a job row that still moves after
  archiving — the retroactive pass can add values, and a `columns`
  function can be registered or rewritten at any time. Freezing them
  made the jobs list (projected live) and the detail endpoint (served
  from the blob) show different numbers for the same job."""
  return view.model_dump_json(exclude=READOUT_FIELDS).encode("utf-8")


def splice_readout_cell(blob: bytes, cell: ReadoutJobSummary) -> bytes:
  """Put today's readout cell onto a frozen blob.

  Surgery rather than parse-and-redump: the blob exists so the hot
  path never re-serialises a job's instances, and doing that to
  attach five small fields would undo the whole point. Both ends are
  ours, so the shape is known — a JSON object, so there is one
  closing brace to insert before."""
  stripped = blob.rstrip()
  if not stripped.endswith(b"}") or stripped == b"{}":
    return blob  # pragma: no cover — a FullJobOut is never empty
  tail = ReadoutCellOut.of(cell).model_dump_json().encode("utf-8")
  return stripped[:-1] + b"," + tail[1:]


def full_job_bytes(
  scheduler: Scheduler,
  job_id: str,
  readouts: ReadoutSummaryFn | None = None,
) -> bytes:
  if scheduler.is_archived(job_id):
    cell = (
      readouts(job_id) if readouts is not None else ReadoutJobSummary()
    )
    return splice_readout_cell(scheduler.archived_bytes(job_id), cell)
  return (
    full_job_view(scheduler, job_id, readouts)
    .model_dump_json()
    .encode("utf-8")
  )


def build_full_jobs_body(
  scheduler: Scheduler,
  aids: list[str],
  readouts: ReadoutSummaryFn | None = None,
) -> bytes:
  """Concatenated JSON array; archived jobs splice their
  cached bytes verbatim so the hot path never rebuilds them.

  Those cached bytes carry the readout columns as of the last time
  values were written for the job — the readout service refreshes
  them when a backfill adds any, so an archived job's columns are
  never silently stale."""
  parts: list[bytes] = [b"["]
  for i, aid in enumerate(aids):
    if i > 0:
      parts.append(b",")
    parts.append(full_job_bytes(scheduler, aid, readouts))
  parts.append(b"]")
  return b"".join(parts)


def sse(event: str, payload: BaseModel | dict[str, Any]) -> str:
  """One SSE frame: `event:` + JSON `data:` + blank line."""
  if isinstance(payload, BaseModel):
    data = payload.model_dump_json()
  else:
    data = json.dumps(payload, default=str)
  return f"event: {event}\ndata: {data}\n\n"
