"""Wire shapes: request/response DTOs, snapshot builders, SSE
framing. Pure projections over scheduler/metrics state — no
mutations, no side effects."""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from dispatcher.api.settings import Settings
from dispatcher.core.metrics import JobMetrics, MetricsCache

if TYPE_CHECKING:
  from dispatcher.core.models import InstanceView
  from dispatcher.core.scheduler import Scheduler


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


class JobSummaryOut(BaseModel):
  job_id: str
  label: str
  weight: int
  max_concurrent: int | None
  pause_on_error: bool | None
  paused: bool
  counts: JobCountsOut
  home_root: str
  alias: str
  scope: str = ""
  tags: list[str] = Field(default_factory=list)
  pool: str = "default"
  image_id: str = ""
  source_sha256: str = ""
  archived_at: datetime | None = None
  archive_kind: str = ""


class JobSummaryWithMetricsOut(JobSummaryOut):
  metrics: JobMetrics


class InstanceViewOut(BaseModel):
  instance_id: str
  host: str
  dispatched_at: datetime
  # Outcome `values` for terminal instances whose envelope is cached;
  # None before then.
  values: dict[str, float] | None = None


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


class MonitorOut(ClusterSnapshotOut):
  jobs: list[JobSummaryWithMetricsOut]


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


def snapshot_job(scheduler: Scheduler, job_id: str) -> JobSummaryOut:
  state = scheduler.job_state(job_id)
  return JobSummaryOut(
    job_id=job_id,
    label=state.label,
    weight=state.weight,
    max_concurrent=state.max_concurrent,
    pause_on_error=state.pause_on_error,
    paused=state.paused,
    counts=job_counts(scheduler, job_id),
    home_root=str(state.home_root),
    alias=state.alias,
    scope=state.scope,
    tags=list(state.tags),
    pool=state.pool or "default",
    image_id=state.image_id,
    source_sha256=state.source_sha256,
    archived_at=state.archived_at,
    archive_kind=state.archive_kind or "",
  )


def snapshot_job_with_metrics(
  scheduler: Scheduler, metrics: MetricsCache, job_id: str
) -> JobSummaryWithMetricsOut:
  base = snapshot_job(scheduler, job_id)
  return JobSummaryWithMetricsOut(
    **base.model_dump(), metrics=metrics.get(job_id)
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


def full_job_view(scheduler: Scheduler, job_id: str) -> FullJobOut:
  view = scheduler.job_view(job_id)
  base = snapshot_job(scheduler, job_id)

  def _values_for(task_id: str) -> dict[str, float] | None:
    outcome = scheduler.outcome_of(job_id, task_id)
    if outcome is None:
      return None
    return {
      k: float(v)
      for k, v in outcome.values.items()
      if isinstance(v, (int, float)) and not isinstance(v, bool)
    }

  def _bucket(
    d: dict[str, InstanceView],
  ) -> dict[str, InstanceViewOut]:
    return {
      tn: InstanceViewOut(
        instance_id=tv.instance_id,
        host=tv.host,
        dispatched_at=tv.dispatched_at,
        values=_values_for(tn),
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


def full_job_bytes(scheduler: Scheduler, job_id: str) -> bytes:
  if scheduler.is_archived(job_id):
    return scheduler.archived_bytes(job_id)
  return full_job_view(scheduler, job_id).model_dump_json().encode("utf-8")


def build_full_jobs_body(scheduler: Scheduler, aids: list[str]) -> bytes:
  """Concatenated JSON array; archived jobs splice their
  cached bytes verbatim so the hot path never rebuilds them."""
  parts: list[bytes] = [b"["]
  for i, aid in enumerate(aids):
    if i > 0:
      parts.append(b",")
    parts.append(full_job_bytes(scheduler, aid))
  parts.append(b"]")
  return b"".join(parts)


def sse(event: str, payload: BaseModel | dict[str, Any]) -> str:
  """One SSE frame: `event:` + JSON `data:` + blank line."""
  if isinstance(payload, BaseModel):
    data = payload.model_dump_json()
  else:
    data = json.dumps(payload, default=str)
  return f"event: {event}\ndata: {data}\n\n"
