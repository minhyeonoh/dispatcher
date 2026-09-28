"""FastAPI wrapper around Scheduler + DispatcherRuntime.

`create_app(config)` returns an app whose lifespan owns the
scheduler, the runtime loop, the docker-events streams, and the
background loops (resolver / GC / auto-archive / notify), all
under a supervisor that restarts on unexpected death.

Endpoints:
  POST   /attempts                    submit
  GET    /attempts[?full=1&scope=…]   list (summaries or full)
  GET    /attempts/{id}               full view
  PATCH  /attempts/{id}               knobs
  DELETE /attempts/{id}               cancel + remote kill
  POST   /attempts/{id}/reclaim       running → pending (paused only)
  POST   /attempts/{id}/retry-done-err
  POST   /attempts/{id}/archive · /unarchive
  GET    /state · /monitor · /monitor/stream · /health
  PATCH  /settings
  GET/PUT /filter-presets             opaque UI blob
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import (
  RedirectResponse,
  Response,
  StreamingResponse,
)
from pydantic import (
  BaseModel,
  ConfigDict,
  Field,
  NonNegativeInt,
  PositiveFloat,
  PositiveInt,
)

from dispatcher.containers import (
  DockerEventStreamManager,
  census_host,
)
from dispatcher.event_bus import EventBus
from dispatcher.event_log import (
  ReplayError,
  append_event,
  append_index_entry,
  event_log_path_for,
  find_event_logs,
  read_events,
  replay_events,
  scan_outcomes,
  seq_in_trial_name,
)
from dispatcher.host_autotune import (
  HOST_METRICS_FILENAME,
  HostAutotuneConfig,
  HostAutotunePatch,
  HostAutotuneState,
  autotune_tick,
  load_ring,
  truncate_ring_file,
)
from dispatcher.metrics import AttemptMetrics, MetricsCache
from dispatcher.models import HostSettings, TrialView
from dispatcher.notify import (
  NotifyConfig,
  NotifyManager,
  NotifyPatch,
  TelegramSender,
  telegram_bot_token_from_env,
)
from dispatcher.outcome import CompletionSnapshot
from dispatcher.runtime import DispatcherRuntime
from dispatcher.scheduler import (
  AliasCollisionError,
  AliasFormatError,
  NotArchivableError,
  NotArchivedError,
  Scheduler,
)

if TYPE_CHECKING:
  from collections.abc import Awaitable, Callable

  from dispatcher.models import AttemptState

logger = logging.getLogger(__name__)


# ── config ───────────────────────────────────────────────────────


class StateReconciliationConfig(BaseModel):
  """Resolver knobs (periodic unknown/ghosted sweep)."""

  max_concurrent_probes: PositiveInt = 16
  seconds_between_probes: PositiveFloat = 30.0


class OrphanGCConfig(BaseModel):
  enabled: bool = True
  seconds_between_sweeps: PositiveFloat = 30.0
  min_container_age_s: PositiveFloat = 90.0
  """Containers younger than this are never touched — protects
  the dispatch-race window."""


class ArchiveConfig(BaseModel):
  auto_after_days: NonNegativeInt = 0
  """0 = auto-archive disabled."""

  scan_interval_seconds: PositiveFloat = 3600.0


class DispatcherConfig(BaseModel):
  model_config = ConfigDict(arbitrary_types_allowed=True)

  max_concurrent: NonNegativeInt
  hosts: dict[str, HostSettings]
  self_host: str
  # Dispatcher-owned state (attempts index, blobs). Attempt homes
  # live wherever each submission says.
  data_dir: Path
  tick_interval: float = 0.5
  use_docker_events: bool = True
  state_reconciliation: StateReconciliationConfig = Field(
    default_factory=StateReconciliationConfig
  )
  orphan_gc: OrphanGCConfig = Field(default_factory=OrphanGCConfig)
  host_autotune: HostAutotuneConfig = Field(
    default_factory=HostAutotuneConfig
  )
  pool_caps: dict[str, NonNegativeInt] = Field(default_factory=dict)
  notify: NotifyConfig = Field(default_factory=NotifyConfig)
  archive: ArchiveConfig = Field(default_factory=ArchiveConfig)
  # Optional built web UI to serve at /ui.
  ui_dist: Path | None = None


class HostSettingsPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  max_concurrent: NonNegativeInt | None = None
  active: bool | None = None
  alive: bool | None = None


class StateReconciliationPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  max_concurrent_probes: PositiveInt | None = None
  seconds_between_probes: PositiveFloat | None = None


class OrphanGCPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  enabled: bool | None = None
  seconds_between_sweeps: PositiveFloat | None = None
  min_container_age_s: PositiveFloat | None = None


class ArchivePatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  auto_after_days: NonNegativeInt | None = None
  scan_interval_seconds: PositiveFloat | None = None


class SettingsPatch(BaseModel):
  """Partial update; unknown fields are rejected, not dropped."""

  model_config = ConfigDict(extra="forbid")

  hosts: dict[str, HostSettingsPatch] | None = None
  max_concurrent: NonNegativeInt | None = None
  state_reconciliation: StateReconciliationPatch | None = None
  orphan_gc: OrphanGCPatch | None = None
  host_autotune: HostAutotunePatch | None = None
  notify: NotifyPatch | None = None
  archive: ArchivePatch | None = None
  # Whole-dict replacement; {} clears all caps.
  pool_caps: dict[str, NonNegativeInt] | None = None


class RetryDoneErrRequest(BaseModel):
  """Empty body = retry every done_err trial. `trial_names`
  overrides the host/since filters."""

  model_config = ConfigDict(extra="forbid")

  host: str | None = None
  since_iso: datetime | None = None
  trial_names: list[str] | None = None


# ── in-process state ─────────────────────────────────────────────


@dataclass
class ServerState:
  config: DispatcherConfig
  scheduler: Scheduler
  runtime: DispatcherRuntime
  metrics: MetricsCache
  event_bus: EventBus
  runtime_task: asyncio.Task[None] | None = None
  resolver_task: asyncio.Task[None] | None = None
  gc_task: asyncio.Task[None] | None = None
  autotune_task: asyncio.Task[None] | None = None
  archive_task: asyncio.Task[None] | None = None
  notify_task: asyncio.Task[None] | None = None
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


def _get_state(app: FastAPI) -> ServerState:
  return app.state.dispatcher  # type: ignore[no-any-return]


def _filter_presets_path(config: DispatcherConfig) -> Path:
  return config.data_dir / "filter-presets.json"


def _pool_caps_path(config: DispatcherConfig) -> Path:
  return config.data_dir / "pool-caps.json"


def _load_pool_caps(config: DispatcherConfig) -> dict[str, int]:
  """Corrupt/missing blob → {} (never blocks startup)."""
  path = _pool_caps_path(config)
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


def _save_pool_caps(
  config: DispatcherConfig, caps: dict[str, int]
) -> None:
  path = _pool_caps_path(config)
  path.parent.mkdir(parents=True, exist_ok=True)
  tmp = path.with_suffix(path.suffix + ".tmp")
  try:
    tmp.write_text(json.dumps(caps, indent=2, sort_keys=True))
    tmp.replace(path)
  except OSError as exc:
    logger.warning("pool_caps blob write failed path=%s err=%s", path, exc)


# ── wire models ──────────────────────────────────────────────────


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


def _attempt_counts(
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


def _snapshot_attempt(
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
    counts=_attempt_counts(scheduler, attempt_id),
    home_root=str(state.home_root),
    alias=state.alias,
    scope=state.scope,
    tags=list(state.tags),
    pool=state.pool or "default",
    archived_at=state.archived_at,
    archive_kind=state.archive_kind or "",
  )


def _snapshot_attempt_with_metrics(
  scheduler: Scheduler, metrics: MetricsCache, attempt_id: str
) -> AttemptSummaryWithMetricsOut:
  base = _snapshot_attempt(scheduler, attempt_id)
  return AttemptSummaryWithMetricsOut(
    **base.model_dump(), metrics=metrics.get(attempt_id)
  )


def _cluster_snapshot(st: ServerState) -> ClusterSnapshotOut:
  return ClusterSnapshotOut(
    config=ClusterConfigOut.model_validate(
      st.config, from_attributes=True
    ),
    running_total=st.scheduler.running_total,
    running_per_host=st.scheduler.running_per_host(),
    running_per_pool=st.scheduler.pool_running_snapshot(),
  )


def _full_attempt_view(
  scheduler: Scheduler, attempt_id: str
) -> FullAttemptOut:
  view = scheduler.attempt_view(attempt_id)
  base = _snapshot_attempt(scheduler, attempt_id)

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


def _full_attempt_bytes(scheduler: Scheduler, attempt_id: str) -> bytes:
  if scheduler.is_archived(attempt_id):
    return scheduler.archived_bytes(attempt_id)
  return (
    _full_attempt_view(scheduler, attempt_id)
    .model_dump_json()
    .encode("utf-8")
  )


def _build_full_attempts_body(
  scheduler: Scheduler, aids: list[str]
) -> bytes:
  """Concatenated JSON array; archived attempts splice their
  cached bytes verbatim so the hot path never rebuilds them."""
  parts: list[bytes] = [b"["]
  for i, aid in enumerate(aids):
    if i > 0:
      parts.append(b",")
    parts.append(_full_attempt_bytes(scheduler, aid))
  parts.append(b"]")
  return b"".join(parts)


# ── app factory ──────────────────────────────────────────────────


def create_app(
  config: DispatcherConfig,
  *,
  dispatch: Callable[..., Any] | None = None,
  poll: Callable[[Path], CompletionSnapshot | None] | None = None,
  clock: Callable[[], datetime] | None = None,
  heartbeat_interval: float = 15.0,
) -> FastAPI:
  clock_fn = clock or (lambda: datetime.now(UTC))

  @contextlib.asynccontextmanager
  async def lifespan(app: FastAPI):
    metrics = MetricsCache()
    event_bus = EventBus()
    persisted_caps = _load_pool_caps(config)
    if persisted_caps:
      config.pool_caps = {
        **dict(config.pool_caps),
        **persisted_caps,
      }
    scheduler = Scheduler(
      max_concurrent=config.max_concurrent,
      hosts=config.hosts,
      clock=clock_fn,
      name_gen=lambda task: server_state.next_trial_name(task),
      pool_caps=dict(config.pool_caps),
    )
    docker_events: DockerEventStreamManager | None = None
    runtime = DispatcherRuntime(
      scheduler,
      self_host=config.self_host,
      dispatch=dispatch,
      poll=poll,
      tick_interval=config.tick_interval,
      metrics=metrics,
      event_bus=event_bus,
    )
    if config.use_docker_events:
      docker_events = DockerEventStreamManager(
        self_host=config.self_host,
        on_event=runtime.handle_docker_die,
        on_alive_change=runtime.handle_alive_change,
      )
      runtime._docker_events = docker_events
    server_state = ServerState(
      config=config,
      scheduler=scheduler,
      runtime=runtime,
      metrics=metrics,
      event_bus=event_bus,
    )
    app.state.dispatcher = server_state
    # Restore before anything can dispatch: rebuild AttemptStates
    # from event logs, buckets from outcome files, and push the
    # trial-name counter past every name on disk.
    server_state.advance_seq_to(
      _restore_attempts_from_disk(scheduler, metrics, config.data_dir)
    )
    if docker_events is not None:
      # Census BEFORE the stream: docker's event buffer may have
      # rolled past completions that fired during downtime.
      await _run_startup_census(runtime, config)
      docker_events.start(list(config.hosts.keys()))
    # Blocking resolver pass so restart-adopted running trials
    # re-book their slots before the dispatch loop reads them.
    startup_outcomes = await runtime.resolve_state_once(
      max_concurrent_probes=(
        config.state_reconciliation.max_concurrent_probes
      ),
    )
    if any(v > 0 for v in startup_outcomes.values()):
      logger.info(
        "resolver (startup): %s",
        ", ".join(
          f"{k}={v}" for k, v in startup_outcomes.items() if v > 0
        ),
      )
    server_state.runtime_task = asyncio.create_task(
      _supervised("runtime_task", runtime.run_forever)
    )
    server_state.resolver_task = asyncio.create_task(
      _supervised(
        "resolver_task",
        lambda: _resolver_forever(runtime, config),
      )
    )
    server_state.gc_task = asyncio.create_task(
      _supervised("gc_task", lambda: _gc_forever(runtime, config))
    )
    metrics_file = config.data_dir / HOST_METRICS_FILENAME
    initial_ring = load_ring(
      metrics_file, config.host_autotune.ring_buffer_size
    )
    try:
      truncate_ring_file(metrics_file, initial_ring)
    except OSError:
      logger.exception(
        "host_autotune: initial ring truncate failed (%s)",
        metrics_file,
      )
    autotune_state = HostAutotuneState(
      metrics_file=metrics_file, ring=initial_ring
    )
    server_state.autotune_task = asyncio.create_task(
      _supervised(
        "autotune_task",
        lambda: _autotune_forever(server_state, autotune_state),
      )
    )
    server_state.archive_task = asyncio.create_task(
      _supervised(
        "archive_task",
        lambda: _archive_forever(app, server_state, clock_fn),
      )
    )
    server_state.notify_sender = TelegramSender(
      telegram_bot_token_from_env()
    )
    notify_manager = NotifyManager(
      bus=event_bus,
      scheduler=scheduler,
      config=config.notify,
      sender=server_state.notify_sender,
      metrics=metrics,
    )
    server_state.notify_task = asyncio.create_task(
      _supervised("notify_task", notify_manager.run)
    )
    try:
      yield
    finally:
      for task in (
        server_state.runtime_task,
        server_state.resolver_task,
        server_state.gc_task,
        server_state.autotune_task,
        server_state.archive_task,
        server_state.notify_task,
      ):
        if task is not None:
          task.cancel()
          with contextlib.suppress(asyncio.CancelledError):
            await task
      if server_state.notify_sender is not None:
        await server_state.notify_sender.close()
      if docker_events is not None:
        await docker_events.stop()

  app = FastAPI(title="dispatcher", version="0.1.0", lifespan=lifespan)
  app.add_middleware(GZipMiddleware, minimum_size=1024)

  # Handlers are async so their (pure-sync) bodies run on the
  # event-loop thread that owns the runtime tick — no scheduler
  # access can interleave mid-handler.

  @app.get("/health")
  async def health() -> HealthOut:
    return HealthOut(status="ok")

  @app.get("/state")
  async def get_state() -> StateOut:
    st = _get_state(app)
    cluster = _cluster_snapshot(st)
    return StateOut(
      **cluster.model_dump(),
      attempts=[
        _snapshot_attempt(st.scheduler, aid)
        for aid in st.scheduler.all_attempt_ids()
      ],
    )

  @app.get("/monitor")
  async def get_monitor() -> MonitorOut:
    st = _get_state(app)
    cluster = _cluster_snapshot(st)
    return MonitorOut(
      **cluster.model_dump(),
      attempts=[
        _snapshot_attempt_with_metrics(st.scheduler, st.metrics, aid)
        for aid in st.scheduler.all_attempt_ids()
      ],
    )

  @app.get("/monitor/stream")
  async def monitor_stream():
    st = _get_state(app)
    sub = st.event_bus.subscribe(maxsize=256)

    state_change_events = frozenset(
      {
        "trial_dispatched",
        "trial_completed",
        "trial_reclassified",
        "trial_requeued",
        "attempt_paused_on_error",
        "attempt_submitted",
        "attempt_patched",
        "attempt_cancelled",
        "attempt_reclaimed",
        "attempt_retried",
        "attempt_archived",
        "attempt_unarchived",
        "attempt_drained",
      }
    )

    async def gen():
      try:
        yield _sse("snapshot", _cluster_snapshot(st))
        for aid in list(st.scheduler.all_attempt_ids()):
          yield _sse(
            "attempt_updated",
            _snapshot_attempt_with_metrics(st.scheduler, st.metrics, aid),
          )
        while True:
          ev = None
          with anyio.move_on_after(heartbeat_interval) as scope:
            ev = await sub.get()
          if scope.cancel_called or ev is None:
            yield _sse("heartbeat", {"at": clock_fn().isoformat()})
            continue
          if ev.type in state_change_events:
            aid = ev.payload.get("attempt_id")
            if aid and st.scheduler.has_attempt(aid):
              yield _sse(
                "attempt_updated",
                _snapshot_attempt_with_metrics(
                  st.scheduler, st.metrics, aid
                ),
              )
            yield _sse("cluster_updated", _cluster_snapshot(st))
          yield _sse(ev.type, ev.payload)
      finally:
        sub.close()

    return StreamingResponse(
      gen(),
      media_type="text/event-stream",
      headers={
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",
      },
    )

  @app.get("/attempts", response_model=None)
  async def list_attempts(
    full: bool = False, scope: str = ""
  ) -> list[AttemptSummaryOut] | Response:
    st = _get_state(app)
    aids = st.scheduler.all_attempt_ids()
    if scope:
      aids = [
        aid
        for aid in aids
        if st.scheduler.attempt_state(aid).scope == scope
      ]
    if full:
      body = await asyncio.to_thread(
        _build_full_attempts_body, st.scheduler, aids
      )
      return Response(content=body, media_type="application/json")
    return await asyncio.to_thread(
      lambda: [_snapshot_attempt(st.scheduler, aid) for aid in aids]
    )

  @app.get("/attempts/{attempt_id}", response_model=None)
  async def get_attempt(
    attempt_id: str,
  ) -> FullAttemptOut | Response:
    st = _get_state(app)
    if not st.scheduler.has_attempt(attempt_id):
      raise HTTPException(
        status_code=404, detail=f"attempt {attempt_id!r} not found"
      )
    if st.scheduler.is_archived(attempt_id):
      return Response(
        content=st.scheduler.archived_bytes(attempt_id),
        media_type="application/json",
      )
    return _full_attempt_view(st.scheduler, attempt_id)

  @app.post("/attempts")
  async def submit_attempt(
    payload: dict[str, Any],
  ) -> dict[str, Any]:
    return _submit_payload(app, payload, clock_fn)

  @app.patch("/attempts/{attempt_id}")
  async def patch_attempt(
    attempt_id: str, payload: dict[str, Any]
  ) -> AttemptSummaryOut:
    return _patch_attempt(app, attempt_id, payload, clock_fn)

  @app.delete("/attempts/{attempt_id}")
  async def cancel_attempt(attempt_id: str) -> dict[str, Any]:
    return _cancel_attempt(app, attempt_id, clock_fn)

  @app.post("/attempts/{attempt_id}/reclaim")
  async def reclaim_attempt(attempt_id: str) -> dict[str, Any]:
    return _reclaim_attempt(app, attempt_id, clock_fn)

  @app.post("/attempts/{attempt_id}/retry-done-err")
  async def retry_done_err(
    attempt_id: str, payload: RetryDoneErrRequest | None = None
  ) -> dict[str, Any]:
    return _retry_done_err(app, attempt_id, payload, clock_fn)

  @app.post("/attempts/{attempt_id}/archive")
  async def archive_attempt_ep(attempt_id: str) -> dict[str, Any]:
    return _archive_attempt(app, attempt_id, clock_fn, kind="manual")

  @app.post("/attempts/{attempt_id}/unarchive")
  async def unarchive_attempt_ep(
    attempt_id: str,
  ) -> dict[str, Any]:
    return _unarchive_attempt(app, attempt_id, clock_fn)

  @app.patch("/settings")
  async def patch_settings(
    payload: SettingsPatch,
  ) -> ClusterConfigOut:
    _apply_settings_patch(app, payload)
    return _cluster_snapshot(_get_state(app)).config

  @app.get("/filter-presets")
  async def get_filter_presets() -> dict[str, Any]:
    path = _filter_presets_path(config)
    if not path.is_file():
      return {"presets": []}
    try:
      return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
      raise HTTPException(
        status_code=500, detail=f"failed reading {path}: {exc}"
      ) from exc

  @app.put("/filter-presets")
  async def put_filter_presets(
    payload: dict[str, Any],
  ) -> dict[str, Any]:
    """Opaque UI blob; whole-document last-write-wins. Atomic
    write so a crash can't corrupt it."""
    path = _filter_presets_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)
    return payload

  _mount_ui(app, config.ui_dist)

  return app


# ── settings ─────────────────────────────────────────────────────


def _apply_settings_patch(app: FastAPI, patch: SettingsPatch) -> None:
  st = _get_state(app)
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
    _save_pool_caps(st.config, normalised)
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


# ── submit ───────────────────────────────────────────────────────


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
    raise HTTPException(
      status_code=400, detail="home_root must be a non-empty string"
    )
  if not Path(home_root).is_absolute():
    raise HTTPException(
      status_code=400,
      detail=f"home_root must be absolute (got {home_root!r})",
    )
  # One home_root per attempt, ever: two attempts sharing one
  # would interleave trial dirs and merge their event logs — the
  # kind of silent cross-contamination no readout would catch.
  resolved = str(Path(home_root))
  for aid in st.scheduler.iter_attempt_ids():
    other = st.scheduler.attempt_state(aid)
    if str(other.home_root) == resolved:
      raise HTTPException(
        status_code=409,
        detail=(
          f"home_root {home_root!r} already belongs to attempt {aid!r}"
        ),
      )
  task_list = payload.get("task_list")
  if not isinstance(task_list, list) or not task_list:
    raise HTTPException(
      status_code=400, detail="task_list must be a non-empty list"
    )
  if len(set(task_list)) != len(task_list):
    raise HTTPException(
      status_code=400, detail="task_list contains duplicates"
    )
  payloads = payload.get("payloads") or {}
  if not isinstance(payloads, dict):
    raise HTTPException(
      status_code=400, detail="payloads must be an object"
    )
  stray = set(payloads) - set(task_list)
  if stray:
    raise HTTPException(
      status_code=400,
      detail=(
        f"payloads for unknown tasks: {sorted(stray)[:5]} — "
        f"likely a typo; every payload key must be in task_list"
      ),
    )


def _submit_payload(
  app: FastAPI,
  payload: dict[str, Any],
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  st = _get_state(app)
  _validate_submit(st, payload)
  prepared = _prepare_submit(payload)
  # Alias minted BEFORE the submit event so the handle the
  # operator uses lives on disk from birth.
  if not prepared.get("alias"):
    prepared["alias"] = st.scheduler.mint_alias(prepared["attempt_id"])
  events: list[dict[str, Any]] = [{"type": "submit", **prepared}]
  replay_out = replay_events(events)
  if replay_out is None:  # pragma: no cover — no cancel possible
    raise HTTPException(status_code=500, detail="internal")
  attempt, _ = replay_out
  try:
    st.scheduler.submit(attempt)
  except AliasCollisionError as exc:
    raise HTTPException(
      status_code=409, detail=f"alias {exc.alias!r} already in use"
    ) from exc
  except ValueError as exc:
    raise HTTPException(status_code=409, detail=str(exc)) from exc
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
    _snapshot_attempt_with_metrics(
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


def _patch_attempt(
  app: FastAPI,
  attempt_id: str,
  payload: dict[str, Any],
  clock_fn: Callable[[], datetime],
) -> AttemptSummaryOut:
  if not isinstance(payload, dict) or not payload:
    raise HTTPException(
      status_code=400,
      detail="body must be a non-empty {knob: value} object",
    )
  unknown = set(payload) - _ALLOWED_PATCH_KNOBS
  if unknown:
    raise HTTPException(
      status_code=400,
      detail=(
        f"unknown knob(s): {sorted(unknown)}; "
        f"allowed: {sorted(_ALLOWED_PATCH_KNOBS)}"
      ),
    )
  st = _get_state(app)
  if not st.scheduler.has_attempt(attempt_id):
    raise HTTPException(
      status_code=404, detail=f"attempt {attempt_id!r} not found"
    )
  if st.scheduler.is_archived(attempt_id):
    raise HTTPException(
      status_code=409,
      detail=(
        f"attempt {attempt_id!r} is archived — POST "
        f"/attempts/{attempt_id}/unarchive first"
      ),
    )
  # Alias goes through the uniqueness-enforcing entry point.
  alias_value = payload.pop("alias", None)
  if alias_value is not None:
    if not isinstance(alias_value, str):
      raise HTTPException(status_code=400, detail="alias must be a string")
    try:
      st.scheduler.set_alias(attempt_id, alias_value)
    except AliasFormatError as exc:
      raise HTTPException(
        status_code=400,
        detail=(f"alias {exc.alias!r} invalid: non-empty, ≤120 chars"),
      ) from exc
    except AliasCollisionError as exc:
      other = st.scheduler.attempt_id_of_alias(exc.alias)
      raise HTTPException(
        status_code=409,
        detail=(f"alias {exc.alias!r} already used by attempt {other!r}"),
      ) from exc
  if "pool" in payload:
    raw_pool = payload["pool"]
    if not isinstance(raw_pool, str):
      raise HTTPException(status_code=400, detail="pool must be a string")
    payload["pool"] = raw_pool.strip() or "default"
  if "tags" in payload:
    raw = payload["tags"]
    if not isinstance(raw, list) or not all(
      isinstance(t, str) for t in raw
    ):
      raise HTTPException(
        status_code=400, detail="tags must be a list of strings"
      )
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
    raise HTTPException(status_code=400, detail=str(exc)) from exc
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
    _snapshot_attempt_with_metrics(
      st.scheduler, st.metrics, attempt_id
    ).model_dump(mode="json"),
  )
  return _snapshot_attempt(st.scheduler, attempt_id)


def _cancel_attempt(
  app: FastAPI,
  attempt_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  st = _get_state(app)
  if not st.scheduler.has_attempt(attempt_id):
    raise HTTPException(
      status_code=404, detail=f"attempt {attempt_id!r} not found"
    )
  final_snapshot = _snapshot_attempt(st.scheduler, attempt_id)
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
    raise HTTPException(
      status_code=404, detail=f"attempt {attempt_id!r} not found"
    ) from exc
  if st.scheduler.is_archived(attempt_id):
    raise HTTPException(
      status_code=409,
      detail=(
        f"attempt {attempt_id!r} is archived; POST "
        f"/attempts/{attempt_id}/unarchive first"
      ),
    )
  if not state.paused:
    raise HTTPException(
      status_code=409,
      detail=(
        f'attempt {attempt_id!r} is not paused; PATCH {{"paused": '
        f"true}} first so {verb} doesn't race the dispatch loop"
      ),
    )
  return state


def _reclaim_attempt(
  app: FastAPI,
  attempt_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  st = _get_state(app)
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


def _retry_done_err(
  app: FastAPI,
  attempt_id: str,
  payload: RetryDoneErrRequest | None,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  st = _get_state(app)
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


def _archive_attempt(
  app: FastAPI,
  attempt_id: str,
  clock_fn: Callable[[], datetime],
  *,
  kind: str = "manual",
) -> dict[str, Any]:
  st = _get_state(app)
  try:
    view = _full_attempt_view(st.scheduler, attempt_id)
  except KeyError as exc:
    raise HTTPException(
      status_code=404, detail=f"attempt {attempt_id!r} not found"
    ) from exc
  payload_bytes = view.model_dump_json().encode("utf-8")
  now = clock_fn()
  try:
    st.scheduler.archive_attempt(
      attempt_id, at=now, kind=kind, payload_bytes=payload_bytes
    )
  except NotArchivableError as exc:
    raise HTTPException(status_code=409, detail=exc.reason) from exc
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
  counts = _attempt_counts(st.scheduler, attempt_id)
  warnings: list[str] = []
  if counts.done_err > 0:
    warnings.append(
      f"{counts.done_err} trial(s) ended in done_err — archived "
      f"anyway (unarchive at any time to inspect / retry)"
    )
  snapshot = _snapshot_attempt(st.scheduler, attempt_id)
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


def _unarchive_attempt(
  app: FastAPI,
  attempt_id: str,
  clock_fn: Callable[[], datetime],
) -> dict[str, Any]:
  st = _get_state(app)
  try:
    st.scheduler.unarchive_attempt(attempt_id)
  except KeyError as exc:
    raise HTTPException(
      status_code=404, detail=f"attempt {attempt_id!r} not found"
    ) from exc
  except NotArchivedError as exc:
    raise HTTPException(
      status_code=409,
      detail=f"attempt {exc.attempt_id!r} is not archived",
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
  snapshot = _snapshot_attempt(st.scheduler, attempt_id)
  st.event_bus.publish("attempt_unarchived", {"attempt_id": attempt_id})
  return {
    "attempt_id": attempt_id,
    "status": "unarchived",
    "final": snapshot.model_dump(mode="json"),
  }


# ── restore ──────────────────────────────────────────────────────


def _restore_attempts_from_disk(
  scheduler: Scheduler, metrics: MetricsCache, data_dir: Path
) -> int:
  """Rebuild every live attempt from its event log + on-disk
  outcomes. Malformed logs are skipped (one corrupt attempt must
  not block startup). Returns the highest trial-name counter seen
  so the namer never re-mints a used name."""
  max_seq = 0
  for log_path in find_event_logs(data_dir):
    try:
      events = read_events(log_path)
    except (OSError, ValueError) as exc:
      logger.warning("restore: cannot read %s: %s", log_path, exc)
      continue
    if not events:
      continue
    try:
      result = replay_events(events)
    except ReplayError as exc:
      logger.warning("restore: replay failed for %s: %s", log_path, exc)
      continue
    if result is None:
      continue  # cancelled — log stays as audit trail
    attempt, dispatch_log = result
    completed = scan_outcomes(attempt.home_root)
    done_ok: dict[str, TrialView] = {}
    done_err: dict[str, TrialView] = {}
    unknown: dict[str, TrialView] = {}
    # A task can carry several dispatches (infra requeue appends
    # without erasing). Later dispatches supersede earlier ones —
    # counting both would put one task in two buckets at once.
    latest_trial: dict[str, str] = {}
    for entry in dispatch_log:
      # Counter first, before any continue: a name handed out is
      # a name taken.
      max_seq = max(max_seq, seq_in_trial_name(entry.trial_name))
      if entry.attempt_id != attempt.attempt_id:
        continue
      done_ok.pop(entry.task_name, None)
      done_err.pop(entry.task_name, None)
      unknown.pop(entry.task_name, None)
      latest_trial[entry.task_name] = entry.trial_name
      outcome = completed.get(entry.trial_name)
      if outcome is not None:
        error_present = (not outcome.ok) or outcome.error is not None
        if error_present and outcome.infra:
          # The same requeue rule the live path applies — a trial
          # that completed during the shutdown window must not be
          # frozen as done_err while its in-flight cohort gets
          # requeued. Not bucketing routes it to pending.
          logger.warning(
            "restore: requeueing infra failure attempt=%s task=%s",
            attempt.attempt_id,
            entry.task_name,
          )
          continue
        tv = TrialView(
          task_name=entry.task_name,
          state="done_err" if error_present else "done_ok",
          trial_name=entry.trial_name,
          host=entry.host,
          dispatched_at=entry.dispatched_at,
        )
        (done_err if error_present else done_ok)[entry.task_name] = tv
      else:
        # Dispatched, no readable outcome — could be running,
        # crashed, or NFS-lagged. Park in unknown; the startup
        # resolver reclassifies on evidence.
        unknown[entry.task_name] = TrialView(
          task_name=entry.task_name,
          state="unknown",
          trial_name=entry.trial_name,
          host=entry.host,
          dispatched_at=entry.dispatched_at,
        )
    needs_alias_backfill = not attempt.alias
    try:
      scheduler.restore(
        attempt,
        running={},
        done_ok=done_ok,
        done_err=done_err,
        unknown=unknown,
      )
    except (ValueError, AliasCollisionError) as exc:
      logger.warning(
        "restore: attempt %s not restored (%s)",
        attempt.attempt_id,
        exc,
      )
      continue
    if needs_alias_backfill:
      try:
        append_event(
          log_path,
          {
            "type": "patch",
            "attempt_id": attempt.attempt_id,
            "at": attempt.submitted_at.isoformat(),
            "alias": attempt.alias,
          },
        )
      except OSError as exc:
        logger.warning(
          "restore: alias backfill failed for %s: %s",
          attempt.attempt_id,
          exc,
        )
    # Seed caches from the LATEST trial of each task only — a
    # superseded (requeued) trial's outcome must not win, and
    # must not double-count in metrics.
    trial_to_task = {trial: task for task, trial in latest_trial.items()}
    for trial_name, outcome in completed.items():
      task_name = trial_to_task.get(trial_name)
      if task_name is None:
        continue
      # Skip outcomes routed back to pending by the infra rule.
      if task_name not in done_ok and task_name not in done_err:
        continue
      scheduler.seed_outcome(attempt.attempt_id, task_name, outcome)
      error_present = (not outcome.ok) or outcome.error is not None
      metrics.record_completion(
        attempt.attempt_id,
        CompletionSnapshot(
          outcome_exists=True,
          error_present=error_present,
          values={
            k: float(v)
            for k, v in outcome.values.items()
            if isinstance(v, (int, float)) and not isinstance(v, bool)
          },
          outcome=outcome,
        ),
      )
    # Re-archive: replay left the marks set; buckets + caches are
    # rebuilt, so the bytes regenerate. A precondition failure
    # (something reclassified to unknown) leaves it live.
    if attempt.archived_at is not None:
      try:
        view = _full_attempt_view(scheduler, attempt.attempt_id)
        scheduler.archive_attempt(
          attempt.attempt_id,
          at=attempt.archived_at,
          kind=attempt.archive_kind or "manual",
          payload_bytes=view.model_dump_json().encode("utf-8"),
        )
      except NotArchivableError as exc:
        logger.warning(
          "restore: cannot re-archive %s: %s (leaving live)",
          attempt.attempt_id,
          exc.reason,
        )
      except Exception:
        logger.exception(
          "restore: archive rehydrate failed for %s",
          attempt.attempt_id,
        )
  return max_seq


# ── background loops ─────────────────────────────────────────────


async def _run_startup_census(
  runtime: DispatcherRuntime, config: DispatcherConfig
) -> None:
  for host in config.hosts:
    try:
      containers = await census_host(host, self_host=config.self_host)
    except Exception:
      logger.warning(
        "startup census failed for %s (stream will retry)",
        host,
        exc_info=True,
      )
      continue
    await runtime.reconcile_from_census(host, containers)


async def _supervised(
  name: str,
  factory: Callable[[], Awaitable[None]],
  *,
  restart_delay_s: float = 30.0,
) -> None:
  """Restart-on-death shell for the long-running tasks.

  Catches BaseException (not Exception): asyncio swallows an
  unhandled exception in a strongly-referenced Task silently — a
  dead GC loop once served HTTP for 25h with no log line. Only
  CancelledError propagates (clean shutdown)."""
  while True:
    try:
      await factory()
      return
    except asyncio.CancelledError:
      raise
    except BaseException:
      logger.exception("%s died; restarting in %ss", name, restart_delay_s)
      await asyncio.sleep(restart_delay_s)


async def _resolver_forever(
  runtime: DispatcherRuntime, config: DispatcherConfig
) -> None:
  """Knobs re-read each tick so PATCH /settings applies on the
  next boundary."""
  while True:
    await asyncio.sleep(config.state_reconciliation.seconds_between_probes)
    try:
      outcomes = await runtime.resolve_state_once(
        max_concurrent_probes=(
          config.state_reconciliation.max_concurrent_probes
        ),
      )
    except Exception:
      logger.exception("resolver tick failed")
      continue
    if any(v > 0 for v in outcomes.values()):
      logger.info(
        "resolver: %s",
        ", ".join(f"{k}={v}" for k, v in outcomes.items() if v > 0),
      )


async def _gc_forever(
  runtime: DispatcherRuntime, config: DispatcherConfig
) -> None:
  while True:
    await asyncio.sleep(config.orphan_gc.seconds_between_sweeps)
    if not config.orphan_gc.enabled:
      continue
    try:
      removed = await runtime.gc_orphans_once(
        min_container_age_s=config.orphan_gc.min_container_age_s,
      )
    except Exception:
      logger.exception("orphan_gc sweep failed")
      continue
    if removed:
      # WARNING: the dispatcher touched containers — operators
      # must see this even at quiet log levels.
      logger.warning(
        "orphan_gc: removed %s container(s) — %s",
        sum(removed.values()),
        ", ".join(f"{h}={n}" for h, n in removed.items()),
      )


async def _autotune_forever(
  server_state: ServerState,
  autotune_state: HostAutotuneState,
) -> None:
  """Knobs re-read each tick; applied caps mirror into config so
  /state readback stays truthful."""
  config = server_state.config

  def _apply(host: str, cap: int) -> None:
    new = server_state.scheduler.set_host_settings(
      host, max_concurrent=cap
    )
    config.hosts[host] = new.model_copy()

  while True:
    await asyncio.sleep(config.host_autotune.seconds_between_ticks)
    if not config.host_autotune.enabled:
      continue
    try:
      applied = await autotune_tick(
        state=autotune_state,
        scheduler=server_state.scheduler,
        config=config.host_autotune,
        self_host=config.self_host,
        apply_cap=_apply,
      )
    except Exception:
      logger.exception("host_autotune tick failed")
      continue
    if applied:
      logger.warning(
        "host_autotune: %s",
        ", ".join(f"{h}={cap}" for h, cap in applied.items()),
      )


async def _archive_forever(
  app: FastAPI,
  server_state: ServerState,
  clock_fn: Callable[[], datetime],
) -> None:
  config = server_state.config
  while True:
    await asyncio.sleep(config.archive.scan_interval_seconds)
    threshold_days = config.archive.auto_after_days
    if threshold_days <= 0:
      continue
    try:
      candidates = _scan_auto_archive_candidates(
        server_state, clock_fn(), threshold_days
      )
    except Exception:
      logger.exception("archive scan failed")
      continue
    archived = 0
    for aid in candidates:
      try:
        _archive_attempt(app, aid, clock_fn, kind="auto")
        archived += 1
      except HTTPException as exc:
        # A resolver may have re-flipped a trial between the scan
        # and the archive; skip, re-check next tick.
        logger.info("auto-archive skipped attempt=%s: %s", aid, exc.detail)
      except Exception:
        logger.exception("auto-archive failed attempt=%s", aid)
    if archived:
      logger.warning(
        "auto-archive: promoted %d attempt(s) (threshold=%d days)",
        archived,
        threshold_days,
      )


def _scan_auto_archive_candidates(
  server_state: ServerState,
  now: datetime,
  threshold_days: int,
) -> list[str]:
  """Live attempts that are fully terminal AND whose event log
  has been idle past the threshold (every dispatch/patch/
  transition appends, so log mtime is 'last activity')."""
  threshold_dt = now - timedelta(days=threshold_days)
  scheduler = server_state.scheduler
  out: list[str] = []
  for aid in list(scheduler.iter_attempt_ids()):
    if scheduler.is_archived(aid):
      continue
    view = scheduler.attempt_view(aid)
    if view.pending or view.running or view.unknown or view.ghosted:
      continue
    log_path = event_log_path_for(scheduler.attempt_state(aid))
    try:
      mtime = datetime.fromtimestamp(log_path.stat().st_mtime, tz=UTC)
    except OSError:
      continue  # never archive from thin air
    if mtime > threshold_dt:
      continue
    out.append(aid)
  return out


# ── misc ─────────────────────────────────────────────────────────


def _mk_attempt_id(label: str) -> str:
  return datetime.now(UTC).strftime(
    "att-%Y%m%dT%H%M%S%fZ-"
  ) + label.replace("/", "-").replace(" ", "-")


def _sse(event: str, payload: BaseModel | dict[str, Any]) -> str:
  if isinstance(payload, BaseModel):
    data = payload.model_dump_json()
  else:
    data = json.dumps(payload, default=str)
  return f"event: {event}\ndata: {data}\n\n"


def _mount_ui(app: FastAPI, ui_dist: Path | None) -> None:
  if ui_dist is None or not ui_dist.is_dir():
    return
  from fastapi.staticfiles import StaticFiles

  app.mount(
    "/ui",
    StaticFiles(directory=str(ui_dist), html=True),
    name="ui",
  )

  @app.get("/", include_in_schema=False)
  def _index_redirect() -> RedirectResponse:
    return RedirectResponse(url="/ui/")
