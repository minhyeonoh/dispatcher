"""FastAPI edge: app factory, lifespan (startup sequence + loop
wiring), endpoints as thin translators over `dispatcher.api.ops`.

Layer map:
  config.py   configuration + PATCH /settings shapes
  wire.py     DTOs, snapshot builders, SSE framing
  ops.py      operation bodies (HTTP-free; raise OpError)
  restore.py  startup restore from event logs + outcomes
  loops.py    background-loop shells
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import TYPE_CHECKING, Any

import anyio
from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import (
  JSONResponse,
  Response,
  StreamingResponse,
)

from dispatcher.api import ops

# FastAPI resolves endpoint annotations at runtime, so these
# must stay runtime imports.
from dispatcher.api.config import Config  # noqa: TC001
from dispatcher.api.ops import ServerState
from dispatcher.api.restore import restore_jobs_from_disk
from dispatcher.api.settings import (
  Settings,
  SettingsPatch,
  apply_patch_pure,
  load_settings,
  save_settings,
)
from dispatcher.api.wire import (
  ArenaDetailOut,
  ArenaSummaryOut,
  FullJobOut,
  HealthOut,
  JobSummaryOut,
  RetryDoneErrRequest,
  StateOut,
  arena_members,
  build_full_jobs_body,
  cluster_snapshot,
  full_job_view,
  snapshot_arena,
  snapshot_arenas,
  snapshot_job,
  sse,
)
from dispatcher.core import clock as clock_mod
from dispatcher.core.containers import (
  DockerEventStreamManager,
  census_host,
  resolve_image_id,
)
from dispatcher.core.event_bus import EventBus
from dispatcher.core.loops import LoopSkip, supervised
from dispatcher.core.runtime import (
  DispatcherRuntime,
  resolver_loop,
)
from dispatcher.core.scheduler import Scheduler
from dispatcher.services.auto_archive import archive_loop
from dispatcher.services.host_autotune import (
  HOST_METRICS_FILENAME,
  HostAutotuneState,
  autotune_loop,
  load_ring,
  truncate_ring_file,
)
from dispatcher.services.notify import (
  NotifyManager,
  TelegramSender,
  telegram_bot_token_from_env,
)
from dispatcher.services.orphan_gc import OrphanGC, gc_loop
from dispatcher.services.readouts import (
  ReadoutRegistry,
  ReadoutService,
  docker_readout_run,
  readout_sweep_loop,
)

if TYPE_CHECKING:
  from collections.abc import Awaitable, Callable
  from datetime import datetime
  from pathlib import Path

  from dispatcher.core.outcome import CompletionSnapshot

logger = logging.getLogger(__name__)


def _get_state(app: FastAPI) -> ServerState:
  return app.state.dispatcher  # type: ignore[no-any-return]


def create_app(
  config: Config,
  *,
  settings: Settings | None = None,
  settings_overrides: SettingsPatch | None = None,
  dispatch: Callable[..., Any] | None = None,
  poll: Callable[[Path], CompletionSnapshot | None] | None = None,
  clock: Callable[[], datetime] | None = None,
  resolve_image: Callable[[str], str] | None = None,
  readout_runner: Callable[..., Any] | None = None,
  heartbeat_interval: float = 15.0,
) -> FastAPI:
  """`settings` is the SEED, used only when no settings.json is
  persisted yet (first boot). Afterwards the persisted document
  wins across restarts; `settings_overrides` (CLI-explicit flags)
  are applied on top either way and persisted."""
  clock_fn = clock or clock_mod.now
  seed = settings

  @contextlib.asynccontextmanager
  async def lifespan(app: FastAPI):
    event_bus = EventBus()
    persisted = load_settings(config.data_dir)
    settings = persisted if persisted is not None else seed
    if settings is None:
      raise RuntimeError(
        "no persisted settings and no seed supplied — first boot "
        "needs --host/--max-concurrent (or a Settings object)"
      )
    if settings_overrides is not None:
      _apply_boot_overrides(settings, settings_overrides)
    save_settings(config.data_dir, settings)
    scheduler = Scheduler(
      max_concurrent=settings.max_concurrent,
      hosts=settings.hosts,
      clock=clock_fn,
      id_gen=lambda task: server_state.next_instance_id(task),
      pool_caps=dict(settings.pool_caps),
    )
    # Readouts are built before the runtime so the completion path
    # can call `request` synchronously — that hook is where "values
    # appear with no artificial delay" actually comes from.
    readout_registry = ReadoutRegistry.load(config.data_dir)

    def _refresh_archive_bytes(job_id: str) -> None:
      """An archived job serves pre-serialized bytes, so values
      written by a backfill would otherwise never reach a reader.
      Re-freeze with the new columns in them."""
      if not scheduler.is_archived(job_id):
        return
      state = scheduler.job_state(job_id)
      if state.archived_at is None:  # pragma: no cover — defensive
        return
      try:
        view = full_job_view(scheduler, job_id, readouts.summary)
        scheduler.archive_job(
          job_id,
          at=state.archived_at,
          kind=state.archive_kind or "manual",
          payload_bytes=view.model_dump_json().encode("utf-8"),
        )
      except Exception:
        logger.exception("readout: archive refresh failed job=%s", job_id)

    readouts = ReadoutService(
      scheduler=scheduler,
      registry=readout_registry,
      settings=settings.readouts,
      runner=_pick_readout_runner(readout_runner, dispatch),
      clock_fn=clock_fn,
      on_values_written=_refresh_archive_bytes,
    )
    docker_events: DockerEventStreamManager | None = None
    runtime = DispatcherRuntime(
      scheduler,
      self_host=config.self_host,
      dispatch=dispatch,
      poll=poll,
      tick_interval=config.tick_interval,
      event_bus=event_bus,
      on_instance_scored=readouts.request,
    )
    if config.use_docker_events:
      docker_events = DockerEventStreamManager(
        self_host=config.self_host,
        on_event=runtime.handle_docker_die,
        on_alive_change=runtime.handle_alive_change,
      )
      runtime._docker_events = docker_events
    # Image pinning only when the REAL dispatch path is in play —
    # injected fake dispatch (tests) must not touch docker.
    resolver = resolve_image
    if resolver is None and dispatch is None:
      resolver = lambda ref: resolve_image_id(  # noqa: E731
        ref, self_host=config.self_host
      )
    server_state = ServerState(
      config=config,
      settings=settings,
      scheduler=scheduler,
      runtime=runtime,
      event_bus=event_bus,
      resolve_image=resolver,
      readouts=readouts,
    )
    app.state.dispatcher = server_state
    # Restore before anything can dispatch: rebuild JobStates
    # from event logs, buckets from outcome files, and push the
    # instance-id counter past every name on disk.
    server_state.advance_seq_to(
      restore_jobs_from_disk(scheduler, config.data_dir)
    )
    if docker_events is not None:
      # Census BEFORE the stream: docker's event buffer may have
      # rolled past completions that fired during downtime.
      await _run_startup_census(runtime, config, settings)
      docker_events.start(list(settings.hosts.keys()))
    # Blocking resolver pass so restart-adopted running instances
    # re-book their slots before the dispatch loop reads them.
    startup_outcomes = await runtime.resolve_state_once(
      max_concurrent_probes=(
        settings.state_reconciliation.max_concurrent_probes
      ),
    )
    if any(v > 0 for v in startup_outcomes.values()):
      logger.info(
        "resolver (startup): %s",
        ", ".join(
          f"{k}={v}" for k, v in startup_outcomes.items() if v > 0
        ),
      )
    # Values for live jobs, so the first jobs table already carries
    # readout columns and a truthful lag instead of nulls.
    await readouts.load_live()
    metrics_file = config.data_dir / HOST_METRICS_FILENAME
    initial_ring = load_ring(
      metrics_file, settings.host_autotune.ring_buffer_size
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
    orphan_gc = OrphanGC(scheduler, self_host=config.self_host)

    def _apply_autotune_cap(host: str, cap: int) -> None:
      # Mirror into settings so /state readback stays truthful.
      # Deliberately NOT persisted — see the ratchet note in
      # api/settings.py.
      new = scheduler.set_host_settings(host, max_concurrent=cap)
      settings.hosts[host] = new.model_copy()

    async def _auto_archive_one(aid: str) -> None:
      try:
        await ops.archive_job(server_state, aid, clock_fn, kind="auto")
      except ops.OpError as exc:
        # Precondition re-check lost a race with the resolver —
        # skip this tick, not an error.
        raise LoopSkip(str(exc)) from exc

    server_state.notify_sender = TelegramSender(
      telegram_bot_token_from_env()
    )
    notify_manager = NotifyManager(
      bus=event_bus,
      scheduler=scheduler,
      config=settings.notify,
      sender=server_state.notify_sender,
    )
    loops: list[tuple[str, Callable[[], Awaitable[None]]]] = [
      ("runtime", runtime.run_forever),
      (
        "resolver",
        lambda: resolver_loop(runtime, settings.state_reconciliation),
      ),
      (
        "orphan_gc",
        lambda: gc_loop(orphan_gc, settings.orphan_gc),
      ),
      (
        "host_autotune",
        lambda: autotune_loop(
          scheduler,
          config.self_host,
          settings.host_autotune,
          autotune_state,
          _apply_autotune_cap,
        ),
      ),
      (
        "auto_archive",
        lambda: archive_loop(
          scheduler,
          settings.archive,
          clock_fn,
          _auto_archive_one,
        ),
      ),
      ("notify", notify_manager.run),
      (
        "readout_sweep",
        lambda: readout_sweep_loop(readouts, settings.readouts),
      ),
    ]
    server_state.tasks = [
      asyncio.create_task(supervised(name, factory))
      for name, factory in loops
    ]
    try:
      yield
    finally:
      for task in reversed(server_state.tasks):
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
          await task
      # Readout passes are per-job tasks outside `tasks`; a
      # container in flight is abandoned, and the pairs it did not
      # report stay unscored for the next boot's sweep.
      await readouts.close()
      if server_state.notify_sender is not None:
        await server_state.notify_sender.close()
      if docker_events is not None:
        await docker_events.stop()

  app = FastAPI(
    title="dispatcher",
    version="0.1.0",
    lifespan=lifespan,
    # The UI owns the root URL space (/jobs/… is a page); the
    # whole JSON surface, docs included, lives under /api.
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    redoc_url=None,
  )
  app.add_middleware(GZipMiddleware, minimum_size=1024)
  api = APIRouter(prefix="/api")

  @app.exception_handler(ops.OpError)
  async def _op_error(request: Request, exc: ops.OpError):
    return JSONResponse({"detail": str(exc)}, status_code=exc.status)

  # Handlers are async so their (pure-sync) bodies run on the
  # event-loop thread that owns the runtime tick — no scheduler
  # access can interleave mid-handler.

  @api.get("/health")
  async def health() -> HealthOut:
    return HealthOut(status="ok")

  @api.get("/state")
  async def get_state() -> StateOut:
    st = _get_state(app)
    cluster = cluster_snapshot(
      st.config.self_host, st.settings, st.scheduler
    )
    return StateOut(
      **cluster.model_dump(),
      jobs=[
        snapshot_job(st.scheduler, aid, st.readout_fn)
        for aid in st.scheduler.all_job_ids()
      ],
    )

  @api.get("/monitor/stream")
  async def monitor_stream():
    st = _get_state(app)
    sub = st.event_bus.subscribe(maxsize=256)

    state_change_events = frozenset(
      {
        "instance_dispatched",
        "instance_completed",
        "instance_reclassified",
        "instance_requeued",
        "job_paused_on_error",
        "job_submitted",
        "job_patched",
        "job_cancelled",
        "job_reclaimed",
        "job_retried",
        "job_archived",
        "job_unarchived",
        "job_drained",
      }
    )

    async def gen():
      try:
        yield sse(
          "snapshot",
          cluster_snapshot(st.config.self_host, st.settings, st.scheduler),
        )
        for aid in list(st.scheduler.all_job_ids()):
          yield sse(
            "job_updated",
            snapshot_job(st.scheduler, aid, st.readout_fn),
          )
        while True:
          ev = None
          with anyio.move_on_after(heartbeat_interval) as scope:
            ev = await sub.get()
          if scope.cancel_called or ev is None:
            yield sse("heartbeat", {"at": clock_fn().isoformat()})
            continue
          if ev.type in state_change_events:
            aid = ev.payload.get("job_id")
            if aid and st.scheduler.has_job(aid):
              yield sse(
                "job_updated",
                snapshot_job(st.scheduler, aid, st.readout_fn),
              )
            yield sse(
              "cluster_updated",
              cluster_snapshot(
                st.config.self_host, st.settings, st.scheduler
              ),
            )
          yield sse(ev.type, ev.payload)
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

  @api.get("/jobs", response_model=None)
  async def list_jobs(
    full: bool = False, arena: str = ""
  ) -> list[JobSummaryOut] | Response:
    st = _get_state(app)
    aids = st.scheduler.all_job_ids()
    if arena:
      aids = [
        aid for aid in aids if st.scheduler.job_state(aid).arena == arena
      ]
    if full:
      body = await asyncio.to_thread(
        build_full_jobs_body, st.scheduler, aids, st.readout_fn
      )
      return Response(content=body, media_type="application/json")
    return await asyncio.to_thread(
      lambda: [
        snapshot_job(st.scheduler, aid, st.readout_fn) for aid in aids
      ]
    )

  @api.get("/jobs/{job_id}", response_model=None)
  async def get_job(
    job_id: str,
  ) -> FullJobOut | Response:
    st = _get_state(app)
    if not st.scheduler.has_job(job_id):
      raise HTTPException(
        status_code=404, detail=f"job {job_id!r} not found"
      )
    if st.scheduler.is_archived(job_id):
      return Response(
        content=st.scheduler.archived_bytes(job_id),
        media_type="application/json",
      )
    return full_job_view(st.scheduler, job_id, st.readout_fn)

  @api.post("/jobs")
  async def submit_job(
    payload: dict[str, Any],
  ) -> dict[str, Any]:
    return await ops.submit_job(_get_state(app), payload, clock_fn)

  @api.get("/arenas")
  async def list_arenas() -> list[ArenaSummaryOut]:
    st = _get_state(app)
    return await asyncio.to_thread(snapshot_arenas, st.scheduler)

  # `{arena:path}` — arena names are slash paths and a name
  # addresses its subtree. The action-suffix routes still match:
  # the greedy path segment backtracks over the trailing verb.

  @api.get("/arenas/{arena:path}")
  async def get_arena(arena: str) -> ArenaDetailOut:
    st = _get_state(app)
    name = ops.normalize_arena(arena)
    if not name:
      raise HTTPException(status_code=404, detail="empty arena path")
    members = arena_members(st.scheduler, name)
    if not members:
      raise HTTPException(
        status_code=404, detail=f"arena {name!r} has no jobs"
      )
    return await asyncio.to_thread(
      snapshot_arena, st.scheduler, name, members, st.readout_fn
    )

  @api.post("/arenas/{arena:path}/pause")
  async def arena_pause(arena: str) -> dict[str, Any]:
    return await ops.arena_set_paused(
      _get_state(app), arena, True, clock_fn
    )

  @api.post("/arenas/{arena:path}/resume")
  async def arena_resume(arena: str) -> dict[str, Any]:
    return await ops.arena_set_paused(
      _get_state(app), arena, False, clock_fn
    )

  @api.post("/arenas/{arena:path}/reclaim")
  async def arena_reclaim(arena: str) -> dict[str, Any]:
    return await ops.arena_reclaim(_get_state(app), arena, clock_fn)

  @api.post("/arenas/{arena:path}/cancel")
  async def arena_cancel(
    arena: str, payload: dict[str, Any] | None = None
  ) -> dict[str, Any]:
    confirm = bool((payload or {}).get("confirm"))
    return await ops.arena_cancel(
      _get_state(app), arena, confirm, clock_fn
    )

  @api.patch("/jobs/{job_id}")
  async def patch_job(
    job_id: str, payload: dict[str, Any]
  ) -> JobSummaryOut:
    return await ops.patch_job(_get_state(app), job_id, payload, clock_fn)

  @api.delete("/jobs/{job_id}")
  async def cancel_job(job_id: str) -> dict[str, Any]:
    return await ops.cancel_job(_get_state(app), job_id, clock_fn)

  @api.post("/jobs/{job_id}/reclaim")
  async def reclaim_job(job_id: str) -> dict[str, Any]:
    return await ops.reclaim_job(_get_state(app), job_id, clock_fn)

  @api.get("/jobs/{job_id}/instances/{instance_id}/outcome")
  async def get_instance_outcome(
    job_id: str, instance_id: str
  ) -> dict[str, Any]:
    return await ops.get_instance_outcome(
      _get_state(app), job_id, instance_id
    )

  @api.post("/jobs/{job_id}/instances/{instance_id}/reclaim")
  async def reclaim_instance(
    job_id: str, instance_id: str
  ) -> dict[str, Any]:
    return await ops.reclaim_instance(
      _get_state(app), job_id, instance_id, clock_fn
    )

  @api.post("/jobs/{job_id}/retry-done-err")
  async def retry_done_err(
    job_id: str, payload: RetryDoneErrRequest | None = None
  ) -> dict[str, Any]:
    return await ops.retry_done_err(
      _get_state(app), job_id, payload, clock_fn
    )

  @api.post("/jobs/{job_id}/archive")
  async def archive_job_ep(job_id: str) -> dict[str, Any]:
    return await ops.archive_job(
      _get_state(app), job_id, clock_fn, kind="manual"
    )

  @api.post("/jobs/{job_id}/unarchive")
  async def unarchive_job_ep(
    job_id: str,
  ) -> dict[str, Any]:
    return await ops.unarchive_job(_get_state(app), job_id, clock_fn)

  # Readout registration is body/query-addressed, not path-
  # addressed: an arena name is a slash path, so
  # `/readouts/{arena}/{name}` could not be split unambiguously.

  @api.get("/readouts")
  async def list_readouts() -> dict[str, Any]:
    return ops.list_readouts(_get_state(app))

  @api.post("/readouts")
  async def register_readout(payload: dict[str, Any]) -> dict[str, Any]:
    return ops.register_readout(_get_state(app), payload)

  @api.delete("/readouts")
  async def unregister_readout(arena: str, name: str) -> dict[str, Any]:
    return ops.unregister_readout(_get_state(app), arena, name)

  @api.get("/jobs/{job_id}/readouts")
  async def get_job_readouts(job_id: str) -> dict[str, Any]:
    return await ops.get_job_readouts(_get_state(app), job_id)

  @api.patch("/settings")
  async def patch_settings(payload: SettingsPatch) -> Settings:
    st = _get_state(app)
    ops.apply_settings(st, payload)
    return st.settings

  @api.get("/filter-presets")
  async def get_filter_presets() -> dict[str, Any]:
    path = ops.filter_presets_path(config)
    if not path.is_file():
      return {"presets": []}
    try:
      return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
      raise HTTPException(
        status_code=500, detail=f"failed reading {path}: {exc}"
      ) from exc

  @api.put("/filter-presets")
  async def put_filter_presets(
    payload: dict[str, Any],
  ) -> dict[str, Any]:
    """Opaque UI blob; whole-document last-write-wins. Atomic
    write so a crash can't corrupt it."""
    path = ops.filter_presets_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)
    return payload

  # Order matters: /api routes first, SPA catch-all last.
  app.include_router(api)
  _mount_ui(app, config.ui_dist)

  return app


def _apply_boot_overrides(
  settings: Settings, overrides: SettingsPatch
) -> None:
  """CLI-explicit flags win over the persisted document at boot.
  Same merge rules as PATCH /settings, minus scheduler side
  effects (the scheduler is built after this)."""
  if overrides.hosts is not None:
    for host, hp in overrides.hosts.items():
      existing = settings.hosts.get(host)
      if existing is None:
        from dispatcher.core.models import HostSettings

        settings.hosts[host] = HostSettings(
          max_concurrent=hp.max_concurrent or 0,
          active=hp.active if hp.active is not None else True,
          alive=hp.alive if hp.alive is not None else True,
        )
      else:
        if hp.max_concurrent is not None:
          existing.max_concurrent = hp.max_concurrent
        if hp.active is not None:
          existing.active = hp.active
        if hp.alive is not None:
          existing.alive = hp.alive
  if overrides.max_concurrent is not None:
    settings.max_concurrent = overrides.max_concurrent
  if overrides.pool_caps is not None:
    settings.pool_caps = {
      k: int(v) for k, v in overrides.pool_caps.items()
    }
  apply_patch_pure(settings, overrides)


def _pick_readout_runner(
  injected: Callable[..., Any] | None,
  dispatch: Callable[..., Any] | None,
) -> Any:
  """Real docker only when the real dispatch path is in play — the
  same rule image pinning follows. With fake dispatch and no
  injected runner, passes produce nothing rather than shelling out."""
  if injected is not None:
    return injected
  if dispatch is not None:

    async def _none(*_args: Any, **_kwargs: Any) -> str:
      return ""

    return _none
  return docker_readout_run


async def _run_startup_census(
  runtime: DispatcherRuntime,
  config: Config,
  settings: Settings,
) -> None:
  for host in settings.hosts:
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


def _mount_ui(app: FastAPI, ui_dist: Path | None) -> None:
  """Mount the SPA at the ROOT url space, last.

  The UI owns the human-facing paths (`/jobs/abc` is a page) and
  every JSON route lives under `/api` — registered before this
  mount, so they win the match. Whatever is left over reaches the
  SPA."""
  if ui_dist is None or not ui_dist.is_dir():
    return
  from fastapi.staticfiles import StaticFiles
  from starlette.exceptions import (
    HTTPException as StarletteHTTPException,
  )

  class _SpaStaticFiles(StaticFiles):
    """Missing file → index.html, so a deep link (`/jobs/x`)
    loads the app instead of 404ing. Starlette signals the miss
    either way depending on version — as a 404 response or a
    (starlette-level, NOT the fastapi subclass) HTTPException.

    `/api/*` is exempt: an unmatched API path must stay a 404,
    never become a 200 page — a client eating HTML while
    believing it got JSON is the worse failure."""

    async def get_response(self, path: str, scope):  # type: ignore[no-untyped-def]
      try:
        response = await super().get_response(path, scope)
      except StarletteHTTPException as exc:
        if exc.status_code != 404 or _is_api_path(path):
          raise
        return await super().get_response("index.html", scope)
      if response.status_code == 404 and not _is_api_path(path):
        return await super().get_response("index.html", scope)
      return response

  app.mount(
    "/",
    _SpaStaticFiles(directory=str(ui_dist), html=True),
    name="ui",
  )


def _is_api_path(path: str) -> bool:
  return path == "api" or path.startswith("api/")
