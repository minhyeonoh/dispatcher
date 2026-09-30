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
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import (
  JSONResponse,
  RedirectResponse,
  Response,
  StreamingResponse,
)

from dispatcher.api import ops

# FastAPI resolves endpoint annotations at runtime, so these
# must stay runtime imports.
from dispatcher.api.config import Config  # noqa: TC001
from dispatcher.api.ops import ServerState
from dispatcher.api.restore import restore_attempts_from_disk
from dispatcher.api.settings import (
  Settings,
  SettingsPatch,
  apply_patch_pure,
  load_settings,
  save_settings,
)
from dispatcher.api.wire import (
  AttemptSummaryOut,
  FullAttemptOut,
  HealthOut,
  MonitorOut,
  RetryDoneErrRequest,
  StateOut,
  build_full_attempts_body,
  cluster_snapshot,
  full_attempt_view,
  snapshot_attempt,
  snapshot_attempt_with_metrics,
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
from dispatcher.core.metrics import MetricsCache
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
    metrics = MetricsCache()
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
      name_gen=lambda task: server_state.next_trial_name(task),
      pool_caps=dict(settings.pool_caps),
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
      metrics=metrics,
      event_bus=event_bus,
      resolve_image=resolver,
    )
    app.state.dispatcher = server_state
    # Restore before anything can dispatch: rebuild AttemptStates
    # from event logs, buckets from outcome files, and push the
    # trial-name counter past every name on disk.
    server_state.advance_seq_to(
      restore_attempts_from_disk(scheduler, metrics, config.data_dir)
    )
    if docker_events is not None:
      # Census BEFORE the stream: docker's event buffer may have
      # rolled past completions that fired during downtime.
      await _run_startup_census(runtime, config, settings)
      docker_events.start(list(settings.hosts.keys()))
    # Blocking resolver pass so restart-adopted running trials
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
        await ops.archive_attempt(server_state, aid, clock_fn, kind="auto")
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
      metrics=metrics,
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
      if server_state.notify_sender is not None:
        await server_state.notify_sender.close()
      if docker_events is not None:
        await docker_events.stop()

  app = FastAPI(title="dispatcher", version="0.1.0", lifespan=lifespan)
  app.add_middleware(GZipMiddleware, minimum_size=1024)

  @app.exception_handler(ops.OpError)
  async def _op_error(request: Request, exc: ops.OpError):
    return JSONResponse({"detail": str(exc)}, status_code=exc.status)

  # Handlers are async so their (pure-sync) bodies run on the
  # event-loop thread that owns the runtime tick — no scheduler
  # access can interleave mid-handler.

  @app.get("/health")
  async def health() -> HealthOut:
    return HealthOut(status="ok")

  @app.get("/state")
  async def get_state() -> StateOut:
    st = _get_state(app)
    cluster = cluster_snapshot(
      st.config.self_host, st.settings, st.scheduler
    )
    return StateOut(
      **cluster.model_dump(),
      attempts=[
        snapshot_attempt(st.scheduler, aid)
        for aid in st.scheduler.all_attempt_ids()
      ],
    )

  @app.get("/monitor")
  async def get_monitor() -> MonitorOut:
    st = _get_state(app)
    cluster = cluster_snapshot(
      st.config.self_host, st.settings, st.scheduler
    )
    return MonitorOut(
      **cluster.model_dump(),
      attempts=[
        snapshot_attempt_with_metrics(st.scheduler, st.metrics, aid)
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
        yield sse(
          "snapshot",
          cluster_snapshot(st.config.self_host, st.settings, st.scheduler),
        )
        for aid in list(st.scheduler.all_attempt_ids()):
          yield sse(
            "attempt_updated",
            snapshot_attempt_with_metrics(st.scheduler, st.metrics, aid),
          )
        while True:
          ev = None
          with anyio.move_on_after(heartbeat_interval) as scope:
            ev = await sub.get()
          if scope.cancel_called or ev is None:
            yield sse("heartbeat", {"at": clock_fn().isoformat()})
            continue
          if ev.type in state_change_events:
            aid = ev.payload.get("attempt_id")
            if aid and st.scheduler.has_attempt(aid):
              yield sse(
                "attempt_updated",
                snapshot_attempt_with_metrics(
                  st.scheduler, st.metrics, aid
                ),
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
        build_full_attempts_body, st.scheduler, aids
      )
      return Response(content=body, media_type="application/json")
    return await asyncio.to_thread(
      lambda: [snapshot_attempt(st.scheduler, aid) for aid in aids]
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
    return full_attempt_view(st.scheduler, attempt_id)

  @app.post("/attempts")
  async def submit_attempt(
    payload: dict[str, Any],
  ) -> dict[str, Any]:
    return await ops.submit_attempt(_get_state(app), payload, clock_fn)

  @app.patch("/attempts/{attempt_id}")
  async def patch_attempt(
    attempt_id: str, payload: dict[str, Any]
  ) -> AttemptSummaryOut:
    return await ops.patch_attempt(
      _get_state(app), attempt_id, payload, clock_fn
    )

  @app.delete("/attempts/{attempt_id}")
  async def cancel_attempt(attempt_id: str) -> dict[str, Any]:
    return await ops.cancel_attempt(_get_state(app), attempt_id, clock_fn)

  @app.post("/attempts/{attempt_id}/reclaim")
  async def reclaim_attempt(attempt_id: str) -> dict[str, Any]:
    return await ops.reclaim_attempt(_get_state(app), attempt_id, clock_fn)

  @app.post("/attempts/{attempt_id}/trials/{trial_name}/reclaim")
  async def reclaim_trial(
    attempt_id: str, trial_name: str
  ) -> dict[str, Any]:
    return await ops.reclaim_trial(
      _get_state(app), attempt_id, trial_name, clock_fn
    )

  @app.post("/attempts/{attempt_id}/retry-done-err")
  async def retry_done_err(
    attempt_id: str, payload: RetryDoneErrRequest | None = None
  ) -> dict[str, Any]:
    return await ops.retry_done_err(
      _get_state(app), attempt_id, payload, clock_fn
    )

  @app.post("/attempts/{attempt_id}/archive")
  async def archive_attempt_ep(attempt_id: str) -> dict[str, Any]:
    return await ops.archive_attempt(
      _get_state(app), attempt_id, clock_fn, kind="manual"
    )

  @app.post("/attempts/{attempt_id}/unarchive")
  async def unarchive_attempt_ep(
    attempt_id: str,
  ) -> dict[str, Any]:
    return await ops.unarchive_attempt(
      _get_state(app), attempt_id, clock_fn
    )

  @app.patch("/settings")
  async def patch_settings(payload: SettingsPatch) -> Settings:
    st = _get_state(app)
    ops.apply_settings(st, payload)
    return st.settings

  @app.get("/filter-presets")
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

  @app.put("/filter-presets")
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
