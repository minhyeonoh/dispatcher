"""Background-loop layer: supervision, cadence, error isolation.

Every long-running task runs inside `supervised` (restart on
unexpected death) and periodic ones inside `every` (sleep →
enabled check → tick, one tick's failure never stops the next).
Tick bodies live in their domain modules; this file owns only
when they run and how they fail.

Knobs are re-read through `interval_fn` / `enabled_fn` closures
on every iteration — snapshotting values would silently break
PATCH /settings' next-boundary semantics.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from dispatcher.event_log import event_log_path_for
from dispatcher.host_autotune import autotune_tick

if TYPE_CHECKING:
  from collections.abc import Awaitable, Callable

  from dispatcher.config import DispatcherConfig
  from dispatcher.host_autotune import HostAutotuneState
  from dispatcher.orphan_gc import OrphanGC
  from dispatcher.runtime import DispatcherRuntime
  from dispatcher.scheduler import Scheduler

logger = logging.getLogger(__name__)


class LoopSkip(Exception):
  """A per-item precondition failed between scan and apply —
  expected under concurrency; logged at INFO, retried next tick."""


async def supervised(
  name: str,
  factory: Callable[[], Awaitable[None]],
  *,
  restart_delay_s: float = 30.0,
) -> None:
  """Restart-on-death shell.

  Catches BaseException (not Exception): asyncio swallows an
  unhandled exception in a strongly-referenced Task silently — a
  dead GC loop once served HTTP for 25h with no log line. Only
  CancelledError propagates (clean shutdown); a factory that
  returns normally ends the supervisor too."""
  while True:
    try:
      await factory()
      return
    except asyncio.CancelledError:
      raise
    except BaseException:
      logger.exception("%s died; restarting in %ss", name, restart_delay_s)
      await asyncio.sleep(restart_delay_s)


async def every(
  name: str,
  interval_fn: Callable[[], float],
  tick: Callable[[], Awaitable[None]],
  *,
  enabled_fn: Callable[[], bool] | None = None,
) -> None:
  """Periodic shell. First tick fires after one interval, not
  immediately — startup work (restore, census) gets to settle
  before any sweep reads scheduler state."""
  while True:
    await asyncio.sleep(interval_fn())
    if enabled_fn is not None and not enabled_fn():
      continue
    try:
      await tick()
    except Exception:
      logger.exception("%s tick failed", name)


# ── the loops ────────────────────────────────────────────────────


async def resolver_loop(
  runtime: DispatcherRuntime, config: DispatcherConfig
) -> None:
  async def tick() -> None:
    outcomes = await runtime.resolve_state_once(
      max_concurrent_probes=(
        config.state_reconciliation.max_concurrent_probes
      ),
    )
    if any(v > 0 for v in outcomes.values()):
      logger.info(
        "resolver: %s",
        ", ".join(f"{k}={v}" for k, v in outcomes.items() if v > 0),
      )

  await every(
    "resolver",
    lambda: config.state_reconciliation.seconds_between_probes,
    tick,
  )


async def gc_loop(gc: OrphanGC, config: DispatcherConfig) -> None:
  async def tick() -> None:
    removed = await gc.sweep_once(
      min_container_age_s=config.orphan_gc.min_container_age_s,
    )
    if removed:
      # WARNING: the dispatcher touched containers — operators
      # must see this even at quiet log levels.
      logger.warning(
        "orphan_gc: removed %s container(s) — %s",
        sum(removed.values()),
        ", ".join(f"{h}={n}" for h, n in removed.items()),
      )

  await every(
    "orphan_gc",
    lambda: config.orphan_gc.seconds_between_sweeps,
    tick,
    enabled_fn=lambda: config.orphan_gc.enabled,
  )


async def autotune_loop(
  scheduler: Scheduler,
  config: DispatcherConfig,
  autotune_state: HostAutotuneState,
  apply_cap: Callable[[str, int], None],
) -> None:
  """`apply_cap` lets the server mirror applied caps into its
  config so /state readback stays truthful."""

  async def tick() -> None:
    applied = await autotune_tick(
      state=autotune_state,
      scheduler=scheduler,
      config=config.host_autotune,
      self_host=config.self_host,
      apply_cap=apply_cap,
    )
    if applied:
      logger.warning(
        "host_autotune: %s",
        ", ".join(f"{h}={cap}" for h, cap in applied.items()),
      )

  await every(
    "host_autotune",
    lambda: config.host_autotune.seconds_between_ticks,
    tick,
    enabled_fn=lambda: config.host_autotune.enabled,
  )


async def archive_loop(
  scheduler: Scheduler,
  config: DispatcherConfig,
  clock_fn: Callable[[], datetime],
  archive_one: Callable[[str], None],
) -> None:
  """`archive_one(aid)` applies one auto-archive (the server's
  operation, shared with the endpoint). It raises LoopSkip when a
  precondition re-check fails — a resolver may have re-flipped a
  trial between scan and apply."""

  async def tick() -> None:
    threshold_days = config.archive.auto_after_days
    if threshold_days <= 0:
      return
    candidates = scan_auto_archive_candidates(
      scheduler, clock_fn(), threshold_days
    )
    archived = 0
    for aid in candidates:
      try:
        archive_one(aid)
        archived += 1
      except LoopSkip as exc:
        logger.info("auto-archive skipped attempt=%s: %s", aid, exc)
      except Exception:
        logger.exception("auto-archive failed attempt=%s", aid)
    if archived:
      logger.warning(
        "auto-archive: promoted %d attempt(s) (threshold=%d days)",
        archived,
        threshold_days,
      )

  await every(
    "auto_archive",
    lambda: config.archive.scan_interval_seconds,
    tick,
  )


def scan_auto_archive_candidates(
  scheduler: Scheduler,
  now: datetime,
  threshold_days: int,
) -> list[str]:
  """Live attempts that are fully terminal AND whose event log
  has been idle past the threshold (every dispatch/patch/
  transition appends, so log mtime is 'last activity')."""
  threshold_dt = now - timedelta(days=threshold_days)
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
