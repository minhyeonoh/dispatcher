"""Background-loop layer: supervision, cadence, error isolation.

Every long-running task runs inside `supervised` (restart on
unexpected death) and periodic ones inside `every` (sleep →
enabled check → tick, one tick's failure never stops the next).
Tick bodies and the per-loop entrypoints live with their owners
(core/runtime for the resolver, each service module for its own);
this file owns only the shells.

Knobs are re-read through `interval_fn` / `enabled_fn` closures
on every iteration — snapshotting values would silently break
PATCH /settings' next-boundary semantics.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from collections.abc import Awaitable, Callable


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
