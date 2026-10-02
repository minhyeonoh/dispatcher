"""Background-loop layer: supervision, cadence, error isolation.

Three shells, and only shells — tick bodies and per-loop entrypoints
live with their owners (core/runtime for the resolver, each service
module for its own):

- `supervised` — restart on unexpected death.
- `every` — periodic: sleep → enabled check → tick, one tick's failure
  never stopping the next.
- `fan_out` — one tick's work spread across its items, so a slow or
  wedged item does not hold the rest.

Knobs are re-read through `interval_fn` / `enabled_fn` closures
on every iteration — snapshotting values would silently break
PATCH /settings' next-boundary semantics.

None of them retries, deliberately. A shell that retried would have to
own a backoff and a give-up rule, and that is policy — it belongs to
whatever knows what the work means. The loops that converge do it by
re-deriving their work from durable state on the next tick instead of
remembering a failed attempt (`services.packer.missing` is the clearest
case), which is also what keeps a down host from becoming a spin.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import anyio

if TYPE_CHECKING:
  from collections.abc import Awaitable, Callable, Iterable


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


async def fan_out[T](
  name: str,
  items: Iterable[T],
  work: Callable[[T], Awaitable[None]],
  *,
  max_concurrent: int = 8,
) -> None:
  """Run `work` over `items` concurrently, isolating each failure.

  The property this buys is that one item cannot hold the others. A
  sequential `for host in hosts: await …` reads harmlessly and is not:
  a host whose filesystem or docker daemon has wedged takes its
  deadline, and every host behind it waits that long too — so one dark
  machine slows the whole sweep, and in a periodic loop the NEXT tick
  waits as well, because the tick before it has not returned.

  An item's exception is logged and dropped. Siblings keep going and
  the sweep still completes, which is what lets a caller treat "this
  tick did not finish everything" as normal rather than exceptional.

  No deadline here: the work already carries one, because every
  outbound call goes through `hosts.run_on`/`run_argv`. Putting a
  second one in the shell would mean two numbers that have to agree
  about the same wait.

  `CancelledError` propagates, so shutdown is not something an item can
  swallow."""
  if max_concurrent < 1:
    max_concurrent = 1
  sem = anyio.Semaphore(max_concurrent)

  async def one(item: T) -> None:
    async with sem:
      try:
        await work(item)
      except asyncio.CancelledError:
        raise
      except Exception:
        logger.exception("%s failed on %r", name, item)

  async with anyio.create_task_group() as tg:
    for item in items:
      tg.start_soon(one, item)


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
