"""Automatic packing: finished instance homes appended to their host's
archive as they complete.

Driven by the terminal pipeline, never by a timer. An instance going
`done_ok`/`done_err` is exactly the event that makes its home
immutable, so there is nothing to poll for and no interval to tune —
the same reasoning that keeps the readout service free of loops.

Batching falls out of the queue rather than a knob. The consumer
blocks on `get()`, and when it wakes it takes everything else already
waiting; under a trickle of completions that is a batch of one, under
a burst it is however many arrived while the last append ran. There is
no "wait N seconds to collect more" — nothing is ever held back — and
so no batch size or timeout to get wrong. It is the same shape as the
aggregate pool's request coalescing.

Sequential, one append at a time across all hosts. An ssh round trip
measured 0.29s and the append itself 0.14s, so even 1,344 completions
is about ten minutes of work spread over a job that runs for hours,
and coalescing collapses the bursts that would matter. The cost is
that an unreachable host holds the queue for its ssh timeout before
the next host is served; the benefit is that there is exactly one
writer per archive without a lock, one task to supervise, and no
per-host lifecycle to reason about.

Nothing here can fail a run. `offer` only enqueues, the consumer
swallows everything, and a pack that never happens just means readers
stay on NFS — see `core.pack` for why that is safe by construction.

A service and not core, sitting beside `auto_archive`, which it most
resembles: both tidy up a finished job's storage, both stay off until
switched on, and neither takes part in scoring. The lifespan's task
list is not interval-only — `supervised` is a restart-on-death shell
that knows nothing about time, `every` is the separate periodic one,
and `notify` already sits there as an event-driven consumer — so being
queue-driven is no reason to live somewhere else.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, PositiveInt

from dispatcher.core.hosts import run_on
from dispatcher.core.pack import (
  bust_dir_cache,
  instance_home_for,
  pack_dir,
  pack_path,
  pack_shell_cmd,
  packed_instances,
)

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.scheduler import Scheduler

logger = logging.getLogger(__name__)


class PackSettings(BaseModel):
  auto: bool = False
  """Off by default.

  Packing reads every finished instance home back over NFS from the
  host that wrote it, so it competes for the same bandwidth as running
  trials. The win is real but belongs to analysis, not to the run in
  flight, so switching it on is a deliberate choice — and one worth
  making after seeing the numbers on a job of your own size."""

  processors: PositiveInt = 2
  """`mksquashfs` workers on the remote host. Low on purpose: the
  append is 0.14s of CPU against a 0.29s ssh round trip, so more
  workers buy nothing and would take cores from trials."""

  timeout_sec: float = 600.0
  """Backstop for one append, not a tuning knob. Measured work is
  sub-second; this exists because the consumer is sequential, so an
  append that hung with no deadline would stop packing for the rest of
  the server's life."""


class PackPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  auto: bool | None = None
  processors: PositiveInt | None = None
  timeout_sec: float | None = None


def apply_patch(settings: PackSettings, patch: PackPatch) -> None:
  if patch.auto is not None:
    settings.auto = patch.auto
  if patch.processors is not None:
    settings.processors = patch.processors
  if patch.timeout_sec is not None:
    settings.timeout_sec = patch.timeout_sec


class Packer:
  """One queue, one consumer, one append at a time."""

  def __init__(
    self,
    *,
    scheduler: Scheduler,
    settings: PackSettings,
    self_host: str,
  ) -> None:
    self._scheduler = scheduler
    self._settings = settings
    self._self_host = self_host
    self._queue: asyncio.Queue[tuple[str, str, str]] = asyncio.Queue()

  def offer(self, job_id: str, instance_id: str, host: str) -> None:
    """Note that an instance is ready to pack. Returns immediately.

    Called from the terminal pipeline, which is the hot path — the
    comment there about adding "one NFS append and no other work" has
    to keep being true, so this does nothing but enqueue. Dropping the
    offer when packing is off keeps the queue from growing behind a
    consumer that will never read it."""
    if not self._settings.auto or not host or not instance_id:
      return
    self._queue.put_nowait((job_id, instance_id, host))

  async def run(self) -> None:
    """Drain forever. Cancelled by the lifespan, like every other
    background task — and CancelledError must travel, so nothing here
    catches it. `_append` swallows every other failure itself, which
    is why this loop needs no guard of its own."""
    while True:
      first = await self._queue.get()
      batch = [first, *self._drain()]
      for (job_id, host), ids in _group(batch).items():
        await self._append(job_id, host, ids)

  def _drain(self) -> list[tuple[str, str, str]]:
    """Everything already waiting, without blocking.

    This is the whole of the batching policy: take what arrived while
    the last append was running."""
    out: list[tuple[str, str, str]] = []
    while True:
      try:
        out.append(self._queue.get_nowait())
      except asyncio.QueueEmpty:
        return out

  async def _append(
    self, job_id: str, host: str, instance_ids: list[str]
  ) -> None:
    try:
      state = self._scheduler.job_state(job_id)
    except KeyError:
      return
    home_root = state.home_root
    try:
      await asyncio.to_thread(bust_dir_cache, home_root)
      already = await asyncio.to_thread(
        packed_instances, pack_path(home_root, host)
      )
      todo = [i for i in instance_ids if i not in already]
      # One stat each, because `mksquashfs` given a source it cannot
      # stat fails the WHOLE invocation — so a single home that is
      # missing (removed, or never written) would cost every instance
      # batched with it, and those offers are already consumed. The
      # precondition belongs here, not in the error path.
      todo = await asyncio.to_thread(_existing, home_root, todo)
      if not todo:
        return
      done = await run_on(
        host,
        self._self_host,
        pack_shell_cmd(
          home_root, host, todo, processors=self._settings.processors
        ),
        timeout=self._settings.timeout_sec,
      )
      await asyncio.to_thread(bust_dir_cache, home_root)
      packs = pack_dir(home_root)
      if packs.is_dir():
        await asyncio.to_thread(bust_dir_cache, packs)
      now = await asyncio.to_thread(
        packed_instances, pack_path(home_root, host)
      )
      landed = [i for i in todo if i in now]
      if len(landed) != len(todo):
        # Checked against the archive, not the exit code: a truncated
        # archive that exits 0 is the failure that would be served to
        # readers.
        logger.warning(
          "pack: %d/%d landed job=%s host=%s rc=%s %s",
          len(landed),
          len(todo),
          job_id,
          host,
          done.returncode,
          (done.stderr or "").strip()[-300:],
        )
    except asyncio.CancelledError:
      raise
    except Exception:
      # A pack is an optimisation. Losing one costs a slower read and
      # nothing else, so it must never reach the caller.
      logger.exception("pack failed job=%s host=%s", job_id, host)


def _existing(home_root: Path, instance_ids: list[str]) -> list[str]:
  """The ones that actually have a home to pack."""
  out = []
  for instance_id in instance_ids:
    if instance_home_for(home_root, instance_id).is_dir():
      out.append(instance_id)
    else:
      logger.warning(
        "pack: no home for %s under %s, skipping",
        instance_id,
        home_root,
      )
  return out


def _group(
  batch: list[tuple[str, str, str]],
) -> dict[tuple[str, str], list[str]]:
  """`(job, host) → instance ids`, which is one archive each.

  Deduplicated: a restart can re-offer an instance that is already in
  flight, and `mksquashfs` given the same source twice is not worth
  finding out about."""
  out: dict[tuple[str, str], list[str]] = {}
  for job_id, instance_id, host in batch:
    ids = out.setdefault((job_id, host), [])
    if instance_id not in ids:
      ids.append(instance_id)
  return out
