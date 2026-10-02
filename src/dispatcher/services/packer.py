"""Automatic packing: finished instance homes appended to their host's
archive, and the gap that says how much is not there yet.

Driven by the terminal pipeline, never by a timer. An instance going
`done_ok`/`done_err` is exactly the event that makes its home
immutable, so there is nothing to poll for and no interval to tune —
the same reasoning that keeps the readout service free of loops.

**What is queued is a job, not an instance.** A completion only marks
its job as worth looking at; the work itself is re-derived from disk
every time, by asking each archive what it already holds and comparing
that against the job's terminal instances. That is what makes the gap
converge rather than leak: a failed append, a host that was away, a
restart mid-job, a spell with `auto` switched off — none of them lose
anything, because nothing was ever remembered in the first place. The
next completion in that job re-derives the whole gap and closes it.

The same computation answers `pack_lag`, so the number an operator can
see and the work the packer will do cannot drift apart — one function,
two readers.

Batching falls out of it. One wake handles everything a job is missing
on a host, so a trickle of completions gives batches of one and a burst
gives however many accumulated. Nothing is held back and there is no
size or interval to get wrong.

Sequential, one append at a time across all hosts: exactly one writer
per archive without a lock, one task to supervise, no per-host
lifecycle. The cost is that an unreachable host holds the queue for its
timeout before the next is served.

When a pass makes no progress and work remains it stops rather than
re-queueing — the readout service's rule, for the same reason: a host
that is down must not become a spin. The gap stays visible, and
`dispatcher pack` is there for jobs no longer producing completions to
re-drive them (and for everything that finished before any of this
existed).

Nothing here can fail a run. `offer` only marks, the consumer swallows
everything, and a pack that has not happened yet is a slower read and
never a wrong one — see `core.pack` for why that is safe by
construction.

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
  packed_hosts,
  packed_instances,
)

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.scheduler import Scheduler

  _Stamp = tuple[int, int] | None
  """An archive's (mtime_ns, size), or None when it does not exist."""

logger = logging.getLogger(__name__)

_TERMINAL = ("done_ok", "done_err")
"""The only states whose homes may be packed.

This is the one place the design can be WRONG rather than slow: a pack
is preferred over the original, so an archive holding a half-written
home would serve that half as the whole. `running` is mid-write by
definition and `unknown`/`ghosted` mean no terminal signal was
found — which is exactly what a home being written looks like."""


class PackSettings(BaseModel):
  auto: bool = False
  """Off by default.

  Packing reads every finished instance home back over NFS from the
  host that wrote it, so it competes for the same bandwidth as running
  trials. The win is real but belongs to analysis, not to the run in
  flight, so switching it on is a deliberate choice — and one worth
  making after seeing the numbers on a job of your own size.

  `pack_lag` is reported either way, so a job's gap is visible whether
  or not anything is closing it automatically."""

  processors: PositiveInt = 2
  """`mksquashfs` workers on the remote host. Low on purpose: the
  append is 0.14s of CPU against a 0.29s ssh round trip, so more
  workers buy nothing and would take cores from trials."""

  timeout_sec: float = 600.0
  """Backstop for one append, not a tuning knob. Measured work is
  sub-second; this exists because the consumer is sequential, so an
  append that hung with no deadline would stop packing for the rest of
  the server's life. Both hang paths are real: `flock` without `-w`
  waits forever, so a hand-run `dispatcher pack` would block this, and
  the NFS mounts are `hard` — they block rather than erroring when a
  server goes away."""


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
  """One queue of job ids, one consumer, one append at a time."""

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
    self._queue: asyncio.Queue[str] = asyncio.Queue()
    self._queued: set[str] = set()
    # job_id → host → (stamp, what that archive holds). A cache OF
    # DISK keyed by the archive's own (mtime, size), not a record of
    # what this process did — `dispatcher pack` writes the same
    # archives from another process, and a cache that only knew about
    # its own appends reported a gap that had already been closed.
    # Validating by stat costs one call and owes nothing to knowing
    # who the writer was.
    self._held: dict[str, dict[str, tuple[_Stamp, set[str]]]] = {}

  # ── the gap ──────────────────────────────────────────────────

  def missing(self, job_id: str) -> dict[str, list[str]]:
    """`host → terminal instances of this job not in its archive`.

    The single source of truth for both the work and the number: the
    packer appends exactly this, and `pack_lag` counts exactly this.
    Re-derived from the scheduler view and the archives, so it owes
    nothing to any earlier attempt."""
    try:
      view = self._scheduler.job_view(job_id)
    except KeyError:
      return {}
    held = self._held.get(job_id, {})
    out: dict[str, list[str]] = {}
    for bucket in _TERMINAL:
      for tv in getattr(view, bucket).values():
        if not tv.host:
          continue
        entry = held.get(tv.host)
        if entry is not None and tv.instance_id in entry[1]:
          continue
        out.setdefault(tv.host, []).append(tv.instance_id)
    return out

  def pack_lag(self, job_id: str) -> int | None:
    """How many terminal instances are not in an archive, or None when
    this job's archives have not been read yet.

    Pure projection over memory — no disk, no await, because this is
    rendered on every job row of every poll. Reading disk here would
    put `unsquashfs` on the HTTP path, where a sick NFS server turns
    `GET /jobs` into a minutes-long wait at exactly the moment the
    operator most needs the page.

    None rather than a count before the first read: a cold cache means
    "nothing is archived", which would report every terminal instance
    as unpacked — wrong in the alarming direction, and it once sent me
    to `dispatcher pack` for a job that needed nothing. The dash the UI
    draws for None says "not known yet", which is true."""
    if job_id not in self._held:
      return None
    return sum(len(v) for v in self.missing(job_id).values())

  async def load(self, job_id: str) -> None:
    """Make the cache agree with the archives on disk.

    Called by the packer's own work, not by a read: a client asking for
    a job row must never be what goes to the filesystem.

    Re-reads an archive only when its (mtime, size) moved, so the
    steady state is one glob and one stat per archive and the contents
    are decompressed again only after somebody wrote them — whoever
    that somebody was, which is how `dispatcher pack` from another
    process gets noticed."""
    try:
      state = self._scheduler.job_state(job_id)
    except KeyError:
      return
    self._held[job_id] = await asyncio.to_thread(
      _read_held, state.home_root, self._held.get(job_id, {})
    )

  async def load_many(self, job_ids: list[str]) -> None:
    for job_id in job_ids:
      try:
        await self.load(job_id)
      except OSError as exc:
        logger.warning("pack load failed job=%s: %s", job_id, exc)

  async def load_all(self) -> None:
    """Boot: read every job's archives once, archived included.

    So that rows are answerable from the first page view rather than
    showing dashes until something completes. Archived jobs are where a
    finished experiment's files are, and `dispatcher pack` works on
    them, so they have a real gap worth reporting.

    One glob and one stat per archive for jobs that have none — which
    is most of them — so this is cheap even over NFS. It is also the
    only place a read of disk is allowed to delay anything, and at boot
    there is nothing yet to delay."""
    await self.load_many(list(self._scheduler.iter_job_ids()))

  # ── the queue ────────────────────────────────────────────────

  def offer(self, job_id: str, instance_id: str, host: str) -> None:
    """Note that a job has something new worth packing.

    Called from the terminal pipeline, which is the hot path — the
    comment there about adding "one NFS append and no other work" has
    to keep being true, so this does nothing but mark. The instance is
    not remembered: the consumer re-derives the whole gap, which is
    what lets an earlier failure be picked up by a later completion.

    De-duplicated, so a burst of completions on one job is one wake
    rather than hundreds."""
    if not self._settings.auto or not host or not instance_id:
      return
    # Whatever the archive held is now out of date for this host.
    self._held.get(job_id, {}).pop(host, None)
    if job_id in self._queued:
      return
    self._queued.add(job_id)
    self._queue.put_nowait(job_id)

  async def run(self) -> None:
    """Drain forever. Cancelled by the lifespan, like every other
    background task — and CancelledError must travel, so nothing here
    catches it. `_append` absorbs every other failure itself, which is
    why this loop needs no guard of its own."""
    while True:
      job_id = await self._queue.get()
      self._queued.discard(job_id)
      await self._pass(job_id)

  async def _pass(self, job_id: str) -> None:
    """One look at a job: append everything it is missing.

    Not re-queued on failure. A pass that moved nothing while work
    remains would re-queue into a spin against a host that is down, so
    the gap is left visible instead — the next completion in this job
    re-drives it, and `dispatcher pack` covers a job that has stopped
    producing them."""
    # The one place disk is read before deriving the gap — the packer's
    # own work, off the HTTP path.
    await self.load(job_id)
    before = self.pack_lag(job_id) or 0
    for host, instance_ids in self.missing(job_id).items():
      await self._append(job_id, host, instance_ids)
    after = self.pack_lag(job_id) or 0
    if after and after >= before:
      logger.warning(
        "pack: job=%s made no progress, %d instance(s) unpacked",
        job_id,
        after,
      )

  # ── one archive ──────────────────────────────────────────────

  async def _append(
    self, job_id: str, host: str, instance_ids: list[str]
  ) -> None:
    try:
      state = self._scheduler.job_state(job_id)
    except KeyError:
      return
    home_root = state.home_root
    try:
      # One stat each, because `mksquashfs` given a source it cannot
      # stat fails the WHOLE invocation — so a single home that is
      # missing (removed, or never written) would cost every instance
      # batched with it. The precondition belongs here, not in the
      # error path.
      todo = await asyncio.to_thread(_existing, home_root, instance_ids)
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
      now, stamp = await asyncio.to_thread(_read_one_host, home_root, host)
      self._held.setdefault(job_id, {})[host] = (stamp, now)
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
      # nothing else, so it must never reach the caller — and the gap
      # stays in `pack_lag`, so it is not lost either.
      self._held.get(job_id, {}).pop(host, None)
      logger.exception("pack failed job=%s host=%s", job_id, host)


def _stamp_of(archive: Path) -> _Stamp:
  """What makes a cached read still valid. None when the archive is
  not there, so its appearance invalidates too."""
  try:
    st = archive.stat()
  except OSError:
    return None
  return (st.st_mtime_ns, st.st_size)


def _see_fresh(home_root: Path) -> None:
  """Drop this client's cached listings for the pack dir and its
  parent.

  `mksquashfs` ran on another host, and the negative-dentry cache
  answers "no such file" for up to acdirmax — 60s by default — which
  once made three good appends report as failures."""
  bust_dir_cache(home_root)
  packs = pack_dir(home_root)
  if packs.is_dir():
    bust_dir_cache(packs)


def _read_one_host(home_root: Path, host: str) -> tuple[set[str], _Stamp]:
  """One archive, right after writing it — contents and stamp read
  together so the pair cannot disagree."""
  _see_fresh(home_root)
  archive = pack_path(home_root, host)
  return packed_instances(archive), _stamp_of(archive)


def _read_held(
  home_root: Path, known: dict[str, tuple[_Stamp, set[str]]]
) -> dict[str, tuple[_Stamp, set[str]]]:
  """What every archive of this job holds, reusing what still matches.

  `unsquashfs` runs only for an archive whose stat moved, which is what
  keeps this cheap enough to sit in front of a read while still noticing
  a write from another process."""
  _see_fresh(home_root)
  out: dict[str, tuple[_Stamp, set[str]]] = {}
  for host in packed_hosts(home_root):
    archive = pack_path(home_root, host)
    stamp = _stamp_of(archive)
    cached = known.get(host)
    if cached is not None and cached[0] == stamp:
      out[host] = cached
      continue
    out[host] = (stamp, packed_instances(archive))
  return out


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
