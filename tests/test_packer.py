"""Automatic packing: the gap, its convergence, and the promises that
make it safe to leave on.

The design's claim is that nothing is ever lost — the work is
re-derived from disk on every look rather than remembered, so a failed
append, a host that was away or a spell with `auto` off all show up in
`pack_lag` and get closed by the next completion. The convergence test
is the one that would catch that claim breaking.

Three other properties each get a test because each is load-bearing:
`offer` never blocks or raises on the terminal path, cancellation
travels so shutdown cannot hang, and only terminal instances are ever
packed."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from dispatcher.core.models import HostSettings
from dispatcher.core.pack import pack_path, packed_instances
from dispatcher.core.scheduler import Scheduler
from dispatcher.services.packer import (
  Packer,
  PackPatch,
  PackSettings,
  apply_patch,
)
from tests.test_runtime import mk_job
from tests.test_scheduler import clock_from, id_gen

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.models import JobState

SELF = "ml10"
HOSTS = (SELF,)
"""One host, and it IS `self_host`.

`run_on` really does ssh for any other name, so a second host here
would send an append to a live machine — where the test's `tmp_path`
does not exist, since /tmp is per-node. A test that appends must stay
local; the one test that needs two hosts never appends."""


def mk_packer(
  tmp_path: Path,
  *,
  auto: bool = True,
  tasks: int = 2,
  hosts: tuple[str, ...] = HOSTS,
) -> tuple[Packer, Scheduler, JobState]:
  """A real `Scheduler`, so `job_view` and `job_state` behave —
  including raising for a job that is gone, a case the packer
  survives."""
  scheduler = Scheduler(
    max_concurrent=tasks,
    hosts={h: HostSettings(max_concurrent=tasks) for h in hosts},
    clock=clock_from(),
    id_gen=id_gen(),
  )
  job = mk_job(tmp_path / "home", [f"t{i}" for i in range(tasks)])
  job.home_root.mkdir(parents=True, exist_ok=True)
  scheduler.submit(job)
  packer = Packer(
    scheduler=scheduler,
    settings=PackSettings(auto=auto),
    self_host=SELF,
  )
  return packer, scheduler, job


def finish(
  packer: Packer,
  scheduler: Scheduler,
  job: JobState,
  task_id: str,
  *,
  state: str = "done_ok",
  lay: bool = True,
  tell: bool = True,
) -> str:
  """Move a task to a terminal state the way the runtime would, write
  its home, and offer it — `lay=False` and `tell=False` isolate the
  cases where one of those did not happen."""
  scheduler.dispatch_one()
  scheduler.transition_instance(
    job_id=job.job_id,
    task_id=task_id,
    from_state="running",
    to_state=state,  # type: ignore[arg-type]
  )
  view = getattr(scheduler.job_view(job.job_id), state)[task_id]
  if lay:
    home = job.home_root / view.instance_id
    (home / "agent").mkdir(parents=True, exist_ok=True)
    (home / "agent" / "events.jsonl").write_text(
      f"from {view.instance_id}\n", encoding="utf-8"
    )
  if tell:
    packer.offer(job.job_id, view.instance_id, view.host)
  return view.instance_id


async def drain(packer: Packer, job_id: str) -> None:
  """Run one pass by hand, the way the consumer would."""
  await packer._pass(job_id)


def held(packer: Packer, job_id: str) -> set[str]:
  """Every instance the cache believes is archived, flattened.

  Reaches into `_held` in one place so the tests do not all couple to
  its shape — it is a `(stamp, contents)` pair per host, and the stamp
  is what lets another process's write invalidate it."""
  return {i for _stamp, ids in packer._held[job_id].values() for i in ids}


# ── offer ────────────────────────────────────────────────────────


def test_offer_is_dropped_when_packing_is_off(tmp_path: Path):
  # Not merely ignored downstream — never queued, so a server with
  # packing off cannot accumulate a backlog behind a consumer that
  # will not act on it.
  packer, _s, job = mk_packer(tmp_path, auto=False)
  packer.offer(job.job_id, "i-1", "ml9")
  assert packer._queue.qsize() == 0


def test_offer_ignores_an_instance_with_no_host(tmp_path: Path):
  packer, _s, job = mk_packer(tmp_path)
  packer.offer(job.job_id, "i-1", "")
  packer.offer(job.job_id, "", "ml9")
  assert packer._queue.qsize() == 0


def test_offer_queues_a_job_once_however_many_completions(
  tmp_path: Path,
):
  # What is queued is a JOB, so a burst on one job is one wake. The
  # consumer re-derives the whole gap anyway, so remembering each
  # instance would buy nothing.
  packer, _s, job = mk_packer(tmp_path)
  for i in range(200):
    packer.offer(job.job_id, f"i-{i}", "ml9")
  assert packer._queue.qsize() == 1


def test_offer_does_not_block_or_raise(tmp_path: Path):
  # It runs on the terminal pipeline, so this is the whole contract.
  packer, _s, job = mk_packer(tmp_path)
  for i in range(500):
    packer.offer(job.job_id, f"i-{i}", SELF)
  packer.offer("job-that-went-away", "i-x", "ml9")
  assert packer._queue.qsize() == 2


# ── the gap ──────────────────────────────────────────────────────


async def test_lag_counts_terminal_instances_not_in_an_archive(
  tmp_path: Path,
):
  packer, scheduler, job = mk_packer(tmp_path)
  finish(packer, scheduler, job, "t0")
  finish(packer, scheduler, job, "t1")
  await packer.load(job.job_id)
  assert packer.pack_lag(job.job_id) == 2
  await drain(packer, job.job_id)
  assert packer.pack_lag(job.job_id) == 0


async def test_only_terminal_instances_are_counted_or_packed(
  tmp_path: Path,
):
  # The one way this design could be WRONG rather than slow: a pack is
  # preferred over the original, so a half-written home inside one
  # would be served as the whole.
  packer, scheduler, job = mk_packer(tmp_path, tasks=2)
  scheduler.dispatch_one()  # t0 left running
  finish(packer, scheduler, job, "t1")
  await drain(packer, job.job_id)
  assert packer.pack_lag(job.job_id) == 0  # the running one is not owed
  assert len(held(packer, job.job_id)) == 1


async def test_done_err_is_packed_too(tmp_path: Path):
  packer, scheduler, job = mk_packer(tmp_path)
  finish(packer, scheduler, job, "t0", state="done_err")
  await drain(packer, job.job_id)
  assert packer.pack_lag(job.job_id) == 0


async def test_lag_is_visible_with_auto_off(tmp_path: Path):
  # The number has to be honest whether or not anything is closing it:
  # that is what makes `dispatcher pack` something an operator can see
  # a reason to run.
  packer, scheduler, job = mk_packer(tmp_path, auto=False)
  finish(packer, scheduler, job, "t0")
  await packer.load(job.job_id)
  assert packer.pack_lag(job.job_id) == 1
  assert packer._queue.qsize() == 0


# ── convergence ──────────────────────────────────────────────────


async def test_a_failed_append_is_picked_up_by_a_later_completion(
  tmp_path: Path,
):
  """The whole point of re-deriving instead of remembering.

  An append that fails loses its offer — and must not lose the work.
  Here the pack dir is blocked by a FILE so `mkdir -p` cannot make it,
  the blockage is cleared, and the NEXT completion closes both."""
  packer, scheduler, job = mk_packer(tmp_path, tasks=2)
  blocked = pack_path(job.home_root, SELF).parent
  blocked.write_text("not a directory", encoding="utf-8")

  first = finish(packer, scheduler, job, "t0")
  await drain(packer, job.job_id)
  assert packer.pack_lag(job.job_id) == 1  # still owed, and visible

  blocked.unlink()
  second = finish(packer, scheduler, job, "t1")
  await drain(packer, job.job_id)

  assert packer.pack_lag(job.job_id) == 0
  assert held(packer, job.job_id) == {first, second}


async def test_an_instance_that_was_never_offered_is_still_packed(
  tmp_path: Path,
):
  # Covers a restart mid-job and a spell with `auto` off: the gap comes
  # from disk, so an instance nobody told the packer about is picked up
  # by the next pass all the same.
  packer, scheduler, job = mk_packer(tmp_path, tasks=2)
  missed = finish(packer, scheduler, job, "t0", tell=False)
  finish(packer, scheduler, job, "t1")
  await drain(packer, job.job_id)
  assert missed in held(packer, job.job_id)


async def test_a_pass_that_moves_nothing_does_not_requeue(
  tmp_path: Path,
):
  # No progress with work remaining must not become a spin against a
  # host that is down — the gap is left visible instead.
  packer, scheduler, job = mk_packer(tmp_path)
  blocked = pack_path(job.home_root, SELF).parent
  blocked.write_text("not a directory", encoding="utf-8")
  finish(packer, scheduler, job, "t0")
  packer._queue.get_nowait()
  packer._queued.discard(job.job_id)
  await drain(packer, job.job_id)
  assert packer.pack_lag(job.job_id) == 1
  assert packer._queue.qsize() == 0


async def test_a_homeless_instance_does_not_cost_its_batch(
  tmp_path: Path,
):
  # `mksquashfs` fails the WHOLE invocation on a source it cannot
  # stat, so without the precondition check one missing home would
  # keep everything batched with it out of the archive.
  packer, scheduler, job = mk_packer(tmp_path, tasks=2)
  finish(packer, scheduler, job, "t0", lay=False)
  good = finish(packer, scheduler, job, "t1")
  await drain(packer, job.job_id)
  assert good in held(packer, job.job_id)


# ── the consumer ─────────────────────────────────────────────────


async def test_the_consumer_closes_the_gap(tmp_path: Path):
  packer, scheduler, job = mk_packer(tmp_path)
  finish(packer, scheduler, job, "t0")
  finish(packer, scheduler, job, "t1")
  task = asyncio.create_task(packer.run())
  async with asyncio.timeout(60):
    while packer.pack_lag(job.job_id) != 0 or not packer._held:
      await asyncio.sleep(0.05)
  task.cancel()
  with pytest.raises(asyncio.CancelledError):
    await task
  # The originals are untouched, which is what keeps readers correct
  # whichever path they take.
  for archive in (job.home_root / ".packs").glob("*.sqfs"):
    assert packed_instances(archive)
  assert list(job.home_root.glob("t*__*"))


async def test_cancellation_travels_so_shutdown_cannot_hang(
  tmp_path: Path,
):
  # The lifespan does `task.cancel()` then `await task`. A consumer
  # that swallowed CancelledError would leave that await forever.
  packer, _s, _job = mk_packer(tmp_path)
  task = asyncio.create_task(packer.run())
  await asyncio.sleep(0)
  task.cancel()
  async with asyncio.timeout(5):
    with pytest.raises(asyncio.CancelledError):
      await task


async def test_an_unknown_job_is_skipped_not_fatal(tmp_path: Path):
  packer, scheduler, job = mk_packer(tmp_path)
  packer.offer("job-that-went-away", "i-1", SELF)
  finish(packer, scheduler, job, "t0")
  task = asyncio.create_task(packer.run())
  async with asyncio.timeout(60):
    while packer.pack_lag(job.job_id) != 0 or not packer._held:
      await asyncio.sleep(0.05)
  task.cancel()
  with pytest.raises(asyncio.CancelledError):
    await task


# ── the cache must not lie ───────────────────────────────────────


async def test_an_unread_job_reports_null_not_a_count(tmp_path: Path):
  """Found on the live server: every job reported its full terminal
  count as unpacked, including ones that were fully packed.

  A cold cache means "nothing is archived", so deriving a NUMBER from
  it is wrong in the alarming direction — it sent me to `dispatcher
  pack` for a job that needed nothing. The answer is None: the UI draws
  a dash, which says "not known yet" and is true."""
  packer, scheduler, job = mk_packer(tmp_path)
  finish(packer, scheduler, job, "t0")
  await drain(packer, job.job_id)
  assert packer.pack_lag(job.job_id) == 0

  # A fresh packer over the same archives is the cold-cache case.
  cold = Packer(
    scheduler=scheduler,
    settings=PackSettings(auto=True),
    self_host=SELF,
  )
  assert cold.pack_lag(job.job_id) is None  # not 1, and not 0
  await cold.load(job.job_id)
  assert cold.pack_lag(job.job_id) == 0


async def test_pack_lag_never_touches_disk(tmp_path: Path, monkeypatch):
  """It is rendered on every job row of every poll, so `unsquashfs` on
  that path would make `GET /jobs` wait on NFS — minutes of it when a
  server is sick, which is exactly when the page matters."""
  packer, scheduler, job = mk_packer(tmp_path)
  finish(packer, scheduler, job, "t0")
  await drain(packer, job.job_id)

  def explode(*_a, **_k):
    raise AssertionError("pack_lag read disk")

  monkeypatch.setattr(
    "dispatcher.services.packer.packed_instances", explode
  )
  monkeypatch.setattr("dispatcher.services.packer.packed_hosts", explode)
  assert packer.pack_lag(job.job_id) == 0
  assert packer.missing(job.job_id) == {}


async def test_load_all_covers_archived_jobs(tmp_path: Path):
  # Archived is where a finished experiment's files are, and
  # `dispatcher pack` works on them, so they have a real gap worth
  # reporting from the first page view.
  packer, scheduler, job = mk_packer(tmp_path)
  finish(packer, scheduler, job, "t0")
  assert packer.pack_lag(job.job_id) is None
  await packer.load_all()
  assert packer.pack_lag(job.job_id) == 1


async def test_an_archive_written_by_another_process_is_noticed(
  tmp_path: Path,
):
  """Found on the live server: `dispatcher pack` closed a job's gap and
  the server kept reporting it.

  Only `offer` invalidated the cache, so a write from any other process
  — which is exactly what the catch-up command is — was invisible. The
  cache is keyed by the archive's (mtime, size) now, so a load notices
  whoever wrote it."""
  packer, scheduler, job = mk_packer(tmp_path, tasks=2)
  first = finish(packer, scheduler, job, "t0")
  await drain(packer, job.job_id)
  assert packer.pack_lag(job.job_id) == 0

  # A second terminal instance, packed by somebody else entirely.
  second = finish(packer, scheduler, job, "t1", tell=False)
  other = Packer(
    scheduler=scheduler,
    settings=PackSettings(auto=True),
    self_host=SELF,
  )
  await drain(other, job.job_id)
  assert held(other, job.job_id) == {first, second}

  # The first packer never saw that happen — and must still agree.
  await packer.load(job.job_id)
  assert packer.pack_lag(job.job_id) == 0
  assert held(packer, job.job_id) == {first, second}


async def test_a_cached_read_is_reused_while_the_archive_is_untouched(
  tmp_path: Path,
):
  # The stamp is what makes the check cheap: an unchanged archive must
  # not be decompressed again on every job row of every poll.
  packer, scheduler, job = mk_packer(tmp_path)
  finish(packer, scheduler, job, "t0")
  await drain(packer, job.job_id)
  before = packer._held[job.job_id]
  await packer.load(job.job_id)
  after = packer._held[job.job_id]
  # Same tuples, not merely equal sets — the contents were not re-read.
  assert all(after[h] is before[h] for h in before)


# ── settings ─────────────────────────────────────────────────────


def test_packing_is_off_by_default():
  # It reads finished homes back over NFS, so it competes with running
  # trials; switching it on is the operator's call. `pack_lag` is
  # reported either way.
  assert PackSettings().auto is False


def test_patch_applies_each_knob():
  settings = PackSettings()
  apply_patch(settings, PackPatch(auto=True, processors=4, timeout_sec=5))
  assert (settings.auto, settings.processors, settings.timeout_sec) == (
    True,
    4,
    5,
  )
  apply_patch(settings, PackPatch())
  assert settings.auto is True


# ── telling the server about an outside write ────────────────────


def test_packs_changed_reloads_one_job(tmp_path: Path):
  """The server never reads archives on its HTTP path, so it cannot see
  a write it did not make — `dispatcher pack` is exactly that writer.

  Rather than poll, the writer says so. Without this the gap the command
  just closed kept being reported until the next completion or a
  restart, which is the same symptom as the cache bug it replaced."""
  from tests.test_server import mk_client

  with mk_client(tmp_path) as client:
    body = {
      "label": "arm",
      "task_ids": ["t0"],
      "home_root": str(tmp_path / "home"),
      "container": {"image": "img", "command": ["true"]},
    }
    created = client.post("/jobs", json=body)
    assert created.status_code == 200
    job_id = created.json()["job_id"]

    resp = client.post(f"/jobs/{job_id}/packs-changed")
    assert resp.status_code == 200
    assert resp.json()["job_id"] == job_id
    # Nothing is packed, but the job IS read now — so the answer is a
    # number rather than the "not known yet" null.
    assert resp.json()["pack_lag"] == 0


def test_packs_changed_404s_for_an_unknown_job(tmp_path: Path):
  from tests.test_server import mk_client

  with mk_client(tmp_path) as client:
    assert client.post("/jobs/nope/packs-changed").status_code == 404
