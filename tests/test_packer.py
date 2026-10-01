"""Automatic packing: the queue, the coalescing, and the promises that
make it safe to leave on.

Three properties carry the design, so each gets a test that would fail
if it broke: `offer` never blocks or raises on the terminal path,
cancellation travels so shutdown cannot hang, and a failing append is
invisible to everything else."""

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
  _group,
  apply_patch,
)
from tests.test_runtime import mk_job
from tests.test_scheduler import clock_from, id_gen

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.models import JobState


def mk_packer(
  tmp_path: Path, *, auto: bool = True, self_host: str = "ml10"
) -> tuple[Packer, JobState]:
  """A real `Scheduler`, so `job_state` behaves — including raising for
  a job that is gone, which is a case the packer has to survive."""
  scheduler = Scheduler(
    max_concurrent=2,
    hosts={"ml10": HostSettings(max_concurrent=2)},
    clock=clock_from(),
    id_gen=id_gen(),
  )
  job = mk_job(tmp_path / "home", ["t1", "t2"])
  job.home_root.mkdir(parents=True, exist_ok=True)
  scheduler.submit(job)
  packer = Packer(
    scheduler=scheduler,
    settings=PackSettings(auto=auto),
    self_host=self_host,
  )
  return packer, job


def lay_down(home_root: Path, instance_id: str) -> None:
  home = home_root / instance_id
  home.mkdir(parents=True, exist_ok=True)
  (home / "outcome.json").write_text('{"ok": true}', encoding="utf-8")


# ── the queue ────────────────────────────────────────────────────


def test_offer_is_dropped_when_packing_is_off(tmp_path: Path):
  # Not merely ignored downstream — never queued, so a server with
  # packing off cannot accumulate a backlog behind a consumer that
  # will not act on it.
  packer, job = mk_packer(tmp_path, auto=False)
  packer.offer(job.job_id, "i-1", "ml9")
  assert packer._queue.qsize() == 0


def test_offer_ignores_an_instance_with_no_host(tmp_path: Path):
  packer, job = mk_packer(tmp_path)
  packer.offer(job.job_id, "i-1", "")
  packer.offer(job.job_id, "", "ml9")
  assert packer._queue.qsize() == 0


def test_offer_does_not_block_or_raise(tmp_path: Path):
  # It runs on the terminal pipeline, so this is the whole contract.
  packer, job = mk_packer(tmp_path)
  for i in range(500):
    packer.offer(job.job_id, f"i-{i}", "ml9")
  assert packer._queue.qsize() == 500


def test_offer_for_an_unknown_job_is_harmless(tmp_path: Path):
  packer, _job = mk_packer(tmp_path)
  packer.offer("job-that-went-away", "i-1", "ml9")
  assert packer._queue.qsize() == 1


# ── coalescing ───────────────────────────────────────────────────


def test_group_is_one_archive_per_entry():
  grouped = _group(
    [
      ("job-a", "i-1", "ml9"),
      ("job-a", "i-2", "ml9"),
      ("job-a", "i-3", "ml10"),
      ("job-b", "i-4", "ml9"),
    ]
  )
  assert grouped == {
    ("job-a", "ml9"): ["i-1", "i-2"],
    ("job-a", "ml10"): ["i-3"],
    ("job-b", "ml9"): ["i-4"],
  }


def test_group_dedupes_a_re_offered_instance():
  # A restart can re-offer what is already in flight.
  assert _group([("j", "i-1", "ml9"), ("j", "i-1", "ml9")]) == {
    ("j", "ml9"): ["i-1"]
  }


def test_drain_takes_everything_waiting(tmp_path: Path):
  packer, job = mk_packer(tmp_path)
  for i in range(5):
    packer.offer(job.job_id, f"i-{i}", "ml9")
  first = packer._queue.get_nowait()
  rest = packer._drain()
  assert len(rest) == 4
  assert packer._queue.qsize() == 0
  assert first[1] == "i-0"


# ── running it for real ──────────────────────────────────────────


async def test_a_batch_becomes_one_archive(tmp_path: Path):
  packer, job = mk_packer(tmp_path)
  for name in ("i-1", "i-2"):
    lay_down(job.home_root, name)
    packer.offer(job.job_id, name, "ml10")
  task = asyncio.create_task(packer.run())
  archive = pack_path(job.home_root, "ml10")
  async with asyncio.timeout(60):
    while not archive.is_file():
      await asyncio.sleep(0.05)
    while packed_instances(archive) != {"i-1", "i-2"}:
      await asyncio.sleep(0.05)
  task.cancel()
  with pytest.raises(asyncio.CancelledError):
    await task
  # Both offers coalesced into a single archive — and the originals
  # are untouched, which is what keeps readers correct either way.
  assert (job.home_root / "i-1" / "outcome.json").is_file()


async def test_cancellation_travels_so_shutdown_cannot_hang(
  tmp_path: Path,
):
  # The lifespan does `task.cancel()` then `await task`. A consumer
  # that swallowed CancelledError would leave that await forever.
  packer, _job = mk_packer(tmp_path)
  task = asyncio.create_task(packer.run())
  await asyncio.sleep(0)
  task.cancel()
  async with asyncio.timeout(5):
    with pytest.raises(asyncio.CancelledError):
      await task


async def test_a_homeless_instance_does_not_cost_its_batch(
  tmp_path: Path,
):
  # `mksquashfs` fails the WHOLE invocation on a source it cannot
  # stat, and the offers in that batch are already consumed — so
  # without the precondition check one missing home would silently
  # keep every instance batched with it out of the archive forever.
  packer, job = mk_packer(tmp_path)
  lay_down(job.home_root, "i-ok")
  packer.offer(job.job_id, "never-existed", "ml10")
  packer.offer(job.job_id, "i-ok", "ml10")
  assert packer._queue.qsize() == 2  # one batch, as the consumer sees it
  task = asyncio.create_task(packer.run())
  archive = pack_path(job.home_root, "ml10")
  async with asyncio.timeout(60):
    while "i-ok" not in packed_instances(archive):
      await asyncio.sleep(0.05)
  task.cancel()
  with pytest.raises(asyncio.CancelledError):
    await task
  assert packed_instances(archive) == {"i-ok"}


async def test_the_loop_survives_a_failing_append(tmp_path: Path):
  # An archive path that cannot be written at all: the consumer must
  # log and come back for more rather than die on it.
  packer, job = mk_packer(tmp_path)
  blocked = pack_path(job.home_root, "ml9").parent
  blocked.parent.mkdir(parents=True, exist_ok=True)
  blocked.write_text("not a directory", encoding="utf-8")
  lay_down(job.home_root, "i-1")
  packer.offer(job.job_id, "i-1", "ml9")
  task = asyncio.create_task(packer.run())
  await asyncio.sleep(2.0)
  assert not task.done()
  task.cancel()
  with pytest.raises(asyncio.CancelledError):
    await task


async def test_an_unknown_job_is_skipped_not_fatal(tmp_path: Path):
  packer, job = mk_packer(tmp_path)
  packer.offer("job-that-went-away", "i-1", "ml10")
  lay_down(job.home_root, "i-2")
  packer.offer(job.job_id, "i-2", "ml10")
  task = asyncio.create_task(packer.run())
  async with asyncio.timeout(60):
    while "i-2" not in packed_instances(pack_path(job.home_root, "ml10")):
      await asyncio.sleep(0.05)
  task.cancel()
  with pytest.raises(asyncio.CancelledError):
    await task


async def test_an_already_packed_instance_is_not_appended_twice(
  tmp_path: Path,
):
  packer, job = mk_packer(tmp_path)
  lay_down(job.home_root, "i-1")
  packer.offer(job.job_id, "i-1", "ml10")
  task = asyncio.create_task(packer.run())
  archive = pack_path(job.home_root, "ml10")
  async with asyncio.timeout(60):
    while "i-1" not in packed_instances(archive):
      await asyncio.sleep(0.05)
  size = archive.stat().st_size
  packer.offer(job.job_id, "i-1", "ml10")
  await asyncio.sleep(1.0)
  task.cancel()
  with pytest.raises(asyncio.CancelledError):
    await task
  assert archive.stat().st_size == size


# ── settings ─────────────────────────────────────────────────────


def test_packing_is_off_by_default():
  # It reads finished homes back over NFS, so it competes with running
  # trials; switching it on is the operator's call.
  assert PackSettings().auto is False


def test_patch_applies_each_knob():
  settings = PackSettings()
  apply_patch(settings, PackPatch(auto=True, processors=4))
  assert (settings.auto, settings.processors) == (True, 4)
  apply_patch(settings, PackPatch())
  assert (settings.auto, settings.processors) == (True, 4)
