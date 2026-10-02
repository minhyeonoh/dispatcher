"""Loop shells: supervision, cadence, error isolation."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import pytest

from dispatcher.core.loops import LoopSkip, every, fan_out, supervised
from dispatcher.services.auto_archive import (
  archive_loop,
  scan_auto_archive_candidates,
)

if TYPE_CHECKING:
  from pathlib import Path


# ── supervised ───────────────────────────────────────────────────


def test_supervised_restarts_after_death():
  async def main():
    calls = [0]
    done = asyncio.Event()

    async def factory():
      calls[0] += 1
      if calls[0] == 1:
        raise RuntimeError("boom")  # first life dies
      done.set()  # second life runs

    task = asyncio.ensure_future(
      supervised("t", factory, restart_delay_s=0)
    )
    await asyncio.wait_for(done.wait(), timeout=5)
    task.cancel()
    assert calls[0] == 2

  asyncio.run(main())


def test_supervised_restarts_after_base_exception():
  # The incident class this guards: a BaseException slipping past
  # `except Exception` killed the GC task silently for 25h.
  async def main():
    calls = [0]
    done = asyncio.Event()

    async def factory():
      calls[0] += 1
      if calls[0] == 1:
        raise SystemExit(1)
      done.set()

    task = asyncio.ensure_future(
      supervised("t", factory, restart_delay_s=0)
    )
    await asyncio.wait_for(done.wait(), timeout=5)
    task.cancel()
    assert calls[0] == 2

  asyncio.run(main())


def test_supervised_propagates_cancellation():
  async def main():
    started = asyncio.Event()

    async def factory():
      started.set()
      await asyncio.sleep(3600)

    task = asyncio.ensure_future(supervised("t", factory))
    await started.wait()
    task.cancel()
    try:
      await task
      raise AssertionError("expected CancelledError")
    except asyncio.CancelledError:
      pass

  asyncio.run(main())


def test_supervised_exits_when_factory_returns():
  async def main():
    async def factory():
      return None

    await asyncio.wait_for(supervised("t", factory), timeout=5)

  asyncio.run(main())


# ── every ────────────────────────────────────────────────────────


def _run_ticks(tick, *, enabled_fn=None, until: int = 3, intervals=None):
  """Drive `every` until the tick counter reaches `until`."""

  async def main():
    seen_intervals: list[float] = []

    def interval_fn() -> float:
      if intervals is not None:
        seen_intervals.append(intervals[len(seen_intervals)])
        return 0
      return 0

    stop = asyncio.Event()
    count = [0]

    async def counting_tick():
      count[0] += 1
      await tick(count[0])
      if count[0] >= until:
        stop.set()

    task = asyncio.ensure_future(
      every(
        "t",
        interval_fn if intervals is not None else (lambda: 0),
        counting_tick,
        enabled_fn=enabled_fn,
      )
    )
    await asyncio.wait_for(stop.wait(), timeout=5)
    task.cancel()
    return count[0]

  return asyncio.run(main())


def test_every_tick_failure_does_not_stop_next_tick():
  async def tick(n: int):
    if n == 1:
      raise RuntimeError("one bad tick")

  assert _run_ticks(tick, until=3) == 3


def test_every_skips_ticks_while_disabled():
  enabled = [False]
  fired = []

  async def main():
    stop = asyncio.Event()

    async def tick():
      fired.append(True)
      stop.set()

    async def flip_soon():
      # A few disabled iterations pass, then enable.
      await asyncio.sleep(0.05)
      enabled[0] = True

    task = asyncio.ensure_future(
      every("t", lambda: 0.01, tick, enabled_fn=lambda: enabled[0])
    )
    flip = asyncio.ensure_future(flip_soon())
    await asyncio.wait_for(stop.wait(), timeout=5)
    task.cancel()
    flip.cancel()

  asyncio.run(main())
  assert fired == [True]


def test_every_rereads_interval_each_iteration():
  # PATCH /settings must apply on the next boundary — the shell
  # must call interval_fn every loop, not snapshot it.
  seen: list[int] = []

  async def main():
    stop = asyncio.Event()

    def interval_fn() -> float:
      seen.append(len(seen))
      return 0

    async def tick():
      if len(seen) >= 3:
        stop.set()

    task = asyncio.ensure_future(every("t", interval_fn, tick))
    await asyncio.wait_for(stop.wait(), timeout=5)
    task.cancel()

  asyncio.run(main())
  assert len(seen) >= 3


# ── archive loop ─────────────────────────────────────────────────


def _archive_settings(days: int):
  from dispatcher.services.auto_archive import ArchiveSettings

  return ArchiveSettings(auto_after_days=days, scan_interval_seconds=0.001)


def test_archive_loop_applies_and_skips(monkeypatch, tmp_path: Path):
  from datetime import UTC, datetime

  import dispatcher.services.auto_archive as loops_mod

  applied: list[str] = []

  async def archive_one(aid: str) -> None:
    if aid == "skippy":
      raise LoopSkip("re-flipped to unknown")
    applied.append(aid)

  monkeypatch.setattr(
    loops_mod,
    "scan_auto_archive_candidates",
    lambda scheduler, now, days: ["a1", "skippy", "a2"],
  )

  async def main():
    task = asyncio.ensure_future(
      archive_loop(
        scheduler=None,  # type: ignore[arg-type] — scan is stubbed
        settings=_archive_settings(7),
        clock_fn=lambda: datetime.now(UTC),
        archive_one=archive_one,
      )
    )
    for _ in range(200):
      if applied:
        break
      await asyncio.sleep(0.01)
    task.cancel()

  asyncio.run(main())
  assert applied[:2] == ["a1", "a2"]


async def _noop_archive(aid: str) -> None:
  return None


def test_archive_loop_disabled_at_zero_days(monkeypatch):
  from datetime import UTC, datetime

  import dispatcher.services.auto_archive as loops_mod

  scanned = []
  monkeypatch.setattr(
    loops_mod,
    "scan_auto_archive_candidates",
    lambda *a: scanned.append(True) or [],
  )

  async def main():
    task = asyncio.ensure_future(
      archive_loop(
        scheduler=None,  # type: ignore[arg-type]
        settings=_archive_settings(0),
        clock_fn=lambda: datetime.now(UTC),
        archive_one=_noop_archive,
      )
    )
    await asyncio.sleep(0.1)
    task.cancel()

  asyncio.run(main())
  assert scanned == []


# ── candidate scan ───────────────────────────────────────────────


def test_scan_candidates_terminal_and_idle_only(tmp_path: Path):
  import os
  from datetime import UTC, datetime

  from dispatcher.core.models import Outcome
  from tests.test_runtime import mk_job, mk_sched
  from tests.test_scheduler import complete_ok

  sched = mk_sched(10)
  # idle-done: terminal, old log.
  sched.submit(mk_job(tmp_path / "old", ["t1"], job_id="job-old"))
  # fresh-done: terminal, recent log.
  sched.submit(mk_job(tmp_path / "new", ["t1"], job_id="job-new"))
  # busy: still pending.
  sched.submit(mk_job(tmp_path / "busy", ["t1"], job_id="job-busy"))
  for aid, home in (("job-old", "old"), ("job-new", "new")):
    action = None
    while True:
      action = sched.dispatch_one()
      assert action is not None
      if action.job_id == aid:
        break
      sched.transition_instance(
        job_id=action.job_id,
        task_id=action.task_id,
        from_state="running",
        to_state="done_ok",
        outcome=Outcome(ok=True),
      )
    complete_ok(sched, action)
    log = tmp_path / home / ".dispatcher-state.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text("{}\n")
  # Age the old log 10 days back.
  old_log = tmp_path / "old" / ".dispatcher-state.jsonl"
  past = datetime.now(UTC).timestamp() - 10 * 86400
  os.utime(old_log, (past, past))

  out = scan_auto_archive_candidates(
    sched, datetime.now(UTC), threshold_days=7
  )
  assert out == ["job-old"]


# ── fan_out ──────────────────────────────────────────────────────


async def test_fan_out_runs_every_item():
  seen: list[int] = []

  async def work(i: int) -> None:
    seen.append(i)

  await fan_out("t", range(5), work)
  assert sorted(seen) == [0, 1, 2, 3, 4]


async def test_a_slow_item_does_not_hold_the_others():
  """The whole reason this shell exists.

  Sequentially, one item that takes its deadline makes every item
  behind it wait that long too — and in a periodic loop the next tick
  waits as well, since the current one has not returned. So one dark
  host could stop a sweep outright."""
  done: list[str] = []

  async def work(item: str) -> None:
    if item == "slow":
      await asyncio.sleep(0.4)
    done.append(item)

  started = time.monotonic()
  await fan_out("t", ["slow", "a", "b", "c"], work)
  elapsed = time.monotonic() - started
  # Sequential would be 0.4s + the rest; concurrent is ~0.4s total, and
  # the fast ones finished long before the slow one.
  assert elapsed < 0.4 * 2
  assert done[-1] == "slow"


async def test_one_failure_does_not_stop_the_siblings():
  done: list[int] = []

  async def work(i: int) -> None:
    if i == 2:
      raise RuntimeError("boom")
    done.append(i)

  await fan_out("t", range(5), work)
  assert sorted(done) == [0, 1, 3, 4]


async def test_fan_out_does_not_retry():
  # A shell that retried would need a backoff and a give-up rule, which
  # is policy. Converging callers re-derive their work next tick.
  calls: list[int] = []

  async def work(i: int) -> None:
    calls.append(i)
    raise RuntimeError("boom")

  await fan_out("t", [1], work)
  assert calls == [1]


async def test_fan_out_bounds_concurrency():
  live = 0
  peak = 0

  async def work(_i: int) -> None:
    nonlocal live, peak
    live += 1
    peak = max(peak, live)
    await asyncio.sleep(0.02)
    live -= 1

  await fan_out("t", range(20), work, max_concurrent=3)
  assert peak <= 3


async def test_fan_out_cancellation_propagates():
  # Shutdown must not be something an item can swallow.
  async def work(_i: int) -> None:
    await asyncio.sleep(10)

  task = asyncio.create_task(fan_out("t", range(3), work))
  await asyncio.sleep(0.05)
  task.cancel()
  with pytest.raises(asyncio.CancelledError):
    await task


async def test_fan_out_on_no_items():
  await fan_out("t", [], lambda _x: asyncio.sleep(0))
