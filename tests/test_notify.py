"""NotifyManager threshold semantics + persistence."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dispatcher.core.event_bus import EventBus
from dispatcher.core.event_log import RUN_LOG_FILENAME, read_events
from dispatcher.core.models import HostSettings, Outcome
from dispatcher.core.scheduler import Scheduler
from dispatcher.services.notify import (
  NotifyManager,
  NotifySettings,
  TelegramSender,
)
from tests.test_runtime import mk_job
from tests.test_scheduler import clock_from, id_gen

if TYPE_CHECKING:
  from pathlib import Path


def run_check(manager: NotifyManager, aid: str) -> None:
  """_fire schedules the send with create_task, so checks need a
  running loop."""
  import asyncio

  async def _c() -> None:
    await manager.check(aid)

  asyncio.run(_c())


class _FakeSender(TelegramSender):
  def __init__(self) -> None:
    super().__init__("token")
    self.sent: list[tuple[str, str]] = []

  async def send(self, chat_id: str, text: str) -> None:
    self.sent.append((chat_id, text))


def _mk(
  tmp_path: Path,
  tasks: list[str],
  thresholds: list[float],
  *,
  enabled: bool = True,
) -> tuple[Scheduler, NotifyManager, _FakeSender, NotifySettings]:
  sched = Scheduler(
    max_concurrent=10,
    hosts={"ml10": HostSettings(max_concurrent=10)},
    clock=clock_from(),
    id_gen=id_gen(),
  )
  sched.submit(mk_job(tmp_path, tasks))
  config = NotifySettings(
    enabled=enabled,
    thresholds=thresholds,
    telegram_chat_id="chat",
  )
  sender = _FakeSender()
  manager = NotifyManager(
    bus=EventBus(),
    scheduler=sched,
    config=config,
    sender=sender,
  )
  return sched, manager, sender, config


def _complete_n(sched: Scheduler, n: int) -> None:
  for _ in range(n):
    action = sched.dispatch_one()
    assert action is not None
    sched.transition_instance(
      job_id=action.job_id,
      task_id=action.task_id,
      from_state="running",
      to_state="done_ok",
      outcome=Outcome(ok=True),
    )


def test_check_fires_each_threshold_exactly_once(tmp_path: Path):
  sched, manager, sender, _ = _mk(
    tmp_path, ["t1", "t2", "t3", "t4"], [0.5, 1.0]
  )
  _complete_n(sched, 2)  # 50%
  run_check(manager, "job-001")
  assert len(sender.sent) == 1
  run_check(manager, "job-001")  # idempotent
  assert len(sender.sent) == 1
  _complete_n(sched, 2)  # 100%
  run_check(manager, "job-001")
  assert len(sender.sent) == 2


def test_unknown_does_not_count_toward_ratio(tmp_path: Path):
  # unknown/ghosted have no evidence — announcing on them would
  # claim progress that a resolver may retract.
  sched, manager, sender, _ = _mk(tmp_path, ["t1", "t2"], [1.0])
  _complete_n(sched, 1)
  action = sched.dispatch_one()
  assert action is not None
  sched.transition_instance(
    job_id="job-001",
    task_id=action.task_id,
    from_state="running",
    to_state="unknown",
  )
  run_check(manager, "job-001")
  assert sender.sent == []


def test_100_percent_fires_only_when_done(tmp_path: Path):
  sched, manager, sender, _ = _mk(tmp_path, ["t1", "t2"], [1.0])
  _complete_n(sched, 1)
  run_check(manager, "job-001")
  assert sender.sent == []
  _complete_n(sched, 1)
  run_check(manager, "job-001")
  assert len(sender.sent) == 1


def test_fired_thresholds_persist_to_event_log(tmp_path: Path):
  sched, manager, _, _ = _mk(tmp_path, ["t1"], [1.0])
  _complete_n(sched, 1)
  run_check(manager, "job-001")
  events = read_events(tmp_path / RUN_LOG_FILENAME)
  fired = [e for e in events if e["type"] == "notify_fired"]
  assert len(fired) == 1
  assert fired[0]["threshold"] == 1.0
  # In-memory bookkeeping mirrors the log.
  assert sched.job_state("job-001").notified_thresholds == [1.0]


def test_disabled_still_bookkeeps_but_does_not_send(
  tmp_path: Path,
):
  sched, manager, sender, _ = _mk(tmp_path, ["t1"], [1.0], enabled=False)
  _complete_n(sched, 1)
  run_check(manager, "job-001")
  assert sender.sent == []
  # Bookkeeping ran: re-enabling later won't backfire this one.
  assert sched.job_state("job-001").notified_thresholds == [1.0]


def test_out_of_range_thresholds_ignored(tmp_path: Path):
  sched, manager, sender, _ = _mk(tmp_path, ["t1"], [0.0, -1.0, 1.5])
  _complete_n(sched, 1)
  run_check(manager, "job-001")
  assert sender.sent == []


def test_cancelled_job_check_is_noop(tmp_path: Path):
  sched, manager, sender, _ = _mk(tmp_path, ["t1"], [1.0])
  sched.cancel("job-001")
  run_check(manager, "job-001")
  assert sender.sent == []
