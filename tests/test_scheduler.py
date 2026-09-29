"""Scheduler contract: WRR cursor, caps, buckets, slot accounting,
pause-on-error, reclaim, alias.

Live classification (outcome file → bucket) lives in the runtime;
these tests drive `transition_trial` directly with the state the
classifier would have produced.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from dispatcher.core.event_log import replay_events
from dispatcher.core.models import HostSettings
from dispatcher.core.scheduler import (
  AliasCollisionError,
  AliasFormatError,
  Scheduler,
)

if TYPE_CHECKING:
  from collections.abc import Callable, Iterable

  from dispatcher.core.models import AttemptState, DispatchEntry

# ── helpers ──────────────────────────────────────────────────────


def mk_attempt(
  attempt_id: str,
  task_list: Iterable[str],
  *,
  weight: int = 1,
  max_concurrent: int | None = None,
  pause_on_error: bool | None = None,
  paused: bool = False,
  pool: str | None = None,
) -> AttemptState:
  """Build an AttemptState through replay — the same path restore
  uses — so submit-event shape drift breaks tests, not prod."""
  submit = {
    "type": "submit",
    "attempt_id": attempt_id,
    "label": attempt_id,
    "task_list": list(task_list),
    "home_root": f"/data/{attempt_id}",
    "container": {"image": "img"},
    "submitted_at": "2026-09-28T10:00:00+00:00",
    "alias": f"alias-{attempt_id}",
  }
  if pool is not None:
    submit["pool"] = pool
  events: list[dict] = [submit]
  patch: dict = {}
  if weight != 1:
    patch["weight"] = weight
  if max_concurrent is not None:
    patch["max_concurrent"] = max_concurrent
  if pause_on_error is not None:
    patch["pause_on_error"] = pause_on_error
  if paused:
    patch["paused"] = True
  if patch:
    events.append(
      {"type": "patch", "attempt_id": attempt_id, "at": "…", **patch}
    )
  out = replay_events(events)
  assert out is not None
  return out[0]


def clock_from(
  start: datetime = datetime(2026, 9, 28, 10, 0),
) -> Callable[[], datetime]:
  t = [start]

  def _now() -> datetime:
    v = t[0]
    t[0] = v + timedelta(seconds=1)
    return v

  return _now


def name_gen() -> Callable[[str], str]:
  counter = [0]

  def _gen(task_name: str) -> str:
    counter[0] += 1
    return f"{task_name}__t{counter[0]}"

  return _gen


def drain(sched: Scheduler, limit: int = 100) -> list[DispatchEntry]:
  out: list[DispatchEntry] = []
  for _ in range(limit):
    action = sched.dispatch_one()
    if action is None:
      return out
    out.append(action)
  raise AssertionError(f"more than {limit} dispatches; loop?")


def complete_ok(sched: Scheduler, action: DispatchEntry) -> None:
  sched.transition_trial(
    attempt_id=action.attempt_id,
    task_name=action.task_name,
    from_state="running",
    to_state="done_ok",
  )


def complete_err(sched: Scheduler, action: DispatchEntry) -> None:
  sched.transition_trial(
    attempt_id=action.attempt_id,
    task_name=action.task_name,
    from_state="running",
    to_state="done_err",
  )


def die_without_outcome(
  sched: Scheduler, attempt_id: str, task_name: str
) -> None:
  sched.transition_trial(
    attempt_id=attempt_id,
    task_name=task_name,
    from_state="running",
    to_state="unknown",
  )


def mk_sched(
  *,
  max_concurrent: int = 100,
  hosts: dict[str, HostSettings] | None = None,
  pool_caps: dict[str, int] | None = None,
) -> Scheduler:
  return Scheduler(
    max_concurrent=max_concurrent,
    hosts=hosts or {"ml10": HostSettings(max_concurrent=100)},
    clock=clock_from(),
    name_gen=name_gen(),
    pool_caps=pool_caps,
  )


# ── basic dispatch + FIFO ────────────────────────────────────────


def test_single_attempt_dispatches_all_tasks_in_order():
  sched = mk_sched()
  sched.submit(mk_attempt("A", ["t1", "t2", "t3"]))
  actions = drain(sched)
  assert [a.task_name for a in actions] == ["t1", "t2", "t3"]
  assert all(a.attempt_id == "A" for a in actions)


def test_global_cap_zero_dispatches_nothing():
  sched = mk_sched(max_concurrent=0)
  sched.submit(mk_attempt("A", ["t1", "t2"]))
  assert sched.dispatch_one() is None


def test_global_cap_limits_dispatch_burst():
  sched = mk_sched(max_concurrent=2)
  sched.submit(mk_attempt("A", ["t1", "t2", "t3", "t4"]))
  assert len(drain(sched)) == 2
  assert sched.running_total == 2


def test_dispatched_task_leaves_pending():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t1", "t2"]))
  sched.dispatch_one()
  view = sched.attempt_view("A")
  assert view.pending == ["t2"]
  assert list(view.running) == ["t1"]


# ── round-robin ──────────────────────────────────────────────────


def test_two_attempts_equal_weight_interleave_one_one():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["a1", "a2", "a3"]))
  sched.submit(mk_attempt("B", ["b1", "b2", "b3"]))
  seq: list[str] = []
  for _ in range(6):
    action = sched.dispatch_one()
    assert action is not None
    seq.append(action.task_name)
    complete_ok(sched, action)
  assert seq == ["a1", "b1", "a2", "b2", "a3", "b3"]


def test_two_attempts_weight_3_to_1():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["a1", "a2", "a3", "a4"], weight=3))
  sched.submit(mk_attempt("B", ["b1", "b2"]))
  seq: list[str] = []
  for _ in range(6):
    action = sched.dispatch_one()
    assert action is not None
    seq.append(action.task_name)
    complete_ok(sched, action)
  assert seq[:4] == ["a1", "a2", "a3", "b1"]
  assert seq[4:6] == ["a4", "b2"]


def test_smaller_attempt_drains_then_larger_alone():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["a1", "a2", "a3", "a4"]))
  sched.submit(mk_attempt("B", ["b1"]))
  seq: list[str] = []
  for _ in range(5):
    action = sched.dispatch_one()
    assert action is not None
    seq.append(action.task_name)
    complete_ok(sched, action)
  assert seq == ["a1", "b1", "a2", "a3", "a4"]


# ── pause ────────────────────────────────────────────────────────


def test_paused_attempt_skipped_by_cursor():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["a1", "a2"], paused=True))
  sched.submit(mk_attempt("B", ["b1"]))
  action = sched.dispatch_one()
  assert action is not None and action.task_name == "b1"
  assert sched.attempt_view("A").pending == ["a1", "a2"]


def test_pause_mid_run_stops_further_dispatch_for_that_attempt():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["a1", "a2"]))
  sched.submit(mk_attempt("B", ["b1", "b2"]))
  a1 = sched.dispatch_one()
  assert a1 is not None
  complete_ok(sched, a1)
  b1 = sched.dispatch_one()
  assert b1 is not None
  complete_ok(sched, b1)
  sched.patch("A", paused=True)
  action = sched.dispatch_one()
  assert action is not None and action.task_name == "b2"


def test_unpause_rejoins_rotation_no_owed_turns():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["a1", "a2", "a3"], paused=True))
  sched.submit(mk_attempt("B", ["b1", "b2", "b3"]))
  for _ in range(3):
    action = sched.dispatch_one()
    assert action is not None and action.attempt_id == "B"
    complete_ok(sched, action)
  sched.patch("A", paused=False)
  seq: list[str] = []
  for _ in range(3):
    action = sched.dispatch_one()
    assert action is not None
    seq.append(action.task_name)
    complete_ok(sched, action)
  assert seq == ["a1", "a2", "a3"]


# ── per-attempt max_concurrent ───────────────────────────────────


def test_max_concurrent_1_fences_second_dispatch():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["a1", "a2", "a3"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None and action.task_name == "a1"
  assert sched.dispatch_one() is None


def test_max_concurrent_1_next_dispatch_after_completion():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["a1", "a2"], max_concurrent=1))
  first = sched.dispatch_one()
  assert first is not None and first.task_name == "a1"
  complete_ok(sched, first)
  second = sched.dispatch_one()
  assert second is not None and second.task_name == "a2"


def test_max_concurrent_k_allows_k_parallel():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["a1", "a2", "a3", "a4"], max_concurrent=2))
  a = sched.dispatch_one()
  b = sched.dispatch_one()
  assert a is not None and b is not None
  assert a.task_name == "a1" and b.task_name == "a2"
  assert sched.dispatch_one() is None
  complete_ok(sched, a)
  c = sched.dispatch_one()
  assert c is not None and c.task_name == "a3"


# ── host caps ────────────────────────────────────────────────────


def test_no_free_host_returns_none_even_if_global_has_room():
  sched = mk_sched(
    max_concurrent=10,
    hosts={
      "ml10": HostSettings(max_concurrent=1),
      "ml9": HostSettings(max_concurrent=1),
    },
  )
  sched.submit(mk_attempt("A", ["a1", "a2", "a3", "a4"]))
  first = sched.dispatch_one()
  second = sched.dispatch_one()
  assert first is not None and second is not None
  assert {first.host, second.host} == {"ml10", "ml9"}
  assert sched.dispatch_one() is None


def test_host_freed_reopens_dispatch():
  sched = mk_sched(
    max_concurrent=10,
    hosts={
      "ml10": HostSettings(max_concurrent=1),
      "ml9": HostSettings(max_concurrent=1),
    },
  )
  sched.submit(mk_attempt("A", ["a1", "a2", "a3"]))
  first = sched.dispatch_one()
  assert first is not None
  sched.dispatch_one()
  assert sched.dispatch_one() is None
  complete_ok(sched, first)
  third = sched.dispatch_one()
  assert third is not None
  assert third.host == first.host


def test_pick_host_least_utilization():
  sched = mk_sched(
    max_concurrent=100,
    hosts={
      "ml10": HostSettings(max_concurrent=25),
      "ml9": HostSettings(max_concurrent=5),
    },
  )
  sched.submit(mk_attempt("A", [f"t{i}" for i in range(30)]))
  actions = [sched.dispatch_one() for _ in range(6)]
  for i in range(6):
    prefix = actions[: i + 1]
    delta = (
      sum(1 for a in prefix if a is not None and a.host == "ml10") / 25
      - sum(1 for a in prefix if a is not None and a.host == "ml9") / 5
    )
    assert abs(delta) <= 0.21


# ── pause-on-error ───────────────────────────────────────────────


def test_pause_on_error_default_derives_from_max_concurrent_1():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["a1", "a2"], max_concurrent=1))
  a1 = sched.dispatch_one()
  assert a1 is not None
  complete_err(sched, a1)
  assert sched.attempt_paused("A") is True
  assert sched.dispatch_one() is None


def test_pause_on_error_default_off_for_parallel_attempts():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["a1", "a2"]))
  a1 = sched.dispatch_one()
  assert a1 is not None
  complete_err(sched, a1)
  assert sched.attempt_paused("A") is False
  nxt = sched.dispatch_one()
  assert nxt is not None and nxt.task_name == "a2"


def test_pause_on_error_true_pauses_even_for_parallel():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["a1", "a2"], pause_on_error=True))
  a1 = sched.dispatch_one()
  assert a1 is not None
  complete_err(sched, a1)
  assert sched.attempt_paused("A") is True


def test_pause_on_error_false_never_pauses_sequential():
  sched = mk_sched(max_concurrent=10)
  sched.submit(
    mk_attempt("A", ["a1", "a2"], max_concurrent=1, pause_on_error=False)
  )
  a1 = sched.dispatch_one()
  assert a1 is not None
  complete_err(sched, a1)
  assert sched.attempt_paused("A") is False
  nxt = sched.dispatch_one()
  assert nxt is not None and nxt.task_name == "a2"


def test_pause_on_error_skips_when_no_pending_left():
  # Pausing after the LAST trial's error would trap the attempt in
  # paused=True with nothing to block, stalling the drain hook.
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["a1"], max_concurrent=1))
  a1 = sched.dispatch_one()
  assert a1 is not None
  complete_err(sched, a1)
  assert sched.attempt_paused("A") is False


def test_clean_completion_is_not_a_pause_trigger():
  # A clean outcome whose *content* says the answer was wrong is a
  # normal completion — the dispatcher never scores `data`.
  sched = mk_sched(max_concurrent=10)
  sched.submit(
    mk_attempt("A", ["a1", "a2"], max_concurrent=1, pause_on_error=True)
  )
  a1 = sched.dispatch_one()
  assert a1 is not None
  complete_ok(sched, a1)
  assert sched.attempt_paused("A") is False
  nxt = sched.dispatch_one()
  assert nxt is not None and nxt.task_name == "a2"


def test_pause_on_error_blocks_dispatch_while_unknown_present():
  # An unresolved trial may still come back done_err; dispatching
  # ahead of the resolver would race the pause the operator wants.
  sched = mk_sched(max_concurrent=10)
  sched.submit(
    mk_attempt("A", ["a1", "a2"], max_concurrent=1, pause_on_error=True)
  )
  a1 = sched.dispatch_one()
  assert a1 is not None
  die_without_outcome(sched, "A", "a1")
  assert sched.attempt_paused("A") is False
  assert sched.dispatch_one() is None
  sched.transition_trial(
    attempt_id="A",
    task_name="a1",
    from_state="unknown",
    to_state="done_ok",
  )
  nxt = sched.dispatch_one()
  assert nxt is not None and nxt.task_name == "a2"


def test_pause_on_error_off_dispatches_even_with_unknown():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["a1", "a2"]))
  sched.dispatch_one()
  die_without_outcome(sched, "A", "a1")
  nxt = sched.dispatch_one()
  assert nxt is not None and nxt.task_name == "a2"


def test_death_before_outcome_parks_in_unknown_without_pausing():
  # No outcome is NOT evidence of failure (NFS lag looks the same);
  # pausing here would pause sequential attempts on every flake.
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["a1", "a2"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None
  die_without_outcome(sched, "A", "a1")
  view = sched.attempt_view("A")
  assert set(view.unknown) == {"a1"}
  assert not view.done_ok and not view.done_err and not view.ghosted
  assert sched.attempt_paused("A") is False


# ── rotation membership ──────────────────────────────────────────


def test_new_attempt_appends_to_rotation_tail():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["a1", "a2", "a3"]))
  sched.submit(mk_attempt("B", ["b1", "b2", "b3"]))
  a1 = sched.dispatch_one()
  assert a1 is not None
  complete_ok(sched, a1)
  b1 = sched.dispatch_one()
  assert b1 is not None
  complete_ok(sched, b1)
  sched.submit(mk_attempt("C", ["c1", "c2"]))
  seq: list[str] = []
  for _ in range(3):
    action = sched.dispatch_one()
    assert action is not None
    seq.append(action.task_name)
    complete_ok(sched, action)
  assert seq == ["a2", "b2", "c1"]


def test_drained_attempt_skipped_by_rotation():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["a1"]))
  sched.submit(mk_attempt("B", ["b1", "b2", "b3"]))
  a1 = sched.dispatch_one()
  assert a1 is not None
  complete_ok(sched, a1)
  b1 = sched.dispatch_one()
  assert b1 is not None
  complete_ok(sched, b1)
  b2 = sched.dispatch_one()
  assert b2 is not None and b2.task_name == "b2"


# ── FIFO order ───────────────────────────────────────────────────


def test_task_dispatch_order_matches_task_list():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t3", "t1", "t2"]))
  seq: list[str] = []
  for _ in range(3):
    action = sched.dispatch_one()
    assert action is not None
    seq.append(action.task_name)
    complete_ok(sched, action)
  assert seq == ["t3", "t1", "t2"]


def test_task_list_order_preserved_after_pause_unpause():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t1", "t2", "t3"], max_concurrent=1))
  t1 = sched.dispatch_one()
  assert t1 is not None
  complete_ok(sched, t1)
  sched.patch("A", paused=True)
  assert sched.dispatch_one() is None
  sched.patch("A", paused=False)
  t2 = sched.dispatch_one()
  assert t2 is not None and t2.task_name == "t2"


# ── transition graph + slot accounting ───────────────────────────


def test_transition_trial_rejects_invalid_source_bucket():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t1"]))
  with pytest.raises(ValueError, match="unsupported trial source"):
    sched.transition_trial(
      attempt_id="A",
      task_name="t1",
      from_state="done_ok",
      to_state="done_err",
    )


def test_transition_trial_rejects_illegal_edge():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t1"]))
  sched.dispatch_one()
  with pytest.raises(ValueError, match="unsupported trial transition"):
    sched.transition_trial(
      attempt_id="A",
      task_name="t1",
      from_state="running",
      to_state="running",
    )


def test_running_to_unknown_releases_host_slot():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t1"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None
  assert sched.running_per_host()[action.host] == 1
  die_without_outcome(sched, "A", "t1")
  assert sched.running_per_host()[action.host] == 0


def test_unknown_to_ghosted_does_not_change_host_slot():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t1"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None
  die_without_outcome(sched, "A", "t1")
  before = sched.running_per_host()[action.host]
  sched.transition_trial(
    attempt_id="A",
    task_name="t1",
    from_state="unknown",
    to_state="ghosted",
  )
  assert sched.running_per_host()[action.host] == before


def test_unknown_to_running_reacquires_host_slot():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t1"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None
  die_without_outcome(sched, "A", "t1")
  assert sched.running_per_host()[action.host] == 0
  sched.transition_trial(
    attempt_id="A",
    task_name="t1",
    from_state="unknown",
    to_state="running",
  )
  assert sched.running_per_host()[action.host] == 1


# ── has_work (drain contract) ────────────────────────────────────


def test_has_work_true_when_unknown_present():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t1"], max_concurrent=1))
  sched.dispatch_one()
  die_without_outcome(sched, "A", "t1")
  assert sched.has_work() is True


def test_has_work_true_when_ghosted_present():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t1"], max_concurrent=1))
  sched.dispatch_one()
  die_without_outcome(sched, "A", "t1")
  sched.transition_trial(
    attempt_id="A",
    task_name="t1",
    from_state="unknown",
    to_state="ghosted",
  )
  assert sched.has_work() is True


def test_has_work_false_when_only_terminal_present():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["t1"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None
  complete_ok(sched, action)
  assert sched.has_work() is False


# ── reclaim ──────────────────────────────────────────────────────


def test_reclaim_from_running_moves_task_back_to_pending():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["t1", "t2", "t3"]))
  sched.dispatch_one()
  sched.dispatch_one()
  view = sched.attempt_view("A")
  assert set(view.running) == {"t1", "t2"}
  assert view.pending == ["t3"]
  assert sched.reclaim_from_running("A", "t1") is True
  view = sched.attempt_view("A")
  # t1 returns at its task_list position, before t3.
  assert view.pending == ["t1", "t3"]
  assert set(view.running) == {"t2"}


def test_reclaim_decrements_host_running():
  sched = mk_sched(
    max_concurrent=10,
    hosts={"ml10": HostSettings(max_concurrent=2)},
  )
  sched.submit(mk_attempt("A", ["t1", "t2"]))
  sched.dispatch_one()
  assert sched.running_per_host()["ml10"] == 1
  sched.reclaim_from_running("A", "t1")
  assert sched.running_per_host()["ml10"] == 0


def test_reclaim_returns_false_when_task_not_running():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["t1"]))
  a = sched.dispatch_one()
  assert a is not None
  complete_ok(sched, a)
  assert sched.reclaim_from_running("A", "t1") is False
  view = sched.attempt_view("A")
  assert set(view.done_ok) == {"t1"}
  assert view.pending == []


def test_reclaimed_task_can_be_redispatched():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["t1"]))
  first = sched.dispatch_one()
  assert first is not None and first.task_name == "t1"
  sched.reclaim_from_running("A", "t1")
  second = sched.dispatch_one()
  assert second is not None and second.task_name == "t1"
  assert second.trial_name != first.trial_name


# ── infra requeue ────────────────────────────────────────────────


def test_infra_requeue_bounded_by_budget():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["t1"]))
  for _ in range(Scheduler.MAX_INFRA_RETRIES):
    action = sched.dispatch_one()
    assert action is not None
    assert sched.requeue_after_infra_failure("A", "t1", "running") is True
  action = sched.dispatch_one()
  assert action is not None
  assert sched.requeue_after_infra_failure("A", "t1", "running") is False


def test_infra_requeue_from_parked_shares_the_budget():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_attempt("A", ["t1"]))
  sched.dispatch_one()
  die_without_outcome(sched, "A", "t1")
  assert sched.requeue_after_infra_failure("A", "t1", "unknown") is True
  # The task is pending again; budget counted 1.
  assert sched.attempt_view("A").pending == ["t1"]
  for _ in range(Scheduler.MAX_INFRA_RETRIES - 1):
    sched.dispatch_one()
    assert sched.requeue_after_infra_failure("A", "t1", "running") is True
  sched.dispatch_one()
  assert sched.requeue_after_infra_failure("A", "t1", "running") is False


def test_infra_requeue_from_parked_does_not_release_slot_twice():
  sched = mk_sched(
    max_concurrent=10,
    hosts={"ml10": HostSettings(max_concurrent=2)},
  )
  sched.submit(mk_attempt("A", ["t1", "t2"]))
  sched.dispatch_one()
  sched.dispatch_one()
  die_without_outcome(sched, "A", "t1")  # slot released here
  assert sched.running_per_host()["ml10"] == 1
  sched.requeue_after_infra_failure("A", "t1", "unknown")
  assert sched.running_per_host()["ml10"] == 1


# ── alias ────────────────────────────────────────────────────────


def test_mint_alias_returns_fresh_hyphenated_slug():
  sched = mk_sched()
  alias = sched.mint_alias("A")
  assert alias
  assert "-" in alias


def test_submit_rejects_empty_alias():
  sched = mk_sched()
  a = mk_attempt("A", ["t1"])
  a.alias = ""
  with pytest.raises(ValueError, match="empty alias"):
    sched.submit(a)


def test_submit_accepts_operator_supplied_alias_verbatim():
  sched = mk_sched()
  a = mk_attempt("A", ["t1"])
  a.alias = "my-run"
  sched.submit(a)
  assert sched.attempt_id_of_alias("my-run") == "A"


def test_submit_rejects_duplicate_alias():
  sched = mk_sched()
  first = mk_attempt("A", ["t1"])
  first.alias = "shared-name"
  sched.submit(first)
  second = mk_attempt("B", ["t1"])
  second.alias = "shared-name"
  with pytest.raises(AliasCollisionError):
    sched.submit(second)


def test_cancel_releases_alias_for_reuse():
  sched = mk_sched()
  first = mk_attempt("A", ["t1"])
  first.alias = "reusable"
  sched.submit(first)
  sched.cancel("A")
  assert sched.attempt_id_of_alias("reusable") is None
  second = mk_attempt("B", ["t1"])
  second.alias = "reusable"
  sched.submit(second)
  assert sched.attempt_id_of_alias("reusable") == "B"


def test_set_alias_renames_and_frees_the_old_handle():
  sched = mk_sched()
  a = mk_attempt("A", ["t1"])
  sched.submit(a)
  original = a.alias
  prev = sched.set_alias("A", "new-name")
  assert prev == original
  assert a.alias == "new-name"
  assert sched.attempt_id_of_alias("new-name") == "A"
  assert sched.attempt_id_of_alias(original) is None


def test_set_alias_no_op_when_unchanged():
  sched = mk_sched()
  a = mk_attempt("A", ["t1"])
  a.alias = "same"
  sched.submit(a)
  assert sched.set_alias("A", "same") == "same"
  assert a.alias == "same"


def test_set_alias_rejects_collision():
  sched = mk_sched()
  a1 = mk_attempt("A", ["t1"])
  a1.alias = "taken"
  sched.submit(a1)
  a2 = mk_attempt("B", ["t1"])
  sched.submit(a2)
  with pytest.raises(AliasCollisionError):
    sched.set_alias("B", "taken")


def test_set_alias_rejects_empty_and_over_length():
  sched = mk_sched()
  a = mk_attempt("A", ["t1"])
  sched.submit(a)
  with pytest.raises(AliasFormatError):
    sched.set_alias("A", "")
  with pytest.raises(AliasFormatError):
    sched.set_alias("A", "x" * 121)


def test_set_alias_accepts_arbitrary_charset():
  sched = mk_sched()
  a = mk_attempt("A", ["t1"])
  sched.submit(a)
  for candidate in ("Foo Bar", "α-β", "run_1/branch2", "☃"):
    sched.set_alias("A", candidate)
    assert a.alias == candidate


# ── cancel slot release ──────────────────────────────────────────


def test_cancel_releases_host_slots_of_running_trials():
  sched = mk_sched(
    max_concurrent=10,
    hosts={"ml10": HostSettings(max_concurrent=2)},
  )
  sched.submit(mk_attempt("A", ["t1", "t2"]))
  sched.dispatch_one()
  sched.dispatch_one()
  assert sched.running_per_host()["ml10"] == 2
  runtime = sched.cancel("A")
  assert set(runtime.running) == {"t1", "t2"}
  assert sched.running_per_host()["ml10"] == 0


def test_cancel_preserves_cursor_rotation():
  sched = mk_sched(max_concurrent=1)
  sched.submit(mk_attempt("A", ["a1", "a2"]))
  sched.submit(mk_attempt("B", ["b1", "b2"]))
  sched.submit(mk_attempt("C", ["c1", "c2"]))
  a1 = sched.dispatch_one()
  assert a1 is not None and a1.task_name == "a1"
  complete_ok(sched, a1)
  # Cursor now at B. Cancelling A (before the cursor) must not
  # skip B's turn.
  sched.cancel("A")
  nxt = sched.dispatch_one()
  assert nxt is not None and nxt.task_name == "b1"
