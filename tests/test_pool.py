"""Named concurrency pools: per-pool running ledger + caps."""

from __future__ import annotations

import pytest

from dispatcher.core.event_log import replay_events
from dispatcher.core.models import HostSettings
from tests.test_scheduler import (
  complete_ok,
  die_without_outcome,
  mk_job,
  mk_sched,
)


def test_pool_defaults_to_default_on_legacy_submit():
  a = mk_job("A", ["t1"])
  assert a.pool == "default"


def test_pool_replays_from_submit_and_patch():
  out = replay_events(
    [
      {
        "type": "submit",
        "job_id": "A",
        "label": "A",
        "task_ids": ["t1"],
        "home_root": "/data/A",
        "container": {"image": "img"},
        "submitted_at": "2026-09-28T10:00:00+00:00",
        "alias": "alias-A",
        "pool": "gpu",
      },
      {"type": "patch", "job_id": "A", "pool": "cpu"},
    ]
  )
  assert out is not None
  assert out[0].pool == "cpu"


def test_pool_cap_blocks_only_its_own_pool():
  sched = mk_sched(max_concurrent=10, pool_caps={"gpu": 1})
  sched.submit(mk_job("A", ["a1", "a2"], pool="gpu"))
  sched.submit(mk_job("B", ["b1", "b2"], pool="cpu"))
  first = sched.dispatch_one()
  assert first is not None and first.job_id == "A"
  # gpu at cap → next dispatches all land on B.
  second = sched.dispatch_one()
  third = sched.dispatch_one()
  assert second is not None and second.job_id == "B"
  assert third is not None and third.job_id == "B"
  assert sched.dispatch_one() is None


def test_pool_completion_frees_slot_for_next_dispatch():
  sched = mk_sched(max_concurrent=10, pool_caps={"gpu": 1})
  sched.submit(mk_job("A", ["a1", "a2"], pool="gpu"))
  first = sched.dispatch_one()
  assert first is not None
  assert sched.dispatch_one() is None
  complete_ok(sched, first)
  nxt = sched.dispatch_one()
  assert nxt is not None and nxt.task_id == "a2"


def test_unlisted_pool_is_unbounded():
  sched = mk_sched(max_concurrent=10, pool_caps={"gpu": 1})
  sched.submit(mk_job("A", ["a1", "a2", "a3"], pool="other"))
  assert len([sched.dispatch_one() for _ in range(3)]) == 3
  assert sched.pool_running_snapshot() == {"other": 3}


def test_pool_cap_zero_freezes_the_pool():
  sched = mk_sched(max_concurrent=10, pool_caps={"gpu": 0})
  sched.submit(mk_job("A", ["a1"], pool="gpu"))
  assert sched.dispatch_one() is None


def test_patch_pool_moves_running_counts():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_job("A", ["a1", "a2"], pool="gpu"))
  sched.dispatch_one()
  sched.dispatch_one()
  assert sched.pool_running_snapshot() == {"gpu": 2}
  sched.patch("A", pool="cpu")
  assert sched.pool_running_snapshot() == {"cpu": 2}
  # Completion decrements the NEW pool, no drift.
  sched.transition_instance(
    job_id="A",
    task_id="a1",
    from_state="running",
    to_state="done_ok",
  )
  assert sched.pool_running_snapshot() == {"cpu": 1}


def test_set_pool_caps_takes_effect_next_dispatch():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_job("A", ["a1", "a2"], pool="gpu"))
  first = sched.dispatch_one()
  assert first is not None
  old = sched.set_pool_caps({"gpu": 1})
  assert old == {}
  assert sched.dispatch_one() is None


def test_cancel_releases_pool_counts():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_job("A", ["a1", "a2"], pool="gpu"))
  sched.dispatch_one()
  sched.dispatch_one()
  sched.cancel("A")
  assert sched.pool_running_snapshot() == {}


def test_wrr_still_interleaves_within_pool_cap():
  sched = mk_sched(max_concurrent=1, pool_caps={"p": 5})
  sched.submit(mk_job("A", ["a1", "a2"], pool="p"))
  sched.submit(mk_job("B", ["b1", "b2"], pool="p"))
  seq = []
  for _ in range(4):
    action = sched.dispatch_one()
    assert action is not None
    seq.append(action.task_id)
    complete_ok(sched, action)
  assert seq == ["a1", "b1", "a2", "b2"]


def test_wrr_cursor_skips_pool_blocked_job():
  sched = mk_sched(max_concurrent=10, pool_caps={"gpu": 1})
  sched.submit(mk_job("A", ["a1", "a2"], pool="gpu"))
  sched.submit(mk_job("B", ["b1"], pool="cpu"))
  first = sched.dispatch_one()
  assert first is not None and first.job_id == "A"
  # A's pool is at cap — the SAME tick can still dispatch B.
  second = sched.dispatch_one()
  assert second is not None and second.job_id == "B"


def test_pool_caps_snapshot_is_readonly():
  sched = mk_sched(pool_caps={"gpu": 2})
  snap = sched.pool_caps_snapshot()
  snap["gpu"] = 99
  assert sched.pool_caps_snapshot() == {"gpu": 2}


def test_pool_caps_negative_rejected():
  sched = mk_sched()
  with pytest.raises(ValueError, match=">= 0"):
    sched.set_pool_caps({"gpu": -1})


def test_reclaim_from_running_releases_pool_count():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_job("A", ["t1"], pool="gpu"))
  sched.dispatch_one()
  assert sched.pool_running_snapshot() == {"gpu": 1}
  sched.reclaim_from_running("A", "t1")
  assert sched.pool_running_snapshot() == {}


def test_infra_requeue_releases_pool_count():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_job("A", ["t1"], pool="gpu"))
  sched.dispatch_one()
  assert sched.requeue_after_infra_failure("A", "t1", "running") is True
  assert sched.pool_running_snapshot() == {}


def test_terminal_transition_decrements_pool():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_job("A", ["t1", "t2"], pool="gpu"))
  sched.dispatch_one()
  sched.dispatch_one()
  die_without_outcome(sched, "A", "t1")
  assert sched.pool_running_snapshot() == {"gpu": 1}
  sched.transition_instance(
    job_id="A",
    task_id="t2",
    from_state="running",
    to_state="done_ok",
  )
  assert sched.pool_running_snapshot() == {}


def test_retry_from_done_err_does_not_touch_pool():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_job("A", ["t1"], pool="gpu"))
  sched.dispatch_one()
  sched.transition_instance(
    job_id="A",
    task_id="t1",
    from_state="running",
    to_state="done_err",
  )
  assert sched.pool_running_snapshot() == {}
  sched.retry_from_done_err("A", "t1")
  assert sched.pool_running_snapshot() == {}


def test_restore_bumps_pool_for_preexisting_running():
  from datetime import UTC, datetime

  from dispatcher.core.models import InstanceView

  sched = mk_sched(
    max_concurrent=10,
    hosts={"ml10": HostSettings(max_concurrent=5)},
  )
  a = mk_job("A", ["t1", "t2"], pool="gpu")
  tv = InstanceView(
    task_id="t1",
    state="running",
    instance_id="t1__0000001",
    host="ml10",
    dispatched_at=datetime.now(UTC),
  )
  sched.restore(a, running={"t1": tv}, done_ok={}, done_err={})
  assert sched.pool_running_snapshot() == {"gpu": 1}
  assert sched.running_per_host()["ml10"] == 1


def test_unknown_to_running_rebumps_pool():
  sched = mk_sched(max_concurrent=10)
  sched.submit(mk_job("A", ["t1"], pool="gpu"))
  sched.dispatch_one()
  die_without_outcome(sched, "A", "t1")
  assert sched.pool_running_snapshot() == {}
  sched.transition_instance(
    job_id="A",
    task_id="t1",
    from_state="unknown",
    to_state="running",
  )
  assert sched.pool_running_snapshot() == {"gpu": 1}
