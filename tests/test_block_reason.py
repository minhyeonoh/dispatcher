"""`block_reason` and instance timing.

Every case asserts the reason AND that `dispatch_one` agrees with
it — the point of the feature is that the answer an operator reads
is the decision the scheduler made, so a test that only checked the
string would miss the whole risk.
"""

from __future__ import annotations

from dispatcher.core.models import HostSettings
from tests.test_scheduler import (
  complete_ok,
  die_without_outcome,
  mk_job,
  mk_sched,
)


def test_no_reason_while_dispatchable():
  sched = mk_sched()
  sched.submit(mk_job("A", ["t1", "t2"]))
  assert sched.block_reason("A") is None
  assert sched.dispatch_one() is not None


def test_no_reason_when_nothing_pending():
  sched = mk_sched()
  sched.submit(mk_job("A", ["t1"]))
  action = sched.dispatch_one()
  assert action is not None
  complete_ok(sched, action)
  # Drained is not blocked.
  assert sched.block_reason("A") is None
  assert sched.dispatch_one() is None


def test_paused():
  sched = mk_sched()
  sched.submit(mk_job("A", ["t1"], paused=True))
  assert sched.block_reason("A") == "paused"
  assert sched.dispatch_one() is None


def test_job_cap():
  sched = mk_sched()
  sched.submit(mk_job("A", ["t1", "t2"], max_concurrent=1))
  assert sched.dispatch_one() is not None
  assert sched.block_reason("A") == "job_cap"
  assert sched.dispatch_one() is None


def test_pool_cap():
  sched = mk_sched(pool_caps={"gpu": 1})
  sched.submit(mk_job("A", ["t1", "t2"], pool="gpu"))
  assert sched.dispatch_one() is not None
  assert sched.block_reason("A") == "pool_cap"
  assert sched.dispatch_one() is None


def test_global_cap_outranks_the_job_s_own_reason():
  # dispatch_one reads the global cap BEFORE picking a job, so that
  # is the reason even when the job would also be pool-blocked.
  sched = mk_sched(max_concurrent=1, pool_caps={"gpu": 1})
  sched.submit(mk_job("A", ["t1", "t2"], pool="gpu"))
  assert sched.dispatch_one() is not None
  assert sched.block_reason("A") == "global_cap"
  assert sched.dispatch_one() is None


def test_no_host():
  sched = mk_sched(hosts={"ml10": HostSettings(max_concurrent=1)})
  sched.submit(mk_job("A", ["t1", "t2"]))
  assert sched.dispatch_one() is not None
  assert sched.block_reason("A") == "no_host"
  assert sched.dispatch_one() is None


def test_no_host_when_every_host_is_inactive():
  sched = mk_sched(
    hosts={"ml10": HostSettings(max_concurrent=4, active=False)}
  )
  sched.submit(mk_job("A", ["t1"]))
  assert sched.block_reason("A") == "no_host"
  assert sched.dispatch_one() is None


def test_awaiting_resolution():
  sched = mk_sched()
  sched.submit(mk_job("A", ["t1", "t2"], pause_on_error=True))
  action = sched.dispatch_one()
  assert action is not None
  die_without_outcome(sched, "A", action.task_id)
  # An unknown instance might still turn out to be an error, so the
  # job waits for the resolver rather than racing the pause.
  assert sched.block_reason("A") == "awaiting_resolution"
  assert sched.dispatch_one() is None


def test_reason_clears_when_the_cap_frees():
  sched = mk_sched()
  sched.submit(mk_job("A", ["t1", "t2"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None
  assert sched.block_reason("A") == "job_cap"
  complete_ok(sched, action)
  assert sched.block_reason("A") is None
  assert sched.dispatch_one() is not None


# ── instance timing ──────────────────────────────────────────────


def test_finished_at_stamped_on_leaving_running():
  sched = mk_sched()
  sched.submit(mk_job("A", ["t1"]))
  action = sched.dispatch_one()
  assert action is not None
  assert sched.job_view("A").running["t1"].finished_at is None
  complete_ok(sched, action)
  tv = sched.job_view("A").done_ok["t1"]
  assert tv.finished_at is not None
  assert tv.finished_at >= tv.dispatched_at


def test_finished_at_survives_reclassification_unchanged():
  # The resolver can take minutes to settle an unknown; restamping
  # on that transition would report the lag as runtime.
  sched = mk_sched()
  sched.submit(mk_job("A", ["t1"]))
  action = sched.dispatch_one()
  assert action is not None
  die_without_outcome(sched, "A", "t1")
  at_unknown = sched.job_view("A").unknown["t1"].finished_at
  assert at_unknown is not None
  sched.transition_instance(
    job_id="A",
    task_id="t1",
    from_state="unknown",
    to_state="done_ok",
  )
  assert sched.job_view("A").done_ok["t1"].finished_at == at_unknown


def test_finished_at_cleared_when_unknown_returns_to_running():
  sched = mk_sched()
  sched.submit(mk_job("A", ["t1"]))
  assert sched.dispatch_one() is not None
  die_without_outcome(sched, "A", "t1")
  assert sched.job_view("A").unknown["t1"].finished_at is not None
  # The resolver found the container alive after all.
  sched.transition_instance(
    job_id="A",
    task_id="t1",
    from_state="unknown",
    to_state="running",
  )
  assert sched.job_view("A").running["t1"].finished_at is None


def test_restore_recovers_finished_at_from_the_envelope_mtime(
  tmp_path,
):
  """Durations must survive a restart: the field is in-memory, so
  restore reads the outcome file's mtime — the worker writes that
  file immediately before exiting."""
  import json
  import os

  from dispatcher.core.event_log import scan_outcomes_with_mtime

  home = tmp_path / "home"
  inst = home / "t1__0000001"
  inst.mkdir(parents=True)
  (inst / "outcome.json").write_text(json.dumps({"ok": True}))
  stamp = 1790000000.0
  os.utime(inst / "outcome.json", (stamp, stamp))

  found = scan_outcomes_with_mtime(home)["t1__0000001"]
  assert found.outcome.ok is True
  assert found.finished_at.timestamp() == stamp
  # KST-aware, like every timestamp the dispatcher mints.
  assert found.finished_at.tzinfo is not None


def test_scan_outcomes_still_returns_bare_envelopes(tmp_path):
  import json

  from dispatcher.core.event_log import scan_outcomes

  inst = tmp_path / "t1__0000001"
  inst.mkdir(parents=True)
  (inst / "outcome.json").write_text(json.dumps({"ok": False}))
  assert scan_outcomes(tmp_path)["t1__0000001"].ok is False
