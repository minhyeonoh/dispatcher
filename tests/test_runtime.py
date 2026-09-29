"""DispatcherRuntime: dispatch flow, die handling, resolver,
infra requeue, GC — with fake dispatch/poll/docker."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest

from dispatcher import labels
from dispatcher.dispatch import DispatchError
from dispatcher.metrics import MetricsCache
from dispatcher.models import HostSettings
from dispatcher.outcome import CompletionSnapshot
from dispatcher.runtime import DispatcherRuntime
from dispatcher.scheduler import Scheduler
from tests.test_scheduler import clock_from, name_gen

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.models import AttemptState, DispatchEntry


def mk_attempt(
  home_root: Path,
  task_list: list[str],
  *,
  attempt_id: str = "att-001",
  max_concurrent: int | None = None,
  payloads: dict | None = None,
) -> AttemptState:
  from dispatcher.event_log import replay_events

  events: list[dict] = [
    {
      "type": "submit",
      "attempt_id": attempt_id,
      "label": "demo",
      "task_list": task_list,
      "home_root": str(home_root),
      "container": {"image": "img"},
      "submitted_at": "2026-09-28T10:00:00+00:00",
      "alias": f"{attempt_id}-alias",
      "payloads": payloads or {},
    }
  ]
  if max_concurrent is not None:
    events.append(
      {
        "type": "patch",
        "attempt_id": attempt_id,
        "max_concurrent": max_concurrent,
        "at": "…",
      }
    )
  out = replay_events(events)
  assert out is not None
  return out[0]


def mk_sched(caps: int = 2) -> Scheduler:
  return Scheduler(
    max_concurrent=caps,
    hosts={"ml10": HostSettings(max_concurrent=caps)},
    clock=clock_from(),
    name_gen=name_gen(),
  )


async def null_dispatch(action, state) -> None:
  return None


def clean(values: dict[str, float] | None = None):
  return CompletionSnapshot(
    outcome_exists=True,
    error_present=False,
    values=values or {},
  )


def errored(infra: bool = False):
  return CompletionSnapshot(
    outcome_exists=True, error_present=True, infra=infra
  )


# ── happy path ───────────────────────────────────────────────────


def test_runtime_dispatches_and_completes_all_tasks(tmp_path: Path):
  sched = mk_sched()
  sched.submit(mk_attempt(tmp_path, ["t1", "t2", "t3"]))
  dispatched: list[DispatchEntry] = []

  async def fake_dispatch(action, state) -> None:
    dispatched.append(action)

  seen: set[Path] = set()

  def fake_poll(trial_home: Path):
    if trial_home in seen:
      return None
    seen.add(trial_home)
    return clean()

  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    dispatch=fake_dispatch,
    poll=fake_poll,
    tick_interval=0,
  )
  asyncio.run(runtime.run_until_done(max_ticks=20))
  assert len(dispatched) == 3
  assert {a.task_name for a in dispatched} == {"t1", "t2", "t3"}
  view = sched.attempt_view("att-001")
  assert set(view.done_ok) == {"t1", "t2", "t3"}
  assert view.pending == [] and view.running == {}


def test_runtime_writes_trial_spec_before_dispatch(tmp_path: Path):
  sched = mk_sched(1)
  sched.submit(mk_attempt(tmp_path, ["t1"], payloads={"t1": {"n": 7}}))
  captured: list[dict] = []

  async def fake_dispatch(action, state) -> None:
    # By dispatch time the spec file must exist in the trial home.
    spec_path = tmp_path / action.trial_name / "trial.json"
    assert spec_path.exists()
    captured.append(json.loads(spec_path.read_text()))

  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    dispatch=fake_dispatch,
    poll=lambda _p: clean(),
    tick_interval=0,
  )
  asyncio.run(runtime.run_until_done(max_ticks=5))
  assert len(captured) == 1
  spec = captured[0]
  assert spec["task_name"] == "t1"
  assert spec["attempt_id"] == "att-001"
  assert spec["payload"] == {"n": 7}
  assert spec["home"] == "/dispatcher/home"


def test_runtime_pause_on_error_pauses_attempt(tmp_path: Path):
  sched = mk_sched(1)
  sched.submit(mk_attempt(tmp_path, ["t1", "t2"], max_concurrent=1))
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    dispatch=null_dispatch,
    poll=lambda _p: errored(),
    tick_interval=0,
  )
  asyncio.run(runtime.run_until_done(max_ticks=5))
  assert sched.attempt_paused("att-001") is True
  view = sched.attempt_view("att-001")
  assert set(view.done_err) == {"t1"}
  assert view.pending == ["t2"]


def test_paused_attempt_drains_when_last_trial_finishes(
  tmp_path: Path,
):
  sched = mk_sched(1)
  sched.submit(mk_attempt(tmp_path, ["t1"]))
  drained: list[str] = []
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    dispatch=null_dispatch,
    poll=lambda _p: clean(),
    tick_interval=0,
    on_attempt_drained=lambda state, _o: drained.append(state.attempt_id),
  )

  async def _run():
    await runtime._dispatch_available()
    sched.patch("att-001", paused=True)
    runtime._poll_all()

  asyncio.run(_run())
  # Pause blocks FUTURE dispatch; with everything terminal it must
  # not block finalisation — and drain must not unpause.
  assert drained == ["att-001"]
  assert sched.attempt_paused("att-001") is True


def test_resolver_applies_pause_before_completion_callbacks(
  tmp_path: Path,
):
  sched = mk_sched(1)
  sched.submit(mk_attempt(tmp_path, ["t1", "t2"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None
  sched.transition_trial(
    attempt_id="att-001",
    task_name="t1",
    from_state="running",
    to_state="unknown",
  )
  callback_pause_states: list[bool] = []
  drained: list[str] = []
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    poll=lambda _p: errored(),
    on_trial_completed=lambda state, _o: callback_pause_states.append(
      state.paused
    ),
    on_attempt_drained=lambda state, _o: drained.append(state.attempt_id),
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["done_err"] == 1
  assert callback_pause_states == [True]
  assert sched.attempt_paused("att-001") is True
  assert sched.attempt_view("att-001").pending == ["t2"]
  assert drained == []


def test_resolver_drains_last_error_without_pausing(tmp_path: Path):
  sched = mk_sched(1)
  sched.submit(mk_attempt(tmp_path, ["t1"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None
  sched.transition_trial(
    attempt_id="att-001",
    task_name="t1",
    from_state="running",
    to_state="unknown",
  )
  drained_pause_states: list[bool] = []
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    poll=lambda _p: errored(),
    on_attempt_drained=lambda state, _o: drained_pause_states.append(
      state.paused
    ),
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["done_err"] == 1
  assert drained_pause_states == [False]
  assert sched.attempt_paused("att-001") is False


def test_live_and_resolver_share_terminal_pipeline(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  observed: list[tuple[str, str, str]] = []
  event_orders: dict[str, list[str]] = {}

  class RecordingBus:
    def __init__(self, events: list[str]) -> None:
      self._events = events

    def publish(self, event_type, _payload):
      self._events.append(event_type)

  for mode in ("live", "resolver"):
    sched = mk_sched(1)
    home = tmp_path / mode
    sched.submit(mk_attempt(home, ["t1", "t2"], max_concurrent=1))
    action = sched.dispatch_one()
    assert action is not None
    snapshot = errored()
    event_types: list[str] = []
    metrics = MetricsCache()
    runtime = DispatcherRuntime(
      sched,
      self_host="ml10",
      poll=lambda _p, _s=snapshot: _s,
      metrics=metrics,
      event_bus=RecordingBus(event_types),  # type: ignore[arg-type]
    )
    original = runtime._apply_terminal_transition

    def record(*, _mode=mode, _original=original, **kwargs):
      observed.append((_mode, kwargs["from_state"], kwargs["to_state"]))
      return _original(**kwargs)

    monkeypatch.setattr(runtime, "_apply_terminal_transition", record)
    if mode == "live":
      runtime._apply_trial_completion(
        action.attempt_id,
        action.task_name,
        action.trial_name,
        snapshot,
      )
    else:
      sched.transition_trial(
        attempt_id="att-001",
        task_name="t1",
        from_state="running",
        to_state="unknown",
      )
      outcomes = asyncio.run(
        runtime.resolve_state_once(max_concurrent_probes=1)
      )
      assert outcomes["done_err"] == 1
    assert metrics.get("att-001").err == 1
    assert sched.attempt_paused("att-001") is True
    event_orders[mode] = event_types

  assert observed == [
    ("live", "running", "done_err"),
    ("resolver", "unknown", "done_err"),
  ]
  assert event_orders == {
    "live": ["trial_completed", "attempt_paused_on_error"],
    "resolver": ["trial_reclassified", "attempt_paused_on_error"],
  }


# ── resolver: container probe branches ───────────────────────────


def _seed_unknown(sched: Scheduler, home: Path) -> DispatchEntry:
  sched.submit(mk_attempt(home, ["t1"]))
  action = sched.dispatch_one()
  assert action is not None
  sched.transition_trial(
    attempt_id="att-001",
    task_name="t1",
    from_state="running",
    to_state="unknown",
  )
  return action


@pytest.mark.parametrize("status", ["gone", "exited", "dead", "created"])
def test_resolver_promotes_definitively_dead_to_ghosted(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: str
):
  sched = mk_sched(1)
  _seed_unknown(sched, tmp_path)

  async def fake_probe(host, trial_name, *, self_host, **kw):
    return status

  monkeypatch.setattr("dispatcher.runtime.probe_trial", fake_probe)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["ghosted"] == 1, outcomes
  view = sched.attempt_view("att-001")
  assert set(view.ghosted) == {"t1"}
  assert set(view.unknown) == set()


@pytest.mark.parametrize(
  "status", ["paused", "restarting", "removing", "weird-future"]
)
def test_resolver_leaves_transient_states_in_unknown(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: str
):
  sched = mk_sched(1)
  _seed_unknown(sched, tmp_path)

  async def fake_probe(host, trial_name, *, self_host, **kw):
    return status

  monkeypatch.setattr("dispatcher.runtime.probe_trial", fake_probe)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["unchanged"] == 1, outcomes
  assert set(sched.attempt_view("att-001").unknown) == {"t1"}


def test_resolver_adopts_still_running_container(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  sched = mk_sched(1)
  _seed_unknown(sched, tmp_path)

  async def fake_probe(host, trial_name, *, self_host, **kw):
    return "running"

  monkeypatch.setattr("dispatcher.runtime.probe_trial", fake_probe)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["running"] == 1
  assert set(sched.attempt_view("att-001").running) == {"t1"}
  # Host slot re-booked.
  assert sched.running_per_host()["ml10"] == 1


# ── ghosted re-poll ──────────────────────────────────────────────


def _seed_ghosted(sched: Scheduler, home: Path) -> None:
  _seed_unknown(sched, home)
  sched.transition_trial(
    attempt_id="att-001",
    task_name="t1",
    from_state="unknown",
    to_state="ghosted",
  )


def test_ghosted_re_poll_stays_ghosted_without_outcome(
  tmp_path: Path,
):
  sched = mk_sched(1)
  _seed_ghosted(sched, tmp_path)
  metrics = MetricsCache()
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None, metrics=metrics
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["unchanged"] == 1
  assert set(sched.attempt_view("att-001").ghosted) == {"t1"}
  m = metrics.get("att-001")
  assert m.ok == 0 and m.err == 0


def test_ghosted_re_poll_promotes_on_late_error(tmp_path: Path):
  sched = mk_sched(1)
  _seed_ghosted(sched, tmp_path)
  metrics = MetricsCache()
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    poll=lambda _p: errored(),
    metrics=metrics,
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["done_err"] == 1
  view = sched.attempt_view("att-001")
  assert set(view.done_err) == {"t1"}
  assert set(view.ghosted) == set()
  assert metrics.get("att-001").err == 1


def test_ghosted_re_poll_promotes_on_late_ok(tmp_path: Path):
  sched = mk_sched(1)
  _seed_ghosted(sched, tmp_path)
  metrics = MetricsCache()
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    poll=lambda _p: clean({"reward": 1.0}),
    metrics=metrics,
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["done_ok"] == 1
  m = metrics.get("att-001")
  assert m.ok == 1
  assert m.means == {"reward": 1.0}


# ── docker die handler ───────────────────────────────────────────


def _die_event(trial_name: str, exit_code: str) -> dict:
  return {
    "Actor": {
      "Attributes": {
        labels.TRIAL: trial_name,
        "exitCode": exit_code,
      }
    }
  }


def _sched_with_running(home: Path) -> tuple[Scheduler, str]:
  sched = mk_sched(1)
  sched.submit(mk_attempt(home, ["t1"]))
  action = sched.dispatch_one()
  assert action is not None
  return sched, action.trial_name


def test_die_nonzero_exit_with_clean_outcome_is_done_ok(
  tmp_path: Path,
):
  # Sibling teardown can SIGKILL (137) after the work finished and
  # wrote its envelope — the envelope wins.
  sched, trial_name = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: clean()
  )
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(trial_name, "137"))
  )
  view = sched.attempt_view("att-001")
  assert set(view.done_ok) == {"t1"}
  assert set(view.done_err) == set()


def test_die_with_error_outcome_stays_done_err(tmp_path: Path):
  sched, trial_name = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: errored()
  )
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(trial_name, "1"))
  )
  assert set(sched.attempt_view("att-001").done_err) == {"t1"}


def test_die_without_outcome_goes_to_unknown(tmp_path: Path):
  sched, trial_name = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  runtime._NFS_POLL_RETRY_DELAYS = ()  # no sleeping in tests
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(trial_name, "137"))
  )
  view = sched.attempt_view("att-001")
  assert set(view.unknown) == {"t1"}


def test_die_for_foreign_trial_is_ignored(tmp_path: Path):
  sched, _trial = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: clean()
  )
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event("not-ours", "0"))
  )
  # Still running — nothing moved.
  assert set(sched.attempt_view("att-001").running) == {"t1"}


def test_duplicate_die_is_ignored(tmp_path: Path):
  # docker --since replay can double-fire across reconnects; the
  # second event finds no running trial and drops.
  sched, trial_name = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: clean()
  )
  ev = _die_event(trial_name, "0")
  asyncio.run(runtime.handle_docker_die("ml10", ev))
  asyncio.run(runtime.handle_docker_die("ml10", ev))
  assert set(sched.attempt_view("att-001").done_ok) == {"t1"}


# ── infra requeue flows ──────────────────────────────────────────


def test_envelope_infra_requeues_instead_of_scoring(tmp_path: Path):
  sched, trial_name = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: errored(infra=True)
  )
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(trial_name, "75"))
  )
  view = sched.attempt_view("att-001")
  assert view.pending == ["t1"]
  assert not view.done_err


def test_infra_exit_without_outcome_requeues_at_ghost_promotion(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  # 137-killed container, no envelope: park unknown first (the
  # outcome may be NFS-lagged), requeue only once the container is
  # confirmed gone and the file still isn't there.
  sched, trial_name = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  runtime._NFS_POLL_RETRY_DELAYS = ()
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(trial_name, "137"))
  )
  assert set(sched.attempt_view("att-001").unknown) == {"t1"}

  async def fake_probe(host, tn, *, self_host, **kw):
    return "gone"

  monkeypatch.setattr("dispatcher.runtime.probe_trial", fake_probe)
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["requeued"] == 1
  view = sched.attempt_view("att-001")
  assert view.pending == ["t1"]
  assert not view.ghosted


def test_non_infra_exit_without_outcome_ghosts(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  sched, trial_name = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  runtime._NFS_POLL_RETRY_DELAYS = ()
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(trial_name, "1"))
  )

  async def fake_probe(host, tn, *, self_host, **kw):
    return "gone"

  monkeypatch.setattr("dispatcher.runtime.probe_trial", fake_probe)
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["ghosted"] == 1
  assert set(sched.attempt_view("att-001").ghosted) == {"t1"}


def test_infra_requeue_budget_exhaustion_scores_done_err(
  tmp_path: Path,
):
  sched = mk_sched(1)
  sched.submit(mk_attempt(tmp_path, ["t1"]))
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: errored(infra=True)
  )
  for i in range(Scheduler.MAX_INFRA_RETRIES + 1):
    action = sched.dispatch_one()
    assert action is not None, f"round {i}"
    runtime._apply_trial_completion(
      action.attempt_id,
      action.task_name,
      action.trial_name,
      errored(infra=True),
    )
  view = sched.attempt_view("att-001")
  assert set(view.done_err) == {"t1"}
  assert view.pending == []


# ── dispatch failure ─────────────────────────────────────────────


def test_dispatch_failure_requeues(tmp_path: Path):
  sched = mk_sched(1)
  sched.submit(mk_attempt(tmp_path, ["t1"]))

  async def failing_dispatch(action, state) -> None:
    raise DispatchError("no such image")

  runtime = DispatcherRuntime(
    sched, self_host="ml10", dispatch=failing_dispatch
  )
  asyncio.run(runtime._dispatch_available())
  # Requeued up to the budget, then parked in unknown — never a
  # silent drop, never scored as the work's failure.
  view = sched.attempt_view("att-001")
  assert set(view.unknown) == {"t1"}
  assert view.running == {}


def test_dispatch_failure_does_not_log_phantom_dispatch(
  tmp_path: Path,
):
  from dispatcher.event_log import RUN_LOG_FILENAME, read_events

  sched = mk_sched(1)
  sched.submit(mk_attempt(tmp_path, ["t1"]))

  async def failing_dispatch(action, state) -> None:
    raise DispatchError("boom")

  runtime = DispatcherRuntime(
    sched, self_host="ml10", dispatch=failing_dispatch
  )
  asyncio.run(runtime._dispatch_available())
  log = tmp_path / RUN_LOG_FILENAME
  if log.exists():
    types = [e["type"] for e in read_events(log)]
    assert "dispatch" not in types


# ── census reconcile ─────────────────────────────────────────────


def test_reconcile_from_census_scores_exited_container(
  tmp_path: Path,
):
  sched, trial_name = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: clean()
  )
  containers = [
    {
      "State": "exited",
      "Status": "Exited (0) 5 minutes ago",
      "Labels": f"{labels.MANAGED}=1,{labels.TRIAL}={trial_name}",
    },
    {"State": "running", "Labels": ""},
  ]
  asyncio.run(runtime.reconcile_from_census("ml10", containers))
  assert set(sched.attempt_view("att-001").done_ok) == {"t1"}


def test_reconcile_remembers_infra_exit_code(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  sched, trial_name = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  runtime._NFS_POLL_RETRY_DELAYS = ()
  containers = [
    {
      "State": "exited",
      "Status": "Exited (137) 2 minutes ago",
      "Labels": f"{labels.MANAGED}=1,{labels.TRIAL}={trial_name}",
    }
  ]
  asyncio.run(runtime.reconcile_from_census("ml10", containers))
  assert set(sched.attempt_view("att-001").unknown) == {"t1"}

  async def fake_probe(host, tn, *, self_host, **kw):
    return "exited"

  monkeypatch.setattr("dispatcher.runtime.probe_trial", fake_probe)
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["requeued"] == 1
