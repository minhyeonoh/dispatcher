"""DispatcherRuntime: dispatch flow, die handling, resolver,
infra requeue, GC — with fake dispatch/poll/docker."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest

from dispatcher.core import labels
from dispatcher.core.dispatch import DispatchError
from dispatcher.core.models import HostSettings
from dispatcher.core.outcome import CompletionSnapshot
from dispatcher.core.runtime import DispatcherRuntime
from dispatcher.core.scheduler import Scheduler
from tests.test_scheduler import clock_from, id_gen

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.models import DispatchEntry, JobState


def mk_job(
  home_root: Path,
  task_ids: list[str],
  *,
  job_id: str = "job-001",
  max_concurrent: int | None = None,
  payloads: dict | None = None,
) -> JobState:
  from dispatcher.core.event_log import replay_events

  events: list[dict] = [
    {
      "type": "submit",
      "job_id": job_id,
      "label": "demo",
      "task_ids": task_ids,
      "home_root": str(home_root),
      "container": {"image": "img"},
      "submitted_at": "2026-09-28T10:00:00+00:00",
      "alias": f"{job_id}-alias",
      "payloads": payloads or {},
    }
  ]
  if max_concurrent is not None:
    events.append(
      {
        "type": "patch",
        "job_id": job_id,
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
    id_gen=id_gen(),
  )


async def null_dispatch(action, state) -> None:
  return None


def clean():
  return CompletionSnapshot(
    outcome_exists=True,
    error_present=False,
  )


def errored(infra: bool = False):
  return CompletionSnapshot(
    outcome_exists=True, error_present=True, infra=infra
  )


# ── happy path ───────────────────────────────────────────────────


def test_runtime_dispatches_and_completes_all_tasks(tmp_path: Path):
  sched = mk_sched()
  sched.submit(mk_job(tmp_path, ["t1", "t2", "t3"]))
  dispatched: list[DispatchEntry] = []

  async def fake_dispatch(action, state) -> None:
    dispatched.append(action)

  seen: set[Path] = set()

  def fake_poll(instance_home: Path):
    if instance_home in seen:
      return None
    seen.add(instance_home)
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
  assert {a.task_id for a in dispatched} == {"t1", "t2", "t3"}
  view = sched.job_view("job-001")
  assert set(view.done_ok) == {"t1", "t2", "t3"}
  assert view.pending == [] and view.running == {}


def test_runtime_writes_instance_spec_before_dispatch(tmp_path: Path):
  sched = mk_sched(1)
  sched.submit(mk_job(tmp_path, ["t1"], payloads={"t1": {"n": 7}}))
  captured: list[dict] = []

  async def fake_dispatch(action, state) -> None:
    # By dispatch time the spec file must exist in the instance home.
    spec_path = tmp_path / action.instance_id / "instance.json"
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
  assert spec["task_id"] == "t1"
  assert spec["job_id"] == "job-001"
  assert spec["payload"] == {"n": 7}
  assert spec["home"] == "/dispatcher/home"


def test_runtime_pause_on_error_pauses_job(tmp_path: Path):
  sched = mk_sched(1)
  sched.submit(mk_job(tmp_path, ["t1", "t2"], max_concurrent=1))
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    dispatch=null_dispatch,
    poll=lambda _p: errored(),
    tick_interval=0,
  )
  asyncio.run(runtime.run_until_done(max_ticks=5))
  assert sched.job_paused("job-001") is True
  view = sched.job_view("job-001")
  assert set(view.done_err) == {"t1"}
  assert view.pending == ["t2"]


def test_paused_job_drains_when_last_instance_finishes(
  tmp_path: Path,
):
  sched = mk_sched(1)
  sched.submit(mk_job(tmp_path, ["t1"]))
  drained: list[str] = []
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    dispatch=null_dispatch,
    poll=lambda _p: clean(),
    tick_interval=0,
    on_job_drained=lambda state, _o: drained.append(state.job_id),
  )

  async def _run():
    await runtime._dispatch_available()
    sched.patch("job-001", paused=True)
    await runtime._poll_all()

  asyncio.run(_run())
  # Pause blocks FUTURE dispatch; with everything terminal it must
  # not block finalisation — and drain must not unpause.
  assert drained == ["job-001"]
  assert sched.job_paused("job-001") is True


def test_resolver_applies_pause_before_completion_callbacks(
  tmp_path: Path,
):
  sched = mk_sched(1)
  sched.submit(mk_job(tmp_path, ["t1", "t2"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None
  sched.transition_instance(
    job_id="job-001",
    task_id="t1",
    from_state="running",
    to_state="unknown",
  )
  callback_pause_states: list[bool] = []
  drained: list[str] = []
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    poll=lambda _p: errored(),
    on_instance_completed=lambda state, _o: callback_pause_states.append(
      state.paused
    ),
    on_job_drained=lambda state, _o: drained.append(state.job_id),
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["done_err"] == 1
  assert callback_pause_states == [True]
  assert sched.job_paused("job-001") is True
  assert sched.job_view("job-001").pending == ["t2"]
  assert drained == []


def test_resolver_drains_last_error_without_pausing(tmp_path: Path):
  sched = mk_sched(1)
  sched.submit(mk_job(tmp_path, ["t1"], max_concurrent=1))
  action = sched.dispatch_one()
  assert action is not None
  sched.transition_instance(
    job_id="job-001",
    task_id="t1",
    from_state="running",
    to_state="unknown",
  )
  drained_pause_states: list[bool] = []
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    poll=lambda _p: errored(),
    on_job_drained=lambda state, _o: drained_pause_states.append(
      state.paused
    ),
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["done_err"] == 1
  assert drained_pause_states == [False]
  assert sched.job_paused("job-001") is False


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
    sched.submit(mk_job(home, ["t1", "t2"], max_concurrent=1))
    action = sched.dispatch_one()
    assert action is not None
    snapshot = errored()
    event_types: list[str] = []
    runtime = DispatcherRuntime(
      sched,
      self_host="ml10",
      poll=lambda _p, _s=snapshot: _s,
      event_bus=RecordingBus(event_types),  # type: ignore[arg-type]
    )
    original = runtime._apply_terminal_transition

    def record(*, _mode=mode, _original=original, **kwargs):
      observed.append((_mode, kwargs["from_state"], kwargs["to_state"]))
      return _original(**kwargs)

    monkeypatch.setattr(runtime, "_apply_terminal_transition", record)
    if mode == "live":
      asyncio.run(
        runtime._apply_instance_completion(
          action.job_id,
          action.task_id,
          action.instance_id,
          snapshot,
        )
      )
    else:
      sched.transition_instance(
        job_id="job-001",
        task_id="t1",
        from_state="running",
        to_state="unknown",
      )
      outcomes = asyncio.run(
        runtime.resolve_state_once(max_concurrent_probes=1)
      )
      assert outcomes["done_err"] == 1
    assert set(sched.job_view("job-001").done_err) == {"t1"}
    assert sched.job_paused("job-001") is True
    event_orders[mode] = event_types

  assert observed == [
    ("live", "running", "done_err"),
    ("resolver", "unknown", "done_err"),
  ]
  assert event_orders == {
    "live": ["instance_completed", "job_paused_on_error"],
    "resolver": ["instance_reclassified", "job_paused_on_error"],
  }


# ── resolver: container probe branches ───────────────────────────


def _seed_unknown(sched: Scheduler, home: Path) -> DispatchEntry:
  sched.submit(mk_job(home, ["t1"]))
  action = sched.dispatch_one()
  assert action is not None
  sched.transition_instance(
    job_id="job-001",
    task_id="t1",
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

  async def fake_probe(host, instance_id, *, self_host, **kw):
    return status

  monkeypatch.setattr("dispatcher.core.runtime.probe_instance", fake_probe)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["ghosted"] == 1, outcomes
  view = sched.job_view("job-001")
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

  async def fake_probe(host, instance_id, *, self_host, **kw):
    return status

  monkeypatch.setattr("dispatcher.core.runtime.probe_instance", fake_probe)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["unchanged"] == 1, outcomes
  assert set(sched.job_view("job-001").unknown) == {"t1"}


def test_resolver_adopts_still_running_container(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  sched = mk_sched(1)
  _seed_unknown(sched, tmp_path)

  async def fake_probe(host, instance_id, *, self_host, **kw):
    return "running"

  monkeypatch.setattr("dispatcher.core.runtime.probe_instance", fake_probe)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["running"] == 1
  assert set(sched.job_view("job-001").running) == {"t1"}
  # Host slot re-booked.
  assert sched.running_per_host()["ml10"] == 1


# ── ghosted re-poll ──────────────────────────────────────────────


def _seed_ghosted(sched: Scheduler, home: Path) -> None:
  _seed_unknown(sched, home)
  sched.transition_instance(
    job_id="job-001",
    task_id="t1",
    from_state="unknown",
    to_state="ghosted",
  )


def test_ghosted_re_poll_stays_ghosted_without_outcome(
  tmp_path: Path,
):
  sched = mk_sched(1)
  _seed_ghosted(sched, tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["unchanged"] == 1
  assert set(sched.job_view("job-001").ghosted) == {"t1"}


def test_ghosted_re_poll_promotes_on_late_error(tmp_path: Path):
  sched = mk_sched(1)
  _seed_ghosted(sched, tmp_path)
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    poll=lambda _p: errored(),
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["done_err"] == 1
  view = sched.job_view("job-001")
  assert set(view.done_err) == {"t1"}
  assert set(view.ghosted) == set()


def test_ghosted_re_poll_promotes_on_late_ok(tmp_path: Path):
  sched = mk_sched(1)
  _seed_ghosted(sched, tmp_path)
  runtime = DispatcherRuntime(
    sched,
    self_host="ml10",
    poll=lambda _p: clean(),
  )
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["done_ok"] == 1
  assert set(sched.job_view("job-001").done_ok) == {"t1"}


# ── docker die handler ───────────────────────────────────────────


def _die_event(instance_id: str, exit_code: str) -> dict:
  return {
    "Actor": {
      "Attributes": {
        labels.INSTANCE: instance_id,
        "exitCode": exit_code,
      }
    }
  }


def _sched_with_running(home: Path) -> tuple[Scheduler, str]:
  sched = mk_sched(1)
  sched.submit(mk_job(home, ["t1"]))
  action = sched.dispatch_one()
  assert action is not None
  return sched, action.instance_id


def test_die_nonzero_exit_with_clean_outcome_is_done_ok(
  tmp_path: Path,
):
  # Sibling teardown can SIGKILL (137) after the work finished and
  # wrote its envelope — the envelope wins.
  sched, instance_id = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: clean()
  )
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(instance_id, "137"))
  )
  view = sched.job_view("job-001")
  assert set(view.done_ok) == {"t1"}
  assert set(view.done_err) == set()


def test_die_with_error_outcome_stays_done_err(tmp_path: Path):
  sched, instance_id = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: errored()
  )
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(instance_id, "1"))
  )
  assert set(sched.job_view("job-001").done_err) == {"t1"}


def test_die_without_outcome_goes_to_unknown(tmp_path: Path):
  sched, instance_id = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  runtime._NFS_POLL_RETRY_DELAYS = ()  # no sleeping in tests
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(instance_id, "137"))
  )
  view = sched.job_view("job-001")
  assert set(view.unknown) == {"t1"}


def test_die_for_foreign_instance_is_ignored(tmp_path: Path):
  sched, _instance = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: clean()
  )
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event("not-ours", "0"))
  )
  # Still running — nothing moved.
  assert set(sched.job_view("job-001").running) == {"t1"}


def test_duplicate_die_is_ignored(tmp_path: Path):
  # docker --since replay can double-fire across reconnects; the
  # second event finds no running instance and drops.
  sched, instance_id = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: clean()
  )
  ev = _die_event(instance_id, "0")
  asyncio.run(runtime.handle_docker_die("ml10", ev))
  asyncio.run(runtime.handle_docker_die("ml10", ev))
  assert set(sched.job_view("job-001").done_ok) == {"t1"}


# ── infra requeue flows ──────────────────────────────────────────


def test_envelope_infra_requeues_instead_of_scoring(tmp_path: Path):
  sched, instance_id = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: errored(infra=True)
  )
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(instance_id, "75"))
  )
  view = sched.job_view("job-001")
  assert view.pending == ["t1"]
  assert not view.done_err


def test_infra_exit_without_outcome_requeues_at_ghost_promotion(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  # 137-killed container, no envelope: park unknown first (the
  # outcome may be NFS-lagged), requeue only once the container is
  # confirmed gone and the file still isn't there.
  sched, instance_id = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  runtime._NFS_POLL_RETRY_DELAYS = ()
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(instance_id, "137"))
  )
  assert set(sched.job_view("job-001").unknown) == {"t1"}

  async def fake_probe(host, tn, *, self_host, **kw):
    return "gone"

  monkeypatch.setattr("dispatcher.core.runtime.probe_instance", fake_probe)
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["requeued"] == 1
  view = sched.job_view("job-001")
  assert view.pending == ["t1"]
  assert not view.ghosted


def test_non_infra_exit_without_outcome_ghosts(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  sched, instance_id = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  runtime._NFS_POLL_RETRY_DELAYS = ()
  asyncio.run(
    runtime.handle_docker_die("ml10", _die_event(instance_id, "1"))
  )

  async def fake_probe(host, tn, *, self_host, **kw):
    return "gone"

  monkeypatch.setattr("dispatcher.core.runtime.probe_instance", fake_probe)
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["ghosted"] == 1
  assert set(sched.job_view("job-001").ghosted) == {"t1"}


def test_infra_requeue_budget_exhaustion_scores_done_err(
  tmp_path: Path,
):
  sched = mk_sched(1)
  sched.submit(mk_job(tmp_path, ["t1"]))
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: errored(infra=True)
  )
  for i in range(Scheduler.MAX_INFRA_RETRIES + 1):
    action = sched.dispatch_one()
    assert action is not None, f"round {i}"
    asyncio.run(
      runtime._apply_instance_completion(
        action.job_id,
        action.task_id,
        action.instance_id,
        errored(infra=True),
      )
    )
  view = sched.job_view("job-001")
  assert set(view.done_err) == {"t1"}
  assert view.pending == []


# ── dispatch failure ─────────────────────────────────────────────


def test_dispatch_failure_requeues(tmp_path: Path):
  sched = mk_sched(1)
  sched.submit(mk_job(tmp_path, ["t1"]))

  async def failing_dispatch(action, state) -> None:
    raise DispatchError("no such image")

  runtime = DispatcherRuntime(
    sched, self_host="ml10", dispatch=failing_dispatch
  )
  asyncio.run(runtime._dispatch_available())
  # Requeued up to the budget, then parked in unknown — never a
  # silent drop, never scored as the work's failure.
  view = sched.job_view("job-001")
  assert set(view.unknown) == {"t1"}
  assert view.running == {}


def test_dispatch_failure_does_not_log_phantom_dispatch(
  tmp_path: Path,
):
  from dispatcher.core.event_log import RUN_LOG_FILENAME, read_events

  sched = mk_sched(1)
  sched.submit(mk_job(tmp_path, ["t1"]))

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
  sched, instance_id = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: clean()
  )
  containers = [
    {
      "State": {"Status": "exited", "ExitCode": 0},
      "Config": {
        "Labels": {labels.MANAGED: "1", labels.INSTANCE: instance_id}
      },
    },
    {"State": {"Status": "running"}, "Config": {"Labels": {}}},
  ]
  asyncio.run(runtime.reconcile_from_census("ml10", containers))
  assert set(sched.job_view("job-001").done_ok) == {"t1"}


def test_reconcile_remembers_infra_exit_code(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  sched, instance_id = _sched_with_running(tmp_path)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  runtime._NFS_POLL_RETRY_DELAYS = ()
  containers = [
    {
      "State": {"Status": "exited", "ExitCode": 137},
      "Config": {
        "Labels": {labels.MANAGED: "1", labels.INSTANCE: instance_id}
      },
    }
  ]
  asyncio.run(runtime.reconcile_from_census("ml10", containers))
  assert set(sched.job_view("job-001").unknown) == {"t1"}

  async def fake_probe(host, tn, *, self_host, **kw):
    return "exited"

  monkeypatch.setattr("dispatcher.core.runtime.probe_instance", fake_probe)
  outcomes = asyncio.run(
    runtime.resolve_state_once(max_concurrent_probes=1)
  )
  assert outcomes["requeued"] == 1


# ── stale observations (async windows + latent 7s die window) ───


def test_stale_die_observation_cannot_score_new_instance(
  tmp_path: Path,
):
  # A die observation for instance N arriving AFTER the task was
  # requeued and re-dispatched as instance N+1 must be dropped —
  # applying it would score the NEW instance with the OLD instance's
  # outcome. This window existed even pre-async (the die
  # handler's NFS retry pause); the guard closes it.
  sched = mk_sched(1)
  sched.submit(mk_job(tmp_path, ["t1"]))
  first = sched.dispatch_one()
  assert first is not None
  # Task goes back to pending (infra requeue) and gets a fresh
  # instance while the old die observation is still in flight.
  assert sched.requeue_after_infra_failure("job-001", "t1", "running")
  second = sched.dispatch_one()
  assert second is not None
  assert second.instance_id != first.instance_id

  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: clean()
  )
  # Old instance's completion lands late.
  asyncio.run(
    runtime._apply_instance_completion(
      "job-001", "t1", first.instance_id, clean()
    )
  )
  view = sched.job_view("job-001")
  # New instance still running, untouched; nothing scored.
  assert set(view.running) == {"t1"}
  assert view.running["t1"].instance_id == second.instance_id
  assert not view.done_ok and not view.done_err


def test_stale_infra_observation_cannot_requeue_new_instance(
  tmp_path: Path,
):
  sched = mk_sched(1)
  sched.submit(mk_job(tmp_path, ["t1"]))
  first = sched.dispatch_one()
  assert first is not None
  sched.requeue_after_infra_failure("job-001", "t1", "running")
  second = sched.dispatch_one()
  assert second is not None
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  asyncio.run(
    runtime._apply_instance_completion(
      "job-001", "t1", first.instance_id, errored(infra=True)
    )
  )
  view = sched.job_view("job-001")
  # The stale infra report must not bounce the LIVE instance.
  assert view.running["t1"].instance_id == second.instance_id


# ── NFS cache bust in the retry ladder ──────────────────────────


def test_retry_ladder_busts_cache_between_jobs(tmp_path: Path):
  # Simulates the measured failure: the file "exists" but the
  # client cache answers miss until a bust invalidates it. The
  # die handler must land done_ok on its own, without parking in
  # unknown for the resolver.
  sched, instance_id = _sched_with_running(tmp_path)
  busted = [False]

  def fake_poll(instance_home):
    return clean() if busted[0] else None

  runtime = DispatcherRuntime(sched, self_host="ml10", poll=fake_poll)
  runtime._NFS_POLL_RETRY_DELAYS = (0.0,)

  def fake_bust(instance_home) -> None:
    busted[0] = True

  import dispatcher.core.runtime as rt

  orig = rt.bust_dir_cache
  rt.bust_dir_cache = fake_bust
  try:
    asyncio.run(
      runtime.handle_docker_die("ml10", _die_event(instance_id, "0"))
    )
  finally:
    rt.bust_dir_cache = orig
  view = sched.job_view("job-001")
  assert set(view.done_ok) == {"t1"}
  assert not view.unknown


def test_bust_dir_cache_is_best_effort(tmp_path: Path):
  from dispatcher.core.outcome import bust_dir_cache

  # Owned dir: probe leaves no trace.
  d = tmp_path / "t"
  d.mkdir()
  bust_dir_cache(d)
  assert list(d.iterdir()) == []
  # Missing dir: silently does nothing (behaviour falls back to
  # today's miss → resolver path).
  bust_dir_cache(tmp_path / "nope")
