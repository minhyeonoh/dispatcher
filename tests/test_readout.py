"""Readouts: value files, the registry, the live path (the worker
scoring itself), the retroactive pass, and the HTTP surface."""

from __future__ import annotations

import asyncio
import json
import pathlib
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from dispatcher.api.wire import snapshot_job
from dispatcher.core.models import HostSettings
from dispatcher.core.outcome import read_completion
from dispatcher.core.readout import (
  BadReadout,
  ReadoutSpec,
  ReadoutValue,
  aggregate,
  append_values,
  parse_result_lines,
  read_instance_readouts,
  read_values,
  request_path,
  values_path,
)
from dispatcher.core.readout_service import (
  ReadoutConflict,
  ReadoutRegistry,
  ReadoutService,
  ReadoutSettings,
  build_readout_argv,
)
from dispatcher.core.scheduler import Scheduler
from dispatcher_sdk import readout as sdk_readout
from tests.test_runtime import mk_job
from tests.test_scheduler import clock_from, id_gen

if TYPE_CHECKING:
  from dispatcher.core.models import JobState


# ── value files ──────────────────────────────────────────────────


def test_read_values_last_line_wins(tmp_path: Path):
  append_values(
    tmp_path,
    "reward",
    [
      ReadoutValue(name="reward", instance_id="i1", value=0.1),
      ReadoutValue(name="reward", instance_id="i2", value=0.2),
      ReadoutValue(name="reward", instance_id="i1", value=0.9),
    ],
  )
  values = read_values(tmp_path, "reward")
  assert values["i1"].value == 0.9
  assert values["i2"].value == 0.2
  # The superseded line is still in the file — a recompute shadows
  # history, it does not erase it.
  assert len(values_path(tmp_path, "reward").read_text().splitlines()) == 3


def test_read_values_skips_a_torn_line(tmp_path: Path):
  path = values_path(tmp_path, "r")
  path.parent.mkdir(parents=True)
  good = ReadoutValue(
    name="r", instance_id="i1", value=1
  ).model_dump_json()
  path.write_text(good + "\n{ broken\n", encoding="utf-8")
  assert set(read_values(tmp_path, "r")) == {"i1"}


def test_read_values_of_missing_file_is_empty(tmp_path: Path):
  assert read_values(tmp_path, "nope") == {}


def test_aggregate_means_numbers_and_counts_errors():
  agg = aggregate(
    {
      "i1": ReadoutValue(instance_id="i1", value=1.0),
      "i2": ReadoutValue(instance_id="i2", value=0.0),
      "i3": ReadoutValue(instance_id="i3", ok=False, error="boom"),
    }
  )
  assert (agg.n, agg.errors, agg.mean) == (3, 1, 0.5)


def test_aggregate_treats_bools_as_a_pass_rate():
  agg = aggregate(
    {
      "i1": ReadoutValue(instance_id="i1", value=True),
      "i2": ReadoutValue(instance_id="i2", value=False),
    }
  )
  assert agg.mean == 0.5


def test_aggregate_excludes_nulls_from_the_mean():
  """A readout saying "not applicable here" must not be averaged as
  zero — that is the difference between reporting a broken run and
  reporting a worse method."""
  agg = aggregate(
    {
      "i1": ReadoutValue(instance_id="i1", value=0.8),
      "i2": ReadoutValue(instance_id="i2", value=0.6),
      "i3": ReadoutValue(instance_id="i3", value=None),
    }
  )
  assert (agg.n, agg.nulls, agg.mean) == (3, 1, 0.7)


def test_aggregate_refuses_a_mean_over_mixed_values():
  agg = aggregate(
    {
      "i1": ReadoutValue(instance_id="i1", value=1.0),
      "i2": ReadoutValue(instance_id="i2", value="TGC"),
    }
  )
  assert agg.n == 2
  assert agg.mean is None


def test_parse_result_lines_ignores_operator_output():
  line = ReadoutValue(
    name="r", instance_id="i1", value=3
  ).model_dump_json()
  stdout = (
    "loading model…\n"
    + sdk_readout.RESULT_PREFIX
    + line
    + "\ndone\n"
    + sdk_readout.RESULT_PREFIX
    + "{ not json\n"
  )
  parsed = parse_result_lines(stdout)
  assert [v.instance_id for v in parsed] == ["i1"]


# ── registry ─────────────────────────────────────────────────────


def mk_registry(tmp_path: Path) -> ReadoutRegistry:
  return ReadoutRegistry(tmp_path / "readouts.json")


def spec(
  name: str, entrypoint: str = "myrepo.readouts:reward"
) -> ReadoutSpec:
  return ReadoutSpec(name=name, entrypoint=entrypoint)


def test_registry_applies_to_the_whole_subtree(tmp_path: Path):
  reg = mk_registry(tmp_path)
  reg.register("bench", spec("reward"))
  reg.register("bench/v7", spec("tgc", "myrepo.readouts:tgc"))
  assert [s.name for s in reg.for_arena("bench/v7/front5")] == [
    "reward",
    "tgc",
  ]
  assert [s.name for s in reg.for_arena("bench")] == ["reward"]
  # Segment-aware: bench/v70 is not under bench/v7.
  assert [s.name for s in reg.for_arena("bench/v70")] == ["reward"]


def test_registry_gives_nothing_to_an_arena_less_job(tmp_path: Path):
  reg = mk_registry(tmp_path)
  reg.register("bench", spec("reward"))
  assert reg.for_arena("") == []


def test_registry_refuses_the_same_name_on_one_path(tmp_path: Path):
  reg = mk_registry(tmp_path)
  reg.register("bench", spec("reward"))
  with pytest.raises(ReadoutConflict):
    reg.register("bench/v7", spec("reward", "other:reward"))
  # The reverse direction too — registering an ancestor after a
  # descendant would shadow just as badly.
  reg2 = mk_registry(tmp_path / "b")
  reg2.register("bench/v7", spec("reward"))
  with pytest.raises(ReadoutConflict):
    reg2.register("bench", spec("reward", "other:reward"))


def test_registry_allows_the_same_name_on_a_sibling_path(tmp_path: Path):
  reg = mk_registry(tmp_path)
  reg.register("a/one", spec("reward"))
  reg.register("a/two", spec("reward", "other:reward"))
  assert [s.name for s in reg.for_arena("a/one")] == ["reward"]


def test_registry_replaces_by_name_at_the_same_node(tmp_path: Path):
  reg = mk_registry(tmp_path)
  reg.register("bench", spec("reward", "a:one"))
  reg.register("bench", spec("reward", "b:two"))
  specs = reg.for_arena("bench")
  assert len(specs) == 1
  assert specs[0].entrypoint == "b:two"


def test_registry_round_trips_through_disk(tmp_path: Path):
  reg = mk_registry(tmp_path)
  reg.register("bench/v7", spec("reward"))
  again = ReadoutRegistry.load(tmp_path)
  assert [s.name for s in again.for_arena("bench/v7")] == ["reward"]


def test_registry_refuses_to_boot_on_a_corrupt_file(tmp_path: Path):
  (tmp_path / "readouts.json").write_text("{ broken", encoding="utf-8")
  with pytest.raises(RuntimeError, match="unreadable"):
    ReadoutRegistry.load(tmp_path)


def test_unregister_keeps_the_values(tmp_path: Path):
  reg = mk_registry(tmp_path)
  reg.register("bench", spec("reward"))
  append_values(
    tmp_path, "reward", [ReadoutValue(instance_id="i1", value=1)]
  )
  assert reg.unregister("bench", "reward") is True
  assert reg.unregister("bench", "reward") is False
  assert read_values(tmp_path, "reward") != {}


def test_bad_name_and_entrypoint_are_rejected(tmp_path: Path):
  reg = mk_registry(tmp_path)
  with pytest.raises(BadReadout):
    reg.register("bench", ReadoutSpec(name="../etc", entrypoint="a:b"))
  with pytest.raises(BadReadout):
    reg.register("bench", ReadoutSpec(name="ok", entrypoint="no-colon"))


# ── container argv ───────────────────────────────────────────────


def test_argv_mounts_the_job_home_read_only(tmp_path: Path):
  job = mk_job(tmp_path / "home", ["t1"])
  argv = build_readout_argv(job)
  assert f"{job.home_root}:/dispatcher/job:ro" in argv
  assert "--rm" in argv
  # Never the labels the instance machinery keys on: a readout
  # container must be invisible to the die filter and the GC.
  joined = " ".join(argv)
  assert "dispatcher.readout=" in joined
  assert "dispatcher.managed" not in joined
  assert "dispatcher.set" not in joined
  assert argv[-1] == "dispatcher_sdk.readout"


def test_argv_skips_extra_args_and_mounts_the_source(tmp_path: Path):
  job = mk_job(tmp_path / "home", ["t1"])
  job.container.extra_args = ["--gpus", "all"]
  job.source_sha256 = "deadbeef"
  argv = build_readout_argv(job)
  assert "--gpus" not in argv
  assert (
    f"{job.home_root / '.source.tar'}:/dispatcher/source.tar:ro" in argv
  )


# ── in-container runner ──────────────────────────────────────────


def mk_instance_dir(job_dir: Path, instance_id: str, data: Any) -> None:
  home = job_dir / instance_id
  home.mkdir(parents=True)
  (home / "outcome.json").write_text(
    json.dumps({"ok": True, "data": data}), encoding="utf-8"
  )
  (home / "instance.json").write_text(
    json.dumps({"task_id": "t1", "payload": {"seed": 3}}),
    encoding="utf-8",
  )


def run_sdk(
  monkeypatch,
  capsys,
  tmp_path: Path,
  *,
  entrypoint: str,
  timeout_sec: float = 60.0,
) -> list[dict[str, Any]]:
  job_dir = tmp_path / "job"
  mk_instance_dir(job_dir, "i1", {"reward": 0.5})
  request = tmp_path / "request.json"
  request.write_text(
    json.dumps(
      {
        "job_id": "job-1",
        "job_dir": str(job_dir),
        "readouts": [
          {
            "name": "reward",
            "entrypoint": entrypoint,
            "timeout_sec": timeout_sec,
          }
        ],
        "instances": [
          {
            "instance_id": "i1",
            "task_id": "t1",
            "state": "done_ok",
            "readouts": ["reward"],
          }
        ],
      }
    ),
    encoding="utf-8",
  )
  monkeypatch.setenv("DISPATCHER_READOUT_REQUEST", str(request))
  assert sdk_readout.main() == 0
  out = capsys.readouterr().out
  return [
    json.loads(line.split(sdk_readout.RESULT_PREFIX, 1)[1])
    for line in out.splitlines()
    if sdk_readout.RESULT_PREFIX in line
  ]


def test_sdk_runner_emits_a_value(monkeypatch, capsys, tmp_path: Path):
  import sys
  import types

  module = types.ModuleType("fakerepo")

  def reward(instance):
    assert instance.state == "done_ok"
    assert instance.payload == {"seed": 3}
    return instance.data["reward"]

  module.__dict__["reward"] = reward
  monkeypatch.setitem(sys.modules, "fakerepo", module)
  rows = run_sdk(
    monkeypatch, capsys, tmp_path, entrypoint="fakerepo:reward"
  )
  assert rows == [
    {
      "name": "reward",
      "instance_id": "i1",
      "task_id": "t1",
      "ok": True,
      "value": 0.5,
    }
  ]


def test_sdk_runner_records_a_raise_as_an_error(
  monkeypatch, capsys, tmp_path: Path
):
  import sys
  import types

  module = types.ModuleType("fakerepo2")

  def reward(instance):
    raise KeyError("tgc")

  module.__dict__["reward"] = reward
  monkeypatch.setitem(sys.modules, "fakerepo2", module)
  rows = run_sdk(
    monkeypatch, capsys, tmp_path, entrypoint="fakerepo2:reward"
  )
  assert rows[0]["ok"] is False
  assert "KeyError" in rows[0]["error"]


def test_sdk_runner_records_an_unimportable_entrypoint(
  monkeypatch, capsys, tmp_path: Path
):
  rows = run_sdk(
    monkeypatch, capsys, tmp_path, entrypoint="nosuchmodule:reward"
  )
  assert rows[0]["ok"] is False
  assert "ModuleNotFoundError" in rows[0]["error"]


def test_sdk_runner_interrupts_a_hanging_readout(
  monkeypatch, capsys, tmp_path: Path
):
  import sys
  import time
  import types

  module = types.ModuleType("fakerepo3")

  def reward(instance):
    time.sleep(30)

  module.__dict__["reward"] = reward
  monkeypatch.setitem(sys.modules, "fakerepo3", module)
  rows = run_sdk(
    monkeypatch,
    capsys,
    tmp_path,
    entrypoint="fakerepo3:reward",
    timeout_sec=0.05,
  )
  assert rows[0]["ok"] is False
  assert "ReadoutTimeout" in rows[0]["error"]


def test_sdk_runner_without_a_request_refuses_to_guess(monkeypatch):
  monkeypatch.delenv("DISPATCHER_READOUT_REQUEST", raising=False)
  assert sdk_readout.main() == 2


# ── the live path: the worker scores itself ──────────────────────


def _worker_env(home: Path) -> dict[str, str]:
  return {
    "DISPATCHER_HOME": str(home),
    "DISPATCHER_JOB": "job-1",
    "DISPATCHER_TASK": "t1",
    "DISPATCHER_INSTANCE": "t1__0000001",
    "DISPATCHER_SET_LABEL": "dispatcher.set=t1__0000001",
  }


def _worker_spec(
  home: Path, *, readouts: list[dict], payload: Any = None
) -> None:
  home.mkdir(parents=True, exist_ok=True)
  (home / "instance.json").write_text(
    json.dumps(
      {
        "job_id": "job-1",
        "task_id": "t1",
        "instance_id": "t1__0000001",
        "home": str(home),
        "payload": payload,
        "readouts": readouts,
      }
    )
  )


def _fake_module(monkeypatch, name: str, **funcs) -> None:
  import sys
  import types

  module = types.ModuleType(name)
  for attr, func in funcs.items():
    module.__dict__[attr] = func
  monkeypatch.setitem(sys.modules, name, module)


def _run_worker(work, home: Path) -> int:
  from dispatcher_sdk import run

  codes: list[int] = []
  run(work, env=_worker_env(home), _exit=codes.append)
  return codes[0] if codes else -1


def test_worker_scores_itself_in_process(monkeypatch, tmp_path: Path):
  _fake_module(
    monkeypatch,
    "liverepo",
    reward=lambda inst: inst.data["reward"],
    solved=lambda inst: inst.state == "done_ok",
  )
  home = tmp_path / "t1__0000001"
  _worker_spec(
    home,
    readouts=[
      {"name": "reward", "entrypoint": "liverepo:reward"},
      {"name": "solved", "entrypoint": "liverepo:solved"},
    ],
  )
  code = _run_worker(lambda _i: {"reward": 0.5}, home)
  assert code == 0
  values = read_instance_readouts(home)
  assert {(v.name, v.value) for v in values} == {
    ("reward", 0.5),
    ("solved", True),
  }


def test_worker_writes_values_before_the_envelope(
  monkeypatch, tmp_path: Path
):
  """The envelope is the completion signal, so the values have to be
  on disk already when it lands — otherwise the dispatcher reads the
  home, finds nothing, and the operator has to run the retroactive
  pass for a value the instance already knew."""
  home = tmp_path / "t1__0000001"
  order: list[str] = []
  real_replace = Path.replace

  def spy(self, target):  # type: ignore[no-untyped-def]
    order.append(Path(target).name)
    return real_replace(self, target)

  monkeypatch.setattr(Path, "replace", spy)
  _fake_module(monkeypatch, "ordrepo", reward=lambda _i: 1)
  _worker_spec(
    home, readouts=[{"name": "reward", "entrypoint": "ordrepo:reward"}]
  )
  _run_worker(lambda _i: {}, home)
  assert order == ["readouts.json", "outcome.json"]


def test_a_raising_readout_cannot_fail_the_work(
  monkeypatch, tmp_path: Path
):
  def boom(_inst):
    raise KeyError("tgc")

  _fake_module(monkeypatch, "boomrepo", reward=boom)
  home = tmp_path / "t1__0000001"
  _worker_spec(
    home, readouts=[{"name": "reward", "entrypoint": "boomrepo:reward"}]
  )
  code = _run_worker(lambda _i: {"fine": True}, home)
  assert code == 0  # the WORK succeeded
  envelope = json.loads((home / "outcome.json").read_text())
  assert envelope["ok"] is True
  assert envelope["data"] == {"fine": True}
  value = read_instance_readouts(home)[0]
  assert value.ok is False
  assert "KeyError" in value.error


def test_a_failed_run_is_still_scored(monkeypatch, tmp_path: Path):
  _fake_module(
    monkeypatch,
    "failrepo",
    solved=lambda inst: inst.state == "done_ok",
    kind=lambda inst: (inst.payload or {}).get("kind", "?"),
  )
  home = tmp_path / "t1__0000001"
  _worker_spec(
    home,
    readouts=[
      {"name": "solved", "entrypoint": "failrepo:solved"},
      {"name": "kind", "entrypoint": "failrepo:kind"},
    ],
    payload={"kind": "error"},
  )

  def work(_inst):
    raise ValueError("did not converge")

  assert _run_worker(work, home) == 1
  values = {v.name: v.value for v in read_instance_readouts(home)}
  assert values == {"solved": False, "kind": "error"}


def test_an_infra_failure_is_not_scored(monkeypatch, tmp_path: Path):
  """The task will be requeued, so scoring it would attach a value
  to an instance that never counted."""
  from dispatcher_sdk import InfraFailure

  _fake_module(monkeypatch, "infrarepo", solved=lambda _i: False)
  home = tmp_path / "t1__0000001"
  _worker_spec(
    home, readouts=[{"name": "solved", "entrypoint": "infrarepo:solved"}]
  )

  def work(_inst):
    raise InfraFailure("backend vanished")

  assert _run_worker(work, home) == 75
  assert read_instance_readouts(home) == []


def test_read_completion_picks_up_the_values(tmp_path: Path):
  home = tmp_path / "i1"
  home.mkdir()
  (home / "readouts.json").write_text(
    json.dumps([{"name": "reward", "instance_id": "i1", "value": 0.4}])
  )
  (home / "outcome.json").write_text(json.dumps({"ok": True, "data": 1}))
  snapshot = read_completion(home)
  assert snapshot is not None
  assert [v.value for v in snapshot.readouts] == [0.4]


def test_a_broken_values_file_does_not_change_classification(
  tmp_path: Path,
):
  home = tmp_path / "i1"
  home.mkdir()
  (home / "readouts.json").write_text("{ broken")
  (home / "outcome.json").write_text(json.dumps({"ok": True, "data": 1}))
  snapshot = read_completion(home)
  assert snapshot is not None
  assert snapshot.error_present is False
  assert snapshot.readouts == []


# ── the service ──────────────────────────────────────────────────


class FakeRunner:
  """Stands in for the retroactive container."""

  def __init__(self, *, value: Any = 1.0, fail: bool = False) -> None:
    self.requests: list[dict[str, Any]] = []
    self._value = value
    self._fail = fail

  async def __call__(self, state: JobState, timeout: float) -> str:
    request = json.loads(
      request_path(state.home_root).read_text(encoding="utf-8")
    )
    self.requests.append(request)
    if self._fail:
      raise RuntimeError("container would not start")
    lines = [
      sdk_readout.RESULT_PREFIX
      + json.dumps(
        {
          "name": name,
          "instance_id": row["instance_id"],
          "task_id": row["task_id"],
          "ok": True,
          "value": self._value,
        }
      )
      for row in request["instances"]
      for name in row["readouts"]
    ]
    return "\n".join(lines) + "\n"

  @property
  def batches(self) -> list[list[str]]:
    return [
      [i["instance_id"] for i in r["instances"]] for r in self.requests
    ]


def mk_world(
  tmp_path: Path,
  *,
  tasks: list[str],
  arena: str = "bench/v7",
  settings: ReadoutSettings | None = None,
  runner: FakeRunner | None = None,
) -> tuple[Scheduler, ReadoutService, FakeRunner, JobState]:
  scheduler = Scheduler(
    max_concurrent=len(tasks),
    hosts={"ml10": HostSettings(max_concurrent=len(tasks))},
    clock=clock_from(),
    id_gen=id_gen(),
  )
  job = mk_job(tmp_path / "home", tasks)
  job.arena = arena
  job.image_id = "sha256:abc"
  job.source_sha256 = "deadbeef"
  scheduler.submit(job)
  registry = mk_registry(tmp_path)
  registry.register(arena, spec("reward"))
  runner = runner or FakeRunner()
  service = ReadoutService(
    scheduler=scheduler,
    registry=registry,
    settings=settings or ReadoutSettings(),
    runner=runner,
  )
  return scheduler, service, runner, job


def finish(scheduler: Scheduler, job_id: str, task_id: str) -> str:
  scheduler.dispatch_one()
  scheduler.transition_instance(
    job_id=job_id,
    task_id=task_id,
    from_state="running",
    to_state="done_ok",
  )
  return scheduler.job_view(job_id).done_ok[task_id].instance_id


async def test_record_folds_a_workers_values_into_the_index(
  tmp_path: Path,
):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1"])
  instance_id = finish(scheduler, job.job_id, "t1")
  await service.record(
    job.job_id,
    "t1",
    instance_id,
    [ReadoutValue(name="reward", instance_id=instance_id, value=0.9)],
  )
  stored = read_values(job.home_root, "reward")[instance_id]
  assert stored.value == 0.9
  # Stamped with what computed it, so a number stays traceable even
  # if a name is ever reused.
  assert stored.source_sha256 == "deadbeef"
  assert stored.image_id == "sha256:abc"
  assert service.summary(job.job_id).lag == 0
  # The live path never starts a container.
  assert runner.requests == []


async def test_record_drops_names_the_arena_does_not_register(
  tmp_path: Path,
):
  """The worker is told what to run, but the registry decides what
  counts — a stale instance.json must not mint a new column."""
  scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1"])
  instance_id = finish(scheduler, job.job_id, "t1")
  await service.record(
    job.job_id,
    "t1",
    instance_id,
    [
      ReadoutValue(name="reward", instance_id=instance_id, value=1),
      ReadoutValue(name="retired", instance_id=instance_id, value=2),
    ],
  )
  assert read_values(job.home_root, "retired") == {}
  assert read_values(job.home_root, "reward") != {}


async def test_lag_names_what_the_live_path_missed(tmp_path: Path):
  scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1", "t2"])
  finish(scheduler, job.job_id, "t1")
  instance_id = finish(scheduler, job.job_id, "t2")
  await service.record(
    job.job_id,
    "t2",
    instance_id,
    [ReadoutValue(name="reward", instance_id=instance_id, value=1)],
  )
  assert service.summary(job.job_id).lag == 1
  assert service.jobs_with_lag([job.job_id]) == [job.job_id]


async def test_lag_is_unknown_until_values_are_loaded(tmp_path: Path):
  scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1"])
  finish(scheduler, job.job_id, "t1")
  assert service.summary(job.job_id).lag is None
  await service.load_live()
  assert service.summary(job.job_id).lag == 1


async def test_summary_reaches_the_job_row(tmp_path: Path):
  scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1"])
  instance_id = finish(scheduler, job.job_id, "t1")
  await service.record(
    job.job_id,
    "t1",
    instance_id,
    [ReadoutValue(name="reward", instance_id=instance_id, value=1.0)],
  )
  row = snapshot_job(scheduler, job.job_id, service.summary)
  assert row.readouts["reward"].mean == 1.0
  assert row.readout_lag == 0
  # Without the projection the row simply carries no columns.
  assert snapshot_job(scheduler, job.job_id).readouts == {}


async def test_specs_for_job_feeds_instance_json(tmp_path: Path):
  _scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1"])
  assert [s.name for s in service.specs_for_job(job.job_id)] == ["reward"]
  service.registry.unregister("bench/v7", "reward")
  assert service.specs_for_job(job.job_id) == []


# ── the retroactive pass ─────────────────────────────────────────


async def drain(service: ReadoutService, job_ids: list[str], **kw):
  return [r async for r in service.compute(job_ids, **kw)]


async def test_compute_fills_in_what_the_live_path_missed(
  tmp_path: Path,
):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1", "t2"])
  for task in ("t1", "t2"):
    finish(scheduler, job.job_id, task)
  reports = await drain(service, [job.job_id])
  assert len(reports) == 1
  assert (reports[0].written, reports[0].remaining) == (2, 0)
  assert len(read_values(job.home_root, "reward")) == 2
  assert service.summary(job.job_id).lag == 0


async def test_compute_batches_and_keeps_going(tmp_path: Path):
  scheduler, service, runner, job = mk_world(
    tmp_path,
    tasks=[f"t{i}" for i in range(7)],
    settings=ReadoutSettings(batch=3),
  )
  for i in range(7):
    finish(scheduler, job.job_id, f"t{i}")
  reports = await drain(service, [job.job_id])
  assert [len(b) for b in runner.batches] == [3, 3, 1]
  assert [r.remaining for r in reports] == [4, 1, 0]
  assert len(read_values(job.home_root, "reward")) == 7


async def test_compute_skips_values_already_present(tmp_path: Path):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1", "t2"])
  instance_id = finish(scheduler, job.job_id, "t1")
  finish(scheduler, job.job_id, "t2")
  await service.record(
    job.job_id,
    "t1",
    instance_id,
    [ReadoutValue(name="reward", instance_id=instance_id, value=5)],
  )
  reports = await drain(service, [job.job_id])
  assert runner.batches == [[s for s in runner.batches[0]]]
  assert instance_id not in runner.batches[0]
  assert reports[0].written == 1
  # And a second run has nothing to do at all.
  assert await drain(service, [job.job_id]) == []


async def test_compute_restricted_to_one_name(tmp_path: Path):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1"])
  service.registry.register("bench/v7", spec("tgc", "other:tgc"))
  finish(scheduler, job.job_id, "t1")
  await drain(service, [job.job_id], names=["tgc"])
  assert runner.requests[0]["instances"][0]["readouts"] == ["tgc"]
  assert read_values(job.home_root, "reward") == {}
  assert len(read_values(job.home_root, "tgc")) == 1


async def test_a_failed_pass_reports_and_stops(tmp_path: Path):
  scheduler, service, _runner, job = mk_world(
    tmp_path, tasks=["t1"], runner=FakeRunner(fail=True)
  )
  finish(scheduler, job.job_id, "t1")
  reports = await drain(service, [job.job_id])
  assert len(reports) == 1
  assert "RuntimeError" in reports[0].error
  assert read_values(job.home_root, "reward") == {}
  # Still visible as work to do, so a re-run picks it up.
  assert service.summary(job.job_id).lag == 1


async def test_a_pass_that_reports_nothing_does_not_spin(
  tmp_path: Path,
):
  class Silent(FakeRunner):
    async def __call__(self, state: JobState, timeout: float) -> str:
      await super().__call__(state, timeout)
      return "no result lines here\n"

  scheduler, service, runner, job = mk_world(
    tmp_path,
    tasks=[f"t{i}" for i in range(5)],
    settings=ReadoutSettings(batch=2),
    runner=Silent(),
  )
  for i in range(5):
    finish(scheduler, job.job_id, f"t{i}")
  reports = await drain(service, [job.job_id])
  assert len(reports) == 1
  assert reports[0].written == 0
  assert reports[0].unreported == 2


async def test_values_a_pass_did_not_ask_for_are_refused(
  tmp_path: Path,
):
  class Liar(FakeRunner):
    async def __call__(self, state: JobState, timeout: float) -> str:
      await super().__call__(state, timeout)
      return (
        sdk_readout.RESULT_PREFIX
        + json.dumps(
          {
            "name": "reward",
            "instance_id": "an-instance-of-another-job",
            "ok": True,
            "value": 99,
          }
        )
        + "\n"
      )

  scheduler, service, _runner, job = mk_world(
    tmp_path, tasks=["t1"], runner=Liar()
  )
  finish(scheduler, job.job_id, "t1")
  await drain(service, [job.job_id])
  assert read_values(job.home_root, "reward") == {}


async def test_an_unregistered_arena_has_nothing_to_compute(
  tmp_path: Path,
):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1"])
  service.registry.unregister("bench/v7", "reward")
  finish(scheduler, job.job_id, "t1")
  assert await drain(service, [job.job_id]) == []
  assert runner.requests == []


async def test_values_from_a_previous_generation_are_read_back(
  tmp_path: Path,
):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1"])
  instance_id = finish(scheduler, job.job_id, "t1")
  append_values(
    job.home_root,
    "reward",
    [ReadoutValue(name="reward", instance_id=instance_id, value=7)],
  )
  assert await drain(service, [job.job_id]) == []
  assert runner.requests == []
  assert service.summary(job.job_id).aggregates["reward"].mean == 7.0


# ── CLI ──────────────────────────────────────────────────────────


def test_cli_target_is_an_arena_unless_it_names_a_job():
  from dispatcher.tools.readout_cli import classify_target

  assert classify_target("bench/v7") == {"arena": "bench/v7"}
  assert classify_target("bench") == {"arena": "bench"}
  assert classify_target("job-2026-x") == {"job_id": "job-2026-x"}


# ── HTTP surface ─────────────────────────────────────────────────


def http_client(tmp_path: Path, *, value: Any = 0.75, live: bool = True):
  """A real server with a fake dispatch that behaves like a worker:
  it scores itself (writing readouts.json BEFORE outcome.json, as
  the SDK does) unless `live=False`, which simulates instances that
  finished before the readout existed."""
  from dispatcher.api.app import create_app
  from tests.test_server import api_client, mk_config, mk_settings

  runner = FakeRunner(value=value)

  async def fake_dispatch(action, state) -> None:
    home = state.home_root / action.instance_id
    home.mkdir(parents=True, exist_ok=True)
    specs = [
      s["name"]
      for s in json.loads((home / "instance.json").read_text())["readouts"]
    ]
    if live and specs:
      (home / "readouts.json").write_text(
        json.dumps(
          [
            {
              "name": name,
              "instance_id": action.instance_id,
              "task_id": action.task_id,
              "ok": True,
              "value": value,
            }
            for name in specs
          ]
        )
      )
    (home / "outcome.json").write_text(
      json.dumps({"ok": True, "data": {"reward": value}})
    )

  app = create_app(
    mk_config(tmp_path),
    settings=mk_settings(),
    dispatch=fake_dispatch,
    readout_runner=runner,
  )
  return api_client(app), runner


def register(client, arena: str, name: str = "reward"):
  return client.post(
    "/readouts",
    json={
      "arena": arena,
      "name": name,
      "entrypoint": f"myrepo.readouts:{name}",
    },
  )


def submit(client, tmp_path: Path, *, arena: str, tasks: list[str]):
  from tests.test_server import payload

  resp = client.post(
    "/jobs",
    json=payload(
      task_ids=tasks,
      home_root=tmp_path / "home",
      extra={"arena": arena},
    ),
  )
  assert resp.status_code == 200, resp.text
  return resp.json()["job_id"]


def settle(client, job_id: str, predicate) -> dict:
  row: dict[str, Any] = {}
  for _ in range(400):
    row = client.get(f"/jobs/{job_id}").json()
    if predicate(row):
      return row
  return row


def test_http_register_list_unregister(tmp_path: Path):
  client, _runner = http_client(tmp_path)
  with client:
    resp = register(client, "bench/v7")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["arena"] == "bench/v7"
    # Nothing has finished yet, so nothing needs the CLI.
    assert body["needs_backfill"] == []
    assert body["hint"] == ""

    listed = client.get("/readouts").json()["by_arena"]
    specs = listed["bench/v7"]["readouts"]
    assert [s["name"] for s in specs] == ["reward"]
    assert specs[0]["timeout_sec"] == 600.0
    assert listed["bench/v7"]["columns"] == ""

    clash = register(client, "bench")
    assert clash.status_code == 409

    bad = client.post(
      "/readouts",
      json={"arena": "bench/v7", "name": "r", "entrypoint": "nocolon"},
    )
    assert bad.status_code == 400

    gone = client.request(
      "DELETE", "/readouts", params={"arena": "bench/v7", "name": "reward"}
    )
    assert gone.status_code == 200
    assert client.get("/readouts").json()["by_arena"] == {}
    missing = client.request(
      "DELETE", "/readouts", params={"arena": "bench/v7", "name": "reward"}
    )
    assert missing.status_code == 404


def test_http_readout_requires_an_arena(tmp_path: Path):
  client, _runner = http_client(tmp_path)
  with client:
    resp = client.post(
      "/readouts", json={"arena": "", "name": "r", "entrypoint": "a:b"}
    )
    assert resp.status_code == 400
    assert "arena" in resp.json()["detail"]


def test_http_live_path_needs_no_container(tmp_path: Path):
  client, runner = http_client(tmp_path, value=0.75)
  with client:
    assert register(client, "bench/v7").status_code == 200
    job_id = submit(client, tmp_path, arena="bench/v7", tasks=["t1", "t2"])
    row = settle(
      client,
      job_id,
      lambda r: r["readouts"].get("reward", {}).get("n") == 2,
    )
    assert row["readouts"]["reward"]["mean"] == 0.75
    assert row["readout_lag"] == 0
    # The whole point: no retroactive container ever ran.
    assert runner.requests == []

    detail = client.get(f"/jobs/{job_id}/readouts").json()
    assert len(detail["values"]["reward"]) == 2
    assert detail["summary"]["lag"] == 0


def test_http_registering_late_points_at_the_command(tmp_path: Path):
  """Registration does not start containers. It reports what would
  need the retroactive pass and names the command."""
  client, runner = http_client(tmp_path, value=0.5, live=False)
  with client:
    job_id = submit(client, tmp_path, arena="bench/v7", tasks=["t1"])
    settle(client, job_id, lambda r: r["counts"]["done_ok"] == 1)
    body = register(client, "bench").json()
    assert body["needs_backfill"] == [job_id]
    assert body["hint"] == "dispatcher readout bench --name reward"
    assert runner.requests == []
    row = client.get(f"/jobs/{job_id}").json()
    assert row["readout_lag"] == 1


def test_http_compute_streams_one_line_per_pass(tmp_path: Path):
  client, runner = http_client(tmp_path, value=0.5, live=False)
  with client:
    job_id = submit(
      client, tmp_path, arena="bench/v7", tasks=["t1", "t2", "t3"]
    )
    settle(client, job_id, lambda r: r["counts"]["done_ok"] == 3)
    assert register(client, "bench/v7").status_code == 200

    with client.stream(
      "POST", "/readouts/compute", json={"arena": "bench/v7"}
    ) as resp:
      assert resp.status_code == 200
      reports = [
        json.loads(line) for line in resp.iter_lines() if line.strip()
      ]
    assert len(reports) == 1
    assert reports[0]["written"] == 3
    assert reports[0]["remaining"] == 0
    assert len(runner.requests) == 1

    row = client.get(f"/jobs/{job_id}").json()
    assert row["readouts"]["reward"]["mean"] == 0.5
    assert row["readout_lag"] == 0

    # Idempotent: a second run has nothing left to do.
    with client.stream(
      "POST", "/readouts/compute", json={"job_id": job_id}
    ) as resp:
      assert [ln for ln in resp.iter_lines() if ln.strip()] == []


def test_http_compute_rejects_an_unknown_target(tmp_path: Path):
  client, _runner = http_client(tmp_path)
  with client:
    resp = client.post("/readouts/compute", json={"arena": "nope"})
    assert resp.status_code == 404
    resp = client.post("/readouts/compute", json={})
    assert resp.status_code == 400


def test_http_an_unregistered_arena_carries_no_columns(tmp_path: Path):
  client, runner = http_client(tmp_path)
  with client:
    job_id = submit(client, tmp_path, arena="other", tasks=["t1"])
    row = settle(client, job_id, lambda r: r["counts"]["done_ok"] == 1)
    assert row["readouts"] == {}
    assert row["readout_lag"] == 0
    assert runner.requests == []


# ── operator columns: the resident process ───────────────────────


def fake_spawn(script: str):
  """Spawn a real python process (not docker) running the SDK
  aggregate loop against a module we write on the fly. Exercises the
  actual protocol — pipes, ndjson, one line in one line out."""
  import os
  import sys
  import tempfile

  tmp = tempfile.mkdtemp()
  pathlib.Path(tmp, "opcolumns.py").write_text(script, encoding="utf-8")

  async def spawn(state):
    env = dict(os.environ)
    env["PYTHONPATH"] = (
      tmp + os.pathsep + str(pathlib.Path("src").resolve())
    )
    return await asyncio.create_subprocess_exec(
      sys.executable,
      "-m",
      "dispatcher_sdk.aggregate",
      stdin=asyncio.subprocess.PIPE,
      stdout=asyncio.subprocess.PIPE,
      stderr=asyncio.subprocess.DEVNULL,
      env=env,
    )

  return spawn


MEDIAN_COLUMNS = """
def columns(job):
  xs = sorted(v for v in job.columns["reward"] if v is not None)
  return {
    "reward_median": xs[len(xs) // 2] if xs else None,
    "n_hosts": len(set(job.columns["host"])),
    "ok_rate": job.done_ok / max(1, job.done_ok + job.done_err),
    "label": "arbitrary strings are fine",
  }
"""


async def mk_columns_world(
  tmp_path: Path, *, script: str = MEDIAN_COLUMNS, tasks: int = 5
):
  from dispatcher.core.aggregate_pool import AggregatePool

  scheduler, service, runner, job = mk_world(
    tmp_path, tasks=[f"t{i}" for i in range(tasks)]
  )
  pool = AggregatePool(spawn=fake_spawn(script), request_timeout_sec=10.0)
  service._pool = pool
  service.registry.set_columns("bench/v7", "opcolumns:columns")
  for i in range(tasks):
    instance_id = finish(scheduler, job.job_id, f"t{i}")
    await service.record(
      job.job_id,
      f"t{i}",
      instance_id,
      [
        ReadoutValue(
          name="reward", instance_id=instance_id, value=0.1 * (i + 1)
        )
      ],
    )
  return scheduler, service, pool, job


async def test_operator_columns_reach_the_job_row(tmp_path: Path):
  scheduler, service, pool, job = await mk_columns_world(tmp_path)
  try:
    # Dirty until a read asks — nothing runs in the background.
    assert service.summary(job.job_id).columns == {}
    assert service.summary(job.job_id).columns_stale is True

    await service.refresh([job.job_id])
    cell = service.summary(job.job_id)
    assert cell.columns["reward_median"] == pytest.approx(0.3)
    assert cell.columns["n_hosts"] == 1
    assert cell.columns["ok_rate"] == 1.0
    assert cell.columns["label"] == "arbitrary strings are fine"
    assert cell.columns_stale is False
    assert cell.columns_error == ""
    # And the facts stay alongside the operator's numbers.
    assert cell.aggregates["reward"].n == 5

    row = snapshot_job(scheduler, job.job_id, service.summary)
    assert row.columns["reward_median"] == pytest.approx(0.3)
  finally:
    await pool.close()


async def test_one_process_serves_many_refreshes(tmp_path: Path):
  """Resident, not per-request: the whole point of the design."""
  scheduler, service, pool, job = await mk_columns_world(tmp_path)
  try:
    for _ in range(5):
      service.invalidate([job.job_id])
      await service.refresh([job.job_id])
    held = pool.snapshot()
    assert len(held) == 1
    assert held[0]["requests"] == 5
    assert held[0]["alive"] is True
  finally:
    await pool.close()


async def test_a_clean_job_costs_nothing(tmp_path: Path):
  scheduler, service, pool, job = await mk_columns_world(tmp_path)
  try:
    await service.refresh([job.job_id])
    before = pool.snapshot()[0]["requests"]
    await service.refresh([job.job_id])  # not dirty
    assert pool.snapshot()[0]["requests"] == before
  finally:
    await pool.close()


async def test_new_values_make_columns_stale_then_fresh(tmp_path: Path):
  scheduler, service, pool, job = await mk_columns_world(tmp_path, tasks=6)
  try:
    await service.refresh([job.job_id])
    assert service.summary(job.job_id).columns_stale is False
    # One more value: the single invalidation path is `_append`.
    instance_id = scheduler.job_view(job.job_id).done_ok["t0"].instance_id
    await service.record(
      job.job_id,
      "t0",
      instance_id,
      [ReadoutValue(name="reward", instance_id=instance_id, value=9.0)],
    )
    assert service.summary(job.job_id).columns_stale is True
    # Stale shows the LAST GOOD numbers rather than nothing.
    assert "reward_median" in service.summary(job.job_id).columns
    await service.refresh([job.job_id])
    assert service.summary(job.job_id).columns_stale is False
  finally:
    await pool.close()


async def test_a_raising_columns_function_keeps_the_last_good_numbers(
  tmp_path: Path,
):
  scheduler, service, pool, job = await mk_columns_world(tmp_path)
  try:
    await service.refresh([job.job_id])
    good = dict(service.summary(job.job_id).columns)
    assert good
    # Point at a function that raises; values unchanged.
    service.registry.set_columns("bench/v7", "opcolumns:boom")
    service.invalidate([job.job_id])
    await service.refresh([job.job_id])
    cell = service.summary(job.job_id)
    assert cell.columns == good  # never invented, never blanked
    assert cell.columns_stale is True  # and said so
    assert "AttributeError" in cell.columns_error or cell.columns_error
  finally:
    await pool.close()


async def test_a_wedged_columns_function_times_out(tmp_path: Path):
  from dispatcher.core.aggregate_pool import AggregatePool

  scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1"])
  pool = AggregatePool(
    spawn=fake_spawn("import time\ndef columns(job):\n  time.sleep(30)\n"),
    request_timeout_sec=0.4,
  )
  service._pool = pool
  service.registry.set_columns("bench/v7", "opcolumns:columns")
  instance_id = finish(scheduler, job.job_id, "t1")
  await service.record(
    job.job_id,
    "t1",
    instance_id,
    [ReadoutValue(name="reward", instance_id=instance_id, value=1.0)],
  )
  try:
    await service.refresh([job.job_id])
    cell = service.summary(job.job_id)
    assert cell.columns == {}
    assert "exceeded" in cell.columns_error
    assert cell.columns_stale is True
    # The wedged process was dropped, not reused — a late answer on
    # that pipe would desynchronise every later request.
    assert pool.snapshot() == []
  finally:
    await pool.close()


async def test_the_pool_is_capped_and_evicts_lru(tmp_path: Path):
  from dispatcher.core.aggregate_pool import AggregatePool

  pool = AggregatePool(
    spawn=fake_spawn(MEDIAN_COLUMNS),
    max_processes=2,
    request_timeout_sec=10.0,
  )
  scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1"])
  finish(scheduler, job.job_id, "t1")
  frame = {"columns": {"reward": [1.0], "host": ["ml10"]}, "done_ok": 1}
  try:
    for i in range(4):
      job.image_id = f"sha256:image{i}"
      await pool.compute(job, entrypoint="opcolumns:columns", frame=frame)
    held = pool.snapshot()
    assert len(held) == 2  # the cap is a real ceiling
    assert {h["image_id"] for h in held} == {
      "sha256:image2",
      "sha256:image3",
    }
  finally:
    await pool.close()


async def test_frame_carries_metadata_and_marks_errors(tmp_path: Path):
  scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1", "t2"])
  ok_id = finish(scheduler, job.job_id, "t1")
  err_id = finish(scheduler, job.job_id, "t2")
  await service.record(
    job.job_id,
    "t1",
    ok_id,
    [ReadoutValue(name="reward", instance_id=ok_id, value=0.5)],
  )
  await service.record(
    job.job_id,
    "t2",
    err_id,
    [
      ReadoutValue(
        name="reward", instance_id=err_id, ok=False, error="KeyError"
      )
    ],
  )
  frame = service._frame(
    job.job_id, service.registry.for_arena("bench/v7")
  )
  cols = frame["columns"]
  assert set(cols) >= {
    "instance_id",
    "task_id",
    "state",
    "host",
    "dispatched_at",
    "finished_at",
    "duration_s",
    "reward",
  }
  assert cols["host"] == ["ml10", "ml10"]
  # An errored readout is None in the frame — same as a null return —
  # and the distinction lives in `errors`.
  assert sorted(v for v in cols["reward"] if v is not None) == [0.5]
  assert frame["errors"] == {"reward": 1}
  assert (frame["done_ok"], frame["done_err"]) == (2, 0)


async def test_no_pool_means_no_columns(tmp_path: Path):
  """A dispatcher without the pool (tests, fake dispatch) simply
  serves no operator columns — never an error."""
  scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1"])
  service.registry.set_columns("bench/v7", "opcolumns:columns")
  instance_id = finish(scheduler, job.job_id, "t1")
  await service.record(
    job.job_id,
    "t1",
    instance_id,
    [ReadoutValue(name="reward", instance_id=instance_id, value=1.0)],
  )
  await service.refresh([job.job_id])
  assert service.summary(job.job_id).columns == {}


def test_registry_keeps_the_pre_columns_file_format(tmp_path: Path):
  """The registry on the running dispatcher predates `columns`; a
  migration that lost it would lose the only copy of that intent."""
  (tmp_path / "readouts.json").write_text(
    json.dumps(
      {"bench": [{"name": "reward", "entrypoint": "readouts:reward"}]}
    ),
    encoding="utf-8",
  )
  reg = ReadoutRegistry.load(tmp_path)
  assert [s.name for s in reg.for_arena("bench")] == ["reward"]
  assert reg.columns_entrypoint("bench") == ""
  reg.set_columns("bench", "readouts:columns")
  assert ReadoutRegistry.load(tmp_path).columns_entrypoint("bench/v7") == (
    "readouts:columns"
  )


def test_columns_entrypoint_takes_the_nearest_node(tmp_path: Path):
  reg = mk_registry(tmp_path)
  reg.set_columns("bench", "a:columns")
  reg.set_columns("bench/v7", "b:columns")
  assert reg.columns_entrypoint("bench/v7/front5") == "b:columns"
  assert reg.columns_entrypoint("bench/v8") == "a:columns"
  assert reg.columns_entrypoint("other") == ""
