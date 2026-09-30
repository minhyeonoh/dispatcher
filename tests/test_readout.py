"""Readouts: value files, registry, the coalescing executor, the
in-container runner, and the HTTP surface."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import pytest

from dispatcher.api.wire import snapshot_job
from dispatcher.core.models import HostSettings
from dispatcher.core.readout import (
  BadReadout,
  ReadoutSpec,
  ReadoutValue,
  aggregate,
  append_values,
  parse_result_lines,
  read_values,
  request_path,
  values_path,
)
from dispatcher.core.scheduler import Scheduler
from dispatcher.services.readouts import (
  ReadoutConflict,
  ReadoutRegistry,
  ReadoutService,
  ReadoutSettings,
  build_readout_argv,
)
from dispatcher_sdk import readout as sdk_readout
from tests.test_runtime import mk_job
from tests.test_scheduler import clock_from, id_gen

if TYPE_CHECKING:
  from pathlib import Path

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


# ── executor ─────────────────────────────────────────────────────


class FakeRunner:
  """Stands in for the container. Records every pass and answers
  with whatever the test wants for that batch."""

  def __init__(
    self,
    *,
    value: Any = 1.0,
    fail: bool = False,
    on_pass: Any = None,
  ) -> None:
    self.requests: list[dict[str, Any]] = []
    self._value = value
    self._fail = fail
    self._on_pass = on_pass

  async def __call__(self, state: JobState, timeout: float) -> str:
    request = json.loads(
      request_path(state.home_root).read_text(encoding="utf-8")
    )
    self.requests.append(request)
    if self._on_pass is not None:
      await self._on_pass(len(self.requests))
    if self._fail:
      raise RuntimeError("container would not start")
    lines = []
    for row in request["instances"]:
      for name in row["readouts"]:
        lines.append(
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
        )
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


def finish(scheduler: Scheduler, job_id: str, task_id: str) -> None:
  scheduler.dispatch_one()
  scheduler.transition_instance(
    job_id=job_id,
    task_id=task_id,
    from_state="running",
    to_state="done_ok",
  )


async def drain(service: ReadoutService) -> None:
  """Let every in-flight pass finish. A pass may queue another, so
  loop until no task is left."""
  for _ in range(100):
    tasks = list(service._tasks.values())
    if not tasks:
      return
    await asyncio.gather(*tasks, return_exceptions=True)
  raise AssertionError("readout passes did not settle")


async def until(predicate, what: str) -> None:
  """Wait for real work (the pass does thread hops) to reach a
  point, rather than guessing a sleep."""
  for _ in range(500):
    if predicate():
      return
    await asyncio.sleep(0.005)
  raise AssertionError(f"timed out waiting for {what}")


async def test_a_finished_instance_gets_a_value_with_no_delay(
  tmp_path: Path,
):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1"])
  finish(scheduler, job.job_id, "t1")
  service.request(job.job_id)
  await drain(service)
  values = read_values(job.home_root, "reward")
  assert [v.value for v in values.values()] == [1.0]
  # One pass, and it was started without waiting on any timer.
  assert len(runner.requests) == 1
  assert service.summary(job.job_id).lag == 0


async def test_a_burst_costs_two_passes_not_one_per_instance(
  tmp_path: Path,
):
  gate = asyncio.Event()

  async def hold(n: int) -> None:
    if n == 1:
      await gate.wait()

  runner = FakeRunner(on_pass=hold)
  scheduler, service, runner, job = mk_world(
    tmp_path, tasks=[f"t{i}" for i in range(6)], runner=runner
  )
  finish(scheduler, job.job_id, "t0")
  service.request(job.job_id)
  await until(lambda: bool(runner.requests), "the first pass to snapshot")
  for i in range(1, 6):
    finish(scheduler, job.job_id, f"t{i}")
    service.request(job.job_id)
  gate.set()
  await drain(service)
  # First pass saw only t0; the five that arrived while it computed
  # folded into exactly ONE more pass.
  assert len(runner.batches) == 2
  assert len(runner.batches[0]) == 1
  assert len(runner.batches[1]) == 5
  assert len(read_values(job.home_root, "reward")) == 6


async def test_a_request_while_waiting_for_a_slot_is_not_lost(
  tmp_path: Path,
):
  """The request lands before the pass takes its snapshot, so it
  needs no queueing — but it must not be dropped either."""
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1", "t2"])
  finish(scheduler, job.job_id, "t1")
  service.request(job.job_id)
  # Same tick, before the task has run at all.
  finish(scheduler, job.job_id, "t2")
  service.request(job.job_id)
  await drain(service)
  assert len(runner.requests) == 1
  assert len(read_values(job.home_root, "reward")) == 2


async def test_the_batch_cap_pipelines_instead_of_growing(
  tmp_path: Path,
):
  scheduler, service, runner, job = mk_world(
    tmp_path,
    tasks=[f"t{i}" for i in range(7)],
    settings=ReadoutSettings(batch_cap=3),
  )
  for i in range(7):
    finish(scheduler, job.job_id, f"t{i}")
  service.request(job.job_id)
  await drain(service)
  assert [len(b) for b in runner.batches] == [3, 3, 1]
  assert len(read_values(job.home_root, "reward")) == 7


async def test_nothing_unscored_starts_no_container(tmp_path: Path):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1"])
  # An `unknown` transition (or a duplicate request) must not pay
  # for a container just to discover there is nothing to do.
  service.request(job.job_id)
  await drain(service)
  assert runner.requests == []


async def test_a_job_with_no_registered_readout_is_untouched(
  tmp_path: Path,
):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1"])
  service.registry.unregister("bench/v7", "reward")
  finish(scheduler, job.job_id, "t1")
  service.request(job.job_id)
  await drain(service)
  assert runner.requests == []
  assert service.summary(job.job_id).lag == 0


async def test_a_container_failure_leaves_the_pair_unscored(
  tmp_path: Path,
):
  scheduler, service, runner, job = mk_world(
    tmp_path, tasks=["t1"], runner=FakeRunner(fail=True)
  )
  finish(scheduler, job.job_id, "t1")
  service.request(job.job_id)
  await drain(service)
  assert read_values(job.home_root, "reward") == {}
  assert service.summary(job.job_id).lag == 1
  # The sweep is the retry path, and it sees the lag.
  assert service.sweep_live() == 1


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
            "instance_id": "some-other-job-instance",
            "ok": True,
            "value": 99,
          }
        )
        + "\n"
      )

  scheduler, service, runner, job = mk_world(
    tmp_path, tasks=["t1"], runner=Liar()
  )
  finish(scheduler, job.job_id, "t1")
  service.request(job.job_id)
  await drain(service)
  assert read_values(job.home_root, "reward") == {}


async def test_already_computed_values_are_not_recomputed(
  tmp_path: Path,
):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1"])
  finish(scheduler, job.job_id, "t1")
  service.request(job.job_id)
  await drain(service)
  service.request(job.job_id)
  await drain(service)
  assert len(runner.requests) == 1


async def test_values_from_a_previous_generation_are_read_back(
  tmp_path: Path,
):
  scheduler, service, runner, job = mk_world(tmp_path, tasks=["t1"])
  finish(scheduler, job.job_id, "t1")
  instance_id = scheduler.job_view(job.job_id).done_ok["t1"].instance_id
  append_values(
    job.home_root,
    "reward",
    [ReadoutValue(name="reward", instance_id=instance_id, value=7)],
  )
  service.request(job.job_id)
  await drain(service)
  assert runner.requests == []
  assert service.summary(job.job_id).aggregates["reward"].mean == 7.0


async def test_lag_is_unknown_until_values_are_loaded(tmp_path: Path):
  scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1"])
  finish(scheduler, job.job_id, "t1")
  assert service.summary(job.job_id).lag is None
  await service.load_live()
  assert service.summary(job.job_id).lag == 1


async def test_summary_reaches_the_job_row(tmp_path: Path):
  scheduler, service, _runner, job = mk_world(tmp_path, tasks=["t1"])
  finish(scheduler, job.job_id, "t1")
  service.request(job.job_id)
  await drain(service)
  row = snapshot_job(scheduler, job.job_id, service.summary)
  assert row.readouts["reward"].mean == 1.0
  assert row.readout_lag == 0
  # Without the projection the row simply carries no columns.
  assert snapshot_job(scheduler, job.job_id).readouts == {}


async def test_disabled_readouts_start_nothing(tmp_path: Path):
  scheduler, service, runner, job = mk_world(
    tmp_path, tasks=["t1"], settings=ReadoutSettings(enabled=False)
  )
  finish(scheduler, job.job_id, "t1")
  service.request(job.job_id)
  await drain(service)
  assert runner.requests == []


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


# ── HTTP surface ─────────────────────────────────────────────────


def http_client(tmp_path: Path, *, value: Any = 0.75):
  """A real server with fake dispatch AND a fake readout
  container, so the completion → request → value path runs end to
  end without docker."""
  from dispatcher.api.app import create_app
  from tests.test_server import api_client, mk_config, mk_settings

  async def fake_dispatch(action, state) -> None:
    home = state.home_root / action.instance_id
    home.mkdir(parents=True, exist_ok=True)
    (home / "outcome.json").write_text(
      json.dumps({"ok": True, "data": {"reward": value}}),
      encoding="utf-8",
    )

  runner = FakeRunner(value=value)
  app = create_app(
    mk_config(tmp_path),
    settings=mk_settings(),
    dispatch=fake_dispatch,
    readout_runner=runner,
  )
  return api_client(app), runner


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


def test_http_register_list_unregister(tmp_path: Path):
  client, _runner = http_client(tmp_path)
  with client:
    resp = client.post(
      "/readouts",
      json={
        "arena": "bench/v7",
        "name": "reward",
        "entrypoint": "myrepo.readouts:reward",
      },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["arena"] == "bench/v7"

    listed = client.get("/readouts").json()["by_arena"]
    assert [s["name"] for s in listed["bench/v7"]] == ["reward"]

    # Same name on the same path is a 409, not a silent overwrite.
    clash = client.post(
      "/readouts",
      json={
        "arena": "bench",
        "name": "reward",
        "entrypoint": "other:reward",
      },
    )
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


def test_http_values_appear_in_the_job_row_and_endpoint(
  tmp_path: Path,
):
  client, runner = http_client(tmp_path, value=0.75)
  with client:
    assert (
      client.post(
        "/readouts",
        json={
          "arena": "bench/v7",
          "name": "reward",
          "entrypoint": "myrepo.readouts:reward",
        },
      ).status_code
      == 200
    )
    job_id = submit(client, tmp_path, arena="bench/v7", tasks=["t1", "t2"])
    row: dict[str, Any] = {}
    for _ in range(300):
      row = client.get(f"/jobs/{job_id}").json()
      if row["readouts"].get("reward", {}).get("n") == 2:
        break
    assert row["readouts"]["reward"]["mean"] == 0.75
    assert row["readout_lag"] == 0

    detail = client.get(f"/jobs/{job_id}/readouts").json()
    assert len(detail["values"]["reward"]) == 2
    assert {v["value"] for v in detail["values"]["reward"]} == {0.75}
    assert detail["summary"]["lag"] == 0
    # The whole burst rode few passes, not one per instance.
    assert 1 <= len(runner.requests) <= 2


def test_http_registration_backfills_what_already_finished(
  tmp_path: Path,
):
  client, _runner = http_client(tmp_path, value=0.5)
  with client:
    job_id = submit(client, tmp_path, arena="bench/v7", tasks=["t1"])
    for _ in range(300):
      if client.get(f"/jobs/{job_id}").json()["counts"]["done_ok"] == 1:
        break
    resp = client.post(
      "/readouts",
      json={
        "arena": "bench",
        "name": "reward",
        "entrypoint": "myrepo.readouts:reward",
      },
    )
    assert resp.status_code == 200
    assert resp.json()["backfilling"] == [job_id]
    row: dict[str, Any] = {}
    for _ in range(300):
      row = client.get(f"/jobs/{job_id}").json()
      if row["readouts"].get("reward", {}).get("n") == 1:
        break
    assert row["readouts"]["reward"]["mean"] == 0.5


def test_http_an_unregistered_arena_carries_no_columns(tmp_path: Path):
  client, runner = http_client(tmp_path)
  with client:
    job_id = submit(client, tmp_path, arena="other", tasks=["t1"])
    for _ in range(300):
      if client.get(f"/jobs/{job_id}").json()["counts"]["done_ok"] == 1:
        break
    row = client.get(f"/jobs/{job_id}").json()
    assert row["readouts"] == {}
    assert row["readout_lag"] == 0
    assert runner.requests == []
