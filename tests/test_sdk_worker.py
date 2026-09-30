"""dispatcher_sdk worker lifecycle: spec load, outcome write,
exit codes — and that the dispatcher reads back what the SDK
writes (round-trip)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from dispatcher_sdk import (
  EX_INFRA,
  InfraFailure,
  Result,
  load_instance,
  run,
)

if TYPE_CHECKING:
  from pathlib import Path


def _spec(home: Path, payload=None) -> dict:
  home.mkdir(parents=True, exist_ok=True)
  spec = {
    "job_id": "job-1",
    "task_id": "t1",
    "instance_id": "t1__0000001",
    "home": str(home),
    "payload": payload,
  }
  (home / "instance.json").write_text(json.dumps(spec))
  return spec


def _env(home: Path) -> dict[str, str]:
  return {
    "DISPATCHER_HOME": str(home),
    "DISPATCHER_JOB": "job-1",
    "DISPATCHER_TASK": "t1",
    "DISPATCHER_INSTANCE": "t1__0000001",
    "DISPATCHER_SET_LABEL": "dispatcher.set=t1__0000001",
  }


class _Exit(Exception):
  def __init__(self, code: int) -> None:
    self.code = code


def _run(work, home: Path) -> tuple[int, dict | None]:
  codes: list[int] = []

  def fake_exit(code: int) -> None:
    codes.append(code)

  run(work, env=_env(home), _exit=fake_exit)
  outcome_path = home / "outcome.json"
  outcome = (
    json.loads(outcome_path.read_text()) if outcome_path.exists() else None
  )
  return codes[0], outcome


def test_load_instance_reads_spec_and_env(tmp_path: Path):
  _spec(tmp_path, payload={"n": 3})
  instance = load_instance(env=_env(tmp_path))
  assert instance.task == "t1"
  assert instance.instance == "t1__0000001"
  assert instance.job == "job-1"
  assert instance.payload == {"n": 3}
  assert instance.home == tmp_path
  assert instance.set_label == "dispatcher.set=t1__0000001"


def test_load_instance_missing_spec_raises_infra(tmp_path: Path):
  with pytest.raises(InfraFailure):
    load_instance(env=_env(tmp_path), timeout_s=0.1)


def test_ok_result_roundtrips_through_dispatcher_reader(
  tmp_path: Path,
):
  from dispatcher.core.outcome import read_completion

  _spec(tmp_path)
  code, outcome = _run(
    lambda t: Result(values={"reward": 0.5}, data={"answer": 42}),
    tmp_path,
  )
  assert code == 0
  assert outcome is not None and outcome["ok"] is True
  snap = read_completion(tmp_path)
  assert snap is not None
  assert snap.error_present is False
  assert snap.values == {"reward": 0.5}
  assert snap.outcome is not None
  assert snap.outcome.data == {"answer": 42}


def test_plain_return_becomes_data(tmp_path: Path):
  _spec(tmp_path)
  code, outcome = _run(lambda t: [1, 2, 3], tmp_path)
  assert code == 0
  assert outcome is not None
  assert outcome["data"] == [1, 2, 3]
  assert outcome["values"] == {}


def test_exception_writes_error_and_exits_1(tmp_path: Path):
  from dispatcher.core.outcome import read_completion

  _spec(tmp_path)

  def boom(t):
    raise ValueError("bad input")

  code, outcome = _run(boom, tmp_path)
  assert code == 1
  assert outcome is not None
  assert outcome["ok"] is False
  assert outcome["error"]["type"] == "ValueError"
  assert "bad input" in outcome["error"]["message"]
  snap = read_completion(tmp_path)
  assert snap is not None
  assert snap.error_present is True
  assert snap.infra is False


def test_infra_failure_writes_infra_and_exits_75(tmp_path: Path):
  from dispatcher.core.outcome import read_completion

  _spec(tmp_path)

  def swap(t):
    raise InfraFailure("backend swapped")

  code, outcome = _run(swap, tmp_path)
  assert code == EX_INFRA
  assert outcome is not None
  assert outcome["infra"] is True
  snap = read_completion(tmp_path)
  assert snap is not None
  assert snap.infra is True  # the dispatcher will requeue


def test_missing_spec_exits_75_without_outcome(tmp_path: Path):
  codes: list[int] = []
  run(
    lambda t: None,
    env={"DISPATCHER_HOME": str(tmp_path / "nope")},
    spec_timeout_s=0.1,
    _exit=codes.append,
  )
  assert codes == [EX_INFRA]


def test_outcome_write_is_atomic(tmp_path: Path):
  # The envelope appears only via rename — no .tmp leftovers, no
  # partial file for the dispatcher to mis-park on.
  _spec(tmp_path)
  code, _ = _run(lambda t: None, tmp_path)
  assert code == 0
  leftovers = [p.name for p in tmp_path.iterdir() if p.suffix == ".tmp"]
  assert leftovers == []
