"""docker-run command construction."""

from __future__ import annotations

import shlex
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from dispatcher.core import labels
from dispatcher.core.dispatch import (
  build_argv,
  build_remote_command,
)
from dispatcher.core.models import DispatchEntry
from tests.test_runtime import mk_attempt

if TYPE_CHECKING:
  from pathlib import Path


def _action(host: str = "ml9") -> DispatchEntry:
  return DispatchEntry(
    attempt_id="att-001",
    task_id="t1",
    trial_id="t1__0000001",
    host=host,
    dispatched_at=datetime.now(UTC),
  )


def test_remote_command_shape(tmp_path: Path):
  state = mk_attempt(tmp_path, ["t1"], payloads={"t1": 1})
  state.container.command = ["python", "-m", "worker"]
  state.container.env = {"A": "1"}
  state.env = {"SECRET": "x y"}  # needs quoting
  state.container.mounts = ["/nfs/data:/data:ro"]
  state.container.extra_args = ["--gpus", "all"]
  cmd = build_remote_command(
    _action(), state, trial_home=tmp_path / "t1__0000001"
  )
  parts = shlex.split(cmd.split("&&", 1)[1])
  # mkdir -p precedes docker run (remote negative-dentry guard).
  assert cmd.startswith(f"mkdir -p {tmp_path}/t1__0000001 && ")
  assert parts[:3] == ["docker", "run", "-d"]
  # Labels: managed/trial/set/attempt.
  for kv in (
    f"{labels.MANAGED}=1",
    f"{labels.TRIAL}=t1__0000001",
    f"{labels.SET}=t1__0000001",
    f"{labels.ATTEMPT}=att-001",
  ):
    assert kv in parts
  # Home mount + extra mounts.
  assert f"{tmp_path}/t1__0000001:/dispatcher/home" in parts
  assert "/nfs/data:/data:ro" in parts
  # Env: spec + attempt + injected identity.
  assert "A=1" in parts
  assert "SECRET=x y" in parts
  assert "DISPATCHER_TRIAL=t1__0000001" in parts
  assert "DISPATCHER_TASK=t1" in parts
  assert "DISPATCHER_ATTEMPT=att-001" in parts
  assert "DISPATCHER_HOME=/dispatcher/home" in parts
  assert f"DISPATCHER_SET_LABEL={labels.SET}=t1__0000001" in parts
  # extra_args before image; command after image.
  i_img = parts.index("img")
  assert parts.index("--gpus") < i_img
  assert parts[i_img + 1 :] == ["python", "-m", "worker"]


def test_attempt_env_overrides_container_env(tmp_path: Path):
  state = mk_attempt(tmp_path, ["t1"])
  state.container.env = {"K": "spec"}
  state.env = {"K": "attempt"}
  cmd = build_remote_command(_action(), state, trial_home=tmp_path / "t")
  assert "K=attempt" in cmd
  assert "K=spec" not in cmd


def test_argv_local_uses_bash(tmp_path: Path):
  state = mk_attempt(tmp_path, ["t1"])
  argv = build_argv(
    _action(host="ml10"),
    state,
    trial_home=tmp_path / "t",
    self_host="ml10",
  )
  assert argv[0] == "bash"


def test_argv_remote_uses_ssh(tmp_path: Path):
  state = mk_attempt(tmp_path, ["t1"])
  argv = build_argv(
    _action(host="ml9"),
    state,
    trial_home=tmp_path / "t",
    self_host="ml10",
  )
  assert argv[0] == "ssh"
  assert "ml9" in argv
  assert "docker" in argv[-1]


def test_trial_id_case_preserved_in_labels(tmp_path: Path):
  state = mk_attempt(tmp_path, ["T"])
  action = DispatchEntry(
    attempt_id="att-001",
    task_id="T",
    trial_id="trial_T2019__0000001",
    host="ml9",
    dispatched_at=datetime.now(UTC),
  )
  cmd = build_remote_command(action, state, trial_home=tmp_path / "t")
  assert f"{labels.TRIAL}=trial_T2019__0000001" in cmd
  assert "trial_t2019" not in cmd
