"""docker-run command construction."""

from __future__ import annotations

import asyncio
import os
import shlex
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from dispatcher.core import labels
from dispatcher.core.dispatch import (
  DispatchError,
  build_argv,
  build_remote_command,
  docker_dispatch,
)
from dispatcher.core.hosts import run_argv
from dispatcher.core.models import DispatchEntry
from tests.test_runtime import mk_job

if TYPE_CHECKING:
  from pathlib import Path


def _action(host: str = "ml9") -> DispatchEntry:
  return DispatchEntry(
    job_id="job-001",
    task_id="t1",
    instance_id="t1__0000001",
    host=host,
    dispatched_at=datetime.now(UTC),
  )


def test_remote_command_shape(tmp_path: Path):
  state = mk_job(tmp_path, ["t1"], payloads={"t1": 1})
  state.container.command = ["python", "-m", "worker"]
  state.container.env = {"A": "1"}
  state.env = {"SECRET": "x y"}  # needs quoting
  state.container.mounts = ["/nfs/data:/data:ro"]
  state.container.extra_args = ["--gpus", "all"]
  cmd = build_remote_command(
    _action(), state, instance_home=tmp_path / "t1__0000001"
  )
  parts = shlex.split(cmd.split("&&", 1)[1])
  # mkdir -p precedes docker run (remote negative-dentry guard).
  assert cmd.startswith(f"mkdir -p {tmp_path}/t1__0000001 && ")
  assert parts[:3] == ["docker", "run", "-d"]
  # Labels: managed/instance/set/job.
  for kv in (
    f"{labels.MANAGED}=1",
    f"{labels.INSTANCE}=t1__0000001",
    f"{labels.SET}=t1__0000001",
    f"{labels.JOB}=job-001",
  ):
    assert kv in parts
  # Home mount + extra mounts.
  assert f"{tmp_path}/t1__0000001:/dispatcher/home" in parts
  assert "/nfs/data:/data:ro" in parts
  # Env: spec + job + injected identity.
  assert "A=1" in parts
  assert "SECRET=x y" in parts
  assert "DISPATCHER_INSTANCE=t1__0000001" in parts
  assert "DISPATCHER_TASK=t1" in parts
  assert "DISPATCHER_JOB=job-001" in parts
  assert "DISPATCHER_HOME=/dispatcher/home" in parts
  assert f"DISPATCHER_SET_LABEL={labels.SET}=t1__0000001" in parts
  # extra_args before image; command after image.
  i_img = parts.index("img")
  assert parts.index("--gpus") < i_img
  assert parts[i_img + 1 :] == ["python", "-m", "worker"]


def test_job_env_overrides_container_env(tmp_path: Path):
  state = mk_job(tmp_path, ["t1"])
  state.container.env = {"K": "spec"}
  state.env = {"K": "job"}
  cmd = build_remote_command(
    _action(), state, instance_home=tmp_path / "t"
  )
  assert "K=job" in cmd
  assert "K=spec" not in cmd


def test_argv_local_uses_bash(tmp_path: Path):
  state = mk_job(tmp_path, ["t1"])
  argv = build_argv(
    _action(host="ml10"),
    state,
    instance_home=tmp_path / "t",
    self_host="ml10",
  )
  assert argv[0] == "bash"


def test_argv_remote_uses_ssh(tmp_path: Path):
  state = mk_job(tmp_path, ["t1"])
  argv = build_argv(
    _action(host="ml9"),
    state,
    instance_home=tmp_path / "t",
    self_host="ml10",
  )
  assert argv[0] == "ssh"
  assert "ml9" in argv
  assert "docker" in argv[-1]


def test_instance_id_case_preserved_in_labels(tmp_path: Path):
  state = mk_job(tmp_path, ["T"])
  action = DispatchEntry(
    job_id="job-001",
    task_id="T",
    instance_id="instance_T2019__0000001",
    host="ml9",
    dispatched_at=datetime.now(UTC),
  )
  cmd = build_remote_command(action, state, instance_home=tmp_path / "t")
  assert f"{labels.INSTANCE}=instance_T2019__0000001" in cmd
  assert "instance_t2019" not in cmd


# ── deadlines ────────────────────────────────────────────────────


def _alive(pid: int) -> bool:
  """Whether a pid still exists.

  Asked by signal 0 rather than by `pgrep -f <marker>`: a marker string
  matches the command line of whatever shell wrote this test file too,
  which is how the first version of these tests reported a survivor
  that was never the child."""
  try:
    os.kill(pid, 0)
  except ProcessLookupError:
    return False
  except PermissionError:
    return True
  return True


async def test_run_argv_returns_output_and_exit_code():
  done = await run_argv(["bash", "-c", "echo out; echo err >&2; exit 3"])
  assert done.returncode == 3
  assert done.stdout.strip() == "out"
  assert done.stderr.strip() == "err"


async def test_run_argv_times_out_and_kills_the_child(tmp_path: Path):
  # A real hang, not a mock: the deadline has to fire AND leave nothing
  # running, since a leaked ssh holds its remote command alive too.
  pidfile = tmp_path / "pid"
  started = time.monotonic()
  with pytest.raises(TimeoutError):
    await run_argv(
      ["bash", "-c", f"echo $$ > {pidfile}; sleep 300"], timeout=0.3
    )
  assert time.monotonic() - started < 10
  await asyncio.sleep(0.2)
  assert not _alive(int(pidfile.read_text()))


async def test_run_argv_kills_the_whole_group(tmp_path: Path):
  # `start_new_session` puts the child in its own group and the timeout
  # path kills the GROUP, so a grandchild — the shape `ssh` has, with a
  # remote command behind it — cannot outlive the client.
  pidfile = tmp_path / "pid"
  with pytest.raises(TimeoutError):
    await run_argv(
      ["bash", "-c", f"sleep 300 & echo $! > {pidfile}; wait"],
      timeout=0.3,
    )
  await asyncio.sleep(0.2)
  grandchild = int(pidfile.read_text())
  assert not _alive(grandchild)


async def test_a_hanging_dispatch_raises_DispatchError(
  monkeypatch, tmp_path: Path
):
  """The whole reason for the deadline.

  Dispatch walks instances one at a time, so a `docker run` that never
  returns used to stop dispatch for the entire cluster — with no crash
  for `supervised` to restart and no log line. It has to surface as a
  DispatchError, which is the path a failed dispatch already has."""
  job = mk_job(tmp_path / "home", ["t1"])
  monkeypatch.setattr(
    "dispatcher.core.dispatch.build_argv",
    lambda *a, **k: ["bash", "-c", "sleep 300"],
  )
  # Patching the module global is only effective because
  # `docker_dispatch` PASSES it rather than relying on `run_argv`'s own
  # default — the first version of this test waited the real 30s and
  # still passed, which is the shape of a test that proves nothing.
  monkeypatch.setattr("dispatcher.core.dispatch.DEFAULT_TIMEOUT", 0.3)
  started = time.monotonic()
  with pytest.raises(DispatchError, match="timed out after 0.3s"):
    await docker_dispatch(
      _action(),
      job,
      instance_home=tmp_path / "home" / "i",
      self_host="ml10",
    )
  assert time.monotonic() - started < 10
