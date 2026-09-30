"""Trial dispatch: the dispatcher itself starts each trial's main
container with `docker run -d` on the chosen host.

The daemon owns the container from that moment — it survives ssh
close, dispatcher crash, and network partition. Worker code inside
may start sibling containers; the contract is that every sibling
carries the trial's SET label (`labels.set_label`), or cleanup
cannot see it.
"""

from __future__ import annotations

import asyncio
import shlex
from typing import TYPE_CHECKING

from dispatcher.core import labels
from dispatcher.core.hosts import SSH_OPTS

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.models import AttemptState, DispatchEntry


class DispatchError(RuntimeError):
  """The docker-run invocation itself failed (unreachable host,
  missing image, bad spec) — the trial never started. Distinct
  from trial failure, which arrives via the outcome envelope."""


# Env the dispatcher injects into every main container. The worker
# runtime (dispatcher_sdk) reads these; research code should too
# instead of hardcoding paths.
ENV_TRIAL = "DISPATCHER_TRIAL"
ENV_TASK = "DISPATCHER_TASK"
ENV_ATTEMPT = "DISPATCHER_ATTEMPT"
ENV_HOME = "DISPATCHER_HOME"
ENV_SET_LABEL = "DISPATCHER_SET_LABEL"
ENV_SOURCE = "DISPATCHER_SOURCE"

SOURCE_TAR_FILENAME = ".source.tar"
SOURCE_MOUNT = "/dispatcher/source.tar"


def build_remote_command(
  action: DispatchEntry,
  state: AttemptState,
  *,
  trial_home: Path,
) -> str:
  """The shell command executed on the dispatch host.

  `mkdir -p` runs on the REMOTE host before `docker run`: the
  home dir was created launcher-side over NFS, and a remote
  negative-dentry cache could otherwise let docker create the
  bind-mount source itself — as root, on NFS. The remote mkdir is
  idempotent and invalidates that client's cache."""
  spec = state.container
  parts: list[str] = ["docker", "run", "-d"]
  for k, v in labels.main_labels(
    state.attempt_id, action.trial_id
  ).items():
    parts += ["--label", f"{k}={v}"]
  parts += ["-v", f"{trial_home}:{spec.home_mount}"]
  # Frozen source archive (attempt-level, ro). The SDK bootstrap
  # untars it to container-local fs — one sequential NFS read per
  # trial instead of per-file import traffic.
  if state.source_sha256:
    source_path = state.home_root / SOURCE_TAR_FILENAME
    parts += ["-v", f"{source_path}:{SOURCE_MOUNT}:ro"]
  for mount in spec.mounts:
    parts += ["-v", mount]
  env = {
    **spec.env,
    **state.env,
    ENV_TRIAL: action.trial_id,
    ENV_TASK: action.task_name,
    ENV_ATTEMPT: state.attempt_id,
    ENV_HOME: spec.home_mount,
    ENV_SET_LABEL: labels.set_label(action.trial_id),
    **({ENV_SOURCE: SOURCE_MOUNT} if state.source_sha256 else {}),
  }
  for k, v in env.items():
    parts += ["-e", f"{k}={v}"]
  parts += spec.extra_args
  # Pinned ID when submit resolved one — a tag re-pushed
  # mid-sweep can't change what runs.
  parts.append(state.image_id or spec.image)
  parts += spec.command
  quoted = " ".join(shlex.quote(p) for p in parts)
  return f"mkdir -p {shlex.quote(str(trial_home))} && {quoted}"


def build_argv(
  action: DispatchEntry,
  state: AttemptState,
  *,
  trial_home: Path,
  self_host: str,
) -> list[str]:
  remote_cmd = build_remote_command(action, state, trial_home=trial_home)
  if action.host == self_host:
    return ["bash", "-c", remote_cmd]
  return ["ssh", *SSH_OPTS, action.host, remote_cmd]


async def docker_dispatch(
  action: DispatchEntry,
  state: AttemptState,
  *,
  trial_home: Path,
  self_host: str,
) -> None:
  """Fire the docker run and wait only for its (fast) return —
  the daemon holds the container afterwards.

  `start_new_session=True`: without it, self-host dispatch runs
  in the dispatcher's process group and a tmux C-c on the
  dispatcher pane forwards SIGINT into an in-flight docker/ssh
  client."""
  argv = build_argv(
    action, state, trial_home=trial_home, self_host=self_host
  )
  proc = await asyncio.create_subprocess_exec(
    *argv,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
    start_new_session=True,
  )
  stdout, stderr = await proc.communicate()
  if proc.returncode != 0:
    raise DispatchError(
      f"dispatch to {action.host!r} failed "
      f"(exit {proc.returncode}): "
      f"stdout={stdout.decode(errors='replace')!r} "
      f"stderr={stderr.decode(errors='replace')!r}"
    )
