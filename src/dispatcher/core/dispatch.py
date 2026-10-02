"""Instance dispatch: the dispatcher itself starts each instance's main
container with `docker run -d` on the chosen host.

The daemon owns the container from that moment — it survives ssh
close, dispatcher crash, and network partition. Worker code inside
may start sibling containers; the contract is that every sibling
carries the instance's SET label (`labels.set_label`), or cleanup
cannot see it.
"""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

from dispatcher.core import labels
from dispatcher.core.hosts import (
  DEFAULT_TIMEOUT,
  SSH_OPTS,
  run_argv,
)

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.models import DispatchEntry, JobState


class DispatchError(RuntimeError):
  """The docker-run invocation itself failed (unreachable host,
  missing image, bad spec) — the instance never started. Distinct
  from instance failure, which arrives via the outcome envelope."""


# Env the dispatcher injects into every main container. The worker
# runtime (dispatcher_sdk) reads these; research code should too
# instead of hardcoding paths.
ENV_INSTANCE = "DISPATCHER_INSTANCE"
ENV_TASK = "DISPATCHER_TASK"
ENV_JOB = "DISPATCHER_JOB"
ENV_HOME = "DISPATCHER_HOME"
ENV_SET_LABEL = "DISPATCHER_SET_LABEL"
ENV_SOURCE = "DISPATCHER_SOURCE"

SOURCE_TAR_FILENAME = ".source.tar"
SOURCE_MOUNT = "/dispatcher/source.tar"


def build_remote_command(
  action: DispatchEntry,
  state: JobState,
  *,
  instance_home: Path,
) -> str:
  """The shell command executed on the dispatch host.

  `mkdir -p` runs on the REMOTE host before `docker run`: the
  home dir was created launcher-side over NFS, and a remote
  negative-dentry cache could otherwise let docker create the
  bind-mount source itself — as root, on NFS. The remote mkdir is
  idempotent and invalidates that client's cache."""
  spec = state.container
  parts: list[str] = ["docker", "run", "-d"]
  for k, v in labels.main_labels(state.job_id, action.instance_id).items():
    parts += ["--label", f"{k}={v}"]
  parts += ["-v", f"{instance_home}:{spec.home_mount}"]
  # Frozen source archive (job-level, ro). The SDK bootstrap
  # untars it to container-local fs — one sequential NFS read per
  # instance instead of per-file import traffic.
  if state.source_sha256:
    source_path = state.home_root / SOURCE_TAR_FILENAME
    parts += ["-v", f"{source_path}:{SOURCE_MOUNT}:ro"]
  for mount in spec.mounts:
    parts += ["-v", mount]
  env = {
    **spec.env,
    **state.env,
    ENV_INSTANCE: action.instance_id,
    ENV_TASK: action.task_id,
    ENV_JOB: state.job_id,
    ENV_HOME: spec.home_mount,
    ENV_SET_LABEL: labels.set_label(action.instance_id),
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
  return f"mkdir -p {shlex.quote(str(instance_home))} && {quoted}"


def build_argv(
  action: DispatchEntry,
  state: JobState,
  *,
  instance_home: Path,
  self_host: str,
) -> list[str]:
  remote_cmd = build_remote_command(
    action, state, instance_home=instance_home
  )
  if action.host == self_host:
    return ["bash", "-c", remote_cmd]
  return ["ssh", *SSH_OPTS, action.host, remote_cmd]


async def docker_dispatch(
  action: DispatchEntry,
  state: JobState,
  *,
  instance_home: Path,
  self_host: str,
) -> None:
  """Fire the docker run and wait only for its (fast) return —
  the daemon holds the container afterwards.

  Deadlined, because "fast" is an expectation and not a guarantee, and
  this is the worst place in the server to be wrong about it: the
  dispatch walk is sequential, so one `docker run` that never returns
  stops dispatch for the WHOLE cluster — and without
  `--no-docker-events`, the same tick's completion poll with it. A hang
  is not a crash, so `supervised` would not restart anything and no log
  line would appear. `ssh`'s ConnectTimeout does not cover it: that is
  reaching the host, and a remote daemon that wedges after connecting
  waits forever.

  A timeout becomes a `DispatchError` so it lands in the path a failed
  dispatch already has — requeue, and park in `unknown` past the
  budget. A hang the operator can see beats a hang they cannot, and it
  is the same evidence either way: no container was confirmed started,
  and the GC-orphan sweep reaps one if it was."""
  argv = build_argv(
    action, state, instance_home=instance_home, self_host=self_host
  )
  # Passed, not left to `run_argv`'s own default: the deadline and the
  # message it produces have to come from one place, or the error can
  # name a number that is not the one that fired.
  try:
    done = await run_argv(argv, timeout=DEFAULT_TIMEOUT)
  except TimeoutError as exc:
    raise DispatchError(
      f"dispatch to {action.host!r} timed out after {DEFAULT_TIMEOUT}s"
    ) from exc
  if done.returncode != 0:
    raise DispatchError(
      f"dispatch to {action.host!r} failed "
      f"(exit {done.returncode}): "
      f"stdout={done.stdout!r} stderr={done.stderr!r}"
    )
