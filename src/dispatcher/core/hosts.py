"""Run a shell command on a host — locally when it's the
dispatcher's own host, over `ssh -o BatchMode=yes` otherwise.

Everything here carries a deadline, and that is the point. The server
is a single event loop with sequential stretches in it (dispatch walks
one instance at a time, a periodic loop's next tick waits for this
one), so a child that never returns does not slow the dispatcher down —
it stops that part of it, silently, with no log line and nothing for
`supervised` to restart, because a hang is not a crash.

Hangs here are not hypothetical. `ssh`'s ConnectTimeout only covers
reaching the host; a remote `docker run` that wedges after connecting
waits forever, and the NFS mounts are `hard`, so they block rather than
erroring when a server goes away."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import subprocess

import anyio

logger = logging.getLogger(__name__)

SSH_OPTS: tuple[str, ...] = (
  "-o",
  "BatchMode=yes",
  "-o",
  "ConnectTimeout=10",
)


def host_argv(host: str, self_host: str, cmd: str) -> list[str]:
  """`cmd` is a shell string; it goes through bash on both legs so
  behaviour is identical locally and remotely."""
  if host == self_host:
    return ["bash", "-c", cmd]
  return ["ssh", *SSH_OPTS, host, cmd]


DEFAULT_TIMEOUT = 30.0
"""The deadline every caller gets unless it says otherwise.

Not a tuning knob — a backstop. The commands here (`docker run -d`, a
`docker ps` census, an `rm -f`) answer in well under a second when the
host is healthy, so 30s only ever fires on something that was never
going to finish. Short enough that a wedged host does not hold the
dispatcher for long, long enough that a merely busy one is not failed
for being slow."""


async def run_on(
  host: str,
  self_host: str,
  cmd: str,
  *,
  timeout: float | None = DEFAULT_TIMEOUT,
) -> subprocess.CompletedProcess[str]:
  """Never raises on non-zero exit — caller checks returncode.
  On timeout the process gets SIGTERM and TimeoutError raises."""
  argv = host_argv(host, self_host, cmd)
  with anyio.fail_after(timeout):
    proc = await anyio.run_process(argv, check=False)
  return subprocess.CompletedProcess(
    args=argv,
    returncode=proc.returncode,
    stdout=proc.stdout.decode() if proc.stdout else "",
    stderr=proc.stderr.decode() if proc.stderr else "",
  )


async def run_argv(
  argv: list[str],
  *,
  timeout: float | None = DEFAULT_TIMEOUT,
  capture: bool = True,
) -> subprocess.CompletedProcess[str]:
  """`run_on` for callers that already have an argv list.

  Always `start_new_session=True`, so the child leads its own process
  group: a tmux C-c on the dispatcher's pane cannot forward SIGINT into
  an in-flight docker or ssh client, and on timeout the whole group can
  be killed rather than just the client that was waiting on it.

  Raises `TimeoutError` on the deadline, like `run_on`. Never raises on
  a non-zero exit."""
  proc = await asyncio.create_subprocess_exec(
    *argv,
    stdout=asyncio.subprocess.PIPE if capture else None,
    stderr=asyncio.subprocess.PIPE if capture else None,
    start_new_session=True,
  )
  try:
    async with asyncio.timeout(timeout):
      stdout_b, stderr_b = await proc.communicate()
  except TimeoutError:
    # Kill the GROUP, not the process: an ssh whose remote command is
    # stuck leaves the remote running, and killing only the client we
    # are awaiting would leak it.
    with contextlib.suppress(OSError, ProcessLookupError):
      os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    with contextlib.suppress(Exception):
      await proc.wait()
    logger.warning("timed out after %ss: %s", timeout, argv[0])
    raise
  return subprocess.CompletedProcess(
    args=argv,
    returncode=proc.returncode if proc.returncode is not None else -1,
    stdout=(stdout_b or b"").decode("utf-8", "replace"),
    stderr=(stderr_b or b"").decode("utf-8", "replace"),
  )
