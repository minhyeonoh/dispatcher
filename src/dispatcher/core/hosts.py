"""Run a shell command on a host — locally when it's the
dispatcher's own host, over `ssh -o BatchMode=yes` otherwise."""

from __future__ import annotations

import subprocess

import anyio

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


async def run_on(
  host: str,
  self_host: str,
  cmd: str,
  *,
  timeout: float | None = 30,
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
