"""Everything that talks to docker daemons: the per-host `docker
events` stream (edge-triggered completion detection), the one-shot
census (level-triggered reconcile), the per-trial state probe, and
label parsing helpers.

All lookups key on `dispatcher.*` labels, never container names —
docker/compose rewrite names (lowercasing, suffixes) and every
name-keyed lookup the old router had eventually killed a live
trial or missed a dead one.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import shlex
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import anyio

from dispatcher.core import clock, labels
from dispatcher.core.hosts import SSH_OPTS, run_on

if TYPE_CHECKING:
  from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
  )

  from anyio.abc import ByteReceiveStream, Process

logger = logging.getLogger(__name__)

OnEvent = "Callable[[str, dict[str, Any]], Awaitable[None]]"
OnAliveChange = "Callable[[str, bool], Awaitable[None]]"

# Reconnect backoff: grows through the list, caps at the tail.
_BACKOFF_SEQUENCE: tuple[float, ...] = (
  1.0,
  2.0,
  5.0,
  10.0,
  30.0,
  60.0,
)


# ── parsing helpers ──────────────────────────────────────────────


def container_labels(inspect_row: dict[str, Any]) -> dict[str, str]:
  """`.Config.Labels` from a `docker inspect` row — a real dict,
  unlike `docker ps`'s comma-joined Labels string whose parsing a
  foreign label VALUE could poison."""
  config = inspect_row.get("Config") or {}
  out = config.get("Labels") or {}
  return out if isinstance(out, dict) else {}


def parse_created(s: str) -> datetime | None:
  """`docker inspect` `.Created`: RFC3339 with nanoseconds
  ("2026-08-03T11:56:15.123456789Z"). fromisoformat only takes
  microseconds, so trim the fraction; None on garbage — callers
  err on the side of not touching the container."""
  s = s.strip()
  if not s:
    return None
  if s.endswith("Z"):
    s = s[:-1] + "+00:00"
  head, dot, tail = s.partition(".")
  if dot:
    frac = ""
    i = 0
    while i < len(tail) and tail[i].isdigit():
      frac += tail[i]
      i += 1
    s = head + "." + (frac[:6] or "0") + tail[i:]
  try:
    dt = datetime.fromisoformat(s)
  except ValueError:
    return None
  if dt.tzinfo is None:
    return None
  return clock.to_kst(dt)


async def iter_lines(
  stream: ByteReceiveStream,
) -> AsyncIterator[bytes]:
  """Newline-split an anyio byte stream; final unterminated chunk
  is yielded too."""
  buf = b""
  async for chunk in stream:
    buf += chunk
    while True:
      nl = buf.find(b"\n")
      if nl < 0:
        break
      yield buf[: nl + 1]
      buf = buf[nl + 1 :]
  if buf:
    yield buf


# ── docker events stream ─────────────────────────────────────────


@dataclass
class _HostStream:
  """One long-running `docker events` subprocess for one host,
  with reconnect backoff and alive tracking. `--since
  last_ack_time` on reconnect replays the gap from docker's
  buffer; gaps past the buffer are healed by census."""

  host: str
  self_host: str
  on_event: Callable[[str, dict[str, Any]], Awaitable[None]]
  on_alive_change: Callable[[str, bool], Awaitable[None]]
  alive_timeout_sec: float = 30.0

  last_ack_time: datetime = field(default_factory=clock.now)

  _proc: Process | None = field(default=None, init=False)
  _task: asyncio.Task[None] | None = field(default=None, init=False)
  _stop_event: anyio.Event = field(default_factory=anyio.Event, init=False)
  _alive: bool = field(default=True, init=False)
  _first_failure_at: datetime | None = field(default=None, init=False)

  def start(self) -> None:
    if self._task is not None and not self._task.done():
      return
    self._stop_event = anyio.Event()
    self._task = asyncio.create_task(
      self._run_forever(), name=f"docker-events-{self.host}"
    )

  async def stop(self) -> None:
    """Best-effort shutdown; never raises."""
    self._stop_event.set()
    proc = self._proc
    if proc is not None:
      with contextlib.suppress(ProcessLookupError):
        proc.terminate()
    task = self._task
    if task is not None:
      with contextlib.suppress(Exception):
        with anyio.move_on_after(5.0):
          await task
    self._task = None
    self._proc = None

  async def _run_forever(self) -> None:
    backoff_i = 0
    while not self._stop_event.is_set():
      try:
        await self._one_connect()
        # Clean EOF (daemon restart etc.) — reconnect fresh.
        backoff_i = 0
      except asyncio.CancelledError:
        raise
      except Exception as exc:
        logger.warning(
          "docker events %s: connect failed (%s: %s)",
          self.host,
          type(exc).__name__,
          exc,
        )
        await self._mark_failure()
      if self._stop_event.is_set():
        break
      delay = _BACKOFF_SEQUENCE[min(backoff_i, len(_BACKOFF_SEQUENCE) - 1)]
      backoff_i += 1
      with anyio.move_on_after(delay):
        await self._stop_event.wait()
      if self._stop_event.is_set():
        break

  async def _one_connect(self) -> None:
    argv = self._build_argv()
    proc = await anyio.open_process(
      argv,
      stdout=asyncio.subprocess.PIPE,
      stderr=asyncio.subprocess.PIPE,
    )
    self._proc = proc
    try:
      stdout = proc.stdout
      if stdout is None:
        raise RuntimeError("subprocess opened without stdout pipe")
      async for line in iter_lines(stdout):
        await self._mark_alive()
        stripped = line.strip()
        if not stripped:
          continue
        try:
          event = json.loads(stripped)
        except json.JSONDecodeError:
          logger.warning(
            "docker events %s: unparseable line: %r",
            self.host,
            stripped[:200],
          )
          continue
        try:
          await self.on_event(self.host, event)
        except Exception:
          logger.exception(
            "docker events %s: on_event handler raised", self.host
          )
        event_time = event.get("time")
        if isinstance(event_time, (int, float)):
          self.last_ack_time = clock.from_timestamp(float(event_time))
      await proc.wait()
      rc = proc.returncode
      if rc not in (0, None):
        stderr_bytes = b""
        stderr = proc.stderr
        if stderr is not None:
          async for chunk in stderr:
            stderr_bytes += chunk
        raise RuntimeError(
          f"docker events exited {rc}: "
          f"{stderr_bytes.decode('utf-8', 'replace')[:500]}"
        )
    finally:
      self._proc = None

  def _build_argv(self) -> list[str]:
    cmd = [
      "docker",
      "events",
      "--since",
      self.last_ack_time.isoformat(),
      "--filter",
      f"label={labels.MANAGED}={labels.MANAGED_VALUE}",
      "--filter",
      "event=die",
      "--format",
      "{{json .}}",
    ]
    if self.host == self.self_host:
      return cmd
    # shlex.join: the remote shell would word-split `{{json .}}`
    # otherwise.
    return ["ssh", *SSH_OPTS, self.host, shlex.join(cmd)]

  async def _mark_alive(self) -> None:
    self._first_failure_at = None
    if self._alive:
      return
    self._alive = True
    try:
      await self.on_alive_change(self.host, True)
    except Exception:
      logger.exception("on_alive_change(%s, True) raised", self.host)

  async def _mark_failure(self) -> None:
    now = clock.now()
    if self._first_failure_at is None:
      self._first_failure_at = now
      return
    elapsed = (now - self._first_failure_at).total_seconds()
    if elapsed < self.alive_timeout_sec or not self._alive:
      return
    self._alive = False
    try:
      await self.on_alive_change(self.host, False)
    except Exception:
      logger.exception("on_alive_change(%s, False) raised", self.host)

  @property
  def alive(self) -> bool:
    return self._alive


class DockerEventStreamManager:
  """One `_HostStream` per host. Alive flips reach the scheduler
  through the caller-supplied callback only."""

  def __init__(
    self,
    *,
    self_host: str,
    on_event: Callable[[str, dict[str, Any]], Awaitable[None]],
    on_alive_change: Callable[[str, bool], Awaitable[None]],
    alive_timeout_sec: float = 30.0,
  ) -> None:
    self._self_host = self_host
    self._on_event = on_event
    self._on_alive_change = on_alive_change
    self._alive_timeout_sec = alive_timeout_sec
    self._streams: dict[str, _HostStream] = {}

  def start(self, hosts: list[str]) -> None:
    for host in hosts:
      self.ensure_host(host)

  async def stop(self) -> None:
    if not self._streams:
      return
    async with anyio.create_task_group() as tg:
      for s in self._streams.values():
        tg.start_soon(s.stop)
    self._streams.clear()

  def ensure_host(self, host: str) -> None:
    if host in self._streams:
      return
    stream = _HostStream(
      host=host,
      self_host=self._self_host,
      on_event=self._on_event,
      on_alive_change=self._on_alive_change,
      alive_timeout_sec=self._alive_timeout_sec,
    )
    self._streams[host] = stream
    stream.start()

  async def drop_host(self, host: str) -> None:
    stream = self._streams.pop(host, None)
    if stream is not None:
      await stream.stop()

  def alive(self, host: str) -> bool:
    stream = self._streams.get(host)
    if stream is None:
      return True
    return stream.alive


# ── one-shot queries ─────────────────────────────────────────────


async def census_host(
  host: str,
  *,
  self_host: str,
  label_filter: str | None = None,
  timeout_sec: float = 30.0,
) -> list[dict[str, Any]]:
  """Full `docker inspect` rows for every container matching the
  label filter (default: the managed main-container label; GC
  passes the SET key). One ssh round trip: `ps -aq` for ids, then
  `inspect` for REAL JSON — labels as a dict, ExitCode as an int,
  Created as RFC3339 — instead of `docker ps`'s human-oriented
  strings. RuntimeError on ssh / docker failure — callers treat
  that as "no evidence this tick", never as "host is empty"."""
  flt = label_filter or f"{labels.MANAGED}={labels.MANAGED_VALUE}"
  inner = (
    "ids=$(docker ps -aq --filter "
    + shlex.quote(f"label={flt}")
    + '); if [ -n "$ids" ]; then docker inspect $ids; '
    "else echo '[]'; fi"
  )
  argv = (
    ["bash", "-c", inner]
    if host == self_host
    else ["ssh", *SSH_OPTS, host, inner]
  )
  stdout = b""
  stderr = b""
  return_code: int | None = None
  proc = await anyio.open_process(
    argv,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
  )
  try:
    with anyio.fail_after(timeout_sec):
      assert proc.stdout is not None
      assert proc.stderr is not None
      async for chunk in proc.stdout:
        stdout += chunk
      async for chunk in proc.stderr:
        stderr += chunk
      await proc.wait()
      return_code = proc.returncode
  except TimeoutError as exc:
    with contextlib.suppress(ProcessLookupError):
      proc.terminate()
    raise RuntimeError(
      f"census {host} timed out after {timeout_sec}s"
    ) from exc
  if return_code != 0:
    raise RuntimeError(
      f"census {host} exited {return_code}: "
      f"{stderr.decode('utf-8', 'replace')[:500]}"
    )
  try:
    rows = json.loads(stdout.decode("utf-8", "replace") or "[]")
  except json.JSONDecodeError as exc:
    raise RuntimeError(
      f"census {host}: inspect output unparseable: {exc}"
    ) from exc
  if not isinstance(rows, list):
    raise RuntimeError(
      f"census {host}: inspect returned {type(rows).__name__}"
    )
  return [r for r in rows if isinstance(r, dict)]


async def probe_trial(
  host: str,
  trial_id: str,
  *,
  self_host: str,
  timeout_sec: float = 10.0,
) -> str:
  """State of a trial's MAIN container: docker's own status word,
  or "gone" when docker has no record. Raises RuntimeError on a
  real infra problem (never guesses "gone" from a failed ssh)."""
  cmd = (
    "ids=$(docker ps -aq --filter "
    + shlex.quote(f"label={labels.TRIAL}={trial_id}")
    + '); if [ -n "$ids" ]; then '
    "docker inspect --format '{{.State.Status}}' $ids; fi"
  )
  r = await run_on(host, self_host, cmd, timeout=timeout_sec)
  if r.returncode != 0:
    raise RuntimeError(
      f"docker probe on {host!r} for trial {trial_id!r} exited "
      f"{r.returncode}: {r.stderr.strip()[:500]}"
    )
  states = [s.strip() for s in r.stdout.splitlines() if s.strip()]
  if not states:
    return "gone"
  if "running" in states:
    return "running"
  return states[0]


async def remove_trial_sets(
  host: str,
  trial_ids: list[str],
  *,
  self_host: str,
) -> int:
  """`docker rm -f` every container carrying a trial's SET label
  (main + siblings). Returns the count of removed container ids;
  non-fatal on failure (partial removals still counted — docker
  prints each removed id even when others in the batch fail)."""
  if not trial_ids:
    return 0
  inner = "\n".join(
    "ids=$(docker ps -aq --filter "
    + shlex.quote(f"label={labels.SET}={name}")
    + '); [ -n "$ids" ] && docker rm -f $ids'
    for name in trial_ids
  )
  argv = (
    ["bash", "-c", inner]
    if host == self_host
    else ["ssh", *SSH_OPTS, host, inner]
  )
  try:
    proc = await asyncio.create_subprocess_exec(
      *argv,
      stdout=asyncio.subprocess.PIPE,
      stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
  except (OSError, RuntimeError) as exc:
    logger.warning("gc: rm on %s raised: %s", host, exc)
    return 0
  if proc.returncode != 0:
    logger.warning(
      "gc: rm on %s exited %s: %s",
      host,
      proc.returncode,
      stderr.decode("utf-8", "replace")[:300],
    )
  return len(
    [
      ln
      for ln in stdout.decode("utf-8", "replace").splitlines()
      if ln.strip()
    ]
  )


# ── image identity + distribution ────────────────────────────────


def resolve_image_id(ref: str, *, self_host: str) -> str:
  """Resolve an image reference (tag or id) to its immutable ID
  on the launcher. Runs synchronously (called from the submit
  path; ~50ms). Raises RuntimeError when the image isn't present
  — the submitter must build/load it on the launcher first, and
  a loud 400 at submit beats trials failing host by host."""
  import subprocess

  r = subprocess.run(
    ["docker", "image", "inspect", "--format", "{{.Id}}", ref],
    capture_output=True,
    text=True,
    timeout=30,
  )
  if r.returncode != 0:
    raise RuntimeError(
      f"image {ref!r} not found on {self_host} — build or "
      f"`docker load` it on the launcher before submitting"
    )
  image_id = r.stdout.strip().splitlines()[0]
  if not image_id:
    raise RuntimeError(f"image {ref!r}: empty id from inspect")
  return image_id


async def ensure_image_on_host(
  host: str,
  image_id: str,
  *,
  self_host: str,
  timeout_sec: float = 600.0,
) -> None:
  """Guarantee `image_id` exists on `host`, shipping it from the
  launcher (`docker save | ssh docker load`) when missing.
  Environment images are few and change rarely, so the transfer
  is a once-per-(image, host) event. Raises RuntimeError on
  failure — the dispatch path turns that into a requeue, never a
  half-started trial."""
  probe = await run_on(
    host,
    self_host,
    "docker image inspect --format ok " + shlex.quote(image_id),
    timeout=30,
  )
  if probe.returncode == 0:
    return
  if host == self_host:
    # resolve_image_id already proved it exists locally; a local
    # miss here means it was pruned since submit.
    raise RuntimeError(
      f"image {image_id} vanished from {self_host} (pruned?)"
    )
  logger.warning(
    "shipping image %s to %s (save|load)", image_id[:19], host
  )
  cmd = (
    "docker save "
    + shlex.quote(image_id)
    + " | ssh "
    + " ".join(SSH_OPTS)
    + " "
    + shlex.quote(host)
    + " docker load"
  )
  with anyio.fail_after(timeout_sec):
    proc = await anyio.run_process(["bash", "-c", cmd], check=False)
  if proc.returncode != 0:
    raise RuntimeError(
      f"image ship to {host} failed (exit {proc.returncode}): "
      f"{proc.stderr.decode(errors='replace')[:500]}"
    )
