"""Resident aggregate processes: operator Python that turns a job's
values into column numbers, in real time.

A column like "median reward over successes" or "cost per task" is
arbitrary operator code over a job's whole value set. The dispatcher
cannot run it in-process — a hang or a memory blowup there kills the
scheduler — and it cannot afford a fresh container per update, which
costs ~740ms and drags a trigger/coalescing/sweep apparatus behind
it.

So: start ONE long-lived container from the research image and talk
to it. A request is a columnar frame on stdin, the answer is a line
on stdout, and the round trip is ~6ms for a 1344-instance job
(measured). At that price there is nothing to schedule — no timer,
no queue, no semaphore, no coalescing. Braintrust's "remote evals"
are the same shape: they trigger, a resident process of yours
executes.

Nothing here polls. A process starts on the first call that needs
it, and dies when the pool evicts it (LRU, or idle past the
timeout, both checked opportunistically on the next call) or when
the server stops. Reaping on use rather than on a timer means the
only way a process lingers is that nothing ever asks again — which
is exactly when it costs nothing.

**The aggregate sees the frame and nothing else.** No filesystem,
no job home: one process serves every job sharing its (image,
source), so there is no single home it could mount. Per-instance
readouts are where artifact access belongs; by the time values are
being rolled up, the reading is done.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dispatcher.core import labels
from dispatcher.core.dispatch import (
  ENV_JOB,
  ENV_SOURCE,
  SOURCE_MOUNT,
  SOURCE_TAR_FILENAME,
)
from dispatcher.core.readout import (
  SDK_MOUNT,
  protocol_command,
  sdk_dir,
)

if TYPE_CHECKING:
  from dispatcher.core.models import JobState

logger = logging.getLogger(__name__)

AGGREGATE_LABEL_VALUE = "aggregate"


class AggregateError(RuntimeError):
  """The aggregate could not be computed — process failed, timed
  out, or the operator's function raised. The caller keeps whatever
  column values it had and marks them stale; it never invents a
  number."""


@dataclass
class _Process:
  """One resident container, addressed by its (image, source) key.

  `lock` serialises requests: the protocol is one line in, one line
  out, so two concurrent callers would interleave and read each
  other's answers. Serialising is free — a request is milliseconds."""

  key: tuple[str, str]
  proc: asyncio.subprocess.Process
  lock: asyncio.Lock = field(default_factory=asyncio.Lock)
  last_used: float = 0.0
  requests: int = 0

  @property
  def alive(self) -> bool:
    return self.proc.returncode is None

  async def close(self) -> None:
    with contextlib.suppress(ProcessLookupError, OSError):
      if self.proc.stdin is not None and not self.proc.stdin.is_closing():
        self.proc.stdin.close()
    with contextlib.suppress(ProcessLookupError):
      self.proc.kill()
    with contextlib.suppress(Exception):
      await self.proc.wait()


class AggregatePool:
  """Keyed by `(image_id, source_sha256)` — the code is what
  matters, not the job. One sweep's arms share a process; a
  re-submitted sweep with new code gets its own. The cap is what
  keeps months of submissions from accumulating processes."""

  def __init__(
    self,
    *,
    max_processes: int = 4,
    request_timeout_sec: float = 5.0,
    idle_timeout_sec: float = 900.0,
    spawn: SpawnFn | None = None,
    monotonic: Monotonic | None = None,
  ) -> None:
    self._max = max(1, max_processes)
    self._timeout = request_timeout_sec
    self._idle = idle_timeout_sec
    self._spawn = spawn or _docker_spawn
    self._now = monotonic or asyncio.get_event_loop().time
    self._procs: dict[tuple[str, str], _Process] = {}
    # One lock for start/evict so two first-callers don't both spawn.
    self._admin = asyncio.Lock()

  # ── the call ─────────────────────────────────────────────────

  async def compute(
    self,
    state: JobState,
    *,
    source: str,
    frame: dict[str, Any],
  ) -> dict[str, Any]:
    """Hand one job's frame to the operator's function and return
    the column dict it produced. Raises AggregateError on any
    failure — a stale number is the caller's decision, not ours."""
    # Keyed by IMAGE alone: the function arrives with the request, so
    # two jobs with different frozen archives can share one process.
    # (It is still the archive that `import myrepo…` resolves against,
    # which the mount provides per spawn.)
    key = (state.image_id or state.container.image, state.source_sha256)
    request = json.dumps(
      {"job_id": state.job_id, "source": source, "frame": frame},
      default=str,
    )
    # Serialising a 1344-row frame is ~1.7ms and grows with the job,
    # so it does not belong on the event loop.
    payload = await asyncio.to_thread(lambda: (request + "\n").encode())

    for attempt in (1, 2):
      process = await self._acquire(key, state)
      try:
        async with process.lock:
          reply = await self._roundtrip(process, payload)
      except (BrokenPipeError, ConnectionResetError, EOFError) as exc:
        # The process died between acquire and write — most likely it
        # was killed or crashed on a previous request. Drop it and
        # try once with a fresh one before giving up.
        await self._drop(key)
        if attempt == 2:
          raise AggregateError(f"aggregate process died: {exc}") from exc
        continue
      except TimeoutError as exc:
        # A wedged process cannot be reused: it may still write the
        # late answer and desynchronise the stream.
        await self._drop(key)
        raise AggregateError(
          f"aggregate for {state.job_id} exceeded {self._timeout}s"
        ) from exc
      process.last_used = self._now()
      process.requests += 1
      await self._reap()
      if not reply.get("ok"):
        raise AggregateError(str(reply.get("error") or "unknown"))
      values = reply.get("values")
      if not isinstance(values, dict):
        raise AggregateError(
          f"columns returned {type(values).__name__}, expected a "
          f"dict of column name → value"
        )
      return values
    raise AggregateError("unreachable")  # pragma: no cover

  async def _roundtrip(
    self, process: _Process, payload: bytes
  ) -> dict[str, Any]:
    stdin, stdout = process.proc.stdin, process.proc.stdout
    if stdin is None or stdout is None:  # pragma: no cover
      raise EOFError("process has no pipes")
    stdin.write(payload)
    await stdin.drain()
    line = await asyncio.wait_for(stdout.readline(), self._timeout)
    if not line:
      raise EOFError("aggregate process closed stdout")
    try:
      reply = json.loads(line)
    except json.JSONDecodeError as exc:
      raise AggregateError(
        f"aggregate process wrote unparseable output: {line[:200]!r}"
      ) from exc
    if not isinstance(reply, dict):  # pragma: no cover
      raise AggregateError("aggregate reply is not an object")
    return reply

  # ── lifecycle ────────────────────────────────────────────────

  async def _acquire(
    self, key: tuple[str, str], state: JobState
  ) -> _Process:
    async with self._admin:
      existing = self._procs.get(key)
      if existing is not None and existing.alive:
        return existing
      if existing is not None:
        await existing.close()
        del self._procs[key]
      # Evict before spawning so the cap is a real ceiling.
      while len(self._procs) >= self._max:
        victim = min(self._procs.values(), key=lambda p: p.last_used)
        logger.info(
          "aggregate pool full (%d); evicting %s",
          self._max,
          victim.key[0][:19],
        )
        await victim.close()
        del self._procs[victim.key]
      proc = await self._spawn(state)
      process = _Process(key=key, proc=proc, last_used=self._now())
      self._procs[key] = process
      return process

  async def _drop(self, key: tuple[str, str]) -> None:
    async with self._admin:
      process = self._procs.pop(key, None)
    if process is not None:
      await process.close()

  async def _reap(self) -> None:
    """Close processes idle past the timeout. Piggybacks on traffic —
    no timer, and a process only lingers when nothing is asking,
    which is when it costs nothing."""
    now = self._now()
    stale = [
      p
      for p in list(self._procs.values())
      if not p.lock.locked() and now - p.last_used > self._idle
    ]
    for process in stale:
      async with self._admin:
        if self._procs.get(process.key) is process:
          del self._procs[process.key]
      await process.close()

  async def close(self) -> None:
    processes = list(self._procs.values())
    self._procs.clear()
    for process in processes:
      await process.close()

  def snapshot(self) -> list[dict[str, Any]]:
    """What the pool is holding — for /state, so an operator can see
    these exist rather than wondering about stray containers."""
    return [
      {
        "image_id": p.key[0],
        "source_sha256": p.key[1],
        "requests": p.requests,
        "alive": p.alive,
      }
      for p in self._procs.values()
    ]


# ── the container ────────────────────────────────────────────────


def build_aggregate_argv(state: JobState) -> list[str]:
  """`docker run -i` for a resident aggregate process.

  Differences from an instance's container, all deliberate: no job
  home mount (the frame arrives on stdin, and one process serves
  many jobs), no `extra_args` (a column has no business claiming
  GPUs), and a label outside the managed/set namespace so neither
  the die-event filter nor the orphan GC can see it."""
  spec = state.container
  argv = [
    "docker",
    "run",
    "--rm",
    "-i",
    "--label",
    f"{labels.READOUT}={AGGREGATE_LABEL_VALUE}",
    "-v",
    f"{sdk_dir()}:{SDK_MOUNT}:ro",
  ]
  if state.source_sha256:
    argv += [
      "-v",
      f"{state.home_root / SOURCE_TAR_FILENAME}:{SOURCE_MOUNT}:ro",
    ]
  env = {
    **spec.env,
    **state.env,
    ENV_JOB: state.job_id,
    **({ENV_SOURCE: SOURCE_MOUNT} if state.source_sha256 else {}),
  }
  for key, value in env.items():
    argv += ["-e", f"{key}={value}"]
  argv.append(state.image_id or spec.image)
  argv += protocol_command("dispatcher_sdk.aggregate")
  return argv


async def _docker_spawn(state: JobState) -> asyncio.subprocess.Process:
  """On the launcher, with pipes held open. `start_new_session=True`
  so a tmux C-c on the dispatcher pane cannot forward SIGINT into
  it."""
  return await asyncio.create_subprocess_exec(
    *build_aggregate_argv(state),
    stdin=asyncio.subprocess.PIPE,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.DEVNULL,
    start_new_session=True,
  )


if TYPE_CHECKING:
  from collections.abc import Awaitable, Callable

  SpawnFn = Callable[[JobState], Awaitable[asyncio.subprocess.Process]]
  Monotonic = Callable[[], float]
