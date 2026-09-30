"""Readout execution: registry, executor, sweep.

Two triggers, one mechanism.

    instance → done_ok/done_err ──┐
    readout registered ───────────┼──→ request(job) ──→ one container
    periodic sweep (lag > 0) ─────┘                     pass for that job

`request(job)` is the whole scheduler. There is no timer and no
debounce: the first request starts a container immediately, so the
common case (one instance finishing) pays zero artificial delay.
Requests arriving while a pass is computing are folded into exactly
ONE more pass after it — the running container's own duration is
the batching window, which is a delay we are already spending
rather than one we invented. A burst of 500 completions therefore
costs two container runs, not 500.

The batch is capped (`batch_cap`). Without the cap, a readout
heavy enough to lose the race against completion would grow its
batch monotonically until the job ended, and then hold one
container for an hour — turning "immediately" into "eventually"
with nothing in the record saying so. With it, backlog accumulates
on the filesystem (instances with no value) instead of inside one
container, the loop simply runs back-to-back passes, and
`ReadoutJobSummary.lag` makes the backlog a number the operator
can read.

Containers run on the launcher only. It is the host that resolved
the image at submit, the job home is on a shared filesystem
anyway, and host selection is the one piece of complexity this
path can do entirely without.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import (
  BaseModel,
  ConfigDict,
  PositiveFloat,
  PositiveInt,
)

from dispatcher.core import clock, labels
from dispatcher.core.dispatch import (
  ENV_JOB,
  ENV_SOURCE,
  SOURCE_MOUNT,
  SOURCE_TAR_FILENAME,
)
from dispatcher.core.loops import every
from dispatcher.core.readout import (
  READOUT_DIRNAME,
  REQUEST_FILENAME,
  BadReadout,
  ReadoutAggregate,
  ReadoutJobSummary,
  ReadoutSpec,
  ReadoutValue,
  aggregate,
  append_values,
  parse_result_lines,
  read_values,
  write_request,
)

if TYPE_CHECKING:
  from collections.abc import Awaitable, Callable
  from datetime import datetime
  from pathlib import Path

  from dispatcher.core.models import JobState
  from dispatcher.core.scheduler import Scheduler

  ReadoutRunner = Callable[["JobState", float], Awaitable[str]]
  """(job, timeout) → the container's stdout. Injected so tests
  never touch docker."""

logger = logging.getLogger(__name__)

READOUTS_FILENAME = "readouts.json"

# Container-side paths. The job home is mounted READ-ONLY: the
# dispatcher is the only writer of value files, which is what makes
# the append-only files single-writer and lock-free.
JOB_MOUNT = "/dispatcher/job"
ENV_JOB_DIR = "DISPATCHER_JOB_DIR"
ENV_REQUEST = "DISPATCHER_READOUT_REQUEST"

_RUNNER_COMMAND = (
  "python",
  "-m",
  "dispatcher_sdk.bootstrap",
  "--",
  "python",
  "-m",
  "dispatcher_sdk.readout",
)


class ReadoutSettings(BaseModel):
  enabled: bool = True
  """Off = no container ever starts. Values already on disk stay
  readable; only computation stops."""

  batch_cap: PositiveInt = 64
  """Instances per container pass. Bounds worst-case latency at
  `batch_cap × per-instance cost` and bounds how much work one
  container death can lose."""

  max_concurrent_jobs: PositiveInt = 2
  """Concurrent readout containers on the launcher. Boot-time only
  — the gate is sized once, so this is deliberately absent from
  ReadoutPatch rather than silently ignored there."""

  sweep_interval_seconds: PositiveFloat = 300.0
  """Cadence of the lag sweep, which exists to heal transient
  failures (a container that could not start), NOT to deliver
  values — the live path already does that with no delay."""

  container_timeout_sec: PositiveFloat = 900.0
  """Outer backstop. The per-readout timeout inside the runner is
  the real guard; this catches a container that never gets that
  far."""


class ReadoutPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  enabled: bool | None = None
  batch_cap: PositiveInt | None = None
  sweep_interval_seconds: PositiveFloat | None = None
  container_timeout_sec: PositiveFloat | None = None


def apply_patch(settings: ReadoutSettings, patch: ReadoutPatch) -> None:
  if patch.enabled is not None:
    settings.enabled = patch.enabled
  if patch.batch_cap is not None:
    settings.batch_cap = patch.batch_cap
  if patch.sweep_interval_seconds is not None:
    settings.sweep_interval_seconds = patch.sweep_interval_seconds
  if patch.container_timeout_sec is not None:
    settings.container_timeout_sec = patch.container_timeout_sec


# ── registry ─────────────────────────────────────────────────────


class ReadoutConflict(ValueError):
  """The name is already registered elsewhere on this arena path."""


def _on_same_path(a: str, b: str) -> bool:
  """One arena path contains the other. Segment-aware, so
  `bench/v7` and `bench/v70` are unrelated."""
  return a == b or a.startswith(b + "/") or b.startswith(a + "/")


class ReadoutRegistry:
  """arena path → the readouts registered at that node.

  Registration is by arena because an arena is the comparison unit:
  the jobs you put side by side are the jobs that must be scored
  the same way. A path names its SUBTREE, so a readout at `bench`
  applies to every job under `bench/...` too — the same inheritance
  `arena_members` already gives group operations.

  An arena is still derived state; nothing here creates one.
  Registering for a path that no job names yet is legal and useful
  (register, then submit)."""

  def __init__(
    self,
    path: Path,
    by_arena: dict[str, list[ReadoutSpec]] | None = None,
  ) -> None:
    self._path = path
    self._by_arena: dict[str, list[ReadoutSpec]] = by_arena or {}

  @classmethod
  def load(cls, data_dir: Path) -> ReadoutRegistry:
    """A corrupt file raises rather than booting empty: the
    registry is operator intent that nothing else on disk can
    reconstruct, and silently dropping it would leave columns
    quietly unfilled."""
    path = data_dir / READOUTS_FILENAME
    if not path.is_file():
      return cls(path)
    try:
      raw = json.loads(path.read_text(encoding="utf-8"))
      by_arena = {
        arena: [ReadoutSpec.model_validate(s).validated() for s in specs]
        for arena, specs in raw.items()
      }
    except Exception as exc:
      raise RuntimeError(
        f"readout registry {path} is unreadable ({exc}); refusing "
        f"to boot with silently-dropped readouts — fix or delete it"
      ) from exc
    return cls(path, by_arena)

  def save(self) -> None:
    self._path.parent.mkdir(parents=True, exist_ok=True)
    tmp = self._path.with_suffix(".json.tmp")
    body = {
      arena: [s.model_dump(mode="json") for s in specs]
      for arena, specs in sorted(self._by_arena.items())
    }
    tmp.write_text(
      json.dumps(body, indent=2, sort_keys=True), encoding="utf-8"
    )
    tmp.replace(self._path)

  def snapshot(self) -> dict[str, list[ReadoutSpec]]:
    return {a: list(s) for a, s in sorted(self._by_arena.items())}

  def for_arena(self, arena: str) -> list[ReadoutSpec]:
    """Everything that applies to a job in `arena` — its own node
    plus every ancestor. Jobs with no arena get nothing: a readout
    needs a comparison unit to belong to."""
    if not arena:
      return []
    segments = arena.split("/")
    out: list[ReadoutSpec] = []
    seen: set[str] = set()
    for i in range(1, len(segments) + 1):
      for spec in self._by_arena.get("/".join(segments[:i]), []):
        if spec.name in seen:
          continue
        seen.add(spec.name)
        out.append(spec)
    return sorted(out, key=lambda s: s.name)

  def conflicting_node(self, arena: str, name: str) -> str | None:
    for node, specs in self._by_arena.items():
      if node == arena or not _on_same_path(node, arena):
        continue
      if any(s.name == name for s in specs):
        return node
    return None

  def register(self, arena: str, spec: ReadoutSpec) -> None:
    """Add or replace by name at this node.

    Refuses a name already registered on the same path. A name is
    the whole identity of a readout, so two definitions on one path
    would fill one column from two computations — and the jobs that
    disagreed would be exactly the ones being compared. Better a
    409 than a column nobody can trust."""
    spec = spec.validated()
    clash = self.conflicting_node(arena, spec.name)
    if clash is not None:
      raise ReadoutConflict(
        f"readout {spec.name!r} is already registered at {clash!r}, "
        f"which is on the same arena path as {arena!r} — a name is "
        f"a readout's identity; use a different name "
        f"({spec.name}-v2) for a different computation"
      )
    specs = [
      s for s in self._by_arena.get(arena, []) if s.name != spec.name
    ]
    specs.append(spec)
    self._by_arena[arena] = sorted(specs, key=lambda s: s.name)
    self.save()

  def unregister(self, arena: str, name: str) -> bool:
    """Stop computing it. Values already written stay on disk —
    they are the record of a finished experiment, not a cache."""
    specs = self._by_arena.get(arena)
    if not specs:
      return False
    kept = [s for s in specs if s.name != name]
    if len(kept) == len(specs):
      return False
    if kept:
      self._by_arena[arena] = kept
    else:
      del self._by_arena[arena]
    self.save()
    return True


# ── executor ─────────────────────────────────────────────────────


@dataclass
class _Target:
  """One finished instance and the readouts it still needs."""

  instance_id: str
  task_id: str
  state: str
  names: list[str]


class ReadoutService:
  def __init__(
    self,
    *,
    scheduler: Scheduler,
    registry: ReadoutRegistry,
    settings: ReadoutSettings,
    runner: ReadoutRunner,
    clock_fn: Callable[[], datetime] = clock.now,
    on_values_written: Callable[[str], None] | None = None,
  ) -> None:
    self._scheduler = scheduler
    self._registry = registry
    self._settings = settings
    self._runner = runner
    self._clock = clock_fn
    self._on_values_written = on_values_written
    # job_id → readout name → instance_id → value.
    self._values: dict[str, dict[str, dict[str, ReadoutValue]]] = {}
    # (job_id, name) pairs read off disk. Per-NAME, so registering a
    # readout later still picks up values a previous dispatcher
    # generation wrote for it.
    self._loaded: set[tuple[str, str]] = set()
    self._tasks: dict[str, asyncio.Task[None]] = {}
    # A job in `_computing` has its snapshot taken; a job whose task
    # exists but is absent here is still waiting for a slot, and the
    # pass it is about to take will see any state changed since.
    # That distinction is what lets `request` be a no-op in the
    # second case without losing work.
    self._computing: set[str] = set()
    self._pending: set[str] = set()
    self._slots = asyncio.Semaphore(settings.max_concurrent_jobs)

  @property
  def registry(self) -> ReadoutRegistry:
    return self._registry

  # ── trigger ──────────────────────────────────────────────────

  def request(self, job_id: str) -> None:
    """Ask for a readout pass. Synchronous, idempotent, no delay.

    Safe to call from a completion handler: it only touches
    in-memory sets and creates a task."""
    if not self._settings.enabled:
      return
    task = self._tasks.get(job_id)
    if task is not None and not task.done():
      if job_id in self._computing:
        self._pending.add(job_id)
      return
    self._tasks[job_id] = asyncio.create_task(
      self._run(job_id), name=f"readout-{job_id}"
    )

  def sweep_live(self) -> int:
    """Re-request every live job carrying lag. The live path
    already delivers values, so anything still missing here means a
    pass failed — this is the retry, not the delivery."""
    n = 0
    for job_id in list(self._scheduler.iter_job_ids()):
      if self._scheduler.is_archived(job_id):
        continue
      if (self.summary(job_id).lag or 0) > 0:
        self.request(job_id)
        n += 1
    return n

  async def close(self) -> None:
    tasks = list(self._tasks.values())
    for task in tasks:
      task.cancel()
    for task in tasks:
      with contextlib.suppress(BaseException):
        await task

  # ── the loop ─────────────────────────────────────────────────

  async def _run(self, job_id: str) -> None:
    try:
      while True:
        async with self._slots:
          self._computing.add(job_id)
          try:
            more = await self._compute_once(job_id)
          except Exception:
            logger.exception("readout pass failed job=%s", job_id)
            more = False
          finally:
            self._computing.discard(job_id)
        # Nothing may await between the discard above and this
        # decision, or a `request` landing in the gap would find no
        # task computing, take the "waiting for a slot" branch, and
        # be dropped. Semaphore release is synchronous, so it does
        # not yield — keep it that way.
        if more or job_id in self._pending:
          self._pending.discard(job_id)
          continue
        return
    finally:
      self._tasks.pop(job_id, None)
      self._pending.discard(job_id)

  async def _compute_once(self, job_id: str) -> bool:
    """One container pass. Returns whether work remains (the cap
    truncated the batch), which the loop turns into another pass
    immediately."""
    try:
      state = self._scheduler.job_state(job_id)
    except KeyError:
      return False  # cancelled under us
    specs = self._registry.for_arena(state.arena)
    if not specs:
      return False
    await self._ensure_loaded(job_id, state, specs)
    targets = self._unscored(job_id, specs)
    if not targets:
      # The common no-op: an `unknown` transition, or a request that
      # raced another pass. Costs a dict walk, never a container.
      return False
    cap = max(1, self._settings.batch_cap)
    batch, more = targets[:cap], len(targets) > cap
    asked: dict[str, set[str]] = {}
    for target in batch:
      for name in target.names:
        asked.setdefault(name, set()).add(target.instance_id)

    await asyncio.to_thread(
      write_request,
      state.home_root,
      {
        "job_id": job_id,
        "job_dir": JOB_MOUNT,
        "readouts": [s.model_dump(mode="json") for s in specs],
        "instances": [
          {
            "instance_id": t.instance_id,
            "task_id": t.task_id,
            "state": t.state,
            "readouts": t.names,
          }
          for t in batch
        ],
      },
    )
    stdout = await self._runner(
      state, self._settings.container_timeout_sec
    )

    now = self._clock()
    by_name: dict[str, list[ReadoutValue]] = {}
    for row in parse_result_lines(stdout):
      if row.instance_id not in asked.get(row.name, ()):
        # Never write a value nobody asked for: a confused runner
        # must not be able to score instances outside its batch.
        continue
      by_name.setdefault(row.name, []).append(
        row.model_copy(update={"at": now})
      )
    written = 0
    for name, values in by_name.items():
      await asyncio.to_thread(append_values, state.home_root, name, values)
      # Disk first, memory second: a failed append must leave the
      # pair unscored so the sweep retries it.
      index = self._values.setdefault(job_id, {}).setdefault(name, {})
      for value in values:
        index[value.instance_id] = value
      written += len(values)
    unreported = sum(len(ids) for ids in asked.values()) - written
    if unreported > 0:
      logger.warning(
        "readout job=%s: %d/%d pair(s) unreported by the container "
        "— they stay unscored and the sweep retries them",
        job_id,
        unreported,
        unreported + written,
      )
    if written and self._on_values_written is not None:
      self._on_values_written(job_id)
    return more

  def _unscored(
    self, job_id: str, specs: list[ReadoutSpec]
  ) -> list[_Target]:
    """Finished instances missing at least one value, in completion
    order — so a truncated batch works through the oldest first and
    nothing starves."""
    view = self._scheduler.job_view(job_id)
    values = self._values.get(job_id, {})
    out: list[_Target] = []
    for bucket in ("done_ok", "done_err"):
      for task_id, tv in getattr(view, bucket).items():
        missing = [
          s.name
          for s in specs
          if tv.instance_id not in values.get(s.name, {})
        ]
        if missing:
          out.append(
            _Target(
              instance_id=tv.instance_id,
              task_id=task_id,
              state=bucket,
              names=missing,
            )
          )
    return out

  # ── reading ──────────────────────────────────────────────────

  async def _ensure_loaded(
    self, job_id: str, state: JobState, specs: list[ReadoutSpec]
  ) -> None:
    missing = [
      s.name for s in specs if (job_id, s.name) not in self._loaded
    ]
    if not missing:
      return
    home_root = state.home_root
    loaded = await asyncio.to_thread(
      lambda: {n: read_values(home_root, n) for n in missing}
    )
    index = self._values.setdefault(job_id, {})
    for name, values in loaded.items():
      index.setdefault(name, {}).update(values)
      self._loaded.add((job_id, name))

  async def load(self, job_id: str) -> dict[str, dict[str, ReadoutValue]]:
    """Every value for a job, reading from disk on first ask — an
    archived job's values are not held in memory until something
    looks at them."""
    try:
      state = self._scheduler.job_state(job_id)
    except KeyError:
      return {}
    specs = self._registry.for_arena(state.arena)
    if not specs:
      return {}
    await self._ensure_loaded(job_id, state, specs)
    index = self._values.get(job_id, {})
    return {s.name: dict(index.get(s.name, {})) for s in specs}

  async def load_live(self) -> None:
    """Boot: pull values for live jobs that have readouts, so the
    first jobs table already carries columns and lag."""
    for job_id in list(self._scheduler.iter_job_ids()):
      if self._scheduler.is_archived(job_id):
        continue
      try:
        state = self._scheduler.job_state(job_id)
      except KeyError:
        continue
      specs = self._registry.for_arena(state.arena)
      if not specs:
        continue
      try:
        await self._ensure_loaded(job_id, state, specs)
      except OSError as exc:
        logger.warning("readout load failed job=%s: %s", job_id, exc)

  def summary(self, job_id: str) -> ReadoutJobSummary:
    """The job row's readout cell. Pure projection over what is
    already in memory — no disk, no await."""
    try:
      state = self._scheduler.job_state(job_id)
    except KeyError:
      return ReadoutJobSummary()
    specs = self._registry.for_arena(state.arena)
    if not specs:
      return ReadoutJobSummary(lag=0)
    terminal = self._scheduler.terminal_count(job_id)
    index = self._values.get(job_id, {})
    aggregates: dict[str, ReadoutAggregate] = {}
    lag = 0
    unloaded = False
    for spec in specs:
      if (job_id, spec.name) not in self._loaded:
        unloaded = True
        continue
      values = index.get(spec.name, {})
      aggregates[spec.name] = aggregate(values)
      # Values can include instances a requeue superseded, so the
      # count is a lower bound on lag, not an exact one. It is an
      # operator signal (is computation keeping up?), not a ledger.
      lag += max(0, terminal - len(values))
    return ReadoutJobSummary(
      aggregates=aggregates, lag=None if unloaded else lag
    )


# ── the container ────────────────────────────────────────────────


class ReadoutRunError(RuntimeError):
  """The readout container could not be run at all."""


def build_readout_argv(state: JobState) -> list[str]:
  """`docker run` for one readout pass.

  Deliberately unlike an instance's container in three ways: the
  job home is mounted read-only (the dispatcher owns the value
  files), `container.extra_args` is NOT applied (a readout has no
  business claiming GPUs or the host network), and the command is
  the SDK runner rather than the job's own — the operator supplies
  an entrypoint, not a process."""
  spec = state.container
  argv = [
    "docker",
    "run",
    "--rm",
    "--label",
    f"{labels.READOUT}={state.job_id}",
    "-v",
    f"{state.home_root}:{JOB_MOUNT}:ro",
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
    ENV_JOB_DIR: JOB_MOUNT,
    ENV_REQUEST: f"{JOB_MOUNT}/{READOUT_DIRNAME}/{REQUEST_FILENAME}",
    **({ENV_SOURCE: SOURCE_MOUNT} if state.source_sha256 else {}),
  }
  for key, value in env.items():
    argv += ["-e", f"{key}={value}"]
  argv.append(state.image_id or spec.image)
  argv += _RUNNER_COMMAND
  return argv


async def docker_readout_run(state: JobState, timeout_sec: float) -> str:
  """Run the pass on the launcher and return its stdout.

  Stdout is returned even on a non-zero exit: the runner emits one
  result line per pair as it goes, so a crash halfway still carries
  real values, and discarding them would make the batch's worst
  instance cost the whole batch. Pairs with no line stay unscored
  and the sweep retries them.

  `start_new_session=True` — without it a tmux C-c on the
  dispatcher pane forwards SIGINT into an in-flight docker
  client."""
  argv = build_readout_argv(state)
  proc = await asyncio.create_subprocess_exec(
    *argv,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
    start_new_session=True,
  )
  try:
    stdout_b, stderr_b = await asyncio.wait_for(
      proc.communicate(), timeout=timeout_sec
    )
  except TimeoutError as exc:
    # Killing the client leaves the container running (the daemon
    # owns it), so reap it by label — one pass per job at a time
    # means the label is unambiguous.
    await kill_readout_containers(state.job_id)
    raise ReadoutRunError(
      f"readout container for {state.job_id} exceeded {timeout_sec}s"
    ) from exc
  stdout = stdout_b.decode("utf-8", "replace")
  if proc.returncode != 0:
    logger.warning(
      "readout container job=%s exited %s: %s",
      state.job_id,
      proc.returncode,
      stderr_b.decode("utf-8", "replace")[-500:],
    )
  return stdout


async def kill_readout_containers(job_id: str) -> None:
  inner = (
    "ids=$(docker ps -aq --filter "
    f"label={labels.READOUT}={job_id}"
    '); [ -n "$ids" ] && docker rm -f $ids > /dev/null 2>&1; true'
  )
  proc = await asyncio.create_subprocess_exec(
    "bash",
    "-c",
    inner,
    stdout=asyncio.subprocess.DEVNULL,
    stderr=asyncio.subprocess.DEVNULL,
    start_new_session=True,
  )
  await proc.wait()


# ── loop ─────────────────────────────────────────────────────────


async def readout_sweep_loop(
  service: ReadoutService, settings: ReadoutSettings
) -> None:
  async def tick() -> None:
    n = service.sweep_live()
    if n:
      logger.info("readout sweep: re-requested %d job(s) with lag", n)

  await every(
    "readout_sweep",
    lambda: settings.sweep_interval_seconds,
    tick,
    enabled_fn=lambda: settings.enabled,
  )


__all__ = [
  "BadReadout",
  "ReadoutConflict",
  "ReadoutPatch",
  "ReadoutRegistry",
  "ReadoutRunError",
  "ReadoutService",
  "ReadoutSettings",
  "ReadoutSpec",
  "apply_patch",
  "build_readout_argv",
  "docker_readout_run",
  "readout_sweep_loop",
]
