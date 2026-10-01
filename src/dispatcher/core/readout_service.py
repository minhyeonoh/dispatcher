"""Readout registry, the column index, and the retroactive pass.

Two paths, deliberately NOT merged — the same split Harbor and
Braintrust arrived at:

**Live.** The worker scores itself (`dispatcher_sdk.run`) and leaves
`readouts.json` beside the envelope. The dispatcher reads it in the
same look that detects completion and folds it into the job's column
index. No container, no trigger, no timer, no delay — and nothing in
this module schedules anything. `record()` is the whole live path.

**Retroactive.** An operator runs `dispatcher readout`, which asks
for pairs the live path never produced: a readout registered after
the run, a redefinition, an instance that died before scoring
itself. `compute()` starts a container from the job's pinned image,
one pass at a time, and appends what it reports. Nothing here runs
on its own — the request drives it, so there is no concurrency to
bound and no starvation to reason about.

`ReadoutJobSummary.lag` is the number that connects them: 0 in
steady state (the live path leaves nothing behind), and anything
above 0 names work for the retroactive command.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, PositiveInt

from dispatcher.core import clock, labels
from dispatcher.core.aggregate_pool import AggregateError, AggregatePool
from dispatcher.core.dispatch import (
  ENV_JOB,
  ENV_SOURCE,
  SOURCE_MOUNT,
  SOURCE_TAR_FILENAME,
)
from dispatcher.core.readout import (
  COLUMNS_NAME,
  DESCRIPTIONS_NAME,
  READOUT_DIRNAME,
  REQUEST_FILENAME,
  SDK_MOUNT,
  BadReadout,
  ReadoutAggregate,
  ReadoutJobSummary,
  ReadoutSpec,
  ReadoutValue,
  aggregate,
  append_values,
  check_source,
  parse_result_lines,
  protocol_command,
  read_values,
  record_source,
  sdk_dir,
  write_request,
)

if TYPE_CHECKING:
  from collections.abc import AsyncIterator, Awaitable, Callable
  from datetime import datetime
  from pathlib import Path

  from dispatcher.core.models import JobState
  from dispatcher.core.scheduler import Scheduler

  ReadoutRunner = Callable[["JobState", float], Awaitable[str]]
  """(job, timeout) → the container's stdout. Injected so tests
  never touch docker."""

logger = logging.getLogger(__name__)

READOUTS_FILENAME = "readouts.json"


def _sha(text: str) -> str:
  return hashlib.sha256(text.encode("utf-8")).hexdigest() if text else ""


# Container-side paths for the retroactive pass. The job home is
# mounted READ-ONLY: the dispatcher is the only writer of the column
# index, which is what makes those append-only files single-writer
# and lock-free.
JOB_MOUNT = "/dispatcher/job"
ENV_JOB_DIR = "DISPATCHER_JOB_DIR"
ENV_REQUEST = "DISPATCHER_READOUT_REQUEST"


class ReadoutSettings(BaseModel):
  """Only the retroactive pass has knobs; the live path has none to
  have — it is the worker's own process doing its own work."""

  batch: PositiveInt = 64
  """Instances per retroactive container pass. Bounds how much one
  container death loses, and keeps progress visible per pass."""

  container_timeout_sec: float = 3600.0
  """Outer backstop for one pass. The per-readout timeout inside the
  runner is the real guard; this catches a container that never gets
  that far."""

  max_aggregate_processes: PositiveInt = 4
  """Resident column processes, keyed by (image, source). The cap is
  what stops months of submissions from accumulating them; the LRU
  evicts the least recently used."""

  aggregate_timeout_sec: float = 5.0
  """One column computation. Measured at ~6ms for 1344 instances, so
  this only ever catches a runaway."""

  aggregate_idle_sec: float = 900.0
  """Close a process unused this long. Checked when another request
  arrives, never on a timer."""


class ReadoutPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  batch: PositiveInt | None = None
  container_timeout_sec: float | None = None
  max_aggregate_processes: PositiveInt | None = None
  aggregate_timeout_sec: float | None = None
  aggregate_idle_sec: float | None = None


def apply_patch(settings: ReadoutSettings, patch: ReadoutPatch) -> None:
  if patch.batch is not None:
    settings.batch = patch.batch
  if patch.container_timeout_sec is not None:
    settings.container_timeout_sec = patch.container_timeout_sec
  if patch.max_aggregate_processes is not None:
    settings.max_aggregate_processes = patch.max_aggregate_processes
  if patch.aggregate_timeout_sec is not None:
    settings.aggregate_timeout_sec = patch.aggregate_timeout_sec
  if patch.aggregate_idle_sec is not None:
    settings.aggregate_idle_sec = patch.aggregate_idle_sec


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
  the jobs you put side by side are the jobs that must be scored the
  same way. A path names its SUBTREE, so a readout at `bench`
  applies to every job under `bench/...` too — the same inheritance
  `arena_members` gives group operations.

  An arena is still derived state; nothing here creates one.
  Registering for a path no job names yet is legal and is in fact
  the useful order: register, then submit, and every instance scores
  itself on the way out."""

  def __init__(
    self,
    path: Path,
    by_arena: dict[str, list[ReadoutSpec]] | None = None,
    columns: dict[str, str] | None = None,
  ) -> None:
    self._path = path
    self._by_arena: dict[str, list[ReadoutSpec]] = by_arena or {}
    # arena → the SOURCE of a `columns(job)` function. Separate from
    # the readout list because it is a different kind of thing: one
    # per arena, job-level, and it runs in a resident process rather
    # than in each instance.
    self._columns: dict[str, str] = columns or {}

  @classmethod
  def load(cls, data_dir: Path) -> ReadoutRegistry:
    """A corrupt file raises rather than booting empty: the registry
    is operator intent that nothing else on disk can reconstruct, and
    silently dropping it would leave every new instance unscored."""
    path = data_dir / READOUTS_FILENAME
    if not path.is_file():
      return cls(path)
    try:
      raw = json.loads(path.read_text(encoding="utf-8"))
      by_arena: dict[str, list[ReadoutSpec]] = {}
      columns: dict[str, str] = {}
      for arena, node in raw.items():
        by_arena[arena] = [
          ReadoutSpec.model_validate(s).validated()
          for s in node.get("readouts") or []
        ]
        if node.get("columns"):
          # Validated on the way in as well as at registration: a
          # stored function that no longer meets the contract would
          # otherwise serve columns nobody can explain, and "it was
          # registered before the rule" is the grandfather path this
          # repo does not keep.
          check_source(
            str(node["columns"]), COLUMNS_NAME, also=(DESCRIPTIONS_NAME,)
          )
          columns[arena] = str(node["columns"])
    except Exception as exc:
      raise RuntimeError(
        f"readout registry {path} is unreadable ({exc}); refusing "
        f"to boot with silently-dropped readouts — fix or delete it"
      ) from exc
    return cls(path, by_arena, columns)

  def save(self) -> None:
    self._path.parent.mkdir(parents=True, exist_ok=True)
    tmp = self._path.with_suffix(".json.tmp")
    body: dict[str, dict[str, Any]] = {}
    for arena in sorted(set(self._by_arena) | set(self._columns)):
      node: dict[str, Any] = {
        "readouts": [
          s.model_dump(mode="json") for s in self._by_arena.get(arena, [])
        ]
      }
      if self._columns.get(arena):
        node["columns"] = self._columns[arena]
      body[arena] = node
    tmp.write_text(
      json.dumps(body, indent=2, sort_keys=True), encoding="utf-8"
    )
    tmp.replace(self._path)

  def snapshot(self) -> dict[str, dict[str, Any]]:
    return {
      arena: {
        "readouts": [
          s.model_dump(mode="json") for s in self._by_arena.get(arena, [])
        ],
        "columns": self._columns.get(arena, ""),
        "columns_sha256": _sha(self._columns.get(arena, "")),
      }
      for arena in sorted(set(self._by_arena) | set(self._columns))
    }

  def columns_node(self, arena: str) -> str:
    """WHERE the `columns` function serving this arena is registered,
    or empty. `appworld/v7` resolves to `appworld` when only the root
    defines one.

    The node, not the source text, is what a table should key a
    column on: a function edited in place is still the same column to
    an operator, and keying on the text would reset their column
    choices every time they tweaked it."""
    if not arena:
      return ""
    segments = arena.split("/")
    for i in range(len(segments), 0, -1):
      node = "/".join(segments[:i])
      if self._columns.get(node):
        return node
    return ""

  def columns_source(self, arena: str) -> str:
    """The nearest `columns` function on this arena's path, or empty.

    Nearest wins rather than erroring: unlike a readout name (which
    IS a column's identity), this is one function per arena deciding
    the whole column set, so a subtree overriding its parent is a
    coherent thing to want."""
    return self._columns.get(self.columns_node(arena), "")

  def set_columns(self, arena: str, source: str) -> None:
    if source:
      # Both functions, checked here: "I will document it later" is
      # exactly the state a column picker cannot render.
      check_source(source, COLUMNS_NAME, also=(DESCRIPTIONS_NAME,))
      self._columns[arena] = source
    else:
      self._columns.pop(arena, None)
    self.save()

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
    disagreed would be exactly the ones being compared."""
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
    """Stop computing it. Values already written stay on disk — they
    are the record of a finished experiment, not a cache."""
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


# ── service ──────────────────────────────────────────────────────


@dataclass
class _Target:
  """One finished instance and the readouts it still needs."""

  instance_id: str
  task_id: str
  state: str
  names: list[str]


@dataclass
class PassReport:
  """One retroactive container pass, for the CLI to print."""

  job_id: str
  instances: int
  written: int
  unreported: int
  remaining: int
  error: str = ""


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
    pool: AggregatePool | None = None,
  ) -> None:
    self._scheduler = scheduler
    self._registry = registry
    self._settings = settings
    self._runner = runner
    self._clock = clock_fn
    self._on_values_written = on_values_written
    self._pool = pool
    # job_id → readout name → instance_id → value.
    self._values: dict[str, dict[str, dict[str, ReadoutValue]]] = {}
    # (job_id, name) pairs read off disk. Per-NAME, so registering a
    # readout later still picks up values an earlier dispatcher
    # generation wrote for it.
    self._loaded: set[tuple[str, str]] = set()
    # Operator columns: the last answer per job, and whether its
    # input changed since. Values change in exactly ONE function
    # (`_append`), so marking dirty is one line and there is no
    # "did I miss an invalidation path" to worry about.
    self._columns: dict[str, dict[str, Any]] = {}
    self._dirty: set[str] = set()
    self._column_error: dict[str, str] = {}
    # What each `columns` function says its keys mean, keyed by the
    # source hash. Per SOURCE, not per job — the text is the same for
    # every job that resolves to it, and repeating it on each row
    # would put the same paragraph in a table cell N times.
    self._descriptions: dict[str, dict[str, str]] = {}

  @property
  def registry(self) -> ReadoutRegistry:
    return self._registry

  def specs_for_job(self, job_id: str) -> list[ReadoutSpec]:
    """What a job's instances should score themselves on. Read at
    dispatch and stamped into `instance.json`."""
    try:
      state = self._scheduler.job_state(job_id)
    except KeyError:
      return []
    return self._registry.for_arena(state.arena)

  # ── live path ────────────────────────────────────────────────

  async def record(
    self,
    job_id: str,
    task_id: str,
    instance_id: str,
    values: list[ReadoutValue],
  ) -> None:
    """Fold a worker's self-scored values into the job's column
    index. The entire live path.

    Called from the terminal pipeline with what the completion read
    already found on disk, so this adds one NFS append and no other
    work. Values the job does not register are dropped — the worker
    is told what to run, but the registry decides what counts."""
    if not values:
      return
    try:
      state = self._scheduler.job_state(job_id)
    except KeyError:
      return
    specs = self._registry.for_arena(state.arena)
    # Read the existing files once per (job, name) before adding to
    # them. Marking them loaded without reading would hide values an
    # earlier dispatcher generation wrote, and the job row would
    # report a lag that the retroactive pass then found nothing to
    # fix. In steady state `load_all` already did this at boot.
    await self._ensure_loaded(job_id, state, specs)
    # Keep a copy of the registered code beside the values it made.
    # The registry under --data-dir is the authority; this is the
    # durable record on shared storage, the same relationship
    # `.source.tar` has with git.
    await self._record_sources(state, specs)
    by_sha = {s.name: s.source_sha256 for s in specs}
    registered = set(by_sha)
    now = self._clock()
    by_name: dict[str, list[ReadoutValue]] = {}
    for value in values:
      if value.name not in registered:
        continue
      by_name.setdefault(value.name, []).append(
        value.model_copy(
          update={
            "instance_id": instance_id,
            "task_id": task_id or value.task_id,
            "at": now,
            "source_sha256": state.source_sha256,
            "image_id": state.image_id,
            "readout_sha256": by_sha.get(value.name, ""),
          }
        )
      )
    await self._append(job_id, state, by_name)

  async def _record_sources(
    self, state: JobState, specs: list[ReadoutSpec]
  ) -> None:
    def _write() -> None:
      for spec in specs:
        record_source(state.home_root, spec.name, spec.source)

    try:
      await asyncio.to_thread(_write)
    except OSError as exc:
      # A record, not a dependency: losing it costs provenance, not
      # a value, so it must not stop the value from being written.
      logger.warning(
        "readout source record failed job=%s: %s", state.job_id, exc
      )

  # ── retroactive path ─────────────────────────────────────────

  async def compute(
    self, job_ids: list[str], *, names: list[str] | None = None
  ) -> AsyncIterator[PassReport]:
    """Fill in missing values, one container pass at a time,
    yielding a report per pass so the caller can show progress.

    Sequential on purpose. This is an operator command, not a
    service: the cost of running it is the operator's to see, and a
    single ordered stream of passes needs no concurrency limit, no
    coalescing, and no starvation story."""
    for job_id in job_ids:
      while True:
        report = await self._one_pass(job_id, names)
        if report is None:
          break
        yield report
        if report.error or report.remaining <= 0:
          break
        if report.written == 0:
          # No progress and work remaining: stop rather than spin.
          # Every target already had its chance this pass.
          logger.warning(
            "readout: job=%s made no progress, %d pair(s) left",
            job_id,
            report.remaining,
          )
          break

  async def _one_pass(
    self, job_id: str, names: list[str] | None
  ) -> PassReport | None:
    try:
      state = self._scheduler.job_state(job_id)
    except KeyError:
      return None
    specs = [
      s
      for s in self._registry.for_arena(state.arena)
      if names is None or s.name in names
    ]
    if not specs:
      return None
    await self._ensure_loaded(job_id, state, specs)
    await self._record_sources(state, specs)
    targets = self._unscored(job_id, specs)
    if not targets:
      return None
    by_sha = {s.name: s.source_sha256 for s in specs}
    cap = max(1, self._settings.batch)
    batch, remaining = targets[:cap], max(0, len(targets) - cap)
    asked: dict[str, set[str]] = {}
    for target in batch:
      for name in target.names:
        asked.setdefault(name, set()).add(target.instance_id)
    total_asked = sum(len(ids) for ids in asked.values())

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
    try:
      stdout = await self._runner(
        state, self._settings.container_timeout_sec
      )
    except Exception as exc:
      logger.exception("readout pass failed job=%s", job_id)
      return PassReport(
        job_id=job_id,
        instances=len(batch),
        written=0,
        unreported=total_asked,
        remaining=remaining + len(batch),
        error=f"{type(exc).__name__}: {exc}",
      )

    now = self._clock()
    by_name: dict[str, list[ReadoutValue]] = {}
    for row in parse_result_lines(stdout):
      if row.instance_id not in asked.get(row.name, ()):
        # Never write a value nobody asked for: a confused runner
        # must not score instances outside its batch.
        continue
      by_name.setdefault(row.name, []).append(
        row.model_copy(
          update={
            "at": now,
            "source_sha256": state.source_sha256,
            "image_id": state.image_id,
            "readout_sha256": by_sha.get(row.name, ""),
          }
        )
      )
    written = await self._append(job_id, state, by_name)
    return PassReport(
      job_id=job_id,
      instances=len(batch),
      written=written,
      unreported=total_asked - written,
      remaining=remaining,
    )

  # ── shared ───────────────────────────────────────────────────

  async def _append(
    self,
    job_id: str,
    state: JobState,
    by_name: dict[str, list[ReadoutValue]],
  ) -> int:
    """Disk first, memory second: a failed append must leave the
    pair unscored so `lag` still names it."""
    written = 0
    for name, values in by_name.items():
      await asyncio.to_thread(append_values, state.home_root, name, values)
      index = self._values.setdefault(job_id, {}).setdefault(name, {})
      for value in values:
        index[value.instance_id] = value
      written += len(values)
    if written:
      # THE invalidation. Values change here and nowhere else, so
      # this one line is the whole staleness story — no timer
      # watches for drift and no path can forget to mark.
      self._dirty.add(job_id)
      if self._on_values_written is not None:
        self._on_values_written(job_id)
    return written

  # ── operator columns ─────────────────────────────────────────

  def _frame(
    self, job_id: str, specs: list[ReadoutSpec]
  ) -> dict[str, Any]:
    """The job as a columnar frame: row per finished instance.

    Columnar rather than records because it is half the bytes and
    the fastest path into a DataFrame on the other side (measured:
    3.3ms vs 5.9ms round trip at 1344 rows)."""
    view = self._scheduler.job_view(job_id)
    values = self._values.get(job_id, {})
    cols: dict[str, list[Any]] = {
      key: []
      for key in (
        "instance_id",
        "task_id",
        "state",
        "host",
        "dispatched_at",
        "finished_at",
        "duration_s",
      )
    }
    for spec in specs:
      cols[spec.name] = []
    errors = dict.fromkeys((s.name for s in specs), 0)
    for bucket in ("done_ok", "done_err"):
      for task_id, tv in getattr(view, bucket).items():
        cols["instance_id"].append(tv.instance_id)
        cols["task_id"].append(task_id)
        cols["state"].append(bucket)
        cols["host"].append(tv.host)
        cols["dispatched_at"].append(tv.dispatched_at.isoformat())
        cols["finished_at"].append(
          tv.finished_at.isoformat() if tv.finished_at else None
        )
        cols["duration_s"].append(
          (tv.finished_at - tv.dispatched_at).total_seconds()
          if tv.finished_at
          else None
        )
        for spec in specs:
          found = values.get(spec.name, {}).get(tv.instance_id)
          # A readout that raised and one that returned None are both
          # None here. `errors` keeps the distinction for the rare
          # column that needs it.
          cols[spec.name].append(
            None if found is None or not found.ok else found.value
          )
          if found is not None and not found.ok:
            errors[spec.name] += 1
    return {
      "columns": cols,
      "errors": {k: v for k, v in errors.items() if v},
      "done_ok": len(view.done_ok),
      "done_err": len(view.done_err),
    }

  def invalidate(self, job_ids: list[str]) -> None:
    """Mark columns as needing recomputation. Used when the
    `columns` function itself changes — the values did not move, but
    what they mean did."""
    self._dirty.update(job_ids)

  async def refresh(self, job_ids: list[str]) -> None:
    """Recompute operator columns for any of these jobs whose values
    changed. Called by read paths before they project — so the cost
    is paid at most once per change, and not at all for a job nobody
    is looking at."""
    if self._pool is None:
      return
    for job_id in job_ids:
      if job_id not in self._dirty:
        continue
      try:
        state = self._scheduler.job_state(job_id)
      except KeyError:
        self._dirty.discard(job_id)
        continue
      source = self._registry.columns_source(state.arena)
      if not source:
        self._dirty.discard(job_id)
        continue
      specs = self._registry.for_arena(state.arena)
      frame = self._frame(job_id, specs)
      try:
        values, described = await self._pool.compute(
          state, source=source, frame=frame
        )
        self._columns[job_id] = values
        self._descriptions[_sha(source)] = described
        missing = [k for k in values if k not in described]
        if missing:
          # The numbers are kept — they are the valuable half, and
          # hiding real data to punish missing documentation is the
          # wrong trade. But it has to be impossible to ignore.
          self._column_error[job_id] = (
            f"no description for: {', '.join(sorted(missing))}"
          )
        else:
          self._column_error.pop(job_id, None)
        # Cleared only on success: a failed refresh leaves the job
        # dirty so the next read tries again, and the last good
        # numbers stay visible marked stale.
        self._dirty.discard(job_id)
      except AggregateError as exc:
        self._column_error[job_id] = str(exc)
        logger.warning("columns failed job=%s: %s", job_id, exc)

  def _unscored(
    self, job_id: str, specs: list[ReadoutSpec]
  ) -> list[_Target]:
    """Finished instances missing at least one value, in completion
    order — so a truncated batch works through the oldest first."""
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

  async def load_many(self, job_ids: list[str]) -> None:
    """Pull values for these jobs off disk so `summary` can answer
    truthfully. A read, never a computation — this is what makes
    registering a readout able to say how much work it created."""
    for job_id in job_ids:
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
        continue
      # Columns are a cache and the cache is empty here — on boot
      # because the process is new, on registration because what the
      # values MEAN just changed. Either way every loaded job is
      # dirty. Without this a finished job never recomputes (no
      # further value will ever arrive to mark it) and its columns
      # would stay blank forever.
      self._dirty.add(job_id)

  async def load_all(self) -> None:
    """Boot: pull values for every job that has readouts, archived
    included.

    Archived is where a finished experiment's numbers are, and an
    unloaded job reports empty columns — which on a table reads as
    "this arena defines none" rather than "not read yet"."""
    await self.load_many(list(self._scheduler.iter_job_ids()))

  def summary(self, job_id: str) -> ReadoutJobSummary:
    """The job row's readout cell. Pure projection over what is
    already in memory — no disk, no await."""
    try:
      state = self._scheduler.job_state(job_id)
    except KeyError:
      return ReadoutJobSummary()
    specs = self._registry.for_arena(state.arena)
    columns_sha = _sha(self._registry.columns_source(state.arena))
    columns_node = self._registry.columns_node(state.arena)
    if not specs:
      return ReadoutJobSummary(
        lag=0,
        columns_source_sha256=columns_sha,
        columns_source_arena=columns_node,
      )
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
      # Values can include instances a requeue superseded, so this is
      # a lower bound on lag, not an exact count. It is an operator
      # signal ("run the retroactive pass"), not a ledger.
      lag += max(0, terminal - len(values))
    return ReadoutJobSummary(
      aggregates=aggregates,
      lag=None if unloaded else lag,
      # The operator's own columns, if this arena registered a
      # `columns` function. `aggregates` stays either way — n /
      # errors / nulls / lag are facts the operator should not have
      # to reproduce, and a column function is free to ignore them.
      columns=dict(self._columns.get(job_id, {})),
      # Stale covers both "the input moved" and "never computed":
      # either way what is on screen is not the answer. Empty AND
      # stale is the second case, which is what stops a blank cell
      # from reading as "this arena defines no columns".
      columns_stale=(
        bool(columns_sha)
        and (job_id in self._dirty or job_id not in self._columns)
      ),
      columns_error=self._column_error.get(job_id, ""),
      columns_source_sha256=columns_sha,
      columns_source_arena=columns_node,
    )

  def descriptions(self) -> dict[str, dict[str, str]]:
    """`{source arena: {key: text}}` — what the column picker shows
    beside each checkbox."""
    out: dict[str, dict[str, str]] = {}
    for node, source in self._registry.snapshot().items():
      described = self._descriptions.get(_sha(source.get("columns") or ""))
      if described:
        out[node] = dict(described)
    return out

  def jobs_with_lag(self, job_ids: list[str]) -> list[str]:
    return [j for j in job_ids if (self.summary(j).lag or 0) > 0]


# ── the retroactive container ────────────────────────────────────


class ReadoutRunError(RuntimeError):
  """The readout container could not be run at all."""


def build_readout_argv(state: JobState) -> list[str]:
  """`docker run` for one retroactive pass.

  Deliberately unlike an instance's container in three ways: the job
  home is mounted read-only (the dispatcher owns the column index),
  `container.extra_args` is NOT applied (a readout has no business
  claiming GPUs or the host network), and the command is the SDK
  runner rather than the job's own.

  It runs on the launcher. That host resolved the image at submit,
  the job home is on shared storage anyway, and host selection is
  complexity an operator-invoked command does not need."""
  spec = state.container
  argv = [
    "docker",
    "run",
    "--rm",
    "--label",
    f"{labels.READOUT}={state.job_id}",
    "-v",
    f"{state.home_root}:{JOB_MOUNT}:ro",
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
    ENV_JOB_DIR: JOB_MOUNT,
    ENV_REQUEST: f"{JOB_MOUNT}/{READOUT_DIRNAME}/{REQUEST_FILENAME}",
    **({ENV_SOURCE: SOURCE_MOUNT} if state.source_sha256 else {}),
  }
  for key, value in env.items():
    argv += ["-e", f"{key}={value}"]
  argv.append(state.image_id or spec.image)
  argv += protocol_command("dispatcher_sdk.readout")
  return argv


async def docker_readout_run(state: JobState, timeout_sec: float) -> str:
  """Run one pass on the launcher and return its stdout.

  Stdout is returned even on a non-zero exit: the runner emits one
  result line per pair as it goes, so a crash halfway still carries
  real values, and discarding them would make the batch's worst
  instance cost the whole batch. Pairs with no line stay unscored and
  keep showing up in `lag`.

  `start_new_session=True` — without it a tmux C-c on the dispatcher
  pane forwards SIGINT into an in-flight docker client."""
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


__all__ = [
  "BadReadout",
  "JOB_MOUNT",
  "PassReport",
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
]
