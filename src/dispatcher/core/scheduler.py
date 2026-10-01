"""Job-level weighted-round-robin scheduler.

Pure in-memory state machine: owns the RR cursor and the
pending/running/terminal partition per job. Never touches the
filesystem, ssh, or docker — the runtime executes each returned
DispatchEntry and feeds instance transitions back in.

Bucket semantics (see also JobView):
- running  → unknown | done_ok | done_err
- unknown  → running | ghosted | done_ok | done_err
- ghosted  → done_ok | done_err
Only the running→* edge releases a host/pool slot; unknown→running
re-acquires one. `unknown` and `ghosted` block both job drain
and archive — resolving them is what the resolver is for, and
draining past them would silently drop instances from the record.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from coolname import generate_slug

from dispatcher.core.models import (
  BlockReason,
  DispatchEntry,
  HostSettings,
  InstanceView,
  InstanceViewState,
  JobState,
  JobView,
)
from dispatcher.core.pick_host import pick_host

logger = logging.getLogger(__name__)


class AliasCollisionError(ValueError):
  def __init__(self, alias: str) -> None:
    super().__init__(alias)
    self.alias = alias


class AliasFormatError(ValueError):
  """Empty or over-length alias. Charset is unrestricted."""

  def __init__(self, alias: str) -> None:
    super().__init__(alias)
    self.alias = alias


class NotArchivableError(ValueError):
  """Job still has pending/running/unknown/ghosted instances.
  done_err does not disqualify."""

  def __init__(self, job_id: str, reason: str) -> None:
    super().__init__(f"{job_id}: {reason}")
    self.job_id = job_id
    self.reason = reason


class NotArchivedError(ValueError):
  def __init__(self, job_id: str) -> None:
    super().__init__(job_id)
    self.job_id = job_id


if TYPE_CHECKING:
  from collections.abc import Callable, Iterator
  from datetime import datetime

  from dispatcher.core.models import Outcome


@dataclass
class _JobRuntime:
  """Per-job bookkeeping. `pending` preserves task_ids order;
  other buckets are keyed by task_id. `outcomes` caches the
  parsed envelope per terminal task so read paths never re-hit
  NFS."""

  state: JobState
  pending: list[str] = field(default_factory=list)
  running: dict[str, InstanceView] = field(default_factory=dict)
  done_ok: dict[str, InstanceView] = field(default_factory=dict)
  done_err: dict[str, InstanceView] = field(default_factory=dict)
  ghosted: dict[str, InstanceView] = field(default_factory=dict)
  unknown: dict[str, InstanceView] = field(default_factory=dict)
  outcomes: dict[str, Outcome | None] = field(default_factory=dict)


class Scheduler:
  MAX_INFRA_RETRIES = 5
  """Requeue budget per task for host-killed instances. Past it the
  done_err stands — endless "infra" readings mean the reading is
  wrong."""

  _ALIAS_MAX_LEN = 120
  _ALIAS_GEN_TRIES = 8

  def __init__(
    self,
    *,
    max_concurrent: int,
    hosts: dict[str, HostSettings],
    clock: Callable[[], datetime],
    id_gen: Callable[[str], str],
    pool_caps: dict[str, int] | None = None,
  ) -> None:
    self._max_concurrent = max_concurrent
    # Pools: a job whose pool has no declared cap is
    # unbounded at this layer (global + host caps still apply).
    self._pool_caps: dict[str, int] = dict(pool_caps or {})
    self._pool_running: dict[str, int] = {}
    # Copy so external mutation of the caller's map can't skew
    # slot accounting.
    self._hosts: dict[str, HostSettings] = {
      h: s.model_copy() for h, s in hosts.items()
    }
    self._host_running: dict[str, int] = {host: 0 for host in hosts}
    self._clock = clock
    self._id_gen = id_gen

    self._jobs: dict[str, _JobRuntime] = {}
    # RR rotation (live jobs only) + full submission order
    # (live + archived; cancel removes from both).
    self._active_order: list[str] = []
    self._all_order: list[str] = []
    # Pre-serialized wire blob per archived job.
    self._archived_bytes: dict[str, bytes] = {}
    # job_id → task_id → infra-requeue count.
    self._infra_retries: dict[str, dict[str, int]] = {}
    self._cursor = 0
    self._turns_taken = 0
    self._alias_to_id: dict[str, str] = {}

  # ── mutations ──────────────────────────────────────────────

  def submit(self, job: JobState) -> None:
    """Register at the tail of the rotation. `job.alias` must
    already be minted — it has to live in the persisted submit
    event, or a restart re-mints the handle under the operator."""
    if job.job_id in self._jobs:
      raise ValueError(f"job already submitted: {job.job_id!r}")
    if not job.alias:
      raise ValueError(
        f"job {job.job_id!r} submitted with empty "
        f"alias — mint via mint_alias() before the submit event"
      )
    if job.alias in self._alias_to_id:
      raise AliasCollisionError(job.alias)
    self._alias_to_id[job.alias] = job.job_id
    self._jobs[job.job_id] = _JobRuntime(
      state=job, pending=list(job.task_ids)
    )
    self._active_order.append(job.job_id)
    self._all_order.append(job.job_id)

  def restore(
    self,
    job: JobState,
    *,
    running: dict[str, InstanceView],
    done_ok: dict[str, InstanceView],
    done_err: dict[str, InstanceView],
    ghosted: dict[str, InstanceView] | None = None,
    unknown: dict[str, InstanceView] | None = None,
  ) -> None:
    """Register with pre-populated buckets (startup restore).
    `pending` is derived as task_ids minus every bucketed task.

    Slot accounting: only `running` bumps host/pool counts —
    `unknown` released its slot when the death was observed. A
    restart-adopted unknown whose container is still executing is
    re-counted when the resolver moves it back to running; the
    server runs that resolver pass before dispatch starts."""
    if job.job_id in self._jobs:
      raise ValueError(f"job already submitted: {job.job_id!r}")
    if not job.alias:
      raise ValueError(
        f"job {job.job_id!r} restored with empty alias — the submit "
        f"event is supposed to carry one"
      )
    if job.alias in self._alias_to_id:
      raise AliasCollisionError(job.alias)
    self._alias_to_id[job.alias] = job.job_id
    ghosted = ghosted or {}
    unknown = unknown or {}
    reserved = (
      set(running)
      | set(done_ok)
      | set(done_err)
      | set(ghosted)
      | set(unknown)
    )
    pending = [t for t in job.task_ids if t not in reserved]
    self._jobs[job.job_id] = _JobRuntime(
      state=job,
      pending=pending,
      running=dict(running),
      done_ok=dict(done_ok),
      done_err=dict(done_err),
      ghosted=dict(ghosted),
      unknown=dict(unknown),
    )
    self._active_order.append(job.job_id)
    self._all_order.append(job.job_id)
    for tv in running.values():
      self._host_running[tv.host] = self._host_running.get(tv.host, 0) + 1
      self._pool_bump(job.job_id, 1)

  # ── alias ──────────────────────────────────────────────────

  def mint_alias(self, seed_id: str) -> str:
    """Fresh 2-word slug; deterministic-suffix fallback on
    repeated collision."""
    for _ in range(self._ALIAS_GEN_TRIES):
      candidate = generate_slug(2)
      if candidate not in self._alias_to_id:
        return candidate
    suffix = hashlib.sha256(seed_id.encode()).hexdigest()[:4]
    fallback = f"{generate_slug(2)}-{suffix}"
    while fallback in self._alias_to_id:
      fallback = (
        f"{generate_slug(2)}-"
        f"{hashlib.sha256(fallback.encode()).hexdigest()[:4]}"
      )
    return fallback

  def set_alias(self, job_id: str, new_alias: str) -> str:
    """Rename; returns the previous value. No-op rename is
    accepted."""
    runtime = self._jobs[job_id]
    if not new_alias or len(new_alias) > self._ALIAS_MAX_LEN:
      raise AliasFormatError(new_alias)
    current = runtime.state.alias
    if new_alias == current:
      return current
    other = self._alias_to_id.get(new_alias)
    if other is not None and other != job_id:
      raise AliasCollisionError(new_alias)
    if current and self._alias_to_id.get(current) == job_id:
      del self._alias_to_id[current]
    self._alias_to_id[new_alias] = job_id
    runtime.state.alias = new_alias
    return current

  def job_id_of_alias(self, alias: str) -> str | None:
    return self._alias_to_id.get(alias)

  # ── knobs ──────────────────────────────────────────────────

  def patch(self, job_id: str, **fields: object) -> None:
    """Mutate scheduler knobs. A pool change moves the job's
    running count between pool ledgers, or completions would
    decrement the wrong pool and drift it permanently."""
    runtime = self._jobs[job_id]
    for key, value in fields.items():
      if key not in _KNOB_FIELDS:
        raise ValueError(f"unknown scheduler knob: {key!r}")
      if key == "pool":
        old_pool = runtime.state.pool or "default"
        new_pool = (
          value if isinstance(value, str) else "default"
        ) or "default"
        if new_pool != old_pool:
          n_running = len(runtime.running)
          if n_running > 0:
            self._pool_running[old_pool] = max(
              0, self._pool_running.get(old_pool, 0) - n_running
            )
            self._pool_running[new_pool] = (
              self._pool_running.get(new_pool, 0) + n_running
            )
      setattr(runtime.state, key, value)

  def cancel(self, job_id: str) -> _JobRuntime:
    """Drop the job; returns its runtime so the caller can
    kill remote instances. Slots of still-running instances are
    released."""
    runtime = self._jobs.pop(job_id)
    alias = runtime.state.alias
    if alias and self._alias_to_id.get(alias) == job_id:
      del self._alias_to_id[alias]
    if job_id in self._active_order:
      self._remove_from_rotation(job_id)
    if job_id in self._all_order:
      self._all_order.remove(job_id)
    self._archived_bytes.pop(job_id, None)
    pool = runtime.state.pool or "default"
    for tv in runtime.running.values():
      self._host_running[tv.host] = max(
        0, self._host_running.get(tv.host, 0) - 1
      )
      self._pool_running[pool] = max(
        0, self._pool_running.get(pool, 0) - 1
      )
    return runtime

  def _remove_from_rotation(self, job_id: str) -> None:
    """Remove from _active_order preserving the cursor invariant:
    removal before the cursor shifts it; removal at the cursor
    lets the next job slide in; clamp on empty."""
    cur_index = self._active_order.index(job_id)
    self._active_order.remove(job_id)
    if not self._active_order:
      self._cursor = 0
    elif cur_index < self._cursor:
      self._cursor -= 1
    else:
      self._cursor = self._cursor % len(self._active_order)

  # ── archive ────────────────────────────────────────────────

  def archive_job(
    self,
    job_id: str,
    *,
    at: datetime,
    kind: str,
    payload_bytes: bytes,
  ) -> None:
    """Freeze a fully-terminal job off the dispatch cursor and
    the live serialization path. Idempotent (bytes refresh)."""
    runtime = self._jobs[job_id]
    if runtime.pending:
      raise NotArchivableError(
        job_id, f"{len(runtime.pending)} instance(s) still pending"
      )
    if runtime.running:
      raise NotArchivableError(
        job_id, f"{len(runtime.running)} instance(s) still running"
      )
    if runtime.unknown:
      raise NotArchivableError(
        job_id,
        f"{len(runtime.unknown)} instance(s) in unknown "
        "(resolver pending)",
      )
    if runtime.ghosted:
      raise NotArchivableError(
        job_id, f"{len(runtime.ghosted)} instance(s) in ghosted"
      )
    kind = kind or "manual"
    if kind not in ("manual", "auto"):
      raise ValueError(
        f"archive kind must be 'manual' or 'auto', got {kind!r}"
      )
    runtime.state.archived_at = at
    runtime.state.archive_kind = kind
    self._archived_bytes[job_id] = payload_bytes
    if job_id in self._active_order:
      self._remove_from_rotation(job_id)

  def unarchive_job(self, job_id: str) -> None:
    runtime = self._jobs[job_id]
    if runtime.state.archived_at is None:
      raise NotArchivedError(job_id)
    runtime.state.archived_at = None
    runtime.state.archive_kind = ""
    self._archived_bytes.pop(job_id, None)
    if job_id not in self._active_order:
      self._active_order.append(job_id)

  def is_archived(self, job_id: str) -> bool:
    runtime = self._jobs.get(job_id)
    return runtime is not None and runtime.state.archived_at is not None

  def archived_bytes(self, job_id: str) -> bytes:
    return self._archived_bytes[job_id]

  def all_job_ids(self) -> list[str]:
    """Every job (live + archived) in submission order."""
    return list(self._all_order)

  # ── pools ──────────────────────────────────────────────────

  def _pool_of(self, job_id: str) -> str:
    return self._jobs[job_id].state.pool or "default"

  def _pool_bump(self, job_id: str, delta: int) -> None:
    pool = self._pool_of(job_id)
    self._pool_running[pool] = max(
      0, self._pool_running.get(pool, 0) + delta
    )

  def pool_running_snapshot(self) -> dict[str, int]:
    return {p: n for p, n in self._pool_running.items() if n > 0}

  def pool_caps_snapshot(self) -> dict[str, int]:
    return dict(self._pool_caps)

  def set_pool_caps(self, caps: dict[str, int]) -> dict[str, int]:
    """Whole-dict replacement; returns the previous caps. A cap
    below the current running count stops new dispatch until the
    count drops — running instances are never killed."""
    for name, cap in caps.items():
      if cap < 0:
        raise ValueError(
          f"pool cap must be >= 0, got {cap} for pool {name!r}"
        )
    old = dict(self._pool_caps)
    self._pool_caps = dict(caps)
    return old

  # ── cluster knobs ──────────────────────────────────────────

  def set_max_concurrent(self, cap: int) -> int:
    if cap < 0:
      raise ValueError(f"cap must be >= 0, got {cap}")
    old = self._max_concurrent
    self._max_concurrent = cap
    return old

  def set_host_settings(
    self,
    host: str,
    *,
    max_concurrent: int | None = None,
    active: bool | None = None,
    alive: bool | None = None,
  ) -> HostSettings:
    """Merge-update one host (new hosts default to cap 0 — present
    but idle until the operator sets a cap). Returns the new
    settings."""
    existing = self._hosts.get(host)
    if existing is None:
      new = HostSettings(
        max_concurrent=(
          max_concurrent if max_concurrent is not None else 0
        ),
        active=active if active is not None else True,
        alive=alive if alive is not None else True,
      )
    else:
      new = HostSettings(
        max_concurrent=(
          existing.max_concurrent
          if max_concurrent is None
          else max_concurrent
        ),
        active=existing.active if active is None else active,
        alive=existing.alive if alive is None else alive,
      )
    self._hosts[host] = new
    self._host_running.setdefault(host, 0)
    return new

  def host_settings(self, host: str) -> HostSettings:
    return self._hosts[host]

  def all_host_settings(self) -> dict[str, HostSettings]:
    return dict(self._hosts)

  # ── dispatch ───────────────────────────────────────────────

  def dispatch_one(self) -> DispatchEntry | None:
    """Dispatch one instance, advancing the RR cursor one turn
    against the job's weight. None when nothing is
    dispatchable (caps, pauses, no hosts, no work)."""
    if self.running_total >= self._max_concurrent:
      return None
    if not self._active_order:
      return None

    picked = self._find_next_dispatchable_job()
    if picked is None:
      return None
    aid, runtime = picked

    host = pick_host(self._hosts, self._host_running)
    if host is None:
      return None

    task_id = runtime.pending.pop(0)
    instance_id = self._id_gen(task_id)
    dispatched_at = self._clock()
    runtime.running[task_id] = InstanceView(
      task_id=task_id,
      state="running",
      instance_id=instance_id,
      host=host,
      dispatched_at=dispatched_at,
    )
    self._host_running[host] = self._host_running.get(host, 0) + 1
    self._pool_bump(aid, 1)

    self._turns_taken += 1
    if self._turns_taken >= max(1, runtime.state.weight):
      self._advance_cursor()

    return DispatchEntry(
      job_id=aid,
      task_id=task_id,
      instance_id=instance_id,
      host=host,
      dispatched_at=dispatched_at,
    )

  # ── transitions ────────────────────────────────────────────

  def transition_instance(
    self,
    *,
    job_id: str,
    task_id: str,
    from_state: InstanceViewState,
    to_state: InstanceViewState,
    outcome: Outcome | None = None,
  ) -> bool:
    """The single bucket-move primitive for live completion and
    resolver reclassification. Owns edge validation, slot
    accounting, the outcome cache, and the pause-on-error
    postcondition. Returns whether this transition newly
    auto-paused the job."""
    allowed_targets = {
      "running": {"unknown", "done_ok", "done_err"},
      "unknown": {"running", "ghosted", "done_ok", "done_err"},
      "ghosted": {"done_ok", "done_err"},
    }
    if from_state not in allowed_targets:
      raise ValueError(f"unsupported instance source: {from_state!r}")
    if to_state not in allowed_targets[from_state]:
      raise ValueError(
        f"unsupported instance transition: {from_state!r} → {to_state!r}"
      )

    runtime = self._jobs[job_id]
    source = getattr(runtime, from_state)
    instance_view = source.pop(task_id)
    host = instance_view.host
    if from_state == "running":
      self._host_running[host] = max(
        0, self._host_running.get(host, 0) - 1
      )
      self._pool_bump(job_id, -1)

    finished_at = instance_view.finished_at
    if to_state == "running":
      # unknown→running: the resolver found the container alive, so
      # it never finished.
      finished_at = None
    elif from_state == "running" and finished_at is None:
      # First exit from running is when it stopped running; a later
      # unknown→done_ok reclassification must not restamp it, or
      # every duration would grow by the resolver's lag.
      finished_at = self._clock()

    destination = getattr(runtime, to_state)
    destination[task_id] = InstanceView(
      task_id=task_id,
      state=to_state,
      instance_id=instance_view.instance_id,
      host=host,
      dispatched_at=instance_view.dispatched_at,
      finished_at=finished_at,
    )
    if to_state == "running":
      self._host_running[host] = self._host_running.get(host, 0) + 1
      self._pool_bump(job_id, 1)
    else:
      runtime.outcomes[task_id] = outcome

    return self._apply_pause_on_error(job_id, to_state)

  def instance_view_in(
    self, job_id: str, state: InstanceViewState, task_id: str
  ) -> InstanceView | None:
    """The InstanceView sitting in one bucket, or None — lets event
    consumers read host/dispatched_at back after a transition."""
    runtime = self._jobs.get(job_id)
    if runtime is None:
      return None
    bucket = getattr(runtime, state, None)
    if not isinstance(bucket, dict):
      return None
    return bucket.get(task_id)

  def job_outcomes(self, job_id: str) -> list[Outcome]:
    """Every cached non-None Outcome, in task-id order."""
    runtime = self._jobs[job_id]
    return [
      o for _, o in sorted(runtime.outcomes.items()) if o is not None
    ]

  def outcome_of(self, job_id: str, task_id: str) -> Outcome | None:
    runtime = self._jobs.get(job_id)
    if runtime is None:
      return None
    return runtime.outcomes.get(task_id)

  def seed_outcome(
    self, job_id: str, task_id: str, outcome: Outcome
  ) -> None:
    """Startup restore: preload a completed instance's parsed
    envelope. Idempotent."""
    self._jobs[job_id].outcomes[task_id] = outcome

  # ── requeue / retry / reclaim ──────────────────────────────

  def requeue_after_infra_failure(
    self,
    job_id: str,
    task_id: str,
    from_state: InstanceViewState = "running",
  ) -> bool:
    """Move a host-killed instance back to pending instead of
    scoring it. Callable from `running` (live path) AND from
    `unknown`/`ghosted` (resolver path — under load the outcome is
    rarely visible at first look, and a requeue rule that only the
    live path applies silently scores the common case). One retry
    budget across all entry points; returns False once exhausted
    or when the instance already left the source bucket."""
    seen = self._infra_retries.setdefault(job_id, {})
    if seen.get(task_id, 0) >= self.MAX_INFRA_RETRIES:
      return False
    reclaimed = (
      self.reclaim_from_running(job_id, task_id)
      if from_state == "running"
      else self.reclaim_from_parked(job_id, task_id, from_state)
    )
    if not reclaimed:
      return False
    seen[task_id] = seen.get(task_id, 0) + 1
    logger.warning(
      "infra failure requeued (%d/%d): job=%s task=%s",
      seen[task_id],
      self.MAX_INFRA_RETRIES,
      job_id,
      task_id,
    )
    return True

  def retry_from_done_err(self, job_id: str, task_id: str) -> bool:
    """Operator retry: done_err → pending (outcome cache entry
    dropped). Re-dispatch mints a fresh instance_id, so the old
    instance dir is never reused."""
    runtime = self._jobs[job_id]
    if task_id not in runtime.done_err:
      return False
    runtime.done_err.pop(task_id)
    runtime.outcomes.pop(task_id, None)
    self._rebuild_pending(runtime)
    return True

  def reclaim_from_running(self, job_id: str, task_id: str) -> bool:
    """Undo a dispatch: running → pending, releasing the slot.
    False when the instance already left running (completion won the
    race — treat as no-op)."""
    runtime = self._jobs[job_id]
    if task_id not in runtime.running:
      return False
    tv = runtime.running.pop(task_id)
    self._host_running[tv.host] = max(
      0, self._host_running.get(tv.host, 0) - 1
    )
    self._pool_bump(job_id, -1)
    self._rebuild_pending(runtime)
    return True

  def reclaim_from_parked(
    self,
    job_id: str,
    task_id: str,
    from_state: InstanceViewState,
  ) -> bool:
    """`reclaim_from_running` for unknown/ghosted — with NO slot
    release: the slot went back when the instance left `running`, and
    releasing again would let the host over-dispatch for the rest
    of the run."""
    if from_state not in ("unknown", "ghosted"):
      raise ValueError(
        f"reclaim_from_parked expects unknown/ghosted, got {from_state!r}"
      )
    runtime = self._jobs[job_id]
    bucket = getattr(runtime, from_state)
    if task_id not in bucket:
      return False
    bucket.pop(task_id)
    self._rebuild_pending(runtime)
    return True

  def _rebuild_pending(self, runtime: _JobRuntime) -> None:
    """Recompute pending from task_ids order minus everything
    still bucketed — keeps requeue insertion at list position and
    self-heals any drift."""
    dispatched = (
      set(runtime.running)
      | set(runtime.done_ok)
      | set(runtime.done_err)
      | set(runtime.ghosted)
      | set(runtime.unknown)
    )
    runtime.pending = [
      t for t in runtime.state.task_ids if t not in dispatched
    ]

  # ── observers ──────────────────────────────────────────────

  @property
  def running_total(self) -> int:
    return sum(len(r.running) for r in self._jobs.values())

  def running_per_host(self) -> dict[str, int]:
    return dict(self._host_running)

  def job_view(self, job_id: str) -> JobView:
    runtime = self._jobs[job_id]
    return JobView(
      pending=list(runtime.pending),
      running=dict(runtime.running),
      done_ok=dict(runtime.done_ok),
      done_err=dict(runtime.done_err),
      ghosted=dict(runtime.ghosted),
      unknown=dict(runtime.unknown),
    )

  def terminal_count(self, job_id: str) -> int:
    """done_ok + done_err, without building a JobView.

    `job_view` copies six dicts, which on a 1300-instance job is
    real work to repeat inside a row projection that already called
    it once. Readout lag needs only this number."""
    runtime = self._jobs[job_id]
    return len(runtime.done_ok) + len(runtime.done_err)

  def job_state(self, job_id: str) -> JobState:
    return self._jobs[job_id].state

  def job_paused(self, job_id: str) -> bool:
    return self._jobs[job_id].state.paused

  def has_job(self, job_id: str) -> bool:
    return job_id in self._jobs

  def iter_job_ids(self) -> Iterator[str]:
    yield from self._jobs

  def iter_running(self) -> Iterator[tuple[str, InstanceView]]:
    for aid, runtime in self._jobs.items():
      for instance_view in runtime.running.values():
        yield aid, instance_view

  def iter_unknown(self) -> Iterator[tuple[str, str, InstanceView]]:
    """Snapshotted so callers may reclassify during iteration."""
    for aid, runtime in list(self._jobs.items()):
      for task_id, tv in list(runtime.unknown.items()):
        yield aid, task_id, tv

  def iter_ghosted(self) -> Iterator[tuple[str, str, InstanceView]]:
    for aid, runtime in list(self._jobs.items()):
      for task_id, tv in list(runtime.ghosted.items()):
        yield aid, task_id, tv

  def has_work(self) -> bool:
    """unknown and ghosted both count as work: they must be
    resolved into evidence-based terminal states, never silently
    dropped."""
    for runtime in self._jobs.values():
      if (
        runtime.pending
        or runtime.running
        or runtime.unknown
        or runtime.ghosted
      ):
        return True
    return False

  # ── internals ──────────────────────────────────────────────

  def _find_next_dispatchable_job(
    self,
  ) -> tuple[str, _JobRuntime] | None:
    """Advance the cursor to the next dispatchable job (one
    full rotation max). Skipping resets owed turns."""
    n = len(self._active_order)
    for _ in range(n):
      aid = self._active_order[self._cursor]
      runtime = self._jobs[aid]
      if self._job_dispatchable(runtime):
        return aid, runtime
      self._advance_cursor()
    return None

  def _job_dispatchable(self, runtime: _JobRuntime) -> bool:
    return self._job_block_reason(runtime) is None

  def _job_block_reason(self, runtime: _JobRuntime) -> BlockReason | None:
    """Why the cursor skips this job, or None if it would dispatch.

    `_job_dispatchable` is defined as "no reason", so the answer the
    operator reads can never drift from the decision the scheduler
    made — they are one function."""
    if runtime.state.paused:
      return "paused"
    if not runtime.pending:
      return "no_pending"
    if runtime.state.max_concurrent is not None:
      if len(runtime.running) >= runtime.state.max_concurrent:
        return "job_cap"
    pool = runtime.state.pool or "default"
    pool_cap = self._pool_caps.get(pool)
    if pool_cap is not None:
      if self._pool_running.get(pool, 0) >= pool_cap:
        return "pool_cap"
    # Unresolved instances might still become done_err; dispatching
    # ahead of that resolution would race the pause the operator
    # asked for.
    if self._effective_pause_on_error(runtime.state):
      if runtime.unknown or runtime.ghosted:
        return "awaiting_resolution"
    return None

  def block_reason(self, job_id: str) -> BlockReason | None:
    """What holds this job's pending work back right now, or None
    when it is simply waiting its turn in the rotation.

    Layered in the order `dispatch_one` checks them, so the reason
    is the one that would actually stop this job: the global cap is
    read before any job is picked, the host fleet after."""
    runtime = self._jobs[job_id]
    if not runtime.pending:
      return None
    if self.running_total >= self._max_concurrent:
      return "global_cap"
    own = self._job_block_reason(runtime)
    if own is not None:
      return None if own == "no_pending" else own
    if pick_host(self._hosts, self._host_running) is None:
      return "no_host"
    return None

  def _advance_cursor(self) -> None:
    if not self._active_order:
      return
    self._cursor = (self._cursor + 1) % len(self._active_order)
    self._turns_taken = 0

  @staticmethod
  def _effective_pause_on_error(state: JobState) -> bool:
    if state.pause_on_error is not None:
      return state.pause_on_error
    return state.max_concurrent == 1

  def _apply_pause_on_error(
    self, job_id: str, to_state: InstanceViewState
  ) -> bool:
    """Returns True only when this transition flipped paused
    False→True. Only done_err with pending work triggers —
    unknown must NOT pause (an NFS-lag false negative would pause
    every sequential job)."""
    runtime = self._jobs[job_id]
    if (
      to_state != "done_err"
      or not runtime.pending
      or not self._effective_pause_on_error(runtime.state)
    ):
      return False
    if runtime.state.paused:
      return False
    runtime.state.paused = True
    return True


_KNOB_FIELDS = frozenset(
  {
    "paused",
    "weight",
    "max_concurrent",
    "pause_on_error",
    "arena",
    "pool",
  }
)
