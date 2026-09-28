"""Attempt-level weighted-round-robin scheduler.

Pure in-memory state machine: owns the RR cursor and the
pending/running/terminal partition per attempt. Never touches the
filesystem, ssh, or docker — the runtime executes each returned
DispatchEntry and feeds trial transitions back in.

Bucket semantics (see also AttemptView):
- running  → unknown | done_ok | done_err
- unknown  → running | ghosted | done_ok | done_err
- ghosted  → done_ok | done_err
Only the running→* edge releases a host/pool slot; unknown→running
re-acquires one. `unknown` and `ghosted` block both attempt drain
and archive — resolving them is the resolver's job, and draining
past them would silently drop trials from the record.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from coolname import generate_slug

from dispatcher.models import (
  AttemptState,
  AttemptView,
  DispatchEntry,
  HostSettings,
  TrialView,
  TrialViewState,
)
from dispatcher.pick_host import pick_host

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
  """Attempt still has pending/running/unknown/ghosted trials.
  done_err does not disqualify."""

  def __init__(self, attempt_id: str, reason: str) -> None:
    super().__init__(f"{attempt_id}: {reason}")
    self.attempt_id = attempt_id
    self.reason = reason


class NotArchivedError(ValueError):
  def __init__(self, attempt_id: str) -> None:
    super().__init__(attempt_id)
    self.attempt_id = attempt_id


if TYPE_CHECKING:
  from collections.abc import Callable, Iterator
  from datetime import datetime

  from dispatcher.models import Outcome


@dataclass
class _AttemptRuntime:
  """Per-attempt bookkeeping. `pending` preserves task_list order;
  other buckets are keyed by task_name. `outcomes` caches the
  parsed envelope per terminal task so read paths never re-hit
  NFS."""

  state: AttemptState
  pending: list[str] = field(default_factory=list)
  running: dict[str, TrialView] = field(default_factory=dict)
  done_ok: dict[str, TrialView] = field(default_factory=dict)
  done_err: dict[str, TrialView] = field(default_factory=dict)
  ghosted: dict[str, TrialView] = field(default_factory=dict)
  unknown: dict[str, TrialView] = field(default_factory=dict)
  outcomes: dict[str, Outcome | None] = field(default_factory=dict)


class Scheduler:
  MAX_INFRA_RETRIES = 5
  """Requeue budget per task for host-killed trials. Past it the
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
    name_gen: Callable[[str], str],
    pool_caps: dict[str, int] | None = None,
  ) -> None:
    self._max_concurrent = max_concurrent
    # Pools: an attempt whose pool has no declared cap is
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
    self._name_gen = name_gen

    self._attempts: dict[str, _AttemptRuntime] = {}
    # RR rotation (live attempts only) + full submission order
    # (live + archived; cancel removes from both).
    self._active_order: list[str] = []
    self._all_order: list[str] = []
    # Pre-serialized wire blob per archived attempt.
    self._archived_bytes: dict[str, bytes] = {}
    # attempt_id → task_name → infra-requeue count.
    self._infra_retries: dict[str, dict[str, int]] = {}
    self._cursor = 0
    self._turns_taken = 0
    self._alias_to_id: dict[str, str] = {}

  # ── mutations ──────────────────────────────────────────────

  def submit(self, attempt: AttemptState) -> None:
    """Register at the tail of the rotation. `attempt.alias` must
    already be minted — it has to live in the persisted submit
    event, or a restart re-mints the handle under the operator."""
    if attempt.attempt_id in self._attempts:
      raise ValueError(
        f"attempt already submitted: {attempt.attempt_id!r}"
      )
    if not attempt.alias:
      raise ValueError(
        f"attempt {attempt.attempt_id!r} submitted with empty "
        f"alias — mint via mint_alias() before the submit event"
      )
    if attempt.alias in self._alias_to_id:
      raise AliasCollisionError(attempt.alias)
    self._alias_to_id[attempt.alias] = attempt.attempt_id
    self._attempts[attempt.attempt_id] = _AttemptRuntime(
      state=attempt, pending=list(attempt.task_list)
    )
    self._active_order.append(attempt.attempt_id)
    self._all_order.append(attempt.attempt_id)

  def restore(
    self,
    attempt: AttemptState,
    *,
    running: dict[str, TrialView],
    done_ok: dict[str, TrialView],
    done_err: dict[str, TrialView],
    ghosted: dict[str, TrialView] | None = None,
    unknown: dict[str, TrialView] | None = None,
  ) -> None:
    """Register with pre-populated buckets (startup restore).
    `pending` is derived as task_list minus every bucketed task.

    Slot accounting: only `running` bumps host/pool counts —
    `unknown` released its slot when the death was observed. A
    restart-adopted unknown whose container is still executing is
    re-counted when the resolver moves it back to running; the
    server runs that resolver pass before dispatch starts."""
    if attempt.attempt_id in self._attempts:
      raise ValueError(
        f"attempt already submitted: {attempt.attempt_id!r}"
      )
    # Pre-alias logs replay with alias="" — mint here; the caller
    # persists the backfill.
    if not attempt.alias:
      attempt.alias = self.mint_alias(attempt.attempt_id)
    elif attempt.alias in self._alias_to_id:
      raise AliasCollisionError(attempt.alias)
    self._alias_to_id[attempt.alias] = attempt.attempt_id
    ghosted = ghosted or {}
    unknown = unknown or {}
    reserved = (
      set(running)
      | set(done_ok)
      | set(done_err)
      | set(ghosted)
      | set(unknown)
    )
    pending = [t for t in attempt.task_list if t not in reserved]
    self._attempts[attempt.attempt_id] = _AttemptRuntime(
      state=attempt,
      pending=pending,
      running=dict(running),
      done_ok=dict(done_ok),
      done_err=dict(done_err),
      ghosted=dict(ghosted),
      unknown=dict(unknown),
    )
    self._active_order.append(attempt.attempt_id)
    self._all_order.append(attempt.attempt_id)
    for tv in running.values():
      self._host_running[tv.host] = self._host_running.get(tv.host, 0) + 1
      self._pool_bump(attempt.attempt_id, 1)

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

  def set_alias(self, attempt_id: str, new_alias: str) -> str:
    """Rename; returns the previous value. No-op rename is
    accepted."""
    runtime = self._attempts[attempt_id]
    if not new_alias or len(new_alias) > self._ALIAS_MAX_LEN:
      raise AliasFormatError(new_alias)
    current = runtime.state.alias
    if new_alias == current:
      return current
    other = self._alias_to_id.get(new_alias)
    if other is not None and other != attempt_id:
      raise AliasCollisionError(new_alias)
    if current and self._alias_to_id.get(current) == attempt_id:
      del self._alias_to_id[current]
    self._alias_to_id[new_alias] = attempt_id
    runtime.state.alias = new_alias
    return current

  def attempt_id_of_alias(self, alias: str) -> str | None:
    return self._alias_to_id.get(alias)

  # ── knobs ──────────────────────────────────────────────────

  def patch(self, attempt_id: str, **fields: object) -> None:
    """Mutate scheduler knobs. A pool change moves the attempt's
    running count between pool ledgers, or completions would
    decrement the wrong pool and drift it permanently."""
    runtime = self._attempts[attempt_id]
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

  def cancel(self, attempt_id: str) -> _AttemptRuntime:
    """Drop the attempt; returns its runtime so the caller can
    kill remote trials. Slots of still-running trials are
    released."""
    runtime = self._attempts.pop(attempt_id)
    alias = runtime.state.alias
    if alias and self._alias_to_id.get(alias) == attempt_id:
      del self._alias_to_id[alias]
    if attempt_id in self._active_order:
      self._remove_from_rotation(attempt_id)
    if attempt_id in self._all_order:
      self._all_order.remove(attempt_id)
    self._archived_bytes.pop(attempt_id, None)
    pool = runtime.state.pool or "default"
    for tv in runtime.running.values():
      self._host_running[tv.host] = max(
        0, self._host_running.get(tv.host, 0) - 1
      )
      self._pool_running[pool] = max(
        0, self._pool_running.get(pool, 0) - 1
      )
    return runtime

  def _remove_from_rotation(self, attempt_id: str) -> None:
    """Remove from _active_order preserving the cursor invariant:
    removal before the cursor shifts it; removal at the cursor
    lets the next attempt slide in; clamp on empty."""
    cur_index = self._active_order.index(attempt_id)
    self._active_order.remove(attempt_id)
    if not self._active_order:
      self._cursor = 0
    elif cur_index < self._cursor:
      self._cursor -= 1
    else:
      self._cursor = self._cursor % len(self._active_order)

  # ── archive ────────────────────────────────────────────────

  def archive_attempt(
    self,
    attempt_id: str,
    *,
    at: datetime,
    kind: str,
    payload_bytes: bytes,
  ) -> None:
    """Freeze a fully-terminal attempt off the dispatch cursor and
    the live serialization path. Idempotent (bytes refresh)."""
    runtime = self._attempts[attempt_id]
    if runtime.pending:
      raise NotArchivableError(
        attempt_id, f"{len(runtime.pending)} trial(s) still pending"
      )
    if runtime.running:
      raise NotArchivableError(
        attempt_id, f"{len(runtime.running)} trial(s) still running"
      )
    if runtime.unknown:
      raise NotArchivableError(
        attempt_id,
        f"{len(runtime.unknown)} trial(s) in unknown (resolver pending)",
      )
    if runtime.ghosted:
      raise NotArchivableError(
        attempt_id, f"{len(runtime.ghosted)} trial(s) in ghosted"
      )
    kind = kind or "manual"
    if kind not in ("manual", "auto"):
      raise ValueError(
        f"archive kind must be 'manual' or 'auto', got {kind!r}"
      )
    runtime.state.archived_at = at
    runtime.state.archive_kind = kind
    self._archived_bytes[attempt_id] = payload_bytes
    if attempt_id in self._active_order:
      self._remove_from_rotation(attempt_id)

  def unarchive_attempt(self, attempt_id: str) -> None:
    runtime = self._attempts[attempt_id]
    if runtime.state.archived_at is None:
      raise NotArchivedError(attempt_id)
    runtime.state.archived_at = None
    runtime.state.archive_kind = ""
    self._archived_bytes.pop(attempt_id, None)
    if attempt_id not in self._active_order:
      self._active_order.append(attempt_id)

  def is_archived(self, attempt_id: str) -> bool:
    runtime = self._attempts.get(attempt_id)
    return runtime is not None and runtime.state.archived_at is not None

  def archived_bytes(self, attempt_id: str) -> bytes:
    return self._archived_bytes[attempt_id]

  def all_attempt_ids(self) -> list[str]:
    """Every attempt (live + archived) in submission order."""
    return list(self._all_order)

  # ── pools ──────────────────────────────────────────────────

  def _pool_of(self, attempt_id: str) -> str:
    return self._attempts[attempt_id].state.pool or "default"

  def _pool_bump(self, attempt_id: str, delta: int) -> None:
    pool = self._pool_of(attempt_id)
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
    count drops — running trials are never killed."""
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
    """Dispatch one trial, advancing the RR cursor one turn
    against the attempt's weight. None when nothing is
    dispatchable (caps, pauses, no hosts, no work)."""
    if self.running_total >= self._max_concurrent:
      return None
    if not self._active_order:
      return None

    picked = self._find_next_dispatchable_attempt()
    if picked is None:
      return None
    aid, runtime = picked

    host = pick_host(self._hosts, self._host_running)
    if host is None:
      return None

    task_name = runtime.pending.pop(0)
    trial_name = self._name_gen(task_name)
    dispatched_at = self._clock()
    runtime.running[task_name] = TrialView(
      task_name=task_name,
      state="running",
      trial_name=trial_name,
      host=host,
      dispatched_at=dispatched_at,
    )
    self._host_running[host] = self._host_running.get(host, 0) + 1
    self._pool_bump(aid, 1)

    self._turns_taken += 1
    if self._turns_taken >= max(1, runtime.state.weight):
      self._advance_cursor()

    return DispatchEntry(
      attempt_id=aid,
      task_name=task_name,
      trial_name=trial_name,
      host=host,
      dispatched_at=dispatched_at,
    )

  # ── transitions ────────────────────────────────────────────

  def transition_trial(
    self,
    *,
    attempt_id: str,
    task_name: str,
    from_state: TrialViewState,
    to_state: TrialViewState,
    outcome: Outcome | None = None,
  ) -> bool:
    """The single bucket-move primitive for live completion and
    resolver reclassification. Owns edge validation, slot
    accounting, the outcome cache, and the pause-on-error
    postcondition. Returns whether this transition newly
    auto-paused the attempt."""
    allowed_targets = {
      "running": {"unknown", "done_ok", "done_err"},
      "unknown": {"running", "ghosted", "done_ok", "done_err"},
      "ghosted": {"done_ok", "done_err"},
    }
    if from_state not in allowed_targets:
      raise ValueError(f"unsupported trial source: {from_state!r}")
    if to_state not in allowed_targets[from_state]:
      raise ValueError(
        f"unsupported trial transition: {from_state!r} → {to_state!r}"
      )

    runtime = self._attempts[attempt_id]
    source = getattr(runtime, from_state)
    trial_view = source.pop(task_name)
    host = trial_view.host
    if from_state == "running":
      self._host_running[host] = max(
        0, self._host_running.get(host, 0) - 1
      )
      self._pool_bump(attempt_id, -1)

    destination = getattr(runtime, to_state)
    destination[task_name] = TrialView(
      task_name=task_name,
      state=to_state,
      trial_name=trial_view.trial_name,
      host=host,
      dispatched_at=trial_view.dispatched_at,
    )
    if to_state == "running":
      self._host_running[host] = self._host_running.get(host, 0) + 1
      self._pool_bump(attempt_id, 1)
    else:
      runtime.outcomes[task_name] = outcome

    return self._apply_pause_on_error(attempt_id, to_state)

  def trial_view_in(
    self, attempt_id: str, state: TrialViewState, task_name: str
  ) -> TrialView | None:
    """The TrialView sitting in one bucket, or None — lets event
    consumers read host/dispatched_at back after a transition."""
    runtime = self._attempts.get(attempt_id)
    if runtime is None:
      return None
    bucket = getattr(runtime, state, None)
    if not isinstance(bucket, dict):
      return None
    return bucket.get(task_name)

  def attempt_outcomes(self, attempt_id: str) -> list[Outcome]:
    """Every cached non-None Outcome, in task-name order."""
    runtime = self._attempts[attempt_id]
    return [
      o for _, o in sorted(runtime.outcomes.items()) if o is not None
    ]

  def outcome_of(self, attempt_id: str, task_name: str) -> Outcome | None:
    runtime = self._attempts.get(attempt_id)
    if runtime is None:
      return None
    return runtime.outcomes.get(task_name)

  def seed_outcome(
    self, attempt_id: str, task_name: str, outcome: Outcome
  ) -> None:
    """Startup restore: preload a completed trial's parsed
    envelope. Idempotent."""
    self._attempts[attempt_id].outcomes[task_name] = outcome

  # ── requeue / retry / reclaim ──────────────────────────────

  def requeue_after_infra_failure(
    self,
    attempt_id: str,
    task_name: str,
    from_state: TrialViewState = "running",
  ) -> bool:
    """Move a host-killed trial back to pending instead of
    scoring it. Callable from `running` (live path) AND from
    `unknown`/`ghosted` (resolver path — under load the outcome is
    rarely visible at first look, and a requeue rule that only the
    live path applies silently scores the common case). One retry
    budget across all entry points; returns False once exhausted
    or when the trial already left the source bucket."""
    seen = self._infra_retries.setdefault(attempt_id, {})
    if seen.get(task_name, 0) >= self.MAX_INFRA_RETRIES:
      return False
    reclaimed = (
      self.reclaim_from_running(attempt_id, task_name)
      if from_state == "running"
      else self.reclaim_from_parked(attempt_id, task_name, from_state)
    )
    if not reclaimed:
      return False
    seen[task_name] = seen.get(task_name, 0) + 1
    logger.warning(
      "infra failure requeued (%d/%d): attempt=%s task=%s",
      seen[task_name],
      self.MAX_INFRA_RETRIES,
      attempt_id,
      task_name,
    )
    return True

  def retry_from_done_err(self, attempt_id: str, task_name: str) -> bool:
    """Operator retry: done_err → pending (outcome cache entry
    dropped). Re-dispatch mints a fresh trial_name, so the old
    trial dir is never reused."""
    runtime = self._attempts[attempt_id]
    if task_name not in runtime.done_err:
      return False
    runtime.done_err.pop(task_name)
    runtime.outcomes.pop(task_name, None)
    self._rebuild_pending(runtime)
    return True

  def reclaim_from_running(self, attempt_id: str, task_name: str) -> bool:
    """Undo a dispatch: running → pending, releasing the slot.
    False when the trial already left running (completion won the
    race — treat as no-op)."""
    runtime = self._attempts[attempt_id]
    if task_name not in runtime.running:
      return False
    tv = runtime.running.pop(task_name)
    self._host_running[tv.host] = max(
      0, self._host_running.get(tv.host, 0) - 1
    )
    self._pool_bump(attempt_id, -1)
    self._rebuild_pending(runtime)
    return True

  def reclaim_from_parked(
    self,
    attempt_id: str,
    task_name: str,
    from_state: TrialViewState,
  ) -> bool:
    """`reclaim_from_running` for unknown/ghosted — with NO slot
    release: the slot went back when the trial left `running`, and
    releasing again would let the host over-dispatch for the rest
    of the run."""
    if from_state not in ("unknown", "ghosted"):
      raise ValueError(
        f"reclaim_from_parked expects unknown/ghosted, got {from_state!r}"
      )
    runtime = self._attempts[attempt_id]
    bucket = getattr(runtime, from_state)
    if task_name not in bucket:
      return False
    bucket.pop(task_name)
    self._rebuild_pending(runtime)
    return True

  def _rebuild_pending(self, runtime: _AttemptRuntime) -> None:
    """Recompute pending from task_list order minus everything
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
      t for t in runtime.state.task_list if t not in dispatched
    ]

  # ── observers ──────────────────────────────────────────────

  @property
  def running_total(self) -> int:
    return sum(len(r.running) for r in self._attempts.values())

  def running_per_host(self) -> dict[str, int]:
    return dict(self._host_running)

  def attempt_view(self, attempt_id: str) -> AttemptView:
    runtime = self._attempts[attempt_id]
    return AttemptView(
      pending=list(runtime.pending),
      running=dict(runtime.running),
      done_ok=dict(runtime.done_ok),
      done_err=dict(runtime.done_err),
      ghosted=dict(runtime.ghosted),
      unknown=dict(runtime.unknown),
    )

  def attempt_state(self, attempt_id: str) -> AttemptState:
    return self._attempts[attempt_id].state

  def attempt_paused(self, attempt_id: str) -> bool:
    return self._attempts[attempt_id].state.paused

  def has_attempt(self, attempt_id: str) -> bool:
    return attempt_id in self._attempts

  def iter_attempt_ids(self) -> Iterator[str]:
    yield from self._attempts

  def iter_running(self) -> Iterator[tuple[str, TrialView]]:
    for aid, runtime in self._attempts.items():
      for trial_view in runtime.running.values():
        yield aid, trial_view

  def iter_unknown(self) -> Iterator[tuple[str, str, TrialView]]:
    """Snapshotted so callers may reclassify during iteration."""
    for aid, runtime in list(self._attempts.items()):
      for task_name, tv in list(runtime.unknown.items()):
        yield aid, task_name, tv

  def iter_ghosted(self) -> Iterator[tuple[str, str, TrialView]]:
    for aid, runtime in list(self._attempts.items()):
      for task_name, tv in list(runtime.ghosted.items()):
        yield aid, task_name, tv

  def has_work(self) -> bool:
    """unknown and ghosted both count as work: they must be
    resolved into evidence-based terminal states, never silently
    dropped."""
    for runtime in self._attempts.values():
      if (
        runtime.pending
        or runtime.running
        or runtime.unknown
        or runtime.ghosted
      ):
        return True
    return False

  # ── internals ──────────────────────────────────────────────

  def _find_next_dispatchable_attempt(
    self,
  ) -> tuple[str, _AttemptRuntime] | None:
    """Advance the cursor to the next dispatchable attempt (one
    full rotation max). Skipping resets owed turns."""
    n = len(self._active_order)
    for _ in range(n):
      aid = self._active_order[self._cursor]
      runtime = self._attempts[aid]
      if self._attempt_dispatchable(runtime):
        return aid, runtime
      self._advance_cursor()
    return None

  def _attempt_dispatchable(self, runtime: _AttemptRuntime) -> bool:
    if runtime.state.paused:
      return False
    if not runtime.pending:
      return False
    if runtime.state.max_concurrent is not None:
      if len(runtime.running) >= runtime.state.max_concurrent:
        return False
    pool = runtime.state.pool or "default"
    pool_cap = self._pool_caps.get(pool)
    if pool_cap is not None:
      if self._pool_running.get(pool, 0) >= pool_cap:
        return False
    # Unresolved trials might still become done_err; dispatching
    # ahead of that resolution would race the pause the operator
    # asked for.
    if self._effective_pause_on_error(runtime.state):
      if runtime.unknown or runtime.ghosted:
        return False
    return True

  def _advance_cursor(self) -> None:
    if not self._active_order:
      return
    self._cursor = (self._cursor + 1) % len(self._active_order)
    self._turns_taken = 0

  @staticmethod
  def _effective_pause_on_error(state: AttemptState) -> bool:
    if state.pause_on_error is not None:
      return state.pause_on_error
    return state.max_concurrent == 1

  def _apply_pause_on_error(
    self, attempt_id: str, to_state: TrialViewState
  ) -> bool:
    """Returns True only when this transition flipped paused
    False→True. Only done_err with pending work triggers —
    unknown must NOT pause (an NFS-lag false negative would pause
    every sequential attempt)."""
    runtime = self._attempts[attempt_id]
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
    "tags",
    "pool",
  }
)
