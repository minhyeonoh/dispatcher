"""DispatcherRuntime — the async loop that drives the scheduler
against real docker dispatch and outcome polling.

    Scheduler                      (in-memory state machine)
      ↓ dispatch_one()
    runtime._do_dispatch           (write trial.json, docker run -d)
      ↓
    main container on host        (worker runs, writes outcome.json,
      ↓ die event                  exits)
    runtime._handle_docker_die     (read outcome → transition)

Completion detection is edge-triggered (docker events) with two
level-triggered backstops: the startup census (completions that
fired while the dispatcher was down) and the periodic resolver
(unknown/ghosted reclassification). The NFS poll loop exists for
tests and docker-less deployments.

Evidence rules, in one place:
- outcome.json readable   → done_ok / done_err (envelope only;
  the main container's exit code is bookkeeping once a clean
  envelope exists — sibling teardown can SIGKILL long after the
  work finished).
- envelope says infra     → requeue (bounded), never scored.
- no outcome + die seen   → unknown; the exit code is remembered.
- unknown + container definitively gone:
    remembered infra exit → requeue (bounded)
    otherwise             → ghosted ("maybe ghosted": every
    resolver tick re-polls it, so a late NFS commit still
    upgrades it to done_ok/done_err).
- dispatch itself failed  → the trial never started; requeue
  (bounded), else park unknown for the operator.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

import anyio
from pydantic import BaseModel, ConfigDict, PositiveFloat, PositiveInt

from dispatcher.core import clock, labels
from dispatcher.core.containers import (
  container_labels,
  ensure_image_on_host,
  probe_trial,
)
from dispatcher.core.dispatch import DispatchError, docker_dispatch
from dispatcher.core.event_log import (
  append_event_async,
  event_log_path_for,
)
from dispatcher.core.models import (
  INFRA_EXIT_CODES,
  TRIAL_SPEC_FILENAME,
)
from dispatcher.core.outcome import (
  CompletionSnapshot,
  bust_dir_cache,
  read_completion,
  trial_home_for,
)

if TYPE_CHECKING:
  from collections.abc import Awaitable, Callable
  from pathlib import Path

  from dispatcher.core.containers import DockerEventStreamManager
  from dispatcher.core.event_bus import EventBus
  from dispatcher.core.metrics import MetricsCache
  from dispatcher.core.models import (
    AttemptState,
    DispatchEntry,
    Outcome,
    TrialView,
    TrialViewState,
  )
  from dispatcher.core.scheduler import Scheduler

  DispatchCallable = Callable[
    [DispatchEntry, AttemptState], Awaitable[None]
  ]
  PollCallable = Callable[[Path], CompletionSnapshot | None]
  AttemptHook = Callable[[AttemptState, list[Outcome]], None]


logger = logging.getLogger(__name__)


class StateReconciliationSettings(BaseModel):
  """Resolver knobs (periodic unknown/ghosted sweep)."""

  max_concurrent_probes: PositiveInt = 16
  seconds_between_probes: PositiveFloat = 30.0


class StateReconciliationPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  max_concurrent_probes: PositiveInt | None = None
  seconds_between_probes: PositiveFloat | None = None


def apply_state_reconciliation_patch(
  settings: StateReconciliationSettings,
  patch: StateReconciliationPatch,
) -> None:
  if patch.max_concurrent_probes is not None:
    settings.max_concurrent_probes = patch.max_concurrent_probes
  if patch.seconds_between_probes is not None:
    settings.seconds_between_probes = patch.seconds_between_probes


def classify(snapshot: CompletionSnapshot | None) -> TrialViewState:
  """The one copy of the terminal classification rule."""
  if snapshot is None or not snapshot.outcome_exists:
    return "unknown"
  if snapshot.error_present:
    return "done_err"
  return "done_ok"


class DispatcherRuntime:
  # Bridging retries for NFS attribute-cache lag between the die
  # event and the outcome.json becoming visible to this client.
  # Each retry is preceded by a directory cache bust (see
  # `bust_dir_cache`) — without it these retries just re-ask the
  # local cache, which is how 43% of the old router's trials
  # parked in unknown over files that already existed. The
  # ladder is insurance for the bust not taking; misses still
  # fall to `unknown` and the resolver catches up.
  _NFS_POLL_RETRY_DELAYS: tuple[float, ...] = (1.0, 2.0, 4.0, 8.0)

  def __init__(
    self,
    scheduler: Scheduler,
    *,
    self_host: str,
    dispatch: DispatchCallable | None = None,
    poll: PollCallable | None = None,
    tick_interval: float = 0.5,
    metrics: MetricsCache | None = None,
    event_bus: EventBus | None = None,
    on_attempt_drained: AttemptHook | None = None,
    on_trial_completed: AttemptHook | None = None,
    docker_event_manager: DockerEventStreamManager | None = None,
  ) -> None:
    self._sched = scheduler
    self._self_host = self_host
    self._dispatch = dispatch or self._default_dispatch
    self._poll = poll or read_completion
    self._tick_interval = tick_interval
    self._metrics = metrics
    self._event_bus = event_bus
    self._on_attempt_drained = on_attempt_drained
    self._on_trial_completed = on_trial_completed
    self._docker_events = docker_event_manager
    # (image_id, host) pairs verified present. Restart clears it;
    # re-verification is one cheap inspect per pair.
    self._images_ensured: set[tuple[str, str]] = set()
    self._image_locks: dict[tuple[str, str], asyncio.Lock] = {}
    # (attempt_id, trial_name) → main-container exit code, kept
    # while the trial sits in unknown so the resolver can requeue
    # infra kills (75/137/143/255) instead of ghosting them.
    # Restart loses it; the startup census re-supplies exit codes.
    self._last_exit: dict[tuple[str, str], int] = {}

  # ── main loop ──────────────────────────────────────────────

  async def run_forever(self) -> None:
    """Drain the dispatch pipeline each tick; poll only when no
    docker-events manager is attached."""
    poll_active = self._docker_events is None
    while True:
      await self._dispatch_available()
      if poll_active:
        await self._poll_all()
      await asyncio.sleep(self._tick_interval)

  async def run_until_done(self, *, max_ticks: int | None = None) -> None:
    ticks = 0
    while self._sched.has_work():
      await self._dispatch_available()
      await self._poll_all()
      await asyncio.sleep(self._tick_interval)
      ticks += 1
      if max_ticks is not None and ticks >= max_ticks:
        return

  async def _dispatch_available(self) -> None:
    while True:
      action = self._sched.dispatch_one()
      if action is None:
        return
      await self._do_dispatch(action)

  async def _do_dispatch(self, action: DispatchEntry) -> None:
    state = self._sched.attempt_state(action.attempt_id)
    trial_home = trial_home_for(state.home_root, action.trial_name)
    spec = {
      "attempt_id": action.attempt_id,
      "task_name": action.task_name,
      "trial_name": action.trial_name,
      "home": state.container.home_mount,
      "payload": state.payloads.get(action.task_name),
    }

    def _stage() -> None:
      # NFS writes off the loop (R5): a slow write stalls this
      # dispatch, not the whole runtime.
      trial_home.mkdir(parents=True, exist_ok=True)
      (trial_home / TRIAL_SPEC_FILENAME).write_text(
        json.dumps(spec, indent=2)
      )

    await asyncio.to_thread(_stage)
    try:
      await self._dispatch(action, state)
    except DispatchError as exc:
      # The container never started — nothing to watch, nothing
      # on the host. Requeue like any machine failure; past the
      # budget, park in unknown so the resolver/operator sees it
      # instead of a silent drop.
      logger.warning(
        "dispatch failed: attempt=%s task=%s host=%s: %s",
        action.attempt_id,
        action.task_name,
        action.host,
        exc,
      )
      if not self._sched.requeue_after_infra_failure(
        action.attempt_id, action.task_name, "running"
      ):
        await self._apply_terminal_transition(
          aid=action.attempt_id,
          task_name=action.task_name,
          trial_name=action.trial_name,
          from_state="running",
          to_state="unknown",
          snapshot=None,
        )
      elif self._event_bus is not None:
        self._event_bus.publish(
          "trial_requeued",
          {
            "attempt_id": action.attempt_id,
            "task_name": action.task_name,
            "trial_name": action.trial_name,
            "from_state": "running",
          },
        )
      return
    # Persisted AFTER the docker run returned success, so a
    # failed dispatch never records a phantom. A crash in the
    # window between docker-run and this append leaves a running
    # container the log doesn't know — the startup census +
    # GC-orphan path reap it.
    await append_event_async(
      event_log_path_for(state),
      {
        "type": "dispatch",
        "attempt_id": action.attempt_id,
        "task_name": action.task_name,
        "trial_name": action.trial_name,
        "host": action.host,
        "at": action.dispatched_at.isoformat(),
      },
    )
    if self._event_bus is not None:
      self._event_bus.publish(
        "trial_dispatched",
        {
          "attempt_id": action.attempt_id,
          "task_name": action.task_name,
          "trial_name": action.trial_name,
          "host": action.host,
          "dispatched_at": action.dispatched_at.isoformat(),
        },
      )

  async def _default_dispatch(
    self, action: DispatchEntry, state: AttemptState
  ) -> None:
    if state.image_id:
      await self._ensure_image(action.host, state.image_id)
    await docker_dispatch(
      action,
      state,
      trial_home=trial_home_for(state.home_root, action.trial_name),
      self_host=self._self_host,
    )

  async def _ensure_image(self, host: str, image_id: str) -> None:
    """Once per (image, host): verify or ship. Failures raise
    DispatchError so the normal requeue path applies. Per-pair
    lock so parallel dispatches don't ship the same image
    twice."""
    key = (image_id, host)
    if key in self._images_ensured:
      return
    lock = self._image_locks.setdefault(key, asyncio.Lock())
    async with lock:
      if key in self._images_ensured:
        return
      try:
        await ensure_image_on_host(
          host, image_id, self_host=self._self_host
        )
      except (RuntimeError, TimeoutError) as exc:
        raise DispatchError(str(exc)) from exc
      self._images_ensured.add(key)

  # ── completion: poll fallback ──────────────────────────────

  async def _poll_all(self) -> None:
    for aid, trial_view in list(self._sched.iter_running()):
      state = self._sched.attempt_state(aid)
      trial_home = trial_home_for(state.home_root, trial_view.trial_name)
      snapshot = await asyncio.to_thread(self._poll, trial_home)
      if snapshot is None:
        continue
      await self._apply_trial_completion(
        aid,
        trial_view.task_name,
        trial_view.trial_name,
        snapshot,
      )

  # ── completion: docker die events ──────────────────────────

  async def handle_docker_die(
    self, host: str, event: dict[str, Any]
  ) -> None:
    """Silently ignores events for trials we don't know — another
    dispatcher sharing the cluster, or a duplicate from docker's
    --since replay across reconnects."""
    actor = event.get("Actor") or {}
    attrs = actor.get("Attributes") or {}
    trial_name = attrs.get(labels.TRIAL) or ""
    if not trial_name:
      return
    try:
      exit_code = int(attrs.get("exitCode") or "0")
    except (TypeError, ValueError):
      exit_code = 1

    match: tuple[str, str] | None = None
    for aid, tv in self._sched.iter_running():
      if tv.trial_name == trial_name:
        match = (aid, tv.task_name)
        break
    if match is None:
      return
    aid, task_name = match

    state = self._sched.attempt_state(aid)
    trial_home = trial_home_for(state.home_root, trial_name)
    snapshot = await self._poll_with_nfs_retry(trial_home)
    if snapshot is None:
      snapshot = CompletionSnapshot(
        outcome_exists=False,
        error_present=False,
        exit_code=exit_code,
      )
      # Remember the exit for the resolver's ghost-or-requeue
      # decision.
      self._last_exit[(aid, trial_name)] = exit_code
    else:
      # A readable envelope wins over the exit code: the work
      # finished and wrote its result; whatever killed the
      # container afterwards is teardown bookkeeping.
      snapshot.exit_code = exit_code
    await self._apply_trial_completion(
      aid, task_name, trial_name, snapshot
    )

  async def reconcile_from_census(
    self, host: str, containers: list[dict[str, Any]]
  ) -> None:
    """Fold a startup census (docker inspect rows) into scheduler
    state by synthesizing die events — completions during
    downtime run the exact same code path as live ones."""
    for c in containers:
      state = c.get("State") or {}
      if state.get("Status") != "exited":
        continue
      trial_name = container_labels(c).get(labels.TRIAL)
      if not trial_name:
        continue
      exit_code = state.get("ExitCode")
      if not isinstance(exit_code, int):
        exit_code = 1  # never pretend clean on a missing field
      synthetic = {
        "Actor": {
          "Attributes": {
            labels.TRIAL: trial_name,
            "exitCode": str(exit_code),
          }
        }
      }
      await self.handle_docker_die(host, synthetic)

  async def handle_alive_change(self, host: str, alive: bool) -> None:
    logger.info("host %s alive=%s", host, alive)
    try:
      self._sched.set_host_settings(host, alive=alive)
    except Exception:
      logger.exception(
        "set_host_settings(%s, alive=%s) failed", host, alive
      )
    if self._event_bus is not None:
      self._event_bus.publish(
        "host_alive_change", {"host": host, "alive": alive}
      )

  async def _poll_with_nfs_retry(
    self, trial_home: Path
  ) -> CompletionSnapshot | None:
    snapshot = await asyncio.to_thread(self._poll, trial_home)
    if snapshot is not None:
      return snapshot
    for delay in self._NFS_POLL_RETRY_DELAYS:
      await asyncio.sleep(delay)
      snapshot = await asyncio.to_thread(self._poll_busted, trial_home)
      if snapshot is not None:
        return snapshot
    return None

  def _poll_busted(self, trial_home: Path) -> CompletionSnapshot | None:
    """Cache bust, then poll — one thread hop for both."""
    bust_dir_cache(trial_home)
    return self._poll(trial_home)

  # ── terminal pipeline ──────────────────────────────────────

  def _observation_is_stale(
    self, aid: str, task_name: str, trial_name: str, from_state: str
  ) -> bool:
    """True when the bucket no longer holds THIS trial — the
    observation raced a reclaim/requeue/cancel across an await.
    Acting on it would score a NEW trial with an OLD trial's
    outcome, which is exactly the contamination class this repo
    exists to prevent (latent even pre-async: the die handler's
    7s NFS window). Stale observations are dropped; the current
    occupant's own signals classify it."""
    tv = self._sched.trial_view_in(aid, from_state, task_name)  # type: ignore[arg-type]
    if tv is None or tv.trial_name != trial_name:
      logger.warning(
        "stale observation dropped: attempt=%s task=%s trial=%s "
        "(bucket %s now holds %s)",
        aid,
        task_name,
        trial_name,
        from_state,
        tv.trial_name if tv else "nothing",
      )
      return True
    return False

  async def _apply_trial_completion(
    self,
    aid: str,
    task_name: str,
    trial_name: str,
    snapshot: CompletionSnapshot,
  ) -> None:
    to_state = classify(snapshot)
    if self._requeued_instead_of_scored(
      aid, task_name, trial_name, to_state, snapshot, "running"
    ):
      return
    await self._apply_terminal_transition(
      aid=aid,
      task_name=task_name,
      trial_name=trial_name,
      from_state="running",
      to_state=to_state,
      snapshot=snapshot,
    )

  def _requeued_instead_of_scored(
    self,
    aid: str,
    task_name: str,
    trial_name: str,
    to_state: TrialViewState,
    snapshot: CompletionSnapshot | None,
    from_state: TrialViewState,
  ) -> bool:
    """Requeue when the envelope itself says the machine failed.
    Runs BEFORE the terminal pipeline so a requeued trial leaves
    no aggregate, metric, or transition event behind. Shared by
    the live path and both resolver paths — under load the
    outcome is rarely visible at first look, and those parked
    trials deserve the same rule."""
    if to_state != "done_err" or snapshot is None:
      return False
    if not snapshot.infra:
      return False
    if self._observation_is_stale(aid, task_name, trial_name, from_state):
      return True  # handled: dropped, nothing to score
    if not self._sched.requeue_after_infra_failure(
      aid, task_name, from_state
    ):
      return False
    self._last_exit.pop((aid, trial_name), None)
    logger.warning(
      "requeued infra failure from %s: attempt=%s task=%s trial=%s",
      from_state,
      aid,
      task_name,
      trial_name,
    )
    if self._event_bus is not None:
      self._event_bus.publish(
        "trial_requeued",
        {
          "attempt_id": aid,
          "task_name": task_name,
          "trial_name": trial_name,
          "from_state": from_state,
        },
      )
    return True

  async def _apply_terminal_transition(
    self,
    *,
    aid: str,
    task_name: str,
    trial_name: str,
    from_state: TrialViewState,
    to_state: TrialViewState,
    snapshot: CompletionSnapshot | None,
  ) -> None:
    """One ordered path for every completion-like transition:

      stale guard → scheduler state + pause → hooks → drain →
      metrics → bus events → event-log appends (awaited, last)

    unknown/ghosted enter this pipeline (they are unresolved
    terminal observations) and keep blocking drain."""
    if to_state in ("done_ok", "done_err") and snapshot is None:
      raise ValueError(f"{to_state} transition requires a snapshot")
    if self._observation_is_stale(aid, task_name, trial_name, from_state):
      return

    paused_on_error = self._sched.transition_trial(
      attempt_id=aid,
      task_name=task_name,
      from_state=from_state,
      to_state=to_state,
      outcome=(snapshot.outcome if snapshot is not None else None),
    )
    if to_state in ("done_ok", "done_err"):
      self._last_exit.pop((aid, trial_name), None)

    state = self._sched.attempt_state(aid)
    events_to_append: list[dict] = []
    if to_state == "unknown":
      # Breadcrumb only — restore re-derives the bucket from disk;
      # this keeps an audit trail of when sight was lost.
      events_to_append.append(
        {
          "type": "unknown",
          "attempt_id": aid,
          "task_name": task_name,
          "trial_name": trial_name,
          "at": clock.now().isoformat(),
        }
      )
    if self._on_trial_completed is not None:
      self._on_trial_completed(state, self._sched.attempt_outcomes(aid))
    if self._on_attempt_drained is not None or (
      self._event_bus is not None
    ):
      view = self._sched.attempt_view(aid)
      drained = (
        not view.pending
        and not view.running
        and not view.unknown
        and not view.ghosted
      )
      if drained:
        if self._on_attempt_drained is not None:
          self._on_attempt_drained(
            state, self._sched.attempt_outcomes(aid)
          )
        if self._event_bus is not None:
          self._event_bus.publish("attempt_drained", {"attempt_id": aid})
    if self._metrics is not None and snapshot is not None:
      if from_state == "running":
        self._metrics.record_completion(aid, snapshot)
      elif from_state == "unknown":
        self._metrics.reclassify_from_unknown(
          aid, to_state=to_state, values=snapshot.values
        )
      else:
        self._metrics.reclassify_from_ghosted(
          aid, to_state=to_state, values=snapshot.values
        )
    if paused_on_error:
      events_to_append.append(
        {
          "type": "pause_on_error",
          "attempt_id": aid,
          "task_name": task_name,
          "at": clock.now().isoformat(),
        }
      )
    if self._event_bus is not None:
      moved = self._sched.trial_view_in(aid, to_state, task_name)
      trial_row = {
        "host": moved.host if moved is not None else None,
        "dispatched_at": (
          moved.dispatched_at.isoformat() if moved is not None else None
        ),
      }
      if from_state == "running":
        assert snapshot is not None
        self._event_bus.publish(
          "trial_completed",
          {
            "attempt_id": aid,
            "task_name": task_name,
            "trial_name": trial_name,
            "outcome_exists": snapshot.outcome_exists,
            "error_present": snapshot.error_present,
            "values": snapshot.values,
            "to_state": to_state,
            **trial_row,
          },
        )
      else:
        self._event_bus.publish(
          "trial_reclassified",
          {
            "attempt_id": aid,
            "task_name": task_name,
            "trial_name": trial_name,
            "from_state": from_state,
            "to_state": to_state,
            "values": (snapshot.values if snapshot is not None else None),
            **trial_row,
          },
        )
      if paused_on_error:
        self._event_bus.publish(
          "attempt_paused_on_error",
          {"attempt_id": aid, "task_name": task_name},
        )
    # Appends LAST: every in-memory mutation (scheduler, metrics,
    # hooks, bus) completed synchronously above, so nothing can
    # observe a half-applied transition across these awaits. A
    # crash before the append loses only breadcrumbs that restore
    # re-derives from disk anyway.
    for ev in events_to_append:
      await append_event_async(event_log_path_for(state), ev)

  # ── resolver ───────────────────────────────────────────────

  async def resolve_state_once(
    self, *, max_concurrent_probes: int
  ) -> dict[str, int]:
    """One reconciliation pass over unknown AND ghosted.

    unknown → outcome now visible: done_ok/done_err (or infra
    requeue); container running: back to running (slot re-booked);
    container definitively gone (gone/exited/dead/created):
    remembered infra exit → requeue, else ghosted; transient
    docker states and probe errors: unchanged, next tick.

    ghosted → re-poll outcome only (container is known gone); a
    late NFS commit upgrades it, otherwise unchanged.

    Scheduler/metrics mutations happen on the event loop with no
    await between read and reclassify."""
    counts = {
      "done_ok": 0,
      "done_err": 0,
      "ghosted": 0,
      "running": 0,
      "requeued": 0,
      "unchanged": 0,
      "error": 0,
    }
    unknown_entries = list(self._sched.iter_unknown())
    ghosted_entries = list(self._sched.iter_ghosted())
    if not unknown_entries and not ghosted_entries:
      return counts
    sem = anyio.Semaphore(max(1, max_concurrent_probes))

    async def probe_unknown(
      aid: str, task_name: str, tv: TrialView
    ) -> None:
      async with sem:
        try:
          outcome = await self._resolve_unknown_one(aid, task_name, tv)
        except Exception:
          logger.exception(
            "resolver (unknown) failed for %s / %s", aid, task_name
          )
          outcome = "error"
      counts[outcome] = counts.get(outcome, 0) + 1

    async def probe_ghosted(
      aid: str, task_name: str, tv: TrialView
    ) -> None:
      async with sem:
        try:
          outcome = await self._resolve_ghosted_one(aid, task_name, tv)
        except Exception:
          logger.exception(
            "resolver (ghosted) failed for %s / %s", aid, task_name
          )
          outcome = "error"
      counts[outcome] = counts.get(outcome, 0) + 1

    async with anyio.create_task_group() as tg:
      for aid, task_name, tv in unknown_entries:
        tg.start_soon(probe_unknown, aid, task_name, tv)
      for aid, task_name, tv in ghosted_entries:
        tg.start_soon(probe_ghosted, aid, task_name, tv)
    return counts

  async def _resolve_ghosted_one(
    self, aid: str, task_name: str, tv: TrialView
  ) -> str:
    state = self._sched.attempt_state(aid)
    snapshot = await asyncio.to_thread(
      self._poll_busted,
      trial_home_for(state.home_root, tv.trial_name),
    )
    if snapshot is None:
      return "unchanged"
    to_state = classify(snapshot)
    if self._requeued_instead_of_scored(
      aid, task_name, tv.trial_name, to_state, snapshot, "ghosted"
    ):
      return "requeued"
    await self._apply_terminal_transition(
      aid=aid,
      task_name=task_name,
      trial_name=tv.trial_name,
      from_state="ghosted",
      to_state=to_state,
      snapshot=snapshot,
    )
    return to_state

  async def _resolve_unknown_one(
    self, aid: str, task_name: str, tv: TrialView
  ) -> str:
    state = self._sched.attempt_state(aid)
    # Cheap NFS look first — catches lag tails past the die
    # handler's retry window. Busted: at 30s cadence the
    # directory cache can still be live (acdirmax up to 60s).
    snapshot = await asyncio.to_thread(
      self._poll_busted,
      trial_home_for(state.home_root, tv.trial_name),
    )
    if snapshot is not None:
      to_state = classify(snapshot)
      if self._requeued_instead_of_scored(
        aid, task_name, tv.trial_name, to_state, snapshot, "unknown"
      ):
        return "requeued"
      await self._apply_terminal_transition(
        aid=aid,
        task_name=task_name,
        trial_name=tv.trial_name,
        from_state="unknown",
        to_state=to_state,
        snapshot=snapshot,
      )
      return to_state
    status = await probe_trial(
      tv.host, tv.trial_name, self_host=self._self_host
    )
    # gone/exited/dead/created: no more output is coming from the
    # container (docker never auto-transitions out of these), so
    # waiting longer in unknown buys nothing.
    if status in ("gone", "exited", "dead", "created"):
      exit_code = self._last_exit.get((aid, tv.trial_name))
      if (
        exit_code in INFRA_EXIT_CODES
        and self._sched.requeue_after_infra_failure(
          aid, task_name, "unknown"
        )
      ):
        # Host-killed (137/143), worker-stated tempfail (75), or
        # ssh-shaped death (255) with no envelope — run it again
        # rather than ghosting a machine failure.
        self._last_exit.pop((aid, tv.trial_name), None)
        logger.warning(
          "requeued infra exit %s: attempt=%s task=%s trial=%s",
          exit_code,
          aid,
          task_name,
          tv.trial_name,
        )
        if self._event_bus is not None:
          self._event_bus.publish(
            "trial_requeued",
            {
              "attempt_id": aid,
              "task_name": task_name,
              "trial_name": tv.trial_name,
              "from_state": "unknown",
            },
          )
        return "requeued"
      await self._apply_terminal_transition(
        aid=aid,
        task_name=task_name,
        trial_name=tv.trial_name,
        from_state="unknown",
        to_state="ghosted",
        snapshot=None,
      )
      return "ghosted"
    if status == "running":
      self._sched.transition_trial(
        attempt_id=aid,
        task_name=task_name,
        from_state="unknown",
        to_state="running",
        outcome=None,
      )
      # Not terminal — no drain/metric hooks; a future die event
      # runs the normal completion path.
      if self._event_bus is not None:
        self._event_bus.publish(
          "trial_reclassified",
          {
            "attempt_id": aid,
            "task_name": task_name,
            "trial_name": tv.trial_name,
            "from_state": "unknown",
            "to_state": "running",
          },
        )
      return "running"
    # paused / restarting / removing / future statuses — keep in
    # unknown rather than misclassify.
    return "unchanged"

  # ── remote kill (cancel / reclaim) ─────────────────────────

  def fire_kill_trials(self, running: dict[str, TrialView]) -> None:
    """Fire-and-forget `docker rm -f` of each trial's container
    set. A lingering container is a leak the GC sweeps up, not a
    correctness problem."""
    import shlex as _shlex
    import subprocess

    for tv in running.values():
      inner = (
        "ids=$(docker ps -aq --filter "
        + _shlex.quote(f"label={labels.SET}={tv.trial_name}")
        + '); [ -n "$ids" ] && docker rm -f $ids '
        "> /dev/null 2>&1; true"
      )
      argv = (
        ["bash", "-c", inner]
        if tv.host == self._self_host
        else [
          "ssh",
          "-o",
          "BatchMode=yes",
          "-o",
          "ConnectTimeout=10",
          tv.host,
          inner,
        ]
      )
      subprocess.Popen(
        argv,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
      )


async def resolver_loop(
  runtime: DispatcherRuntime,
  settings: StateReconciliationSettings,
) -> None:
  """Periodic reconciliation; knobs re-read each tick."""
  from dispatcher.core.loops import every

  async def tick() -> None:
    outcomes = await runtime.resolve_state_once(
      max_concurrent_probes=settings.max_concurrent_probes,
    )
    if any(v > 0 for v in outcomes.values()):
      logger.info(
        "resolver: %s",
        ", ".join(f"{k}={v}" for k, v in outcomes.items() if v > 0),
      )

  await every(
    "resolver",
    lambda: settings.seconds_between_probes,
    tick,
  )
