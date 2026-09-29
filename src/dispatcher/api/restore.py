"""Startup restore: rebuild every live attempt from its event
log + on-disk outcomes."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dispatcher.api.wire import full_attempt_view
from dispatcher.core.event_log import (
  ReplayError,
  append_event,
  find_event_logs,
  read_events,
  replay_events,
  scan_outcomes,
  seq_in_trial_name,
)
from dispatcher.core.models import TrialView
from dispatcher.core.outcome import CompletionSnapshot
from dispatcher.core.scheduler import (
  AliasCollisionError,
  NotArchivableError,
)

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.metrics import MetricsCache
  from dispatcher.core.scheduler import Scheduler

logger = logging.getLogger(__name__)


def restore_attempts_from_disk(
  scheduler: Scheduler, metrics: MetricsCache, data_dir: Path
) -> int:
  """Rebuild every live attempt from its event log + on-disk
  outcomes. Malformed logs are skipped (one corrupt attempt must
  not block startup). Returns the highest trial-name counter seen
  so the namer never re-mints a used name."""
  max_seq = 0
  for log_path in find_event_logs(data_dir):
    try:
      events = read_events(log_path)
    except (OSError, ValueError) as exc:
      logger.warning("restore: cannot read %s: %s", log_path, exc)
      continue
    if not events:
      continue
    try:
      result = replay_events(events)
    except ReplayError as exc:
      logger.warning("restore: replay failed for %s: %s", log_path, exc)
      continue
    if result is None:
      continue  # cancelled — log stays as audit trail
    attempt, dispatch_log = result
    completed = scan_outcomes(attempt.home_root)
    done_ok: dict[str, TrialView] = {}
    done_err: dict[str, TrialView] = {}
    unknown: dict[str, TrialView] = {}
    # A task can carry several dispatches (infra requeue appends
    # without erasing). Later dispatches supersede earlier ones —
    # counting both would put one task in two buckets at once.
    latest_trial: dict[str, str] = {}
    for entry in dispatch_log:
      # Counter first, before any continue: a name handed out is
      # a name taken.
      max_seq = max(max_seq, seq_in_trial_name(entry.trial_name))
      if entry.attempt_id != attempt.attempt_id:
        continue
      done_ok.pop(entry.task_name, None)
      done_err.pop(entry.task_name, None)
      unknown.pop(entry.task_name, None)
      latest_trial[entry.task_name] = entry.trial_name
      outcome = completed.get(entry.trial_name)
      if outcome is not None:
        error_present = (not outcome.ok) or outcome.error is not None
        if error_present and outcome.infra:
          # The same requeue rule the live path applies — a trial
          # that completed during the shutdown window must not be
          # frozen as done_err while its in-flight cohort gets
          # requeued. Not bucketing routes it to pending.
          logger.warning(
            "restore: requeueing infra failure attempt=%s task=%s",
            attempt.attempt_id,
            entry.task_name,
          )
          continue
        tv = TrialView(
          task_name=entry.task_name,
          state="done_err" if error_present else "done_ok",
          trial_name=entry.trial_name,
          host=entry.host,
          dispatched_at=entry.dispatched_at,
        )
        (done_err if error_present else done_ok)[entry.task_name] = tv
      else:
        # Dispatched, no readable outcome — could be running,
        # crashed, or NFS-lagged. Park in unknown; the startup
        # resolver reclassifies on evidence.
        unknown[entry.task_name] = TrialView(
          task_name=entry.task_name,
          state="unknown",
          trial_name=entry.trial_name,
          host=entry.host,
          dispatched_at=entry.dispatched_at,
        )
    needs_alias_backfill = not attempt.alias
    try:
      scheduler.restore(
        attempt,
        running={},
        done_ok=done_ok,
        done_err=done_err,
        unknown=unknown,
      )
    except (ValueError, AliasCollisionError) as exc:
      logger.warning(
        "restore: attempt %s not restored (%s)",
        attempt.attempt_id,
        exc,
      )
      continue
    if needs_alias_backfill:
      try:
        append_event(
          log_path,
          {
            "type": "patch",
            "attempt_id": attempt.attempt_id,
            "at": attempt.submitted_at.isoformat(),
            "alias": attempt.alias,
          },
        )
      except OSError as exc:
        logger.warning(
          "restore: alias backfill failed for %s: %s",
          attempt.attempt_id,
          exc,
        )
    # Seed caches from the LATEST trial of each task only — a
    # superseded (requeued) trial's outcome must not win, and
    # must not double-count in metrics.
    trial_to_task = {trial: task for task, trial in latest_trial.items()}
    for trial_name, outcome in completed.items():
      task_name = trial_to_task.get(trial_name)
      if task_name is None:
        continue
      # Skip outcomes routed back to pending by the infra rule.
      if task_name not in done_ok and task_name not in done_err:
        continue
      scheduler.seed_outcome(attempt.attempt_id, task_name, outcome)
      error_present = (not outcome.ok) or outcome.error is not None
      metrics.record_completion(
        attempt.attempt_id,
        CompletionSnapshot(
          outcome_exists=True,
          error_present=error_present,
          values={
            k: float(v)
            for k, v in outcome.values.items()
            if isinstance(v, (int, float)) and not isinstance(v, bool)
          },
          outcome=outcome,
        ),
      )
    # Re-archive: replay left the marks set; buckets + caches are
    # rebuilt, so the bytes regenerate. A precondition failure
    # (something reclassified to unknown) leaves it live.
    if attempt.archived_at is not None:
      try:
        view = full_attempt_view(scheduler, attempt.attempt_id)
        scheduler.archive_attempt(
          attempt.attempt_id,
          at=attempt.archived_at,
          kind=attempt.archive_kind or "manual",
          payload_bytes=view.model_dump_json().encode("utf-8"),
        )
      except NotArchivableError as exc:
        logger.warning(
          "restore: cannot re-archive %s: %s (leaving live)",
          attempt.attempt_id,
          exc.reason,
        )
      except Exception:
        logger.exception(
          "restore: archive rehydrate failed for %s",
          attempt.attempt_id,
        )
  return max_seq
