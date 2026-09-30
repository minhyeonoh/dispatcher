"""Startup restore: rebuild every live job from its event
log + on-disk outcomes."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dispatcher.api.wire import full_job_view
from dispatcher.core.event_log import (
  ReplayError,
  append_event,
  find_event_logs,
  read_events,
  replay_events,
  scan_outcomes,
  seq_in_instance_id,
)
from dispatcher.core.models import InstanceView
from dispatcher.core.scheduler import (
  AliasCollisionError,
  NotArchivableError,
)

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.scheduler import Scheduler

logger = logging.getLogger(__name__)


def restore_jobs_from_disk(scheduler: Scheduler, data_dir: Path) -> int:
  """Rebuild every live job from its event log + on-disk
  outcomes. Malformed logs are skipped (one corrupt job must
  not block startup). Returns the highest instance-id counter seen
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
    job, dispatch_log = result
    completed = scan_outcomes(job.home_root)
    done_ok: dict[str, InstanceView] = {}
    done_err: dict[str, InstanceView] = {}
    unknown: dict[str, InstanceView] = {}
    # A task can carry several dispatches (infra requeue appends
    # without erasing). Later dispatches supersede earlier ones —
    # counting both would put one task in two buckets at once.
    latest_instance: dict[str, str] = {}
    for entry in dispatch_log:
      # Counter first, before any continue: a name handed out is
      # a name taken.
      max_seq = max(max_seq, seq_in_instance_id(entry.instance_id))
      if entry.job_id != job.job_id:
        continue
      done_ok.pop(entry.task_id, None)
      done_err.pop(entry.task_id, None)
      unknown.pop(entry.task_id, None)
      latest_instance[entry.task_id] = entry.instance_id
      outcome = completed.get(entry.instance_id)
      if outcome is not None:
        error_present = (not outcome.ok) or outcome.error is not None
        if error_present and outcome.infra:
          # The same requeue rule the live path applies — an instance
          # that completed during the shutdown window must not be
          # frozen as done_err while its in-flight cohort gets
          # requeued. Not bucketing routes it to pending.
          logger.warning(
            "restore: requeueing infra failure job=%s task=%s",
            job.job_id,
            entry.task_id,
          )
          continue
        tv = InstanceView(
          task_id=entry.task_id,
          state="done_err" if error_present else "done_ok",
          instance_id=entry.instance_id,
          host=entry.host,
          dispatched_at=entry.dispatched_at,
        )
        (done_err if error_present else done_ok)[entry.task_id] = tv
      else:
        # Dispatched, no readable outcome — could be running,
        # crashed, or NFS-lagged. Park in unknown; the startup
        # resolver reclassifies on evidence.
        unknown[entry.task_id] = InstanceView(
          task_id=entry.task_id,
          state="unknown",
          instance_id=entry.instance_id,
          host=entry.host,
          dispatched_at=entry.dispatched_at,
        )
    needs_alias_backfill = not job.alias
    try:
      scheduler.restore(
        job,
        running={},
        done_ok=done_ok,
        done_err=done_err,
        unknown=unknown,
      )
    except (ValueError, AliasCollisionError) as exc:
      logger.warning(
        "restore: job %s not restored (%s)",
        job.job_id,
        exc,
      )
      continue
    if needs_alias_backfill:
      try:
        append_event(
          log_path,
          {
            "type": "patch",
            "job_id": job.job_id,
            "at": job.submitted_at.isoformat(),
            "alias": job.alias,
          },
        )
      except OSError as exc:
        logger.warning(
          "restore: alias backfill failed for %s: %s",
          job.job_id,
          exc,
        )
    # Seed the outcome cache from the LATEST instance of each task
    # only — a superseded (requeued) instance's outcome must not
    # win.
    instance_to_task = {
      instance: task for task, instance in latest_instance.items()
    }
    for instance_id, outcome in completed.items():
      task_id = instance_to_task.get(instance_id)
      if task_id is None:
        continue
      # Skip outcomes routed back to pending by the infra rule.
      if task_id not in done_ok and task_id not in done_err:
        continue
      scheduler.seed_outcome(job.job_id, task_id, outcome)
    # Re-archive: replay left the marks set; buckets + caches are
    # rebuilt, so the bytes regenerate. A precondition failure
    # (something reclassified to unknown) leaves it live.
    if job.archived_at is not None:
      try:
        view = full_job_view(scheduler, job.job_id)
        scheduler.archive_job(
          job.job_id,
          at=job.archived_at,
          kind=job.archive_kind or "manual",
          payload_bytes=view.model_dump_json().encode("utf-8"),
        )
      except NotArchivableError as exc:
        logger.warning(
          "restore: cannot re-archive %s: %s (leaving live)",
          job.job_id,
          exc.reason,
        )
      except Exception:
        logger.exception(
          "restore: archive rehydrate failed for %s",
          job.job_id,
        )
  return max_seq
