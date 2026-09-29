"""Auto-archive: freeze fully-terminal, long-idle attempts off
the hot serialization path. Optional — off until
`archive.auto_after_days > 0`."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from pydantic import (
  BaseModel,
  ConfigDict,
  NonNegativeInt,
  PositiveFloat,
)

from dispatcher.core.event_log import event_log_path_for
from dispatcher.core.loops import LoopSkip, every

if TYPE_CHECKING:
  from collections.abc import Callable

  from dispatcher.core.scheduler import Scheduler

logger = logging.getLogger(__name__)


class ArchiveSettings(BaseModel):
  auto_after_days: NonNegativeInt = 0
  """0 = auto-archive disabled."""

  scan_interval_seconds: PositiveFloat = 3600.0


class ArchivePatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  auto_after_days: NonNegativeInt | None = None
  scan_interval_seconds: PositiveFloat | None = None


def apply_patch(settings: ArchiveSettings, patch: ArchivePatch) -> None:
  if patch.auto_after_days is not None:
    settings.auto_after_days = patch.auto_after_days
  if patch.scan_interval_seconds is not None:
    settings.scan_interval_seconds = patch.scan_interval_seconds


async def archive_loop(
  scheduler: Scheduler,
  settings: ArchiveSettings,
  clock_fn: Callable[[], datetime],
  archive_one: Callable[[str], None],
) -> None:
  """`archive_one(aid)` applies one auto-archive (the server's
  operation, shared with the endpoint). It raises LoopSkip when a
  precondition re-check fails — a resolver may have re-flipped a
  trial between scan and apply."""

  async def tick() -> None:
    threshold_days = settings.auto_after_days
    if threshold_days <= 0:
      return
    candidates = scan_auto_archive_candidates(
      scheduler, clock_fn(), threshold_days
    )
    archived = 0
    for aid in candidates:
      try:
        archive_one(aid)
        archived += 1
      except LoopSkip as exc:
        logger.info("auto-archive skipped attempt=%s: %s", aid, exc)
      except Exception:
        logger.exception("auto-archive failed attempt=%s", aid)
    if archived:
      logger.warning(
        "auto-archive: promoted %d attempt(s) (threshold=%d days)",
        archived,
        threshold_days,
      )

  await every(
    "auto_archive",
    lambda: settings.scan_interval_seconds,
    tick,
  )


def scan_auto_archive_candidates(
  scheduler: Scheduler,
  now: datetime,
  threshold_days: int,
) -> list[str]:
  """Live attempts that are fully terminal AND whose event log
  has been idle past the threshold (every dispatch/patch/
  transition appends, so log mtime is 'last activity')."""
  threshold_dt = now - timedelta(days=threshold_days)
  out: list[str] = []
  for aid in list(scheduler.iter_attempt_ids()):
    if scheduler.is_archived(aid):
      continue
    view = scheduler.attempt_view(aid)
    if view.pending or view.running or view.unknown or view.ghosted:
      continue
    log_path = event_log_path_for(scheduler.attempt_state(aid))
    try:
      mtime = datetime.fromtimestamp(log_path.stat().st_mtime, tz=UTC)
    except OSError:
      continue  # never archive from thin air
    if mtime > threshold_dt:
      continue
    out.append(aid)
  return out
