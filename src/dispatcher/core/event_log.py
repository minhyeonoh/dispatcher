"""Per-attempt event log: append-only jsonl, replayed at startup.

Event types:
- `submit`         — first event; AttemptState fields verbatim.
- `patch`          — knob mutation, last-wins per field.
- `dispatch`       — appends a DispatchEntry.
- `pause_on_error` — automatic pause record; sets paused=True.
- `unknown`        — audit breadcrumb (trial ended, no outcome);
                     replay ignores it — restore re-derives the
                     bucket from disk.
- `reclaim`/`retry`— operator retraction; erases the NAMED
                     dispatch from the log so restore sees the
                     task as pending. Matching is by trial_id:
                     an infra requeue leaves two dispatches for
                     one task, and erasing "the last one for the
                     task" once deleted a successful retry while
                     restoring the dead first try.
- `notify_fired`   — threshold bookkeeping.
- `archive` / `unarchive` — archive marks.
- `cancel`         — replay short-circuits to None; restore skips
                     the attempt (log stays as audit trail).
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from dispatcher.core.models import (
  OUTCOME_FILENAME,
  AttemptState,
  DispatchEntry,
  Outcome,
)

RUN_LOG_FILENAME = ".dispatcher-state.jsonl"
INDEX_FILENAME = "attempts-index.jsonl"


class ReplayError(Exception):
  """Malformed event stream (empty, missing submit, unknown type,
  attempt_id mismatch, bad record)."""


_KNOB_FIELDS = (
  "paused",
  "weight",
  "max_concurrent",
  "pause_on_error",
  "alias",
  "tags",
  "pool",
)


def replay_events(
  events: list[dict[str, Any]],
) -> tuple[AttemptState, list[DispatchEntry]] | None:
  """Fold one attempt's event stream into (AttemptState,
  dispatch log). Returns None when the stream contains `cancel`."""
  if not events:
    raise ReplayError("empty event stream")

  first = events[0]
  if first.get("type") != "submit":
    raise ReplayError(
      f"first event must be a submit, got {first.get('type')!r}"
    )

  state = _apply_submit(first)
  log: list[DispatchEntry] = []

  for i, ev in enumerate(events[1:], start=1):
    typ = ev.get("type")
    if typ is None:
      raise ReplayError(f"event {i}: missing 'type'")
    if ev.get("attempt_id") != state.attempt_id:
      raise ReplayError(
        f"event {i}: attempt_id mismatch "
        f"({ev.get('attempt_id')!r} vs {state.attempt_id!r})"
      )

    if typ == "submit":
      raise ReplayError(f"event {i}: duplicate 'submit'")
    elif typ == "patch":
      _apply_patch(state, ev, event_index=i)
    elif typ == "dispatch":
      log.append(_parse_dispatch(ev, event_index=i))
    elif typ == "pause_on_error":
      state.paused = True
    elif typ == "unknown":
      pass
    elif typ in ("reclaim", "retry"):
      _erase_dispatch(log, ev, kind=typ, event_index=i)
    elif typ == "notify_fired":
      t = ev.get("threshold")
      if (
        isinstance(t, (int, float))
        and float(t) not in state.notified_thresholds
      ):
        state.notified_thresholds.append(float(t))
    elif typ == "cancel":
      return None
    elif typ == "archive":
      _apply_archive(state, ev, event_index=i)
    elif typ == "unarchive":
      state.archived_at = None
      state.archive_kind = ""
    else:
      raise ReplayError(f"event {i}: unknown type {typ!r}")

  return state, log


def _apply_submit(ev: dict[str, Any]) -> AttemptState:
  payload = {k: v for k, v in ev.items() if k not in ("type", "at")}
  try:
    return AttemptState.model_validate(payload)
  except Exception as e:
    raise ReplayError(f"submit invalid: {e}") from e


def _apply_patch(
  state: AttemptState, ev: dict[str, Any], *, event_index: int
) -> None:
  touched = 0
  for key, value in ev.items():
    if key in ("type", "attempt_id", "at"):
      continue
    if key not in _KNOB_FIELDS:
      raise ReplayError(
        f"event {event_index}: patch of unknown field {key!r}"
      )
    setattr(state, key, value)
    touched += 1
  if touched == 0:
    raise ReplayError(
      f"event {event_index}: patch with no field mutations"
    )


def _apply_archive(
  state: AttemptState, ev: dict[str, Any], *, event_index: int
) -> None:
  at = ev.get("at")
  kind = ev.get("kind", "manual")
  if kind not in ("manual", "auto"):
    raise ReplayError(
      f"event {event_index}: archive kind must be 'manual' or "
      f"'auto', got {kind!r}"
    )
  if isinstance(at, str):
    try:
      at_dt = datetime.fromisoformat(at)
    except ValueError as e:
      raise ReplayError(
        f"event {event_index}: archive 'at' not ISO-8601: {at!r}"
      ) from e
  elif isinstance(at, datetime):
    at_dt = at
  else:
    raise ReplayError(
      f"event {event_index}: archive missing / bad 'at' ({at!r})"
    )
  state.archived_at = at_dt
  state.archive_kind = kind


def _erase_dispatch(
  log: list[DispatchEntry],
  ev: dict[str, Any],
  *,
  kind: str,
  event_index: int,
) -> None:
  task_id = ev.get("task_id")
  if task_id is None:
    raise ReplayError(f"event {event_index}: {kind} missing 'task_id'")
  trial_id = ev.get("trial_id")
  if trial_id is None:
    raise ReplayError(f"event {event_index}: {kind} missing 'trial_id'")
  # A trial_id matching nothing erases nothing: the dispatch it
  # names is already gone from the log.
  for j in range(len(log) - 1, -1, -1):
    if log[j].task_id != task_id:
      continue
    if log[j].trial_id != trial_id:
      continue
    del log[j]
    return


def _parse_dispatch(
  ev: dict[str, Any], *, event_index: int
) -> DispatchEntry:
  payload = {k: v for k, v in ev.items() if k != "type"}
  if "dispatched_at" not in payload and "at" in payload:
    payload["dispatched_at"] = payload.pop("at")
  else:
    payload.pop("at", None)
  try:
    return DispatchEntry.model_validate(payload)
  except Exception as e:
    raise ReplayError(f"event {event_index}: dispatch invalid: {e}") from e


# ── persistence ──────────────────────────────────────────────────


def event_log_path_for(state: AttemptState) -> Path:
  """The attempt's on-disk log — inside its home_root so the log
  rides with the attempt's artifacts."""
  return state.home_root / RUN_LOG_FILENAME


def append_event(log_path: Path, event: dict[str, Any]) -> None:
  """Append one event as one JSON line. A crash between the
  in-memory mutation and this call loses one event; a crash
  mid-append leaves a truncated last line that `read_events`
  drops."""
  log_path.parent.mkdir(parents=True, exist_ok=True)
  with log_path.open("a", encoding="utf-8") as f:
    f.write(json.dumps(event, default=str) + "\n")


# One lock per log path. Offloading appends to threads makes
# concurrent appends to the same file possible, and O_APPEND is
# NOT atomic on NFS — unserialized appends can interleave bytes.
# Every append that can run concurrently (runtime, ops, notify)
# must go through append_event_async; plain append_event is for
# single-threaded startup paths (restore backfill).
_append_locks: dict[Path, asyncio.Lock] = {}


async def append_event_async(
  log_path: Path, event: dict[str, Any]
) -> None:
  """`append_event` off the event loop, serialized per path so a
  slow NFS write stalls only its caller, never the loop — and so
  two appends can't interleave on the wire."""
  lock = _append_locks.setdefault(log_path, asyncio.Lock())
  async with lock:
    await asyncio.to_thread(append_event, log_path, event)


async def append_index_entry_async(
  data_dir: Path, entry: dict[str, Any]
) -> None:
  lock = _append_locks.setdefault(index_path(data_dir), asyncio.Lock())
  async with lock:
    await asyncio.to_thread(append_index_entry, data_dir, entry)


def read_events(log_path: Path) -> list[dict[str, Any]]:
  """Parse a log file. A malformed LAST line (crash mid-append) is
  dropped silently; malformed earlier lines mean corruption and
  raise."""
  events: list[dict[str, Any]] = []
  lines = log_path.read_text(encoding="utf-8").splitlines()
  for i, line in enumerate(lines):
    if not line.strip():
      continue
    try:
      events.append(json.loads(line))
    except json.JSONDecodeError:
      if i != len(lines) - 1:
        raise
  return events


def index_path(data_dir: Path) -> Path:
  """Append-only index of submitted/cancelled attempts. Startup
  reads this instead of walking the filesystem — home roots live
  on NFS where a tree walk over years of artifacts stalls for
  minutes."""
  return data_dir / INDEX_FILENAME


def append_index_entry(data_dir: Path, entry: dict[str, Any]) -> None:
  path = index_path(data_dir)
  path.parent.mkdir(parents=True, exist_ok=True)
  with path.open("a", encoding="utf-8") as f:
    f.write(json.dumps(entry, default=str) + "\n")


def find_event_logs(data_dir: Path) -> list[Path]:
  """Log paths of every submitted-and-not-cancelled attempt, in
  deterministic (sorted) order. Malformed index lines are
  skipped."""
  index = index_path(data_dir)
  if not index.is_file():
    return []
  submitted: dict[str, Path] = {}
  cancelled: set[str] = set()
  for line in index.read_text(encoding="utf-8").splitlines():
    if not line.strip():
      continue
    try:
      entry = json.loads(line)
    except json.JSONDecodeError:
      continue
    aid = entry.get("attempt_id")
    if not isinstance(aid, str):
      continue
    event = entry.get("event")
    if event == "submit":
      log_str = entry.get("log_path")
      if isinstance(log_str, str):
        submitted[aid] = Path(log_str)
    elif event == "cancel":
      cancelled.add(aid)
  return sorted(
    log for aid, log in submitted.items() if aid not in cancelled
  )


def scan_outcomes(home_root: Path) -> dict[str, Outcome]:
  """`{trial_id: Outcome}` for every trial dir under an attempt
  with a parseable outcome.json. Missing / malformed files are
  omitted — same non-answer as the live poll, so restore and the
  resolver classify them identically (unknown)."""
  out: dict[str, Outcome] = {}
  if not home_root.is_dir():
    return out
  for trial_dir in home_root.iterdir():
    if not trial_dir.is_dir() or trial_dir.name.startswith("."):
      continue
    path = trial_dir / OUTCOME_FILENAME
    if not path.is_file():
      continue
    try:
      out[trial_dir.name] = Outcome.model_validate_json(
        path.read_text(encoding="utf-8")
      )
    except Exception:
      continue
  return out


def seq_in_trial_id(trial_id: str) -> int:
  """The monotonic counter a dispatcher-minted trial name carries
  (`<task>__<seq>`), or 0 for foreign names. Restore feeds the max
  into the namer so a restart never re-mints a name that already
  owns a trial dir — the old dir's outcome would be read as the
  new trial's before it ran."""
  _, separator, suffix = trial_id.rpartition("__")
  if not separator or not suffix.isdigit():
    return 0
  return int(suffix)
