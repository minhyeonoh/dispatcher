"""On-disk log/index behaviour: truncated tails, index-driven
discovery, outcome scanning, trial-name counter parsing."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from dispatcher.event_log import (
  append_event,
  append_index_entry,
  find_event_logs,
  read_events,
  scan_outcomes,
  seq_in_trial_name,
)

if TYPE_CHECKING:
  from pathlib import Path


def test_append_then_read_round_trips(tmp_path: Path):
  log = tmp_path / "state.jsonl"
  append_event(log, {"type": "submit", "attempt_id": "A"})
  append_event(log, {"type": "patch", "attempt_id": "A", "w": 2})
  assert read_events(log) == [
    {"type": "submit", "attempt_id": "A"},
    {"type": "patch", "attempt_id": "A", "w": 2},
  ]


def test_append_creates_parent_dirs(tmp_path: Path):
  log = tmp_path / "a" / "b" / "state.jsonl"
  append_event(log, {"type": "submit"})
  assert read_events(log) == [{"type": "submit"}]


def test_truncated_last_line_dropped_silently(tmp_path: Path):
  log = tmp_path / "state.jsonl"
  append_event(log, {"type": "submit", "attempt_id": "A"})
  with log.open("a") as f:
    f.write('{"type": "dis')  # crash mid-append
  events = read_events(log)
  assert events == [{"type": "submit", "attempt_id": "A"}]


def test_malformed_middle_line_raises(tmp_path: Path):
  log = tmp_path / "state.jsonl"
  log.write_text('{"a": 1}\ngarbage\n{"b": 2}\n')
  with pytest.raises(json.JSONDecodeError):
    read_events(log)


def test_blank_lines_skipped(tmp_path: Path):
  log = tmp_path / "state.jsonl"
  log.write_text('{"a": 1}\n\n{"b": 2}\n')
  assert read_events(log) == [{"a": 1}, {"b": 2}]


# ── index ────────────────────────────────────────────────────────


def test_find_event_logs_empty_when_no_index(tmp_path: Path):
  assert find_event_logs(tmp_path) == []


def test_find_event_logs_lists_submitted(tmp_path: Path):
  append_index_entry(
    tmp_path,
    {"event": "submit", "attempt_id": "A", "log_path": "/x/a.jsonl"},
  )
  append_index_entry(
    tmp_path,
    {"event": "submit", "attempt_id": "B", "log_path": "/x/b.jsonl"},
  )
  assert [str(p) for p in find_event_logs(tmp_path)] == [
    "/x/a.jsonl",
    "/x/b.jsonl",
  ]


def test_find_event_logs_skips_cancelled(tmp_path: Path):
  append_index_entry(
    tmp_path,
    {"event": "submit", "attempt_id": "A", "log_path": "/x/a.jsonl"},
  )
  append_index_entry(tmp_path, {"event": "cancel", "attempt_id": "A"})
  assert find_event_logs(tmp_path) == []


def test_find_event_logs_skips_malformed_lines(tmp_path: Path):
  append_index_entry(
    tmp_path,
    {"event": "submit", "attempt_id": "A", "log_path": "/x/a.jsonl"},
  )
  from dispatcher.event_log import index_path

  with index_path(tmp_path).open("a") as f:
    f.write("not-json\n")
  assert [str(p) for p in find_event_logs(tmp_path)] == ["/x/a.jsonl"]


# ── outcome scan ─────────────────────────────────────────────────


def test_scan_outcomes_maps_trial_to_envelope(tmp_path: Path):
  t1 = tmp_path / "t1__0000001"
  t1.mkdir()
  (t1 / "outcome.json").write_text(
    json.dumps({"ok": True, "values": {"r": 1.0}})
  )
  t2 = tmp_path / "t2__0000002"
  t2.mkdir()  # no outcome — omitted
  out = scan_outcomes(tmp_path)
  assert set(out) == {"t1__0000001"}
  assert out["t1__0000001"].values == {"r": 1.0}


def test_scan_outcomes_skips_malformed_and_dotdirs(tmp_path: Path):
  bad = tmp_path / "t1__0000001"
  bad.mkdir()
  (bad / "outcome.json").write_text("{broken")
  dot = tmp_path / ".dispatcher-stuff"
  dot.mkdir()
  (dot / "outcome.json").write_text(json.dumps({"ok": True}))
  assert scan_outcomes(tmp_path) == {}


def test_scan_outcomes_missing_root_is_empty(tmp_path: Path):
  assert scan_outcomes(tmp_path / "nope") == {}


# ── trial-name counter ───────────────────────────────────────────


def test_seq_parsed_from_a_minted_name():
  assert seq_in_trial_name("task_a__0000042") == 42


def test_seq_of_foreign_name_is_zero():
  assert seq_in_trial_name("task_a__abc1234") == 0


def test_seq_without_separator_is_zero():
  assert seq_in_trial_name("task_a-0000042") == 0
