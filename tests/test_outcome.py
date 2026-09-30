"""Outcome-envelope reading."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from dispatcher.core.outcome import instance_home_for, read_completion

if TYPE_CHECKING:
  from pathlib import Path


def write_outcome(instance_home: Path, payload: dict) -> None:
  instance_home.mkdir(parents=True, exist_ok=True)
  (instance_home / "outcome.json").write_text(json.dumps(payload))


def test_no_outcome_returns_none(tmp_path: Path):
  (tmp_path / "t").mkdir()
  assert read_completion(tmp_path / "t") is None


def test_missing_instance_dir_returns_none(tmp_path: Path):
  assert read_completion(tmp_path / "nope") is None


def test_ok_outcome_is_clean_completion(tmp_path: Path):
  write_outcome(tmp_path / "t", {"ok": True})
  snap = read_completion(tmp_path / "t")
  assert snap is not None
  assert snap.outcome_exists is True
  assert snap.error_present is False
  assert snap.infra is False


def test_error_outcome_flags_error(tmp_path: Path):
  write_outcome(
    tmp_path / "t",
    {"ok": False, "error": {"type": "E", "message": "boom"}},
  )
  snap = read_completion(tmp_path / "t")
  assert snap is not None
  assert snap.error_present is True
  assert snap.outcome is not None
  assert snap.outcome.error is not None
  assert snap.outcome.error.message == "boom"


def test_ok_false_without_error_object_still_error(tmp_path: Path):
  write_outcome(tmp_path / "t", {"ok": False})
  snap = read_completion(tmp_path / "t")
  assert snap is not None
  assert snap.error_present is True


def test_ok_true_with_error_object_read_as_error(tmp_path: Path):
  # Contradictory envelope — don't trust the flag.
  write_outcome(
    tmp_path / "t",
    {"ok": True, "error": {"type": "E", "message": "?"}},
  )
  snap = read_completion(tmp_path / "t")
  assert snap is not None
  assert snap.error_present is True


def test_values_extracted(tmp_path: Path):
  write_outcome(tmp_path / "t", {"ok": True, "values": {"reward": 0.75}})
  snap = read_completion(tmp_path / "t")
  assert snap is not None
  assert snap.values == {"reward": 0.75}


def test_value_zero_is_preserved(tmp_path: Path):
  write_outcome(tmp_path / "t", {"ok": True, "values": {"reward": 0.0}})
  snap = read_completion(tmp_path / "t")
  assert snap is not None
  assert snap.values == {"reward": 0.0}


def test_missing_values_yields_empty(tmp_path: Path):
  write_outcome(tmp_path / "t", {"ok": True})
  snap = read_completion(tmp_path / "t")
  assert snap is not None
  assert snap.values == {}


def test_infra_flag_carried(tmp_path: Path):
  write_outcome(
    tmp_path / "t",
    {
      "ok": False,
      "error": {"type": "InfraFailure", "message": "worker swap"},
      "infra": True,
    },
  )
  snap = read_completion(tmp_path / "t")
  assert snap is not None
  assert snap.infra is True


def test_malformed_json_returns_none(tmp_path: Path):
  d = tmp_path / "t"
  d.mkdir()
  (d / "outcome.json").write_text('{"ok": tru')  # truncated write
  assert read_completion(d) is None


def test_wrong_schema_returns_none(tmp_path: Path):
  write_outcome(tmp_path / "t", {"okay": "yes"})
  assert read_completion(tmp_path / "t") is None


def test_opaque_data_passthrough(tmp_path: Path):
  write_outcome(
    tmp_path / "t",
    {"ok": True, "data": {"anything": [1, 2, {"x": None}]}},
  )
  snap = read_completion(tmp_path / "t")
  assert snap is not None
  assert snap.outcome is not None
  assert snap.outcome.data == {"anything": [1, 2, {"x": None}]}


def test_instance_home_for_composition(tmp_path: Path):
  assert (
    instance_home_for(tmp_path, "t1__0000001") == tmp_path / "t1__0000001"
  )
