"""Autotune math, tracker, ring persistence."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from dispatcher.host_autotune import (
  HostAutotuneState,
  TrialPeak,
  advised_cap,
  append_peak,
  load_ring,
  p75,
  peak_estimate,
  truncate_ring_file,
  update_tracker,
)
from dispatcher.host_metrics import HostSample, TrialStat

if TYPE_CHECKING:
  from pathlib import Path

GIB = 1024**3
NOW = datetime(2026, 9, 28, 12, 0, tzinfo=UTC)


def _peak(rss: int, host: str = "h", name: str = "t") -> TrialPeak:
  return TrialPeak(
    host=host,
    trial_name=name,
    peak_rss=rss,
    sample_count=3,
    first_seen=NOW,
    last_seen=NOW,
  )


def _sample(trials: list[TrialStat]) -> HostSample:
  return HostSample(
    host="h",
    mem_total_bytes=256 * GIB,
    mem_avail_bytes=100 * GIB,
    nproc=64,
    loadavg_5m=1.0,
    disk_root_free_bytes=GIB,
    disk_root_total_bytes=10 * GIB,
    trials=tuple(trials),
  )


# ── p75 ──────────────────────────────────────────────────────────


def test_p75_single_value():
  assert p75([10]) == 10


def test_p75_two_values_linear():
  assert p75([0, 100]) == 75


def test_p75_ignores_order():
  assert p75([3, 1, 2, 4]) == p75([4, 3, 2, 1])


def test_p75_empty_raises():
  with pytest.raises(ValueError):
    p75([])


# ── peak_estimate ────────────────────────────────────────────────


def test_peak_estimate_uses_host_ring_when_full():
  host_ring = [_peak(2 * GIB)] * 8
  global_ring = [_peak(9 * GIB)] * 20
  assert peak_estimate(host_ring, global_ring, 8, GIB) == 2 * GIB


def test_peak_estimate_falls_back_to_global():
  host_ring = [_peak(2 * GIB)] * 3  # below bootstrap
  global_ring = [_peak(4 * GIB)] * 8
  assert peak_estimate(host_ring, global_ring, 8, GIB) == 4 * GIB


def test_peak_estimate_none_when_below_floor_globally():
  assert peak_estimate([_peak(GIB)], [_peak(GIB)] * 2, 8, GIB) is None


def test_peak_estimate_floor_wins_over_small_p75():
  ring = [_peak(100)] * 8
  assert peak_estimate(ring, [], 8, GIB) == GIB


# ── advised_cap ──────────────────────────────────────────────────


def test_advised_cap_headroom_math():
  cap = advised_cap(
    mem_total_bytes=100 * GIB,
    mem_avail_bytes=60 * GIB,
    running_count=3,
    peak_estimate_bytes=10 * GIB,
    reserve_fraction=0.1,  # reserve 10 GiB → 50 free → 5 headroom
    operator_ceiling=100,
  )
  assert cap == 8


def test_advised_cap_ceiling_wins():
  cap = advised_cap(
    mem_total_bytes=100 * GIB,
    mem_avail_bytes=90 * GIB,
    running_count=0,
    peak_estimate_bytes=GIB,
    reserve_fraction=0.1,
    operator_ceiling=4,
  )
  assert cap == 4


def test_advised_cap_never_zero():
  cap = advised_cap(
    mem_total_bytes=100 * GIB,
    mem_avail_bytes=0,
    running_count=0,
    peak_estimate_bytes=10 * GIB,
    reserve_fraction=0.1,
    operator_ceiling=100,
  )
  assert cap == 1


def test_advised_cap_running_absorbs_negative_headroom():
  cap = advised_cap(
    mem_total_bytes=100 * GIB,
    mem_avail_bytes=5 * GIB,
    running_count=7,
    peak_estimate_bytes=10 * GIB,
    reserve_fraction=0.1,
    operator_ceiling=100,
  )
  assert cap == 7  # running + 0 headroom


# ── tracker ──────────────────────────────────────────────────────


def _state(tmp_path: Path) -> HostAutotuneState:
  return HostAutotuneState(metrics_file=tmp_path / "m.jsonl")


def test_update_tracker_new_container_seeds_max(tmp_path: Path):
  state = _state(tmp_path)
  update_tracker(
    state, "h", _sample([TrialStat("c1", 0.0, 5 * GIB)]), NOW, 50
  )
  assert state.tracked["h"]["c1"].max_rss == 5 * GIB
  assert state.tracked["h"]["c1"].sample_count == 1


def test_update_tracker_persistent_container_grows_max(
  tmp_path: Path,
):
  state = _state(tmp_path)
  update_tracker(
    state, "h", _sample([TrialStat("c1", 0.0, 5 * GIB)]), NOW, 50
  )
  update_tracker(
    state,
    "h",
    _sample([TrialStat("c1", 0.0, 7 * GIB)]),
    NOW + timedelta(minutes=1),
    50,
  )
  update_tracker(
    state,
    "h",
    _sample([TrialStat("c1", 0.0, 6 * GIB)]),
    NOW + timedelta(minutes=2),
    50,
  )
  tt = state.tracked["h"]["c1"]
  assert tt.max_rss == 7 * GIB  # monotone
  assert tt.sample_count == 3


def test_update_tracker_vanished_container_flushes_to_ring(
  tmp_path: Path,
):
  state = _state(tmp_path)
  update_tracker(
    state, "h", _sample([TrialStat("c1", 0.0, 5 * GIB)]), NOW, 50
  )
  completed = update_tracker(
    state, "h", _sample([]), NOW + timedelta(minutes=1), 50
  )
  assert [p.trial_name for p in completed] == ["c1"]
  assert [p.peak_rss for p in state.ring["h"]] == [5 * GIB]
  assert state.tracked["h"] == {}


def test_update_tracker_ring_trims_to_size(tmp_path: Path):
  state = _state(tmp_path)
  for i in range(5):
    update_tracker(
      state,
      "h",
      _sample([TrialStat(f"c{i}", 0.0, GIB)]),
      NOW + timedelta(minutes=i),
      50,
    )
    update_tracker(
      state,
      "h",
      _sample([]),
      NOW + timedelta(minutes=i, seconds=30),
      ring_size=3,
    )
  assert len(state.ring["h"]) == 3


# ── ring persistence ─────────────────────────────────────────────


def test_load_ring_missing_file_empty(tmp_path: Path):
  assert load_ring(tmp_path / "nope.jsonl", 50) == {}


def test_append_then_load_round_trips(tmp_path: Path):
  f = tmp_path / "m.jsonl"
  append_peak(f, _peak(5 * GIB, name="a"))
  append_peak(f, _peak(6 * GIB, name="b"))
  ring = load_ring(f, 50)
  assert [p.trial_name for p in ring["h"]] == ["a", "b"]


def test_load_ring_trims_per_host(tmp_path: Path):
  f = tmp_path / "m.jsonl"
  for i in range(10):
    append_peak(f, _peak(i * GIB, name=f"t{i}"))
  ring = load_ring(f, 3)
  assert [p.trial_name for p in ring["h"]] == ["t7", "t8", "t9"]


def test_load_ring_skips_malformed_line(tmp_path: Path):
  f = tmp_path / "m.jsonl"
  append_peak(f, _peak(GIB, name="good"))
  with f.open("a") as fh:
    fh.write("{truncat\n")
  ring = load_ring(f, 50)
  assert [p.trial_name for p in ring["h"]] == ["good"]


def test_truncate_ring_file_shrinks_disk(tmp_path: Path):
  f = tmp_path / "m.jsonl"
  for i in range(10):
    append_peak(f, _peak(GIB, name=f"t{i}"))
  ring = load_ring(f, 2)
  truncate_ring_file(f, ring)
  assert len(f.read_text().splitlines()) == 2
