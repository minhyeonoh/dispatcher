"""Host selection policy."""

from __future__ import annotations

from dispatcher.core.models import HostSettings
from dispatcher.core.pick_host import pick_host


def _h(cap: int, active: bool = True, alive: bool = True):
  return HostSettings(max_concurrent=cap, active=active, alive=alive)


def test_single_host_free_is_picked():
  assert pick_host({"a": _h(2)}, {"a": 1}) == "a"


def test_single_host_full_returns_none():
  assert pick_host({"a": _h(2)}, {"a": 2}) is None


def test_two_hosts_one_full_picks_free_one():
  assert pick_host({"a": _h(1), "b": _h(1)}, {"a": 1, "b": 0}) == "b"


def test_lower_utilization_wins():
  assert pick_host({"a": _h(10), "b": _h(10)}, {"a": 5, "b": 2}) == "b"


def test_heterogeneous_cap_respects_ratio():
  # a: 5/25 = 0.2 vs b: 2/5 = 0.4 → a wins despite more running.
  assert pick_host({"a": _h(25), "b": _h(5)}, {"a": 5, "b": 2}) == "a"


def test_tied_utilization_picks_first_in_mapping():
  assert pick_host({"a": _h(10), "b": _h(10)}, {"a": 3, "b": 3}) == "a"


def test_tied_utilization_reversed_insertion_order():
  assert pick_host({"b": _h(10), "a": _h(10)}, {"a": 3, "b": 3}) == "b"


def test_all_full_returns_none():
  assert pick_host({"a": _h(1), "b": _h(2)}, {"a": 1, "b": 2}) is None


def test_empty_hosts_returns_none():
  assert pick_host({}, {}) is None


def test_zero_cap_host_never_picked():
  assert pick_host({"a": _h(0), "b": _h(1)}, {}) == "b"


def test_zero_cap_only_returns_none():
  assert pick_host({"a": _h(0)}, {}) is None


def test_inactive_host_never_picked():
  assert pick_host({"a": _h(5, active=False), "b": _h(1)}, {}) == "b"


def test_dead_host_never_picked():
  assert pick_host({"a": _h(5, alive=False), "b": _h(1)}, {}) == "b"


def test_all_inactive_returns_none():
  assert (
    pick_host({"a": _h(5, active=False), "b": _h(5, active=False)}, {})
    is None
  )


def test_running_missing_host_treated_as_zero():
  assert pick_host({"a": _h(2)}, {}) == "a"


def test_running_extra_host_ignored():
  assert pick_host({"a": _h(2)}, {"a": 0, "zzz": 99}) == "a"


def test_raise_cap_reopens_candidacy():
  hosts = {"a": _h(1)}
  running = {"a": 1}
  assert pick_host(hosts, running) is None
  hosts["a"] = _h(2)
  assert pick_host(hosts, running) == "a"


def test_lower_cap_makes_host_soft_over():
  # Cap lowered below current running: host is simply ineligible;
  # nothing crashes, nothing gets killed.
  assert pick_host({"a": _h(1)}, {"a": 3}) is None
