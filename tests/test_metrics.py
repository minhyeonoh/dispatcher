"""MetricsCache counting rules."""

from __future__ import annotations

import pytest

from dispatcher.core.metrics import MetricsCache
from dispatcher.core.outcome import CompletionSnapshot


def snap(
  *,
  exists: bool = True,
  error: bool = False,
  values: dict[str, float] | None = None,
) -> CompletionSnapshot:
  return CompletionSnapshot(
    outcome_exists=exists,
    error_present=error,
    values=values or {},
  )


def test_empty_metrics_has_no_means():
  m = MetricsCache().get("A")
  assert m.ok == 0 and m.err == 0
  assert m.means == {}
  assert m.means_all == {}


def test_single_ok_value_gives_that_value_as_mean():
  c = MetricsCache()
  c.record_completion("A", snap(values={"reward": 0.5}))
  assert c.get("A").means == {"reward": 0.5}


def test_mean_over_ok_instances_reporting_the_key():
  c = MetricsCache()
  c.record_completion("A", snap(values={"reward": 1.0}))
  c.record_completion("A", snap(values={"reward": 0.0}))
  c.record_completion("A", snap())  # ok, key absent
  m = c.get("A")
  assert m.ok == 3
  assert m.means == {"reward": 0.5}


def test_ok_completion_increments_ok_and_values():
  c = MetricsCache()
  c.record_completion("A", snap(values={"reward": 1.0}))
  m = c.get("A")
  assert m.ok == 1 and m.err == 0
  assert m.value_counts == {"reward": 1}


def test_error_completion_increments_err_only():
  c = MetricsCache()
  c.record_completion("A", snap(error=True, values={"reward": 1.0}))
  m = c.get("A")
  assert m.ok == 0 and m.err == 1
  assert m.value_counts == {}


def test_completion_without_outcome_is_a_no_op():
  # The instance is `unknown` — no evidence, no contribution. A later
  # reclassify adds it, so err never inflates on NFS lag.
  c = MetricsCache()
  c.record_completion("A", snap(exists=False))
  m = c.get("A")
  assert m.ok == 0 and m.err == 0


def test_ok_without_values_counts_ok_but_no_means():
  c = MetricsCache()
  c.record_completion("A", snap())
  m = c.get("A")
  assert m.ok == 1
  assert m.means == {}


def test_multiple_value_keys_accumulate_independently():
  c = MetricsCache()
  c.record_completion("A", snap(values={"reward": 1.0, "steps": 10.0}))
  c.record_completion("A", snap(values={"reward": 0.0}))
  m = c.get("A")
  assert m.means == {"reward": 0.5, "steps": 10.0}


def test_get_of_unknown_job_returns_zero_metrics():
  m = MetricsCache().get("nope")
  assert m.ok == 0 and m.err == 0


def test_multiple_jobs_are_isolated():
  c = MetricsCache()
  c.record_completion("A", snap(values={"r": 1.0}))
  c.record_completion("B", snap(error=True))
  assert c.get("A").ok == 1 and c.get("A").err == 0
  assert c.get("B").ok == 0 and c.get("B").err == 1


def test_remove_clears_the_job():
  c = MetricsCache()
  c.record_completion("A", snap())
  c.remove("A")
  assert c.get("A").ok == 0


def test_remove_unknown_job_is_noop():
  MetricsCache().remove("nope")


def test_reclassify_from_unknown_to_done_ok_adds_ok_and_values():
  c = MetricsCache()
  c.reclassify_from_unknown(
    "A", to_state="done_ok", values={"reward": 1.0}
  )
  m = c.get("A")
  assert m.ok == 1
  assert m.means == {"reward": 1.0}


def test_reclassify_from_unknown_to_done_err_adds_err_only():
  c = MetricsCache()
  c.reclassify_from_unknown("A", to_state="done_err", values=None)
  m = c.get("A")
  assert m.err == 1 and m.ok == 0


def test_reclassify_from_unknown_to_ghosted_is_a_no_op():
  c = MetricsCache()
  c.reclassify_from_unknown("A", to_state="ghosted", values=None)
  m = c.get("A")
  assert m.ok == 0 and m.err == 0


def test_reclassify_from_unknown_to_running_is_a_no_op():
  c = MetricsCache()
  c.reclassify_from_unknown("A", to_state="running", values=None)
  m = c.get("A")
  assert m.ok == 0 and m.err == 0


def test_reclassify_from_ghosted_to_done_ok_adds_ok_and_values():
  c = MetricsCache()
  c.reclassify_from_ghosted(
    "A", to_state="done_ok", values={"reward": 0.25}
  )
  m = c.get("A")
  assert m.ok == 1
  assert m.means == {"reward": 0.25}


def test_reclassify_from_ghosted_to_done_err_adds_err():
  c = MetricsCache()
  c.reclassify_from_ghosted("A", to_state="done_err", values=None)
  assert c.get("A").err == 1


def test_reclassify_from_ghosted_rejects_non_terminal_states():
  c = MetricsCache()
  for state in ("ghosted", "running", "unknown"):
    with pytest.raises(ValueError):
      c.reclassify_from_ghosted("A", to_state=state, values=None)


def test_means_all_counts_err_as_zero_but_not_ghosted():
  c = MetricsCache()
  c.record_completion("A", snap(values={"reward": 1.0}))
  c.record_completion("A", snap(error=True))
  # A ghosted instance contributes nothing anywhere.
  c.reclassify_from_unknown("A", to_state="ghosted", values=None)
  m = c.get("A")
  assert m.means == {"reward": 1.0}
  assert m.means_all == {"reward": 0.5}


def test_undo_done_err_rolls_back_one_err():
  c = MetricsCache()
  c.record_completion("A", snap(error=True))
  c.undo_done_err("A")
  assert c.get("A").err == 0
  c.undo_done_err("A")  # floored at zero
  assert c.get("A").err == 0


def test_non_numeric_values_are_skipped():
  c = MetricsCache()
  c.record_completion(
    "A",
    CompletionSnapshot(
      outcome_exists=True,
      error_present=False,
      values={"reward": 1.0, "flag": True},  # type: ignore[dict-item]
    ),
  )
  assert c.get("A").means == {"reward": 1.0}
