"""Event-log replay: fold submit/patch/dispatch/… into
(AttemptState, dispatch log)."""

from __future__ import annotations

from typing import Any

import pytest

from dispatcher.event_log import ReplayError, replay_events


def submit_event(attempt_id: str = "A", **extra: Any) -> dict:
  return {
    "type": "submit",
    "attempt_id": attempt_id,
    "label": attempt_id,
    "task_list": ["t1", "t2"],
    "home_root": f"/data/{attempt_id}",
    "container": {"image": "img"},
    "submitted_at": "2026-09-28T10:00:00+00:00",
    "alias": f"alias-{attempt_id}",
    **extra,
  }


def dispatch_event(task: str, trial: str, attempt_id: str = "A") -> dict:
  return {
    "type": "dispatch",
    "attempt_id": attempt_id,
    "task_name": task,
    "trial_name": trial,
    "host": "ml10",
    "at": "2026-09-28T10:00:01+00:00",
  }


# ── submit ───────────────────────────────────────────────────────


def test_submit_populates_immutable_fields():
  out = replay_events([submit_event()])
  assert out is not None
  state, log = out
  assert state.attempt_id == "A"
  assert state.task_list == ["t1", "t2"]
  assert str(state.home_root) == "/data/A"
  assert state.container.image == "img"
  assert state.alias == "alias-A"
  assert log == []


def test_submit_defaults_scheduler_knobs():
  out = replay_events([submit_event()])
  assert out is not None
  state, _ = out
  assert state.paused is False
  assert state.weight == 1
  assert state.max_concurrent is None
  assert state.pause_on_error is None
  assert state.pool == "default"
  assert state.tags == []
  assert state.notified_thresholds == []
  assert state.archived_at is None


# ── patch ────────────────────────────────────────────────────────


def _patch(attempt_id: str = "A", **fields: Any) -> dict:
  return {
    "type": "patch",
    "attempt_id": attempt_id,
    "at": "…",
    **fields,
  }


def test_patch_sets_paused():
  out = replay_events([submit_event(), _patch(paused=True)])
  assert out is not None
  assert out[0].paused is True


def test_patch_weight():
  out = replay_events([submit_event(), _patch(weight=7)])
  assert out is not None
  assert out[0].weight == 7


def test_patch_max_concurrent():
  out = replay_events([submit_event(), _patch(max_concurrent=3)])
  assert out is not None
  assert out[0].max_concurrent == 3


def test_patch_pause_on_error_both_ways():
  out = replay_events([submit_event(), _patch(pause_on_error=True)])
  assert out is not None
  assert out[0].pause_on_error is True
  out = replay_events([submit_event(), _patch(pause_on_error=False)])
  assert out is not None
  assert out[0].pause_on_error is False


def test_multiple_patches_last_wins_per_field():
  out = replay_events(
    [
      submit_event(),
      _patch(weight=2),
      _patch(weight=9),
      _patch(paused=True),
      _patch(paused=False),
    ]
  )
  assert out is not None
  state, _ = out
  assert state.weight == 9
  assert state.paused is False


def test_patch_orthogonal_fields_all_apply():
  out = replay_events(
    [
      submit_event(),
      _patch(weight=2),
      _patch(max_concurrent=4),
      _patch(pool="gpu"),
      _patch(tags=["x", "y"]),
    ]
  )
  assert out is not None
  state, _ = out
  assert state.weight == 2
  assert state.max_concurrent == 4
  assert state.pool == "gpu"
  assert state.tags == ["x", "y"]


def test_patch_single_call_multiple_fields():
  out = replay_events([submit_event(), _patch(weight=2, paused=True)])
  assert out is not None
  assert out[0].weight == 2
  assert out[0].paused is True


def test_patch_unknown_field_raises():
  with pytest.raises(ReplayError, match="unknown field"):
    replay_events([submit_event(), _patch(bogus=1)])


def test_patch_without_mutations_raises():
  with pytest.raises(ReplayError, match="no field mutations"):
    replay_events([submit_event(), {"type": "patch", "attempt_id": "A"}])


# ── dispatch ─────────────────────────────────────────────────────


def test_dispatch_events_appended_to_log():
  out = replay_events(
    [
      submit_event(),
      dispatch_event("t1", "t1__0000001"),
      dispatch_event("t2", "t2__0000002"),
    ]
  )
  assert out is not None
  _, log = out
  assert [(e.task_name, e.trial_name) for e in log] == [
    ("t1", "t1__0000001"),
    ("t2", "t2__0000002"),
  ]
  assert all(e.host == "ml10" for e in log)


def test_dispatch_preserves_order_across_patches():
  out = replay_events(
    [
      submit_event(),
      dispatch_event("t1", "n1"),
      _patch(weight=3),
      dispatch_event("t2", "n2"),
    ]
  )
  assert out is not None
  _, log = out
  assert [e.trial_name for e in log] == ["n1", "n2"]


def test_dispatch_missing_required_field_raises():
  ev = dispatch_event("t1", "n1")
  del ev["host"]
  with pytest.raises(ReplayError, match="dispatch invalid"):
    replay_events([submit_event(), ev])


# ── pause_on_error event ─────────────────────────────────────────


def test_pause_on_error_event_sets_paused_true():
  out = replay_events(
    [
      submit_event(),
      {"type": "pause_on_error", "attempt_id": "A", "task_name": "t1"},
    ]
  )
  assert out is not None
  assert out[0].paused is True


def test_pause_on_error_after_unpause_still_pauses():
  out = replay_events(
    [
      submit_event(),
      {"type": "pause_on_error", "attempt_id": "A"},
      _patch(paused=False),
      {"type": "pause_on_error", "attempt_id": "A"},
    ]
  )
  assert out is not None
  assert out[0].paused is True


# ── structural errors ────────────────────────────────────────────


def test_empty_event_list_raises():
  with pytest.raises(ReplayError, match="empty"):
    replay_events([])


def test_missing_submit_first_raises():
  with pytest.raises(ReplayError, match="first event"):
    replay_events([_patch(paused=True)])


def test_unknown_type_raises():
  with pytest.raises(ReplayError, match="unknown type"):
    replay_events([submit_event(), {"type": "wat", "attempt_id": "A"}])


def test_missing_type_raises():
  with pytest.raises(ReplayError, match="missing 'type'"):
    replay_events([submit_event(), {"attempt_id": "A"}])


def test_attempt_id_mismatch_raises():
  with pytest.raises(ReplayError, match="mismatch"):
    replay_events([submit_event(), _patch(attempt_id="B", paused=True)])


def test_second_submit_raises():
  with pytest.raises(ReplayError, match="duplicate"):
    replay_events([submit_event(), submit_event()])


# ── cancel ───────────────────────────────────────────────────────


def test_cancel_short_circuits_to_none():
  assert (
    replay_events([submit_event(), {"type": "cancel", "attempt_id": "A"}])
    is None
  )


# ── reclaim / retry retraction ───────────────────────────────────


def _retract(kind: str, task: str, trial: str | None) -> dict:
  ev: dict[str, Any] = {
    "type": kind,
    "attempt_id": "A",
    "task_name": task,
  }
  if trial is not None:
    ev["trial_name"] = trial
  return ev


def test_reclaim_erases_matching_dispatch_entry():
  out = replay_events(
    [
      submit_event(),
      dispatch_event("t1", "n1"),
      dispatch_event("t2", "n2"),
      _retract("reclaim", "t1", "n1"),
    ]
  )
  assert out is not None
  _, log = out
  assert [e.trial_name for e in log] == ["n2"]


def test_reclaim_then_redispatch_preserves_new_entry():
  out = replay_events(
    [
      submit_event(),
      dispatch_event("t1", "n1"),
      _retract("reclaim", "t1", "n1"),
      dispatch_event("t1", "n3"),
    ]
  )
  assert out is not None
  _, log = out
  assert [e.trial_name for e in log] == ["n3"]


def test_retry_erases_the_named_trial_not_the_last_one():
  # An infra requeue leaves TWO dispatches for one task. A retry
  # aimed at the failed FIRST trial must erase that one, not the
  # later (successful) dispatch.
  out = replay_events(
    [
      submit_event(),
      dispatch_event("t1", "n1"),
      dispatch_event("t1", "n2"),
      _retract("retry", "t1", "n1"),
    ]
  )
  assert out is not None
  _, log = out
  assert [e.trial_name for e in log] == ["n2"]


def test_retract_of_an_already_erased_trial_is_noop():
  out = replay_events(
    [
      submit_event(),
      dispatch_event("t1", "n1"),
      _retract("reclaim", "t1", "n1"),
      _retract("reclaim", "t1", "n1"),
    ]
  )
  assert out is not None
  _, log = out
  assert log == []


def test_reclaim_of_never_dispatched_task_is_noop():
  out = replay_events(
    [
      submit_event(),
      dispatch_event("t1", "n1"),
      _retract("reclaim", "t2", "nX"),
    ]
  )
  assert out is not None
  _, log = out
  assert [e.trial_name for e in log] == ["n1"]


@pytest.mark.parametrize("kind", ["reclaim", "retry"])
def test_retract_missing_task_name_raises(kind: str):
  with pytest.raises(ReplayError, match="missing 'task_name'"):
    replay_events(
      [
        submit_event(),
        dispatch_event("t1", "n1"),
        {"type": kind, "attempt_id": "A", "trial_name": "n1"},
      ]
    )


@pytest.mark.parametrize("kind", ["reclaim", "retry"])
def test_retract_missing_trial_name_raises(kind: str):
  # No legacy last-by-task fallback in this repo: erasing "the
  # last dispatch for the task" deletes the wrong trial once a
  # requeue has appended a second dispatch.
  with pytest.raises(ReplayError, match="missing 'trial_name'"):
    replay_events(
      [
        submit_event(),
        dispatch_event("t1", "n1"),
        {"type": kind, "attempt_id": "A", "task_name": "t1"},
      ]
    )


# ── notify_fired ─────────────────────────────────────────────────


def _fired(threshold: object) -> dict:
  return {
    "type": "notify_fired",
    "attempt_id": "A",
    "threshold": threshold,
  }


def test_notify_fired_replays_into_notified_thresholds():
  out = replay_events([submit_event(), _fired(0.5), _fired(1.0)])
  assert out is not None
  assert out[0].notified_thresholds == [0.5, 1.0]


def test_notify_fired_dedup_on_replay():
  out = replay_events([submit_event(), _fired(0.5), _fired(0.5)])
  assert out is not None
  assert out[0].notified_thresholds == [0.5]


def test_notify_fired_bad_threshold_is_dropped_silently():
  out = replay_events([submit_event(), _fired("half")])
  assert out is not None
  assert out[0].notified_thresholds == []


# ── archive / unarchive ──────────────────────────────────────────


def test_archive_stamps_marks():
  out = replay_events(
    [
      submit_event(),
      {
        "type": "archive",
        "attempt_id": "A",
        "kind": "auto",
        "at": "2026-09-28T12:00:00+00:00",
      },
    ]
  )
  assert out is not None
  state, _ = out
  assert state.archived_at is not None
  assert state.archive_kind == "auto"


def test_unarchive_clears_marks():
  out = replay_events(
    [
      submit_event(),
      {
        "type": "archive",
        "attempt_id": "A",
        "kind": "manual",
        "at": "2026-09-28T12:00:00+00:00",
      },
      {"type": "unarchive", "attempt_id": "A"},
    ]
  )
  assert out is not None
  state, _ = out
  assert state.archived_at is None
  assert state.archive_kind == ""


def test_archive_bad_kind_raises():
  with pytest.raises(ReplayError, match="archive kind"):
    replay_events(
      [
        submit_event(),
        {
          "type": "archive",
          "attempt_id": "A",
          "kind": "weird",
          "at": "2026-09-28T12:00:00+00:00",
        },
      ]
    )


def test_archive_bad_timestamp_raises():
  with pytest.raises(ReplayError, match="not ISO-8601"):
    replay_events(
      [
        submit_event(),
        {
          "type": "archive",
          "attempt_id": "A",
          "kind": "manual",
          "at": "yesterday",
        },
      ]
    )
