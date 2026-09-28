"""Monitor state folding, SSE parsing, metric selection."""

from __future__ import annotations

from dispatcher.monitor_client import (
  MonitorState,
  parse_sse_lines,
  primary_metric,
  render_compact,
  render_detail,
)


def test_parse_sse_lines_frames():
  lines = [
    "event: snapshot",
    'data: {"running_total": 3}',
    "",
    "event: heartbeat",
    'data: {"at": "t0"}',
    "",
  ]
  frames = list(parse_sse_lines(lines))
  assert frames == [
    ("snapshot", {"running_total": 3}),
    ("heartbeat", {"at": "t0"}),
  ]


def test_parse_sse_lines_drops_bad_json_frame():
  lines = ["event: x", "data: {broken", "", "event: y", "data: {}", ""]
  assert list(parse_sse_lines(lines)) == [("y", {})]


def test_state_apply_snapshot_and_updates():
  s = MonitorState()
  s.apply("snapshot", {"running_total": 1})
  assert s.cluster == {"running_total": 1}
  s.apply("cluster_updated", {"running_total": 2})
  assert s.cluster["running_total"] == 2
  s.apply("attempt_updated", {"attempt_id": "A", "label": "x"})
  s.apply("attempt_submitted", {"attempt_id": "B", "label": "y"})
  assert s.attempt_order == ["A", "B"]
  # Update in place keeps order stable.
  s.apply("attempt_updated", {"attempt_id": "A", "label": "x2"})
  assert s.attempt_order == ["A", "B"]
  assert s.attempts["A"]["label"] == "x2"


def test_state_cancelled_badges_last_snapshot():
  s = MonitorState()
  s.apply("attempt_updated", {"attempt_id": "A", "label": "x"})
  s.apply("attempt_cancelled", {"attempt_id": "A"})
  assert s.attempts["A"]["cancelled"] is True


def test_state_heartbeat_recorded():
  s = MonitorState()
  s.apply("heartbeat", {"at": "2026-09-28T00:00:00"})
  assert s.last_heartbeat_at == "2026-09-28T00:00:00"


def test_primary_metric_prefers_reward():
  assert primary_metric({"means": {"steps": 9.0, "reward": 0.5}}) == (
    "reward",
    0.5,
  )


def test_primary_metric_falls_back_alphabetical():
  assert primary_metric({"means": {"z": 1.0, "acc": 0.9}}) == (
    "acc",
    0.9,
  )


def test_primary_metric_none_when_empty():
  assert primary_metric({}) is None
  assert primary_metric({"means": {}}) is None


def test_renderers_do_not_crash_on_empty_and_full_state():
  s = MonitorState()
  render_compact(s)
  s.apply(
    "snapshot",
    {
      "config": {
        "self_host": "ml10",
        "max_concurrent": 4,
        "hosts": {"ml10": {"max_concurrent": 4, "active": True}},
      },
      "running_total": 1,
      "running_per_host": {"ml10": 1},
    },
  )
  s.apply(
    "attempt_updated",
    {
      "attempt_id": "A",
      "alias": "brave-otter",
      "label": "demo",
      "paused": False,
      "weight": 1,
      "max_concurrent": None,
      "counts": {
        "pending": 1,
        "running": 1,
        "done_ok": 1,
        "done_err": 1,
        "ghosted": 0,
        "unknown": 1,
        "total": 5,
      },
      "metrics": {
        "ok": 1,
        "err": 1,
        "means": {"reward": 0.5},
      },
    },
  )
  render_compact(s)
  render_detail(s, "A")
  render_detail(s, "missing")
