"""Weight-tuner planning rules."""

from __future__ import annotations

from dispatcher.tools.weight_tuner import plan_weights


def _job(
  aid: str,
  *,
  mean: float | None = None,
  ok: int = 0,
  paused: bool = False,
  pending: int = 1,
  running: int = 0,
  weight: int = 1,
) -> dict:
  return {
    "job_id": aid,
    "paused": paused,
    "weight": weight,
    "counts": {"pending": pending, "running": running},
    "metrics": {
      "ok": ok,
      "means": {} if mean is None else {"reward": mean},
    },
  }


def _plan(jobs: list[dict], top_n: int = 2) -> dict[str, int]:
  return plan_weights(
    {"jobs": jobs},
    metric="reward",
    top_n=top_n,
    boosted_weight=50,
  )


def test_top_n_by_metric_boosted_rest_base():
  targets = _plan(
    [
      _job("A", mean=0.9),
      _job("B", mean=0.5),
      _job("C", mean=0.1),
    ]
  )
  assert targets == {"A": 50, "B": 50, "C": 1}


def test_ok_breaks_metric_ties():
  targets = _plan(
    [
      _job("A", mean=0.5, ok=10),
      _job("B", mean=0.5, ok=3),
      _job("C", mean=0.5, ok=7),
    ],
    top_n=1,
  )
  assert targets["A"] == 50
  assert targets["B"] == 1 and targets["C"] == 1


def test_paused_excluded_entirely():
  targets = _plan([_job("A", mean=0.9, paused=True)])
  assert "A" not in targets


def test_drained_excluded_entirely():
  targets = _plan([_job("A", mean=0.9, pending=0, running=0)])
  assert "A" not in targets


def test_unranked_get_base_weight():
  targets = _plan([_job("A"), _job("B", mean=0.2)], top_n=1)
  assert targets == {"B": 50, "A": 1}
