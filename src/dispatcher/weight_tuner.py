"""Periodic weight rebalancer: boost the top-N attempts by a
chosen outcome metric, reset everyone else to weight=1. Paused
and drained attempts are excluded (nothing left to boost).
Weight only — never touches paused / caps.

Run via `dispatcher weight-tuner --server … --metric reward`.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from typing import Any

from dispatcher_sdk import client as sdk_client

logger = logging.getLogger("weight_tuner")


def plan_weights(
  monitor: dict[str, Any],
  *,
  metric: str,
  top_n: int,
  boosted_weight: int,
  base_weight: int = 1,
) -> dict[str, int]:
  """Target weights for every non-paused, non-drained attempt.
  Rank by (metric mean desc, ok desc); attempts with no value for
  the metric can't be ranked and stay at base."""
  ranked: list[tuple[float, int, str]] = []
  unranked: list[str] = []
  for a in monitor.get("attempts", []):
    if a.get("paused"):
      continue
    c = a.get("counts") or {}
    if int(c.get("pending", 0)) == 0 and int(c.get("running", 0)) == 0:
      continue  # drained — boosting is churn
    m = a.get("metrics") or {}
    mean = (m.get("means") or {}).get(metric)
    if mean is None:
      unranked.append(a["attempt_id"])
      continue
    ranked.append((float(mean), int(m.get("ok", 0)), a["attempt_id"]))
  ranked.sort(key=lambda r: (-r[0], -r[1]))
  targets: dict[str, int] = {aid: base_weight for _, _, aid in ranked}
  for _, _, aid in ranked[:top_n]:
    targets[aid] = boosted_weight
  for aid in unranked:
    targets[aid] = base_weight
  return targets


def apply_weights(
  server: str,
  targets: dict[str, int],
  current_weights: dict[str, int],
) -> list[tuple[str, int, int]]:
  """PATCH only where the weight differs; per-attempt errors are
  logged and skipped so one stale attempt doesn't stop the pass."""
  applied: list[tuple[str, int, int]] = []
  for aid, target in targets.items():
    old = current_weights.get(aid)
    if old == target:
      continue
    try:
      sdk_client.patch_attempt(server, aid, {"weight": target})
      applied.append((aid, old if old is not None else -1, target))
    except sdk_client.ClientError as exc:
      logger.warning("patch %s weight=%d failed: %s", aid, target, exc)
  return applied


def _current_weights(monitor: dict[str, Any]) -> dict[str, int]:
  return {
    a["attempt_id"]: int(a["weight"]) for a in monitor.get("attempts", [])
  }


def tick(
  server: str, metric: str, top_n: int, boosted_weight: int
) -> None:
  monitor = sdk_client.get_monitor(server)
  targets = plan_weights(
    monitor,
    metric=metric,
    top_n=top_n,
    boosted_weight=boosted_weight,
  )
  applied = apply_weights(server, targets, _current_weights(monitor))
  if not applied:
    logger.info("no changes")
    return
  bumped = [f"{aid}: {old}→{new}" for aid, old, new in applied]
  logger.info("applied %d change(s): %s", len(applied), " · ".join(bumped))


def main(argv: list[str] | None = None) -> int:
  ap = argparse.ArgumentParser(
    description=(
      "boost the top-N attempts by an outcome metric; others "
      "stay at weight=1"
    )
  )
  ap.add_argument("--server", default="http://127.0.0.1:7200")
  ap.add_argument("--metric", default="reward")
  ap.add_argument("--interval", type=float, default=300.0)
  ap.add_argument("--top-n", type=int, default=5)
  ap.add_argument("--boosted-weight", type=int, default=50)
  ap.add_argument("--once", action="store_true")
  args = ap.parse_args(argv)

  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
  )
  try:
    tick(args.server, args.metric, args.top_n, args.boosted_weight)
  except Exception:
    logger.exception("initial tick failed")
  if args.once:
    return 0

  stop = [False]

  def _handle(signum: int, _frame: object) -> None:
    logger.info("signal %d — exiting after current tick", signum)
    stop[0] = True

  for sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(sig, _handle)

  next_at = time.monotonic() + args.interval
  while not stop[0]:
    while not stop[0] and time.monotonic() < next_at:
      time.sleep(min(1.0, next_at - time.monotonic()))
    if stop[0]:
      break
    try:
      tick(args.server, args.metric, args.top_n, args.boosted_weight)
    except Exception:
      logger.exception("tick failed, will retry next interval")
    next_at = time.monotonic() + args.interval
  return 0


if __name__ == "__main__":
  sys.exit(main())
