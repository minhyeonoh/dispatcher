"""A stand-in experiment: sleep, then report.

Shaped like a real run rather than a smoke test — instances take
minutes, not milliseconds, and the outcome mix includes genuine
errors and a transient infra failure, so the dispatcher's
done_ok / done_err / requeue paths all get exercised.

What the dispatcher supplies (see `dispatcher_sdk.load_instance`):
its identity, the trial home, and the per-task `payload` the
submitter attached. Everything about WHAT to compute lives here,
on the research-repo side.
"""

from __future__ import annotations

import os
import sys
import time
from typing import TYPE_CHECKING

from dispatcher_sdk import InfraFailure, run

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher_sdk import InstanceContext

# Sleeping is the "work". Chunked so the log shows progress and a
# kill lands promptly rather than at the end of one long syscall.
_TICK_S = 10.0


def _sleep_with_progress(total_s: float) -> None:
  start = time.monotonic()
  while True:
    elapsed = time.monotonic() - start
    if elapsed >= total_s:
      return
    print(f"… {elapsed:6.1f}s / {total_s:.1f}s", flush=True)
    time.sleep(min(_TICK_S, total_s - elapsed))


def _flaky_marker(instance: InstanceContext) -> Path:
  """One marker per TASK, in the job's home root (the parent of
  this instance's home) — it has to outlive this instance for the
  retry to see it."""
  marker_dir = instance.home.parent / ".flaky-seen"
  marker_dir.mkdir(parents=True, exist_ok=True)
  return marker_dir / instance.task


def work(instance: InstanceContext) -> dict[str, object]:
  payload = instance.payload or {}
  duration_s = float(payload.get("duration_s", 60.0))
  kind = str(payload.get("kind", "ok"))
  reward = float(payload.get("reward", 0.0))

  print(
    f"instance={instance.instance} task={instance.task} "
    f"host={os.uname().nodename} kind={kind} "
    f"duration={duration_s:.1f}s",
    flush=True,
  )
  _sleep_with_progress(duration_s)

  if kind == "error":
    # The WORK failed: a real experiment's bad config, bad data,
    # agent crash. Scored as done_err, never retried on its own.
    raise ValueError(
      f"task {instance.task} failed to converge (simulated)"
    )

  if kind == "flaky":
    marker = _flaky_marker(instance)
    if not marker.exists():
      # The MACHINE failed, not the work — the dispatcher requeues
      # the task with a fresh instance instead of scoring it. The
      # marker makes the second instance succeed, which is what a
      # transient blip looks like.
      marker.write_text(instance.instance, encoding="utf-8")
      raise InfraFailure(
        f"backend vanished under {instance.instance} (simulated)"
      )
    print(
      f"retry after infra failure (first was {marker.read_text()})",
      flush=True,
    )

  return {
    "task": instance.task,
    "instance": instance.instance,
    "host": os.uname().nodename,
    "kind": kind,
    "duration_s": duration_s,
    # A metric lives in `data` like any other result: the
    # dispatcher does not read it, a later readout does.
    "reward": reward,
    "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
  }


if __name__ == "__main__":
  sys.exit(run(work) or 0)
