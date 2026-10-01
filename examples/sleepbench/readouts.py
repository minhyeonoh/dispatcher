"""sleepbench's readouts: what its columns mean.

Frozen into the same source tar as `worker.py`, so the code that
scored a run is recorded next to the run.

These run in the instance's **own worker process**, right after
`work` returns — `dispatcher_sdk.run` calls them on the envelope it
is about to write. The same functions are what the retroactive
`dispatcher readout` command imports in a container, so they cannot
behave differently depending on which path ran them.

A readout never writes anything; the dispatcher owns the value
files. Everything it needs was put in `outcome["data"]` by the
worker, which is why `data` is worth being generous with.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from dispatcher_sdk.readout import ReadoutInstance


def reward(instance: ReadoutInstance) -> float | None:
  """The metric. `None` for a failed instance — a value, not a
  retry: averaging a crash as 0.0 would quietly drag the arm's mean
  down and read as a worse method rather than a broken run."""
  if instance.state != "done_ok":
    return None
  return float(instance.data["reward"])


def solved(instance: ReadoutInstance) -> bool:
  """A pass/fail column. Booleans aggregate to a rate, so this
  shows up as a percentage without anyone writing an aggregator."""
  return instance.state == "done_ok"


def wall_seconds(instance: ReadoutInstance) -> float | None:
  """How long the work claimed to take. Reads `data`, not the
  dispatcher's own timing, so it is the WORKER's account of itself
  — useful precisely where the two disagree."""
  if instance.state != "done_ok":
    return None
  return float(instance.data["duration_s"])


def kind(instance: ReadoutInstance) -> str:
  """The task's intended outcome class, as the submitter set it.

  A non-numeric column on purpose: it gets a count but no mean,
  because there is no honest average of `ok` and `error`. Reading
  it from `payload` rather than `data` also makes it available for
  instances that failed before returning anything."""
  return str((instance.payload or {}).get("kind", "?"))
