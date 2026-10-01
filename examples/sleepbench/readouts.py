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


def columns(job):
  """The job's columns, computed in a resident process whenever a
  value changes.

  This is the arbitrary half: the keys become the table columns and
  nothing about them is declared anywhere. The dispatcher hands over
  one row per finished instance and takes a dict back — it never
  learns what a median is.

  Note what the frame makes easy that a per-instance readout cannot:
  filtering (`df[df.state == …]`), a quantile, arithmetic ACROSS
  readouts, a per-host breakdown. Those were the gaps."""
  df = job.df
  ok = df[df.state == "done_ok"]
  return {
    # A median rather than a mean — one slow instance should not move
    # the column, which is exactly what `mean` would let it do.
    "reward_median": ok.reward.median(),
    "solved": df.solved.mean(),
    # Arithmetic across readouts. `wall_seconds` is the worker's own
    # account of itself; duration_s is the dispatcher's.
    "overhead_s": (df.duration_s - df.wall_seconds).median(),
    "p90_duration": df.duration_s.quantile(0.9),
    # And a non-numeric column, because the operator decides.
    "slowest_host": (
      df.groupby("host").duration_s.mean().idxmax() if len(df) else None
    ),
  }


def column_descriptions():
  """What each column means, shown in the UI's column picker.

  Required alongside `columns` and checked at registration — a key
  with a value and no explanation is the one state a picker cannot
  render, and leaving it until later means never."""
  return {
    "reward_median": (
      "middle reward over successful instances; a median so one slow "
      "or lucky instance cannot move the arm"
    ),
    "solved": "share of instances that finished with a clean envelope",
    "overhead_s": (
      "median of (dispatcher-observed duration − the worker's own "
      "reported wall time): what the harness cost on top of the work"
    ),
    "p90_duration": "90th percentile instance duration, in seconds",
    "slowest_host": "host with the highest mean instance duration",
  }
