"""Worker-side runtime for dispatcher trials. Stdlib only, so any
research repo's container image can vendor or install it without
dependency friction.

Usage inside the main container:

    from dispatcher_sdk import Result, run

    def work(trial):
      ...  # trial.task, trial.payload, trial.home
      return Result(values={"reward": 1.0}, data={...})

    if __name__ == "__main__":
      run(work)

`run` reads the trial spec, executes `work`, writes the outcome
envelope atomically, and exits with the contract code (0 ok / 1
error / 75 infra). Raise `InfraFailure` when the machine — not
the work — failed (backend swapped, host draining): the
dispatcher requeues instead of scoring.

Sibling containers a worker starts MUST carry the label in
`trial.set_label` (`docker run --label "$DISPATCHER_SET_LABEL"`),
or cleanup cannot see them and they leak."""

from __future__ import annotations

from dispatcher_sdk.worker import (
  EX_INFRA,
  InfraFailure,
  Result,
  TrialContext,
  load_trial,
  run,
  write_outcome,
)

__all__ = [
  "EX_INFRA",
  "InfraFailure",
  "Result",
  "TrialContext",
  "load_trial",
  "run",
  "write_outcome",
]
