"""Worker-side runtime for dispatcher instances. Stdlib only, so any
research repo's container image can vendor or install it without
dependency friction.

Usage inside the main container:

    from dispatcher_sdk import run

    def work(instance):
      ...  # instance.task, instance.payload, instance.home
      return {"reward": 1.0}  # any JSON → the envelope's `data`

    if __name__ == "__main__":
      run(work)

`run` reads the instance spec, executes `work`, writes the outcome
envelope atomically, and exits with the contract code (0 ok / 1
error / 75 infra). Raise `InfraFailure` when the machine — not
the work — failed (backend swapped, host draining): the
dispatcher requeues instead of scoring.

Sibling containers a worker starts MUST carry the label in
`instance.set_label` (`docker run --label "$DISPATCHER_SET_LABEL"`),
or cleanup cannot see them and they leak."""

from __future__ import annotations

from dispatcher_sdk.worker import (
  EX_INFRA,
  InfraFailure,
  InstanceContext,
  load_instance,
  run,
  write_outcome,
)

__all__ = [
  "EX_INFRA",
  "InfraFailure",
  "InstanceContext",
  "load_instance",
  "run",
  "write_outcome",
]
