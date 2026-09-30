"""Container-label contract.

Every trial is identified on docker hosts by labels, never by
container name (docker/compose rewrite names — lowercasing bit the
old router; labels pass through verbatim).

- MANAGED on the main container only: marks it as the one whose
  exit ends the trial.
- TRIAL on the main container only: carries the trial name.
- SET on EVERY container belonging to the trial (main included).
  Cleanup expands through this label, so sibling containers a
  worker starts must carry it or they leak.
- ATTEMPT on the main container: reverse lookup for operators.
"""

from __future__ import annotations

MANAGED = "dispatcher.managed"
TRIAL = "dispatcher.trial"
SET = "dispatcher.set"
ATTEMPT = "dispatcher.attempt"

MANAGED_VALUE = "1"


def main_labels(attempt_id: str, trial_id: str) -> dict[str, str]:
  """Labels the dispatcher stamps on the main container."""
  return {
    MANAGED: MANAGED_VALUE,
    TRIAL: trial_id,
    SET: trial_id,
    ATTEMPT: attempt_id,
  }


def set_label(trial_id: str) -> str:
  """`k=v` form a worker passes to `docker run --label` for
  sibling containers."""
  return f"{SET}={trial_id}"
