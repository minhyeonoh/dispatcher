"""Container-label contract.

Every instance is identified on docker hosts by labels, never by
container name (docker/compose rewrite names — lowercasing bit the
old router; labels pass through verbatim).

- MANAGED on the main container only: marks it as the one whose
  exit ends the instance.
- INSTANCE on the main container only: carries the instance id.
- SET on EVERY container belonging to the instance (main included).
  Cleanup expands through this label, so sibling containers a
  worker starts must carry it or they leak.
- JOB on the main container: reverse lookup for operators.
"""

from __future__ import annotations

MANAGED = "dispatcher.managed"
INSTANCE = "dispatcher.instance"
SET = "dispatcher.set"
JOB = "dispatcher.job"

MANAGED_VALUE = "1"


def main_labels(job_id: str, instance_id: str) -> dict[str, str]:
  """Labels the dispatcher stamps on the main container."""
  return {
    MANAGED: MANAGED_VALUE,
    INSTANCE: instance_id,
    SET: instance_id,
    JOB: job_id,
  }


def set_label(instance_id: str) -> str:
  """`k=v` form a worker passes to `docker run --label` for
  sibling containers."""
  return f"{SET}={instance_id}"
