"""Orphan-container GC: remove container sets whose instance the
scheduler no longer owns.

Four layers between a live instance and a wrong `rm -f`: the
SET-label filter, the running+unknown preserve set, the age floor
(dispatch race window), and two-tick confirmation. A failed
census advances nothing — a network blip must not count as an
observation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, PositiveFloat

from dispatcher.core import clock, labels
from dispatcher.core.containers import (
  census_host,
  container_labels,
  parse_created,
  remove_instance_sets,
)
from dispatcher.core.loops import fan_out

if TYPE_CHECKING:
  from datetime import datetime

  from dispatcher.core.scheduler import Scheduler

logger = logging.getLogger(__name__)


class OrphanGCSettings(BaseModel):
  enabled: bool = True
  seconds_between_sweeps: PositiveFloat = 30.0
  min_container_age_s: PositiveFloat = 90.0
  """Containers younger than this are never touched — protects
  the dispatch-race window."""


class OrphanGCPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  enabled: bool | None = None
  seconds_between_sweeps: PositiveFloat | None = None
  min_container_age_s: PositiveFloat | None = None


def apply_patch(settings: OrphanGCSettings, patch: OrphanGCPatch) -> None:
  if patch.enabled is not None:
    settings.enabled = patch.enabled
  if patch.seconds_between_sweeps is not None:
    settings.seconds_between_sweeps = patch.seconds_between_sweeps
  if patch.min_container_age_s is not None:
    settings.min_container_age_s = patch.min_container_age_s


class OrphanGC:
  """Holds the per-host suspect memory between sweeps."""

  def __init__(self, scheduler: Scheduler, *, self_host: str) -> None:
    self._sched = scheduler
    self._self_host = self_host
    # host → set-name → first-seen-as-orphan. A set must be
    # orphan on two consecutive sweeps before removal.
    self._suspects: dict[str, dict[str, datetime]] = {}

  async def sweep_once(
    self, *, min_container_age_s: float
  ) -> dict[str, int]:
    now = clock.now()
    preserved: set[str] = set()
    for _aid, tv in self._sched.iter_running():
      preserved.add(tv.instance_id)
    # Right after a restart every previously-running instance sits
    # in unknown until the resolver speaks; GC must not beat it
    # to a live container.
    for _aid, _task, tv in self._sched.iter_unknown():
      preserved.add(tv.instance_id)

    per_host_removed: dict[str, int] = {}

    async def one_host(host: str) -> None:
      removed = await self._sweep_host(
        host,
        now=now,
        preserved=preserved,
        min_container_age_s=min_container_age_s,
      )
      if removed > 0:
        per_host_removed[host] = removed

    # Per host, concurrently: this used to be a sequential walk, so a
    # machine whose docker daemon had wedged held every host behind it
    # for its census deadline — and because the next tick waits for this
    # one, one dark machine could stop collection outright. Each task
    # touches only its own host's keys.
    await fan_out("gc", list(self._sched.all_host_settings()), one_host)
    return per_host_removed

  async def _sweep_host(
    self,
    host: str,
    *,
    now: datetime,
    preserved: set[str],
    min_container_age_s: float,
  ) -> int:
    """One host's census → suspect accrual → removal. Returns how many
    container sets it removed."""
    try:
      containers = await census_host(
        host,
        self_host=self._self_host,
        label_filter=labels.SET,
      )
    except RuntimeError as exc:
      logger.warning("gc: census %s failed: %s", host, exc)
      return 0

    per_set_min_age_s: dict[str, float] = {}
    for c in containers:
      set_name = container_labels(c).get(labels.SET)
      if not set_name:
        continue
      created_at = parse_created(c.get("Created", ""))
      if created_at is None:
        continue  # unparseable — err on the side of caution
      age_s = (now - created_at).total_seconds()
      prev = per_set_min_age_s.get(set_name)
      per_set_min_age_s[set_name] = (
        age_s if prev is None else min(prev, age_s)
      )

    suspects_prev = self._suspects.get(host, {})
    suspects_next: dict[str, datetime] = {}
    to_delete: list[str] = []
    for set_name, min_age_s in per_set_min_age_s.items():
      if set_name in preserved:
        continue
      if min_age_s < min_container_age_s:
        # Hard "not this tick" — suspect state only accrues once the
        # whole set has aged past the floor.
        continue
      if set_name in suspects_prev:
        to_delete.append(set_name)
      else:
        suspects_next[set_name] = now

    self._suspects[host] = suspects_next

    if not to_delete:
      return 0
    return await remove_instance_sets(
      host, to_delete, self_host=self._self_host
    )


async def gc_loop(gc: OrphanGC, settings: OrphanGCSettings) -> None:
  from dispatcher.core.loops import every

  async def tick() -> None:
    removed = await gc.sweep_once(
      min_container_age_s=settings.min_container_age_s,
    )
    if removed:
      # WARNING: the dispatcher touched containers — operators
      # must see this even at quiet log levels.
      logger.warning(
        "orphan_gc: removed %s container(s) — %s",
        sum(removed.values()),
        ", ".join(f"{h}={n}" for h, n in removed.items()),
      )

  await every(
    "orphan_gc",
    lambda: settings.seconds_between_sweeps,
    tick,
    enabled_fn=lambda: settings.enabled,
  )
