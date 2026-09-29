"""Orphan-GC safety layers: preserve sets, age floor, two-tick
confirmation, census-failure handling."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from dispatcher.core import labels
from dispatcher.services.orphan_gc import OrphanGC
from tests.test_runtime import _seed_unknown, mk_attempt, mk_sched

if TYPE_CHECKING:
  from pathlib import Path


# ── orphan GC ────────────────────────────────────────────────────


def _census_row(set_name: str, created: str) -> dict:
  return {
    "Id": "deadbeef",
    "Config": {"Labels": {labels.SET: set_name}},
    "Created": created,
  }


OLD = "2026-09-28T00:00:00.000000000Z"


def test_gc_preserves_running_trial_with_uppercase_name(
  tmp_path: Path, monkeypatch
):
  # Labels carry the trial name verbatim (unlike compose project
  # names, which docker lowercases — that mismatch once rm -f'd
  # live trials mid-request).
  task = "trial_T20190907_004351_281384"
  sched = mk_sched(1)
  sched.submit(mk_attempt(tmp_path, [task]))
  action = sched.dispatch_one()
  assert action is not None
  assert action.trial_name != action.trial_name.lower()

  async def fake_census(host, *, self_host, label_filter=None):
    return [_census_row(action.trial_name, OLD)]

  removed: list[str] = []

  async def fake_remove(host, names, *, self_host):
    removed.extend(names)
    return len(names)

  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.census_host", fake_census
  )
  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.remove_trial_sets", fake_remove
  )
  gc = OrphanGC(sched, self_host="ml10")
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  assert removed == []


def test_gc_preserves_unknown_trials(tmp_path: Path, monkeypatch):
  # Right after restart every adopted trial sits in unknown; GC
  # must not beat the resolver to a container that may be alive.
  sched = mk_sched(1)
  action = _seed_unknown(sched, tmp_path)

  async def fake_census(host, *, self_host, label_filter=None):
    return [_census_row(action.trial_name, OLD)]

  removed: list[str] = []

  async def fake_remove(host, names, *, self_host):
    removed.extend(names)
    return len(names)

  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.census_host", fake_census
  )
  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.remove_trial_sets", fake_remove
  )
  gc = OrphanGC(sched, self_host="ml10")
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  assert removed == []


def test_gc_removes_orphan_only_on_second_tick(
  tmp_path: Path, monkeypatch
):
  sched = mk_sched(1)  # no attempts — everything is orphan

  async def fake_census(host, *, self_host, label_filter=None):
    return [_census_row("stray__0000001", OLD)]

  removed: list[str] = []

  async def fake_remove(host, names, *, self_host):
    removed.extend(names)
    return len(names)

  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.census_host", fake_census
  )
  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.remove_trial_sets", fake_remove
  )
  gc = OrphanGC(sched, self_host="ml10")
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  assert removed == []  # first sighting: suspect only
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  assert removed == ["stray__0000001"]


def test_gc_age_floor_protects_fresh_containers(
  tmp_path: Path, monkeypatch
):
  from datetime import UTC, datetime

  sched = mk_sched(1)
  now_str = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

  async def fake_census(host, *, self_host, label_filter=None):
    return [_census_row("young__0000001", now_str)]

  removed: list[str] = []

  async def fake_remove(host, names, *, self_host):
    removed.extend(names)
    return len(names)

  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.census_host", fake_census
  )
  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.remove_trial_sets", fake_remove
  )
  gc = OrphanGC(sched, self_host="ml10")
  for _ in range(3):
    asyncio.run(gc.sweep_once(min_container_age_s=3600))
  assert removed == []


def test_gc_failed_census_does_not_advance_suspects(
  tmp_path: Path, monkeypatch
):
  sched = mk_sched(1)
  calls = [0]

  async def flaky_census(host, *, self_host, label_filter=None):
    calls[0] += 1
    if calls[0] == 2:
      raise RuntimeError("network blip")
    return [_census_row("stray__0000001", OLD)]

  removed: list[str] = []

  async def fake_remove(host, names, *, self_host):
    removed.extend(names)
    return len(names)

  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.census_host", flaky_census
  )
  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.remove_trial_sets", fake_remove
  )
  gc = OrphanGC(sched, self_host="ml10")
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  # Tick 2 fails — suspect state must survive untouched, but the
  # blip itself must not count as the confirming observation.
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  assert removed == []
  # Tick 3 re-observes → now confirmed.
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  assert removed == ["stray__0000001"]


def test_gc_unparseable_created_at_is_skipped(tmp_path: Path, monkeypatch):
  sched = mk_sched(1)

  async def fake_census(host, *, self_host, label_filter=None):
    return [_census_row("stray__0000001", "not a date")]

  removed: list[str] = []

  async def fake_remove(host, names, *, self_host):
    removed.extend(names)
    return len(names)

  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.census_host", fake_census
  )
  monkeypatch.setattr(
    "dispatcher.services.orphan_gc.remove_trial_sets", fake_remove
  )
  gc = OrphanGC(sched, self_host="ml10")
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  asyncio.run(gc.sweep_once(min_container_age_s=0))
  assert removed == []
