"""Host selection: argmin(running/cap) over active+alive hosts
with a free slot; tie-break by insertion order of the settings
map; None when nobody is eligible."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from dispatcher.core.models import HostSettings


def pick_host(
  hosts: dict[str, HostSettings],
  host_running: dict[str, int],
) -> str | None:
  candidates = [
    h
    for h, s in hosts.items()
    if s.active
    and s.alive
    and s.max_concurrent > 0
    and host_running.get(h, 0) < s.max_concurrent
  ]
  if not candidates:
    return None
  return min(
    candidates,
    key=lambda h: host_running.get(h, 0) / hosts[h].max_concurrent,
  )
