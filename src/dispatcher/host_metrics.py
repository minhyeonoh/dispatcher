"""Per-host resource sampling: one ssh round-trip collects
mem/load/disk plus per-trial-container `docker stats` (filtered
by the managed label). Measurement only — cap policy lives in
host_autotune."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import anyio

from dispatcher import labels
from dispatcher.hosts import run_on

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TrialStat:
  """`docker stats` snapshot for one container. CPU% is a spike
  detector, not a steady-state measure — IO-bound trials sitting
  on an LLM call read 0%."""

  name: str
  cpu_percent: float
  rss_bytes: int


@dataclass(frozen=True)
class HostSample:
  host: str
  mem_total_bytes: int
  mem_avail_bytes: int
  nproc: int
  loadavg_5m: float
  disk_root_free_bytes: int
  disk_root_total_bytes: int
  trials: tuple[TrialStat, ...]


# One shell script, one ssh. `docker stats` gets explicit ids —
# without them it lists every container on a shared host.
_STATS_FMT = "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"
_PROBE_SCRIPT = (
  "set -u\n"
  "grep -E '^(MemTotal|MemAvailable):' /proc/meminfo\n"
  "cat /proc/loadavg\n"
  "nproc\n"
  "df -PB1 / | tail -1\n"
  "containers=$(docker ps --filter "
  f"label={labels.MANAGED}={labels.MANAGED_VALUE}"
  " -q 2>/dev/null || true)\n"
  'if [ -n "$containers" ]; then\n'
  f"  docker stats --no-stream --format '{_STATS_FMT}'"
  " $containers 2>/dev/null\n"
  "fi\n"
)

_UNITS = {
  "B": 1,
  "KB": 1000,
  "MB": 1000**2,
  "GB": 1000**3,
  "TB": 1000**4,
  "KiB": 1024,
  "MiB": 1024**2,
  "GiB": 1024**3,
  "TiB": 1024**4,
}

_SIZE_RE = re.compile(r"^([\d.]+)([KMGT]?i?B)$")


def _parse_size(s: str) -> int:
  m = _SIZE_RE.match(s.strip())
  if not m:
    raise ValueError(f"unparseable docker size {s!r}")
  return int(float(m.group(1)) * _UNITS[m.group(2)])


def _parse_probe(out: str) -> HostSample:
  """Positional parse: four fixed sections, then zero or more
  docker-stats lines."""
  lines = out.splitlines()
  if len(lines) < 5:
    raise ValueError(
      f"probe output too short ({len(lines)} lines): {out[:500]!r}"
    )
  mem_total = mem_avail = 0
  for line in lines[:2]:
    key, _, rest = line.partition(":")
    kb = int(rest.strip().split()[0])
    if key == "MemTotal":
      mem_total = kb * 1024
    elif key == "MemAvailable":
      mem_avail = kb * 1024
    else:
      raise ValueError(f"unexpected meminfo key {key!r}")
  loadavg_5m = float(lines[2].split()[1])
  nproc = int(lines[3].strip())
  df_fields = lines[4].split()
  disk_total = int(df_fields[1])
  disk_free = int(df_fields[3])
  trials: list[TrialStat] = []
  for line in lines[5:]:
    line = line.strip()
    if not line:
      continue
    parts = line.split("|")
    if len(parts) != 3:
      logger.warning("skipping unparseable docker stats line: %r", line)
      continue
    name, cpu_s, mem_s = parts
    trials.append(
      TrialStat(
        name=name,
        cpu_percent=float(cpu_s.rstrip("%")),
        rss_bytes=_parse_size(mem_s.split("/")[0]),
      )
    )
  return HostSample(
    host="",  # attached by the caller
    mem_total_bytes=mem_total,
    mem_avail_bytes=mem_avail,
    nproc=nproc,
    loadavg_5m=loadavg_5m,
    disk_root_free_bytes=disk_free,
    disk_root_total_bytes=disk_total,
    trials=tuple(trials),
  )


async def sample_host(
  host: str,
  *,
  self_host: str,
  timeout_sec: float = 20.0,
) -> HostSample:
  r = await run_on(host, self_host, _PROBE_SCRIPT, timeout=timeout_sec)
  if r.returncode != 0:
    raise RuntimeError(
      f"host_metrics probe on {host!r} exited {r.returncode}: "
      f"{r.stderr.strip()[:500]}"
    )
  sample = _parse_probe(r.stdout)
  return HostSample(
    host=host,
    mem_total_bytes=sample.mem_total_bytes,
    mem_avail_bytes=sample.mem_avail_bytes,
    nproc=sample.nproc,
    loadavg_5m=sample.loadavg_5m,
    disk_root_free_bytes=sample.disk_root_free_bytes,
    disk_root_total_bytes=sample.disk_root_total_bytes,
    trials=sample.trials,
  )


async def sample_hosts(
  hosts: list[str],
  self_host: str,
  timeout_sec: float = 20.0,
) -> dict[str, HostSample | Exception]:
  """Concurrent; per-host failure isolated as the value."""
  results: dict[str, HostSample | Exception] = {}

  async def _one(h: str) -> None:
    try:
      results[h] = await sample_host(
        h, self_host=self_host, timeout_sec=timeout_sec
      )
    except Exception as exc:
      results[h] = exc

  async with anyio.create_task_group() as tg:
    for h in hosts:
      tg.start_soon(_one, h)
  return results
