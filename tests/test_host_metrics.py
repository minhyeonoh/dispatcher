"""Probe-output parsing."""

from __future__ import annotations

import pytest

from dispatcher.host_metrics import _parse_probe, _parse_size

PROBE_OUT = """\
MemTotal:       263856792 kB
MemAvailable:   200000000 kB
0.52 1.20 1.50 2/3000 12345
64
/dev/sda1 1000000000000 400000000000 600000000000 40% /
c1|3.5%|2.5GiB / 251GiB
c2|0.0%|219.2MiB / 251GiB
"""


def test_parse_probe_positional_sections():
  s = _parse_probe(PROBE_OUT)
  assert s.mem_total_bytes == 263856792 * 1024
  assert s.mem_avail_bytes == 200000000 * 1024
  assert s.loadavg_5m == 1.20
  assert s.nproc == 64
  assert s.disk_root_total_bytes == 1000000000000
  assert s.disk_root_free_bytes == 600000000000
  assert [t.name for t in s.trials] == ["c1", "c2"]
  assert s.trials[0].cpu_percent == 3.5
  assert s.trials[0].rss_bytes == int(2.5 * 1024**3)
  assert s.trials[1].rss_bytes == int(219.2 * 1024**2)


def test_parse_probe_no_containers():
  head = "\n".join(PROBE_OUT.splitlines()[:5]) + "\n"
  s = _parse_probe(head)
  assert s.trials == ()


def test_parse_probe_short_output_raises():
  with pytest.raises(ValueError, match="too short"):
    _parse_probe("MemTotal: 1 kB\n")


def test_parse_probe_skips_bad_stats_line():
  s = _parse_probe(PROBE_OUT + "garbage-line-no-pipes\n")
  assert [t.name for t in s.trials] == ["c1", "c2"]


def test_parse_size_units():
  assert _parse_size("1KiB") == 1024
  assert _parse_size("1.5MB") == 1500000
  assert _parse_size("2GiB") == 2 * 1024**3


def test_parse_size_garbage_raises():
  with pytest.raises(ValueError):
    _parse_size("lots")
