"""Per-host squashfs packs of finished instance homes, and the read
path that prefers one when it is there.

An instance home stops changing when its work ends: the worker writes
`readouts.json` and then the envelope, and nothing writes there again
— the retroactive pass appends to `<home_root>/.readouts/` instead and
takes the home read-only. So the bulk of a job is exactly what a
read-only archive wants, while the part that keeps growing stays an
ordinary directory.

Measured on a real 41,892-file job tree, from a node that had touched
neither path: whole-file reads ran 320 files/s over NFS and 2,857
files/s through a squashfs mount, and random `stat` 64k/s against
167k/s. The win is not compression — an uncompressed archive scored
the same on metadata — it is that the tree's index arrives in a few
sequential reads and then answers from RAM, with no close-to-open
revalidation forcing it to ask again. Compression (9.2x on that tree)
only buys the wire, which matters because the cluster is on 1 GbE.

One archive per (job, host), not per job and not per instance:

- per instance would mean one FUSE process per instance — measured at
  8.1ms and 1.5MB each, so a 1,344-instance job would cost 11s, 1,344
  processes and 2GB of RSS to open;
- per job would have every host appending to one file;
- per host gives exactly one writer per path, the same property that
  lets `.readouts/<name>.jsonl` do without a lock, and the dispatcher
  already records which host ran each instance, so a reader finds the
  right archive without consulting any bookkeeping.

Nothing here deletes. The original tree stays — it is the authority,
it is what writers use, and it costs little to keep (the archive of
that 3.8G tree was 415MB). A pack that is missing, stale, corrupt, or
simply not mounted is therefore a slower read and never a wrong one,
which is what lets the whole thing be a pure optimisation.
"""

from __future__ import annotations

from pathlib import Path

from dispatcher.core.outcome import instance_home_for

PACK_DIRNAME = ".packs"
"""Dot-prefixed and beside `.readouts/` for the same reason: the scan
that finds instances under a home root skips names starting with "."
(`event_log.scan_outcomes_with_mtime`), so a new sidecar directory
needs no change there."""

DEFAULT_MOUNT_BASE = Path.home() / ".cache" / "dispatcher" / "mounts"
"""Per-node and rebuildable from the archives, so a cache dir rather
than anything under `--data-dir`."""


def pack_dir(home_root: Path) -> Path:
  return home_root / PACK_DIRNAME


def pack_path(home_root: Path, host: str) -> Path:
  """The archive holding the instances that finished on `host`."""
  return pack_dir(home_root) / f"{host}.sqfs"


def lock_path(home_root: Path, host: str) -> Path:
  """Serialises appenders for one archive.

  Not for ordinary operation — the dispatcher keeps one appender per
  host already. This covers the two cases that outlive a single
  process: a restart with an append in flight, and a second
  dispatcher pointed at the same job."""
  return pack_dir(home_root) / f"{host}.lock"


def packed_hosts(home_root: Path) -> list[str]:
  """Hosts that have an archive for this job.

  Read off the filenames rather than a manifest: `mount` wants to
  know what exists, and the archives are the thing that exists."""
  try:
    return sorted(p.stem for p in pack_dir(home_root).glob("*.sqfs"))
  except OSError:
    return []


def mount_dir(job_id: str, host: str, *, base: Path | None = None) -> Path:
  """Where this node mounts one archive.

  Keyed by both because a node may hold several jobs open at once and
  the host is half the archive's identity."""
  return (base or DEFAULT_MOUNT_BASE) / job_id / host


def read_home_for(
  home_root: Path,
  instance_id: str,
  *,
  job_id: str,
  host: str = "",
  mount_base: Path | None = None,
) -> tuple[Path, bool]:
  """Where to READ one instance home, and whether that is a pack.

  Writers keep using `instance_home_for`: a pack is read-only by
  construction and the original tree is the authority. Keeping the
  two functions apart is what makes "writes go to NFS, reads prefer
  the pack" a property of the code rather than a convention.

  A mounted archive proves itself — the directory is either there or
  it is not — so this asks the filesystem instead of trusting a
  record. That way a mount that has gone away cannot leave a reader
  pointed at nothing, and an archive nobody mounted degrades to the
  NFS path rather than to an error."""
  if host:
    candidate = mount_dir(job_id, host, base=mount_base) / instance_id
    if candidate.is_dir():
      return candidate, True
  return instance_home_for(home_root, instance_id), False
