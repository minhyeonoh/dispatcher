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

import shlex
import shutil
import subprocess
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

_ROOT_PREFIX = "squashfs-root/"

MKSQUASHFS_OPTS = (
  "-comp",
  "zstd",
  "-Xcompression-level",
  "6",
  "-no-xattrs",
  "-quiet",
  "-no-progress",
  "-keep-as-directory",
)
"""The same flags whether the archive is being created or extended —
appending is `mksquashfs`'s DEFAULT (there is no `-append` flag, only
`-noappend` to turn it off), and passing the archive's own compressor
again is accepted.

`-keep-as-directory` is the one that must not be dropped. Without it
a SINGLE source directory contributes its CONTENTS to the archive
root rather than itself, so a batch of one instance would land
`agent/`, `outcome.json` and friends at the top instead of under
`<instance_id>/`. Batches of one are the normal case when completions
trickle, so the wrong layout would be the common one.

`-no-progress` as well as `-quiet`: the two are separate flags and
`-quiet` alone still draws the bar, which lands in the operator's
report when the command runs over ssh."""


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


def job_mount_dir(job_id: str, *, base: Path | None = None) -> Path:
  """Everything this node has mounted for one job.

  Its children are named by host, which is what lets `umount` find
  them without asking a server — unmounting has to work when the
  dispatcher is down."""
  return (base or DEFAULT_MOUNT_BASE) / job_id


def mount_dir(job_id: str, host: str, *, base: Path | None = None) -> Path:
  """Where this node mounts one archive.

  Keyed by both because a node may hold several jobs open at once and
  the host is half the archive's identity."""
  return job_mount_dir(job_id, base=base) / host


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


# ── what an archive already holds ─────────────────────────────────


def list_top_level_cmd(archive: Path) -> list[str]:
  """List an archive's top-level names and nothing deeper.

  `-max-depth 1` because the top level IS the instance list, and the
  depth bound is what keeps this cheap on a big archive."""
  return ["unsquashfs", "-l", "-max-depth", "1", str(archive)]


def parse_top_level(stdout: str) -> set[str]:
  """Instance ids from `list_top_level_cmd` output.

  Lines are `squashfs-root/<name>`, plus a bare `squashfs-root` for
  the root itself. Anything with a further slash is ignored so a
  future change to the depth bound cannot silently start reporting
  files as instances."""
  out: set[str] = set()
  for line in stdout.splitlines():
    line = line.strip()
    if not line.startswith(_ROOT_PREFIX):
      continue
    name = line[len(_ROOT_PREFIX) :]
    if name and "/" not in name:
      out.add(name)
  return out


def packed_instances(archive: Path) -> set[str]:
  """Which instance homes this archive already holds.

  Asked of the archive instead of a record kept beside it. Measured
  0.02s on a 415MB / 41,892-file archive and 0.00s with the depth
  bound, over NFS — `unsquashfs` reads only the metadata tables — so
  a manifest would buy nothing and would introduce the one failure a
  manifest always introduces: disagreeing with the thing it
  describes.

  A missing or unreadable archive is an empty set, which makes the
  caller treat every instance as unpacked. That is the safe
  direction: it re-packs work rather than skipping it."""
  if not archive.is_file():
    return set()
  try:
    done = subprocess.run(
      list_top_level_cmd(archive),
      capture_output=True,
      text=True,
      timeout=120,
      check=False,
    )
  except (OSError, subprocess.SubprocessError):
    return set()
  if done.returncode != 0:
    return set()
  return parse_top_level(done.stdout)


# ── building the archive ──────────────────────────────────────────


def pack_shell_cmd(
  home_root: Path,
  host: str,
  instance_ids: list[str],
  *,
  processors: int = 2,
) -> str:
  """The shell line that appends these instance homes to `<host>`'s
  archive, to be run ON that host.

  On that host because the instances just finished there, so the
  files are still in its page cache and reading them back is close to
  free; the dispatcher would have to pull them over NFS instead.

  `flock` is not for the dispatcher's own appends — it keeps one
  appender per host already. It covers what outlives a single
  process: a restart with an append in flight, and a second
  dispatcher pointed at the same job."""
  archive = pack_path(home_root, host)
  sources = [str(instance_home_for(home_root, i)) for i in instance_ids]
  inner = [
    "mksquashfs",
    *sources,
    str(archive),
    *MKSQUASHFS_OPTS,
    "-processors",
    str(processors),
  ]
  return (
    f"mkdir -p {shlex.quote(str(pack_dir(home_root)))} && "
    f"flock {shlex.quote(str(lock_path(home_root, host)))} "
    f"{shlex.join(inner)}"
  )


# ── mounting ──────────────────────────────────────────────────────


def squashfuse_bin() -> str | None:
  """The mounter, preferring the low-level build.

  `squashfuse_ll` uses FUSE's low-level API and is the faster of the
  two; both ship in the same package. Neither is installed on these
  hosts by default, so callers have to be able to say so — hence a
  None rather than an exception."""
  for name in ("squashfuse_ll", "squashfuse"):
    found = shutil.which(name)
    if found:
      return found
  return None


def mount_cmd(archive: Path, mountpoint: Path, binary: str) -> list[str]:
  return [binary, str(archive), str(mountpoint)]


def umount_cmd(mountpoint: Path) -> list[str]:
  """`fusermount3 -u`, not `umount`: the mount belongs to this user
  and unmounting it must not need root."""
  return ["fusermount3", "-u", str(mountpoint)]
