"""Measure what packing buys on a real tree of instance homes.

The number worth knowing is the I/O term of a retroactive readout pass:
it reads every finished instance's files, and packing changes only how.
Container startup and the operator's readout function are the same work
either way, so this leaves them out rather than diluting the ratio with
them.

    uv run python examples/packbench.py \\
      --source <a dir of instance homes> \\
      --work <SHARED-FS>/packbench \\
      --on ml5

`--source` is read-only; point it at a real job's home root (or any
directory whose children are instance homes). `--work` must be on the
shared filesystem, because the archives have to be visible from the
node doing the reading. `--on` should be a host that has NOT touched
the source tree — the SERVER-side cache is warm either way, which
favours NFS, so the comparison stays conservative.

Measured on a v7 job's 418 instances (19,814 files, 1.6GB) with the
archives split eight ways, read from a node that had not seen it:

    NFS       182.80s     108 files/s    8.8 MB/s
    packed      5.12s   3,867 files/s  315.5 MB/s
              (3.69s of that is extraction)

    35.7x on the I/O term — per batch of 64, 26.11s against 0.73s

8.8 MB/s is a twelfth of the 1 GbE wire, which is the whole point: the
cost is per-file latency, not bandwidth. The packed side beats the wire
because it pulls 186MB of compressed archive and expands it locally.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PACK_MODULE = HERE.parent / "src" / "dispatcher" / "core" / "pack.py"

# The reading half runs on another node, where this repo does not
# exist. `core/pack.py` is stdlib-only on purpose, so shipping it and
# this file is the whole dependency.
READER = """
import shutil, sys, tempfile, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "mod"))
from dispatcher.core.pack import (
  pack_path,
  packed_hosts,
  packed_instances,
  stage_instances,
)

BATCH = 64


def read_all(root):
  files = total = 0
  for path in root.rglob("*"):
    if not path.is_file():
      continue
    try:
      total += len(path.read_bytes())
      files += 1
    except OSError:
      pass
  return files, total


def batches(items, n):
  return [items[i : i + n] for i in range(0, len(items), n)]


source, work = Path(sys.argv[1]), Path(sys.argv[2])
# Taken from the archives so both sides read the SAME set. An instance
# the archives lack would make the NFS side do strictly more work and
# look slower for a reason that is not about packing.
instances = sorted(
  i
  for host in packed_hosts(work)
  for i in packed_instances(pack_path(work, host))
)
groups = batches(instances, BATCH)
print(f"{len(instances)} instances, {len(groups)} batch(es) of {BATCH}")

t0 = time.perf_counter()
nf = nb = 0
for group in groups:
  for name in group:
    f, b = read_all(source / name)
    nf += f
    nb += b
nfs = time.perf_counter() - t0

t0 = time.perf_counter()
pf = pb = 0
stage = 0.0
for group in groups:
  dest = Path(tempfile.mkdtemp(prefix="packbench-"))
  try:
    s0 = time.perf_counter()
    missing = stage_instances(work, group, dest)
    stage += time.perf_counter() - s0
    if missing:
      print(f"  !! {len(missing)} of {len(group)} did not stage")
    f, b = read_all(dest)
    pf += f
    pb += b
  finally:
    shutil.rmtree(dest, ignore_errors=True)
packed = time.perf_counter() - t0

row = "  {:9s}{:7d} files{:9.1f} MB{:9.2f}s{:9.0f} files/s{:8.1f} MB/s"
print(row.format("NFS", nf, nb / 1e6, nfs, nf / nfs, nb / 1e6 / nfs))
print(
  row.format(
    "packed", pf, pb / 1e6, packed, pf / packed, pb / 1e6 / packed
  )
)
print(f"  {'':9s}of which extraction {stage:.2f}s")
if pf != nf:
  print(f"  !! file counts differ ({nf} vs {pf}) — not comparable")
print(f"\\n  {nfs / packed:.1f}x on the I/O term")
print(
  f"  per batch of {BATCH}: NFS {nfs / len(groups):.2f}s"
  f" -> packed {packed / len(groups):.2f}s"
)
"""


def run(argv: list[str], **kw) -> subprocess.CompletedProcess[str]:
  return subprocess.run(
    argv, text=True, capture_output=True, check=False, **kw
  )


def build_archives(
  source: Path, work: Path, hosts: list[str], processors: int
) -> None:
  """Split the source's instance homes across `hosts` worth of
  archives, which is the layout the packer produces — one per host that
  ran some of the job."""
  homes = sorted(p for p in source.iterdir() if p.is_dir())
  if not homes:
    sys.exit(f"no instance homes under {source}")
  packs = work / ".packs"
  packs.mkdir(parents=True, exist_ok=True)
  print(f"{len(homes)} instance homes -> {len(hosts)} archive(s)")
  for i, host in enumerate(hosts):
    chunk = [str(p) for p in homes[i :: len(hosts)]]
    if not chunk:
      continue
    archive = packs / f"{host}.sqfs"
    archive.unlink(missing_ok=True)
    done = run(
      [
        "mksquashfs",
        *chunk,
        str(archive),
        "-comp",
        "zstd",
        "-Xcompression-level",
        "6",
        "-no-xattrs",
        "-quiet",
        "-no-progress",
        "-keep-as-directory",
        "-noappend",
        "-processors",
        str(processors),
      ]
    )
    if done.returncode != 0:
      sys.exit(f"mksquashfs failed for {host}: {done.stderr.strip()}")
    size = archive.stat().st_size
    print(f"  {host:6s} {len(chunk):4d} homes  {size / 1e6:7.1f} MB")


def main(argv: list[str] | None = None) -> int:
  ap = argparse.ArgumentParser(description=__doc__)
  ap.add_argument(
    "--source",
    required=True,
    type=Path,
    help="read-only dir whose children are instance homes",
  )
  ap.add_argument(
    "--work",
    required=True,
    type=Path,
    help="scratch dir on the SHARED filesystem for the archives",
  )
  ap.add_argument(
    "--on",
    required=True,
    help="host to read from; one that has not touched --source",
  )
  ap.add_argument(
    "--hosts",
    type=int,
    default=8,
    help="how many archives to split into (the packer makes one per "
    "host that ran part of the job)",
  )
  ap.add_argument("--processors", type=int, default=8)
  ap.add_argument(
    "--keep",
    action="store_true",
    help="leave the archives behind for another run",
  )
  args = ap.parse_args(argv)

  source = args.source.expanduser().resolve()
  work = args.work.expanduser().resolve()
  if not source.is_dir():
    sys.exit(f"no such source dir: {source}")

  build_archives(
    source,
    work,
    [f"h{i}" for i in range(args.hosts)],
    args.processors,
  )

  # Ship the reader and the one module it needs. `core/pack.py` being
  # stdlib-only is what makes this a copy rather than an install.
  mod = work / "mod" / "dispatcher" / "core"
  mod.mkdir(parents=True, exist_ok=True)
  for pkg in (work / "mod" / "dispatcher", mod):
    (pkg / "__init__.py").write_text("", encoding="utf-8")
  shutil.copy(PACK_MODULE, mod / "pack.py")
  reader = work / "packbench_reader.py"
  reader.write_text(READER, encoding="utf-8")

  print(f"\nreading on {args.on}")
  done = run(
    [
      "ssh",
      "-o",
      "BatchMode=yes",
      args.on,
      f"python3 {reader} {source} {work}",
    ]
  )
  print(done.stdout.rstrip())
  if done.returncode != 0:
    print(done.stderr.strip()[-600:], file=sys.stderr)
  if not args.keep:
    shutil.rmtree(work, ignore_errors=True)
    print(f"\nremoved {work}")
  else:
    print(f"\nkept {work}")
  return done.returncode


if __name__ == "__main__":
  sys.exit(main())
