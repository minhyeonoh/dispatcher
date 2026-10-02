"""`dispatcher path`, `readouts`, `pack`, `mount`, `umount`.

`path` and `readouts` are the read side, and exist so that an
analysis script never spells out a job's layout: scripts written
against them get the faster read once packs exist, without being
edited, and the one place that knows whether a pack is mounted is
`core.pack`.

`pack` builds the archives and `mount`/`umount` attach them here.
Commands, not a background loop: `pack` reaches out over ssh and
competes for the same NFS bandwidth as running trials, and `mount`
changes this node's mount table. Both are an operator's decision to
make and watch, the same reason `dispatcher readout` is a command.

They also separate two things that are easy to conflate, because the
answer is different for each:

- a readout VALUE lives in `<home_root>/.readouts/<name>.jsonl`, one
  file per readout with one line per instance, already consolidated
  by the append that wrote it. Reading a job's rewards is four
  sequential file reads, or one request to a server that holds them
  in memory. Packing would not help it and could not: the retroactive
  pass appends there forever.
- an instance's OUTPUT — `agent/events.jsonl`, `chats.jsonl`, the
  `agent/llm/*.md` dumps — is 34 of a trial's 35 files and is what a
  pack is for.

So: `readouts` for numbers, `path` for files. Reaching for `path` to
find a readout value is the slow way round by a factor of ~15.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

from dispatcher.core.pack import (
  bust_dir_cache,
  instance_from_tree,
  job_mount_dir,
  mount_cmd,
  mount_dir,
  mounted_hosts,
  pack_dir,
  pack_path,
  pack_shell_cmd,
  packed_hosts,
  packed_instances,
  read_home_for,
  squashfuse_bin,
  umount_cmd,
)

_BUCKETS = ("done_ok", "done_err", "running", "ghosted", "unknown")

_PACKABLE = ("done_ok", "done_err")
"""Only instances whose envelope the server has already read.

This is the one place the design can produce a WRONG read rather
than a slow one. A pack never deletes, but `read_home_for` prefers
it, so an archive holding a half-written instance home would serve
that half as though it were the whole. `running` is mid-write by
definition, and `unknown`/`ghosted` mean the dispatcher has no
terminal signal — which is exactly the state a home being written
looks like. Packing any of them trades the design's safety property
for a few seconds."""

_INSTALL_HINT = (
  "squashfuse is not installed. It is a 27KB package and needs no "
  "root:\n"
  "  curl -O http://archive.ubuntu.com/ubuntu/pool/universe/s/"
  "squashfuse/squashfuse_0.5.0-2build1_amd64.deb\n"
  "  dpkg-deb -x squashfuse_0.5.0-2build1_amd64.deb /tmp/sqf\n"
  "  cp /tmp/sqf/usr/bin/squashfuse_ll ~/bin/"
)


def resolve_instance(job: dict[str, Any], ident: str) -> tuple[str, str]:
  """`(instance_id, host)` for a task id OR an instance id.

  The buckets in a `GET /jobs/{id}` body are keyed by TASK id, while
  the instance id — the directory name under the home root — is a
  field inside each view (`task-001` keys a view whose instance is
  `task-001__0000288`). Both are accepted because the task id is what
  an operator actually has: it is what they named, what the sweep
  lists, and what a retry keeps. The instance id is what the
  filesystem uses.

  Every bucket is searched, not just the finished ones: an instance
  parked in `unknown` still has output worth reading, and `running` is
  the case someone looking mid-job is in. A task sits in exactly one
  bucket, so a task-id hit is unambiguous and names the current
  attempt — which is the one whose files are on disk."""
  for bucket in _BUCKETS:
    view = (job.get(bucket) or {}).get(ident)
    if isinstance(view, dict):
      return str(view.get("instance_id") or ident), str(
        view.get("host") or ""
      )
  for bucket in _BUCKETS:
    for view in (job.get(bucket) or {}).values():
      if isinstance(view, dict) and view.get("instance_id") == ident:
        return ident, str(view.get("host") or "")
  # Unknown to the server — still answer with the NFS layout rather
  # than refuse, since the directory may well be there.
  return ident, ""


def shape_values(
  payload: dict[str, Any],
  names: list[str],
  *,
  full: bool = False,
) -> dict[str, dict[str, Any]]:
  """`{readout: {instance_id: value}}` from a `/readouts` body.

  The default drops everything but the value because that is what a
  figure needs and a dict of scalars is what pandas wants. `full`
  keeps the whole record — `ok`, `error`, `at`, and the three hashes
  that answer "which code produced this number", which is the
  question you have when two runs disagree."""
  wanted = set(names)
  out: dict[str, dict[str, Any]] = {}
  for name, rows in (payload.get("values") or {}).items():
    if wanted and name not in wanted:
      continue
    by_instance: dict[str, Any] = {}
    for row in rows:
      if not isinstance(row, dict):
        continue
      instance_id = str(row.get("instance_id") or "")
      if not instance_id:
        continue
      by_instance[instance_id] = row if full else row.get("value")
    out[name] = by_instance
  return out


def _get(server: str, path: str) -> dict[str, Any] | None:
  # Imported here, not at module scope: `path --home-root`, `umount`
  # and the whole offline half must run on a node where nothing is
  # installed, and httpx is the one thing in this file that is not
  # stdlib.
  import httpx

  url = server.rstrip("/") + path
  try:
    resp = httpx.get(url, timeout=60.0)
  except httpx.HTTPError as exc:
    print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
    return None
  if resp.status_code != 200:
    print(
      f"{resp.status_code}: {resp.text.strip()[:400]}", file=sys.stderr
    )
    return None
  body = resp.json()
  return body if isinstance(body, dict) else None


def _offline_instance(home_root: Path, ident: str) -> str | None:
  """Resolve an ident against the tree, with no server.

  Refuses an ambiguous task id rather than guessing which attempt was
  meant — the one case where picking for the operator would quietly
  hand back the wrong trial's files."""
  candidates = instance_from_tree(home_root, ident)
  if len(candidates) == 1:
    return candidates[0]
  if not candidates:
    print(
      f"no instance under {home_root} matching {ident!r}",
      file=sys.stderr,
    )
    return None
  print(
    f"{ident!r} matches {len(candidates)} attempts — name one:",
    file=sys.stderr,
  )
  for name in candidates:
    print(f"  {name}", file=sys.stderr)
  return None


def print_path(
  *,
  server: str,
  job_id: str,
  ident: str,
  home_root: Path | None = None,
  mount_base: Path | None = None,
  out: Any = sys.stdout,
) -> int:
  """Print the read path for one instance home, and nothing else.

  Stdout stays a bare path so `$(dispatcher path …)` works. When a
  pack exists but is not mounted here the note goes to stderr: the
  answer is still correct, it is just slower than it needs to be, and
  that is worth saying exactly once where it cannot corrupt the
  substitution.

  `home_root` skips the server entirely. The two things the server is
  asked for are the home root and which host ran the instance; given
  the first, the second is only a shortcut (`read_home_for` scans the
  mounts instead). That matters on a node that cannot reach the
  dispatcher, which is every node but the launcher today."""
  if home_root is not None:
    instance_id = _offline_instance(home_root, ident)
    if instance_id is None:
      return 1
    return _emit_path(
      home_root,
      instance_id,
      job_id=job_id,
      host="",
      mount_base=mount_base,
      unpacked=unpacked_from_tree(home_root),
      exact=False,
      out=out,
    )
  job = _get(server, f"/api/jobs/{job_id}")
  if job is None:
    return 1
  home_root = Path(str(job.get("home_root") or ""))
  if not home_root.is_absolute():
    print(f"job {job_id!r} has no home_root", file=sys.stderr)
    return 1
  instance_id, host = resolve_instance(job, ident)
  lag = job.get("pack_lag")
  return _emit_path(
    home_root,
    instance_id,
    job_id=job_id,
    host=host,
    mount_base=mount_base,
    unpacked=lag if isinstance(lag, int) else None,
    exact=isinstance(lag, int),
    out=out,
  )


def unpacked_from_tree(home_root: Path) -> int:
  """Instance homes on disk that no archive holds, counted locally.

  The server's `pack_lag` is the better answer — it knows which
  instances are terminal, and an unfinished one is not owed a pack. This
  is the offline stand-in: every directory that looks like an instance
  home, minus everything the archives hold. It can overcount by however
  many instances are still running, which is why the prose says "look
  like"."""
  try:
    on_disk = {
      p.name
      for p in home_root.iterdir()
      if p.is_dir() and not p.name.startswith(".")
    }
  except OSError:
    return 0
  held: set[str] = set()
  for host in packed_hosts(home_root):
    held |= packed_instances(pack_path(home_root, host))
  return len(on_disk - held)


def pack_advice(
  job_id: str,
  *,
  home_root: Path,
  mount_base: Path | None,
  unpacked: int | None,
  exact: bool,
) -> list[str]:
  """Prose for the bottom of a read command's output.

  Two different things are worth saying and they are independent: that
  reads here are going over NFS because nothing is mounted, and that
  some of this job is not in an archive at all. Saying only the first
  would send someone to `mount` for instances no archive contains."""
  lines: list[str] = []
  if packed_hosts(home_root) and not mounted_hosts(
    job_id, base=mount_base
  ):
    lines.append(
      f"This job has archives, but none are mounted here, so reads "
      f"are going over NFS. `dispatcher mount {job_id}` attaches "
      f"them."
    )
  if unpacked:
    what = (
      f"{unpacked} finished instance(s)"
      if exact
      else f"{unpacked} director(y/ies) that look like instance homes"
    )
    lines.append(
      f"{what} of this job are not in any archive, so they will be "
      f"read over NFS however this is mounted. "
      f"`dispatcher pack {job_id}` adds them."
    )
  return lines


def _emit_path(
  home_root: Path,
  instance_id: str,
  *,
  job_id: str,
  host: str,
  mount_base: Path | None,
  unpacked: int | None,
  exact: bool,
  out: Any,
) -> int:
  path, _packed = read_home_for(
    home_root,
    instance_id,
    job_id=job_id,
    host=host,
    mount_base=mount_base,
  )
  print(path, file=out)
  # Stdout stays a bare path so `$(dispatcher path …)` works; the
  # advice goes to stderr, where it cannot corrupt the substitution.
  for line in pack_advice(
    job_id,
    home_root=home_root,
    mount_base=mount_base,
    unpacked=unpacked,
    exact=exact,
  ):
    print(line, file=sys.stderr)
  return 0


def dump_readouts(
  *,
  server: str,
  job_id: str,
  names: list[str],
  full: bool = False,
  out: Any = sys.stdout,
) -> int:
  """Every readout value this job carries, as JSON on stdout."""
  payload = _get(server, f"/api/jobs/{job_id}/readouts")
  if payload is None:
    return 1
  shaped = shape_values(payload, names, full=full)
  json.dump(shaped, out, indent=2, sort_keys=True, default=str)
  print(file=out)
  return 0


# ── building and attaching the archives ──────────────────────────


def packable_by_host(
  job: dict[str, Any], hosts: list[str]
) -> dict[str, list[str]]:
  """`{host: [instance_id]}` for the instances it is safe to pack.

  Grouped by host because that is the archive's identity, and taken
  only from the terminal buckets — see `_PACKABLE` for why that bound
  is the design's safety property rather than a nicety."""
  out: dict[str, list[str]] = {}
  wanted = set(hosts)
  for bucket in _PACKABLE:
    for view in (job.get(bucket) or {}).values():
      if not isinstance(view, dict):
        continue
      host = str(view.get("host") or "")
      instance_id = str(view.get("instance_id") or "")
      if not host or not instance_id:
        continue
      if wanted and host not in wanted:
        continue
      out.setdefault(host, []).append(instance_id)
  return {h: sorted(v) for h, v in sorted(out.items())}


def _see_fresh(home_root: Path) -> None:
  """Drop this client's cached listings for the pack dir and its
  parent, so a just-written archive is visible.

  mksquashfs ran on another host. Without this the launcher's
  negative-dentry cache keeps answering "no such file" for up to
  acdirmax — 60s by default — and the verification below reads that
  as a failed pack while the archive sits there complete. The old
  router measured the same trap as 43% of instances parking in
  `unknown`, which is why `outcome.bust_dir_cache` exists; this is
  the same trick applied one directory up, because what appeared is
  `.packs/` itself and then a file inside it."""
  bust_dir_cache(home_root)
  packs = pack_dir(home_root)
  if packs.is_dir():
    bust_dir_cache(packs)


def _run(cmd: list[str], *, timeout: float) -> tuple[int, str]:
  try:
    done = subprocess.run(
      cmd,
      capture_output=True,
      text=True,
      timeout=timeout,
      check=False,
      # A tmux C-c on the operator's pane must not forward SIGINT
      # into an append that is rewriting an archive's tables.
      start_new_session=True,
    )
  except subprocess.TimeoutExpired:
    return 124, "timed out"
  except OSError as exc:
    return 1, str(exc)
  tail = (done.stderr or done.stdout or "").strip()
  return done.returncode, tail[-400:]


def do_pack(
  *,
  server: str,
  job_id: str,
  hosts: list[str],
  this_node: str | None = None,
  processors: int = 2,
  dry_run: bool = False,
  timeout: float = 3600.0,
  out: Any = sys.stdout,
) -> int:
  """Append every not-yet-packed terminal instance to its host's
  archive.

  Idempotent and safe to interrupt: what is already in an archive is
  read from the archive itself, so a re-run picks up exactly what the
  last one did not finish. Nothing is deleted either way, so the
  worst outcome of a failed pack is that reads stay on NFS."""
  job = _get(server, f"/api/jobs/{job_id}")
  if job is None:
    return 1
  home_root = Path(str(job.get("home_root") or ""))
  if not home_root.is_absolute():
    print(f"job {job_id!r} has no home_root", file=sys.stderr)
    return 1
  groups = packable_by_host(job, hosts)
  if not groups:
    print("nothing terminal to pack", file=out)
    return 0
  failures = 0
  packed_any = False
  _see_fresh(home_root)
  for host, instance_ids in groups.items():
    already = packed_instances(pack_path(home_root, host))
    todo = [i for i in instance_ids if i not in already]
    if not todo:
      print(f"{host:6s} {len(already):>5} packed, nothing new", file=out)
      continue
    cmd = pack_shell_cmd(home_root, host, todo, processors=processors)
    if dry_run:
      print(f"{host:6s} +{len(todo)} would run: {cmd}", file=out)
      continue
    # Imported here so the offline half of this file stays free of
    # the dispatcher's own dependencies (hosts.py reaches anyio).
    from dispatcher.core.hosts import SSH_OPTS

    # Skip ssh when the host is the machine running this command —
    # whoever that is, which is not necessarily the launcher.
    local = this_node if this_node is not None else socket.gethostname()
    argv = (
      ["bash", "-c", cmd]
      if host == local
      else ["ssh", *SSH_OPTS, host, cmd]
    )
    code, tail = _run(argv, timeout=timeout)
    _see_fresh(home_root)
    # Verify against the archive rather than the exit code: a
    # truncated archive that still exits 0 is the failure worth
    # catching, because readers would be served the truncation.
    now = packed_instances(pack_path(home_root, host))
    missing = [i for i in todo if i not in now]
    if code != 0 or missing:
      failures += 1
      print(
        f"{host:6s} +{len(todo) - len(missing)}/{len(todo)}  FAILED"
        + (f" (exit {code})" if code else "")
        + (f" {len(missing)} missing" if missing else "")
        + (f"  {tail}" if tail else ""),
        file=out,
      )
    else:
      print(f"{host:6s} +{len(todo):<5} now {len(now)} packed", file=out)
      packed_any = True
  # The server never reads archives on its own HTTP path, so it cannot
  # see a write this command made. Telling it is cheaper and more
  # certain than having it poll, and an operator who just ran this is
  # looking at the page now. Best-effort: the gap it closed is real
  # whether or not the server has caught up to it.
  if packed_any and not dry_run:
    _post(server, f"/api/jobs/{job_id}/packs-changed")
  return 1 if failures else 0


def _post(server: str, path: str) -> None:
  import httpx

  try:
    httpx.post(server.rstrip("/") + path, timeout=30.0)
  except httpx.HTTPError as exc:
    print(
      f"note: packed, but could not tell the server ({exc}) — its "
      f"pack_lag will catch up on the next completion or restart",
      file=sys.stderr,
    )


def do_mount(
  *,
  server: str,
  job_id: str,
  hosts: list[str],
  home_root: Path | None = None,
  mount_base: Path | None = None,
  out: Any = sys.stdout,
) -> int:
  """Attach this job's archives on this node.

  `home_root` skips the server, which is the whole of what it is asked
  for here."""
  binary = squashfuse_bin()
  if binary is None:
    print(_INSTALL_HINT, file=sys.stderr)
    return 1
  lag: int | None = None
  exact = False
  if home_root is None:
    job = _get(server, f"/api/jobs/{job_id}")
    if job is None:
      return 1
    home_root = Path(str(job.get("home_root") or ""))
    raw = job.get("pack_lag")
    if isinstance(raw, int):
      lag, exact = raw, True
  if not home_root.is_absolute():
    print(f"job {job_id!r} has no home_root", file=sys.stderr)
    return 1
  # The archives were written from other hosts, so this client may
  # still be caching a listing that predates them.
  _see_fresh(home_root)
  available = packed_hosts(home_root)
  chosen = (
    [h for h in available if h in set(hosts)] if hosts else available
  )
  if lag is None:
    lag = unpacked_from_tree(home_root)
  if not chosen:
    print(f"no packs under {home_root / '.packs'}", file=out)
    _advise(job_id, home_root, mount_base, lag, exact, out)
    return 0
  failures = 0
  for host in chosen:
    point = mount_dir(job_id, host, base=mount_base)
    if point.is_dir() and any(point.iterdir()):
      print(f"{host:6s} already mounted at {point}", file=out)
      continue
    point.mkdir(parents=True, exist_ok=True)
    code, tail = _run(
      mount_cmd(pack_path(home_root, host), point, binary), timeout=120
    )
    if code != 0:
      failures += 1
      print(f"{host:6s} FAILED {tail}", file=out)
    else:
      print(f"{host:6s} {point}", file=out)
  # After the report, not before: what the mounts do NOT cover is the
  # thing that decides whether a bulk read is about to be slow, and it
  # is only worth reading once the mounts themselves are listed.
  _advise(job_id, home_root, mount_base, lag, exact, out)
  return 1 if failures else 0


def _advise(
  job_id: str,
  home_root: Path,
  mount_base: Path | None,
  unpacked: int | None,
  exact: bool,
  out: Any,
) -> None:
  lines = pack_advice(
    job_id,
    home_root=home_root,
    mount_base=mount_base,
    unpacked=unpacked,
    exact=exact,
  )
  if lines:
    print(file=out)
  for line in lines:
    print(line, file=out)


def do_umount(
  *,
  job_id: str,
  mount_base: Path | None = None,
  out: Any = sys.stdout,
) -> int:
  """Detach every archive this node holds for a job.

  Takes no server: unmounting has to work when the dispatcher is
  down, and the mount points are discoverable from the filesystem."""
  base = job_mount_dir(job_id, base=mount_base)
  if not base.is_dir():
    print(f"nothing mounted under {base}", file=out)
    return 0
  failures = 0
  for point in sorted(p for p in base.iterdir() if p.is_dir()):
    code, tail = _run(umount_cmd(point), timeout=60)
    if code != 0:
      failures += 1
      print(f"{point.name:6s} FAILED {tail}", file=out)
    else:
      point.rmdir()
      print(f"{point.name:6s} unmounted", file=out)
  return 1 if failures else 0
