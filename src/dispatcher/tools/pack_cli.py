"""`dispatcher path` and `dispatcher readouts` — the read side.

Both exist so that an analysis script never spells out a job's
layout. Today `path` almost always answers with the NFS home and
`readouts` is a thin wrapper over an endpoint; that is the point —
once packs land, scripts written against these two get the faster
read without being edited, and the one place that knows whether a
pack is mounted is `core.pack`.

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
import sys
from pathlib import Path
from typing import Any

import httpx

from dispatcher.core.pack import mount_dir, pack_path, read_home_for

_BUCKETS = ("done_ok", "done_err", "running", "ghosted", "unknown")


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


def print_path(
  *,
  server: str,
  job_id: str,
  ident: str,
  mount_base: Path | None = None,
  out: Any = sys.stdout,
) -> int:
  """Print the read path for one instance home, and nothing else.

  Stdout stays a bare path so `$(dispatcher path …)` works. When a
  pack exists but is not mounted here the note goes to stderr: the
  answer is still correct, it is just slower than it needs to be, and
  that is worth saying exactly once where it cannot corrupt the
  substitution."""
  job = _get(server, f"/api/jobs/{job_id}")
  if job is None:
    return 1
  home_root = Path(str(job.get("home_root") or ""))
  if not home_root.is_absolute():
    print(f"job {job_id!r} has no home_root", file=sys.stderr)
    return 1
  instance_id, host = resolve_instance(job, ident)
  path, packed = read_home_for(
    home_root,
    instance_id,
    job_id=job_id,
    host=host,
    mount_base=mount_base,
  )
  print(path, file=out)
  if not packed and host and pack_path(home_root, host).is_file():
    where = mount_dir(job_id, host, base=mount_base)
    print(
      f"note: {host} has a pack but it is not mounted at {where} — "
      f"`dispatcher mount {job_id}` reads it instead",
      file=sys.stderr,
    )
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
