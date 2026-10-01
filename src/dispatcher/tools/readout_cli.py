"""`dispatcher readout` — register readouts from a file, and run the
retroactive pass.

What gets registered is the CODE, not a path to it, which is what
makes a readout written today computable against a run from two
months ago. Writing Python inside a JSON body is miserable, so the
registration subcommands take a FILE: your repo keeps the functions
under version control and tested, and the CLI ships their text.

A thin client over `POST /api/readouts/compute`: the server owns the
containers (it runs on the launcher, which holds the pinned images),
and the stream it returns is one line per pass so the operator
watches progress instead of waiting on a silent request.

Safe to interrupt and safe to re-run. Only missing pairs are ever
computed, so Ctrl-C costs at most the pass in flight.
"""

from __future__ import annotations

import json
import pathlib
import sys
from typing import Any

import httpx

_TARGET_JOB_PREFIX = "job-"


def classify_target(target: str) -> dict[str, Any]:
  """An arena is a path and a job_id starts with `job-`; nothing
  else needs asking. A bare word is read as an arena, because that
  is what a single-segment arena looks like."""
  if target.startswith(_TARGET_JOB_PREFIX) and "/" not in target:
    return {"job_id": target}
  return {"arena": target}


def run_readout(
  *,
  server: str,
  target: str,
  names: list[str],
  out: Any = sys.stdout,
) -> int:
  body: dict[str, Any] = classify_target(target)
  if names:
    body["names"] = names
  url = server.rstrip("/") + "/api/readouts/compute"
  totals = {"passes": 0, "written": 0, "unreported": 0, "errors": 0}
  try:
    # No read timeout: one pass is a container run against operator
    # code, and the generous per-readout budget means it can
    # legitimately take a long while.
    with httpx.stream(
      "POST",
      url,
      json=body,
      timeout=httpx.Timeout(30.0, read=None),
    ) as resp:
      if resp.status_code != 200:
        resp.read()
        detail = resp.text.strip()[:500]
        print(f"{resp.status_code}: {detail}", file=sys.stderr)
        return 1
      for line in resp.iter_lines():
        if not line.strip():
          continue
        try:
          report = json.loads(line)
        except json.JSONDecodeError:
          continue
        totals["passes"] += 1
        totals["written"] += int(report.get("written") or 0)
        totals["unreported"] += int(report.get("unreported") or 0)
        error = str(report.get("error") or "")
        if error:
          totals["errors"] += 1
        print(
          f"{report.get('job_id', '?')}  "
          f"{report.get('instances', 0):>4} instance(s)  "
          f"+{report.get('written', 0):<5} written  "
          f"{report.get('remaining', 0):>5} left"
          + (f"  ERROR {error}" if error else ""),
          file=out,
          flush=True,
        )
  except KeyboardInterrupt:
    print("\ninterrupted — re-run to continue", file=sys.stderr)
    return 130
  except httpx.HTTPError as exc:
    print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
    return 1
  if totals["passes"] == 0:
    print("nothing to compute — every pair already has a value", file=out)
    return 0
  print(
    f"\n{totals['passes']} pass(es)  {totals['written']} value(s) written"
    + (
      f"  {totals['unreported']} unreported"
      if totals["unreported"]
      else ""
    )
    + (
      f"  {totals['errors']} failed pass(es)" if totals["errors"] else ""
    ),
    file=out,
  )
  return 1 if totals["errors"] else 0


def _read_file(path: str) -> str:
  try:
    return pathlib.Path(path).read_text(encoding="utf-8")
  except OSError as exc:
    print(f"cannot read {path}: {exc}", file=sys.stderr)
    raise SystemExit(1) from exc


def _post(
  server: str, path: str, body: dict[str, Any], *, method: str = "POST"
) -> int:
  url = server.rstrip("/") + path
  try:
    resp = httpx.request(method, url, json=body, timeout=120.0)
  except httpx.HTTPError as exc:
    print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
    return 1
  if resp.status_code != 200:
    detail = resp.text.strip()[:600]
    print(f"{resp.status_code}: {detail}", file=sys.stderr)
    return 1
  out = resp.json()
  needs = out.get("needs_backfill") or []
  for job, err in (out.get("errors") or {}).items():
    print(f"  {job}: {err}", file=sys.stderr)
  if needs:
    print(f"{len(needs)} job(s) need values: {out.get('hint') or ''}")
  else:
    print("registered")
  return 0


def add_readout(
  *, server: str, arena: str, file: str, name: str, timeout_sec: float
) -> int:
  """Register one per-instance readout from a file.

  The whole file is sent, not just the one function: helpers beside
  it stay available, and the record keeps the context a reader of the
  value would want."""
  return _post(
    server,
    "/api/readouts",
    {
      "arena": arena,
      "name": name,
      "source": _read_file(file),
      "timeout_sec": timeout_sec,
    },
  )


def set_columns(*, server: str, arena: str, file: str) -> int:
  """Register the arena's `columns(job)` function from a file."""
  return _post(
    server,
    "/api/readouts/columns",
    {"arena": arena, "source": _read_file(file)},
    method="PUT",
  )
