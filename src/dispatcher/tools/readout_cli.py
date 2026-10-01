"""`dispatcher readout` — the retroactive readout pass, as a
command.

A thin client over `POST /api/readouts/compute`: the server owns the
containers (it runs on the launcher, which holds the pinned images),
and the stream it returns is one line per pass so the operator
watches progress instead of waiting on a silent request.

Safe to interrupt and safe to re-run. Only missing pairs are ever
computed, so Ctrl-C costs at most the pass in flight.
"""

from __future__ import annotations

import json
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
