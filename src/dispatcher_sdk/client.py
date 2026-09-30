"""HTTP client for the dispatcher server. Stdlib urllib — a
research repo can submit without adding dependencies."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


class ClientError(RuntimeError):
  """Non-2xx response; carries status + body."""

  def __init__(self, status: int, body: str) -> None:
    super().__init__(f"HTTP {status}: {body[:500]}")
    self.status = status
    self.body = body


def _request(
  server: str,
  method: str,
  path: str,
  payload: dict[str, Any] | None = None,
  *,
  timeout: float = 30.0,
) -> Any:
  url = server.rstrip("/") + path
  data = (
    json.dumps(payload).encode("utf-8") if payload is not None else None
  )
  req = urllib.request.Request(
    url,
    data=data,
    method=method,
    headers={"Content-Type": "application/json"},
  )
  try:
    with urllib.request.urlopen(req, timeout=timeout) as resp:
      return json.loads(resp.read().decode("utf-8"))
  except urllib.error.HTTPError as exc:
    body = exc.read().decode("utf-8", "replace")
    raise ClientError(exc.code, body) from exc


def submit_job(server: str, payload: dict[str, Any]) -> dict[str, Any]:
  """POST /jobs. Required payload keys: label, task_ids,
  home_root (absolute, unique per job), container ({image,
  command, env, mounts, home_mount, extra_args}). Optional:
  payloads (per-task, keys ⊆ task_ids), env, pool, tags, scope,
  paused, weight, max_concurrent, pause_on_error, alias."""
  return _request(server, "POST", "/jobs", payload)


def get_state(server: str) -> dict[str, Any]:
  return _request(server, "GET", "/state")


def list_jobs(
  server: str, *, full: bool = False, scope: str = ""
) -> list[dict[str, Any]]:
  qs = []
  if full:
    qs.append("full=1")
  if scope:
    qs.append(f"scope={scope}")
  path = "/jobs" + ("?" + "&".join(qs) if qs else "")
  return _request(server, "GET", path)


def get_job(server: str, job_id: str) -> dict[str, Any]:
  return _request(server, "GET", f"/jobs/{job_id}")


def patch_job(
  server: str, job_id: str, knobs: dict[str, Any]
) -> dict[str, Any]:
  return _request(server, "PATCH", f"/jobs/{job_id}", knobs)


def cancel_job(server: str, job_id: str) -> dict[str, Any]:
  return _request(server, "DELETE", f"/jobs/{job_id}")


def reclaim_job(server: str, job_id: str) -> dict[str, Any]:
  return _request(server, "POST", f"/jobs/{job_id}/reclaim", {})


def reclaim_instance(
  server: str, job_id: str, instance_id: str
) -> dict[str, Any]:
  """Kill one running instance; its task re-queues with a fresh
  instance. No pause needed."""
  return _request(
    server,
    "POST",
    f"/jobs/{job_id}/instances/{instance_id}/reclaim",
    {},
  )


def retry_done_err(
  server: str,
  job_id: str,
  body: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return _request(
    server,
    "POST",
    f"/jobs/{job_id}/retry-done-err",
    body or {},
  )


def patch_settings(server: str, patch: dict[str, Any]) -> dict[str, Any]:
  return _request(server, "PATCH", "/settings", patch)
