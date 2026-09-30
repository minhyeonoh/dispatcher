"""Container-layer helpers: label parsing, line splitting, event
stream argv + end-to-end via a docker stub, census/probe parsing."""

from __future__ import annotations

import asyncio
import json
import os
import stat
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from dispatcher.core import labels
from dispatcher.core.containers import (
  DockerEventStreamManager,
  _HostStream,
  container_labels,
  iter_lines,
  parse_created,
  probe_instance,
)

if TYPE_CHECKING:
  from pathlib import Path


# ── parsing ──────────────────────────────────────────────────────


def test_container_labels_is_a_real_dict():
  row = {
    "Config": {
      "Labels": {
        "dispatcher.instance": "t1__0000001",
        # A foreign label whose VALUE embeds our key=value shape
        # — poisonous to comma-string parsing, inert on a dict.
        "foreign": "x,dispatcher.set=evil",
      }
    }
  }
  assert container_labels(row)["dispatcher.instance"] == "t1__0000001"
  assert "dispatcher.set" not in container_labels(row)


def test_container_labels_missing_or_null():
  assert container_labels({}) == {}
  assert container_labels({"Config": {"Labels": None}}) == {}
  assert container_labels({"Config": None}) == {}


def test_container_labels_preserves_case():
  row = {
    "Config": {"Labels": {"dispatcher.set": "instance_T2019__0000001"}}
  }
  assert (
    container_labels(row)["dispatcher.set"] == "instance_T2019__0000001"
  )


def test_parse_created_nanoseconds_z():
  dt = parse_created("2026-08-03T11:56:15.123456789Z")
  assert dt is not None
  assert dt.microsecond == 123456
  assert dt.tzinfo is not None


def test_parse_created_offset_form():
  dt = parse_created("2026-08-03T02:56:15.5+00:00")
  assert dt is not None
  assert dt.hour == 11  # normalised to KST (+09:00)


def test_parse_created_no_fraction():
  assert parse_created("2026-08-03T11:56:15Z") is not None


def test_parse_created_garbage_is_none():
  assert parse_created("not a date") is None
  assert parse_created("") is None
  assert parse_created("2026-08-03T11:56:15") is None  # naive


# ── iter_lines ───────────────────────────────────────────────────


class _FakeByteStream:
  def __init__(self, chunks: list[bytes]) -> None:
    self._chunks = chunks

  def __aiter__(self):
    return self

  async def __anext__(self) -> bytes:
    if not self._chunks:
      raise StopAsyncIteration
    return self._chunks.pop(0)


def test_iter_lines_chunk_boundary_split():
  async def _run():
    stream = _FakeByteStream([b"hel", b"lo\nwor", b"ld\n"])
    return [line async for line in iter_lines(stream)]  # type: ignore[arg-type]

  assert asyncio.run(_run()) == [b"hello\n", b"world\n"]


def test_iter_lines_trailing_no_newline():
  async def _run():
    stream = _FakeByteStream([b"partial"])
    return [line async for line in iter_lines(stream)]  # type: ignore[arg-type]

  assert asyncio.run(_run()) == [b"partial"]


def test_iter_lines_empty_stream():
  async def _run():
    stream = _FakeByteStream([])
    return [line async for line in iter_lines(stream)]  # type: ignore[arg-type]

  assert asyncio.run(_run()) == []


# ── _HostStream argv ─────────────────────────────────────────────


def _make_stream(**overrides) -> _HostStream:
  async def _noop_event(host, event) -> None:
    pass

  async def _noop_alive(host, alive) -> None:
    pass

  defaults = dict(
    host="ml9",
    self_host="ml10",
    on_event=_noop_event,
    on_alive_change=_noop_alive,
  )
  defaults.update(overrides)
  return _HostStream(**defaults)  # type: ignore[arg-type]


def test_build_argv_local_skips_ssh():
  stream = _make_stream(host="ml10", self_host="ml10")
  argv = stream._build_argv()
  assert argv[0] == "docker"
  assert "ssh" not in argv


def test_build_argv_remote_uses_ssh_single_arg():
  stream = _make_stream(host="ml9", self_host="ml10")
  argv = stream._build_argv()
  assert argv[0] == "ssh"
  assert "ml9" in argv
  # The docker invocation must be ONE shell-quoted trailing arg so
  # the remote shell doesn't word-split `{{json .}}`.
  assert "docker" in argv[-1]
  assert "'{{json .}}'" in argv[-1]


def test_build_argv_includes_since_and_filters():
  fixed = datetime(2026, 9, 21, 3, 0, 0, tzinfo=UTC)
  stream = _make_stream(last_ack_time=fixed)
  remote_cmd = stream._build_argv()[-1]
  assert "--since" in remote_cmd
  assert "2026-09-21T03:00:00" in remote_cmd
  assert f"label={labels.MANAGED}={labels.MANAGED_VALUE}" in remote_cmd
  assert "event=die" in remote_cmd


# ── end-to-end via docker stub ───────────────────────────────────


def _write_docker_stub(tmp_path: Path, lines: list[str]) -> Path:
  script = tmp_path / "docker"
  body = "#!/usr/bin/env bash\n"
  for ln in lines:
    body += f"echo {ln!r}\n"
  script.write_text(body)
  script.chmod(
    script.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH
  )
  return script


def test_stream_dispatches_events_and_advances_ack(tmp_path: Path):
  _write_docker_stub(
    tmp_path,
    lines=[
      json.dumps(
        {
          "Actor": {
            "Attributes": {
              labels.INSTANCE: "task1__0000001",
              "exitCode": "0",
            }
          },
          "time": 1721551200,
        }
      ),
      json.dumps(
        {
          "Actor": {
            "Attributes": {
              labels.INSTANCE: "task2__0000002",
              "exitCode": "137",
            }
          },
          "time": 1721551201,
        }
      ),
    ],
  )
  events: list[tuple[str, dict]] = []
  alive_changes: list[tuple[str, bool]] = []

  async def on_event(host, event):
    events.append((host, event))

  async def on_alive(host, alive):
    alive_changes.append((host, alive))

  old_path = os.environ.get("PATH", "")
  os.environ["PATH"] = f"{tmp_path}{os.pathsep}{old_path}"

  async def _run() -> _HostStream:
    stream = _HostStream(
      host="local",
      self_host="local",
      on_event=on_event,
      on_alive_change=on_alive,
      alive_timeout_sec=0.1,
    )
    await stream._one_connect()
    return stream

  try:
    stream = asyncio.run(_run())
  finally:
    os.environ["PATH"] = old_path

  assert len(events) == 2
  assert stream.last_ack_time == datetime.fromtimestamp(1721551201, UTC)
  # Already-alive stream: no transition callback on first event.
  assert alive_changes == []


def test_manager_start_ensure_drop(tmp_path: Path):
  _write_docker_stub(tmp_path, lines=[])

  async def _noop_event(host, event):
    pass

  async def _noop_alive(host, alive):
    pass

  old_path = os.environ.get("PATH", "")
  os.environ["PATH"] = f"{tmp_path}{os.pathsep}{old_path}"

  async def _run() -> None:
    mgr = DockerEventStreamManager(
      self_host="local",
      on_event=_noop_event,
      on_alive_change=_noop_alive,
    )
    mgr.start(["local", "other-local"])
    assert mgr.alive("local") is True
    assert mgr.alive("other-local") is True
    mgr.ensure_host("local")  # idempotent
    await mgr.drop_host("other-local")
    assert mgr.alive("other-local") is True  # untracked → default
    await mgr.stop()

  try:
    asyncio.run(_run())
  finally:
    os.environ["PATH"] = old_path


# ── probe ────────────────────────────────────────────────────────


def test_probe_gone_when_no_records(monkeypatch):
  class _R:
    returncode = 0
    stdout = "\n"
    stderr = ""

  async def fake_run_on(host, self_host, cmd, timeout=None):
    return _R()

  monkeypatch.setattr("dispatcher.core.containers.run_on", fake_run_on)
  status = asyncio.run(
    probe_instance("ml9", "t1__0000001", self_host="ml10")
  )
  assert status == "gone"


def test_probe_running_wins_over_exited(monkeypatch):
  class _R:
    returncode = 0
    stdout = "exited\nrunning\n"
    stderr = ""

  async def fake_run_on(host, self_host, cmd, timeout=None):
    return _R()

  monkeypatch.setattr("dispatcher.core.containers.run_on", fake_run_on)
  status = asyncio.run(
    probe_instance("ml9", "t1__0000001", self_host="ml10")
  )
  assert status == "running"


def test_probe_queries_by_label_with_verbatim_case(monkeypatch):
  seen: list[str] = []

  class _R:
    returncode = 0
    stdout = "running\n"
    stderr = ""

  async def fake_run_on(host, self_host, cmd, timeout=None):
    seen.append(cmd)
    return _R()

  monkeypatch.setattr("dispatcher.core.containers.run_on", fake_run_on)
  asyncio.run(
    probe_instance(
      "ml9",
      "instance_T20190907_004351__0081119",
      self_host="ml10",
    )
  )
  # Labels pass the instance name through verbatim — no lowercasing,
  # no name conventions — and the state comes from inspect, not
  # from human-oriented ps output.
  assert "instance_T20190907_004351__0081119" in seen[0]
  assert labels.INSTANCE in seen[0]
  assert "docker inspect" in seen[0]


def test_probe_raises_on_docker_failure(monkeypatch):
  import pytest

  class _R:
    returncode = 1
    stdout = ""
    stderr = "Cannot connect to the Docker daemon"

  async def fake_run_on(host, self_host, cmd, timeout=None):
    return _R()

  monkeypatch.setattr("dispatcher.core.containers.run_on", fake_run_on)
  with pytest.raises(RuntimeError, match="docker probe"):
    asyncio.run(probe_instance("ml9", "t1__0000001", self_host="ml10"))
