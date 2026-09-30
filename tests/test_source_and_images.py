"""Source-archive contract + image pinning/distribution."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
from typing import TYPE_CHECKING

import pytest

from dispatcher.core.dispatch import (
  DispatchError,
  build_remote_command,
)
from dispatcher.core.models import DispatchEntry
from dispatcher.core.runtime import DispatcherRuntime
from tests.test_runtime import mk_attempt, mk_sched
from tests.test_server import mk_client, payload

if TYPE_CHECKING:
  from pathlib import Path


def _tar_bytes(files: dict[str, bytes]) -> bytes:
  buf = io.BytesIO()
  with tarfile.open(fileobj=buf, mode="w") as tf:
    for name, data in files.items():
      info = tarfile.TarInfo(name)
      info.size = len(data)
      tf.addfile(info, io.BytesIO(data))
  return buf.getvalue()


def _b64(files: dict[str, bytes]) -> str:
  return base64.b64encode(_tar_bytes(files)).decode()


# ── submit: source archive ───────────────────────────────────────


def test_submit_stores_source_tar_and_sha(tmp_path: Path):
  blob = _tar_bytes({"mod.py": b"X = 1\n"})
  with mk_client(tmp_path) as client:
    resp = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={
          "paused": True,
          "source_tar_b64": base64.b64encode(blob).decode(),
        },
      ),
    )
    assert resp.status_code == 200
    aid = resp.json()["attempt_id"]
    # The archive is on plain NFS-side storage — docker prune
    # can never touch the arm record.
    stored = (tmp_path / "a" / ".source.tar").read_bytes()
    assert stored == blob
    detail = client.get(f"/attempts/{aid}").json()
    assert detail["source_sha256"] == hashlib.sha256(blob).hexdigest()


def test_submit_source_survives_restart(tmp_path: Path):
  with mk_client(tmp_path) as client:
    client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        attempt_id="A",
        extra={
          "paused": True,
          "source_tar_b64": _b64({"m.py": b"pass\n"}),
        },
      ),
    )
  with mk_client(tmp_path) as client2:
    detail = client2.get("/attempts/A").json()
    assert detail["source_sha256"] != ""
    assert (tmp_path / "a" / ".source.tar").is_file()


def test_submit_bad_base64_is_400(tmp_path: Path):
  with mk_client(tmp_path) as client:
    resp = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"source_tar_b64": "no+t/base64!!"},
      ),
    )
    assert resp.status_code == 400


def test_require_source_rejects_bare_submit(tmp_path: Path):
  with mk_client(tmp_path) as client:
    client.patch("/settings", json={"require_source": True})
    resp = client.post(
      "/attempts",
      json=payload(task_list=["t1"], home_root=tmp_path / "a"),
    )
    assert resp.status_code == 400
    assert "source" in resp.json()["detail"]
    ok = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "b",
        extra={
          "paused": True,
          "source_tar_b64": _b64({"m.py": b"pass\n"}),
        },
      ),
    )
    assert ok.status_code == 200


# ── submit: image pinning ────────────────────────────────────────


def test_submit_pins_image_id_via_resolver(tmp_path: Path):
  from fastapi.testclient import TestClient

  from dispatcher.api.app import create_app
  from tests.test_server import mk_config, mk_settings

  async def fake_dispatch(action, state) -> None:
    return None

  app = create_app(
    mk_config(tmp_path),
    settings=mk_settings(),
    dispatch=fake_dispatch,
    poll=lambda _p: None,
    resolve_image=lambda ref: f"sha256:pinned-{ref}",
  )
  with TestClient(app) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    detail = client.get(f"/attempts/{aid}").json()
    assert detail["image_id"] == "sha256:pinned-img"


def test_submit_unresolvable_image_is_400(tmp_path: Path):
  from fastapi.testclient import TestClient

  from dispatcher.api.app import create_app
  from tests.test_server import mk_config, mk_settings

  def resolver(ref: str) -> str:
    raise RuntimeError(f"image {ref!r} not found on ml10")

  async def fake_dispatch(action, state) -> None:
    return None

  app = create_app(
    mk_config(tmp_path),
    settings=mk_settings(),
    dispatch=fake_dispatch,
    poll=lambda _p: None,
    resolve_image=resolver,
  )
  with TestClient(app) as client:
    resp = client.post(
      "/attempts",
      json=payload(task_list=["t1"], home_root=tmp_path / "a"),
    )
    assert resp.status_code == 400
    assert "not found" in resp.json()["detail"]


def test_fake_dispatch_mode_pins_nothing(tmp_path: Path):
  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    assert client.get(f"/attempts/{aid}").json()["image_id"] == ""


# ── dispatch command: pinned id + source mount ───────────────────


def _action() -> DispatchEntry:
  from datetime import UTC, datetime

  return DispatchEntry(
    attempt_id="att-001",
    task_name="t1",
    trial_name="t1__0000001",
    host="ml9",
    dispatched_at=datetime.now(UTC),
  )


def test_command_runs_pinned_id_not_tag(tmp_path: Path):
  state = mk_attempt(tmp_path, ["t1"])
  state.image_id = "sha256:deadbeef"
  cmd = build_remote_command(_action(), state, trial_home=tmp_path / "t")
  assert "sha256:deadbeef" in cmd
  # The mutable tag must not be what runs.
  assert " img " not in f" {cmd} "


def test_command_mounts_source_when_present(tmp_path: Path):
  state = mk_attempt(tmp_path, ["t1"])
  state.source_sha256 = "abc"
  cmd = build_remote_command(_action(), state, trial_home=tmp_path / "t")
  assert f"{tmp_path}/.source.tar:/dispatcher/source.tar:ro" in cmd
  assert "DISPATCHER_SOURCE=/dispatcher/source.tar" in cmd


def test_command_no_source_no_mount(tmp_path: Path):
  state = mk_attempt(tmp_path, ["t1"])
  cmd = build_remote_command(_action(), state, trial_home=tmp_path / "t")
  assert ".source.tar" not in cmd
  assert "DISPATCHER_SOURCE" not in cmd


# ── runtime: ensure-image path ───────────────────────────────────


def test_ensure_image_runs_once_per_image_host(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  sched = mk_sched(2)
  attempt = mk_attempt(tmp_path, ["t1", "t2"])
  attempt.image_id = "sha256:env1"
  sched.submit(attempt)
  ensured: list[tuple[str, str]] = []

  async def fake_ensure(host, image_id, *, self_host, **kw):
    ensured.append((host, image_id))

  monkeypatch.setattr(
    "dispatcher.core.runtime.ensure_image_on_host", fake_ensure
  )
  shipped: list[str] = []

  async def fake_docker_dispatch(action, state, **kw):
    shipped.append(action.trial_name)

  monkeypatch.setattr(
    "dispatcher.core.runtime.docker_dispatch",
    fake_docker_dispatch,
  )
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  asyncio.run(runtime._dispatch_available())
  assert len(shipped) == 2
  # Two trials, same image, same host → ONE ensure.
  assert ensured == [("ml10", "sha256:env1")]


def test_ensure_image_failure_requeues_not_scores(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  sched = mk_sched(1)
  attempt = mk_attempt(tmp_path, ["t1"])
  attempt.image_id = "sha256:gone"
  sched.submit(attempt)

  async def fail_ensure(host, image_id, *, self_host, **kw):
    raise RuntimeError("ship failed")

  monkeypatch.setattr(
    "dispatcher.core.runtime.ensure_image_on_host", fail_ensure
  )
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  asyncio.run(runtime._dispatch_available())
  view = sched.attempt_view("att-001")
  # Bounded requeues exhausted → parked unknown; never done_err.
  assert set(view.unknown) == {"t1"}
  assert not view.done_err


def test_dispatch_error_carries_ensure_reason(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  async def fail_ensure(host, image_id, *, self_host, **kw):
    raise RuntimeError("image sha256:x vanished (pruned?)")

  monkeypatch.setattr(
    "dispatcher.core.runtime.ensure_image_on_host", fail_ensure
  )
  sched = mk_sched(1)
  attempt = mk_attempt(tmp_path, ["t1"])
  attempt.image_id = "sha256:x"
  sched.submit(attempt)
  runtime = DispatcherRuntime(
    sched, self_host="ml10", poll=lambda _p: None
  )
  with pytest.raises(DispatchError, match="vanished"):
    asyncio.run(runtime._ensure_image("ml10", "sha256:x"))


# ── sdk bootstrap ────────────────────────────────────────────────


def test_bootstrap_unpacks_and_execs(tmp_path: Path):
  src_tar = tmp_path / "source.tar"
  src_tar.write_bytes(
    _tar_bytes({"mymod.py": b"VALUE = 'from-frozen-source'\n"})
  )
  dest = tmp_path / "unpacked"
  env = {
    **os.environ,
    "DISPATCHER_SOURCE": str(src_tar),
    "DISPATCHER_SOURCE_DEST": str(dest),
  }
  r = subprocess.run(
    [
      sys.executable,
      "-m",
      "dispatcher_sdk.bootstrap",
      "--",
      sys.executable,
      "-c",
      "import mymod, json; print(json.dumps(mymod.VALUE))",
    ],
    capture_output=True,
    text=True,
    env=env,
    cwd=str(tmp_path),
  )
  assert r.returncode == 0, r.stderr
  assert json.loads(r.stdout) == "from-frozen-source"


def test_bootstrap_without_source_is_plain_exec(tmp_path: Path):
  env = {k: v for k, v in os.environ.items()}
  env.pop("DISPATCHER_SOURCE", None)
  r = subprocess.run(
    [
      sys.executable,
      "-m",
      "dispatcher_sdk.bootstrap",
      "--",
      sys.executable,
      "-c",
      "print('plain')",
    ],
    capture_output=True,
    text=True,
    env=env,
  )
  assert r.returncode == 0
  assert r.stdout.strip() == "plain"


def test_bootstrap_unreadable_source_exits_75(
  tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
  monkeypatch.setenv("DISPATCHER_SOURCE", str(tmp_path / "missing.tar"))
  monkeypatch.setenv("DISPATCHER_SOURCE_DEST", str(tmp_path / "d"))
  import dispatcher_sdk.bootstrap as bs

  # Direct call (subprocess would sleep through retries).
  orig = bs._READ_RETRIES
  bs._READ_RETRIES = ()
  try:
    rc = bs.main(
      ["--", sys.executable, "-c", "print('never')"],
    )
  finally:
    bs._READ_RETRIES = orig
  assert rc == 75
