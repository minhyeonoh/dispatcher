"""HTTP surface + startup restore, with fake dispatch/poll."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from fastapi.testclient import TestClient

from dispatcher.api.app import create_app
from dispatcher.api.config import Config
from dispatcher.api.settings import Settings
from dispatcher.core.models import HostSettings

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.models import DispatchEntry


def mk_config(tmp_path: Path) -> Config:
  return Config(
    self_host="ml10",
    data_dir=tmp_path / "dispatcher-data",
    tick_interval=0.01,
    # Tests drive completion themselves; a real events stream
    # would ssh out.
    use_docker_events=False,
  )


def mk_settings() -> Settings:
  return Settings(
    max_concurrent=4,
    hosts={"ml10": HostSettings(max_concurrent=4)},
  )


def mk_client(
  tmp_path: Path,
  *,
  dispatched: list[DispatchEntry] | None = None,
) -> TestClient:
  captured = dispatched if dispatched is not None else []

  async def fake_dispatch(action, state) -> None:
    captured.append(action)

  app = create_app(
    mk_config(tmp_path),
    settings=mk_settings(),
    dispatch=fake_dispatch,
    poll=lambda _p: None,
  )
  return TestClient(app)


def payload(
  *,
  task_list: list[str],
  home_root: Path,
  label: str = "demo",
  attempt_id: str | None = None,
  extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
  base: dict[str, Any] = {
    "task_list": task_list,
    "home_root": str(home_root),
    "label": label,
    "container": {"image": "img"},
  }
  if attempt_id is not None:
    base["attempt_id"] = attempt_id
  if extra:
    base.update(extra)
  return base


# ── health / submit / state ──────────────────────────────────────


def test_health_returns_ok(tmp_path: Path):
  with mk_client(tmp_path) as client:
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_submit_returns_id_alias_and_appears_in_state(
  tmp_path: Path,
):
  with mk_client(tmp_path) as client:
    resp = client.post(
      "/attempts",
      json=payload(
        task_list=["t1", "t2"],
        home_root=tmp_path / "a",
        attempt_id="att-fixed",
      ),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["attempt_id"] == "att-fixed"
    assert body["status"] == "submitted"
    assert body["alias"]

    state = client.get("/state").json()
    ids = {a["attempt_id"] for a in state["attempts"]}
    assert "att-fixed" in ids
    detail = next(
      a for a in state["attempts"] if a["attempt_id"] == "att-fixed"
    )
    assert detail["counts"]["total"] == 2
    assert detail["home_root"] == str(tmp_path / "a")


def test_submit_generates_attempt_id_when_omitted(tmp_path: Path):
  with mk_client(tmp_path) as client:
    resp = client.post(
      "/attempts",
      json=payload(task_list=["t1"], home_root=tmp_path / "a"),
    )
    assert resp.status_code == 200
    assert resp.json()["attempt_id"].startswith("att-")


def test_submit_duplicate_id_returns_409(tmp_path: Path):
  with mk_client(tmp_path) as client:
    p1 = payload(
      task_list=["t1"],
      home_root=tmp_path / "a",
      attempt_id="dup",
    )
    assert client.post("/attempts", json=p1).status_code == 200
    p2 = payload(
      task_list=["t1"],
      home_root=tmp_path / "b",
      attempt_id="dup",
    )
    assert client.post("/attempts", json=p2).status_code == 409


def test_submit_duplicate_home_root_returns_409(tmp_path: Path):
  # Two attempts sharing a home would interleave trial dirs and
  # merge event logs — silent cross-contamination.
  with mk_client(tmp_path) as client:
    p1 = payload(task_list=["t1"], home_root=tmp_path / "same")
    assert client.post("/attempts", json=p1).status_code == 200
    p2 = payload(task_list=["t2"], home_root=tmp_path / "same")
    resp = client.post("/attempts", json=p2)
    assert resp.status_code == 409
    assert "home_root" in resp.json()["detail"]


def test_submit_relative_home_root_returns_400(tmp_path: Path):
  with mk_client(tmp_path) as client:
    p = payload(task_list=["t1"], home_root=tmp_path / "a")
    p["home_root"] = "relative/path"
    assert client.post("/attempts", json=p).status_code == 400


def test_submit_empty_or_duplicate_tasks_return_400(
  tmp_path: Path,
):
  with mk_client(tmp_path) as client:
    p = payload(task_list=[], home_root=tmp_path / "a")
    assert client.post("/attempts", json=p).status_code == 400
    p = payload(task_list=["t1", "t1"], home_root=tmp_path / "b")
    assert client.post("/attempts", json=p).status_code == 400


def test_submit_stray_payload_key_returns_400(tmp_path: Path):
  with mk_client(tmp_path) as client:
    p = payload(
      task_list=["t1"],
      home_root=tmp_path / "a",
      extra={"payloads": {"t2": 1}},  # typo — t2 not in tasks
    )
    resp = client.post("/attempts", json=p)
    assert resp.status_code == 400
    assert "typo" in resp.json()["detail"]


def test_submit_accepts_scheduler_knobs(tmp_path: Path):
  with mk_client(tmp_path) as client:
    p = payload(
      task_list=["t1"],
      home_root=tmp_path / "a",
      extra={"paused": True, "weight": 7, "pool": "gpu"},
    )
    aid = client.post("/attempts", json=p).json()["attempt_id"]
    detail = client.get(f"/attempts/{aid}").json()
    assert detail["paused"] is True
    assert detail["weight"] == 7
    assert detail["pool"] == "gpu"


# ── views ────────────────────────────────────────────────────────


def test_get_attempt_missing_returns_404(tmp_path: Path):
  with mk_client(tmp_path) as client:
    assert client.get("/attempts/nope").status_code == 404


def test_full_view_partitions_tasks(tmp_path: Path):
  from dispatcher.core.models import Outcome

  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1", "t2", "t3", "t4"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    sched = st.scheduler
    sched.patch(aid, paused=False)
    a1 = sched.dispatch_one()
    a2 = sched.dispatch_one()
    assert a1 is not None and a2 is not None
    sched.transition_trial(
      attempt_id=aid,
      task_name=a1.task_name,
      from_state="running",
      to_state="done_ok",
      outcome=Outcome(ok=True, values={"reward": 0.5}),
    )
    detail = client.get(f"/attempts/{aid}").json()
    assert set(detail["done_ok"]) == {a1.task_name}
    assert detail["done_ok"][a1.task_name]["values"] == {"reward": 0.5}
    assert set(detail["running"]) == {a2.task_name}
    assert set(detail["pending"]) == {"t3", "t4"}


def test_list_attempts_matches_state_and_scope_filter(
  tmp_path: Path,
):
  with mk_client(tmp_path) as client:
    client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        attempt_id="A",
        extra={"scope": "bench1", "paused": True},
      ),
    )
    client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "b",
        attempt_id="B",
        extra={"scope": "bench2", "paused": True},
      ),
    )
    all_rows = client.get("/attempts").json()
    assert {a["attempt_id"] for a in all_rows} == {"A", "B"}
    scoped = client.get("/attempts", params={"scope": "bench1"})
    assert {a["attempt_id"] for a in scoped.json()} == {"A"}
    full = client.get("/attempts", params={"full": 1}).json()
    assert {a["attempt_id"] for a in full} == {"A", "B"}
    assert "pending" in full[0]


# ── patch ────────────────────────────────────────────────────────


def test_patch_knobs_and_persistence(tmp_path: Path):
  from dispatcher.core.event_log import RUN_LOG_FILENAME, read_events

  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    resp = client.patch(
      f"/attempts/{aid}", json={"weight": 9, "tags": ["x", "x "]}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["weight"] == 9
    assert body["tags"] == ["x"]  # deduped + stripped
    events = read_events(tmp_path / "a" / RUN_LOG_FILENAME)
    patched = [e for e in events if e["type"] == "patch"]
    assert any(e.get("weight") == 9 for e in patched)


def test_patch_unknown_knob_returns_400(tmp_path: Path):
  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(task_list=["t1"], home_root=tmp_path / "a"),
    ).json()["attempt_id"]
    assert (
      client.patch(f"/attempts/{aid}", json={"bogus": 1}).status_code
      == 400
    )


def test_patch_alias_collision_returns_409(tmp_path: Path):
  with mk_client(tmp_path) as client:
    a = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"alias": "one", "paused": True},
      ),
    ).json()["attempt_id"]
    assert a
    b = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "b",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    assert (
      client.patch(f"/attempts/{b}", json={"alias": "one"}).status_code
      == 409
    )


# ── cancel ───────────────────────────────────────────────────────


def test_delete_removes_attempt_and_skips_restore(tmp_path: Path):
  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    kills: list[dict] = []
    st.runtime.fire_kill_trials = kills.append  # type: ignore[method-assign]
    resp = client.delete(f"/attempts/{aid}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert client.get(f"/attempts/{aid}").status_code == 404
    assert len(kills) == 1
  # Restart: the cancelled attempt must NOT come back.
  with mk_client(tmp_path) as client2:
    assert client2.get("/attempts").json() == []


# ── reclaim / retry ──────────────────────────────────────────────


def test_reclaim_requires_paused(tmp_path: Path):
  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(task_list=["t1"], home_root=tmp_path / "a"),
    ).json()["attempt_id"]
    resp = client.post(f"/attempts/{aid}/reclaim")
    assert resp.status_code == 409
    assert "paused" in resp.json()["detail"]


def test_reclaim_returns_running_tasks_to_pending(tmp_path: Path):
  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1", "t2"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    sched = st.scheduler
    sched.patch(aid, paused=False)
    assert sched.dispatch_one() is not None
    sched.patch(aid, paused=True)
    kills: list[dict] = []
    st.runtime.fire_kill_trials = kills.append  # type: ignore[method-assign]
    resp = client.post(f"/attempts/{aid}/reclaim")
    assert resp.status_code == 200
    assert resp.json()["reclaimed"] == ["t1"]
    assert len(kills) == 1
    detail = client.get(f"/attempts/{aid}").json()
    assert detail["pending"] == ["t1", "t2"]


def test_retry_done_err_moves_back_to_pending(tmp_path: Path):
  from dispatcher.core.models import Outcome

  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    sched = st.scheduler
    sched.patch(aid, paused=False)
    assert sched.dispatch_one() is not None
    sched.transition_trial(
      attempt_id=aid,
      task_name="t1",
      from_state="running",
      to_state="done_err",
      outcome=Outcome(ok=False),
    )
    st.metrics.record_completion(
      aid,
      __import__(
        "dispatcher.core.outcome", fromlist=["CompletionSnapshot"]
      ).CompletionSnapshot(outcome_exists=True, error_present=True),
    )
    sched.patch(aid, paused=True)
    resp = client.post(f"/attempts/{aid}/retry-done-err", json={})
    assert resp.status_code == 200
    assert resp.json()["retried"] == ["t1"]
    assert st.metrics.get(aid).err == 0
    detail = client.get(f"/attempts/{aid}").json()
    assert detail["pending"] == ["t1"]
    assert detail["done_err"] == {}


# ── archive ──────────────────────────────────────────────────────


def test_archive_roundtrip_and_guards(tmp_path: Path):
  from dispatcher.core.models import Outcome

  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    # Not terminal yet → 409.
    assert client.post(f"/attempts/{aid}/archive").status_code == 409
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    sched = st.scheduler
    sched.patch(aid, paused=False)
    assert sched.dispatch_one() is not None
    sched.transition_trial(
      attempt_id=aid,
      task_name="t1",
      from_state="running",
      to_state="done_ok",
      outcome=Outcome(ok=True),
    )
    resp = client.post(f"/attempts/{aid}/archive")
    assert resp.status_code == 200
    assert resp.json()["status"] == "archived"
    # Archived attempts are read-only.
    assert (
      client.patch(f"/attempts/{aid}", json={"weight": 2}).status_code
      == 409
    )
    # Cached bytes served verbatim.
    detail = client.get(f"/attempts/{aid}").json()
    assert set(detail["done_ok"]) == {"t1"}
    # Unarchive → live again.
    assert client.post(f"/attempts/{aid}/unarchive").status_code == 200
    assert client.post(f"/attempts/{aid}/unarchive").status_code == 409


# ── settings ─────────────────────────────────────────────────────


def test_patch_settings_hosts_and_caps(tmp_path: Path):
  with mk_client(tmp_path) as client:
    resp = client.patch(
      "/settings",
      json={
        "max_concurrent": 9,
        "hosts": {"ml9": {"max_concurrent": 3}},
        "pool_caps": {"gpu": 2},
      },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["max_concurrent"] == 9
    assert body["hosts"]["ml9"]["max_concurrent"] == 3
    assert body["pool_caps"] == {"gpu": 2}
  # pool_caps persist across restart.
  with mk_client(tmp_path) as client2:
    state = client2.get("/state").json()
    assert state["settings"]["pool_caps"] == {"gpu": 2}


def test_all_settings_persist_across_restart(tmp_path: Path):
  # The old router persisted only pool caps; every other PATCH
  # silently reverted on restart. Now the whole document survives
  # and beats the seed.
  with mk_client(tmp_path) as client:
    resp = client.patch(
      "/settings",
      json={
        "max_concurrent": 9,
        "orphan_gc": {"min_container_age_s": 5.0},
        "notify": {"telegram_chat_id": "chat-1"},
      },
    )
    assert resp.status_code == 200
  with mk_client(tmp_path) as client2:
    st = client2.get("/state").json()["settings"]
    assert st["max_concurrent"] == 9  # seed said 4
    assert st["orphan_gc"]["min_container_age_s"] == 5.0
    assert st["notify"]["telegram_chat_id"] == "chat-1"


def test_boot_overrides_beat_persisted(tmp_path: Path):
  from dispatcher.api.settings import SettingsPatch

  with mk_client(tmp_path) as client:
    client.patch("/settings", json={"max_concurrent": 9})

  async def fake_dispatch(action, state) -> None:
    return None

  app = create_app(
    mk_config(tmp_path),
    settings=mk_settings(),
    settings_overrides=SettingsPatch(max_concurrent=2),
    dispatch=fake_dispatch,
    poll=lambda _p: None,
  )
  with TestClient(app) as client2:
    st = client2.get("/state").json()["settings"]
    assert st["max_concurrent"] == 2  # explicit flag wins
  # And the override itself persisted.
  with mk_client(tmp_path) as client3:
    st = client3.get("/state").json()["settings"]
    assert st["max_concurrent"] == 2


def test_patch_settings_unknown_key_rejected(tmp_path: Path):
  with mk_client(tmp_path) as client:
    resp = client.patch("/settings", json={"bogus": 1})
    assert resp.status_code == 422


# ── monitor ──────────────────────────────────────────────────────


def test_monitor_reports_metrics(tmp_path: Path):
  from dispatcher.core.outcome import CompletionSnapshot

  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    st.metrics.record_completion(
      aid,
      CompletionSnapshot(
        outcome_exists=True,
        error_present=False,
        values={"reward": 1.0},
      ),
    )
    body = client.get("/monitor").json()
    row = next(a for a in body["attempts"] if a["attempt_id"] == aid)
    assert row["metrics"]["ok"] == 1
    assert row["metrics"]["means"] == {"reward": 1.0}


def test_monitor_stream_mounted_and_sse_frame_shape(
  tmp_path: Path,
):
  # In-process HTTP harnesses buffer whole responses, so the
  # never-ending stream is checked as: route mounted + frame
  # serializer correct. Event publication is pinned in
  # test_runtime.
  from dispatcher.api.wire import sse as _sse

  async def fake_dispatch(action, state) -> None:
    return None

  app = create_app(
    mk_config(tmp_path),
    settings=mk_settings(),
    dispatch=fake_dispatch,
    poll=lambda _p: None,
  )
  paths = {getattr(route, "path", None) for route in app.routes}
  assert "/monitor/stream" in paths
  frame = _sse("snapshot", {"nested": {"inner": [1, 2, 3]}})
  assert frame.startswith("event: snapshot\ndata: ")
  assert frame.endswith("\n\n")
  data_line = frame.split("\n")[1]
  assert json.loads(data_line[len("data: ") :]) == {
    "nested": {"inner": [1, 2, 3]}
  }


# ── restore ──────────────────────────────────────────────────────


def _write_log_and_index(
  tmp_path: Path,
  home: Path,
  events: list[dict],
) -> None:
  from dispatcher.core.event_log import (
    append_event,
    append_index_entry,
  )

  log = home / ".dispatcher-state.jsonl"
  for ev in events:
    append_event(log, ev)
  append_index_entry(
    mk_config(tmp_path).data_dir,
    {
      "event": "submit",
      "attempt_id": events[0]["attempt_id"],
      "log_path": str(log),
    },
  )


def _submit_ev(home: Path, tasks: list[str]) -> dict:
  return {
    "type": "submit",
    "attempt_id": "att-restore",
    "label": "demo",
    "task_list": tasks,
    "home_root": str(home),
    "container": {"image": "img"},
    "submitted_at": "2026-09-28T10:00:00+00:00",
    "alias": "restore-alias",
    "paused": True,
  }


def _dispatch_ev(task: str, trial: str) -> dict:
  return {
    "type": "dispatch",
    "attempt_id": "att-restore",
    "task_name": task,
    "trial_name": trial,
    "host": "ml10",
    "at": "2026-09-28T10:00:01+00:00",
  }


def _write_outcome(home: Path, trial: str, body: dict) -> None:
  d = home / trial
  d.mkdir(parents=True, exist_ok=True)
  (d / "outcome.json").write_text(json.dumps(body))


def test_restore_rebuilds_buckets_and_metrics(tmp_path: Path):
  home = tmp_path / "home"
  _write_log_and_index(
    tmp_path,
    home,
    [
      _submit_ev(home, ["t1", "t2"]),
      _dispatch_ev("t1", "t1__0000001"),
      _dispatch_ev("t2", "t2__0000002"),
    ],
  )
  _write_outcome(
    home, "t1__0000001", {"ok": True, "values": {"reward": 1.0}}
  )
  _write_outcome(
    home,
    "t2__0000002",
    {"ok": False, "error": {"type": "E", "message": "x"}},
  )
  with mk_client(tmp_path) as client:
    detail = client.get("/attempts/att-restore").json()
    assert set(detail["done_ok"]) == {"t1"}
    assert set(detail["done_err"]) == {"t2"}
    assert detail["done_ok"]["t1"]["values"] == {"reward": 1.0}
    assert detail["alias"] == "restore-alias"
    row = next(
      a
      for a in client.get("/monitor").json()["attempts"]
      if a["attempt_id"] == "att-restore"
    )
    assert row["metrics"]["ok"] == 1
    assert row["metrics"]["err"] == 1


def test_restore_requeued_task_occupies_one_bucket(tmp_path: Path):
  # An infra requeue leaves two dispatches for one task; only the
  # LATEST trial's outcome may count, or the task lands in two
  # buckets and both metrics inflate.
  home = tmp_path / "home"
  _write_log_and_index(
    tmp_path,
    home,
    [
      _submit_ev(home, ["t1"]),
      _dispatch_ev("t1", "t1__0000001"),
      _dispatch_ev("t1", "t1__0000002"),
    ],
  )
  _write_outcome(
    home,
    "t1__0000001",
    {"ok": False, "error": {"type": "E", "message": "killed"}},
  )
  _write_outcome(
    home, "t1__0000002", {"ok": True, "values": {"reward": 0.5}}
  )
  with mk_client(tmp_path) as client:
    detail = client.get("/attempts/att-restore").json()
    assert set(detail["done_ok"]) == {"t1"}
    assert detail["done_err"] == {}
    # The retry's values, not the dead first trial's.
    assert detail["done_ok"]["t1"]["values"] == {"reward": 0.5}
    row = next(
      a
      for a in client.get("/monitor").json()["attempts"]
      if a["attempt_id"] == "att-restore"
    )
    assert row["metrics"]["ok"] == 1
    assert row["metrics"]["err"] == 0
    assert row["metrics"]["means"] == {"reward": 0.5}


def test_restore_infra_outcome_goes_back_to_pending(
  tmp_path: Path,
):
  home = tmp_path / "home"
  _write_log_and_index(
    tmp_path,
    home,
    [
      _submit_ev(home, ["t1"]),
      _dispatch_ev("t1", "t1__0000001"),
    ],
  )
  _write_outcome(
    home,
    "t1__0000001",
    {
      "ok": False,
      "error": {"type": "InfraFailure", "message": "swap"},
      "infra": True,
    },
  )
  with mk_client(tmp_path) as client:
    detail = client.get("/attempts/att-restore").json()
    # Requeued, not scored (attempt is paused so it stays pending).
    assert detail["pending"] == ["t1"]
    assert detail["done_err"] == {}


def test_restore_advances_trial_name_counter(tmp_path: Path):
  # A restart that re-minted __0000001 would read the OLD trial
  # dir's outcome as the new trial's — scored before it ran.
  home = tmp_path / "home"
  _write_log_and_index(
    tmp_path,
    home,
    [
      _submit_ev(home, ["t1"]),
      _dispatch_ev("t1", "t1__0000007"),
    ],
  )
  _write_outcome(home, "t1__0000007", {"ok": True})
  dispatched: list = []
  with mk_client(tmp_path, dispatched=dispatched) as client:
    aid = client.post(
      "/attempts",
      json=payload(task_list=["x1"], home_root=tmp_path / "fresh"),
    ).json()["attempt_id"]
    assert aid
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    name = st.next_trial_name("x1")
    assert int(name.rsplit("__", 1)[1]) > 7


def test_restore_skips_corrupt_log_without_blocking(
  tmp_path: Path,
):
  home_bad = tmp_path / "bad"
  home_ok = tmp_path / "ok"
  # Corrupt: first line is not a submit.
  _write_log_and_index(
    tmp_path,
    home_bad,
    [{"type": "patch", "attempt_id": "att-bad", "paused": True}],
  )
  _write_log_and_index(
    tmp_path,
    home_ok,
    [
      {**_submit_ev(home_ok, ["t1"]), "attempt_id": "att-ok"},
    ],
  )
  # Fix the index entry ids (helper stamps events[0] id).
  with mk_client(tmp_path) as client:
    ids = {a["attempt_id"] for a in client.get("/attempts").json()}
    assert "att-ok" in ids
    assert "att-bad" not in ids


def test_restore_archived_attempt_stays_archived(tmp_path: Path):
  home = tmp_path / "home"
  _write_log_and_index(
    tmp_path,
    home,
    [
      _submit_ev(home, ["t1"]),
      _dispatch_ev("t1", "t1__0000001"),
      {
        "type": "archive",
        "attempt_id": "att-restore",
        "kind": "manual",
        "at": "2026-09-28T12:00:00+00:00",
      },
    ],
  )
  _write_outcome(home, "t1__0000001", {"ok": True})
  with mk_client(tmp_path) as client:
    detail = client.get("/attempts/att-restore").json()
    assert detail["archive_kind"] == "manual"
    assert detail["archived_at"] is not None


# ── per-trial reclaim ────────────────────────────────────────────


def test_reclaim_single_trial_spares_the_rest(tmp_path: Path):
  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1", "t2", "t3"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    sched = st.scheduler
    sched.patch(aid, paused=False)
    a1 = sched.dispatch_one()
    a2 = sched.dispatch_one()
    assert a1 is not None and a2 is not None
    sched.patch(aid, paused=True)  # freeze so we can assert
    kills: list[dict] = []
    st.runtime.fire_kill_trials = kills.append  # type: ignore[method-assign]
    resp = client.post(f"/attempts/{aid}/trials/{a1.trial_name}/reclaim")
    assert resp.status_code == 200
    body = resp.json()
    assert body["task_name"] == a1.task_name
    # ONLY the zombie's container set was killed.
    assert len(kills) == 1
    assert list(kills[0]) == [a1.task_name]
    detail = client.get(f"/attempts/{aid}").json()
    # Victim back at its task_list position; the healthy trial
    # untouched.
    assert detail["pending"] == [a1.task_name, "t3"]
    assert set(detail["running"]) == {a2.task_name}


def test_reclaim_single_trial_no_pause_needed_and_redispatches(
  tmp_path: Path,
):
  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    sched = st.scheduler
    sched.patch(aid, paused=False)
    first = sched.dispatch_one()
    assert first is not None
    st.runtime.fire_kill_trials = lambda r: None  # type: ignore[method-assign]
    resp = client.post(
      f"/attempts/{aid}/trials/{first.trial_name}/reclaim"
    )
    assert resp.status_code == 200  # attempt NOT paused — allowed
    second = sched.dispatch_one()
    assert second is not None
    assert second.task_name == "t1"
    assert second.trial_name != first.trial_name


def test_reclaim_trial_wrong_states(tmp_path: Path):
  from dispatcher.core.models import Outcome

  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    sched = st.scheduler
    sched.patch(aid, paused=False)
    action = sched.dispatch_one()
    assert action is not None
    sched.transition_trial(
      attempt_id=aid,
      task_name="t1",
      from_state="running",
      to_state="done_ok",
      outcome=Outcome(ok=True),
    )
    resp = client.post(
      f"/attempts/{aid}/trials/{action.trial_name}/reclaim"
    )
    assert resp.status_code == 409
    assert "done_ok" in resp.json()["detail"]
    assert (
      client.post(
        f"/attempts/{aid}/trials/nope__0000001/reclaim"
      ).status_code
      == 404
    )
    assert (
      client.post("/attempts/ghost/trials/x__0000001/reclaim").status_code
      == 404
    )


def test_reclaim_trial_event_survives_restart(tmp_path: Path):
  with mk_client(tmp_path) as client:
    aid = client.post(
      "/attempts",
      json=payload(
        task_list=["t1"],
        home_root=tmp_path / "a",
        attempt_id="A",
        extra={"paused": True},
      ),
    ).json()["attempt_id"]
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    sched = st.scheduler
    sched.patch(aid, paused=False)
    action = sched.dispatch_one()
    assert action is not None
    st.runtime.fire_kill_trials = lambda r: None  # type: ignore[method-assign]
    client.post(f"/attempts/{aid}/trials/{action.trial_name}/reclaim")
    sched.patch(aid, paused=True)
  # Restart: the reclaim event erased the dispatch, so the task
  # restores as pending — not as unknown-needing-resolution.
  with mk_client(tmp_path) as client2:
    detail = client2.get("/attempts/A").json()
    assert detail["pending"] == ["t1"]
    assert detail["unknown"] == {}
