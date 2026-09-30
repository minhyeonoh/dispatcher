"""Arena: derived job grouping + fan-out group ops.

An arena owns no state — it exists iff a job names it — so every
test here pins the derived-index property: reads aggregate member
jobs, ops loop per-job ops, and nothing arena-side can disagree
with the scheduler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from tests.test_server import mk_client, payload

if TYPE_CHECKING:
  from pathlib import Path


def _submit(client, tmp_path: Path, job_id: str, arena: str = "v7"):
  resp = client.post(
    "/jobs",
    json=payload(
      task_ids=["t1", "t2"],
      home_root=tmp_path / job_id,
      job_id=job_id,
      extra={"arena": arena, "paused": True},
    ),
  )
  assert resp.status_code == 200, resp.text
  return resp


# ── derived index ────────────────────────────────────────────────


def test_arenas_list_aggregates_members(tmp_path: Path):
  with mk_client(tmp_path) as client:
    _submit(client, tmp_path, "A", arena="v7")
    _submit(client, tmp_path, "B", arena="v7")
    _submit(client, tmp_path, "C", arena="accord")
    rows = {r["arena"]: r for r in client.get("/arenas").json()}
    assert set(rows) == {"v7", "accord"}
    assert rows["v7"]["jobs"] == 2
    assert rows["v7"]["counts"]["pending"] == 4
    assert rows["v7"]["counts"]["total"] == 4
    assert rows["accord"]["jobs"] == 1


def test_ungrouped_jobs_do_not_form_an_arena(tmp_path: Path):
  with mk_client(tmp_path) as client:
    client.post(
      "/jobs",
      json=payload(
        task_ids=["t1"],
        home_root=tmp_path / "a",
        extra={"paused": True},
      ),
    )
    assert client.get("/arenas").json() == []


def test_arena_detail_and_unknown_404(tmp_path: Path):
  with mk_client(tmp_path) as client:
    _submit(client, tmp_path, "A")
    detail = client.get("/arenas/v7").json()
    assert [m["job_id"] for m in detail["members"]] == ["A"]
    assert client.get("/arenas/nope").status_code == 404


def test_arena_survives_restart_via_submit_event(tmp_path: Path):
  with mk_client(tmp_path) as client:
    _submit(client, tmp_path, "A")
  with mk_client(tmp_path) as client2:
    rows = client2.get("/arenas").json()
    assert [r["arena"] for r in rows] == ["v7"]


def test_patch_moves_job_between_arenas(tmp_path: Path):
  with mk_client(tmp_path) as client:
    _submit(client, tmp_path, "A", arena="old")
    resp = client.patch("/jobs/A", json={"arena": "new"})
    assert resp.status_code == 200
    assert client.get("/arenas/old").status_code == 404
    detail = client.get("/arenas/new").json()
    assert [m["job_id"] for m in detail["members"]] == ["A"]


# ── group ops: fan-outs ──────────────────────────────────────────


def test_arena_pause_resume_fan_out(tmp_path: Path):
  with mk_client(tmp_path) as client:
    _submit(client, tmp_path, "A")
    _submit(client, tmp_path, "B")
    _submit(client, tmp_path, "C", arena="other")
    out = client.post("/arenas/v7/resume").json()
    assert set(out["changed"]) == {"A", "B"}
    assert out["skipped"] == []
    # The other arena's member is untouched.
    assert client.get("/jobs/C").json()["paused"] is True
    out = client.post("/arenas/v7/pause").json()
    assert set(out["changed"]) == {"A", "B"}
    assert client.get("/jobs/A").json()["paused"] is True


def test_arena_reclaim_respects_per_job_pause_gate(tmp_path: Path):
  with mk_client(tmp_path) as client:
    _submit(client, tmp_path, "A")
    _submit(client, tmp_path, "B")
    st = client.app.state.dispatcher  # type: ignore[union-attr]
    # A: paused with one running instance; B: NOT paused.
    st.scheduler.patch("A", paused=False)
    action = st.scheduler.dispatch_one()
    assert action is not None and action.job_id == "A"
    st.scheduler.patch("A", paused=True)
    st.scheduler.patch("B", paused=False)
    st.runtime.fire_kill_instances = lambda r: None  # type: ignore[method-assign]
    out = client.post("/arenas/v7/reclaim").json()
    assert out["reclaimed"] == {"A": [action.task_id]}
    # B skipped with the same not-paused reason /jobs/B/reclaim
    # would give — never silently forced.
    assert [s["job_id"] for s in out["skipped"]] == ["B"]
    assert "not paused" in out["skipped"][0]["reason"]


def test_arena_cancel_requires_confirm(tmp_path: Path):
  with mk_client(tmp_path) as client:
    _submit(client, tmp_path, "A")
    _submit(client, tmp_path, "B")
    resp = client.post("/arenas/v7/cancel", json={})
    assert resp.status_code == 409
    assert "A" in resp.json()["detail"]
    # Nothing died.
    assert client.get("/jobs/A").status_code == 200
    resp = client.post("/arenas/v7/cancel", json={"confirm": True})
    assert resp.status_code == 200
    assert set(resp.json()["cancelled"]) == {"A", "B"}
    assert client.get("/jobs/A").status_code == 404
    assert client.get("/arenas/v7").status_code == 404
