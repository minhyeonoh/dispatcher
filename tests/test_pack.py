"""Pack layout and the read-path resolver, plus the two read-side CLI
shaping functions.

The resolver is where "writes go to NFS, reads prefer the pack" is
enforced, so the cases that matter are the ones where it must NOT
pick a pack: no host known, no archive, archive present but nothing
mounted. Each of those has to degrade to the original tree, because
the whole design rests on a missing pack being slow rather than
wrong."""

from __future__ import annotations

import io
import json

from dispatcher.core.pack import (
  PACK_DIRNAME,
  lock_path,
  mount_dir,
  pack_path,
  packed_hosts,
  read_home_for,
)
from dispatcher.tools.pack_cli import (
  resolve_instance,
  shape_values,
)


def test_layout_sits_beside_readouts_and_is_dot_prefixed(tmp_path):
  # The home-root scan skips dot-prefixed names, which is the only
  # reason a new sidecar dir costs no change there.
  assert PACK_DIRNAME.startswith(".")
  assert pack_path(tmp_path, "ml9") == tmp_path / ".packs" / "ml9.sqfs"
  assert lock_path(tmp_path, "ml9") == tmp_path / ".packs" / "ml9.lock"


def test_packed_hosts_reads_filenames(tmp_path):
  assert packed_hosts(tmp_path) == []
  (tmp_path / PACK_DIRNAME).mkdir()
  for host in ("ml10", "ml2", "ml9"):
    pack_path(tmp_path, host).write_bytes(b"")
  (tmp_path / PACK_DIRNAME / "ml9.lock").write_bytes(b"")
  assert packed_hosts(tmp_path) == ["ml10", "ml2", "ml9"]


def test_read_home_falls_back_without_a_host(tmp_path):
  path, packed = read_home_for(tmp_path, "inst-1", job_id="job-a")
  assert path == tmp_path / "inst-1"
  assert packed is False


def test_read_home_falls_back_when_nothing_is_mounted(tmp_path):
  # An archive that exists but was never mounted here must still
  # resolve — to the slow path, not to an error.
  (tmp_path / PACK_DIRNAME).mkdir()
  pack_path(tmp_path, "ml9").write_bytes(b"not really an archive")
  path, packed = read_home_for(
    tmp_path,
    "inst-1",
    job_id="job-a",
    host="ml9",
    mount_base=tmp_path / "mounts",
  )
  assert path == tmp_path / "inst-1"
  assert packed is False


def test_read_home_prefers_a_mounted_pack(tmp_path):
  base = tmp_path / "mounts"
  home = mount_dir("job-a", "ml9", base=base) / "inst-1"
  home.mkdir(parents=True)
  path, packed = read_home_for(
    tmp_path, "inst-1", job_id="job-a", host="ml9", mount_base=base
  )
  assert path == home
  assert packed is True


def test_read_home_is_per_instance_not_per_job(tmp_path):
  # Mounting one instance must not claim its siblings: resolution
  # asks the filesystem per instance, so a partially-packed archive
  # cannot strand a reader.
  base = tmp_path / "mounts"
  (mount_dir("job-a", "ml9", base=base) / "inst-1").mkdir(parents=True)
  hit, packed_hit = read_home_for(
    tmp_path, "inst-1", job_id="job-a", host="ml9", mount_base=base
  )
  miss, packed_miss = read_home_for(
    tmp_path, "inst-2", job_id="job-a", host="ml9", mount_base=base
  )
  assert packed_hit is True
  assert hit.name == "inst-1"
  assert packed_miss is False
  assert miss == tmp_path / "inst-2"


def test_mount_dir_separates_jobs_and_hosts(tmp_path):
  a = mount_dir("job-a", "ml9", base=tmp_path)
  b = mount_dir("job-a", "ml10", base=tmp_path)
  c = mount_dir("job-b", "ml9", base=tmp_path)
  assert len({a, b, c}) == 3


# ── the CLI's pure halves ────────────────────────────────────────


def _job_body():
  """The real shape, verified against a live `GET /jobs/{id}`: buckets
  are keyed by TASK id and the instance id — the directory name under
  the home root — is a field inside the view."""
  return {
    "home_root": "/hdd/hdd2/omh/jobs/job-a",
    "done_ok": {
      "task-001": {"instance_id": "task-001__0000288", "host": "ml9"}
    },
    "done_err": {
      "task-002": {"instance_id": "task-002__0000297", "host": "ml10"}
    },
    "running": {
      "task-003": {"instance_id": "task-003__0000301", "host": "ml2"}
    },
    "unknown": {
      "task-004": {"instance_id": "task-004__0000305", "host": "ml5"}
    },
  }


def test_resolve_by_task_id_returns_the_instance_dir_name():
  # The whole point: a task id must not become a path. `task-001` is
  # not a directory; `task-001__0000288` is.
  assert resolve_instance(_job_body(), "task-001") == (
    "task-001__0000288",
    "ml9",
  )


def test_resolve_by_instance_id_too():
  assert resolve_instance(_job_body(), "task-002__0000297") == (
    "task-002__0000297",
    "ml10",
  )


def test_resolve_searches_every_bucket():
  job = _job_body()
  # Running is the mid-job case and unknown still has output worth
  # reading — neither may be skipped.
  assert resolve_instance(job, "task-003")[1] == "ml2"
  assert resolve_instance(job, "task-004")[1] == "ml5"


def test_resolve_unknown_ident_still_answers():
  # Refusing would be worse than guessing the NFS layout: the
  # directory may well be there even when the server has forgotten.
  assert resolve_instance(_job_body(), "nope") == ("nope", "")


def test_resolve_tolerates_a_view_without_a_host():
  assert resolve_instance({"done_ok": {"task-001": {}}}, "task-001") == (
    "task-001",
    "",
  )


def _readout_body():
  return {
    "values": {
      "reward": [
        {"instance_id": "i-1", "value": 1, "ok": True},
        {"instance_id": "i-2", "value": 0, "ok": True},
      ],
      "wall_seconds": [{"instance_id": "i-1", "value": 12.5, "ok": True}],
    }
  }


def test_shape_values_defaults_to_bare_values():
  shaped = shape_values(_readout_body(), [])
  assert shaped == {
    "reward": {"i-1": 1, "i-2": 0},
    "wall_seconds": {"i-1": 12.5},
  }


def test_shape_values_filters_by_name():
  shaped = shape_values(_readout_body(), ["reward"])
  assert set(shaped) == {"reward"}


def test_shape_values_full_keeps_provenance():
  shaped = shape_values(_readout_body(), ["reward"], full=True)
  assert shaped["reward"]["i-1"]["ok"] is True
  assert shaped["reward"]["i-1"]["value"] == 1


def test_shape_values_last_row_wins():
  # The value files are append-only with last-wins, and a recompute
  # appends; the shaping must not resurrect the shadowed value.
  body = {
    "values": {
      "reward": [
        {"instance_id": "i-1", "value": 0},
        {"instance_id": "i-1", "value": 1},
      ]
    }
  }
  assert shape_values(body, []) == {"reward": {"i-1": 1}}


def test_shape_values_skips_rows_without_an_instance():
  body = {
    "values": {"reward": [{"value": 1}, "junk", {"instance_id": ""}]}
  }
  assert shape_values(body, []) == {"reward": {}}


def test_shape_values_on_empty_body():
  assert shape_values({}, []) == {}


def test_dump_shape_is_json_serialisable():
  buf = io.StringIO()
  json.dump(shape_values(_readout_body(), []), buf)
  assert json.loads(buf.getvalue())["reward"]["i-1"] == 1
