"""Submit the sleepbench demo: one arena, several arms.

Shows the whole submitter side of the contract:

  build the env image  →  freeze the worker into a tar  →
  POST one job per arm, each with per-task payloads

Run (the env image must exist on the launcher):

    docker build -t sleepbench-env:1 \
      -f examples/sleepbench/Dockerfile \
      --build-context . examples/sleepbench   # see build.sh
    uv run python examples/sleepbench/submit.py \
      --server http://127.0.0.1:7200 \
      --home-root /hdd/hdd2/omh/dispatcher-demo/<run-id>
"""

from __future__ import annotations

import argparse
import base64
import io
import random
import sys
import tarfile
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from dispatcher_sdk import client

HERE = Path(__file__).resolve().parent

# Arms of one sweep. Same tasks, different knobs — the shape a
# real comparison takes.
ARMS = [
  {"label": "sleepbench-baseline", "weight": 1, "max_concurrent": None},
  {"label": "sleepbench-variant", "weight": 1, "max_concurrent": None},
  # A throttled arm: proves the per-job cap is independent of the
  # host/pool caps above it.
  {"label": "sleepbench-throttled", "weight": 1, "max_concurrent": 2},
]

# The dispatcher bind-mounts only the INSTANCE's own home (at
# `container.home_mount`). Anything an instance must share with
# the task's later instances — a retry marker here — needs its own
# mount, so the job's home_root goes in at this path and the
# payload tells the worker where to find it.
JOB_DIR_MOUNT = "/dispatcher/job"


def freeze_worker() -> str:
  """The frozen source archive: this is both the code delivery and
  the arm record (it outlives any docker prune, on plain shared
  storage next to the results)."""
  buf = io.BytesIO()
  with tarfile.open(fileobj=buf, mode="w") as tf:
    tf.add(HERE / "worker.py", arcname="worker.py")
  return base64.b64encode(buf.getvalue()).decode()


def make_payloads(
  n_tasks: int,
  seed: int,
  *,
  min_s: float,
  max_s: float,
  p_error: float,
  p_flaky: float,
) -> dict[str, dict[str, Any]]:
  """Per-task duration and outcome kind, decided at submit time so
  the intent is in the record, not only in the worker's head."""
  rng = random.Random(seed)
  payloads: dict[str, dict[str, Any]] = {}
  for i in range(n_tasks):
    task_id = f"task-{i:03d}"
    roll = rng.random()
    if roll < p_error:
      kind = "error"
    elif roll < p_error + p_flaky:
      kind = "flaky"
    else:
      kind = "ok"
    payloads[task_id] = {
      "duration_s": round(rng.uniform(min_s, max_s), 1),
      "kind": kind,
      "reward": round(rng.betavariate(5, 3), 4),
      "seed": seed + i,
      "job_dir": JOB_DIR_MOUNT,
    }
  return payloads


def main(argv: list[str] | None = None) -> int:
  ap = argparse.ArgumentParser()
  ap.add_argument("--server", default="http://127.0.0.1:7200")
  ap.add_argument(
    "--home-root",
    required=True,
    help="base dir on the SHARED filesystem; one subdir per arm",
  )
  ap.add_argument("--image", default="sleepbench-env:1")
  ap.add_argument("--arena", default="demo/sleepbench")
  ap.add_argument("--tasks", type=int, default=8)
  ap.add_argument("--min-seconds", type=float, default=60.0)
  ap.add_argument("--max-seconds", type=float, default=300.0)
  ap.add_argument("--seed", type=int, default=7)
  ap.add_argument("--p-error", type=float, default=0.10)
  ap.add_argument("--p-flaky", type=float, default=0.08)
  ap.add_argument(
    "--paused",
    action="store_true",
    help="submit without dispatching (resume from the UI)",
  )
  args = ap.parse_args(argv)

  source = freeze_worker()
  base = Path(args.home_root)
  submitted: list[dict[str, Any]] = []

  for i, arm in enumerate(ARMS):
    payloads = make_payloads(
      args.tasks,
      args.seed + i * 1000,
      min_s=args.min_seconds,
      max_s=args.max_seconds,
      p_error=args.p_error,
      p_flaky=args.p_flaky,
    )
    home_root = base / str(arm["label"])
    payload: dict[str, Any] = {
      "label": arm["label"],
      "arena": args.arena,
      "task_ids": sorted(payloads),
      # One home_root per job, ever — the server 409s a reuse.
      "home_root": str(home_root),
      "source_tar_b64": source,
      "container": {
        "image": args.image,
        "command": [
          "python",
          "-m",
          "dispatcher_sdk.bootstrap",
          "--",
          "python",
          "-m",
          "worker",
        ],
        "mounts": [f"{home_root}:{JOB_DIR_MOUNT}"],
      },
      "payloads": payloads,
      "weight": arm["weight"],
      "paused": args.paused,
    }
    if arm["max_concurrent"] is not None:
      payload["max_concurrent"] = arm["max_concurrent"]
    result = client.submit_job(args.server, payload)
    kinds = sorted({p["kind"] for p in payloads.values()})
    total_s = sum(p["duration_s"] for p in payloads.values())
    print(
      f"{result['alias']:>24}  {arm['label']:<22} "
      f"{len(payloads)} tasks  ~{total_s / 60:.1f} min of work  "
      f"kinds={','.join(kinds)}"
    )
    submitted.append(result)

  print(f"\narena: {args.arena}  ·  {len(submitted)} jobs")
  return 0


if __name__ == "__main__":
  sys.exit(main())
