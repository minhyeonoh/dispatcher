# dispatcher

Schedules containerized instances across a host fleet. Extracted
from the `agents/` experiment router and made self-contained: no
harbor, no benchmark knowledge, no repo-specific config. What a
instance computes and what its results mean stay on the research-repo
side; the dispatcher owns host selection, concurrency caps,
dispatch, failure detection, and state persistence.

## Vocabulary

- **job** — one submission: a task list + how to run one
  instance. Carries scheduler knobs (paused, weight, max_concurrent,
  pause_on_error, pool) and an operator alias.
- **arena** — the long-lived group a job belongs to (`"arena"`
  at submit, movable via PATCH). Derived state: an arena exists
  iff a job names it. Names are slash paths
  (`bench/v7/front5`) and a path addresses its SUBTREE — the
  tree is a naming convention, segment meaning is yours, the
  server never pre-defines structure. `GET /api/arenas` lists the
  paths jobs actually name, with aggregated counts;
  `GET /api/arenas/{path}` aggregates the subtree;
  `POST /api/arenas/{path}/pause|resume|reclaim` fan the per-job
  op out over subtree members; `…/cancel` refuses
  without `{"confirm": true}`, naming every job that would die.
  Submitting many jobs at once (a sweep) is a client convenience
  (`dispatcher_sdk.client.submit_jobs`), not a server concept —
  membership is by reference, whenever the job was submitted.
  Orthogonal to `pool` (capacity axis).
- **task** — one unit of work, by name.
- **instance** — one execution of a task (`<task>__<seq>`). A task
  can have several instances (infra requeue, operator retry); each
  gets a fresh name and a fresh home dir.
- **home** — `<home_root>/<instance_id>/`, a directory on a
  filesystem every dispatch host shares. The dispatcher writes
  `instance.json` into it before dispatch and reads `outcome.json`
  out of it after.
- **worker** — the research repo's code inside the instance's main
  container (see `dispatcher_sdk`).

## What a research repo provides

1. **An ENVIRONMENT image** — deps only, no experiment code.
   Rebuilt when dependencies change, not per submission (so
   hosts hold a handful of env images, not one per sweep). Bake
   `dispatcher_sdk` into it.
2. **A frozen source archive** — the experiment code as a tar,
   attached to the submission (`source_tar_b64`). The dispatcher
   stores it at `<home_root>/.source.tar` (plain shared storage,
   immune to docker prune — this file IS the arm record), mounts
   it read-only into every instance, and
   `dispatcher_sdk.bootstrap` unpacks it to container-local disk
   before exec — one sequential read per instance, no per-file
   NFS traffic. Worker code uses `dispatcher_sdk.run(work)`.
3. **A submission** (`POST /api/jobs`, or
   `dispatcher_sdk.client.submit_job`):

   ```json
   {
     "label": "my-sweep-arm1",
     "arena": "myrepo/my-sweep",
     "task_ids": ["task_a", "task_b"],
     "home_root": "/nfs/exp/my-sweep-arm1",
     "source_tar_b64": "<base64 tar of the frozen code>",
     "container": {
       "image": "myrepo-env:deps-hash",
       "command": ["python", "-m", "dispatcher_sdk.bootstrap",
                   "--", "python", "-m", "myrepo.worker"],
       "env": {"OPENAI_API_KEY": "..."},
       "mounts": ["/nfs/datasets:/data:ro"],
       "extra_args": ["--network", "host"]
     },
     "payloads": {"task_a": {"anything": "json"}}
   }
   ```

   `home_root` must be absolute and is one-job-only — a
   reused home would interleave instance dirs and merge event logs,
   which is silent cross-contamination; the server 409s instead.
   The image must exist on the launcher at submit (400
   otherwise); it is resolved to its immutable ID there, every
   instance runs that exact ID (a tag re-pushed mid-sweep changes
   nothing), and the dispatcher ships the image to any dispatch
   host that lacks it. `settings.require_source` makes the
   source archive mandatory.

## The instance contract

The dispatcher starts the main container itself
(`docker run -d`) with:

- labels `dispatcher.managed=1`, `dispatcher.instance=<instance>`,
  `dispatcher.set=<instance>`, `dispatcher.job=<id>`. All
  docker-side identification is by label; container names are
  never used (docker/compose rewrite names — label values pass
  through verbatim).
- the instance home bind-mounted at `container.home_mount`
  (default `/dispatcher/home`), containing `instance.json`
  (`{job_id, task_id, instance_id, home, payload}`).
- env: `DISPATCHER_INSTANCE`, `DISPATCHER_TASK`,
  `DISPATCHER_JOB`, `DISPATCHER_HOME`,
  `DISPATCHER_SET_LABEL`.

The worker may start sibling containers; **every sibling must
carry the label in `$DISPATCHER_SET_LABEL`** or cleanup cannot
see it and it leaks.

Before exiting, the worker writes `<home>/outcome.json`:

```json
{
  "ok": true,
  "error": null | {"type": "...", "message": "...", "exit_code": 1},
  "infra": false,
  "data": <anything json>
}
```

Only `ok` / `error` / `infra` are contract — the dispatcher's
entire understanding of a result. `data` is opaque passthrough:
metrics (reward, whatever) are the research repo's business,
computed later from the stored envelopes, so put everything a
future readout might need in `data`. Exit codes: 0 ok, 1 error,
75 infra (EX_TEMPFAIL).

## Failure semantics

- envelope present → scored by the envelope alone. The
  container's exit code is bookkeeping once a clean envelope
  exists (sibling teardown can SIGKILL long after the work
  finished).
- `infra: true`, or no envelope + main-container exit in
  {75, 137, 143, 255} once the container is confirmed gone →
  the task is **requeued** (bounded, 5 per task), never scored.
- no envelope, container died → `unknown`; a resolver
  reclassifies on evidence (late file → done; container alive →
  running; gone → ghosted). `unknown`/`ghosted` block job
  drain and archive — nothing is silently dropped.
- orphan container sets are removed only after: SET-label match,
  not owned by any running/unknown instance, older than the age
  floor, AND seen orphan on two consecutive sweeps.

## Run it

```
uv sync
dispatcher serve --self-host ml10 --data-dir ~/dispatcher-data \
  --host ml10=8 --host ml9=8 --max-concurrent 12 --port 7200 \
  --ui-dist ui/apps/monitor/dist
dispatcher monitor --server http://127.0.0.1:7200
```

## URL space

Pages and JSON are siblings: the web UI owns the root paths a
human types, and the whole JSON surface — docs included — lives
under `/api`, so `/arenas/bench/v7` is the page and
`/api/arenas/bench/v7` is its data.

```
/                                overview: what needs attention
/jobs                            every job
/jobs/{job_id}                   job detail (an alias in the url
                                 redirects to its job_id — aliases
                                 are renameable, ids are not)
/jobs/{job_id}/instances/{id}    one instance
/arenas/{path}                   arena subtree
/hosts  ·  /settings             fleet · the settings document
/api/**                          JSON  ·  /api/docs = OpenAPI UI
```

`--ui-dist` mounts the built SPA at `/` (unmatched page paths fall
back to it, so deep links survive a refresh; `/api/*` never
does). Without the flag the server is API-only. See
`ui/README.md` to build the UI.

All timestamps the dispatcher mints (event logs, API responses,
job ids) are timezone-aware KST (+09:00); external times
(docker, file mtimes) are converted at the boundary.

State: per-job event log at
`<home_root>/.dispatcher-state.jsonl` (replayed on restart), an
jobs index under `--data-dir`, and `settings.json` — every
`PATCH /api/settings` persists the whole runtime-tunable document,
so operator tuning survives restarts. `--host`/`--max-concurrent`
are required on the first boot only; afterwards the persisted
settings win and explicit flags act as overrides.
`TELEGRAM_BOT_TOKEN` in the env enables progress notifications
(configure thresholds via `PATCH /api/settings`).
