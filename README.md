# dispatcher

Schedules containerized trials across a host fleet. Extracted
from the `agents/` experiment router and made self-contained: no
harbor, no benchmark knowledge, no repo-specific config. What a
trial computes and what its results mean stay on the research-repo
side; the dispatcher owns host selection, concurrency caps,
dispatch, failure detection, and state persistence.

## Vocabulary

- **attempt** — one submission: a task list + how to run one
  trial. Carries scheduler knobs (paused, weight, max_concurrent,
  pause_on_error, pool) and an operator alias.
- **task** — one unit of work, by name.
- **trial** — one execution of a task (`<task>__<seq>`). A task
  can have several trials (infra requeue, operator retry); each
  gets a fresh name and a fresh home dir.
- **home** — `<home_root>/<trial_name>/`, a directory on a
  filesystem every dispatch host shares. The dispatcher writes
  `trial.json` into it before dispatch and reads `outcome.json`
  out of it after.
- **worker** — the research repo's code inside the trial's main
  container (see `dispatcher_sdk`).

## What a research repo provides

1. **A container image** whose entrypoint runs one trial and, at
   the end, writes the outcome envelope and exits. Use
   `dispatcher_sdk.run(work)` — it handles the spec read, the
   atomic envelope write, and the exit-code contract.
2. **A submission** (`POST /attempts`, or
   `dispatcher_sdk.client.submit_attempt`):

   ```json
   {
     "label": "my-sweep-arm1",
     "task_list": ["task_a", "task_b"],
     "home_root": "/nfs/exp/my-sweep-arm1",
     "container": {
       "image": "myrepo-trial:abc123",
       "command": ["python", "-m", "myrepo.trial"],
       "env": {"OPENAI_API_KEY": "..."},
       "mounts": ["/nfs/datasets:/data:ro"],
       "extra_args": ["--network", "host"]
     },
     "payloads": {"task_a": {"anything": "json"}}
   }
   ```

   `home_root` must be absolute and is one-attempt-only — a
   reused home would interleave trial dirs and merge event logs,
   which is silent cross-contamination; the server 409s instead.

## The trial contract

The dispatcher starts the main container itself
(`docker run -d`) with:

- labels `dispatcher.managed=1`, `dispatcher.trial=<trial>`,
  `dispatcher.set=<trial>`, `dispatcher.attempt=<id>`. All
  docker-side identification is by label; container names are
  never used (docker/compose rewrite names — label values pass
  through verbatim).
- the trial home bind-mounted at `container.home_mount`
  (default `/dispatcher/home`), containing `trial.json`
  (`{attempt_id, task_name, trial_name, home, payload}`).
- env: `DISPATCHER_TRIAL`, `DISPATCHER_TASK`,
  `DISPATCHER_ATTEMPT`, `DISPATCHER_HOME`,
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
  "values": {"reward": 1.0},
  "data": <anything json>
}
```

Only `ok` / `error` / `infra` / `values` are contract: `values`
means surface in the monitor and the weight tuner; `data` is
opaque to the dispatcher. Exit codes: 0 ok, 1 error, 75 infra
(EX_TEMPFAIL).

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
  running; gone → ghosted). `unknown`/`ghosted` block attempt
  drain and archive — nothing is silently dropped.
- orphan container sets are removed only after: SET-label match,
  not owned by any running/unknown trial, older than the age
  floor, AND seen orphan on two consecutive sweeps.

## Run it

```
uv sync
dispatcher serve --self-host ml10 --data-dir ~/dispatcher-data \
  --host ml10=8 --host ml9=8 --max-concurrent 12 --port 7200
dispatcher monitor --server http://127.0.0.1:7200
```

All timestamps the dispatcher mints (event logs, API responses,
attempt ids) are timezone-aware KST (+09:00); external times
(docker, file mtimes) are converted at the boundary.

State: per-attempt event log at
`<home_root>/.dispatcher-state.jsonl` (replayed on restart), an
attempts index under `--data-dir`, and `settings.json` — every
`PATCH /settings` persists the whole runtime-tunable document, so
operator tuning survives restarts. `--host`/`--max-concurrent`
are required on the first boot only; afterwards the persisted
settings win and explicit flags act as overrides.
`TELEGRAM_BOT_TOKEN` in the env enables progress notifications
(configure thresholds via `PATCH /settings`).
