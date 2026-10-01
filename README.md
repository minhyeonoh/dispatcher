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
  (`{job_id, task_id, instance_id, home, payload, readouts}`).
- env: `DISPATCHER_INSTANCE`, `DISPATCHER_TASK`,
  `DISPATCHER_JOB`, `DISPATCHER_HOME`,
  `DISPATCHER_SET_LABEL`.

The worker may start sibling containers; **every sibling must
carry the label in `$DISPATCHER_SET_LABEL`** or cleanup cannot
see it and it leaks.

Before exiting, the worker writes `<home>/readouts.json` (see
Readouts) and then `<home>/outcome.json`:

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
derived later by a **readout** (below), so put everything a future
readout might need in `data`. Exit codes: 0 ok, 1 error, 75 infra
(EX_TEMPFAIL).

## Readouts

The dispatcher cannot know what `reward` or `tgc` means — every
benchmark computes it from its own envelope. So the computation
stays on the research-repo side and the dispatcher only runs it:

```python
# myrepo/readouts.py
def reward(instance):
  return instance.data["reward"]        # instance.outcome["data"]
```

Register it on an arena, and it becomes a column:

```
dispatcher readout myrepo/my-sweep --add reward --file myrepo/readouts.py
```

**What is registered is the CODE, not a path to it.** The file stays
in your repo under version control; the CLI ships its text. That is
what makes a readout written today computable against a run from two
months ago — an entrypoint string would have to resolve inside each
job's frozen archive, which for a finished sweep it never will. Heavy
logic can still live in your repo: the source executes with the job's
archive importable, so `from myrepo.grade import tgc` works and only
the glue is registered.

- **Registration is per arena**, and a path covers its subtree —
  the jobs you compare are the jobs scored the same way.
  Jobs with no arena get none.
- **Registration validates the source**: it must parse and must
  define `def <name>(…)`. A typo is a 400 now rather than an error
  record on every instance hours later.
- **The name is the identity.** A changed computation is a
  different readout (`reward-v1` / `reward-v2`), never a new
  version of the same one: a column that silently changes meaning
  invalidates every figure already drawn from it. Registering a
  name that already exists elsewhere on the same arena path is a
  409, not an overwrite.
- **One value per instance** is a readout's whole contract. With no
  `columns` function (below) the table rolls those values up itself:
  numbers get a mean, bools get a rate (`True` counts 1), a column
  with a string in it gets a count and nothing invented on top. In
  every case `None` means "not applicable here" and is excluded from
  the mean rather than averaged as zero — the difference between
  reporting a broken run and reporting a worse method.

### Where it runs

**The instance's own worker process.** `dispatcher_sdk.run(work)`
calls the registered readouts right after `work` returns, on the
envelope it is about to write, and leaves them in
`<home>/readouts.json` — **before** `outcome.json`, because the
envelope is the dispatcher's completion signal and anything the
instance wants read must already be on disk when it lands. The
dispatcher picks the values up in the same look that detects
completion and appends them to the job's column index.

So the normal path has no second container, no trigger, no timer,
and no delay — a finished instance arrives already scored. The
dispatcher never runs readout code; it tells the instance what to
run (via `instance.json`) and files the answer. That is the same
shape Harbor's verifier and Braintrust's `Eval()` settled on: the
thing doing the work scores itself.

Two consequences worth stating. A readout's cost is part of the
instance's wall clock and holds a dispatch slot — accepted
deliberately, with a generous per-readout timeout (600s default)
rather than a tight fence. And a readout **cannot** change the
envelope: it runs after the outcome is decided, every exception is
caught, and a scoring bug can never turn finished work into failed
work. A raising readout is recorded as an error against that one
instance and never retried, since the envelope is immutable and the
same code would raise forever.

### The retroactive path is a command

```
dispatcher readout <arena|job_id> [--name reward]
```

For the three cases the live path cannot cover: a readout
registered after a run finished, a redefinition, an instance that
died before scoring itself. It starts a container from the job's
**pinned image** on the launcher (job home mounted read-only,
`container.extra_args` deliberately not applied) running
`python -m dispatcher_sdk.readout`, which calls the same functions
on the same object the worker would have — so a readout cannot
behave differently depending on which path ran it.

It is a command and not a background loop on purpose: it starts
containers, and that is an operator's decision to make and watch.

The containers the dispatcher starts for its OWN purposes (this one
and the column process) get `dispatcher_sdk` **mounted from the
launcher**, ahead of whatever the image baked. `image_id` is pinned
so the work is reproducible, which is right — but these containers
do not run the work, they run the dispatcher's side of a protocol it
also implements. Without the mount, a job pinned to a months-old
image could never be read by a newer dispatcher.
Progress streams one line per pass; it is safe to interrupt and
safe to re-run, because only missing pairs are ever computed.
`readout_lag` on every job row is what tells you to run it — 0 in
steady state, so anything above 0 is actionable rather than
transient.

Values live in `<home_root>/.readouts/<name>.jsonl`, append-only,
last line winning, written by the dispatcher alone. Both paths write
the same format, and each line records what produced it: the
registered function's own hash (`readout_sha256`) plus the archive
and image. The functions themselves are kept beside the values in
`<home_root>/.readouts/sources/<name>.<hash>.py` — the registry under
`--data-dir` is the authority, this is the durable copy on shared
storage, the same relationship `.source.tar` has with git.

One subtlety worth knowing: a readout backfilled against an old job
resolves its imports against **that job's** frozen archive, not
today's code. Usually that is what you want (you grade the old run
with the code it ran) and occasionally it is a surprise.

### Columns are yours too

A readout produces one value per instance. What the table SHOWS is a
second question, and the answer is another function of yours —
registered once per arena, handed the job as a frame, returning the
column dict:

```python
def columns(job):
  df = job.df                                  # pandas, if you want it
  ok = df[df.state == "done_ok"]
  return {
    "reward_median": ok.reward.median(),
    "p90_duration":  df.duration_s.quantile(0.9),
    "overhead_s":    (df.duration_s - df.wall_seconds).median(),
    "slowest_host":  df.groupby("host").duration_s.mean().idxmax(),
  }
```

A `columns` registration must also say what its columns MEAN, in the
same file:

```python
def column_descriptions():
  return {"reward_median": "median reward over successful instances"}
```

```
dispatcher readout myrepo/my-sweep --columns --file myrepo/readouts.py
```

Both functions are required and checked at registration — a 400, not a
column nobody can explain. Descriptions live beside the function that
names the keys rather than in the request, which would be a second copy
of the same key list and the first thing to drift; being in the
registered text they also land in the copy kept next to the values, so
a column explains itself years later. The UI shows them in its column
picker. A key with a value but no description still renders its number
— the numbers are the valuable half — with `columns_error` naming it
and the picker marking it undocumented.

Its keys are the columns; nothing is declared in advance. So the
dispatcher never learns what a median is, and the number in the
table is produced by the same code as the number in your paper.

The frame is one row per finished instance: `instance_id`,
`task_id`, `state`, `host`, `dispatched_at`, `finished_at`,
`duration_s`, and one column per readout. `job.df` imports pandas
lazily — `job.columns` (dict of lists) and `job.records` are always
there, so an image without pandas still works and
`pl.DataFrame(job.columns)` covers polars. A readout that returned
`None` and one that raised are both `None` in the frame; `job.errors`
keeps that distinction for the rare column that wants it. There is
no filesystem: one process serves every job sharing an image and
source, so artifact reading belongs in a per-instance readout.

### Inheritance: readouts add up, columns is replaced

Both are registered on an arena path and both reach the subtree, but
they combine differently, because they are different kinds of thing.

A readout is **one named column of per-instance values** — an item.
Two of them are independent, so they accumulate down the path:

```
appworld          readouts: [reward]
appworld/v7       readouts: [v7_probe]
→ a job in appworld/v7 computes BOTH
```

If readouts overrode instead, registering `v7_probe` on the subtree
would silently delete `reward` from those jobs.

`columns` is **one function returning the whole column set** — an
answer, not an item. Two of them cannot both hold, so the nearest
one wins:

```
appworld          columns: def columns(job): return {"tgc": …}
appworld/v7       columns: def columns(job): return {"other": …}
→ a job in appworld/v7 gets ONLY {"other": …}
→ a job in appworld/v1 gets {"tgc": …}
```

Merging the two dicts instead would let both emit `tgc` from
different code — the same "one name, two computations" collision a
readout registration refuses outright, except silent.

**So: define `columns` once at the comparison root.** With `tgc` on
`appworld`, every version arm inherits it and the jobs table can put
them under one sortable header, which is the whole point of the arena
being the comparison unit.

**And when one arm needs an extra number, add a READOUT, not an
override.** Readouts accumulate, so it exists only there; the shared
`columns` function emits it where it finds it:

```python
# registered on appworld/v7 only
def v7_probe(instance):
  return float(instance.data["duration_s"])

# the ONE function on appworld, used by every arm
def columns(job):
  xs = [v for v in job.columns.get("reward", []) if v is not None]
  out = {"tgc": sum(xs) / len(xs) if xs else None}
  if "v7_probe" in job.columns:        # only v7 has it
    out["v7_probe_max"] = max(v for v in job.columns["v7_probe"] if v)
  return out
```

Overriding `columns` on `appworld/v7` to add one key would cost the
table its shared `tgc` column — a different function is a different
header, even when it computes the same thing. The pattern above keeps
one function, so `tgc` stays comparable across every arm and only v7
carries the extra. That asymmetry is the point of it: a subtree can
add **data** freely, while the **view** stays one definition.

**It runs in a resident process**, one per `(image, source)`, started
on the first read that needs it. A round trip is ~3ms for a
1344-instance job, so the trigger is simply "a read, when values
changed since the last one" — no timer, no background loop, no
queue, and no computation at all for a job nobody is looking at. The
invalidation is one line in the one function that appends values.

Failures never invent a number: a timeout (`readouts.
aggregate_timeout_sec`, default 5s) or a raising function leaves the
last good columns in place with `columns_stale` set and
`columns_error` saying why, and the next read retries. The pool is
capped (`readouts.max_aggregate_processes`) and evicts least-recently
used, so months of submissions cannot accumulate processes.

```
GET    /api/readouts                    what is registered where
POST   /api/readouts                    register (no containers; the
                                        reply names jobs needing the
                                        retroactive command)
DELETE /api/readouts?arena=..&name=..   stop computing (values stay)
PUT    /api/readouts/columns            the arena's column function
                                        (body: {arena, source})
GET    /api/jobs/{id}/readouts          every value, per instance
POST   /api/readouts/compute            the retroactive pass, as an
                                        ndjson stream (the CLI's
                                        transport)
```

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
