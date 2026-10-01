# sleepbench

A stand-in experiment, shaped like a real one: instances take
minutes, the outcome mix includes genuine errors and a transient
infra failure, and the arms of one arena differ in their
scheduler knobs. Useful as an end-to-end exercise of a live
dispatcher (image pinning and shipping, source freezing,
requeues, readouts, arena aggregation, the UI) without waiting on
real compute.

It also doubles as the smallest complete answer to "how does a
research repo use this":

```
worker.py     the work — reads its payload, does the thing,
              returns JSON. Nothing dispatcher-specific beyond
              `dispatcher_sdk.run(work)`.
readouts.py   what the columns mean — one function per readout plus
              a columns(job). Registered as TEXT, so these work on
              runs whose archive predates them.
Dockerfile    the ENVIRONMENT image: interpreter + dispatcher_sdk,
              no experiment code.
submit.py     the submitter — freezes worker.py + readouts.py into
              one tar, registers the arena's readouts, and POSTs
              one job per arm with per-task payloads.
```

## Run it

```bash
./examples/sleepbench/build.sh                 # env image, on the launcher
uv run python examples/sleepbench/submit.py \
  --server http://127.0.0.1:7200 \
  --home-root <SHARED-FS>/dispatcher-demo/run-1 \
  --tasks 8 --min-seconds 60 --max-seconds 300
```

`--home-root` must be on a filesystem **every dispatch host
shares, at the same path** — the dispatcher writes
`instance.json` there and reads `outcome.json` back. A local
path (like `/home/...` on these boxes) silently gives every
host its own directory: instances would start and never be
scored. `df -T` it on two hosts before trusting it.

The env image only has to exist on the launcher; the dispatcher
pins it to its immutable id at submit and ships it to any
dispatch host that lacks it.

## What the outcome mix exercises

`submit.py` rolls a kind per task (seeded, so a re-submit with
the same seed is the same sweep):

| kind | ~share | worker does | dispatcher does |
| --- | --- | --- | --- |
| `ok` | 82% | returns a dict | `done_ok` |
| `error` | 10% | raises `ValueError` | `done_err`, no retry |
| `flaky` | 8% | raises `InfraFailure` on its FIRST instance, succeeds on the next | requeues the task, so the task ends `done_ok` with two instances in its chain |

The flaky path is the interesting one: it is how a real transient
failure (a serving backend swapped underneath a trial) is meant
to be reported — `infra=true` means "the machine failed, not the
work", and the task is rerun rather than scored.

## What the readouts show

`submit.py` registers four columns on the arena before submitting,
all out of `readouts.py` in the frozen tar:

| readout | type | aggregates to |
| --- | --- | --- |
| `reward` | float, `None` when the instance failed | a mean over successes only |
| `solved` | bool | a pass rate |
| `wall_seconds` | float | a mean |
| `kind` | str | a count, and deliberately no mean |

Two things worth noticing. `reward` returns `None` rather than
`0.0` for a failed instance — scoring a crash as zero would drag
the arm's mean down and read as a worse method instead of a broken
run. And `kind` is a string on purpose: a column whose values are
not all numbers gets `n` and nothing invented on top of it.

Each instance scores itself: `dispatcher_sdk.run` calls these four
functions right after `work` returns and leaves the values in
`readouts.json` next to the envelope. So `readout_lag` on the jobs
table should sit at **exactly 0** the whole run — a finished
instance arrives already scored, and there is no container, trigger
or timer in the path.

To see the other path, submit with `--no-readouts`, let it finish,
then register and run the command:

```bash
uv run python examples/sleepbench/submit.py … --no-readouts
# … wait for the arena to drain, then register and compute:
dispatcher readout demo/sleepbench --add reward --file examples/sleepbench/readouts.py
dispatcher readout demo/sleepbench
```

It also works for a readout that did not exist when the sweep ran —
write a new function, register it, compute. That is the whole reason
the code is registered rather than a path into the archive.

That is the retroactive path doing what it exists for — and the
same `readouts.py`, imported in a container from the job's pinned
image instead of in the worker.

## And the columns are arbitrary

`readouts.py` also has a `columns(job)` that gets the whole job as a
dataframe and returns whatever the table should show:

| column | why it could not be an enum |
| --- | --- |
| `reward_median` | a median, so one slow instance cannot move it |
| `p90_duration` | a quantile |
| `overhead_s` | arithmetic ACROSS readouts — `duration_s - wall_seconds`, the dispatcher's timing minus the worker's own account |
| `slowest_host` | a group-by, returning a string |

These update while the sweep runs, in a single resident container
shared by all three arms (`docker ps --filter
label=dispatcher.readout=aggregate` — there is exactly one). The
image installs pandas for this; the SDK itself does not need it.

## An instance sees only its own home

Making the retry succeed needs state that outlives the first
instance, and that turns out to be a lesson about the contract:
**the dispatcher bind-mounts the instance's own home and nothing
else**, at a fixed container path (`/dispatcher/home`). So the
container cannot reach its job's `home_root`, cannot see sibling
instances, and cannot see its own path on the host — a marker
written "next to my home" lands in container-local storage and
dies with the container, which would make every retry fail
identically until the requeue budget (5) ran out and the task
parked in `unknown`.

Cross-instance state therefore needs a mount the submitter
arranged. `submit.py` adds
`container.mounts: ["<home_root>:/dispatcher/job"]` and puts that
path in each payload as `job_dir`; the worker writes
`<job_dir>/.flaky-seen/<task_id>` there. If `job_dir` is missing
the worker fails loudly (`done_err`) rather than quietly burning
retries — a wrong retry budget is much harder to notice than a
stated error.
