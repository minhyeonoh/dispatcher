# sleepbench

A stand-in experiment, shaped like a real one: instances take
minutes, the outcome mix includes genuine errors and a transient
infra failure, and the arms of one arena differ in their
scheduler knobs. Useful as an end-to-end exercise of a live
dispatcher (image pinning and shipping, source freezing,
requeues, arena aggregation, the UI) without waiting on real
compute.

It also doubles as the smallest complete answer to "how does a
research repo use this":

```
worker.py     the work — reads its payload, does the thing,
              returns JSON. Nothing dispatcher-specific beyond
              `dispatcher_sdk.run(work)`.
Dockerfile    the ENVIRONMENT image: interpreter + dispatcher_sdk,
              no experiment code.
submit.py     the submitter — freezes worker.py into a tar and
              POSTs one job per arm with per-task payloads.
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
work", and the task is rerun rather than scored. The worker keeps
a marker per task under `<home_root>/.flaky-seen/` so the retry
knows to succeed.
