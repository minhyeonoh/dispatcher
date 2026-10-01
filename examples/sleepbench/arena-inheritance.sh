#!/usr/bin/env bash
# The readouts-accumulate / columns-is-replaced pattern, end to end.
#
# A comparison root (`appworld`) defines the metric once; version arms
# inherit it, so the jobs table puts them under one sortable header.
# One arm then needs an extra number, which it adds as a READOUT
# rather than by overriding `columns` — overriding would give that
# arm a different column function, and a different function cannot
# share a header with the others even when it computes the same thing.
#
# Run after ./build.sh, against a dispatcher with a shared home root:
#   examples/sleepbench/arena-inheritance.sh <SHARED-FS>/aw-demo
set -euo pipefail

SERVER=${SERVER:-http://127.0.0.1:7200}
HOME_ROOT=${1:?usage: arena-inheritance.sh <SHARED-HOME-ROOT>}
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

cat > "$WORK/shared.py" <<'PY'
def reward(instance):
  if instance.state != "done_ok":
    return None
  return float(instance.data["reward"])


def columns(job):
  """The ONE column function, on the comparison root. Every arm uses
  it, so `tgc` is one header across all of them."""
  xs = [v for v in job.columns.get("reward", []) if v is not None]
  out = {"tgc": sum(xs) / len(xs) if xs else None}
  # Emitted only where the readout exists — which is how an arm adds
  # a column without the table losing the shared ones.
  probes = [v for v in job.columns.get("v7_probe", []) if v is not None]
  if probes:
    out["v7_probe_max"] = max(probes)
  return out
PY

cat > "$WORK/v7.py" <<'PY'
def v7_probe(instance):
  """A readout on appworld/v7 only. Readouts ACCUMULATE, so v7 jobs
  get this on top of the inherited `reward`; v1 and v2 never see it."""
  if instance.state != "done_ok":
    return None
  return float(instance.data["duration_s"])
PY

echo "1. the comparison root defines the metric once"
dispatcher readout appworld --server "$SERVER" --add reward --file "$WORK/shared.py"
dispatcher readout appworld --server "$SERVER" --columns --file "$WORK/shared.py"

echo
echo "2. three version arms, each a few short tasks"
for v in v1 v2 v7; do
  uv run python "$HERE/submit.py" --server "$SERVER" \
    --home-root "$HOME_ROOT/$v" --arena "appworld/$v" \
    --tasks 3 --min-seconds 4 --max-seconds 9 --p-flaky 0 \
    --no-readouts | tail -1
done

echo
echo "3. one arm adds DATA (a readout), not a different view"
dispatcher readout appworld/v7 --server "$SERVER" --add v7_probe --file "$WORK/v7.py"

cat <<'TXT'

Once the arms drain:
  - every job shares one columns function, so `tgc` is one sortable
    header across v1, v2 and v7
  - only v7 rows carry `v7_probe_max`; the others show nothing there,
    and the reason is a line in a function you wrote
  - `dispatcher readout appworld/v7` fills in v7_probe for instances
    that finished before it was registered

Compare with overriding instead:
  dispatcher readout appworld/v7 --columns --file <something-else>
and the table loses its shared `tgc` column — a different function is
a different header.
TXT
