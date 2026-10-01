"""Readouts: operator code that turns a finished instance into
values the dispatcher serves as columns.

The dispatcher cannot know what `reward` or `tgc` means — every
benchmark computes it from its own envelope. So the computation is
the research repo's: a named function of one finished instance,
registered on an arena.

**Where it runs is the whole design.** The instance's own worker
process runs it, right after the work finishes, inside the
container that already has every dependency loaded. So a value is
on disk before the outcome envelope is, and the dispatcher reads it
in the same look that detects completion — no second container, no
trigger, no delay. That is the same shape Harbor's verifier and
Braintrust's `Eval()` settled on: the thing doing the work scores
itself.

Only the **retroactive** path needs machinery, and it is a separate
operator command (`dispatcher readout`): a readout registered after
a run finished, a redefined one, or an instance that died before it
could score itself. That path starts a container from the job's
pinned image and computes the missing pairs.

**What is registered is the CODE, not a pointer to it.** An
entrypoint string would have to resolve inside each job's frozen
archive, which makes a readout registered today uncomputable against
a run from two months ago — the exact thing a post-hoc metric is
for. The source travels into the instance (`instance.json`) and into
the aggregate process (with each request) instead, with the job's
archive still importable so heavy logic can stay in the repo.

Identity is the NAME, alone. A changed computation is a different
readout (`reward-v1` / `reward-v2`), never a new version of the
same one: a column that silently changes meaning invalidates every
figure already drawn from it. Each value records the exact function
that produced it (`readout_sha256`) plus the archive and image, so
"which code made this number" is answerable without trusting the
name.

Files, per job:
  <home_root>/.readouts/<name>.jsonl   append-only column index,
                                       one ReadoutValue per line,
                                       last line wins
  <home_root>/.readouts/request.json   what the retroactive pass
                                       was asked to do
  <home_root>/.readouts/sources/       the registered functions, by
                                       hash — the durable record of
                                       what computed these values
and per instance:
  <home>/readouts.json                 what the worker scored
                                       itself, written BEFORE the
                                       outcome envelope
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PositiveFloat

READOUT_DIRNAME = ".readouts"
REQUEST_FILENAME = "request.json"
INSTANCE_READOUTS_FILENAME = "readouts.json"

# The retroactive runner marks its result lines with this prefix so
# the operator's own prints can share stdout without corrupting the
# protocol. U+001F (unit separator) does not occur in ordinary
# program output.
RESULT_PREFIX = "\x1fdispatcher-readout\x1f"

# The name becomes a filename and a wire key, so keep it boring.
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
SOURCES_DIRNAME = "sources"
COLUMNS_NAME = "columns"
"""The function a `columns` registration must define."""


class BadReadout(ValueError):
  """Malformed registration — bad name, or source that does not
  parse or does not define the function it was registered as."""


def check_source(source: str, want: str) -> str:
  """Validate registered code and return its sha256.

  Checked at registration rather than at first use: a typo that only
  surfaces when an instance finishes would be found by the operator
  hours later, with a column of error records to clean up."""
  if not source.strip():
    raise BadReadout("source is empty")
  try:
    tree = ast.parse(source)
  except SyntaxError as exc:
    raise BadReadout(f"source does not parse: {exc}") from exc
  names = [
    node.name
    for node in tree.body
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
  ]
  if want not in names:
    raise BadReadout(
      f"source must define `def {want}(...)` at the top level; "
      f"found {names or 'no functions'}. Helpers alongside it are "
      f"fine, and so is importing from your own frozen source"
    )
  return hashlib.sha256(source.encode("utf-8")).hexdigest()


class ReadoutSpec(BaseModel):
  """One registration: the CODE, not a pointer to it.

  Travels into the instance via `instance.json` and into the
  aggregate process with each request, so nothing has to be
  importable at a particular path and nothing has to have been in a
  job's frozen archive. That is what makes a readout registered
  today computable against a run from two months ago — the whole
  point of a post-hoc metric, and something an entrypoint string
  could not do."""

  model_config = ConfigDict(extra="forbid")

  name: str
  source: str
  """Python defining `def <name>(instance)`. Executed where the
  research code already lives (the worker's process, or the
  aggregate container), with the job's frozen archive importable —
  so heavy logic stays in your repo and only the glue is here."""

  timeout_sec: PositiveFloat = 600.0
  """Per-instance wall clock. Deliberately generous: the readout
  runs inside the instance, so its cost is accepted as part of the
  work rather than fenced off. A readout that parses a large trace
  is a legitimate readout; one that hangs forever is not."""

  @property
  def source_sha256(self) -> str:
    return hashlib.sha256(self.source.encode("utf-8")).hexdigest()

  def validated(self) -> ReadoutSpec:
    if not _NAME_RE.match(self.name):
      raise BadReadout(
        f"readout name {self.name!r} must match "
        f"[A-Za-z0-9][A-Za-z0-9._-]{{0,63}} — it is also a filename"
      )
    check_source(self.source, self.name)
    return self


class ReadoutValue(BaseModel):
  """One line of `<name>.jsonl`, and one entry of a worker's
  `readouts.json`.

  `ok=False` records a readout that RAISED on this instance. It
  still counts as scored: the envelope is immutable and the code is
  the same, so re-running it produces the same exception forever.
  An instance that produced no entry at all is what the retroactive
  path exists for."""

  name: str = ""
  """Redundant with the filename and kept anyway: these files
  outlive the dispatcher that wrote them, and a line that names its
  own readout can be read years later without the directory."""

  instance_id: str
  task_id: str = ""
  ok: bool = True
  value: Any = None
  error: str = ""
  at: datetime | None = None
  """When the DISPATCHER recorded it. Stamped on the way to disk,
  not by the worker: a missing field must not make the line
  unparseable, and the worker's clock is not ours."""

  source_sha256: str = ""
  image_id: str = ""
  readout_sha256: str = ""
  """What computed it. The name is the column's identity; these pin
  the exact code, so a number can be traced back without trusting
  that the name was never reused. `readout_sha256` is the registered
  function itself — finer than the archive hash, which moves with any
  unrelated commit."""


class ReadoutAggregate(BaseModel):
  """Job-level roll-up of one readout's values."""

  n: int = 0
  """Instances with a value of any kind, errors and nulls included."""

  errors: int = 0
  """The readout raised on this instance."""

  nulls: int = 0
  """The readout returned None — "not applicable here". Excluded
  from the mean rather than counted as zero: a failed instance
  scored 0.0 would drag an arm's average down and read as a worse
  method instead of a broken run."""

  mean: float | None = None
  """Present only when EVERY non-null value is a number (bools count
  0/1, so a pass/fail readout means its pass rate). A column with a
  string in it gets no mean — averaging a mixed column would invent
  a statistic."""


class ReadoutJobSummary(BaseModel):
  """What a job row carries about its readouts.

  `lag` = finished (instance, readout) pairs with no value. In the
  live path it is 0 by construction — the worker scores itself, so a
  finished instance arrives already scored. Anything above 0 is
  therefore actionable rather than transient: an instance that died
  before scoring, or a readout registered after the fact. It is the
  number that says "run `dispatcher readout`".

  `None` = values for this job are not loaded, so the honest answer
  is "unknown", not 0."""

  aggregates: dict[str, ReadoutAggregate] = Field(default_factory=dict)
  lag: int | None = None

  columns: dict[str, Any] = Field(default_factory=dict)
  """What the arena's `columns` function returned — arbitrary
  operator-defined numbers, recomputed whenever values change. Empty
  when the arena registers no such function, in which case the table
  falls back to `aggregates`."""

  columns_stale: bool = False
  """Values changed and the columns have not caught up yet (or the
  last attempt failed). Shown rather than hidden: a silently stale
  number is the one failure mode that reaches a figure."""

  columns_error: str = ""
  """Why the last attempt failed, if it did."""


def aggregate(values: dict[str, ReadoutValue]) -> ReadoutAggregate:
  numbers: list[float] = []
  errors = 0
  nulls = 0
  for v in values.values():
    if not v.ok:
      errors += 1
    elif v.value is None:
      nulls += 1
    elif isinstance(v.value, (bool, int, float)):
      # bool is an int subclass and that is the useful reading here.
      numbers.append(float(v.value))
  # Anything left over is non-numeric; the mean is withheld unless
  # every value that had something to say was a number.
  applicable = len(values) - errors - nulls
  mean = (
    sum(numbers) / len(numbers)
    if numbers and len(numbers) == applicable
    else None
  )
  return ReadoutAggregate(
    n=len(values), errors=errors, nulls=nulls, mean=mean
  )


# ── per-instance file (the live path) ────────────────────────────


def instance_readouts_path(instance_home: Path) -> Path:
  return instance_home / INSTANCE_READOUTS_FILENAME


def read_instance_readouts(instance_home: Path) -> list[ReadoutValue]:
  """What the worker scored itself.

  Absent or malformed is simply "no values" — never an error. The
  outcome envelope is the dispatcher's contract; this file is a
  courtesy the worker paid, and a broken one must not change how the
  instance is classified. The retroactive path fills the gap."""
  try:
    raw = json.loads(
      instance_readouts_path(instance_home).read_text(encoding="utf-8")
    )
  except (OSError, json.JSONDecodeError):
    return []
  if not isinstance(raw, list):
    return []
  out: list[ReadoutValue] = []
  for row in raw:
    try:
      out.append(ReadoutValue.model_validate(row))
    except Exception:
      continue
  return out


# ── job-level column index ───────────────────────────────────────


def readout_dir(home_root: Path) -> Path:
  return home_root / READOUT_DIRNAME


def values_path(home_root: Path, name: str) -> Path:
  return readout_dir(home_root) / f"{name}.jsonl"


def request_path(home_root: Path) -> Path:
  return readout_dir(home_root) / REQUEST_FILENAME


def source_path(home_root: Path, name: str, sha256: str) -> Path:
  """Where a registered function is kept as a RECORD, beside the
  values it produced.

  The registry under `--data-dir` is the authority; this is the
  durable copy on shared storage, the same relationship `.source.tar`
  has with git. The hash is in the filename, so a redefinition adds
  a file instead of overwriting one — the code that made an old
  number stays readable."""
  return (
    readout_dir(home_root) / SOURCES_DIRNAME / f"{name}.{sha256[:12]}.py"
  )


def record_source(home_root: Path, name: str, source: str) -> Path:
  path = source_path(
    home_root, name, hashlib.sha256(source.encode("utf-8")).hexdigest()
  )
  if not path.exists():
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
  return path


def read_values(home_root: Path, name: str) -> dict[str, ReadoutValue]:
  """`{instance_id: value}`, last line winning.

  Append-only with last-wins is what makes a recompute possible
  without rewriting history: the old value stays in the file as a
  record, the new one shadows it. Unparseable lines are skipped —
  the file is read far more often than written, and one torn tail
  must not hide a thousand good values."""
  path = values_path(home_root, name)
  out: dict[str, ReadoutValue] = {}
  try:
    text = path.read_text(encoding="utf-8")
  except OSError:
    return out
  for line in text.splitlines():
    if not line.strip():
      continue
    try:
      value = ReadoutValue.model_validate_json(line)
    except Exception:
      continue
    out[value.instance_id] = value
  return out


def append_values(
  home_root: Path, name: str, values: list[ReadoutValue]
) -> None:
  """One open, one write — the whole batch lands as a single append
  so a crash cannot interleave it. Only the DISPATCHER writes these
  files (workers write their own instance home; the retroactive
  container gets the job home read-only), so there is exactly one
  writer per path and no lock is needed."""
  if not values:
    return
  path = values_path(home_root, name)
  path.parent.mkdir(parents=True, exist_ok=True)
  blob = "".join(v.model_dump_json() + "\n" for v in values)
  with path.open("a", encoding="utf-8") as f:
    f.write(blob)


def write_request(home_root: Path, request: dict[str, Any]) -> Path:
  """Stage what the next retroactive pass should do. Read back
  through the job home's read-only mount."""
  path = request_path(home_root)
  path.parent.mkdir(parents=True, exist_ok=True)
  tmp = path.with_suffix(".json.tmp")
  tmp.write_text(json.dumps(request, indent=2, default=str), "utf-8")
  tmp.replace(path)
  return path


# The dispatcher's OWN protocol code, mounted from the launcher.
#
# `image_id` is pinned so the WORK is reproducible, and that is
# right — but these containers do not run the work. They run
# `dispatcher_sdk.readout` / `.aggregate`, which are the dispatcher's
# side of a protocol it also implements. Taking those from whatever
# the research image happened to bake means a job pinned to a
# months-old image can never be read by a newer dispatcher — which
# would undo the one capability source registration exists for.
# Measured the hard way: a stale baked SDK recorded six errors for a
# readout that was perfectly fine.
#
# The worker's container keeps the baked copy: that one IS the work,
# and pinning it with the image is the point.
SDK_MOUNT = "/dispatcher/sdk"


def sdk_dir() -> Path:
  import dispatcher_sdk

  return Path(dispatcher_sdk.__file__).resolve().parent.parent


def protocol_command(module: str) -> list[str]:
  """Run `module` with the mounted SDK ahead of the image's own.

  A shell wrapper because `docker run -e PYTHONPATH=…` REPLACES the
  image's value rather than prepending to it, and the image's own
  entry has to survive. `bootstrap` then prepends the job's unpacked
  source, so resolution ends up: job code, live SDK, image."""
  return [
    "sh",
    "-c",
    f"PYTHONPATH={SDK_MOUNT}${{PYTHONPATH:+:$PYTHONPATH}} "
    f"exec python -m dispatcher_sdk.bootstrap "
    f"-- python -m {module}",
  ]


def parse_result_lines(stdout: str) -> list[ReadoutValue]:
  """Pull result records out of a retroactive container's stdout.

  Everything without the marker is the operator's own output and is
  left for the log: a readout that prints is not a readout that
  failed."""
  out: list[ReadoutValue] = []
  for line in stdout.splitlines():
    marker = line.find(RESULT_PREFIX)
    if marker < 0:
      continue
    payload = line[marker + len(RESULT_PREFIX) :]
    try:
      out.append(ReadoutValue.model_validate_json(payload))
    except Exception:
      continue
  return out
