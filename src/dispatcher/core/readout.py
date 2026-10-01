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

Identity is the NAME, alone. A changed computation is a different
readout (`reward-v1` / `reward-v2`), never a new version of the
same one: a column that silently changes meaning invalidates every
figure already drawn from it. Each value also records the
`source_sha256` + `image_id` that produced it, so "which code made
this number" is answerable without trusting the name.

Files, per job:
  <home_root>/.readouts/<name>.jsonl   append-only column index,
                                       one ReadoutValue per line,
                                       last line wins
  <home_root>/.readouts/request.json   what the retroactive pass
                                       was asked to do
and per instance:
  <home>/readouts.json                 what the worker scored
                                       itself, written BEFORE the
                                       outcome envelope
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, PositiveFloat

if TYPE_CHECKING:
  from pathlib import Path

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
# `module.path:callable` — resolved where the code lives, never here.
_ENTRYPOINT_RE = re.compile(
  r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$"
)


class BadReadout(ValueError):
  """Malformed registration (name or entrypoint)."""


class ReadoutSpec(BaseModel):
  """One registration. Travels into the instance via
  `instance.json`, so the worker knows what to score itself on."""

  model_config = ConfigDict(extra="forbid")

  name: str
  entrypoint: str
  """`module:callable`, imported inside the research repo's own
  container — never by the dispatcher, which has no idea what that
  repo's modules are."""

  timeout_sec: PositiveFloat = 600.0
  """Per-instance wall clock. Deliberately generous: the readout
  runs inside the instance, so its cost is accepted as part of the
  work rather than fenced off. A readout that parses a large trace
  is a legitimate readout; one that hangs forever is not."""

  def validated(self) -> ReadoutSpec:
    if not _NAME_RE.match(self.name):
      raise BadReadout(
        f"readout name {self.name!r} must match "
        f"[A-Za-z0-9][A-Za-z0-9._-]{{0,63}} — it is also a filename"
      )
    if not _ENTRYPOINT_RE.match(self.entrypoint):
      raise BadReadout(
        f"entrypoint {self.entrypoint!r} must be 'module.path:callable'"
      )
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
  """What computed it. The name is the column's identity, but these
  two pin the exact code — so a number can be traced back without
  trusting that the name never got reused."""


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
