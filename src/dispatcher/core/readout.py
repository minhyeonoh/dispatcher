"""Readouts: operator code that turns finished instances into
values the dispatcher can serve as columns.

The dispatcher cannot know what `reward` or `tgc` means — every
benchmark computes it differently, out of its own envelope. So the
computation is the research repo's: a named Python callable that
the dispatcher runs INSIDE a container built from the job's pinned
image, once per finished instance, and whose return value it
appends to a file under the job's home.

Identity is the NAME, alone. A changed computation is a different
readout (`reward-v1` / `reward-v2`), never a new version of the
same one: a column that silently changes meaning invalidates every
figure already drawn from it, and nothing in the record would say
so. That rule is why there is no version field here.

One value per instance is the whole contract. Job- and arena-level
numbers are arithmetic over those values (see `aggregate`), so no
operator code runs at that level and there is nothing to keep in
sync.

Files, per job:
  <home_root>/.readouts/<name>.jsonl   append-only, one ReadoutValue
                                       per line, last line wins
  <home_root>/.readouts/request.json   what the current container
                                       pass was asked to do
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

# The readout runner marks its result lines with this prefix so
# the operator's own prints can share stdout without corrupting
# the protocol. U+001F (unit separator) does not occur in ordinary
# program output.
RESULT_PREFIX = "\x1fdispatcher-readout\x1f"

# The name becomes a filename and a wire key, so keep it boring.
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
# `module.path:callable` — resolved in the container, never here.
_ENTRYPOINT_RE = re.compile(
  r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$"
)


class BadReadout(ValueError):
  """Malformed registration (name or entrypoint)."""


class ReadoutSpec(BaseModel):
  """One registration.

  `entrypoint` is imported by the in-container runner, never by
  the dispatcher — the dispatcher has no idea what the research
  repo's modules are, and importing operator code into the
  scheduler process is exactly the coupling this design avoids."""

  model_config = ConfigDict(extra="forbid")

  name: str
  entrypoint: str
  timeout_sec: PositiveFloat = 60.0
  """Per-INSTANCE wall clock inside the runner. The container also
  gets an outer timeout; this one exists so a single pathological
  instance cannot eat the whole batch."""

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
  """One line of `<name>.jsonl`.

  `ok=False` records a readout that RAISED on this instance. It
  still counts as scored: the envelope is immutable and the code is
  the same, so re-running it produces the same exception forever.
  Batch-level failures (container never started, runner died) emit
  no line at all, which is what makes those retry.

  `name` is redundant with the filename and kept anyway: these
  files outlive the dispatcher that wrote them, and a line that
  names its own readout can be read years later without the
  directory around it."""

  name: str = ""
  instance_id: str
  task_id: str = ""
  ok: bool = True
  value: Any = None
  error: str = ""
  at: datetime | None = None
  """When the DISPATCHER recorded it. Stamped on the way to disk,
  not by the runner: the container's clock is not ours, and a
  missing field must not make the line unparseable."""


class ReadoutAggregate(BaseModel):
  """Job-level roll-up of one readout's values."""

  n: int = 0
  """Instances with a value of any kind, errors and nulls included —
  the denominator behind `readout_lag`."""

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

  `lag` = (instance, readout) pairs with a finished instance and no
  value yet. It is the number that makes a too-heavy readout
  visible: if computation cannot keep up with completion, this
  grows monotonically instead of the delay hiding somewhere.
  `None` = values for this job are not loaded, so the honest answer
  is "unknown", not 0."""

  aggregates: dict[str, ReadoutAggregate] = Field(default_factory=dict)
  lag: int | None = None


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


# ── on-disk values ───────────────────────────────────────────────


def readout_dir(home_root: Path) -> Path:
  return home_root / READOUT_DIRNAME


def values_path(home_root: Path, name: str) -> Path:
  return readout_dir(home_root) / f"{name}.jsonl"


def request_path(home_root: Path) -> Path:
  return readout_dir(home_root) / REQUEST_FILENAME


def read_values(home_root: Path, name: str) -> dict[str, ReadoutValue]:
  """`{instance_id: value}`, last line winning.

  Append-only with last-wins is what makes a recompute possible
  without ever rewriting history: the old value stays in the file
  as a record, the new one shadows it. Unparseable lines are
  skipped — the file is read far more often than it is written, and
  one torn tail must not hide a thousand good values."""
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
  """One open, one write, one flush — the whole batch lands as a
  single append so a crash cannot interleave it with another
  writer's lines. Only the dispatcher writes these files (the
  container's job home mount is read-only), so there is exactly
  one writer per path and no lock is needed."""
  if not values:
    return
  path = values_path(home_root, name)
  path.parent.mkdir(parents=True, exist_ok=True)
  blob = "".join(v.model_dump_json() + "\n" for v in values)
  with path.open("a", encoding="utf-8") as f:
    f.write(blob)


def write_request(home_root: Path, request: dict[str, Any]) -> Path:
  """Stage what the next container pass should do. Read back
  through the job home's read-only mount, so the dispatcher stays
  the only writer here too."""
  path = request_path(home_root)
  path.parent.mkdir(parents=True, exist_ok=True)
  tmp = path.with_suffix(".json.tmp")
  tmp.write_text(json.dumps(request, indent=2, default=str), "utf-8")
  tmp.replace(path)
  return path


def parse_result_lines(stdout: str) -> list[ReadoutValue]:
  """Pull result records out of the container's stdout.

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
