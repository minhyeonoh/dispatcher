"""Resident aggregate process: operator code that turns a job's
values into column numbers.

    python -m dispatcher_sdk.bootstrap \
      -- python -m dispatcher_sdk.aggregate

Started once by the dispatcher and kept alive. Reads one JSON
request per line on stdin, writes one JSON reply per line on
stdout, forever. Each request carries a whole job as a columnar
frame; the reply is whatever dict the operator's function returned.

The operator writes two functions:

    def columns(job):
      df = job.df                              # pandas, if you want it
      ok = df[df.state == "done_ok"]
      return {
        "reward":        ok.reward.median(),
        "p90_latency":   df.latency.quantile(0.9),
        "cost_per_task": df.tokens.sum() * 3e-6 / max(1, len(df)),
        "worst_host":    df.groupby("host").solved.mean().idxmin(),
      }

…and one that says what they mean:

    def column_descriptions():
      return {
        "reward": "median reward over successful instances",
        "p90_latency": "90th percentile of per-instance latency",
      }

Its keys become the job's columns. Nothing is declared in advance —
the operator owns the whole column set from one place, and the
dispatcher learns no statistics. The descriptions sit beside the
function that names the keys rather than in the registration request,
which would be a second copy of the same key list and the first thing
to drift; being in the registered text they are also in the copy kept
beside the values, so a column explains itself years later.

The dispatcher registers that TEXT and sends it with every request,
so changing what a column means is one call and works on arenas that
finished weeks ago. Your repo is still importable, so heavy logic can
stay there.

`job.df` imports pandas lazily, so this module stays stdlib-only
unless you ask for a DataFrame. `job.columns` (dict of lists) and
`job.records` (list of dicts) are always there, which is also how
you reach polars: `pl.DataFrame(job.columns)`.

What the frame does NOT give you is the filesystem. One process
serves every job that shares an image and source archive, so there
is no single job home to mount — and by aggregate time the reading
is done. Anything an artifact can tell you belongs in a per-instance
readout (`dispatcher_sdk.readout`), whose value then shows up here
as a column.
"""

from __future__ import annotations

import hashlib
import json
import sys
import traceback
from typing import Any


class JobFrame:
  """One job's values, row per instance.

  Columns are `instance_id`, `task_id`, `state`, `host`,
  `dispatched_at`, `finished_at`, `duration_s`, and one per readout.
  A readout that returned `None` OR raised is `None` here — the
  frame cannot tell those apart, deliberately, because a column
  almost never should. `errors` has the per-readout counts when you
  do care, and `records` carries the raw rows."""

  __slots__ = ("_df", "columns", "done_err", "done_ok", "errors", "job_id")

  def __init__(self, payload: dict[str, Any]) -> None:
    frame = payload.get("frame") or {}
    self.job_id: str = str(payload.get("job_id") or "")
    self.columns: dict[str, list[Any]] = {
      str(k): list(v)
      for k, v in (frame.get("columns") or {}).items()
      if isinstance(v, list)
    }
    self.errors: dict[str, int] = {
      str(k): int(v) for k, v in (frame.get("errors") or {}).items()
    }
    self.done_ok: int = int(frame.get("done_ok") or 0)
    self.done_err: int = int(frame.get("done_err") or 0)
    self._df: Any = None

  def __len__(self) -> int:
    for values in self.columns.values():
      return len(values)
    return 0

  @property
  def records(self) -> list[dict[str, Any]]:
    """Row dicts. Dependency-free; the slow path into any frame
    library (`columns` is faster for pandas and polars alike)."""
    keys = list(self.columns)
    return [
      dict(zip(keys, row, strict=False))
      for row in zip(*self.columns.values(), strict=False)
    ]

  @property
  def df(self) -> Any:
    """A pandas DataFrame, built once per request.

    pandas is imported here and nowhere else, so a research image
    without it can still use `columns`/`records` — the SDK itself
    stays stdlib-only."""
    if self._df is None:
      try:
        import pandas as pd
      except ImportError as exc:  # pragma: no cover
        raise ImportError(
          "job.df needs pandas in your image; use job.columns "
          "(dict of lists) or job.records for a dependency-free "
          "path, or pl.DataFrame(job.columns) for polars"
        ) from exc
      self._df = pd.DataFrame(self.columns)
    return self._df


def compile_source(source: str, name: str = "columns") -> Any:
  """Turn registered code into a callable. See
  `dispatcher_sdk.readout.compile_source` — same contract, so a
  column function and a readout are written the same way."""
  namespace: dict[str, Any] = {"__name__": f"dispatcher_{name}"}
  exec(compile(source, f"<{name}>", "exec"), namespace)
  func = namespace.get(name)
  if not callable(func):
    raise TypeError(f"source does not define a callable named {name!r}")
  return func


def _jsonable(value: Any) -> Any:
  """Numbers out of pandas/numpy are not JSON-serialisable, and NaN
  is not valid JSON. Coerce here rather than letting a column break
  the whole reply."""
  if value is None or isinstance(value, (bool, int, str)):
    return value
  if isinstance(value, float):
    return value if value == value and abs(value) != float("inf") else None
  for attr in ("item", "tolist"):
    method = getattr(value, attr, None)
    if callable(method):
      try:
        return _jsonable(method())
      except Exception:
        break
  if isinstance(value, (list, tuple)):
    return [_jsonable(v) for v in value]
  if isinstance(value, dict):
    return {str(k): _jsonable(v) for k, v in value.items()}
  return str(value)


def _describe(source: str, cache: dict[str, Any]) -> dict[str, str]:
  """`column_descriptions()` from the same source, compiled once.

  Absent or raising yields nothing rather than failing the request:
  the dispatcher notices undescribed keys itself and says so, and
  losing the numbers over missing documentation would be the wrong
  trade."""
  key = f"desc:{hashlib.sha256(source.encode('utf-8')).hexdigest()}"
  if key not in cache:
    try:
      out = compile_source(source, "column_descriptions")()
      cache[key] = (
        {str(k): str(v) for k, v in out.items()}
        if isinstance(out, dict)
        else {}
      )
    except BaseException as exc:
      print(
        f"dispatcher_sdk.aggregate: column_descriptions failed: {exc}",
        file=sys.stderr,
        flush=True,
      )
      cache[key] = {}
  return cache[key]  # type: ignore[no-any-return]


def handle(
  payload: dict[str, Any], cache: dict[str, Any]
) -> dict[str, Any]:
  """One request → one reply. Never raises: this process outlives
  every request it serves, so a bad column must not end it."""
  source = str(payload.get("source") or "")
  digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
  entrypoint = f"columns@{digest[:12]}"
  try:
    # Keyed by hash: the same function arrives with every request and
    # is compiled once, while a redefinition recompiles immediately.
    func = cache.get(digest)
    if func is None:
      func = compile_source(source)
      cache[digest] = func
    out = func(JobFrame(payload))
    if not isinstance(out, dict):
      return {
        "ok": False,
        "error": (
          f"{entrypoint} returned {type(out).__name__}; a columns "
          f"function returns a dict of column name → value"
        ),
      }
    return {
      "ok": True,
      "values": {str(k): _jsonable(v) for k, v in out.items()},
      "descriptions": _describe(source, cache),
    }
  except BaseException as exc:
    message = "".join(traceback.format_exception_only(exc)).strip()
    print(
      f"dispatcher_sdk.aggregate: {entrypoint} failed on "
      f"{payload.get('job_id')}: {message}",
      file=sys.stderr,
      flush=True,
    )
    return {"ok": False, "error": message[:2000]}


def main(argv: list[str] | None = None) -> int:
  del argv
  cache: dict[str, Any] = {}
  for line in sys.stdin:
    if not line.strip():
      continue
    try:
      payload = json.loads(line)
    except json.JSONDecodeError as exc:
      reply = {"ok": False, "error": f"unparseable request: {exc}"}
    else:
      reply = (
        handle(payload, cache)
        if isinstance(payload, dict)
        else {"ok": False, "error": "request is not an object"}
      )
    # One line, flushed, before reading the next: the dispatcher is
    # blocked on exactly this line.
    sys.stdout.write(json.dumps(reply, default=str) + "\n")
    sys.stdout.flush()
  return 0


if __name__ == "__main__":
  sys.exit(main())
