"""Resident aggregate process: operator code that turns a job's
values into column numbers.

    python -m dispatcher_sdk.bootstrap \
      -- python -m dispatcher_sdk.aggregate

Started once by the dispatcher and kept alive. Reads one JSON
request per line on stdin, writes one JSON reply per line on
stdout, forever. Each request carries a whole job as a columnar
frame; the reply is whatever dict the operator's function returned.

The operator writes one function:

    def columns(job):
      df = job.df                              # pandas, if you want it
      ok = df[df.state == "done_ok"]
      return {
        "reward":        ok.reward.median(),
        "p90_latency":   df.latency.quantile(0.9),
        "cost_per_task": df.tokens.sum() * 3e-6 / max(1, len(df)),
        "worst_host":    df.groupby("host").solved.mean().idxmin(),
      }

Its keys become the job's columns. Nothing is declared in advance —
the operator owns the whole column set from one place, and the
dispatcher learns no statistics.

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

import json
import sys
import traceback
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from collections.abc import Callable


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


def _resolve(entrypoint: str) -> Callable[[JobFrame], Any]:
  from importlib import import_module

  module_name, _, attr = entrypoint.partition(":")
  module = import_module(module_name)
  func = getattr(module, attr)
  if not callable(func):
    raise TypeError(f"{entrypoint} is not callable")
  return func  # type: ignore[no-any-return]


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


def handle(
  payload: dict[str, Any], cache: dict[str, Any]
) -> dict[str, Any]:
  """One request → one reply. Never raises: this process outlives
  every request it serves, so a bad column must not end it."""
  entrypoint = str(payload.get("entrypoint") or "")
  try:
    func = cache.get(entrypoint)
    if func is None:
      func = _resolve(entrypoint)
      cache[entrypoint] = func
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
