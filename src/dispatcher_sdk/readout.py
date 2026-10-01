"""Readouts: turning a finished instance into values, inside the
research repo's own container.

A readout is a plain function:

    def reward(instance):
      return instance.data["reward"]

Return any JSON-serialisable value. `None` is a value ("not
applicable here"), recorded as null; it is not a request to try
again. Raising is recorded as an error against that one instance and
touches nothing else.

This module has ONE implementation (`run_readouts`) and two callers:

- **the worker, in-process** — `dispatcher_sdk.run(work)` calls it
  right after `work` returns, writes `readouts.json` next to the
  envelope, and the dispatcher reads it in the same look that
  detects completion. This is the normal path: no second container,
  no trigger, no delay.
- **the retroactive runner** — `python -m dispatcher_sdk.readout`,
  started by the dispatcher in a container when `dispatcher readout`
  asks for pairs the live path never produced (a readout registered
  after the run, a redefinition, an instance that died first). It
  reads a request file, scores many instances, and prints marked
  lines the dispatcher parses back.

Both call the same functions on the same `ReadoutInstance`, so a
readout cannot behave differently depending on which path ran it.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from collections.abc import Callable, Iterator

# Must match dispatcher.core.readout.RESULT_PREFIX. Duplicated
# rather than imported: the SDK installs into research images that
# do not carry the dispatcher package.
RESULT_PREFIX = "\x1fdispatcher-readout\x1f"

INSTANCE_SPEC_FILENAME = "instance.json"
OUTCOME_FILENAME = "outcome.json"
READOUTS_FILENAME = "readouts.json"

_DEFAULT_TIMEOUT_S = 600.0


class ReadoutTimeout(Exception):
  """This readout outran its per-instance budget."""


@dataclass
class ReadoutInstance:
  """One finished instance, as a readout sees it."""

  instance_id: str
  task_id: str
  state: str
  """`done_ok` or `done_err`. A readout that only makes sense for
  successes should check this rather than assume."""

  home: Path
  """The instance's directory. In the worker it is the live home; in
  the retroactive path it is that same directory seen read-only
  through the job mount. Everything the worker left behind — traces,
  logs — is under it, so a readout that needs more than the envelope
  reads it from here."""

  outcome: dict[str, Any]
  payload: Any

  @property
  def data(self) -> Any:
    """`outcome["data"]` — the research repo's own return value,
    which is what most readouts actually want."""
    return self.outcome.get("data")


# ── the one implementation ───────────────────────────────────────


class _Deadline:
  """Per-instance wall clock via SIGALRM.

  An interval timer is the only way to interrupt operator code that
  is not cooperating (a blocking read, a tight loop). Single-threaded
  and Linux-only, which the container is. Where unavailable the
  readout runs to completion — in the worker that means the instance
  takes longer; in the retroactive path the container timeout is the
  backstop."""

  def __init__(self) -> None:
    self._armed = hasattr(signal, "setitimer")
    if self._armed:
      try:
        signal.signal(signal.SIGALRM, self._fire)
      except ValueError:
        # Not the main thread — leave the timer off rather than
        # failing the whole scoring pass.
        self._armed = False

  @staticmethod
  def _fire(_signum: int, _frame: Any) -> None:
    raise ReadoutTimeout("readout exceeded its timeout")

  def __enter__(self) -> _Deadline:
    return self

  def __exit__(self, *_exc: object) -> None:
    self.clear()

  def arm(self, seconds: float) -> None:
    if self._armed:
      signal.setitimer(signal.ITIMER_REAL, max(0.001, seconds))

  def clear(self) -> None:
    if self._armed:
      signal.setitimer(signal.ITIMER_REAL, 0)


def _resolve(entrypoint: str) -> Callable[[ReadoutInstance], Any]:
  from importlib import import_module

  module_name, _, attr = entrypoint.partition(":")
  module = import_module(module_name)
  func = getattr(module, attr)
  if not callable(func):
    raise TypeError(f"{entrypoint} is not callable")
  return func  # type: ignore[no-any-return]


def run_readouts(
  specs: list[dict[str, Any]],
  instance: ReadoutInstance,
  *,
  only: list[str] | None = None,
  log: Callable[[str], None] | None = None,
) -> list[dict[str, Any]]:
  """Score one instance against every spec (or just `only`).

  Never raises. A broken entrypoint or a raising readout becomes an
  `ok: false` record for that readout alone — which matters most in
  the worker, where an exception escaping here would turn a
  successful instance into a failed one."""
  out: list[dict[str, Any]] = []
  wanted = None if only is None else set(only)
  with _Deadline() as deadline:
    for spec in specs:
      name = str(spec.get("name") or "")
      if not name or (wanted is not None and name not in wanted):
        continue
      record: dict[str, Any] = {
        "name": name,
        "instance_id": instance.instance_id,
        "task_id": instance.task_id,
      }
      try:
        func = _resolve(str(spec.get("entrypoint") or ""))
        deadline.arm(float(spec.get("timeout_sec") or _DEFAULT_TIMEOUT_S))
        try:
          record["ok"] = True
          record["value"] = func(instance)
        finally:
          deadline.clear()
      except BaseException as exc:
        message = "".join(traceback.format_exception_only(exc)).strip()
        record["ok"] = False
        record.pop("value", None)
        record["error"] = message[:2000]
        if log is not None:
          log(f"{name} failed on {instance.instance_id}: {message}")
      out.append(record)
  return out


# ── the worker's call site ───────────────────────────────────────


def write_instance_readouts(
  home: Path, records: list[dict[str, Any]]
) -> None:
  """Write `readouts.json` into the instance home.

  The caller must do this BEFORE the outcome envelope: the envelope
  is the dispatcher's completion signal, and anything the instance
  wants read has to already be on disk when that signal lands. (The
  same ordering lesson as writing a result before tearing the
  container down.)"""
  path = home / READOUTS_FILENAME
  tmp = path.with_suffix(".json.tmp")
  tmp.write_text(json.dumps(records, default=str), encoding="utf-8")
  tmp.replace(path)


def score_self(
  *,
  home: Path,
  instance_id: str,
  task_id: str,
  specs: list[dict[str, Any]],
  outcome: dict[str, Any],
  payload: Any,
  log: Callable[[str], None] | None = None,
) -> None:
  """Run the instance's registered readouts against the envelope it
  is about to write, and leave the values beside it.

  Called from `dispatcher_sdk.run` after the outcome is DECIDED and
  before it is written. Never raises: scoring is not the work, and a
  scoring bug must not be able to reclassify the work."""
  if not specs:
    return
  state = "done_ok" if outcome.get("ok") else "done_err"
  try:
    records = run_readouts(
      specs,
      ReadoutInstance(
        instance_id=instance_id,
        task_id=task_id,
        state=state,
        home=home,
        outcome=outcome,
        payload=payload,
      ),
      log=log,
    )
    write_instance_readouts(home, records)
  except BaseException as exc:  # pragma: no cover — belt and braces
    if log is not None:
      log(f"scoring aborted: {type(exc).__name__}: {exc}")


# ── the retroactive runner's call site ───────────────────────────


def _read_json(path: Path) -> Any:
  try:
    return json.loads(path.read_text(encoding="utf-8"))
  except (OSError, json.JSONDecodeError):
    return None


def _emit(record: dict[str, Any]) -> None:
  # One line, flushed: the dispatcher keeps whatever reached it, so
  # a container killed mid-batch still delivers what it finished.
  sys.stdout.write(RESULT_PREFIX + json.dumps(record, default=str) + "\n")
  sys.stdout.flush()


def _requested(
  request: dict[str, Any], job_dir: Path
) -> Iterator[tuple[ReadoutInstance, list[str]]]:
  for row in request.get("instances") or []:
    instance_id = row.get("instance_id") or ""
    if not instance_id:
      continue
    home = job_dir / instance_id
    spec = _read_json(home / INSTANCE_SPEC_FILENAME) or {}
    yield (
      ReadoutInstance(
        instance_id=instance_id,
        task_id=row.get("task_id") or spec.get("task_id") or "",
        state=row.get("state") or "",
        home=home,
        outcome=_read_json(home / OUTCOME_FILENAME) or {},
        payload=spec.get("payload"),
      ),
      list(row.get("readouts") or []),
    )


def main(argv: list[str] | None = None) -> int:
  del argv
  request_path = os.environ.get("DISPATCHER_READOUT_REQUEST", "")
  if not request_path:
    print(
      "dispatcher_sdk.readout: DISPATCHER_READOUT_REQUEST unset — "
      "this module is started by the dispatcher, not by hand (the "
      "normal path is dispatcher_sdk.run scoring in-process)",
      file=sys.stderr,
    )
    return 2
  request = _read_json(Path(request_path))
  if not isinstance(request, dict):
    print(
      f"dispatcher_sdk.readout: unreadable request {request_path}",
      file=sys.stderr,
    )
    return 2
  job_dir = Path(
    request.get("job_dir")
    or os.environ.get("DISPATCHER_JOB_DIR", "/dispatcher/job")
  )
  specs = [
    s
    for s in (request.get("readouts") or [])
    if isinstance(s, dict) and s.get("name")
  ]

  def _log(message: str) -> None:
    print(f"dispatcher_sdk.readout: {message}", file=sys.stderr)

  pairs = 0
  for instance, names in _requested(request, job_dir):
    for record in run_readouts(specs, instance, only=names, log=_log):
      _emit(record)
      pairs += 1
  _log(f"{pairs} pair(s)")
  return 0


if __name__ == "__main__":
  sys.exit(main())
