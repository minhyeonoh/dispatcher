"""Readout runner: turns finished instances into values, inside
the research repo's own container.

    python -m dispatcher_sdk.bootstrap -- python -m dispatcher_sdk.readout

The dispatcher stages a request file and starts this module; it
imports each registered entrypoint, calls it once per instance, and
prints one marked line per (instance, readout) pair. The dispatcher
reads those lines back and owns writing them down — this process
writes nothing (the job home is mounted read-only).

A readout is a plain function:

    def reward(instance):
      return instance.outcome["data"]["reward"]

Return any JSON-serialisable value. `None` is a value ("nothing to
report"), recorded as null; it is not a request to try again.
Raising is recorded as an error against that one instance and does
not touch the others — the envelope is immutable, so the same code
would raise forever, and retrying it would never converge.

Results stream out as they are produced. A runner killed halfway
therefore leaves the pairs it already finished usable, and only the
rest come back next pass.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import traceback
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from collections.abc import Callable

# Must match dispatcher.core.readout.RESULT_PREFIX. Duplicated
# rather than imported: the SDK installs into research images that
# do not carry the dispatcher package.
RESULT_PREFIX = "\x1fdispatcher-readout\x1f"

INSTANCE_SPEC_FILENAME = "instance.json"
OUTCOME_FILENAME = "outcome.json"


class ReadoutTimeout(Exception):
  """This readout outran its per-instance budget."""


@dataclass
class ReadoutInstance:
  """One finished instance, as a readout sees it.

  `home` is the instance's directory inside the read-only job
  mount — everything the worker left behind is under it, so a
  readout that needs more than the envelope (a trace, a log) reads
  it from here."""

  instance_id: str
  task_id: str
  state: str
  """`done_ok` or `done_err`. A readout that only makes sense for
  successes should check this rather than assume."""

  home: Path
  outcome: dict[str, Any]
  payload: Any

  @property
  def data(self) -> Any:
    """`outcome["data"]` — the research repo's own return value,
    which is what most readouts actually want."""
    return self.outcome.get("data")


def _read_json(path: Path) -> Any:
  try:
    return json.loads(path.read_text(encoding="utf-8"))
  except (OSError, json.JSONDecodeError):
    return None


def _resolve(entrypoint: str) -> Callable[[ReadoutInstance], Any]:
  module_name, _, attr = entrypoint.partition(":")
  module = import_module(module_name)
  func = getattr(module, attr)
  if not callable(func):
    raise TypeError(f"{entrypoint} is not callable")
  return func  # type: ignore[no-any-return]


def _emit(record: dict[str, Any]) -> None:
  # One line, flushed: the dispatcher may be killed mid-batch and
  # whatever reached it is kept.
  sys.stdout.write(RESULT_PREFIX + json.dumps(record, default=str) + "\n")
  sys.stdout.flush()


def _ok(name: str, inst: ReadoutInstance, value: Any) -> None:
  _emit(
    {
      "name": name,
      "instance_id": inst.instance_id,
      "task_id": inst.task_id,
      "ok": True,
      "value": value,
    }
  )


def _err(name: str, inst: ReadoutInstance, exc: BaseException) -> None:
  message = "".join(traceback.format_exception_only(exc)).strip()
  print(
    f"dispatcher_sdk.readout: {name} failed on "
    f"{inst.instance_id}: {message}",
    file=sys.stderr,
  )
  _emit(
    {
      "name": name,
      "instance_id": inst.instance_id,
      "task_id": inst.task_id,
      "ok": False,
      "error": message[:2000],
    }
  )


class _Deadline:
  """Per-instance wall clock via SIGALRM.

  An interval timer is the only way to interrupt arbitrary operator
  code that is not cooperating (a blocking read, a tight loop).
  Single-threaded and Linux-only, which the container is. Where it
  is unavailable the readout simply runs to completion and the
  dispatcher's container timeout is the backstop."""

  def __init__(self) -> None:
    self._armed = hasattr(signal, "setitimer")
    if self._armed:
      signal.signal(signal.SIGALRM, self._fire)

  @staticmethod
  def _fire(_signum: int, _frame: Any) -> None:
    raise ReadoutTimeout("readout exceeded its timeout")

  def __call__(self, seconds: float) -> None:
    if self._armed:
      signal.setitimer(signal.ITIMER_REAL, max(0.001, seconds))

  def clear(self) -> None:
    if self._armed:
      signal.setitimer(signal.ITIMER_REAL, 0)


def _load_instances(
  request: dict[str, Any], job_dir: Path
) -> list[tuple[ReadoutInstance, list[str]]]:
  out: list[tuple[ReadoutInstance, list[str]]] = []
  for row in request.get("instances") or []:
    instance_id = row.get("instance_id") or ""
    if not instance_id:
      continue
    home = job_dir / instance_id
    spec = _read_json(home / INSTANCE_SPEC_FILENAME) or {}
    out.append(
      (
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
    )
  return out


def main(argv: list[str] | None = None) -> int:
  del argv
  request_path = os.environ.get("DISPATCHER_READOUT_REQUEST", "")
  if not request_path:
    print(
      "dispatcher_sdk.readout: DISPATCHER_READOUT_REQUEST unset — "
      "this module is started by the dispatcher, not by hand",
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
  specs = {
    s["name"]: s
    for s in (request.get("readouts") or [])
    if isinstance(s, dict) and s.get("name")
  }
  # Resolution failures are recorded per instance, not raised: a
  # typo'd entrypoint in one readout must not stop the others from
  # filling their columns.
  resolved: dict[
    str, Callable[[ReadoutInstance], Any] | BaseException
  ] = {}
  deadline = _Deadline()
  pairs = 0
  for inst, names in _load_instances(request, job_dir):
    for name in names:
      spec = specs.get(name)
      if spec is None:
        continue
      pairs += 1
      func = resolved.get(name)
      if func is None:
        try:
          func = _resolve(str(spec.get("entrypoint") or ""))
        except BaseException as exc:
          func = exc
        resolved[name] = func
      if isinstance(func, BaseException):
        _err(name, inst, func)
        continue
      deadline(float(spec.get("timeout_sec") or 60.0))
      try:
        _ok(name, inst, func(inst))
      except BaseException as exc:
        _err(name, inst, exc)
      finally:
        deadline.clear()
  print(
    f"dispatcher_sdk.readout: {pairs} pair(s) at "
    f"{datetime.now(UTC).isoformat()}",
    file=sys.stderr,
  )
  return 0


if __name__ == "__main__":
  sys.exit(main())
