"""Instance lifecycle inside the main container."""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dispatcher_sdk.readout import score_self

if TYPE_CHECKING:
  from collections.abc import Callable

EX_OK = 0
EX_ERROR = 1
EX_INFRA = 75  # EX_TEMPFAIL — the dispatcher requeues this

INSTANCE_SPEC_FILENAME = "instance.json"
OUTCOME_FILENAME = "outcome.json"


class InfraFailure(Exception):
  """The machine under the instance failed, not the work. The
  outcome is written with infra=true and the process exits 75;
  the dispatcher reruns the task instead of scoring it."""


@dataclass
class InstanceContext:
  job: str
  task: str
  instance: str
  home: Path
  payload: Any
  set_label: str
  """`k=v` string for `docker run --label` on every sibling
  container this instance starts. Unlabelled siblings leak."""

  readouts: list[dict[str, Any]] = field(default_factory=list)
  """The readouts registered on this job's arena, as the dispatcher
  stamped them into `instance.json`. `run()` applies them to the
  envelope it is about to write — see `dispatcher_sdk.readout`.
  Worker code does not normally touch this."""


def load_instance(
  *,
  timeout_s: float = 15.0,
  env: dict[str, str] | None = None,
) -> InstanceContext:
  """Read the spec the dispatcher wrote into the instance home.

  Retries briefly: the file was written launcher-side over a
  shared filesystem, and this container may open it before the
  local client cache has caught up."""
  e = env if env is not None else dict(os.environ)
  home = Path(e.get("DISPATCHER_HOME", "/dispatcher/home"))
  spec_path = home / INSTANCE_SPEC_FILENAME
  deadline = time.monotonic() + timeout_s
  last_exc: Exception | None = None
  while True:
    try:
      spec = json.loads(spec_path.read_text(encoding="utf-8"))
      break
    except (OSError, json.JSONDecodeError) as exc:
      last_exc = exc
      if time.monotonic() >= deadline:
        raise InfraFailure(
          f"instance spec unreadable after {timeout_s}s: "
          f"{spec_path}: {last_exc}"
        ) from last_exc
      time.sleep(0.5)
  readouts = spec.get("readouts")
  return InstanceContext(
    job=spec.get("job_id") or e.get("DISPATCHER_JOB", ""),
    task=spec.get("task_id") or e.get("DISPATCHER_TASK", ""),
    instance=spec.get("instance_id") or e.get("DISPATCHER_INSTANCE", ""),
    home=home,
    payload=spec.get("payload"),
    set_label=e.get("DISPATCHER_SET_LABEL", ""),
    readouts=readouts if isinstance(readouts, list) else [],
  )


def write_outcome(home: Path, envelope: dict[str, Any]) -> None:
  """Atomic (tmp + rename): the dispatcher may read the file the
  instant it appears, and a torn write would park the instance in
  `unknown` until a resolver pass."""
  path = home / OUTCOME_FILENAME
  tmp = path.with_suffix(".json.tmp")
  tmp.write_text(json.dumps(envelope, default=str), encoding="utf-8")
  tmp.replace(path)


def _finish(
  instance: InstanceContext,
  envelope: dict[str, Any],
  *,
  score: bool,
) -> None:
  """Score, then write the envelope. In that order, always.

  The envelope is the dispatcher's completion signal, so everything
  this instance wants read has to be on disk BEFORE it lands —
  otherwise the dispatcher reads the home, finds no values, and the
  operator has to run the retroactive path for a value the instance
  already knew.

  `score=False` for an infra failure: the machine failed, the task
  will be requeued, and scoring a run that is about to be rerun
  would put a value on an instance that never counted."""
  if score:
    score_self(
      home=instance.home,
      instance_id=instance.instance,
      task_id=instance.task,
      specs=instance.readouts,
      outcome=envelope,
      payload=instance.payload,
      log=lambda m: print(f"dispatcher_sdk.readout: {m}", file=sys.stderr),
    )
  write_outcome(instance.home, envelope)


def run(
  work: Callable[[InstanceContext], Any],
  *,
  env: dict[str, str] | None = None,
  spec_timeout_s: float = 15.0,
  _exit: Callable[[int], None] = sys.exit,
) -> None:
  """Full lifecycle: load spec → work → readouts → write outcome →
  exit.

  The outcome is written BEFORE the process (and thus the
  container) exits, so the die event the dispatcher sees always
  comes after the envelope hit the filesystem — the reader only
  has to wait out cache lag, never the write itself.

  Readouts run here, in this process, on the envelope about to be
  written: every dependency is already loaded, the artifacts are
  local, and the dispatcher finds the values in the same look that
  detects completion. They cannot change the envelope — a scoring
  bug must never be able to turn finished work into failed work."""
  try:
    instance = load_instance(env=env, timeout_s=spec_timeout_s)
  except InfraFailure as exc:
    # No spec, no home to write into that we trust — exit 75 and
    # let the exit code carry the classification.
    print(f"dispatcher_sdk: {exc}", file=sys.stderr)
    _exit(EX_INFRA)
    return
  try:
    out = work(instance)
  except InfraFailure as exc:
    _finish(
      instance,
      {
        "ok": False,
        "error": {
          "type": "InfraFailure",
          "message": str(exc),
          "exit_code": EX_INFRA,
        },
        "infra": True,
      },
      score=False,
    )
    _exit(EX_INFRA)
    return
  except BaseException as exc:
    _finish(
      instance,
      {
        "ok": False,
        "error": {
          "type": type(exc).__name__,
          "message": "".join(traceback.format_exception_only(exc)).strip(),
          "exit_code": EX_ERROR,
        },
        "infra": False,
      },
      # A failed run still gets scored: `solved` is False, `kind`
      # comes from the payload, and a column that skipped failures
      # would quietly report a mean over successes only.
      score=True,
    )
    _exit(EX_ERROR)
    return
  _finish(
    instance,
    {"ok": True, "error": None, "infra": False, "data": out},
    score=True,
  )
  _exit(EX_OK)
