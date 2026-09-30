"""Trial lifecycle inside the main container."""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from collections.abc import Callable

EX_OK = 0
EX_ERROR = 1
EX_INFRA = 75  # EX_TEMPFAIL — the dispatcher requeues this

TRIAL_SPEC_FILENAME = "trial.json"
OUTCOME_FILENAME = "outcome.json"


class InfraFailure(Exception):
  """The machine under the trial failed, not the work. The
  outcome is written with infra=true and the process exits 75;
  the dispatcher reruns the task instead of scoring it."""


@dataclass
class Result:
  """What `work` returns. `values` are numeric results surfaced
  in the dispatcher's monitor (per-attempt means); `data` is
  opaque and passed through to the envelope untouched."""

  values: dict[str, float] = field(default_factory=dict)
  data: Any = None


@dataclass
class TrialContext:
  attempt: str
  task: str
  trial: str
  home: Path
  payload: Any
  set_label: str
  """`k=v` string for `docker run --label` on every sibling
  container this trial starts. Unlabelled siblings leak."""


def load_trial(
  *,
  timeout_s: float = 15.0,
  env: dict[str, str] | None = None,
) -> TrialContext:
  """Read the spec the dispatcher wrote into the trial home.

  Retries briefly: the file was written launcher-side over a
  shared filesystem, and this container may open it before the
  local client cache has caught up."""
  e = env if env is not None else dict(os.environ)
  home = Path(e.get("DISPATCHER_HOME", "/dispatcher/home"))
  spec_path = home / TRIAL_SPEC_FILENAME
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
          f"trial spec unreadable after {timeout_s}s: "
          f"{spec_path}: {last_exc}"
        ) from last_exc
      time.sleep(0.5)
  return TrialContext(
    attempt=spec.get("attempt_id") or e.get("DISPATCHER_ATTEMPT", ""),
    task=spec.get("task_id") or e.get("DISPATCHER_TASK", ""),
    trial=spec.get("trial_id") or e.get("DISPATCHER_TRIAL", ""),
    home=home,
    payload=spec.get("payload"),
    set_label=e.get("DISPATCHER_SET_LABEL", ""),
  )


def write_outcome(home: Path, envelope: dict[str, Any]) -> None:
  """Atomic (tmp + rename): the dispatcher may read the file the
  instant it appears, and a torn write would park the trial in
  `unknown` until a resolver pass."""
  path = home / OUTCOME_FILENAME
  tmp = path.with_suffix(".json.tmp")
  tmp.write_text(json.dumps(envelope, default=str), encoding="utf-8")
  tmp.replace(path)


def run(
  work: Callable[[TrialContext], Any],
  *,
  env: dict[str, str] | None = None,
  spec_timeout_s: float = 15.0,
  _exit: Callable[[int], None] = sys.exit,
) -> None:
  """Full lifecycle: load spec → work → write outcome → exit.

  The outcome is written BEFORE the process (and thus the
  container) exits, so the die event the dispatcher sees always
  comes after the envelope hit the filesystem — the reader only
  has to wait out cache lag, never the write itself."""
  try:
    trial = load_trial(env=env, timeout_s=spec_timeout_s)
  except InfraFailure as exc:
    # No spec, no home to write into that we trust — exit 75 and
    # let the exit code carry the classification.
    print(f"dispatcher_sdk: {exc}", file=sys.stderr)
    _exit(EX_INFRA)
    return
  try:
    out = work(trial)
  except InfraFailure as exc:
    write_outcome(
      trial.home,
      {
        "ok": False,
        "error": {
          "type": "InfraFailure",
          "message": str(exc),
          "exit_code": EX_INFRA,
        },
        "infra": True,
      },
    )
    _exit(EX_INFRA)
    return
  except BaseException as exc:
    write_outcome(
      trial.home,
      {
        "ok": False,
        "error": {
          "type": type(exc).__name__,
          "message": "".join(traceback.format_exception_only(exc)).strip(),
          "exit_code": EX_ERROR,
        },
        "infra": False,
      },
    )
    _exit(EX_ERROR)
    return
  if isinstance(out, Result):
    envelope: dict[str, Any] = {
      "ok": True,
      "error": None,
      "infra": False,
      "values": dict(out.values),
      "data": out.data,
    }
  else:
    envelope = {
      "ok": True,
      "error": None,
      "infra": False,
      "values": {},
      "data": out,
    }
  write_outcome(trial.home, envelope)
  _exit(EX_OK)
