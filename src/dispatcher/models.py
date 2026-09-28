"""Data models.

Truth-ownership split:
- `AttemptState` — persisted intent: what was submitted plus the
  mutable scheduler knobs. Rebuilt from the event log on restart.
- `DispatchEntry` — one row of the append-only dispatch log: which
  host got which trial.
- `TrialView` / `AttemptView` — derived execution state, never
  persisted.
- `Outcome` — the envelope a trial leaves at
  `<home>/outcome.json`. The dispatcher reads only the contract
  fields (`ok`, `error`, `infra`, `values`); `data` is opaque to
  it and belongs to the research repo.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import (
  BaseModel,
  ConfigDict,
  Field,
  NonNegativeInt,
)

OUTCOME_FILENAME = "outcome.json"
TRIAL_SPEC_FILENAME = "trial.json"

# Main-container exit codes that mean "the machine under the trial
# failed, not the work": 75 = EX_TEMPFAIL (stated by the worker via
# InfraFailure), 137/143 = SIGKILL/SIGTERM (host sweep, OOM,
# reboot), 255 = ssh-layer death. A trial ending this way with no
# outcome envelope is requeued, not scored.
INFRA_EXIT_CODES = frozenset({75, 137, 143, 255})


class HostSettings(BaseModel):
  """Per-host scheduling settings.

  `active` is operator intent (set via PATCH /settings); `alive`
  is an infrastructure fact auto-flipped by the docker-events
  stream. Dispatch requires both."""

  max_concurrent: NonNegativeInt
  active: bool = True
  alive: bool = True


class ContainerSpec(BaseModel):
  """How to start one trial's main container. Everything here is
  a template shared by every trial of the attempt; the dispatcher
  adds per-trial identity (labels, env, home mount) at dispatch."""

  model_config = ConfigDict(extra="forbid")

  image: str
  command: list[str] = Field(default_factory=list)
  env: dict[str, str] = Field(default_factory=dict)
  # Raw `docker run -v` strings ("src:dst[:opts]").
  mounts: list[str] = Field(default_factory=list)
  # Container-side path where the trial home dir is bind-mounted.
  home_mount: str = "/dispatcher/home"
  # Escape hatch: raw args inserted before the image
  # (e.g. ["--gpus", "all", "--network", "host"]).
  extra_args: list[str] = Field(default_factory=list)


class AttemptState(BaseModel):
  """Persisted attempt intent. Serialised verbatim into the
  `submit` event; keep every field JSON-serialisable."""

  model_config = ConfigDict(arbitrary_types_allowed=True)

  attempt_id: str
  label: str
  task_list: list[str]
  # Directory that holds one subdir per trial
  # (`<home_root>/<trial_name>/`). Must be unique per attempt and
  # visible on every dispatch host (shared filesystem).
  home_root: Path
  container: ContainerSpec
  submitted_at: datetime
  # Opaque per-task payload delivered to the trial via
  # `<home>/trial.json`. Tasks absent from the map get null.
  payloads: dict[str, Any] = Field(default_factory=dict)
  # Extra env for the main container (merged over container.env;
  # per-attempt credentials go here).
  env: dict[str, str] = Field(default_factory=dict)
  metadata: dict[str, Any] = Field(default_factory=dict)
  # Free-form grouping string for UIs (empty = unscoped).
  scope: str = ""
  pool: str = "default"
  tags: list[str] = Field(default_factory=list)
  # Progress-notification thresholds already fired (replayed from
  # `notify_fired` events so restarts don't re-announce).
  notified_thresholds: list[float] = Field(default_factory=list)

  # Scheduler knobs — mutable via `patch` events.
  paused: bool = False
  max_concurrent: int | None = None
  weight: int = 1
  pause_on_error: bool | None = None
  """None = auto: pause on error iff max_concurrent == 1."""
  alias: str = ""
  archived_at: datetime | None = None
  archive_kind: str = ""
  """"manual" | "auto"; empty when not archived."""


class DispatchEntry(BaseModel):
  """One dispatch: also the shape `Scheduler.dispatch_one`
  returns for the runtime to execute."""

  attempt_id: str
  task_name: str
  trial_name: str
  host: str
  dispatched_at: datetime


TrialViewState = Literal[
  "done_ok", "done_err", "running", "ghosted", "unknown", "pending"
]


class TrialView(BaseModel):
  """One dispatched trial's snapshot. Pending tasks are plain
  strings in `AttemptView.pending`, never TrialViews."""

  task_name: str
  state: TrialViewState
  trial_name: str
  host: str
  dispatched_at: datetime


class AttemptView(BaseModel):
  """Derived per-attempt buckets.

  `unknown` = trial ended (or was restart-adopted) without a
  readable outcome — could be NFS lag, a crash, or still actually
  running; a resolver reclassifies it on evidence. `ghosted` =
  container confirmed no longer executing and still no outcome;
  kept non-final because a late NFS commit can still surface the
  file. Both block attempt drain."""

  done_ok: dict[str, TrialView] = Field(default_factory=dict)
  done_err: dict[str, TrialView] = Field(default_factory=dict)
  running: dict[str, TrialView] = Field(default_factory=dict)
  ghosted: dict[str, TrialView] = Field(default_factory=dict)
  unknown: dict[str, TrialView] = Field(default_factory=dict)
  pending: list[str] = Field(default_factory=list)


class OutcomeError(BaseModel):
  type: str = ""
  message: str = ""
  exit_code: int | None = None


class Outcome(BaseModel):
  """The trial-side result envelope (`<home>/outcome.json`).

  Contract fields only; `data` is whatever the research repo
  returns and is passed through untouched."""

  ok: bool
  error: OutcomeError | None = None
  # The failure was the machine's, not the work's — requeue
  # instead of scoring. Stated by the worker (e.g. its serving
  # backend was swapped underneath it).
  infra: bool = False
  # Numeric results for monitoring (means surface per attempt).
  values: dict[str, float] = Field(default_factory=dict)
  data: Any = None
