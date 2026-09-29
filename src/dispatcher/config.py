"""Server configuration + PATCH /settings shapes."""

from __future__ import annotations

from pathlib import Path

from pydantic import (
  BaseModel,
  ConfigDict,
  Field,
  NonNegativeInt,
  PositiveFloat,
  PositiveInt,
)

from dispatcher.host_autotune import (
  HostAutotuneConfig,
  HostAutotunePatch,
)
from dispatcher.models import HostSettings
from dispatcher.notify import NotifyConfig, NotifyPatch


class StateReconciliationConfig(BaseModel):
  """Resolver knobs (periodic unknown/ghosted sweep)."""

  max_concurrent_probes: PositiveInt = 16
  seconds_between_probes: PositiveFloat = 30.0


class OrphanGCConfig(BaseModel):
  enabled: bool = True
  seconds_between_sweeps: PositiveFloat = 30.0
  min_container_age_s: PositiveFloat = 90.0
  """Containers younger than this are never touched — protects
  the dispatch-race window."""


class ArchiveConfig(BaseModel):
  auto_after_days: NonNegativeInt = 0
  """0 = auto-archive disabled."""

  scan_interval_seconds: PositiveFloat = 3600.0


class DispatcherConfig(BaseModel):
  model_config = ConfigDict(arbitrary_types_allowed=True)

  max_concurrent: NonNegativeInt
  hosts: dict[str, HostSettings]
  self_host: str
  # Dispatcher-owned state (attempts index, blobs). Attempt homes
  # live wherever each submission says.
  data_dir: Path
  tick_interval: float = 0.5
  use_docker_events: bool = True
  state_reconciliation: StateReconciliationConfig = Field(
    default_factory=StateReconciliationConfig
  )
  orphan_gc: OrphanGCConfig = Field(default_factory=OrphanGCConfig)
  host_autotune: HostAutotuneConfig = Field(
    default_factory=HostAutotuneConfig
  )
  pool_caps: dict[str, NonNegativeInt] = Field(default_factory=dict)
  notify: NotifyConfig = Field(default_factory=NotifyConfig)
  archive: ArchiveConfig = Field(default_factory=ArchiveConfig)
  # Optional built web UI to serve at /ui.
  ui_dist: Path | None = None


class HostSettingsPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  max_concurrent: NonNegativeInt | None = None
  active: bool | None = None
  alive: bool | None = None


class StateReconciliationPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  max_concurrent_probes: PositiveInt | None = None
  seconds_between_probes: PositiveFloat | None = None


class OrphanGCPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  enabled: bool | None = None
  seconds_between_sweeps: PositiveFloat | None = None
  min_container_age_s: PositiveFloat | None = None


class ArchivePatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  auto_after_days: NonNegativeInt | None = None
  scan_interval_seconds: PositiveFloat | None = None


class SettingsPatch(BaseModel):
  """Partial update; unknown fields are rejected, not dropped."""

  model_config = ConfigDict(extra="forbid")

  hosts: dict[str, HostSettingsPatch] | None = None
  max_concurrent: NonNegativeInt | None = None
  state_reconciliation: StateReconciliationPatch | None = None
  orphan_gc: OrphanGCPatch | None = None
  host_autotune: HostAutotunePatch | None = None
  notify: NotifyPatch | None = None
  archive: ArchivePatch | None = None
  # Whole-dict replacement; {} clears all caps.
  pool_caps: dict[str, NonNegativeInt] | None = None
