"""Runtime-tunable settings: everything `PATCH /settings` can
change, persisted as one document.

Persistence rule: the WHOLE Settings object is saved atomically
after every successful operator patch (and on first boot), so no
knob silently reverts on restart — the old router persisted only
pool caps and lost every other PATCH at every restart.

Two invariants for anyone touching this:
- Sub-objects (`orphan_gc`, `notify`, …) are mutated IN PLACE by
  the per-service `apply_patch` functions and never reassigned —
  running loops and the notify manager hold references to them.
- Autotune's cap changes update `hosts` in memory but are NOT
  persisted; only operator patches save. Persisting them would
  re-anchor the operator ceiling at the auto-lowered value on
  restart and ratchet caps downward forever."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from pydantic import (
  BaseModel,
  ConfigDict,
  Field,
  NonNegativeInt,
)

from dispatcher.core.models import HostSettings
from dispatcher.core.readout_service import ReadoutPatch, ReadoutSettings
from dispatcher.core.readout_service import (
  apply_patch as _apply_readouts,
)
from dispatcher.core.runtime import (
  StateReconciliationPatch,
  StateReconciliationSettings,
  apply_state_reconciliation_patch,
)

# Field names on Settings shadow module names in annotation
# resolution, so import the classes directly.
from dispatcher.services.auto_archive import (
  ArchivePatch,
  ArchiveSettings,
)
from dispatcher.services.auto_archive import (
  apply_patch as _apply_archive,
)
from dispatcher.services.host_autotune import (
  HostAutotunePatch,
  HostAutotuneSettings,
)
from dispatcher.services.host_autotune import (
  apply_patch as _apply_autotune,
)
from dispatcher.services.notify import NotifyPatch, NotifySettings
from dispatcher.services.notify import (
  apply_patch as _apply_notify,
)
from dispatcher.services.orphan_gc import (
  OrphanGCPatch,
  OrphanGCSettings,
)
from dispatcher.services.orphan_gc import (
  apply_patch as _apply_gc,
)
from dispatcher.services.packer import PackPatch, PackSettings
from dispatcher.services.packer import (
  apply_patch as _apply_pack,
)

if TYPE_CHECKING:
  from pathlib import Path

logger = logging.getLogger(__name__)

SETTINGS_FILENAME = "settings.json"


class Settings(BaseModel):
  max_concurrent: NonNegativeInt
  hosts: dict[str, HostSettings]
  # Reject submissions without a frozen source archive. Off by
  # default; labs that want the arm record guaranteed flip it.
  require_source: bool = False
  # Whole-dict replacement on patch; {} clears all caps.
  pool_caps: dict[str, NonNegativeInt] = Field(default_factory=dict)
  state_reconciliation: StateReconciliationSettings = Field(
    default_factory=StateReconciliationSettings
  )
  orphan_gc: OrphanGCSettings = Field(default_factory=OrphanGCSettings)
  host_autotune: HostAutotuneSettings = Field(
    default_factory=HostAutotuneSettings
  )
  notify: NotifySettings = Field(default_factory=NotifySettings)
  archive: ArchiveSettings = Field(default_factory=ArchiveSettings)
  readouts: ReadoutSettings = Field(default_factory=ReadoutSettings)
  pack: PackSettings = Field(default_factory=PackSettings)


class HostSettingsPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  max_concurrent: NonNegativeInt | None = None
  active: bool | None = None
  alive: bool | None = None


class SettingsPatch(BaseModel):
  """Partial update; unknown fields are rejected, not dropped —
  a typo'd knob must 422, not silently no-op."""

  model_config = ConfigDict(extra="forbid")

  hosts: dict[str, HostSettingsPatch] | None = None
  max_concurrent: NonNegativeInt | None = None
  require_source: bool | None = None
  pool_caps: dict[str, NonNegativeInt] | None = None
  state_reconciliation: StateReconciliationPatch | None = None
  orphan_gc: OrphanGCPatch | None = None
  host_autotune: HostAutotunePatch | None = None
  notify: NotifyPatch | None = None
  archive: ArchivePatch | None = None
  readouts: ReadoutPatch | None = None
  pack: PackPatch | None = None


def apply_patch_pure(settings: Settings, patch: SettingsPatch) -> None:
  """Merge the scheduler-independent parts of a patch into
  `settings`, in place. Scheduler side effects (host settings,
  caps) belong to `ops.apply_settings`, which calls this too."""
  if patch.require_source is not None:
    settings.require_source = patch.require_source
  if patch.state_reconciliation is not None:
    apply_state_reconciliation_patch(
      settings.state_reconciliation, patch.state_reconciliation
    )
  if patch.orphan_gc is not None:
    _apply_gc(settings.orphan_gc, patch.orphan_gc)
  if patch.host_autotune is not None:
    _apply_autotune(settings.host_autotune, patch.host_autotune)
  if patch.notify is not None:
    _apply_notify(settings.notify, patch.notify)
  if patch.archive is not None:
    _apply_archive(settings.archive, patch.archive)
  if patch.readouts is not None:
    _apply_readouts(settings.readouts, patch.readouts)
  if patch.pack is not None:
    _apply_pack(settings.pack, patch.pack)


# ── persistence ──────────────────────────────────────────────────


def settings_path(data_dir: Path) -> Path:
  return data_dir / SETTINGS_FILENAME


def load_settings(data_dir: Path) -> Settings | None:
  """None when no file exists (first boot). A corrupt file raises
  instead of being ignored — writes are atomic, so corruption
  means real damage, and silently falling back to defaults would
  drop the operator's tuning without a trace."""
  path = settings_path(data_dir)
  if not path.is_file():
    return None
  try:
    return Settings.model_validate_json(path.read_text(encoding="utf-8"))
  except Exception as exc:
    raise RuntimeError(
      f"settings file {path} is unreadable ({exc}); refusing to "
      f"boot with silently-reset settings — fix or delete the file"
    ) from exc


def save_settings(data_dir: Path, settings: Settings) -> None:
  """Atomic (tmp + rename). Failure is logged, not fatal — the
  in-memory settings are already applied."""
  path = settings_path(data_dir)
  path.parent.mkdir(parents=True, exist_ok=True)
  tmp = path.with_suffix(path.suffix + ".tmp")
  try:
    tmp.write_text(
      json.dumps(
        settings.model_dump(mode="json"), indent=2, sort_keys=True
      ),
      encoding="utf-8",
    )
    tmp.replace(path)
  except OSError as exc:
    logger.warning("settings save failed path=%s err=%s", path, exc)
