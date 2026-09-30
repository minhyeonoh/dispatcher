"""Boot configuration — fixed for the process lifetime.

Everything an operator can change at runtime lives in
`dispatcher.api.settings.Settings`; if a value is in here,
changing it means restarting the server."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, ConfigDict


class Config(BaseModel):
  model_config = ConfigDict(arbitrary_types_allowed=True)

  self_host: str
  # Dispatcher-owned state (jobs index, settings.json,
  # blobs). Job homes live wherever each submission says.
  data_dir: Path
  tick_interval: float = 0.5
  use_docker_events: bool = True
  # Optional built web UI to serve at /ui.
  ui_dist: Path | None = None
