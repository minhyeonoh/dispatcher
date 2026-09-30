"""Print the server's OpenAPI document to stdout.

The UI generates its TypeScript types from this (`pnpm gen:api`
in ui/apps/monitor), so wire.py stays the single source of truth
for shapes crossing the HTTP boundary. Builds the app without
running the lifespan — no docker, no data dir side effects.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from dispatcher.api.app import create_app
from dispatcher.api.config import Config
from dispatcher.api.settings import Settings


def main() -> int:
  with tempfile.TemporaryDirectory() as tmp:
    app = create_app(
      Config(self_host="schema", data_dir=Path(tmp)),
      settings=Settings(max_concurrent=1, hosts={}),
      dispatch=_never_dispatch,
      poll=lambda _p: None,
    )
    json.dump(app.openapi(), sys.stdout, indent=2)
    sys.stdout.write("\n")
  return 0


async def _never_dispatch(action: object, state: object) -> None:
  raise AssertionError("schema export never dispatches")


if __name__ == "__main__":
  sys.exit(main())
