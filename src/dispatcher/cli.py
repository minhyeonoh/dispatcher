"""`dispatcher serve` — run the server.

Example:
  dispatcher serve --self-host ml10 --data-dir ~/dispatcher-data \\
    --host ml10=8 --host ml9=8 --max-concurrent 12 --port 7200
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
  ap = argparse.ArgumentParser(prog="dispatcher")
  sub = ap.add_subparsers(dest="cmd", required=True)
  serve = sub.add_parser("serve", help="run the dispatcher server")
  serve.add_argument("--self-host", required=True)
  serve.add_argument(
    "--data-dir",
    required=True,
    type=Path,
    help="dispatcher-owned state dir (attempts index, blobs)",
  )
  serve.add_argument(
    "--host",
    action="append",
    default=[],
    metavar="NAME=CAP",
    help=(
      "dispatch host and its max concurrent trials; repeatable. "
      "Required on first boot; later boots reuse the persisted "
      "settings.json and flags act as explicit overrides"
    ),
  )
  serve.add_argument("--max-concurrent", type=int, default=None)
  serve.add_argument("--port", type=int, default=7200)
  serve.add_argument("--bind", default="127.0.0.1")
  serve.add_argument(
    "--no-docker-events",
    action="store_true",
    help="NFS-poll completion detection instead of docker events",
  )
  serve.add_argument(
    "--ui-dist",
    type=Path,
    default=None,
    help="built web UI to serve at /ui",
  )
  monitor = sub.add_parser(
    "monitor", help="live terminal monitor over the SSE stream"
  )
  monitor.add_argument("--server", default="http://127.0.0.1:7200")
  monitor.add_argument("--detail", default=None, metavar="ATTEMPT_ID")
  monitor.add_argument("--refresh-per-second", type=int, default=4)

  args = ap.parse_args(argv)

  if args.cmd == "monitor":
    from rich.console import Console

    from dispatcher.tools.monitor_ui import run_monitor

    run_monitor(
      server=args.server,
      detail=args.detail,
      refresh_per_second=args.refresh_per_second,
      console=Console(),
    )
    return 0

  import uvicorn

  from dispatcher.api.app import create_app
  from dispatcher.api.config import Config
  from dispatcher.api.settings import (
    HostSettingsPatch,
    Settings,
    SettingsPatch,
  )
  from dispatcher.core.models import HostSettings

  hosts: dict[str, HostSettings] = {}
  for spec in args.host:
    name, sep, cap = spec.partition("=")
    if not sep or not cap.isdigit():
      ap.error(f"--host expects NAME=CAP, got {spec!r}")
    hosts[name] = HostSettings(max_concurrent=int(cap))

  config = Config(
    self_host=args.self_host,
    data_dir=args.data_dir.expanduser(),
    use_docker_events=not args.no_docker_events,
    ui_dist=args.ui_dist,
  )
  # First boot needs a full seed (--host + --max-concurrent);
  # later boots reuse the persisted settings.json, with any
  # explicitly-passed flags applied on top as overrides.
  seed: Settings | None = None
  if hosts and args.max_concurrent is not None:
    seed = Settings(max_concurrent=args.max_concurrent, hosts=hosts)
  overrides = SettingsPatch(
    hosts=(
      {
        name: HostSettingsPatch(max_concurrent=hs.max_concurrent)
        for name, hs in hosts.items()
      }
      if hosts
      else None
    ),
    max_concurrent=args.max_concurrent,
  )
  app = create_app(config, settings=seed, settings_overrides=overrides)
  uvicorn.run(app, host=args.bind, port=args.port)
  return 0


if __name__ == "__main__":
  sys.exit(main())
