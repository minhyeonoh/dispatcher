"""`dispatcher serve` — run the server.

Example:
  dispatcher serve --self-host ml10 --data-dir ~/dispatcher-data \\
    --host ml10=8 --host ml9=8 --max-concurrent 12 --port 7200

`dispatcher readout <arena> --add reward --file readouts.py`
registers a readout's CODE (not a path to it), so it is computable
against runs that finished before it existed. `--columns --file …`
registers the arena's column function the same way.

`dispatcher readout <arena|job_id>` fills in readout values the
live path never produced — a readout registered after a run
finished, a redefinition, an instance that died before scoring
itself. A command and not a background loop on purpose: it starts
containers, and that is an operator's decision to make and watch.

`dispatcher readouts <job_id>` dumps those values as JSON, and
`dispatcher path <job_id> <instance_id>` prints where to read one
instance's files. Both exist so an analysis script never spells out
a job's layout: the same two commands keep answering correctly once
instance homes are packed, and `core.pack` is the only place that
knows whether a pack is mounted. Note the split — values come from
the consolidated column index, files come from the instance home.
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
    help="dispatcher-owned state dir (jobs index, blobs)",
  )
  serve.add_argument(
    "--host",
    action="append",
    default=[],
    metavar="NAME=CAP",
    help=(
      "dispatch host and its max concurrent instances; repeatable. "
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
    help=(
      "built web UI to serve at the root url space (the JSON API "
      "stays under /api); omit for an API-only server"
    ),
  )
  monitor = sub.add_parser(
    "monitor", help="live terminal monitor over the SSE stream"
  )
  monitor.add_argument("--server", default="http://127.0.0.1:7200")
  monitor.add_argument("--detail", default=None, metavar="JOB_ID")
  monitor.add_argument("--refresh-per-second", type=int, default=4)
  readout = sub.add_parser(
    "readout",
    help="register readouts from a file, or compute missing values",
  )
  readout.add_argument(
    "--add",
    metavar="NAME",
    default=None,
    help="register a per-instance readout named NAME (needs --file)",
  )
  readout.add_argument(
    "--columns",
    action="store_true",
    help="register the arena's columns(job) function (needs --file)",
  )
  readout.add_argument(
    "--file",
    default=None,
    metavar="PY",
    help="the python file whose text is registered",
  )
  readout.add_argument("--timeout-sec", type=float, default=600.0)
  readout.add_argument(
    "target",
    metavar="ARENA|JOB_ID",
    help=(
      "an arena path (covers its subtree) or one job_id; anything "
      "containing '/' or starting with 'job-' is disambiguated "
      "automatically"
    ),
  )
  readout.add_argument(
    "--name",
    action="append",
    default=[],
    metavar="READOUT",
    help="only this readout; repeatable (default: all registered)",
  )
  readout.add_argument("--server", default="http://127.0.0.1:7200")

  readouts = sub.add_parser(
    "readouts",
    help="dump a job's readout values as JSON",
  )
  readouts.add_argument("job_id", metavar="JOB_ID")
  readouts.add_argument(
    "--name",
    action="append",
    default=[],
    metavar="READOUT",
    help="only this readout; repeatable (default: all)",
  )
  readouts.add_argument(
    "--full",
    action="store_true",
    help=(
      "whole records (ok, error, at, and the hashes that say which "
      "code produced the number) instead of bare values"
    ),
  )
  readouts.add_argument("--server", default="http://127.0.0.1:7200")

  path_cmd = sub.add_parser(
    "path",
    help="print where to read one instance home",
  )
  path_cmd.add_argument("job_id", metavar="JOB_ID")
  path_cmd.add_argument(
    "ident",
    metavar="TASK_ID|INSTANCE_ID",
    help=(
      "a task id (what the sweep lists, kept across retries) or an "
      "instance id (the directory name); both resolve"
    ),
  )
  path_cmd.add_argument(
    "--mount-base",
    type=Path,
    default=None,
    help="override where this node mounts packs",
  )
  path_cmd.add_argument("--server", default="http://127.0.0.1:7200")

  args = ap.parse_args(argv)

  if args.cmd == "readout":
    from dispatcher.tools.readout_cli import (
      add_readout,
      run_readout,
      set_columns,
    )

    if args.add or args.columns:
      if not args.file:
        ap.error("--add/--columns need --file")
      if args.add:
        return add_readout(
          server=args.server,
          arena=args.target,
          file=args.file,
          name=args.add,
          timeout_sec=args.timeout_sec,
        )
      return set_columns(
        server=args.server, arena=args.target, file=args.file
      )
    return run_readout(
      server=args.server,
      target=args.target,
      names=list(args.name),
    )

  if args.cmd == "readouts":
    from dispatcher.tools.pack_cli import dump_readouts

    return dump_readouts(
      server=args.server,
      job_id=args.job_id,
      names=list(args.name),
      full=args.full,
    )

  if args.cmd == "path":
    from dispatcher.tools.pack_cli import print_path

    return print_path(
      server=args.server,
      job_id=args.job_id,
      ident=args.ident,
      mount_base=args.mount_base,
    )

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
