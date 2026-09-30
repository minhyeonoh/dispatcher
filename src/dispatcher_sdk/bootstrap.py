"""Container entrypoint shim: unpack the frozen source archive,
then exec the real command.

    command: ["python", "-m", "dispatcher_sdk.bootstrap", "--",
              "python", "-m", "myrepo.worker"]

Reads `$DISPATCHER_SOURCE` (the ro-mounted `.source.tar`), untars
it to container-LOCAL fs (`$DISPATCHER_SOURCE_DEST`, default
/dispatcher/src) so imports never touch the shared filesystem,
prepends the dest to PYTHONPATH, and exec's the remaining argv.

No source env set → straight exec (image-only repos can still
use the shim unconditionally). Source declared but unreadable →
exit 75: that's the machine (NFS) failing, not the work, and the
dispatcher requeues it."""

from __future__ import annotations

import os
import sys
import tarfile
import time

EX_INFRA = 75

_DEFAULT_DEST = "/dispatcher/src"
_READ_RETRIES = (0.5, 1.0, 2.0)


def _unpack(src: str, dest: str) -> None:
  last: Exception | None = None
  for delay in (0.0, *_READ_RETRIES):
    if delay:
      time.sleep(delay)
    try:
      with tarfile.open(src) as tf:
        tf.extractall(dest, filter="data")
      return
    except (OSError, tarfile.TarError) as exc:
      last = exc
  raise RuntimeError(f"source archive unreadable: {src}: {last}")


def main(argv: list[str] | None = None) -> int:
  args = list(sys.argv[1:] if argv is None else argv)
  if args and args[0] == "--":
    args = args[1:]
  if not args:
    print(
      "usage: python -m dispatcher_sdk.bootstrap -- CMD [ARG…]",
      file=sys.stderr,
    )
    return 2
  src = os.environ.get("DISPATCHER_SOURCE", "")
  if src:
    dest = os.environ.get("DISPATCHER_SOURCE_DEST", _DEFAULT_DEST)
    try:
      _unpack(src, dest)
    except RuntimeError as exc:
      print(f"dispatcher_sdk.bootstrap: {exc}", file=sys.stderr)
      return EX_INFRA
    os.environ["PYTHONPATH"] = (
      dest + os.pathsep + os.environ.get("PYTHONPATH", "")
    ).rstrip(os.pathsep)
  os.execvp(args[0], args)
  return 0  # pragma: no cover — execvp does not return


if __name__ == "__main__":
  sys.exit(main())
