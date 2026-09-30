"""Terminal-signal reading: `<instance_home>/outcome.json`.

Missing file and malformed JSON both return None — "no scoreable
signal" — so every caller routes them to `unknown` uniformly and
the resolver decides later. Treating a half-written file as an
error would score NFS lag as a failure."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from dispatcher.core.models import OUTCOME_FILENAME, Outcome

if TYPE_CHECKING:
  from pathlib import Path


@dataclass
class CompletionSnapshot:
  """What one look at an instance's terminal state produced.

  `exit_code` is the main container's exit status when the caller
  learned it from docker (die event / census); None when the only
  evidence is the filesystem."""

  outcome_exists: bool
  error_present: bool
  infra: bool = False
  values: dict[str, float] = field(default_factory=dict)
  outcome: Outcome | None = None
  exit_code: int | None = None


def read_completion(instance_home: Path) -> CompletionSnapshot | None:
  path = instance_home / OUTCOME_FILENAME
  try:
    raw = path.read_text(encoding="utf-8")
  except OSError:
    # Missing, TOCTOU-removed, permission, NFS IO — all the same
    # non-answer.
    return None
  try:
    parsed = Outcome.model_validate_json(raw)
  except Exception:
    return None
  # Belt-and-suspenders: an envelope claiming ok while carrying an
  # error object is contradictory — read it as an error rather
  # than trust the flag.
  error_present = (not parsed.ok) or parsed.error is not None
  values = {
    k: float(v)
    for k, v in parsed.values.items()
    if isinstance(v, (int, float)) and not isinstance(v, bool)
  }
  return CompletionSnapshot(
    outcome_exists=True,
    error_present=error_present,
    infra=parsed.infra,
    values=values,
    outcome=parsed,
  )


def bust_dir_cache(instance_home: Path) -> None:
  """Invalidate THIS client's NFS cache for an instance home we own.

  The outcome is written by a container on another host; this
  client's negative-dentry / directory-attribute cache keeps
  answering "no such file" for up to acdirmax (default 60s) —
  measured on the old router as 43% of all instances parking in
  unknown while the file already existed. Writing into the
  directory ourselves bumps its mtime locally, so the kernel
  discards its own cache and the next lookup goes to the server.
  Best-effort: any failure just leaves today's behaviour (miss →
  resolver later)."""
  probe = instance_home / ".nfs-probe"
  try:
    probe.touch()
    probe.unlink(missing_ok=True)
  except OSError:
    pass


def instance_home_for(home_root: Path, instance_id: str) -> Path:
  return home_root / instance_id
