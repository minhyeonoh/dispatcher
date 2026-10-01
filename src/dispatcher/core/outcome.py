"""Terminal-signal reading: `<instance_home>/outcome.json`.

Missing file and malformed JSON both return None — "no scoreable
signal" — so every caller routes them to `unknown` uniformly and
the resolver decides later. Treating a half-written file as an
error would score NFS lag as a failure."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from dispatcher.core.models import OUTCOME_FILENAME, Outcome
from dispatcher.core.pack import bust_dir_cache, instance_home_for
from dispatcher.core.readout import read_instance_readouts

if TYPE_CHECKING:
  from pathlib import Path

  from dispatcher.core.readout import ReadoutValue


@dataclass
class CompletionSnapshot:
  """What one look at an instance's terminal state produced.

  `exit_code` is the main container's exit status when the caller
  learned it from docker (die event / census); None when the only
  evidence is the filesystem."""

  outcome_exists: bool
  error_present: bool
  infra: bool = False
  outcome: Outcome | None = None
  exit_code: int | None = None
  readouts: list[ReadoutValue] = field(default_factory=list)
  """What the worker scored itself, picked up in the same look.

  NOT evidence — nothing here changes how the instance is
  classified. It rides along because this is the one moment the
  instance home is being read anyway, and a second visit later is
  exactly the delay the live path exists to avoid."""


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
  return CompletionSnapshot(
    outcome_exists=True,
    error_present=error_present,
    infra=parsed.infra,
    outcome=parsed,
    # The worker writes its values BEFORE the envelope, so an
    # envelope we can read means they are already on disk.
    readouts=read_instance_readouts(instance_home),
  )


__all__ = [
  "CompletionSnapshot",
  "bust_dir_cache",
  "instance_home_for",
  "read_completion",
]
