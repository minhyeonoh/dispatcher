"""Incremental per-attempt aggregates over outcome `values`.

Fed by the runtime on every terminal observation, so read paths
(`GET /monitor`, notify, weight tuner) never touch the
filesystem. The dispatcher doesn't know what any value means —
it accumulates per-key sums/counts and serves means.

Counting rules mirror the scheduler's evidence standard:
- no outcome file → no contribution (the trial is `unknown`; a
  resolver adds the contribution when evidence arrives, so `err`
  never inflates on NFS lag).
- outcome with error → err++ only.
- outcome ok → ok++, fold every numeric value.
- ghosted → no contribution ever (nothing to score)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, computed_field

if TYPE_CHECKING:
  from dispatcher.outcome import CompletionSnapshot


class AttemptMetrics(BaseModel):
  ok: int = 0
  err: int = 0
  value_sums: dict[str, float] = Field(default_factory=dict)
  value_counts: dict[str, int] = Field(default_factory=dict)

  @computed_field
  @property
  def means(self) -> dict[str, float]:
    """Per-key mean over the ok trials that reported the key."""
    return {
      k: self.value_sums[k] / n
      for k, n in self.value_counts.items()
      if n > 0
    }

  @computed_field
  @property
  def means_all(self) -> dict[str, float]:
    """Err-inclusive mean: every done_err counts as 0 in the
    denominator. Ghosted stays excluded (no evidence)."""
    return {
      k: self.value_sums[k] / (n + self.err)
      for k, n in self.value_counts.items()
      if n + self.err > 0
    }


@dataclass
class MetricsCache:
  _by_attempt: dict[str, AttemptMetrics] = field(default_factory=dict)

  def record_completion(
    self, attempt_id: str, snapshot: CompletionSnapshot
  ) -> None:
    if not snapshot.outcome_exists:
      return
    m = self._by_attempt.setdefault(attempt_id, AttemptMetrics())
    if snapshot.error_present:
      m.err += 1
    else:
      m.ok += 1
      self._fold_values(m, snapshot.values)

  def reclassify_from_unknown(
    self,
    attempt_id: str,
    *,
    to_state: str,
    values: dict[str, float] | None,
  ) -> None:
    """Pure add — unknown never contributed."""
    self._add_terminal(attempt_id, to_state, values)

  def reclassify_from_ghosted(
    self,
    attempt_id: str,
    *,
    to_state: str,
    values: dict[str, float] | None,
  ) -> None:
    if to_state not in ("done_ok", "done_err"):
      raise ValueError(
        f"reclassify_from_ghosted: to_state must be "
        f"done_ok/done_err, got {to_state!r}"
      )
    self._add_terminal(attempt_id, to_state, values)

  def undo_done_err(self, attempt_id: str) -> None:
    """Roll back one err++ for an operator retry. Floored at
    zero."""
    m = self._by_attempt.get(attempt_id)
    if m is None:
      return
    m.err = max(0, m.err - 1)

  def _add_terminal(
    self,
    attempt_id: str,
    to_state: str,
    values: dict[str, float] | None,
  ) -> None:
    if to_state not in ("done_ok", "done_err"):
      return
    m = self._by_attempt.setdefault(attempt_id, AttemptMetrics())
    if to_state == "done_err":
      m.err += 1
      return
    m.ok += 1
    self._fold_values(m, values)

  @staticmethod
  def _fold_values(
    m: AttemptMetrics, values: dict[str, float] | None
  ) -> None:
    for k, v in (values or {}).items():
      if isinstance(v, bool) or not isinstance(v, (int, float)):
        continue
      m.value_sums[k] = m.value_sums.get(k, 0.0) + float(v)
      m.value_counts[k] = m.value_counts.get(k, 0) + 1

  def get(self, attempt_id: str) -> AttemptMetrics:
    return self._by_attempt.get(attempt_id, AttemptMetrics())

  def remove(self, attempt_id: str) -> None:
    self._by_attempt.pop(attempt_id, None)
