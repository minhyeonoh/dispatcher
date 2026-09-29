"""One timezone for the whole system: Asia/Seoul.

Every timestamp the dispatcher mints — event logs, wire
responses, attempt ids, notify records — is timezone-AWARE KST,
so ISO strings carry +09:00 and stay unambiguous. Timestamps
read from outside (docker's RFC3339, file mtimes, docker-events
epoch seconds) are converted to KST at the boundary. Arithmetic
is safe regardless: aware datetimes compare correctly across
offsets, so old UTC-stamped logs keep replaying fine."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def now() -> datetime:
  return datetime.now(KST)


def from_timestamp(ts: float) -> datetime:
  return datetime.fromtimestamp(ts, tz=KST)


def to_kst(dt: datetime) -> datetime:
  """Aware datetime → KST. Naive input is a bug upstream."""
  return dt.astimezone(KST)
