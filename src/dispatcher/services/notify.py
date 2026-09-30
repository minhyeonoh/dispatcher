"""Attempt-progress notifications to Telegram.

Fires when an attempt's evidence-based completion ratio
(done_ok + done_err over task_ids; unknown/ghosted excluded —
no evidence, no announcement) first crosses a threshold. Fired
thresholds persist as `notify_fired` events so restarts never
re-announce.

Bot token is env-only (TELEGRAM_BOT_TOKEN) — it must never ride
the settings blob any GET could read."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

import httpx
from pydantic import BaseModel, ConfigDict, Field

from dispatcher.core import clock
from dispatcher.core.event_log import (
  append_event_async,
  event_log_path_for,
)

if TYPE_CHECKING:
  from dispatcher.core.event_bus import EventBus
  from dispatcher.core.metrics import MetricsCache
  from dispatcher.core.models import AttemptState, AttemptView
  from dispatcher.core.scheduler import Scheduler


logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"


class NotifySettings(BaseModel):
  """Live-patchable; the manager re-reads every field per event."""

  enabled: bool = True
  thresholds: list[float] = Field(default_factory=list)
  telegram_chat_id: str = ""


class NotifyPatch(BaseModel):
  model_config = ConfigDict(extra="forbid")

  enabled: bool | None = None
  thresholds: list[float] | None = None
  telegram_chat_id: str | None = None


def apply_patch(settings: NotifySettings, patch: NotifyPatch) -> None:
  if patch.enabled is not None:
    settings.enabled = patch.enabled
  if patch.thresholds is not None:
    settings.thresholds = [float(t) for t in patch.thresholds]
  if patch.telegram_chat_id is not None:
    settings.telegram_chat_id = patch.telegram_chat_id


class TelegramSender:
  """Missing token → no-op; failures logged and swallowed so a
  Telegram outage never reaches the event loop."""

  def __init__(self, bot_token: str | None) -> None:
    self._bot_token = (bot_token or "").strip()
    self._client: httpx.AsyncClient | None = None
    self._closed = False

  @property
  def has_token(self) -> bool:
    return bool(self._bot_token)

  async def send(self, chat_id: str, text: str) -> None:
    if self._closed or not self._bot_token or not chat_id:
      return
    if self._client is None:
      self._client = httpx.AsyncClient(timeout=10.0)
    url = f"https://api.telegram.org/bot{self._bot_token}/sendMessage"
    payload = {
      "chat_id": chat_id,
      "text": text,
      "disable_web_page_preview": True,
    }
    try:
      resp = await self._client.post(url, json=payload)
      if resp.status_code >= 400:
        logger.warning(
          "telegram send failed status=%d body=%s",
          resp.status_code,
          resp.text[:300],
        )
    except Exception:
      logger.exception("telegram send raised")

  async def close(self) -> None:
    self._closed = True
    if self._client is not None:
      try:
        await self._client.aclose()
      except Exception:
        logger.exception("telegram client close raised")
      self._client = None


class NotifyManager:
  """Bus subscriber: threshold check on each terminal event.
  Persist FIRST, then send — a crash between the two misses one
  message rather than double-sending."""

  def __init__(
    self,
    *,
    bus: EventBus,
    scheduler: Scheduler,
    config: NotifySettings,
    sender: TelegramSender,
    metrics: MetricsCache | None = None,
  ) -> None:
    self._bus = bus
    self._sched = scheduler
    self._config = config
    self._sender = sender
    self._metrics = metrics
    self._send_tasks: set[asyncio.Task[None]] = set()

  async def run(self) -> None:
    sub = self._bus.subscribe(maxsize=1024)
    try:
      async for ev in sub:
        if ev.type not in (
          "trial_completed",
          "trial_reclassified",
        ):
          continue
        aid = ev.payload.get("attempt_id")
        if not isinstance(aid, str):
          continue
        try:
          await self.check(aid)
        except Exception:
          logger.exception("notify check failed attempt=%s", aid)
    finally:
      sub.close()
      if self._send_tasks:
        try:
          await asyncio.wait_for(
            asyncio.gather(*self._send_tasks, return_exceptions=True),
            timeout=5.0,
          )
        except TimeoutError:
          logger.warning(
            "notify: %d send task(s) unfinished; abandoning",
            len(self._send_tasks),
          )

  async def check(self, aid: str) -> None:
    try:
      state = self._sched.attempt_state(aid)
    except KeyError:
      return  # cancelled between event and lookup
    total = len(state.task_ids)
    if total <= 0:
      return
    view = self._sched.attempt_view(aid)
    done = len(view.done_ok) + len(view.done_err)
    ratio = done / total
    thresholds = sorted(
      float(t) for t in self._config.thresholds if 0.0 < float(t) <= 1.0
    )
    for t in thresholds:
      # 1e-9 slop absorbs float drift when done/total == t.
      if ratio + 1e-9 < t:
        break
      if t in state.notified_thresholds:
        continue
      await self._fire(state, view, threshold=t, done=done, total=total)

  async def _fire(
    self,
    state: AttemptState,
    view: AttemptView,
    *,
    threshold: float,
    done: int,
    total: int,
  ) -> None:
    fired_at = clock.now().isoformat()
    try:
      await append_event_async(
        event_log_path_for(state),
        {
          "type": "notify_fired",
          "attempt_id": state.attempt_id,
          "threshold": threshold,
          "fired_at": fired_at,
        },
      )
    except OSError:
      logger.exception(
        "notify_fired append failed attempt=%s threshold=%.4f",
        state.attempt_id,
        threshold,
      )
      return
    state.notified_thresholds.append(float(threshold))
    if not self._config.enabled:
      return
    chat_id = (self._config.telegram_chat_id or "").strip()
    if not chat_id or not self._sender.has_token:
      return
    text = self._format_message(
      state, view, threshold=threshold, done=done, total=total
    )
    task = asyncio.create_task(self._sender.send(chat_id, text))
    self._send_tasks.add(task)
    task.add_done_callback(self._send_tasks.discard)

  def _format_message(
    self,
    state: AttemptState,
    view: AttemptView,
    *,
    threshold: float,
    done: int,
    total: int,
  ) -> str:
    pct_threshold = int(round(threshold * 100))
    pct_actual = int(round(done / total * 100)) if total else 0
    lines = [
      f"[dispatcher] {state.alias} · {state.label}",
      f"progress: {done}/{total} ({pct_actual}%) ≥ {pct_threshold}%",
      f"done_ok/err: {len(view.done_ok)}/{len(view.done_err)}",
    ]
    if self._metrics is not None:
      try:
        m = self._metrics.get(state.attempt_id)
      except Exception:
        m = None
      if m is not None:
        for key, mean in sorted(m.means.items()):
          lines.append(
            f"{key}: {mean:.3f} over {m.value_counts[key]} scored"
          )
    return "\n".join(lines)


def telegram_bot_token_from_env() -> str | None:
  v = os.environ.get(TELEGRAM_BOT_TOKEN_ENV)
  return v if v else None
