"""Runtime glue for `dispatcher monitor`: SSE consumer thread +
Rich Live loop + keyboard listener. Pure state/render logic lives
in monitor_client so it tests without a terminal."""

from __future__ import annotations

import select
import sys
import termios
import threading
import time
import tty
from typing import TYPE_CHECKING

import httpx
from rich.live import Live

from dispatcher.monitor_client import (
  MonitorState,
  parse_sse_lines,
  render_compact,
  render_detail,
)

if TYPE_CHECKING:
  from rich.console import Console


_BACKOFFS = [0.5, 1.0, 2.0, 5.0, 10.0]


def _sse_worker(
  server: str,
  state: MonitorState,
  state_lock: threading.Lock,
  stop_flag: threading.Event,
) -> None:
  attempt = 0
  while not stop_flag.is_set():
    try:
      with httpx.Client(timeout=None) as client:
        with client.stream(
          "GET",
          f"{server.rstrip('/')}/monitor/stream",
          headers={"accept": "text/event-stream"},
        ) as response:
          if response.status_code != 200:
            with state_lock:
              state.connected = False
              state.connection_error = f"HTTP {response.status_code}"
          else:
            with state_lock:
              state.connected = True
              state.connection_error = None
            attempt = 0
            for event_type, payload in parse_sse_lines(
              response.iter_lines()
            ):
              if stop_flag.is_set():
                return
              with state_lock:
                state.apply(event_type, payload)
    except (httpx.HTTPError, OSError) as exc:
      with state_lock:
        state.connected = False
        state.connection_error = f"{type(exc).__name__}: {exc}"

    if stop_flag.is_set():
      return
    delay = _BACKOFFS[min(attempt, len(_BACKOFFS) - 1)]
    attempt += 1
    slept = 0.0
    while slept < delay and not stop_flag.is_set():
      time.sleep(0.05)
      slept += 0.05


def _key_poller(stop_flag: threading.Event, poll_timeout: float = 0.1):
  """Raw-mode key reader; RuntimeError on non-TTY (caller falls
  back to Ctrl+C only)."""
  fd = sys.stdin.fileno()
  if not sys.stdin.isatty():
    raise RuntimeError("stdin is not a TTY; key polling disabled")
  old_attrs = termios.tcgetattr(fd)
  try:
    tty.setcbreak(fd)
    while not stop_flag.is_set():
      readable, _, _ = select.select([fd], [], [], poll_timeout)
      if readable:
        try:
          yield sys.stdin.read(1)
        except (OSError, ValueError):
          return
  finally:
    termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)


def run_monitor(
  *,
  server: str,
  detail: str | None,
  refresh_per_second: int,
  console: Console,
) -> None:
  state = MonitorState()
  state_lock = threading.Lock()
  stop_flag = threading.Event()

  worker = threading.Thread(
    target=_sse_worker,
    args=(server, state, state_lock, stop_flag),
    daemon=True,
  )
  worker.start()

  def _snapshot():
    with state_lock:
      if detail is not None:
        return render_detail(state, detail)
      return render_compact(state)

  try:
    with Live(
      _snapshot(),
      console=console,
      refresh_per_second=refresh_per_second,
      screen=False,
    ) as live:
      try:
        for key in _key_poller(stop_flag):
          if key in ("q", "\x03"):
            break
          live.update(_snapshot())
      except RuntimeError:
        while not stop_flag.is_set():
          time.sleep(1.0 / max(1, refresh_per_second))
          live.update(_snapshot())
  except KeyboardInterrupt:
    pass
  finally:
    stop_flag.set()
    worker.join(timeout=1.0)
