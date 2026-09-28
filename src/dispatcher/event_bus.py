"""In-process pub/sub feeding the SSE stream.

Publishers are synchronous; each subscriber has a bounded buffer
and surplus events are dropped rather than backpressuring the
runtime loop — stream consumers reconcile via heartbeats and
on-reconnect snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import anyio


@dataclass
class Event:
  type: str
  payload: dict[str, Any]


class Subscription:
  """One subscriber's receive side. Iterate with `async for`;
  `close()` unregisters."""

  def __init__(self, bus: EventBus, max_buffer_size: int = 256) -> None:
    self._bus = bus
    self._send, self._recv = anyio.create_memory_object_stream[Event](
      max_buffer_size=max_buffer_size
    )
    self._closed = False

  def push(self, event: Event) -> None:
    if self._closed:
      return
    try:
      self._send.send_nowait(event)
    except anyio.WouldBlock:
      pass  # full — drop rather than block the publisher
    except anyio.BrokenResourceError:
      self._closed = True

  async def get(self) -> Event:
    return await self._recv.receive()

  def __aiter__(self) -> Subscription:
    return self

  async def __anext__(self) -> Event:
    try:
      return await self._recv.receive()
    except (anyio.EndOfStream, anyio.ClosedResourceError) as exc:
      raise StopAsyncIteration from exc

  def close(self) -> None:
    if self._closed:
      return
    self._closed = True
    self._bus._unsubscribe(self)
    self._send.close()
    self._recv.close()


@dataclass
class EventBus:
  _subs: list[Subscription] = field(default_factory=list)

  def subscribe(self, maxsize: int = 256) -> Subscription:
    sub = Subscription(self, max_buffer_size=maxsize)
    self._subs.append(sub)
    return sub

  def _unsubscribe(self, sub: Subscription) -> None:
    if sub in self._subs:
      self._subs.remove(sub)

  def publish(self, event_type: str, payload: dict[str, Any]) -> None:
    ev = Event(type=event_type, payload=payload)
    for sub in list(self._subs):
      sub.push(ev)

  @property
  def subscriber_count(self) -> int:
    return len(self._subs)
