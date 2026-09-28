"""EventBus fan-out and drop-don't-block."""

from __future__ import annotations

import anyio

from dispatcher.event_bus import EventBus


def test_subscribe_then_publish_delivers_to_that_sub():
  async def main():
    bus = EventBus()
    sub = bus.subscribe()
    bus.publish("x", {"k": 1})
    ev = await sub.get()
    assert ev.type == "x"
    assert ev.payload == {"k": 1}
    sub.close()

  anyio.run(main)


def test_publish_before_subscribe_is_not_seen():
  async def main():
    bus = EventBus()
    bus.publish("early", {})
    sub = bus.subscribe()
    bus.publish("late", {})
    ev = await sub.get()
    assert ev.type == "late"
    sub.close()

  anyio.run(main)


def test_multiple_subs_all_see_events():
  async def main():
    bus = EventBus()
    s1 = bus.subscribe()
    s2 = bus.subscribe()
    bus.publish("x", {"n": 7})
    assert (await s1.get()).payload == {"n": 7}
    assert (await s2.get()).payload == {"n": 7}
    s1.close()
    s2.close()

  anyio.run(main)


def test_close_removes_the_subscription_from_the_bus():
  async def main():
    bus = EventBus()
    sub = bus.subscribe()
    assert bus.subscriber_count == 1
    sub.close()
    assert bus.subscriber_count == 0
    bus.publish("x", {})  # no crash on closed sub

  anyio.run(main)


def test_full_queue_drops_events_rather_than_blocking_publisher():
  async def main():
    bus = EventBus()
    sub = bus.subscribe(maxsize=2)
    for i in range(5):
      bus.publish("x", {"i": i})
    # Only the first two landed; publisher never blocked.
    assert (await sub.get()).payload == {"i": 0}
    assert (await sub.get()).payload == {"i": 1}
    sub.close()

  anyio.run(main)
