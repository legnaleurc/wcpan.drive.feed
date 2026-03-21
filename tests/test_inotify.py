import asyncio
import unittest
from pathlib import Path

from wcpan.drive.feed._watcher._inotify import events_with_move_timeout


class TestEventsWithMoveTimeout(unittest.IsolatedAsyncioTestCase):
    async def test_passes_through_events(self):
        sentinel = object()

        async def source():
            yield sentinel

        pending_from: dict[int, tuple[Path, bool]] = {}
        result: list[object] = []
        async for event in events_with_move_timeout(
            source(), pending_from, stale_timeout=0.05
        ):
            result.append(event)

        self.assertEqual(result, [sentinel])

    async def test_yields_none_on_timeout_when_pending(self):
        async def never_yields():
            await asyncio.sleep(10)
            return
            yield  # makes this an async generator

        pending_from = {1: (Path("/some/path"), False)}
        result: list[object] = []
        async for event in events_with_move_timeout(
            never_yields(), pending_from, stale_timeout=0.05
        ):
            result.append(event)
            break  # stop after the timeout-injected None

        self.assertEqual(result, [None])
