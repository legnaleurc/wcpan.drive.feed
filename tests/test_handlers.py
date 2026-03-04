import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from aiohttp.test_utils import AioHTTPTestCase

from wcpan.drive.feed._db import SUPER_ROOT_ID
from wcpan.drive.feed._types import NodeRecord, RemovedChange, UpdatedChange


_NOW = datetime(2026, 3, 4, 12, 0, 0, tzinfo=timezone.utc)


def _make_node(node_id: str = "test-node-id") -> NodeRecord:
    return NodeRecord(
        node_id=node_id,
        parent_id=SUPER_ROOT_ID,
        name="test.txt",
        is_directory=False,
        ctime=_NOW,
        mtime=_NOW,
        mime_type="text/plain",
        hash="abc123",
        size=100,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )


def _make_app_with_mocks(cursor_val=5, changes=None, root_node=None):
    from aiohttp import web

    from wcpan.drive.feed._handlers import handle_changes, handle_cursor, handle_root

    if changes is None:
        changes = []
    if root_node is None:
        root_node = _make_node(SUPER_ROOT_ID)

    off_main = AsyncMock()

    async def mock_off_main(fn, *args, **kwargs):
        from wcpan.drive.feed._db import get_changes_since, get_cursor, get_node_by_id

        if fn is get_cursor:
            return cursor_val
        if fn is get_changes_since:
            c = args[0] if args else 0
            return (changes, max(cursor_val, c))
        if fn is get_node_by_id:
            return root_node
        return None

    off_main.side_effect = mock_off_main

    from wcpan.drive.feed._keys import APP_OFF_MAIN

    app = web.Application()
    app[APP_OFF_MAIN] = off_main
    app.router.add_get("/api/v1/cursor", handle_cursor)
    app.router.add_get("/api/v1/changes", handle_changes)
    app.router.add_get("/api/v1/root", handle_root)
    return app


class TestCursorHandler(AioHTTPTestCase):
    async def get_application(self):
        return _make_app_with_mocks(cursor_val=42)

    async def test_cursor_returns_value(self):
        resp = await self.client.get("/api/v1/cursor")
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(data["cursor"], 42)


class TestChangesHandler(AioHTTPTestCase):
    async def get_application(self):
        node = _make_node("node-001")
        changes = [UpdatedChange(removed=False, node=node)]
        return _make_app_with_mocks(cursor_val=10, changes=changes)

    async def test_changes_returns_list(self):
        resp = await self.client.get("/api/v1/changes?cursor=0")
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertIn("cursor", data)
        self.assertIn("changes", data)
        self.assertEqual(len(data["changes"]), 1)
        self.assertFalse(data["changes"][0]["removed"])
        self.assertEqual(data["changes"][0]["node"]["id"], "node-001")

    async def test_changes_default_cursor(self):
        resp = await self.client.get("/api/v1/changes")
        self.assertEqual(resp.status, 200)

    async def test_invalid_cursor_returns_400(self):
        resp = await self.client.get("/api/v1/changes?cursor=not_a_number")
        self.assertEqual(resp.status, 400)


class TestRootHandler(AioHTTPTestCase):
    async def get_application(self):
        root = NodeRecord(
            node_id=SUPER_ROOT_ID,
            parent_id=None,
            name="",
            is_directory=True,
            ctime=_NOW,
            mtime=_NOW,
            mime_type="",
            hash="",
            size=0,
            is_image=False,
            is_video=False,
            width=0,
            height=0,
            ms_duration=0,
        )
        return _make_app_with_mocks(root_node=root)

    async def test_root_returns_super_root(self):
        resp = await self.client.get("/api/v1/root")
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(data["id"], SUPER_ROOT_ID)
        self.assertIsNone(data["parent_id"])
        self.assertTrue(data["is_directory"])


class TestChangesRemoved(AioHTTPTestCase):
    async def get_application(self):
        changes = [RemovedChange(removed=True, node_id="gone-node")]
        return _make_app_with_mocks(cursor_val=5, changes=changes)

    async def test_removed_change_has_null_node(self):
        resp = await self.client.get("/api/v1/changes?cursor=0")
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(len(data["changes"]), 1)
        self.assertTrue(data["changes"][0]["removed"])
        self.assertNotIn("node", data["changes"][0])


if __name__ == "__main__":
    unittest.main()
