import asyncio
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from wcpan.drive.feed._db import (
    SUPER_ROOT_ID,
    OffMainThread,
    bulk_delete_nodes,
    bulk_emit_changes,
    bulk_upsert_nodes,
    emit_change,
    get_all_nodes,
    get_changes_since,
    get_cursor,
    get_node_by_id,
    upsert_node,
    upsert_node_and_emit_change,
)
from wcpan.drive.feed._lib import is_updated_change
from wcpan.drive.feed._types import NodeRecord

from ._lib import create_db_sandbox, node_id_from_change


_NOW = datetime(2026, 3, 4, 12, 0, 0, tzinfo=timezone.utc)


def _make_node(
    node_id: str, parent_id: str | None = SUPER_ROOT_ID, name: str = "test.txt"
) -> NodeRecord:
    return NodeRecord(
        node_id=node_id,
        parent_id=parent_id,
        name=name,
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


class TestInitialization(unittest.TestCase):
    def test_super_root_exists(self):
        with create_db_sandbox() as dsn:
            node = get_node_by_id(dsn, SUPER_ROOT_ID)
            self.assertIsNotNone(node)
            assert node
            self.assertEqual(node.node_id, SUPER_ROOT_ID)
            self.assertIsNone(node.parent_id)
            self.assertEqual(node.name, "")
            self.assertTrue(node.is_directory)

    def test_initial_cursor_is_zero(self):
        with create_db_sandbox() as dsn:
            self.assertEqual(get_cursor(dsn), 0)


class TestUpsertNode(unittest.TestCase):
    def test_insert_node(self):
        with create_db_sandbox() as dsn:
            node = _make_node("node-001")
            upsert_node(dsn, node)
            found = get_node_by_id(dsn, "node-001")
            self.assertIsNotNone(found)
            assert found
            self.assertEqual(found.name, "test.txt")
            self.assertEqual(found.hash, "abc123")

    def test_upsert_updates_existing(self):
        with create_db_sandbox() as dsn:
            node = _make_node("node-001")
            upsert_node(dsn, node)
            updated = NodeRecord(
                node_id="node-001",
                parent_id=SUPER_ROOT_ID,
                name="renamed.txt",
                is_directory=False,
                ctime=_NOW,
                mtime=_NOW,
                mime_type="text/plain",
                hash="def456",
                size=200,
                is_image=False,
                is_video=False,
                width=0,
                height=0,
                ms_duration=0,
            )
            upsert_node(dsn, updated)
            found = get_node_by_id(dsn, "node-001")
            assert found
            self.assertEqual(found.name, "renamed.txt")
            self.assertEqual(found.hash, "def456")


class TestChanges(unittest.TestCase):
    def test_emit_and_get_changes(self):
        with create_db_sandbox() as dsn:
            node = _make_node("node-001")
            upsert_node(dsn, node)
            emit_change(dsn, "node-001", is_removed=False)

            changes, new_cursor = get_changes_since(dsn, 0)
            self.assertEqual(len(changes), 1)

            change = changes[0]
            assert is_updated_change(change)
            self.assertFalse(change.removed)
            self.assertIsNotNone(change.node)
            self.assertEqual(change.node.node_id, "node-001")
            self.assertGreater(new_cursor, 0)

    def test_changes_merge_last_wins(self):
        with create_db_sandbox() as dsn:
            node = _make_node("node-001")
            upsert_node(dsn, node)
            emit_change(dsn, "node-001", is_removed=False)
            emit_change(dsn, "node-001", is_removed=False)
            emit_change(dsn, "node-001", is_removed=False)

            changes, _ = get_changes_since(dsn, 0)
            # Should be merged to 1 change
            self.assertEqual(len(changes), 1)

    def test_create_then_delete_skipped(self):
        with create_db_sandbox() as dsn:
            node = _make_node("node-001")
            upsert_node(dsn, node)
            emit_change(dsn, "node-001", is_removed=False)
            # Now delete from DB before querying
            from wcpan.drive.feed._db import delete_node

            delete_node(dsn, "node-001")
            emit_change(dsn, "node-001", is_removed=True)

            changes, _ = get_changes_since(dsn, 0)
            # Last event is is_removed=True → returns removed change
            self.assertEqual(len(changes), 1)
            self.assertTrue(changes[0].removed)

    def test_cursor_advances(self):
        with create_db_sandbox() as dsn:
            node1 = _make_node("node-001", name="a.txt")
            node2 = _make_node("node-002", name="b.txt")
            upsert_node(dsn, node1)
            upsert_node(dsn, node2)
            emit_change(dsn, "node-001", is_removed=False)
            cursor_after_first = get_cursor(dsn)

            emit_change(dsn, "node-002", is_removed=False)
            changes, new_cursor = get_changes_since(dsn, cursor_after_first)
            self.assertEqual(len(changes), 1)

            change = changes[0]
            assert is_updated_change(change)
            self.assertEqual(change.node.node_id, "node-002")
            self.assertGreater(new_cursor, cursor_after_first)

    def test_no_changes_returns_same_cursor(self):
        with create_db_sandbox() as dsn:
            changes, new_cursor = get_changes_since(dsn, 0)
            self.assertEqual(changes, [])
            self.assertEqual(new_cursor, 0)


class TestGetAllNodes(unittest.TestCase):
    def test_returns_super_root(self):
        with create_db_sandbox() as dsn:
            result = get_all_nodes(dsn)
            self.assertIn(SUPER_ROOT_ID, result)
            self.assertEqual(result[SUPER_ROOT_ID].node_id, SUPER_ROOT_ID)

    def test_returns_inserted_nodes_keyed_by_node_id(self):
        with create_db_sandbox() as dsn:
            node1 = _make_node("node-001", name="a.txt")
            node2 = _make_node("node-002", name="b.txt")
            upsert_node(dsn, node1)
            upsert_node(dsn, node2)

            result = get_all_nodes(dsn)
            self.assertIn("node-001", result)
            self.assertIn("node-002", result)
            self.assertEqual(result["node-001"].name, "a.txt")
            self.assertEqual(result["node-002"].name, "b.txt")


class TestBulkUpsertNodes(unittest.TestCase):
    def test_inserts_multiple_nodes(self):
        with create_db_sandbox() as dsn:
            node1 = _make_node("node-001", name="a.txt")
            node2 = _make_node("node-002", name="b.txt")
            bulk_upsert_nodes(dsn, [node1, node2])

            self.assertIsNotNone(get_node_by_id(dsn, "node-001"))
            self.assertIsNotNone(get_node_by_id(dsn, "node-002"))

    def test_updates_existing_nodes(self):
        with create_db_sandbox() as dsn:
            node = _make_node("node-001", name="a.txt")
            upsert_node(dsn, node)

            updated = _make_node("node-001", name="renamed.txt")
            bulk_upsert_nodes(dsn, [updated])

            found = get_node_by_id(dsn, "node-001")
            assert found
            self.assertEqual(found.name, "renamed.txt")

    def test_empty_list_is_noop(self):
        with create_db_sandbox() as dsn:
            bulk_upsert_nodes(dsn, [])  # should not raise


class TestBulkEmitChanges(unittest.TestCase):
    def test_inserts_multiple_changes(self):
        with create_db_sandbox() as dsn:
            node1 = _make_node("node-001", name="a.txt")
            node2 = _make_node("node-002", name="b.txt")
            upsert_node(dsn, node1)
            upsert_node(dsn, node2)

            bulk_emit_changes(dsn, [("node-001", False), ("node-002", True)])

            changes, _ = get_changes_since(dsn, 0)
            node_ids = {node_id_from_change(c) for c in changes}
            self.assertIn("node-001", node_ids)
            self.assertIn("node-002", node_ids)

    def test_empty_list_is_noop(self):
        with create_db_sandbox() as dsn:
            bulk_emit_changes(dsn, [])  # should not raise


def _make_file_node(i: int, parent_id: str = SUPER_ROOT_ID) -> NodeRecord:
    return NodeRecord(
        node_id=str(uuid.uuid4()),
        parent_id=parent_id,
        name=f"file-{i:04d}.txt",
        is_directory=False,
        ctime=_NOW,
        mtime=_NOW,
        mime_type="text/plain",
        hash=f"hash-{i:04d}",
        size=i,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )


class TestConcurrentReadWrite(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_writes_do_not_corrupt(self):
        with create_db_sandbox() as dsn:
            nodes = [_make_file_node(i) for i in range(50)]
            pool = ThreadPoolExecutor(max_workers=4)
            try:
                off_main = OffMainThread(dsn=dsn, pool=pool)
                await asyncio.gather(
                    *(off_main(upsert_node_and_emit_change, node) for node in nodes)
                )
            finally:
                pool.shutdown(wait=True, cancel_futures=True)
            result = get_all_nodes(dsn)
            # 50 inserted nodes + super root
            self.assertEqual(len(result), 51)
            for node in nodes:
                self.assertIn(node.node_id, result)

    async def test_concurrent_reads_and_writes_are_stable(self):
        with create_db_sandbox() as dsn:
            nodes = [_make_file_node(i) for i in range(50)]
            pool = ThreadPoolExecutor(max_workers=4)
            try:
                off_main = OffMainThread(dsn=dsn, pool=pool)
                writes = [off_main(upsert_node_and_emit_change, node) for node in nodes]
                reads = [off_main(get_all_nodes) for _ in range(10)]
                tasks = writes + reads
                await asyncio.gather(*tasks)
            finally:
                pool.shutdown(wait=True, cancel_futures=True)
            result = get_all_nodes(dsn)
            self.assertEqual(len(result), 51)
            for node in nodes:
                self.assertIn(node.node_id, result)


class TestBulkDeleteNodes(unittest.TestCase):
    def test_deletes_multiple_nodes(self):
        with create_db_sandbox() as dsn:
            node1 = _make_node("node-001", name="a.txt")
            node2 = _make_node("node-002", name="b.txt")
            upsert_node(dsn, node1)
            upsert_node(dsn, node2)

            bulk_delete_nodes(dsn, ["node-001", "node-002"])

            self.assertIsNone(get_node_by_id(dsn, "node-001"))
            self.assertIsNone(get_node_by_id(dsn, "node-002"))

    def test_empty_list_is_noop(self):
        with create_db_sandbox() as dsn:
            bulk_delete_nodes(dsn, [])  # should not raise
