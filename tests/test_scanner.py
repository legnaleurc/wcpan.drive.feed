import asyncio
import unittest
from datetime import datetime, timezone

from wcpan.drive.feed._db import (
    Storage,
    get_changes_since,
    get_node_by_id,
    node_id_from_stat,
    upsert_node,
)
from wcpan.drive.feed._lib import is_removed_change
from wcpan.drive.feed._scanner import _scan_directory
from wcpan.drive.feed._types import MetadataQueue, NodeRecord, WriteQueue

from ._lib import create_db_sandbox, create_fs_sandbox


def _insert_dir_node(dsn: str, path, parent_id: str) -> str:
    st = path.stat()
    node_id = node_id_from_stat(st)
    upsert_node(
        dsn,
        NodeRecord(
            node_id=node_id,
            parent_id=parent_id,
            name=path.name,
            is_directory=True,
            ctime=datetime.fromtimestamp(st.st_ctime, tz=timezone.utc),
            mtime=datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
            mime_type="",
            hash="",
            size=0,
            is_image=False,
            is_video=False,
            width=0,
            height=0,
            ms_duration=0,
        ),
    )
    return node_id


def _insert_file_node(dsn: str, path, parent_id: str) -> str:
    st = path.stat()
    node_id = node_id_from_stat(st)
    upsert_node(
        dsn,
        NodeRecord(
            node_id=node_id,
            parent_id=parent_id,
            name=path.name,
            is_directory=False,
            ctime=datetime.fromtimestamp(st.st_ctime, tz=timezone.utc),
            mtime=datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
            mime_type="text/plain",
            hash="old_hash",
            size=st.st_size,
            is_image=False,
            is_video=False,
            width=0,
            height=0,
            ms_duration=0,
        ),
    )
    return node_id


def _make_off_main():
    from concurrent.futures import ThreadPoolExecutor

    from wcpan.drive.feed._lib import OffMainThread

    pool = ThreadPoolExecutor(max_workers=1)
    return OffMainThread(pool), pool


def _make_storage(dsn: str):
    return Storage(dsn)


def _make_write_queue() -> WriteQueue:
    return asyncio.Queue()


def _make_metadata_queue() -> MetadataQueue:
    return asyncio.Queue()


async def _drain_write_queue(wq: WriteQueue) -> None:
    while not wq.empty():
        task = wq.get_nowait()
        task()
        wq.task_done()


class TestExcludeOnStartup(unittest.IsolatedAsyncioTestCase):
    async def test_excluded_dir_skipped(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()

            root_id = node_id_from_stat(tmp.stat())
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq = _make_metadata_queue()
            wq = _make_write_queue()
            await _scan_directory(storage, off_main, tmp, root_id, wq, mq)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, node_id_from_stat(ea_dir.stat()))
            self.assertIsNone(node)
            pool.shutdown(wait=False)

    async def test_files_under_excluded_dir_skipped(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()
            thumb = ea_dir / "thumb.jpg"
            thumb.write_bytes(b"")

            root_id = node_id_from_stat(tmp.stat())
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq = _make_metadata_queue()
            wq = _make_write_queue()
            await _scan_directory(storage, off_main, tmp, root_id, wq, mq)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, node_id_from_stat(thumb.stat()))
            self.assertIsNone(node)
            pool.shutdown(wait=False)


class TestDeletedDirCleansSubtree(unittest.IsolatedAsyncioTestCase):
    async def test_deleted_dir_removes_subtree_from_db_and_changes(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            subdir = tmp / "subdir"
            subdir.mkdir()
            f = subdir / "child.txt"
            f.write_text("hi")

            root_id = node_id_from_stat(tmp.stat())
            dir_id = _insert_dir_node(dsn, subdir, root_id)
            file_id = _insert_file_node(dsn, f, dir_id)

            # Delete the entire subtree from the filesystem before the scan
            f.unlink()
            subdir.rmdir()

            storage = _make_storage(dsn)
            off_main, pool = _make_off_main()
            mq = _make_metadata_queue()
            wq = _make_write_queue()
            await _scan_directory(storage, off_main, tmp, root_id, wq, mq)
            await _drain_write_queue(wq)

            # Both nodes must be removed from the DB
            self.assertIsNone(get_node_by_id(dsn, dir_id))
            self.assertIsNone(get_node_by_id(dsn, file_id))

            # Removal changes must be emitted for both
            changes, _ = get_changes_since(dsn, 0)
            removed_ids = {c.node_id for c in changes if is_removed_change(c)}
            self.assertIn(dir_id, removed_ids)
            self.assertIn(file_id, removed_ids)
            pool.shutdown(wait=False)
