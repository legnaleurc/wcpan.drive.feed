import asyncio
import unittest
from datetime import datetime, timezone
from pathlib import Path

from wcpan.drive.feed._app import _scan_directory
from wcpan.drive.feed._db import (
    SUPER_ROOT_ID,
    emit_change,
    get_changes_since,
    get_node_by_id,
    node_id_from_stat,
    upsert_node,
)
from wcpan.drive.feed._lib import is_removed_change
from wcpan.drive.feed._types import MetadataQueue, NodeRecord, WriteQueue
from wcpan.drive.feed._watcher._lib import (
    events_with_move_timeout,
    flush_pending_moves,
    on_close_write,
    on_delete,
    on_dir_created,
    on_file_stub,
    on_move,
)

from ._lib import create_db_sandbox, create_fs_sandbox, node_id_from_change


def _make_off_main():
    from concurrent.futures import ThreadPoolExecutor

    from wcpan.drive.feed._lib import OffMainThread

    pool = ThreadPoolExecutor(max_workers=1)
    return OffMainThread(pool), pool


def _make_storage(dsn: str):
    from wcpan.drive.feed._db import Storage

    return Storage(dsn)


def _make_write_queue() -> WriteQueue:
    return asyncio.Queue()


async def _drain_write_queue(wq: WriteQueue) -> None:
    """Run all queued write tasks synchronously (for test assertions)."""
    while not wq.empty():
        task = wq.get_nowait()
        task()
        wq.task_done()


def _insert_dir_node(dsn: str, path: Path, parent_id: str) -> str:
    st = path.stat()
    node_id = node_id_from_stat(st)
    node = NodeRecord(
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
    )
    upsert_node(dsn, node)
    return node_id


def _insert_file_node(dsn: str, path: Path, parent_id: str) -> str:
    st = path.stat()
    node_id = node_id_from_stat(st)
    node = NodeRecord(
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
    )
    upsert_node(dsn, node)
    return node_id


class TestOnFileStub(unittest.IsolatedAsyncioTestCase):
    async def test_creates_stub_node(self):
        """on_file_stub queues a pending node for metadata — does NOT write to DB."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "hello.txt"
            f.write_text("hello")
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()

            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            await on_file_stub(f, storage, off_main, mq)

            # File stays out of DB until metadata worker runs
            node = get_node_by_id(dsn, node_id_from_stat(f.stat()))
            self.assertIsNone(node)
            # Pending node is queued for metadata computation
            self.assertFalse(mq.empty())
            pool.shutdown(wait=False)

    async def test_no_change_emitted(self):
        """File stubs are silent — change is emitted only after metadata completes."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "hello.txt"
            f.write_text("hello")
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()

            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            await on_file_stub(f, storage, off_main, mq)

            changes, _ = get_changes_since(dsn, 0)
            node_ids = {node_id_from_change(c) for c in changes}
            self.assertNotIn(node_id_from_stat(f.stat()), node_ids)
            pool.shutdown(wait=False)

    async def test_missing_file_is_ignored(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            await on_file_stub(tmp / "nonexistent.txt", storage, off_main, mq)
            pool.shutdown(wait=False)

    async def test_parent_not_in_db_is_ignored(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            sub = tmp / "subdir"
            sub.mkdir()
            f = sub / "hello.txt"
            f.write_text("hello")
            # tmp is NOT in DB — only SUPER_ROOT is
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            await on_file_stub(f, storage, off_main, mq)

            self.assertTrue(mq.empty())
            pool.shutdown(wait=False)


class TestOnCloseWrite(unittest.IsolatedAsyncioTestCase):
    async def test_updates_existing_node(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "data.txt"
            f.write_text("v1")
            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            file_id = _insert_file_node(dsn, f, parent_id)

            f.write_text("v2")
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            await on_close_write(f, storage, off_main, mq)

            # Node is queued for metadata — no DB write yet
            self.assertFalse(mq.empty())
            # Change is emitted by the metadata worker, not on_close_write itself
            changes, _ = get_changes_since(dsn, 0)
            node_ids = {node_id_from_change(c) for c in changes}
            self.assertNotIn(file_id, node_ids)
            pool.shutdown(wait=False)

    async def test_inserts_stub_if_node_missing(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "new.txt"
            f.write_text("content")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            await on_close_write(f, storage, off_main, mq)

            # Node is queued for metadata — no DB write yet
            self.assertFalse(mq.empty())
            pool.shutdown(wait=False)


class TestOnDelete(unittest.IsolatedAsyncioTestCase):
    async def test_delete_file_emits_remove(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "bye.txt"
            f.write_text("bye")
            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            file_id = _insert_file_node(dsn, f, parent_id)
            f.unlink()

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            await on_delete(f, False, storage, off_main, wq)
            await _drain_write_queue(wq)

            changes, _ = get_changes_since(dsn, 0)
            removed = [
                c for c in changes if is_removed_change(c) and c.node_id == file_id
            ]
            self.assertEqual(len(removed), 1)
            pool.shutdown(wait=False)

    async def test_delete_dir_recursive(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            subdir = tmp / "subdir"
            subdir.mkdir()
            f = subdir / "child.txt"
            f.write_text("hi")

            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            dir_id = _insert_dir_node(dsn, subdir, parent_id)
            file_id = _insert_file_node(dsn, f, dir_id)
            emit_change(dsn, dir_id, is_removed=False)
            emit_change(dsn, file_id, is_removed=False)

            f.unlink()
            subdir.rmdir()

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            await on_delete(subdir, True, storage, off_main, wq)
            await _drain_write_queue(wq)

            changes, _ = get_changes_since(dsn, 0)
            removed_ids = {c.node_id for c in changes if is_removed_change(c)}
            self.assertIn(dir_id, removed_ids)
            self.assertIn(file_id, removed_ids)
            pool.shutdown(wait=False)


class TestOnMove(unittest.IsolatedAsyncioTestCase):
    async def test_rename_updates_name(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            src = tmp / "orig.txt"
            src.write_text("content")
            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            file_id = _insert_file_node(dsn, src, parent_id)

            dst = tmp / "renamed.txt"
            src.rename(dst)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            await on_move(src, dst, False, storage, off_main, wq)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, file_id)
            assert node
            self.assertEqual(node.name, "renamed.txt")
            self.assertEqual(node.hash, "old_hash")  # unchanged on move
            pool.shutdown(wait=False)

    async def test_move_dir_updates_db(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            src = tmp / "srcdir"
            src.mkdir()
            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            dir_id = _insert_dir_node(dsn, src, parent_id)

            dst = tmp / "dstdir"
            src.rename(dst)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            await on_move(src, dst, True, storage, off_main, wq)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, dir_id)
            assert node
            self.assertEqual(node.name, "dstdir")
            pool.shutdown(wait=False)

    async def test_untracked_source_returns_false(self):
        """Moving an untracked file (e.g. excluded temp) returns False so caller
        can treat the destination as a new arrival."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            src = tmp / "._tmp_upload"
            src.write_text("content")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            # src is NOT inserted into DB (simulates an excluded temp file)

            dst = tmp / "final.txt"
            src.rename(dst)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            result = await on_move(src, dst, False, storage, off_main, wq)

            self.assertFalse(result)
            # dst should also not be in DB — caller is responsible for inserting it
            self.assertIsNone(get_node_by_id(dsn, node_id_from_stat(dst.stat())))
            pool.shutdown(wait=False)

    async def test_move_to_unknown_parent_is_ignored(self):
        """Moving to a destination whose parent is not in DB emits nothing."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            outside = tmp / "outside"
            outside.mkdir()
            src = outside / "srcdir"
            src.mkdir()
            # outside is NOT in DB; only tmp is
            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            dir_id = _insert_dir_node(dsn, src, parent_id)

            dst = outside / "dstdir"
            src.rename(dst)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            await on_move(src, dst, True, storage, off_main, wq)
            await _drain_write_queue(wq)

            changes, _ = get_changes_since(dsn, 0)
            node_ids = {node_id_from_change(c) for c in changes}
            self.assertNotIn(dir_id, node_ids)
            pool.shutdown(wait=False)


class TestOnDirCreated(unittest.IsolatedAsyncioTestCase):
    async def test_creates_node(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            new_dir = tmp / "newdir"
            new_dir.mkdir()
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            await on_dir_created(
                new_dir, storage, off_main, mq, wq, scan_contents=False
            )
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, node_id_from_stat(new_dir.stat()))
            self.assertIsNotNone(node)
            assert node
            self.assertTrue(node.is_directory)
            pool.shutdown(wait=False)

    async def test_emits_change(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            new_dir = tmp / "newdir"
            new_dir.mkdir()
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            await on_dir_created(
                new_dir, storage, off_main, mq, wq, scan_contents=False
            )
            await _drain_write_queue(wq)

            dir_id = node_id_from_stat(new_dir.stat())
            changes, _ = get_changes_since(dsn, 0)
            node_ids = {node_id_from_change(c) for c in changes}
            self.assertIn(dir_id, node_ids)
            pool.shutdown(wait=False)

    async def test_scan_contents_on_move_in(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            moved_in = tmp / "moved"
            moved_in.mkdir()
            (moved_in / "file.txt").write_text("data")

            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            await on_dir_created(
                moved_in, storage, off_main, mq, wq, scan_contents=True
            )
            await _drain_write_queue(wq)

            self.assertFalse(mq.empty())
            pool.shutdown(wait=False)


class TestExcludeOnStartup(unittest.IsolatedAsyncioTestCase):
    async def test_excluded_dir_skipped(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()

            root_id = node_id_from_stat(tmp.stat())
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
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
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            await _scan_directory(storage, off_main, tmp, root_id, wq, mq)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, node_id_from_stat(thumb.stat()))
            self.assertIsNone(node)
            pool.shutdown(wait=False)


class TestExcludeOnEvents(unittest.IsolatedAsyncioTestCase):
    async def test_file_stub_excluded_name(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "Thumbs.db"
            f.write_bytes(b"")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            await on_file_stub(f, storage, off_main, mq)

            self.assertTrue(mq.empty())
            pool.shutdown(wait=False)

    async def test_dir_created_excluded_name(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            await on_dir_created(ea_dir, storage, off_main, mq, wq, scan_contents=False)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, node_id_from_stat(ea_dir.stat()))
            self.assertIsNone(node)
            pool.shutdown(wait=False)

    async def test_close_write_excluded_name(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "desktop.ini"
            f.write_text("[autorun]")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            await on_close_write(f, storage, off_main, mq)

            self.assertTrue(mq.empty())
            pool.shutdown(wait=False)

    async def test_move_to_excluded_name(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            src = tmp / "mydir"
            src.mkdir()
            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            dir_id = _insert_dir_node(dsn, src, parent_id)

            dst = tmp / "@eaDir"
            src.rename(dst)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            await on_move(src, dst, True, storage, off_main, wq)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, dir_id)
            self.assertIsNotNone(node)
            assert node
            self.assertEqual(node.name, "mydir")
            pool.shutdown(wait=False)


class TestFlushPendingMoves(unittest.IsolatedAsyncioTestCase):
    async def test_flushes_stale_move_as_delete(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "moved.txt"
            f.write_text("data")
            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            file_id = _insert_file_node(dsn, f, parent_id)
            f.unlink()

            pending_from = {1: (f, False)}
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            await flush_pending_moves(pending_from, storage, off_main, wq)
            await _drain_write_queue(wq)

            self.assertEqual(pending_from, {})
            changes, _ = get_changes_since(dsn, 0)
            removed = [
                c for c in changes if is_removed_change(c) and c.node_id == file_id
            ]
            self.assertEqual(len(removed), 1)
            pool.shutdown(wait=False)

    async def test_empty_pending_is_noop(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as _:
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            pending_from: dict[int, tuple[Path, bool]] = {}
            await flush_pending_moves(pending_from, storage, off_main, wq)
            self.assertEqual(pending_from, {})
            pool.shutdown(wait=False)


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


class TestExcludeUnderExcludedFolder(unittest.IsolatedAsyncioTestCase):
    """These tests expose the orphaned-node bug: events for items whose parent
    is an excluded (and therefore absent) directory still insert a node with
    parent_id=NULL instead of being silently dropped."""

    async def test_file_stub_under_excluded_dir(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()
            thumb = ea_dir / "thumbnail.jpg"
            thumb.write_bytes(b"")
            # @eaDir is on disk but NOT in DB; only tmp is in DB
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            await on_file_stub(thumb, storage, off_main, mq)

            # Parent not in DB → ignored
            self.assertTrue(mq.empty())
            pool.shutdown(wait=False)

    async def test_dir_created_under_excluded_dir(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()
            sub = ea_dir / "sub"
            sub.mkdir()
            # @eaDir is on disk but NOT in DB; only tmp is in DB
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            await on_dir_created(sub, storage, off_main, mq, wq, scan_contents=False)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, node_id_from_stat(sub.stat()))
            self.assertIsNone(
                node
            )  # currently fails: node inserted with parent_id=NULL
            pool.shutdown(wait=False)

    async def test_close_write_under_excluded_dir(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()
            thumb = ea_dir / "thumbnail.jpg"
            thumb.write_bytes(b"")
            # @eaDir is on disk but NOT in DB; only tmp is in DB
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            await on_close_write(thumb, storage, off_main, mq)

            # Parent not in DB → ignored
            self.assertTrue(mq.empty())
            pool.shutdown(wait=False)
