import asyncio
import unittest
from datetime import datetime, timezone
from pathlib import Path

from wcpan.drive.feed._db import (
    SUPER_ROOT_ID,
    emit_change,
    get_changes_since,
    get_node_by_id,
    node_id_from_stat,
    upsert_node,
)
from wcpan.drive.feed._lib import is_removed_change
from wcpan.drive.feed._scanner import _scan_directory
from wcpan.drive.feed._types import MetadataQueue, NodeRecord, WriteQueue
from wcpan.drive.feed._watcher._lib import (
    WatcherHandlers,
    events_with_move_timeout,
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
            wq = _make_write_queue()

            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_file_stub(f)

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
            wq = _make_write_queue()

            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_file_stub(f)

            changes, _ = get_changes_since(dsn, 0)
            node_ids = {node_id_from_change(c) for c in changes}
            self.assertNotIn(node_id_from_stat(f.stat()), node_ids)
            pool.shutdown(wait=False)

    async def test_missing_file_is_ignored(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_file_stub(tmp / "nonexistent.txt")
            pool.shutdown(wait=False)

    async def test_parent_not_in_db_queues_for_deferred_check(self):
        """Parent DB check is deferred to write time; on_file_stub still queues the node."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            sub = tmp / "subdir"
            sub.mkdir()
            f = sub / "hello.txt"
            f.write_text("hello")
            # tmp is NOT in DB — only SUPER_ROOT is
            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_file_stub(f)

            # File is still queued — parent check happens in write_worker
            self.assertFalse(mq.empty())
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
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_close_write(f)

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
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_close_write(f)

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
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_delete(f, False)
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
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_delete(subdir, True)
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
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_move(src, dst, False)
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
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_move(src, dst, True)
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
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            result = await handlers.on_move(src, dst, False)

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
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_move(src, dst, True)
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
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_dir_created(new_dir, False)
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
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_dir_created(new_dir, False)
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
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_dir_created(moved_in, True)
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
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_file_stub(f)

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
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_dir_created(ea_dir, False)
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
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_close_write(f)

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
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_move(src, dst, True)
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
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.flush_pending_moves(pending_from)
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
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            pending_from: dict[int, tuple[Path, bool]] = {}
            await handlers.flush_pending_moves(pending_from)
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
        """File under excluded dir is queued to mq; write is dropped because parent not in DB."""
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
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_file_stub(thumb)

            # File is queued — parent check (and drop) happens in write_worker
            self.assertFalse(mq.empty())
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
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_dir_created(sub, False)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, node_id_from_stat(sub.stat()))
            self.assertIsNone(node)
            pool.shutdown(wait=False)

    async def test_close_write_under_excluded_dir(self):
        """File under excluded dir is queued to mq; write is dropped because parent not in DB."""
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
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_close_write(thumb)

            # File is queued — parent check (and drop) happens in write_worker
            self.assertFalse(mq.empty())
            pool.shutdown(wait=False)


class TestIgnoreFileCreate(unittest.IsolatedAsyncioTestCase):
    async def test_file_create_event_ignored(self):
        """CREATE for a file is a no-op — nothing goes on write_queue or metadata_queue."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "new.txt"
            f.write_text("partial")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()

            # Simulate the CREATE path: only call on_dir_created for dirs; for
            # files do nothing (as per the fixed _inotify.py handler).
            # We verify that neither queue receives anything.
            self.assertTrue(mq.empty())
            self.assertTrue(wq.empty())
            pool.shutdown(wait=False)


class TestTmpExclude(unittest.IsolatedAsyncioTestCase):
    async def test_close_write_on_tmp_is_ignored(self):
        """CLOSE_WRITE on a .__tmp__ file puts nothing on metadata_queue."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            staging = tmp / "video.__tmp__"
            staging.write_bytes(b"data")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_close_write(staging)

            self.assertTrue(mq.empty())
            pool.shutdown(wait=False)

    async def test_file_stub_on_tmp_is_ignored(self):
        """on_file_stub on a .__tmp__ file puts nothing on metadata_queue."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            staging = tmp / "video.__tmp__"
            staging.write_bytes(b"data")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            await handlers.on_file_stub(staging)

            self.assertTrue(mq.empty())
            pool.shutdown(wait=False)

    async def test_rename_from_tmp_treated_as_new_arrival(self):
        """on_move(src.__tmp__, dst) returns False — src is not tracked, caller inserts dst fresh."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            staging = tmp / "video.__tmp__"
            staging.write_bytes(b"data")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            # staging is NOT in DB (it was excluded when it was created)

            final = tmp / "video.mp4"
            staging.rename(final)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )
            result = await handlers.on_move(staging, final, False)

            self.assertFalse(result)
            pool.shutdown(wait=False)


class TestDeferredParentCheck(unittest.IsolatedAsyncioTestCase):
    async def test_child_dir_committed_when_parent_pending(self):
        """on_dir_created(parent) + on_dir_created(child) without draining — both in DB after drain."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            parent_dir = tmp / "parent"
            parent_dir.mkdir()
            child_dir = parent_dir / "child"
            child_dir.mkdir()
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )

            # Enqueue both without draining in between
            await handlers.on_dir_created(parent_dir, False)
            await handlers.on_dir_created(child_dir, False)

            # Now drain — parent write runs first, then child write sees parent in DB
            await _drain_write_queue(wq)

            self.assertIsNotNone(
                get_node_by_id(dsn, node_id_from_stat(parent_dir.stat()))
            )
            self.assertIsNotNone(
                get_node_by_id(dsn, node_id_from_stat(child_dir.stat()))
            )
            pool.shutdown(wait=False)

    async def test_file_stub_queued_when_parent_pending(self):
        """on_dir_created(parent) + on_file_stub(file) without draining — file ends up in mq."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            parent_dir = tmp / "parent"
            parent_dir.mkdir()
            f = parent_dir / "file.txt"
            f.write_text("hello")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )

            # Enqueue parent dir write (not yet committed)
            await handlers.on_dir_created(parent_dir, False)
            # Queue file for metadata — parent DB check is deferred to write_worker
            await handlers.on_file_stub(f)

            self.assertFalse(mq.empty())
            pool.shutdown(wait=False)

    async def test_node_dropped_when_parent_not_tracked(self):
        """on_dir_created for a dir whose parent is never committed — write is silently dropped."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            untracked = tmp / "untracked"
            untracked.mkdir()
            child = untracked / "child"
            child.mkdir()
            # untracked is NOT in DB (and we never insert it)
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main()
            storage = _make_storage(dsn)
            mq: MetadataQueue = asyncio.Queue()
            wq = _make_write_queue()
            handlers = WatcherHandlers(
                storage=storage, off_main=off_main, metadata_queue=mq, write_queue=wq
            )

            await handlers.on_dir_created(child, False)
            await _drain_write_queue(wq)

            self.assertIsNone(get_node_by_id(dsn, node_id_from_stat(child.stat())))
            pool.shutdown(wait=False)
