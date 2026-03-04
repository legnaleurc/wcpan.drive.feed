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
from wcpan.drive.feed._types import NodeRecord
from wcpan.drive.feed._app import _scan_directory
from wcpan.drive.feed._watcher import (
    _on_close_write,
    _on_delete,
    _on_dir_created,
    _on_file_stub,
    _on_move,
)

from ._lib import create_db_sandbox, create_fs_sandbox


def _make_off_main(dsn: str):
    from concurrent.futures import ThreadPoolExecutor

    from wcpan.drive.feed._db import OffMainProcess

    pool = ThreadPoolExecutor(max_workers=1)
    return OffMainProcess(dsn=dsn, pool=pool), pool


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
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "hello.txt"
            f.write_text("hello")
            off_main, pool = _make_off_main(dsn)

            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            await _on_file_stub(f, off_main)

            node = get_node_by_id(dsn, node_id_from_stat(f.stat()))
            self.assertIsNotNone(node)
            self.assertEqual(node.name, "hello.txt")
            self.assertEqual(node.hash, "")
            pool.shutdown(wait=False)

    async def test_missing_file_is_ignored(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            off_main, pool = _make_off_main(dsn)
            await _on_file_stub(tmp / "nonexistent.txt", off_main)
            pool.shutdown(wait=False)


class TestOnCloseWrite(unittest.IsolatedAsyncioTestCase):
    async def test_updates_existing_node(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "data.txt"
            f.write_text("v1")
            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            file_id = _insert_file_node(dsn, f, parent_id)

            f.write_text("v2")
            off_main, pool = _make_off_main(dsn)
            mq: asyncio.Queue = asyncio.Queue()
            await _on_close_write(f, off_main, mq)

            node = get_node_by_id(dsn, file_id)
            self.assertEqual(node.hash, "")  # cleared for re-hash
            self.assertFalse(mq.empty())

            changes, _ = get_changes_since(dsn, 0)
            node_ids = {c.node.node_id if not c.removed else c.node_id for c in changes}
            self.assertIn(file_id, node_ids)
            pool.shutdown(wait=False)

    async def test_inserts_stub_if_node_missing(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "new.txt"
            f.write_text("content")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main(dsn)
            mq: asyncio.Queue = asyncio.Queue()
            await _on_close_write(f, off_main, mq)

            node = get_node_by_id(dsn, node_id_from_stat(f.stat()))
            self.assertIsNotNone(node)
            pool.shutdown(wait=False)


class TestOnDelete(unittest.IsolatedAsyncioTestCase):
    async def test_delete_file_emits_remove(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "bye.txt"
            f.write_text("bye")
            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            file_id = _insert_file_node(dsn, f, parent_id)
            f.unlink()

            off_main, pool = _make_off_main(dsn)
            await _on_delete(f, False, off_main)

            changes, _ = get_changes_since(dsn, 0)
            removed = [c for c in changes if c.removed and c.node_id == file_id]
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

            off_main, pool = _make_off_main(dsn)
            await _on_delete(subdir, True, off_main)

            changes, _ = get_changes_since(dsn, 0)
            removed_ids = {c.node_id for c in changes if c.removed}
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

            off_main, pool = _make_off_main(dsn)
            await _on_move(src, dst, False, off_main)

            node = get_node_by_id(dsn, file_id)
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

            off_main, pool = _make_off_main(dsn)
            await _on_move(src, dst, True, off_main)

            node = get_node_by_id(dsn, dir_id)
            self.assertEqual(node.name, "dstdir")
            pool.shutdown(wait=False)


class TestOnDirCreated(unittest.IsolatedAsyncioTestCase):
    async def test_creates_node(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            new_dir = tmp / "newdir"
            new_dir.mkdir()
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main(dsn)
            mq: asyncio.Queue = asyncio.Queue()
            await _on_dir_created(new_dir, off_main, mq, scan_contents=False)

            node = get_node_by_id(dsn, node_id_from_stat(new_dir.stat()))
            self.assertIsNotNone(node)
            self.assertTrue(node.is_directory)
            pool.shutdown(wait=False)

    async def test_scan_contents_on_move_in(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            moved_in = tmp / "moved"
            moved_in.mkdir()
            (moved_in / "file.txt").write_text("data")

            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main(dsn)
            mq: asyncio.Queue = asyncio.Queue()
            await _on_dir_created(moved_in, off_main, mq, scan_contents=True)

            self.assertFalse(mq.empty())
            pool.shutdown(wait=False)


class TestExcludeOnStartup(unittest.IsolatedAsyncioTestCase):
    async def test_excluded_dir_skipped(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()

            root_id = node_id_from_stat(tmp.stat())
            off_main, pool = _make_off_main(dsn)
            mq: asyncio.Queue = asyncio.Queue()
            await _scan_directory(off_main, tmp, root_id, mq)

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
            off_main, pool = _make_off_main(dsn)
            mq: asyncio.Queue = asyncio.Queue()
            await _scan_directory(off_main, tmp, root_id, mq)

            node = get_node_by_id(dsn, node_id_from_stat(thumb.stat()))
            self.assertIsNone(node)
            pool.shutdown(wait=False)


class TestExcludeOnEvents(unittest.IsolatedAsyncioTestCase):
    async def test_file_stub_excluded_name(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "Thumbs.db"
            f.write_bytes(b"")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main(dsn)
            await _on_file_stub(f, off_main)

            node = get_node_by_id(dsn, node_id_from_stat(f.stat()))
            self.assertIsNone(node)
            pool.shutdown(wait=False)

    async def test_dir_created_excluded_name(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main(dsn)
            mq: asyncio.Queue = asyncio.Queue()
            await _on_dir_created(ea_dir, off_main, mq, scan_contents=False)

            node = get_node_by_id(dsn, node_id_from_stat(ea_dir.stat()))
            self.assertIsNone(node)
            pool.shutdown(wait=False)

    async def test_close_write_excluded_name(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "desktop.ini"
            f.write_text("[autorun]")
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main(dsn)
            mq: asyncio.Queue = asyncio.Queue()
            await _on_close_write(f, off_main, mq)

            node = get_node_by_id(dsn, node_id_from_stat(f.stat()))
            self.assertIsNone(node)
            pool.shutdown(wait=False)

    async def test_move_to_excluded_name(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            src = tmp / "mydir"
            src.mkdir()
            parent_id = _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)
            dir_id = _insert_dir_node(dsn, src, parent_id)

            dst = tmp / "@eaDir"
            src.rename(dst)

            off_main, pool = _make_off_main(dsn)
            await _on_move(src, dst, True, off_main)

            node = get_node_by_id(dsn, dir_id)
            self.assertIsNotNone(node)
            self.assertEqual(node.name, "mydir")
            pool.shutdown(wait=False)


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

            off_main, pool = _make_off_main(dsn)
            await _on_file_stub(thumb, off_main)

            node = get_node_by_id(dsn, node_id_from_stat(thumb.stat()))
            self.assertIsNone(node)  # currently fails: node inserted with parent_id=NULL
            pool.shutdown(wait=False)

    async def test_dir_created_under_excluded_dir(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()
            sub = ea_dir / "sub"
            sub.mkdir()
            # @eaDir is on disk but NOT in DB; only tmp is in DB
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main(dsn)
            mq: asyncio.Queue = asyncio.Queue()
            await _on_dir_created(sub, off_main, mq, scan_contents=False)

            node = get_node_by_id(dsn, node_id_from_stat(sub.stat()))
            self.assertIsNone(node)  # currently fails: node inserted with parent_id=NULL
            pool.shutdown(wait=False)

    async def test_close_write_under_excluded_dir(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()
            thumb = ea_dir / "thumbnail.jpg"
            thumb.write_bytes(b"")
            # @eaDir is on disk but NOT in DB; only tmp is in DB
            _insert_dir_node(dsn, tmp, SUPER_ROOT_ID)

            off_main, pool = _make_off_main(dsn)
            mq: asyncio.Queue = asyncio.Queue()
            await _on_close_write(thumb, off_main, mq)

            node = get_node_by_id(dsn, node_id_from_stat(thumb.stat()))
            self.assertIsNone(node)  # currently fails: node inserted with parent_id=NULL
            pool.shutdown(wait=False)


if __name__ == "__main__":
    unittest.main()
