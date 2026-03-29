import asyncio
import os
import unittest
from datetime import datetime, timezone

from wcpan.drive.feed._db import (
    SUPER_ROOT_ID,
    Storage,
    get_all_nodes,
    get_changes_since,
    get_node_by_id,
    node_id_from_stat,
    upsert_node,
)
from wcpan.drive.feed._lib import is_removed_change
from wcpan.drive.feed._scanner import Scanner
from wcpan.drive.feed._types import (
    Config,
    InotifyWatcherConfig,
    MetadataQueue,
    NodeRecord,
    WriteQueue,
)

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


def _make_storage(dsn: str) -> Storage:
    return Storage(dsn)


def _make_write_queue() -> WriteQueue:
    return asyncio.Queue()


def _make_metadata_queue() -> MetadataQueue:
    return asyncio.Queue()


def _make_config(watches: dict[str, str], exclude: tuple[str, ...] = ()) -> Config:
    return Config(
        host="localhost",
        port=8080,
        database_url=":memory:",
        watches=watches,
        watcher=InotifyWatcherConfig(),
        exclude=exclude,
    )


def _make_scanner(
    storage: Storage,
    config: Config,
    wq: WriteQueue,
    mq: MetadataQueue,
) -> Scanner:
    return Scanner(
        storage=storage, off_main=None, config=config, write_queue=wq, metadata_queue=mq
    )  # type: ignore[arg-type]


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
            storage = _make_storage(dsn)
            mq = _make_metadata_queue()
            wq = _make_write_queue()
            config = _make_config({})
            scanner = _make_scanner(storage, config, wq, mq)
            await scanner._scan_directory(tmp, root_id, {}, set())
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, node_id_from_stat(ea_dir.stat()))
            self.assertIsNone(node)

    async def test_files_under_excluded_dir_skipped(self):
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            ea_dir = tmp / "@eaDir"
            ea_dir.mkdir()
            thumb = ea_dir / "thumb.jpg"
            thumb.write_bytes(b"")

            root_id = node_id_from_stat(tmp.stat())
            storage = _make_storage(dsn)
            mq = _make_metadata_queue()
            wq = _make_write_queue()
            config = _make_config({})
            scanner = _make_scanner(storage, config, wq, mq)
            await scanner._scan_directory(tmp, root_id, {}, set())
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, node_id_from_stat(thumb.stat()))
            self.assertIsNone(node)


class TestCleanupStaleNodes(unittest.IsolatedAsyncioTestCase):
    async def test_narrow_to_wide_preserves_subdir_content(self):
        """Config changes from one root to its subdirs — content preserved, old root deleted."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            root = tmp / "root"
            root.mkdir()
            dir_a = root / "a"
            dir_a.mkdir()
            dir_b = root / "b"
            dir_b.mkdir()
            file_a = dir_a / "file_a.txt"
            file_a.write_text("hello")
            file_b = dir_b / "file_b.txt"
            file_b.write_text("world")

            root_id = _insert_dir_node(dsn, root, SUPER_ROOT_ID)
            a_id = _insert_dir_node(dsn, dir_a, root_id)
            b_id = _insert_dir_node(dsn, dir_b, root_id)
            file_a_id = _insert_file_node(dsn, file_a, a_id)
            file_b_id = _insert_file_node(dsn, file_b, b_id)

            config = _make_config({"a": str(dir_a), "b": str(dir_b)})
            snapshot = get_all_nodes(dsn)
            globally_seen: set[str] = set()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            scanner = _make_scanner(storage, config, wq, _make_metadata_queue())

            # Scan each new watch root
            await scanner._scan_directory(dir_a, a_id, snapshot, globally_seen)
            await scanner._scan_directory(dir_b, b_id, snapshot, globally_seen)
            await scanner._cleanup_stale_nodes(snapshot, globally_seen)
            await _drain_write_queue(wq)

            self.assertIsNone(get_node_by_id(dsn, root_id))
            self.assertIsNotNone(get_node_by_id(dsn, a_id))
            self.assertIsNotNone(get_node_by_id(dsn, b_id))
            self.assertIsNotNone(get_node_by_id(dsn, file_a_id))
            self.assertIsNotNone(get_node_by_id(dsn, file_b_id))

    async def test_wide_to_narrow_preserves_subdir_content(self):
        """Config changes from two roots to their parent — content preserved, roots re-parented."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            root = tmp / "root"
            root.mkdir()
            dir_a = root / "a"
            dir_a.mkdir()
            dir_b = root / "b"
            dir_b.mkdir()
            file_a = dir_a / "file_a.txt"
            file_a.write_text("hello")

            # Old state: a and b are watch roots
            a_id = _insert_dir_node(dsn, dir_a, SUPER_ROOT_ID)
            b_id = _insert_dir_node(dsn, dir_b, SUPER_ROOT_ID)
            file_a_id = _insert_file_node(dsn, file_a, a_id)

            new_root_id = node_id_from_stat(root.stat())
            config = _make_config({"root": str(root)})
            snapshot = get_all_nodes(dsn)
            globally_seen: set[str] = set()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            scanner = _make_scanner(storage, config, wq, _make_metadata_queue())

            # Scan the new parent root
            await scanner._scan_directory(root, new_root_id, snapshot, globally_seen)
            await scanner._cleanup_stale_nodes(snapshot, globally_seen)
            await _drain_write_queue(wq)

            # a and b re-parented under root, content preserved
            node_a = get_node_by_id(dsn, a_id)
            self.assertIsNotNone(node_a)
            self.assertEqual(node_a.parent_id, new_root_id)
            self.assertIsNotNone(get_node_by_id(dsn, b_id))
            node_file = get_node_by_id(dsn, file_a_id)
            self.assertIsNotNone(node_file)
            self.assertEqual(node_file.hash, "old_hash")

    async def test_truly_stale_nodes_deleted(self):
        """Nodes not found during any scan are deleted with removal changes."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            watched = tmp / "watched"
            watched.mkdir()
            stale_dir = tmp / "stale"
            stale_dir.mkdir()
            stale_file = stale_dir / "file.txt"
            stale_file.write_text("gone")

            watched_id = _insert_dir_node(dsn, watched, SUPER_ROOT_ID)
            stale_id = _insert_dir_node(dsn, stale_dir, SUPER_ROOT_ID)
            stale_file_id = _insert_file_node(dsn, stale_file, stale_id)

            config = _make_config({"watched": str(watched)})
            snapshot = get_all_nodes(dsn)
            globally_seen: set[str] = set()
            storage = _make_storage(dsn)
            wq = _make_write_queue()
            scanner = _make_scanner(storage, config, wq, _make_metadata_queue())

            # Only scan 'watched'; stale_dir and stale_file are never seen
            await scanner._scan_directory(watched, watched_id, snapshot, globally_seen)
            await scanner._cleanup_stale_nodes(snapshot, globally_seen)
            await _drain_write_queue(wq)

            self.assertIsNone(get_node_by_id(dsn, stale_id))
            self.assertIsNone(get_node_by_id(dsn, stale_file_id))
            changes, _, _ = get_changes_since(dsn, 0, 1000)
            removed_ids = {c.node_id for c in changes if is_removed_change(c)}
            self.assertIn(stale_id, removed_ids)
            self.assertIn(stale_file_id, removed_ids)


class TestScanDirectoryRename(unittest.IsolatedAsyncioTestCase):
    async def test_file_renamed_same_dir_preserves_metadata(self):
        """A renamed file (same inode, same mtime) keeps its hash — no recompute."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            f = tmp / "file.txt"
            f.write_text("content")
            root_id = node_id_from_stat(tmp.stat())
            file_id = _insert_file_node(dsn, f, root_id)

            renamed = tmp / "renamed.txt"
            os.rename(f, renamed)

            snapshot = get_all_nodes(dsn)
            globally_seen: set[str] = set()
            storage = _make_storage(dsn)
            mq = _make_metadata_queue()
            wq = _make_write_queue()
            scanner = _make_scanner(storage, _make_config({}), wq, mq)
            await scanner._scan_directory(tmp, root_id, snapshot, globally_seen)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, file_id)
            self.assertIsNotNone(node)
            self.assertEqual(node.name, "renamed.txt")
            self.assertEqual(node.hash, "old_hash")

    async def test_file_moved_to_different_dir_preserves_metadata(self):
        """A file moved to a different directory keeps its metadata — no recompute."""
        with create_db_sandbox() as dsn, create_fs_sandbox() as tmp:
            dir_a = tmp / "a"
            dir_a.mkdir()
            dir_b = tmp / "b"
            dir_b.mkdir()
            f = dir_a / "file.txt"
            f.write_text("content")

            root_id = node_id_from_stat(tmp.stat())
            a_id = _insert_dir_node(dsn, dir_a, root_id)
            b_id = _insert_dir_node(dsn, dir_b, root_id)
            file_id = _insert_file_node(dsn, f, a_id)

            dest = dir_b / "file.txt"
            os.rename(f, dest)

            snapshot = get_all_nodes(dsn)
            globally_seen: set[str] = set()
            storage = _make_storage(dsn)
            mq = _make_metadata_queue()
            wq = _make_write_queue()
            scanner = _make_scanner(storage, _make_config({}), wq, mq)
            await scanner._scan_directory(tmp, root_id, snapshot, globally_seen)
            await _drain_write_queue(wq)

            node = get_node_by_id(dsn, file_id)
            self.assertIsNotNone(node)
            self.assertEqual(node.parent_id, b_id)
            self.assertEqual(node.hash, "old_hash")


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

            f.unlink()
            subdir.rmdir()

            config = _make_config({"root": str(tmp)})
            snapshot = get_all_nodes(dsn)
            globally_seen: set[str] = set()
            storage = _make_storage(dsn)
            mq = _make_metadata_queue()
            wq = _make_write_queue()
            scanner = _make_scanner(storage, config, wq, mq)
            await scanner._scan_directory(tmp, root_id, snapshot, globally_seen)
            await scanner._cleanup_stale_nodes(snapshot, globally_seen)
            await _drain_write_queue(wq)

            self.assertIsNone(get_node_by_id(dsn, dir_id))
            self.assertIsNone(get_node_by_id(dsn, file_id))

            changes, _, _h = get_changes_since(dsn, 0, 1000)
            removed_ids = {c.node_id for c in changes if is_removed_change(c)}
            self.assertIn(dir_id, removed_ids)
            self.assertIn(file_id, removed_ids)
