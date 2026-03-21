import asyncio
import unittest

from wcpan.drive.feed._db import Storage, get_node_by_id, node_id_from_stat
from wcpan.drive.feed._scanner import _scan_directory
from wcpan.drive.feed._types import MetadataQueue, WriteQueue

from ._lib import create_db_sandbox, create_fs_sandbox


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
