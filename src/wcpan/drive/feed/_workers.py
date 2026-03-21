import asyncio
import os
from functools import partial
from logging import getLogger

from ._db import Storage
from ._lib import OffMainThread
from ._metadata import compute_file_metadata
from ._types import MetadataQueue, NodeRecord, WriteQueue


_L = getLogger(__name__)

NUM_META_WORKERS = os.process_cpu_count() or 1
_META_QUEUE_SIZE = NUM_META_WORKERS * 2
_WRITE_QUEUE_SIZE = NUM_META_WORKERS * 2
_WAL_CHECKPOINT_INTERVAL = 300.0  # seconds


def create_metadata_queue() -> MetadataQueue:
    return asyncio.Queue(maxsize=_META_QUEUE_SIZE)


def create_write_queue() -> WriteQueue:
    return asyncio.Queue(maxsize=_WRITE_QUEUE_SIZE)


async def checkpoint_worker(write_queue: WriteQueue, storage: Storage) -> None:
    while True:
        await asyncio.sleep(_WAL_CHECKPOINT_INTERVAL)
        await write_queue.put(storage.checkpoint)


async def write_worker(write_queue: WriteQueue, off_main: OffMainThread) -> None:
    while True:
        task = await write_queue.get()
        try:
            await off_main(task)
        except Exception:
            _L.exception("write task failed")
            raise
        finally:
            write_queue.task_done()


async def metadata_worker(
    metadata_queue: MetadataQueue,
    write_queue: WriteQueue,
    storage: Storage,
    off_main: OffMainThread,
) -> None:
    while True:
        pending_node, path = await metadata_queue.get()
        _L.debug("metadata dequeue: %s", path)
        try:
            meta = await off_main(compute_file_metadata, path)
            node = NodeRecord(
                node_id=pending_node.node_id,
                parent_id=pending_node.parent_id,
                name=pending_node.name,
                is_directory=False,
                ctime=pending_node.ctime,
                mtime=pending_node.mtime,
                mime_type=meta.mime_type,
                hash=meta.hash,
                size=meta.size,
                is_image=meta.is_image,
                is_video=meta.is_video,
                width=meta.width,
                height=meta.height,
                ms_duration=meta.ms_duration,
            )

            await write_queue.put(
                partial(storage.upsert_node_if_parent_known_and_emit_change, node)
            )
            _L.debug(
                "metadata done: %s mime=%s hash=%s", path, meta.mime_type, meta.hash
            )
        except Exception:
            _L.exception("metadata failed for %s", path)
        finally:
            metadata_queue.task_done()
