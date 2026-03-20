import asyncio
import os
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from functools import partial
from logging import getLogger
from pathlib import Path

from aiohttp import web

from ._db import (
    SUPER_ROOT_ID,
    Storage,
    node_id_from_stat,
)
from ._exclude import is_excluded
from ._handlers import handle_changes, handle_cursor, handle_node_path, handle_root
from ._keys import (
    APP_CONFIG,
    APP_KEY_READY,
    APP_OFF_MAIN,
    APP_STORAGE,
    APP_WATCH_ROOT_PATHS,
)
from ._lib import OffMainThread, stat_to_times
from ._metadata import compute_file_metadata
from ._types import Config, MetadataQueue, NodeRecord, WriteQueue


type _Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


_L = getLogger(__name__)

_NUM_META_WORKERS = os.process_cpu_count() or 1
_META_QUEUE_SIZE = _NUM_META_WORKERS * 2
_WRITE_QUEUE_SIZE = _NUM_META_WORKERS * 2
_WAL_CHECKPOINT_INTERVAL = 300.0  # seconds


async def _scan_directory(
    storage: Storage,
    off_main: OffMainThread,
    watch_root: Path,
    watch_root_id: str,
    write_queue: WriteQueue,
    metadata_queue: MetadataQueue,
    exclude: tuple[str, ...] = (),
) -> None:
    # 1. Bulk load all existing nodes; build parent→children index for deletion detection.
    existing_by_id = await off_main(storage.get_all_nodes)
    children_by_parent: dict[str, list[str]] = {}
    for node_id, node in existing_by_id.items():
        if node.parent_id is not None:
            children_by_parent.setdefault(node.parent_id, []).append(node_id)

    pending_upserts: list[NodeRecord] = []
    pending_dir_and_delete_changes: list[tuple[str, bool]] = []
    pending_deletes: list[str] = []
    pending_meta: list[tuple[NodeRecord, Path]] = []

    # Stack: (path, parent_id)
    stack: list[tuple[Path, str]] = [(watch_root, watch_root_id)]

    while stack:
        dir_path, parent_id = stack.pop()
        try:
            entries = list(dir_path.iterdir())
        except OSError:
            continue

        dir_seen: set[str] = set()

        for entry in entries:
            if is_excluded(entry.name, exclude):
                _L.debug("scan exclude: %s", entry)
                continue
            try:
                st = entry.stat()
            except OSError:
                continue

            entry_id = node_id_from_stat(st)
            dir_seen.add(entry_id)
            ctime, mtime = stat_to_times(st)
            existing = existing_by_id.get(entry_id)

            if existing is not None:
                if entry.is_dir():
                    stack.append((entry, existing.node_id))
                    continue
                # File: check mtime
                existing_mtime_us = int(existing.mtime.timestamp() * 1_000_000)
                new_mtime_us = int(mtime.timestamp() * 1_000_000)
                if existing_mtime_us == new_mtime_us:
                    _L.debug("scan unchanged: %s", entry)
                    continue  # unchanged
                # mtime changed — queue for metadata (no DB write until metadata is ready)
                _L.debug("scan update: %s", entry)
                updated = NodeRecord(
                    node_id=existing.node_id,
                    parent_id=existing.parent_id,
                    name=entry.name,
                    is_directory=False,
                    ctime=existing.ctime,
                    mtime=mtime,
                    mime_type=existing.mime_type,
                    hash="",
                    size=st.st_size,
                    is_image=existing.is_image,
                    is_video=existing.is_video,
                    width=existing.width,
                    height=existing.height,
                    ms_duration=existing.ms_duration,
                )
                pending_meta.append((updated, entry))
            else:
                # New node
                _L.debug("scan new %s: %s", "dir" if entry.is_dir() else "file", entry)
                node = NodeRecord(
                    node_id=entry_id,
                    parent_id=parent_id,
                    name=entry.name,
                    is_directory=entry.is_dir(),
                    ctime=ctime,
                    mtime=mtime,
                    mime_type="",
                    hash="",
                    size=st.st_size if not entry.is_dir() else 0,
                    is_image=False,
                    is_video=False,
                    width=0,
                    height=0,
                    ms_duration=0,
                )
                if entry.is_dir():
                    pending_upserts.append(node)
                    pending_dir_and_delete_changes.append((entry_id, False))
                    stack.append((entry, entry_id))
                else:
                    # Files stay out of DB until metadata is ready
                    pending_meta.append((node, entry))

        # Per-directory deletion detection using pre-loaded index (no extra DB queries).
        for node_id in children_by_parent.get(parent_id, []):
            if node_id not in dir_seen:
                _L.debug("scan delete: %s", node_id)
                pending_deletes.append(node_id)
                pending_dir_and_delete_changes.append((node_id, True))

    _L.debug(
        "scan %s: %d upserts, %d deletes",
        watch_root,
        len(pending_upserts),
        len(pending_deletes),
    )

    # 2. Flush as a single write_queue task so the upsert always precedes any
    #    metadata update writes (write_queue is FIFO, single consumer).
    def _flush(
        pending_upserts=pending_upserts,
        pending_deletes=pending_deletes,
        pending_dir_and_delete_changes=pending_dir_and_delete_changes,
    ) -> None:
        if pending_upserts:
            storage.bulk_upsert_nodes(pending_upserts)
        if pending_deletes:
            storage.bulk_delete_nodes(pending_deletes)
        if pending_dir_and_delete_changes:
            storage.bulk_emit_changes(pending_dir_and_delete_changes)

    await write_queue.put(_flush)

    # 3. Enqueue metadata jobs; write_queue's FIFO order ensures the flush
    #    above runs before any metadata upsert reaches the DB.
    for item in pending_meta:
        await metadata_queue.put(item)


async def _checkpoint_worker(write_queue: WriteQueue, storage: Storage) -> None:
    while True:
        await asyncio.sleep(_WAL_CHECKPOINT_INTERVAL)
        await write_queue.put(storage.checkpoint)


async def _write_worker(write_queue: WriteQueue, off_main: OffMainThread) -> None:
    while True:
        task = await write_queue.get()
        try:
            await off_main(task)
        except Exception:
            _L.exception("write task failed")
            raise
        finally:
            write_queue.task_done()


async def _metadata_worker(
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

            await write_queue.put(partial(storage.upsert_node_and_emit_change, node))
            _L.debug(
                "metadata done: %s mime=%s hash=%s", path, meta.mime_type, meta.hash
            )
        except Exception:
            _L.exception("metadata failed for %s", path)
        finally:
            metadata_queue.task_done()


@contextmanager
def _managed_pool() -> Generator[ThreadPoolExecutor, None, None]:
    pool = ThreadPoolExecutor()
    try:
        yield pool
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


@asynccontextmanager
async def _background[T](
    group: asyncio.TaskGroup, c: Coroutine[None, None, T]
) -> AsyncGenerator[None, None]:
    task = group.create_task(c)
    try:
        yield
    finally:
        task.cancel()


async def _reconcile_stale_roots(
    storage: Storage,
    off_main: OffMainThread,
    config: Config,
    write_queue: WriteQueue,
) -> None:
    config_root_ids: set[str] = set()
    for p in config.watches.values():
        try:
            config_root_ids.add(node_id_from_stat(Path(p).resolve().stat()))
        except OSError:
            pass
    db_root_ids = set(await off_main(storage.get_all_node_ids_by_parent, SUPER_ROOT_ID))
    for stale_id in db_root_ids - config_root_ids:
        _L.info("removing stale watch root: %s", stale_id)
        child_ids = await off_main(storage.get_all_node_ids_under, stale_id)
        all_ids = child_ids + [stale_id]
        await write_queue.put(partial(storage.delete_nodes_and_emit_changes, all_ids))


async def _scan_all_watch_paths(
    storage: Storage,
    off_main: OffMainThread,
    config: Config,
    write_queue: WriteQueue,
    metadata_queue: MetadataQueue,
) -> None:
    for namespace, watch_path_str in config.watches.items():
        watch_root = Path(watch_path_str).resolve()
        try:
            st = watch_root.stat()
        except OSError:
            continue
        watch_root_id = node_id_from_stat(st)
        ctime, mtime = stat_to_times(st)
        root_node = NodeRecord(
            node_id=watch_root_id,
            parent_id=SUPER_ROOT_ID,
            name=namespace,
            is_directory=True,
            ctime=ctime,
            mtime=mtime,
            mime_type="",
            hash="",
            size=0,
            is_image=False,
            is_video=False,
            width=0,
            height=0,
            ms_duration=0,
        )

        await write_queue.put(partial(storage.upsert_node_and_emit_change, root_node))
        _L.info("scanning watch root: %s -> %s", namespace, watch_root)
        await _scan_directory(
            storage,
            off_main,
            watch_root,
            watch_root_id,
            write_queue,
            metadata_queue,
            config.exclude,
        )


def _build_watch_root_paths(config: Config) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for watch_path_str in config.watches.values():
        watch_root = Path(watch_path_str).resolve()
        try:
            st = watch_root.stat()
            result[node_id_from_stat(st)] = watch_root
        except OSError:
            pass
    return result


async def _startup(
    storage: Storage,
    off_main: OffMainThread,
    config: Config,
    write_queue: WriteQueue,
    metadata_queue: MetadataQueue,
    ready_event: asyncio.Event,
) -> None:
    await _reconcile_stale_roots(storage, off_main, config, write_queue)
    await _scan_all_watch_paths(storage, off_main, config, write_queue, metadata_queue)
    _L.info("startup scan complete; waiting for queues to drain")
    await metadata_queue.join()
    await write_queue.join()
    _L.info("queues idle; server is ready")
    ready_event.set()


@web.middleware
async def _ready_middleware(
    request: web.Request, handler: _Handler
) -> web.StreamResponse:
    if not request.app[APP_KEY_READY].is_set():
        raise web.HTTPServiceUnavailable(reason="Server starting")
    return await handler(request)


async def _app_lifecycle(app: web.Application) -> AsyncGenerator[None, None]:
    config: Config = app[APP_CONFIG]
    dsn = config.database_url

    async with AsyncExitStack() as stack:
        # 1. Single thread pool for both DB ops and metadata computation
        pool = stack.enter_context(_managed_pool())
        off_main = OffMainThread(pool)
        storage = Storage(dsn)
        app[APP_OFF_MAIN] = off_main
        app[APP_STORAGE] = storage

        # 2. Initialize DB
        _L.info("initializing database: %s", dsn)
        await off_main(storage.ensure_schema)
        await off_main(storage.upsert_super_root)

        # 3. Queues and ready event
        metadata_queue: asyncio.Queue[tuple[NodeRecord, Path]] = asyncio.Queue(
            maxsize=_META_QUEUE_SIZE
        )
        write_queue: WriteQueue = asyncio.Queue(maxsize=_WRITE_QUEUE_SIZE)
        ready_event = asyncio.Event()
        app[APP_KEY_READY] = ready_event
        app[APP_WATCH_ROOT_PATHS] = _build_watch_root_paths(config)

        from ._watcher import make_watcher_backend

        watcher_fn = make_watcher_backend(config.watcher)

        # 4. Start background tasks under a single TaskGroup
        group = await stack.enter_async_context(asyncio.TaskGroup())

        # Single write consumer — serialises all DB writes
        await stack.enter_async_context(
            _background(group, _write_worker(write_queue, off_main))
        )

        # Periodic WAL checkpoint — keeps WAL bounded during normal operation
        await stack.enter_async_context(
            _background(group, _checkpoint_worker(write_queue, storage))
        )

        # N metadata workers — each computes file metadata and enqueues its own write
        for _ in range(_NUM_META_WORKERS):
            await stack.enter_async_context(
                _background(
                    group,
                    _metadata_worker(metadata_queue, write_queue, storage, off_main),
                )
            )

        # Watcher starts BEFORE scan to capture events that arrive during scanning
        await stack.enter_async_context(
            _background(
                group,
                watcher_fn(
                    list(config.watches.values()),
                    storage,
                    off_main,
                    metadata_queue,
                    write_queue,
                    exclude=config.exclude,
                ),
            )
        )

        # Startup: scan → drain queues → set ready
        await stack.enter_async_context(
            _background(
                group,
                _startup(
                    storage, off_main, config, write_queue, metadata_queue, ready_event
                ),
            )
        )

        yield


def create_app(config: Config) -> web.Application:
    app = web.Application(middlewares=[_ready_middleware])
    app[APP_CONFIG] = config
    app.cleanup_ctx.append(_app_lifecycle)
    app.router.add_get("/api/v1/cursor", handle_cursor)
    app.router.add_get("/api/v1/changes", handle_changes)
    app.router.add_get("/api/v1/root", handle_root)
    app.router.add_get("/api/v1/nodes/{id}/path", handle_node_path)
    return app
