import asyncio
from collections.abc import AsyncGenerator, Coroutine, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from logging import getLogger
from pathlib import Path

from aiohttp import web

from ._db import (
    SUPER_ROOT_ID,
    OffMainThread,
    bulk_delete_nodes,
    bulk_emit_changes,
    bulk_upsert_nodes,
    checkpoint,
    emit_change,
    ensure_schema,
    get_all_node_ids_by_parent,
    get_all_node_ids_under,
    get_all_nodes,
    node_id_from_stat,
    update_node_metadata,
    upsert_node,
    upsert_super_root,
)
from ._exclude import is_excluded
from ._handlers import handle_changes, handle_cursor, handle_node_path, handle_root
from ._keys import APP_CONFIG, APP_OFF_MAIN, APP_WATCH_ROOT_PATHS
from ._lib import stat_to_times
from ._metadata import compute_file_metadata
from ._types import Config, NodeRecord


_L = getLogger(__name__)


async def _scan_directory(
    off_main: OffMainThread,
    watch_root: Path,
    watch_root_id: str,
    metadata_queue: asyncio.Queue[tuple[str, Path]],
    exclude: tuple[str, ...] = (),
) -> None:
    # 1. Bulk load all existing nodes; build parent→children index for deletion detection.
    existing_by_id = await off_main(get_all_nodes)
    children_by_parent: dict[str, list[str]] = {}
    for node_id, node in existing_by_id.items():
        if node.parent_id is not None:
            children_by_parent.setdefault(node.parent_id, []).append(node_id)

    pending_upserts: list[NodeRecord] = []
    pending_changes: list[tuple[str, bool]] = []
    pending_deletes: list[str] = []
    pending_meta: list[tuple[str, Path]] = []

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
                # mtime changed — queue update
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
                pending_upserts.append(updated)
                pending_changes.append((existing.node_id, False))
                pending_meta.append((existing.node_id, entry))
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
                pending_upserts.append(node)
                pending_changes.append((entry_id, False))
                if entry.is_dir():
                    stack.append((entry, entry_id))
                else:
                    pending_meta.append((entry_id, entry))

        # Per-directory deletion detection using pre-loaded index (no extra DB queries).
        for node_id in children_by_parent.get(parent_id, []):
            if node_id not in dir_seen:
                _L.debug("scan delete: %s", node_id)
                pending_deletes.append(node_id)
                pending_changes.append((node_id, True))

    _L.debug(
        "scan %s: %d upserts, %d deletes",
        watch_root,
        len(pending_upserts),
        len(pending_deletes),
    )

    # 2. Flush: one transaction each.
    if pending_upserts:
        await off_main(bulk_upsert_nodes, pending_upserts)
    if pending_deletes:
        await off_main(bulk_delete_nodes, pending_deletes)
    if pending_changes:
        await off_main(bulk_emit_changes, pending_changes)

    # 3. Enqueue metadata after nodes are committed to DB.
    for item in pending_meta:
        await metadata_queue.put(item)


async def _metadata_worker(
    metadata_queue: asyncio.Queue[tuple[str, Path]],
    off_main: OffMainThread,
) -> None:
    while True:
        node_id, path = await metadata_queue.get()
        _L.debug("metadata dequeue: %s", path)
        try:
            meta = await off_main.run(compute_file_metadata, path)
            await off_main(
                update_node_metadata,
                node_id,
                mime_type=meta.mime_type,
                hash=meta.hash,
                size=meta.size,
                is_image=meta.is_image,
                is_video=meta.is_video,
                width=meta.width,
                height=meta.height,
                ms_duration=meta.ms_duration,
            )
            await off_main(emit_change, node_id, is_removed=False)
            _L.debug(
                "metadata done: %s mime=%s hash=%s", path, meta.mime_type, meta.hash
            )
        except Exception:
            _L.exception("metadata failed for %s", path)


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
    off_main: OffMainThread,
    config: Config,
) -> None:
    config_root_ids: set[str] = set()
    for p in config.watches.values():
        try:
            config_root_ids.add(node_id_from_stat(Path(p).resolve().stat()))
        except OSError:
            pass
    db_root_ids = set(await off_main(get_all_node_ids_by_parent, SUPER_ROOT_ID))
    for stale_id in db_root_ids - config_root_ids:
        _L.info("removing stale watch root: %s", stale_id)
        child_ids = await off_main(get_all_node_ids_under, stale_id)
        all_ids = child_ids + [stale_id]
        await off_main(bulk_emit_changes, [(nid, True) for nid in all_ids])
        await off_main(bulk_delete_nodes, all_ids)


async def _scan_all_watch_paths(
    off_main: OffMainThread,
    config: Config,
    metadata_queue: asyncio.Queue[tuple[str, Path]],
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
        await off_main(upsert_node, root_node)
        await off_main(emit_change, watch_root_id, is_removed=False)
        _L.info("scanning watch root: %s -> %s", namespace, watch_root)
        await _scan_directory(
            off_main, watch_root, watch_root_id, metadata_queue, config.exclude
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


async def _app_lifecycle(app: web.Application) -> AsyncGenerator[None, None]:
    config: Config = app[APP_CONFIG]
    dsn = config.database_url

    async with AsyncExitStack() as stack:
        # 1. Single thread pool for both DB ops and metadata computation
        pool = stack.enter_context(_managed_pool())
        off_main = OffMainThread(dsn=dsn, pool=pool)
        app[APP_OFF_MAIN] = off_main

        # 2. Initialize DB — routed through db_pool so all subsequent ops
        #    share the same thread and see the same WAL snapshot.
        _L.info("initializing database: %s", dsn)
        await off_main(ensure_schema)
        await off_main(upsert_super_root)

        # 3. Reconcile: remove watch roots that are no longer in config
        await _reconcile_stale_roots(off_main, config)

        # 4. Metadata queue
        metadata_queue: asyncio.Queue[tuple[str, Path]] = asyncio.Queue()

        # 5. Scan each watch path
        await _scan_all_watch_paths(off_main, config, metadata_queue)

        # 6. Checkpoint: merge WAL into main db so committed scan data survives
        #    a forced container stop without needing the WAL file.
        await off_main(checkpoint)

        # 7. Build watch_root_paths mapping (node_id → real Path)
        app[APP_WATCH_ROOT_PATHS] = _build_watch_root_paths(config)

        # 8. Start background tasks under a single TaskGroup
        from ._watcher import make_watcher_backend

        watcher_fn = make_watcher_backend(config.watcher)
        group = await stack.enter_async_context(asyncio.TaskGroup())
        await stack.enter_async_context(
            _background(group, _metadata_worker(metadata_queue, off_main))
        )
        await stack.enter_async_context(
            _background(
                group,
                watcher_fn(
                    list(config.watches.values()),
                    off_main,
                    metadata_queue,
                    exclude=config.exclude,
                ),
            )
        )

        yield


def create_app(config: Config) -> web.Application:
    app = web.Application()
    app[APP_CONFIG] = config
    app.cleanup_ctx.append(_app_lifecycle)
    app.router.add_get("/api/v1/cursor", handle_cursor)
    app.router.add_get("/api/v1/changes", handle_changes)
    app.router.add_get("/api/v1/root", handle_root)
    app.router.add_get("/api/v1/nodes/{id}/path", handle_node_path)
    return app
