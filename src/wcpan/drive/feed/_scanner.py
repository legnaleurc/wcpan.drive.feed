from functools import partial
from logging import getLogger
from pathlib import Path

from ._db import SUPER_ROOT_ID, Storage, node_id_from_stat
from ._exclude import is_excluded
from ._lib import OffMainThread, stat_to_times
from ._types import Config, MetadataQueue, NodeRecord, WriteQueue


_L = getLogger(__name__)


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
                # If it's a directory, recursively collect all descendants using the
                # in-memory index so orphaned subtrees are fully cleaned up.
                if existing_by_id[node_id].is_directory:
                    stack_del = [node_id]
                    while stack_del:
                        pid = stack_del.pop()
                        for child_id in children_by_parent.get(pid, []):
                            pending_deletes.append(child_id)
                            pending_dir_and_delete_changes.append((child_id, True))
                            stack_del.append(child_id)
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
    await write_queue.put(
        partial(
            storage.bulk_scan_flush,
            pending_upserts,
            pending_deletes,
            pending_dir_and_delete_changes,
        )
    )

    # 3. Enqueue metadata jobs; write_queue's FIFO order ensures the flush
    #    above runs before any metadata upsert reaches the DB.
    for item in pending_meta:
        await metadata_queue.put(item)


async def reconcile_stale_roots(
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


async def scan_all_watch_paths(
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


def build_watch_root_paths(config: Config) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for watch_path_str in config.watches.values():
        watch_root = Path(watch_path_str).resolve()
        try:
            st = watch_root.stat()
            result[node_id_from_stat(st)] = watch_root
        except OSError:
            pass
    return result
