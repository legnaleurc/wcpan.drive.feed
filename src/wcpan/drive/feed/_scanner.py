import asyncio
from dataclasses import replace
from functools import partial
from logging import getLogger
from pathlib import Path

from ._db import SUPER_ROOT_ID, Storage, node_id_from_stat
from ._exclude import is_excluded
from ._lib import OffMainThread, stat_to_times
from ._types import Config, MetadataQueue, NodeRecord, WriteQueue


_L = getLogger(__name__)


class Scanner:
    def __init__(
        self,
        storage: Storage,
        off_main: OffMainThread,
        config: Config,
        write_queue: WriteQueue,
        metadata_queue: MetadataQueue,
    ) -> None:
        self._storage = storage
        self._off_main = off_main
        self._config = config
        self._write_queue = write_queue
        self._metadata_queue = metadata_queue

    async def startup(self, ready_event: asyncio.Event) -> None:
        snapshot = await self._off_main(self._storage.get_all_nodes)
        globally_seen: set[str] = set()
        await self._scan_all_watch_paths(snapshot, globally_seen)
        await self._cleanup_stale_nodes(snapshot, globally_seen)
        _L.info("startup scan complete; waiting for queues to drain")
        await self._metadata_queue.join()
        await self._write_queue.join()
        _L.info("queues idle; server is ready")
        ready_event.set()

    async def _scan_all_watch_paths(
        self,
        snapshot: dict[str, NodeRecord],
        globally_seen: set[str],
    ) -> None:
        for namespace, watch_path_str in self._config.watches.items():
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

            await self._write_queue.put(
                partial(self._storage.upsert_node_and_emit_change, root_node)
            )
            _L.info("scanning watch root: %s -> %s", namespace, watch_root)
            await self._scan_directory(
                watch_root, watch_root_id, snapshot, globally_seen
            )

    async def _cleanup_stale_nodes(
        self,
        snapshot: dict[str, NodeRecord],
        globally_seen: set[str],
    ) -> None:
        config_root_ids: set[str] = set()
        for p in self._config.watches.values():
            try:
                config_root_ids.add(node_id_from_stat(Path(p).resolve().stat()))
            except OSError:
                pass
        to_delete = [
            nid
            for nid in snapshot
            if nid != SUPER_ROOT_ID
            and nid not in config_root_ids
            and nid not in globally_seen
        ]
        if to_delete:
            await self._write_queue.put(
                partial(self._storage.delete_nodes_and_emit_changes, to_delete)
            )

    async def _scan_directory(
        self,
        watch_root: Path,
        watch_root_id: str,
        snapshot: dict[str, NodeRecord],
        globally_seen: set[str],
    ) -> None:
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

            for entry in entries:
                if is_excluded(entry.name, self._config.exclude):
                    _L.debug("scan exclude: %s", entry)
                    continue
                try:
                    st = entry.stat()
                except OSError:
                    continue

                entry_id = node_id_from_stat(st)
                ctime, mtime = stat_to_times(st)
                existing = snapshot.get(entry_id)

                if existing is not None:
                    globally_seen.add(entry_id)
                    if entry.is_dir():
                        if (
                            existing.parent_id != parent_id
                            or existing.name != entry.name
                        ):
                            pending_upserts.append(
                                replace(existing, parent_id=parent_id, name=entry.name)
                            )
                            pending_dir_and_delete_changes.append((entry_id, False))
                        stack.append((entry, existing.node_id))
                        continue
                    # File: check mtime
                    existing_mtime_us = int(existing.mtime.timestamp() * 1_000_000)
                    new_mtime_us = int(mtime.timestamp() * 1_000_000)
                    if existing_mtime_us == new_mtime_us:
                        if (
                            existing.parent_id != parent_id
                            or existing.name != entry.name
                        ):
                            pending_upserts.append(
                                replace(existing, parent_id=parent_id, name=entry.name)
                            )
                            pending_dir_and_delete_changes.append((entry_id, False))
                        else:
                            _L.debug("scan unchanged: %s", entry)
                        continue  # unchanged
                    # mtime changed — queue for metadata (no DB write until metadata is ready)
                    _L.debug("scan update: %s", entry)
                    pending_meta.append(
                        (
                            replace(
                                existing,
                                parent_id=parent_id,
                                name=entry.name,
                                mtime=mtime,
                                hash="",
                                size=st.st_size,
                            ),
                            entry,
                        )
                    )
                else:
                    # New node
                    globally_seen.add(entry_id)
                    _L.debug(
                        "scan new %s: %s", "dir" if entry.is_dir() else "file", entry
                    )
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

        _L.debug(
            "scan %s: %d upserts, %d deletes",
            watch_root,
            len(pending_upserts),
            len(pending_deletes),
        )

        # Flush as a single write_queue task so the upsert always precedes any
        # metadata update writes (write_queue is FIFO, single consumer).
        await self._write_queue.put(
            partial(
                self._storage.bulk_scan_flush,
                pending_upserts,
                pending_deletes,
                pending_dir_and_delete_changes,
            )
        )

        for item in pending_meta:
            await self._metadata_queue.put(item)


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
