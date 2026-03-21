from functools import partial
from logging import getLogger
from pathlib import Path

from .._db import (
    Storage,
    node_id_from_stat,
)
from .._exclude import is_excluded, is_path_excluded
from .._lib import OffMainThread, stat_to_times
from .._types import MetadataQueue, NodeRecord, WriteQueue


_L = getLogger(__name__)


def get_parent_node_id(path: Path) -> str | None:
    try:
        st = path.parent.stat()
    except OSError:
        return None
    return node_id_from_stat(st)


class WatcherHandlers:
    def __init__(
        self,
        *,
        storage: Storage,
        off_main: OffMainThread,
        metadata_queue: MetadataQueue,
        write_queue: WriteQueue,
        exclude: tuple[str, ...] = (),
    ) -> None:
        self._storage = storage
        self._off_main = off_main
        self._metadata_queue = metadata_queue
        self._write_queue = write_queue
        self._exclude = exclude

    async def on_new_file(self, path: Path) -> None:
        """Queue a complete file (new to DB) for metadata computation — no DB write until metadata is ready."""
        if is_path_excluded(path, self._exclude):
            return
        try:
            st = path.stat()
        except OSError:
            return
        ctime, mtime = stat_to_times(st)
        node_id = node_id_from_stat(st)
        _L.debug("create file stub: %s", path)
        parent_id = get_parent_node_id(path)
        if parent_id is None:
            return
        node = NodeRecord(
            node_id=node_id,
            parent_id=parent_id,
            name=path.name,
            is_directory=False,
            ctime=ctime,
            mtime=mtime,
            mime_type="",
            hash="",
            size=st.st_size,
            is_image=False,
            is_video=False,
            width=0,
            height=0,
            ms_duration=0,
        )
        await self._metadata_queue.put((node, path))

    async def on_close_write(self, path: Path) -> None:
        """Queue file node for metadata computation on CLOSE_WRITE — no DB write until metadata is ready."""
        if is_path_excluded(path, self._exclude):
            return
        try:
            st = path.stat()
        except OSError:
            return
        ctime, mtime = stat_to_times(st)
        node_id = node_id_from_stat(st)
        _L.debug("close write: %s", path)
        existing = await self._off_main(self._storage.get_node_by_id, node_id)
        if existing is None:
            parent_id = get_parent_node_id(path)
            if parent_id is None:
                return
            node = NodeRecord(
                node_id=node_id,
                parent_id=parent_id,
                name=path.name,
                is_directory=False,
                ctime=ctime,
                mtime=mtime,
                mime_type="",
                hash="",
                size=st.st_size,
                is_image=False,
                is_video=False,
                width=0,
                height=0,
                ms_duration=0,
            )
        else:
            node = NodeRecord(
                node_id=existing.node_id,
                parent_id=existing.parent_id,
                name=existing.name,
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
        await self._metadata_queue.put((node, path))

    async def on_delete(self, path: Path, is_dir: bool) -> None:
        """Emit remove for a deleted file or directory (recursively for dirs)."""
        _L.debug("delete: %s (is_dir=%s)", path, is_dir)
        try:
            parent_st = path.parent.stat()
        except OSError:
            return
        parent_node = await self._off_main(
            self._storage.get_node_by_id, node_id_from_stat(parent_st)
        )
        if parent_node is None:
            return
        node = await self._off_main(
            self._storage.get_node_by_parent_name, parent_node.node_id, path.name
        )
        if node is None:
            return
        node_ids = [node.node_id]
        if is_dir:
            node_ids += await self._off_main(
                self._storage.get_all_node_ids_under, node.node_id
            )

        await self._write_queue.put(
            partial(self._storage.delete_nodes_and_emit_changes, node_ids)
        )

    async def on_move(self, src: Path, dst: Path, is_dir: bool) -> bool:
        """Update parent_id + name for a renamed/moved node (no re-hash needed).

        Returns False if the source node was not in the DB (caller should treat dst
        as a newly-arrived file/directory instead).
        """
        if is_path_excluded(dst, self._exclude):
            return True
        _L.debug("move: %s -> %s", src, dst)
        try:
            st = dst.stat()
        except OSError:
            return True
        node_id = node_id_from_stat(st)
        existing = await self._off_main(self._storage.get_node_by_id, node_id)
        if existing is None:
            return False
        new_parent = get_parent_node_id(dst)
        if new_parent is None:
            return True
        node = NodeRecord(
            node_id=existing.node_id,
            parent_id=new_parent,
            name=dst.name,
            is_directory=is_dir,
            ctime=existing.ctime,
            mtime=existing.mtime,
            mime_type=existing.mime_type,
            hash=existing.hash,
            size=existing.size,
            is_image=existing.is_image,
            is_video=existing.is_video,
            width=existing.width,
            height=existing.height,
            ms_duration=existing.ms_duration,
        )

        await self._write_queue.put(
            partial(self._storage.upsert_node_if_parent_known_and_emit_change, node)
        )
        return True

    async def on_dir_created(self, path: Path, scan_contents: bool) -> None:
        """Insert a directory node into the DB.

        scan_contents=True when the directory was moved in from outside the watched
        area and may already contain files that won't trigger further events.
        """
        if is_path_excluded(path, self._exclude):
            return
        try:
            st = path.stat()
        except OSError:
            return
        ctime, mtime = stat_to_times(st)
        node_id = node_id_from_stat(st)
        _L.debug("dir created: %s (scan_contents=%s)", path, scan_contents)
        parent_id = get_parent_node_id(path)
        if parent_id is None:
            return
        node = NodeRecord(
            node_id=node_id,
            parent_id=parent_id,
            name=path.name,
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
            partial(self._storage.upsert_node_if_parent_known_and_emit_change, node)
        )

        if not scan_contents:
            return

        # Recursively insert DB records for pre-existing contents (moved-in dir).
        stack = [(path, node_id)]
        while stack:
            cur_dir, cur_parent_id = stack.pop()
            try:
                entries = list(cur_dir.iterdir())
            except OSError:
                continue
            for entry in entries:
                if is_excluded(entry.name, self._exclude):
                    continue
                try:
                    est = entry.stat()
                except OSError:
                    continue
                ectime, emtime = stat_to_times(est)
                eid = node_id_from_stat(est)
                enode = NodeRecord(
                    node_id=eid,
                    parent_id=cur_parent_id,
                    name=entry.name,
                    is_directory=entry.is_dir(),
                    ctime=ectime,
                    mtime=emtime,
                    mime_type="",
                    hash="",
                    size=est.st_size if not entry.is_dir() else 0,
                    is_image=False,
                    is_video=False,
                    width=0,
                    height=0,
                    ms_duration=0,
                )
                if entry.is_dir():
                    await self._write_queue.put(
                        partial(
                            self._storage.upsert_node_if_parent_known_and_emit_change,
                            enode,
                        )
                    )
                    stack.append((entry, eid))
                else:
                    await self._metadata_queue.put((enode, entry))

    async def flush_pending_moves(
        self, pending_from: dict[int, tuple[Path, bool]]
    ) -> None:
        for cookie, (stale_path, stale_is_dir) in list(pending_from.items()):
            del pending_from[cookie]
            try:
                await self.on_delete(stale_path, stale_is_dir)
            except Exception:
                _L.exception("delete failed for stale move: %s", stale_path)
