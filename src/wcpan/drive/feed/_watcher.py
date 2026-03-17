import asyncio
from logging import getLogger
from pathlib import Path

from asyncinotify import Mask, RecursiveInotify


_L = getLogger(__name__)

from ._db import (
    OffMainThread,
    delete_nodes_and_emit_changes,
    get_all_node_ids_under,
    get_node_by_id,
    get_node_by_parent_name,
    node_id_from_stat,
    upsert_node_and_emit_change,
)
from ._exclude import is_excluded
from ._lib import stat_to_times
from ._types import NodeRecord


# RecursiveInotify internally adds MOVED_FROM | MOVED_TO | CREATE | IGNORED
# to every directory watch for management; we add our file-event flags on top.
_MASK = Mask.CREATE | Mask.DELETE | Mask.CLOSE_WRITE | Mask.MOVED_FROM | Mask.MOVED_TO


async def _get_parent_node_id(path: Path, off_main: OffMainThread) -> str | None:
    try:
        st = path.parent.stat()
    except OSError:
        return None
    parent_id = node_id_from_stat(st)
    parent = await off_main(get_node_by_id, parent_id)
    return parent_id if parent else None


async def _on_file_stub(
    path: Path, off_main: OffMainThread, exclude: tuple[str, ...] = ()
) -> None:
    """Insert a stub node (hash='') on IN_CREATE for a file."""
    if is_excluded(path.name, exclude):
        return
    try:
        st = path.stat()
    except OSError:
        return
    ctime, mtime = stat_to_times(st)
    node_id = node_id_from_stat(st)
    _L.debug("create file stub: %s", path)
    parent_id = await _get_parent_node_id(path, off_main)
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
    await off_main(upsert_node_and_emit_change, node)


async def _on_close_write(
    path: Path,
    off_main: OffMainThread,
    metadata_queue: asyncio.Queue[tuple[str, Path]],
    exclude: tuple[str, ...] = (),
) -> None:
    """Update or insert file node on IN_CLOSE_WRITE, then queue for metadata."""
    if is_excluded(path.name, exclude):
        return
    try:
        st = path.stat()
    except OSError:
        return
    _, mtime = stat_to_times(st)
    node_id = node_id_from_stat(st)
    _L.debug("close write: %s", path)
    existing = await off_main(get_node_by_id, node_id)
    if existing is None:
        # Missed the CREATE — insert stub now
        await _on_file_stub(path, off_main, exclude)
        existing = await off_main(get_node_by_id, node_id)
        if existing is None:
            return
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
    await off_main(upsert_node_and_emit_change, node)
    await metadata_queue.put((existing.node_id, path))


async def _on_delete(
    path: Path,
    is_dir: bool,
    off_main: OffMainThread,
) -> None:
    """Emit remove for a deleted file or directory (recursively for dirs).

    Watch removal is handled automatically by RecursiveInotify via IGNORED events.
    """
    _L.debug("delete: %s (is_dir=%s)", path, is_dir)
    try:
        parent_st = path.parent.stat()
    except OSError:
        return
    parent_node = await off_main(get_node_by_id, node_id_from_stat(parent_st))
    if parent_node is None:
        return
    node = await off_main(get_node_by_parent_name, parent_node.node_id, path.name)
    if node is None:
        return
    node_ids = [node.node_id]
    if is_dir:
        node_ids += await off_main(get_all_node_ids_under, node.node_id)
    await off_main(delete_nodes_and_emit_changes, node_ids)


async def _on_move(
    src: Path,
    dst: Path,
    is_dir: bool,
    off_main: OffMainThread,
    exclude: tuple[str, ...] = (),
) -> bool:
    """Update parent_id + name for a renamed/moved node (no re-hash needed).

    Watch updates for directories are handled automatically by RecursiveInotify.
    Returns False if the source node was not in the DB (caller should treat dst
    as a newly-arrived file/directory instead).
    """
    if is_excluded(dst.name, exclude):
        return True
    _L.debug("move: %s -> %s", src, dst)
    try:
        st = dst.stat()
    except OSError:
        return True
    node_id = node_id_from_stat(st)
    existing = await off_main(get_node_by_id, node_id)
    if existing is None:
        # Source was never tracked (e.g. temp file that was excluded).
        # Signal the caller to treat dst as a new arrival.
        return False
    new_parent = await _get_parent_node_id(dst, off_main)
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
    await off_main(upsert_node_and_emit_change, node)
    return True


async def _on_dir_created(
    path: Path,
    off_main: OffMainThread,
    metadata_queue: asyncio.Queue[tuple[str, Path]],
    *,
    scan_contents: bool,
    exclude: tuple[str, ...] = (),
) -> None:
    """Insert a directory node into the DB.

    RecursiveInotify already adds the inotify watch before yielding the event.
    scan_contents=True when the directory was moved in from outside the watched
    area and may already contain files that won't trigger further events.
    """
    if is_excluded(path.name, exclude):
        return
    try:
        st = path.stat()
    except OSError:
        return
    ctime, mtime = stat_to_times(st)
    node_id = node_id_from_stat(st)
    _L.debug("dir created: %s (scan_contents=%s)", path, scan_contents)
    parent_id = await _get_parent_node_id(path, off_main)
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
    await off_main(upsert_node_and_emit_change, node)

    if not scan_contents:
        return

    # Recursively insert DB records for pre-existing contents (moved-in dir).
    # Watches are already in place from RecursiveInotify.
    stack = [(path, node_id)]
    while stack:
        cur_dir, cur_parent_id = stack.pop()
        try:
            entries = list(cur_dir.iterdir())
        except OSError:
            continue
        for entry in entries:
            if is_excluded(entry.name, exclude):
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
            await off_main(upsert_node_and_emit_change, enode)
            if entry.is_dir():
                stack.append((entry, eid))
            else:
                await metadata_queue.put((eid, entry))


def _node_id_for(path: Path) -> str | None:
    try:
        return node_id_from_stat(path.stat())
    except OSError:
        return None


async def _flush_pending_moves(
    pending_from: dict[int, tuple[Path, bool]], off_main: OffMainThread
) -> None:
    for cookie, (stale_path, stale_is_dir) in list(pending_from.items()):
        del pending_from[cookie]
        try:
            await _on_delete(stale_path, stale_is_dir, off_main)
        except Exception:
            _L.exception("delete failed for stale move: %s", stale_path)


async def _events_with_move_timeout(source, pending_from, stale_timeout=1.0):
    """Yield inotify events, injecting None when a pending MOVED_FROM goes
    unmatched for stale_timeout seconds (file moved outside watched area).
    """
    src = aiter(source)
    while True:
        timeout = stale_timeout if pending_from else None
        try:
            async with asyncio.timeout(timeout):
                yield await anext(src)
        except TimeoutError:
            yield None
        except StopAsyncIteration:
            return


async def run_watcher(
    watch_paths: list[str],
    off_main: OffMainThread,
    metadata_queue: asyncio.Queue[tuple[str, Path]],
    *,
    exclude: tuple[str, ...] = (),
) -> None:
    """Main watcher coroutine. Uses RecursiveInotify for automatic recursive watching."""
    # pending MOVED_FROM: cookie → (src_path, is_dir)
    pending_from: dict[int, tuple[Path, bool]] = {}

    with RecursiveInotify() as inotify:
        for watch_path_str in watch_paths:
            inotify.add_recursive_watch(Path(watch_path_str).resolve(), _MASK)

        async for event in _events_with_move_timeout(inotify, pending_from):
            if event is None:
                await _flush_pending_moves(pending_from, off_main)
                continue

            if event.path is None:
                if Mask.Q_OVERFLOW in event.mask:
                    _L.warning(
                        "inotify queue overflow — some filesystem events may have been missed"
                    )
                continue
            if Mask.IGNORED in event.mask:
                continue

            is_dir = Mask.ISDIR in event.mask
            path = event.path

            # Flush unmatched MOVED_FROM entries as deletes
            if Mask.MOVED_TO not in event.mask:
                await _flush_pending_moves(pending_from, off_main)

            try:
                _L.debug("event %s: %s", event.mask, path)
                if Mask.MOVED_FROM in event.mask:
                    pending_from[event.cookie] = (path, is_dir)

                elif Mask.MOVED_TO in event.mask:
                    # Assume new arrival unless _on_move confirms it was tracked.
                    is_new_arrival = True
                    if event.cookie in pending_from:
                        src_path, _ = pending_from.pop(event.cookie)
                        is_new_arrival = not await _on_move(
                            src_path, path, is_dir, off_main, exclude
                        )
                        if is_new_arrival:
                            _L.debug(
                                "move source untracked, treating dst as new: %s", path
                            )

                    if is_new_arrival:
                        # Moved in from outside watched area, or source was an
                        # untracked temp file (e.g. excluded upload staging file).
                        if is_dir:
                            await _on_dir_created(
                                path,
                                off_main,
                                metadata_queue,
                                scan_contents=True,
                                exclude=exclude,
                            )
                        else:
                            await _on_file_stub(path, off_main, exclude)
                            node_id = _node_id_for(path)
                            if node_id is not None:
                                await metadata_queue.put((node_id, path))

                elif Mask.CREATE in event.mask:
                    if is_dir:
                        await _on_dir_created(
                            path,
                            off_main,
                            metadata_queue,
                            scan_contents=False,
                            exclude=exclude,
                        )
                    else:
                        # Stub only; metadata arrives on CLOSE_WRITE
                        await _on_file_stub(path, off_main, exclude)

                elif Mask.DELETE in event.mask:
                    await _on_delete(path, is_dir, off_main)

                elif Mask.CLOSE_WRITE in event.mask:
                    await _on_close_write(path, off_main, metadata_queue, exclude)

            except Exception:
                _L.exception("event handler failed: %s %s", event.mask, path)
