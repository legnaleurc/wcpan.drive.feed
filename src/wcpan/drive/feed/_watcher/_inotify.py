import asyncio
from logging import getLogger
from pathlib import Path

from asyncinotify import Mask, RecursiveInotify

from .._db import Storage
from .._lib import OffMainThread
from .._types import NodeRecord, WriteQueue
from ._lib import (
    events_with_move_timeout,
    flush_pending_moves,
    on_close_write,
    on_delete,
    on_dir_created,
    on_file_stub,
    on_move,
)


_L = getLogger(__name__)

# RecursiveInotify internally adds MOVED_FROM | MOVED_TO | CREATE | IGNORED
# to every directory watch for management; we add our file-event flags on top.
_MASK = Mask.CREATE | Mask.DELETE | Mask.CLOSE_WRITE | Mask.MOVED_FROM | Mask.MOVED_TO


async def run_watcher(
    watch_paths: list[str],
    storage: Storage,
    off_main: OffMainThread,
    metadata_queue: asyncio.Queue[tuple[NodeRecord, Path]],
    write_queue: WriteQueue,
    *,
    exclude: tuple[str, ...] = (),
) -> None:
    """Main watcher coroutine. Uses RecursiveInotify for automatic recursive watching."""
    # pending MOVED_FROM: cookie → (src_path, is_dir)
    pending_from: dict[int, tuple[Path, bool]] = {}

    with RecursiveInotify() as inotify:
        for watch_path_str in watch_paths:
            inotify.add_recursive_watch(Path(watch_path_str).resolve(), _MASK)

        async for event in events_with_move_timeout(inotify, pending_from):
            if event is None:
                await flush_pending_moves(pending_from, storage, off_main, write_queue)
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
                await flush_pending_moves(pending_from, storage, off_main, write_queue)

            try:
                _L.debug("event %s: %s", event.mask, path)
                if Mask.MOVED_FROM in event.mask:
                    pending_from[event.cookie] = (path, is_dir)

                elif Mask.MOVED_TO in event.mask:
                    # Assume new arrival unless on_move confirms it was tracked.
                    is_new_arrival = True
                    if event.cookie in pending_from:
                        src_path, _ = pending_from.pop(event.cookie)
                        is_new_arrival = not await on_move(
                            src_path,
                            path,
                            is_dir,
                            storage,
                            off_main,
                            write_queue,
                            exclude,
                        )
                        if is_new_arrival:
                            _L.debug(
                                "move source untracked, treating dst as new: %s", path
                            )

                    if is_new_arrival:
                        # Moved in from outside watched area, or source was an
                        # untracked temp file (e.g. excluded upload staging file).
                        if is_dir:
                            await on_dir_created(
                                path,
                                storage,
                                off_main,
                                metadata_queue,
                                write_queue,
                                scan_contents=True,
                                exclude=exclude,
                            )
                        else:
                            await on_file_stub(
                                path, storage, off_main, metadata_queue, exclude
                            )

                elif Mask.CREATE in event.mask:
                    if is_dir:
                        await on_dir_created(
                            path,
                            storage,
                            off_main,
                            metadata_queue,
                            write_queue,
                            scan_contents=False,
                            exclude=exclude,
                        )
                    else:
                        # Stub only; metadata arrives on CLOSE_WRITE
                        await on_file_stub(
                            path, storage, off_main, metadata_queue, exclude
                        )

                elif Mask.DELETE in event.mask:
                    await on_delete(path, is_dir, storage, off_main, write_queue)

                elif Mask.CLOSE_WRITE in event.mask:
                    await on_close_write(
                        path, storage, off_main, metadata_queue, exclude
                    )

            except Exception:
                _L.exception("event handler failed: %s %s", event.mask, path)
