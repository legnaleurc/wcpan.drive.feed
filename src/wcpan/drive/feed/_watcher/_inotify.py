import asyncio
from collections.abc import AsyncIterator
from logging import getLogger
from pathlib import Path

from asyncinotify import Mask, RecursiveInotify

from ._lib import WatcherHandlers


_L = getLogger(__name__)


async def events_with_move_timeout[T](
    source: AsyncIterator[T],
    pending_from: dict[int, tuple[Path, bool]],
    *,
    stale_timeout: float = 1.0,
):
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


# RecursiveInotify internally adds MOVED_FROM | MOVED_TO | CREATE | IGNORED
# to every directory watch for management; we add our file-event flags on top.
_MASK = Mask.CREATE | Mask.DELETE | Mask.CLOSE_WRITE | Mask.MOVED_FROM | Mask.MOVED_TO


async def run_watcher(
    watch_paths: list[str],
    *,
    handlers: WatcherHandlers,
) -> None:
    """Main watcher coroutine. Uses RecursiveInotify for automatic recursive watching."""
    # pending MOVED_FROM: cookie → (src_path, is_dir)
    pending_from: dict[int, tuple[Path, bool]] = {}

    with RecursiveInotify() as inotify:
        for watch_path_str in watch_paths:
            inotify.add_recursive_watch(Path(watch_path_str).resolve(), _MASK)

        async for event in events_with_move_timeout(inotify, pending_from):
            if event is None:
                handlers.flush_pending_moves(pending_from)
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
                handlers.flush_pending_moves(pending_from)

            _L.debug("event %s: %s", event.mask, path)
            if Mask.MOVED_FROM in event.mask:
                pending_from[event.cookie] = (path, is_dir)

            elif Mask.MOVED_TO in event.mask:
                if event.cookie in pending_from:
                    src_path, _ = pending_from.pop(event.cookie)
                    handlers.on_move(src_path, path, is_dir)
                else:
                    if is_dir:
                        handlers.on_dir_created(path, True)
                    else:
                        handlers.on_new_file(path)

            elif Mask.CREATE in event.mask:
                if is_dir:
                    handlers.on_dir_created(path, False)
                # else: ignore — file is empty/partial; metadata arrives on CLOSE_WRITE

            elif Mask.DELETE in event.mask:
                handlers.on_delete(path, is_dir)

            elif Mask.CLOSE_WRITE in event.mask:
                handlers.on_close_write(path)
