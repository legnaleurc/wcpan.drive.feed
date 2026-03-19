import asyncio
from pathlib import Path
from typing import Protocol

from .._db import OffMainThread
from .._types import FanotifyWatcherConfig, InotifyWatcherConfig, WatcherConfig
from ._lib import WriteQueue


class WatcherBackend(Protocol):
    async def __call__(
        self,
        watch_paths: list[str],
        off_main: OffMainThread,
        metadata_queue: asyncio.Queue[tuple[str, Path]],
        write_queue: WriteQueue,
        *,
        exclude: tuple[str, ...] = (),
    ) -> None: ...


def make_watcher_backend(config: WatcherConfig) -> WatcherBackend:
    match config:
        case FanotifyWatcherConfig():
            from ._fanotify import FanotifyWatcher

            return FanotifyWatcher()
        case InotifyWatcherConfig():
            from ._inotify import run_watcher

            return run_watcher
