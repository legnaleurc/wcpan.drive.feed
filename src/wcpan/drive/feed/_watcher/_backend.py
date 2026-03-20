import asyncio
from pathlib import Path
from typing import Protocol

from .._db import Storage
from .._lib import OffMainThread
from .._types import (
    FanotifyWatcherConfig,
    InotifyWatcherConfig,
    NodeRecord,
    WatcherConfig,
    WriteQueue,
)


class WatcherBackend(Protocol):
    async def __call__(
        self,
        watch_paths: list[str],
        *,
        storage: Storage,
        off_main: OffMainThread,
        metadata_queue: asyncio.Queue[tuple[NodeRecord, Path]],
        write_queue: WriteQueue,
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
