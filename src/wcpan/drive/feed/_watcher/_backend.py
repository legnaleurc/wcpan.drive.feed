from typing import Protocol

from .._types import FanotifyWatcherConfig, InotifyWatcherConfig, WatcherConfig
from ._lib import WatcherHandlers


class WatcherBackend(Protocol):
    async def __call__(
        self,
        watch_paths: list[str],
        *,
        handlers: WatcherHandlers,
    ) -> None: ...


def make_watcher_backend(config: WatcherConfig) -> WatcherBackend:
    match config:
        case FanotifyWatcherConfig():
            from ._fanotify import FanotifyWatcher

            return FanotifyWatcher()
        case InotifyWatcherConfig():
            from ._inotify import run_watcher

            return run_watcher
