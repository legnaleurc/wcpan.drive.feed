from collections.abc import Callable
from concurrent.futures import Executor
from datetime import datetime, timezone
from functools import partial
from os import stat_result
from typing import TypeGuard

from ._types import MergedChange, NodeRecord, RemovedChange, UpdatedChange


class OffMainThread:
    def __init__(self, pool: Executor) -> None:
        self._pool = pool

    async def __call__[**A, R](
        self, fn: Callable[A, R], *args: A.args, **kwargs: A.kwargs
    ) -> R:
        from asyncio import get_running_loop

        loop = get_running_loop()
        return await loop.run_in_executor(self._pool, partial(fn, *args, **kwargs))


def is_removed_change(change: MergedChange) -> TypeGuard[RemovedChange]:
    return change.removed


def is_updated_change(change: MergedChange) -> TypeGuard[UpdatedChange]:
    return not change.removed


def dispatch_change[R](
    change: MergedChange,
    *,
    on_updated: Callable[[NodeRecord], R],
    on_removed: Callable[[str], R],
) -> R:
    match change:
        case UpdatedChange():
            return on_updated(change.node)
        case RemovedChange():
            return on_removed(change.node_id)


def stat_to_times(st: stat_result) -> tuple[datetime, datetime]:
    ctime = datetime.fromtimestamp(st.st_ctime, tz=timezone.utc)
    mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
    return ctime, mtime
