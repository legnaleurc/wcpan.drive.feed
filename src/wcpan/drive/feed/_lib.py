from collections.abc import Callable
from datetime import datetime, timezone
from os import stat_result
from typing import TypeGuard

from ._types import MergedChange, NodeRecord, RemovedChange, UpdatedChange


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
