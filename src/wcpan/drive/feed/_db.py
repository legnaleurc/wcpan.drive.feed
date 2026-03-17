import os
import uuid
from collections.abc import Callable
from concurrent.futures import Executor
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from sqlite3 import Row, connect
from typing import Concatenate

from ._types import MergedChange, NodeParams, NodeRecord, RemovedChange, UpdatedChange


SUPER_ROOT_ID = "00000000-0000-0000-0000-000000000000"

_SCHEMA_VERSION = 1

_DDL = """
PRAGMA user_version = 1;
PRAGMA journal_mode = WAL;
PRAGMA cache_size = -65536;

CREATE TABLE IF NOT EXISTS nodes (
    node_id     TEXT    PRIMARY KEY,
    parent_id   TEXT,
    name        TEXT    NOT NULL,
    is_directory INTEGER NOT NULL DEFAULT 0,
    ctime       INTEGER NOT NULL,
    mtime       INTEGER NOT NULL,
    mime_type   TEXT    NOT NULL DEFAULT '',
    hash        TEXT    NOT NULL DEFAULT '',
    size        INTEGER NOT NULL DEFAULT 0,
    is_image    INTEGER NOT NULL DEFAULT 0,
    is_video    INTEGER NOT NULL DEFAULT 0,
    width       INTEGER NOT NULL DEFAULT 0,
    height      INTEGER NOT NULL DEFAULT 0,
    ms_duration INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_nodes_parent_id ON nodes(parent_id);

CREATE TABLE IF NOT EXISTS changes (
    change_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id    TEXT    NOT NULL,
    is_removed INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_changes_node_id ON changes(node_id);
"""


def node_id_from_stat(st: os.stat_result) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{st.st_dev}:{st.st_ino}"))


class OffMainThread:
    def __init__(self, *, dsn: str, pool: Executor) -> None:
        self._dsn = dsn
        self._pool = pool

    async def __call__[**A, R](
        self, fn: Callable[Concatenate[str, A], R], *args: A.args, **kwargs: A.kwargs
    ) -> R:
        from asyncio import get_running_loop
        from functools import partial

        bound = partial(fn, self._dsn, *args, **kwargs)
        loop = get_running_loop()
        return await loop.run_in_executor(self._pool, bound)

    async def run[**A, R](
        self, fn: Callable[A, R], *args: A.args, **kwargs: A.kwargs
    ) -> R:
        from asyncio import get_running_loop
        from functools import partial

        loop = get_running_loop()
        return await loop.run_in_executor(self._pool, partial(fn, *args, **kwargs))


@contextmanager
def _connect(dsn: str, *, timeout: float = 5.0):
    with closing(connect(dsn, timeout=timeout)) as db:
        db.row_factory = Row
        yield db


@contextmanager
def read_only(dsn: str, *, timeout: float = 5.0):
    with _connect(dsn, timeout=timeout) as db, closing(db.cursor()) as cursor:
        yield cursor


@contextmanager
def read_write(dsn: str, *, timeout: float = 5.0):
    with _connect(dsn, timeout=timeout) as db, closing(db.cursor()) as cursor:
        try:
            yield cursor
            if db.in_transaction:
                db.commit()
        except Exception:
            if db.in_transaction:
                db.rollback()
            raise


def ensure_schema(dsn: str) -> None:
    version = get_schema_version(dsn)
    if version == 0:
        initialize_db(dsn)
        return
    if version != _SCHEMA_VERSION:
        raise RuntimeError(
            f"Database schema version {version} does not match "
            f"expected version {_SCHEMA_VERSION}. "
            "Back up and delete the database file, then restart the server."
        )


def checkpoint(dsn: str) -> None:
    """Merge WAL into the main database file and truncate the WAL.

    Call this after the initial scan so the WAL is empty before the process
    can be killed. SQLite WAL mode keeps committed data in the WAL until a
    checkpoint runs; if the process is killed first, committed-but-not-
    checkpointed data survives in the WAL and is replayed on next open — but
    only if the WAL file is not deleted.
    """
    with read_write(dsn) as cursor:
        cursor.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def initialize_db(dsn: str) -> None:
    with read_write(dsn) as cursor:
        cursor.executescript(_DDL)


def upsert_super_root(dsn: str) -> None:
    now = _now_us()
    with read_write(dsn) as cursor:
        cursor.execute(
            """
            INSERT OR IGNORE INTO nodes
                (node_id, parent_id, name, is_directory, ctime, mtime)
            VALUES (?, NULL, '', 1, ?, ?)
            """,
            (SUPER_ROOT_ID, now, now),
        )


_UPSERT_NODE_SQL = """
INSERT INTO nodes
    (node_id, parent_id, name, is_directory, ctime, mtime,
     mime_type, hash, size, is_image, is_video,
     width, height, ms_duration)
VALUES (:node_id, :parent_id, :name, :is_directory, :ctime, :mtime,
        :mime_type, :hash, :size, :is_image, :is_video,
        :width, :height, :ms_duration)
ON CONFLICT(node_id) DO UPDATE SET
    parent_id   = excluded.parent_id,
    name        = excluded.name,
    is_directory = excluded.is_directory,
    ctime       = excluded.ctime,
    mtime       = excluded.mtime,
    mime_type   = excluded.mime_type,
    hash        = excluded.hash,
    size        = excluded.size,
    is_image    = excluded.is_image,
    is_video    = excluded.is_video,
    width       = excluded.width,
    height      = excluded.height,
    ms_duration = excluded.ms_duration
"""


def _node_to_params(node: NodeRecord) -> NodeParams:
    return NodeParams(
        node_id=node.node_id,
        parent_id=node.parent_id,
        name=node.name,
        is_directory=1 if node.is_directory else 0,
        ctime=_dt_to_us(node.ctime),
        mtime=_dt_to_us(node.mtime),
        mime_type=node.mime_type,
        hash=node.hash,
        size=node.size,
        is_image=1 if node.is_image else 0,
        is_video=1 if node.is_video else 0,
        width=node.width,
        height=node.height,
        ms_duration=node.ms_duration,
    )


def upsert_node(dsn: str, node: NodeRecord) -> None:
    with read_write(dsn) as cursor:
        cursor.execute(_UPSERT_NODE_SQL, _node_to_params(node))


def upsert_node_and_emit_change(dsn: str, node: NodeRecord) -> None:
    """Atomically upsert a node and emit an update change record."""
    with read_write(dsn) as cursor:
        cursor.execute(_UPSERT_NODE_SQL, _node_to_params(node))
        cursor.execute(
            "INSERT INTO changes (node_id, is_removed) VALUES (?, 0)",
            (node.node_id,),
        )


def delete_nodes_and_emit_changes(dsn: str, node_ids: list[str]) -> None:
    """Atomically delete nodes and emit removal change records."""
    with read_write(dsn) as cursor:
        cursor.executemany(
            "INSERT INTO changes (node_id, is_removed) VALUES (?, 1)",
            [(nid,) for nid in node_ids],
        )
        cursor.executemany(
            "DELETE FROM nodes WHERE node_id = ?",
            [(nid,) for nid in node_ids],
        )


def get_node_by_id(dsn: str, node_id: str) -> NodeRecord | None:
    with read_only(dsn) as cursor:
        cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
        row = cursor.fetchone()
    if row is None:
        return None
    return _row_to_node(row)


def get_node_by_parent_name(dsn: str, parent_id: str, name: str) -> NodeRecord | None:
    with read_only(dsn) as cursor:
        cursor.execute(
            "SELECT * FROM nodes WHERE parent_id = ? AND name = ?",
            (parent_id, name),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return _row_to_node(row)


def get_children(dsn: str, parent_id: str) -> list[NodeRecord]:
    with read_only(dsn) as cursor:
        cursor.execute("SELECT * FROM nodes WHERE parent_id = ?", (parent_id,))
        rows = cursor.fetchall()
    return [_row_to_node(r) for r in rows]


def delete_node(dsn: str, node_id: str) -> None:
    with read_write(dsn) as cursor:
        cursor.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))


def emit_change(dsn: str, node_id: str, *, is_removed: bool) -> int:
    with read_write(dsn) as cursor:
        cursor.execute(
            "INSERT INTO changes (node_id, is_removed) VALUES (?, ?)",
            (node_id, 1 if is_removed else 0),
        )
        return cursor.lastrowid  # type: ignore[return-value]


def get_cursor(dsn: str) -> int:
    with read_only(dsn) as cursor:
        cursor.execute("SELECT MAX(change_id) FROM changes")
        row = cursor.fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def get_changes_since(dsn: str, cursor: int) -> tuple[list[MergedChange], int]:
    with read_only(dsn) as c:
        c.execute(
            "SELECT change_id, node_id, is_removed FROM changes WHERE change_id > ? ORDER BY change_id",
            (cursor,),
        )
        rows = c.fetchall()

    if not rows:
        return [], cursor

    last_event: dict[str, tuple[bool, int]] = {}
    max_id = cursor
    for row in rows:
        last_event[row["node_id"]] = (bool(row["is_removed"]), row["change_id"])
        max_id = max(max_id, row["change_id"])

    update_ids = [nid for nid, (removed, _) in last_event.items() if not removed]
    if update_ids:
        placeholders = ",".join("?" * len(update_ids))
        with read_only(dsn) as c:
            c.execute(
                f"SELECT * FROM nodes WHERE node_id IN ({placeholders})",
                update_ids,
            )
            nodes_by_id = {row["node_id"]: _row_to_node(row) for row in c.fetchall()}
    else:
        nodes_by_id = {}

    result: list[MergedChange] = []
    for node_id, (is_removed, _) in last_event.items():
        if is_removed:
            result.append(RemovedChange(removed=True, node_id=node_id))
        else:
            node = nodes_by_id.get(node_id)
            if node is not None:
                result.append(UpdatedChange(removed=False, node=node))

    return result, max_id


def get_all_node_ids_under(dsn: str, parent_id: str) -> list[str]:
    """Recursively collect all node_ids under parent_id (depth-first)."""
    result: list[str] = []
    stack = [parent_id]
    while stack:
        pid = stack.pop()
        with read_only(dsn) as cursor:
            cursor.execute(
                "SELECT node_id, is_directory FROM nodes WHERE parent_id = ?", (pid,)
            )
            rows = cursor.fetchall()
        for row in rows:
            result.append(row["node_id"])
            if row["is_directory"]:
                stack.append(row["node_id"])
    return result


def get_all_node_ids_by_parent(dsn: str, parent_id: str) -> list[str]:
    with read_only(dsn) as cursor:
        cursor.execute("SELECT node_id FROM nodes WHERE parent_id = ?", (parent_id,))
        rows = cursor.fetchall()
    return [r["node_id"] for r in rows]


def get_all_nodes(dsn: str) -> dict[str, NodeRecord]:
    """Load all nodes in a single query, keyed by node_id."""
    with read_only(dsn) as cursor:
        cursor.execute("SELECT * FROM nodes")
        rows = cursor.fetchall()
    return {row["node_id"]: _row_to_node(row) for row in rows}


def _row_to_node(row: Row) -> NodeRecord:
    return NodeRecord(
        node_id=row["node_id"],
        parent_id=row["parent_id"],
        name=row["name"],
        is_directory=bool(row["is_directory"]),
        ctime=_us_to_dt(row["ctime"]),
        mtime=_us_to_dt(row["mtime"]),
        mime_type=row["mime_type"],
        hash=row["hash"],
        size=row["size"],
        is_image=bool(row["is_image"]),
        is_video=bool(row["is_video"]),
        width=row["width"],
        height=row["height"],
        ms_duration=row["ms_duration"],
    )


def _dt_to_us(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000)


def _us_to_dt(us: int) -> datetime:
    return datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc)


def _now_us() -> int:
    from datetime import datetime, timezone

    return int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000)


def update_node_metadata(
    dsn: str,
    node_id: str,
    *,
    mime_type: str,
    hash: str,
    size: int,
    is_image: bool,
    is_video: bool,
    width: int,
    height: int,
    ms_duration: int,
) -> None:
    with read_write(dsn) as cursor:
        cursor.execute(
            """
            UPDATE nodes SET
                mime_type   = ?,
                hash        = ?,
                size        = ?,
                is_image    = ?,
                is_video    = ?,
                width       = ?,
                height      = ?,
                ms_duration = ?
            WHERE node_id = ?
            """,
            (
                mime_type,
                hash,
                size,
                1 if is_image else 0,
                1 if is_video else 0,
                width,
                height,
                ms_duration,
                node_id,
            ),
        )


def update_node_mtime_size(
    dsn: str, node_id: str, *, mtime: datetime, size: int
) -> None:
    with read_write(dsn) as cursor:
        cursor.execute(
            "UPDATE nodes SET mtime = ?, size = ?, hash = '' WHERE node_id = ?",
            (_dt_to_us(mtime), size, node_id),
        )


def get_schema_version(dsn: str) -> int:
    with read_only(dsn) as cursor:
        cursor.execute("PRAGMA user_version")
        row = cursor.fetchone()
    return int(row[0])


def bulk_upsert_nodes(dsn: str, items: list[NodeRecord]) -> None:
    """Insert/update many nodes in a single transaction."""
    with read_write(dsn) as cursor:
        cursor.executemany(_UPSERT_NODE_SQL, [_node_to_params(n) for n in items])


def bulk_emit_changes(dsn: str, changes: list[tuple[str, bool]]) -> None:
    """Insert many change records in a single transaction."""
    with read_write(dsn) as cursor:
        cursor.executemany(
            "INSERT INTO changes (node_id, is_removed) VALUES (?, ?)",
            [(node_id, 1 if removed else 0) for node_id, removed in changes],
        )


def get_ancestor_chain(dsn: str, node_id: str) -> list[NodeRecord] | None:
    if node_id == SUPER_ROOT_ID:
        return []
    chain: list[NodeRecord] = []
    current_id = node_id
    with read_only(dsn) as cursor:
        while current_id != SUPER_ROOT_ID:
            cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (current_id,))
            row = cursor.fetchone()
            if row is None:
                return None
            node = _row_to_node(row)
            chain.append(node)
            current_id = node.parent_id
    return chain


def bulk_delete_nodes(dsn: str, node_ids: list[str]) -> None:
    """Delete many nodes in a single transaction."""
    with read_write(dsn) as cursor:
        cursor.executemany(
            "DELETE FROM nodes WHERE node_id = ?",
            [(nid,) for nid in node_ids],
        )
