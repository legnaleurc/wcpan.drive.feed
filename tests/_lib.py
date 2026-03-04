import tempfile
from contextlib import contextmanager
from pathlib import Path

from wcpan.drive.feed._db import initialize_db, upsert_super_root


@contextmanager
def create_db_sandbox():
    """Create a temporary SQLite DB with schema initialized."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        dsn = f.name
    initialize_db(dsn)
    upsert_super_root(dsn)
    try:
        yield dsn
    finally:
        Path(dsn).unlink(missing_ok=True)


@contextmanager
def create_fs_sandbox():
    """Create a temporary directory for filesystem tests."""
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)
