"""Bounded catalog-plan reuse; authority revisions are maintained transactionally."""
from contextlib import contextmanager
import sqlite3


def supported(connection: sqlite3.Connection) -> bool:
    return connection.execute("PRAGMA user_version").fetchone()[0] >= 23


def revision(connection: sqlite3.Connection) -> int:
    return int(connection.execute("SELECT revision FROM capture_catalog_revision WHERE id=1").fetchone()[0])


@contextmanager
def projection(connection: sqlite3.Connection):
    """Suppress only derived projection writes, for this serialized transaction."""
    if not supported(connection):
        yield
        return
    if not connection.in_transaction:
        raise ValueError("catalog projection requires writer transaction")
    connection.execute("UPDATE capture_catalog_revision SET projection_depth=projection_depth+1 WHERE id=1")
    try:
        yield
    finally:
        connection.execute("UPDATE capture_catalog_revision SET projection_depth=projection_depth-1 WHERE id=1")
