"""SQLite connection management for the local history store."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


def connect_sqlite(
    db_path: str | Path,
    *,
    timeout: float = 5.0,
    uri: bool = False,
) -> sqlite3.Connection:
    """Open a SQLite connection configured for local poller/UI concurrency."""

    database = str(db_path)
    connection = sqlite3.connect(database, timeout=timeout, uri=uri)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=5000")

    journal_mode_row = connection.execute("PRAGMA journal_mode=WAL").fetchone()
    journal_mode = str(journal_mode_row[0]).lower() if journal_mode_row else ""
    if not _is_in_memory_database(database, uri=uri) and journal_mode != "wal":
        connection.close()
        raise RuntimeError(
            "SQLite WAL mode is required for overlapping poller writes and UI reads"
        )

    return connection


@contextmanager
def get_connection(
    db_path: str | Path,
    *,
    timeout: float = 5.0,
    uri: bool = False,
) -> Iterator[sqlite3.Connection]:
    """Yield a configured SQLite connection and close it afterwards."""

    connection = connect_sqlite(db_path, timeout=timeout, uri=uri)
    try:
        yield connection
    finally:
        connection.close()


def _is_in_memory_database(database: str, *, uri: bool) -> bool:
    if database == ":memory:":
        return True
    if not uri:
        return False
    return database.startswith("file::memory:") or "mode=memory" in database
