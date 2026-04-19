"""DDL definitions and schema setup for the local SQLite store."""

from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS spiders (
    spider_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    col_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    cmd TEXT NOT NULL,
    param TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schedules (
    schedule_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    spider_id TEXT NOT NULL,
    cron TEXT NOT NULL,
    cmd TEXT NOT NULL,
    param TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_snapshots (
    task_id TEXT PRIMARY KEY,
    spider_id TEXT NOT NULL,
    schedule_id TEXT,
    status TEXT NOT NULL,
    cmd TEXT NOT NULL,
    param TEXT NOT NULL,
    create_ts TEXT,
    start_ts TEXT,
    end_ts TEXT,
    runtime_ms INTEGER NOT NULL CHECK (runtime_ms >= 0),
    is_manual INTEGER NOT NULL CHECK (is_manual IN (0, 1)),
    execution_key TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    terminal_seen_at TEXT
);

CREATE TABLE IF NOT EXISTS task_log_cursors (
    task_id TEXT PRIMARY KEY,
    page_size INTEGER NOT NULL CHECK (page_size > 0),
    next_page INTEGER NOT NULL CHECK (next_page > 0),
    api_total_lines INTEGER NOT NULL CHECK (api_total_lines >= 0),
    assembled_line_count INTEGER NOT NULL CHECK (assembled_line_count >= 0),
    assembled_log_text TEXT NOT NULL,
    is_complete INTEGER NOT NULL CHECK (is_complete IN (0, 1)),
    final_sync_done INTEGER NOT NULL CHECK (final_sync_done IN (0, 1)),
    last_log_sync_at TEXT,
    terminal_seen_at TEXT,
    FOREIGN KEY (task_id) REFERENCES task_snapshots(task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS run_summaries (
    task_id TEXT PRIMARY KEY,
    execution_key TEXT NOT NULL,
    run_result TEXT NOT NULL,
    confidence TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    counters_json TEXT NOT NULL,
    error_family TEXT,
    parsed_at TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES task_snapshots(task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS incidents (
    incident_id INTEGER PRIMARY KEY AUTOINCREMENT,
    incident_key TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    execution_key TEXT NOT NULL,
    severity TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS spider_profiles (
    execution_key TEXT PRIMARY KEY,
    spider_id TEXT NOT NULL,
    profile_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schedules_spider_id
ON schedules (spider_id);

CREATE INDEX IF NOT EXISTS idx_task_snapshots_schedule_event
ON task_snapshots (schedule_id, create_ts, task_id);

CREATE INDEX IF NOT EXISTS idx_task_snapshots_execution_event
ON task_snapshots (execution_key, create_ts, task_id);

CREATE INDEX IF NOT EXISTS idx_task_snapshots_status_seen
ON task_snapshots (status, last_seen_at, task_id);

CREATE INDEX IF NOT EXISTS idx_task_log_cursors_pending
ON task_log_cursors (final_sync_done, last_log_sync_at, task_id);

CREATE INDEX IF NOT EXISTS idx_run_summaries_execution_key
ON run_summaries (execution_key, parsed_at);

CREATE INDEX IF NOT EXISTS idx_incidents_entity
ON incidents (entity_type, entity_id, last_seen_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_incidents_open_key
ON incidents (incident_key)
WHERE closed_at IS NULL;
"""


def ensure_schema(connection: sqlite3.Connection) -> None:
    """Create the v1 schema if it is missing."""

    current_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if current_version > SCHEMA_VERSION:
        raise RuntimeError(
            "DB schema version "
            f"{current_version} is newer than supported {SCHEMA_VERSION}"
        )

    with connection:
        connection.executescript(SCHEMA_SQL)
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
