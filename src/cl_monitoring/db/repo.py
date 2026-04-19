"""Repository methods for the local SQLite history store."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from cl_monitoring.domain import (
    Confidence,
    ErrorFamily,
    RunResult,
    RunSummary,
    ScheduleSnapshot,
    SpiderSnapshot,
    TaskSnapshot,
)

from .tables import ensure_schema


@dataclass(frozen=True)
class StoredTaskSnapshot:
    snapshot: TaskSnapshot
    first_seen_at: datetime
    last_seen_at: datetime
    terminal_seen_at: datetime | None


@dataclass(frozen=True)
class TaskLogCursor:
    task_id: str
    page_size: int
    next_page: int
    api_total_lines: int
    assembled_line_count: int
    assembled_log_text: str
    is_complete: bool
    final_sync_done: bool
    last_log_sync_at: datetime | None
    terminal_seen_at: datetime | None

    @property
    def assembled_lines(self) -> list[str]:
        if not self.assembled_log_text:
            return []
        return self.assembled_log_text.splitlines()


@dataclass(frozen=True)
class IncidentProjection:
    incident_key: str
    entity_type: str
    entity_id: str
    execution_key: str
    severity: str
    reason_code: str
    evidence: list[str]


@dataclass(frozen=True)
class IncidentRecord:
    incident_id: int
    incident_key: str
    entity_type: str
    entity_id: str
    execution_key: str
    severity: str
    reason_code: str
    evidence: list[str]
    opened_at: datetime
    closed_at: datetime | None
    last_seen_at: datetime


@dataclass(frozen=True)
class SpiderProfileRecord:
    execution_key: str
    spider_id: str
    profile: dict[str, Any]
    updated_at: datetime


class LocalRepository:
    """Typed read/write access to the local SQLite store."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        ensure_schema(self._connection)

    def save_spiders(
        self,
        spiders: list[SpiderSnapshot],
        *,
        seen_at: datetime | None = None,
    ) -> None:
        if not spiders:
            return

        seen_value = _dt_to_db(_coerce_timestamp(seen_at))
        rows = [
            (
                spider.id,
                spider.name,
                spider.col_id,
                spider.project_id,
                spider.cmd,
                spider.param,
                seen_value,
            )
            for spider in spiders
        ]
        with self._connection:
            self._connection.executemany(
                """
                INSERT INTO spiders (
                    spider_id,
                    name,
                    col_id,
                    project_id,
                    cmd,
                    param,
                    last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(spider_id) DO UPDATE SET
                    name = excluded.name,
                    col_id = excluded.col_id,
                    project_id = excluded.project_id,
                    cmd = excluded.cmd,
                    param = excluded.param,
                    last_seen_at = excluded.last_seen_at
                """,
                rows,
            )

    def list_spiders(self) -> list[SpiderSnapshot]:
        rows = self._connection.execute(
            "SELECT * FROM spiders ORDER BY spider_id"
        ).fetchall()
        return [_spider_from_row(row) for row in rows]

    def delete_spiders(
        self,
        spider_ids: set[str] | list[str] | tuple[str, ...],
    ) -> None:
        unique_ids = sorted({spider_id for spider_id in spider_ids if spider_id})
        if not unique_ids:
            return

        placeholders = ", ".join("?" for _ in unique_ids)
        with self._connection:
            self._connection.execute(
                f"DELETE FROM spiders WHERE spider_id IN ({placeholders})",
                tuple(unique_ids),
            )

    def save_schedules(
        self,
        schedules: list[ScheduleSnapshot],
        *,
        seen_at: datetime | None = None,
    ) -> None:
        if not schedules:
            return

        seen_value = _dt_to_db(_coerce_timestamp(seen_at))
        rows = [
            (
                schedule.id,
                schedule.name,
                schedule.spider_id,
                schedule.cron,
                schedule.cmd,
                schedule.param,
                int(schedule.enabled),
                seen_value,
            )
            for schedule in schedules
        ]
        with self._connection:
            self._connection.executemany(
                """
                INSERT INTO schedules (
                    schedule_id,
                    name,
                    spider_id,
                    cron,
                    cmd,
                    param,
                    enabled,
                    last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(schedule_id) DO UPDATE SET
                    name = excluded.name,
                    spider_id = excluded.spider_id,
                    cron = excluded.cron,
                    cmd = excluded.cmd,
                    param = excluded.param,
                    enabled = excluded.enabled,
                    last_seen_at = excluded.last_seen_at
                """,
                rows,
            )

    def list_schedules(self) -> list[ScheduleSnapshot]:
        rows = self._connection.execute(
            "SELECT * FROM schedules ORDER BY schedule_id"
        ).fetchall()
        return [_schedule_from_row(row) for row in rows]

    def save_task_snapshots(
        self,
        tasks: list[TaskSnapshot],
        *,
        seen_at: datetime | None = None,
    ) -> None:
        if not tasks:
            return

        seen_value = _dt_to_db(_coerce_timestamp(seen_at))
        rows = []
        for task in tasks:
            terminal_seen_at = seen_value if _is_terminal(task.status) else None
            rows.append(
                (
                    task.id,
                    task.spider_id,
                    task.schedule_id,
                    task.status,
                    task.cmd,
                    task.param,
                    _dt_to_db(task.create_ts),
                    _dt_to_db(task.start_ts),
                    _dt_to_db(task.end_ts),
                    _runtime_to_ms(task.runtime),
                    int(task.is_manual),
                    task.execution_key,
                    seen_value,
                    seen_value,
                    terminal_seen_at,
                )
            )

        with self._connection:
            self._connection.executemany(
                """
                INSERT INTO task_snapshots (
                    task_id,
                    spider_id,
                    schedule_id,
                    status,
                    cmd,
                    param,
                    create_ts,
                    start_ts,
                    end_ts,
                    runtime_ms,
                    is_manual,
                    execution_key,
                    first_seen_at,
                    last_seen_at,
                    terminal_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    spider_id = excluded.spider_id,
                    schedule_id = excluded.schedule_id,
                    status = excluded.status,
                    cmd = excluded.cmd,
                    param = excluded.param,
                    create_ts = excluded.create_ts,
                    start_ts = excluded.start_ts,
                    end_ts = excluded.end_ts,
                    runtime_ms = excluded.runtime_ms,
                    is_manual = excluded.is_manual,
                    execution_key = excluded.execution_key,
                    last_seen_at = CASE
                        WHEN excluded.last_seen_at > task_snapshots.last_seen_at
                            THEN excluded.last_seen_at
                        ELSE task_snapshots.last_seen_at
                    END,
                    terminal_seen_at = COALESCE(
                        task_snapshots.terminal_seen_at,
                        excluded.terminal_seen_at
                    )
                """,
                rows,
            )

    def get_task_snapshot(self, task_id: str) -> TaskSnapshot | None:
        row = self._connection.execute(
            "SELECT * FROM task_snapshots WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            return None
        return _task_from_row(row)

    def get_task_record(self, task_id: str) -> StoredTaskSnapshot | None:
        row = self._connection.execute(
            "SELECT * FROM task_snapshots WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            return None
        return _stored_task_from_row(row)

    def list_task_snapshots(self) -> list[TaskSnapshot]:
        rows = self._connection.execute(
            "SELECT * FROM task_snapshots "
            "ORDER BY COALESCE(create_ts, start_ts, end_ts, last_seen_at), "
            "task_id"
        ).fetchall()
        return [_task_from_row(row) for row in rows]

    def list_tasks_for_schedule(self, schedule_id: str) -> list[TaskSnapshot]:
        rows = self._connection.execute(
            """
            SELECT *
            FROM task_snapshots
            WHERE schedule_id = ? AND is_manual = 0
            ORDER BY COALESCE(create_ts, start_ts, end_ts, last_seen_at), task_id
            """,
            (schedule_id,),
        ).fetchall()
        return [_task_from_row(row) for row in rows]

    def list_manual_tasks_for_execution_key(
        self, execution_key: str
    ) -> list[TaskSnapshot]:
        rows = self._connection.execute(
            """
            SELECT *
            FROM task_snapshots
            WHERE execution_key = ? AND is_manual = 1
            ORDER BY COALESCE(create_ts, start_ts, end_ts, last_seen_at), task_id
            """,
            (execution_key,),
        ).fetchall()
        return [_task_from_row(row) for row in rows]

    def list_distinct_task_spider_ids(self) -> set[str]:
        rows = self._connection.execute(
            "SELECT DISTINCT spider_id FROM task_snapshots WHERE spider_id <> ''"
        ).fetchall()
        return {str(row[0]) for row in rows}

    def list_tasks_requiring_log_sync(self) -> list[StoredTaskSnapshot]:
        rows = self._connection.execute(
            """
            SELECT t.*
            FROM task_snapshots AS t
            LEFT JOIN task_log_cursors AS c ON c.task_id = t.task_id
            WHERE t.status IN ('pending', 'running')
               OR COALESCE(c.final_sync_done, 0) = 0
            ORDER BY COALESCE(
                t.create_ts,
                t.start_ts,
                t.end_ts,
                t.last_seen_at
            ),
                     t.task_id
            """
        ).fetchall()
        return [_stored_task_from_row(row) for row in rows]

    def get_log_cursor(self, task_id: str) -> TaskLogCursor | None:
        row = self._connection.execute(
            "SELECT * FROM task_log_cursors WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            return None
        return _cursor_from_row(row)

    def save_log_cursor(self, cursor: TaskLogCursor) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO task_log_cursors (
                    task_id,
                    page_size,
                    next_page,
                    api_total_lines,
                    assembled_line_count,
                    assembled_log_text,
                    is_complete,
                    final_sync_done,
                    last_log_sync_at,
                    terminal_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    page_size = excluded.page_size,
                    next_page = CASE
                        WHEN excluded.next_page > task_log_cursors.next_page
                            THEN excluded.next_page
                        ELSE task_log_cursors.next_page
                    END,
                    api_total_lines = CASE
                        WHEN excluded.api_total_lines > task_log_cursors.api_total_lines
                            THEN excluded.api_total_lines
                        ELSE task_log_cursors.api_total_lines
                    END,
                    assembled_line_count = CASE
                        WHEN excluded.assembled_line_count
                            >= task_log_cursors.assembled_line_count
                            THEN excluded.assembled_line_count
                        ELSE task_log_cursors.assembled_line_count
                    END,
                    assembled_log_text = CASE
                        WHEN excluded.assembled_line_count
                            >= task_log_cursors.assembled_line_count
                            THEN excluded.assembled_log_text
                        ELSE task_log_cursors.assembled_log_text
                    END,
                    is_complete = CASE
                        WHEN task_log_cursors.is_complete = 1
                            OR excluded.is_complete = 1
                            THEN 1
                        ELSE 0
                    END,
                    final_sync_done = CASE
                        WHEN task_log_cursors.final_sync_done = 1
                            OR excluded.final_sync_done = 1
                            THEN 1
                        ELSE 0
                    END,
                    last_log_sync_at = CASE
                        WHEN task_log_cursors.last_log_sync_at IS NULL
                            THEN excluded.last_log_sync_at
                        WHEN excluded.last_log_sync_at IS NULL
                            THEN task_log_cursors.last_log_sync_at
                        WHEN excluded.last_log_sync_at
                            > task_log_cursors.last_log_sync_at
                            THEN excluded.last_log_sync_at
                        ELSE task_log_cursors.last_log_sync_at
                    END,
                    terminal_seen_at = COALESCE(
                        task_log_cursors.terminal_seen_at,
                        excluded.terminal_seen_at
                    )
                """,
                (
                    cursor.task_id,
                    cursor.page_size,
                    cursor.next_page,
                    cursor.api_total_lines,
                    cursor.assembled_line_count,
                    cursor.assembled_log_text,
                    int(cursor.is_complete),
                    int(cursor.final_sync_done),
                    _dt_to_db(cursor.last_log_sync_at),
                    _dt_to_db(cursor.terminal_seen_at),
                ),
            )

    def upsert_run_summary(
        self,
        summary: RunSummary,
        *,
        parsed_at: datetime | None = None,
    ) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO run_summaries (
                    task_id,
                    execution_key,
                    run_result,
                    confidence,
                    reason_code,
                    evidence_json,
                    counters_json,
                    error_family,
                    parsed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    execution_key = excluded.execution_key,
                    run_result = excluded.run_result,
                    confidence = excluded.confidence,
                    reason_code = excluded.reason_code,
                    evidence_json = excluded.evidence_json,
                    counters_json = excluded.counters_json,
                    error_family = excluded.error_family,
                    parsed_at = excluded.parsed_at
                """,
                (
                    summary.task_id,
                    summary.execution_key,
                    summary.run_result.value,
                    summary.confidence.value,
                    summary.reason_code,
                    _json_dumps(summary.evidence),
                    _json_dumps(summary.counters),
                    summary.error_family.value if summary.error_family else None,
                    _dt_to_db(_coerce_timestamp(parsed_at)),
                ),
            )

    def get_run_summary(self, task_id: str) -> RunSummary | None:
        return self.get_run_summaries([task_id]).get(task_id)

    def get_run_summaries(
        self,
        task_ids: list[str] | None = None,
    ) -> dict[str, RunSummary]:
        if task_ids is not None and not task_ids:
            return {}

        params: tuple[str, ...] = ()
        query = "SELECT * FROM run_summaries"
        if task_ids is not None:
            placeholders = ", ".join("?" for _ in task_ids)
            query += f" WHERE task_id IN ({placeholders})"
            params = tuple(task_ids)
        query += " ORDER BY task_id"

        rows = self._connection.execute(query, params).fetchall()
        return {
            summary.task_id: summary
            for summary in (_summary_from_row(row) for row in rows)
        }

    def record_incident(
        self,
        projection: IncidentProjection,
        *,
        observed_at: datetime | None = None,
    ) -> IncidentRecord:
        observed_value = _dt_to_db(_coerce_timestamp(observed_at))
        open_incident = self.get_open_incident(projection.incident_key)

        with self._connection:
            if open_incident is None:
                self._connection.execute(
                    """
                    INSERT INTO incidents (
                        incident_key,
                        entity_type,
                        entity_id,
                        execution_key,
                        severity,
                        reason_code,
                        evidence_json,
                        opened_at,
                        closed_at,
                        last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
                    """,
                    (
                        projection.incident_key,
                        projection.entity_type,
                        projection.entity_id,
                        projection.execution_key,
                        projection.severity,
                        projection.reason_code,
                        _json_dumps(projection.evidence),
                        observed_value,
                        observed_value,
                    ),
                )
            else:
                self._connection.execute(
                    """
                    UPDATE incidents
                    SET entity_type = ?,
                        entity_id = ?,
                        execution_key = ?,
                        severity = ?,
                        reason_code = ?,
                        evidence_json = ?,
                        last_seen_at = ?
                    WHERE incident_id = ?
                    """,
                    (
                        projection.entity_type,
                        projection.entity_id,
                        projection.execution_key,
                        projection.severity,
                        projection.reason_code,
                        _json_dumps(projection.evidence),
                        observed_value,
                        open_incident.incident_id,
                    ),
                )

        incident = self.get_open_incident(projection.incident_key)
        if incident is None:
            raise RuntimeError("failed to persist open incident")
        return incident

    def resolve_incident(
        self,
        incident_key: str,
        *,
        resolved_at: datetime | None = None,
    ) -> None:
        resolved_value = _dt_to_db(_coerce_timestamp(resolved_at))
        with self._connection:
            self._connection.execute(
                """
                UPDATE incidents
                SET closed_at = ?,
                    last_seen_at = ?
                WHERE incident_key = ? AND closed_at IS NULL
                """,
                (resolved_value, resolved_value, incident_key),
            )

    def list_incidents(self, *, include_closed: bool = True) -> list[IncidentRecord]:
        query = "SELECT * FROM incidents"
        if not include_closed:
            query += " WHERE closed_at IS NULL"
        query += " ORDER BY incident_id"
        rows = self._connection.execute(query).fetchall()
        return [_incident_from_row(row) for row in rows]

    def list_open_incidents(self) -> list[IncidentRecord]:
        return self.list_incidents(include_closed=False)

    def get_open_incident(self, incident_key: str) -> IncidentRecord | None:
        row = self._connection.execute(
            """
            SELECT *
            FROM incidents
            WHERE incident_key = ? AND closed_at IS NULL
            ORDER BY incident_id DESC
            LIMIT 1
            """,
            (incident_key,),
        ).fetchone()
        if row is None:
            return None
        return _incident_from_row(row)

    def upsert_spider_profile(
        self,
        execution_key: str,
        *,
        spider_id: str,
        profile: dict[str, Any],
        updated_at: datetime | None = None,
    ) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO spider_profiles (
                    execution_key,
                    spider_id,
                    profile_json,
                    updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(execution_key) DO UPDATE SET
                    spider_id = excluded.spider_id,
                    profile_json = excluded.profile_json,
                    updated_at = excluded.updated_at
                """,
                (
                    execution_key,
                    spider_id,
                    _json_dumps(profile),
                    _dt_to_db(_coerce_timestamp(updated_at)),
                ),
            )

    def get_spider_profile(self, execution_key: str) -> SpiderProfileRecord | None:
        row = self._connection.execute(
            "SELECT * FROM spider_profiles WHERE execution_key = ?",
            (execution_key,),
        ).fetchone()
        if row is None:
            return None
        return _spider_profile_from_row(row)


def _task_from_row(row: sqlite3.Row) -> TaskSnapshot:
    return TaskSnapshot(
        id=str(row["task_id"]),
        spider_id=str(row["spider_id"]),
        schedule_id=_nullable_text(row["schedule_id"]),
        status=str(row["status"]),
        cmd=str(row["cmd"]),
        param=str(row["param"]),
        create_ts=_dt_from_db(row["create_ts"]),
        start_ts=_dt_from_db(row["start_ts"]),
        end_ts=_dt_from_db(row["end_ts"]),
        runtime=timedelta(milliseconds=int(row["runtime_ms"])),
        is_manual=bool(row["is_manual"]),
        execution_key=str(row["execution_key"]),
    )


def _stored_task_from_row(row: sqlite3.Row) -> StoredTaskSnapshot:
    return StoredTaskSnapshot(
        snapshot=_task_from_row(row),
        first_seen_at=_required_dt_from_db(row["first_seen_at"]),
        last_seen_at=_required_dt_from_db(row["last_seen_at"]),
        terminal_seen_at=_dt_from_db(row["terminal_seen_at"]),
    )


def _spider_from_row(row: sqlite3.Row) -> SpiderSnapshot:
    return SpiderSnapshot(
        id=str(row["spider_id"]),
        name=str(row["name"]),
        col_id=str(row["col_id"]),
        project_id=str(row["project_id"]),
        cmd=str(row["cmd"]),
        param=str(row["param"]),
    )


def _schedule_from_row(row: sqlite3.Row) -> ScheduleSnapshot:
    return ScheduleSnapshot(
        id=str(row["schedule_id"]),
        name=str(row["name"]),
        spider_id=str(row["spider_id"]),
        cron=str(row["cron"]),
        cmd=str(row["cmd"]),
        param=str(row["param"]),
        enabled=bool(row["enabled"]),
    )


def _cursor_from_row(row: sqlite3.Row) -> TaskLogCursor:
    return TaskLogCursor(
        task_id=str(row["task_id"]),
        page_size=int(row["page_size"]),
        next_page=int(row["next_page"]),
        api_total_lines=int(row["api_total_lines"]),
        assembled_line_count=int(row["assembled_line_count"]),
        assembled_log_text=str(row["assembled_log_text"]),
        is_complete=bool(row["is_complete"]),
        final_sync_done=bool(row["final_sync_done"]),
        last_log_sync_at=_dt_from_db(row["last_log_sync_at"]),
        terminal_seen_at=_dt_from_db(row["terminal_seen_at"]),
    )


def _summary_from_row(row: sqlite3.Row) -> RunSummary:
    error_family = _nullable_text(row["error_family"])
    return RunSummary(
        task_id=str(row["task_id"]),
        execution_key=str(row["execution_key"]),
        run_result=RunResult(str(row["run_result"])),
        confidence=Confidence(str(row["confidence"])),
        reason_code=str(row["reason_code"]),
        evidence=_json_loads(row["evidence_json"]),
        counters=_json_loads(row["counters_json"]),
        error_family=ErrorFamily(error_family) if error_family is not None else None,
    )


def _incident_from_row(row: sqlite3.Row) -> IncidentRecord:
    return IncidentRecord(
        incident_id=int(row["incident_id"]),
        incident_key=str(row["incident_key"]),
        entity_type=str(row["entity_type"]),
        entity_id=str(row["entity_id"]),
        execution_key=str(row["execution_key"]),
        severity=str(row["severity"]),
        reason_code=str(row["reason_code"]),
        evidence=_json_loads(row["evidence_json"]),
        opened_at=_required_dt_from_db(row["opened_at"]),
        closed_at=_dt_from_db(row["closed_at"]),
        last_seen_at=_required_dt_from_db(row["last_seen_at"]),
    )


def _spider_profile_from_row(row: sqlite3.Row) -> SpiderProfileRecord:
    return SpiderProfileRecord(
        execution_key=str(row["execution_key"]),
        spider_id=str(row["spider_id"]),
        profile=_json_loads(row["profile_json"]),
        updated_at=_required_dt_from_db(row["updated_at"]),
    )


def _nullable_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _json_loads(value: Any) -> Any:
    return json.loads(str(value))


def _dt_to_db(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _dt_from_db(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _coerce_timestamp(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _required_dt_from_db(value: Any) -> datetime:
    dt_value = _dt_from_db(value)
    if dt_value is None:
        raise RuntimeError("expected non-null datetime in repository row")
    return dt_value


def _runtime_to_ms(value: timedelta) -> int:
    return max(0, int(value.total_seconds() * 1000))


def _is_terminal(status: str) -> bool:
    return status not in {"pending", "running"}
