"""FastAPI routes serving HTML pages from local SQLite only."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from cl_monitoring.db.engine import get_connection
from cl_monitoring.db.repo import LocalRepository
from cl_monitoring.domain.normalizers import build_execution_key

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))

_USEFUL_RUN_RESULTS = ("success", "success_probable", "partial_success")
_BOARD_RECOVERY_WINDOW = timedelta(hours=24)
_INCIDENTS_CLOSED_WINDOW = timedelta(days=7)


@dataclass(frozen=True)
class StatusBadge:
    tone: str
    label: str


@dataclass(frozen=True)
class RuntimeBaseline:
    runtime_ms: int
    sample_count: int


@dataclass(frozen=True)
class ProjectBoardRowView:
    project_id: str
    spider_id: str
    spider_name: str
    active_task_count: int
    open_task_issues: int
    open_schedule_issues: int
    worst_open_severity: str | None
    latest_terminal_task_id: str | None
    latest_terminal_run_result: str | None
    latest_terminal_reason_code: str | None
    latest_terminal_at: datetime | None
    oldest_active_at: datetime | None
    newest_active_at: datetime | None
    recent_recovery_count: int
    latest_recovery_at: datetime | None
    open_issue_reason_code: str | None
    open_issue_evidence: list[str]
    latest_run_evidence: list[str]
    recent_recovery_reason_code: str | None
    recent_recovery_evidence: list[str]
    recovered_manually: bool
    recovery_support_evidence: list[str]
    baseline: RuntimeBaseline | None
    badges: list[StatusBadge]


@dataclass(frozen=True)
class ProjectGroupView:
    project_id: str
    rows: list[ProjectBoardRowView]
    active_spider_count: int
    open_issue_count: int
    recent_recovery_count: int


@dataclass(frozen=True)
class SpiderHeaderView:
    spider_id: str
    name: str
    project_id: str
    col_id: str
    cmd: str
    param: str


@dataclass(frozen=True)
class SpiderActiveRunView:
    task_id: str
    status: str
    schedule_id: str | None
    is_manual: bool
    execution_key: str
    create_ts: datetime | None
    start_ts: datetime | None
    runtime_ms: int
    last_seen_at: datetime | None
    run_result: str | None
    confidence: str | None
    reason_code: str | None
    evidence: list[str]
    baseline: RuntimeBaseline | None
    badges: list[StatusBadge]


@dataclass(frozen=True)
class SpiderRunView:
    task_id: str
    status: str
    schedule_id: str | None
    is_manual: bool
    execution_key: str
    create_ts: datetime | None
    start_ts: datetime | None
    end_ts: datetime | None
    runtime_ms: int
    run_result: str | None
    confidence: str | None
    reason_code: str | None
    evidence: list[str]
    counters: dict[str, int]
    baseline: RuntimeBaseline | None
    badges: list[StatusBadge]


@dataclass(frozen=True)
class SpiderScheduleView:
    schedule_id: str
    name: str
    cron: str
    enabled: bool
    open_severity: str | None
    open_reason_code: str | None
    open_evidence: list[str]
    open_last_seen_at: datetime | None
    latest_closed_at: datetime | None
    latest_closed_reason_code: str | None
    last_scheduled_ts: datetime | None
    baseline: RuntimeBaseline | None
    recovered_manually: bool
    recovery_support_evidence: list[str]
    badges: list[StatusBadge]


@dataclass(frozen=True)
class RecoveryView:
    incident_id: int
    entity_type: str
    entity_id: str
    execution_key: str
    reason_code: str
    evidence: list[str]
    opened_at: datetime
    closed_at: datetime | None
    schedule_name: str
    recovered_manually: bool
    support_reason_code: str | None
    support_evidence: list[str]
    badges: list[StatusBadge]


@dataclass(frozen=True)
class SpiderPageView:
    header: SpiderHeaderView
    active_runs: list[SpiderActiveRunView]
    schedules: list[SpiderScheduleView]
    recent_runs: list[SpiderRunView]
    recent_recoveries: list[RecoveryView]


@dataclass(frozen=True)
class IncidentFeedRowView:
    incident_id: int
    entity_type: str
    entity_id: str
    execution_key: str
    severity: str
    reason_code: str
    evidence: list[str]
    opened_at: datetime
    last_seen_at: datetime
    closed_at: datetime | None
    spider_id: str | None
    spider_name: str | None
    schedule_name: str | None
    task_context_evidence: list[str]
    schedule_last_scheduled_ts: datetime | None
    recovered_manually: bool
    recovery_support_evidence: list[str]
    badges: list[StatusBadge]


@dataclass(frozen=True)
class IncidentsPageView:
    open_incidents: list[IncidentFeedRowView]
    recent_closed_incidents: list[IncidentFeedRowView]
    closed_since: datetime


class DashboardStore:
    """Read-oriented dashboard queries on top of the local SQLite truth layer."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def list_project_groups(self, *, now: datetime) -> list[ProjectGroupView]:
        recovered_since = now - _BOARD_RECOVERY_WINDOW
        spider_rows = self._connection.execute(
            """
            SELECT spider_id, name, project_id, cmd, param
            FROM spiders
            ORDER BY project_id, name, spider_id
            """
        ).fetchall()
        active_by_spider = self._map_rows(
            self._connection.execute(
                """
                SELECT spider_id,
                       COUNT(*) AS active_task_count,
                       MIN(COALESCE(start_ts, create_ts, last_seen_at)) AS oldest_active_ts,
                       MAX(COALESCE(start_ts, create_ts, last_seen_at)) AS newest_active_ts
                FROM task_snapshots
                WHERE status IN ('pending', 'running')
                GROUP BY spider_id
                """
            ).fetchall(),
            "spider_id",
        )
        incident_by_spider = self._map_rows(
            self._connection.execute(
                """
                SELECT spider_id,
                       open_task_issues,
                       open_schedule_issues,
                       worst_open_severity_rank,
                       latest_incident_seen_at
                FROM (
                    SELECT spider_id,
                           SUM(CASE WHEN entity_type = 'task' THEN 1 ELSE 0 END) AS open_task_issues,
                           SUM(CASE WHEN entity_type = 'schedule' THEN 1 ELSE 0 END)
                               AS open_schedule_issues,
                           MAX(
                               CASE severity WHEN 'critical' THEN 2 WHEN 'warning' THEN 1 ELSE 0 END
                           ) AS worst_open_severity_rank,
                           MAX(last_seen_at) AS latest_incident_seen_at
                    FROM (
                        SELECT i.entity_type, i.severity, i.last_seen_at, t.spider_id
                        FROM incidents AS i
                        JOIN task_snapshots AS t
                          ON i.entity_type = 'task' AND i.entity_id = t.task_id
                        WHERE i.closed_at IS NULL

                        UNION ALL

                        SELECT i.entity_type, i.severity, i.last_seen_at, s.spider_id
                        FROM incidents AS i
                        JOIN schedules AS s
                          ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
                        WHERE i.closed_at IS NULL
                    ) AS scoped_incidents
                    GROUP BY spider_id
                )
                """
            ).fetchall(),
            "spider_id",
        )
        latest_terminal_by_spider = self._map_rows(
            self._connection.execute(
                """
                WITH ranked AS (
                    SELECT t.spider_id,
                           t.task_id,
                           rs.run_result,
                           rs.confidence,
                           rs.reason_code,
                           COALESCE(t.end_ts, t.start_ts, t.create_ts, t.last_seen_at) AS sort_ts,
                           ROW_NUMBER() OVER (
                               PARTITION BY t.spider_id
                               ORDER BY COALESCE(t.end_ts, t.start_ts, t.create_ts, t.last_seen_at) DESC,
                                        t.task_id DESC
                           ) AS rn
                    FROM task_snapshots AS t
                    LEFT JOIN run_summaries AS rs ON rs.task_id = t.task_id
                    WHERE t.status NOT IN ('pending', 'running')
                )
                SELECT spider_id, task_id, run_result, confidence, reason_code, sort_ts
                FROM ranked
                WHERE rn = 1
                """
            ).fetchall(),
            "spider_id",
        )
        latest_open_incident_by_spider = self._map_rows(
            self._connection.execute(
                """
                WITH scoped_incidents AS (
                    SELECT i.incident_id,
                           i.entity_type,
                           i.severity,
                           i.reason_code,
                           i.evidence_json,
                           i.last_seen_at,
                           t.spider_id
                    FROM incidents AS i
                    JOIN task_snapshots AS t
                      ON i.entity_type = 'task' AND i.entity_id = t.task_id
                    WHERE i.closed_at IS NULL

                    UNION ALL

                    SELECT i.incident_id,
                           i.entity_type,
                           i.severity,
                           i.reason_code,
                           i.evidence_json,
                           i.last_seen_at,
                           s.spider_id
                    FROM incidents AS i
                    JOIN schedules AS s
                      ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
                    WHERE i.closed_at IS NULL
                ), ranked AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY spider_id
                               ORDER BY CASE severity WHEN 'critical' THEN 0 ELSE 1 END,
                                        last_seen_at DESC,
                                        incident_id DESC
                           ) AS rn
                    FROM scoped_incidents
                )
                SELECT spider_id, reason_code, evidence_json
                FROM ranked
                WHERE rn = 1
                """
            ).fetchall(),
            "spider_id",
        )
        latest_recovery_rows = self._connection.execute(
            """
            WITH ranked AS (
                SELECT s.spider_id,
                       i.incident_id,
                       i.execution_key,
                       i.reason_code,
                       i.evidence_json,
                       i.opened_at,
                       i.closed_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY s.spider_id
                           ORDER BY i.closed_at DESC, i.incident_id DESC
                       ) AS rn
                FROM incidents AS i
                JOIN schedules AS s
                  ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
                WHERE i.closed_at IS NOT NULL AND i.closed_at >= ?
            )
            SELECT spider_id, incident_id, execution_key, reason_code, evidence_json, opened_at, closed_at
            FROM ranked
            WHERE rn = 1
            """,
            (_dt_to_db(recovered_since),),
        ).fetchall()
        latest_recovery_by_spider = self._map_rows(latest_recovery_rows, "spider_id")
        recovery_counts_by_spider = self._map_rows(
            self._connection.execute(
                """
                SELECT s.spider_id,
                       COUNT(*) AS recent_recovery_count,
                       MAX(i.closed_at) AS latest_recovery_at
                FROM incidents AS i
                JOIN schedules AS s
                  ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
                WHERE i.closed_at IS NOT NULL AND i.closed_at >= ?
                GROUP BY s.spider_id
                """,
                (_dt_to_db(recovered_since),),
            ).fetchall(),
            "spider_id",
        )
        baselines_by_execution_key = self._runtime_baselines_by_execution_key(
            [
                build_execution_key(
                    str(row["spider_id"]),
                    str(row["cmd"]),
                    str(row["param"]),
                )
                for row in spider_rows
            ]
        )

        groups: dict[str, list[ProjectBoardRowView]] = defaultdict(list)
        for row in spider_rows:
            spider_id = str(row["spider_id"])
            project_id = str(row["project_id"])
            active_row = active_by_spider.get(spider_id)
            incident_row = incident_by_spider.get(spider_id)
            latest_row = latest_terminal_by_spider.get(spider_id)
            open_preview = latest_open_incident_by_spider.get(spider_id)
            recovery_row = latest_recovery_by_spider.get(spider_id)
            recovery_counts = recovery_counts_by_spider.get(spider_id)
            execution_key = build_execution_key(
                spider_id,
                str(row["cmd"]),
                str(row["param"]),
            )
            recovery_support = self._manual_recovery_support(
                execution_key=execution_key,
                opened_at=_dt_from_db(recovery_row["opened_at"]) if recovery_row is not None else None,
                closed_at=_dt_from_db(recovery_row["closed_at"]) if recovery_row is not None else None,
            )
            worst_open_severity = self._severity_from_rank(
                int(incident_row["worst_open_severity_rank"])
            ) if incident_row is not None else None
            board_row = ProjectBoardRowView(
                project_id=project_id,
                spider_id=spider_id,
                spider_name=str(row["name"]),
                active_task_count=int(active_row["active_task_count"]) if active_row is not None else 0,
                open_task_issues=int(incident_row["open_task_issues"]) if incident_row is not None else 0,
                open_schedule_issues=(
                    int(incident_row["open_schedule_issues"]) if incident_row is not None else 0
                ),
                worst_open_severity=worst_open_severity,
                latest_terminal_task_id=(
                    str(latest_row["task_id"]) if latest_row is not None else None
                ),
                latest_terminal_run_result=(
                    _nullable_text(latest_row["run_result"]) if latest_row is not None else None
                ),
                latest_terminal_reason_code=(
                    _nullable_text(latest_row["reason_code"]) if latest_row is not None else None
                ),
                latest_terminal_at=(
                    _dt_from_db(latest_row["sort_ts"]) if latest_row is not None else None
                ),
                oldest_active_at=(
                    _dt_from_db(active_row["oldest_active_ts"]) if active_row is not None else None
                ),
                newest_active_at=(
                    _dt_from_db(active_row["newest_active_ts"]) if active_row is not None else None
                ),
                recent_recovery_count=(
                    int(recovery_counts["recent_recovery_count"])
                    if recovery_counts is not None
                    else 0
                ),
                latest_recovery_at=(
                    _dt_from_db(recovery_counts["latest_recovery_at"])
                    if recovery_counts is not None
                    else None
                ),
                open_issue_reason_code=(
                    _nullable_text(open_preview["reason_code"]) if open_preview is not None else None
                ),
                open_issue_evidence=(
                    _preview_list(_json_list(open_preview["evidence_json"]))
                    if open_preview is not None
                    else []
                ),
                latest_run_evidence=self._latest_run_evidence(
                    _nullable_text(latest_row["task_id"]) if latest_row is not None else None
                ),
                recent_recovery_reason_code=(
                    _nullable_text(recovery_row["reason_code"]) if recovery_row is not None else None
                ),
                recent_recovery_evidence=(
                    _preview_list(_json_list(recovery_row["evidence_json"]))
                    if recovery_row is not None
                    else []
                ),
                recovered_manually=recovery_support is not None,
                recovery_support_evidence=(
                    _preview_list(recovery_support["evidence"]) if recovery_support is not None else []
                ),
                baseline=baselines_by_execution_key.get(execution_key),
                badges=self._project_board_badges(
                    active_task_count=int(active_row["active_task_count"]) if active_row is not None else 0,
                    worst_open_severity=worst_open_severity,
                    open_schedule_issues=(
                        int(incident_row["open_schedule_issues"])
                        if incident_row is not None
                        else 0
                    ),
                    recovered_manually=recovery_support is not None,
                    recent_recovery_count=(
                        int(recovery_counts["recent_recovery_count"])
                        if recovery_counts is not None
                        else 0
                    ),
                ),
            )
            groups[project_id].append(board_row)

        project_groups: list[ProjectGroupView] = []
        for project_id, rows in groups.items():
            ordered_rows = sorted(rows, key=self._project_board_sort_key)
            project_groups.append(
                ProjectGroupView(
                    project_id=project_id,
                    rows=ordered_rows,
                    active_spider_count=sum(1 for row in ordered_rows if row.active_task_count > 0),
                    open_issue_count=sum(
                        row.open_task_issues + row.open_schedule_issues for row in ordered_rows
                    ),
                    recent_recovery_count=sum(row.recent_recovery_count for row in ordered_rows),
                )
            )
        return sorted(project_groups, key=lambda group: group.project_id)

    def get_spider_page(self, spider_id: str) -> SpiderPageView | None:
        row = self._connection.execute(
            """
            SELECT spider_id, name, project_id, col_id, cmd, param
            FROM spiders
            WHERE spider_id = ?
            """,
            (spider_id,),
        ).fetchone()
        if row is None:
            return None

        header = SpiderHeaderView(
            spider_id=str(row["spider_id"]),
            name=str(row["name"]),
            project_id=str(row["project_id"]),
            col_id=str(row["col_id"]),
            cmd=str(row["cmd"]),
            param=str(row["param"]),
        )
        active_rows = self._connection.execute(
            """
            SELECT t.task_id,
                   t.status,
                   t.schedule_id,
                   t.is_manual,
                   t.execution_key,
                   t.create_ts,
                   t.start_ts,
                   t.runtime_ms,
                   t.last_seen_at,
                   rs.run_result,
                   rs.confidence,
                   rs.reason_code,
                   rs.evidence_json
            FROM task_snapshots AS t
            LEFT JOIN run_summaries AS rs ON rs.task_id = t.task_id
            WHERE t.spider_id = ? AND t.status IN ('pending', 'running')
            ORDER BY COALESCE(t.start_ts, t.create_ts, t.last_seen_at) DESC, t.task_id DESC
            """,
            (spider_id,),
        ).fetchall()
        schedule_rows = self._connection.execute(
            """
            SELECT s.schedule_id,
                   s.name,
                   s.cron,
                   s.enabled,
                   s.cmd,
                   s.param,
                   open_i.severity AS open_severity,
                   open_i.reason_code AS open_reason_code,
                   open_i.evidence_json AS open_evidence_json,
                   open_i.last_seen_at AS open_last_seen_at,
                   closed_i.closed_at AS latest_closed_at,
                   closed_i.reason_code AS latest_closed_reason_code,
                   closed_i.opened_at AS latest_opened_at,
                   last_task.last_scheduled_ts
            FROM schedules AS s
            LEFT JOIN incidents AS open_i
              ON open_i.entity_type = 'schedule'
             AND open_i.entity_id = s.schedule_id
             AND open_i.closed_at IS NULL
            LEFT JOIN (
                SELECT entity_id, closed_at, reason_code, opened_at
                FROM (
                    SELECT i.entity_id,
                           i.closed_at,
                           i.reason_code,
                           i.opened_at,
                           ROW_NUMBER() OVER (
                               PARTITION BY i.entity_id
                               ORDER BY i.closed_at DESC, i.incident_id DESC
                           ) AS rn
                    FROM incidents AS i
                    WHERE i.entity_type = 'schedule' AND i.closed_at IS NOT NULL
                )
                WHERE rn = 1
            ) AS closed_i ON closed_i.entity_id = s.schedule_id
            LEFT JOIN (
                SELECT schedule_id,
                       MAX(COALESCE(end_ts, start_ts, create_ts, last_seen_at)) AS last_scheduled_ts
                FROM task_snapshots
                WHERE spider_id = ? AND is_manual = 0
                GROUP BY schedule_id
            ) AS last_task ON last_task.schedule_id = s.schedule_id
            WHERE s.spider_id = ?
            ORDER BY s.enabled DESC, s.name, s.schedule_id
            """,
            (spider_id, spider_id),
        ).fetchall()
        recent_run_rows = self._connection.execute(
            """
            SELECT t.task_id,
                   t.status,
                   t.schedule_id,
                   t.is_manual,
                   t.execution_key,
                   t.create_ts,
                   t.start_ts,
                   t.end_ts,
                   t.runtime_ms,
                   rs.run_result,
                   rs.confidence,
                   rs.reason_code,
                   rs.evidence_json,
                   rs.counters_json
            FROM task_snapshots AS t
            LEFT JOIN run_summaries AS rs ON rs.task_id = t.task_id
            WHERE t.spider_id = ?
            ORDER BY COALESCE(t.end_ts, t.start_ts, t.create_ts, t.last_seen_at) DESC, t.task_id DESC
            LIMIT 20
            """,
            (spider_id,),
        ).fetchall()
        recovery_rows = self._connection.execute(
            """
            SELECT i.incident_id,
                   i.entity_type,
                   i.entity_id,
                   i.execution_key,
                   i.reason_code,
                   i.evidence_json,
                   i.opened_at,
                   i.closed_at,
                   s.name AS schedule_name
            FROM incidents AS i
            JOIN schedules AS s
              ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
            WHERE s.spider_id = ? AND i.closed_at IS NOT NULL
            ORDER BY i.closed_at DESC, i.incident_id DESC
            LIMIT 5
            """,
            (spider_id,),
        ).fetchall()

        execution_baseline_keys = [build_execution_key(header.spider_id, header.cmd, header.param)]
        execution_baseline_keys.extend(str(active_row["execution_key"]) for active_row in active_rows)
        execution_baseline_keys.extend(str(run_row["execution_key"]) for run_row in recent_run_rows)
        execution_baseline_keys.extend(
            build_execution_key(
                header.spider_id,
                str(schedule_row["cmd"]),
                str(schedule_row["param"]),
            )
            for schedule_row in schedule_rows
        )
        execution_baselines = self._runtime_baselines_by_execution_key(execution_baseline_keys)

        schedule_baselines = self._runtime_baselines_by_schedule_id(
            [str(schedule_row["schedule_id"]) for schedule_row in schedule_rows]
        )
        active_runs = [
            SpiderActiveRunView(
                task_id=str(active_row["task_id"]),
                status=str(active_row["status"]),
                schedule_id=_nullable_text(active_row["schedule_id"]),
                is_manual=bool(active_row["is_manual"]),
                execution_key=str(active_row["execution_key"]),
                create_ts=_dt_from_db(active_row["create_ts"]),
                start_ts=_dt_from_db(active_row["start_ts"]),
                runtime_ms=int(active_row["runtime_ms"]),
                last_seen_at=_dt_from_db(active_row["last_seen_at"]),
                run_result=_nullable_text(active_row["run_result"]),
                confidence=_nullable_text(active_row["confidence"]),
                reason_code=_nullable_text(active_row["reason_code"]),
                evidence=_preview_list(_json_list(active_row["evidence_json"])),
                baseline=self._pick_baseline(
                    schedule_id=_nullable_text(active_row["schedule_id"]),
                    execution_key=str(active_row["execution_key"]),
                    schedule_baselines=schedule_baselines,
                    execution_baselines=execution_baselines,
                ),
                badges=self._run_badges(
                    status=str(active_row["status"]),
                    run_result=_nullable_text(active_row["run_result"]),
                    is_manual=bool(active_row["is_manual"]),
                ),
            )
            for active_row in active_rows
        ]
        recent_runs = [
            SpiderRunView(
                task_id=str(run_row["task_id"]),
                status=str(run_row["status"]),
                schedule_id=_nullable_text(run_row["schedule_id"]),
                is_manual=bool(run_row["is_manual"]),
                execution_key=str(run_row["execution_key"]),
                create_ts=_dt_from_db(run_row["create_ts"]),
                start_ts=_dt_from_db(run_row["start_ts"]),
                end_ts=_dt_from_db(run_row["end_ts"]),
                runtime_ms=int(run_row["runtime_ms"]),
                run_result=_nullable_text(run_row["run_result"]),
                confidence=_nullable_text(run_row["confidence"]),
                reason_code=_nullable_text(run_row["reason_code"]),
                evidence=_preview_list(_json_list(run_row["evidence_json"])),
                counters=_json_dict(run_row["counters_json"]),
                baseline=self._pick_baseline(
                    schedule_id=_nullable_text(run_row["schedule_id"]),
                    execution_key=str(run_row["execution_key"]),
                    schedule_baselines=schedule_baselines,
                    execution_baselines=execution_baselines,
                ),
                badges=self._run_badges(
                    status=str(run_row["status"]),
                    run_result=_nullable_text(run_row["run_result"]),
                    is_manual=bool(run_row["is_manual"]),
                ),
            )
            for run_row in recent_run_rows
        ]
        schedules = []
        for schedule_row in schedule_rows:
            execution_key = build_execution_key(
                header.spider_id,
                str(schedule_row["cmd"]),
                str(schedule_row["param"]),
            )
            recovery_support = self._manual_recovery_support(
                execution_key=execution_key,
                opened_at=_dt_from_db(schedule_row["latest_opened_at"]),
                closed_at=_dt_from_db(schedule_row["latest_closed_at"]),
            )
            open_severity = _nullable_text(schedule_row["open_severity"])
            schedules.append(
                SpiderScheduleView(
                    schedule_id=str(schedule_row["schedule_id"]),
                    name=str(schedule_row["name"]),
                    cron=str(schedule_row["cron"]),
                    enabled=bool(schedule_row["enabled"]),
                    open_severity=open_severity,
                    open_reason_code=_nullable_text(schedule_row["open_reason_code"]),
                    open_evidence=_preview_list(_json_list(schedule_row["open_evidence_json"])),
                    open_last_seen_at=_dt_from_db(schedule_row["open_last_seen_at"]),
                    latest_closed_at=_dt_from_db(schedule_row["latest_closed_at"]),
                    latest_closed_reason_code=_nullable_text(
                        schedule_row["latest_closed_reason_code"]
                    ),
                    last_scheduled_ts=_dt_from_db(schedule_row["last_scheduled_ts"]),
                    baseline=schedule_baselines.get(str(schedule_row["schedule_id"])),
                    recovered_manually=recovery_support is not None,
                    recovery_support_evidence=(
                        _preview_list(recovery_support["evidence"]) if recovery_support is not None else []
                    ),
                    badges=self._schedule_badges(
                        open_severity=open_severity,
                        recovered_manually=recovery_support is not None,
                    ),
                )
            )

        recent_recoveries = []
        for recovery_row in recovery_rows:
            recovery_support = self._manual_recovery_support(
                execution_key=str(recovery_row["execution_key"]),
                opened_at=_required_dt_from_db(recovery_row["opened_at"]),
                closed_at=_dt_from_db(recovery_row["closed_at"]),
            )
            recent_recoveries.append(
                RecoveryView(
                    incident_id=int(recovery_row["incident_id"]),
                    entity_type=str(recovery_row["entity_type"]),
                    entity_id=str(recovery_row["entity_id"]),
                    execution_key=str(recovery_row["execution_key"]),
                    reason_code=str(recovery_row["reason_code"]),
                    evidence=_preview_list(_json_list(recovery_row["evidence_json"])),
                    opened_at=_required_dt_from_db(recovery_row["opened_at"]),
                    closed_at=_dt_from_db(recovery_row["closed_at"]),
                    schedule_name=str(recovery_row["schedule_name"]),
                    recovered_manually=recovery_support is not None,
                    support_reason_code=(
                        str(recovery_support["reason_code"])
                        if recovery_support is not None
                        else None
                    ),
                    support_evidence=(
                        _preview_list(recovery_support["evidence"]) if recovery_support is not None else []
                    ),
                    badges=self._recovery_badges(recovery_support is not None),
                )
            )

        return SpiderPageView(
            header=header,
            active_runs=active_runs,
            schedules=schedules,
            recent_runs=recent_runs,
            recent_recoveries=recent_recoveries,
        )

    def get_incidents_page(self, *, now: datetime) -> IncidentsPageView:
        closed_since = now - _INCIDENTS_CLOSED_WINDOW
        open_rows = self._connection.execute(
            """
            SELECT i.incident_id,
                   i.entity_type,
                   i.entity_id,
                   i.execution_key,
                   i.severity,
                   i.reason_code,
                   i.evidence_json,
                   i.opened_at,
                   i.last_seen_at,
                   i.closed_at,
                   COALESCE(t.spider_id, s.spider_id) AS spider_id,
                   sp.name AS spider_name,
                   s.name AS schedule_name
            FROM incidents AS i
            LEFT JOIN task_snapshots AS t
              ON i.entity_type = 'task' AND i.entity_id = t.task_id
            LEFT JOIN schedules AS s
              ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
            LEFT JOIN spiders AS sp
              ON sp.spider_id = COALESCE(t.spider_id, s.spider_id)
            WHERE i.closed_at IS NULL
            ORDER BY CASE i.severity WHEN 'critical' THEN 0 ELSE 1 END,
                     i.last_seen_at DESC,
                     i.incident_id DESC
            """
        ).fetchall()
        closed_rows = self._connection.execute(
            """
            SELECT i.incident_id,
                   i.entity_type,
                   i.entity_id,
                   i.execution_key,
                   i.severity,
                   i.reason_code,
                   i.evidence_json,
                   i.opened_at,
                   i.last_seen_at,
                   i.closed_at,
                   COALESCE(t.spider_id, s.spider_id) AS spider_id,
                   sp.name AS spider_name,
                   s.name AS schedule_name
            FROM incidents AS i
            LEFT JOIN task_snapshots AS t
              ON i.entity_type = 'task' AND i.entity_id = t.task_id
            LEFT JOIN schedules AS s
              ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
            LEFT JOIN spiders AS sp
              ON sp.spider_id = COALESCE(t.spider_id, s.spider_id)
            WHERE i.closed_at IS NOT NULL AND i.closed_at >= ?
            ORDER BY i.closed_at DESC, i.incident_id DESC
            """,
            (_dt_to_db(closed_since),),
        ).fetchall()

        return IncidentsPageView(
            open_incidents=self._incident_feed_rows(open_rows),
            recent_closed_incidents=self._incident_feed_rows(closed_rows),
            closed_since=closed_since,
        )

    def _incident_feed_rows(self, rows: list[sqlite3.Row]) -> list[IncidentFeedRowView]:
        schedule_last_run_map = self._schedule_last_scheduled_map(
            [
                str(row["entity_id"])
                for row in rows
                if str(row["entity_type"]) == "schedule"
            ]
        )
        task_context_map = self._task_summary_evidence_map(
            [
                str(row["entity_id"])
                for row in rows
                if str(row["entity_type"]) == "task"
            ]
        )
        incident_rows: list[IncidentFeedRowView] = []
        for row in rows:
            entity_type = str(row["entity_type"])
            closed_at = _dt_from_db(row["closed_at"])
            recovery_support = None
            if entity_type == "schedule" and closed_at is not None:
                recovery_support = self._manual_recovery_support(
                    execution_key=str(row["execution_key"]),
                    opened_at=_required_dt_from_db(row["opened_at"]),
                    closed_at=closed_at,
                )
            incident_rows.append(
                IncidentFeedRowView(
                    incident_id=int(row["incident_id"]),
                    entity_type=entity_type,
                    entity_id=str(row["entity_id"]),
                    execution_key=str(row["execution_key"]),
                    severity=str(row["severity"]),
                    reason_code=str(row["reason_code"]),
                    evidence=_preview_list(_json_list(row["evidence_json"])),
                    opened_at=_required_dt_from_db(row["opened_at"]),
                    last_seen_at=_required_dt_from_db(row["last_seen_at"]),
                    closed_at=closed_at,
                    spider_id=_nullable_text(row["spider_id"]),
                    spider_name=_nullable_text(row["spider_name"]),
                    schedule_name=_nullable_text(row["schedule_name"]),
                    task_context_evidence=(
                        task_context_map.get(str(row["entity_id"]), [])
                        if entity_type == "task"
                        else []
                    ),
                    schedule_last_scheduled_ts=(
                        schedule_last_run_map.get(str(row["entity_id"]))
                        if entity_type == "schedule"
                        else None
                    ),
                    recovered_manually=recovery_support is not None,
                    recovery_support_evidence=(
                        _preview_list(recovery_support["evidence"]) if recovery_support is not None else []
                    ),
                    badges=self._incident_badges(
                        severity=str(row["severity"]),
                        entity_type=entity_type,
                        closed_at=closed_at,
                        recovered_manually=recovery_support is not None,
                    ),
                )
            )
        return incident_rows

    def _runtime_baselines_by_execution_key(
        self, execution_keys: list[str]
    ) -> dict[str, RuntimeBaseline]:
        unique_keys = sorted({key for key in execution_keys if key})
        if not unique_keys:
            return {}
        result_placeholders = ", ".join("?" for _ in _USEFUL_RUN_RESULTS)
        placeholders = ", ".join("?" for _ in unique_keys)
        rows = self._connection.execute(
            f"""
            SELECT t.execution_key,
                   CAST(AVG(t.runtime_ms) AS INTEGER) AS baseline_runtime_ms,
                   COUNT(*) AS sample_count
            FROM task_snapshots AS t
            JOIN run_summaries AS rs ON rs.task_id = t.task_id
            WHERE t.status NOT IN ('pending', 'running')
              AND rs.run_result IN ({result_placeholders})
              AND t.runtime_ms > 0
              AND t.is_manual = 0
              AND t.execution_key IN ({placeholders})
            GROUP BY t.execution_key
            """,
            (*_USEFUL_RUN_RESULTS, *unique_keys),
        ).fetchall()
        return {
            str(row["execution_key"]): RuntimeBaseline(
                runtime_ms=int(row["baseline_runtime_ms"]),
                sample_count=int(row["sample_count"]),
            )
            for row in rows
        }

    def _runtime_baselines_by_schedule_id(
        self, schedule_ids: list[str]
    ) -> dict[str, RuntimeBaseline]:
        unique_ids = sorted({schedule_id for schedule_id in schedule_ids if schedule_id})
        if not unique_ids:
            return {}
        result_placeholders = ", ".join("?" for _ in _USEFUL_RUN_RESULTS)
        placeholders = ", ".join("?" for _ in unique_ids)
        rows = self._connection.execute(
            f"""
            SELECT t.schedule_id,
                   CAST(AVG(t.runtime_ms) AS INTEGER) AS baseline_runtime_ms,
                   COUNT(*) AS sample_count
            FROM task_snapshots AS t
            JOIN run_summaries AS rs ON rs.task_id = t.task_id
            WHERE t.status NOT IN ('pending', 'running')
              AND rs.run_result IN ({result_placeholders})
              AND t.runtime_ms > 0
              AND t.is_manual = 0
              AND t.schedule_id IN ({placeholders})
            GROUP BY t.schedule_id
            """,
            (*_USEFUL_RUN_RESULTS, *unique_ids),
        ).fetchall()
        return {
            str(row["schedule_id"]): RuntimeBaseline(
                runtime_ms=int(row["baseline_runtime_ms"]),
                sample_count=int(row["sample_count"]),
            )
            for row in rows
        }

    def _manual_recovery_support(
        self,
        *,
        execution_key: str,
        opened_at: datetime | None,
        closed_at: datetime | None,
    ) -> dict[str, Any] | None:
        if opened_at is None or closed_at is None:
            return None
        row = self._connection.execute(
            """
            SELECT rs.reason_code, rs.evidence_json
            FROM task_snapshots AS t
            JOIN run_summaries AS rs ON rs.task_id = t.task_id
            WHERE t.execution_key = ?
              AND t.is_manual = 1
              AND rs.run_result IN ('success', 'success_probable', 'partial_success')
              AND COALESCE(t.end_ts, t.start_ts, t.create_ts, t.last_seen_at) >= ?
              AND COALESCE(t.end_ts, t.start_ts, t.create_ts, t.last_seen_at) <= ?
            ORDER BY COALESCE(t.end_ts, t.start_ts, t.create_ts, t.last_seen_at) DESC,
                     t.task_id DESC
            LIMIT 1
            """,
            (execution_key, _dt_to_db(opened_at), _dt_to_db(closed_at)),
        ).fetchone()
        if row is None:
            return None
        return {
            "reason_code": str(row["reason_code"]),
            "evidence": _json_list(row["evidence_json"]),
        }

    def _latest_run_evidence(self, task_id: str | None) -> list[str]:
        if not task_id:
            return []
        row = self._connection.execute(
            "SELECT evidence_json FROM run_summaries WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            return []
        return _preview_list(_json_list(row["evidence_json"]))

    def _task_summary_evidence_map(self, task_ids: list[str]) -> dict[str, list[str]]:
        unique_ids = sorted({task_id for task_id in task_ids if task_id})
        if not unique_ids:
            return {}
        placeholders = ", ".join("?" for _ in unique_ids)
        rows = self._connection.execute(
            f"SELECT task_id, evidence_json FROM run_summaries WHERE task_id IN ({placeholders})",
            tuple(unique_ids),
        ).fetchall()
        return {
            str(row["task_id"]): _preview_list(_json_list(row["evidence_json"]))
            for row in rows
        }

    def _schedule_last_scheduled_map(self, schedule_ids: list[str]) -> dict[str, datetime]:
        unique_ids = sorted({schedule_id for schedule_id in schedule_ids if schedule_id})
        if not unique_ids:
            return {}
        placeholders = ", ".join("?" for _ in unique_ids)
        rows = self._connection.execute(
            f"""
            SELECT schedule_id,
                   MAX(COALESCE(end_ts, start_ts, create_ts, last_seen_at)) AS last_scheduled_ts
            FROM task_snapshots
            WHERE is_manual = 0 AND schedule_id IN ({placeholders})
            GROUP BY schedule_id
            """,
            tuple(unique_ids),
        ).fetchall()
        return {
            str(row["schedule_id"]): _required_dt_from_db(row["last_scheduled_ts"])
            for row in rows
            if row["last_scheduled_ts"] not in {None, ""}
        }

    @staticmethod
    def _map_rows(rows: list[sqlite3.Row], key: str) -> dict[str, sqlite3.Row]:
        return {str(row[key]): row for row in rows}

    @staticmethod
    def _pick_baseline(
        *,
        schedule_id: str | None,
        execution_key: str,
        schedule_baselines: dict[str, RuntimeBaseline],
        execution_baselines: dict[str, RuntimeBaseline],
    ) -> RuntimeBaseline | None:
        if schedule_id is not None and schedule_id in schedule_baselines:
            return schedule_baselines[schedule_id]
        return execution_baselines.get(execution_key)

    @staticmethod
    def _severity_from_rank(rank: int) -> str | None:
        if rank >= 2:
            return "critical"
        if rank == 1:
            return "warning"
        return None

    @staticmethod
    def _project_board_sort_key(row: ProjectBoardRowView) -> tuple[int, str, str]:
        if row.worst_open_severity == "critical":
            priority = 0
        elif row.active_task_count > 0:
            priority = 1
        elif row.worst_open_severity == "warning":
            priority = 2
        else:
            priority = 3
        return (priority, row.spider_name.lower(), row.spider_id)

    @staticmethod
    def _project_board_badges(
        *,
        active_task_count: int,
        worst_open_severity: str | None,
        open_schedule_issues: int,
        recovered_manually: bool,
        recent_recovery_count: int,
    ) -> list[StatusBadge]:
        badges: list[StatusBadge] = []
        if worst_open_severity is not None:
            badges.append(StatusBadge(tone=worst_open_severity, label=worst_open_severity))
        if active_task_count > 0:
            badges.append(StatusBadge(tone="active", label="running"))
        if open_schedule_issues > 0:
            badges.append(StatusBadge(tone="warning", label="stale/missed"))
        if recovered_manually:
            badges.append(StatusBadge(tone="success", label="recovered manually"))
        elif recent_recovery_count > 0:
            badges.append(StatusBadge(tone="muted", label="recovered"))
        return badges

    @staticmethod
    def _run_badges(*, status: str, run_result: str | None, is_manual: bool) -> list[StatusBadge]:
        badges = [StatusBadge(tone="active" if status in {"pending", "running"} else "muted", label=status)]
        if run_result is not None:
            badges.append(StatusBadge(tone=_run_result_tone(run_result), label=run_result))
        if is_manual:
            badges.append(StatusBadge(tone="muted", label="manual"))
        return badges

    @staticmethod
    def _schedule_badges(
        *, open_severity: str | None, recovered_manually: bool
    ) -> list[StatusBadge]:
        badges: list[StatusBadge] = []
        if open_severity is not None:
            badges.append(StatusBadge(tone=open_severity, label=open_severity))
            badges.append(StatusBadge(tone="warning", label="stale/missed"))
        if recovered_manually:
            badges.append(StatusBadge(tone="success", label="recovered manually"))
        return badges

    @staticmethod
    def _recovery_badges(recovered_manually: bool) -> list[StatusBadge]:
        badges = [StatusBadge(tone="muted", label="recovered")]
        if recovered_manually:
            badges.append(StatusBadge(tone="success", label="recovered manually"))
        return badges

    @staticmethod
    def _incident_badges(
        *,
        severity: str,
        entity_type: str,
        closed_at: datetime | None,
        recovered_manually: bool,
    ) -> list[StatusBadge]:
        badges = [StatusBadge(tone=severity, label=severity), StatusBadge(tone="muted", label=entity_type)]
        if closed_at is not None:
            badges.append(StatusBadge(tone="muted", label="closed"))
        if recovered_manually:
            badges.append(StatusBadge(tone="success", label="recovered manually"))
        return badges


def _get_dashboard_store(request: Request) -> Iterator[DashboardStore]:
    db_path = Path(str(request.app.state.db_path))
    with get_connection(db_path) as connection:
        LocalRepository(connection)
        yield DashboardStore(connection)


def _now(request: Request) -> datetime:
    now_provider = getattr(request.app.state, "now_provider", None)
    if callable(now_provider):
        current_time = now_provider()
        if current_time.tzinfo is None:
            return current_time.replace(tzinfo=UTC)
        return current_time.astimezone(UTC)
    return datetime.now(UTC)


@router.get("/", response_class=HTMLResponse)
def project_board(
    request: Request,
    store: DashboardStore = Depends(_get_dashboard_store),
) -> HTMLResponse:
    groups = store.list_project_groups(now=_now(request))
    return templates.TemplateResponse(
        request=request,
        name="project_board.html",
        context={
            "groups": groups,
            "page_title": "Project board",
        },
    )


@router.get("/spiders/{spider_id}", response_class=HTMLResponse)
def spider_detail(
    spider_id: str,
    request: Request,
    store: DashboardStore = Depends(_get_dashboard_store),
) -> HTMLResponse:
    page = store.get_spider_page(spider_id)
    if page is None:
        raise HTTPException(status_code=404, detail="spider not found")
    return templates.TemplateResponse(
        request=request,
        name="spider_detail.html",
        context={
            "page": page,
            "page_title": page.header.name,
        },
    )


@router.get("/incidents", response_class=HTMLResponse)
def incidents(
    request: Request,
    store: DashboardStore = Depends(_get_dashboard_store),
) -> HTMLResponse:
    page = store.get_incidents_page(now=_now(request))
    return templates.TemplateResponse(
        request=request,
        name="incidents.html",
        context={
            "page": page,
            "page_title": "Incidents",
        },
    )


def _nullable_text(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _json_list(value: Any) -> list[str]:
    if value in {None, ""}:
        return []
    loaded = json.loads(str(value))
    if not isinstance(loaded, list):
        return []
    return [str(item) for item in loaded]


def _json_dict(value: Any) -> dict[str, int]:
    if value in {None, ""}:
        return {}
    loaded = json.loads(str(value))
    if not isinstance(loaded, dict):
        return {}
    counters: dict[str, int] = {}
    for key, raw_value in loaded.items():
        if isinstance(raw_value, bool) or not isinstance(raw_value, int):
            continue
        counters[str(key)] = raw_value
    return counters


def _preview_list(items: list[str], *, limit: int = 3) -> list[str]:
    return [item for item in items if item][:limit]


def _dt_from_db(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)


def _required_dt_from_db(value: Any) -> datetime:
    parsed = _dt_from_db(value)
    if parsed is None:
        raise RuntimeError("expected non-null datetime value")
    return parsed


def _dt_to_db(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return "n/a"
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def _format_duration_ms(value: int | None) -> str:
    if value is None:
        return "n/a"
    total_seconds = max(0, value // 1000)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m {seconds:02d}s"


def _compact_counters(counters: dict[str, int]) -> list[str]:
    previews: list[str] = []
    for key, value in counters.items():
        if value <= 0:
            continue
        label = key.replace("_", " ")
        previews.append(f"{label}: {value}")
    return previews[:4]


def _run_result_tone(run_result: str) -> str:
    if run_result == "failed":
        return "critical"
    if run_result in {"partial_success", "cancelled", "rule_stopped", "unknown"}:
        return "warning"
    return "success"


templates.env.globals["format_datetime"] = _format_datetime
templates.env.globals["format_duration_ms"] = _format_duration_ms
templates.env.globals["compact_counters"] = _compact_counters
