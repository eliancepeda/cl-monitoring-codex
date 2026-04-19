"""Incremental local-history poller for the read-only companion app."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from cl_monitoring.db.repo import (
    IncidentProjection,
    LocalRepository,
    StoredTaskSnapshot,
    TaskLogCursor,
)
from cl_monitoring.domain import (
    RunResult,
    RunSummary,
    ScheduleHealth,
    ScheduleHealthStatus,
)
from cl_monitoring.domain.normalizers import (
    build_execution_key,
    normalize_schedule,
    normalize_spider,
    normalize_task,
)
from cl_monitoring.parsers import parse_crawllib_default
from cl_monitoring.status.engine import ScheduleEngine
from integrations.crawlab.readonly_client import ReadonlyCrawlabClient


TASK_STATUS_QUERIES = ("running", "pending", "finished", "error", "cancelled")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PollerConfig:
    spider_sync_interval: timedelta = timedelta(minutes=15)
    schedule_sync_interval: timedelta = timedelta(minutes=15)
    task_sync_interval: timedelta = timedelta(seconds=30)
    log_sync_interval: timedelta = timedelta(seconds=10)
    tick_interval: timedelta = timedelta(seconds=5)
    task_page_size: int = 100
    task_max_pages: int = 5
    log_page_size: int = 1000
    log_max_pages_per_sync: int = 5

    def __post_init__(self) -> None:
        for name in (
            "spider_sync_interval",
            "schedule_sync_interval",
            "task_sync_interval",
            "log_sync_interval",
            "tick_interval",
        ):
            if getattr(self, name) <= timedelta(0):
                raise ValueError(f"{name} must be positive")
        for name in ("task_page_size", "task_max_pages", "log_page_size", "log_max_pages_per_sync"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be at least 1")


@dataclass(frozen=True)
class LogFetchResult:
    lines: list[str]
    api_total_lines: int
    next_page: int
    is_complete: bool


class Poller:
    """Poll Crawlab through the single readonly client and persist local state."""

    def __init__(
        self,
        client: ReadonlyCrawlabClient,
        repo: LocalRepository,
        *,
        config: PollerConfig | None = None,
        schedule_engine: ScheduleEngine | None = None,
    ) -> None:
        self._client = client
        self._repo = repo
        self._config = config or PollerConfig()
        self._schedule_engine = schedule_engine or ScheduleEngine()
        self._last_spider_sync_at: datetime | None = None
        self._last_schedule_sync_at: datetime | None = None
        self._last_task_sync_at: datetime | None = None
        self._last_log_sync_at: datetime | None = None

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> None:
        while True:
            await self.sync_once()
            if stop_event is not None and stop_event.is_set():
                return
            try:
                if stop_event is None:
                    await asyncio.sleep(self._config.tick_interval.total_seconds())
                else:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self._config.tick_interval.total_seconds(),
                    )
            except TimeoutError:
                continue

    async def sync_once(self, *, now: datetime | None = None, force: bool = False) -> None:
        current_time = _coerce_utc(now)
        seen_task_spider_ids: set[str] = set()
        seen_schedule_spider_ids: set[str] = set()

        if force or _is_due(
            self._last_schedule_sync_at,
            self._config.schedule_sync_interval,
            current_time,
        ):
            schedules = await self.sync_schedules(now=current_time)
            seen_schedule_spider_ids = {schedule.spider_id for schedule in schedules}
            self._last_schedule_sync_at = current_time

        if force or _is_due(
            self._last_task_sync_at,
            self._config.task_sync_interval,
            current_time,
        ):
            tasks = await self.sync_tasks(now=current_time)
            seen_task_spider_ids = {task.spider_id for task in tasks}
            self._last_task_sync_at = current_time

        if force or _is_due(
            self._last_spider_sync_at,
            self._config.spider_sync_interval,
            current_time,
        ):
            await self.sync_spiders(
                extra_spider_ids=seen_schedule_spider_ids | seen_task_spider_ids,
                now=current_time,
            )
            self._last_spider_sync_at = current_time

        if force or _is_due(
            self._last_log_sync_at,
            self._config.log_sync_interval,
            current_time,
        ):
            await self.sync_logs(now=current_time)
            self._last_log_sync_at = current_time

        self.refresh_incidents(now=current_time)

    async def sync_schedules(self, *, now: datetime | None = None) -> list[Any]:
        current_time = _coerce_utc(now)
        payload = await self._client.get_json("/api/schedules")
        schedules = [normalize_schedule(item) for item in _unwrap_list_payload(payload)]
        self._repo.save_schedules(schedules, seen_at=current_time)
        return schedules

    async def sync_spiders(
        self,
        *,
        extra_spider_ids: set[str] | None = None,
        now: datetime | None = None,
    ) -> list[Any]:
        current_time = _coerce_utc(now)
        spider_ids = {schedule.spider_id for schedule in self._repo.list_schedules()}
        spider_ids.update(self._repo.list_distinct_task_spider_ids())
        if extra_spider_ids:
            spider_ids.update(extra_spider_ids)

        missing_spider_ids: set[str] = set()
        spiders = []
        for spider_id in sorted(spider_id for spider_id in spider_ids if spider_id):
            try:
                payload = await self._client.get_json(f"/api/spiders/{spider_id}")
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    raise
                missing_spider_ids.add(spider_id)
                logger.warning(
                    "Spider metadata is unresolved for %s because "
                    "/api/spiders/%s returned 404; keeping local schedules/tasks "
                    "and continuing sync",
                    spider_id,
                    spider_id,
                )
                continue
            spider_payload = _unwrap_dict_payload(payload)
            if spider_payload is not None:
                spiders.append(normalize_spider(spider_payload))

        if missing_spider_ids:
            self._repo.delete_spiders(missing_spider_ids)
        self._repo.save_spiders(spiders, seen_at=current_time)
        return spiders

    async def sync_tasks(self, *, now: datetime | None = None) -> list[Any]:
        current_time = _coerce_utc(now)
        raw_tasks_by_id: dict[str, dict[str, Any]] = {}

        for status in TASK_STATUS_QUERIES:
            tasks, _meta = await self._client.get_paginated(
                "/api/tasks",
                page_size=self._config.task_page_size,
                max_pages=self._config.task_max_pages,
                conditions=_build_conditions({"key": "status", "op": "eq", "value": status}),
                stats="true",
            )
            for raw_task in tasks:
                task_id = str(raw_task.get("_id", ""))
                if task_id:
                    raw_tasks_by_id[task_id] = raw_task

        normalized_tasks = [
            normalize_task(raw_task, now=current_time)
            for raw_task in raw_tasks_by_id.values()
        ]
        self._repo.save_task_snapshots(normalized_tasks, seen_at=current_time)
        return normalized_tasks

    async def sync_logs(self, *, now: datetime | None = None) -> None:
        current_time = _coerce_utc(now)
        for stored_task in self._repo.list_tasks_requiring_log_sync():
            await self._sync_task_log(stored_task, now=current_time)

    def refresh_incidents(self, *, now: datetime | None = None) -> None:
        current_time = _coerce_utc(now)
        run_summaries = self._repo.get_run_summaries()

        for summary in run_summaries.values():
            projection = _task_incident_projection(summary)
            incident_key = f"task:{summary.task_id}"
            if projection is None:
                self._repo.resolve_incident(incident_key, resolved_at=current_time)
            else:
                self._repo.record_incident(projection, observed_at=current_time)

        for schedule in self._repo.list_schedules():
            execution_key = build_execution_key(
                schedule.spider_id,
                schedule.cmd,
                schedule.param,
            )
            health = self._schedule_engine.evaluate(
                schedule,
                self._repo.list_tasks_for_schedule(schedule.id),
                self._repo.list_manual_tasks_for_execution_key(execution_key),
                run_summaries,
                now=current_time,
            )
            projection = _schedule_incident_projection(health)
            incident_key = f"schedule:{schedule.id}"
            if projection is None:
                self._repo.resolve_incident(incident_key, resolved_at=current_time)
            else:
                self._repo.record_incident(projection, observed_at=current_time)

    async def _sync_task_log(
        self,
        stored_task: StoredTaskSnapshot,
        *,
        now: datetime,
    ) -> None:
        snapshot = stored_task.snapshot
        cursor = self._repo.get_log_cursor(snapshot.id) or TaskLogCursor(
            task_id=snapshot.id,
            page_size=self._config.log_page_size,
            next_page=1,
            api_total_lines=0,
            assembled_line_count=0,
            assembled_log_text="",
            is_complete=False,
            final_sync_done=False,
            last_log_sync_at=None,
            terminal_seen_at=stored_task.terminal_seen_at,
        )
        start_page = max(1, cursor.next_page - 1)
        fetched = await self._fetch_log_window(snapshot.id, start_page=start_page)
        merged_lines = _merge_log_lines(
            cursor.assembled_lines,
            fetched.lines,
            start_page=start_page,
            page_size=cursor.page_size,
        )
        updated_cursor = TaskLogCursor(
            task_id=snapshot.id,
            page_size=cursor.page_size,
            next_page=max(cursor.next_page, fetched.next_page),
            api_total_lines=max(cursor.api_total_lines, fetched.api_total_lines, len(merged_lines)),
            assembled_line_count=max(cursor.assembled_line_count, len(merged_lines)),
            assembled_log_text="\n".join(merged_lines),
            is_complete=cursor.is_complete or fetched.is_complete,
            final_sync_done=cursor.final_sync_done or _is_terminal_status(snapshot.status),
            last_log_sync_at=now,
            terminal_seen_at=cursor.terminal_seen_at or stored_task.terminal_seen_at,
        )
        self._repo.save_log_cursor(updated_cursor)

        summary = parse_crawllib_default(
            snapshot,
            merged_lines,
            is_complete=updated_cursor.is_complete,
        )
        self._repo.upsert_run_summary(summary, parsed_at=now)

    async def _fetch_log_window(self, task_id: str, *, start_page: int) -> LogFetchResult:
        all_lines: list[str] = []
        last_page = start_page - 1
        api_total_lines = 0
        complete = False

        for page in range(start_page, start_page + self._config.log_max_pages_per_sync):
            payload = await self._client.get_json(
                f"/api/tasks/{task_id}/logs",
                page=page,
                size=self._config.log_page_size,
            )
            page_lines, reported_total = _parse_log_page(payload)
            api_total_lines = max(api_total_lines, reported_total)
            if not page_lines:
                complete = True
                break

            all_lines.extend(page_lines)
            last_page = page

            if api_total_lines > 0 and page * self._config.log_page_size >= api_total_lines:
                complete = True
                break
            if api_total_lines == 0 and len(page_lines) < self._config.log_page_size:
                complete = True
                break

        if last_page < start_page:
            next_page = start_page
        else:
            next_page = last_page + 1

        return LogFetchResult(
            lines=all_lines,
            api_total_lines=max(api_total_lines, len(all_lines)),
            next_page=next_page,
            is_complete=complete,
        )


def _build_conditions(*conditions: dict[str, str]) -> str:
    return json.dumps(list(conditions), separators=(",", ":"))


def _unwrap_list_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data", [])
        return data if isinstance(data, list) else []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _unwrap_dict_payload(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        if data is None:
            return payload
    return payload if isinstance(payload, dict) else None


def _parse_log_page(payload: Any) -> tuple[list[str], int]:
    if isinstance(payload, dict):
        data = payload.get("data", [])
        total = payload.get("total")
        reported_total = int(total) if isinstance(total, int) else 0
        if isinstance(data, list):
            lines = [_extract_log_line(item) for item in data]
            return lines, max(reported_total, len(lines))
        if isinstance(data, str):
            lines = data.splitlines()
            return lines, max(reported_total, len(lines))
        return [], reported_total
    if isinstance(payload, list):
        lines = [_extract_log_line(item) for item in payload]
        return lines, len(lines)
    if isinstance(payload, str):
        lines = payload.splitlines()
        return lines, len(lines)
    return [], 0


def _extract_log_line(item: Any) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        message = item.get("msg", item.get("message", ""))
        return str(message)
    return str(item)


def _merge_log_lines(
    existing_lines: list[str],
    fetched_lines: list[str],
    *,
    start_page: int,
    page_size: int,
) -> list[str]:
    if not fetched_lines:
        return list(existing_lines)

    overlap_start = min((start_page - 1) * page_size, len(existing_lines))
    prefix = existing_lines[:overlap_start]
    existing_tail = existing_lines[overlap_start:]
    overlap = _longest_overlap(existing_tail, fetched_lines)
    merged = prefix + existing_tail + fetched_lines[overlap:]
    if len(merged) < len(existing_lines):
        return list(existing_lines)
    return merged


def _longest_overlap(existing_tail: list[str], fetched_lines: list[str]) -> int:
    max_overlap = min(len(existing_tail), len(fetched_lines))
    for size in range(max_overlap, 0, -1):
        if existing_tail[-size:] == fetched_lines[:size]:
            return size
    return 0


def _task_incident_projection(summary: RunSummary) -> IncidentProjection | None:
    severity_by_result = {
        RunResult.FAILED: "critical",
        RunResult.CANCELLED: "warning",
        RunResult.RULE_STOPPED: "warning",
        RunResult.PARTIAL_SUCCESS: "warning",
        RunResult.UNKNOWN: "warning",
    }
    severity = severity_by_result.get(summary.run_result)
    if severity is None or summary.reason_code == "unknown_running_or_pending":
        return None
    return IncidentProjection(
        incident_key=f"task:{summary.task_id}",
        entity_type="task",
        entity_id=summary.task_id,
        execution_key=summary.execution_key,
        severity=severity,
        reason_code=summary.reason_code,
        evidence=list(summary.evidence),
    )


def _schedule_incident_projection(
    health: ScheduleHealth,
) -> IncidentProjection | None:
    severity_by_health = {
        ScheduleHealthStatus.DELAYED_START: "warning",
        ScheduleHealthStatus.RUNNING_LONG: "warning",
        ScheduleHealthStatus.MISSED_SCHEDULE: "critical",
    }
    severity = severity_by_health.get(health.health)
    if severity is None:
        return None
    return IncidentProjection(
        incident_key=f"schedule:{health.schedule_id}",
        entity_type="schedule",
        entity_id=health.schedule_id,
        execution_key=health.execution_key,
        severity=severity,
        reason_code=health.reason_code,
        evidence=list(health.evidence),
    )


def _is_due(last_run_at: datetime | None, interval: timedelta, now: datetime) -> bool:
    if last_run_at is None:
        return True
    return now - last_run_at >= interval


def _coerce_utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        raise ValueError("poller timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _is_terminal_status(status: str) -> bool:
    return status not in {"pending", "running"}
