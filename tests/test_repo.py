from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from cl_monitoring.crawlab.client import (
    ReadonlyCrawlabClient as RuntimeReadonlyCrawlabClient,
)
from cl_monitoring.db.engine import connect_sqlite
from cl_monitoring.db.repo import (
    IncidentProjection,
    LocalRepository,
    TaskLogCursor,
)
from cl_monitoring.domain import (
    Confidence,
    RunResult,
    RunSummary,
    ScheduleSnapshot,
    SpiderSnapshot,
    TaskSnapshot,
)
from cl_monitoring.domain.normalizers import build_execution_key
from integrations.crawlab.readonly_client import (
    ReadonlyCrawlabClient as SourceReadonlyCrawlabClient,
)


def dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 4, 19, hour, minute, tzinfo=UTC)


def make_task(
    task_id: str,
    *,
    spider_id: str = "SPIDER_ID_001",
    schedule_id: str | None = "SCHEDULE_ID_001",
    status: str = "finished",
    cmd: str = "python spider.py",
    param: str = "--region eu",
    create_ts: datetime | None = None,
    start_ts: datetime | None = None,
    runtime: timedelta = timedelta(minutes=5),
    is_manual: bool = False,
) -> TaskSnapshot:
    if create_ts is None:
        create_ts = dt(12)
    execution_key = build_execution_key(spider_id, cmd, param)
    end_ts = (
        None if status in {"pending", "running"} else (start_ts or create_ts) + runtime
    )
    return TaskSnapshot(
        id=task_id,
        spider_id=spider_id,
        schedule_id=schedule_id,
        status=status,
        cmd=cmd,
        param=param,
        create_ts=create_ts,
        start_ts=start_ts,
        end_ts=end_ts,
        runtime=runtime,
        is_manual=is_manual,
        execution_key=execution_key,
    )


def make_summary(task_id: str, execution_key: str) -> RunSummary:
    return RunSummary(
        task_id=task_id,
        execution_key=execution_key,
        run_result=RunResult.SUCCESS,
        confidence=Confidence.HIGH,
        reason_code="success_summary_marker",
        evidence=["| Резюме: ✅"],
        counters={},
    )


def test_connect_sqlite_enables_wal_for_file_database(tmp_path: Path) -> None:
    connection = connect_sqlite(tmp_path / "history.sqlite3")
    try:
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
    finally:
        connection.close()

    assert str(journal_mode).lower() == "wal"
    assert foreign_keys == 1


def test_repo_persists_core_state_and_log_queue() -> None:
    connection = connect_sqlite(":memory:")
    repo = LocalRepository(connection)

    spider = SpiderSnapshot(
        id="SPIDER_ID_001",
        name="demo spider",
        col_id="COL_ID_001",
        project_id="PROJECT_ID_001",
        cmd="python spider.py",
        param="--region eu",
    )
    schedule = ScheduleSnapshot(
        id="SCHEDULE_ID_001",
        name="hourly demo",
        spider_id=spider.id,
        cron="0 * * * *",
        cmd=spider.cmd,
        param=spider.param,
        enabled=True,
    )
    running = make_task(
        "TASK_ID_001",
        status="running",
        create_ts=dt(12),
        start_ts=dt(12, 1),
        runtime=timedelta(minutes=3),
    )
    finished = make_task(
        "TASK_ID_001",
        status="finished",
        create_ts=dt(12),
        start_ts=dt(12, 1),
        runtime=timedelta(minutes=9),
    )
    manual = make_task(
        "TASK_ID_002",
        schedule_id=None,
        status="finished",
        create_ts=dt(13),
        start_ts=dt(13),
        runtime=timedelta(minutes=4),
        is_manual=True,
    )

    repo.save_spiders([spider], seen_at=dt(11, 55))
    repo.save_schedules([schedule], seen_at=dt(11, 56))
    repo.save_task_snapshots([running], seen_at=dt(12, 4))
    repo.save_task_snapshots([finished, manual], seen_at=dt(12, 12))

    stored = repo.get_task_record("TASK_ID_001")
    assert stored is not None
    assert stored.snapshot.status == "finished"
    assert stored.first_seen_at == dt(12, 4)
    assert stored.last_seen_at == dt(12, 12)
    assert stored.terminal_seen_at == dt(12, 12)

    pending_before_cursor = {
        task.snapshot.id for task in repo.list_tasks_requiring_log_sync()
    }
    assert pending_before_cursor == {"TASK_ID_001", "TASK_ID_002"}

    cursor = TaskLogCursor(
        task_id="TASK_ID_001",
        page_size=1000,
        next_page=3,
        api_total_lines=4,
        assembled_line_count=4,
        assembled_log_text="line 1\nline 2\nline 3\nline 4",
        is_complete=True,
        final_sync_done=True,
        last_log_sync_at=dt(12, 13),
        terminal_seen_at=dt(12, 12),
    )
    repo.save_log_cursor(cursor)

    summary = make_summary("TASK_ID_001", finished.execution_key)
    repo.upsert_run_summary(summary, parsed_at=dt(12, 14))
    repo.upsert_spider_profile(
        finished.execution_key,
        spider_id=finished.spider_id,
        profile={"owner_note": "keep an eye on runtime"},
        updated_at=dt(12, 15),
    )

    loaded_cursor = repo.get_log_cursor("TASK_ID_001")
    profile = repo.get_spider_profile(finished.execution_key)
    assert loaded_cursor == cursor
    assert repo.get_run_summary("TASK_ID_001") == summary
    assert profile is not None
    assert profile.profile == {
        "owner_note": "keep an eye on runtime"
    }
    assert repo.list_spiders() == [spider]
    assert repo.list_schedules() == [schedule]
    assert [task.id for task in repo.list_tasks_for_schedule(schedule.id)] == [
        "TASK_ID_001"
    ]
    assert [
        task.id
        for task in repo.list_manual_tasks_for_execution_key(manual.execution_key)
    ] == ["TASK_ID_002"]

    pending_after_cursor = {
        task.snapshot.id for task in repo.list_tasks_requiring_log_sync()
    }
    assert pending_after_cursor == {"TASK_ID_002"}


def test_repo_incident_lifecycle_keeps_history_without_duplicate_open_rows() -> None:
    connection = connect_sqlite(":memory:")
    repo = LocalRepository(connection)
    projection = IncidentProjection(
        incident_key="task:TASK_ID_500",
        entity_type="task",
        entity_id="TASK_ID_500",
        execution_key="SPIDER_ID_001:python spider.py:--region eu",
        severity="critical",
        reason_code="failed_error_without_positive_signal",
        evidence=["Traceback (most recent call last)"],
    )

    first = repo.record_incident(projection, observed_at=dt(14))
    second = repo.record_incident(projection, observed_at=dt(14, 5))

    assert first.incident_id == second.incident_id
    assert len(repo.list_open_incidents()) == 1
    assert repo.list_open_incidents()[0].last_seen_at == dt(14, 5)

    repo.resolve_incident(projection.incident_key, resolved_at=dt(14, 10))
    assert repo.get_open_incident(projection.incident_key) is None

    reopened = repo.record_incident(projection, observed_at=dt(14, 20))
    all_incidents = repo.list_incidents()

    assert reopened.incident_id != first.incident_id
    assert len(all_incidents) == 2
    assert all_incidents[0].closed_at == dt(14, 10)
    assert all_incidents[1].closed_at is None


def test_runtime_client_module_is_thin_reexport() -> None:
    assert RuntimeReadonlyCrawlabClient is SourceReadonlyCrawlabClient
