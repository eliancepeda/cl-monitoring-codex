from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from cl_monitoring.app import create_app
from cl_monitoring.db.engine import connect_sqlite
from cl_monitoring.db.repo import IncidentProjection, LocalRepository
from cl_monitoring.domain import (
    Confidence,
    RunResult,
    RunSummary,
    ScheduleSnapshot,
    SpiderSnapshot,
    TaskSnapshot,
)
from cl_monitoring.domain.normalizers import build_execution_key
from cl_monitoring.settings import RuntimeSettings, build_runtime_settings

UNRESOLVED_SPIDER_ID = "SPIDER_ID_404"
UNRESOLVED_SCHEDULE_ID = "SCHEDULE_ID_404"


def dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 4, 19, hour, minute, tzinfo=UTC)


def make_spider(
    spider_id: str,
    *,
    name: str,
    project_id: str = "PROJECT_ID_001",
    col_id: str = "COL_ID_001",
    cmd: str = "python spider.py",
    param: str = "--region eu",
) -> SpiderSnapshot:
    return SpiderSnapshot(
        id=spider_id,
        name=name,
        col_id=col_id,
        project_id=project_id,
        cmd=cmd,
        param=param,
    )


def make_schedule(
    schedule_id: str,
    *,
    spider_id: str,
    name: str = "hourly demo",
    cron: str = "0 * * * *",
    cmd: str = "python spider.py",
    param: str = "--region eu",
) -> ScheduleSnapshot:
    return ScheduleSnapshot(
        id=schedule_id,
        name=name,
        spider_id=spider_id,
        cron=cron,
        cmd=cmd,
        param=param,
        enabled=True,
    )


def make_task(
    task_id: str,
    *,
    spider_id: str,
    schedule_id: str | None,
    status: str,
    create_ts: datetime,
    start_ts: datetime | None,
    runtime: timedelta,
    cmd: str = "python spider.py",
    param: str = "--region eu",
    is_manual: bool = False,
) -> TaskSnapshot:
    end_ts = (
        None
        if status in {"pending", "running"}
        else (start_ts or create_ts) + runtime
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
        execution_key=build_execution_key(spider_id, cmd, param),
    )


def make_summary(
    task_id: str,
    execution_key: str,
    *,
    run_result: RunResult,
    confidence: Confidence,
    reason_code: str,
    evidence: list[str],
    counters: dict[str, int] | None = None,
) -> RunSummary:
    return RunSummary(
        task_id=task_id,
        execution_key=execution_key,
        run_result=run_result,
        confidence=confidence,
        reason_code=reason_code,
        evidence=evidence,
        counters=counters or {},
    )


def build_web_fixture(db_path) -> None:
    connection = connect_sqlite(db_path)
    repo = LocalRepository(connection)

    spider_one = make_spider("SPIDER_ID_001", name="shop spider")
    spider_two = make_spider(
        "SPIDER_ID_002",
        name="catalog spider",
        col_id="COL_ID_002",
    )
    schedule_one = make_schedule(
        "SCHEDULE_ID_001",
        spider_id=spider_one.id,
        name="shop hourly",
    )
    unresolved_schedule = make_schedule(
        UNRESOLVED_SCHEDULE_ID,
        spider_id=UNRESOLVED_SPIDER_ID,
        name="orphan hourly",
    )

    repo.save_spiders([spider_one, spider_two], seen_at=dt(9, 0))
    repo.save_schedules([schedule_one, unresolved_schedule], seen_at=dt(9, 0))

    baseline_tasks = [
        make_task(
            "TASK_ID_100",
            spider_id=spider_one.id,
            schedule_id=schedule_one.id,
            status="finished",
            create_ts=dt(11, 0),
            start_ts=dt(11, 1),
            runtime=timedelta(minutes=8),
        ),
        make_task(
            "TASK_ID_101",
            spider_id=spider_one.id,
            schedule_id=schedule_one.id,
            status="finished",
            create_ts=dt(12, 0),
            start_ts=dt(12, 1),
            runtime=timedelta(minutes=9),
        ),
        make_task(
            "TASK_ID_102",
            spider_id=spider_one.id,
            schedule_id=schedule_one.id,
            status="finished",
            create_ts=dt(13, 0),
            start_ts=dt(13, 1),
            runtime=timedelta(minutes=10),
        ),
    ]
    failed_terminal = make_task(
        "TASK_ID_103",
        spider_id=spider_one.id,
        schedule_id=schedule_one.id,
        status="finished",
        create_ts=dt(14, 15),
        start_ts=dt(14, 16),
        runtime=timedelta(minutes=4),
    )
    running = make_task(
        "TASK_ID_104",
        spider_id=spider_one.id,
        schedule_id=schedule_one.id,
        status="running",
        create_ts=dt(14, 40),
        start_ts=dt(14, 41),
        runtime=timedelta(minutes=12),
    )
    manual_recovery = make_task(
        "TASK_ID_105",
        spider_id=spider_one.id,
        schedule_id=None,
        status="finished",
        create_ts=dt(10, 39),
        start_ts=dt(10, 40),
        runtime=timedelta(minutes=5),
        is_manual=True,
    )
    spider_two_task = make_task(
        "TASK_ID_200",
        spider_id=spider_two.id,
        schedule_id=None,
        status="finished",
        create_ts=dt(9, 30),
        start_ts=dt(9, 31),
        runtime=timedelta(minutes=6),
        is_manual=True,
    )

    repo.save_task_snapshots(
        baseline_tasks + [failed_terminal, running, manual_recovery, spider_two_task],
        seen_at=dt(14, 55),
    )

    execution_key_one = baseline_tasks[0].execution_key
    repo.upsert_run_summary(
        make_summary(
            "TASK_ID_100",
            execution_key_one,
            run_result=RunResult.SUCCESS,
            confidence=Confidence.HIGH,
            reason_code="success_summary_marker",
            evidence=["Summary: scheduled run ok", "put_to_parser=18"],
            counters={"summary_events": 1, "put_to_parser": 18},
        ),
        parsed_at=dt(11, 10),
    )
    repo.upsert_run_summary(
        make_summary(
            "TASK_ID_101",
            execution_key_one,
            run_result=RunResult.SUCCESS,
            confidence=Confidence.HIGH,
            reason_code="success_summary_marker",
            evidence=["Summary: all items parsed"],
            counters={"summary_events": 1, "item_events": 24},
        ),
        parsed_at=dt(12, 11),
    )
    repo.upsert_run_summary(
        make_summary(
            "TASK_ID_102",
            execution_key_one,
            run_result=RunResult.PARTIAL_SUCCESS,
            confidence=Confidence.MEDIUM,
            reason_code="partial_success_positive_progress_with_errors",
            evidence=["Summary: parser progressed with some errors"],
            counters={"item_events": 12, "summary_events": 1},
        ),
        parsed_at=dt(13, 15),
    )
    repo.upsert_run_summary(
        make_summary(
            "TASK_ID_103",
            execution_key_one,
            run_result=RunResult.FAILED,
            confidence=Confidence.HIGH,
            reason_code="failed_error_without_positive_signal",
            evidence=["Traceback line 1", "Traceback line 2", "No positive markers"],
            counters={"lines_seen": 220},
        ),
        parsed_at=dt(14, 21),
    )
    repo.upsert_run_summary(
        make_summary(
            "TASK_ID_104",
            execution_key_one,
            run_result=RunResult.UNKNOWN,
            confidence=Confidence.LOW,
            reason_code="unknown_running_or_pending",
            evidence=["Crawler heartbeat", "still processing queue"],
        ),
        parsed_at=dt(14, 55),
    )
    repo.upsert_run_summary(
        make_summary(
            "TASK_ID_105",
            execution_key_one,
            run_result=RunResult.SUCCESS,
            confidence=Confidence.HIGH,
            reason_code="success_summary_marker",
            evidence=["Manual rerun succeeded", "Recovered missing schedule output"],
            counters={"summary_events": 1},
        ),
        parsed_at=dt(10, 46),
    )
    repo.upsert_run_summary(
        make_summary(
            "TASK_ID_200",
            spider_two_task.execution_key,
            run_result=RunResult.SUCCESS,
            confidence=Confidence.HIGH,
            reason_code="success_summary_marker",
            evidence=["Catalog run clean"],
        ),
        parsed_at=dt(9, 38),
    )

    repo.record_incident(
        IncidentProjection(
            incident_key=f"schedule:{schedule_one.id}",
            entity_type="schedule",
            entity_id=schedule_one.id,
            execution_key=execution_key_one,
            severity="critical",
            reason_code="missed_expected_fire_window",
            evidence=["Expected hourly fire missing", "Grace window exhausted"],
        ),
        observed_at=dt(10, 30),
    )
    repo.resolve_incident(f"schedule:{schedule_one.id}", resolved_at=dt(10, 50))
    repo.record_incident(
        IncidentProjection(
            incident_key=f"schedule:{schedule_one.id}",
            entity_type="schedule",
            entity_id=schedule_one.id,
            execution_key=execution_key_one,
            severity="critical",
            reason_code="missed_expected_fire_window",
            evidence=[
                "Expected hourly fire missing",
                "Still no scheduled task after deadline",
            ],
        ),
        observed_at=dt(14, 45),
    )
    repo.record_incident(
        IncidentProjection(
            incident_key="task:TASK_ID_103",
            entity_type="task",
            entity_id="TASK_ID_103",
            execution_key=execution_key_one,
            severity="critical",
            reason_code="failed_error_without_positive_signal",
            evidence=["Traceback line 1", "Traceback line 2"],
        ),
        observed_at=dt(14, 22),
    )
    repo.record_incident(
        IncidentProjection(
            incident_key=f"schedule:{unresolved_schedule.id}",
            entity_type="schedule",
            entity_id=unresolved_schedule.id,
            execution_key=build_execution_key(
                unresolved_schedule.spider_id,
                unresolved_schedule.cmd,
                unresolved_schedule.param,
            ),
            severity="warning",
            reason_code="delayed_start_waiting_for_first_task",
            evidence=[
                "Schedule still exists upstream",
                "Spider metadata is unresolved upstream",
            ],
        ),
        observed_at=dt(14, 48),
    )

    connection.close()


def sqlite_only_settings(db_path) -> RuntimeSettings:
    return build_runtime_settings(db_path=db_path)


def build_sqlite_only_app(db_path) -> object:
    return create_app(
        settings=sqlite_only_settings(db_path),
        now_provider=lambda: dt(15, 0),
    )


def test_project_board_uses_local_sqlite_data(tmp_path) -> None:
    db_path = tmp_path / "web.sqlite3"
    build_web_fixture(db_path)
    app = build_sqlite_only_app(db_path)

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "Project board" in response.text
    assert "shop spider" in response.text
    assert "catalog spider" in response.text
    assert f"Unresolved spider {UNRESOLVED_SPIDER_ID}" in response.text
    assert (
        "Upstream spider metadata is missing; this view is built from local schedules."
        in response.text
    )
    assert "unresolved" in response.text
    assert "critical" in response.text
    assert "running" in response.text
    assert "stale/missed" in response.text
    assert "recovered manually" in response.text
    assert "Baseline runtime" in response.text
    assert "9m 00s from 3 local runs" in response.text
    assert "Parsed summary" in response.text
    assert "Traceback line 1" in response.text
    assert "Manual rerun succeeded" in response.text
    assert "<button" not in response.text


def test_spider_detail_shows_active_runs_schedules_and_recoveries(tmp_path) -> None:
    db_path = tmp_path / "web.sqlite3"
    build_web_fixture(db_path)
    app = build_sqlite_only_app(db_path)

    with TestClient(app) as client:
        response = client.get("/spiders/SPIDER_ID_001")

    assert response.status_code == 200
    assert "shop spider" in response.text
    assert "Active runs" in response.text
    assert "TASK_ID_104" in response.text
    assert "12m 00s" in response.text
    assert "9m 00s from 3 parsed runs" in response.text
    assert "Crawler heartbeat" in response.text
    assert "Schedules" in response.text
    assert "shop hourly" in response.text
    assert "missed_expected_fire_window" in response.text
    assert "Recent runs" in response.text
    assert "failed_error_without_positive_signal" in response.text
    assert "Recent recoveries" in response.text
    assert "Manual rerun succeeded" in response.text
    assert "Recovered manually support" in response.text
    assert "<button" not in response.text


def test_spider_detail_renders_unresolved_spider_from_local_schedule(tmp_path) -> None:
    db_path = tmp_path / "web.sqlite3"
    build_web_fixture(db_path)
    app = build_sqlite_only_app(db_path)

    with TestClient(app) as client:
        response = client.get(f"/spiders/{UNRESOLVED_SPIDER_ID}")

    assert response.status_code == 200
    assert f"Unresolved spider {UNRESOLVED_SPIDER_ID}" in response.text
    assert (
        "Upstream spider metadata is missing; this view is built from local schedules."
        in response.text
    )
    assert "unresolved" in response.text
    assert "orphan hourly" in response.text
    assert "No running or pending tasks for this spider." in response.text
    assert "No recent runs recorded for this spider." in response.text


def test_incidents_page_lists_open_and_recently_closed_incidents(tmp_path) -> None:
    db_path = tmp_path / "web.sqlite3"
    build_web_fixture(db_path)
    app = build_sqlite_only_app(db_path)

    with TestClient(app) as client:
        response = client.get("/incidents")

    assert response.status_code == 200
    assert "Open incidents" in response.text
    assert "Recovered in last 7 days" in response.text
    assert "failed_error_without_positive_signal" in response.text
    assert "missed_expected_fire_window" in response.text
    assert f"Unresolved spider {UNRESOLVED_SPIDER_ID}" in response.text
    assert "incident link stays on the local unresolved detail page" in response.text
    assert "Traceback line 1" in response.text
    assert "Latest scheduled run" in response.text
    assert "Recovered manually support" in response.text
    assert "Manual rerun succeeded" in response.text
    assert "<button" not in response.text


def test_incidents_link_to_unresolved_spider_detail_does_not_404(tmp_path) -> None:
    db_path = tmp_path / "web.sqlite3"
    build_web_fixture(db_path)
    app = build_sqlite_only_app(db_path)

    with TestClient(app) as client:
        incidents_response = client.get("/incidents")
        detail_response = client.get(f"/spiders/{UNRESOLVED_SPIDER_ID}")

    assert incidents_response.status_code == 200
    assert f'/spiders/{UNRESOLVED_SPIDER_ID}' in incidents_response.text
    assert detail_response.status_code == 200


def test_spider_detail_returns_404_for_missing_spider(tmp_path) -> None:
    db_path = tmp_path / "web.sqlite3"
    build_web_fixture(db_path)
    app = build_sqlite_only_app(db_path)

    with TestClient(app) as client:
        response = client.get("/spiders/SPIDER_ID_999")

    assert response.status_code == 404
