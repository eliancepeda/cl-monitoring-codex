from __future__ import annotations

from datetime import UTC, datetime, timedelta

from cl_monitoring.domain.models import (
    Confidence,
    RunResult,
    RunSummary,
    ScheduleHealthStatus,
    ScheduleSnapshot,
    TaskSnapshot,
)
from cl_monitoring.domain.normalizers import build_execution_key, normalize_task
from cl_monitoring.status.engine import ScheduleEngine

SPIDER_ID = "SPIDER_ID_100"
SCHEDULE_ID = "SCHEDULE_ID_100"
CMD = "python spider.py"
PARAM = "--region eu"
EXECUTION_KEY = build_execution_key(SPIDER_ID, CMD, PARAM)


def dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 4, 19, hour, minute, tzinfo=UTC)


def make_schedule(enabled: bool = True) -> ScheduleSnapshot:
    return ScheduleSnapshot(
        id=SCHEDULE_ID,
        name="hourly spider",
        spider_id=SPIDER_ID,
        cron="0 * * * *",
        cmd=CMD,
        param=PARAM,
        enabled=enabled,
    )


def make_task(
    task_id: str,
    *,
    create_ts: datetime,
    start_ts: datetime | None,
    runtime: timedelta,
    status: str = "finished",
    schedule_id: str | None = SCHEDULE_ID,
    is_manual: bool = False,
    execution_key: str = EXECUTION_KEY,
) -> TaskSnapshot:
    end_ts = (
        None if status in {"pending", "running"} else (start_ts or create_ts) + runtime
    )
    return TaskSnapshot(
        id=task_id,
        spider_id=SPIDER_ID,
        schedule_id=schedule_id,
        status=status,
        cmd=CMD,
        param=PARAM,
        create_ts=create_ts,
        start_ts=start_ts,
        end_ts=end_ts,
        runtime=runtime,
        is_manual=is_manual,
        execution_key=execution_key,
    )


def make_manual_task(
    task_id: str,
    *,
    create_ts: datetime,
    start_ts: datetime | None,
    runtime: timedelta,
    status: str = "finished",
) -> TaskSnapshot:
    return make_task(
        task_id,
        create_ts=create_ts,
        start_ts=start_ts,
        runtime=runtime,
        status=status,
        schedule_id=None,
        is_manual=True,
    )


def make_running_task_from_raw(
    task_id: str, *, create_ts: datetime, start_ts: datetime, now: datetime
) -> TaskSnapshot:
    return normalize_task(
        {
            "_id": task_id,
            "spider_id": SPIDER_ID,
            "schedule_id": SCHEDULE_ID,
            "status": "running",
            "cmd": CMD,
            "param": PARAM,
            "create_ts": create_ts.isoformat().replace("+00:00", "Z"),
            "stat": {
                "start_ts": start_ts.isoformat().replace("+00:00", "Z"),
                "end_ts": "0001-01-01T00:00:00Z",
                "runtime_duration": 0,
            },
        },
        now=now,
    )


def make_summary(task_id: str, result: RunResult) -> RunSummary:
    reason_codes = {
        RunResult.SUCCESS: "success_summary_marker",
        RunResult.SUCCESS_PROBABLE: "success_probable_positive_progress_complete_log",
        RunResult.PARTIAL_SUCCESS: "partial_success_positive_progress_with_errors",
        RunResult.CANCELLED: "cancelled_api_status",
        RunResult.FAILED: "failed_error_without_positive_signal",
        RunResult.UNKNOWN: "unknown_finished_without_positive_signal",
        RunResult.RULE_STOPPED: "rule_stopped_auto_stop",
    }
    confidence = Confidence.HIGH if result != RunResult.UNKNOWN else Confidence.LOW
    return RunSummary(
        task_id=task_id,
        execution_key=EXECUTION_KEY,
        run_result=result,
        confidence=confidence,
        reason_code=reason_codes[result],
        evidence=[result.value],
        counters={},
    )


def test_engine_marks_on_time_for_recent_observed_fire() -> None:
    engine = ScheduleEngine()
    schedule = make_schedule()
    scheduled_history = [
        make_task(
            "T10", create_ts=dt(10), start_ts=dt(10, 1), runtime=timedelta(minutes=8)
        ),
        make_task(
            "T11", create_ts=dt(11), start_ts=dt(11, 1), runtime=timedelta(minutes=9)
        ),
        make_task(
            "T12", create_ts=dt(12), start_ts=dt(12, 2), runtime=timedelta(minutes=10)
        ),
    ]

    health = engine.evaluate(schedule, scheduled_history, [], now=dt(12, 30))

    assert health.health is ScheduleHealthStatus.ON_TIME
    assert health.reason_code == "on_time_observed_fire"
    assert health.confidence is Confidence.HIGH
    assert health.counters["start_delay_ms"] == 120000
    assert health.counters["baseline_samples"] == 2


def test_engine_marks_queued_start_with_observed_fire_inside_grace() -> None:
    engine = ScheduleEngine()
    schedule = make_schedule()
    scheduled_history = [
        make_task(
            "T10", create_ts=dt(10), start_ts=dt(10, 1), runtime=timedelta(minutes=8)
        ),
        make_task(
            "T11", create_ts=dt(11), start_ts=dt(11, 1), runtime=timedelta(minutes=9)
        ),
        make_task(
            "T12",
            create_ts=dt(12),
            start_ts=None,
            runtime=timedelta(0),
            status="pending",
        ),
    ]

    health = engine.evaluate(schedule, scheduled_history, [], now=dt(12, 3))

    assert health.health is ScheduleHealthStatus.QUEUED_START
    assert health.reason_code == "queued_after_observed_fire"
    assert health.counters["lateness_ms"] == 0


def test_engine_marks_delayed_start_after_queue_grace() -> None:
    engine = ScheduleEngine()
    schedule = make_schedule()
    scheduled_history = [
        make_task(
            "T10", create_ts=dt(10), start_ts=dt(10, 1), runtime=timedelta(minutes=8)
        ),
        make_task(
            "T11", create_ts=dt(11), start_ts=dt(11, 1), runtime=timedelta(minutes=9)
        ),
        make_task(
            "T12",
            create_ts=dt(12),
            start_ts=None,
            runtime=timedelta(0),
            status="pending",
        ),
    ]

    health = engine.evaluate(schedule, scheduled_history, [], now=dt(12, 7))

    assert health.health is ScheduleHealthStatus.DELAYED_START
    assert health.reason_code == "delayed_start_after_observed_fire"
    assert health.counters["lateness_ms"] == 120000


def test_engine_marks_running_as_expected_inside_runtime_baseline() -> None:
    engine = ScheduleEngine()
    schedule = make_schedule()
    scheduled_history = [
        make_task(
            "T10", create_ts=dt(10), start_ts=dt(10, 1), runtime=timedelta(minutes=10)
        ),
        make_task(
            "T11", create_ts=dt(11), start_ts=dt(11, 1), runtime=timedelta(minutes=12)
        ),
        make_task(
            "T12",
            create_ts=dt(12),
            start_ts=dt(12, 1),
            runtime=timedelta(minutes=11),
            status="running",
        ),
    ]

    health = engine.evaluate(schedule, scheduled_history, [], now=dt(12, 12))

    assert health.health is ScheduleHealthStatus.RUNNING_AS_EXPECTED
    assert health.reason_code == "running_within_baseline"
    assert health.confidence is Confidence.HIGH
    assert health.counters["current_runtime_ms"] == 660000
    assert health.counters["baseline_runtime_ms"] == 720000


def test_engine_marks_running_long_with_execution_key_fallback_baseline() -> None:
    engine = ScheduleEngine()
    schedule = make_schedule()
    now = dt(11, 25)
    scheduled_history = [
        make_task(
            "T10", create_ts=dt(10), start_ts=dt(10, 1), runtime=timedelta(minutes=9)
        ),
        make_running_task_from_raw(
            "T11", create_ts=dt(11), start_ts=dt(11, 1), now=now
        ),
    ]
    manual_history = [
        make_manual_task(
            "M08", create_ts=dt(8), start_ts=dt(8, 1), runtime=timedelta(minutes=10)
        ),
        make_manual_task(
            "M09", create_ts=dt(9), start_ts=dt(9, 1), runtime=timedelta(minutes=11)
        ),
    ]
    run_summaries = {
        "M08": make_summary("M08", RunResult.SUCCESS),
        "M09": make_summary("M09", RunResult.SUCCESS),
    }

    health = engine.evaluate(
        schedule,
        scheduled_history,
        manual_history,
        run_summaries,
        now=now,
    )

    assert health.health is ScheduleHealthStatus.RUNNING_LONG
    assert health.reason_code == "running_exceeds_baseline"
    assert health.confidence is Confidence.MEDIUM
    assert health.counters["current_runtime_ms"] == 1440000
    assert health.counters["baseline_samples"] == 3
    assert any("execution_key history fallback" in line for line in health.evidence)


def test_engine_marks_missed_schedule_after_expected_window_without_fire() -> None:
    engine = ScheduleEngine()
    schedule = make_schedule()
    scheduled_history = [
        make_task(
            "T10", create_ts=dt(10), start_ts=dt(10, 1), runtime=timedelta(minutes=8)
        ),
        make_task(
            "T11", create_ts=dt(11), start_ts=dt(11, 1), runtime=timedelta(minutes=9)
        ),
        make_task(
            "T12", create_ts=dt(12), start_ts=dt(12, 1), runtime=timedelta(minutes=10)
        ),
    ]

    health = engine.evaluate(schedule, scheduled_history, [], now=dt(13, 5))

    assert health.health is ScheduleHealthStatus.MISSED_SCHEDULE
    assert health.reason_code == "missed_expected_fire_window"
    assert health.counters["missed_windows"] == 1
    assert health.counters["lateness_ms"] == 180000


def test_engine_marks_recovered_by_manual_rerun_after_missed_schedule() -> None:
    engine = ScheduleEngine()
    schedule = make_schedule()
    scheduled_history = [
        make_task(
            "T10", create_ts=dt(10), start_ts=dt(10, 1), runtime=timedelta(minutes=8)
        ),
        make_task(
            "T11", create_ts=dt(11), start_ts=dt(11, 1), runtime=timedelta(minutes=9)
        ),
        make_task(
            "T12", create_ts=dt(12), start_ts=dt(12, 1), runtime=timedelta(minutes=10)
        ),
    ]
    manual_history = [
        make_manual_task(
            "M13",
            create_ts=dt(13, 10),
            start_ts=dt(13, 10),
            runtime=timedelta(minutes=5),
        ),
    ]
    run_summaries = {"M13": make_summary("M13", RunResult.SUCCESS)}

    health = engine.evaluate(
        schedule,
        scheduled_history,
        manual_history,
        run_summaries,
        now=dt(13, 20),
    )

    assert health.health is ScheduleHealthStatus.RECOVERED_BY_MANUAL_RERUN
    assert health.reason_code == "recovered_by_manual_success"
    assert health.counters["manual_recovery_runs"] == 1
    assert any("manual recovery via task M13" in line for line in health.evidence)


def test_engine_marks_recovered_by_manual_rerun_after_failed_scheduled_run() -> None:
    engine = ScheduleEngine()
    schedule = make_schedule()
    scheduled_history = [
        make_task(
            "T10", create_ts=dt(10), start_ts=dt(10, 1), runtime=timedelta(minutes=8)
        ),
        make_task(
            "T11", create_ts=dt(11), start_ts=dt(11, 1), runtime=timedelta(minutes=9)
        ),
        make_task(
            "T12", create_ts=dt(12), start_ts=dt(12, 1), runtime=timedelta(minutes=10)
        ),
    ]
    manual_history = [
        make_manual_task(
            "M12",
            create_ts=dt(12, 20),
            start_ts=dt(12, 20),
            runtime=timedelta(minutes=6),
        ),
    ]
    run_summaries = {
        "T12": make_summary("T12", RunResult.FAILED),
        "M12": make_summary("M12", RunResult.SUCCESS),
    }

    health = engine.evaluate(
        schedule,
        scheduled_history,
        manual_history,
        run_summaries,
        now=dt(12, 25),
    )

    assert health.health is ScheduleHealthStatus.RECOVERED_BY_MANUAL_RERUN
    assert health.reason_code == "recovered_by_manual_success"
    assert any("ended as failed" in line for line in health.evidence)


def test_engine_stays_conservative_when_history_is_too_thin_for_missed_claim() -> None:
    engine = ScheduleEngine()
    schedule = make_schedule()
    scheduled_history = [
        make_task(
            "T10", create_ts=dt(10), start_ts=dt(10, 1), runtime=timedelta(minutes=8)
        ),
        make_task(
            "T11", create_ts=dt(11), start_ts=dt(11, 1), runtime=timedelta(minutes=9)
        ),
    ]

    health = engine.evaluate(schedule, scheduled_history, [], now=dt(12, 10))

    assert health.health is ScheduleHealthStatus.ON_TIME
    assert health.reason_code == "insufficient_history_for_strong_judgment"
    assert health.confidence is Confidence.LOW
    assert health.counters["lateness_ms"] == 480000
