"""Tests for frozen shared domain runtime models."""

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from cl_monitoring.domain.models import (
    Confidence,
    ErrorFamily,
    RUN_SUMMARY_COUNTER_KEYS,
    RunResult,
    RunSummary,
    SCHEDULE_HEALTH_COUNTER_KEYS,
    ScheduleHealth,
    ScheduleHealthStatus,
    ScheduleSnapshot,
    TaskSnapshot,
)


def test_run_summary_freezes_shared_counter_shape_and_reason_code():
    summary = RunSummary(
        task_id="ID_745",
        execution_key="SPIDER_ID_201:python board_new.py:",
        run_result=RunResult.FAILED,
        confidence=Confidence.HIGH,
        reason_code="failed_ban_429_error_auto_stop",
        evidence=[
            "Got ban status code 429, reinit client...",
            "Exception: error_auto_stop (6) is reached",
        ],
        counters={
            "lines_seen": 4000,
            "item_events": 18,
            "put_to_parser": 2,
            "404_gone": 1,
            "ban_429_markers": 1,
            "error_auto_stop_markers": 1,
        },
        error_family=ErrorFamily.ANTI_BOT,
    )

    assert list(summary.counters) == list(RUN_SUMMARY_COUNTER_KEYS)
    assert summary.counters["gone_404"] == 1
    assert summary.counters["summary_events"] == 0
    assert summary.counters["cancel_markers"] == 0

    payload = summary.model_dump(mode="json")
    assert payload["reason_code"] == "failed_ban_429_error_auto_stop"
    assert payload["error_family"] == "anti_bot"


def test_run_summary_keeps_legacy_parser_compatibility_by_inferencing_reason_code():
    summary = RunSummary(
        task_id="ID_745",
        execution_key="test_key",
        run_result=RunResult.FAILED,
        confidence=Confidence.HIGH,
        evidence=[
            "Got ban status code 429, reinit client...",
            "Exception: error_auto_stop (6) is reached",
        ],
        counters={
            "ban_429_markers": 1,
            "error_auto_stop_markers": 1,
        },
        error_family=ErrorFamily.ANTI_BOT,
    )

    assert summary.reason_code == "failed_ban_429_error_auto_stop"
    assert summary.counters["lines_seen"] == 0


def test_run_summary_rejects_unknown_reason_code():
    with pytest.raises(ValidationError, match="reason_code"):
        RunSummary(
            task_id="ID_999",
            execution_key="test_key",
            run_result=RunResult.UNKNOWN,
            confidence=Confidence.LOW,
            reason_code="new_reason_code_not_frozen",
            evidence=[],
            counters={},
        )


def test_run_summary_rejects_unknown_counter_key():
    with pytest.raises(ValidationError, match="unsupported counter key"):
        RunSummary(
            task_id="ID_999",
            execution_key="test_key",
            run_result=RunResult.UNKNOWN,
            confidence=Confidence.LOW,
            reason_code="unknown_incomplete_log",
            evidence=[],
            counters={"unexpected_counter": 1},
        )


def test_schedule_health_freezes_shared_shape_and_counter_keys():
    health = ScheduleHealth(
        schedule_id="SCHEDULE_ID_005",
        execution_key="SPIDER_ID_201:python board_new.py:",
        health=ScheduleHealthStatus.RUNNING_LONG,
        confidence=Confidence.MEDIUM,
        reason_code="running_exceeds_baseline",
        evidence=["current runtime 43000000 ms exceeds baseline 42728500 ms"],
        counters={
            "scheduled_tasks_seen": 12,
            "running_tasks_seen": 1,
            "terminal_tasks_seen": 11,
            "baseline_samples": 5,
            "current_runtime_ms": 43000000,
            "baseline_runtime_ms": 42728500,
        },
    )

    assert list(health.counters) == list(SCHEDULE_HEALTH_COUNTER_KEYS)
    assert health.counters["manual_tasks_seen"] == 0
    assert health.counters["start_delay_ms"] == 0
    assert health.model_dump(mode="json")["health"] == "running_long"


def test_schedule_health_rejects_unknown_reason_code():
    with pytest.raises(ValidationError, match="reason_code"):
        ScheduleHealth(
            schedule_id="SCHEDULE_ID_005",
            execution_key="SPIDER_ID_201:python board_new.py:",
            health=ScheduleHealthStatus.ON_TIME,
            confidence=Confidence.LOW,
            reason_code="new_schedule_reason_not_frozen",
            evidence=[],
            counters={},
        )


def test_shared_snapshots_forbid_extra_fields():
    task_kwargs = {
        "id": "ID_753",
        "spider_id": "SPIDER_ID_201",
        "schedule_id": "SCHEDULE_ID_005",
        "status": "running",
        "cmd": "python board_new.py",
        "param": "",
        "create_ts": datetime(2026, 4, 17, 4, 3, 0, tzinfo=timezone.utc),
        "start_ts": datetime(2026, 4, 17, 4, 3, 1, tzinfo=timezone.utc),
        "end_ts": None,
        "runtime": timedelta(minutes=5),
        "is_manual": False,
        "execution_key": "SPIDER_ID_201:python board_new.py:",
    }
    schedule_kwargs = {
        "id": "SCHEDULE_ID_005",
        "name": "nightly board",
        "spider_id": "SPIDER_ID_201",
        "cron": "0 4 * * *",
        "cmd": "python board_new.py",
        "param": "",
        "enabled": True,
    }

    with pytest.raises(ValidationError, match="extra_forbidden"):
        TaskSnapshot(**task_kwargs, parser_private_state="no")

    with pytest.raises(ValidationError, match="extra_forbidden"):
        ScheduleSnapshot(**schedule_kwargs, derived_window_ms=5000)
