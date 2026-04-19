"""Tests for domain normalization functions.

Each test must use an anonymized fixture (AGENTS.md § Workflow).
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from cl_monitoring.domain.normalizers import (
    build_execution_key,
    compute_live_runtime,
    is_manual_run,
    normalize_id,
    normalize_schedule,
    normalize_spider,
    normalize_task,
    normalize_time,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "api"


def load_fixture(name: str) -> dict[str, Any]:
    with open(FIXTURES_DIR / name) as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"Expected JSON object fixture for {name}")
    return cast(dict[str, Any], data)


def test_normalize_id() -> None:
    assert normalize_id("000000000000000000000000") is None
    assert normalize_id("") == ""
    assert normalize_id("some_id") == "some_id"


def test_normalize_time() -> None:
    assert normalize_time("0001-01-01T00:00:00Z") is None
    assert normalize_time("") is None

    dt = normalize_time("2026-03-20T05:53:11.508Z")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 3
    assert dt.day == 20
    assert dt.hour == 5
    assert dt.minute == 53
    assert dt.second == 11
    assert dt.microsecond == 508000
    assert dt.tzinfo == UTC


def test_build_execution_key() -> None:
    assert (
        build_execution_key("spider_1", "python main.py", "")
        == "spider_1:python main.py:"
    )
    assert (
        build_execution_key("spider_1", "  python main.py  ", "  --args  ")
        == "spider_1:python main.py:--args"
    )


def test_compute_live_runtime() -> None:
    # Ended with duration
    assert compute_live_runtime(datetime.now(UTC), 5203832) == timedelta(
        milliseconds=5203832
    )

    # Running without duration
    start_ts = datetime(2026, 4, 18, 12, 0, 0, tzinfo=UTC)
    now = datetime(2026, 4, 18, 12, 5, 0, tzinfo=UTC)
    assert compute_live_runtime(start_ts, 0, now=now) == timedelta(minutes=5)

    # Not started
    assert compute_live_runtime(None, 0) == timedelta(0)


def test_is_manual_run() -> None:
    assert is_manual_run(None) is True
    assert is_manual_run("123") is False


def test_normalize_task_manual() -> None:
    raw_task = load_fixture("task_ID_748.json")
    task = normalize_task(raw_task)

    assert task.id == "ID_748"
    assert task.spider_id == "SPIDER_ID_201"
    assert task.schedule_id is None
    assert task.status == "finished"
    assert task.is_manual is True
    assert task.cmd == "python board_new.py"
    assert task.param == ""
    assert task.execution_key == "SPIDER_ID_201:python board_new.py:"
    assert task.runtime == timedelta(milliseconds=5203832)
    assert task.start_ts is not None
    assert task.end_ts is not None


def test_normalize_task_scheduled() -> None:
    raw_task = load_fixture("task_ID_753.json")
    now = datetime(2026, 4, 17, 4, 10, 0, tzinfo=UTC)
    task = normalize_task(raw_task, now=now)

    assert task.id == "ID_753"
    assert task.spider_id == "SPIDER_ID_201"
    assert task.schedule_id == "SCHEDULE_ID_005"
    assert task.status == "running"
    assert task.is_manual is False
    assert task.runtime == timedelta(minutes=6, seconds=59, milliseconds=870)
    assert task.end_ts is None


def test_normalize_task_zero_create_ts_to_none() -> None:
    raw_task = load_fixture("task_ID_748.json")
    raw_task["create_ts"] = "0001-01-01T00:00:00Z"

    task = normalize_task(raw_task)

    assert task.create_ts is None


def test_finished_task_zero_runtime_does_not_keep_growing() -> None:
    raw_task = load_fixture("task_ID_748.json")
    raw_task["status"] = "finished"
    raw_task["stat"]["runtime_duration"] = 0
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=UTC)

    task = normalize_task(raw_task, now=now)

    assert task.runtime == timedelta(0)


def test_missing_schedule_id_is_not_treated_as_manual_run() -> None:
    raw_task = load_fixture("task_ID_748.json")
    raw_task.pop("schedule_id", None)

    task = normalize_task(raw_task)

    assert task.schedule_id == ""
    assert task.is_manual is False


def test_normalize_spider() -> None:
    raw_spider = load_fixture("spider_ID_736.json")
    spider = normalize_spider(raw_spider)

    assert spider.id == "ID_736"
    assert spider.name == "rei.com"
    assert spider.col_id == "66aca75d116add6c8f4e45fc"
    assert spider.project_id == "PROJECT_ID_001"
    assert spider.cmd == "python rei_com.py"
    assert spider.param == ""


def test_normalize_schedule() -> None:
    with open(FIXTURES_DIR / "schedules.json") as f:
        schedules = json.load(f)
    raw_schedule = schedules[0]

    schedule = normalize_schedule(raw_schedule)

    assert schedule.id == "ID_001"
    assert schedule.name == "1 - 3"
    assert schedule.spider_id == "SPIDER_ID_001"
    assert schedule.cron == "0 14 * * *"
    assert schedule.cmd == "python matalan_co_uk.py"
    assert schedule.param == ""
    assert schedule.enabled is False
