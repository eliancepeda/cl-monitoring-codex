"""Tests for collector discovery, project scoping, and dry-run output.

Verifies:
- Condition-based discovery builds correct Crawlab conditions params.
- Project scoping filters tasks by spider project_id.
- Long-running schedule selection prefers median runtime over task count.
- Sampling produces correct candidate groups.
- Estimation helpers compute reasonable numbers.

All tests are offline — no network access.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from tools.collect_fixtures import (
    _build_conditions,
    build_execution_key,
    compute_project_histogram,
    estimate_api_calls,
    estimate_files,
    filter_tasks_by_project,
    find_long_running_schedule,
    sample_candidates,
)


# ── Test data ──────────────────────────────────────────────────────────

ZERO_OID = "000000000000000000000000"

SPIDER_A = {
    "_id": "spider_aaa",
    "project_id": "proj_001",
    "name": "spider_a",
}

SPIDER_B = {
    "_id": "spider_bbb",
    "project_id": "proj_002",
    "name": "spider_b",
}

SPIDERS = {
    "spider_aaa": SPIDER_A,
    "spider_bbb": SPIDER_B,
}


def _make_task(
    task_id: str,
    status: str,
    spider_id: str = "spider_aaa",
    schedule_id: str = "sched_001",
    runtime_ms: int = 0,
    create_ts: str = "2026-04-16T05:00:00Z",
) -> dict[str, Any]:
    """Build a task dict for testing."""
    return {
        "_id": task_id,
        "status": status,
        "spider_id": spider_id,
        "schedule_id": schedule_id,
        "cmd": "python spider.py",
        "param": "",
        "create_ts": create_ts,
        "stat": {
            "runtime_duration": runtime_ms,
        },
    }


# ── Condition builder tests ────────────────────────────────────────────


class TestConditionBuilder:
    """Verify Crawlab conditions param format."""

    def test_single_condition(self) -> None:
        result = _build_conditions({"key": "status", "op": "eq", "value": "running"})
        parsed = json.loads(result)
        assert len(parsed) == 1
        assert parsed[0]["key"] == "status"
        assert parsed[0]["op"] == "eq"
        assert parsed[0]["value"] == "running"

    def test_multiple_conditions(self) -> None:
        result = _build_conditions(
            {"key": "status", "op": "eq", "value": "finished"},
            {"key": "spider_id", "op": "eq", "value": "abc123"},
        )
        parsed = json.loads(result)
        assert len(parsed) == 2

    def test_zero_oid_condition(self) -> None:
        result = _build_conditions(
            {"key": "schedule_id", "op": "eq", "value": ZERO_OID}
        )
        parsed = json.loads(result)
        assert parsed[0]["value"] == ZERO_OID


# ── Project scoping tests ─────────────────────────────────────────────


class TestProjectScoping:
    """Verify task filtering by project_id."""

    def test_filter_includes_allowed_project(self) -> None:
        tasks = [
            _make_task("t1", "finished", spider_id="spider_aaa"),
            _make_task("t2", "finished", spider_id="spider_bbb"),
        ]
        filtered = filter_tasks_by_project(tasks, SPIDERS, ["proj_001"])
        assert len(filtered) == 1
        assert filtered[0]["_id"] == "t1"

    def test_filter_excludes_disallowed_project(self) -> None:
        tasks = [
            _make_task("t1", "finished", spider_id="spider_bbb"),
        ]
        filtered = filter_tasks_by_project(tasks, SPIDERS, ["proj_001"])
        assert len(filtered) == 0

    def test_filter_empty_allowed_returns_all(self) -> None:
        """Empty allowed list means no filtering."""
        tasks = [
            _make_task("t1", "finished", spider_id="spider_aaa"),
            _make_task("t2", "finished", spider_id="spider_bbb"),
        ]
        filtered = filter_tasks_by_project(tasks, SPIDERS, [])
        assert len(filtered) == 2

    def test_filter_unknown_spider_excluded(self) -> None:
        """Task with unknown spider_id is excluded when filter is set."""
        tasks = [
            _make_task("t1", "finished", spider_id="unknown_spider"),
        ]
        filtered = filter_tasks_by_project(tasks, SPIDERS, ["proj_001"])
        assert len(filtered) == 0

    def test_project_histogram(self) -> None:
        tasks = [
            _make_task("t1", "finished", spider_id="spider_aaa"),
            _make_task("t2", "finished", spider_id="spider_aaa"),
            _make_task("t3", "finished", spider_id="spider_bbb"),
        ]
        hist = compute_project_histogram(tasks, SPIDERS)
        assert hist["proj_001"] == 2
        assert hist["proj_002"] == 1


# ── Sampling tests ─────────────────────────────────────────────────────


class TestSampling:
    """Verify candidate sampling."""

    def test_sample_respects_max_per_class(self) -> None:
        candidates = {
            "running": [
                _make_task("t1", "running"),
                _make_task("t2", "running"),
                _make_task("t3", "running"),
            ],
            "finished": [
                _make_task("t4", "finished"),
            ],
        }
        sampled, selected_ids = sample_candidates(candidates, max_per_class=2)
        assert len(selected_ids["running"]) <= 2
        assert len(selected_ids["finished"]) == 1
        # Total sampled should be at most 3 (2 running + 1 finished)
        assert len(sampled) <= 3

    def test_sample_deduplicates(self) -> None:
        """Same task appearing in multiple candidate groups is not duplicated."""
        shared_task = _make_task("t1", "running", schedule_id=ZERO_OID)
        candidates = {
            "running": [shared_task],
            "manual": [shared_task],
        }
        sampled, _ = sample_candidates(candidates, max_per_class=2)
        task_ids = [t["_id"] for t in sampled]
        assert task_ids.count("t1") == 1

    def test_sample_prefers_recent(self) -> None:
        candidates = {
            "finished": [
                _make_task("old", "finished", create_ts="2026-04-01T00:00:00Z"),
                _make_task("new", "finished", create_ts="2026-04-16T00:00:00Z"),
            ],
        }
        sampled, selected_ids = sample_candidates(candidates, max_per_class=1)
        assert selected_ids["finished"] == ["new"]


# ── Long-running schedule selection tests ──────────────────────────────


class TestLongRunningScheduleSelection:
    """Verify schedule selection prefers median runtime over task count."""

    def test_prefers_higher_median_runtime(self) -> None:
        """Schedule with higher median runtime wins over more tasks."""
        tasks = [
            # sched_slow: 2 tasks, high runtime
            _make_task("t1", "finished", schedule_id="sched_slow", runtime_ms=100000),
            _make_task("t2", "finished", schedule_id="sched_slow", runtime_ms=120000),
            # sched_fast: 5 tasks, low runtime
            _make_task("t3", "finished", schedule_id="sched_fast", runtime_ms=1000),
            _make_task("t4", "finished", schedule_id="sched_fast", runtime_ms=1200),
            _make_task("t5", "finished", schedule_id="sched_fast", runtime_ms=1100),
            _make_task("t6", "finished", schedule_id="sched_fast", runtime_ms=900),
            _make_task("t7", "finished", schedule_id="sched_fast", runtime_ms=1300),
        ]
        schedules = [
            {"_id": "sched_slow", "spider_id": "spider_aaa"},
            {"_id": "sched_fast", "spider_id": "spider_aaa"},
        ]
        sched_id, info = find_long_running_schedule(
            tasks, schedules, SPIDERS, ["proj_001"],
        )
        assert sched_id == "sched_slow"
        assert info["median_runtime_ms"] == 110000.0

    def test_prefers_running_chain(self) -> None:
        """Schedule with active running chain has priority."""
        tasks = [
            # sched_a: higher median, no running
            _make_task("t1", "finished", schedule_id="sched_a", runtime_ms=200000),
            _make_task("t2", "finished", schedule_id="sched_a", runtime_ms=180000),
            # sched_b: lower median, but has running task
            _make_task("t3", "finished", schedule_id="sched_b", runtime_ms=50000),
            _make_task("t4", "running", schedule_id="sched_b", runtime_ms=0),
        ]
        schedules = [
            {"_id": "sched_a", "spider_id": "spider_aaa"},
            {"_id": "sched_b", "spider_id": "spider_aaa"},
        ]
        sched_id, info = find_long_running_schedule(
            tasks, schedules, SPIDERS, ["proj_001"],
        )
        assert sched_id == "sched_b"
        assert info["has_running"] is True

    def test_filters_by_project_scope(self) -> None:
        """Out-of-scope schedules are excluded."""
        tasks = [
            # In scope (spider_aaa → proj_001)
            _make_task("t1", "finished", spider_id="spider_aaa",
                       schedule_id="sched_in", runtime_ms=5000),
            # Out of scope (spider_bbb → proj_002)
            _make_task("t2", "finished", spider_id="spider_bbb",
                       schedule_id="sched_out", runtime_ms=500000),
        ]
        schedules = [
            {"_id": "sched_in", "spider_id": "spider_aaa"},
            {"_id": "sched_out", "spider_id": "spider_bbb"},
        ]
        sched_id, info = find_long_running_schedule(
            tasks, schedules, SPIDERS, ["proj_001"],
        )
        assert sched_id == "sched_in"

    def test_no_tasks_returns_none(self) -> None:
        sched_id, info = find_long_running_schedule([], [], {}, [])
        assert sched_id is None
        assert info == {}

    def test_only_manual_tasks_returns_none(self) -> None:
        """Manual tasks (zero schedule_id) should not be selected."""
        tasks = [
            _make_task("t1", "finished", schedule_id=ZERO_OID, runtime_ms=100000),
        ]
        sched_id, info = find_long_running_schedule(tasks, [], SPIDERS, [])
        assert sched_id is None


# ── Execution key tests ───────────────────────────────────────────────


class TestExecutionKey:
    """Verify execution key construction."""

    def test_basic_key(self) -> None:
        task = {"spider_id": "s1", "cmd": "python spider.py", "param": "-as 80"}
        assert build_execution_key(task) == "s1|python spider.py|-as 80"

    def test_strips_whitespace(self) -> None:
        task = {"spider_id": "s1", "cmd": "  python spider.py  ", "param": "  "}
        assert build_execution_key(task) == "s1|python spider.py|"

    def test_none_values(self) -> None:
        task = {"spider_id": "s1", "cmd": None, "param": None}
        assert build_execution_key(task) == "s1||"


# ── Estimation tests ──────────────────────────────────────────────────


class TestEstimation:
    """Verify API call and file count estimation."""

    def test_api_calls_estimation(self) -> None:
        coll = {"collect_results_for_sampled_tasks_only": True}
        est = estimate_api_calls(10, 5, coll)
        # 5 status + 1 manual + 1 schedules + 5 spiders + 10 logs + 10 results
        assert est["total"] == 32

    def test_files_estimation(self) -> None:
        coll = {
            "collect_raw": True,
            "collect_redacted": True,
            "collect_results_for_sampled_tasks_only": True,
            "generate_expected_skeletons": True,
        }
        est = estimate_files(5, 3, coll)
        # per task: 4 files (task + log + results + expected)
        # raw: 5*4 + 3 + 1 = 24
        # redacted: 5*4 + 3 + 1 = 24
        assert est["raw"] == 24
        assert est["redacted"] == 24
        assert est["total"] == 48
