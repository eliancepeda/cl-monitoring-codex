from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import yaml

from cl_monitoring.domain import TaskSnapshot
from cl_monitoring.domain.normalizers import normalize_task
from cl_monitoring.parsers.crawllib_default import (
    build_synthetic_task,
    parse_crawllib_default,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
API_DIR = FIXTURES_DIR / "api"
LOG_DIR = FIXTURES_DIR / "logs"
EXPECTED_DIR = FIXTURES_DIR / "expected"

SYNTHETIC_TASKS = {
    "ID_821": build_synthetic_task("ID_821", status="error"),
    "ID_822": build_synthetic_task("ID_822", status="finished"),
}


def _load_expected(path: Path) -> dict[str, Any]:
    content = "\n".join(
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.startswith("#")
    )
    parsed = yaml.safe_load(content)
    if not isinstance(parsed, dict):
        raise TypeError(f"Expected YAML mapping in {path}")
    return cast(dict[str, Any], parsed)


def _load_task(task_id: str) -> TaskSnapshot:
    api_path = API_DIR / f"task_{task_id}.json"
    if api_path.exists():
        return normalize_task(json.loads(api_path.read_text(encoding="utf-8")))
    return SYNTHETIC_TASKS[task_id]


def _is_complete(expected: dict[str, Any]) -> bool:
    return "log may be truncated at collector page limit" not in expected["evidence"]


def test_crawllib_parser_matches_expected_corpus() -> None:
    for expected_path in sorted(EXPECTED_DIR.glob("task_ID_*_log.yaml")):
        expected = _load_expected(expected_path)
        task_id = expected["task_id"]
        log_lines = (
            (LOG_DIR / f"{task_id}.log").read_text(encoding="utf-8").splitlines()
        )
        task = _load_task(task_id)

        summary = parse_crawllib_default(
            task, log_lines, is_complete=_is_complete(expected)
        )
        summary_repeat = parse_crawllib_default(
            task, log_lines, is_complete=_is_complete(expected)
        )

        assert summary == summary_repeat
        assert summary.task_id == expected["task_id"]
        assert summary.run_result.value == expected["run_result"]
        assert summary.confidence.value == expected["confidence"]
        assert summary.reason_code == expected["reason_code"]
        assert summary.counters == expected["counters"]
        assert summary.evidence == expected["evidence"]


def test_crawllib_parser_supports_incremental_paginated_input() -> None:
    expected = _load_expected(EXPECTED_DIR / "task_ID_793_log.yaml")
    task = _load_task("ID_793")
    log_lines = (LOG_DIR / "ID_793.log").read_text(encoding="utf-8").splitlines()

    partial = parse_crawllib_default(task, log_lines[:900], is_complete=False)
    final = parse_crawllib_default(
        task, log_lines[:900] + log_lines[900:], is_complete=True
    )

    assert partial.run_result.value == "unknown"
    assert partial.reason_code == "unknown_incomplete_log"
    assert final.run_result.value == expected["run_result"]
    assert final.reason_code == expected["reason_code"]
    assert final.counters == expected["counters"]


def test_summary_success_marker_is_counted_separately_from_summary_block() -> None:
    expected = _load_expected(EXPECTED_DIR / "task_ID_807_log.yaml")
    task = _load_task("ID_807")
    log_lines = (LOG_DIR / "ID_807.log").read_text(encoding="utf-8").splitlines()

    summary = parse_crawllib_default(task, log_lines, is_complete=True)

    assert (
        summary.counters["summary_events"]
        == expected["counters"]["summary_events"]
        == 1
    )
    assert (
        summary.counters["resume_success_markers"]
        == expected["counters"]["resume_success_markers"]
        == 1
    )
    assert summary.reason_code == "success_summary_marker"


def test_wrapped_parse_php_cancelled_error_falls_through_to_partial_success() -> None:
    expected = _load_expected(EXPECTED_DIR / "task_ID_823_log.yaml")
    task = _load_task("ID_823")
    log_lines = (LOG_DIR / "ID_823.log").read_text(encoding="utf-8").splitlines()

    summary = parse_crawllib_default(task, log_lines, is_complete=True)

    assert summary.run_result.value == expected["run_result"] == "partial_success"
    assert summary.reason_code == expected["reason_code"]
    assert summary.evidence == expected["evidence"]
    assert summary.counters == expected["counters"]
    assert summary.counters["cancel_markers"] == 2


def test_plain_cancelled_error_marker_stays_cancelled() -> None:
    expected = _load_expected(EXPECTED_DIR / "task_ID_821_log.yaml")
    task = _load_task("ID_821")
    log_lines = (LOG_DIR / "ID_821.log").read_text(encoding="utf-8").splitlines()

    summary = parse_crawllib_default(task, log_lines, is_complete=True)

    assert summary.run_result.value == expected["run_result"] == "cancelled"
    assert (
        summary.reason_code
        == expected["reason_code"]
        == "cancelled_marker_with_terminal_context"
    )
    assert summary.evidence == expected["evidence"]
    assert summary.counters == expected["counters"]
