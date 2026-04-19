"""Deterministic runtime parser for crawllib-style task logs."""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Iterable

from cl_monitoring.domain import (
    Confidence,
    ErrorFamily,
    RUN_SUMMARY_COUNTER_KEYS,
    RunResult,
    RunSummary,
    TaskSnapshot,
)


ITEM_EVENT_RE = re.compile(
    r'(?:\(Id:\s*\d+.*\):\s*\{.*"price"\s*:|\bЦена\b)',
    re.IGNORECASE,
)
PUT_TO_PARSER_RE = re.compile(r"\bput_to_parser\b", re.IGNORECASE)
SUMMARY_EVENT_RE = re.compile(r"^\|\s*Статистика\s*$")
RESUME_SUCCESS_RE = re.compile(r"\|\s*Резюме:\s*✅")
IS_SUCCESS_TRUE_RE = re.compile(r'"isSuccess"\s*:\s*true')
SKU_NOT_FOUND_RE = re.compile(r"(?:sku_not_found|не наш[её]л SKU)", re.IGNORECASE)
GONE_404_RE = re.compile(
    r"(?:got gone status code\s*404|gone status code\s*404|status code\s*404.*\bgone\b)",
    re.IGNORECASE,
)
CANCEL_MARKER_RE = re.compile(
    r"(?:CancelledError|task was cancelled|cancelled by supervisor|operation cancelled)",
    re.IGNORECASE,
)
WRAPPED_PARSE_PHP_CANCEL_RE = re.compile(r"parse\.php error", re.IGNORECASE)
ERROR_AUTO_STOP_RE = re.compile(r"\berror_auto_stop\b.*\bis reached\b", re.IGNORECASE)
AUTO_STOP_RE = re.compile(
    r"(?:\bauto[_ -]?stop\b(?!\s*=).*\bis reached\b|max.?runtime.*exceeded|killed by scheduler)",
    re.IGNORECASE,
)
BAN_429_RE = re.compile(
    r"(?:too many requests|got ban status code\s*\(?429\)?|ban status code\s*\(?429\)?|status code\s*429)",
    re.IGNORECASE,
)
ERROR_SIGNAL_RE = re.compile(
    r"(?:Traceback \(most recent call last\)|\bexception\b|^ERROR\b|\bKilled\b)",
    re.IGNORECASE,
)
FAILED_EVIDENCE_RE = re.compile(r"(?:Traceback \(most recent call last\)|Exception:)", re.IGNORECASE)
INCOMPLETE_LOG_EVIDENCE = "log may be truncated at collector page limit"


def parse_crawllib_default(
    task: TaskSnapshot,
    log_lines: Iterable[str],
    *,
    is_complete: bool,
) -> RunSummary:
    """Parse ordered log input into a deterministic RunSummary.

    The function is intentionally stateless. Callers can pass a full log,
    a cumulative log assembled from several pages, or an incremental snapshot
    together with an explicit completeness flag.
    """

    lines = [line.rstrip("\n") for line in log_lines]
    counters = _collect_counters(lines)
    status = (task.status or "").lower()

    if status == "cancelled":
        return _build_summary(
            task,
            run_result=RunResult.CANCELLED,
            confidence=Confidence.HIGH,
            reason_code="cancelled_api_status",
            counters=counters,
            evidence=["task.status=cancelled"],
            error_family=ErrorFamily.CANCELLED,
        )

    if status in {"pending", "running"}:
        evidence = [f"task.status={status}"]
        if not is_complete:
            evidence.append(INCOMPLETE_LOG_EVIDENCE)
        evidence.extend(_evidence_lines(lines, PUT_TO_PARSER_RE, IS_SUCCESS_TRUE_RE))
        return _build_summary(
            task,
            run_result=RunResult.UNKNOWN,
            confidence=Confidence.LOW,
            reason_code="unknown_running_or_pending",
            counters=counters,
            evidence=evidence,
        )

    cancel_line = _first_matching_line(lines, CANCEL_MARKER_RE)
    if cancel_line and _is_wrapped_parse_php_cancel(status, lines):
        cancel_line = None
    if cancel_line:
        return _build_summary(
            task,
            run_result=RunResult.CANCELLED,
            confidence=Confidence.HIGH,
            reason_code="cancelled_marker_with_terminal_context",
            counters=counters,
            evidence=[cancel_line],
            error_family=ErrorFamily.CANCELLED,
        )

    ban_line = _first_matching_line(lines, BAN_429_RE)
    error_auto_stop_line = _first_matching_line(lines, ERROR_AUTO_STOP_RE)
    if ban_line and error_auto_stop_line:
        return _build_summary(
            task,
            run_result=RunResult.FAILED,
            confidence=Confidence.HIGH,
            reason_code="failed_ban_429_error_auto_stop",
            counters=counters,
            evidence=_dedupe([ban_line, error_auto_stop_line]),
            error_family=ErrorFamily.ANTI_BOT,
        )

    resume_success_line = _first_matching_line(lines, RESUME_SUCCESS_RE)
    if resume_success_line:
        return _build_summary(
            task,
            run_result=RunResult.SUCCESS,
            confidence=Confidence.HIGH,
            reason_code="success_summary_marker",
            counters=counters,
            evidence=[resume_success_line],
        )

    auto_stop_line = _first_matching_line(lines, AUTO_STOP_RE)
    if auto_stop_line:
        return _build_summary(
            task,
            run_result=RunResult.RULE_STOPPED,
            confidence=Confidence.HIGH,
            reason_code="rule_stopped_auto_stop",
            counters=counters,
            evidence=_evidence_lines(lines, AUTO_STOP_RE, PUT_TO_PARSER_RE),
        )

    if not is_complete:
        evidence = [INCOMPLETE_LOG_EVIDENCE]
        evidence.extend(_evidence_lines(lines, PUT_TO_PARSER_RE, IS_SUCCESS_TRUE_RE))
        return _build_summary(
            task,
            run_result=RunResult.UNKNOWN,
            confidence=Confidence.LOW,
            reason_code="unknown_incomplete_log",
            counters=counters,
            evidence=evidence,
        )

    has_positive_progress = _has_positive_progress(counters)
    has_error_signal = _has_error_signal(lines)
    has_partial_errors = bool(
        status in {"error", "abnormal"}
        or has_error_signal
        or counters["sku_not_found"] > 0
        or counters["gone_404"] > 0
        or counters["error_auto_stop_markers"] > 0
    )

    if has_positive_progress and has_partial_errors:
        return _build_summary(
            task,
            run_result=RunResult.PARTIAL_SUCCESS,
            confidence=Confidence.MEDIUM,
            reason_code="partial_success_positive_progress_with_errors",
            counters=counters,
            evidence=_evidence_lines(
                lines,
                IS_SUCCESS_TRUE_RE,
                SKU_NOT_FOUND_RE,
                GONE_404_RE,
                PUT_TO_PARSER_RE,
            ),
        )

    if has_positive_progress:
        evidence = _evidence_lines(lines, IS_SUCCESS_TRUE_RE, PUT_TO_PARSER_RE)
        if not evidence:
            item_line = _first_matching_line(lines, ITEM_EVENT_RE)
            if item_line:
                evidence.append(item_line)
        return _build_summary(
            task,
            run_result=RunResult.SUCCESS_PROBABLE,
            confidence=Confidence.MEDIUM,
            reason_code="success_probable_positive_progress_complete_log",
            counters=counters,
            evidence=evidence,
        )

    if status in {"error", "abnormal"} or has_error_signal:
        evidence = _evidence_lines(lines, FAILED_EVIDENCE_RE)
        return _build_summary(
            task,
            run_result=RunResult.FAILED,
            confidence=Confidence.MEDIUM,
            reason_code="failed_error_without_positive_signal",
            counters=counters,
            evidence=evidence,
        )

    return _build_summary(
        task,
        run_result=RunResult.UNKNOWN,
        confidence=Confidence.LOW,
        reason_code="unknown_finished_without_positive_signal",
        counters=counters,
        evidence=[],
    )


def _build_summary(
    task: TaskSnapshot,
    *,
    run_result: RunResult,
    confidence: Confidence,
    reason_code: str,
    counters: dict[str, int],
    evidence: list[str],
    error_family: ErrorFamily | None = None,
) -> RunSummary:
    return RunSummary(
        task_id=task.id,
        execution_key=task.execution_key,
        run_result=run_result,
        confidence=confidence,
        reason_code=reason_code,
        evidence=evidence,
        counters=counters,
        error_family=error_family,
    )


def _collect_counters(lines: list[str]) -> dict[str, int]:
    counters = {key: 0 for key in RUN_SUMMARY_COUNTER_KEYS}
    counters["lines_seen"] = len(lines)
    counters["item_events"] = _count_matches(lines, ITEM_EVENT_RE)
    counters["put_to_parser"] = _count_matches(lines, PUT_TO_PARSER_RE)
    counters["summary_events"] = _count_matches(lines, SUMMARY_EVENT_RE)
    counters["resume_success_markers"] = _count_matches(lines, RESUME_SUCCESS_RE)
    counters["is_success_true"] = _count_matches(lines, IS_SUCCESS_TRUE_RE)
    counters["sku_not_found"] = _count_matches(lines, SKU_NOT_FOUND_RE)
    counters["gone_404"] = _count_matches(lines, GONE_404_RE)
    counters["cancel_markers"] = _count_matches(lines, CANCEL_MARKER_RE)
    counters["error_auto_stop_markers"] = _count_matches(lines, ERROR_AUTO_STOP_RE)
    counters["auto_stop_markers"] = _count_matches(lines, AUTO_STOP_RE)
    counters["ban_429_markers"] = _count_matches(lines, BAN_429_RE)
    return counters


def _count_matches(lines: list[str], pattern: re.Pattern[str]) -> int:
    return sum(1 for line in lines if pattern.search(line))


def _first_matching_line(lines: list[str], pattern: re.Pattern[str]) -> str | None:
    for line in lines:
        if pattern.search(line):
            return line.strip()
    return None


def _evidence_lines(lines: list[str], *patterns: re.Pattern[str]) -> list[str]:
    return _dedupe(
        [line for pattern in patterns if (line := _first_matching_line(lines, pattern))]
    )


def _dedupe(lines: list[str]) -> list[str]:
    deduped: list[str] = []
    for line in lines:
        if line not in deduped:
            deduped.append(line)
    return deduped


def _has_positive_progress(counters: dict[str, int]) -> bool:
    return any(
        counters[key] > 0
        for key in (
            "item_events",
            "put_to_parser",
            "summary_events",
            "resume_success_markers",
            "is_success_true",
        )
    )


def _has_error_signal(lines: list[str]) -> bool:
    return _first_matching_line(lines, ERROR_SIGNAL_RE) is not None


def _is_wrapped_parse_php_cancel(status: str, lines: list[str]) -> bool:
    # Some crawllib failures are re-raised through CancelledError transport.
    return status == "error" and _first_matching_line(lines, WRAPPED_PARSE_PHP_CANCEL_RE) is not None


def build_synthetic_task(task_id: str, *, status: str) -> TaskSnapshot:
    """Small test helper for synthetic fixture cases without API payloads."""

    spider_id = "SPIDER_ID_SYNTHETIC"
    cmd = "python synthetic.py"
    param = ""
    return TaskSnapshot(
        id=task_id,
        spider_id=spider_id,
        schedule_id="SCHEDULE_ID_SYNTHETIC",
        status=status,
        cmd=cmd,
        param=param,
        create_ts=None,
        start_ts=None,
        end_ts=None,
        runtime=timedelta(0),
        is_manual=False,
        execution_key=f"{spider_id}:{cmd}:{param}",
    )
