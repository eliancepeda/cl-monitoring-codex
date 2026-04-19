"""Log classifier for Crawlab task logs.

Two-phase classification system:

Phase 1 — Candidate discovery (metadata only):
    CandidateClass assigned from task status/schedule_id.
    No log content needed.  Used for targeted sampling.

Phase 2 — Final log classification (after fetching logs):
    FinalLogClass assigned by inspecting log text.
    Determines actual outcome for fixture labeling.

Key rule (AGENTS.md § Domain rules, user_scope.yml):
    result_count / item_scraped_count in task metadata must NOT be treated
    as reliable success signals.  Only log content analysis can determine
    success grade.

Classification is deterministic — no LLM logic (AGENTS.md § Workflow).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

ZERO_OBJECT_ID = "000000000000000000000000"

SUMMARY_MARKER_RE = re.compile(r"\|\s*Резюме:\s*✅")
PUT_TO_PARSER_RE = re.compile(r"\bput_to_parser\b", re.IGNORECASE)
IS_SUCCESS_TRUE_RE = re.compile(r'"isSuccess"\s*:\s*true')
ITEM_EVENT_RE = re.compile(r'(?:\{"price"\s*:|\bЦена\b)')
SCRAPY_PROGRESS_RE = re.compile(
    r"(?:item_scraped_count|finish_reason|Dumping Scrapy stats)", re.IGNORECASE
)
SKU_NOT_FOUND_RE = re.compile(r"(?:sku_not_found|не наш[её]л SKU)", re.IGNORECASE)
GONE_404_RE = re.compile(r"(?:\b404\b|\bgone\b)", re.IGNORECASE)
CANCEL_MARKER_RE = re.compile(r"\bcancel(?:led)?\b", re.IGNORECASE)
ERROR_AUTO_STOP_RE = re.compile(r"\berror_auto_stop\b", re.IGNORECASE)
AUTO_STOP_MARKER_RE = re.compile(
    r"(?:\bauto[_ -]?stop\b(?!\s*=)|max.?runtime.*exceeded|killed by scheduler)",
    re.IGNORECASE,
)
BAN_429_RE = re.compile(
    r"(?:\b429\b|Too Many Requests|ban status code\s*\(?429\)?|status code\s*429)",
    re.IGNORECASE,
)


class LogClass(StrEnum):
    """Classification categories for Crawlab task log content."""

    SCRAPY_START = "scrapy_start"
    SCRAPY_STATS = "scrapy_stats"
    SCRAPY_ERROR = "scrapy_error"
    SCRAPY_ITEM_DROP = "scrapy_item_drop"
    SCRAPY_WARNING = "scrapy_warning"
    SYSTEM_INFO = "system_info"
    CUSTOM_PRINT = "custom_print"
    EMPTY_LOG = "empty_log"


# ── Pattern definitions ────────────────────────────────────────────────

# Order matters: more specific patterns first
LOG_PATTERNS: list[tuple[LogClass, re.Pattern[str]]] = [
    (
        LogClass.SCRAPY_STATS,
        re.compile(
            r"(?:downloader/response_count|item_scraped_count|"
            r"finish_reason|Dumping Scrapy stats)",
            re.IGNORECASE,
        ),
    ),
    (
        LogClass.SCRAPY_ERROR,
        re.compile(
            r"(?:Traceback \(most recent call last\)|"
            r"Error processing|Spider error|"
            r"twisted\.internet\.error|"
            r"Exception:|ERROR\s)",
            re.IGNORECASE,
        ),
    ),
    (
        LogClass.SCRAPY_ITEM_DROP,
        re.compile(
            r"(?:Dropped:|DropItem|item dropped)",
            re.IGNORECASE,
        ),
    ),
    (
        LogClass.SCRAPY_WARNING,
        re.compile(r"\bWARNING\b"),
    ),
    (
        LogClass.SCRAPY_START,
        re.compile(
            r"(?:Crawl started|Spider opened|Scrapy \d+\.\d+|"
            r"Bot name:|Spider started)",
            re.IGNORECASE,
        ),
    ),
    (
        LogClass.SYSTEM_INFO,
        re.compile(
            r"(?:crawlab|task runner|PID|node_id|worker|"
            r"Connecting to|Closing spider|"
            r"^\[\w+\]\s+\d{4})",
            re.IGNORECASE,
        ),
    ),
]


@dataclass
class LogClassification:
    """Result of classifying a task's log content."""

    task_id: str
    total_lines: int
    classes_found: list[str] = field(default_factory=list)
    class_line_counts: dict[str, int] = field(default_factory=dict)
    error_lines: int = 0
    warning_lines: int = 0
    scrapy_stats_found: bool = False
    has_traceback: bool = False


def classify_log_text(text: str, task_id: str = "") -> LogClassification:
    """Classify log text into categories.

    Args:
        text: Raw log text content.
        task_id: Task identifier for the result.

    Returns:
        LogClassification with per-class line counts and metadata.
    """
    if not text or not text.strip():
        return LogClassification(
            task_id=task_id,
            total_lines=0,
            classes_found=[LogClass.EMPTY_LOG.value],
            class_line_counts={LogClass.EMPTY_LOG.value: 1},
        )

    lines = text.splitlines()
    class_counts: dict[str, int] = {}
    error_count = 0
    warning_count = 0
    stats_found = False
    has_traceback = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        matched = False
        for log_class, pattern in LOG_PATTERNS:
            if pattern.search(stripped):
                class_counts[log_class.value] = class_counts.get(log_class.value, 0) + 1
                matched = True

                if log_class == LogClass.SCRAPY_ERROR:
                    error_count += 1
                    if "Traceback" in stripped:
                        has_traceback = True
                elif log_class == LogClass.SCRAPY_WARNING:
                    warning_count += 1
                elif log_class == LogClass.SCRAPY_STATS:
                    stats_found = True

                break  # First match wins per line

        if not matched:
            class_counts[LogClass.CUSTOM_PRINT.value] = (
                class_counts.get(LogClass.CUSTOM_PRINT.value, 0) + 1
            )

    return LogClassification(
        task_id=task_id,
        total_lines=len(lines),
        classes_found=sorted(class_counts.keys()),
        class_line_counts=class_counts,
        error_lines=error_count,
        warning_lines=warning_count,
        scrapy_stats_found=stats_found,
        has_traceback=has_traceback,
    )


# ── Phase 1: Candidate classification (metadata only) ──────────────────


class CandidateClass(StrEnum):
    """Phase-1 candidate classes assigned from task metadata only.

    No log inspection.  Used for targeted discovery and sampling.
    Finished tasks stay 'finished_candidate' — no success inference
    from metadata (result_count is unreliable in this Crawlab usage).
    """

    RUNNING = "running"
    PENDING = "pending"
    FINISHED_CANDIDATE = "finished_candidate"
    ERROR_CANDIDATE = "error_candidate"
    CANCELLED_CANDIDATE = "cancelled_candidate"


def is_manual_run(task: dict[str, Any]) -> bool:
    """Detect manual runs by zero schedule_id (AGENTS.md § Domain rules)."""
    return task.get("schedule_id") == ZERO_OBJECT_ID


def classify_candidate(task: dict[str, Any]) -> CandidateClass:
    """Classify a task into a candidate class from metadata only.

    This is Phase 1: NO log content, NO result_count inference.
    Finished tasks are always 'finished_candidate' regardless of
    any metadata fields like result_count or item_scraped_count.

    Args:
        task: Task dict from Crawlab API.

    Returns:
        CandidateClass enum value.
    """
    status = (task.get("status", "") or "").lower()

    if status == "running":
        return CandidateClass.RUNNING
    if status == "pending":
        return CandidateClass.PENDING
    if status == "cancelled":
        return CandidateClass.CANCELLED_CANDIDATE
    if status == "error" or status == "abnormal":
        return CandidateClass.ERROR_CANDIDATE
    if status == "finished":
        return CandidateClass.FINISHED_CANDIDATE

    # Unknown status → treat as error candidate for investigation
    return CandidateClass.ERROR_CANDIDATE


# ── Phase 2: Final log classification (after fetching logs) ────────────


class FinalLogClass(StrEnum):
    """Phase-2 final classes assigned after inspecting log content.

    Determines actual outcome for fixture labeling and sampling.
    """

    SUCCESS_STRONG = "success_strong"
    SUCCESS_PROBABLE = "success_probable"
    PARTIAL_SUCCESS = "partial_success"
    AUTO_STOP = "auto_stop"
    BAN_429 = "ban_429"
    CANCELLED = "cancelled"
    FAILED_OTHER = "failed_other"
    UNKNOWN = "unknown"


def classify_final(
    task: dict[str, Any],
    log_text: str | None = None,
) -> FinalLogClass:
    """Classify a task into a final log class after inspecting logs.

    This is Phase 2: uses task status + log content for deep classification.
    Does NOT use result_count or item_scraped_count from task metadata
    as success signals.

    Args:
        task: Task dict from Crawlab API.
        log_text: Log content (None if not fetched).

    Returns:
        FinalLogClass enum value.
    """
    status = (task.get("status", "") or "").lower()

    if status == "cancelled":
        return FinalLogClass.CANCELLED

    if status == "pending":
        return FinalLogClass.UNKNOWN

    if status == "running":
        return FinalLogClass.UNKNOWN

    if status in ("error", "abnormal"):
        if log_text and _has_ban_pattern(log_text):
            return FinalLogClass.BAN_429
        if log_text and _has_auto_stop_pattern(log_text):
            return FinalLogClass.AUTO_STOP
        error_msg = task.get("error", "")
        if _has_ban_pattern(error_msg):
            return FinalLogClass.BAN_429
        if log_text and _has_positive_progress(log_text):
            return FinalLogClass.PARTIAL_SUCCESS
        return FinalLogClass.FAILED_OTHER

    if status == "finished":
        return _classify_finished_final(task, log_text)

    return FinalLogClass.UNKNOWN


def _classify_finished_final(
    task: dict[str, Any],
    log_text: str | None,
) -> FinalLogClass:
    """Subclassify a finished task by inspecting log content.

    Key rule: result_count in task.stat is NOT used as a success signal.
    Only log content (Scrapy stats block) determines success grade.
    """
    if not log_text:
        return FinalLogClass.UNKNOWN

    if SUMMARY_MARKER_RE.search(log_text):
        return FinalLogClass.SUCCESS_STRONG

    if _has_ban_pattern(log_text):
        return FinalLogClass.BAN_429

    if _has_auto_stop_pattern(log_text):
        return FinalLogClass.AUTO_STOP

    classification = classify_log_text(log_text)
    if classification.scrapy_stats_found:
        if classification.error_lines > 0:
            if _stats_have_items(log_text):
                return FinalLogClass.PARTIAL_SUCCESS
            return FinalLogClass.FAILED_OTHER
        if _stats_have_items(log_text):
            return FinalLogClass.SUCCESS_STRONG
        return FinalLogClass.UNKNOWN

    has_positive_progress = _has_positive_progress(log_text)
    has_partial_errors = bool(
        classification.error_lines > 0
        or SKU_NOT_FOUND_RE.search(log_text)
        or GONE_404_RE.search(log_text)
    )

    if has_positive_progress and has_partial_errors and not _looks_incomplete(log_text):
        return FinalLogClass.PARTIAL_SUCCESS

    if has_positive_progress and not _looks_incomplete(log_text):
        return FinalLogClass.SUCCESS_PROBABLE

    if classification.error_lines > 0:
        return FinalLogClass.FAILED_OTHER

    return FinalLogClass.UNKNOWN


def _has_ban_pattern(text: str) -> bool:
    """Check if text contains 429/ban indicators."""
    return bool(BAN_429_RE.search(text))


def _has_auto_stop_pattern(text: str) -> bool:
    """Check if text contains auto-stop indicators."""
    return bool(AUTO_STOP_MARKER_RE.search(text))


def _stats_have_items(log_text: str) -> bool:
    """Check if Scrapy stats in log text indicate items were actually scraped."""
    match = re.search(r"item_scraped_count['\"]?\s*[:=]\s*(\d+)", log_text)
    if match:
        return int(match.group(1)) > 0
    return False


# ── Draft expected YAML generation ──────────────────────────────────────


def build_expected_log_fixture(
    task: dict[str, Any],
    log_text: str,
    *,
    page_size: int | None = None,
    max_pages: int | None = None,
) -> dict[str, Any]:
    """Build collector-side expected data for a log fixture.

    This helper exists only to prepare the offline golden corpus. It is not a
    runtime parser API.
    """
    lines = log_text.splitlines()
    lines_seen = len(lines)
    classification = classify_log_text(log_text, task.get("_id", ""))
    counters = {
        "lines_seen": lines_seen,
        "item_events": _count_matches(ITEM_EVENT_RE, lines),
        "put_to_parser": _count_matches(PUT_TO_PARSER_RE, lines),
        "summary_events": _count_matches(SUMMARY_MARKER_RE, lines),
        "resume_success_markers": 0,
        "is_success_true": _count_matches(IS_SUCCESS_TRUE_RE, lines),
        "sku_not_found": _count_matches(SKU_NOT_FOUND_RE, lines),
        "gone_404": _count_matches(GONE_404_RE, lines),
        "cancel_markers": _count_matches(CANCEL_MARKER_RE, lines),
        "auto_stop_markers": _count_matches(AUTO_STOP_MARKER_RE, lines),
        "error_auto_stop_markers": _count_matches(ERROR_AUTO_STOP_RE, lines),
        "ban_429_markers": _count_matches(BAN_429_RE, lines),
    }
    status = (task.get("status", "") or "").lower()
    complete_log = _is_complete_log(
        log_text,
        status=status,
        page_size=page_size,
        max_pages=max_pages,
    )
    has_positive_progress = any(
        counters[key] > 0
        for key in ("item_events", "put_to_parser", "summary_events", "is_success_true")
    ) or _stats_have_items(log_text)
    has_partial_errors = bool(
        classification.error_lines > 0
        or status in {"error", "abnormal"}
        or any(counters[key] > 0 for key in ("sku_not_found", "gone_404"))
    )
    errors_without_positive = bool(
        (classification.error_lines > 0 or status in {"error", "abnormal"})
        and not has_positive_progress
    )
    evidence: list[str] = []

    if status == "cancelled":
        evidence.append("task.status=cancelled")
        return _expected_payload(
            task_id=task.get("_id", ""),
            run_result="cancelled",
            confidence="high",
            reason_code="cancelled_api_status",
            counters=counters,
            evidence=evidence,
        )

    if status in {"pending", "running"}:
        evidence.append(f"task.status={status}")
        if not complete_log:
            evidence.append("log may be truncated at collector page limit")
        if line := _first_matching_line(PUT_TO_PARSER_RE, lines):
            evidence.append(line)
        if line := _first_matching_line(IS_SUCCESS_TRUE_RE, lines):
            evidence.append(line)
        return _expected_payload(
            task_id=task.get("_id", ""),
            run_result="unknown",
            confidence="low",
            reason_code="unknown_running_or_pending",
            counters=counters,
            evidence=evidence,
        )

    if counters["ban_429_markers"] > 0 and counters["error_auto_stop_markers"] > 0:
        evidence.extend(_evidence_lines(lines, BAN_429_RE, ERROR_AUTO_STOP_RE))
        return _expected_payload(
            task_id=task.get("_id", ""),
            run_result="failed",
            confidence="high",
            reason_code="failed_ban_429_error_auto_stop",
            counters=counters,
            evidence=evidence,
        )

    if counters["summary_events"] > 0:
        evidence.extend(_evidence_lines(lines, SUMMARY_MARKER_RE, PUT_TO_PARSER_RE))
        return _expected_payload(
            task_id=task.get("_id", ""),
            run_result="success",
            confidence="high",
            reason_code="success_summary_marker",
            counters=counters,
            evidence=evidence,
        )

    if counters["auto_stop_markers"] > 0:
        evidence.extend(_evidence_lines(lines, AUTO_STOP_MARKER_RE, PUT_TO_PARSER_RE))
        return _expected_payload(
            task_id=task.get("_id", ""),
            run_result="rule_stopped",
            confidence="high",
            reason_code="rule_stopped_auto_stop",
            counters=counters,
            evidence=evidence,
        )

    if not complete_log:
        evidence.append("log may be truncated at collector page limit")
        if line := _first_matching_line(PUT_TO_PARSER_RE, lines):
            evidence.append(line)
        if line := _first_matching_line(IS_SUCCESS_TRUE_RE, lines):
            evidence.append(line)
        return _expected_payload(
            task_id=task.get("_id", ""),
            run_result="unknown",
            confidence="low",
            reason_code="unknown_incomplete_log",
            counters=counters,
            evidence=evidence,
        )

    if has_positive_progress and has_partial_errors:
        evidence.extend(
            _evidence_lines(
                lines,
                IS_SUCCESS_TRUE_RE,
                SKU_NOT_FOUND_RE,
                GONE_404_RE,
                PUT_TO_PARSER_RE,
            )
        )
        return _expected_payload(
            task_id=task.get("_id", ""),
            run_result="partial_success",
            confidence="medium",
            reason_code="partial_success_positive_progress_with_errors",
            counters=counters,
            evidence=evidence,
        )

    if has_positive_progress:
        evidence.extend(_evidence_lines(lines, IS_SUCCESS_TRUE_RE, PUT_TO_PARSER_RE))
        return _expected_payload(
            task_id=task.get("_id", ""),
            run_result="success_probable",
            confidence="medium",
            reason_code="success_probable_positive_progress_complete_log",
            counters=counters,
            evidence=evidence,
        )

    if status in {"error", "abnormal"} or errors_without_positive:
        if line := _first_matching_line(
            re.compile(r"(?:Exception:|Traceback)", re.IGNORECASE), lines
        ):
            evidence.append(line)
        return _expected_payload(
            task_id=task.get("_id", ""),
            run_result="failed",
            confidence="medium",
            reason_code="failed_error_without_positive_signal",
            counters=counters,
            evidence=evidence,
        )

    return _expected_payload(
        task_id=task.get("_id", ""),
        run_result="unknown",
        confidence="low",
        reason_code="unknown_finished_without_positive_signal",
        counters=counters,
        evidence=evidence,
    )


def generate_expected_yaml(
    expected_data: dict[str, Any],
    output_dir: Path,
) -> Path:
    """Write expected/*.yaml for a redacted log fixture."""
    output_dir.mkdir(parents=True, exist_ok=True)

    safe_task_id = str(expected_data["task_id"]).replace("/", "_")
    filename = f"task_{safe_task_id}_log.yaml"
    filepath = output_dir / filename

    # Write YAML with TODO markers
    lines = ["# Draft expected output — auto-generated by classify_logs.py"]
    lines.append("# TODO: verify all values after manual review of the log fixture")
    lines.append("")
    lines.append(yaml.dump(expected_data, default_flow_style=False, sort_keys=False))

    filepath.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Generated expected YAML: %s", filepath)
    return filepath


def _count_matches(pattern: re.Pattern[str], lines: list[str]) -> int:
    return sum(1 for line in lines if pattern.search(line))


def _first_matching_line(pattern: re.Pattern[str], lines: list[str]) -> str | None:
    for line in lines:
        if pattern.search(line):
            return line.strip()
    return None


def _evidence_lines(lines: list[str], *patterns: re.Pattern[str]) -> list[str]:
    evidence: list[str] = []
    for pattern in patterns:
        line = _first_matching_line(pattern, lines)
        if line and line not in evidence:
            evidence.append(line)
    return evidence


def _has_positive_progress(log_text: str) -> bool:
    return bool(
        SUMMARY_MARKER_RE.search(log_text)
        or PUT_TO_PARSER_RE.search(log_text)
        or IS_SUCCESS_TRUE_RE.search(log_text)
        or ITEM_EVENT_RE.search(log_text)
        or _stats_have_items(log_text)
    )


def _looks_incomplete(log_text: str) -> bool:
    lines_seen = len(log_text.splitlines())
    return (
        lines_seen >= 4000
        and not SUMMARY_MARKER_RE.search(log_text)
        and not IS_SUCCESS_TRUE_RE.search(log_text)
    )


def _is_complete_log(
    log_text: str,
    *,
    status: str,
    page_size: int | None,
    max_pages: int | None,
) -> bool:
    if status == "cancelled":
        return True
    if page_size and max_pages:
        limit = page_size * max_pages
        if (
            len(log_text.splitlines()) >= limit
            and not SUMMARY_MARKER_RE.search(log_text)
            and not IS_SUCCESS_TRUE_RE.search(log_text)
        ):
            return False
    return not _looks_incomplete(log_text)


def _expected_payload(
    *,
    task_id: str,
    run_result: str,
    confidence: str,
    reason_code: str,
    counters: dict[str, int],
    evidence: list[str],
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "run_result": run_result,
        "confidence": confidence,
        "reason_code": reason_code,
        "counters": counters,
        "evidence": evidence,
    }


def generate_manifest_entry(
    task_id: str,
    final_class: FinalLogClass,
    candidate_class: CandidateClass,
    log_classification: LogClassification | None,
    fixture_paths: dict[str, str],
    *,
    is_manual: bool = False,
) -> dict[str, Any]:
    """Generate a single entry for fixtures/manifest.md.

    Args:
        task_id: Redacted task ID.
        final_class: Final log class after inspection.
        candidate_class: Original candidate class from metadata.
        log_classification: Optional log classification result.
        fixture_paths: Map of fixture type → relative path.
        is_manual: Whether this was a manual run.

    Returns:
        Dict suitable for manifest generation.
    """
    entry: dict[str, Any] = {
        "task_id": task_id,
        "candidate_class": candidate_class.value,
        "final_class": final_class.value,
        "trigger": "manual" if is_manual else "scheduled",
        "files": fixture_paths,
    }
    if log_classification:
        entry["log_classes"] = log_classification.classes_found
        entry["total_log_lines"] = log_classification.total_lines
        entry["error_lines"] = log_classification.error_lines
    return entry
