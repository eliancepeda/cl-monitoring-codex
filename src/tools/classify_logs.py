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
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class LogClass(str, Enum):
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
                class_counts[log_class.value] = (
                    class_counts.get(log_class.value, 0) + 1
                )
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


class CandidateClass(str, Enum):
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
    schedule_id = task.get("schedule_id", "")
    return schedule_id == "000000000000000000000000" or not schedule_id


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


class FinalLogClass(str, Enum):
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
        error_msg = task.get("error", "")
        if _has_ban_pattern(error_msg):
            return FinalLogClass.BAN_429
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
    # Check for auto_stop signal
    if log_text and _has_auto_stop_pattern(log_text):
        return FinalLogClass.AUTO_STOP

    # Check for ban/429
    if log_text and _has_ban_pattern(log_text):
        return FinalLogClass.BAN_429

    if log_text:
        classification = classify_log_text(log_text)

        # Has errors → partial success at best
        if classification.error_lines > 0:
            if classification.scrapy_stats_found:
                return FinalLogClass.PARTIAL_SUCCESS
            return FinalLogClass.FAILED_OTHER

        # Has stats and no errors → check for item count IN LOGS
        if classification.scrapy_stats_found:
            if _stats_have_items(log_text):
                return FinalLogClass.SUCCESS_STRONG
            else:
                return FinalLogClass.SUCCESS_PROBABLE

        # No stats block → probable success (could be non-scrapy spider)
        return FinalLogClass.SUCCESS_PROBABLE

    # No log text available → unknown (we cannot infer success)
    return FinalLogClass.UNKNOWN


def _has_ban_pattern(text: str) -> bool:
    """Check if text contains 429/ban indicators."""
    ban_patterns = [
        r"\b429\b",
        r"Too Many Requests",
        r"rate.?limit",
        r"banned",
        r"blocked",
        r"captcha",
    ]
    for pattern in ban_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def _has_auto_stop_pattern(text: str) -> bool:
    """Check if text contains auto-stop indicators."""
    auto_stop_patterns = [
        r"auto.?stop",
        r"max.?runtime",
        r"timeout.*exceeded",
        r"killed by scheduler",
    ]
    for pattern in auto_stop_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def _stats_have_items(log_text: str) -> bool:
    """Check if Scrapy stats in log text indicate items were actually scraped."""
    match = re.search(r"item_scraped_count['\"]?\s*[:=]\s*(\d+)", log_text)
    if match:
        return int(match.group(1)) > 0
    return False


# ── Draft expected YAML generation ──────────────────────────────────────

def generate_expected_yaml(
    classification: LogClassification,
    output_dir: Path,
) -> Path:
    """Generate a draft expected/*.yaml skeleton for a log fixture.

    Generated files have # TODO: verify markers for manual review.

    Args:
        classification: Result from classify_log_text.
        output_dir: Directory to write YAML files to (fixtures/expected/).

    Returns:
        Path to the generated YAML file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    safe_task_id = classification.task_id.replace("/", "_")
    filename = f"task_{safe_task_id}_log.yaml"
    filepath = output_dir / filename

    data = {
        "task_id": classification.task_id,
        "log_classes_found": classification.classes_found,
        "stats": {
            "total_lines": classification.total_lines,
            "error_lines": classification.error_lines,
            "warning_lines": classification.warning_lines,
            "scrapy_stats_block_found": classification.scrapy_stats_found,
            "has_traceback": classification.has_traceback,
        },
        "class_line_counts": classification.class_line_counts,
    }

    # Write YAML with TODO markers
    lines = ["# Draft expected output — auto-generated by classify_logs.py"]
    lines.append("# TODO: verify all values after manual review of the log fixture")
    lines.append("")
    lines.append(yaml.dump(data, default_flow_style=False, sort_keys=False))

    filepath.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Generated expected YAML: %s", filepath)
    return filepath


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
