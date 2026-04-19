"""Internal domain models (post-normalization).

These models represent the app's own view of the data,
decoupled from Crawlab API response shapes.
"""

from datetime import datetime, timedelta
from enum import Enum, StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class RunResult(StrEnum):
    SUCCESS = "success"
    SUCCESS_PROBABLE = "success_probable"
    PARTIAL_SUCCESS = "partial_success"
    RULE_STOPPED = "rule_stopped"
    CANCELLED = "cancelled"
    FAILED = "failed"
    UNKNOWN = "unknown"


class Confidence(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ScheduleHealthStatus(StrEnum):
    ON_TIME = "on_time"
    QUEUED_START = "queued_start"
    DELAYED_START = "delayed_start"
    RUNNING_AS_EXPECTED = "running_as_expected"
    RUNNING_LONG = "running_long"
    MISSED_SCHEDULE = "missed_schedule"
    RECOVERED_BY_MANUAL_RERUN = "recovered_by_manual_rerun"


class ErrorFamily(StrEnum):
    ANTI_BOT = "anti_bot"
    CANCELLED = "cancelled"
    CRASH = "crash"


RUN_SUMMARY_REASON_CODES = frozenset(
    {
        "cancelled_api_status",
        "cancelled_marker_with_terminal_context",
        "failed_ban_429_error_auto_stop",
        "failed_error_without_positive_signal",
        "rule_stopped_auto_stop",
        "success_summary_marker",
        "success_probable_positive_progress_complete_log",
        "partial_success_positive_progress_with_errors",
        "unknown_running_or_pending",
        "unknown_incomplete_log",
        "unknown_finished_without_positive_signal",
    }
)

RUN_SUMMARY_COUNTER_KEYS = (
    "lines_seen",
    "item_events",
    "put_to_parser",
    "summary_events",
    "resume_success_markers",
    "is_success_true",
    "sku_not_found",
    "gone_404",
    "cancel_markers",
    "auto_stop_markers",
    "error_auto_stop_markers",
    "ban_429_markers",
)

SCHEDULE_HEALTH_REASON_CODES = frozenset(
    {
        "on_time_observed_fire",
        "queued_after_observed_fire",
        "delayed_start_after_observed_fire",
        "running_within_baseline",
        "running_exceeds_baseline",
        "missed_expected_fire_window",
        "recovered_by_manual_success",
        "insufficient_history_for_strong_judgment",
    }
)

SCHEDULE_HEALTH_COUNTER_KEYS = (
    "scheduled_tasks_seen",
    "manual_tasks_seen",
    "running_tasks_seen",
    "terminal_tasks_seen",
    "baseline_samples",
    "current_runtime_ms",
    "baseline_runtime_ms",
    "start_delay_ms",
    "lateness_ms",
    "missed_windows",
    "manual_recovery_runs",
)


def _enum_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    return value


def _normalize_counters(
    raw_value: Any,
    *,
    allowed_keys: tuple[str, ...],
    aliases: dict[str, str] | None = None,
) -> dict[str, int]:
    if raw_value is None:
        raw_value = {}

    if not isinstance(raw_value, dict):
        raise TypeError("counters must be provided as a dict[str, int]")

    normalized = {key: 0 for key in allowed_keys}
    key_aliases = aliases or {}

    for raw_key, raw_count in raw_value.items():
        key = key_aliases.get(raw_key, raw_key)
        if key not in normalized:
            raise ValueError(f"unsupported counter key: {raw_key}")
        if isinstance(raw_count, bool) or not isinstance(raw_count, int):
            raise ValueError(f"counter '{key}' must be an integer")
        normalized[key] = raw_count

    return normalized


def _infer_legacy_run_reason_code(data: dict[str, Any]) -> str:
    run_result = _enum_value(data.get("run_result"))
    error_family = _enum_value(data.get("error_family"))

    if run_result == RunResult.CANCELLED.value:
        return "cancelled_marker_with_terminal_context"
    if run_result == RunResult.FAILED.value:
        if error_family == ErrorFamily.ANTI_BOT.value:
            return "failed_ban_429_error_auto_stop"
        return "failed_error_without_positive_signal"
    if run_result == RunResult.RULE_STOPPED.value:
        return "rule_stopped_auto_stop"
    if run_result == RunResult.SUCCESS.value:
        return "success_summary_marker"
    if run_result == RunResult.SUCCESS_PROBABLE.value:
        return "success_probable_positive_progress_complete_log"
    if run_result == RunResult.PARTIAL_SUCCESS.value:
        return "partial_success_positive_progress_with_errors"
    return "unknown_incomplete_log"


class RunSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str
    execution_key: str
    run_result: RunResult
    confidence: Confidence
    reason_code: str
    evidence: list[str]
    counters: dict[str, int]
    error_family: ErrorFamily | None = None

    @model_validator(mode="before")
    @classmethod
    def _freeze_runtime_shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        payload = dict(data)
        if not payload.get("reason_code"):
            payload["reason_code"] = _infer_legacy_run_reason_code(payload)
        payload["counters"] = _normalize_counters(
            payload.get("counters"),
            allowed_keys=RUN_SUMMARY_COUNTER_KEYS,
            aliases={"404_gone": "gone_404"},
        )
        return payload

    @field_validator("reason_code")
    @classmethod
    def _validate_reason_code(cls, value: str) -> str:
        if value not in RUN_SUMMARY_REASON_CODES:
            raise ValueError("reason_code must use the frozen RunSummary catalog")
        return value


class ScheduleHealth(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schedule_id: str
    execution_key: str
    health: ScheduleHealthStatus
    confidence: Confidence
    reason_code: str
    evidence: list[str]
    counters: dict[str, int]

    @model_validator(mode="before")
    @classmethod
    def _freeze_runtime_shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        payload = dict(data)
        payload["counters"] = _normalize_counters(
            payload.get("counters"),
            allowed_keys=SCHEDULE_HEALTH_COUNTER_KEYS,
        )
        return payload

    @field_validator("reason_code")
    @classmethod
    def _validate_reason_code(cls, value: str) -> str:
        if value not in SCHEDULE_HEALTH_REASON_CODES:
            raise ValueError("reason_code must use the frozen ScheduleHealth catalog")
        return value


class TaskSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    spider_id: str
    schedule_id: str | None
    status: str
    cmd: str
    param: str
    create_ts: datetime | None
    start_ts: datetime | None
    end_ts: datetime | None
    runtime: timedelta
    is_manual: bool
    execution_key: str


class SpiderSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    col_id: str
    project_id: str
    cmd: str
    param: str


class ScheduleSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    name: str
    spider_id: str
    cron: str
    cmd: str
    param: str
    enabled: bool
