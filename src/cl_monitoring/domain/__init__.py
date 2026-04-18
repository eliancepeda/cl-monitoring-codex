"""Domain logic — normalization and internal models."""

from .models import (
    Confidence,
    ErrorFamily,
    RUN_SUMMARY_COUNTER_KEYS,
    RUN_SUMMARY_REASON_CODES,
    RunResult,
    RunSummary,
    SCHEDULE_HEALTH_COUNTER_KEYS,
    SCHEDULE_HEALTH_REASON_CODES,
    ScheduleHealth,
    ScheduleHealthStatus,
    ScheduleSnapshot,
    SpiderSnapshot,
    TaskSnapshot,
)

__all__ = [
    "Confidence",
    "ErrorFamily",
    "RUN_SUMMARY_COUNTER_KEYS",
    "RUN_SUMMARY_REASON_CODES",
    "RunResult",
    "RunSummary",
    "SCHEDULE_HEALTH_COUNTER_KEYS",
    "SCHEDULE_HEALTH_REASON_CODES",
    "ScheduleHealth",
    "ScheduleHealthStatus",
    "ScheduleSnapshot",
    "SpiderSnapshot",
    "TaskSnapshot",
]
