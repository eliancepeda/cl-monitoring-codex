"""Internal models for deterministic schedule-health reasoning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from pydantic import BaseModel, ConfigDict, field_validator


class ScheduleEngineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    fire_window: timedelta = timedelta(minutes=2)
    queue_grace: timedelta = timedelta(minutes=5)
    runtime_grace: timedelta = timedelta(minutes=2)
    min_fire_interval_samples: int = 2
    min_runtime_baseline_samples: int = 2

    @field_validator("fire_window", "queue_grace", "runtime_grace")
    @classmethod
    def _validate_positive_timedelta(cls, value: timedelta) -> timedelta:
        if value <= timedelta(0):
            raise ValueError("engine windows and grace periods must be positive")
        return value

    @field_validator("min_fire_interval_samples", "min_runtime_baseline_samples")
    @classmethod
    def _validate_positive_sample_count(cls, value: int) -> int:
        if value < 1:
            raise ValueError("baseline sample counts must be at least 1")
        return value


@dataclass(frozen=True)
class ObservedFireBaseline:
    interval_minutes: int
    sample_count: int
    last_fire_minute: datetime
    expected_fire_minute: datetime
    window_start: datetime
    window_end: datetime


@dataclass(frozen=True)
class RuntimeBaseline:
    baseline_runtime: timedelta
    sample_count: int
    used_execution_key_fallback: bool = False
