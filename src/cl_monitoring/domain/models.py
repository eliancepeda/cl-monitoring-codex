"""Internal domain models (post-normalization).

These models represent the app's own view of the data,
decoupled from Crawlab API response shapes.
"""

from datetime import datetime, timedelta
from typing import Optional, List, Dict
from pydantic import BaseModel
from enum import Enum


class RunResult(str, Enum):
    SUCCESS = "success"
    SUCCESS_PROBABLE = "success_probable"
    PARTIAL_SUCCESS = "partial_success"
    RULE_STOPPED = "rule_stopped"
    CANCELLED = "cancelled"
    FAILED = "failed"
    UNKNOWN = "unknown"


class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ErrorFamily(str, Enum):
    ANTI_BOT = "anti_bot"
    CANCELLED = "cancelled"
    CRASH = "crash"


class RunSummary(BaseModel):
    task_id: str
    execution_key: str
    run_result: RunResult
    error_family: Optional[ErrorFamily]
    confidence: Confidence
    counters: Dict[str, int]
    evidence: List[str]


class TaskSnapshot(BaseModel):
    id: str
    spider_id: str
    schedule_id: Optional[str]
    status: str
    cmd: str
    param: str
    create_ts: Optional[datetime]
    start_ts: Optional[datetime]
    end_ts: Optional[datetime]
    runtime: timedelta
    is_manual: bool
    execution_key: str


class SpiderSnapshot(BaseModel):
    id: str
    name: str
    col_id: str
    project_id: str
    cmd: str
    param: str


class ScheduleSnapshot(BaseModel):
    id: str
    name: str
    spider_id: str
    cron: str
    cmd: str
    param: str
    enabled: bool
