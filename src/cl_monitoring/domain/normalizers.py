"""Normalization functions for Crawlab data.

Rules (AGENTS.md § Domain rules):
- "000000000000000000000000" → None  (zero-id)
- "0001-01-01T00:00:00Z"    → None  (null time)
- Manual run detection: schedule_id is zero-id
- execution_key = spider_id + normalized cmd + normalized param
- Live runtime: now() - start_at  when runtime_duration == 0
"""

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

from .models import TaskSnapshot, SpiderSnapshot, ScheduleSnapshot


ZERO_ID = "000000000000000000000000"
NULL_TIME = "0001-01-01T00:00:00Z"


def normalize_id(val: Optional[str]) -> Optional[str]:
    if val is None or val == ZERO_ID:
        return None
    return val


def normalize_time(val: str) -> Optional[datetime]:
    if not val or val == NULL_TIME:
        return None
    val = val.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(val)
    except ValueError:
        return None


def normalize_cmd_or_param(val: str) -> str:
    return val.strip() if val else ""


def build_execution_key(spider_id: str, cmd: str, param: str) -> str:
    norm_cmd = normalize_cmd_or_param(cmd)
    norm_param = normalize_cmd_or_param(param)
    return f"{spider_id}:{norm_cmd}:{norm_param}"


def compute_live_runtime(
    start_at: Optional[datetime], runtime_duration: int, now: Optional[datetime] = None
) -> timedelta:
    if runtime_duration > 0:
        return timedelta(milliseconds=runtime_duration)

    if not start_at:
        return timedelta(0)

    current_time = now or datetime.now(timezone.utc)
    res = current_time - start_at
    # If the computed time is negative for some reason (clocks sync issue etc), return 0
    return max(res, timedelta(0))


def is_manual_run(schedule_id: Optional[str]) -> bool:
    return schedule_id is None


def normalize_task(raw: Dict[str, Any], now: Optional[datetime] = None) -> TaskSnapshot:
    task_id = raw.get("_id", "")
    spider_id = raw.get("spider_id", "")
    status = raw.get("status", "unknown")

    raw_schedule_id = raw.get("schedule_id", "")
    schedule_id = normalize_id(raw_schedule_id)

    cmd = raw.get("cmd", "")
    param = raw.get("param", "")

    stat = raw.get("stat", {})

    create_ts = normalize_time(raw.get("create_ts", ""))
    start_ts = normalize_time(stat.get("start_ts", ""))
    end_ts = normalize_time(stat.get("end_ts", ""))

    runtime_duration = stat.get("runtime_duration", 0)
    runtime = timedelta(milliseconds=runtime_duration)
    if runtime_duration <= 0 and status == "running":
        runtime = compute_live_runtime(start_ts, runtime_duration, now)

    return TaskSnapshot(
        id=task_id,
        spider_id=spider_id,
        schedule_id=schedule_id,
        status=status,
        cmd=cmd,
        param=param,
        create_ts=create_ts,
        start_ts=start_ts,
        end_ts=end_ts,
        runtime=runtime,
        is_manual=is_manual_run(schedule_id),
        execution_key=build_execution_key(spider_id, cmd, param),
    )


def normalize_spider(raw: Dict[str, Any]) -> SpiderSnapshot:
    return SpiderSnapshot(
        id=raw.get("_id", ""),
        name=raw.get("name", ""),
        col_id=raw.get("col_id", ""),
        project_id=raw.get("project_id", ""),
        cmd=raw.get("cmd", ""),
        param=raw.get("param", ""),
    )


def normalize_schedule(raw: Dict[str, Any]) -> ScheduleSnapshot:
    return ScheduleSnapshot(
        id=raw.get("_id", ""),
        name=raw.get("name", ""),
        spider_id=raw.get("spider_id", ""),
        cron=raw.get("cron", ""),
        cmd=raw.get("cmd", ""),
        param=raw.get("param", ""),
        enabled=raw.get("enabled", False),
    )
