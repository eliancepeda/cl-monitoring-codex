from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from cl_monitoring.db.engine import connect_sqlite
from cl_monitoring.db.repo import LocalRepository
from cl_monitoring.domain import (
    Confidence,
    RunResult,
    RunSummary,
    ScheduleSnapshot,
    SpiderSnapshot,
    TaskSnapshot,
)
from cl_monitoring.domain.normalizers import build_execution_key
from cl_monitoring.sync.poller import Poller, PollerConfig


SPIDER_ID = "SPIDER_ID_100"
MISSING_SPIDER_ID = "SPIDER_ID_404"
ERROR_SPIDER_ID = "SPIDER_ID_500"
SCHEDULE_ID = "SCHEDULE_ID_100"
MISSING_SCHEDULE_ID = "SCHEDULE_ID_404"
ERROR_SCHEDULE_ID = "SCHEDULE_ID_500"
CMD = "python spider.py"
PARAM = "--region eu"
EXECUTION_KEY = build_execution_key(SPIDER_ID, CMD, PARAM)


def dt(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 4, 19, hour, minute, tzinfo=UTC)


class FakeReadonlyClient:
    def __init__(
        self,
        *,
        schedules: list[dict[str, Any]] | None = None,
        spiders: dict[str, dict[str, Any]] | None = None,
        spider_status_codes: dict[str, int] | None = None,
        tasks_by_status: dict[str, list[dict[str, Any]]] | None = None,
        log_pages: dict[str, dict[int, list[str]]] | None = None,
    ) -> None:
        self.schedules = schedules or []
        self.spiders = spiders or {}
        self.spider_status_codes = spider_status_codes or {}
        self.tasks_by_status = tasks_by_status or {}
        self.log_pages = log_pages or {}
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def get_json(self, path: str, **params: Any) -> Any:
        self.calls.append(("get_json", path, dict(params)))
        if path == "/api/schedules":
            return {"data": self.schedules}
        if path.startswith("/api/spiders/"):
            spider_id = path.rsplit("/", 1)[-1]
            if spider_id in self.spider_status_codes:
                request = httpx.Request("GET", f"https://crawlab.example{path}")
                response = httpx.Response(
                    self.spider_status_codes[spider_id],
                    request=request,
                )
                raise httpx.HTTPStatusError(
                    f"{response.status_code} response",
                    request=request,
                    response=response,
                )
            return {"data": self.spiders[spider_id]}
        if path.endswith("/logs"):
            task_id = path.split("/")[-2]
            page = int(params["page"])
            size = int(params["size"])
            lines = list(self.log_pages.get(task_id, {}).get(page, []))
            total = 0
            for page_num, page_lines in self.log_pages.get(task_id, {}).items():
                total = max(total, (page_num - 1) * size + len(page_lines))
            return {"data": [{"msg": line} for line in lines], "total": total}
        raise AssertionError(f"unexpected get_json path: {path}")

    async def get_paginated(
        self,
        path: str,
        *,
        page_size: int,
        max_pages: int,
        **extra_params: Any,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        del page_size, max_pages
        self.calls.append(("get_paginated", path, dict(extra_params)))
        assert path == "/api/tasks"
        conditions = json.loads(extra_params["conditions"])
        status = conditions[0]["value"]
        items = list(self.tasks_by_status.get(status, []))
        return items, {
            "api_reported_total": len(items),
            "pages_fetched": 1,
            "records_fetched": len(items),
        }


def make_schedule_raw() -> dict[str, Any]:
    return {
        "_id": SCHEDULE_ID,
        "name": "hourly demo",
        "spider_id": SPIDER_ID,
        "cron": "0 * * * *",
        "cmd": CMD,
        "param": PARAM,
        "enabled": True,
    }


def make_schedule_raw_for(spider_id: str, schedule_id: str) -> dict[str, Any]:
    return {
        "_id": schedule_id,
        "name": f"schedule for {spider_id}",
        "spider_id": spider_id,
        "cron": "0 * * * *",
        "cmd": CMD,
        "param": PARAM,
        "enabled": True,
    }


def make_spider_raw() -> dict[str, Any]:
    return {
        "_id": SPIDER_ID,
        "name": "demo spider",
        "col_id": "COL_ID_100",
        "project_id": "PROJECT_ID_100",
        "cmd": CMD,
        "param": PARAM,
    }


def make_spider_snapshot(spider_id: str) -> SpiderSnapshot:
    return SpiderSnapshot(
        id=spider_id,
        name=f"cached spider {spider_id}",
        col_id="COL_ID_OLD",
        project_id="PROJECT_ID_OLD",
        cmd=CMD,
        param=PARAM,
    )


def make_task_raw(
    task_id: str,
    *,
    status: str,
    create_ts: datetime,
    start_ts: datetime | None,
    end_ts: datetime | None,
    runtime_ms: int,
    schedule_id: str = SCHEDULE_ID,
) -> dict[str, Any]:
    return {
        "_id": task_id,
        "spider_id": SPIDER_ID,
        "status": status,
        "cmd": CMD,
        "param": PARAM,
        "schedule_id": schedule_id,
        "create_ts": create_ts.isoformat().replace("+00:00", "Z"),
        "stat": {
            "start_ts": _iso_or_zero(start_ts),
            "end_ts": _iso_or_zero(end_ts),
            "runtime_duration": runtime_ms,
        },
    }


def make_task_snapshot(
    task_id: str,
    *,
    create_ts: datetime,
    start_ts: datetime | None,
    runtime: timedelta,
    status: str = "finished",
    schedule_id: str | None = SCHEDULE_ID,
    is_manual: bool = False,
) -> TaskSnapshot:
    end_ts = (
        None
        if status in {"pending", "running"}
        else (start_ts or create_ts) + runtime
    )
    return TaskSnapshot(
        id=task_id,
        spider_id=SPIDER_ID,
        schedule_id=schedule_id,
        status=status,
        cmd=CMD,
        param=PARAM,
        create_ts=create_ts,
        start_ts=start_ts,
        end_ts=end_ts,
        runtime=runtime,
        is_manual=is_manual,
        execution_key=EXECUTION_KEY,
    )


def make_schedule_snapshot() -> ScheduleSnapshot:
    return ScheduleSnapshot(
        id=SCHEDULE_ID,
        name="hourly demo",
        spider_id=SPIDER_ID,
        cron="0 * * * *",
        cmd=CMD,
        param=PARAM,
        enabled=True,
    )


def make_success_summary(task_id: str) -> RunSummary:
    return RunSummary(
        task_id=task_id,
        execution_key=EXECUTION_KEY,
        run_result=RunResult.SUCCESS,
        confidence=Confidence.HIGH,
        reason_code="success_summary_marker",
        evidence=["| Резюме: ✅"],
        counters={},
    )


async def test_poller_resumes_from_cursor_with_one_page_overlap_and_final_sync(
) -> None:
    repo = LocalRepository(connect_sqlite(":memory:"))
    config = PollerConfig(
        task_page_size=10,
        task_max_pages=1,
        log_page_size=2,
        log_max_pages_per_sync=5,
    )

    client1 = FakeReadonlyClient(
        schedules=[make_schedule_raw()],
        spiders={SPIDER_ID: make_spider_raw()},
        tasks_by_status={
            "running": [
                make_task_raw(
                    "TASK_ID_900",
                    status="running",
                    create_ts=dt(12),
                    start_ts=dt(12, 1),
                    end_ts=None,
                    runtime_ms=0,
                )
            ]
        },
        log_pages={
            "TASK_ID_900": {
                1: ["line 1", "line 2"],
                2: ["line 3", "line 4"],
            }
        },
    )

    poller1 = Poller(client1, repo, config=config)
    await poller1.sync_once(now=dt(12, 4), force=True)

    first_cursor = repo.get_log_cursor("TASK_ID_900")
    first_summary = repo.get_run_summary("TASK_ID_900")
    assert first_cursor is not None
    assert first_cursor.next_page == 3
    assert first_cursor.assembled_line_count == 4
    assert first_cursor.final_sync_done is False
    assert first_summary is not None
    assert first_summary.reason_code == "unknown_running_or_pending"

    client2 = FakeReadonlyClient(
        schedules=[make_schedule_raw()],
        spiders={SPIDER_ID: make_spider_raw()},
        tasks_by_status={
            "finished": [
                make_task_raw(
                    "TASK_ID_900",
                    status="finished",
                    create_ts=dt(12),
                    start_ts=dt(12, 1),
                    end_ts=dt(12, 6),
                    runtime_ms=300000,
                )
            ]
        },
        log_pages={
            "TASK_ID_900": {
                1: ["line 1", "line 2"],
                2: ["line 3", "line 4"],
                3: ["| Резюме: ✅"],
            }
        },
    )

    poller2 = Poller(client2, repo, config=config)
    await poller2.sync_once(now=dt(12, 7), force=True)

    final_cursor = repo.get_log_cursor("TASK_ID_900")
    final_summary = repo.get_run_summary("TASK_ID_900")
    assert final_cursor is not None
    assert final_cursor.next_page == 4
    assert final_cursor.assembled_line_count == 5
    assert final_cursor.final_sync_done is True
    assert final_summary is not None
    assert final_summary.run_result is RunResult.SUCCESS
    assert final_summary.reason_code == "success_summary_marker"
    assert repo.list_tasks_requiring_log_sync() == []

    pages_queried = [
        params["page"]
        for kind, path, params in client2.calls
        if kind == "get_json" and path.endswith("/logs")
    ]
    assert pages_queried == [2, 3]


async def test_poller_skips_spider_detail_404_and_keeps_other_spiders() -> None:
    repo = LocalRepository(connect_sqlite(":memory:"))
    repo.save_spiders([make_spider_snapshot(MISSING_SPIDER_ID)], seen_at=dt(8, 0))

    client = FakeReadonlyClient(
        schedules=[
            make_schedule_raw(),
            make_schedule_raw_for(MISSING_SPIDER_ID, MISSING_SCHEDULE_ID),
        ],
        spiders={SPIDER_ID: make_spider_raw()},
        spider_status_codes={MISSING_SPIDER_ID: 404},
    )

    poller = Poller(client, repo)
    await poller.sync_once(now=dt(9, 0), force=True)

    assert sorted(schedule.id for schedule in repo.list_schedules()) == [
        SCHEDULE_ID,
        MISSING_SCHEDULE_ID,
    ]
    assert [spider.id for spider in repo.list_spiders()] == [SPIDER_ID]


async def test_poller_reraises_non_404_spider_detail_failure() -> None:
    repo = LocalRepository(connect_sqlite(":memory:"))
    client = FakeReadonlyClient(
        schedules=[make_schedule_raw_for(ERROR_SPIDER_ID, ERROR_SCHEDULE_ID)],
        spider_status_codes={ERROR_SPIDER_ID: 500},
    )

    poller = Poller(client, repo)

    with pytest.raises(httpx.HTTPStatusError):
        await poller.sync_once(now=dt(9, 0), force=True)


def test_poller_projects_and_closes_schedule_incidents() -> None:
    repo = LocalRepository(connect_sqlite(":memory:"))
    repo.save_schedules([make_schedule_snapshot()], seen_at=dt(9, 50))
    repo.save_task_snapshots(
        [
            make_task_snapshot(
                "T10",
                create_ts=dt(10),
                start_ts=dt(10, 1),
                runtime=timedelta(minutes=8),
            ),
            make_task_snapshot(
                "T11",
                create_ts=dt(11),
                start_ts=dt(11, 1),
                runtime=timedelta(minutes=9),
            ),
            make_task_snapshot(
                "T12",
                create_ts=dt(12),
                start_ts=dt(12, 1),
                runtime=timedelta(minutes=10),
            ),
        ],
        seen_at=dt(12, 15),
    )

    poller = Poller(FakeReadonlyClient(), repo)
    poller.refresh_incidents(now=dt(13, 5))

    incident = repo.get_open_incident(f"schedule:{SCHEDULE_ID}")
    assert incident is not None
    assert incident.reason_code == "missed_expected_fire_window"
    assert incident.severity == "critical"

    manual_recovery = make_task_snapshot(
        "M13",
        create_ts=dt(13, 10),
        start_ts=dt(13, 10),
        runtime=timedelta(minutes=5),
        schedule_id=None,
        is_manual=True,
    )
    repo.save_task_snapshots([manual_recovery], seen_at=dt(13, 10))
    repo.upsert_run_summary(make_success_summary("M13"), parsed_at=dt(13, 10))

    poller.refresh_incidents(now=dt(13, 20))

    assert repo.get_open_incident(f"schedule:{SCHEDULE_ID}") is None
    history = repo.list_incidents()
    assert len(history) == 1
    assert history[0].closed_at == dt(13, 20)


def _iso_or_zero(value: datetime | None) -> str:
    if value is None:
        return "0001-01-01T00:00:00Z"
    return value.isoformat().replace("+00:00", "Z")
