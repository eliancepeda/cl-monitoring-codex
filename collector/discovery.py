from collections import defaultdict
from typing import Any

from collector.config import (
    DEFAULT_MAX_PROJECT_PAGES,
    DEFAULT_MAX_SCHEDULE_PAGES,
    DEFAULT_MAX_SPIDER_PAGES,
    DEFAULT_MAX_TASK_PAGES,
)
from collector.library_inspection import inspect_wheel_sources
from collector.log_analysis import analyze_log_text
from collector.normalize import build_observation_unit
from collector.reporting import render_report_documents


PROJECT_KEYS = ("project_id", "projectId", "project")
SPIDER_KEYS = ("spider_id", "spiderId", "spider")
SCHEDULE_KEYS = ("schedule_id", "scheduleId", "schedule")
NODE_KEYS = ("node_id", "nodeId", "node")
REPRESENTATIVE_CASE_TYPES = (
    "failed",
    "long-running",
    "manual rerun candidate",
    "http error spike",
    "finished but suspicious",
    "successful",
)
FAILURE_STATUSES = {"failed", "error"}
SENSITIVE_NODE_FIELDS = {"key"}


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    return []


def _extract_record(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return {}


def _redact_sensitive_node_fields(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {
            key: "[REDACTED]"
            if key in SENSITIVE_NODE_FIELDS
            else _redact_sensitive_node_fields(value)
            for key, value in payload.items()
        }
    if isinstance(payload, list):
        return [_redact_sensitive_node_fields(item) for item in payload]
    return payload


def _extract_identifier(value: Any) -> Any:
    if isinstance(value, dict):
        return value.get("_id") or value.get("id")
    return value


def _first_matching_id(record: dict[str, Any], keys: tuple[str, ...], expected: set[str]) -> str | None:
    for key in keys:
        if key not in record:
            continue
        raw_value = record.get(key)
        value = _extract_identifier(raw_value)
        if value in expected:
            return str(value)

    spider = record.get("spider")
    if isinstance(spider, dict):
        nested_project_id = _extract_identifier(
            spider.get("project_id") or spider.get("projectId") or spider.get("project")
        )
        if nested_project_id in expected:
            return str(nested_project_id)
    return None


def _record_id(record: dict[str, Any]) -> str | None:
    value = record.get("_id") or record.get("id")
    return str(value) if value is not None else None


def _first_present_id(record: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        if key not in record:
            continue
        value = _extract_identifier(record.get(key))
        if value is not None:
            return str(value)
    return None


def _spider_context(task: dict[str, Any], spider_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    spider_id = _first_present_id(task, SPIDER_KEYS)
    if spider_id is None:
        return {"name": "unknown-spider"}
    return spider_lookup.get(spider_id, {"_id": spider_id, "name": spider_id})


def _safe_get(transport, api_map, open_questions, path: str, query: dict[str, Any] | None = None):
    try:
        response = transport.get(path, query)
    except RuntimeError as exc:
        api_map.append(
            {
                "path": path,
                "method": "GET",
                "query": dict(query or {}),
                "source": "documented",
                "status": "unknown",
                "notes": str(exc),
            }
        )
        open_questions.append(f"Unable to observe `{path}`: {exc}")
        return None

    api_map.append(
        {
            "path": path,
            "method": "GET",
            "query": dict(query or {}),
            "source": "documented",
            "status": "observed",
            "notes": f"HTTP {response.status}",
        }
    )
    return response


def _coerce_log_payload(payload: Any) -> list[str] | str:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [str(item) for item in data]
        if isinstance(data, str):
            return data
    return ""


def _select_case_type(task: dict[str, Any], log_result: dict[str, Any]) -> str:
    status = str(task.get("status", "")).lower()
    normalized_params = task["observation"]["normalized_params"]["parameters"]
    param_keys = {item["normalized_key"] for item in normalized_params}
    signals = log_result["signals"]
    has_positive_finished_signal = (
        signals["has_summary"] or signals["has_progress"] or signals["found_or_written"]
    )

    if signals["http_403_404_5xx"]:
        return "http error spike"
    if status in FAILURE_STATUSES:
        return "failed"
    if status in {"running", "pending"}:
        return "long-running"
    if "spi" in param_keys:
        return "manual rerun candidate"
    if status == "finished" and (signals["empty_log"] or not has_positive_finished_signal):
        return "finished but suspicious"
    return "successful"


def _entity_fields(records: list[dict[str, Any]]) -> list[str]:
    fields = set()
    for record in records:
        fields.update(record.keys())
    return sorted(fields)


def _normalize_status(value: Any) -> str:
    return str(value or "unknown").lower()


def _parameter_pattern(observation: dict[str, Any]) -> list[str]:
    parameters = observation["normalized_params"]["parameters"]
    if not parameters:
        return ["no-params"]
    return sorted(f"{item['normalized_key']}={item['value']}" for item in parameters)


def _trigger_kind_from_observation(observation: dict[str, Any]) -> dict[str, str]:
    if observation["schedule_id"] != "unscheduled":
        return {"kind": "schedule", "status": "fact"}
    return {"kind": "manual-or-unknown", "status": "unknown"}


def _resolve_project_id(
    task: dict[str, Any],
    target_project_ids: set[str],
    spider_lookup: dict[str, dict[str, Any]],
) -> str | None:
    project_id = _first_matching_id(task, PROJECT_KEYS, target_project_ids)
    if project_id is not None:
        return project_id

    spider_id = _first_present_id(task, SPIDER_KEYS)
    spider = spider_lookup.get(spider_id or "")
    if spider is not None:
        return _first_matching_id(spider, PROJECT_KEYS, target_project_ids)
    return None


def _has_long_log_candidate(task: dict[str, Any]) -> bool:
    payloads = [task]
    for key in ("pagination", "stats", "log", "logs"):
        value = task.get(key)
        if isinstance(value, dict):
            payloads.append(value)

    for payload in payloads:
        for key in (
            "pages",
            "total_pages",
            "page_count",
            "pageCount",
            "log_pages",
            "logPages",
        ):
            value = payload.get(key)
            if isinstance(value, int) and value > 1:
                return True

        for key in (
            "total",
            "log_count",
            "logCount",
            "logs_total",
            "logsTotal",
            "total_logs",
            "totalLogs",
            "line_count",
            "lineCount",
        ):
            value = payload.get(key)
            if isinstance(value, int) and value > 100:
                return True

    return False


def _build_inventory_tasks(
    collected_tasks: list[dict[str, Any]],
    spider_lookup: dict[str, dict[str, Any]],
    target_project_ids: set[str],
    scheduled_spider_ids: set[str],
) -> list[dict[str, Any]]:
    inventory_tasks: list[dict[str, Any]] = []
    seen_task_ids: set[str] = set()

    for task in collected_tasks:
        task_id = _record_id(task)
        if task_id is None or task_id in seen_task_ids:
            continue

        project_id = _resolve_project_id(task, target_project_ids, spider_lookup)
        if project_id is None:
            continue

        observation = build_observation_unit(task, _spider_context(task, spider_lookup))
        spider_id = _first_present_id(task, SPIDER_KEYS) or observation["spider_id"]
        inventory_tasks.append(
            {
                "task_id": task_id,
                "project_id": project_id,
                "spider_id": spider_id,
                "spider_name": observation["spider_name"],
                "status": _normalize_status(task.get("status")),
                "param_pattern": _parameter_pattern(observation),
                "has_schedule": (
                    observation["schedule_id"] != "unscheduled"
                    or spider_id in scheduled_spider_ids
                ),
                "has_long_log_candidate": _has_long_log_candidate(task),
                "observation": observation,
                "raw_task": task,
            }
        )
        seen_task_ids.add(task_id)

    return inventory_tasks


def _task_inventory_shape(
    task: dict[str, Any],
    spider_lookup: dict[str, dict[str, Any]],
    target_project_ids: set[str],
) -> tuple[str, str, str, tuple[str, ...]] | None:
    project_id = _resolve_project_id(task, target_project_ids, spider_lookup)
    if project_id is None:
        return None

    observation = build_observation_unit(task, _spider_context(task, spider_lookup))
    spider_id = _first_present_id(task, SPIDER_KEYS) or observation["spider_id"]
    return (
        project_id,
        spider_id,
        _normalize_status(task.get("status")),
        tuple(_parameter_pattern(observation)),
    )


def _update_task_inventory_progress(
    tasks: list[dict[str, Any]],
    spider_lookup: dict[str, dict[str, Any]],
    target_project_ids: set[str],
    project_candidate_task_ids: dict[str, set[str]],
    project_candidate_counts: dict[str, int],
    seen_task_ids: set[str],
    seen_shapes: set[tuple[str, str, str, tuple[str, ...]]],
) -> tuple[bool, bool]:
    page_has_new_ids = False
    page_has_new_shapes = False

    for task in tasks:
        task_id = _record_id(task)
        if task_id is not None and task_id not in seen_task_ids:
            seen_task_ids.add(task_id)
            page_has_new_ids = True

        shape = _task_inventory_shape(task, spider_lookup, target_project_ids)
        if shape is None:
            continue

        project_id = shape[0]
        if task_id is not None:
            project_candidate_task_ids[project_id].add(task_id)
            project_candidate_counts[project_id] = len(project_candidate_task_ids[project_id])

        if shape not in seen_shapes:
            seen_shapes.add(shape)
            page_has_new_shapes = True

    return page_has_new_ids, page_has_new_shapes


def _build_task_inventory(
    inventory_tasks: list[dict[str, Any]],
    project_ids: tuple[str, ...],
    pages: dict[str, int],
    task_inventory_status: dict[str, Any],
) -> dict[str, Any]:
    task_inventory = {
        "projects": {project_id: {"task_ids": [], "spiders": {}} for project_id in project_ids},
        "tasks": [],
        "pages": pages,
        "task_inventory_status": task_inventory_status,
    }

    for item in inventory_tasks:
        project_payload = task_inventory["projects"].setdefault(
            item["project_id"], {"task_ids": [], "spiders": {}}
        )
        project_payload["task_ids"].append(item["task_id"])

        spider_payload = project_payload["spiders"].setdefault(
            item["spider_id"],
            {
                "spider_id": item["spider_id"],
                "spider_name": item["spider_name"],
                "status_counts": {},
                "recent_task_ids": [],
                "params_patterns": [],
                "has_schedule": False,
                "has_long_log_candidate": False,
            },
        )
        spider_payload["status_counts"][item["status"]] = (
            spider_payload["status_counts"].get(item["status"], 0) + 1
        )
        spider_payload["recent_task_ids"].append(item["task_id"])
        if item["param_pattern"] not in spider_payload["params_patterns"]:
            spider_payload["params_patterns"].append(item["param_pattern"])
        spider_payload["has_schedule"] = (
            spider_payload["has_schedule"] or item["has_schedule"]
        )
        spider_payload["has_long_log_candidate"] = (
            spider_payload["has_long_log_candidate"] or item["has_long_log_candidate"]
        )

        task_inventory["tasks"].append(
            {
                "task_id": item["task_id"],
                "project_id": item["project_id"],
                "spider_id": item["spider_id"],
                "spider_name": item["spider_name"],
                "status": item["status"],
                "param_pattern": item["param_pattern"],
                "has_schedule": item["has_schedule"],
                "has_long_log_candidate": item["has_long_log_candidate"],
            }
        )

    return task_inventory


def _sorted_spider_entries(project_payload: dict[str, Any]) -> list[dict[str, Any]]:
    spiders = list(project_payload.get("spiders", {}).values())
    spiders.sort(
        key=lambda item: (
            -len(item["status_counts"]),
            -len(item["params_patterns"]),
            -int(item["has_schedule"]),
            -int(item["has_long_log_candidate"]),
            item["spider_name"],
            item["spider_id"],
        )
    )
    return spiders


def _build_spider_coverage(
    task_inventory: dict[str, Any],
    settings,
    target_spiders: list[dict[str, Any]],
    target_schedules: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    spider_coverage = {"projects": {}}
    selected_spider_ids: dict[str, list[str]] = {}
    spider_project_lookup: dict[str, str] = {}
    spider_maps: dict[str, dict[str, dict[str, Any]]] = {
        project_id: {} for project_id in settings.project_ids
    }

    for spider in target_spiders:
        spider_id = _record_id(spider)
        project_id = _first_matching_id(spider, PROJECT_KEYS, set(settings.project_ids))
        if spider_id is None or project_id is None:
            continue
        spider_project_lookup[spider_id] = project_id
        spider_maps[project_id].setdefault(
            spider_id,
            {
                "spider_id": spider_id,
                "spider_name": spider.get("name") or spider_id,
                "status_counts": {},
                "recent_task_ids": [],
                "params_patterns": [],
                "has_schedule": False,
                "has_long_log_candidate": False,
            },
        )

    for schedule in target_schedules:
        spider_id = _first_present_id(schedule, SPIDER_KEYS)
        if spider_id is None:
            continue
        project_id = _first_matching_id(schedule, PROJECT_KEYS, set(settings.project_ids))
        if project_id is None:
            project_id = spider_project_lookup.get(spider_id)
        if project_id is None:
            continue
        spider_maps[project_id].setdefault(
            spider_id,
            {
                "spider_id": spider_id,
                "spider_name": spider_id,
                "status_counts": {},
                "recent_task_ids": [],
                "params_patterns": [],
                "has_schedule": False,
                "has_long_log_candidate": False,
            },
        )["has_schedule"] = True

    for project_id in settings.project_ids:
        project_payload = task_inventory["projects"].get(project_id, {"spiders": {}})
        for spider_id, item in project_payload.get("spiders", {}).items():
            spider_payload = spider_maps[project_id].setdefault(
                spider_id,
                {
                    "spider_id": spider_id,
                    "spider_name": item["spider_name"],
                    "status_counts": {},
                    "recent_task_ids": [],
                    "params_patterns": [],
                    "has_schedule": False,
                    "has_long_log_candidate": False,
                },
            )
            spider_payload["status_counts"].update(item["status_counts"])
            for task_id in item["recent_task_ids"]:
                if task_id not in spider_payload["recent_task_ids"]:
                    spider_payload["recent_task_ids"].append(task_id)
            for pattern in item["params_patterns"]:
                if pattern not in spider_payload["params_patterns"]:
                    spider_payload["params_patterns"].append(pattern)
            spider_payload["has_schedule"] = (
                spider_payload["has_schedule"] or item["has_schedule"]
            )
            spider_payload["has_long_log_candidate"] = (
                spider_payload["has_long_log_candidate"] or item["has_long_log_candidate"]
            )

        spiders = _sorted_spider_entries({"spiders": spider_maps[project_id]})
        chosen_ids = [item["spider_id"] for item in spiders]
        selected_spider_ids[project_id] = chosen_ids

        spider_coverage["projects"][project_id] = {
            "selected_spider_ids": chosen_ids,
            "spiders": [
                {
                    **item,
                    "selected_for_detail": True,
                    "detail_task_ids": [],
                    "log_task_ids": [],
                }
                for item in spiders
            ],
        }

    return spider_coverage, selected_spider_ids


def _pick_task_for_spider(
    candidates: list[dict[str, Any]],
    seen_statuses: set[str],
    seen_patterns: set[tuple[str, ...]],
) -> dict[str, Any] | None:
    if not candidates:
        return None

    return min(
        candidates,
        key=lambda item: (
            1 if item["status"] in seen_statuses else 0,
            1 if tuple(item["param_pattern"]) in seen_patterns else 0,
            1 if not item["has_schedule"] else 0,
            1 if not item["has_long_log_candidate"] else 0,
        ),
    )


def _plan_detail_and_log_sampling(
    inventory_tasks: list[dict[str, Any]],
    spider_coverage: dict[str, Any],
    selected_spider_ids: dict[str, list[str]],
    settings,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    tasks_by_spider: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in inventory_tasks:
        key = (item["project_id"], item["spider_id"])
        if item["spider_id"] in selected_spider_ids.get(item["project_id"], []):
            tasks_by_spider[key].append(item)

    detail_tasks: list[dict[str, Any]] = []
    chosen_detail_ids: set[str] = set()
    seen_statuses_by_project: dict[str, set[str]] = {
        project_id: set() for project_id in settings.project_ids
    }
    seen_patterns_by_spider: dict[tuple[str, str], set[tuple[str, ...]]] = defaultdict(set)

    for _ in range(settings.max_tasks_per_spider_for_detail):
        made_progress = False
        for project_id in settings.project_ids:
            for spider_id in selected_spider_ids.get(project_id, []):
                if len(detail_tasks) >= settings.max_task_details_total:
                    break
                key = (project_id, spider_id)
                remaining = [
                    item
                    for item in tasks_by_spider.get(key, [])
                    if item["task_id"] not in chosen_detail_ids
                ]
                choice = _pick_task_for_spider(
                    remaining,
                    seen_statuses_by_project[project_id],
                    seen_patterns_by_spider[key],
                )
                if choice is None:
                    continue
                detail_tasks.append(choice)
                chosen_detail_ids.add(choice["task_id"])
                seen_statuses_by_project[project_id].add(choice["status"])
                seen_patterns_by_spider[key].add(tuple(choice["param_pattern"]))
                made_progress = True
            if len(detail_tasks) >= settings.max_task_details_total:
                break
        if not made_progress or len(detail_tasks) >= settings.max_task_details_total:
            break

    log_tasks: list[dict[str, Any]] = []
    chosen_log_ids: set[str] = set()
    chosen_log_counts_by_project: dict[str, int] = {
        project_id: 0 for project_id in settings.project_ids
    }
    seen_log_statuses: dict[str, set[str]] = {
        project_id: set() for project_id in settings.project_ids
    }
    seen_log_patterns: dict[tuple[str, str], set[tuple[str, ...]]] = defaultdict(set)
    detail_by_spider: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in detail_tasks:
        detail_by_spider[(item["project_id"], item["spider_id"])].append(item)

    max_log_rounds = max(
        settings.max_tasks_per_spider_for_detail,
        settings.max_log_tasks_per_project,
    )
    for _ in range(max_log_rounds):
        made_progress = False
        for project_id in settings.project_ids:
            if len(log_tasks) >= settings.max_log_tasks_total:
                break
            for spider_id in selected_spider_ids.get(project_id, []):
                if len(log_tasks) >= settings.max_log_tasks_total:
                    break
                if (
                    chosen_log_counts_by_project[project_id]
                    >= settings.max_log_tasks_per_project
                ):
                    break
                key = (project_id, spider_id)
                remaining = [
                    item
                    for item in detail_by_spider.get(key, [])
                    if item["task_id"] not in chosen_log_ids
                ]
                choice = _pick_task_for_spider(
                    sorted(
                        remaining,
                        key=lambda item: (
                            0 if item["has_long_log_candidate"] else 1,
                            0 if item["has_schedule"] else 1,
                        ),
                    ),
                    seen_log_statuses[project_id],
                    seen_log_patterns[key],
                )
                if choice is None:
                    continue
                log_tasks.append(choice)
                chosen_log_ids.add(choice["task_id"])
                chosen_log_counts_by_project[project_id] += 1
                seen_log_statuses[project_id].add(choice["status"])
                seen_log_patterns[key].add(tuple(choice["param_pattern"]))
                made_progress = True
        if len(log_tasks) >= settings.max_log_tasks_total:
            break
        if not made_progress or all(
            chosen_log_counts_by_project[project_id]
            >= settings.max_log_tasks_per_project
            for project_id in settings.project_ids
        ):
            break

    for project_payload in spider_coverage["projects"].values():
        spider_map = {
            item["spider_id"]: item
            for item in project_payload["spiders"]
            if item["selected_for_detail"]
        }
        for item in detail_tasks:
            spider_payload = spider_map.get(item["spider_id"])
            if spider_payload is not None and item["task_id"] not in spider_payload["detail_task_ids"]:
                spider_payload["detail_task_ids"].append(item["task_id"])
        for item in log_tasks:
            spider_payload = spider_map.get(item["spider_id"])
            if spider_payload is not None and item["task_id"] not in spider_payload["log_task_ids"]:
                spider_payload["log_task_ids"].append(item["task_id"])

    return detail_tasks, log_tasks


def _positive_int(value: Any) -> int | None:
    return value if isinstance(value, int) and value > 0 else None


def _reliable_log_total_pages(payload: Any, page_size: int) -> int | None:
    if not isinstance(payload, dict):
        return None

    pagination = payload.get("pagination") if isinstance(payload.get("pagination"), dict) else {}
    page_candidates = {
        value
        for value in (
            _positive_int(payload.get("pages")),
            _positive_int(payload.get("total_pages")),
            _positive_int(payload.get("page_count")),
            _positive_int(payload.get("pageCount")),
            _positive_int(pagination.get("pages")),
            _positive_int(pagination.get("total_pages")),
            _positive_int(pagination.get("page_count")),
            _positive_int(pagination.get("pageCount")),
        )
        if value is not None
    }
    if len(page_candidates) > 1:
        return None

    total_candidates = {
        value
        for value in (
            _positive_int(payload.get("total")),
            _positive_int(payload.get("count")),
            _positive_int(pagination.get("total")),
            _positive_int(pagination.get("count")),
        )
        if value is not None
    }
    if len(total_candidates) > 1:
        return None

    total_pages = next(iter(page_candidates), None)
    total_items = next(iter(total_candidates), None)
    if total_pages is None and total_items is not None:
        total_pages = max(1, (total_items + page_size - 1) // page_size)

    if total_pages is None:
        return None

    current_page = (
        _positive_int(payload.get("page"))
        or _positive_int(pagination.get("page"))
        or 1
    )
    if current_page != 1:
        return None

    if total_items is not None and total_pages > 1 and total_items <= page_size:
        return None

    return total_pages


def _derive_log_sampling_plan(payload: Any, page_size: int) -> tuple[list[int], str, dict[str, Any]]:
    total_pages = _reliable_log_total_pages(payload, page_size)
    if total_pages is None:
        return [1], "unknown pagination", {"status": "unknown", "total_pages": "unknown"}
    if total_pages <= 1:
        return [1], "first page only", {"status": "known", "total_pages": total_pages}
    if total_pages == 2:
        return [1, 2], "first + last", {"status": "known", "total_pages": total_pages}

    middle_page = (total_pages + 1) // 2
    return [1, middle_page, total_pages], "first + middle + last", {
        "status": "known",
        "total_pages": total_pages,
    }


def _coerce_log_lines(payload: Any) -> list[str]:
    coerced = _coerce_log_payload(payload)
    if isinstance(coerced, list):
        return [str(item) for item in coerced]
    if coerced:
        return str(coerced).splitlines()
    return []


def _compact_log_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in analysis.items()
        if key != "joined"
    }


def _sample_log_pages(
    transport,
    api_map,
    open_questions,
    store,
    task_id: str,
    page_size: int,
    max_log_pages: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    first_response = _safe_get(
        transport,
        api_map,
        open_questions,
        f"/api/tasks/{task_id}/logs",
        {"page": 1, "size": page_size},
    )
    first_payload = first_response.json_data if first_response else {"status": "unknown"}

    page_numbers, strategy, pagination = _derive_log_sampling_plan(first_payload, page_size)
    sampled_pages: list[dict[str, Any]] = []
    combined_lines: list[str] = []

    for page_number in page_numbers[:max_log_pages]:
        if page_number == 1:
            payload = first_payload
        else:
            response = _safe_get(
                transport,
                api_map,
                open_questions,
                f"/api/tasks/{task_id}/logs",
                {"page": page_number, "size": page_size},
            )
            payload = response.json_data if response else {"status": "unknown"}

        store.write_json(f"raw/tasks/logs-{task_id}-page-{page_number}.json", payload)
        if page_number == 1:
            store.write_json(f"raw/tasks/logs-{task_id}.json", payload)

        lines = _coerce_log_lines(payload)
        combined_lines.extend(lines)
        analysis = analyze_log_text(lines)
        sampled_pages.append(
            {
                "page": page_number,
                "line_count": analysis["line_count"],
                "char_count": analysis["char_count"],
                "signals": analysis["signals"],
                "tail": analysis["tail"],
                "stable_fragments": analysis["stable_fragments"],
                "unstable_fragments": analysis["unstable_fragments"],
            }
        )

    combined_analysis = analyze_log_text(combined_lines)
    return {
        "task_id": task_id,
        "strategy": strategy,
        "pagination": pagination,
        "pages": sampled_pages,
        "combined_analysis": _compact_log_analysis(combined_analysis),
    }, combined_analysis


def _mark_spider_long_log_candidate(
    task_inventory: dict[str, Any],
    spider_coverage: dict[str, Any],
    project_id: str,
    spider_id: str,
) -> None:
    project_payload = task_inventory["projects"].get(project_id, {})
    spider_payload = project_payload.get("spiders", {}).get(spider_id)
    if spider_payload is not None:
        spider_payload["has_long_log_candidate"] = True

    for spider_payload in spider_coverage["projects"].get(project_id, {}).get("spiders", []):
        if spider_payload["spider_id"] == spider_id:
            spider_payload["has_long_log_candidate"] = True
            break


def _record_pagination_limit(
    open_questions: list[str], path: str, pages_fetched: int, max_pages: int, last_page_count: int
) -> None:
    if pages_fetched == max_pages and last_page_count:
        open_questions.append(
            f"Coverage for `{path}` may still be limited by global pagination after {max_pages} pages, not confirmed absence in the target projects."
        )


def _page_cap(settings, attr_name: str, default_value: int) -> int:
    configured = getattr(settings, attr_name)
    if configured == default_value and settings.max_pages < default_value:
        return settings.max_pages
    return configured


def _log_sampling_shortfall_reason(
    sampled_tasks: int,
    quota: int,
    candidate_count: int,
    total_sampled_tasks: int,
    max_log_tasks_total: int,
    task_inventory_stop_reason: str,
) -> str | None:
    if sampled_tasks >= quota:
        return None
    if total_sampled_tasks >= max_log_tasks_total:
        return "global log task cap was reached"
    if task_inventory_stop_reason == "page-cap":
        return "task inventory hit configured page cap"
    if candidate_count < quota:
        return "observed target-task candidate set was smaller than the quota"
    return "available sampled tasks fell short of the quota for an unknown reason"


def run_discovery(settings, transport, store, wheel_targets=None):
    open_questions: list[str] = []
    api_map: list[dict[str, Any]] = []
    target_project_ids = set(settings.project_ids)
    project_page_cap = _page_cap(settings, "max_project_pages", DEFAULT_MAX_PROJECT_PAGES)
    spider_page_cap = _page_cap(settings, "max_spider_pages", DEFAULT_MAX_SPIDER_PAGES)
    schedule_page_cap = _page_cap(settings, "max_schedule_pages", DEFAULT_MAX_SCHEDULE_PAGES)
    task_page_cap = _page_cap(settings, "max_task_pages", DEFAULT_MAX_TASK_PAGES)

    target_projects: list[dict[str, Any]] = []
    found_project_ids: set[str] = set()
    project_pages_fetched = 0
    project_last_page_count = 0
    for page in range(1, project_page_cap + 1):
        projects_response = _safe_get(
            transport,
            api_map,
            open_questions,
            "/api/projects",
            {"page": page, "size": settings.page_size},
        )
        store.write_json(
            f"raw/projects/page-{page}.json",
            projects_response.json_data if projects_response else {"status": "unknown"},
        )
        projects = _extract_records(projects_response.json_data if projects_response else {})
        project_pages_fetched = page
        project_last_page_count = len(projects)
        for project in projects:
            project_id = _record_id(project)
            if project_id in target_project_ids and project_id not in found_project_ids:
                target_projects.append(project)
                found_project_ids.add(project_id)

        if found_project_ids == target_project_ids or not projects:
            break

    _record_pagination_limit(
        open_questions,
        "/api/projects",
        project_pages_fetched,
        project_page_cap,
        project_last_page_count,
    )

    missing_project_ids = [
        project_id for project_id in settings.project_ids if project_id not in found_project_ids
    ]
    if missing_project_ids:
        open_questions.append(
            "Requested projects not observed after "
            f"{project_page_cap} `/api/projects` pages: "
            + ", ".join(f"`{project_id}`" for project_id in missing_project_ids)
            + "."
        )

    target_spiders: list[dict[str, Any]] = []
    seen_spider_ids: set[str] = set()
    spiders_pages_fetched = 0
    spiders_last_page_count = 0
    for page in range(1, spider_page_cap + 1):
        spiders_response = _safe_get(
            transport,
            api_map,
            open_questions,
            "/api/spiders",
            {"page": page, "page_size": settings.page_size},
        )
        store.write_json(
            f"raw/spiders/page-{page}.json",
            spiders_response.json_data if spiders_response else {"status": "unknown"},
        )
        spiders = _extract_records(spiders_response.json_data if spiders_response else {})
        spiders_pages_fetched = page
        spiders_last_page_count = len(spiders)

        for spider in spiders:
            if not _first_matching_id(spider, PROJECT_KEYS, target_project_ids):
                continue
            spider_id = _record_id(spider)
            if spider_id is None or spider_id in seen_spider_ids:
                continue
            target_spiders.append(spider)
            seen_spider_ids.add(spider_id)

        if not spiders:
            break

    _record_pagination_limit(
        open_questions,
        "/api/spiders",
        spiders_pages_fetched,
        spider_page_cap,
        spiders_last_page_count,
    )

    spider_lookup = {
        _record_id(spider): spider for spider in target_spiders if _record_id(spider) is not None
    }

    target_spider_ids = set(spider_lookup.keys())
    target_schedules: list[dict[str, Any]] = []
    seen_schedule_ids: set[str] = set()
    schedules_pages_fetched = 0
    schedules_last_page_count = 0
    for page in range(1, schedule_page_cap + 1):
        schedules_response = _safe_get(
            transport,
            api_map,
            open_questions,
            "/api/schedules",
            {"page": page, "page_size": settings.page_size},
        )
        store.write_json(
            f"raw/schedules/page-{page}.json",
            schedules_response.json_data if schedules_response else {"status": "unknown"},
        )
        schedules = _extract_records(schedules_response.json_data if schedules_response else {})
        schedules_pages_fetched = page
        schedules_last_page_count = len(schedules)

        for schedule in schedules:
            schedule_id = _record_id(schedule)
            if schedule_id is None or schedule_id in seen_schedule_ids:
                continue
            if not (
                _first_matching_id(schedule, SPIDER_KEYS, target_spider_ids)
                or _first_matching_id(schedule, PROJECT_KEYS, target_project_ids)
            ):
                continue
            target_schedules.append(schedule)
            seen_schedule_ids.add(schedule_id)

        if not schedules:
            break

    _record_pagination_limit(
        open_questions,
        "/api/schedules",
        schedules_pages_fetched,
        schedule_page_cap,
        schedules_last_page_count,
    )

    nodes_response = _safe_get(
        transport,
        api_map,
        open_questions,
        "/api/nodes",
        {"page": 1, "page_size": settings.page_size},
    )
    store.write_json(
        "raw/nodes/page-1.json",
        _redact_sensitive_node_fields(nodes_response.json_data)
        if nodes_response
        else {"status": "unknown"},
    )
    nodes = _extract_records(nodes_response.json_data if nodes_response else {})
    node_lookup = {
        _record_id(node): node for node in nodes if _record_id(node) is not None
    }

    collected_tasks: list[dict[str, Any]] = []
    task_pages_fetched = 0
    task_last_page_count = 0
    stable_task_pages = 0
    task_inventory_stop_reason = "page-cap"
    project_candidate_task_ids = {project_id: set() for project_id in settings.project_ids}
    project_candidate_counts = {project_id: 0 for project_id in settings.project_ids}
    seen_inventory_task_ids: set[str] = set()
    seen_inventory_shapes: set[tuple[str, str, str, tuple[str, ...]]] = set()
    for page in range(1, task_page_cap + 1):
        tasks_response = _safe_get(
            transport,
            api_map,
            open_questions,
            "/api/tasks",
            {"page": page, "size": settings.page_size},
        )
        store.write_json(
            f"raw/tasks/page-{page}.json",
            tasks_response.json_data if tasks_response else {"status": "unknown"},
        )

        tasks = _extract_records(tasks_response.json_data if tasks_response else {})
        task_pages_fetched = page
        task_last_page_count = len(tasks)
        target_tasks = [
            task
            for task in tasks
            if _first_matching_id(task, PROJECT_KEYS, target_project_ids)
            or _first_matching_id(task, SPIDER_KEYS, target_spider_ids)
        ]

        collected_tasks.extend(target_tasks)

        page_has_new_ids, page_has_new_shapes = _update_task_inventory_progress(
            target_tasks,
            spider_lookup,
            target_project_ids,
            project_candidate_task_ids,
            project_candidate_counts,
            seen_inventory_task_ids,
            seen_inventory_shapes,
        )

        enough_project_candidates = all(
            project_candidate_counts[project_id] >= settings.max_log_tasks_per_project
            for project_id in settings.project_ids
        )
        capped_candidate_total = sum(
            min(project_candidate_counts[project_id], settings.max_log_tasks_per_project)
            for project_id in settings.project_ids
        )
        enough_global_candidates = capped_candidate_total >= settings.max_log_tasks_total
        if page_has_new_ids or page_has_new_shapes:
            stable_task_pages = 0
        else:
            stable_task_pages += 1

        if not tasks:
            task_inventory_stop_reason = "empty-page"
            break
        if (
            (enough_project_candidates or enough_global_candidates)
            and stable_task_pages >= settings.task_page_stability_window
        ):
            task_inventory_stop_reason = "stability-window"
            break

    _record_pagination_limit(
        open_questions,
        "/api/tasks",
        task_pages_fetched,
        task_page_cap,
        task_last_page_count,
    )

    scheduled_spider_ids = {
        spider_id
        for spider_id in (
            _first_present_id(schedule, SPIDER_KEYS) for schedule in target_schedules
        )
        if spider_id is not None
    }

    inventory_tasks = _build_inventory_tasks(
        collected_tasks,
        spider_lookup,
        target_project_ids,
        scheduled_spider_ids,
    )
    task_inventory = _build_task_inventory(
        inventory_tasks,
        settings.project_ids,
        {
            "projects": project_pages_fetched,
            "spiders": spiders_pages_fetched,
            "schedules": schedules_pages_fetched,
            "tasks": task_pages_fetched,
        },
        {
            "pages_fetched": task_pages_fetched,
            "page_cap": task_page_cap,
            "stability_window": settings.task_page_stability_window,
            "stop_reason": task_inventory_stop_reason,
            "project_candidate_counts": project_candidate_counts,
        },
    )
    spider_coverage, selected_spider_ids = _build_spider_coverage(
        task_inventory,
        settings,
        target_spiders,
        target_schedules,
    )
    detail_tasks, log_tasks = _plan_detail_and_log_sampling(
        inventory_tasks,
        spider_coverage,
        selected_spider_ids,
        settings,
    )

    log_samples: list[dict[str, Any]] = []
    log_page_samples: list[dict[str, Any]] = []
    normalized_tasks_by_id = {
        item["task_id"]: {
            "task_id": item["task_id"],
            "status": item["raw_task"].get("status"),
            "project_id": item["project_id"],
            "spider_id": item["spider_id"],
            "node_id": _first_present_id(item["raw_task"], NODE_KEYS),
            "observation": item["observation"],
            "trigger_kind": _trigger_kind_from_observation(item["observation"]),
            "log_sampled": False,
        }
        for item in inventory_tasks
    }
    seen_node_ids: set[str] = set()
    chosen_case_types: set[str] = set()
    log_task_ids = {item["task_id"] for item in log_tasks}
    log_page_samples_by_task_id: dict[str, dict[str, Any]] = {}

    for planned_task in detail_tasks:
        task_id = planned_task["task_id"]

        detail_response = _safe_get(
            transport,
            api_map,
            open_questions,
            f"/api/tasks/{task_id}",
        )
        store.write_json(
            f"raw/tasks/detail-{task_id}.json",
            detail_response.json_data if detail_response else {"status": "unknown"},
        )
        detail_task = _extract_record(detail_response.json_data if detail_response else {}) or planned_task[
            "raw_task"
        ]

        log_result = None
        log_page_sample = None
        if task_id in log_task_ids:
            log_page_sample, log_result = _sample_log_pages(
                transport,
                api_map,
                open_questions,
                store,
                task_id,
                settings.log_page_size,
                settings.max_log_pages_per_task,
            )
            log_page_sample["project_id"] = planned_task["project_id"]
            log_page_samples.append(log_page_sample)
            log_page_samples_by_task_id[task_id] = log_page_sample

            if (
                log_page_sample["pagination"]["status"] == "known"
                and log_page_sample["pagination"]["total_pages"] != 1
            ):
                _mark_spider_long_log_candidate(
                    task_inventory,
                    spider_coverage,
                    planned_task["project_id"],
                    planned_task["spider_id"],
                )

            first_page = log_page_sample["pages"][0] if log_page_sample["pages"] else None
            if (
                log_page_sample["strategy"] == "unknown pagination"
                and first_page is not None
                and (
                    first_page["line_count"] >= settings.log_page_size
                    or planned_task["has_long_log_candidate"]
                )
            ):
                open_questions.append(
                    f"Long log candidate for task `{task_id}` had unknown pagination; only the first page was sampled."
                )

        node_id = _first_present_id(detail_task, NODE_KEYS)
        if node_id and node_id not in seen_node_ids:
            seen_node_ids.add(node_id)
            node_response = _safe_get(
                transport,
                api_map,
                open_questions,
                f"/api/nodes/{node_id}",
            )
            store.write_json(
                f"raw/nodes/detail-{node_id}.json",
                _redact_sensitive_node_fields(node_response.json_data)
                if node_response
                else {"status": "unknown"},
            )
            node_detail = _extract_record(node_response.json_data if node_response else {})
            if node_detail:
                node_lookup[node_id] = node_detail

        observation = build_observation_unit(
            detail_task,
            _spider_context(detail_task, spider_lookup),
        )
        normalized_tasks_by_id[task_id] = {
            "task_id": task_id,
            "status": detail_task.get("status"),
            "project_id": _first_matching_id(detail_task, PROJECT_KEYS, target_project_ids),
            "spider_id": _first_present_id(detail_task, SPIDER_KEYS),
            "node_id": node_id,
            "observation": observation,
            "trigger_kind": _trigger_kind_from_observation(observation),
            "log_sampled": task_id in log_task_ids,
        }
        if log_result is None:
            continue

        case_type = _select_case_type(
            {"status": detail_task.get("status"), "observation": observation},
            log_result,
        )
        if case_type in chosen_case_types:
            continue

        log_samples.append(
            {
                "case_type": case_type,
                "task_id": task_id,
                "signals": log_result["signals"],
                "tail": log_result["tail"],
                "stable_fragments": log_result["stable_fragments"],
                "unstable_fragments": log_result["unstable_fragments"],
                "sampling_strategy": log_page_sample["strategy"],
                "sampled_pages": [page["page"] for page in log_page_sample["pages"]],
                "pagination": log_page_sample["pagination"],
            }
        )
        chosen_case_types.add(case_type)

    for case_type in REPRESENTATIVE_CASE_TYPES:
        if case_type not in chosen_case_types:
            open_questions.append(f"Representative case type not observed: `{case_type}`.")

    log_sampling_summary = []
    sampled_log_counts_by_project = {project_id: 0 for project_id in settings.project_ids}
    for item in log_page_samples:
        project_id = item.get("project_id")
        if project_id in sampled_log_counts_by_project:
            sampled_log_counts_by_project[project_id] += 1
    total_sampled_log_tasks = sum(sampled_log_counts_by_project.values())

    for project_id in settings.project_ids:
        sampled_tasks = sampled_log_counts_by_project[project_id]
        quota = settings.max_log_tasks_per_project
        candidate_count = project_candidate_counts.get(project_id, 0)
        shortfall_reason = _log_sampling_shortfall_reason(
            sampled_tasks,
            quota,
            candidate_count,
            total_sampled_log_tasks,
            settings.max_log_tasks_total,
            task_inventory_stop_reason,
        )
        log_sampling_summary.append(
            {
                "project_id": project_id,
                "sampled_tasks": sampled_tasks,
                "quota": quota,
                "shortfall_reason": shortfall_reason,
            }
        )
        if shortfall_reason:
            open_questions.append(
                f"Project `{project_id}` sampled `{sampled_tasks}/{quota}` log tasks because {shortfall_reason}."
            )

        for spider in spider_coverage["projects"].get(project_id, {}).get("spiders", []):
            if spider["recent_task_ids"]:
                continue
            open_questions.append(
                f"Spider `{spider['spider_id']}` was observed in coverage but had no task candidates in sampled task pages."
            )

    wheel_targets = wheel_targets or [
        {
            "wheel_path": "crawlib/crawllib-2.12.3-py3-none-any.whl",
            "internal_path": "crawllib/crawlab.py",
        },
        {
            "wheel_path": "crawlib/cdek_crawlab_module-2.2.1-py3-none-any.whl",
            "internal_path": "cdek_crawlab_module/spider_manager.py",
        },
    ]
    library_observations = inspect_wheel_sources(wheel_targets)

    parameter_records = []
    normalized_tasks = [
        normalized_tasks_by_id[item["task_id"]] for item in inventory_tasks
    ]
    for item in normalized_tasks:
        for parameter in item["observation"]["normalized_params"]["parameters"]:
            parameter_records.append(parameter)

    entity_summary = {
        "projects": {"count": len(target_projects), "fields": _entity_fields(target_projects)},
        "spiders": {"count": len(target_spiders), "fields": _entity_fields(target_spiders)},
        "schedules": {"count": len(target_schedules), "fields": _entity_fields(target_schedules)},
        "tasks": {"count": len(normalized_tasks), "fields": _entity_fields(normalized_tasks)},
        "nodes": {"count": len(node_lookup), "fields": _entity_fields(list(node_lookup.values()))},
    }

    store.write_json("normalized/projects.json", target_projects)
    store.write_json("normalized/spiders.json", target_spiders)
    store.write_json("normalized/schedules.json", target_schedules)
    store.write_json("normalized/tasks.json", normalized_tasks)
    store.write_json("normalized/task-inventory.json", task_inventory)
    store.write_json("normalized/spider-coverage.json", spider_coverage)
    store.write_json("normalized/log-page-samples.json", log_page_samples)
    store.write_json("normalized/log-samples.json", log_samples)
    store.write_json("normalized/library-parameter-observations.json", library_observations)

    bundle = {
        "api_map": api_map,
        "entity_summary": entity_summary,
        "parameter_records": parameter_records,
        "library_observations": library_observations,
        "log_samples": log_samples,
        "log_page_samples": log_page_samples,
        "log_sampling_strategy": [
            {
                "project_id": item.get("project_id"),
                "task_id": item["task_id"],
                "strategy": item["strategy"],
                "pages": [page["page"] for page in item["pages"]],
                "pagination": item["pagination"],
            }
            for item in log_page_samples
        ],
        "log_sampling_summary": log_sampling_summary,
        "open_questions": open_questions,
        "task_inventory": task_inventory,
        "spider_coverage": spider_coverage,
    }
    for relative_path, content in render_report_documents(bundle).items():
        store.write_text(relative_path, content)
    return bundle
