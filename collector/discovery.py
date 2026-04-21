from collections import defaultdict
from typing import Any

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


def run_discovery(settings, transport, store, wheel_targets=None):
    open_questions: list[str] = []
    api_map: list[dict[str, Any]] = []
    target_project_ids = set(settings.project_ids)

    target_projects: list[dict[str, Any]] = []
    found_project_ids: set[str] = set()
    for page in range(1, settings.max_pages + 1):
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
        for project in projects:
            project_id = _record_id(project)
            if project_id in target_project_ids and project_id not in found_project_ids:
                target_projects.append(project)
                found_project_ids.add(project_id)

        if found_project_ids == target_project_ids:
            break

    missing_project_ids = [
        project_id for project_id in settings.project_ids if project_id not in found_project_ids
    ]
    if missing_project_ids:
        open_questions.append(
            "Requested projects not observed after "
            f"{settings.max_pages} `/api/projects` pages: "
            + ", ".join(f"`{project_id}`" for project_id in missing_project_ids)
            + "."
        )

    spiders_response = _safe_get(
        transport,
        api_map,
        open_questions,
        "/api/spiders",
        {"page": 1, "page_size": settings.page_size},
    )
    store.write_json(
        "raw/spiders/page-1.json",
        spiders_response.json_data if spiders_response else {"status": "unknown"},
    )
    spiders = _extract_records(spiders_response.json_data if spiders_response else {})
    target_spiders = [
        spider
        for spider in spiders
        if _first_matching_id(spider, PROJECT_KEYS, target_project_ids)
    ]
    spider_lookup = {
        _record_id(spider): spider for spider in target_spiders if _record_id(spider) is not None
    }

    schedules_response = _safe_get(
        transport,
        api_map,
        open_questions,
        "/api/schedules",
        {"page": 1, "page_size": settings.page_size},
    )
    store.write_json(
        "raw/schedules/page-1.json",
        schedules_response.json_data if schedules_response else {"status": "unknown"},
    )
    schedules = _extract_records(schedules_response.json_data if schedules_response else {})
    target_spider_ids = set(spider_lookup.keys())
    target_schedules = [
        schedule
        for schedule in schedules
        if _first_matching_id(schedule, SPIDER_KEYS, target_spider_ids)
        or _first_matching_id(schedule, PROJECT_KEYS, target_project_ids)
    ]

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
    task_counts_by_spider: defaultdict[str, int] = defaultdict(int)
    important_spider_ids = {
        _first_matching_id(schedule, SPIDER_KEYS, target_spider_ids)
        for schedule in target_schedules
    }
    important_spider_ids.discard(None)

    for page in range(1, settings.max_pages + 1):
        size = settings.page_size if page == 1 else settings.expanded_task_limit
        tasks_response = _safe_get(
            transport,
            api_map,
            open_questions,
            "/api/tasks",
            {"page": page, "size": size},
        )
        store.write_json(
            f"raw/tasks/page-{page}.json",
            tasks_response.json_data if tasks_response else {"status": "unknown"},
        )

        tasks = _extract_records(tasks_response.json_data if tasks_response else {})
        target_tasks = [
            task
            for task in tasks
            if _first_matching_id(task, PROJECT_KEYS, target_project_ids)
            or _first_matching_id(task, SPIDER_KEYS, target_spider_ids)
        ]

        for task in target_tasks:
            collected_tasks.append(task)
            spider_id = _first_present_id(task, SPIDER_KEYS)
            if spider_id:
                task_counts_by_spider[spider_id] += 1
                important_spider_ids.add(spider_id)

        if important_spider_ids and all(
            task_counts_by_spider[spider_id] >= min(settings.expanded_task_limit, 10)
            for spider_id in important_spider_ids
        ):
            break

    failure_candidates: list[dict[str, Any]] = []
    representative_candidates: list[dict[str, Any]] = []
    finished_candidates: list[dict[str, Any]] = []
    chosen_slots: set[str] = set()
    for task in collected_tasks:
        status = str(task.get("status", "")).lower()
        observation = build_observation_unit(
            task,
            _spider_context(task, spider_lookup),
        )
        param_keys = {
            item["normalized_key"] for item in observation["normalized_params"]["parameters"]
        }

        slot = None
        if status in {"running", "pending"} and "long-running" not in chosen_slots:
            slot = "long-running"
        elif "spi" in param_keys and "manual rerun candidate" not in chosen_slots:
            slot = "manual rerun candidate"

        task["observation"] = observation
        if status in {"finished", "success"}:
            finished_candidates.append(task)
        elif status in FAILURE_STATUSES:
            failure_candidates.append(task)
        elif slot is not None:
            representative_candidates.append(task)
            chosen_slots.add(slot)

    log_samples: list[dict[str, Any]] = []
    normalized_tasks: list[dict[str, Any]] = []
    seen_node_ids: set[str] = set()
    chosen_case_types: set[str] = set()

    for task in failure_candidates + representative_candidates + finished_candidates:
        task_id = _record_id(task)
        if task_id is None:
            continue

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
        detail_task = _extract_record(detail_response.json_data if detail_response else {}) or task

        log_response = _safe_get(
            transport,
            api_map,
            open_questions,
            f"/api/tasks/{task_id}/logs",
            {"page": 1, "size": settings.log_page_size},
        )
        store.write_json(
            f"raw/tasks/logs-{task_id}.json",
            log_response.json_data if log_response else {"status": "unknown"},
        )
        log_result = analyze_log_text(
            _coerce_log_payload(log_response.json_data if log_response else {})
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
        trigger_kind = (
            {"kind": "schedule", "status": "fact"}
            if observation["schedule_id"] != "unscheduled"
            else {"kind": "manual-or-unknown", "status": "unknown"}
        )

        case_type = _select_case_type(
            {"status": detail_task.get("status"), "observation": observation},
            log_result,
        )
        normalized_tasks.append(
            {
                "task_id": task_id,
                "status": detail_task.get("status"),
                "project_id": _first_matching_id(detail_task, PROJECT_KEYS, target_project_ids),
                "spider_id": _first_present_id(detail_task, SPIDER_KEYS),
                "node_id": node_id,
                "observation": observation,
                "trigger_kind": trigger_kind,
            }
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
            }
        )
        chosen_case_types.add(case_type)

    for case_type in REPRESENTATIVE_CASE_TYPES:
        if case_type not in chosen_case_types:
            open_questions.append(f"Representative case type not observed: `{case_type}`.")

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
    store.write_json("normalized/log-samples.json", log_samples)
    store.write_json("normalized/library-parameter-observations.json", library_observations)

    bundle = {
        "api_map": api_map,
        "entity_summary": entity_summary,
        "parameter_records": parameter_records,
        "library_observations": library_observations,
        "log_samples": log_samples,
        "open_questions": open_questions,
    }
    for relative_path, content in render_report_documents(bundle).items():
        store.write_text(relative_path, content)
    return bundle
