"""Fixture collector CLI — collects and redacts Crawlab API data.

Modes:
    dry-run   — connect, discover, show sampling plan, exit
    collect   — full collection: fetch, redact, write, generate manifests
    refresh   — re-collect only tasks that changed since last run

Safety (AGENTS.md § Safety, § Fixture collector rules):
    - Only GET requests through ReadonlyCrawlabClient
    - Raw data → fixtures_raw_local/ (gitignored)
    - Redacted data → fixtures/ (committed)
    - Token from env only, never stored

Discovery:
    Uses condition-based queries via Crawlab's conditions=[...] param
    instead of generic task listing.  Each status gets a targeted query.

Scoping:
    allowed_project_ids in config limits collection to specific projects.
    Spider details are hydrated to filter by project_id.

Usage:
    python -m tools.collect_fixtures --dry-run
    python -m tools.collect_fixtures --collect --max-examples-per-class 2
    python -m tools.collect_fixtures --refresh --skip-existing
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from integrations.crawlab.readonly_client import (
    ZERO_OBJECT_ID,
    ReadonlyCrawlabClient,
)
from tools.classify_logs import (
    CandidateClass,
    FinalLogClass,
    LogClassification,
    classify_candidate,
    classify_final,
    classify_log_text,
    generate_expected_yaml,
    generate_manifest_entry,
    is_manual_run,
)
from tools.redact import Redactor, RedactionConfig

logger = logging.getLogger(__name__)

# ── Defaults ───────────────────────────────────────────────────────────
DEFAULT_CONFIG_PATH = Path("config/user_scope.yml")
DEFAULT_RAW_DIR = Path("fixtures_raw_local")
DEFAULT_OUT_DIR = Path("fixtures")
DEFAULT_MAPPING_FILE = "redaction_map.json"

# Crawlab API uses conditions=[{key,op,value}] for filtering
TASK_STATUS_QUERIES = ["running", "pending", "finished", "error", "cancelled"]


def _load_dotenv(path: str = ".env") -> None:
    """Load .env file into os.environ (does not override existing vars).

    Simple KEY=VALUE parser — avoids python-dotenv dependency.
    Ignores comments (#) and empty lines.
    """
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Remove surrounding quotes if present
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        # Don't override existing env vars
        if key not in os.environ:
            os.environ[key] = value


# ── Config loader ──────────────────────────────────────────────────────

def load_config(path: Path) -> dict[str, Any]:
    """Load and validate user_scope.yml."""
    if not path.exists():
        logger.error("Config file not found: %s", path)
        sys.exit(1)
    with open(path) as f:
        cfg = yaml.safe_load(f) or {}

    # Validate security section
    methods = cfg.get("security", {}).get("allowed_methods", [])
    if methods and methods != ["GET"]:
        logger.error("Only GET method is allowed. Found: %s", methods)
        sys.exit(1)

    return cfg


def _get_collection_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Extract collection parameters with defaults."""
    coll = cfg.get("collection", {})
    return {
        "task_page_size": coll.get("task_page_size", 100),
        "max_task_pages": coll.get("max_task_pages", 5),
        "log_page_size": coll.get("log_page_size", 1000),
        "max_log_pages": coll.get("max_log_pages", 4),
        "max_examples_per_class": coll.get("max_examples_per_class", 2),
        "results_row_limit": coll.get("results_row_limit", 10),
        "collect_results_for_sampled_tasks_only": coll.get(
            "collect_results_for_sampled_tasks_only", True
        ),
        "collect_raw": coll.get("collect_raw", True),
        "collect_redacted": coll.get("collect_redacted", True),
        "generate_expected_skeletons": coll.get("generate_expected_skeletons", True),
    }


def _get_scope_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Extract scope parameters."""
    scope = cfg.get("scope", {})
    return {
        "allowed_project_ids": scope.get("allowed_project_ids", []),
        "seed_spider_ids": scope.get("seed_spider_ids", []),
        "seed_schedule_ids": scope.get("seed_schedule_ids", []),
        "discover_from_tasks": scope.get("discover_from_tasks", True),
        "discover_from_schedules": scope.get("discover_from_schedules", False),
    }


# ── Condition-based query builder ──────────────────────────────────────

def _build_conditions(*conditions: dict[str, str]) -> str:
    """Build Crawlab conditions JSON string.

    Crawlab API uses: ?conditions=[{"key":"status","op":"eq","value":"running"}]
    """
    return json.dumps(list(conditions), separators=(",", ":"))


# ── Discovery ──────────────────────────────────────────────────────────

async def discover_candidates_by_status(
    client: ReadonlyCrawlabClient,
    status: str,
    page_size: int,
    max_pages: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch tasks with a specific status using condition-based query."""
    logger.info("Discovering %s tasks...", status)
    conditions = _build_conditions({"key": "status", "op": "eq", "value": status})
    tasks, meta = await client.get_paginated(
        "/api/tasks",
        page_size=page_size,
        max_pages=max_pages,
        conditions=conditions,
        stats="true",
    )
    logger.info("Discovered %d %s tasks", len(tasks), status)
    return tasks, meta


async def discover_manual_tasks(
    client: ReadonlyCrawlabClient,
    page_size: int,
    max_pages: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch manual tasks (schedule_id == zero ObjectId)."""
    logger.info("Discovering manual tasks...")
    conditions = _build_conditions(
        {"key": "schedule_id", "op": "eq", "value": ZERO_OBJECT_ID}
    )
    tasks, meta = await client.get_paginated(
        "/api/tasks",
        page_size=page_size,
        max_pages=max_pages,
        conditions=conditions,
        stats="true",
    )
    logger.info("Discovered %d manual tasks", len(tasks))
    return tasks, meta


async def discover_all_candidates(
    client: ReadonlyCrawlabClient,
    page_size: int,
    max_pages: int,
) -> dict[str, tuple[list[dict[str, Any]], dict[str, Any]]]:
    """Run targeted queries per status + manual detection.

    Returns dict mapping query label → (task list, metadata).
    """
    candidates: dict[str, tuple[list[dict[str, Any]], dict[str, Any]]] = {}

    for status in TASK_STATUS_QUERIES:
        tasks, meta = await discover_candidates_by_status(
            client, status, page_size, max_pages,
        )
        candidates[status] = (tasks, meta)

    # Manual run detection
    manual, meta = await discover_manual_tasks(client, page_size, max_pages=1)
    candidates["manual"] = (manual, meta)

    return candidates


async def discover_schedules(client: ReadonlyCrawlabClient) -> list[dict[str, Any]]:
    """Fetch all schedules."""
    logger.info("Discovering schedules...")
    data = await client.get_json("/api/schedules")
    schedules = data.get("data", []) if isinstance(data, dict) else []
    logger.info("Discovered %d schedules", len(schedules))
    return schedules


async def fetch_spider(
    client: ReadonlyCrawlabClient, spider_id: str,
) -> dict[str, Any] | None:
    """Fetch a single spider by ID."""
    if spider_id == ZERO_OBJECT_ID:
        return None
    try:
        data = await client.get_json(f"/api/spiders/{spider_id}")
        return data.get("data", data) if isinstance(data, dict) else data
    except Exception as e:
        logger.warning("Failed to fetch spider %s: %s", spider_id, e)
        return None


async def fetch_task_logs(
    client: ReadonlyCrawlabClient,
    task_id: str,
    page_size: int,
    max_pages: int,
) -> str:
    """Fetch log text for a task, paginated."""
    try:
        data = await client.get_json(
            f"/api/tasks/{task_id}/logs",
            page=1,
            size=page_size,
        )
        # Crawlab log response may vary; try common shapes
        if isinstance(data, dict):
            items = data.get("data", [])
            if isinstance(items, list):
                # Collect log lines across pages
                all_lines = [_extract_log_line(item) for item in items]
                total = data.get("total", 0)

                for page_num in range(2, max_pages + 1):
                    if (page_num - 1) * page_size >= total:
                        break
                    page_data = await client.get_json(
                        f"/api/tasks/{task_id}/logs",
                        page=page_num,
                        size=page_size,
                    )
                    page_items = page_data.get("data", [])
                    if not page_items:
                        break
                    all_lines.extend(_extract_log_line(item) for item in page_items)

                return "\n".join(all_lines)
            elif isinstance(items, str):
                return items
        elif isinstance(data, str):
            return data
        return str(data)
    except Exception as e:
        logger.warning("Failed to fetch logs for task %s: %s", task_id, e)
        return ""


def _extract_log_line(item: Any) -> str:
    """Extract a log line from a log response item."""
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        return item.get("msg", item.get("message", str(item)))
    return str(item)


async def fetch_results(
    client: ReadonlyCrawlabClient,
    collection_id: str,
    task_id: str,
    row_limit: int,
) -> list[dict[str, Any]]:
    """Fetch result rows for a task from a collection."""
    try:
        data = await client.get_json(
            f"/api/results/{collection_id}",
            page=1,
            size=row_limit,
            _tid=task_id,
        )
        return data.get("data", []) if isinstance(data, dict) else []
    except Exception as e:
        logger.warning(
            "Failed to fetch results for collection %s, task %s: %s",
            collection_id, task_id, e,
        )
        return []


# ── Project scoping ────────────────────────────────────────────────────

async def hydrate_spider_project_ids(
    client: ReadonlyCrawlabClient,
    spider_ids: set[str],
) -> dict[str, dict[str, Any]]:
    """Fetch spider details and return spider_id → spider dict."""
    spiders: dict[str, dict[str, Any]] = {}
    for sid in sorted(spider_ids):
        spider = await fetch_spider(client, sid)
        if spider:
            spiders[sid] = spider
    return spiders


def filter_tasks_by_project(
    tasks: list[dict[str, Any]],
    spiders: dict[str, dict[str, Any]],
    allowed_project_ids: list[str],
) -> list[dict[str, Any]]:
    """Filter tasks to only those whose spider belongs to allowed projects."""
    if not allowed_project_ids:
        return tasks
    allowed = set(allowed_project_ids)
    result = []
    for task in tasks:
        spider_id = task.get("spider_id", "")
        spider = spiders.get(spider_id, {})
        project_id = spider.get("project_id", "")
        if project_id in allowed:
            result.append(task)
    return result


def compute_project_histogram(
    tasks: list[dict[str, Any]],
    spiders: dict[str, dict[str, Any]],
) -> dict[str, int]:
    """Count tasks per project_id."""
    histogram: dict[str, int] = {}
    for task in tasks:
        spider_id = task.get("spider_id", "")
        spider = spiders.get(spider_id, {})
        project_id = spider.get("project_id", "unknown")
        histogram[project_id] = histogram.get(project_id, 0) + 1
    return histogram


# ── Sampling ───────────────────────────────────────────────────────────

def build_execution_key(task: dict[str, Any]) -> str:
    """Build execution_key from spider_id + normalized cmd + normalized param.

    AGENTS.md § Domain rules: execution_key = spider_id + normalized cmd + normalized param.
    """
    spider_id = task.get("spider_id", "")
    cmd = (task.get("cmd", "") or "").strip()
    param = (task.get("param", "") or "").strip()
    return f"{spider_id}|{cmd}|{param}"


def sample_candidates(
    candidates: dict[str, list[dict[str, Any]]],
    max_per_class: int,
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    """Select representative tasks from candidate groups.

    Returns:
        Tuple of (sampled_tasks, selected_ids_per_class)
    """
    sampled: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    selected_ids: dict[str, list[str]] = {}

    for class_key, tasks in candidates.items():
        # Sort by created_at descending (most recent first)
        sorted_tasks = sorted(
            tasks,
            key=lambda t: t.get("create_ts", t.get("created_at", "")),
            reverse=True,
        )
        selected = []
        for task in sorted_tasks:
            if len(selected) >= max_per_class:
                break
            tid = task.get("_id", "")
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                selected.append(task)
                sampled.append(task)

        selected_ids[class_key] = [t.get("_id", "") for t in selected]

    return sampled, selected_ids


# ── Schedule history sampling ──────────────────────────────────────────

def _get_runtime_ms(task: dict[str, Any]) -> int | None:
    """Extract runtime_duration in milliseconds from task stat."""
    stat = task.get("stat", {})
    if isinstance(stat, dict):
        rt = stat.get("runtime_duration", 0)
        if isinstance(rt, (int, float)) and rt > 0:
            return int(rt)
    return None


def find_long_running_schedule(
    all_tasks: list[dict[str, Any]],
    schedules: list[dict[str, Any]],
    spiders: dict[str, dict[str, Any]],
    allowed_project_ids: list[str],
    min_history_tasks: int = 2,
) -> tuple[str | None, dict[str, Any]]:
    """Find a schedule with highest median runtime from allowed scope.

    Prefers:
    1. Schedule from allowed scope
    2. Among those, one with highest observed runtime median
    3. Bonus preference for schedule with active running chain

    Returns:
        (schedule_id, selection_info_dict) or (None, {})
    """
    allowed = set(allowed_project_ids) if allowed_project_ids else None

    # Build schedule → tasks mapping
    schedule_tasks: dict[str, list[dict[str, Any]]] = {}
    for task in all_tasks:
        sid = task.get("schedule_id", "")
        if sid and sid != ZERO_OBJECT_ID:
            schedule_tasks.setdefault(sid, []).append(task)

    if not schedule_tasks:
        return None, {}

    # Filter to allowed scope if set
    if allowed:
        in_scope_schedules: set[str] = set()
        for sched in schedules:
            sched_id = sched.get("_id", "")
            spider_id = sched.get("spider_id", "")
            spider = spiders.get(spider_id, {})
            if spider.get("project_id", "") in allowed:
                in_scope_schedules.add(sched_id)

        # Also check by spider_id on tasks
        for sid, tasks in list(schedule_tasks.items()):
            if sid not in in_scope_schedules:
                # Check if any task's spider is in scope
                task_spider = tasks[0].get("spider_id", "") if tasks else ""
                spider = spiders.get(task_spider, {})
                if spider.get("project_id", "") not in allowed:
                    del schedule_tasks[sid]

    if not schedule_tasks:
        return None, {}

    # Compute median runtime per schedule
    best_id: str | None = None
    best_median: float = 0.0
    best_has_running: bool = False
    best_task_count: int = 0

    fallback_id: str | None = None
    fallback_median: float = 0.0
    fallback_has_running: bool = False
    fallback_task_count: int = 0

    for sid, tasks in schedule_tasks.items():
        runtimes = [r for t in tasks if (r := _get_runtime_ms(t)) is not None]
        has_running = any(
            (t.get("status", "") or "").lower() == "running" for t in tasks
        )
        task_count = len(tasks)

        if not runtimes:
            continue

        median_rt = statistics.median(runtimes)
        is_candidate = task_count >= min_history_tasks

        if is_candidate:
            if best_id is None:
                best_id = sid
                best_median = median_rt
                best_has_running = has_running
                best_task_count = task_count
            elif has_running and not best_has_running:
                best_id = sid
                best_median = median_rt
                best_has_running = has_running
                best_task_count = task_count
            elif has_running == best_has_running and median_rt > best_median:
                best_id = sid
                best_median = median_rt
                best_has_running = has_running
                best_task_count = task_count
        else:
            if fallback_id is None:
                fallback_id = sid
                fallback_median = median_rt
                fallback_has_running = has_running
                fallback_task_count = task_count
            elif has_running and not fallback_has_running:
                fallback_id = sid
                fallback_median = median_rt
                fallback_has_running = has_running
                fallback_task_count = task_count
            elif has_running == fallback_has_running and median_rt > fallback_median:
                fallback_id = sid
                fallback_median = median_rt
                fallback_has_running = has_running
                fallback_task_count = task_count

    if best_id is not None:
        reason_parts = [f"median runtime {best_median/1000:.1f}s"]
        if best_has_running:
            reason_parts.append("active running chain")
        
        return best_id, {
            "schedule_id": best_id,
            "reason": ", ".join(reason_parts),
            "median_runtime_ms": best_median,
            "task_count": best_task_count,
            "has_running": best_has_running,
            "is_fallback": False,
        }
    elif fallback_id is not None:
        reason_parts = [f"median runtime {fallback_median/1000:.1f}s"]
        if fallback_has_running:
            reason_parts.append("active running chain")
        reason_parts.append("fallback_long_running_candidate")
        
        return fallback_id, {
            "schedule_id": fallback_id,
            "reason": ", ".join(reason_parts),
            "median_runtime_ms": fallback_median,
            "task_count": fallback_task_count,
            "has_running": fallback_has_running,
            "is_fallback": True,
        }

    return None, {}


# ── Writer ─────────────────────────────────────────────────────────────

def write_raw_fixture(
    data: Any,
    raw_dir: Path,
    category: str,
    filename: str,
) -> Path:
    """Write raw (unredacted) fixture to fixtures_raw_local/."""
    out_dir = raw_dir / category
    out_dir.mkdir(parents=True, exist_ok=True)
    filepath = out_dir / filename

    if isinstance(data, str):
        filepath.write_text(data, encoding="utf-8")
    else:
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    return filepath


def write_redacted_fixture(
    data: Any,
    out_dir: Path,
    category: str,
    filename: str,
) -> Path:
    """Write redacted fixture to fixtures/."""
    target_dir = out_dir / category
    target_dir.mkdir(parents=True, exist_ok=True)
    filepath = target_dir / filename

    if isinstance(data, str):
        filepath.write_text(data, encoding="utf-8")
    else:
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    return filepath


# ── Manifest generation ────────────────────────────────────────────────

def generate_manifest(
    entries: list[dict[str, Any]],
    out_dir: Path,
    schedules_count: int,
    spiders_count: int,
) -> Path:
    """Generate fixtures/manifest.md with fixture inventory."""
    filepath = out_dir / "manifest.md"

    lines = [
        "# Fixtures Manifest",
        "",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "",
        "## Summary",
        "",
        f"- **Total sampled tasks**: {len(entries)}",
        f"- **Schedules collected**: {schedules_count}",
        f"- **Spiders collected**: {spiders_count}",
        "",
        "## Task Fixtures",
        "",
        "| Task ID | Candidate | Final | Trigger | Log Lines | Errors | Files |",
        "|---------|-----------|-------|---------|-----------|--------|-------|",
    ]

    for entry in entries:
        files = ", ".join(
            f"`{k}`: {v}" for k, v in entry.get("files", {}).items()
        )
        lines.append(
            f"| {entry.get('task_id', '?')} "
            f"| {entry.get('candidate_class', '?')} "
            f"| {entry.get('final_class', '?')} "
            f"| {entry.get('trigger', '?')} "
            f"| {entry.get('total_log_lines', '-')} "
            f"| {entry.get('error_lines', '-')} "
            f"| {files} |"
        )

    lines.extend([
        "",
        "## Notes",
        "",
        "- All IDs are redacted with stable placeholders.",
        "- Zero ObjectId (`000000000000000000000000`) is preserved.",
        "- Zero time (`0001-01-01T00:00:00Z`) is preserved.",
        "- Real timestamps are preserved (v1 decision).",
        "- Files in `expected/` are draft skeletons — verify before use.",
        "",
    ])

    filepath.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Manifest written to %s", filepath)
    return filepath


# ── Estimation helpers ─────────────────────────────────────────────────

def estimate_api_calls(
    sampled_count: int,
    spider_count: int,
    coll: dict[str, Any],
    *,
    has_long_running: bool = False,
    long_running_extra: int = 0,
) -> dict[str, int]:
    """Estimate API call counts for collection phase."""
    calls = {
        "status_queries": len(TASK_STATUS_QUERIES),
        "manual_query": 1,
        "schedules_query": 1,
        "spider_hydration": spider_count,
        "log_fetches": sampled_count + long_running_extra,
        "result_fetches": sampled_count if coll.get("collect_results_for_sampled_tasks_only") else 0,
    }
    calls["total"] = sum(calls.values())
    return calls


def estimate_files(
    sampled_count: int,
    spider_count: int,
    coll: dict[str, Any],
) -> dict[str, int]:
    """Estimate file counts for collection phase."""
    files = {}
    # Each sampled task: task JSON + log + possibly results + possibly expected YAML
    per_task = 1  # task JSON
    per_task += 1  # log
    if coll.get("collect_results_for_sampled_tasks_only"):
        per_task += 1  # results (estimate)
    if coll.get("generate_expected_skeletons"):
        per_task += 1  # expected YAML

    if coll.get("collect_raw"):
        files["raw"] = sampled_count * per_task + spider_count + 1  # +1 for schedules
    if coll.get("collect_redacted"):
        files["redacted"] = sampled_count * per_task + spider_count + 1

    files["total"] = sum(files.values())
    return files


# ── Dry-run ────────────────────────────────────────────────────────────

async def run_dry_run(
    client: ReadonlyCrawlabClient,
    cfg: dict[str, Any],
    raw_dir: Path,
) -> None:
    """Dry-run mode: connect, discover, show plan, save plan, exit."""
    coll = _get_collection_config(cfg)
    scope = _get_scope_config(cfg)
    allowed_project_ids = scope["allowed_project_ids"]

    print("\n=== FIXTURE COLLECTOR — DRY RUN ===\n")

    # 1. Condition-based discovery
    print("─── Condition-based discovery ───")
    candidates_with_meta = await discover_all_candidates(
        client, coll["task_page_size"], max_pages=1,
    )
    candidates = {k: v[0] for k, v in candidates_with_meta.items()}
    candidates_meta = {k: v[1] for k, v in candidates_with_meta.items()}

    # 2. Discover schedules
    schedules = await discover_schedules(client)

    # 3. Collect all unique spider IDs from ALL discovered tasks
    all_tasks_flat: list[dict[str, Any]] = []
    for tasks in candidates.values():
        all_tasks_flat.extend(tasks)

    spider_ids = {
        t.get("spider_id", "") for t in all_tasks_flat
    } - {ZERO_OBJECT_ID, ""}

    # 4. Hydrate spiders for project scoping
    print(f"\nHydrating {len(spider_ids)} spider(s) for project scoping...")
    spiders = await hydrate_spider_project_ids(client, spider_ids)

    # 5. Compute project histogram
    project_histogram = compute_project_histogram(all_tasks_flat, spiders)

    # 6. Filter candidates by allowed projects
    scope_warning = None
    filtered_candidates: dict[str, list[dict[str, Any]]] = {}
    if allowed_project_ids:
        for key, tasks in candidates.items():
            filtered = filter_tasks_by_project(tasks, spiders, allowed_project_ids)
            filtered_candidates[key] = filtered
    else:
        scope_warning = (
            f"No project filter set. Collection would cover "
            f"{len(project_histogram)} project(s) with "
            f"{len(all_tasks_flat)} total tasks. "
            f"Set scope.allowed_project_ids in config to limit scope."
        )
        filtered_candidates = candidates

    # 7. Count manual vs scheduled across filtered candidates
    all_filtered: list[dict[str, Any]] = []
    seen_filtered_ids: set[str] = set()
    for tasks in filtered_candidates.values():
        for t in tasks:
            tid = t.get("_id", "")
            if tid not in seen_filtered_ids:
                seen_filtered_ids.add(tid)
                all_filtered.append(t)

    manual_count = sum(1 for t in all_filtered if is_manual_run(t))
    scheduled_count = len(all_filtered) - manual_count

    # 8. Sample candidates
    _sampled, selected_ids = sample_candidates(
        filtered_candidates, coll["max_examples_per_class"],
    )

    # 9. Long-running schedule selection
    long_sched_id, long_sched_info = find_long_running_schedule(
        all_filtered, schedules, spiders, allowed_project_ids,
    )

    # 10. Filtered spider count
    filtered_spider_ids = {
        t.get("spider_id", "") for t in all_filtered
    } - {ZERO_OBJECT_ID, ""}

    # 11. Estimates
    est_calls = estimate_api_calls(
        len(_sampled), len(filtered_spider_ids), coll,
        has_long_running=bool(long_sched_id),
        long_running_extra=min(
            long_sched_info.get("task_count", 0),
            coll["max_examples_per_class"],
        ) if long_sched_id else 0,
    )
    est_files = estimate_files(len(_sampled), len(filtered_spider_ids), coll)

    # ── Console output ──────────────────────────────────────────────────

    print("\n─── Scope ───")
    print(f"  allowed_project_ids: {allowed_project_ids or '(none — ALL projects)'}")
    print(f"  project histogram:   {json.dumps(project_histogram, indent=4)}")
    if scope_warning:
        print(f"\n  ⚠ SCOPE WARNING: {scope_warning}")

    print("\n─── Candidate counts by targeted query ───")
    for key in TASK_STATUS_QUERIES + ["manual"]:
        raw_count = len(candidates.get(key, []))
        filt_count = len(filtered_candidates.get(key, []))
        selected = selected_ids.get(key, [])
        label = f"  {key}:"
        print(f"{label:<28} {raw_count:>4} found → {filt_count:>4} in scope → {len(selected)} sampled")
        if selected:
            for sid in selected:
                print(f"{'':>30} • {sid}")

    print(f"\n─── Manual vs Scheduled ───")
    print(f"  manual:    {manual_count}")
    print(f"  scheduled: {scheduled_count}")

    print(f"\n─── Long-running schedule ───")
    if long_sched_info:
        print(f"  schedule_id: {long_sched_info.get('schedule_id', '?')}")
        print(f"  reason:      {long_sched_info.get('reason', '?')}")
        print(f"  task_count:  {long_sched_info.get('task_count', '?')}")
    else:
        print("  (none found in scope)")

    print(f"\n─── Estimates ───")
    print(f"  API calls for collection: {est_calls['total']}")
    for k, v in est_calls.items():
        if k != "total":
            print(f"    {k}: {v}")
    print(f"  Files to write:           {est_files.get('total', 0)}")
    for k, v in est_files.items():
        if k != "total":
            print(f"    {k}: {v}")

    print(f"\n─── Config ───")
    print(f"  max_examples_per_class: {coll['max_examples_per_class']}")
    print(f"  max_task_pages:         {coll['max_task_pages']}")
    print(f"  max_log_pages:          {coll['max_log_pages']}")
    print(f"  results_row_limit:      {coll['results_row_limit']}")

    # ── Save JSON plan ──────────────────────────────────────────────────

    plan = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "allowed_project_ids": allowed_project_ids,
            "project_histogram": project_histogram,
            "scope_warning": scope_warning,
        },
        "candidates": {
            key: {
                "api_reported_total": candidates_meta[key].get("api_reported_total", 0),
                "pages_fetched": candidates_meta[key].get("pages_fetched", 0),
                "records_fetched": candidates_meta[key].get("records_fetched", 0),
                "in_scope_count": len(filtered_candidates.get(key, [])),
                "sample_ids": selected_ids.get(key, []),
            }
            for key in TASK_STATUS_QUERIES + ["manual"]
        },
        "manual_vs_scheduled": {
            "manual": manual_count,
            "scheduled": scheduled_count,
        },
        "long_running_schedule": long_sched_info or None,
        "estimates": {
            "api_calls": est_calls,
            "files": est_files,
        },
        "config": coll,
    }

    raw_dir.mkdir(parents=True, exist_ok=True)
    plan_path = raw_dir / "dry_run_plan.json"
    with open(plan_path, "w") as f:
        json.dump(plan, f, indent=2)

    print(f"\n─── Output ───")
    print(f"  Dry-run plan saved to: {plan_path}")
    print(f"\nTo collect, run:  python -m tools.collect_fixtures --collect")

    if scope_warning:
        print(f"\n⚠ Review scope warning above. Set allowed_project_ids before collecting.")


# ── Full collection ────────────────────────────────────────────────────

async def run_collect(
    client: ReadonlyCrawlabClient,
    cfg: dict[str, Any],
    raw_dir: Path,
    out_dir: Path,
    *,
    skip_existing: bool = False,
    skip_logs: bool = False,
    skip_results: bool = False,
) -> None:
    """Full collection mode."""
    coll = _get_collection_config(cfg)
    scope = _get_scope_config(cfg)
    allowed_project_ids = scope["allowed_project_ids"]
    redaction_cfg = RedactionConfig.from_dict(cfg.get("redaction", {}))
    redactor = Redactor(redaction_cfg)

    # Load previous mapping for incremental runs
    mapping_path = raw_dir / DEFAULT_MAPPING_FILE
    redactor.load_mapping(mapping_path)

    print("\n=== FIXTURE COLLECTOR — COLLECTING ===\n")

    # 1. Condition-based discovery
    candidates_with_meta = await discover_all_candidates(
        client, coll["task_page_size"], coll["max_task_pages"],
    )
    candidates = {k: v[0] for k, v in candidates_with_meta.items()}

    # 2. Discover schedules
    schedules = await discover_schedules(client)

    # 3. Hydrate spiders for project scoping
    all_tasks_flat: list[dict[str, Any]] = []
    for tasks in candidates.values():
        all_tasks_flat.extend(tasks)

    spider_ids = {
        t.get("spider_id", "") for t in all_tasks_flat
    } - {ZERO_OBJECT_ID, ""}

    spiders = await hydrate_spider_project_ids(client, spider_ids)

    # 4. Filter by project scope
    if not allowed_project_ids:
        print("ERROR: allowed_project_ids is empty. Run --dry-run first to see project histogram.")
        print("Set scope.allowed_project_ids in config before collecting.")
        sys.exit(1)

    filtered_candidates: dict[str, list[dict[str, Any]]] = {}
    for key, tasks in candidates.items():
        filtered_candidates[key] = filter_tasks_by_project(
            tasks, spiders, allowed_project_ids,
        )

    # 5. Sample candidates
    sampled, _selected_ids = sample_candidates(
        filtered_candidates, coll["max_examples_per_class"],
    )
    print(f"Sampled {len(sampled)} tasks from {len(filtered_candidates)} candidate classes")

    # 6. Long-running schedule history
    all_filtered: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for tasks in filtered_candidates.values():
        for t in tasks:
            tid = t.get("_id", "")
            if tid not in seen_ids:
                seen_ids.add(tid)
                all_filtered.append(t)

    long_sched_id, long_sched_info = find_long_running_schedule(
        all_filtered, schedules, spiders, allowed_project_ids,
    )
    if long_sched_id:
        sched_tasks = [
            t for t in all_filtered
            if t.get("schedule_id") == long_sched_id
        ][:coll["max_examples_per_class"]]
        for t in sched_tasks:
            if t.get("_id") not in {s.get("_id") for s in sampled}:
                sampled.append(t)
        print(f"Added {len(sched_tasks)} tasks for long-running schedule {long_sched_id}")
        print(f"  reason: {long_sched_info.get('reason', '?')}")

    # 7. Fetch logs for sampled tasks
    log_texts: dict[str, str] = {}
    if not skip_logs:
        print(f"Fetching logs for {len(sampled)} tasks...")
        for i, task in enumerate(sampled, 1):
            task_id = task.get("_id", "")
            if skip_existing and _fixture_exists(out_dir, "logs", f"{task_id}.log"):
                logger.debug("Skipping existing log for %s", task_id)
                continue
            log_text = await fetch_task_logs(
                client, task_id, coll["log_page_size"], coll["max_log_pages"],
            )
            log_texts[task_id] = log_text
            print(f"  [{i}/{len(sampled)}] Log for {task_id}: {len(log_text)} chars")

    # 8. Fetch unique spiders (only in-scope ones)
    sampled_spider_ids = {
        t.get("spider_id", "") for t in sampled
    } - {ZERO_OBJECT_ID, ""}
    sampled_spiders: dict[str, dict[str, Any]] = {}
    print(f"Fetching {len(sampled_spider_ids)} spiders...")
    for sid in sampled_spider_ids:
        if sid in spiders:
            sampled_spiders[sid] = spiders[sid]
        elif skip_existing and _fixture_exists(out_dir, "api", f"spider_{sid}.json"):
            continue
        else:
            spider = await fetch_spider(client, sid)
            if spider:
                sampled_spiders[sid] = spider

    # 9. Fetch results for sampled tasks (if enabled)
    results_data: dict[str, list[dict[str, Any]]] = {}
    if not skip_results and coll["collect_results_for_sampled_tasks_only"]:
        print("Fetching results for sampled tasks...")
        for task in sampled:
            task_id = task.get("_id", "")
            spider_id = task.get("spider_id", "")
            spider = sampled_spiders.get(spider_id, {})
            col_name = spider.get("col_name") or spider.get("col_id")
            if col_name:
                rows = await fetch_results(
                    client, col_name, task_id, coll["results_row_limit"],
                )
                if rows:
                    results_data[task_id] = rows

    # 10. Write fixtures
    print("\nWriting fixtures...")
    manifest_entries: list[dict[str, Any]] = []

    # Write schedules
    if coll["collect_raw"]:
        write_raw_fixture(schedules, raw_dir, "api", "schedules.json")
    if coll["collect_redacted"]:
        redacted_schedules = redactor.redact_json(schedules, context="schedules")
        write_redacted_fixture(redacted_schedules, out_dir, "api", "schedules.json")

    # Write spiders
    for sid, spider in sampled_spiders.items():
        if coll["collect_raw"]:
            write_raw_fixture(spider, raw_dir, "api", f"spider_{sid}.json")
        if coll["collect_redacted"]:
            redacted = redactor.redact_json(spider, context="spider")
            redacted_id = redacted.get("_id", sid)
            write_redacted_fixture(
                redacted, out_dir, "api", f"spider_{redacted_id}.json",
            )

    # Write tasks and logs
    for task in sampled:
        task_id = task.get("_id", "")
        log_text = log_texts.get(task_id, "")

        # Two-phase classification
        cand_class = classify_candidate(task)
        final_class = classify_final(task, log_text if log_text else None)
        log_classification: LogClassification | None = None
        if log_text:
            log_classification = classify_log_text(log_text, task_id)

        fixture_paths: dict[str, str] = {}

        # Write raw
        if coll["collect_raw"]:
            write_raw_fixture(task, raw_dir, "api", f"task_{task_id}.json")
            if log_text:
                write_raw_fixture(log_text, raw_dir, "logs", f"{task_id}.log")

        # Write redacted
        redacted_task_id = task_id
        if coll["collect_redacted"]:
            redacted_task = redactor.redact_json(task, context="task")
            redacted_task_id = redacted_task.get("_id", task_id)
            task_path = write_redacted_fixture(
                redacted_task, out_dir, "api", f"task_{redacted_task_id}.json",
            )
            fixture_paths["task"] = str(task_path.relative_to(out_dir))

            if log_text:
                redacted_log = redactor.redact_log_text(log_text)
                log_path = write_redacted_fixture(
                    redacted_log, out_dir, "logs", f"{redacted_task_id}.log",
                )
                fixture_paths["log"] = str(log_path.relative_to(out_dir))

        # Write results
        if task_id in results_data:
            if coll["collect_raw"]:
                write_raw_fixture(
                    results_data[task_id], raw_dir, "results",
                    f"results_{task_id}.json",
                )
            if coll["collect_redacted"]:
                redacted_results = redactor.redact_json(
                    results_data[task_id], context="results",
                )
                results_path = write_redacted_fixture(
                    redacted_results, out_dir, "results",
                    f"results_{redacted_task_id}.json",
                )
                fixture_paths["results"] = str(results_path.relative_to(out_dir))

        # Generate expected YAML
        if log_classification and coll["generate_expected_skeletons"]:
            redacted_classification = LogClassification(
                task_id=redacted_task_id if coll["collect_redacted"] else task_id,
                total_lines=log_classification.total_lines,
                classes_found=log_classification.classes_found,
                class_line_counts=log_classification.class_line_counts,
                error_lines=log_classification.error_lines,
                warning_lines=log_classification.warning_lines,
                scrapy_stats_found=log_classification.scrapy_stats_found,
                has_traceback=log_classification.has_traceback,
            )
            expected_path = generate_expected_yaml(
                redacted_classification, out_dir / "expected",
            )
            fixture_paths["expected"] = str(expected_path.relative_to(out_dir))

        # Manifest entry
        manifest_entries.append(generate_manifest_entry(
            task_id=redacted_task_id if coll["collect_redacted"] else task_id,
            final_class=final_class,
            candidate_class=cand_class,
            log_classification=log_classification,
            fixture_paths=fixture_paths,
            is_manual=is_manual_run(task),
        ))

    # 11. Generate manifest
    generate_manifest(
        manifest_entries, out_dir,
        schedules_count=len(schedules),
        spiders_count=len(sampled_spiders),
    )

    # 12. Save redaction mapping (to gitignored dir)
    redactor.save_mapping(mapping_path)

    print(f"\n✓ Collection complete!")
    print(f"  Raw fixtures:      {raw_dir}/")
    print(f"  Redacted fixtures: {out_dir}/")
    print(f"  Manifest:          {out_dir}/manifest.md")
    print(f"  Redaction map:     {mapping_path}")


def _fixture_exists(out_dir: Path, category: str, filename: str) -> bool:
    """Check if a redacted fixture already exists."""
    return (out_dir / category / filename).exists()


# ── Refresh mode ───────────────────────────────────────────────────────

async def run_refresh(
    client: ReadonlyCrawlabClient,
    cfg: dict[str, Any],
    raw_dir: Path,
    out_dir: Path,
) -> None:
    """Refresh mode: re-collect only tasks not yet in fixtures."""
    # Refresh is just collect with skip_existing=True
    await run_collect(
        client, cfg, raw_dir, out_dir, skip_existing=True,
    )


# ── CLI ────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="collect_fixtures",
        description="Fixture collector for Crawlab companion app (read-only, GET-only).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python -m tools.collect_fixtures --dry-run
  python -m tools.collect_fixtures --collect
  python -m tools.collect_fixtures --collect --max-examples-per-class 3
  python -m tools.collect_fixtures --refresh --skip-existing

Environment:
  CRAWLAB_BASE_URL    Base URL of Crawlab instance
  CRAWLAB_TOKEN       API authentication token
        """,
    )

    # Mode (mutually exclusive)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be collected without writing fixtures",
    )
    mode.add_argument(
        "--collect",
        action="store_true",
        help="Full collection: fetch, redact, write",
    )
    mode.add_argument(
        "--refresh",
        action="store_true",
        help="Re-collect only tasks not yet in fixtures",
    )

    # Options
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to user_scope.yml (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help=f"Output directory for raw data (default: {DEFAULT_RAW_DIR})",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Output directory for redacted fixtures (default: {DEFAULT_OUT_DIR})",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip tasks that already have fixtures",
    )
    parser.add_argument(
        "--skip-logs",
        action="store_true",
        help="Skip log collection (faster iteration)",
    )
    parser.add_argument(
        "--skip-results",
        action="store_true",
        help="Skip results collection",
    )
    parser.add_argument(
        "--max-task-pages",
        type=int,
        default=None,
        help="Override max task pages from config",
    )
    parser.add_argument(
        "--max-log-pages",
        type=int,
        default=None,
        help="Override max log pages from config",
    )
    parser.add_argument(
        "--max-examples-per-class",
        type=int,
        default=None,
        help="Override max examples per class from config",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay between API requests in seconds",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress progress output",
    )

    return parser


async def main(argv: list[str] | None = None) -> None:
    """Main entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    # Logging setup
    level = logging.DEBUG if args.verbose else (logging.WARNING if args.quiet else logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Load config
    cfg = load_config(args.config)

    # Apply CLI overrides
    if args.max_task_pages is not None:
        cfg.setdefault("collection", {})["max_task_pages"] = args.max_task_pages
    if args.max_log_pages is not None:
        cfg.setdefault("collection", {})["max_log_pages"] = args.max_log_pages
    if args.max_examples_per_class is not None:
        cfg.setdefault("collection", {})["max_examples_per_class"] = args.max_examples_per_class

    # Load .env file if it exists (simple key=value parsing)
    _load_dotenv()

    # Get credentials from env
    base_url_env = cfg.get("crawlab", {}).get("base_url_env", "CRAWLAB_BASE_URL")
    token_env = cfg.get("crawlab", {}).get("token_env", "CRAWLAB_TOKEN")

    base_url = os.environ.get(base_url_env, "")
    token = os.environ.get(token_env, "")

    if not base_url:
        print(f"Error: {base_url_env} environment variable is required", file=sys.stderr)
        sys.exit(1)
    if not token:
        print(f"Error: {token_env} environment variable is required", file=sys.stderr)
        sys.exit(1)

    # Strip trailing /api to prevent double path (/api/api/tasks)
    base_url = base_url.rstrip("/")
    if base_url.endswith("/api"):
        base_url = base_url[:-4]

    # Build client
    allowed_paths = cfg.get("security", {}).get("allowed_paths", None)

    async with ReadonlyCrawlabClient(
        base_url=base_url,
        allowed_paths=allowed_paths,
    ) as client:
        if args.dry_run:
            await run_dry_run(client, cfg, args.raw_dir)
        elif args.collect:
            await run_collect(
                client, cfg, args.raw_dir, args.out_dir,
                skip_existing=args.skip_existing,
                skip_logs=args.skip_logs,
                skip_results=args.skip_results,
            )
        elif args.refresh:
            await run_refresh(client, cfg, args.raw_dir, args.out_dir)


def cli_main() -> None:
    """Synchronous wrapper for module execution."""
    asyncio.run(main())


if __name__ == "__main__":
    cli_main()
