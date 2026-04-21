import os
from dataclasses import dataclass
from typing import Mapping, Sequence


DEFAULT_MAX_PROJECT_PAGES = 6
DEFAULT_MAX_SPIDER_PAGES = 20
DEFAULT_MAX_SCHEDULE_PAGES = 20
DEFAULT_MAX_TASK_PAGES = 60
DEFAULT_MAX_LOG_TASKS_PER_PROJECT = 24
DEFAULT_MAX_LOG_PAGES_PER_TASK = 5
DEFAULT_TASK_PAGE_STABILITY_WINDOW = 3


@dataclass(frozen=True)
class Settings:
    base_url: str
    api_key: str
    project_ids: tuple[str, ...]
    output_root: str
    throttle_seconds: float
    page_size: int
    expanded_task_limit: int
    max_pages: int
    log_page_size: int
    max_project_pages: int = DEFAULT_MAX_PROJECT_PAGES
    max_spider_pages: int = DEFAULT_MAX_SPIDER_PAGES
    max_schedule_pages: int = DEFAULT_MAX_SCHEDULE_PAGES
    max_task_pages: int = DEFAULT_MAX_TASK_PAGES
    max_spiders_per_project_for_detail: int = 12
    max_tasks_per_spider_for_detail: int = 6
    max_task_details_total: int = 96
    max_log_tasks_total: int = 48
    max_log_tasks_per_project: int = DEFAULT_MAX_LOG_TASKS_PER_PROJECT
    max_log_pages_per_task: int = DEFAULT_MAX_LOG_PAGES_PER_TASK
    task_page_stability_window: int = DEFAULT_TASK_PAGE_STABILITY_WINDOW


def load_settings(
    project_ids: Sequence[str],
    environ: Mapping[str, str] | None = None,
    output_root: str = "docs/discovery",
    throttle_seconds: float = 0.5,
    page_size: int = 25,
    expanded_task_limit: int = 60,
    max_pages: int = DEFAULT_MAX_TASK_PAGES,
    log_page_size: int = 100,
    max_project_pages: int = DEFAULT_MAX_PROJECT_PAGES,
    max_spider_pages: int = DEFAULT_MAX_SPIDER_PAGES,
    max_schedule_pages: int = DEFAULT_MAX_SCHEDULE_PAGES,
    max_task_pages: int = DEFAULT_MAX_TASK_PAGES,
    max_spiders_per_project_for_detail: int = 12,
    max_tasks_per_spider_for_detail: int = 6,
    max_task_details_total: int = 96,
    max_log_tasks_total: int = 48,
    max_log_tasks_per_project: int = DEFAULT_MAX_LOG_TASKS_PER_PROJECT,
    max_log_pages_per_task: int = DEFAULT_MAX_LOG_PAGES_PER_TASK,
    task_page_stability_window: int = DEFAULT_TASK_PAGE_STABILITY_WINDOW,
) -> Settings:
    if isinstance(project_ids, str):
        raise ValueError("project_ids must be a sequence of strings, not a string")

    env = dict(os.environ if environ is None else environ)
    missing = [
        name
        for name in ("CRAWLAB_BASE_URL", "CRAWLAB_API_KEY")
        if not env.get(name)
    ]
    if missing:
        raise ValueError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    if any(not item.strip() for item in project_ids):
        raise ValueError("project_ids must not contain empty values")

    normalized_project_ids = tuple(item.strip() for item in project_ids)
    if not normalized_project_ids:
        raise ValueError("At least one project id is required")

    return Settings(
        base_url=env["CRAWLAB_BASE_URL"].rstrip("/"),
        api_key=env["CRAWLAB_API_KEY"],
        project_ids=normalized_project_ids,
        output_root=output_root,
        throttle_seconds=throttle_seconds,
        page_size=page_size,
        expanded_task_limit=expanded_task_limit,
        max_pages=max_pages,
        log_page_size=log_page_size,
        max_project_pages=max_project_pages,
        max_spider_pages=max_spider_pages,
        max_schedule_pages=max_schedule_pages,
        max_task_pages=max_task_pages,
        max_spiders_per_project_for_detail=max_spiders_per_project_for_detail,
        max_tasks_per_spider_for_detail=max_tasks_per_spider_for_detail,
        max_task_details_total=max_task_details_total,
        max_log_tasks_total=max_log_tasks_total,
        max_log_tasks_per_project=max_log_tasks_per_project,
        max_log_pages_per_task=max_log_pages_per_task,
        task_page_stability_window=task_page_stability_window,
    )
