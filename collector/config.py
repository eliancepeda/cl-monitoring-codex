import os
from dataclasses import dataclass
from typing import Mapping, Sequence


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


def load_settings(
    project_ids: Sequence[str],
    environ: Mapping[str, str] | None = None,
    output_root: str = "docs/discovery",
    throttle_seconds: float = 0.5,
    page_size: int = 10,
    expanded_task_limit: int = 20,
    max_pages: int = 3,
    log_page_size: int = 100,
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
    )
