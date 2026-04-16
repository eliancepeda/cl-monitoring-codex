"""ReadonlyCrawlabClient — the single gateway to Crawlab API.

Safety invariants (AGENTS.md § Safety):
- Only GET requests are allowed.
- Any attempt to use POST/PUT/PATCH/DELETE MUST raise ReadonlyViolationError.
- Authorization header must never be logged or stored in fixtures.
- Path allowlist is enforced on every request.
- Redirects are disabled to prevent open-redirect abuse.

All Crawlab access in this project MUST go through this client.
"""

from __future__ import annotations

import logging
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

import httpx
import yaml

logger = logging.getLogger(__name__)

# ── Sentinel values (AGENTS.md § Domain rules) ─────────────────────────
ZERO_OBJECT_ID = "000000000000000000000000"
ZERO_TIME = "0001-01-01T00:00:00Z"

# ── Default path allowlist (matches AGENTS.md § Fixture collector rules)
DEFAULT_ALLOWED_PATHS: list[str] = [
    "/api/tasks",
    "/api/tasks/*",
    "/api/spiders/*",
    "/api/schedules",
    "/api/schedules/*",
    "/api/results/*",
]


class ReadonlyViolationError(Exception):
    """Raised when a non-GET operation is attempted."""


class PathNotAllowedError(Exception):
    """Raised when a request targets a path outside the allowlist."""


def _load_allowed_paths(config_path: Path | None = None) -> list[str]:
    """Load allowed paths from user_scope.yml or use defaults."""
    if config_path and config_path.exists():
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        paths = cfg.get("security", {}).get("allowed_paths", [])
        if paths:
            return [str(p) for p in paths]
    return list(DEFAULT_ALLOWED_PATHS)


def _path_matches_allowlist(path: str, allowed: list[str]) -> bool:
    """Check if path matches any pattern in the allowlist.

    Uses fnmatch-style wildcards:
      /api/tasks     — exact match
      /api/tasks/*   — matches /api/tasks/abc123, /api/tasks/abc123/logs
    """
    # Normalize: strip trailing slashes, ensure leading /
    clean = "/" + path.lstrip("/").rstrip("/")
    for pattern in allowed:
        if fnmatch(clean, pattern):
            return True
        # Also check with trailing path segments for nested routes
        # e.g., /api/tasks/*/logs should match /api/tasks/* pattern
        parts = clean.split("/")
        for i in range(len(parts), 1, -1):
            prefix = "/".join(parts[:i])
            if fnmatch(prefix, pattern):
                return True
    return False


class ReadonlyCrawlabClient:
    """Read-only async HTTP client for Crawlab API.

    All requests are validated against a path allowlist and restricted
    to GET method only.  Any mutation attempt raises ReadonlyViolationError.

    Args:
        base_url: Base URL of the Crawlab instance.
        token: API authentication token (from env, never stored).
        allowed_paths: Optional override for path allowlist.
        timeout: Request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        allowed_paths: list[str] | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token  # never logged or persisted
        self._allowed_paths = allowed_paths or list(DEFAULT_ALLOWED_PATHS)
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"Authorization": self._token},
            timeout=timeout,
            follow_redirects=False,
        )

    # ── Public API ──────────────────────────────────────────────────────

    async def get(self, path: str, **params: Any) -> httpx.Response:
        """Execute a GET request after allowlist validation.

        Args:
            path: API path (e.g., "/api/tasks").
            **params: Query parameters.

        Returns:
            httpx.Response with JSON body.

        Raises:
            PathNotAllowedError: If path is not in the allowlist.
        """
        self._assert_path_allowed(path)
        logger.debug("GET %s params=%s", path, params)
        response = await self._client.get(path, params=params)
        response.raise_for_status()
        return response

    async def get_json(self, path: str, **params: Any) -> Any:
        """GET and return parsed JSON body."""
        resp = await self.get(path, **params)
        return resp.json()

    async def get_paginated(
        self,
        path: str,
        *,
        page_size: int = 100,
        max_pages: int = 5,
        **extra_params: Any,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Fetch paginated results using page/size params.

        Crawlab uses page/size query params and returns total in response.

        Args:
            path: API path.
            page_size: Items per page.
            max_pages: Maximum number of pages to fetch.
            **extra_params: Additional query parameters.

        Returns:
            Tuple of (all_items, metadata_dict).
        """
        all_items: list[dict[str, Any]] = []
        reported_total = 0
        pages_fetched = 0

        for page_num in range(1, max_pages + 1):
            params = {"page": page_num, "size": page_size, **extra_params}
            data = await self.get_json(path, **params)

            pages_fetched += 1
            reported_total = data.get("total", 0)

            # Crawlab wraps items in "data" key with "total" at top level
            items = data.get("data", [])
            if not items:
                break
            all_items.extend(items)

            if page_num * page_size >= reported_total:
                break

            logger.debug(
                "Page %d/%d fetched (%d items, total=%d)",
                page_num, max_pages, len(items), reported_total,
            )
            
        meta = {
            "api_reported_total": reported_total,
            "pages_fetched": pages_fetched,
            "records_fetched": len(all_items),
        }
        return all_items, meta

    # ── Write-method guards ─────────────────────────────────────────────

    async def post(self, *args: Any, **kwargs: Any) -> None:
        """Blocked — raises ReadonlyViolationError."""
        raise ReadonlyViolationError(
            "POST requests are forbidden. This is a read-only client."
        )

    async def put(self, *args: Any, **kwargs: Any) -> None:
        """Blocked — raises ReadonlyViolationError."""
        raise ReadonlyViolationError(
            "PUT requests are forbidden. This is a read-only client."
        )

    async def patch(self, *args: Any, **kwargs: Any) -> None:
        """Blocked — raises ReadonlyViolationError."""
        raise ReadonlyViolationError(
            "PATCH requests are forbidden. This is a read-only client."
        )

    async def delete(self, *args: Any, **kwargs: Any) -> None:
        """Blocked — raises ReadonlyViolationError."""
        raise ReadonlyViolationError(
            "DELETE requests are forbidden. This is a read-only client."
        )

    # ── Internal helpers ────────────────────────────────────────────────

    def _assert_path_allowed(self, path: str) -> None:
        """Raise PathNotAllowedError if path is not in the allowlist."""
        if not _path_matches_allowlist(path, self._allowed_paths):
            raise PathNotAllowedError(
                f"Path '{path}' is not in the allowed paths: {self._allowed_paths}"
            )

    # ── Lifecycle ───────────────────────────────────────────────────────

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> ReadonlyCrawlabClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ── Safety: never leak token in repr/str ────────────────────────────

    def __repr__(self) -> str:
        return (
            f"ReadonlyCrawlabClient(base_url={self._base_url!r}, "
            f"token='****', "
            f"allowed_paths={self._allowed_paths!r})"
        )

    def __str__(self) -> str:
        return self.__repr__()
