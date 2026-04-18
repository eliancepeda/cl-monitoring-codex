"""Tests for ReadonlyCrawlabClient.

Verifies safety invariants (AGENTS.md § Safety):
- Only GET requests are permitted.
- POST/PUT/PATCH/DELETE raise ReadonlyViolationError.
- Auth header is never leaked into repr/str.
- Path allowlist is enforced.
- Redirects are disabled.
- Pagination uses page/size and respects max_pages.

All tests use mock transport — NO network access.
"""

from __future__ import annotations

import os
import pytest
import httpx

from integrations.crawlab.readonly_client import (
    ReadonlyCrawlabClient,
    ReadonlyViolationError,
    PathNotAllowedError,
    _path_matches_allowlist,
    DEFAULT_ALLOWED_PATHS,
)


# ── Mock transport ─────────────────────────────────────────────────────


def _make_mock_transport(
    responses: dict[str, dict] | None = None,
) -> httpx.MockTransport:
    """Create a mock transport returning canned responses."""
    default_responses = responses or {}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path in default_responses:
            return httpx.Response(
                200,
                json=default_responses[path],
            )
        # Default: return empty data
        return httpx.Response(200, json={"data": [], "total": 0})

    return httpx.MockTransport(handler)


def _make_client(
    transport: httpx.MockTransport | None = None,
    allowed_paths: list[str] | None = None,
) -> ReadonlyCrawlabClient:
    """Create a client with mock transport for testing."""
    os.environ["CRAWLAB_API_TOKEN"] = "test-token-never-real"
    client = ReadonlyCrawlabClient(
        base_url="http://localhost:8080",
        allowed_paths=allowed_paths,
    )
    # Replace the internal httpx client with one using mock transport
    if transport:
        client._client = httpx.AsyncClient(
            base_url="http://localhost:8080",
            headers={"Authorization": "test-token-never-real"},
            transport=transport,
            follow_redirects=False,
        )
    return client


# ── Safety: GET-only enforcement ───────────────────────────────────────


class TestGetOnlyEnforcement:
    """Verify that only GET requests are allowed."""

    @pytest.mark.asyncio
    async def test_get_allowed_path_succeeds(self) -> None:
        transport = _make_mock_transport(
            {
                "/api/tasks": {"data": [{"_id": "abc"}], "total": 1},
            }
        )
        client = _make_client(transport)
        try:
            resp = await client.get("/api/tasks")
            assert resp.status_code == 200
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_post_raises_readonly_violation(self) -> None:
        client = _make_client()
        try:
            with pytest.raises(ReadonlyViolationError, match="POST"):
                await client.post("/api/tasks")
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_put_raises_readonly_violation(self) -> None:
        client = _make_client()
        try:
            with pytest.raises(ReadonlyViolationError, match="PUT"):
                await client.put("/api/tasks/123")
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_patch_raises_readonly_violation(self) -> None:
        client = _make_client()
        try:
            with pytest.raises(ReadonlyViolationError, match="PATCH"):
                await client.patch("/api/tasks/123")
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_delete_raises_readonly_violation(self) -> None:
        client = _make_client()
        try:
            with pytest.raises(ReadonlyViolationError, match="DELETE"):
                await client.delete("/api/tasks/123")
        finally:
            await client.close()


# ── Safety: Auth header never leaked ───────────────────────────────────


class TestTokenSafety:
    """Verify auth tokens are never exposed."""

    def test_repr_does_not_contain_token(self) -> None:
        os.environ["CRAWLAB_API_TOKEN"] = "super-secret-token-12345"
        client = ReadonlyCrawlabClient(
            base_url="http://localhost",
        )
        repr_str = repr(client)
        assert "super-secret-token-12345" not in repr_str
        assert "****" in repr_str

    def test_str_does_not_contain_token(self) -> None:
        os.environ["CRAWLAB_API_TOKEN"] = "another-secret-token"
        client = ReadonlyCrawlabClient(
            base_url="http://localhost",
        )
        str_str = str(client)
        assert "another-secret-token" not in str_str
        assert "****" in str_str


# ── Path allowlist ─────────────────────────────────────────────────────


class TestPathAllowlist:
    """Verify path allowlist enforcement."""

    @pytest.mark.asyncio
    async def test_get_disallowed_path_raises(self) -> None:
        client = _make_client()
        try:
            with pytest.raises(PathNotAllowedError, match="not in the allowed"):
                await client.get("/api/nodes")
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_get_disallowed_arbitrary_path_raises(self) -> None:
        client = _make_client()
        try:
            with pytest.raises(PathNotAllowedError):
                await client.get("/admin/settings")
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_get_absolute_url_raises(self) -> None:
        client = _make_client()
        try:
            with pytest.raises(
                PathNotAllowedError, match="Absolute URLs are forbidden"
            ):
                await client.get("http://evil.com/api/tasks")
        finally:
            await client.close()

    def test_path_matches_exact(self) -> None:
        assert _path_matches_allowlist("/api/tasks", DEFAULT_ALLOWED_PATHS)
        assert _path_matches_allowlist("/api/schedules", DEFAULT_ALLOWED_PATHS)

    def test_path_matches_wildcard(self) -> None:
        assert _path_matches_allowlist("/api/spiders/def456", DEFAULT_ALLOWED_PATHS)
        assert _path_matches_allowlist("/api/schedules/xyz", DEFAULT_ALLOWED_PATHS)
        assert _path_matches_allowlist("/api/results/col_001", DEFAULT_ALLOWED_PATHS)

    def test_path_matches_nested_wildcard(self) -> None:
        assert _path_matches_allowlist(
            "/api/tasks/abc123/logs",
            DEFAULT_ALLOWED_PATHS,
        )

    def test_task_detail_path_rejects_unlisted(self) -> None:
        assert not _path_matches_allowlist(
            "/api/tasks/abc123",
            DEFAULT_ALLOWED_PATHS,
        )

    def test_nested_task_non_log_path_rejects_unlisted(self) -> None:
        assert not _path_matches_allowlist(
            "/api/tasks/abc123/restart",
            DEFAULT_ALLOWED_PATHS,
        )

    def test_path_rejects_unlisted(self) -> None:
        assert not _path_matches_allowlist("/api/nodes", DEFAULT_ALLOWED_PATHS)
        assert not _path_matches_allowlist("/api/users", DEFAULT_ALLOWED_PATHS)
        assert not _path_matches_allowlist("/admin", DEFAULT_ALLOWED_PATHS)

    def test_custom_allowlist(self) -> None:
        custom = ["/api/tasks"]
        assert _path_matches_allowlist("/api/tasks", custom)
        assert not _path_matches_allowlist("/api/spiders/abc", custom)

    @pytest.mark.asyncio
    async def test_get_task_detail_path_raises(self) -> None:
        client = _make_client()
        try:
            with pytest.raises(PathNotAllowedError, match="not in the allowed"):
                await client.get("/api/tasks/abc123")
        finally:
            await client.close()


# ── Redirects ──────────────────────────────────────────────────────────


class TestRedirectsDisabled:
    """Verify that redirects are disabled."""

    def test_client_redirects_disabled(self) -> None:
        os.environ["CRAWLAB_API_TOKEN"] = "test"
        client = ReadonlyCrawlabClient(
            base_url="http://localhost",
        )
        assert client._client.follow_redirects is False


# ── Pagination ─────────────────────────────────────────────────────────


class TestPagination:
    """Verify paginated fetching logic."""

    @pytest.mark.asyncio
    async def test_pagination_fetches_multiple_pages(self) -> None:
        page_counter = {"count": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            page_counter["count"] += 1
            page = int(request.url.params.get("page", "1"))
            if page == 1:
                return httpx.Response(
                    200,
                    json={
                        "data": [{"_id": "t1"}, {"_id": "t2"}],
                        "total": 4,
                    },
                )
            elif page == 2:
                return httpx.Response(
                    200,
                    json={
                        "data": [{"_id": "t3"}, {"_id": "t4"}],
                        "total": 4,
                    },
                )
            return httpx.Response(200, json={"data": [], "total": 4})

        transport = httpx.MockTransport(handler)
        client = _make_client(transport)
        try:
            items, meta = await client.get_paginated(
                "/api/tasks",
                page_size=2,
                max_pages=5,
            )
            assert len(items) == 4
            assert meta["api_reported_total"] == 4
            assert page_counter["count"] == 2
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_pagination_stops_at_max_pages(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "data": [{"_id": "item"}],
                    "total": 1000,
                },
            )

        transport = httpx.MockTransport(handler)
        client = _make_client(transport)
        try:
            items, meta = await client.get_paginated(
                "/api/tasks",
                page_size=1,
                max_pages=3,
            )
            assert len(items) == 3
            assert meta["pages_fetched"] == 3
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_pagination_stops_on_empty_data(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            page = int(request.url.params.get("page", "1"))
            if page == 1:
                return httpx.Response(
                    200,
                    json={
                        "data": [{"_id": "only"}],
                        "total": 1,
                    },
                )
            return httpx.Response(200, json={"data": [], "total": 1})

        transport = httpx.MockTransport(handler)
        client = _make_client(transport)
        try:
            items, meta = await client.get_paginated(
                "/api/tasks",
                page_size=10,
                max_pages=5,
            )
            assert len(items) == 1
            assert meta["records_fetched"] == 1
        finally:
            await client.close()


# ── Context manager ────────────────────────────────────────────────────


class TestContextManager:
    """Verify async context manager works."""

    @pytest.mark.asyncio
    async def test_async_context_manager(self) -> None:
        transport = _make_mock_transport()
        os.environ["CRAWLAB_API_TOKEN"] = "test"
        async with ReadonlyCrawlabClient(
            base_url="http://localhost",
        ) as client:
            client._client = httpx.AsyncClient(
                base_url="http://localhost",
                transport=transport,
            )
            resp = await client.get("/api/tasks")
            assert resp.status_code == 200


# ── Normalization ──────────────────────────────────────────────────────


class TestNormalization:
    """Verify JSON body normalization."""

    @pytest.mark.asyncio
    async def test_normalize_zero_time(self) -> None:
        transport = _make_mock_transport(
            {
                "/api/tasks": {"data": [{"time": "0001-01-01T00:00:00Z"}], "total": 1},
            }
        )
        client = _make_client(transport)
        try:
            data = await client.get_json("/api/tasks")
            item = data.get("data", [])[0]
            assert item["time"] is None
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_normalize_results_data_null(self) -> None:
        transport = _make_mock_transport(
            {
                "/api/results/123": {"data": None, "total": 0},
            }
        )
        client = _make_client(transport)
        try:
            data = await client.get_json("/api/results/123")
            assert data["data"] == []
        finally:
            await client.close()
