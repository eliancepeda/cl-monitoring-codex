from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import cl_monitoring.app as app_module
from cl_monitoring.db.engine import connect_sqlite as real_connect_sqlite
from cl_monitoring.settings import (
    DEFAULT_APP_HOST,
    RuntimeConfigurationError,
    RuntimeMode,
    build_runtime_settings,
    load_settings,
)


class FakeReadonlyClient:
    instances: list[FakeReadonlyClient] = []

    def __init__(self, base_url: str, *, token: str, timeout: float = 30.0) -> None:
        del timeout
        self.base_url = base_url
        self.token = token
        self.closed = False
        FakeReadonlyClient.instances.append(self)

    async def close(self) -> None:
        self.closed = True


class FakePoller:
    instances: list[FakePoller] = []

    def __init__(self, client: FakeReadonlyClient, repo: object) -> None:
        self.client = client
        self.repo = repo
        self.sync_calls: list[dict[str, object]] = []
        self.run_calls = 0
        self.stop_event: asyncio.Event | None = None
        self.stopped = False
        FakePoller.instances.append(self)

    async def sync_once(self, *, now: object = None, force: bool = False) -> None:
        self.sync_calls.append({"now": now, "force": force})

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> None:
        self.run_calls += 1
        assert stop_event is not None
        self.stop_event = stop_event
        await stop_event.wait()
        self.stopped = True


class FailingPoller(FakePoller):
    async def sync_once(self, *, now: object = None, force: bool = False) -> None:
        self.sync_calls.append({"now": now, "force": force})
        raise RuntimeError("initial sync failed")


def test_load_settings_reads_dotenv_and_process_env_override(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / ".env"
    env_db_path = tmp_path / "from-env.sqlite3"
    env_file.write_text(
        "\n".join(
            [
                "CRAWLAB_BASE_URL=https://crawlab-from-file.example/api/",
                "CRAWLAB_TOKEN=file-token",
                f"CL_MONITORING_DB_PATH={env_db_path}",
                "APP_PORT=8787",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("CRAWLAB_TOKEN", "env-token")
    monkeypatch.setenv("APP_PORT", "8899")
    monkeypatch.delenv("CRAWLAB_BASE_URL", raising=False)
    monkeypatch.delenv("CL_MONITORING_DB_PATH", raising=False)

    settings = load_settings(env_file=env_file)

    assert settings.runtime_mode is RuntimeMode.LIVE
    assert settings.live_base_url == "https://crawlab-from-file.example"
    assert settings.live_token == "env-token"
    assert settings.db_path == env_db_path.resolve()
    assert settings.app_port == 8899


def test_main_runs_uvicorn_with_fixed_local_bind(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run(app: object, *, host: str, port: int) -> None:
        captured["app"] = app
        captured["host"] = host
        captured["port"] = port

    settings = build_runtime_settings(
        db_path=tmp_path / "main.sqlite3",
        app_port=8899,
    )
    monkeypatch.setattr(app_module.uvicorn, "run", fake_run)

    app_module.main(settings=settings)

    assert captured["host"] == DEFAULT_APP_HOST
    assert captured["port"] == 8899


def test_create_app_starts_sqlite_only_mode_without_live_resources(tmp_path) -> None:
    db_path = tmp_path / "sqlite-only.sqlite3"
    settings = build_runtime_settings(db_path=db_path)
    app = app_module.create_app(settings=settings)

    with TestClient(app) as client:
        response = client.get("/")

        assert response.status_code == 200
        assert app.state.runtime_mode is RuntimeMode.SQLITE_ONLY
        assert app.state.readonly_client is None
        assert app.state.poller is None
        assert app.state.poller_task is None
        assert Path(app.state.db_path) == db_path.resolve()
        assert db_path.exists()
        assert app.state.writer_connection is not None

    assert app.state.writer_connection is None


def test_create_app_live_mode_runs_initial_sync_and_background_poller(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    FakeReadonlyClient.instances.clear()
    FakePoller.instances.clear()
    monkeypatch.setattr(app_module, "ReadonlyCrawlabClient", FakeReadonlyClient)
    monkeypatch.setattr(app_module, "Poller", FakePoller)

    settings = build_runtime_settings(
        crawlab_base_url="https://crawlab.example/api/",
        crawlab_token="runtime-token",
        db_path=tmp_path / "live.sqlite3",
    )
    app = app_module.create_app(settings=settings)

    with TestClient(app) as client:
        response = client.get("/")

        assert response.status_code == 200
        assert app.state.runtime_mode is RuntimeMode.LIVE
        assert len(FakeReadonlyClient.instances) == 1
        assert len(FakePoller.instances) == 1

        fake_client = FakeReadonlyClient.instances[0]
        fake_poller = FakePoller.instances[0]
        assert fake_client.base_url == "https://crawlab.example"
        assert fake_client.token == "runtime-token"
        assert fake_poller.sync_calls == [{"now": None, "force": True}]
        assert fake_poller.run_calls == 1
        assert fake_poller.stop_event is app.state.poller_stop_event
        assert fake_client.closed is False

    fake_client = FakeReadonlyClient.instances[0]
    fake_poller = FakePoller.instances[0]
    assert fake_poller.stop_event is not None
    assert fake_poller.stop_event.is_set() is True
    assert fake_poller.stopped is True
    assert fake_client.closed is True
    assert app.state.writer_connection is None


def test_create_app_rejects_partial_live_configuration(tmp_path) -> None:
    db_path = tmp_path / "partial.sqlite3"
    settings = build_runtime_settings(
        crawlab_base_url="https://crawlab.example/api",
        db_path=db_path,
    )
    app = app_module.create_app(settings=settings)

    with pytest.raises(
        RuntimeConfigurationError,
        match="Partial live configuration",
    ), TestClient(app):
        pass

    assert not db_path.exists()


def test_create_app_live_startup_failure_closes_client_and_db(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    FakeReadonlyClient.instances.clear()
    FailingPoller.instances.clear()

    captured_connection: dict[str, sqlite3.Connection] = {}

    def capture_connect(db_path: str | Path) -> sqlite3.Connection:
        connection = real_connect_sqlite(db_path)
        captured_connection["connection"] = connection
        return connection

    monkeypatch.setattr(app_module, "ReadonlyCrawlabClient", FakeReadonlyClient)
    monkeypatch.setattr(app_module, "Poller", FailingPoller)
    monkeypatch.setattr(app_module, "connect_sqlite", capture_connect)

    settings = build_runtime_settings(
        crawlab_base_url="https://crawlab.example/api",
        crawlab_token="runtime-token",
        db_path=tmp_path / "startup-failure.sqlite3",
    )
    app = app_module.create_app(settings=settings)

    with pytest.raises(RuntimeError, match="initial sync failed"), TestClient(app):
        pass

    assert len(FakeReadonlyClient.instances) == 1
    assert len(FailingPoller.instances) == 1
    assert FakeReadonlyClient.instances[0].closed is True
    with pytest.raises(sqlite3.ProgrammingError):
        captured_connection["connection"].execute("SELECT 1")
    assert app.state.writer_connection is None
