"""FastAPI application factory and local service entrypoint."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from cl_monitoring.db.engine import connect_sqlite
from cl_monitoring.db.repo import LocalRepository
from cl_monitoring.settings import (
    DEFAULT_APP_HOST,
    DEFAULT_DB_PATH,
    RuntimeMode,
    RuntimeSettings,
    build_runtime_settings,
    load_settings,
)
from cl_monitoring.sync.poller import Poller
from cl_monitoring.web.routes import router
from integrations.crawlab.readonly_client import ReadonlyCrawlabClient


def create_app(
    *,
    settings: RuntimeSettings | None = None,
    db_path: str | Path | None = None,
    now_provider: Callable[[], datetime] | None = None,
) -> FastAPI:
    runtime_settings = _override_db_path(settings, db_path)
    app = FastAPI(
        title="CL Monitoring",
        version="0.1.0",
        lifespan=_build_lifespan(settings=runtime_settings, db_path_override=db_path),
    )

    app.state.db_path = _initial_db_path(runtime_settings, db_path)
    app.state.now_provider = now_provider or _utc_now
    app.state.default_host = DEFAULT_APP_HOST
    app.state.runtime_settings = runtime_settings
    app.state.runtime_mode = None
    app.state.writer_connection = None
    app.state.repo = None
    app.state.readonly_client = None
    app.state.poller = None
    app.state.poller_stop_event = None
    app.state.poller_task = None

    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    app.include_router(router)
    return app


def main(*, settings: RuntimeSettings | None = None) -> None:
    runtime_settings = settings or load_settings()
    app = create_app(settings=runtime_settings)
    uvicorn.run(
        app,
        host=runtime_settings.app_host,
        port=runtime_settings.app_port,
    )


def _build_lifespan(
    *,
    settings: RuntimeSettings | None,
    db_path_override: str | Path | None,
) -> Callable[[FastAPI], AbstractAsyncContextManager[None]]:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        base_settings = settings if settings is not None else load_settings()
        runtime_settings = _override_db_path(base_settings, db_path_override)
        assert runtime_settings is not None
        runtime_mode = runtime_settings.runtime_mode
        db_path = runtime_settings.db_path
        _ensure_db_parent(db_path)

        writer_connection = None
        client = None
        stop_event = None
        poller_task = None

        try:
            writer_connection = connect_sqlite(db_path)
            repo = LocalRepository(writer_connection)

            app.state.db_path = db_path
            app.state.runtime_settings = runtime_settings
            app.state.runtime_mode = runtime_mode
            app.state.writer_connection = writer_connection
            app.state.repo = repo

            if runtime_mode is RuntimeMode.LIVE:
                client = ReadonlyCrawlabClient(
                    base_url=runtime_settings.live_base_url,
                    token=runtime_settings.live_token,
                )
                poller = Poller(client, repo)
                stop_event = asyncio.Event()

                await poller.sync_once(force=True)
                poller_task = asyncio.create_task(
                    poller.run_forever(stop_event=stop_event),
                    name="cl-monitoring-poller",
                )

                app.state.readonly_client = client
                app.state.poller = poller
                app.state.poller_stop_event = stop_event
                app.state.poller_task = poller_task

            yield
        finally:
            if stop_event is not None:
                stop_event.set()

            try:
                if poller_task is not None:
                    await poller_task
            finally:
                try:
                    if client is not None:
                        await client.close()
                finally:
                    if writer_connection is not None:
                        writer_connection.close()

                    app.state.writer_connection = None
                    app.state.repo = None
                    app.state.readonly_client = None
                    app.state.poller = None
                    app.state.poller_stop_event = None
                    app.state.poller_task = None

    return lifespan


def _initial_db_path(
    settings: RuntimeSettings | None,
    db_path_override: str | Path | None,
) -> Path:
    if settings is not None:
        return settings.db_path
    if db_path_override is not None:
        return build_runtime_settings(db_path=db_path_override).db_path
    return DEFAULT_DB_PATH.expanduser().resolve(strict=False)


def _override_db_path(
    settings: RuntimeSettings | None,
    db_path_override: str | Path | None,
) -> RuntimeSettings | None:
    if settings is None or db_path_override is None:
        return settings
    return build_runtime_settings(
        crawlab_base_url=settings.crawlab_base_url,
        crawlab_token=settings.crawlab_token,
        app_port=settings.app_port,
        db_path=db_path_override,
    )


def _ensure_db_parent(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _utc_now() -> datetime:
    return datetime.now(UTC)


app = create_app()


if __name__ == "__main__":
    main()
