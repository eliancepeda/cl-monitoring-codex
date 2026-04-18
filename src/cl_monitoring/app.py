"""FastAPI application factory.

Assembles routes, lifespan events, and middleware.
The app MUST bind to 127.0.0.1 only (see AGENTS.md § Safety).
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from cl_monitoring.web.routes import router

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787
DEFAULT_DB_PATH = Path("cl-monitoring.sqlite3")


def create_app(
    *,
    db_path: str | Path | None = None,
    now_provider: Callable[[], datetime] | None = None,
) -> FastAPI:
    app = FastAPI(title="CL Monitoring", version="0.1.0")

    resolved_db_path = Path(db_path) if db_path is not None else _default_db_path()
    app.state.db_path = resolved_db_path
    app.state.now_provider = now_provider or _utc_now
    app.state.default_host = DEFAULT_HOST

    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    app.include_router(router)
    return app


def main() -> None:
    app = create_app()
    port = _default_port()
    uvicorn.run(app, host=DEFAULT_HOST, port=port)


def _default_db_path() -> Path:
    raw_path = os.environ.get("CL_MONITORING_DB_PATH")
    if not raw_path:
        return DEFAULT_DB_PATH
    return Path(raw_path)


def _default_port() -> int:
    raw_port = os.environ.get("APP_PORT")
    if not raw_port:
        return DEFAULT_PORT
    return int(raw_port)


def _utc_now() -> datetime:
    return datetime.now(UTC)


app = create_app()


if __name__ == "__main__":
    main()
