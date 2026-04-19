"""Runtime settings and service-mode selection for the local app."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_APP_HOST = "127.0.0.1"
DEFAULT_APP_PORT = 8787
DEFAULT_DB_PATH = Path("cl-monitoring.sqlite3")


class RuntimeMode(StrEnum):
    SQLITE_ONLY = "sqlite_only"
    LIVE = "live"


class RuntimeConfigurationError(ValueError):
    """Raised when the runtime environment does not match the service contract."""


class RuntimeSettings(BaseSettings):
    """Single runtime truth source for app startup."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
        frozen=True,
        populate_by_name=True,
    )

    crawlab_base_url: str | None = Field(
        default=None,
        validation_alias="CRAWLAB_BASE_URL",
    )
    crawlab_token: str | None = Field(
        default=None,
        validation_alias="CRAWLAB_TOKEN",
    )
    app_port: int = Field(
        default=DEFAULT_APP_PORT,
        validation_alias="APP_PORT",
        ge=1,
        le=65535,
    )
    db_path: Path = Field(
        default=DEFAULT_DB_PATH,
        validation_alias="CL_MONITORING_DB_PATH",
    )

    @field_validator("crawlab_base_url")
    @classmethod
    def _normalize_base_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_crawlab_base_url(value)

    @field_validator("crawlab_token")
    @classmethod
    def _normalize_token(cls, value: str | None) -> str | None:
        return normalize_crawlab_token(value)

    @field_validator("db_path")
    @classmethod
    def _resolve_db_path(cls, value: Path) -> Path:
        return value.expanduser().resolve(strict=False)

    @property
    def app_host(self) -> str:
        return DEFAULT_APP_HOST

    @property
    def runtime_mode(self) -> RuntimeMode:
        has_base_url = self.crawlab_base_url is not None
        has_token = self.crawlab_token is not None
        if has_base_url and has_token:
            return RuntimeMode.LIVE
        if not has_base_url and not has_token:
            return RuntimeMode.SQLITE_ONLY

        missing = []
        if not has_base_url:
            missing.append("CRAWLAB_BASE_URL")
        if not has_token:
            missing.append("CRAWLAB_TOKEN")
        missing_display = ", ".join(missing)
        raise RuntimeConfigurationError(
            "Partial live configuration: set both CRAWLAB_BASE_URL and "
            "CRAWLAB_TOKEN for live mode, or leave both unset for SQLite-only "
            f"mode. Missing: {missing_display}"
        )

    @property
    def live_base_url(self) -> str:
        if self.runtime_mode is not RuntimeMode.LIVE or self.crawlab_base_url is None:
            raise RuntimeConfigurationError("Live Crawlab base URL is not configured")
        return self.crawlab_base_url

    @property
    def live_token(self) -> str:
        if self.runtime_mode is not RuntimeMode.LIVE or self.crawlab_token is None:
            raise RuntimeConfigurationError("Live Crawlab token is not configured")
        return self.crawlab_token


def normalize_crawlab_base_url(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    if normalized.endswith("/api"):
        normalized = normalized[:-4]
    normalized = normalized.rstrip("/")
    if not normalized:
        raise ValueError("CRAWLAB_BASE_URL must not be empty")
    return normalized


def normalize_crawlab_token(token: str | None) -> str | None:
    if token is None:
        return None
    normalized = token.strip()
    return normalized or None


def build_runtime_settings(
    *,
    crawlab_base_url: str | None = None,
    crawlab_token: str | None = None,
    app_port: int = DEFAULT_APP_PORT,
    db_path: str | Path = DEFAULT_DB_PATH,
) -> RuntimeSettings:
    return RuntimeSettings.model_construct(
        crawlab_base_url=(
            normalize_crawlab_base_url(crawlab_base_url)
            if crawlab_base_url is not None
            else None
        ),
        crawlab_token=normalize_crawlab_token(crawlab_token),
        app_port=app_port,
        db_path=Path(db_path).expanduser().resolve(strict=False),
    )


def load_settings(*, env_file: str | Path | None = None) -> RuntimeSettings:
    if env_file is None:
        return RuntimeSettings()
    return RuntimeSettings(_env_file=env_file)  # type: ignore[call-arg]
