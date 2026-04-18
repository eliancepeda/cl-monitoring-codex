"""Thin runtime re-export of the single readonly Crawlab client."""

from integrations.crawlab.readonly_client import (
    PathNotAllowedError,
    ReadonlyCrawlabClient,
    ReadonlyViolationError,
)

__all__ = [
    "PathNotAllowedError",
    "ReadonlyCrawlabClient",
    "ReadonlyViolationError",
]
