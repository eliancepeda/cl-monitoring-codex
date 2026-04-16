"""Tests for ReadonlyCrawlabClient.

Must verify:
- Only GET requests are permitted.
- POST/PUT/PATCH/DELETE raise an error.
- Auth header is never leaked into fixtures or logs.
"""
