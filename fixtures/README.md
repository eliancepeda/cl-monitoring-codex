# Fixtures

Anonymized test data captured from live Crawlab instance.

## Structure

- `api/` — Raw JSON responses from Crawlab API (anonymized)
- `expected/` — Expected normalization results
- `logs/` — Sample log fragments

## Rules

- **Never** store real tokens, credentials, or PII.
- Keep IDs if they help model the shape, unless they are sensitive.
- Every new parser rule requires at least one fixture and one test (AGENTS.md § Workflow).
- Prefer fixtures over Crawlab docs when payloads differ (AGENTS.md § Truth sources).
- Preserve sentinel values like zero ObjectId and zero time.
- Do not rewrite payload structure.

## Purpose
Fixtures are the source of truth for:
- API shape
- parser behavior
- normalizers
- run classification
- regression tests
