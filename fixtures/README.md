# Fixtures

Anonymized test data captured from live Crawlab instance.

## Structure

- `api/` — Redacted JSON responses from Crawlab API
- `expected/` — Expected normalization results (draft skeletons, verify before use)
- `logs/` — Redacted log fragments from task execution

## Rules

- **Never** store real tokens, credentials, or PII.
- Keep IDs if they help model the shape, unless they are sensitive.
- Every new parser rule requires at least one fixture and one test (AGENTS.md § Workflow).
- Prefer fixtures over Crawlab docs when payloads differ (AGENTS.md § Truth sources).
- Preserve sentinel values like zero ObjectId and zero time.
- Do not rewrite payload structure.

## Sentinel Values

| Value | Meaning |
|-------|---------|
| `000000000000000000000000` | Zero ObjectId — null/missing reference |
| `0001-01-01T00:00:00Z` | Zero time — null/missing timestamp |

## Placeholder Scheme

Redacted values use stable indexed placeholders:

| Field type | Pattern | Example |
|------------|---------|---------|
| `_id` | `ID_NNN` | `ID_001` |
| `spider_id` | `SPIDER_ID_NNN` | `SPIDER_ID_001` |
| `schedule_id` | `SCHEDULE_ID_NNN` | `SCHEDULE_ID_002` |
| `node_id` | `NODE_ID_NNN` | `NODE_ID_001` |
| `user_id` | `USER_ID_NNN` | `USER_ID_001` |
| Hostnames | `HOST_NNN` | `HOST_001` |
| Unix usernames | `USER_NNN` | `USER_001` |

The same real value always maps to the same placeholder within a collection run.

## Purpose

Fixtures are the source of truth for:
- API response shape
- Parser behavior
- Normalizers
- Run classification
- Regression tests

## Collecting Fixtures

```bash
# Dry run — see what would be collected
python -m tools.collect_fixtures --dry-run

# Full collection
python -m tools.collect_fixtures --collect

# Refresh (skip existing)
python -m tools.collect_fixtures --refresh --skip-existing
```

See `manifest.md` for the full fixture inventory after collection.
