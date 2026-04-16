# Project purpose

Local single-user read-only companion app for Crawlab.
The app is only for the owner's own spiders/projects.
It must never perform write operations against Crawlab.

## Safety

- Never send non-GET requests to Crawlab.
- All Crawlab access must go through ReadonlyCrawlabClient.
- Never use login flows, cookies, or browser automation against live Crawlab.
- Never store live tokens in repo, fixtures, logs, screenshots, or tests.
- The browser UI must never call Crawlab directly.
- The app must bind only to 127.0.0.1 by default.

## Truth sources

- Prefer saved fixtures from the live instance over public docs when payloads differ.
- Treat Crawlab docs as secondary to live Network/HAR evidence from this instance.

## Domain rules

- Normalize "000000000000000000000000" as zero-id / null semantics.
- Normalize "0001-01-01T00:00:00Z" as null time.
- Detect manual runs by zero schedule_id.
- execution_key = spider_id + normalized cmd + normalized param.
- Keep schedule_id separate from execution_key.
- For running tasks, compute live runtime as now - start_at when runtime_duration is zero.
- results data may come from results/{col_id} filtered by _tid.

## Workflow

- Use Plan before Build for new milestones.
- Keep one thread per coherent task.
- Every new parser rule requires at least one anonymized fixture and one test.
- Runtime classification must be deterministic; no LLM in production decision logic.
- Record architecture decisions in DECISIONS.md:
  - One entry = one decision; keep it short.
  - Include: context, decision, why, consequences, related files.
  - If obsolete — mark Status: Superseded, do not delete.

## Non-goals for v1

- No run/restart/cancel actions.
- No multi-user auth.
- No direct clone of Crawlab UI.
- No nodes logic until task/schedule/parser logic is stable.

## Fixture collector rules

This repository includes a fixture collection workflow for a local read-only Crawlab companion app.

### Safety
- Never send non-GET requests to Crawlab.
- All Crawlab access must go through ReadonlyCrawlabClient.
- Never use login flows, cookies, browser automation, or raw curl against live Crawlab.
- Never store live tokens, auth headers, or cookies in repo files.
- Raw live responses must be written only to `fixtures_raw_local/`, which is gitignored.
- Redacted fixtures must be written only to `fixtures/`.

### Live Crawlab scope
- Allowed endpoints:
  - `/api/tasks`
  - `/api/tasks/{id}/logs`
  - `/api/spiders/{id}`
  - `/api/schedules`
  - `/api/schedules/{id}`
  - `/api/results/{col_id}`
- Path allowlist and GET-only policy are mandatory.

### Domain specifics
- Preserve zero ObjectId `000000000000000000000000`.
- Preserve zero time `0001-01-01T00:00:00Z`.
- Detect manual tasks by zero `schedule_id`.
- Build `execution_key` from `spider_id + normalized cmd + normalized param`.
- Keep `schedule_id` separate from `execution_key`.
- Task data may come through `results/{col_id}` filtered by `_tid`.

### Fixture pack output
- Build both raw and redacted fixture sets.
- Generate `fixtures/manifest.md`.
- Generate draft `fixtures/expected/*.yaml` for log fixtures.
- Sampling must prefer representative examples over volume.

### Non-goals
- No run/restart/cancel.
- No nodes collection in v1 unless required by an already selected scenario.
- No UI work before fixtures are collected.