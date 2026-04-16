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

## Non-goals for v1

- No run/restart/cancel actions.
- No multi-user auth.
- No direct clone of Crawlab UI.
- No nodes logic until task/schedule/parser logic is stable.