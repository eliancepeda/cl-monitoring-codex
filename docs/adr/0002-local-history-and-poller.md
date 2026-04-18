# ADR 0002: Local History and Incremental Poller

**Status**: Accepted  
**Date**: 2026-04-19  

## Context

`T4` and `T5` already fixed the runtime truth layer:

- `parse_crawllib_default` produces a frozen `RunSummary` from a normalized
  `TaskSnapshot`, ordered log input, and an explicit completeness flag.
- `ScheduleEngine` produces a frozen `ScheduleHealth` from normalized
  schedules, task history, optional run summaries, and `now`.

`T7` now needs a minimal persistence and polling architecture that:

- keeps Crawlab access read-only and GET-only
- survives process restart without rereading every active log from page 1
- keeps UI fully local and independent from live Crawlab polling
- does not introduce a second Crawlab client or a second runtime truth layer

The project constraints remain fixed:

- all Crawlab access must go through `ReadonlyCrawlabClient`
- the browser UI must never call Crawlab directly
- v1 stays inside the existing non-goals: no run/restart/cancel, no nodes,
  no extra UI scope, no second analytics layer

## Decision

### 1. One live ingress, one local truth path

The poller is the only runtime component that reads live Crawlab data.

Runtime data flow for v1 is:

`ReadonlyCrawlabClient` -> normalizers -> SQLite tables ->
`parse_crawllib_default` -> `run_summaries` -> `ScheduleEngine` ->
`incidents`

Consequences:

- UI reads only from local SQLite.
- UI never calls Crawlab directly.
- `src/cl_monitoring/crawlab/client.py` must not become a second client.
  It may only be removed, re-export the existing readonly client, or wrap it
  as a thin adapter with no independent transport logic.

### 2. Persist normalized task history, not raw task payloads

v1 uses `task_snapshots`, not `tasks_raw`.

Why:

- The runtime parser and schedule engine already consume normalized domain
  objects, not raw Crawlab payloads.
- Storing normalized task state keeps the DB aligned with the frozen domain
  contract from `docs/domain/status-parser-contract.md`.
- A raw task table would create a second persistence shape without helping
  the existing parser/status pipeline.

Raw live log text is persisted only as local cursor state needed for
incremental assembly and restart safety.

### 3. Minimal v1 tables

#### `spiders`

Purpose:

- current normalized spider metadata for local joins and later UI reads

Minimum content:

- primary key: `spider_id`
- normalized `SpiderSnapshot` fields:
  `name`, `col_id`, `project_id`, `cmd`, `param`
- local sync metadata: `last_seen_at`

Notes:

- This is the latest local view, not an append-only audit log.

#### `schedules`

Purpose:

- current normalized schedule metadata for schedule-chain evaluation

Minimum content:

- primary key: `schedule_id`
- normalized `ScheduleSnapshot` fields:
  `name`, `spider_id`, `cron`, `cmd`, `param`, `enabled`
- local sync metadata: `last_seen_at`

Notes:

- This is the latest local view, not a cron analytics table.

#### `task_snapshots`

Purpose:

- local history of normalized task runs keyed by Crawlab task id
- shared upstream input for both parser and schedule engine

Minimum content:

- primary key: `task_id`
- normalized `TaskSnapshot` fields:
  `spider_id`, `schedule_id`, `status`, `cmd`, `param`, `create_ts`,
  `start_ts`, `end_ts`, `runtime_ms`, `is_manual`, `execution_key`
- local sync metadata:
  `first_seen_at`, `last_seen_at`, `terminal_seen_at`

Notes:

- One task run already has its own stable `task_id`, so storing one row per
  task preserves history across runs without a second history table.
- `schedule_id` stays separate from `execution_key` exactly as in the domain
  rules.

#### `task_log_cursors`

Purpose:

- restart-safe incremental log sync for tasks that still need log polling
- local cumulative ordered log state for deterministic reparsing

Minimum content:

- primary key and foreign key: `task_id`
- cursor state:
  `page_size`, `next_page`, `api_total_lines`, `assembled_line_count`
- local assembled log state:
  `assembled_log_text`
- completion state:
  `is_complete`, `final_sync_done`, `last_log_sync_at`, `terminal_seen_at`

Notes:

- v1 does not need a separate `task_logs` archive table.
- `assembled_log_text` exists because the runtime parser is defined over full
  ordered input plus `is_complete`; persisting the cumulative text lets the
  poller resume without rereading the entire active log.
- After final sync, the cursor row remains as local sync state, but the task
  leaves the hot log polling set.

#### `run_summaries`

Purpose:

- one current frozen `RunSummary` per task for UI and incident projection

Minimum content:

- primary key and foreign key: `task_id`
- frozen `RunSummary` fields:
  `execution_key`, `run_result`, `confidence`, `reason_code`
- serialized frozen payloads:
  `evidence_json`, `counters_json`
- local sync metadata: `parsed_at`

Notes:

- This table is fully derived from `task_snapshots` plus the assembled task log.
- Re-parsing the same task replaces the same row idempotently.

#### `incidents`

Purpose:

- local durable record of non-nominal parser/status outcomes
- preserves already-opened issues across restart without relying on UI memory

Minimum content:

- primary key: `incident_id`
- stable incident identity: `incident_key`
- scope fields:
  `entity_type`, `entity_id`, `execution_key`
- incident fields:
  `severity`, `reason_code`, `evidence_json`
- lifecycle fields:
  `opened_at`, `closed_at`, `last_seen_at`

Notes:

- `incidents` is a derived local projection from `run_summaries` and schedule
  outputs, not a third classification engine.
- Repeated observation of the same still-open problem updates
  `last_seen_at` instead of opening duplicate rows.
- Resolution closes the open row; it does not delete history.

#### `spider_profiles`

Purpose:

- local-only per-`execution_key` profile records for future profile rules and
  operator notes

Minimum content:

- primary key: `execution_key`
- relation fields: `spider_id`
- local profile payload:
  `profile_json`
- local sync metadata: `updated_at`

Notes:

- This table is not synced from Crawlab.
- It may stay empty in early v1 and still belongs in the schema because later
  rollout/profile work depends on a stable local place for these rules.

### 4. Polling cadence

v1 uses three cadences plus a terminal follow-up sync.

Default cadence:

- `spiders` and `schedules`: every 15 minutes
- `tasks`: every 30 seconds
- logs for locally known running tasks: every 10 seconds
- one immediate final log sync after a task is first observed in a terminal
  state

Semantics:

- Spider and schedule metadata change slowly, so they stay on a slow loop.
- Task polling is the main discovery loop and is faster than metadata polling.
- Running-task log polling is the hottest loop because runtime classification
  changes inside the same task while it is still active.
- A terminal task gets exactly one final log sync attempt after terminal state
  is observed. After that attempt, the task leaves the hot log polling set.

Final-sync rule:

- If task polling observes a transition from `pending` or `running` to a
  terminal state, the poller immediately performs one last log sync for that
  `task_id`.
- That final fetch uses the same incremental cursor path and writes the final
  `RunSummary`.
- After that write, `task_log_cursors.final_sync_done` becomes true even if the
  parser still classifies the result conservatively because the fetched log is
  incomplete.

### 5. Restart-safe incrementality

The poller must resume from local DB state, not from in-memory state.

Persisted resume state is:

- latest normalized task snapshot in `task_snapshots`
- incremental log cursor and cumulative text in `task_log_cursors`
- latest derived summary in `run_summaries`
- already opened issues in `incidents`

Resume rules:

- On startup, the poller reloads tasks whose latest local status is active, or
  whose `final_sync_done` flag is false.
- Log sync resumes from `task_log_cursors.next_page`, not from page 1.
- Resume uses a one-page overlap read and deduplicates against the already
  persisted `assembled_log_text` before advancing the cursor. This keeps the
  cursor safe against page-boundary drift while the task is still writing logs.
- Parser output is recomputed from the persisted cumulative ordered log text
  and then upserted into `run_summaries`.
- If the process stops after a task became terminal but before the final sync
  completed, restart sees `final_sync_done = false` and performs the pending
  final sync exactly once.

Monotonic local-state rules:

- `first_seen_at` is written once.
- `last_seen_at` only moves forward.
- `terminal_seen_at` is set on the first observed terminal state and does not
  move backward.
- `next_page` only moves forward after a successful merge.
- `assembled_line_count` only grows for active tasks.
- `final_sync_done` changes only from false to true.

This gives restart safety without normal full-log rereads.

### 6. Poller relationship to parser and schedule outputs

The poller does not invent new result shapes.

It must reuse the already frozen outputs:

- `task_snapshots` feeds `parse_crawllib_default`
- `task_snapshots`, `schedules`, and `run_summaries` feed `ScheduleEngine`
- `run_summaries` and schedule outputs feed `incidents`

Consequences:

- There is no separate poller-side result schema.
- There is no second parser contract in the DB layer.
- `reason_code`, `confidence`, `evidence`, and `counters` stay exactly aligned
  with the frozen contract from `T3`, `T4`, and `T5`.

## Explicit Prohibitions

- No direct live Crawlab access from browser code, templates, JS, or future UI
  routes.
- No second Crawlab HTTP client in `src/cl_monitoring/crawlab/client.py` or
  anywhere else in the runtime package.
- No poller logic that bypasses `ReadonlyCrawlabClient`.
- No DB design that makes raw task payloads a second runtime truth source.

## Consequences

- `T7` can implement SQLite schema, repository methods, and the poller without
  reopening parser/status architecture.
- Local SQLite becomes the only data source for future UI work.
- Restart safety comes from persisted cursor state and derived summaries, not
  from longer live Crawlab sessions.
- The local history layer stays minimal: normalized snapshots, derived
  summaries, derived incidents, and only as much log persistence as the
  incremental parser path requires.
