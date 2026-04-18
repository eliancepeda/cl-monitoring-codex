# ADR 0003: Minimal Dashboard on Top of Local Truth Layer

**Status**: Accepted  
**Date**: 2026-04-19  

## Context

`T7` already fixed the v1 runtime boundary:

- live Crawlab reads happen only through `ReadonlyCrawlabClient`
- the poller writes normalized local truth into SQLite
- parser truth lives in `run_summaries`
- schedule-trouble truth is projected into `incidents`
- the browser must never call Crawlab directly

`T8` therefore does not need new runtime logic. It needs a minimal UI scope that
answers four operator questions from local SQLite only:

- what is running
- what is broken
- what is overdue
- what recovered recently

The existing web layer is intentionally still a stub:

- `src/cl_monitoring/web/routes.py`
- `src/cl_monitoring/web/templates/base.html`
- `src/cl_monitoring/app.py`

The dashboard must stay aligned with project non-goals:

- no run/restart/cancel actions
- no direct Crawlab calls from browser code
- no clone of Crawlab UI
- no graphs just to look analytical
- no dense filter surface
- no settings/configuration through UI

## Decision

### 1. v1 dashboard is exactly three server-rendered screens

Routes for `T9`:

- `GET /` -> Project board
- `GET /spiders/{spider_id}` -> Spider detail
- `GET /incidents` -> Incidents

All screens are full-page Jinja2 renders. Routes read only from `LocalRepository`
and pass typed view models to templates. Templates do not fetch JSON, do not use
HTMX, and do not call Crawlab.

No schema change is required for `T9`. The UI layer should add read-oriented repo
methods and view dataclasses on top of the existing SQLite tables.

### 2. Project board

Purpose:

- one local landing page grouped by `project_id`
- answer which spiders are currently active, which have open trouble, which have
  overdue schedule problems, and which recovered recently

Screen shape:

- grouped by `project_id`
- one compact row per `spider_id`
- link from each row to `GET /spiders/{spider_id}`
- open critical items sort first, then active spiders, then the rest
- recent recovery marker uses a fixed 24-hour window

SQLite queries needed:

1. Spider base rows

```sql
SELECT spider_id, name, project_id
FROM spiders
ORDER BY project_id, name, spider_id;
```

2. Active tasks per spider

```sql
SELECT spider_id,
       COUNT(*) AS active_task_count,
       MIN(COALESCE(start_ts, create_ts, last_seen_at)) AS oldest_active_ts,
       MAX(COALESCE(start_ts, create_ts, last_seen_at)) AS newest_active_ts
FROM task_snapshots
WHERE status IN ('pending', 'running')
GROUP BY spider_id;
```

3. Open incidents per spider, split by task vs schedule scope

```sql
SELECT spider_id,
       SUM(CASE WHEN entity_type = 'task' THEN 1 ELSE 0 END) AS open_task_issues,
       SUM(CASE WHEN entity_type = 'schedule' THEN 1 ELSE 0 END) AS open_schedule_issues,
       MAX(CASE WHEN severity = 'critical' THEN 2 WHEN severity = 'warning' THEN 1 ELSE 0 END)
           AS worst_open_severity_rank,
       MAX(last_seen_at) AS latest_incident_seen_at
FROM (
    SELECT i.entity_type, i.severity, i.last_seen_at, t.spider_id
    FROM incidents AS i
    JOIN task_snapshots AS t
      ON i.entity_type = 'task' AND i.entity_id = t.task_id
    WHERE i.closed_at IS NULL

    UNION ALL

    SELECT i.entity_type, i.severity, i.last_seen_at, s.spider_id
    FROM incidents AS i
    JOIN schedules AS s
      ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
    WHERE i.closed_at IS NULL
) AS scoped_incidents
GROUP BY spider_id;
```

4. Latest terminal run per spider

```sql
WITH ranked AS (
    SELECT t.spider_id,
           t.task_id,
           rs.run_result,
           rs.confidence,
           rs.reason_code,
           COALESCE(t.end_ts, t.start_ts, t.create_ts, t.last_seen_at) AS sort_ts,
           ROW_NUMBER() OVER (
               PARTITION BY t.spider_id
               ORDER BY COALESCE(t.end_ts, t.start_ts, t.create_ts, t.last_seen_at) DESC,
                        t.task_id DESC
           ) AS rn
    FROM task_snapshots AS t
    LEFT JOIN run_summaries AS rs ON rs.task_id = t.task_id
    WHERE t.status NOT IN ('pending', 'running')
)
SELECT spider_id, task_id, run_result, confidence, reason_code, sort_ts
FROM ranked
WHERE rn = 1;
```

5. Recently recovered schedules per spider from closed incidents

```sql
SELECT s.spider_id,
       COUNT(*) AS recent_recovery_count,
       MAX(i.closed_at) AS latest_recovery_at
FROM incidents AS i
JOIN schedules AS s
  ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
WHERE i.closed_at IS NOT NULL
  AND i.closed_at >= ?
GROUP BY s.spider_id;
```

Mandatory row fields:

These fields are always present in the row shape, but time/result fields may be
`NULL` when a spider has not yet produced that kind of local evidence.

- `project_id`
- `spider_id`
- `spider_name`
- `active_task_count`
- `open_task_issues`
- `open_schedule_issues`
- `worst_open_severity`
- `latest_terminal_task_id`
- `latest_terminal_run_result`
- `latest_terminal_reason_code`
- `latest_terminal_at`
- `recent_recovery_count`
- `latest_recovery_at`

Evidence blocks shown:

- open-issue preview: first 2-3 evidence strings from the worst open incident for
  that spider
- latest-run preview: first 2-3 evidence strings from the latest terminal
  `run_summaries.evidence_json` when a summary exists
- recent-recovery preview: latest closed schedule incident reason plus `closed_at`

Board evidence stays summary-only. It never expands into raw logs.

### 3. Spider detail

Purpose:

- answer everything meaningful for one spider without opening Crawlab
- show active runs, recent outcomes, current schedule trouble, and recent
  recoveries in one place

Screen shape:

- small spider header
- active runs section
- schedules section
- recent runs section
- recent recoveries section
- recent recoveries are limited to the latest 5 closed schedule incidents

SQLite queries needed:

1. Spider header

```sql
SELECT spider_id, name, project_id, col_id, cmd, param
FROM spiders
WHERE spider_id = ?;
```

2. Active runs for the spider

```sql
SELECT t.task_id,
       t.status,
       t.schedule_id,
       t.is_manual,
       t.execution_key,
       t.create_ts,
       t.start_ts,
       t.runtime_ms,
       t.last_seen_at,
       rs.run_result,
       rs.confidence,
       rs.reason_code,
       rs.evidence_json
FROM task_snapshots AS t
LEFT JOIN run_summaries AS rs ON rs.task_id = t.task_id
WHERE t.spider_id = ?
  AND t.status IN ('pending', 'running')
ORDER BY COALESCE(t.start_ts, t.create_ts, t.last_seen_at) DESC, t.task_id DESC;
```

3. Recent runs for the spider

```sql
SELECT t.task_id,
       t.status,
       t.schedule_id,
       t.is_manual,
       t.execution_key,
       t.create_ts,
       t.start_ts,
       t.end_ts,
       t.runtime_ms,
       rs.run_result,
       rs.confidence,
       rs.reason_code,
       rs.evidence_json,
       rs.counters_json
FROM task_snapshots AS t
LEFT JOIN run_summaries AS rs ON rs.task_id = t.task_id
WHERE t.spider_id = ?
ORDER BY COALESCE(t.end_ts, t.start_ts, t.create_ts, t.last_seen_at) DESC, t.task_id DESC
LIMIT 20;
```

4. Schedules for the spider with current open issue and latest recovery markers

```sql
SELECT s.schedule_id,
       s.name,
       s.cron,
       s.enabled,
       open_i.severity AS open_severity,
       open_i.reason_code AS open_reason_code,
       open_i.evidence_json AS open_evidence_json,
       open_i.last_seen_at AS open_last_seen_at,
       closed_i.closed_at AS latest_closed_at,
       closed_i.reason_code AS latest_closed_reason_code,
       last_task.last_scheduled_ts
FROM schedules AS s
LEFT JOIN incidents AS open_i
  ON open_i.entity_type = 'schedule'
 AND open_i.entity_id = s.schedule_id
 AND open_i.closed_at IS NULL
LEFT JOIN (
    SELECT entity_id, closed_at, reason_code
    FROM (
        SELECT i.entity_id,
               i.closed_at,
               i.reason_code,
               ROW_NUMBER() OVER (
                   PARTITION BY i.entity_id
                   ORDER BY i.closed_at DESC, i.incident_id DESC
               ) AS rn
        FROM incidents AS i
        WHERE i.entity_type = 'schedule' AND i.closed_at IS NOT NULL
    )
    WHERE rn = 1
) AS closed_i ON closed_i.entity_id = s.schedule_id
LEFT JOIN (
    SELECT schedule_id,
           MAX(COALESCE(end_ts, start_ts, create_ts, last_seen_at)) AS last_scheduled_ts
    FROM task_snapshots
    WHERE spider_id = ? AND is_manual = 0
    GROUP BY schedule_id
) AS last_task ON last_task.schedule_id = s.schedule_id
WHERE s.spider_id = ?
ORDER BY s.enabled DESC, s.name, s.schedule_id;
```

5. Recent recoveries for the spider

```sql
SELECT i.incident_id,
       i.entity_type,
       i.entity_id,
       i.execution_key,
       i.reason_code,
       i.evidence_json,
       i.opened_at,
       i.closed_at
FROM incidents AS i
JOIN schedules AS s
  ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
WHERE s.spider_id = ?
  AND i.closed_at IS NOT NULL
ORDER BY i.closed_at DESC, i.incident_id DESC
LIMIT 5;
```

Mandatory fields:

These fields are always present in the screen view-model shapes, but current/open
issue and recovery timestamps may be `NULL` when the local history does not yet
contain those observations.

- spider header: `spider_id`, `name`, `project_id`, `col_id`, `cmd`, `param`
- active run row: `task_id`, `status`, `schedule_id`, `is_manual`,
  `execution_key`, `start_ts`, `runtime_ms`, `run_result`, `reason_code`
- schedule row: `schedule_id`, `name`, `cron`, `enabled`, `open_severity`,
  `open_reason_code`, `open_last_seen_at`, `latest_closed_at`,
  `last_scheduled_ts`
- recent run row: `task_id`, `status`, `schedule_id`, `is_manual`, `start_ts`,
  `end_ts`, `runtime_ms`, `run_result`, `confidence`, `reason_code`
- recovery row: `incident_id`, `entity_type`, `entity_id`, `reason_code`,
  `opened_at`, `closed_at`

Evidence blocks shown:

- active run evidence: `run_summaries.evidence_json` preview for active tasks if a
  parser summary already exists
- schedule issue evidence: `incidents.evidence_json` preview for the currently open
  schedule incident
- recent run evidence: `run_summaries.evidence_json` and compact counters preview
- recovery evidence: the closed schedule incident evidence; if a later successful
  run exists, its `run_summaries.evidence_json` may be shown beneath as supporting
  context, but incident closure remains the recovery truth

### 4. Incidents

Purpose:

- dedicated page for what is broken now and what recovered recently
- open incidents first, recently closed incidents second
- no incident acknowledgement workflow in v1
- recently closed means the last 7 days

Recovery semantics in v1:

- recovery is represented by `incidents.closed_at`
- in practice, the useful recovery rows are schedule incidents
- failed task incidents remain historical bad-run facts keyed by `task_id`; they
  do not become "recovered task rows" later

SQLite queries needed:

1. Open incident feed with spider context

```sql
SELECT i.incident_id,
       i.entity_type,
       i.entity_id,
       i.execution_key,
       i.severity,
       i.reason_code,
       i.evidence_json,
       i.opened_at,
       i.last_seen_at,
       i.closed_at,
       COALESCE(t.spider_id, s.spider_id) AS spider_id,
       sp.name AS spider_name
FROM incidents AS i
LEFT JOIN task_snapshots AS t
  ON i.entity_type = 'task' AND i.entity_id = t.task_id
LEFT JOIN schedules AS s
  ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
LEFT JOIN spiders AS sp
  ON sp.spider_id = COALESCE(t.spider_id, s.spider_id)
WHERE i.closed_at IS NULL
ORDER BY CASE i.severity WHEN 'critical' THEN 0 ELSE 1 END,
         i.last_seen_at DESC,
         i.incident_id DESC;
```

2. Recently closed incident feed with spider context

```sql
SELECT i.incident_id,
       i.entity_type,
       i.entity_id,
       i.execution_key,
       i.severity,
       i.reason_code,
       i.evidence_json,
       i.opened_at,
       i.last_seen_at,
       i.closed_at,
       COALESCE(t.spider_id, s.spider_id) AS spider_id,
       sp.name AS spider_name
FROM incidents AS i
LEFT JOIN task_snapshots AS t
  ON i.entity_type = 'task' AND i.entity_id = t.task_id
LEFT JOIN schedules AS s
  ON i.entity_type = 'schedule' AND i.entity_id = s.schedule_id
LEFT JOIN spiders AS sp
  ON sp.spider_id = COALESCE(t.spider_id, s.spider_id)
WHERE i.closed_at IS NOT NULL
  AND i.closed_at >= ?
ORDER BY i.closed_at DESC,
         i.incident_id DESC;
```

Mandatory fields:

- `incident_id`
- `entity_type`
- `entity_id`
- `spider_id`
- `spider_name`
- `execution_key`
- `severity`
- `reason_code`
- `opened_at`
- `last_seen_at`
- `closed_at`

Evidence blocks shown:

- incident evidence: first 3 evidence strings from `incidents.evidence_json`
- task context: latest `run_summaries.evidence_json` for that `task_id` when the
  incident is task-scoped
- schedule context: latest scheduled run timestamp and latest closed incident time
  when the incident is schedule-scoped

The incidents page is the only screen that shows closed incidents directly. That
is how v1 answers "what recovered" without inventing a second recovery model.

### 5. Repository boundary for T9

Routes should not compose these screens directly from ad hoc SQL or raw sqlite
rows. `T9` should extend `LocalRepository` with explicit read methods and typed
result rows for dashboard use.

Minimum new repo methods:

- `list_project_board_rows(recovered_since: datetime) -> list[ProjectBoardRow]`
- `get_project_board_evidence(spider_ids: list[str]) -> dict[str, BoardEvidence]`
- `get_spider_header(spider_id: str) -> SpiderHeader | None`
- `list_spider_active_runs(spider_id: str) -> list[SpiderActiveRunRow]`
- `list_spider_recent_runs(spider_id: str, limit: int = 20) -> list[SpiderRunRow]`
- `list_spider_schedules(spider_id: str) -> list[SpiderScheduleRow]`
- `list_spider_recent_recoveries(spider_id: str, limit: int = 5) -> list[RecoveryRow]`
- `list_open_incident_rows() -> list[IncidentFeedRow]`
- `list_recent_closed_incident_rows(closed_since: datetime) -> list[IncidentFeedRow]`

The web layer reads only these repo methods. It does not call the poller, parser,
schedule engine, or Crawlab client directly.

### 6. Explicit prohibitions

- No action buttons for run, restart, cancel, retry, resolve, or edit.
- No direct Crawlab calls from browser JS, templates, or browser-side fetches.
- No graphs that merely restate counts without adding operational evidence.
- No more than the built-in screen split and URL path; no dense multi-filter panel.
- No configuration through UI for poll cadence, scope, thresholds, or tokens.
- No attempt to clone Crawlab task tables, node views, or operational controls.
- No use of `spider_profiles` in the first dashboard pass.

## Consequences

- `T9` can stay entirely inside repo reads, FastAPI routes, app wiring, and Jinja
  templates.
- The dashboard remains evidence-first and local-only.
- "What is running" comes from active `task_snapshots`.
- "What is broken" comes from open task and schedule incidents plus latest bad run
  summaries.
- "What is overdue" comes from open schedule incidents projected by the existing
  schedule engine path.
- "What recovered" comes from closed schedule incidents, surfaced on the board,
  spider detail, and incidents page.
- v1 does not add a new truth layer, a new incident engine, or any write path.
