# ADR 0001: Local Single-User Read-Only Companion Architecture

**Status**: Accepted  
**Date**: 2026-04-17  

## Context

We are building a local monitoring layer for a live Crawlab instance that must stay strictly read-only and deterministic. We need a clear architectural boundary between Crawlab as the orchestration system and this companion app as the local truth layer for normalized task, schedule, and log-derived status data.

## Decisions

### 1. Project Scope: Local Single-User Read-Only

We will build the application as a strictly read-only, local companion app that binds to `127.0.0.1`.

All Crawlab access must go through a single `ReadonlyCrawlabClient`. The browser/UI must never call Crawlab directly.

**Why:**
- **Safety**: Crawlab manages live production tasks. Enforcing a strict read-only policy via GET-only interfaces ensures zero risk of accidentally running, restarting, or canceling tasks.
- **Safety Boundary**: A single client can enforce GET-only behavior, allowlisted paths, and relative-path-only access so the app cannot drift into host-mismatch or non-approved endpoint usage.
- **Security**: Ensures live tokens, auth headers, and cookies are never stored, leaked, or exposed over external networks.
- **Simplicity**: By isolating the app to local developer use, we eliminate the need for multi-user authentication, login flows, or a complex UI clone. 

### 2. Crawlab as Orchestration Source, Not Analytics Source

Crawlab will be treated purely as an orchestration engine (job scheduling, execution queueing) rather than a source of analytical truth.

**Why:**
- Crawlab's internal metadata-based success signals (e.g. task status = "finished") can be unreliable or lack domain specificity.
- Granular task validation requires deterministic rule-based log classification. No LLM logic is allowed in production runtime classification. We use Crawlab to provide the raw signals, but run independent deterministic extraction locally to derive real status and history.

### 3. Handling Live Payload Characteristics

Based on the raw fixtures collected, our integration layer must accommodate specific Crawlab API behaviors:
- **Zero Schedule ID**: Manual runs are indicated by the zero-id value `"000000000000000000000000"`. Raw fixtures preserve it; domain normalization treats it as zero-id / null semantics while still using it for deterministic manual-run detection.
- **Zero Timestamps**: Unset or empty times are represented by `"0001-01-01T00:00:00Z"`.
- **Null Time Semantics**: Domain normalization treats the zero timestamp as `null time`, not as a synthetic minimum datetime.
- **Running Task Duration**: The metrics for `stat.runtime_duration` and `stat.total_duration` both equal `0` when a task status is `running`. Live runtime must be calculated locally via `now - start_ts`.
- **Execution Key**: Logical jobs are often triggered independently of a specific schedule. Therefore, we compute an `execution_key = spider_id + normalized cmd + normalized param` to identify and group similar executions contextually. Keep `schedule_id` isolated from this key.
- **Results Data Query**: Task results must be fetched by querying `/api/results/{col_id}` with a filter condition on `_tid` matching the exact task id.
- **Results Data Shape**: When Crawlab returns `{"data": null}` for results queries, the integration layer normalizes it to `[]` so downstream code gets a stable iterable shape.

### 4. Allowed Crawlab Surface

The read-only client may access only the following GET routes:
- `/api/tasks`
- `/api/tasks/{id}/logs`
- `/api/spiders/{id}`
- `/api/schedules`
- `/api/schedules/{id}`
- `/api/results/{col_id}`

### 5. Schedule and Timezone Risks

There are inherent risks in timeline prediction:
- **Timezone Interpretations**: Crawlab evaluates CRON schedules assuming its internal server timezone context. Computing the "next expected run" natively in our application runs a high risk of SLA false-positives if the local app timezone or Cron parsing library does not perfectly align with the Crawlab server's timezone settings. No naive CRON estimations should be used without locking down the offset.

### 6. Domain Model Entities

To support this read-only tracking and aggregation layer, the Domain Model must incorporate the following conceptual entities:
- **Spider**: Base definition of the crawler script.
- **Execution**: The logical boundary defining what is being run, mapping to the `execution_key` (`spider_id` + normalized `cmd` + normalized `param`).
- **Schedule**: The recurring timer definition, optionally attached to an Execution.
- **Task**: An individual execution instance of a task tracking state, runtime history, and its success trajectory.
- **LogMarker / Event**: Extracted success, error, or tracking markers derived from parsing real task logs for classification.
- **TaskResult**: The actual parsed entities or aggregated data stats queried via `col_id` + `_tid`.
