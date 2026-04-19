# Structured Spider Markers

## Purpose

This document defines a minimal additive marker contract that spiders can emit
into their own logs so future tooling can parse run lifecycle with less
guesswork.

The contract only defines emitted log lines. It does not assume spider source
code lives in this repository and it does not require runtime app changes in
this planning thread.

## Scope And Non-Goals

- Markers are additive to existing human-readable logs.
- Markers may be emitted from an external spider repository, a shared spider
  helper, or a launch wrapper.
- Adoption is pilot-first. Only selected `execution_key` values need markers at
  the start.
- The contract does not require a rewrite of all spiders.
- Markers do not replace Crawlab task metadata or local task normalization.

## Line Format

- Each marker is one single-line JSON object inside normal stdout/stderr logs.
- All markers use the same envelope:

```json
{"kind":"clm_spider_marker","marker":"RUN_START","v":1,"ts":"2026-04-19T08:00:00Z"}
```

- Required fields:
  - `kind`: exact constant `clm_spider_marker`
  - `marker`: one of `RUN_START`, `HEARTBEAT`, `RUN_END`
  - `v`: exact integer `1`
  - `ts`: RFC3339 UTC timestamp with literal `Z`
- Optional shared fields:
  - `profile`: short string for anti-bot/profile mode when that distinction
    matters
  - `counters`: object with integer values only
  - `outcome`: terminal producer outcome for `RUN_END`
  - `reason_code`: stable snake_case producer-side cause for `RUN_END`
- Pilot rollout is selected per `execution_key`, but markers themselves do not
  need to emit `execution_key`. The observer already gets that from task
  metadata and task-log context.
- Markers must never include tokens, cookies, auth headers, full payload dumps,
  or other secrets.

## Marker Types

### `RUN_START`

- Emit once after spider-specific initialization succeeds and before the main
  fetch loop starts.
- Required fields: envelope only.
- Recommended field: `profile` when the spider can run under different
  anti-bot or environment modes.

Example:

```json
{"kind":"clm_spider_marker","marker":"RUN_START","v":1,"ts":"2026-04-19T08:00:00Z","profile":"default"}
```

### `HEARTBEAT`

- Emit zero or more times while useful work is still live.
- Required fields: envelope only.
- Recommended fields: `counters` with cumulative integer progress values and
  `profile` if the active profile matters.
- `counters` values must be integers and monotonic within a run. Omit unknown
  counters instead of sending `null`.

Example:

```json
{"kind":"clm_spider_marker","marker":"HEARTBEAT","v":1,"ts":"2026-04-19T08:05:00Z","counters":{"item_events":120,"put_to_parser":45}}
```

### `RUN_END`

- Emit once on graceful terminal paths.
- Required fields: envelope plus `outcome` and `reason_code`.
- Recommended fields: `counters`, `profile`.
- `outcome` values for v1:
  - `success`
  - `partial_success`
  - `rule_stopped`
  - `cancelled`
  - `failed`
  - `unknown`
- `success_probable` is intentionally excluded from producer markers. It
  remains an inference state for the tool, not a producer-asserted fact.
- `reason_code` must be a stable snake_case producer-side cause, for example
  `completed`, `auto_stop`, `ban_429`, `cancelled_by_supervisor`, or
  `unhandled_exception`.

Example:

```json
{"kind":"clm_spider_marker","marker":"RUN_END","v":1,"ts":"2026-04-19T08:12:34Z","outcome":"failed","reason_code":"ban_429","profile":"residential","counters":{"item_events":120,"put_to_parser":45}}
```

## Contract Rules

- At most one `RUN_START` and one `RUN_END` are expected per task log.
- Duplicate `RUN_START` or `RUN_END` markers are a producer defect and must be
  treated as rollout evidence, not silently normalized away.
- Missing `HEARTBEAT` is allowed for short runs.
- Missing `RUN_END` is allowed when the process crashes or the log is
  truncated. Consumers must then stay conservative and fall back to existing
  task/log evidence.
- Marker timestamps must be UTC. Local timezone strings and timezone-less
  timestamps are out of contract.
- Marker keys and enum values are ASCII only.
- Extra fields may be emitted, but pilot consumers must ignore unknown keys
  until a later contract revision adopts them explicitly.

## Why This Is Minimal

- `RUN_START` proves that spider code, not just the worker shell, began useful
  work.
- `HEARTBEAT` provides liveness and optional progress without forcing one
  global counter schema.
- `RUN_END` gives one explicit producer-side terminal outcome and cause.
- The contract is small enough to add in external spider repositories or
  wrappers without rewriting every spider.
