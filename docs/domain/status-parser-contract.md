# Status/Parser Contract

## Purpose

This document freezes the shared contract between the v1 runtime parser
and the v1 schedule engine before `T4` and `T5` diverge.

It is intentionally minimal. It fixes:

- input boundaries
- output shapes
- field semantics
- confidence and reason-code semantics
- which shared fields and files must not change in parallel

This document does not implement runtime logic.

## Truth Sources

- `AGENTS.md` is authoritative for safety and domain invariants.
- Saved fixtures and observed payload/log behavior are authoritative when
  they differ from public Crawlab docs.
- Public Crawlab docs are secondary.

Observed fixture facts used here:

- Manual runs are represented by zero `schedule_id`
  `000000000000000000000000` in `task_ID_748.json` and
  `task_ID_749.json`.
- Running tasks can have `runtime_duration == 0` while already started,
  as in `task_ID_753.json` and `task_ID_757.json`.
- Schedule chains already show real observed fire history by task
  `create_ts`, for example `SCHEDULE_ID_006` in `task_ID_750.json`,
  `task_ID_751.json`, and `task_ID_757.json`.
- The single-line log trailer
  `{'status': 'ok', 'message': 'success', 'total': 0, 'data': None, 'error': ''}`
  appears in both cancelled and non-terminal fixtures (`ID_746.log` and
  `ID_755.log`), so it is not a safe standalone terminal signal.
- Some saved logs are truncated at 4000 lines without a terminal marker,
  for example `ID_748.log` and `ID_751.log`. Log completeness must be an
  explicit parser input concern.

## Fixed Domain Invariants

- Manual run detection is based only on zero `schedule_id` semantics.
- Missing or empty `schedule_id` is not a manual run.
- `execution_key = spider_id + normalized cmd + normalized param`.
- `schedule_id` stays separate from `execution_key`.
- For `running` tasks with `runtime_duration == 0`, live runtime is
  computed locally as `now - start_ts`.
- Zero time `0001-01-01T00:00:00Z` means null time.
- Schedule timing must use observed fire history; cron is not blind truth.

## Ownership Boundary

- The runtime parser owns log-marker extraction and run-level outcome
  classification.
- The schedule engine owns schedule-chain health and incident semantics.
- The schedule engine does not parse raw logs.
- The runtime parser does not decide schedule health.
- They meet only through normalized task inputs and the shared output
  shapes defined here.

## Shared Upstream Inputs

The following normalized fields are shared upstream inputs for `T4` and
`T5` and must keep their current semantics.

### `TaskSnapshot`

- `id`
- `spider_id`
- `schedule_id`
- `status`
- `cmd`
- `param`
- `create_ts`
- `start_ts`
- `end_ts`
- `runtime`
- `is_manual`
- `execution_key`

Semantics that are frozen:

- `schedule_id is None` means normalized zero-id manual semantics.
- `schedule_id == ""` or missing raw `schedule_id` is not manual.
- `runtime` for a running task may be synthetic live runtime.
- `runtime` for a non-running task must not keep growing locally.

### `ScheduleSnapshot`

- `id`
- `spider_id`
- `cron`
- `cmd`
- `param`
- `enabled`

Semantics that are frozen:

- `description` is not a truth source for schedule timing.
- `cmd` and `param` remain part of execution identity.

## Runtime Parser Contract

### Input

The runtime parser consumes:

- one normalized `TaskSnapshot`
- ordered log input for that task
- an explicit completeness signal for that log input

The parser may be called on:

- a full fetched log
- a cumulative log assembled from several pages
- an incremental in-flight log snapshot

The parser must be a pure function of the provided input. It must not
require hidden mutable state or crawler-side session state.

### Input Rules

- The parser must preserve the original log order.
- The parser must not assume `page=1,size=1000` or any fixed page size is
  complete.
- If the provided log input is incomplete, the parser must stay
  conservative about terminal outcomes.
- If `task.status` is non-terminal (`pending` or `running`), progress can
  appear in `counters` and `evidence`, but `run_result` must stay
  conservative.
- `finished` plus a non-empty log without positive terminal signals must
  not be upgraded to `success_probable` by default.

### Output Shape: `RunSummary`

Minimal frozen shape:

```python
class RunSummary(BaseModel):
    task_id: str
    execution_key: str
    run_result: RunResult
    confidence: Confidence
    reason_code: str
    evidence: list[str]
    counters: dict[str, int]
```

Shared field semantics:

- `task_id`: exact task identity.
- `execution_key`: normalized logical execution identity.
- `run_result`: one stable v1 outcome enum.
- `confidence`: confidence in the selected `run_result`, not a generic
  quality score.
- `reason_code`: one primary machine-stable snake_case code.
- `evidence`: ordered human-readable evidence lines or short evidence
  summaries.
- `counters`: stable marker counts with integer values only.

Not part of the frozen shared surface:

- parser-local helper fields
- collector-side classes like `FinalLogClass`
- optional legacy fields such as `error_family`

If `T3` keeps `error_family`, it must remain derived detail and must not
replace `reason_code`.

### `run_result` Values

- `success`: explicit strong positive terminal signal.
- `success_probable`: weaker but still positive completed run signal.
- `partial_success`: positive work happened, but terminal outcome was only
  partly successful.
- `rule_stopped`: explicit rule-driven stop such as `auto_stop` without a
  stronger failure reason.
- `cancelled`: externally cancelled run.
- `failed`: explicit failure.
- `unknown`: not enough evidence for a safer terminal result.

### Parser Result Selection Rules

- `cancelled` should rely on task terminal context, not on the one-line
  trailer alone.
- `failed` wins over `rule_stopped` when the log shows `429` or another
  explicit hard-failure pattern together with `error_auto_stop`, as in
  `ID_745.log`.
- `rule_stopped` is valid for explicit `auto_stop` without a stronger hard
  failure.
- `success` requires a strong explicit positive marker, for example the
  observed summary success marker `| Резюме: ✅` in `ID_742.log`,
  `ID_743.log`, and `ID_756.log`.
- `success_probable` is allowed only when the task is terminal, the log is
  complete, there is weaker positive evidence, and there is no stronger
  negative signal.
- `partial_success` is for completed runs that show both positive progress
  and meaningful degradation or partial failure.
- `unknown` is required when the log is incomplete and no safer terminal
  result can be justified.

### Parser `reason_code`

The v1 parser reason-code catalog is:

- `cancelled_api_status`
- `cancelled_marker_with_terminal_context`
- `failed_ban_429_error_auto_stop`
- `failed_error_without_positive_signal`
- `rule_stopped_auto_stop`
- `success_summary_marker`
- `success_probable_positive_progress_complete_log`
- `partial_success_positive_progress_with_errors`
- `unknown_running_or_pending`
- `unknown_incomplete_log`
- `unknown_finished_without_positive_signal`

Exactly one primary `reason_code` must be emitted.

### Parser `confidence`

- `high`: direct explicit terminal signal or strong terminal state plus
  unambiguous markers.
- `medium`: weaker but still plausible completed-run evidence.
- `low`: incomplete log, missing terminal marker, or thin evidence.

### Parser `evidence`

- `evidence` is an ordered list of strings.
- Preserve the first strongest matching signal before adding weaker ones.
- Keep evidence short and deterministic.
- Do not dump entire logs into `evidence`.

Typical examples:

- `"| Резюме: ✅"`
- `"Exception: error_auto_stop (6) is reached"`
- `"Got ban status code 429, reinit client..."`
- `"put_to_parser (250 prices)"`

### Parser `counters`

The parser counter keys are frozen for v1 and must always exist with an
integer value, including `0`.

- `lines_seen`
- `item_events`
- `put_to_parser`
- `summary_events`
- `resume_success_markers`
- `is_success_true`
- `sku_not_found`
- `gone_404`
- `cancel_markers`
- `auto_stop_markers`
- `error_auto_stop_markers`
- `ban_429_markers`

## Schedule Engine Contract

### Input

The schedule engine consumes:

- one normalized `ScheduleSnapshot`
- scheduled `TaskSnapshot` history for the same `schedule_id`
- manual `TaskSnapshot` history for the same `execution_key`
- optional `RunSummary` objects keyed by `task_id`
- `now`

### Input Rules

- Scheduled history and manual history must stay separate.
- Manual runs can influence recovery logic, but they do not become part of
  the schedule chain.
- Observed fire time comes from real scheduled-task `create_ts`, not from
  schedule description text and not from blind cron parsing alone.
- Queue delay comes from `start_ts - create_ts` when both are present.
- Running duration comparisons use normalized `TaskSnapshot.runtime`, so
  live runtime handling stays upstream in the domain normalizer.
- Long-running comparison is primarily schedule-local. If the engine uses
  `execution_key` as a fallback runtime context, that fallback must be
  explicit in `reason_code` or `evidence`.

### Output Shape: `ScheduleHealth`

Minimal frozen shape:

```python
class ScheduleHealth(BaseModel):
    schedule_id: str
    execution_key: str
    health: str
    confidence: Confidence
    reason_code: str
    evidence: list[str]
    counters: dict[str, int]
```

Shared field semantics:

- `schedule_id`: exact schedule-chain identity.
- `execution_key`: logical execution identity used to relate manual runs
  and runtime context.
- `health`: one stable v1 schedule-health enum.
- `confidence`: confidence in that health assessment.
- `reason_code`: one primary machine-stable snake_case code.
- `evidence`: ordered short evidence strings.
- `counters`: stable schedule metrics and durations as integers.

### `health` Values

- `on_time`
- `queued_start`
- `delayed_start`
- `running_as_expected`
- `running_long`
- `missed_schedule`
- `recovered_by_manual_rerun`

### Schedule Health Selection Rules

- `on_time` means a scheduled task was observed in the expected fire
  window and did not violate start-delay or runtime thresholds.
- `queued_start` means the scheduled task was observed by `create_ts` but
  has not started yet, or its start delay is still inside the queue grace
  window.
- `delayed_start` means the scheduled task was observed, but
  `start_ts - create_ts` exceeded the configured grace window.
- `running_as_expected` means the current running task is inside the
  expected runtime envelope.
- `running_long` means the current running task exceeds the observed
  runtime envelope.
- `missed_schedule` means no scheduled fire was observed in the expected
  window and the engine has enough history to justify that judgment.
- `recovered_by_manual_rerun` means a scheduled fire was missed or failed,
  and a later manual run with the same `execution_key` reached a useful
  recovery result (`success`, `success_probable`, or `partial_success`).

### Schedule `reason_code`

The v1 schedule reason-code catalog is:

- `on_time_observed_fire`
- `queued_after_observed_fire`
- `delayed_start_after_observed_fire`
- `running_within_baseline`
- `running_exceeds_baseline`
- `missed_expected_fire_window`
- `recovered_by_manual_success`
- `insufficient_history_for_strong_judgment`

Exactly one primary `reason_code` must be emitted.

### Schedule `confidence`

- `high`: direct observed task evidence and sufficient recent schedule
  history.
- `medium`: direct observed task evidence but thinner baseline.
- `low`: fallback logic, sparse history, or timezone-sensitive judgment.

### Schedule `evidence`

- `evidence` is an ordered list of strings.
- Evidence should mention observed timestamps, task ids, and the baseline
  used for comparison.
- Evidence should say when the engine used manual recovery or an
  execution-key fallback.

Typical examples:

- `"observed fire at 2026-04-16T22:22:00Z from task ID_757"`
- `"start delay 820 ms from create_ts to start_ts"`
- `"current runtime 43000000 ms exceeds baseline 42728500 ms"`
- `"manual recovery via task ID_748 after missed scheduled fire"`

### Schedule `counters`

The schedule counter keys are frozen for v1 and must always exist with an
integer value, including `0`.

- `scheduled_tasks_seen`
- `manual_tasks_seen`
- `running_tasks_seen`
- `terminal_tasks_seen`
- `baseline_samples`
- `current_runtime_ms`
- `baseline_runtime_ms`
- `start_delay_ms`
- `lateness_ms`
- `missed_windows`
- `manual_recovery_runs`

## Shared Cross-Thread Freeze For `T4` And `T5`

The following fields are shared contract surface and must not change in
parallel once `T4` and `T5` start.

### Frozen shared fields

For `RunSummary`:

- `task_id`
- `execution_key`
- `run_result`
- `confidence`
- `reason_code`
- `evidence`
- `counters`

For `ScheduleHealth`:

- `schedule_id`
- `execution_key`
- `health`
- `confidence`
- `reason_code`
- `evidence`
- `counters`

For shared upstream semantics:

- `TaskSnapshot.schedule_id`
- `TaskSnapshot.create_ts`
- `TaskSnapshot.start_ts`
- `TaskSnapshot.end_ts`
- `TaskSnapshot.runtime`
- `TaskSnapshot.is_manual`
- `TaskSnapshot.execution_key`
- `ScheduleSnapshot.id`
- `ScheduleSnapshot.cron`
- `ScheduleSnapshot.cmd`
- `ScheduleSnapshot.param`
- `ScheduleSnapshot.enabled`

### Frozen shared files

These files must not be changed in parallel by `T4` and `T5`.

- `docs/domain/status-parser-contract.md`
- `src/cl_monitoring/domain/models.py`
- `src/cl_monitoring/domain/normalizers.py`
- `DECISIONS.md`

If any of these fields or files must change, `T4` and `T5` stop and the
change happens in a separate sequential thread.

## Explicit Handling Rules

### Manual Runs By Zero `schedule_id`

- Manual runs are detected only by raw zero `schedule_id`
  `000000000000000000000000`.
- After normalization, that becomes manual semantics in the domain.
- Missing or empty `schedule_id` must remain distinct from manual runs.
- Manual runs can share the same `execution_key` as scheduled runs.
- Manual runs can repair schedule health, but they never overwrite the
  original `schedule_id` chain.

### Live Runtime For Running Tasks

- The parser and schedule engine consume normalized `TaskSnapshot.runtime`.
- They must not recompute running duration independently.
- Upstream normalization is responsible for `now - start_ts` when
  `status == "running"` and `runtime_duration == 0`.
- Finished, cancelled, and error tasks must not keep accumulating runtime
  locally.

### Observed Fire Time Instead Of Blind Cron Truth

- The primary schedule signal is observed scheduled-task `create_ts`.
- The engine may use cron only as an expectation helper, never as the only
  source of truth.
- `create_ts` anchors the fire window.
- `start_ts` anchors queue-delay reasoning.
- Runtime expectations come from observed history, not from cron.
- When observed history is too thin, the engine must lower confidence and
  prefer `insufficient_history_for_strong_judgment` over a strong missed
  or delayed claim.

## Non-Goals For This Contract

- runtime implementation details
- DB, poller, or UI shapes
- node logic
- fixture collection mechanics
- new scope beyond v1
