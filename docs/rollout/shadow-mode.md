# Shadow Mode Rollout

## Purpose

This document defines how to pilot structured spider markers safely.

Shadow mode means marker-aware automated decisions are recorded and compared
with manual review, but they do not replace the current local truth, incidents,
or UI-facing state until the pilot passes.

The rollout contract must stay executable even when spider code lives outside
this repository.

## Non-Goals

- No runtime app changes in this planning thread.
- No UI-driven go/no-go decision.
- No mass rewrite of all spiders.
- No rollout wider than the pilot buckets before agreement is proven.

## Preconditions

- The current parser/status pipeline remains the operator truth during the
  pilot.
- Candidate runs are chosen by `execution_key`, not only by `spider_id`.
- Each candidate must have retrievable terminal logs and a reviewer who can
  inspect them.
- Marker emission may be implemented in an external spider repository, a shared
  spider helper, or a launch wrapper.

## Selecting 2-3 Pilot `execution_key`

### Common Filters

- Group recent terminal runs by `execution_key`.
- Use local SQLite history, saved fixtures, or exported task/log history. The
  procedure does not depend on dashboard behavior.
- Exclude keys with missing logs or unstable ownership during the pilot window.
- Freeze the chosen pilot list for the whole shadow window.

### Bucket 1: Stable Short Run

- Frequent terminal runs.
- Low runtime variance.
- Mostly clean `success` or `rule_stopped` outcomes.
- Usually no long queue delay and no known anti-bot complexity.
- Goal: prove basic `RUN_START` and `RUN_END` markers do not create false
  positives.

### Bucket 2: Long-Running Run

- Clearly longer runtime than the median for that spider family.
- Expected to live long enough for at least two `HEARTBEAT` markers.
- Not just queued. Real in-spider work continues for a meaningful period.
- Goal: validate liveness tracking and avoid false hung or false terminal
  calls.

### Bucket 3: Problematic Anti-Bot Run

- Repeated `429`, `auto_stop`, captcha, rotating proxy, or similar
  profile-specific behavior.
- Manual review is currently needed at least sometimes.
- The optional `profile` field on markers is strongly recommended here.
- Goal: expose missing profile rules before wider rollout.

### If Only Two Good Candidates Exist

- Keep the stable short run and the problematic anti-bot run.
- Use the longest remaining retrievable key as the long-running proxy.
- Do not replace the problematic anti-bot bucket with another easy success
  bucket.

## What Shadow Mode Records

For each terminal pilot run, record one comparison row with:

- `task_id`
- `execution_key`
- marker presence summary: `run_start_seen`, `heartbeat_count`, `run_end_seen`
- automated shadow decision: `run_result`, `reason_code`
- blind manual decision: `run_result`, `reason_code`
- mismatch category or `match`
- reviewer timestamp and short note

The storage format is intentionally open. A CSV, SQLite table, or Markdown log
is acceptable. The rollout contract depends on the fields, not on UI or a
specific storage mechanism.

## Blind Comparison Procedure

1. Wait until the run is terminal according to task metadata.
2. Gather the normalized task snapshot, the full available raw log, and the
   extracted marker lines for that task.
3. Record the manual decision before looking at the automated shadow decision.
4. Manual review must choose one terminal result from `success`,
   `partial_success`, `rule_stopped`, `cancelled`, `failed`, `unknown`.
5. Manual review must write one short primary cause. Exact string equality is
   not required, but the cause family must be comparable.
6. Run the marker-aware automated decision in shadow mode.
7. Compare the two decisions in this order:
   - top-level `run_result`
   - primary cause family
   - whether timestamp interpretation changed the judgment
   - whether profile-specific knowledge was required
8. If any field differs, assign exactly one mismatch category from the taxonomy
   below.
9. Shadow decisions may be displayed for inspection, but current operator
   truth, incidents, and UI states must continue to come from the existing
   pipeline until the pilot gate passes.

## Mismatch Taxonomy

### `parser bug`

Use when the automated decision contradicts clear raw evidence or ignores an
explicit marker that should have been decisive.

Default action: fix the parser or marker extraction logic before expanding
coverage.

### `bad threshold`

Use when the evidence is real, but a runtime, heartbeat, or lateness threshold
is too strict or too loose for this `execution_key`.

Default action: tune the threshold and replay the same collected runs before
counting them as passed.

### `timezone ambiguity`

Use when the disagreement is caused by non-UTC producer timestamps, mixed
timezone assumptions, or unclear conversion between marker time and task time.

Default action: block rollout expansion until UTC-only timestamps are restored
and the affected runs are replayed.

### `missing profile rule`

Use when the run is understandable only with profile-specific anti-bot or
environment knowledge that the generic logic does not yet encode.

Default action: add a narrow profile rule, then replay the same collected runs
before expanding to similar keys.

## Pilot Pass Criteria

A pilot `execution_key` passes only when all of the following are true:

- At least 5 terminal runs were observed with marker lines for that key.
- Automated shadow `run_result` matched the blind manual `run_result` on all 5
  runs.
- There are no open `parser bug` or `timezone ambiguity` mismatches.
- Any `bad threshold` or `missing profile rule` mismatch was fixed and replayed
  against the already collected runs.
- The pilot still looks valid when reviewed from raw task/log evidence alone,
  without relying on dashboard presentation.

## Bucket-Specific Minimum Evidence

- Stable short run: all 5 gate runs are consecutive terminal runs with zero
  mismatches.
- Long-running run: within the 5-run gate, at least 3 runs each have at least
  2 `HEARTBEAT` markers before `RUN_END`, and there are zero false terminal
  calls.
- Problematic anti-bot run: within the 5-run gate, at least 1 run is a
  degraded or blocked case such as `429`, `auto_stop`, or a profile-specific
  recovery path.

## Expansion Rule

- Passing one pilot key only authorizes expansion to closely related keys with
  the same spider behavior and profile characteristics.
- Do not roll out globally after one easy success bucket.
- If a new mismatch category appears during expansion, stop and treat it as a
  new pilot boundary.
