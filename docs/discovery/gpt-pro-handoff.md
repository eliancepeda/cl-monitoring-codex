# GPT Pro Handoff

## Scope

This package is a read-only Crawlab discovery snapshot for these target projects:

- `66a25c4d116add6c8f235756`
- `66a384f5116add6c8f235803`

It was collected with a GET-only Python collector. No POST/PUT/PATCH/DELETE calls were used.

## Read First

Suggested reading order:

1. `docs/discovery/entity-summary.md`
2. `docs/discovery/parameter-taxonomy.md`
3. `docs/discovery/log-patterns.md`
4. `docs/discovery/open-questions.md`
5. `docs/discovery/api-map.md`
6. `docs/discovery/normalized/tasks.json`
7. `docs/discovery/raw/tasks/` and `docs/discovery/raw/nodes/` for raw evidence

## What Is In The Package

- `docs/discovery/api-map.md`: observed GET endpoints and query patterns
- `docs/discovery/entity-summary.md`: observed counts and field shapes
- `docs/discovery/parameter-taxonomy.md`: normalized launch-parameter observations and role guesses
- `docs/discovery/log-patterns.md`: representative log-derived cases
- `docs/discovery/open-questions.md`: unresolved gaps from this run
- `docs/discovery/raw/`: raw JSON snapshots used as evidence
- `docs/discovery/normalized/`: prepared JSON slices for downstream analysis

## Current Snapshot

From the latest run:

- Projects observed: `2`
- Spiders observed for target projects: `27`
- Schedules observed: `53`
- Sampled normalized tasks: `4`
- Nodes counted in summary: `10`

Observed GET endpoints in this instance include:

- `/api/projects`
- `/api/spiders`
- `/api/schedules`
- `/api/nodes`
- `/api/tasks`
- `/api/tasks/{id}`
- `/api/tasks/{id}/logs`
- `/api/nodes/{id}`

## Important Normalization Rules Already Applied

- Base observation unit is `spider + schedule_id + normalized params`
- Crawlab zero schedule sentinel `000000000000000000000000` is normalized to `unscheduled`
- `task.param` is preferred over `args` / `command` / `cmd` when present
- Empty-string `param` falls back to the other launch-argument fields
- Raw node `key` values are redacted in stored node artifacts

## Parameters Observed In This Run

Current taxonomy reflects these observed hypothesis-level parameters:

- `as` -> `execution modifier`
- `sp` -> `identity candidate`
- `fp` -> `identity candidate`

Notable current examples are visible in `docs/discovery/normalized/tasks.json`, including:

- `-as 500 -sp 1643`
- `-as 500 -sp 1644`
- `-sp 1 -fp 99`

Wheel/library corroboration is still `unknown` in this worktree, so current parameter-role confidence comes from observed task payloads, not package-source inspection.

## Representative Cases Observed

This run currently includes:

- `http error spike`
- `successful`

This run did **not** observe these representative classes, and they remain open questions:

- `failed`
- `long-running`
- `manual rerun candidate`
- `finished but suspicious`

## Constraints For Downstream Analysis

- Treat raw/normalized artifacts as the source of truth.
- Do not infer unsupported Crawlab endpoints beyond `api-map.md`.
- Keep `fact`, `hypothesis`, and `unknown` separated.
- Do not over-read missing representative classes as absence in the real system; they are absence in this sampled run.

## Remaining Caveats

- Raw task logs can still contain internal file paths, task pointers, and operational details. Safe for internal analysis, not for broad external sharing.
- Library observations are still `unknown` because the referenced wheel contents were not available for confirmation in this worktree snapshot.
- Discovery still samples a subset of tasks/logs rather than full history; use it for requirements/MVP framing, not production completeness claims.
