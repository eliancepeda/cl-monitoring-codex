# DECISIONS

## 2026-04-16 — Pure Jinja2 for v1 UI (no HTMX)

Status: Accepted

Context:
v1 needs a simple server-rendered UI. HTMX would add partial-update
capability but introduces an extra JS dependency.

Decision:
Use pure Jinja2 templates with full-page renders.

Why:
App is local single-user read-only. Minimizing frontend complexity
is more important than dynamic UX while client/normalizers/parser are
unstable.

Consequences:
- No JS dependencies in v1.
- Every navigation is a full page reload.
- Can revisit HTMX when core logic stabilizes.

Related files:
- src/cl_monitoring/web/templates/base.html
- src/cl_monitoring/web/routes.py

---

## 2026-04-16 — hatchling build backend with src-layout

Status: Accepted

Context:
Need a build system that supports src-layout (`src/cl_monitoring/`)
and editable installs for development.

Decision:
Use hatchling as the PEP 517 build backend.

Why:
Lightweight, good src-layout support, no setup.py/setup.cfg needed.

Consequences:
- `pip install -e ".[dev]"` for development.
- Package discovery via `[tool.hatch.build.targets.wheel]`.

Related files:
- pyproject.toml

---

## 2026-04-16 — SQLite with WAL mode

Status: Accepted

Context:
Need local persistence for normalized Crawlab data.
Polling writes and UI reads may overlap.

Decision:
SQLite with `PRAGMA journal_mode=WAL` enabled on first connect.

Why:
Single-user app — SQLite is sufficient. WAL avoids read/write
blocking between the poller and web routes.

Consequences:
- No external database dependency.
- Must enable WAL in engine.py on every new connection.

Related files:
- src/cl_monitoring/db/engine.py

---

## 2026-04-17 — Crawlab API uses plain token, not Bearer

Status: Accepted

Context:
First live dry-run returned 401 Unauthorized. Investigated auth
header format against live instance.

Decision:
Send `Authorization: {token}` without `Bearer` prefix. Crawlab API
expects the raw JWT token directly in the header value.

Why:
Discovered empirically against live instance. Crawlab docs are
ambiguous; live evidence takes precedence (AGENTS.md § Truth sources).

Consequences:
- ReadonlyCrawlabClient sets header as `{"Authorization": token}`.
- Tests use the same format in mock transport.
- If Crawlab is upgraded and changes auth scheme, fix here only.

Related files:
- src/integrations/crawlab/readonly_client.py

---

## 2026-04-17 — Auto-strip /api suffix from CRAWLAB_BASE_URL

Status: Accepted

Context:
First live dry-run hit 404 on `/api/api/tasks` — the .env
`CRAWLAB_BASE_URL` ended with `/api`, and API paths in code also
start with `/api/`.

Decision:
Collector CLI auto-strips trailing `/api` from `base_url` before
passing to the client.

Why:
Users may copy the URL from browser address bar which includes `/api`.
Silently fixing is safer than crashing with a confusing 404.

Consequences:
- Works with both `http://host` and `http://host/api`.
- The client always gets a clean base URL without path suffix.
- Log output shows the corrected URL for debugging.

Related files:
- src/tools/collect_fixtures.py (main → base_url strip)

---

## 2026-04-17 — Collector lives in src/integrations + src/tools, not in cl_monitoring

Status: Accepted

Context:
Fixture collector is a one-shot CLI tool, not part of the runtime
app (web server / poller). Need to decide package placement.

Decision:
Place client in `src/integrations/crawlab/`, collector CLI and helpers
in `src/tools/`. Keep them outside `src/cl_monitoring/`.

Why:
- Clear separation: runtime app vs. development tooling.
- Avoids polluting the runtime package with collection-only code.
- `integrations/` can host other integrations later.
- `tools/` can host other CLI tools (e.g., data migration).

Consequences:
- `pyproject.toml` packages list extended to include both.
- `pythonpath = ["src"]` added to pytest config.
- Imports use `integrations.crawlab.readonly_client`, not `cl_monitoring.crawlab.client`.

Related files:
- src/integrations/crawlab/readonly_client.py
- src/tools/collect_fixtures.py
- src/tools/redact.py
- src/tools/classify_logs.py
- pyproject.toml

---

## 2026-04-17 — Stable indexed placeholders for redaction (CATEGORY_NNN)

Status: Accepted

Context:
Need to redact ObjectIds, hostnames, and other sensitive data in
fixtures while keeping cross-references consistent.

Decision:
Use deterministic indexed placeholders: `{CATEGORY}_{NNN}` where
category is derived from field name (e.g., `SPIDER_ID_001`,
`HOST_003`). Same real value always maps to the same placeholder
within a Redactor instance. Mapping is persisted to
`fixtures_raw_local/redaction_map.json` for incremental runs.

Why:
- Deterministic: no UUIDs, no randomness.
- Readable: `SPIDER_ID_001` is more debuggable than a hash.
- Cross-ref safe: spider_id in tasks matches _id in spider fixture.
- Persistent: mapping survives re-runs for consistency.

Consequences:
- Each field type has its own counter namespace.
- Zero ObjectId and zero time pass through unredacted.
- Mapping file is gitignored (contains reverse lookup to real values).
- Sensitive strings in config use the same scheme (`SENSITIVE_NNN`).

Related files:
- src/tools/redact.py
- config/user_scope.yml (redaction section)

---

## 2026-04-17 — Keep real timestamps in v1 fixtures

Status: Accepted

Context:
Timestamps in Crawlab responses (created_at, start_at, etc.) could
be shifted or preserved. Shifting preserves intervals but obscures
real timing; preserving is simpler but might leak operational info.

Decision:
Preserve real timestamps as-is in redacted fixtures for v1.
Only zero time (`0001-01-01T00:00:00Z`) is special-cased.

Why:
Timestamps are not PII. Real timing data is valuable for runtime
classification, schedule analysis, and debugging normalizers.
Shifting adds complexity with no security benefit for a local app.

Consequences:
- `redaction.preserve_real_timestamps: true` in user_scope.yml.
- Runtime analysis and tests can use real time ranges.
- Can revisit if fixtures are ever published externally.

Related files:
- config/user_scope.yml
- src/tools/redact.py

---

## 2026-04-17 — Two-phase task classification (candidate vs final)

Status: Accepted

Context:
First dry-run showed `finished_weak_success` inferred from task metadata
alone. In this Crawlab usage, `result_count` and `item_scraped_count` in
task metadata are not reliable success signals (observed as 0 for all
tasks including successful ones).

Decision:
Split classification into two phases:
- Phase 1 (`CandidateClass`): assigned from task status metadata only.
  Finished tasks are always `finished_candidate` — no success inference.
- Phase 2 (`FinalLogClass`): assigned after fetching and inspecting log
  content. Determines actual outcome (success_strong, success_probable,
  partial_success, auto_stop, ban_429, cancelled, failed_other, unknown).

`result_count` / `item_scraped_count` in task metadata must never be
used as success signals.

Why:
Live data shows `result_count: 0` for all tasks including
long-running finished ones. Inferring success from metadata would
produce false classifications.

Consequences:
- Dry-run shows only candidate classes (no final log classes).
- Collect mode shows both candidate and final classes.
- TaskClass enum removed; replaced by CandidateClass + FinalLogClass.
- Tests verify result_count never influences candidate classification.

Related files:
- src/tools/classify_logs.py
- src/tools/collect_fixtures.py
- tests/test_fixture_classifier.py

---

## 2026-04-17 — Condition-based task discovery via Crawlab conditions API

Status: Accepted

Context:
First dry-run fetched generic latest tasks page, resulting in 735
schedules and many spider_ids — scope too broad for a personal project
fixture pack.

Decision:
Use Crawlab's `conditions` query param for targeted task queries:
`?conditions=[{"key":"status","op":"eq","value":"running"}]`
instead of generic `/api/tasks?page=1&size=100`.

Run separate queries for each status (running, pending, finished,
error, cancelled) and for manual runs (schedule_id == zero ObjectId).

Why:
Discovered from HAR capture that Crawlab API supports condition-based
filtering via `conditions` JSON array param. Targeted queries give
focused candidate pools instead of a generic latest-first listing.

Consequences:
- 6 targeted API calls instead of 1 generic one per discovery phase.
- Each status gets its own candidate pool with count visibility.
- Manual vs scheduled detection is explicit from the discovery phase.
- Dry-run output shows per-query candidate counts.

Related files:
- src/tools/collect_fixtures.py (discover_all_candidates)
- src/integrations/crawlab/readonly_client.py (get_paginated)

---

## 2026-04-17 — Mandatory project scoping for fixture collection

Status: Accepted

Context:
Lab has many projects. Collecting fixtures across all projects would
produce an unfocused fixture pack with 735+ schedules.

Decision:
Require `scope.allowed_project_ids` in user_scope.yml. During
discovery, hydrate spider details and filter tasks by project_id.
If allowed_project_ids is empty, dry-run shows project histogram
and refuses to collect.

Why:
Personal companion app should only collect fixtures for the owner's
projects. Without scoping, collection pulls data from the entire lab.

Consequences:
- Spider hydration adds API calls (1 per unique spider_id).
- Dry-run shows project_id histogram for scope review.
- Collect mode exits with error if allowed_project_ids is empty.
- Long-running schedule selection is also filtered by scope.

Related files:
- config/user_scope.yml (scope.allowed_project_ids)
- src/tools/collect_fixtures.py (filter_tasks_by_project)

---

## 2026-04-18 — Readonly client allowlist must match the approved GET surface exactly

Status: Accepted

Context:
T0 verification found that the default client allowlist used
`/api/tasks/*`, which was broader than the approved Crawlab surface in
`AGENTS.md` and could allow unrelated nested task routes.

Decision:
Restrict the default `ReadonlyCrawlabClient` allowlist to the exact
approved GET routes: `/api/tasks`, `/api/tasks/{id}/logs`,
`/api/spiders/{id}`, `/api/schedules`, `/api/schedules/{id}`,
`/api/results/{col_id}`. Path checks use segment-based matching, and
absolute URLs remain forbidden.

Why:
The read-only safety boundary must be enforced in code, not only in
docs. Exact matching reduces accidental scope creep and prevents access
to unapproved or write-adjacent nested routes.

Consequences:
- `GET /api/tasks/{id}` is no longer allowed by default.
- Nested task routes are allowed only for `/logs`.
- Host-mismatch protection stays inside the single Crawlab client.

Related files:
- src/integrations/crawlab/readonly_client.py
- tests/test_readonly_client.py
- docs/adr/0001-readonly-companion.md

---

## 2026-04-18 — Domain normalization keeps null-time and zero-id semantics explicit

Status: Accepted

Context:
T0 verification found two unsafe fallbacks in domain normalization:
zero time could turn into a synthetic minimum datetime, and tasks with
`runtime_duration == 0` could keep growing even after leaving `running`.
The same pass also clarified that manual-run detection in the domain
must stay tied to zero `schedule_id` semantics, not empty-string
fallbacks.

Decision:
In domain normalization, zero time stays `None`, live runtime is
synthesized only for `running` tasks with zero runtime duration, and
manual-run detection stays tied to normalized zero-id semantics.

Why:
The domain layer must preserve Crawlab's null semantics explicitly and
must not invent timestamps or runtime growth for non-running tasks.
Keeping zero-id handling explicit also preserves the distinction between
real manual runs and malformed/incomplete payloads.

Consequences:
- `TaskSnapshot.create_ts` is nullable.
- Finished tasks with zero recorded runtime no longer accumulate live runtime.
- Missing or empty `schedule_id` stays distinguishable from zero-id manual runs in the domain layer.

Related files:
- src/cl_monitoring/domain/models.py
- src/cl_monitoring/domain/normalizers.py
- tests/test_normalizers.py
- docs/adr/0001-readonly-companion.md

---

## 2026-04-18 — Freeze shared status/parser contract before parallel T4/T5

Status: Accepted

Context:
`T4` (schedule engine) and `T5` (runtime parser) are the only planned
parallel build threads. The repository already has divergent classifier
and parser shapes, and the project rules require manual-run semantics,
live runtime semantics, and schedule timing semantics to stay stable.

Decision:
Use `docs/domain/status-parser-contract.md` as the single shared contract
for `RunSummary` and `ScheduleHealth`. Freeze shared fields
`run_result/health`, `confidence`, `reason_code`, `evidence`, and
`counters`, and keep manual zero-id semantics, live runtime handling, and
observed fire time semantics fixed during `T4` and `T5`.

Why:
Parallel work is only safe if both threads consume and produce the same
data shape and do not reinterpret the same upstream fields differently.
This keeps schedule logic anchored to observed history and keeps runtime
classification anchored to deterministic evidence.

Consequences:
- `T4` and `T5` must not change shared contract fields in parallel.
- If the shared contract needs to change, both threads stop and a
  sequential thread updates the contract first.
- Schedule timing cannot rely on blind cron truth.
- Manual recovery can influence schedule health without merging manual
  runs into the schedule chain.

Related files:
- docs/domain/status-parser-contract.md
- src/cl_monitoring/domain/models.py
- src/cl_monitoring/domain/normalizers.py

---

## 2026-04-18 — Commit redacted fixture pack, keep only raw live artifacts local

Status: Accepted

Context:
`T2` needs an offline golden corpus in-repo, but the repository ignore rules
still masked both `fixtures/` and `fixtures_raw_local/`. That blocked pushing
the redacted fixture pack even though project rules require committed redacted
fixtures and gitignored raw live payloads.

Decision:
Track `fixtures/` in git and keep only `fixtures_raw_local/` gitignored.

Why:
Offline parser/status work is only reproducible if anonymized fixtures and
golden expectations travel with the repository. Raw live payloads still stay
local because they may contain sensitive operational data.

Consequences:
- `fixtures/` becomes part of normal review and commit flow.
- `fixtures_raw_local/` remains the only local-only fixture workspace.
- Fixture pack changes are now pushable without force-adding ignored files.

Related files:
- .gitignore
- fixtures/
- fixtures_raw_local/

---

## 2026-04-18 — Results fixtures use `/api/results/{col_id}` and preserve empty `_tid` responses

Status: Accepted

Context:
During `T2` live collection, using spider `col_name` for results fixtures was
unsafe because names may contain slashes and do not match the approved
readonly path surface reliably. The same pass also showed that `_tid` queries
can legitimately return an empty result set, which is itself a required test
scenario.

Decision:
Collect results fixtures via `/api/results/{col_id}` using spider `col_id`,
and persist empty `_tid` responses as explicit `[]` fixtures under
`fixtures/api/results_<task_id>.json`.

Why:
`col_id` matches the approved GET surface and is stable for allowlist-safe
collection. Empty `_tid` responses are domain-relevant truth and must remain
visible in the offline corpus instead of being silently skipped.

Consequences:
- Results fixtures live alongside other API fixtures in `fixtures/api/`.
- `results_by_tid_empty` is testable offline from committed fixtures.
- Collector no longer depends on collection-name path quirks.

Related files:
- src/tools/collect_fixtures.py
- fixtures/api/results_*.json
- fixtures/manifest.md

---

## 2026-04-18 — Golden expected log fixtures freeze the shared RunSummary subset

Status: Accepted

Context:
Before `T2`, `fixtures/expected/*.yaml` were draft skeletons in an ad hoc log
classification shape. `T1` had already frozen the shared parser contract
around `run_result`, `confidence`, `reason_code`, `evidence`, and `counters`.

Decision:
Generate and maintain `fixtures/expected/*.yaml` in the shared RunSummary-like
shape: `run_result`, `confidence`, `reason_code`, `counters`, `evidence`.
Keep this generation in `src/tools/classify_logs.py` as collector-side corpus
preparation, not as the runtime parser layer.

Why:
Offline parser work needs golden fixtures that already speak the same contract
as later runtime code. Keeping this logic in tooling preserves the boundary
between fixture preparation and production parser implementation.

Consequences:
- Every log fixture now has a nearby contract-shaped expected YAML.
- Fixture tests can validate parser-facing golden data without live access.
- `src/tools/*` remains tooling-only and does not become runtime parser code.

Related files:
- src/tools/classify_logs.py
- src/tools/collect_fixtures.py
- fixtures/expected/*.yaml
- tests/test_fixture_classifier.py

---

## 2026-04-18 — Explicit auto_stop log marker outranks generic error task status

Status: Accepted

Context:
`T3a` added a real anonymized fixture (`ID_820`) where Crawlab reports
task `status=error`, but the terminal log evidence is the explicit rule
marker `Exception: auto_stop (80) is reached`. Treating that run as a
generic failure would lose the real stop reason and drift from observed
log truth.

Decision:
When a task log contains an explicit `auto_stop` marker and there is no
stronger hard-failure pattern such as `429` together with
`error_auto_stop`, classify the run as rule-stopped / `auto_stop` even if
the raw task status is `error`.

Why:
Observed log truth is more specific than Crawlab's coarse terminal status.
The project already treats explicit runtime evidence as authoritative for
run outcome classification.

Consequences:
- Runtime classification must not collapse explicit `auto_stop` into
  generic failure just because `task.status=error`.
- The offline corpus now includes a real explicit `auto_stop` example.
- Stronger hard-failure evidence still wins over `auto_stop`.

Related files:
- src/tools/classify_logs.py
- fixtures/api/task_ID_820.json
- fixtures/logs/ID_820.log
- fixtures/expected/task_ID_820_log.yaml
- tests/test_fixture_classifier.py

---

## 2026-04-19 — Schedule engine uses observed minute buckets and explicit execution-key fallback

Status: Accepted

Context:
`T4` needs deterministic schedule health without timezone self-deception,
without using schedule description text, and without treating cron alone as
truth. The engine also needs a stable way to reason about long-running
tasks when schedule-local runtime history is thin.

Decision:
Infer expected fire windows from observed scheduled-task `create_ts`
history, normalized to UTC minute buckets, and derive the baseline interval
from observed gaps. Use manual runs only as recovery signals and as an
explicit `execution_key` runtime-baseline fallback when schedule-local
successful runtime samples are insufficient.

Why:
Observed task history is the closest reliable source for real fire timing.
Minute buckets avoid fake precision, and `execution_key` fallback preserves
runtime context without merging manual runs into the schedule chain.

Consequences:
- `missed_schedule` requires enough observed interval history; sparse
  history stays low-confidence.
- `running_long` evidence must say when `execution_key` fallback was used.
- Manual reruns can recover missed or failed schedule health, but they do
  not rewrite the original `schedule_id` chain.

Related files:
- src/cl_monitoring/status/models.py
- src/cl_monitoring/status/engine.py
- tests/test_schedule_engine.py
- MILESTONES.MD

---

## 2026-04-19 — Production crawllib parser stays separate from collector tooling and requires explicit log completeness

Status: Accepted

Context:
`T5` implements the production runtime parser after `T3` froze the shared
`RunSummary` contract. The repository already had collector-side fixture
classification logic in `src/tools/classify_logs.py`, but project rules say
runtime parser logic must stay deterministic, must support paginated and
incremental log input, and must not assume a fixed fetch size is complete.

Decision:
Keep the production crawllib parser in `src/cl_monitoring/parsers/` as a
pure function over normalized task input, ordered log lines, and an explicit
`is_complete` signal. Do not import runtime logic from
`src/tools/classify_logs.py`; keep tooling-only corpus generation separate
from the production parser layer.

Why:
This preserves a clean boundary between offline fixture preparation and
runtime decision logic. An explicit completeness flag avoids hidden
page-size heuristics and lets the parser stay conservative for incremental
or truncated logs.

Consequences:
- Runtime parsing can be called repeatedly on cumulative log snapshots.
- Incomplete logs stay `unknown` unless a stronger terminal marker is already present.
- `src/tools/*` remains collector/tooling code, not shared runtime logic.

Related files:
- src/cl_monitoring/parsers/crawllib_default.py
- src/cl_monitoring/parsers/__init__.py
- tests/test_crawllib_parser.py
- fixtures/logs/ID_821.log
- fixtures/logs/ID_822.log
- fixtures/expected/task_ID_821_log.yaml
- fixtures/expected/task_ID_822_log.yaml

---

## 2026-04-19 — Local history uses normalized task snapshots and cursor-based log sync

Status: Accepted

Context:
`T7` needs local persistence and a restart-safe poller after `T4` and `T5`
froze the parser and schedule outputs. The app must keep UI reads local,
must survive restart without normal full-log rereads, and must not create a
second Crawlab client.

Decision:
Persist local state as normalized `spiders`, `schedules`, `task_snapshots`,
`task_log_cursors`, `run_summaries`, `incidents`, and local-only
`spider_profiles`. Use `task_log_cursors` to assemble logs incrementally and
do one final sync after terminal state. Keep all live Crawlab access inside
the single `ReadonlyCrawlabClient`; UI reads only SQLite.

Why:
This keeps the DB aligned with the frozen domain/runtime contract, makes the
poller restart-safe, and preserves the project's read-only safety boundary.

Consequences:
- No `tasks_raw` table in v1.
- No direct live access from UI.
- No second client in `src/cl_monitoring/crawlab/client.py`.
- Parser/status outputs remain the only runtime truth layer above SQLite.

Related files:
- docs/adr/0002-local-history-and-poller.md
- src/cl_monitoring/db/engine.py
- src/cl_monitoring/db/tables.py
- src/cl_monitoring/db/repo.py
- src/cl_monitoring/sync/poller.py
- src/cl_monitoring/crawlab/client.py

---

## 2026-04-19 — Minimal v1 dashboard is three evidence-first local screens

Status: Accepted

Context:
`T7` already fixed the local SQLite truth path. `T8` needs a minimal UI scope
that answers what is running, broken, overdue, and recently recovered without
reopening runtime logic or imitating Crawlab.

Decision:
Limit v1 UI to three server-rendered screens: project board, spider detail,
and incidents. All browser-visible data comes from repository-backed SQLite
reads only.

Why:
This is the smallest dashboard that surfaces parser/schedule truth safely while
keeping the browser outside Crawlab and avoiding UI scope creep.

Consequences:
- No action buttons.
- No direct Crawlab calls from the browser.
- No graphs-only widgets, dense filters, or UI settings.
- `T9` should touch repo read methods, routes, templates, and app wiring only.

Related files:
- docs/adr/0003-minimal-dashboard.md
- src/cl_monitoring/db/repo.py
- src/cl_monitoring/static/style.css
- src/cl_monitoring/web/routes.py
- src/cl_monitoring/web/templates/base.html
- src/cl_monitoring/web/templates/project_board.html
- src/cl_monitoring/web/templates/spider_detail.html
- src/cl_monitoring/web/templates/incidents.html
- src/cl_monitoring/app.py
