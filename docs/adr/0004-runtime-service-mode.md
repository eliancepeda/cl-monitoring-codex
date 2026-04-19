# ADR 0004: Runtime Service Mode for the Local Owner Workflow

**Status**: Accepted  
**Date**: 2026-04-19

## Context

`T7` and `T9` already produced the pieces of a useful local companion app:

- `Poller` can read live Crawlab through `ReadonlyCrawlabClient` and write the
  local SQLite truth layer.
- the browser UI already reads only from SQLite.
- `src/cl_monitoring/crawlab/client.py` is already constrained to be only a thin
  runtime surface over the single readonly client.

But the default operator entrypoint is still incomplete.

Today `python -m cl_monitoring.app` only starts the web server because:

- `src/cl_monitoring/app.py` builds a FastAPI app and calls `uvicorn.run`, but it
  does not define a lifespan that opens SQLite, creates a runtime client, or
  starts a background `Poller`.
- `src/cl_monitoring/settings.py` is still a stub, so there is no single runtime
  settings layer that decides whether the process should run in live service mode
  or SQLite-only mode.
- the runtime env contract is split: `.env.example` and tooling use
  `CRAWLAB_TOKEN`, while the current client code still looks for
  `CRAWLAB_API_TOKEN`.

This leaves an operator gap: the repository has poller core and a local UI, but
the default launch command does not yet behave like one real local service.

`T11` marker rollout blocking is a separate issue. It does not change the v1
requirement that the local owner workflow must have one clear runtime path for
running the service.

Project constraints remain unchanged:

- all live Crawlab access must stay GET-only and must go through
  `ReadonlyCrawlabClient`
- the browser must never call Crawlab directly
- UI truth remains local SQLite
- parser/status semantics and marker rollout contracts are out of scope here

## Decision

### 1. `python -m cl_monitoring.app` is the single v1 operator entrypoint

For v1, the target owner workflow is one local command:

```bash
python -m cl_monitoring.app
```

That command is not just "start the web server" anymore. It is the runtime
service entrypoint that decides the service mode, opens local state, and, when
live env is available, owns the poller lifecycle.

v1 does not introduce a second required runtime command such as a separate poller
daemon for the normal owner workflow.

### 2. The app owns the background poller lifecycle

The FastAPI app lifespan is the owner of the runtime poller resources.

When the process is in live service mode, the app lifespan owns:

- settings load
- the dedicated SQLite writer connection used by the runtime service
- `LocalRepository`
- the single `ReadonlyCrawlabClient`
- the `Poller` instance
- the background task running `poller.run_forever(...)`

Why this is the decision:

- the poller core already exists and should not remain a disconnected manual
  wiring snippet
- the local operator flow needs one process that keeps SQLite fresh while the UI
  reads from it
- putting lifecycle ownership into the app closes the exact gap that exists now:
  the server starts, but nothing starts the poller

The browser still does not own any of this lifecycle. It remains a pure SQLite
reader.

### 3. Runtime settings come from one settings layer

`src/cl_monitoring/settings.py` becomes the single runtime truth source for the
service process.

Load order for `python -m cl_monitoring.app` is:

1. defaults in the runtime settings model
2. optional `.env`
3. real process environment variables overriding `.env`

The runtime settings layer is responsible for deciding service mode and for
providing normalized values to the app lifecycle.

For v1:

- `APP_PORT` is a runtime setting
- `CL_MONITORING_DB_PATH` is a runtime setting
- host bind remains fixed to `127.0.0.1` and is not an operator choice in v1
- the runtime token truth key is `CRAWLAB_TOKEN`
- `CRAWLAB_API_TOKEN` is not part of the target v1 operator contract

This keeps `.env.example`, runtime docs, and the live service path on one token
name.

### 4. `CRAWLAB_BASE_URL` is normalized once before client creation

The runtime settings layer normalizes the configured Crawlab base URL before the
client is created:

- strip trailing `/`
- if the remaining URL ends with `/api`, strip that suffix exactly once

Result:

- `https://host`
- `https://host/`
- `https://host/api`

all normalize to the same runtime client base URL:

- `https://host`

The client then continues to call only allowlisted `/api/...` paths.

### 5. Service mode is selected from the presence of the full live env

Full live env means both of these settings are present after settings load:

- `CRAWLAB_BASE_URL`
- `CRAWLAB_TOKEN`

Mode selection is:

- full live env present: start live local-service mode
- both live settings absent: start SQLite-only fallback mode
- only one live setting present: fail startup with a clear configuration error

Partial live env must not silently downgrade to SQLite-only mode, because that
would hide operator misconfiguration.

### 6. SQLite-only fallback mode is required

v1 keeps an explicit SQLite-only fallback mode.

Reason:

- the UI truth source is already SQLite, not live Crawlab
- the owner may need to inspect an existing local history DB without a live token
  or without current live access
- this mode does not weaken any safety boundary because it makes zero Crawlab
  requests

SQLite-only fallback mode means:

- open local SQLite
- serve the dashboard
- do not create `ReadonlyCrawlabClient`
- do not create a `Poller`
- do not attempt background sync

If the DB file does not exist yet, the app still creates the local SQLite file
and serves an empty local dashboard.

### 7. Startup lifecycle is fixed

#### SQLite-only mode startup

1. Load runtime settings.
2. Resolve `db_path` and `port`.
3. Open the dedicated SQLite writer connection and create `LocalRepository` so
   schema and WAL are ensured.
4. Store app state needed by request handlers.
5. Start serving HTTP on `127.0.0.1`.

#### Live local-service startup

1. Load runtime settings.
2. Normalize `CRAWLAB_BASE_URL`.
3. Open the dedicated SQLite writer connection and create `LocalRepository` so
   schema and WAL are ensured.
4. Create the single `ReadonlyCrawlabClient` using the normalized base URL and
   the runtime token truth from settings.
5. Create `Poller` on top of that client and repository.
6. Run one forced initial sync via `poller.sync_once(force=True)` before the app
   is considered ready.
7. Start one background task running `poller.run_forever(stop_event=...)`.
8. Start serving HTTP on `127.0.0.1`.

The initial forced sync is part of startup on purpose. It answers the operator
question "why did the old server start with no live refresh?" with one explicit
rule: because there was no lifespan wiring before; now readiness includes the
first live sync when the operator asked for live mode.

If live mode was explicitly selected by full env and the initial sync cannot be
completed, startup fails instead of silently falling back to offline mode.

### 8. Steady-state policy is fixed

After successful live startup:

- steady-state polling stays inside `Poller` and uses the cadence already fixed in
  ADR 0002
- there is exactly one background poller task per app process
- UI requests continue to read SQLite only
- browser activity never triggers direct Crawlab reads

There is no second runtime sync path alongside the app-owned poller.

### 9. Shutdown lifecycle is fixed

On clean process shutdown in live mode:

1. signal the poller stop event
2. await the background poller task to finish its current loop cleanly
3. close `ReadonlyCrawlabClient`
4. close the dedicated SQLite writer connection

On clean shutdown in SQLite-only mode:

1. close the dedicated SQLite writer connection

Request-scoped SQLite reads remain local-only and close through the normal route
dependency path.

### 10. The browser still never calls Crawlab directly

This ADR does not reopen the browser boundary.

The allowed runtime data path remains:

`ReadonlyCrawlabClient` -> normalization -> SQLite -> parser/status projections -> web routes/templates

Not allowed:

- browser-side Crawlab fetches
- browser-side tokens
- web routes that bypass SQLite and call Crawlab directly
- any new runtime Crawlab client besides `ReadonlyCrawlabClient`

### 11. T13 file boundary is explicit

`T13` should change only the files needed to implement this runtime choice.

Primary implementation files for `T13`:

- `src/cl_monitoring/settings.py`
- `src/cl_monitoring/app.py`
- `src/integrations/crawlab/readonly_client.py`
- `.env.example`
- `README.md`

Primary verification files for `T13`:

- `tests/test_web_routes.py`
- `tests/test_readonly_client.py`
- `tests/test_poller.py` only if a small lifecycle seam truly requires it
- `tests/test_app.py` as a new runtime-lifecycle test file

Files that `T13` should not treat as its implementation surface:

- parser/status contract files
- marker rollout docs and contracts
- new UI screens or action buttons
- `src/cl_monitoring/crawlab/client.py` as a second client implementation
- `src/cl_monitoring/sync/poller.py` unless a minimal lifecycle seam is strictly
  necessary

These files become the practical single-owner set for the `T13` build thread:

- `src/cl_monitoring/settings.py`
- `src/cl_monitoring/app.py`
- `src/integrations/crawlab/readonly_client.py`
- `.env.example`
- `README.md`
- `tests/test_readonly_client.py`
- `tests/test_web_routes.py`
- `tests/test_app.py`

## Consequences

- The previous behavior is now explicitly classified as incomplete wiring, not as
  the intended v1 operator flow.
- The app process becomes the only normal owner-facing runtime wrapper for web +
  poller together.
- Live mode is explicit and deterministic: full env starts poller, missing env
  gives SQLite-only mode, partial env is an error.
- `.env.example`, README, and runtime code can converge on one token key and one
  launch story.
- The browser/UI safety boundary remains unchanged: local SQLite only, never live
  Crawlab.
