# Crawlab Live Contract

## Observed

### Endpoints and Payload Shapes

Based on the raw fixtures observed in `fixtures/api/`, we can define the following payload structures:

**1. Tasks (`/api/tasks` & `/api/tasks/{id}`)**
* `_id` (string): The task identifier.
* `spider_id` (string): References the executed spider.
* `status` (string): Examples include `"pending"`, `"running"`, `"finished"`, `"cancelled"`.
* `cmd` (string) & `param` (string): The actual execution command footprint.
* `schedule_id` (string): References the schedule if triggered automatically.
* `stat` (object): Contains timing metrics:
  * `create_ts`, `start_ts`, `end_ts` (string): In ISO 8601 format. Note: Can contain zero times.
  * `wait_duration`, `runtime_duration`, `total_duration` (int): Measured duration in microseconds/milliseconds.

**2. Spiders (`/api/spiders/{id}`)**
* `_id` (string) & `name` (string): Identity of the spider.
* `col_name` (string): Target MongoDB collection where results go (`results_some.name`).
* `stat` (object): Spider-wide aggregates (`tasks`, `results`, `runtime_duration`).
* `cmd`, `param` (string): Default command/params for manual executions.

**3. Schedules (`/api/schedules` & `/api/schedules/{id}`)**
* `_id`, `name`, `spider_id` (string).
* `cron` (string): CRON expression.
* `entry_id` (int): E.g., `1`, `-1` (often `-1` when disabled or in certain states).
* `cmd`, `param` (string): Overridden commands specific to this schedule.
* `enabled` (boolean).

### Task Data Access

Currently, tasks don't embed the result set directly. 
As stated in the project constraints, results are read via `/api/results/{col_id}` and must be filtered by the `_tid` (task ID) parameter.

## Inferred

### Execution Origin (Scheduled vs. Manual)
* **Scheduled**: The `schedule_id` contains a valid Object ID string.
* **Manual**: The `schedule_id` is set to the zero/null sentinel ObjectId `000000000000000000000000`. This allows a deterministic, payload-based check for the task's origin.

### Execution Key Composition
The tracking relies on an `execution_key = spider_id + cmd + param`.
* **Why**: Looking at `schedules.json`, a single `spider_id` (e.g., `SPIDER_ID_012`) has *multiple* schedules running the exact same script but with different parameters (e.g., `-sp 1 -fp 14` vs `-sp 15 -fp 30`). 
By constructing the key this way, we can logically segment separate "jobs" that map to the same spider container. `schedule_id` is omitted from the key so that a manual run mimicking a scheduled run's arguments maps to the exact same workload history.

## Unresolved

* **Nodes Architecture**: Node structures (`node_ids`, mode="selected-nodes") appear heavily in payloads, but are deferred out of scope for v1.
* **Result Payload Shape**: While we know the route to query expected results (`/api/results/{col_id}`), we do not have specific result mock files in the current fixture set to confirm the full nested shape.
