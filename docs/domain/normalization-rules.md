# Normalization Rules

## Observed

When consuming Crawlab API payloads, specific sentinel values must be standardized to prevent logical errors in the domain model.

### Null Object IDs (Sentinel zero)
Fields like `schedule_id`, `parent_id`, `data_source_id`, and `user_id` are often not standard null types when absent.
* **Observe**: `"000000000000000000000000"`
* **Normalize to**: `None` / Domain null.
* **Usage**: Extremely critical for separating manual tasks (`schedule_id` matching this zero-id) from auto-scheduled ones.

### Null Dates (Sentinel zero time)
Dates such as `start_ts` or `end_ts` in pending/running tasks lack valid values since the event has not occurred.
* **Observe**: `"0001-01-01T00:00:00Z"`
* **Normalize to**: `None` / Domain null.
* **Usage**: Required for accurate uptime math. If a record has this value for `start_ts`, it means the task hasn't physically started execution.

## Inferred

### Dynamic Runtime Calculation
For running tasks, Crawlab sometimes keeps `runtime_duration` as `0` while the object is in flight.
* **Rule**: Compute live runtime as `now() - normalized(start_ts)` if `status == "running"` and duration bounds are zero. 

### Parameter Normalization (Execution Key)
When computing the `execution_key`:
* **Observation**: Spaces, trailing arguments, and potential typos may separate identical logical targets.
* **Rule**: We must normalize both `cmd` and `param` (e.g., stripping edge whitespaces, ensuring consistent delimiters) before hashing or concatenating them with `spider_id` to prevent cardinality bloat.

## Unresolved
* **Timezone adjustments**: All timestamps are UTC (`Z`). Verification is needed regarding whether Crawlab ever emits machine-local non-Z dates in older agent versions.
* **Empty strings vs. Null**: Fields like `error` may exist as `""` (empty string). Should empty strings in error boundaries be normalized to boolean false or left as empty strings?
