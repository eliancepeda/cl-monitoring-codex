"""Normalization functions for Crawlab data.

Rules (AGENTS.md § Domain rules):
- "000000000000000000000000" → None  (zero-id)
- "0001-01-01T00:00:00Z"    → None  (null time)
- Manual run detection: schedule_id is zero-id
- execution_key = spider_id + normalized cmd + normalized param
- Live runtime: now() - start_at  when runtime_duration == 0
"""

# TODO: normalize_id(), normalize_time(), build_execution_key(),
#       compute_live_runtime(), is_manual_run()
