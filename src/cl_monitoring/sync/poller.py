"""Background polling loop.

Flow: GET Crawlab API → normalize → write to SQLite.
Interval is configurable (default 60 s).
Failures are logged and recorded in sync_log table.
"""

# TODO: class Poller / async polling loop
