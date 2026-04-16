"""ReadonlyCrawlabClient — the single gateway to Crawlab API.

Safety invariants (AGENTS.md § Safety):
- Only GET requests are allowed.
- Any attempt to use POST/PUT/PATCH/DELETE MUST raise an error.
- Authorization header must never be logged or stored in fixtures.
"""

# TODO: class ReadonlyCrawlabClient: ...
