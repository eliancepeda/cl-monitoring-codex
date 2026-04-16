"""Redaction engine for Crawlab fixture data.

Provides stable, deterministic placeholder replacement for sensitive data.
Preserves zero ObjectId and zero time sentinels per AGENTS.md § Domain rules.
Preserves real timestamps per user_scope.yml redaction.preserve_real_timestamps.

Usage:
    redactor = Redactor(config)
    redacted = redactor.redact_json(raw_json, context="tasks")
    redacted_log = redactor.redact_log_text(raw_log_text)
"""

from __future__ import annotations

import copy
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Sentinel values (AGENTS.md § Domain rules) ─────────────────────────
ZERO_OBJECT_ID = "000000000000000000000000"
ZERO_TIME = "0001-01-01T00:00:00Z"

# ── Regex patterns ──────────────────────────────────────────────────────
# 24-hex-char ObjectId
OBJECT_ID_RE = re.compile(r"^[0-9a-f]{24}$")

# Hostname/IP patterns for log redaction
HOSTNAME_RE = re.compile(
    r"\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}\b"
)
IP_RE = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")

# URL pattern
URL_RE = re.compile(r"https?://[^\s\"',}\]]+")

# Unix path with username-like component
UNIX_PATH_RE = re.compile(r"/(?:home|Users)/([a-zA-Z0-9_.-]+)")

# ── Fields that contain ObjectId references ─────────────────────────────
OBJECT_ID_FIELDS = frozenset({
    "_id", "spider_id", "schedule_id", "node_id", "user_id",
    "task_id", "parent_id", "project_id",
})

# ── Category mapping for field names → placeholder prefix ──────────────
FIELD_CATEGORY_MAP: dict[str, str] = {
    "_id": "ID",
    "spider_id": "SPIDER_ID",
    "schedule_id": "SCHEDULE_ID",
    "node_id": "NODE_ID",
    "user_id": "USER_ID",
    "task_id": "TASK_ID",
    "parent_id": "PARENT_ID",
    "project_id": "PROJECT_ID",
}

# Well-known non-sensitive hostnames to preserve
SAFE_HOSTNAMES = frozenset({
    "localhost", "example.com", "github.com",
})


@dataclass
class RedactionConfig:
    """Configuration for redaction behavior."""
    preserve_zero_id: bool = True
    preserve_zero_time: bool = True
    preserve_real_timestamps: bool = True
    stable_placeholders: bool = True
    redact_hosts_in_logs: bool = True
    sensitive_strings: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RedactionConfig:
        """Create config from user_scope.yml redaction section."""
        return cls(
            preserve_zero_id=data.get("preserve_zero_id", True),
            preserve_zero_time=data.get("preserve_zero_time", True),
            preserve_real_timestamps=data.get("preserve_real_timestamps", True),
            stable_placeholders=data.get("stable_placeholders", True),
            redact_hosts_in_logs=data.get("redact_hosts_in_logs", True),
            sensitive_strings=data.get("sensitive_strings", []),
        )


class Redactor:
    """Deterministic redaction engine with stable indexed placeholders.

    The same real value always maps to the same placeholder within a single
    Redactor instance.  Mapping can be saved for debugging (to gitignored dir).

    Example:
        ObjectId "64a1b2c3d4e5f60718293a4b" → "SPIDER_ID_001"
        Same value everywhere in all fixtures → same placeholder.
    """

    def __init__(self, config: RedactionConfig | None = None) -> None:
        self._config = config or RedactionConfig()
        # category → {real_value: placeholder_string}
        self._maps: dict[str, dict[str, str]] = {}
        # category → next counter
        self._counters: dict[str, int] = {}
        # Collected hostnames for log redaction
        self._host_map: dict[str, str] = {}
        self._host_counter = 0
        # Path username map
        self._user_map: dict[str, str] = {}
        self._user_counter = 0

    # ── Public API ──────────────────────────────────────────────────────

    def redact_json(self, data: Any, context: str = "") -> Any:
        """Deep-walk and redact a JSON-serializable structure.

        Args:
            data: Parsed JSON (dict, list, or primitive).
            context: Human label for logging (e.g., "task", "spider").

        Returns:
            Deep copy with sensitive values replaced by stable placeholders.
        """
        result = copy.deepcopy(data)
        self._walk(result, parent_key="")
        return result

    def redact_log_text(self, text: str) -> str:
        """Redact sensitive patterns in log text.

        Redacts:
        - Hostnames and IPs (if redact_hosts_in_logs)
        - URLs containing real hosts
        - Unix paths with usernames
        - Sensitive strings from config
        """
        result = text

        # Redact sensitive strings first (explicit list from config)
        for s in self._config.sensitive_strings:
            if s and s in result:
                placeholder = self._get_placeholder("SENSITIVE", s)
                result = result.replace(s, placeholder)

        if self._config.redact_hosts_in_logs:
            # Redact full URLs first (before hostname extraction)
            result = URL_RE.sub(lambda m: self._redact_url(m.group(0)), result)
            # Redact standalone IPs
            result = IP_RE.sub(lambda m: self._redact_host(m.group(0)), result)
            # Redact hostnames (after URLs to avoid double-redaction)
            result = HOSTNAME_RE.sub(
                lambda m: self._redact_host_if_not_safe(m.group(0)), result
            )

        # Redact unix paths with usernames
        result = UNIX_PATH_RE.sub(
            lambda m: self._redact_unix_path(m.group(0), m.group(1)), result
        )

        return result

    def get_mapping(self) -> dict[str, dict[str, str]]:
        """Return the current placeholder mapping (for debugging)."""
        return {
            "object_ids": dict(self._maps),
            "hosts": dict(self._host_map),
            "users": dict(self._user_map),
        }

    def save_mapping(self, path: Path) -> None:
        """Persist mapping to a JSON file (should be in gitignored dir).

        This allows debugging and incremental runs.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.get_mapping(), f, indent=2, ensure_ascii=False)
        logger.info("Redaction mapping saved to %s", path)

    def load_mapping(self, path: Path) -> None:
        """Load a previously saved mapping for incremental runs."""
        if not path.exists():
            return
        with open(path) as f:
            saved = json.load(f)

        for category, entries in saved.get("object_ids", {}).items():
            if category not in self._maps:
                self._maps[category] = {}
            self._maps[category].update(entries)
            # Update counter to max existing
            for placeholder in entries.values():
                num = _extract_counter(placeholder)
                if num is not None:
                    self._counters[category] = max(
                        self._counters.get(category, 0), num + 1
                    )

        self._host_map.update(saved.get("hosts", {}))
        if self._host_map:
            max_host = max(
                (_extract_counter(v) or 0) for v in self._host_map.values()
            )
            self._host_counter = max_host + 1

        self._user_map.update(saved.get("users", {}))
        if self._user_map:
            max_user = max(
                (_extract_counter(v) or 0) for v in self._user_map.values()
            )
            self._user_counter = max_user + 1

    # ── Internal: JSON walk ─────────────────────────────────────────────

    def _walk(self, obj: Any, parent_key: str = "") -> Any:
        """Recursively walk and mutate JSON structure in-place."""
        if isinstance(obj, dict):
            for key in list(obj.keys()):
                value = obj[key]
                if isinstance(value, str):
                    obj[key] = self._redact_value(key, value)
                elif isinstance(value, (dict, list)):
                    self._walk(value, parent_key=key)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, str):
                    obj[i] = self._redact_value(parent_key, item)
                elif isinstance(item, (dict, list)):
                    self._walk(item, parent_key=parent_key)
        return obj

    def _redact_value(self, field_name: str, value: str) -> str:
        """Decide whether and how to redact a single string value."""
        # Preserve zero sentinels
        if self._config.preserve_zero_id and value == ZERO_OBJECT_ID:
            return value
        if self._config.preserve_zero_time and value == ZERO_TIME:
            return value

        # Check if this is a known ObjectId field
        if field_name in OBJECT_ID_FIELDS and OBJECT_ID_RE.match(value):
            category = FIELD_CATEGORY_MAP.get(field_name, "ID")
            return self._get_placeholder(category, value)

        # Check for token/auth values — defense in depth
        lower_field = field_name.lower()
        if any(kw in lower_field for kw in ("token", "auth", "secret", "password")):
            return "REDACTED"

        # Redact sensitive strings
        for s in self._config.sensitive_strings:
            if s and s in value:
                placeholder = self._get_placeholder("SENSITIVE", s)
                value = value.replace(s, placeholder)

        return value

    def _get_placeholder(self, category: str, real_value: str) -> str:
        """Return stable placeholder, creating if first seen."""
        if category not in self._maps:
            self._maps[category] = {}
            self._counters[category] = 1

        if real_value in self._maps[category]:
            return self._maps[category][real_value]

        counter = self._counters[category]
        placeholder = f"{category}_{counter:03d}"
        self._maps[category][real_value] = placeholder
        self._counters[category] = counter + 1
        return placeholder

    # ── Internal: Host/URL/path redaction ───────────────────────────────

    def _redact_host(self, host: str) -> str:
        """Replace a hostname/IP with a stable placeholder."""
        if host in self._host_map:
            return self._host_map[host]
        self._host_counter += 1
        placeholder = f"HOST_{self._host_counter:03d}"
        self._host_map[host] = placeholder
        return placeholder

    def _redact_host_if_not_safe(self, host: str) -> str:
        """Redact hostname only if it's not in the safe list."""
        if host.lower() in SAFE_HOSTNAMES:
            return host
        # Don't redact common file extensions mistaken as hostnames
        if host.count(".") == 1:
            ext = host.rsplit(".", 1)[-1]
            if ext in ("py", "js", "json", "yaml", "yml", "log", "txt", "csv", "md"):
                return host
        return self._redact_host(host)

    def _redact_url(self, url: str) -> str:
        """Redact the host portion of a URL, keep path structure."""
        # Extract scheme + host
        if "://" in url:
            scheme_rest = url.split("://", 1)
            scheme = scheme_rest[0]
            rest = scheme_rest[1]
            # Split host from path
            slash_idx = rest.find("/")
            if slash_idx >= 0:
                host = rest[:slash_idx]
                path = rest[slash_idx:]
            else:
                host = rest
                path = ""
            redacted_host = self._redact_host(host.split(":")[0])
            # Preserve port if present
            port = ""
            if ":" in host:
                port = ":" + host.split(":", 1)[1]
            return f"{scheme}://{redacted_host}{port}{path}"
        return url

    def _redact_unix_path(self, full_match: str, username: str) -> str:
        """Redact the username portion of a unix path."""
        if username not in self._user_map:
            self._user_counter += 1
            self._user_map[username] = f"USER_{self._user_counter:03d}"
        return full_match.replace(username, self._user_map[username])


def _extract_counter(placeholder: str) -> int | None:
    """Extract the numeric counter from a placeholder like 'SPIDER_ID_003'."""
    parts = placeholder.rsplit("_", 1)
    if len(parts) == 2:
        try:
            return int(parts[1])
        except ValueError:
            return None
    return None
