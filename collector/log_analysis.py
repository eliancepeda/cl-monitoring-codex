import re
from collections import Counter
from typing import Any


def analyze_log_text(log_payload: Any, tail_lines: int = 20) -> dict[str, Any]:
    if isinstance(log_payload, list):
        lines = [str(item) for item in log_payload]
    else:
        text = str(log_payload or "")
        lines = text.splitlines() if text else []

    joined = "\n".join(lines)
    counts = Counter(line.strip() for line in lines if line.strip())
    stable_fragments = sorted(line for line, count in counts.items() if count > 1)
    unstable_fragments = [line for line, count in counts.items() if count == 1][:10]

    return {
        "joined": joined,
        "line_count": len(lines),
        "char_count": len(joined),
        "tail": lines[-tail_lines:],
        "signals": {
            "empty_log": len(lines) == 0,
            "traceback_or_fatal": bool(
                re.search(r"\b(traceback|fatal|error|exception|failed)\b", joined, re.IGNORECASE)
            ),
            "timeout": bool(re.search(r"timed out|timeout", joined, re.IGNORECASE)),
            "http_403_404_5xx": bool(
                re.search(r"\bhttp(?:\W+error)?\W+(403|404|5\d\d)\b", joined, re.IGNORECASE)
            ),
            "found_or_written": bool(re.search(r"\b(found|written)\b", joined, re.IGNORECASE)),
            "has_summary": bool(re.search(r"\b(summary|stats|finished in)\b", joined, re.IGNORECASE)),
            "has_progress": bool(
                re.search(r"\b(\d+/\d+|processed|progress|page\s*: ?\d+)\b", joined, re.IGNORECASE)
            ),
        },
        "stable_fragments": stable_fragments,
        "unstable_fragments": unstable_fragments,
    }
