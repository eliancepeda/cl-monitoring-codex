# Crawlab Read-Only Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a minimal Python collector that safely performs staged, GET-only discovery against Crawlab for the two approved project IDs and writes raw, normalized, and markdown artifacts for GPT Pro handoff.

**Architecture:** Use a small stdlib-only Python package centered on a single GET-only transport, then layer raw storage, normalization, log analysis, wheel inspection, reporting, and orchestration on top. Keep network logic isolated in one module, treat undocumented behavior as `unknown`, and write artifacts directly into `docs/discovery/` so the live run produces a ready-to-review packet.

**Tech Stack:** Python 3 standard library (`argparse`, `dataclasses`, `json`, `pathlib`, `re`, `shlex`, `time`, `urllib`, `unittest`, `zipfile`)

---

## File Structure

- Create: `collector/__init__.py` - package marker for the collector modules.
- Create: `collector/config.py` - environment-backed runtime settings with safe validation.
- Create: `collector/transport.py` - the only HTTP layer, hard-blocking all methods except `GET`.
- Create: `collector/raw_store.py` - file writer for raw JSON, normalized JSON, and markdown output.
- Create: `collector/normalize.py` - launch-parameter parsing, classification, and observation-unit building.
- Create: `collector/log_analysis.py` - lightweight log feature extraction and tail selection.
- Create: `collector/library_inspection.py` - wheel inspection for spider parameter observations.
- Create: `collector/reporting.py` - markdown rendering for the five discovery docs.
- Create: `collector/discovery.py` - staged discovery orchestration, low-load call policy, and artifact assembly.
- Create: `run_discovery.py` - CLI entry point for the live collector.
- Create: `tests/test_config.py` - settings loader tests.
- Create: `tests/test_transport.py` - GET-only transport and secret-safety tests.
- Create: `tests/test_raw_store.py` - artifact writer tests.
- Create: `tests/test_normalize.py` - parameter taxonomy and observation-unit tests.
- Create: `tests/test_log_analysis.py` - log signal extraction tests.
- Create: `tests/test_library_inspection.py` - wheel inspection tests.
- Create: `tests/test_reporting.py` - markdown report rendering tests.
- Create: `tests/test_discovery.py` - staged discovery orchestration tests with fake transport responses.
- Create: `tests/test_cli.py` - CLI argument parsing test.

### Task 1: Bootstrap Package And Runtime Settings

**Files:**
- Create: `collector/__init__.py`
- Create: `collector/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing settings tests**

```python
import unittest

from collector.config import load_settings


class LoadSettingsTests(unittest.TestCase):
    def test_load_settings_requires_env_vars_without_echoing_values(self):
        with self.assertRaises(ValueError) as context:
            load_settings(project_ids=["project-a", "project-b"], environ={})

        self.assertEqual(
            str(context.exception),
            "Missing required environment variables: CRAWLAB_BASE_URL, CRAWLAB_API_KEY",
        )

    def test_load_settings_normalizes_base_url_and_defaults(self):
        settings = load_settings(
            project_ids=["project-a", "project-b"],
            environ={
                "CRAWLAB_BASE_URL": "https://crawlab.example/",
                "CRAWLAB_API_KEY": "secret-token",
            },
        )

        self.assertEqual(settings.base_url, "https://crawlab.example")
        self.assertEqual(settings.project_ids, ("project-a", "project-b"))
        self.assertEqual(settings.output_root, "docs/discovery")
        self.assertEqual(settings.throttle_seconds, 0.5)
        self.assertEqual(settings.page_size, 10)
        self.assertEqual(settings.expanded_task_limit, 20)
        self.assertEqual(settings.max_pages, 3)
        self.assertEqual(settings.log_page_size, 100)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python3 -m unittest discover -s tests -p "test_config.py" -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector'`

- [ ] **Step 3: Write the minimal package marker and settings loader**

`collector/__init__.py`

```python
"""Read-only Crawlab discovery collector."""
```

`collector/config.py`

```python
import os
from dataclasses import dataclass
from typing import Mapping, Sequence


@dataclass(frozen=True)
class Settings:
    base_url: str
    api_key: str
    project_ids: tuple[str, ...]
    output_root: str
    throttle_seconds: float
    page_size: int
    expanded_task_limit: int
    max_pages: int
    log_page_size: int


def load_settings(
    project_ids: Sequence[str],
    environ: Mapping[str, str] | None = None,
    output_root: str = "docs/discovery",
    throttle_seconds: float = 0.5,
    page_size: int = 10,
    expanded_task_limit: int = 20,
    max_pages: int = 3,
    log_page_size: int = 100,
) -> Settings:
    env = dict(os.environ if environ is None else environ)
    missing = [
        name
        for name in ("CRAWLAB_BASE_URL", "CRAWLAB_API_KEY")
        if not env.get(name)
    ]
    if missing:
        raise ValueError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    normalized_project_ids = tuple(item.strip() for item in project_ids if item.strip())
    if not normalized_project_ids:
        raise ValueError("At least one project id is required")

    return Settings(
        base_url=env["CRAWLAB_BASE_URL"].rstrip("/"),
        api_key=env["CRAWLAB_API_KEY"],
        project_ids=normalized_project_ids,
        output_root=output_root,
        throttle_seconds=throttle_seconds,
        page_size=page_size,
        expanded_task_limit=expanded_task_limit,
        max_pages=max_pages,
        log_page_size=log_page_size,
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python3 -m unittest discover -s tests -p "test_config.py" -v`
Expected: PASS with `OK`

- [ ] **Step 5: Commit the bootstrap**

```bash
git add collector/__init__.py collector/config.py tests/test_config.py
git commit -m "feat: add discovery runtime settings"
```

### Task 2: Add The GET-Only Transport Layer

**Files:**
- Create: `collector/transport.py`
- Test: `tests/test_transport.py`

- [ ] **Step 1: Write the failing transport tests**

```python
import io
import json
import unittest
from urllib.error import HTTPError

from collector.transport import GetOnlyTransport


class FakeHttpResponse:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status = status

    def read(self):
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TransportTests(unittest.TestCase):
    def test_get_builds_url_headers_and_parses_json(self):
        captured = {}

        def opener(request):
            captured["url"] = request.full_url
            captured["auth"] = request.headers["Authorization"]
            captured["method"] = request.get_method()
            return FakeHttpResponse({"status": "ok", "data": []})

        transport = GetOnlyTransport(
            base_url="https://crawlab.example",
            api_key="secret-token",
            throttle_seconds=0,
            opener=opener,
            sleeper=lambda _: None,
            clock=lambda: 1710000000.0,
        )

        response = transport.get("/api/projects", {"page": 1, "size": 10})

        self.assertEqual(
            captured["url"],
            "https://crawlab.example/api/projects?page=1&size=10",
        )
        self.assertEqual(captured["auth"], "secret-token")
        self.assertEqual(captured["method"], "GET")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.json_data, {"status": "ok", "data": []})
        self.assertEqual(response.meta.path, "/api/projects")

    def test_request_rejects_non_get_methods(self):
        transport = GetOnlyTransport(
            base_url="https://crawlab.example",
            api_key="secret-token",
            throttle_seconds=0,
            opener=lambda request: FakeHttpResponse({"status": "ok"}),
            sleeper=lambda _: None,
        )

        with self.assertRaises(ValueError) as context:
            transport.request("POST", "/api/projects")

        self.assertEqual(str(context.exception), "Only GET is allowed for Crawlab discovery")

    def test_http_errors_do_not_echo_api_key(self):
        def opener(request):
            raise HTTPError(
                url=request.full_url,
                code=403,
                msg="Forbidden",
                hdrs=None,
                fp=io.BytesIO(b'{"error": "forbidden"}'),
            )

        transport = GetOnlyTransport(
            base_url="https://crawlab.example",
            api_key="secret-token",
            throttle_seconds=0,
            opener=opener,
            sleeper=lambda _: None,
        )

        with self.assertRaises(RuntimeError) as context:
            transport.get("/api/projects")

        self.assertIn("GET /api/projects failed with status 403", str(context.exception))
        self.assertNotIn("secret-token", str(context.exception))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the transport test to verify it fails**

Run: `python3 -m unittest discover -s tests -p "test_transport.py" -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.transport'`

- [ ] **Step 3: Write the GET-only transport implementation**

`collector/transport.py`

```python
import json
from dataclasses import dataclass
from time import sleep, time
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class ResponseMeta:
    method: str
    path: str
    query: dict[str, Any]
    status: int
    fetched_at: float


@dataclass(frozen=True)
class TransportResponse:
    status: int
    text: str
    json_data: Any
    meta: ResponseMeta


class GetOnlyTransport:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        throttle_seconds: float = 0.5,
        opener: Callable[[Request], Any] = urlopen,
        sleeper: Callable[[float], None] = sleep,
        clock: Callable[[], float] = time,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.throttle_seconds = throttle_seconds
        self.opener = opener
        self.sleeper = sleeper
        self.clock = clock

    def request(
        self,
        method: str,
        path: str,
        query: Mapping[str, Any] | None = None,
    ) -> TransportResponse:
        if method != "GET":
            raise ValueError("Only GET is allowed for Crawlab discovery")

        normalized_query = dict(query or {})
        url = f"{self.base_url}{path}"
        if normalized_query:
            url = f"{url}?{urlencode(normalized_query)}"

        request = Request(
            url,
            headers={"Authorization": self.api_key, "Accept": "application/json"},
            method="GET",
        )

        if self.throttle_seconds > 0:
            self.sleeper(self.throttle_seconds)

        try:
            with self.opener(request) as response:
                text = response.read().decode("utf-8")
                status = getattr(response, "status", 200)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"GET {path} failed with status {exc.code}: {body[:200]}"
            ) from exc
        except URLError as exc:
            raise RuntimeError(f"GET {path} failed: {exc.reason}") from exc

        try:
            json_data = json.loads(text) if text else None
        except json.JSONDecodeError:
            json_data = None

        return TransportResponse(
            status=status,
            text=text,
            json_data=json_data,
            meta=ResponseMeta(
                method="GET",
                path=path,
                query=normalized_query,
                status=status,
                fetched_at=self.clock(),
            ),
        )

    def get(
        self,
        path: str,
        query: Mapping[str, Any] | None = None,
    ) -> TransportResponse:
        return self.request("GET", path, query)
```

- [ ] **Step 4: Run the transport test to verify it passes**

Run: `python3 -m unittest discover -s tests -p "test_transport.py" -v`
Expected: PASS with `OK`

- [ ] **Step 5: Commit the transport layer**

```bash
git add collector/transport.py tests/test_transport.py
git commit -m "feat: add get-only Crawlab transport"
```

### Task 3: Add Artifact Storage Primitives

**Files:**
- Create: `collector/raw_store.py`
- Test: `tests/test_raw_store.py`

- [ ] **Step 1: Write the failing artifact store tests**

```python
import json
import tempfile
import unittest
from pathlib import Path

from collector.raw_store import ArtifactStore


class ArtifactStoreTests(unittest.TestCase):
    def test_write_json_creates_parent_directories(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)

            written_path = store.write_json("raw/projects/page-1.json", {"status": "ok"})

            self.assertTrue(Path(written_path).exists())
            self.assertEqual(
                json.loads(Path(written_path).read_text(encoding="utf-8")),
                {"status": "ok"},
            )

    def test_write_text_creates_markdown_documents(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)

            written_path = store.write_text("api-map.md", "# API Map\n")

            self.assertTrue(Path(written_path).exists())
            self.assertEqual(Path(written_path).read_text(encoding="utf-8"), "# API Map\n")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the artifact store test to verify it fails**

Run: `python3 -m unittest discover -s tests -p "test_raw_store.py" -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.raw_store'`

- [ ] **Step 3: Write the artifact store implementation**

`collector/raw_store.py`

```python
import json
from pathlib import Path
from typing import Any


class ArtifactStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def write_json(self, relative_path: str, payload: Any) -> str:
        target = self.root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return str(target)

    def write_text(self, relative_path: str, content: str) -> str:
        target = self.root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return str(target)
```

- [ ] **Step 4: Run the artifact store test to verify it passes**

Run: `python3 -m unittest discover -s tests -p "test_raw_store.py" -v`
Expected: PASS with `OK`

- [ ] **Step 5: Commit the artifact storage layer**

```bash
git add collector/raw_store.py tests/test_raw_store.py
git commit -m "feat: add discovery artifact storage"
```

### Task 4: Normalize Launch Parameters And Observation Units

**Files:**
- Create: `collector/normalize.py`
- Test: `tests/test_normalize.py`

- [ ] **Step 1: Write the failing normalization tests**

```python
import unittest

from collector.normalize import build_observation_unit, normalize_launch_parameters


class NormalizeTests(unittest.TestCase):
    def test_normalize_launch_parameters_marks_rule_and_hypothesis_roles(self):
        normalized = normalize_launch_parameters("--sp catalog --proxy_country us --debug")
        by_key = {item["normalized_key"]: item for item in normalized["parameters"]}

        self.assertEqual(by_key["sp"]["role"], "identity candidate")
        self.assertEqual(by_key["sp"]["classification_status"], "hypothesis")
        self.assertEqual(by_key["proxy_country"]["role"], "execution modifier")
        self.assertEqual(by_key["proxy_country"]["classification_status"], "fact")
        self.assertEqual(by_key["debug"]["role"], "non-production flag")
        self.assertEqual(by_key["debug"]["classification_status"], "fact")

    def test_build_observation_unit_uses_spider_schedule_and_params(self):
        task = {
            "_id": "task-1",
            "schedule_id": "schedule-1",
            "args": "--sp catalog --fp shoes",
            "status": "finished",
        }
        spider = {"_id": "spider-1", "name": "demo-spider"}

        observation = build_observation_unit(task, spider)

        self.assertEqual(
            observation["observation_key"],
            "demo-spider|schedule-1|fp=shoes|sp=catalog",
        )
        self.assertEqual(observation["task_id"], "task-1")
        self.assertEqual(observation["spider_name"], "demo-spider")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the normalization test to verify it fails**

Run: `python3 -m unittest discover -s tests -p "test_normalize.py" -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.normalize'`

- [ ] **Step 3: Write the normalization implementation**

`collector/normalize.py`

```python
import shlex
from typing import Any


FACT_ROLE_MAP = {
    "proxy_country": "execution modifier",
    "proxy_vpn_country": "execution modifier",
    "debug": "non-production flag",
    "test": "non-production flag",
}

HYPOTHESIS_ROLE_MAP = {
    "sp": "identity candidate",
    "fp": "identity candidate",
    "pc": "shard/scope candidate",
    "tags": "shard/scope candidate",
    "as": "execution modifier",
    "spi": "rerun/recovery indicator",
}


def tokenize_args(raw_args: Any) -> list[str]:
    if raw_args is None:
        return []
    if isinstance(raw_args, str):
        return shlex.split(raw_args)
    if isinstance(raw_args, list):
        return [str(item) for item in raw_args]
    return [str(raw_args)]


def classify_parameter(normalized_key: str) -> dict[str, str]:
    if normalized_key in FACT_ROLE_MAP:
        return {
            "role": FACT_ROLE_MAP[normalized_key],
            "classification_status": "fact",
        }
    if normalized_key in HYPOTHESIS_ROLE_MAP:
        return {
            "role": HYPOTHESIS_ROLE_MAP[normalized_key],
            "classification_status": "hypothesis",
        }
    return {"role": "unknown", "classification_status": "unknown"}


def normalize_launch_parameters(raw_args: Any) -> dict[str, Any]:
    tokens = tokenize_args(raw_args)
    parameters = []
    positionals = []
    index = 0

    while index < len(tokens):
        token = tokens[index]
        if not token.startswith("-"):
            positionals.append(token)
            index += 1
            continue

        value: Any = True
        flag = token
        if "=" in token:
            flag, value = token.split("=", 1)
        elif index + 1 < len(tokens) and not tokens[index + 1].startswith("-"):
            value = tokens[index + 1]
            index += 1

        normalized_key = flag.lstrip("-").replace("-", "_")
        classification = classify_parameter(normalized_key)
        parameters.append(
            {
                "raw_token": token,
                "normalized_key": normalized_key,
                "value": value,
                "role": classification["role"],
                "classification_status": classification["classification_status"],
            }
        )
        index += 1

    return {
        "raw_args": raw_args,
        "tokens": tokens,
        "parameters": parameters,
        "positionals": positionals,
    }


def _first_present(record: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        if isinstance(value, dict):
            nested_id = value.get("_id") or value.get("id")
            if nested_id is not None:
                return nested_id
        return value
    return None


def build_observation_unit(task: dict[str, Any], spider: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_launch_parameters(
        task.get("args") or task.get("command") or task.get("cmd")
    )
    schedule_id = _first_present(task, ("schedule_id", "scheduleId", "schedule")) or "unscheduled"
    ordered_pairs = sorted(
        f"{item['normalized_key']}={item['value']}"
        for item in normalized["parameters"]
    )
    observation_key = "|".join(
        [
            spider.get("name") or spider.get("_id") or "unknown-spider",
            str(schedule_id),
            *(ordered_pairs or ["no-params"]),
        ]
    )
    return {
        "task_id": task.get("_id") or task.get("id"),
        "spider_id": spider.get("_id") or spider.get("id"),
        "spider_name": spider.get("name") or spider.get("_id") or "unknown-spider",
        "schedule_id": str(schedule_id),
        "normalized_params": normalized,
        "observation_key": observation_key,
    }
```

- [ ] **Step 4: Run the normalization test to verify it passes**

Run: `python3 -m unittest discover -s tests -p "test_normalize.py" -v`
Expected: PASS with `OK`

- [ ] **Step 5: Commit the normalization layer**

```bash
git add collector/normalize.py tests/test_normalize.py
git commit -m "feat: normalize discovery launch parameters"
```

### Task 5: Analyze Log Signals Without Full-Text Search

**Files:**
- Create: `collector/log_analysis.py`
- Test: `tests/test_log_analysis.py`

- [ ] **Step 1: Write the failing log analysis tests**

```python
import unittest

from collector.log_analysis import analyze_log_text


class LogAnalysisTests(unittest.TestCase):
    def test_analyze_log_detects_errors_progress_summary_and_tail(self):
        text = "\n".join(
            [
                "start crawl",
                "processed 10/20",
                "HTTP 503 upstream",
                "found 15 items",
                "written 15 items",
                "Traceback: boom",
                "Summary: done",
                "final line",
            ]
        )

        result = analyze_log_text(text, tail_lines=2)

        self.assertTrue(result["signals"]["traceback_or_fatal"])
        self.assertTrue(result["signals"]["http_403_404_5xx"])
        self.assertTrue(result["signals"]["found_or_written"])
        self.assertTrue(result["signals"]["has_summary"])
        self.assertTrue(result["signals"]["has_progress"])
        self.assertEqual(result["tail"], ["Summary: done", "final line"])

    def test_analyze_log_marks_empty_logs(self):
        result = analyze_log_text("")

        self.assertTrue(result["signals"]["empty_log"])
        self.assertEqual(result["tail"], [])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the log analysis test to verify it fails**

Run: `python3 -m unittest discover -s tests -p "test_log_analysis.py" -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.log_analysis'`

- [ ] **Step 3: Write the lightweight log analysis implementation**

`collector/log_analysis.py`

```python
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
        "line_count": len(lines),
        "char_count": len(joined),
        "tail": lines[-tail_lines:],
        "signals": {
            "empty_log": len(lines) == 0,
            "traceback_or_fatal": bool(re.search(r"traceback|fatal", joined, re.IGNORECASE)),
            "timeout": bool(re.search(r"timed out|timeout", joined, re.IGNORECASE)),
            "http_403_404_5xx": bool(re.search(r"\b(403|404|5\d\d)\b", joined)),
            "found_or_written": bool(re.search(r"\b(found|written)\b", joined, re.IGNORECASE)),
            "has_summary": bool(re.search(r"\b(summary|stats|finished in)\b", joined, re.IGNORECASE)),
            "has_progress": bool(re.search(r"\b(\d+/\d+|processed|progress|page \d+)\b", joined, re.IGNORECASE)),
        },
        "stable_fragments": stable_fragments,
        "unstable_fragments": unstable_fragments,
    }
```

- [ ] **Step 4: Run the log analysis test to verify it passes**

Run: `python3 -m unittest discover -s tests -p "test_log_analysis.py" -v`
Expected: PASS with `OK`

- [ ] **Step 5: Commit the log analysis layer**

```bash
git add collector/log_analysis.py tests/test_log_analysis.py
git commit -m "feat: extract discovery log signals"
```

### Task 6: Inspect Local Wheels For Parameter Observations

**Files:**
- Create: `collector/library_inspection.py`
- Test: `tests/test_library_inspection.py`

- [ ] **Step 1: Write the failing wheel inspection tests**

```python
import tempfile
import unittest
import zipfile
from pathlib import Path

from collector.library_inspection import inspect_wheel_sources


class LibraryInspectionTests(unittest.TestCase):
    def test_inspect_wheel_sources_extracts_flags_and_snippets(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            wheel_path = Path(tmp_dir) / "demo.whl"
            with zipfile.ZipFile(wheel_path, "w") as archive:
                archive.writestr(
                    "pkg/spider_manager.py",
                    "command = '--sp catalog --fp shoes --proxy_country us'\n",
                )

            result = inspect_wheel_sources(
                [
                    {
                        "wheel_path": str(wheel_path),
                        "internal_path": "pkg/spider_manager.py",
                    }
                ]
            )

        self.assertEqual(result[0]["status"], "fact")
        self.assertEqual(
            result[0]["matched_flags"],
            ["--fp", "--proxy_country", "--sp"],
        )
        self.assertEqual(
            result[0]["snippets"],
            ["command = '--sp catalog --fp shoes --proxy_country us'"],
        )

    def test_inspect_wheel_sources_marks_missing_internal_paths_unknown(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            wheel_path = Path(tmp_dir) / "demo.whl"
            with zipfile.ZipFile(wheel_path, "w") as archive:
                archive.writestr("pkg/other.py", "pass\n")

            result = inspect_wheel_sources(
                [
                    {
                        "wheel_path": str(wheel_path),
                        "internal_path": "pkg/spider_manager.py",
                    }
                ]
            )

        self.assertEqual(result[0]["status"], "unknown")
        self.assertEqual(result[0]["matched_flags"], [])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the wheel inspection test to verify it fails**

Run: `python3 -m unittest discover -s tests -p "test_library_inspection.py" -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.library_inspection'`

- [ ] **Step 3: Write the wheel inspection implementation**

`collector/library_inspection.py`

```python
import re
from pathlib import Path
from zipfile import ZipFile


def inspect_wheel_sources(targets: list[dict[str, str]]) -> list[dict[str, object]]:
    observations: list[dict[str, object]] = []
    for target in targets:
        wheel_path = Path(target["wheel_path"])
        internal_path = target["internal_path"]

        if not wheel_path.exists():
            observations.append(
                {
                    "wheel_path": str(wheel_path),
                    "internal_path": internal_path,
                    "status": "unknown",
                    "matched_flags": [],
                    "snippets": [],
                }
            )
            continue

        with ZipFile(wheel_path) as archive:
            if internal_path not in archive.namelist():
                observations.append(
                    {
                        "wheel_path": str(wheel_path),
                        "internal_path": internal_path,
                        "status": "unknown",
                        "matched_flags": [],
                        "snippets": [],
                    }
                )
                continue

            source = archive.read(internal_path).decode("utf-8", errors="replace")
            matched_flags = sorted(set(re.findall(r"--?[a-zA-Z][a-zA-Z0-9_-]*", source)))
            snippets = [
                line.strip()
                for line in source.splitlines()
                if any(flag in line for flag in matched_flags)
            ][:20]
            observations.append(
                {
                    "wheel_path": str(wheel_path),
                    "internal_path": internal_path,
                    "status": "fact",
                    "matched_flags": matched_flags,
                    "snippets": snippets,
                }
            )

    return observations
```

- [ ] **Step 4: Run the wheel inspection test to verify it passes**

Run: `python3 -m unittest discover -s tests -p "test_library_inspection.py" -v`
Expected: PASS with `OK`

- [ ] **Step 5: Commit the wheel inspection layer**

```bash
git add collector/library_inspection.py tests/test_library_inspection.py
git commit -m "feat: inspect local spider parameter sources"
```

### Task 7: Render The Discovery Markdown Documents

**Files:**
- Create: `collector/reporting.py`
- Test: `tests/test_reporting.py`

- [ ] **Step 1: Write the failing reporting tests**

```python
import unittest

from collector.reporting import render_report_documents


class ReportingTests(unittest.TestCase):
    def test_render_report_documents_returns_all_expected_files(self):
        documents = render_report_documents(
            {
                "api_map": [
                    {
                        "path": "/api/projects",
                        "method": "GET",
                        "query": {"page": 1, "size": 10},
                        "source": "documented",
                        "status": "observed",
                        "notes": "HTTP 200",
                    }
                ],
                "entity_summary": {
                    "projects": {"count": 2, "fields": ["_id", "name"]},
                    "spiders": {"count": 3, "fields": ["_id", "name", "project_id"]},
                },
                "parameter_records": [
                    {
                        "normalized_key": "proxy_country",
                        "role": "execution modifier",
                        "classification_status": "fact",
                        "value": "us",
                    },
                    {
                        "normalized_key": "sp",
                        "role": "identity candidate",
                        "classification_status": "hypothesis",
                        "value": "catalog",
                    },
                ],
                "library_observations": [
                    {
                        "wheel_path": "crawlib/example.whl",
                        "internal_path": "pkg/spider_manager.py",
                        "status": "fact",
                        "matched_flags": ["--sp", "--fp"],
                        "snippets": ["command = '--sp catalog --fp shoes'"],
                    }
                ],
                "log_samples": [
                    {
                        "case_type": "successful",
                        "task_id": "task-1",
                        "signals": {"has_summary": True, "empty_log": False},
                        "tail": ["Summary: done"],
                    }
                ],
                "open_questions": ["Need to confirm project-to-spider link field on this instance."],
            }
        )

        self.assertEqual(
            sorted(documents.keys()),
            sorted(
                [
                    "api-map.md",
                    "entity-summary.md",
                    "parameter-taxonomy.md",
                    "log-patterns.md",
                    "open-questions.md",
                ]
            ),
        )
        self.assertIn("| `/api/projects` | `GET` |", documents["api-map.md"])
        self.assertIn("## Facts", documents["parameter-taxonomy.md"])
        self.assertIn("## Hypotheses", documents["parameter-taxonomy.md"])
        self.assertIn("Summary: done", documents["log-patterns.md"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the reporting test to verify it fails**

Run: `python3 -m unittest discover -s tests -p "test_reporting.py" -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.reporting'`

- [ ] **Step 3: Write the markdown rendering implementation**

`collector/reporting.py`

```python
from typing import Any


def render_api_map(api_map: list[dict[str, Any]]) -> str:
    lines = [
        "# API Map",
        "",
        "| Path | Method | Query | Source | Status | Notes |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in api_map:
        lines.append(
            "| `{path}` | `{method}` | `{query}` | `{source}` | `{status}` | {notes} |".format(
                path=row["path"],
                method=row["method"],
                query=row["query"],
                source=row["source"],
                status=row["status"],
                notes=row["notes"],
            )
        )
    return "\n".join(lines) + "\n"


def render_entity_summary(entity_summary: dict[str, dict[str, Any]]) -> str:
    lines = ["# Entity Summary", ""]
    for entity_name, payload in entity_summary.items():
        lines.extend(
            [
                f"## {entity_name.title()}",
                "",
                f"- Count: {payload.get('count', 0)}",
                f"- Fields: {', '.join(payload.get('fields', [])) or 'none observed'}",
                "",
            ]
        )
    return "\n".join(lines)


def render_parameter_taxonomy(
    parameter_records: list[dict[str, Any]],
    library_observations: list[dict[str, Any]],
) -> str:
    grouped = {"fact": [], "hypothesis": [], "unknown": []}
    for record in parameter_records:
        grouped.setdefault(record["classification_status"], []).append(record)

    lines = ["# Parameter Taxonomy", ""]
    for heading, key in (("Facts", "fact"), ("Hypotheses", "hypothesis"), ("Unknowns", "unknown")):
        lines.append(f"## {heading}")
        lines.append("")
        records = grouped.get(key, [])
        if not records:
            lines.append("- none")
        else:
            for record in records:
                lines.append(
                    f"- `{record['normalized_key']}` -> `{record['role']}` (value: `{record.get('value')}`)"
                )
        lines.append("")

    lines.append("## Library Observations")
    lines.append("")
    if not library_observations:
        lines.append("- none")
    else:
        for item in library_observations:
            lines.append(
                f"- `{item['internal_path']}` in `{item['wheel_path']}` -> {', '.join(item['matched_flags']) or 'no flags'}"
            )
    lines.append("")
    return "\n".join(lines)


def render_log_patterns(log_samples: list[dict[str, Any]]) -> str:
    lines = ["# Log Patterns", ""]
    if not log_samples:
        lines.extend(["- no representative logs observed", ""])
        return "\n".join(lines)

    for sample in log_samples:
        lines.extend(
            [
                f"## {sample['case_type']}",
                "",
                f"- Task: `{sample['task_id']}`",
                f"- Signals: `{sample['signals']}`",
                f"- Tail: `{sample['tail']}`",
                "",
            ]
        )
    return "\n".join(lines)


def render_open_questions(open_questions: list[str]) -> str:
    lines = ["# Open Questions", ""]
    if not open_questions:
        lines.append("- none")
    else:
        lines.extend(f"- {item}" for item in open_questions)
    lines.append("")
    return "\n".join(lines)


def render_report_documents(bundle: dict[str, Any]) -> dict[str, str]:
    return {
        "api-map.md": render_api_map(bundle["api_map"]),
        "entity-summary.md": render_entity_summary(bundle["entity_summary"]),
        "parameter-taxonomy.md": render_parameter_taxonomy(
            bundle["parameter_records"], bundle["library_observations"]
        ),
        "log-patterns.md": render_log_patterns(bundle["log_samples"]),
        "open-questions.md": render_open_questions(bundle["open_questions"]),
    }
```

- [ ] **Step 4: Run the reporting test to verify it passes**

Run: `python3 -m unittest discover -s tests -p "test_reporting.py" -v`
Expected: PASS with `OK`

- [ ] **Step 5: Commit the reporting layer**

```bash
git add collector/reporting.py tests/test_reporting.py
git commit -m "feat: render discovery report documents"
```

### Task 8: Orchestrate Staged Discovery And Write Artifacts

**Files:**
- Create: `collector/discovery.py`
- Test: `tests/test_discovery.py`

- [ ] **Step 1: Write the failing orchestration test**

```python
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from collector.config import Settings
from collector.discovery import run_discovery
from collector.raw_store import ArtifactStore
from collector.transport import ResponseMeta, TransportResponse


class FakeTransport:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, path, query=None):
        self.calls.append((path, dict(query or {})))
        payload = self.responses[path]
        return TransportResponse(
            status=200,
            text=json.dumps(payload),
            json_data=payload,
            meta=ResponseMeta(
                method="GET",
                path=path,
                query=dict(query or {}),
                status=200,
                fetched_at=1710000000.0,
            ),
        )


class DiscoveryTests(unittest.TestCase):
    def test_run_discovery_writes_raw_normalized_and_markdown_artifacts(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a", "project-b"),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=2,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {
                "data": [
                    {"_id": "project-a", "name": "Project A"},
                    {"_id": "project-b", "name": "Project B"},
                ]
            },
            "/api/spiders": {
                "data": [
                    {"_id": "spider-1", "name": "alpha", "project_id": "project-a"},
                    {"_id": "spider-2", "name": "beta", "project_id": "project-b"},
                ]
            },
            "/api/schedules": {
                "data": [
                    {"_id": "schedule-1", "spider_id": "spider-1", "project_id": "project-a"}
                ]
            },
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "schedule_id": "schedule-1",
                        "status": "finished",
                        "args": "--sp catalog --fp shoes",
                        "node_id": "node-1",
                    },
                    {
                        "_id": "task-2",
                        "project_id": "project-b",
                        "spider_id": "spider-2",
                        "status": "failed",
                        "args": "--spi rerun --debug",
                        "node_id": "node-2",
                    },
                ]
            },
            "/api/nodes": {
                "data": [
                    {"_id": "node-1", "name": "node-a"},
                    {"_id": "node-2", "name": "node-b"},
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "schedule_id": "schedule-1",
                    "status": "finished",
                    "args": "--sp catalog --fp shoes",
                    "node_id": "node-1",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/tasks/task-2": {
                "data": {
                    "_id": "task-2",
                    "project_id": "project-b",
                    "spider_id": "spider-2",
                    "status": "failed",
                    "args": "--spi rerun --debug",
                    "node_id": "node-2",
                }
            },
            "/api/tasks/task-2/logs": {"data": ["HTTP 503 upstream", "Traceback: boom"]},
            "/api/nodes/node-1": {"data": {"_id": "node-1", "name": "node-a"}},
            "/api/nodes/node-2": {"data": {"_id": "node-2", "name": "node-b"}},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            wheel_path = Path(tmp_dir) / "demo.whl"
            with zipfile.ZipFile(wheel_path, "w") as archive:
                archive.writestr(
                    "pkg/spider_manager.py",
                    "command = '--sp catalog --fp shoes --proxy_country us'\n",
                )

            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(
                settings,
                transport,
                store,
                wheel_targets=[
                    {
                        "wheel_path": str(wheel_path),
                        "internal_path": "pkg/spider_manager.py",
                    }
                ],
            )

            self.assertTrue((Path(tmp_dir) / "raw/projects/page-1.json").exists())
            self.assertTrue((Path(tmp_dir) / "normalized/tasks.json").exists())
            self.assertTrue((Path(tmp_dir) / "api-map.md").exists())
            self.assertTrue((Path(tmp_dir) / "parameter-taxonomy.md").exists())
            self.assertIn("/api/tasks/task-1/logs", [path for path, _ in transport.calls])
            self.assertEqual(bundle["entity_summary"]["projects"]["count"], 2)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the orchestration test to verify it fails**

Run: `python3 -m unittest discover -s tests -p "test_discovery.py" -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'collector.discovery'`

- [ ] **Step 3: Write the staged discovery orchestrator**

`collector/discovery.py`

```python
from collections import defaultdict
from typing import Any

from collector.library_inspection import inspect_wheel_sources
from collector.log_analysis import analyze_log_text
from collector.normalize import build_observation_unit
from collector.reporting import render_report_documents


PROJECT_KEYS = ("project_id", "projectId", "project")
SPIDER_KEYS = ("spider_id", "spiderId", "spider")
SCHEDULE_KEYS = ("schedule_id", "scheduleId", "schedule")
NODE_KEYS = ("node_id", "nodeId", "node")


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    return []


def _extract_record(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return {}


def _extract_identifier(value: Any) -> Any:
    if isinstance(value, dict):
        return value.get("_id") or value.get("id")
    return value


def _first_matching_id(record: dict[str, Any], keys: tuple[str, ...], expected: set[str]) -> str | None:
    for key in keys:
        if key not in record:
            continue
        value = _extract_identifier(record.get(key))
        if value in expected:
            return str(value)
    return None


def _record_id(record: dict[str, Any]) -> str | None:
    value = record.get("_id") or record.get("id")
    return str(value) if value is not None else None


def _safe_get(transport, api_map, open_questions, path: str, query: dict[str, Any] | None = None):
    try:
        response = transport.get(path, query)
    except RuntimeError as exc:
        api_map.append(
            {
                "path": path,
                "method": "GET",
                "query": dict(query or {}),
                "source": "documented",
                "status": "unknown",
                "notes": str(exc),
            }
        )
        open_questions.append(f"Unable to observe `{path}`: {exc}")
        return None

    api_map.append(
        {
            "path": path,
            "method": "GET",
            "query": dict(query or {}),
            "source": "documented",
            "status": "observed",
            "notes": f"HTTP {response.status}",
        }
    )
    return response


def _coerce_log_payload(payload: Any) -> list[str] | str:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [str(item) for item in data]
        if isinstance(data, str):
            return data
    return ""


def _select_case_type(task: dict[str, Any], log_result: dict[str, Any]) -> str:
    status = str(task.get("status", "")).lower()
    normalized_params = task["observation"]["normalized_params"]["parameters"]
    param_keys = {item["normalized_key"] for item in normalized_params}

    if status == "failed":
        return "failed"
    if status in {"running", "pending"}:
        return "long-running"
    if "spi" in param_keys:
        return "manual rerun candidate"
    if log_result["signals"]["http_403_404_5xx"]:
        return "http error spike"
    if status == "finished" and (
        log_result["signals"]["empty_log"] or not log_result["signals"]["has_summary"]
    ):
        return "finished but suspicious"
    return "successful"


def _entity_fields(records: list[dict[str, Any]]) -> list[str]:
    fields = set()
    for record in records:
        fields.update(record.keys())
    return sorted(fields)


def run_discovery(settings, transport, store, wheel_targets=None):
    open_questions: list[str] = []
    api_map: list[dict[str, Any]] = []
    target_project_ids = set(settings.project_ids)

    projects_response = _safe_get(
        transport,
        api_map,
        open_questions,
        "/api/projects",
        {"page": 1, "size": settings.page_size},
    )
    store.write_json(
        "raw/projects/page-1.json",
        projects_response.json_data if projects_response else {"status": "unknown"},
    )
    projects = _extract_records(projects_response.json_data if projects_response else {})
    target_projects = [
        project
        for project in projects
        if _record_id(project) in target_project_ids
    ]
    if not target_projects:
        open_questions.append("No target projects observed in the first `/api/projects` page.")

    spiders_response = _safe_get(
        transport,
        api_map,
        open_questions,
        "/api/spiders",
        {"page": 1, "page_size": settings.page_size},
    )
    store.write_json(
        "raw/spiders/page-1.json",
        spiders_response.json_data if spiders_response else {"status": "unknown"},
    )
    spiders = _extract_records(spiders_response.json_data if spiders_response else {})
    target_spiders = [
        spider
        for spider in spiders
        if _first_matching_id(spider, PROJECT_KEYS, target_project_ids)
    ]
    spider_lookup = {
        _record_id(spider): spider for spider in target_spiders if _record_id(spider) is not None
    }

    schedules_response = _safe_get(
        transport,
        api_map,
        open_questions,
        "/api/schedules",
        {"page": 1, "page_size": settings.page_size},
    )
    store.write_json(
        "raw/schedules/page-1.json",
        schedules_response.json_data if schedules_response else {"status": "unknown"},
    )
    schedules = _extract_records(schedules_response.json_data if schedules_response else {})
    target_spider_ids = set(spider_lookup.keys())
    target_schedules = [
        schedule
        for schedule in schedules
        if _first_matching_id(schedule, SPIDER_KEYS, target_spider_ids)
        or _first_matching_id(schedule, PROJECT_KEYS, target_project_ids)
    ]

    nodes_response = _safe_get(
        transport,
        api_map,
        open_questions,
        "/api/nodes",
        {"page": 1, "page_size": settings.page_size},
    )
    store.write_json(
        "raw/nodes/page-1.json",
        nodes_response.json_data if nodes_response else {"status": "unknown"},
    )
    nodes = _extract_records(nodes_response.json_data if nodes_response else {})
    node_lookup = {
        _record_id(node): node for node in nodes if _record_id(node) is not None
    }

    collected_tasks: list[dict[str, Any]] = []
    task_counts_by_spider: defaultdict[str, int] = defaultdict(int)
    important_spider_ids = {
        _first_matching_id(schedule, SPIDER_KEYS, target_spider_ids)
        for schedule in target_schedules
    }
    important_spider_ids.discard(None)

    for page in range(1, settings.max_pages + 1):
        size = settings.page_size if page == 1 else settings.expanded_task_limit
        tasks_response = _safe_get(
            transport,
            api_map,
            open_questions,
            "/api/tasks",
            {"page": page, "size": size},
        )
        store.write_json(
            f"raw/tasks/page-{page}.json",
            tasks_response.json_data if tasks_response else {"status": "unknown"},
        )

        tasks = _extract_records(tasks_response.json_data if tasks_response else {})
        target_tasks = [
            task
            for task in tasks
            if _first_matching_id(task, PROJECT_KEYS, target_project_ids)
            or _first_matching_id(task, SPIDER_KEYS, target_spider_ids)
        ]

        for task in target_tasks:
            collected_tasks.append(task)
            spider_id = _first_matching_id(task, SPIDER_KEYS, target_spider_ids)
            if spider_id:
                task_counts_by_spider[spider_id] += 1
                important_spider_ids.add(spider_id)

        if important_spider_ids and all(
            task_counts_by_spider[spider_id] >= min(settings.expanded_task_limit, 10)
            for spider_id in important_spider_ids
        ):
            break

    representative_candidates: list[dict[str, Any]] = []
    chosen_slots: set[str] = set()
    for task in collected_tasks:
        status = str(task.get("status", "")).lower()
        observation = build_observation_unit(
            task,
            spider_lookup.get(
                _first_matching_id(task, SPIDER_KEYS, target_spider_ids) or "",
                {"name": "unknown-spider"},
            ),
        )
        param_keys = {
            item["normalized_key"] for item in observation["normalized_params"]["parameters"]
        }

        slot = None
        if status == "failed" and "failed" not in chosen_slots:
            slot = "failed"
        elif status in {"running", "pending"} and "long-running" not in chosen_slots:
            slot = "long-running"
        elif "spi" in param_keys and "manual rerun candidate" not in chosen_slots:
            slot = "manual rerun candidate"
        elif status in {"finished", "success"} and "successful" not in chosen_slots:
            slot = "successful"
        elif status in {"finished", "success"} and "finished but suspicious" not in chosen_slots:
            slot = "finished but suspicious"

        if slot is not None:
            task["observation"] = observation
            representative_candidates.append(task)
            chosen_slots.add(slot)

    log_samples: list[dict[str, Any]] = []
    normalized_tasks: list[dict[str, Any]] = []
    seen_node_ids: set[str] = set()

    for task in representative_candidates:
        task_id = _record_id(task)
        if task_id is None:
            continue

        detail_response = _safe_get(
            transport,
            api_map,
            open_questions,
            f"/api/tasks/{task_id}",
        )
        store.write_json(
            f"raw/tasks/detail-{task_id}.json",
            detail_response.json_data if detail_response else {"status": "unknown"},
        )
        detail_task = _extract_record(detail_response.json_data if detail_response else {}) or task

        log_response = _safe_get(
            transport,
            api_map,
            open_questions,
            f"/api/tasks/{task_id}/logs",
            {"page": 1, "size": settings.log_page_size},
        )
        store.write_json(
            f"raw/tasks/logs-{task_id}.json",
            log_response.json_data if log_response else {"status": "unknown"},
        )
        log_result = analyze_log_text(
            _coerce_log_payload(log_response.json_data if log_response else {})
        )

        node_id = _first_matching_id(detail_task, NODE_KEYS, set(node_lookup.keys()))
        if node_id and node_id not in seen_node_ids:
            seen_node_ids.add(node_id)
            node_response = _safe_get(
                transport,
                api_map,
                open_questions,
                f"/api/nodes/{node_id}",
            )
            store.write_json(
                f"raw/nodes/detail-{node_id}.json",
                node_response.json_data if node_response else {"status": "unknown"},
            )

        observation = build_observation_unit(
            detail_task,
            spider_lookup.get(
                _first_matching_id(detail_task, SPIDER_KEYS, target_spider_ids) or "",
                {"name": "unknown-spider"},
            ),
        )
        trigger_kind = (
            {"kind": "schedule", "status": "fact"}
            if observation["schedule_id"] != "unscheduled"
            else {"kind": "manual-or-unknown", "status": "unknown"}
        )

        normalized_tasks.append(
            {
                "task_id": task_id,
                "status": detail_task.get("status"),
                "project_id": _first_matching_id(detail_task, PROJECT_KEYS, target_project_ids),
                "spider_id": _first_matching_id(detail_task, SPIDER_KEYS, target_spider_ids),
                "node_id": node_id,
                "observation": observation,
                "trigger_kind": trigger_kind,
            }
        )
        log_samples.append(
            {
                "case_type": _select_case_type(
                    {"status": detail_task.get("status"), "observation": observation},
                    log_result,
                ),
                "task_id": task_id,
                "signals": log_result["signals"],
                "tail": log_result["tail"],
                "stable_fragments": log_result["stable_fragments"],
                "unstable_fragments": log_result["unstable_fragments"],
            }
        )

    wheel_targets = wheel_targets or [
        {
            "wheel_path": "crawlib/crawllib-2.12.3-py3-none-any.whl",
            "internal_path": "crawllib/crawlab.py",
        },
        {
            "wheel_path": "crawlib/cdek_crawlab_module-2.2.1-py3-none-any.whl",
            "internal_path": "cdek_crawlab_module/spider_manager.py",
        },
    ]
    library_observations = inspect_wheel_sources(wheel_targets)

    parameter_records = []
    for item in normalized_tasks:
        for parameter in item["observation"]["normalized_params"]["parameters"]:
            parameter_records.append(parameter)

    entity_summary = {
        "projects": {"count": len(target_projects), "fields": _entity_fields(target_projects)},
        "spiders": {"count": len(target_spiders), "fields": _entity_fields(target_spiders)},
        "schedules": {"count": len(target_schedules), "fields": _entity_fields(target_schedules)},
        "tasks": {"count": len(normalized_tasks), "fields": _entity_fields(normalized_tasks)},
        "nodes": {"count": len(node_lookup), "fields": _entity_fields(list(node_lookup.values()))},
    }

    store.write_json("normalized/projects.json", target_projects)
    store.write_json("normalized/spiders.json", target_spiders)
    store.write_json("normalized/schedules.json", target_schedules)
    store.write_json("normalized/tasks.json", normalized_tasks)
    store.write_json("normalized/log-samples.json", log_samples)
    store.write_json("normalized/library-parameter-observations.json", library_observations)

    bundle = {
        "api_map": api_map,
        "entity_summary": entity_summary,
        "parameter_records": parameter_records,
        "library_observations": library_observations,
        "log_samples": log_samples,
        "open_questions": open_questions,
    }
    for relative_path, content in render_report_documents(bundle).items():
        store.write_text(relative_path, content)
    return bundle
```

- [ ] **Step 4: Run the orchestration test to verify it passes**

Run: `python3 -m unittest discover -s tests -p "test_discovery.py" -v`
Expected: PASS with `OK`

- [ ] **Step 5: Commit the staged discovery orchestrator**

```bash
git add collector/discovery.py tests/test_discovery.py
git commit -m "feat: orchestrate staged Crawlab discovery"
```

### Task 9: Add The CLI And Run Final Verification

**Files:**
- Create: `run_discovery.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing CLI test**

```python
import unittest

from run_discovery import build_parser


class CliTests(unittest.TestCase):
    def test_build_parser_collects_multiple_project_ids(self):
        args = build_parser().parse_args(
            [
                "--project-id",
                "66a25c4d116add6c8f235756",
                "--project-id",
                "66a384f5116add6c8f235803",
            ]
        )

        self.assertEqual(
            args.project_id,
            ["66a25c4d116add6c8f235756", "66a384f5116add6c8f235803"],
        )
        self.assertEqual(args.output_root, "docs/discovery")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the CLI test to verify it fails**

Run: `python3 -m unittest discover -s tests -p "test_cli.py" -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'run_discovery'`

- [ ] **Step 3: Write the CLI entry point**

`run_discovery.py`

```python
import argparse

from collector.config import load_settings
from collector.discovery import run_discovery
from collector.raw_store import ArtifactStore
from collector.transport import GetOnlyTransport


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run read-only Crawlab discovery")
    parser.add_argument(
        "--project-id",
        action="append",
        required=True,
        dest="project_id",
        help="Target Crawlab project id; pass twice for the approved pair",
    )
    parser.add_argument(
        "--output-root",
        default="docs/discovery",
        help="Artifact output directory",
    )
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    settings = load_settings(project_ids=args.project_id, output_root=args.output_root)
    transport = GetOnlyTransport(
        base_url=settings.base_url,
        api_key=settings.api_key,
        throttle_seconds=settings.throttle_seconds,
    )
    store = ArtifactStore(settings.output_root)
    run_discovery(settings, transport, store)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the CLI test to verify it passes**

Run: `python3 -m unittest discover -s tests -p "test_cli.py" -v`
Expected: PASS with `OK`

- [ ] **Step 5: Run the full verification suite and one live discovery pass**

Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`
Expected: PASS with `OK`

Run: `python3 run_discovery.py --project-id 66a25c4d116add6c8f235756 --project-id 66a384f5116add6c8f235803`
Expected: exit code `0`, writes `docs/discovery/raw/`, `docs/discovery/normalized/`, and the five markdown docs without printing the API key

- [ ] **Step 6: Commit the CLI and verified collector**

```bash
git add run_discovery.py tests/test_cli.py collector docs/discovery
git commit -m "feat: add read-only Crawlab discovery collector"
```
