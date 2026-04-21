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
        normalized_query = dict(query or {})
        self.calls.append((path, normalized_query))
        payload = self.responses.get((path, tuple(sorted(normalized_query.items()))))
        if payload is None:
            payload = self.responses[path]
        return TransportResponse(
            status=200,
            text=json.dumps(payload),
            json_data=payload,
            meta=ResponseMeta(
                method="GET",
                path=path,
                query=normalized_query,
                status=200,
                fetched_at=1710000000.0,
            ),
        )


class DiscoveryTests(unittest.TestCase):
    def test_run_discovery_collects_target_projects_across_pages_and_reports_missing_ids(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a", "project-b", "project-missing"),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=1,
            expanded_task_limit=20,
            max_pages=2,
            log_page_size=100,
        )

        responses = {
            ("/api/projects", (("page", 1), ("size", 1))): {
                "data": [{"_id": "project-a", "name": "Project A"}]
            },
            ("/api/projects", (("page", 2), ("size", 1))): {
                "data": [{"_id": "project-b", "name": "Project B"}]
            },
            "/api/spiders": {
                "data": [
                    {"_id": "spider-1", "name": "alpha", "project_id": "project-a"},
                    {"_id": "spider-2", "name": "beta", "project_id": "project-b"},
                ]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 1))): {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "args": "--sp catalog --fp shoes",
                    }
                ]
            },
            ("/api/tasks", (("page", 2), ("size", 20))): {
                "data": [
                    {
                        "_id": "task-2",
                        "project_id": "project-b",
                        "spider_id": "spider-2",
                        "status": "failed",
                        "args": "--spi rerun --debug",
                    }
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "args": "--sp catalog --fp shoes",
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
                }
            },
            "/api/tasks/task-2/logs": {"data": ["HTTP 503 upstream", "Traceback: boom"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            self.assertEqual(bundle["entity_summary"]["projects"]["count"], 2)
            self.assertTrue((Path(tmp_dir) / "raw/projects/page-1.json").exists())
            self.assertTrue((Path(tmp_dir) / "raw/projects/page-2.json").exists())
            self.assertIn(("/api/projects", {"page": 2, "size": 1}), transport.calls)
            self.assertIn(
                "Requested projects not observed after 2 `/api/projects` pages: `project-missing`.",
                bundle["open_questions"],
            )

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

    def test_run_discovery_uses_log_analysis_to_choose_finished_representatives(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": [{"_id": "node-1", "name": "node-a"}]},
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "args": "--sp catalog --fp shoes",
                        "node_id": "node-1",
                    },
                    {
                        "_id": "task-2",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "args": "--sp catalog --fp hats",
                        "node_id": "node-1",
                    },
                    {
                        "_id": "task-3",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "args": "--sp catalog --fp bags",
                        "node_id": "node-1",
                    },
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "args": "--sp catalog --fp shoes",
                    "node_id": "node-1",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/tasks/task-2": {
                "data": {
                    "_id": "task-2",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "args": "--sp catalog --fp hats",
                    "node_id": "node-1",
                }
            },
            "/api/tasks/task-2/logs": {"data": ["written 5 items", "Summary: done"]},
            "/api/tasks/task-3": {
                "data": {
                    "_id": "task-3",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "args": "--sp catalog --fp bags",
                    "node_id": "node-1",
                }
            },
            "/api/tasks/task-3/logs": {"data": ["started crawl", "wrote output"]},
            "/api/nodes/node-1": {"data": {"_id": "node-1", "name": "node-a"}},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            case_types = {sample["task_id"]: sample["case_type"] for sample in bundle["log_samples"]}

            self.assertEqual(case_types.get("task-1"), "successful")
            self.assertEqual(case_types.get("task-3"), "finished but suspicious")
            self.assertNotIn("task-2", case_types)

    def test_run_discovery_treats_finished_task_with_positive_signals_and_no_summary_as_successful(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": []},
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "args": "--sp catalog --fp shoes",
                    },
                    {
                        "_id": "task-2",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "args": "--sp catalog --fp hats",
                    },
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "args": "--sp catalog --fp shoes",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/tasks/task-2": {
                "data": {
                    "_id": "task-2",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "args": "--sp catalog --fp hats",
                }
            },
            "/api/tasks/task-2/logs": {
                "data": ["processed 10/10 pages", "written 5 items"]
            },
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            case_types = {sample["task_id"]: sample["case_type"] for sample in bundle["log_samples"]}

            self.assertEqual(case_types.get("task-1"), "successful")
            self.assertNotIn("task-2", case_types)

    def test_run_discovery_keeps_deduped_finished_tasks_in_normalized_outputs(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": []},
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-selected",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "param": "-sp 1",
                    },
                    {
                        "_id": "task-deduped",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "param": "-sp 1 -fp 99",
                    },
                ]
            },
            "/api/tasks/task-selected": {
                "data": {
                    "_id": "task-selected",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "param": "-sp 1",
                }
            },
            "/api/tasks/task-selected/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/tasks/task-deduped": {
                "data": {
                    "_id": "task-deduped",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "param": "-sp 1 -fp 99",
                }
            },
            "/api/tasks/task-deduped/logs": {"data": ["written 5 items", "Summary: done"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])
            normalized_tasks = json.loads((Path(tmp_dir) / "normalized/tasks.json").read_text())
            taxonomy = (Path(tmp_dir) / "parameter-taxonomy.md").read_text()

            self.assertEqual(
                [item["task_id"] for item in normalized_tasks],
                ["task-selected", "task-deduped"],
            )
            self.assertIn(
                {"normalized_key": "fp", "value": "99"},
                [
                    {
                        "normalized_key": record["normalized_key"],
                        "value": record["value"],
                    }
                    for record in bundle["parameter_records"]
                ],
            )
            self.assertIn("`fp` -> `identity candidate` (value: `99`)", taxonomy)

    def test_run_discovery_counts_nodes_fetched_from_task_details(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=1,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": [{"_id": "node-1", "name": "node-a"}]},
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "args": "--sp catalog --fp shoes",
                        "node_id": "node-2",
                    }
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "args": "--sp catalog --fp shoes",
                    "node_id": "node-2",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/nodes/node-2": {
                "data": {"_id": "node-2", "name": "node-b", "hostname": "worker-2"}
            },
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            self.assertEqual(bundle["entity_summary"]["nodes"]["count"], 2)
            self.assertEqual(
                bundle["entity_summary"]["nodes"]["fields"],
                ["_id", "hostname", "name"],
            )

    def test_run_discovery_prefers_http_error_spike_when_logs_show_http_failures(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": [{"_id": "node-1", "name": "node-a"}]},
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "failed",
                        "args": "--spi rerun --debug",
                        "node_id": "node-1",
                    }
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "failed",
                    "args": "--spi rerun --debug",
                    "node_id": "node-1",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["GET /feed -> 503", "HTTP 503 upstream"]},
            "/api/nodes/node-1": {"data": {"_id": "node-1", "name": "node-a"}},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            self.assertEqual(bundle["log_samples"][0]["case_type"], "http error spike")

    def test_run_discovery_redacts_sensitive_node_keys_in_raw_artifacts(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {
                "data": [
                    {
                        "_id": "node-1",
                        "name": "node-a",
                        "key": "super-secret-node-key",
                        "metadata": {"key": "nested-secret"},
                    }
                ]
            },
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "args": "--sp catalog --fp shoes",
                        "node_id": "node-1",
                    }
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "args": "--sp catalog --fp shoes",
                    "node_id": "node-1",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/nodes/node-1": {
                "data": {
                    "_id": "node-1",
                    "name": "node-a",
                    "key": "super-secret-node-key",
                    "labels": [{"key": "nested-secret"}],
                }
            },
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            run_discovery(settings, transport, store, wheel_targets=[])

            node_page_payload = json.loads((Path(tmp_dir) / "raw/nodes/page-1.json").read_text())
            node_detail_payload = json.loads((Path(tmp_dir) / "raw/nodes/detail-node-1.json").read_text())

            self.assertEqual(node_page_payload["data"][0]["key"], "[REDACTED]")
            self.assertEqual(node_page_payload["data"][0]["metadata"]["key"], "[REDACTED]")
            self.assertEqual(node_detail_payload["data"]["key"], "[REDACTED]")
            self.assertEqual(node_detail_payload["data"]["labels"][0]["key"], "[REDACTED]")
            self.assertNotIn("super-secret-node-key", json.dumps(node_page_payload))
            self.assertNotIn("super-secret-node-key", json.dumps(node_detail_payload))

    def test_run_discovery_treats_error_status_as_failed_representative(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": []},
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "error",
                        "args": "--spi rerun --debug",
                    }
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "error",
                    "args": "--spi rerun --debug",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["Traceback: boom"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            self.assertEqual(bundle["log_samples"][0]["case_type"], "failed")
            self.assertNotIn(
                "Representative case type not observed: `failed`.",
                bundle["open_questions"],
            )

    def test_run_discovery_keeps_plain_failed_case_when_http_error_failure_appears_first(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": []},
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-http",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "failed",
                        "args": "--spi rerun --debug",
                    },
                    {
                        "_id": "task-plain-failed",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "error",
                        "args": "--debug",
                    },
                ]
            },
            "/api/tasks/task-http": {
                "data": {
                    "_id": "task-http",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "failed",
                    "args": "--spi rerun --debug",
                }
            },
            "/api/tasks/task-http/logs": {"data": ["GET /feed -> 503", "HTTP 503 upstream"]},
            "/api/tasks/task-plain-failed": {
                "data": {
                    "_id": "task-plain-failed",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "error",
                    "args": "--debug",
                }
            },
            "/api/tasks/task-plain-failed/logs": {"data": ["Traceback: boom"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])
            case_types = {sample["task_id"]: sample["case_type"] for sample in bundle["log_samples"]}

            self.assertEqual(case_types.get("task-http"), "http error spike")
            self.assertEqual(case_types.get("task-plain-failed"), "failed")
            self.assertNotIn(
                "Representative case type not observed: `failed`.",
                bundle["open_questions"],
            )

    def test_run_discovery_records_missing_representative_case_types_in_open_questions(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": [{"_id": "node-1", "name": "node-a"}]},
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "args": "--sp catalog --fp shoes",
                        "node_id": "node-1",
                    }
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "args": "--sp catalog --fp shoes",
                    "node_id": "node-1",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/nodes/node-1": {"data": {"_id": "node-1", "name": "node-a"}},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            self.assertIn("Representative case type not observed: `failed`.", bundle["open_questions"])
            self.assertIn("Representative case type not observed: `http error spike`.", bundle["open_questions"])
            self.assertIn(
                "Representative case type not observed: `finished but suspicious`.",
                bundle["open_questions"],
            )

    def test_run_discovery_preserves_task_spider_identity_without_first_page_spider_match(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=1,
            max_pages=2,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {"data": [{"_id": "spider-other", "name": "other", "project_id": "other-project"}]},
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": [{"_id": "node-1", "name": "node-a"}]},
            (
                "/api/tasks",
                (("page", 1), ("size", 10)),
            ): {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-missing",
                        "status": "finished",
                        "args": "--sp catalog --fp shoes",
                        "node_id": "node-1",
                    }
                ]
            },
            (
                "/api/tasks",
                (("page", 2), ("size", 1)),
            ): {"data": []},
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-missing",
                    "status": "finished",
                    "args": "--sp catalog --fp shoes",
                    "node_id": "node-1",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/nodes/node-1": {"data": {"_id": "node-1", "name": "node-a"}},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            run_discovery(settings, transport, store, wheel_targets=[])
            normalized_tasks = json.loads((Path(tmp_dir) / "normalized/tasks.json").read_text())

            self.assertEqual(len(normalized_tasks), 1)
            self.assertEqual(normalized_tasks[0]["spider_id"], "spider-missing")
            self.assertEqual(
                normalized_tasks[0]["observation"]["spider_id"],
                "spider-missing",
            )
            self.assertEqual(
                normalized_tasks[0]["observation"]["spider_name"],
                "spider-missing",
            )
            self.assertNotIn(
                ("/api/tasks", {"page": 2, "size": 1}),
                transport.calls,
            )

    def test_run_discovery_preserves_project_id_from_nested_detail_spider(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": []},
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-1",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "args": "--sp catalog --fp shoes",
                    }
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "spider_id": "spider-1",
                    "spider": {"_id": "spider-1", "project_id": "project-a"},
                    "status": "finished",
                    "args": "--sp catalog --fp shoes",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            run_discovery(settings, transport, store, wheel_targets=[])
            normalized_tasks = json.loads((Path(tmp_dir) / "normalized/tasks.json").read_text())

            self.assertEqual(normalized_tasks[0]["project_id"], "project-a")

    def test_run_discovery_treats_zero_schedule_id_as_unscheduled_and_fetches_unlisted_node(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=1,
            log_page_size=100,
        )

        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            "/api/spiders": {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            "/api/schedules": {"data": []},
            "/api/nodes": {"data": [{"_id": "node-1", "name": "node-a"}]},
            "/api/tasks": {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "schedule_id": "000000000000000000000000",
                        "status": "finished",
                        "args": "--sp catalog --fp shoes",
                    }
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "schedule_id": "000000000000000000000000",
                    "status": "finished",
                    "args": "--sp catalog --fp shoes",
                    "node_id": "node-99",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/nodes/node-99": {"data": {"_id": "node-99", "name": "node-z"}},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            run_discovery(settings, transport, store, wheel_targets=[])
            normalized_tasks = json.loads((Path(tmp_dir) / "normalized/tasks.json").read_text())

            self.assertEqual(normalized_tasks[0]["node_id"], "node-99")
            self.assertEqual(
                normalized_tasks[0]["observation"]["schedule_id"],
                "unscheduled",
            )
            self.assertEqual(
                normalized_tasks[0]["trigger_kind"],
                {"kind": "manual-or-unknown", "status": "unknown"},
            )
            self.assertIn(("/api/nodes/node-99", {}), transport.calls)
            self.assertTrue((Path(tmp_dir) / "raw/nodes/detail-node-99.json").exists())


if __name__ == "__main__":
    unittest.main()
