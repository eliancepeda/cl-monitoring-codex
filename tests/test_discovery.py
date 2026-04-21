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


def _add_paginated_responses(responses, path, items, page_size, size_key):
    if not items:
        responses[(path, (("page", 1), (size_key, page_size)))] = {"data": []}
        return

    total_pages = (len(items) + page_size - 1) // page_size
    for page in range(1, total_pages + 1):
        start = (page - 1) * page_size
        responses[(path, (("page", page), (size_key, page_size)))] = {
            "data": items[start : start + page_size]
        }
    responses[(path, (("page", total_pages + 1), (size_key, page_size)))] = {
        "data": []
    }


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
            ("/api/tasks", (("page", 2), ("size", 1))): {
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

    def test_run_discovery_writes_task_inventory_and_spider_coverage(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a", "project-b"),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=2,
            max_schedule_pages=2,
            max_task_pages=3,
            max_spiders_per_project_for_detail=2,
            max_tasks_per_spider_for_detail=2,
            max_task_details_total=4,
            max_log_tasks_total=2,
            max_log_pages_per_task=3,
        )
        responses = {
            "/api/projects": {
                "data": [
                    {"_id": "project-a", "name": "Project A"},
                    {"_id": "project-b", "name": "Project B"},
                ]
            },
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [
                    {"_id": "spider-1", "name": "alpha", "project_id": "project-a"},
                ]
            },
            ("/api/spiders", (("page", 2), ("page_size", 10))): {
                "data": [
                    {"_id": "spider-2", "name": "beta", "project_id": "project-b"},
                ]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {
                "data": [
                    {"_id": "schedule-1", "spider_id": "spider-1", "project_id": "project-a"}
                ]
            },
            ("/api/schedules", (("page", 2), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "param": "-sp 1 -fp 2",
                    }
                ]
            },
            ("/api/tasks", (("page", 2), ("size", 10))): {
                "data": [
                    {
                        "_id": "task-2",
                        "project_id": "project-b",
                        "spider_id": "spider-2",
                        "status": "error",
                        "param": "-sp 3 -fp 4",
                    }
                ]
            },
            ("/api/tasks", (("page", 3), ("size", 10))): {"data": []},
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "param": "-sp 1 -fp 2",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/tasks/task-2": {
                "data": {
                    "_id": "task-2",
                    "project_id": "project-b",
                    "spider_id": "spider-2",
                    "status": "error",
                    "param": "-sp 3 -fp 4",
                }
            },
            "/api/tasks/task-2/logs": {"data": ["HTTP 503 upstream", "Traceback: boom"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            self.assertTrue((Path(tmp_dir) / "raw/spiders/page-2.json").exists())
            self.assertTrue((Path(tmp_dir) / "raw/schedules/page-2.json").exists())
            self.assertTrue((Path(tmp_dir) / "raw/tasks/page-2.json").exists())
            self.assertTrue((Path(tmp_dir) / "normalized/task-inventory.json").exists())
            self.assertTrue((Path(tmp_dir) / "normalized/spider-coverage.json").exists())
            self.assertIn("task_inventory", bundle)
            self.assertIn("spider_coverage", bundle)
            self.assertEqual(
                bundle["task_inventory"]["projects"]["project-a"]["spiders"]["spider-1"]["status_counts"],
                {"finished": 1},
            )
            self.assertTrue(
                bundle["task_inventory"]["projects"]["project-a"]["spiders"]["spider-1"]["has_schedule"]
            )
            self.assertEqual(
                bundle["spider_coverage"]["projects"]["project-b"]["selected_spider_ids"],
                ["spider-2"],
            )
            self.assertTrue(
                bundle["spider_coverage"]["projects"]["project-a"]["spiders"][0]["has_schedule"]
            )

    def test_run_discovery_collects_expanded_fixture_for_target_projects(self):
        project_ids = (
            "66a25c4d116add6c8f235756",
            "66a384f5116add6c8f235803",
        )
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=project_ids,
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=25,
            expanded_task_limit=60,
            max_pages=6,
            log_page_size=100,
            max_project_pages=6,
            max_spider_pages=20,
            max_schedule_pages=20,
            max_task_pages=60,
            max_spiders_per_project_for_detail=12,
            max_tasks_per_spider_for_detail=6,
            max_task_details_total=96,
            max_log_tasks_total=48,
            max_log_tasks_per_project=24,
            max_log_pages_per_task=5,
        )

        project_names = {
            project_ids[0]: "Readonly Collector A",
            project_ids[1]: "Readonly Collector B",
        }
        spider_ids_by_project = {
            project_id: [
                f"spider-{project_index}-{spider_index:02d}"
                for spider_index in range(1, 13)
            ]
            for project_index, project_id in enumerate(project_ids, start=1)
        }
        node_ids_by_project = {
            project_id: [
                f"node-{project_index}-{node_index}"
                for node_index in range(1, 5)
            ]
            for project_index, project_id in enumerate(project_ids, start=1)
        }

        spiders = []
        schedules = []
        tasks = []
        responses = {
            "/api/projects": {
                "data": [
                    {"_id": project_id, "name": project_names[project_id]}
                    for project_id in project_ids
                ]
            },
            "/api/nodes": {
                "data": [
                    {
                        "_id": node_id,
                        "name": f"worker-{node_id}",
                        "hostname": f"{node_id}.example",
                    }
                    for project_id in project_ids
                    for node_id in node_ids_by_project[project_id]
                ]
            },
        }
        for project_id in project_ids:
            for node_id in node_ids_by_project[project_id]:
                responses[f"/api/nodes/{node_id}"] = {
                    "data": {
                        "_id": node_id,
                        "name": f"worker-{node_id}",
                        "hostname": f"{node_id}.example",
                    }
                }

        for project_index, project_id in enumerate(project_ids, start=1):
            for spider_index, spider_id in enumerate(
                spider_ids_by_project[project_id],
                start=1,
            ):
                spiders.append(
                    {
                        "_id": spider_id,
                        "name": f"collector-{project_index}-{spider_index:02d}",
                        "project_id": project_id,
                    }
                )
                schedules.append(
                    {
                        "_id": f"schedule-{project_index}-{spider_index:02d}",
                        "spider_id": spider_id,
                        "project_id": project_id,
                    }
                )

                node_id = node_ids_by_project[project_id][(spider_index - 1) % 4]
                for task_number, status in enumerate(
                    ("finished", "error", "failed"),
                    start=1,
                ):
                    task_id = f"task-{project_index}-{spider_index:02d}-{task_number}"
                    task = {
                        "_id": task_id,
                        "project_id": project_id,
                        "spider_id": spider_id,
                        "status": status,
                        "param": f"-sp {spider_index} -mode {task_number}",
                        "node_id": node_id,
                    }
                    if task_number == 1:
                        task["logs_total"] = 500
                    tasks.append(task)
                    responses[f"/api/tasks/{task_id}"] = {"data": dict(task)}

                    if task_number == 1:
                        for page_number, marker in ((1, "first"), (3, "middle"), (5, "last")):
                            responses[
                                (
                                    f"/api/tasks/{task_id}/logs",
                                    (("page", page_number), ("size", 100)),
                                )
                            ] = {
                                "data": [
                                    f"{task_id} {marker} page",
                                    "written 25 items",
                                    "Summary: done",
                                ],
                                "total": 500,
                            }
                    else:
                        responses[f"/api/tasks/{task_id}/logs"] = {
                            "data": [
                                f"{task_id} failed",
                                "Traceback: boom",
                            ]
                        }

        _add_paginated_responses(responses, "/api/spiders", spiders, 25, "page_size")
        _add_paginated_responses(responses, "/api/schedules", schedules, 25, "page_size")
        _add_paginated_responses(responses, "/api/tasks", tasks, 25, "size")

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])
            normalized_tasks = json.loads((Path(tmp_dir) / "normalized/tasks.json").read_text())

            self.assertTrue((Path(tmp_dir) / "raw/spiders/page-2.json").exists())
            self.assertTrue((Path(tmp_dir) / "raw/schedules/page-2.json").exists())
            self.assertTrue((Path(tmp_dir) / "raw/tasks/page-4.json").exists())
            self.assertEqual(bundle["entity_summary"]["projects"]["count"], 2)
            self.assertEqual(bundle["entity_summary"]["spiders"]["count"], 24)
            self.assertEqual(bundle["entity_summary"]["schedules"]["count"], 24)
            self.assertEqual(bundle["entity_summary"]["tasks"]["count"], 72)
            self.assertEqual(bundle["entity_summary"]["nodes"]["count"], 8)
            self.assertEqual(len(bundle["log_page_samples"]), 48)
            self.assertEqual(sum(len(item["pages"]) for item in bundle["log_page_samples"]), 96)
            self.assertEqual(sum(1 for item in normalized_tasks if item["log_sampled"]), 48)
            self.assertEqual(
                len(list((Path(tmp_dir) / "raw/tasks").glob("detail-*.json"))),
                72,
            )

            for project_id in project_ids:
                self.assertEqual(
                    len(bundle["spider_coverage"]["projects"][project_id]["selected_spider_ids"]),
                    12,
                )

            project_one_first_spider = spider_ids_by_project[project_ids[0]][0]
            self.assertEqual(
                bundle["task_inventory"]["projects"][project_ids[0]]["spiders"][project_one_first_spider]["status_counts"],
                {"error": 1, "failed": 1, "finished": 1},
            )
            self.assertEqual(
                len(
                    next(
                        item
                        for item in bundle["spider_coverage"]["projects"][project_ids[0]]["spiders"]
                        if item["spider_id"] == project_one_first_spider
                    )["detail_task_ids"]
                ),
                3,
            )
            self.assertEqual(
                len(
                    next(
                        item
                        for item in bundle["spider_coverage"]["projects"][project_ids[0]]["spiders"]
                        if item["spider_id"] == project_one_first_spider
                    )["log_task_ids"]
                ),
                2,
            )
            self.assertEqual(
                {
                    item["project_id"]: item["sampled_tasks"]
                    for item in bundle["log_sampling_summary"]
                },
                {project_ids[0]: 24, project_ids[1]: 24},
            )
            self.assertTrue(
                any(
                    item["sampling_strategy"] == "first + middle + last"
                    and item["sampled_pages"] == [1, 3, 5]
                    for item in bundle["log_samples"]
                )
            )

    def test_run_discovery_stops_task_inventory_after_stability_window(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a", "project-b"),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=25,
            max_log_tasks_per_project=1,
            task_page_stability_window=2,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a"}, {"_id": "project-b"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [
                    {"_id": "spider-a", "name": "alpha", "project_id": "project-a"},
                    {"_id": "spider-b", "name": "beta", "project_id": "project-b"},
                ]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {"_id": "task-a", "project_id": "project-a", "spider_id": "spider-a", "status": "finished", "param": "-sp 1"},
                    {"_id": "task-b", "project_id": "project-b", "spider_id": "spider-b", "status": "error", "param": "-sp 2"},
                ]
            },
            ("/api/tasks", (("page", 2), ("size", 10))): {
                "data": [
                    {"_id": "task-a", "project_id": "project-a", "spider_id": "spider-a", "status": "finished", "param": "-sp 1"},
                    {"_id": "task-b", "project_id": "project-b", "spider_id": "spider-b", "status": "error", "param": "-sp 2"},
                ]
            },
            ("/api/tasks", (("page", 3), ("size", 10))): {
                "data": [
                    {"_id": "task-a", "project_id": "project-a", "spider_id": "spider-a", "status": "finished", "param": "-sp 1"},
                    {"_id": "task-b", "project_id": "project-b", "spider_id": "spider-b", "status": "error", "param": "-sp 2"},
                ]
            },
            "/api/tasks/task-a": {"data": {"_id": "task-a", "project_id": "project-a", "spider_id": "spider-a", "status": "finished", "param": "-sp 1"}},
            "/api/tasks/task-a/logs": {"data": ["written 10 items", "Summary: done"]},
            "/api/tasks/task-b": {"data": {"_id": "task-b", "project_id": "project-b", "spider_id": "spider-b", "status": "error", "param": "-sp 2"}},
            "/api/tasks/task-b/logs": {"data": ["Traceback: boom"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            bundle = run_discovery(settings, FakeTransport(responses), ArtifactStore(tmp_dir), wheel_targets=[])

        self.assertEqual(
            bundle["task_inventory"]["task_inventory_status"]["stop_reason"],
            "stability-window",
        )
        self.assertEqual(
            bundle["task_inventory"]["task_inventory_status"]["pages_fetched"],
            3,
        )

    def test_run_discovery_stops_task_inventory_after_stability_window_once_global_log_cap_is_satisfied(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a", "project-b"),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=25,
            max_log_tasks_total=1,
            max_log_tasks_per_project=2,
            task_page_stability_window=2,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a"}, {"_id": "project-b"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [
                    {"_id": "spider-a", "name": "alpha", "project_id": "project-a"},
                    {"_id": "spider-b", "name": "beta", "project_id": "project-b"},
                ]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {"_id": "task-a", "project_id": "project-a", "spider_id": "spider-a", "status": "finished", "param": "-sp 1"},
                ]
            },
            ("/api/tasks", (("page", 2), ("size", 10))): {
                "data": [
                    {"_id": "task-a", "project_id": "project-a", "spider_id": "spider-a", "status": "finished", "param": "-sp 1"},
                ]
            },
            ("/api/tasks", (("page", 3), ("size", 10))): {
                "data": [
                    {"_id": "task-a", "project_id": "project-a", "spider_id": "spider-a", "status": "finished", "param": "-sp 1"},
                ]
            },
            "/api/tasks/task-a": {"data": {"_id": "task-a", "project_id": "project-a", "spider_id": "spider-a", "status": "finished", "param": "-sp 1"}},
            "/api/tasks/task-a/logs": {"data": ["written 10 items", "Summary: done"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            transport = FakeTransport(responses)
            bundle = run_discovery(settings, transport, ArtifactStore(tmp_dir), wheel_targets=[])

        self.assertEqual(
            bundle["task_inventory"]["task_inventory_status"]["stop_reason"],
            "stability-window",
        )
        self.assertEqual(
            bundle["task_inventory"]["task_inventory_status"]["pages_fetched"],
            3,
        )
        self.assertNotIn(("/api/tasks", {"page": 4, "size": 10}), transport.calls)

    def test_run_discovery_uses_max_pages_as_legacy_fallback_for_widened_caps(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=1,
            expanded_task_limit=20,
            max_pages=2,
            log_page_size=100,
        )
        responses = {
            ("/api/projects", (("page", 1), ("size", 1))): {"data": [{"_id": "project-a"}]},
            ("/api/spiders", (("page", 1), ("page_size", 1))): {
                "data": [{"_id": "spider-a", "name": "alpha", "project_id": "project-a"}]
            },
            ("/api/spiders", (("page", 2), ("page_size", 1))): {
                "data": [{"_id": "spider-b", "name": "beta", "project_id": "project-a"}]
            },
            ("/api/schedules", (("page", 1), ("page_size", 1))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 1))): {
                "data": [{"_id": "task-a", "project_id": "project-a", "spider_id": "spider-a", "status": "finished", "param": "-sp 1"}]
            },
            ("/api/tasks", (("page", 2), ("size", 1))): {
                "data": [{"_id": "task-b", "project_id": "project-a", "spider_id": "spider-b", "status": "finished", "param": "-sp 2"}]
            },
            "/api/tasks/task-a": {"data": {"_id": "task-a", "project_id": "project-a", "spider_id": "spider-a", "status": "finished", "param": "-sp 1"}},
            "/api/tasks/task-a/logs": {"data": ["written 1 item"]},
            "/api/tasks/task-b": {"data": {"_id": "task-b", "project_id": "project-a", "spider_id": "spider-b", "status": "finished", "param": "-sp 2"}},
            "/api/tasks/task-b/logs": {"data": ["written 2 items"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            transport = FakeTransport(responses)
            bundle = run_discovery(settings, transport, ArtifactStore(tmp_dir), wheel_targets=[])

        self.assertEqual(bundle["task_inventory"]["task_inventory_status"]["page_cap"], 2)
        self.assertIn(("/api/spiders", {"page": 2, "page_size": 1}), transport.calls)
        self.assertNotIn(("/api/spiders", {"page": 3, "page_size": 1}), transport.calls)
        self.assertIn(("/api/tasks", {"page": 2, "size": 1}), transport.calls)
        self.assertNotIn(("/api/tasks", {"page": 3, "size": 1}), transport.calls)

    def test_run_discovery_continues_spider_pagination_after_duplicate_only_page(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a", "project-b"),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=1,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=3,
            max_schedule_pages=1,
            max_task_pages=1,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a"}, {"_id": "project-b"}]},
            ("/api/spiders", (("page", 1), ("page_size", 1))): {
                "data": [{"_id": "spider-a", "name": "alpha", "project_id": "project-a"}]
            },
            ("/api/spiders", (("page", 2), ("page_size", 1))): {
                "data": [{"_id": "spider-a", "name": "alpha", "project_id": "project-a"}]
            },
            ("/api/spiders", (("page", 3), ("page_size", 1))): {
                "data": [{"_id": "spider-b", "name": "beta", "project_id": "project-b"}]
            },
            ("/api/schedules", (("page", 1), ("page_size", 1))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 1))): {"data": []},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            transport = FakeTransport(responses)
            bundle = run_discovery(settings, transport, ArtifactStore(tmp_dir), wheel_targets=[])

        self.assertIn(("/api/spiders", {"page": 3, "page_size": 1}), transport.calls)
        self.assertEqual(
            bundle["spider_coverage"]["projects"]["project-b"]["selected_spider_ids"],
            ["spider-b"],
        )

    def test_run_discovery_keeps_all_sampled_inventory_tasks_in_normalized_tasks(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=1,
            max_spiders_per_project_for_detail=1,
            max_tasks_per_spider_for_detail=1,
            max_task_details_total=1,
            max_log_tasks_total=1,
            max_log_pages_per_task=3,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {"_id": "task-1", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 1"},
                    {"_id": "task-2", "project_id": "project-a", "spider_id": "spider-1", "status": "error", "param": "-sp 2"},
                    {"_id": "task-3", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 3"},
                ]
            },
            "/api/tasks/task-1": {
                "data": {"_id": "task-1", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 1"}
            },
            "/api/tasks/task-1/logs": {"data": ["written 1 item", "Summary: done"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            run_discovery(settings, transport, store, wheel_targets=[])
            normalized_tasks = json.loads((Path(tmp_dir) / "normalized/tasks.json").read_text())

            self.assertEqual(
                [item["task_id"] for item in normalized_tasks],
                ["task-1", "task-2", "task-3"],
            )
            self.assertEqual(
                [item["log_sampled"] for item in normalized_tasks],
                [True, False, False],
            )

    def test_run_discovery_keeps_quiet_scheduled_spiders_in_spider_coverage(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=1,
            max_spiders_per_project_for_detail=2,
            max_tasks_per_spider_for_detail=1,
            max_task_details_total=1,
            max_log_tasks_total=1,
            max_log_pages_per_task=3,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [
                    {"_id": "spider-1", "name": "alpha", "project_id": "project-a"},
                    {"_id": "spider-2", "name": "beta", "project_id": "project-a"},
                ]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {
                "data": [{"_id": "schedule-2", "spider_id": "spider-2", "project_id": "project-a"}]
            },
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {"_id": "task-1", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 1"}
                ]
            },
            "/api/tasks/task-1": {
                "data": {"_id": "task-1", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 1"}
            },
            "/api/tasks/task-1/logs": {"data": ["written 1 item", "Summary: done"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            spiders = {
                item["spider_id"]: item
                for item in bundle["spider_coverage"]["projects"]["project-a"]["spiders"]
            }
            self.assertIn("spider-1", spiders)
            self.assertIn("spider-2", spiders)
            self.assertTrue(spiders["spider-2"]["has_schedule"])
            self.assertEqual(spiders["spider-2"]["detail_task_ids"], [])
            self.assertEqual(spiders["spider-2"]["log_task_ids"], [])

    def test_run_discovery_records_candidate_limited_shortfall_and_quiet_spider_gap(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=2,
            max_log_tasks_per_project=3,
            task_page_stability_window=2,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [
                    {"_id": "spider-1", "name": "alpha", "project_id": "project-a"},
                    {"_id": "spider-2", "name": "beta", "project_id": "project-a"},
                ]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {
                "data": [{"_id": "schedule-2", "spider_id": "spider-2", "project_id": "project-a"}]
            },
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "param": "-sp 1",
                    }
                ]
            },
            ("/api/tasks", (("page", 2), ("size", 10))): {"data": []},
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "param": "-sp 1",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 1 item", "Summary: done"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            bundle = run_discovery(settings, FakeTransport(responses), ArtifactStore(tmp_dir), wheel_targets=[])

        self.assertIn(
            "Project `project-a` sampled `1/3` log tasks because observed target-task candidate set was smaller than the quota.",
            bundle["open_questions"],
        )
        self.assertIn(
            "Spider `spider-2` was observed in coverage but had no task candidates in sampled task pages.",
            bundle["open_questions"],
        )

    def test_run_discovery_records_global_log_cap_shortfall_reason(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a", "project-b"),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=1,
            max_log_tasks_total=1,
            max_log_tasks_per_project=2,
            task_page_stability_window=2,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a"}, {"_id": "project-b"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [
                    {"_id": "spider-a1", "name": "alpha-1", "project_id": "project-a"},
                    {"_id": "spider-a2", "name": "alpha-2", "project_id": "project-a"},
                    {"_id": "spider-b1", "name": "beta-1", "project_id": "project-b"},
                    {"_id": "spider-b2", "name": "beta-2", "project_id": "project-b"},
                ]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {"_id": "task-a1", "project_id": "project-a", "spider_id": "spider-a1", "status": "finished", "param": "-sp 1"},
                    {"_id": "task-a2", "project_id": "project-a", "spider_id": "spider-a2", "status": "error", "param": "-sp 2"},
                    {"_id": "task-b1", "project_id": "project-b", "spider_id": "spider-b1", "status": "finished", "param": "-sp 3"},
                    {"_id": "task-b2", "project_id": "project-b", "spider_id": "spider-b2", "status": "error", "param": "-sp 4"},
                ]
            },
        }
        responses["/api/tasks/task-a1"] = {
            "data": {"_id": "task-a1", "project_id": "project-a", "spider_id": "spider-a1", "status": "finished", "param": "-sp 1"}
        }
        responses["/api/tasks/task-a2"] = {
            "data": {"_id": "task-a2", "project_id": "project-a", "spider_id": "spider-a2", "status": "error", "param": "-sp 2"}
        }
        responses["/api/tasks/task-b1"] = {
            "data": {"_id": "task-b1", "project_id": "project-b", "spider_id": "spider-b1", "status": "finished", "param": "-sp 3"}
        }
        responses["/api/tasks/task-b2"] = {
            "data": {"_id": "task-b2", "project_id": "project-b", "spider_id": "spider-b2", "status": "error", "param": "-sp 4"}
        }
        responses["/api/tasks/task-a1/logs"] = {"data": ["written 1 item", "Summary: done"]}

        with tempfile.TemporaryDirectory() as tmp_dir:
            bundle = run_discovery(settings, FakeTransport(responses), ArtifactStore(tmp_dir), wheel_targets=[])

        self.assertIn(
            "Project `project-b` sampled `0/2` log tasks because global log task cap was reached.",
            bundle["open_questions"],
        )

    def test_run_discovery_prefers_page_cap_shortfall_over_candidate_shortfall(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=1,
            max_log_tasks_per_project=3,
            task_page_stability_window=2,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {"_id": "task-1", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 1"}
                ]
            },
            "/api/tasks/task-1": {
                "data": {"_id": "task-1", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 1"}
            },
            "/api/tasks/task-1/logs": {"data": ["written 1 item", "Summary: done"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            bundle = run_discovery(settings, FakeTransport(responses), ArtifactStore(tmp_dir), wheel_targets=[])

        self.assertIn(
            "Project `project-a` sampled `1/3` log tasks because task inventory hit configured page cap.",
            bundle["open_questions"],
        )

    def test_run_discovery_marks_all_observed_spiders_selected_for_detail(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=1,
            max_spiders_per_project_for_detail=2,
            max_log_tasks_per_project=1,
            task_page_stability_window=2,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [
                    {"_id": "spider-1", "name": "alpha", "project_id": "project-a"},
                    {"_id": "spider-2", "name": "beta", "project_id": "project-a"},
                    {"_id": "spider-3", "name": "gamma", "project_id": "project-a"},
                    {"_id": "spider-4", "name": "delta", "project_id": "project-a"},
                    {"_id": "spider-5", "name": "epsilon", "project_id": "project-a"},
                    {"_id": "spider-6", "name": "zeta", "project_id": "project-a"},
                    {"_id": "spider-7", "name": "zz-last", "project_id": "project-a"},
                ]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {
                "data": [{"_id": "schedule-2", "spider_id": "spider-2", "project_id": "project-a"}]
            },
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {"_id": "task-1", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 1"}
                ]
            },
            "/api/tasks/task-1": {
                "data": {"_id": "task-1", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 1"}
            },
            "/api/tasks/task-1/logs": {"data": ["written 10 items", "Summary: done"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            bundle = run_discovery(settings, FakeTransport(responses), ArtifactStore(tmp_dir), wheel_targets=[])

        spiders = {
            item["spider_id"]: item
            for item in bundle["spider_coverage"]["projects"]["project-a"]["spiders"]
        }
        self.assertTrue(spiders["spider-1"]["selected_for_detail"])
        self.assertTrue(spiders["spider-7"]["selected_for_detail"])
        self.assertIn(
            "spider-7",
            bundle["spider_coverage"]["projects"]["project-a"]["selected_spider_ids"],
        )
        self.assertEqual(spiders["spider-7"]["detail_task_ids"], [])
        self.assertEqual(spiders["spider-7"]["log_task_ids"], [])

    def test_run_discovery_uses_per_project_log_quotas_with_spider_breadth(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a", "project-b"),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=1,
            max_log_tasks_total=4,
            max_log_tasks_per_project=2,
            task_page_stability_window=2,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a"}, {"_id": "project-b"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [
                    {"_id": "spider-a1", "name": "alpha-1", "project_id": "project-a"},
                    {"_id": "spider-a2", "name": "alpha-2", "project_id": "project-a"},
                    {"_id": "spider-b1", "name": "beta-1", "project_id": "project-b"},
                    {"_id": "spider-b2", "name": "beta-2", "project_id": "project-b"},
                ]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {"_id": "task-a1", "project_id": "project-a", "spider_id": "spider-a1", "status": "finished", "param": "-sp 1"},
                    {"_id": "task-a2", "project_id": "project-a", "spider_id": "spider-a2", "status": "error", "param": "-sp 2"},
                    {"_id": "task-a3", "project_id": "project-a", "spider_id": "spider-a1", "status": "finished", "param": "-sp 3"},
                    {"_id": "task-b1", "project_id": "project-b", "spider_id": "spider-b1", "status": "finished", "param": "-sp 4"},
                    {"_id": "task-b2", "project_id": "project-b", "spider_id": "spider-b2", "status": "finished", "param": "-sp 5"},
                ]
            },
            "/api/tasks/task-a1": {"data": {"_id": "task-a1", "project_id": "project-a", "spider_id": "spider-a1", "status": "finished", "param": "-sp 1"}},
            "/api/tasks/task-a1/logs": {"data": ["written 1 item", "Summary: done"]},
            "/api/tasks/task-a2": {"data": {"_id": "task-a2", "project_id": "project-a", "spider_id": "spider-a2", "status": "error", "param": "-sp 2"}},
            "/api/tasks/task-a2/logs": {"data": ["Traceback: boom"]},
            "/api/tasks/task-a3": {"data": {"_id": "task-a3", "project_id": "project-a", "spider_id": "spider-a1", "status": "finished", "param": "-sp 3"}},
            "/api/tasks/task-a3/logs": {"data": ["written 9 items", "Summary: done"]},
            "/api/tasks/task-b1": {"data": {"_id": "task-b1", "project_id": "project-b", "spider_id": "spider-b1", "status": "finished", "param": "-sp 4"}},
            "/api/tasks/task-b1/logs": {"data": ["written 2 items", "Summary: done"]},
            "/api/tasks/task-b2": {"data": {"_id": "task-b2", "project_id": "project-b", "spider_id": "spider-b2", "status": "finished", "param": "-sp 5"}},
            "/api/tasks/task-b2/logs": {"data": ["written 3 items", "Summary: done"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            bundle = run_discovery(settings, FakeTransport(responses), ArtifactStore(tmp_dir), wheel_targets=[])
            normalized_tasks = json.loads((Path(tmp_dir) / "normalized/tasks.json").read_text())

        sampled_ids = [item["task_id"] for item in bundle["log_page_samples"]]
        self.assertEqual(sampled_ids, ["task-a1", "task-a2", "task-b1", "task-b2"])
        self.assertEqual(
            [item["task_id"] for item in normalized_tasks],
            ["task-a1", "task-a2", "task-a3", "task-b1", "task-b2"],
        )
        self.assertEqual(
            [item["log_sampled"] for item in normalized_tasks],
            [True, True, False, True, True],
        )

    def test_run_discovery_samples_first_middle_and_last_log_pages(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=1,
            max_spiders_per_project_for_detail=1,
            max_tasks_per_spider_for_detail=1,
            max_task_details_total=1,
            max_log_tasks_total=1,
            max_log_pages_per_task=5,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "param": "-sp 1 -fp 2",
                    }
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "param": "-sp 1 -fp 2",
                }
            },
            ("/api/tasks/task-1/logs", (("page", 1), ("size", 100))): {
                "data": ["page one"],
                "total": 500,
            },
            ("/api/tasks/task-1/logs", (("page", 3), ("size", 100))): {
                "data": ["page three"],
                "total": 500,
            },
            ("/api/tasks/task-1/logs", (("page", 5), ("size", 100))): {
                "data": ["page five"],
                "total": 500,
            },
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            self.assertTrue((Path(tmp_dir) / "raw/tasks/logs-task-1-page-1.json").exists())
            self.assertTrue((Path(tmp_dir) / "raw/tasks/logs-task-1-page-3.json").exists())
            self.assertTrue((Path(tmp_dir) / "raw/tasks/logs-task-1-page-5.json").exists())
            self.assertFalse((Path(tmp_dir) / "raw/tasks/logs-task-1-page-2.json").exists())
            self.assertFalse((Path(tmp_dir) / "raw/tasks/logs-task-1-page-4.json").exists())
            self.assertTrue((Path(tmp_dir) / "normalized/log-page-samples.json").exists())
            self.assertEqual(bundle["log_page_samples"][0]["strategy"], "first + middle + last")
            self.assertEqual(
                [page["page"] for page in bundle["log_page_samples"][0]["pages"]],
                [1, 3, 5],
            )
            self.assertEqual(bundle["log_samples"][0]["sampling_strategy"], "first + middle + last")
            self.assertEqual(bundle["log_samples"][0]["sampled_pages"], [1, 3, 5])
            self.assertNotIn("joined", bundle["log_page_samples"][0]["combined_analysis"])
            self.assertTrue(
                bundle["task_inventory"]["projects"]["project-a"]["spiders"]["spider-1"]["has_long_log_candidate"]
            )
            self.assertTrue(
                bundle["spider_coverage"]["projects"]["project-a"]["spiders"][0]["has_long_log_candidate"]
            )
            self.assertTrue((Path(tmp_dir) / "log-sampling-strategy.md").exists())
            self.assertIn(("/api/tasks/task-1/logs", {"page": 3, "size": 100}), transport.calls)
            self.assertIn(("/api/tasks/task-1/logs", {"page": 5, "size": 100}), transport.calls)
            self.assertNotIn(("/api/tasks/task-1/logs", {"page": 2, "size": 100}), transport.calls)
            self.assertNotIn(("/api/tasks/task-1/logs", {"page": 4, "size": 100}), transport.calls)

    def test_run_discovery_caps_log_sampling_per_project_and_keeps_normalized_log_pages_compact(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=1,
            max_log_tasks_per_project=1,
            task_page_stability_window=2,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {"_id": "task-1", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 1"},
                    {"_id": "task-2", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 2"},
                ]
            },
            "/api/tasks/task-1": {"data": {"_id": "task-1", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 1"}},
            "/api/tasks/task-2": {"data": {"_id": "task-2", "project_id": "project-a", "spider_id": "spider-1", "status": "finished", "param": "-sp 2"}},
            ("/api/tasks/task-1/logs", (("page", 1), ("size", 100))): {"data": ["page one"], "total": 300},
            ("/api/tasks/task-1/logs", (("page", 2), ("size", 100))): {"data": ["page two"], "total": 300},
            ("/api/tasks/task-1/logs", (("page", 3), ("size", 100))): {"data": ["page three"], "total": 300},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            run_discovery(settings, FakeTransport(responses), ArtifactStore(tmp_dir), wheel_targets=[])
            log_page_samples = json.loads((Path(tmp_dir) / "normalized/log-page-samples.json").read_text())

        self.assertEqual(len(log_page_samples), 1)
        self.assertNotIn("joined", log_page_samples[0]["combined_analysis"])

    def test_run_discovery_keeps_unknown_when_log_pagination_cannot_be_proven(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a",),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=1,
            max_spiders_per_project_for_detail=1,
            max_tasks_per_spider_for_detail=1,
            max_task_details_total=1,
            max_log_tasks_total=1,
            max_log_pages_per_task=3,
        )
        responses = {
            "/api/projects": {"data": [{"_id": "project-a", "name": "Project A"}]},
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [{"_id": "spider-1", "name": "alpha", "project_id": "project-a"}]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "param": "-sp 1 -fp 2",
                    }
                ]
            },
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "param": "-sp 1 -fp 2",
                }
            },
            ("/api/tasks/task-1/logs", (("page", 1), ("size", 100))): {
                "data": ["page one only"]
            },
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            self.assertEqual(bundle["log_page_samples"][0]["strategy"], "unknown pagination")
            self.assertEqual(
                [page["page"] for page in bundle["log_page_samples"][0]["pages"]],
                [1],
            )
            strategy = Path(tmp_dir, "log-sampling-strategy.md").read_text(encoding="utf-8")
            self.assertIn("unknown pagination", strategy)

    def test_run_discovery_respects_detail_and_log_caps(self):
        settings = Settings(
            base_url="https://crawlab.example",
            api_key="secret-token",
            project_ids=("project-a", "project-b"),
            output_root="ignored-by-test",
            throttle_seconds=0,
            page_size=10,
            expanded_task_limit=20,
            max_pages=3,
            log_page_size=100,
            max_project_pages=1,
            max_spider_pages=1,
            max_schedule_pages=1,
            max_task_pages=5,
            max_spiders_per_project_for_detail=1,
            max_tasks_per_spider_for_detail=1,
            max_task_details_total=2,
            max_log_tasks_total=1,
            max_log_tasks_per_project=2,
            max_log_pages_per_task=3,
        )
        responses = {
            "/api/projects": {
                "data": [
                    {"_id": "project-a", "name": "Project A"},
                    {"_id": "project-b", "name": "Project B"},
                ]
            },
            ("/api/spiders", (("page", 1), ("page_size", 10))): {
                "data": [
                    {"_id": "spider-1", "name": "alpha", "project_id": "project-a"},
                    {"_id": "spider-2", "name": "beta", "project_id": "project-b"},
                ]
            },
            ("/api/schedules", (("page", 1), ("page_size", 10))): {"data": []},
            "/api/nodes": {"data": []},
            ("/api/tasks", (("page", 1), ("size", 10))): {
                "data": [
                    {
                        "_id": "task-1",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "finished",
                        "param": "-sp 1",
                    },
                    {
                        "_id": "task-2",
                        "project_id": "project-a",
                        "spider_id": "spider-1",
                        "status": "error",
                        "param": "-spi rerun",
                    },
                    {
                        "_id": "task-3",
                        "project_id": "project-b",
                        "spider_id": "spider-2",
                        "status": "finished",
                        "param": "-sp 3",
                    },
                ]
            },
            ("/api/tasks", (("page", 2), ("size", 10))): {"data": []},
            "/api/tasks/task-1": {
                "data": {
                    "_id": "task-1",
                    "project_id": "project-a",
                    "spider_id": "spider-1",
                    "status": "finished",
                    "param": "-sp 1",
                }
            },
            "/api/tasks/task-1/logs": {"data": ["written 1 item", "Summary: done"]},
            "/api/tasks/task-3": {
                "data": {
                    "_id": "task-3",
                    "project_id": "project-b",
                    "spider_id": "spider-2",
                    "status": "finished",
                    "param": "-sp 3",
                }
            },
            "/api/tasks/task-3/logs": {"data": ["written 2 items", "Summary: done"]},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])
            normalized_tasks = json.loads((Path(tmp_dir) / "normalized/tasks.json").read_text())

            self.assertEqual(
                [item["task_id"] for item in bundle["log_page_samples"]],
                ["task-1"],
            )
            self.assertLessEqual(
                len(
                    [
                        path
                        for path, _ in transport.calls
                        if path.startswith("/api/tasks/") and not path.endswith("/logs")
                    ]
                ),
                2,
            )
            self.assertLessEqual(
                len([path for path, _ in transport.calls if path.endswith("/logs")]),
                1,
            )
            self.assertEqual(
                {item["spider_id"] for item in normalized_tasks},
                {"spider-1", "spider-2"},
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
            "/api/tasks/task-2/logs": {"data": ["started crawl", "wrote output"]},
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
            "/api/tasks/task-3/logs": {"data": ["written 5 items", "Summary: done"]},
            "/api/nodes/node-1": {"data": {"_id": "node-1", "name": "node-a"}},
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            transport = FakeTransport(responses)

            bundle = run_discovery(settings, transport, store, wheel_targets=[])

            case_types = {sample["task_id"]: sample["case_type"] for sample in bundle["log_samples"]}

            self.assertEqual(case_types.get("task-1"), "successful")
            self.assertEqual(case_types.get("task-2"), "finished but suspicious")
            self.assertNotIn("task-3", case_types)

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
                (("page", 2), ("size", 10)),
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
