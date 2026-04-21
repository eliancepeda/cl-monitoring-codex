import unittest

from collector.reporting import (
    render_api_map,
    render_entity_summary,
    render_parameter_taxonomy,
    render_report_documents,
)


class ReportingTests(unittest.TestCase):
    def test_render_api_map_escapes_pipe_and_newline_in_notes(self):
        document = render_api_map(
            [
                {
                    "path": "/api/projects",
                    "method": "GET",
                    "query": {},
                    "source": "documented",
                    "status": "observed",
                    "notes": "Line 1 | Line 2\nLine 3",
                }
            ]
        )

        self.assertIn("Line 1 \\| Line 2<br>Line 3", document)

    def test_render_entity_summary_emits_explicit_empty_state(self):
        self.assertEqual(render_entity_summary({}), "# Entity Summary\n\n- none observed\n")

    def test_render_parameter_taxonomy_marks_unknown_library_observations_explicitly(self):
        document = render_parameter_taxonomy(
            [],
            [
                {
                    "wheel_path": "crawlib/example.whl",
                    "internal_path": "pkg/spider_manager.py",
                    "status": "unknown",
                    "matched_flags": [],
                    "snippets": [],
                }
            ],
        )

        self.assertIn(
            "- `pkg/spider_manager.py` in `crawlib/example.whl` -> unknown",
            document,
        )
        self.assertNotIn(
            "- `pkg/spider_manager.py` in `crawlib/example.whl` -> no flags",
            document,
        )

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
                        "sampling_strategy": "first + middle + last",
                        "sampled_pages": [1, 2, 3],
                        "pagination": {"status": "known", "total_pages": 3},
                    }
                ],
                "log_sampling_strategy": [
                    {
                        "task_id": "task-1",
                        "strategy": "first + middle + last",
                        "pages": [1, 2, 3],
                        "pagination": {"status": "known", "total_pages": 3},
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
                    "gpt-pro-handoff.md",
                    "parameter-taxonomy.md",
                    "log-patterns.md",
                    "log-sampling-strategy.md",
                    "open-questions.md",
                ]
            ),
        )
        self.assertIn("| `/api/projects` | `GET` |", documents["api-map.md"])
        self.assertIn("## Facts", documents["parameter-taxonomy.md"])
        self.assertIn("## Hypotheses", documents["parameter-taxonomy.md"])
        self.assertIn("Summary: done", documents["log-patterns.md"])
        self.assertIn("Sampled pages: `[1, 2, 3]`", documents["log-patterns.md"])

    def test_render_report_documents_includes_log_sampling_strategy(self):
        documents = render_report_documents(
            {
                "api_map": [],
                "entity_summary": {},
                "parameter_records": [],
                "library_observations": [],
                "log_samples": [],
                "log_sampling_strategy": [
                    {
                        "task_id": "task-1",
                        "strategy": "first + middle + last",
                        "pages": [1, 2, 3],
                        "pagination": {"status": "known", "total_pages": 3},
                    }
                ],
                "open_questions": [],
            }
        )

        self.assertIn("log-sampling-strategy.md", documents)
        self.assertIn(
            "first + middle + last",
            documents["log-sampling-strategy.md"],
        )

    def test_render_log_sampling_strategy_reports_project_quota_status(self):
        documents = render_report_documents(
            {
                "api_map": [],
                "entity_summary": {},
                "parameter_records": [],
                "library_observations": [],
                "log_samples": [],
                "log_sampling_strategy": [
                    {
                        "project_id": "project-a",
                        "task_id": "task-1",
                        "strategy": "first + middle + last",
                        "pages": [1, 2, 3],
                        "pagination": {"status": "known", "total_pages": 3},
                    }
                ],
                "log_sampling_summary": [
                    {
                        "project_id": "project-a",
                        "sampled_tasks": 17,
                        "quota": 20,
                        "shortfall_reason": "task inventory hit configured page cap",
                    }
                ],
                "open_questions": [],
            }
        )

        self.assertIn("Sampled log tasks: `17/20`", documents["log-sampling-strategy.md"])
        self.assertIn(
            "task inventory hit configured page cap",
            documents["log-sampling-strategy.md"],
        )

    def test_render_report_documents_includes_gpt_pro_handoff(self):
        documents = render_report_documents(
            {
                "api_map": [
                    {
                        "path": "/api/projects",
                        "method": "GET",
                        "query": {},
                        "source": "observed",
                        "status": "observed",
                        "notes": "ok",
                    },
                    {
                        "path": "/api/tasks/{id}/logs",
                        "method": "GET",
                        "query": {},
                        "source": "observed",
                        "status": "observed",
                        "notes": "ok",
                    },
                ],
                "entity_summary": {
                    "projects": {"count": 2, "fields": ["_id", "name"]},
                    "spiders": {"count": 27, "fields": ["_id", "project_id"]},
                    "schedules": {"count": 53, "fields": ["_id", "spider_id"]},
                    "tasks": {"count": 179, "fields": ["task_id", "status"]},
                    "nodes": {"count": 10, "fields": ["_id", "name"]},
                },
                "parameter_records": [
                    {
                        "normalized_key": "sp",
                        "role": "identity candidate",
                        "classification_status": "hypothesis",
                        "value": "1643",
                    }
                ],
                "library_observations": [],
                "log_samples": [
                    {
                        "case_type": "successful",
                        "task_id": "task-1",
                        "signals": {"has_summary": True},
                        "tail": ["Summary: done"],
                        "sampling_strategy": "first + middle + last",
                        "sampled_pages": [1, 3, 5],
                        "pagination": {"status": "known", "total_pages": 5},
                    }
                ],
                "log_sampling_strategy": [],
                "log_sampling_summary": [
                    {
                        "project_id": "project-a",
                        "sampled_tasks": 24,
                        "quota": 24,
                        "shortfall_reason": None,
                    },
                    {
                        "project_id": "project-b",
                        "sampled_tasks": 24,
                        "quota": 24,
                        "shortfall_reason": None,
                    },
                ],
                "open_questions": [
                    "Representative case type not observed: `long-running`.",
                    "Representative case type not observed: `manual rerun candidate`.",
                ],
                "task_inventory": {"projects": {"project-a": {}, "project-b": {}}},
            }
        )

        self.assertIn("gpt-pro-handoff.md", documents)
        self.assertIn("# GPT Pro Handoff", documents["gpt-pro-handoff.md"])
        self.assertIn("`project-a`", documents["gpt-pro-handoff.md"])
        self.assertIn("`project-b`", documents["gpt-pro-handoff.md"])
        self.assertIn("Projects observed: `2`", documents["gpt-pro-handoff.md"])
        self.assertIn("`/api/tasks/{id}/logs`", documents["gpt-pro-handoff.md"])


if __name__ == "__main__":
    unittest.main()
