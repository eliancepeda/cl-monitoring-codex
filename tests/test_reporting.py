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
