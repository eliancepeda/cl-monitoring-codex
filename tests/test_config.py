import unittest

from collector.config import load_settings


class LoadSettingsTests(unittest.TestCase):
    def _load_settings(self):
        return load_settings(
            project_ids=["project-a", "project-b"],
            environ={
                "CRAWLAB_BASE_URL": "https://crawlab.example/",
                "CRAWLAB_API_KEY": "secret-token",
            },
        )

    def test_load_settings_rejects_project_ids_string(self):
        with self.assertRaises(ValueError) as context:
            load_settings(
                project_ids="project-a",
                environ={
                    "CRAWLAB_BASE_URL": "https://crawlab.example/",
                    "CRAWLAB_API_KEY": "secret-token",
                },
            )

        self.assertEqual(
            str(context.exception),
            "project_ids must be a sequence of strings, not a string",
        )

    def test_load_settings_requires_at_least_one_non_whitespace_project_id(self):
        with self.assertRaises(ValueError) as context:
            load_settings(
                project_ids=[" ", "\t"],
                environ={
                    "CRAWLAB_BASE_URL": "https://crawlab.example/",
                    "CRAWLAB_API_KEY": "secret-token",
                },
            )

        self.assertEqual(
            str(context.exception), "project_ids must not contain empty values"
        )

    def test_load_settings_requires_at_least_one_project_id(self):
        with self.assertRaises(ValueError) as context:
            load_settings(
                project_ids=[],
                environ={
                    "CRAWLAB_BASE_URL": "https://crawlab.example/",
                    "CRAWLAB_API_KEY": "secret-token",
                },
            )

        self.assertEqual(str(context.exception), "At least one project id is required")

    def test_load_settings_rejects_mixed_valid_and_whitespace_project_ids(self):
        with self.assertRaises(ValueError) as context:
            load_settings(
                project_ids=["project-a", " "],
                environ={
                    "CRAWLAB_BASE_URL": "https://crawlab.example/",
                    "CRAWLAB_API_KEY": "secret-token",
                },
            )

        self.assertEqual(
            str(context.exception), "project_ids must not contain empty values"
        )

    def test_load_settings_requires_env_vars_without_echoing_values(self):
        with self.assertRaises(ValueError) as context:
            load_settings(project_ids=["project-a", "project-b"], environ={})

        self.assertEqual(
            str(context.exception),
            "Missing required environment variables: CRAWLAB_BASE_URL, CRAWLAB_API_KEY",
        )

    def test_load_settings_normalizes_base_url_and_defaults(self):
        settings = self._load_settings()

        self.assertEqual(settings.base_url, "https://crawlab.example")
        self.assertEqual(settings.project_ids, ("project-a", "project-b"))
        self.assertEqual(settings.output_root, "docs/discovery")
        self.assertEqual(settings.throttle_seconds, 0.5)
        self.assertEqual(settings.page_size, 25)
        self.assertEqual(settings.expanded_task_limit, 60)
        self.assertEqual(settings.max_pages, 60)
        self.assertEqual(settings.log_page_size, 100)

    def test_load_settings_exposes_richer_discovery_caps(self):
        settings = self._load_settings()

        self.assertEqual(settings.max_spiders_per_project_for_detail, 12)
        self.assertEqual(settings.max_tasks_per_spider_for_detail, 6)
        self.assertEqual(settings.max_task_details_total, 96)
        self.assertEqual(settings.max_log_tasks_total, 48)

    def test_load_settings_exposes_widened_discovery_defaults(self):
        settings = self._load_settings()

        self.assertEqual(settings.max_project_pages, 6)
        self.assertEqual(settings.max_spider_pages, 20)
        self.assertEqual(settings.max_schedule_pages, 20)
        self.assertEqual(settings.max_task_pages, 60)
        self.assertEqual(settings.max_log_tasks_per_project, 24)
        self.assertEqual(settings.max_log_pages_per_task, 5)
        self.assertEqual(settings.task_page_stability_window, 3)


if __name__ == "__main__":
    unittest.main()
