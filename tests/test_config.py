import unittest

from collector.config import load_settings


class LoadSettingsTests(unittest.TestCase):
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
