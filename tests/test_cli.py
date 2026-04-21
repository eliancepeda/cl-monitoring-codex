import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from collector.config import Settings
from run_discovery import build_parser, main


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

    def test_main_clears_existing_output_root_before_writing_new_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_root = Path(tmp_dir) / "docs/discovery"
            stale_file = output_root / "raw/stale.json"
            stale_file.parent.mkdir(parents=True, exist_ok=True)
            stale_file.write_text('{"stale": true}\n', encoding="utf-8")

            def fake_run_discovery(settings, transport, store):
                store.write_json("normalized/tasks.json", [{"task_id": "fresh-task"}])

            with patch("run_discovery.load_settings") as load_settings_mock, patch(
                "run_discovery.GetOnlyTransport"
            ), patch("run_discovery.run_discovery", side_effect=fake_run_discovery):
                load_settings_mock.return_value = Settings(
                    base_url="https://crawlab.example",
                    api_key="secret-token",
                    project_ids=("project-a",),
                    output_root=str(output_root),
                    throttle_seconds=0,
                    page_size=100,
                    expanded_task_limit=20,
                    max_pages=5,
                    log_page_size=100,
                )

                exit_code = main(["--project-id", "project-a", "--output-root", str(output_root)])

            self.assertEqual(exit_code, 0)
            self.assertFalse(stale_file.exists())
            self.assertTrue((output_root / "normalized/tasks.json").exists())


if __name__ == "__main__":
    unittest.main()
