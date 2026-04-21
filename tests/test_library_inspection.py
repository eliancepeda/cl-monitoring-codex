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
        self.assertEqual(result[0]["snippets"], [])

    def test_inspect_wheel_sources_marks_malformed_wheels_unknown(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            wheel_path = Path(tmp_dir) / "demo.whl"
            wheel_path.write_bytes(b"not a valid wheel")

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
        self.assertEqual(result[0]["snippets"], [])


if __name__ == "__main__":
    unittest.main()
