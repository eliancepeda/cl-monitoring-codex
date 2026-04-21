import json
import tempfile
import unittest
from pathlib import Path

from collector.raw_store import ArtifactStore


class ArtifactStoreTests(unittest.TestCase):
    def test_write_json_creates_parent_directories_with_expected_format(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            expected_path = Path(tmp_dir) / "raw/projects/page-1.json"

            written_path = store.write_json(
                "raw/projects/page-1.json",
                {"status": "ok", "emoji": "cafe\u0301", "ascii_escape": "caf\u00e9"},
            )

            self.assertEqual(written_path, str(expected_path))
            self.assertTrue(expected_path.exists())
            self.assertEqual(
                expected_path.read_text(encoding="utf-8"),
                '{\n'
                '  "ascii_escape": "caf\\u00e9",\n'
                '  "emoji": "cafe\\u0301",\n'
                '  "status": "ok"\n'
                '}\n',
            )
            self.assertEqual(
                json.loads(expected_path.read_text(encoding="utf-8")),
                {"status": "ok", "emoji": "cafe\u0301", "ascii_escape": "caf\u00e9"},
            )

    def test_write_text_creates_parent_directories_and_returns_written_path(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)
            expected_path = Path(tmp_dir) / "docs/api/api-map.md"

            written_path = store.write_text("docs/api/api-map.md", "# API Map\n")

            self.assertEqual(written_path, str(expected_path))
            self.assertTrue(expected_path.exists())
            self.assertEqual(expected_path.read_text(encoding="utf-8"), "# API Map\n")

    def test_write_json_rejects_escape_attempts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)

            with self.assertRaises(ValueError):
                store.write_json("../outside.json", {"status": "nope"})

            with self.assertRaises(ValueError):
                store.write_json("/tmp/outside.json", {"status": "nope"})

    def test_write_text_rejects_escape_attempts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = ArtifactStore(tmp_dir)

            with self.assertRaises(ValueError):
                store.write_text("../outside.md", "blocked\n")

            with self.assertRaises(ValueError):
                store.write_text("/tmp/outside.md", "blocked\n")


if __name__ == "__main__":
    unittest.main()
