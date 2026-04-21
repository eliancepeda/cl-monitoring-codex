import unittest

from collector.log_analysis import analyze_log_text


class LogAnalysisTests(unittest.TestCase):
    def test_analyze_log_detects_errors_progress_summary_and_tail(self):
        text = "\n".join(
            [
                "start crawl",
                "repeat",
                "processed 10/20",
                "repeat",
                "HTTP 503 upstream",
                "found 15 items",
                "written 15 items",
                "timeout after 5s",
                "Traceback: boom",
                "Summary: done",
                "final line",
            ]
        )

        result = analyze_log_text(text, tail_lines=2)

        self.assertEqual(result["joined"], text)
        self.assertEqual(result["line_count"], 11)
        self.assertEqual(result["char_count"], len(text))
        self.assertTrue(result["signals"]["traceback_or_fatal"])
        self.assertTrue(result["signals"]["timeout"])
        self.assertTrue(result["signals"]["http_403_404_5xx"])
        self.assertTrue(result["signals"]["found_or_written"])
        self.assertTrue(result["signals"]["has_summary"])
        self.assertTrue(result["signals"]["has_progress"])
        self.assertEqual(result["stable_fragments"], ["repeat"])
        self.assertIn("start crawl", result["unstable_fragments"])
        self.assertEqual(result["tail"], ["Summary: done", "final line"])

    def test_analyze_log_marks_empty_logs(self):
        result = analyze_log_text("")

        self.assertTrue(result["signals"]["empty_log"])
        self.assertEqual(result["tail"], [])

    def test_analyze_log_coerces_list_payload_items(self):
        result = analyze_log_text([1, "second line", None])

        self.assertEqual(result["joined"], "1\nsecond line\nNone")
        self.assertEqual(result["line_count"], 3)
        self.assertEqual(result["char_count"], len("1\nsecond line\nNone"))
        self.assertEqual(result["stable_fragments"], [])
        self.assertEqual(result["unstable_fragments"], ["1", "second line", "None"])
        self.assertFalse(result["signals"]["timeout"])

    def test_analyze_log_detects_obvious_generic_error_markers(self):
        result = analyze_log_text("ERROR: request failed with Exception")

        self.assertTrue(result["signals"]["traceback_or_fatal"])

    def test_analyze_log_requires_http_context_for_403_404_5xx_signal(self):
        benign_result = analyze_log_text(
            "task finished\nper page: 500\nauto_stop=500\nfinished in 12s"
        )
        http_result = analyze_log_text(
            "HTTP 503 upstream\nHTTP Error 504\nrequest ended with http 404"
        )

        self.assertFalse(benign_result["signals"]["http_403_404_5xx"])
        self.assertTrue(http_result["signals"]["http_403_404_5xx"])

    def test_analyze_log_detects_page_progress_with_colon_format(self):
        lower_result = analyze_log_text("page: 1")
        upper_result = analyze_log_text("Page: 1")

        self.assertTrue(lower_result["signals"]["has_progress"])
        self.assertTrue(upper_result["signals"]["has_progress"])


if __name__ == "__main__":
    unittest.main()
