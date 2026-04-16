"""Tests for log classifier and two-phase task classifier.

Verifies (AGENTS.md § Workflow):
- Classification is deterministic (no LLM logic).
- All log classes are detected by their patterns.
- Phase 1 (CandidateClass): assigned from metadata only, never infers success.
- Phase 2 (FinalLogClass): assigned after log inspection.
- result_count / item_scraped_count in metadata does NOT affect classification.
- finished tasks are always finished_candidate from metadata.
- Expected YAML generation produces valid files with TODO markers.
- Empty logs are classified as empty_log.
- Manual detection via zero schedule_id.

All tests are offline — no network access.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tools.classify_logs import (
    CandidateClass,
    FinalLogClass,
    LogClass,
    LogClassification,
    classify_candidate,
    classify_final,
    classify_log_text,
    generate_expected_yaml,
    generate_manifest_entry,
    is_manual_run,
)


# ── Sample log texts ───────────────────────────────────────────────────

SCRAPY_START_LOG = """\
2024-01-15 10:30:00 [scrapy.utils.log] INFO: Scrapy 2.11.0 started (bot: mybot)
2024-01-15 10:30:00 [scrapy.core.engine] INFO: Spider opened
"""

SCRAPY_STATS_LOG = """\
2024-01-15 10:35:00 [scrapy.statscollectors] INFO: Dumping Scrapy stats:
{'downloader/response_count': 150,
 'item_scraped_count': 120,
 'finish_reason': 'finished'}
"""

SCRAPY_ERROR_LOG = """\
2024-01-15 10:32:00 [scrapy.core.scraper] ERROR: Error processing
Traceback (most recent call last):
  File "spider.py", line 42, in parse
    item['price'] = response.css('span.price::text').get()
TypeError: 'NoneType' object is not subscriptable
"""

SCRAPY_WARNING_LOG = """\
2024-01-15 10:31:00 [scrapy.core.downloader] WARNING: Retrying GET
2024-01-15 10:31:05 [scrapy.core.downloader] WARNING: Gave up retrying
"""

SCRAPY_ITEM_DROP_LOG = """\
2024-01-15 10:33:00 [scrapy.core.scraper] WARNING: Dropped: duplicate item
2024-01-15 10:33:01 [scrapy.exceptions] DropItem: Missing required field
"""

SYSTEM_INFO_LOG = """\
[crawlab] 2024-01-15 10:29:50 task runner started, PID=12345
[crawlab] 2024-01-15 10:29:51 node_id: abc123
"""

MIXED_LOG = """\
[crawlab] 2024-01-15 10:29:50 task runner started, PID=12345
2024-01-15 10:30:00 [scrapy.core.engine] INFO: Spider opened
Custom spider output: processing page 1
Custom spider output: processing page 2
2024-01-15 10:32:00 [scrapy.core.scraper] ERROR: Error processing
Traceback (most recent call last):
  File "spider.py", line 42, in parse
TypeError: bad value
2024-01-15 10:35:00 [scrapy.statscollectors] INFO: Dumping Scrapy stats:
{'item_scraped_count': 50, 'finish_reason': 'finished'}
"""

BAN_429_LOG = """\
2024-01-15 10:31:00 [scrapy.core.downloader] WARNING: 429 Too Many Requests
2024-01-15 10:31:05 rate limit exceeded, backing off
"""

AUTO_STOP_LOG = """\
2024-01-15 10:35:00 max_runtime exceeded, auto_stop triggered
2024-01-15 10:35:01 [crawlab] killed by scheduler
"""


# ── Log classification tests ──────────────────────────────────────────


class TestLogClassification:
    """Verify log line classification."""

    def test_empty_log(self) -> None:
        result = classify_log_text("", task_id="t1")
        assert LogClass.EMPTY_LOG.value in result.classes_found
        assert result.total_lines == 0

    def test_whitespace_only_log(self) -> None:
        result = classify_log_text("   \n  \n  ", task_id="t2")
        assert LogClass.EMPTY_LOG.value in result.classes_found

    def test_scrapy_start_detected(self) -> None:
        result = classify_log_text(SCRAPY_START_LOG)
        assert LogClass.SCRAPY_START.value in result.classes_found

    def test_scrapy_stats_detected(self) -> None:
        result = classify_log_text(SCRAPY_STATS_LOG)
        assert LogClass.SCRAPY_STATS.value in result.classes_found
        assert result.scrapy_stats_found is True

    def test_scrapy_error_detected(self) -> None:
        result = classify_log_text(SCRAPY_ERROR_LOG)
        assert LogClass.SCRAPY_ERROR.value in result.classes_found
        assert result.error_lines > 0
        assert result.has_traceback is True

    def test_scrapy_warning_detected(self) -> None:
        result = classify_log_text(SCRAPY_WARNING_LOG)
        assert LogClass.SCRAPY_WARNING.value in result.classes_found
        assert result.warning_lines > 0

    def test_scrapy_item_drop_detected(self) -> None:
        result = classify_log_text(SCRAPY_ITEM_DROP_LOG)
        assert LogClass.SCRAPY_ITEM_DROP.value in result.classes_found

    def test_system_info_detected(self) -> None:
        result = classify_log_text(SYSTEM_INFO_LOG)
        assert LogClass.SYSTEM_INFO.value in result.classes_found

    def test_mixed_log_detects_all_classes(self) -> None:
        result = classify_log_text(MIXED_LOG)
        assert LogClass.SYSTEM_INFO.value in result.classes_found
        assert LogClass.SCRAPY_START.value in result.classes_found
        assert LogClass.SCRAPY_ERROR.value in result.classes_found
        assert LogClass.SCRAPY_STATS.value in result.classes_found
        assert LogClass.CUSTOM_PRINT.value in result.classes_found

    def test_custom_print_fallback(self) -> None:
        result = classify_log_text("Hello world\nProcessing item 42\n")
        assert LogClass.CUSTOM_PRINT.value in result.classes_found

    def test_total_lines_counted(self) -> None:
        result = classify_log_text("line1\nline2\nline3\n")
        assert result.total_lines == 3

    def test_classification_is_deterministic(self) -> None:
        """Same input must produce same output — no randomness."""
        r1 = classify_log_text(MIXED_LOG, task_id="det")
        r2 = classify_log_text(MIXED_LOG, task_id="det")
        assert r1.classes_found == r2.classes_found
        assert r1.class_line_counts == r2.class_line_counts
        assert r1.error_lines == r2.error_lines


# ── Phase 1: Candidate classification tests ───────────────────────────


class TestCandidateClassification:
    """Verify Phase 1: candidate classes from metadata only.

    Key invariant: classify_candidate never infers success/failure.
    Finished tasks are always 'finished_candidate'.
    """

    def test_pending_task(self) -> None:
        task = {"status": "pending"}
        assert classify_candidate(task) == CandidateClass.PENDING

    def test_running_task(self) -> None:
        task = {"status": "running"}
        assert classify_candidate(task) == CandidateClass.RUNNING

    def test_cancelled_task(self) -> None:
        task = {"status": "cancelled"}
        assert classify_candidate(task) == CandidateClass.CANCELLED_CANDIDATE

    def test_error_task(self) -> None:
        task = {"status": "error", "error": "spider crashed"}
        assert classify_candidate(task) == CandidateClass.ERROR_CANDIDATE

    def test_abnormal_task(self) -> None:
        task = {"status": "abnormal"}
        assert classify_candidate(task) == CandidateClass.ERROR_CANDIDATE

    def test_finished_task_is_always_candidate(self) -> None:
        """Finished tasks must NEVER be classified as success from metadata."""
        task = {"status": "finished"}
        assert classify_candidate(task) == CandidateClass.FINISHED_CANDIDATE

    def test_finished_with_high_result_count_still_candidate(self) -> None:
        """result_count must NOT influence candidate class."""
        task = {
            "status": "finished",
            "stat": {"result_count": 5000, "item_scraped_count": 5000},
        }
        assert classify_candidate(task) == CandidateClass.FINISHED_CANDIDATE

    def test_finished_with_zero_result_count_still_candidate(self) -> None:
        """Zero result_count must NOT influence candidate class either."""
        task = {
            "status": "finished",
            "stat": {"result_count": 0},
        }
        assert classify_candidate(task) == CandidateClass.FINISHED_CANDIDATE

    def test_unknown_status_becomes_error_candidate(self) -> None:
        task = {"status": "something_new"}
        assert classify_candidate(task) == CandidateClass.ERROR_CANDIDATE

    def test_empty_status(self) -> None:
        task = {"status": ""}
        assert classify_candidate(task) == CandidateClass.ERROR_CANDIDATE

    def test_missing_status(self) -> None:
        task = {}
        assert classify_candidate(task) == CandidateClass.ERROR_CANDIDATE

    def test_candidate_classes_never_contain_final_values(self) -> None:
        """CandidateClass enum must not overlap with FinalLogClass."""
        candidate_values = {c.value for c in CandidateClass}
        final_values = {f.value for f in FinalLogClass}
        assert candidate_values.isdisjoint(final_values)


# ── Phase 2: Final log classification tests ───────────────────────────


class TestFinalLogClassification:
    """Verify Phase 2: final classes after log inspection."""

    def test_cancelled_task(self) -> None:
        task = {"status": "cancelled"}
        assert classify_final(task) == FinalLogClass.CANCELLED

    def test_pending_task_is_unknown(self) -> None:
        task = {"status": "pending"}
        assert classify_final(task) == FinalLogClass.UNKNOWN

    def test_running_task_is_unknown(self) -> None:
        task = {"status": "running"}
        assert classify_final(task) == FinalLogClass.UNKNOWN

    def test_error_task_failed_other(self) -> None:
        task = {"status": "error", "error": "spider crashed"}
        assert classify_final(task) == FinalLogClass.FAILED_OTHER

    def test_error_task_with_ban(self) -> None:
        task = {"status": "error", "error": ""}
        assert classify_final(task, BAN_429_LOG) == FinalLogClass.BAN_429

    def test_finished_strong_success(self) -> None:
        """Finished + stats with items in LOGS → strong success."""
        task = {"status": "finished"}
        log = SCRAPY_STATS_LOG  # has item_scraped_count: 120
        assert classify_final(task, log) == FinalLogClass.SUCCESS_STRONG

    def test_finished_probable_success_no_items_in_log(self) -> None:
        """Finished + stats but no items in log → probable success."""
        task = {"status": "finished"}
        log = "Dumping Scrapy stats:\n{'item_scraped_count': 0, 'finish_reason': 'finished'}"
        assert classify_final(task, log) == FinalLogClass.SUCCESS_PROBABLE

    def test_finished_no_log_is_unknown(self) -> None:
        """Finished with no log → unknown (cannot infer success)."""
        task = {"status": "finished"}
        assert classify_final(task) == FinalLogClass.UNKNOWN

    def test_partial_success(self) -> None:
        """Finished + errors + stats → partial success."""
        task = {"status": "finished"}
        assert classify_final(task, MIXED_LOG) == FinalLogClass.PARTIAL_SUCCESS

    def test_auto_stop(self) -> None:
        """Finished + auto_stop pattern → auto_stop."""
        task = {"status": "finished"}
        assert classify_final(task, AUTO_STOP_LOG) == FinalLogClass.AUTO_STOP

    def test_ban_429_in_finished(self) -> None:
        """Finished + 429 pattern → ban_429."""
        task = {"status": "finished"}
        assert classify_final(task, BAN_429_LOG) == FinalLogClass.BAN_429

    def test_abnormal_with_ban(self) -> None:
        task = {"status": "abnormal"}
        assert classify_final(task, BAN_429_LOG) == FinalLogClass.BAN_429

    def test_abnormal_without_ban(self) -> None:
        task = {"status": "abnormal"}
        assert classify_final(task) == FinalLogClass.FAILED_OTHER


# ── Manual run detection ──────────────────────────────────────────────


class TestManualRunDetection:
    """Verify manual run detection from schedule_id."""

    def test_zero_schedule_id_is_manual(self) -> None:
        task = {"schedule_id": "000000000000000000000000"}
        assert is_manual_run(task) is True

    def test_empty_schedule_id_is_manual(self) -> None:
        task = {"schedule_id": ""}
        assert is_manual_run(task) is True

    def test_missing_schedule_id_is_manual(self) -> None:
        task = {}
        assert is_manual_run(task) is True

    def test_real_schedule_id_is_not_manual(self) -> None:
        task = {"schedule_id": "66a811c5116add6c8f266e8e"}
        assert is_manual_run(task) is False


# ── Expected YAML generation ──────────────────────────────────────────


class TestExpectedYamlGeneration:
    """Verify draft expected/*.yaml generation."""

    def test_generates_yaml_file(self, tmp_path: Path) -> None:
        classification = LogClassification(
            task_id="TASK_ID_001",
            total_lines=50,
            classes_found=["scrapy_start", "scrapy_stats"],
            class_line_counts={"scrapy_start": 2, "scrapy_stats": 5},
            error_lines=0,
            warning_lines=1,
            scrapy_stats_found=True,
            has_traceback=False,
        )
        filepath = generate_expected_yaml(classification, tmp_path)
        assert filepath.exists()
        assert filepath.name == "task_TASK_ID_001_log.yaml"

    def test_yaml_has_todo_marker(self, tmp_path: Path) -> None:
        classification = LogClassification(
            task_id="TASK_ID_002",
            total_lines=10,
            classes_found=["empty_log"],
            class_line_counts={"empty_log": 1},
        )
        filepath = generate_expected_yaml(classification, tmp_path)
        content = filepath.read_text()
        assert "# TODO: verify" in content

    def test_yaml_is_parseable(self, tmp_path: Path) -> None:
        classification = LogClassification(
            task_id="TASK_ID_003",
            total_lines=100,
            classes_found=["scrapy_error", "scrapy_stats"],
            class_line_counts={"scrapy_error": 5, "scrapy_stats": 3},
            error_lines=5,
            scrapy_stats_found=True,
            has_traceback=True,
        )
        filepath = generate_expected_yaml(classification, tmp_path)
        content = filepath.read_text()
        # Remove comment lines and parse YAML
        yaml_content = "\n".join(
            line for line in content.splitlines()
            if not line.startswith("#")
        )
        parsed = yaml.safe_load(yaml_content)
        assert parsed is not None
        assert parsed["task_id"] == "TASK_ID_003"
        assert parsed["stats"]["error_lines"] == 5
        assert parsed["stats"]["has_traceback"] is True


# ── Manifest entry generation ─────────────────────────────────────────


class TestManifestEntry:
    """Verify manifest entry format with two-phase classification."""

    def test_entry_has_required_fields(self) -> None:
        classification = LogClassification(
            task_id="TASK_ID_001",
            total_lines=50,
            classes_found=["scrapy_stats"],
            error_lines=0,
        )
        entry = generate_manifest_entry(
            task_id="TASK_ID_001",
            final_class=FinalLogClass.SUCCESS_STRONG,
            candidate_class=CandidateClass.FINISHED_CANDIDATE,
            log_classification=classification,
            fixture_paths={"task": "api/task_TASK_ID_001.json"},
        )
        assert entry["task_id"] == "TASK_ID_001"
        assert entry["final_class"] == "success_strong"
        assert entry["candidate_class"] == "finished_candidate"
        assert entry["trigger"] == "scheduled"
        assert entry["files"]["task"] == "api/task_TASK_ID_001.json"
        assert entry["total_log_lines"] == 50
        assert entry["error_lines"] == 0

    def test_entry_without_log(self) -> None:
        entry = generate_manifest_entry(
            task_id="TASK_ID_002",
            final_class=FinalLogClass.UNKNOWN,
            candidate_class=CandidateClass.PENDING,
            log_classification=None,
            fixture_paths={"task": "api/task_TASK_ID_002.json"},
        )
        assert "total_log_lines" not in entry

    def test_entry_manual_trigger(self) -> None:
        entry = generate_manifest_entry(
            task_id="TASK_ID_003",
            final_class=FinalLogClass.UNKNOWN,
            candidate_class=CandidateClass.RUNNING,
            log_classification=None,
            fixture_paths={},
            is_manual=True,
        )
        assert entry["trigger"] == "manual"
