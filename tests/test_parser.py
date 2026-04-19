from pathlib import Path

import pytest

from cl_monitoring.domain.models import ErrorFamily, RunResult
from cl_monitoring.parsers.crawllib import parse_crawllib_logs

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "logs"

EXPECTED_RESULTS = {
    "ID_741": (RunResult.CANCELLED, ErrorFamily.CANCELLED),
    "ID_742": (RunResult.SUCCESS, None),
    "ID_743": (RunResult.SUCCESS, None),
    "ID_744": (RunResult.PARTIAL_SUCCESS, None),
    "ID_745": (RunResult.FAILED, ErrorFamily.ANTI_BOT),
    "ID_746": (RunResult.CANCELLED, ErrorFamily.CANCELLED),
    "ID_747": (RunResult.CANCELLED, ErrorFamily.CANCELLED),
    "ID_748": (RunResult.PARTIAL_SUCCESS, None),
    "ID_749": (RunResult.PARTIAL_SUCCESS, None),
    "ID_750": (RunResult.PARTIAL_SUCCESS, None),
    "ID_751": (RunResult.PARTIAL_SUCCESS, None),
    "ID_753": (RunResult.PARTIAL_SUCCESS, None),
    "ID_754": (RunResult.PARTIAL_SUCCESS, None),
    "ID_755": (RunResult.CANCELLED, ErrorFamily.CANCELLED),
    "ID_756": (RunResult.SUCCESS, None),
    "ID_757": (RunResult.PARTIAL_SUCCESS, None),
}


@pytest.mark.parametrize("task_id, expected_tuple", EXPECTED_RESULTS.items())
def test_crawllib_parser_determines_correct_status(
    task_id: str,
    expected_tuple: tuple[RunResult, ErrorFamily | None],
) -> None:
    expected_result, expected_error_family = expected_tuple

    log_file = FIXTURES_DIR / f"{task_id}.log"
    with open(log_file, encoding="utf-8") as f:
        log_lines = f.readlines()

    summary = parse_crawllib_logs(
        task_id=task_id, execution_key="test_key", log_lines=log_lines
    )

    assert summary.run_result == expected_result
    assert summary.error_family == expected_error_family
