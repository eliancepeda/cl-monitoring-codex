import re
from collections.abc import Iterable

from cl_monitoring.domain.models import Confidence, ErrorFamily, RunResult, RunSummary

_EMPTY_SUCCESS_RESPONSE = (
    "{'status': 'ok', 'message': 'success', 'total': 0, "
    "'data': None, 'error': ''}"
)


def parse_crawllib_logs(
    task_id: str, execution_key: str, log_lines: Iterable[str]
) -> RunSummary:
    counters = {
        "item_events": 0,
        "put_to_parser": 0,
        "is_success_true": 0,
        "sku_not_found": 0,
        "404_gone": 0,
    }

    evidence: list[str] = []

    has_cancel = False
    has_auto_stop = False
    has_error_auto_stop = False
    has_429_ban = False
    has_resume_success = False

    for line in log_lines:
        line_clean = line.strip()

        # item events: (Id: ..., Page: ...): {...}
        if re.search(r"\(Id:\s*\d+.*\):\s*\{", line_clean):
            counters["item_events"] += 1

        # put_to_parser
        if "put_to_parser" in line_clean and (
            "INFO:crawllib.managers:" in line_clean or "INFO:HOST_" in line_clean
        ):
            counters["put_to_parser"] += 1

        # isSuccess=true
        if '"isSuccess":true' in line_clean or '"isSuccess": true' in line_clean:
            counters["is_success_true"] += 1

        # sku_not_found
        if "НЕ СПАРШЕНО. Причина: не нашёл SKU" in line_clean:
            counters["sku_not_found"] += 1

        # 404 gone
        if "gone status code 404" in line_clean.lower():
            counters["404_gone"] += 1

        # resume success marker
        if "Резюме: ✅" in line_clean:
            has_resume_success = True
            evidence.append(line_clean)

        # cancel markers
        if "CancelledError:" in line_clean or _EMPTY_SUCCESS_RESPONSE in line_clean:
            has_cancel = True
            evidence.append(line_clean)

        # auto_stop / error_auto_stop
        if "auto_stop" in line_clean and "is reached" in line_clean:
            if "error_auto_stop" in line_clean:
                has_error_auto_stop = True
            else:
                has_auto_stop = True
            evidence.append(line_clean)

        # 429 ban
        if (
            "ban status code 429" in line_clean.lower()
            or "429" in line_clean
            and "ban" in line_clean.lower()
        ):
            has_429_ban = True
            evidence.append(line_clean)

    # Classification logic
    run_result = RunResult.UNKNOWN
    error_family = None
    confidence = Confidence.HIGH

    if has_cancel:
        run_result = RunResult.CANCELLED
        error_family = ErrorFamily.CANCELLED
    elif has_429_ban and has_error_auto_stop:
        run_result = RunResult.FAILED
        error_family = ErrorFamily.ANTI_BOT
    elif has_auto_stop or has_error_auto_stop:
        run_result = RunResult.RULE_STOPPED
    elif has_resume_success:
        run_result = RunResult.SUCCESS
    elif counters["put_to_parser"] > 0 or counters["is_success_true"] > 0:
        run_result = RunResult.PARTIAL_SUCCESS
        confidence = Confidence.MEDIUM
    elif counters["item_events"] > 0:
        run_result = RunResult.PARTIAL_SUCCESS
        confidence = Confidence.LOW

    return RunSummary(
        task_id=task_id,
        execution_key=execution_key,
        run_result=run_result,
        error_family=error_family,
        confidence=confidence,
        reason_code="",
        counters=counters,
        evidence=evidence,
    )
