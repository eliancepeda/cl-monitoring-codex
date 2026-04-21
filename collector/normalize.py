import shlex
from typing import Any


FACT_ROLE_MAP = {
    "proxy_country": "execution modifier",
    "proxy_vpn_country": "execution modifier",
    "debug": "non-production flag",
    "test": "non-production flag",
}

HYPOTHESIS_ROLE_MAP = {
    "sp": "identity candidate",
    "fp": "identity candidate",
    "pc": "shard/scope candidate",
    "tags": "shard/scope candidate",
    "as": "execution modifier",
    "spi": "rerun/recovery indicator",
}


def _is_negative_numeric_literal(token: str) -> bool:
    if not token.startswith("-") or len(token) == 1:
        return False
    numeric_portion = token[1:]
    if numeric_portion.isdigit():
        return True
    if numeric_portion.count(".") == 1:
        left, right = numeric_portion.split(".", 1)
        return bool(left) and bool(right) and left.isdigit() and right.isdigit()
    return False


def tokenize_args(raw_args: Any) -> list[str]:
    if raw_args is None:
        return []
    if isinstance(raw_args, str):
        try:
            return shlex.split(raw_args)
        except ValueError:
            return raw_args.split()
    if isinstance(raw_args, list):
        return [str(item) for item in raw_args]
    return [str(raw_args)]


def classify_parameter(normalized_key: str) -> dict[str, str]:
    if normalized_key in FACT_ROLE_MAP:
        return {
            "role": FACT_ROLE_MAP[normalized_key],
            "classification_status": "fact",
        }
    if normalized_key in HYPOTHESIS_ROLE_MAP:
        return {
            "role": HYPOTHESIS_ROLE_MAP[normalized_key],
            "classification_status": "hypothesis",
        }
    return {"role": "unknown", "classification_status": "unknown"}


def normalize_launch_parameters(raw_args: Any) -> dict[str, Any]:
    tokens = tokenize_args(raw_args)
    parameters = []
    positionals = []
    index = 0

    while index < len(tokens):
        token = tokens[index]
        if not token.startswith("-"):
            positionals.append(token)
            index += 1
            continue

        value: Any = True
        flag = token
        if "=" in token:
            flag, value = token.split("=", 1)
        elif index + 1 < len(tokens) and (
            not tokens[index + 1].startswith("-")
            or _is_negative_numeric_literal(tokens[index + 1])
        ):
            value = tokens[index + 1]
            index += 1

        normalized_key = flag.lstrip("-").replace("-", "_")
        classification = classify_parameter(normalized_key)
        parameters.append(
            {
                "raw_token": token,
                "normalized_key": normalized_key,
                "value": value,
                "role": classification["role"],
                "classification_status": classification["classification_status"],
            }
        )
        index += 1

    return {
        "raw_args": raw_args,
        "tokens": tokens,
        "parameters": parameters,
        "positionals": positionals,
    }


def _first_present(record: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        if isinstance(value, dict):
            nested_id = value.get("_id") or value.get("id")
            if nested_id is not None:
                return nested_id
        return value
    return None


def _normalize_schedule_id(value: Any) -> str:
    if value is None:
        return "unscheduled"
    normalized = str(value)
    if normalized and set(normalized) == {"0"}:
        return "unscheduled"
    return normalized


def build_observation_unit(task: dict[str, Any], spider: dict[str, Any]) -> dict[str, Any]:
    raw_launch_params = task.get("param") or task.get("args") or task.get("command") or task.get("cmd")
    normalized = normalize_launch_parameters(raw_launch_params)
    schedule_id = _normalize_schedule_id(
        _first_present(task, ("schedule_id", "scheduleId", "schedule"))
    )
    ordered_pairs = sorted(
        f"{item['normalized_key']}={item['value']}"
        for item in normalized["parameters"]
    )
    observation_key = "|".join(
        [
            spider.get("name") or spider.get("_id") or "unknown-spider",
            schedule_id,
            *(ordered_pairs or ["no-params"]),
        ]
    )
    return {
        "task_id": task.get("_id") or task.get("id"),
        "spider_id": spider.get("_id") or spider.get("id"),
        "spider_name": spider.get("name") or spider.get("_id") or "unknown-spider",
        "schedule_id": schedule_id,
        "normalized_params": normalized,
        "observation_key": observation_key,
    }
