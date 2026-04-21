from typing import Any


def _normalize_table_cell(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", "<br>")


def render_api_map(api_map: list[dict[str, Any]]) -> str:
    lines = [
        "# API Map",
        "",
        "| Path | Method | Query | Source | Status | Notes |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in api_map:
        lines.append(
            "| `{path}` | `{method}` | `{query}` | `{source}` | `{status}` | {notes} |".format(
                path=row["path"],
                method=row["method"],
                query=row["query"],
                source=row["source"],
                status=row["status"],
                notes=_normalize_table_cell(row["notes"]),
            )
        )
    return "\n".join(lines) + "\n"


def render_entity_summary(entity_summary: dict[str, dict[str, Any]]) -> str:
    lines = ["# Entity Summary", ""]
    if not entity_summary:
        lines.extend(["- none observed", ""])
        return "\n".join(lines)

    for entity_name, payload in entity_summary.items():
        lines.extend(
            [
                f"## {entity_name.title()}",
                "",
                f"- Count: {payload.get('count', 0)}",
                f"- Fields: {', '.join(payload.get('fields', [])) or 'none observed'}",
                "",
            ]
        )
    return "\n".join(lines)


def render_parameter_taxonomy(
    parameter_records: list[dict[str, Any]],
    library_observations: list[dict[str, Any]],
) -> str:
    grouped = {"fact": [], "hypothesis": [], "unknown": []}
    for record in parameter_records:
        grouped.setdefault(record["classification_status"], []).append(record)

    lines = ["# Parameter Taxonomy", ""]
    for heading, key in (("Facts", "fact"), ("Hypotheses", "hypothesis"), ("Unknowns", "unknown")):
        lines.append(f"## {heading}")
        lines.append("")
        records = grouped.get(key, [])
        if not records:
            lines.append("- none")
        else:
            for record in records:
                lines.append(
                    f"- `{record['normalized_key']}` -> `{record['role']}` (value: `{record.get('value')}`)"
                )
        lines.append("")

    lines.append("## Library Observations")
    lines.append("")
    if not library_observations:
        lines.append("- none")
    else:
        for item in library_observations:
            observation = ", ".join(item["matched_flags"]) or (
                "unknown" if item.get("status") == "unknown" else "no flags"
            )
            lines.append(
                f"- `{item['internal_path']}` in `{item['wheel_path']}` -> {observation}"
            )
    lines.append("")
    return "\n".join(lines)


def render_log_patterns(log_samples: list[dict[str, Any]]) -> str:
    lines = ["# Log Patterns", ""]
    if not log_samples:
        lines.extend(["- no representative logs observed", ""])
        return "\n".join(lines)

    for sample in log_samples:
        lines.extend(
            [
                f"## {sample['case_type']}",
                "",
                f"- Task: `{sample['task_id']}`",
                f"- Signals: `{sample['signals']}`",
                f"- Tail: `{sample['tail']}`",
                "",
            ]
        )
    return "\n".join(lines)


def render_open_questions(open_questions: list[str]) -> str:
    lines = ["# Open Questions", ""]
    if not open_questions:
        lines.append("- none")
    else:
        lines.extend(f"- {item}" for item in open_questions)
    lines.append("")
    return "\n".join(lines)


def render_report_documents(bundle: dict[str, Any]) -> dict[str, str]:
    return {
        "api-map.md": render_api_map(bundle["api_map"]),
        "entity-summary.md": render_entity_summary(bundle["entity_summary"]),
        "parameter-taxonomy.md": render_parameter_taxonomy(
            bundle["parameter_records"], bundle["library_observations"]
        ),
        "log-patterns.md": render_log_patterns(bundle["log_samples"]),
        "open-questions.md": render_open_questions(bundle["open_questions"]),
    }
