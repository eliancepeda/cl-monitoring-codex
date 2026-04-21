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
                f"- Sampling: `{sample.get('sampling_strategy', 'first page only')}`",
                f"- Sampled pages: `{sample.get('sampled_pages', [1])}`",
                f"- Signals: `{sample['signals']}`",
                f"- Tail: `{sample['tail']}`",
                "",
            ]
        )
    return "\n".join(lines)


def render_log_sampling_strategy(
    log_sampling_strategy: list[dict[str, Any]], log_sampling_summary: list[dict[str, Any]]
) -> str:
    lines = ["# Log Sampling Strategy", ""]
    if not log_sampling_strategy and not log_sampling_summary:
        lines.extend(["- unknown pagination", ""])
        return "\n".join(lines)

    for summary in log_sampling_summary:
        lines.extend(
            [
                f"## Project {summary['project_id']}",
                "",
                f"- Sampled log tasks: `{summary['sampled_tasks']}/{summary['quota']}`",
            ]
        )
        if summary.get("shortfall_reason"):
            lines.append(f"- Shortfall: {summary['shortfall_reason']}")
        lines.append("")

    for item in log_sampling_strategy:
        pagination = item.get("pagination", {"status": "unknown", "total_pages": "unknown"})
        lines.extend(
            [
                f"## {item['task_id']}",
                "",
                f"- Project: `{item.get('project_id', 'unknown')}`",
                f"- Strategy: {item['strategy']}",
                f"- Pages: {item['pages']}",
                f"- Pagination: {pagination['status']} ({pagination['total_pages']})",
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


def render_gpt_pro_handoff(bundle: dict[str, Any]) -> str:
    task_inventory = bundle.get("task_inventory", {})
    project_ids = list(task_inventory.get("projects", {}).keys())
    if not project_ids:
        project_ids = [
            item["project_id"]
            for item in bundle.get("log_sampling_summary", [])
            if item.get("project_id")
        ]

    entity_summary = bundle.get("entity_summary", {})
    api_paths = sorted(
        {
            row.get("path")
            for row in bundle.get("api_map", [])
            if row.get("method") == "GET" and row.get("path")
        }
    )
    parameter_keys = sorted(
        {
            record["normalized_key"]
            for record in bundle.get("parameter_records", [])
            if record.get("classification_status") == "hypothesis"
        }
    )
    observed_case_types = sorted(
        {item["case_type"] for item in bundle.get("log_samples", []) if item.get("case_type")}
    )
    missing_case_types = sorted(
        item.split("`", 2)[1]
        for item in bundle.get("open_questions", [])
        if item.startswith("Representative case type not observed: `")
    )

    lines = [
        "# GPT Pro Handoff",
        "",
        "## Scope",
        "",
        "This package is a read-only Crawlab discovery snapshot for these target projects:",
        "",
    ]
    if project_ids:
        lines.extend(f"- `{project_id}`" for project_id in project_ids)
    else:
        lines.append("- none observed")

    lines.extend(
        [
            "",
            "It was collected with a GET-only Python collector. No POST/PUT/PATCH/DELETE calls were used.",
            "",
            "## Read First",
            "",
            "1. `docs/discovery/entity-summary.md`",
            "2. `docs/discovery/parameter-taxonomy.md`",
            "3. `docs/discovery/log-patterns.md`",
            "4. `docs/discovery/log-sampling-strategy.md`",
            "5. `docs/discovery/open-questions.md`",
            "6. `docs/discovery/api-map.md`",
            "7. `docs/discovery/normalized/tasks.json`",
            "",
            "## Current Snapshot",
            "",
            f"- Projects observed: `{entity_summary.get('projects', {}).get('count', 0)}`",
            f"- Spiders observed for target projects: `{entity_summary.get('spiders', {}).get('count', 0)}`",
            f"- Schedules observed: `{entity_summary.get('schedules', {}).get('count', 0)}`",
            f"- Sampled normalized tasks: `{entity_summary.get('tasks', {}).get('count', 0)}`",
            f"- Nodes counted in summary: `{entity_summary.get('nodes', {}).get('count', 0)}`",
            "",
            "Observed GET endpoints in this instance include:",
            "",
        ]
    )
    if api_paths:
        lines.extend(f"- `{path}`" for path in api_paths)
    else:
        lines.append("- none observed")

    lines.extend(["", "## Parameters Observed In This Run", ""])
    if parameter_keys:
        lines.extend(f"- `{key}`" for key in parameter_keys)
    else:
        lines.append("- no hypothesis-level parameters observed")

    lines.extend(["", "## Representative Cases Observed", ""])
    if observed_case_types:
        lines.extend(f"- `{case_type}`" for case_type in observed_case_types)
    else:
        lines.append("- none observed")

    if missing_case_types:
        lines.extend(["", "This run did not observe these representative classes:", ""])
        lines.extend(f"- `{case_type}`" for case_type in missing_case_types)

    lines.extend(
        [
            "",
            "## Constraints For Downstream Analysis",
            "",
            "- Treat raw and normalized artifacts as the source of truth.",
            "- Do not infer unsupported Crawlab endpoints beyond `api-map.md`.",
            "- Keep `fact`, `hypothesis`, and `unknown` separated.",
            "- Treat missing representative classes as gaps in this sampled run, not proof of absence in the real system.",
            "",
        ]
    )
    return "\n".join(lines)


def render_report_documents(bundle: dict[str, Any]) -> dict[str, str]:
    return {
        "api-map.md": render_api_map(bundle["api_map"]),
        "entity-summary.md": render_entity_summary(bundle["entity_summary"]),
        "parameter-taxonomy.md": render_parameter_taxonomy(
            bundle["parameter_records"], bundle["library_observations"]
        ),
        "log-patterns.md": render_log_patterns(bundle["log_samples"]),
        "log-sampling-strategy.md": render_log_sampling_strategy(
            bundle["log_sampling_strategy"], bundle.get("log_sampling_summary", [])
        ),
        "open-questions.md": render_open_questions(bundle["open_questions"]),
        "gpt-pro-handoff.md": render_gpt_pro_handoff(bundle),
    }
