from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .storage import atomic_json


def write_report(record: dict[str, Any], destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    record = {**record, "report_written_at": datetime.now(UTC).isoformat()}
    atomic_json(destination / "report.json", record)
    facts = record.get("facts", {})
    rows = "\n".join(f"| {key} | {json.dumps(value)} |" for key, value in facts.items())
    lines = [
        "# RunPod CI Broker report",
        "",
        f"Evidence mode: **{record['mode']}**. Real GPU execution: **"
        + (
            "verified"
            if record["mode"] == "live" and record.get("gpu_verified")
            else "not verified"
        )
        + "**.",
        "",
        f"State: `{record.get('state', 'PLANNED')}`",
        "",
        f"Configuration fingerprint: `{record['config_fingerprint']}`",
        "",
        "| Fact | Value |",
        "|---|---|",
        rows,
        "",
        "## Preflight outcome",
        "",
        "Blockers: " + (", ".join(record.get("blockers", [])) or "none"),
        "",
        "A candidate selection is advisory. It does not authorize paid execution.",
        "",
        "## Candidate decisions",
        "",
        "| GPU | Data center | VRAM GB | USD/GPU-hour | Capacity | Decision |",
        "|---|---|---|---|---|---|",
    ]
    for row in record.get("decisions", []):
        c = row["candidate"]
        decision = ", ".join(row["reasons"]) or "eligible; see ranking below"
        # Report only controlled identifiers, escaping table separators/newlines.
        cells = [
            str(c["gpu_id"]),
            str(c["data_center"]),
            str(c["memory_gb"]),
            str(c["hourly_usd"]),
            c["capacity"],
            decision,
        ]
        cells = [s.replace("|", "\\|").replace("\n", " ") for s in cells]
        lines.append("| " + " | ".join(cells) + " |")
    if "resource_search" in record:
        search = record["resource_search"]
        lines += [
            "",
            "## Resource search",
            "",
            "Objective: "
            + search["policy"]["objective"]
            + ". Stock grades are ordinal hints, not allocation probabilities.",
            "Status: "
            + record.get("state", "unverified")
            + "; HTTP reads: "
            + str(
                search.get("http_requests")
                if search.get("http_requests") is not None
                else "unmeasured"
            )
            + ".",
            "Selection reason: " + json.dumps(record.get("selection_reason")),
            "Candidate quote expires at UTC epoch: "
            + str(record.get("candidate_valid_until", "not a live quote"))
            + ". Refresh inventory and recheck the budget before any create.",
            "",
            "| Order | GPU | Data center | Stock signal | USD/GPU-hour |",
            "|---|---|---|---|---|",
        ]
        for index, item in enumerate(record.get("candidate_queue", [])[:20], 1):
            c = item["candidate"]
            cells = [
                str(index),
                c["gpu_id"],
                c["data_center"],
                item["availability_signal"],
                str(c["hourly_usd"]),
            ]
            lines.append(
                "| " + " | ".join(x.replace("|", "\\|").replace("\n", " ") for x in cells) + " |"
            )
        lines += [
            "",
            "| Query | Elapsed seconds | Selected GPU | Blockers | Next query seconds |",
            "|---|---|---|---|---|",
        ]
        for item in search.get("rounds", []):
            cells = [
                str(item["round"]),
                str(item["elapsed_seconds"]),
                (item["selected"] or {}).get("gpu_id", "none"),
                ", ".join(item["blockers"]) or "none",
                str(item["next_query_in_seconds"]),
            ]
            lines.append(
                "| " + " | ".join(x.replace("|", "\\|").replace("\n", " ") for x in cells) + " |"
            )
        lines += [
            "",
            "Search alone does not allocate compute. All candidates and cooling exclusions are retained in report.json.",
        ]
    if "placement_plan" in record:
        proposal = record["placement_plan"]
        lines += [
            "",
            "## Planner proposal",
            "",
            (
                f"Solver: {proposal['solver']}; status: {proposal['status']}; "
                f"elapsed: {proposal['elapsed_ms']} ms. Optimality: {proposal['optimality']}."
            ),
            "A feasible proposal still requires fresh inventory, budget reservation and durable create intent.",
        ]
    if "allocation_observation" in record:
        observation = record["allocation_observation"]
        lines += [
            "",
            "## Allocation observation",
            "",
            "Outcome: " + observation["outcome"],
            "Request elapsed seconds: " + str(observation["request_elapsed_seconds"]),
            "Search to acknowledgement seconds: "
            + str(observation["search_to_acknowledgement_seconds"]),
            "Authenticated readiness after intent seconds: "
            + str(observation["ready_after_intent_seconds"]),
            "One attempt is not a measured success rate. Acknowledgement does not establish worker readiness or acceptance.",
        ]
    diagnostic = record.get("provider_observations", {}).get("cuda_diagnostics")
    if diagnostic:
        lines += [
            "",
            "## CUDA filter comparison (diagnostic only)",
            "",
            "Status: " + diagnostic["status"],
            "",
            "| GPU | Exact CUDA | Minimum CUDA | No CUDA filter | Inference |",
            "|---|---|---|---|---|",
        ]
        for comparison in diagnostic["comparisons"]:
            signals = comparison["signals"]
            cells = [
                comparison["gpu_id"],
                str(signals.get("exact_cuda")),
                str(signals.get("minimum_cuda")),
                str(signals.get("no_cuda_filter")),
                comparison["inference"],
            ]
            lines.append(
                "| " + " | ".join(x.replace("|", "\\|").replace("\n", " ") for x in cells) + " |"
            )
        lines += [
            "",
            "These are overall catalog signals, not placement admission. Missing or failed probes are unknown.",
            "Sequential queries can observe capacity changes; the comparison does not prove causation or authorize execution.",
        ]
    if "provider_observations" in record:
        lines += [
            "",
            "## Read-only observations",
            "",
            "```json",
            json.dumps(record["provider_observations"], indent=2),
            "```",
            "",
            "Preflight signals are not reservations or runtime GPU verification; see the separate execution facts.",
        ]
    lines += [
        "",
        "## Budget",
        "",
        "```json",
        json.dumps(record["budget"], indent=2),
        "```",
        "",
        "Costs are estimates. Supplier invoice is unavailable; no provider hard cap is claimed.",
        "",
        "Campaign accounting (estimates, never an invoice):",
        "```json",
        json.dumps(record.get("campaign"), indent=2),
        "```",
        "",
        "## Recovery",
        "",
        "Errors: " + json.dumps(record.get("errors", [])),
        "",
        "Create HTTP failure (status is retained; response origin/cause remain unverified):",
        "```json",
        json.dumps(record.get("create_http_failure"), indent=2),
        "```",
        "",
        "Result HTTP failures (response origin is unverified):",
        "```json",
        json.dumps(record.get("result_http_failures", []), indent=2),
        "```",
        "",
        "Startup observations (Pod RUNNING is not worker readiness):",
        "```json",
        json.dumps(record.get("startup_diagnostics"), indent=2),
        "```",
        "",
        "Collector proxy configuration (presence only, not proof of the effective route):",
        "```json",
        json.dumps(record.get("collector_proxy_environment"), indent=2),
        "```",
        "Header hints are untrusted observations, not authenticated response-origin evidence.",
        "",
        "Bounded startup log hints (raw logs are never retained):",
        "```json",
        json.dumps(record.get("startup_logs"), indent=2),
        "```",
        "",
        f"Artifact status: `{record.get('artifact_status', 'not_collected')}`.",
        f"Cleanup: `{record.get('cleanup_verification', 'not_applicable')}`.",
        "Mock cleanup confirms only fake inventory. Read-only reports never imply deletion.",
        "",
    ]
    (destination / "summary.md").write_text("\n".join(lines))
