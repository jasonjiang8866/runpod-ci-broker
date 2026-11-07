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
        f"Evidence mode: **{record['mode']}**. Real GPU execution: **not verified**.",
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
        decision = ", ".join(row["reasons"]) or "eligible; preference then price ranking"
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
            "Capacity signals are not reservations; image pull and runtime GPU checks remain unverified.",
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
        "## Recovery",
        "",
        f"Artifact status: `{record.get('artifact_status', 'not_collected')}`.",
        f"Cleanup: `{record.get('cleanup_verification', 'not_applicable')}`.",
        "Mock cleanup confirms only fake inventory. Read-only reports never imply deletion.",
        "",
    ]
    (destination / "summary.md").write_text("\n".join(lines))
