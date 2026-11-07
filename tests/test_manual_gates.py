import json
from pathlib import Path

import yaml


def test_new_write_permissions_are_manual_main_only_and_no_cloud_key():
    for name, permission in [
        ("publish-worker.yml", "packages"),
        ("verify-shared-state.yml", "contents"),
    ]:
        text = Path(".github/workflows", name).read_text()
        workflow = yaml.load(text, Loader=yaml.BaseLoader)
        assert set(workflow["on"]) == {"workflow_dispatch"}
        assert workflow["permissions"] == {"contents": "read"}
        assert "RUNPOD_API_KEY" not in text
        job = next(iter(workflow["jobs"].values()))
        assert job["permissions"][permission] == "write"
        assert "github.ref == 'refs/heads/main'" in job["if"]
        assert "github.event_name == 'workflow_dispatch'" in job["if"]
        assert "inputs.confirm_" in job["if"]
        assert all(
            i["default"] == "false" for i in workflow["on"]["workflow_dispatch"]["inputs"].values()
        )


def test_budget_record_is_one_campaign_not_a_fresh_allowance_per_retry():
    d = json.loads(Path("docs/authorizations/broker-smoke-20261107.json").read_text())
    assert d["approved_total_budget_usd"] == 20
    assert d["approved_aggregate_seconds"] == d["max_elapsed_seconds"] == 18000
    assert d["max_gpu_hourly_usd"] == 3 and d["gpu_count"] == 1
    assert d["max_active_pods_in_scope"] == 1
    assert len(d["gpu_allowlist"]) == 4
    assert "failed_attempts" in d["included_costs"] and "cleanup" in d["included_costs"]
    assert d["launch_status"] == "not_started" and d["supplier_invoice_usd"] is None
