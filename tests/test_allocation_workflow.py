import json
from pathlib import Path

import pytest
import yaml

from runpod_ci_broker import allocation_workflow as entry
from runpod_ci_broker.handover import GitHubEvidence, context
from runpod_ci_broker.planner import estimate
from runpod_ci_broker.worker import SUPPORTED_GPUS


def spec():
    return entry.Manifest.model_validate_json(entry.MANIFEST.read_text())


def test_reviewed_manifest_has_global_gpu_set_and_preserves_old_budget():
    s = spec()
    s.check()
    assert not s.task.data_centers
    assert set(s.task.gpu_allowlist) == SUPPORTED_GPUS
    assert s.task.gpu_preference[0].endswith("4090")
    b = estimate(s.task)
    assert b["estimated_total_usd"] == 10.002002
    assert b["seconds_per_attempt"] * 3 + 3600 < 18000


@pytest.mark.parametrize(
    "field,value",
    [
        ("prior_spend_usd", 0.0),
        ("total_budget_usd", 21.0),
        ("max_start_attempts", 5),
        ("max_gpu_hourly_usd", 4.0),
    ],
)
def test_manifest_cannot_reset_or_expand_campaign(field, value):
    s = spec()
    setattr(s.task.limits, field, value)
    with pytest.raises(ValueError, match="manifest"):
        s.check()


@pytest.mark.parametrize("delta", [0, 599, 1801, float("nan")])
def test_expired_or_excessive_window_is_refused(delta):
    with pytest.raises(ValueError):
        entry.permit_for(spec().task, {"sha": "a" * 40}, 1000 + delta, 1000)


def test_new_workflows_are_main_manual_only_and_share_legacy_mutex(monkeypatch):
    for name in ["allocation-smoke.yml", "recover-allocation.yml"]:
        text = Path(".github/workflows", name).read_text()
        w = yaml.load(text, Loader=yaml.BaseLoader)
        assert set(w["on"]) <= {"workflow_dispatch", "workflow_run"}
        assert w["concurrency"]["group"] == "broker-approved-campaign"
        assert w["concurrency"]["cancel-in-progress"] == "false"
        job = w["jobs"]["controller"]
        assert "github.ref == 'refs/heads/main'" in job["if"]
        cleanup = next(
            s for s in job["steps"] if s.get("name") == "Terminate owned Pod and verify absence"
        )
        assert cleanup["if"] == "always()"
        assert job["timeout-minutes"] == "25"
        if name.startswith("recover"):
            assert "allocation_workflow start" not in text
            assert w["on"]["workflow_run"]["workflows"] == ["Bounded allocation GPU smoke"]
            collect = next(
                s for s in job["steps"] if "allocation_workflow collect" in s.get("run", "")
            )
            assert collect["if"] == "always() && github.event_name == 'workflow_run'"
        monkeypatch.setenv("GITHUB_REPOSITORY", entry.REPOSITORY)
        monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
        monkeypatch.setenv("GITHUB_SHA", "a" * 40)
        monkeypatch.setenv("GITHUB_RUN_ID", "123")
        monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
        monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
        monkeypatch.setenv(
            "GITHUB_WORKFLOW_REF", f"{entry.REPOSITORY}/.github/workflows/{name}@refs/heads/main"
        )
        assert context()["path"].endswith(name)
    monkeypatch.setenv("GITHUB_REF", "refs/heads/feature")
    with pytest.raises(ValueError):
        context()


@pytest.mark.parametrize(
    "error,expected",
    [
        (entry.LedgerError("ledger_permission_denied"), "ledger_permission_denied"),
        (entry.ProviderError("result_not_ready"), "result_not_ready"),
        (ValueError("secret_credentials"), "allocation_workflow_refused_or_failed"),
        (
            entry.LedgerError("unsafe response: token=secret"),
            "allocation_workflow_refused_or_failed",
        ),
    ],
)
def test_cli_exposes_only_safe_fixed_error_codes(monkeypatch, capsys, error, expected):
    import sys

    def fail():
        raise error

    monkeypatch.setattr(sys, "argv", ["allocation", "cleanup"])
    monkeypatch.setattr(entry, "context", fail)
    assert entry.main() == 2
    assert json.loads(capsys.readouterr().out)["error"] == expected


def test_recovery_checks_exact_original_dispatch_before_any_cloud_use(monkeypatch, tmp_path):
    original = {
        "actions_run_id": "123",
        "actions_run_attempt": 1,
        "allocation": {"permit": {"controller_sha": "a" * 40}},
    }
    github = GitHubEvidence()
    monkeypatch.setattr(github, "completed", lambda _: True)
    event = {
        "id": 123,
        "run_attempt": 1,
        "head_sha": "a" * 40,
        "path": entry.LAUNCH,
        "event": "workflow_dispatch",
        "head_repository": {"full_name": entry.REPOSITORY},
        "head_branch": "main",
    }
    p = tmp_path / "event.json"
    p.write_text(json.dumps({"workflow_run": event}))
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(p))
    entry.verify_recovery({"event": "workflow_run"}, original, github)
    event["id"] = 124
    p.write_text(json.dumps({"workflow_run": event}))
    with pytest.raises(ValueError, match="source_mismatch"):
        entry.verify_recovery({"event": "workflow_run"}, original, github)
    monkeypatch.setattr(github, "completed", lambda _: False)
    with pytest.raises(ValueError, match="not_completed"):
        entry.verify_recovery({"event": "workflow_run"}, original, github)


def test_cli_full_lifecycle_keeps_budget_and_export_identity(tmp_path, monkeypatch):
    import sys
    from datetime import UTC, datetime

    from test_allocation import IDENTITY, Cloud, export_fixture

    from runpod_ci_broker.allocation import BoundedController
    from runpod_ci_broker.ledger import SQLiteStore

    s = spec()
    s.enabled = True
    path = tmp_path / "manifest.json"
    path.write_text(s.model_dump_json())
    monkeypatch.setattr(entry, "MANIFEST", path)
    monkeypatch.setattr(entry, "OUTPUT", tmp_path / "out")
    monkeypatch.setattr(entry, "context", lambda: {**IDENTITY, "path": entry.LAUNCH})
    store = SQLiteStore(tmp_path / "ledger.db")
    monkeypatch.setattr(entry, "GitHubContentsStore", lambda _: store)
    checks = []
    monkeypatch.setattr(entry, "predecessor", lambda *args: checks.append("verified"))
    monkeypatch.setattr(entry, "allocation_predecessor", lambda *args: None)
    cloud = Cloud(s.task)
    monkeypatch.setenv("RUNPOD_API_KEY", "offline-fixture-not-a-credential")
    monkeypatch.setattr(entry.time, "time", cloud.now)

    class Dates:
        @staticmethod
        def now(tz):
            return datetime.fromtimestamp(cloud.now(), UTC)

    monkeypatch.setattr("runpod_ci_broker.provider.datetime", Dates)
    monkeypatch.setattr(entry, "LiveAPI", lambda **kwargs: cloud)
    monkeypatch.setattr(entry.Transport, "get", lambda self, p, q: cloud.read(p, q))
    controllers = []

    def build(*args, **kwargs):
        c = BoundedController(*args, **kwargs, now=cloud.now, sleep=cloud.sleep)
        controllers.append(c)
        return c

    monkeypatch.setattr(entry, "BoundedController", build)

    def run(action, *flags):
        monkeypatch.setattr(sys, "argv", ["allocation", action, *flags])
        return entry.main()

    end = str(cloud.now() + 1800)
    assert run("start", "--confirm-paid", "--window-end", end) == 0
    assert checks == ["verified"] and len(cloud.creates) == 1
    c = controllers[-1]
    github = export_fixture(c, cloud, monkeypatch)
    monkeypatch.setattr(entry, "GitHubEvidence", lambda: github)
    assert run("collect") == 0
    monkeypatch.setenv("RCB_ARTIFACT_ID", "123")
    monkeypatch.setenv("RCB_ARTIFACT_DIGEST", "d" * 64)
    assert run("receipt") == 0
    assert run("cleanup") == 0
    assert run("report") == 0
    assert c.j.read()["state"] == "ACCEPTED" and not cloud.pods
    assert run("start", "--confirm-paid", "--window-end", end) == 0
    assert len(cloud.creates) == 1


def test_disabled_release_never_constructs_cloud_client(tmp_path, monkeypatch):
    import sys

    from runpod_ci_broker.ledger import SQLiteStore

    s = spec()
    s.enabled = False
    p = tmp_path / "disabled.json"
    p.write_text(s.model_dump_json())
    monkeypatch.setattr(entry, "MANIFEST", p)
    monkeypatch.setattr(
        entry,
        "context",
        lambda: {
            "run_id": "123",
            "attempt": 1,
            "sha": "a" * 40,
            "path": entry.LAUNCH,
            "event": "workflow_dispatch",
        },
    )
    monkeypatch.setattr(entry, "GitHubContentsStore", lambda _: SQLiteStore(tmp_path / "ledger.db"))
    monkeypatch.setattr(entry, "LiveAPI", lambda **kwargs: pytest.fail("cloud must not be touched"))
    monkeypatch.setattr(
        sys, "argv", ["allocation", "start", "--confirm-paid", "--window-end", "1800001000"]
    )
    assert entry.main() == 2


def test_predecessor_budget_and_cleanup_are_verified_without_writing(monkeypatch):
    from runpod_ci_broker.campaign import record_hash

    r = {
        "state": "ACCEPTED",
        "facts": {
            "pod_deleted_confirmed": True,
            "experiment_passed": True,
            "results_saved": True,
            "task_completed": True,
        },
        "campaign": {"reserved_usd": 6.001002, "reserved_seconds": 3600},
    }
    scope = {
        "scope": entry.SCOPE,
        "holder": None,
        "controller": {},
        "records": {entry.REGRESSION: r, **{str(i): {} for i in range(5)}},
    }

    class Store:
        def read(self, key):
            return "a" * 40, scope

        def swap(self, *args):
            pytest.fail("historical scope must remain immutable")

    github = GitHubEvidence()
    monkeypatch.setattr(github, "completed", lambda _: True)
    monkeypatch.setattr(entry, "regression_parent", lambda _: None)
    monkeypatch.setattr(entry, "PRIOR_HASH", record_hash(r))
    entry.predecessor(Store(), github)
    r["campaign"]["reserved_usd"] = 0
    with pytest.raises(ValueError, match="unverified"):
        entry.predecessor(Store(), github)


def test_continuation_keeps_original_window_and_imports_only_verified_refusals(monkeypatch):
    from runpod_ci_broker.campaign import record_hash

    task = spec().task
    r = {
        "state": "NO_CAPACITY",
        "create_intent": False,
        "pod_id": None,
        "attempts": 3,
        "attempt_log": [{"outcome": "capacity_refused"}] * 3,
        "allocation": {"reserved_total_microusd": 8001502, "permit": {"window_end": 1794929408.0}},
        "resource_search": {
            "history": {
                "refusals": [
                    {
                        "gpu_id": "NVIDIA GeForce RTX 4090",
                        "data_center": "EU-RO-1",
                        "observed_at": 1794928000.0,
                        "reason": "definitive_capacity_refusal",
                    }
                ]
            }
        },
    }
    scope = {"holder": None, "controller": {}, "records": {entry.ALLOCATION_PARENT: r}}

    class Journal:
        def load(self):
            return "a" * 40, scope

    github = GitHubEvidence()
    monkeypatch.setattr(github, "completed", lambda _: True)
    monkeypatch.setattr(entry, "ALLOCATION_PARENT_HASH", record_hash(r))
    h = entry.allocation_predecessor(Journal(), task, github, 1794929408.0)
    assert h.config_fingerprint == task.fingerprint() and len(h.refusals) == 1
    with pytest.raises(ValueError, match="continuation"):
        entry.allocation_predecessor(Journal(), task, github, 1794929409.0)
    r["attempt_log"][0] = {"outcome": "unknown"}
    with pytest.raises(ValueError, match="continuation"):
        entry.allocation_predecessor(Journal(), task, github, 1794929408.0)
