import json
import re
import subprocess
import sys
from pathlib import Path

import yaml

from runpod_ci_broker.fake import FakeProvider
from runpod_ci_broker.lifecycle import Engine
from runpod_ci_broker.storage import Journal


def cli(tmp_path, *args):
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "runpod_ci_broker.cli",
            "--config",
            "examples/simplexlora-flexible.json",
            "--output",
            str(tmp_path / "out"),
            *args,
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def test_default_is_offline_and_no_paid_launch(tmp_path, monkeypatch):
    monkeypatch.setenv("RUNPOD_API_KEY", "TEST_SECRET_MUST_NOT_APPEAR")
    result = cli(tmp_path)
    assert result.returncode == 0
    report = json.loads((tmp_path / "out/report.json").read_text())
    assert report["mode"] == "offline"
    assert "TEST_SECRET" not in json.dumps(report) + result.stdout + result.stderr
    result = cli(tmp_path, "--action", "run")
    assert result.returncode == 2
    assert "TEST_SECRET" not in result.stdout + result.stderr


def test_missing_credentials_still_produces_unknown_report(tmp_path, monkeypatch):
    monkeypatch.delenv("RUNPOD_API_KEY", raising=False)
    assert cli(tmp_path, "--action", "preflight").returncode == 2
    report = json.loads((tmp_path / "out/report.json").read_text())
    assert report["mode"] == "read_only"
    assert report["provider_observations"]["query_status"] == "credentials_missing"
    assert report["selected"] is None
    assert all(d["candidate"]["capacity"] == "unknown" for d in report["decisions"])


def test_simulator_never_opens_socket(tmp_path, task, candidates, auth, monkeypatch):
    import socket

    def deny(*args, **kwargs):
        raise AssertionError("Network forbidden in simulation")

    monkeypatch.setattr(socket, "create_connection", deny)
    p = FakeProvider(candidates)
    e = Engine(task, p, Journal(tmp_path, task.scope, task.experiment_id), tmp_path / "results")
    assert e.run(auth)["state"] == "ACCEPTED"


def test_workflows_no_pr_secrets_and_actions_pinned():
    for path in Path(".github/workflows").glob("*.yml"):
        text = path.read_text()
        # YAML 1.1 treats `on` as bool; BaseLoader preserves spelling for this structural check.
        workflow = yaml.load(text, Loader=yaml.BaseLoader)
        assert "pull_request_target" not in workflow["on"]
        if "pull_request" in workflow["on"]:
            assert "secrets." not in text and "RUNPOD_API_KEY" not in text
        assert workflow["permissions"] == {"contents": "read"}
        for job in workflow["jobs"].values():
            assert int(job["timeout-minutes"]) <= 15
            for step in job["steps"]:
                if "uses" in step:
                    assert re.search(r"@[a-f0-9]{40}$", step["uses"])
    read_only = yaml.load(
        Path(".github/workflows/read-only-preflight.yml").read_text(), Loader=yaml.BaseLoader
    )
    assert set(read_only["on"]) == {"workflow_dispatch"}
    assert "refs/heads/main" in read_only["jobs"]["preflight"]["if"]


def test_live_execution_disabled_even_with_approval_document(tmp_path, task, auth):
    path = tmp_path / "authorization.json"
    path.write_text(auth.model_dump_json())
    result = cli(tmp_path, "--action", "run", "--authorization", str(path))
    assert result.returncode == 2
    report = json.loads((tmp_path / "out/report.json").read_text())
    assert report["state"] == "EXECUTION_REFUSED"
    assert "live_execution_not_implemented" in report["blockers"]


def test_preflight_preserves_reports_on_malformed_provider_response(tmp_path, monkeypatch, capsys):
    from runpod_ci_broker.cli import main
    from runpod_ci_broker.provider import Transport

    def get(self, path, params):
        return {
            "/catalog/gpus": {
                "gpus": [
                    {
                        "id": "NVIDIA GeForce RTX 4090",
                        "price": {"secure": "TEST_SECRET_MUST_NOT_APPEAR"},
                    }
                ]
            },
            "/pods": {"pods": [], "pagination": {"hasNextPage": False}},
            "/catalog/datacenters": {"dataCenters": []},
        }[path]

    monkeypatch.setenv("RUNPOD_API_KEY", "TEST_SECRET_MUST_NOT_APPEAR")
    monkeypatch.setattr(Transport, "get", get)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "broker",
            "--config",
            "examples/simplexlora-flexible.json",
            "--action",
            "preflight",
            "--output",
            str(tmp_path),
        ],
    )
    assert main() == 2
    report = json.loads((tmp_path / "report.json").read_text())
    markdown = (tmp_path / "summary.md").read_text()
    assert report["mode"] == "read_only" and report["selected"] is None
    assert "query_status_unverified" in report["blockers"]
    assert "catalog_schema_invalid" in markdown
    assert "USD/GPU-hour" in markdown and "Read-only observations" in markdown
    assert "TEST_SECRET" not in json.dumps(report) + markdown + str(capsys.readouterr())
