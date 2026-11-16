"""All cloud operations below are fakes. No real GPU or live deletion evidence."""

import copy
import hashlib
import io
import json
from pathlib import Path
from urllib.error import HTTPError

import pytest
import yaml

from runpod_ci_broker.campaign import CAMPAIGN, SCOPE, may_collect, observe, reserve, validate_task
from runpod_ci_broker.controller import Controller
from runpod_ci_broker.export import ResultHTTPError
from runpod_ci_broker.handover import ControllerJournal, GitHubEvidence, context
from runpod_ci_broker.ledger import LedgerError, SQLiteStore
from runpod_ci_broker.live import LiveAPI
from runpod_ci_broker.models import Candidate, Task
from runpod_ci_broker.provider import ProviderError
from runpod_ci_broker.report import write_report
from runpod_ci_broker.results import save_bundle

IDENTITY = {
    "run_id": "1234",
    "attempt": 1,
    "sha": "a" * 40,
    "path": ".github/workflows/gpu-smoke.yml",
    "event": "workflow_dispatch",
}


def test_real_collector_denial_keeps_safe_evidence_and_cleans(setup, monkeypatch, tmp_path):
    from email.message import Message

    controller, cloud, journal = setup
    controller.start(confirmed=True)
    cloud.pod["runtime"] = {"uptime": 1, "ip": "PRIVATE_SENTINEL"}
    monkeypatch.setenv("HTTPS_PROXY", "https://user:PRIVATE_SENTINEL@proxy.invalid")
    response = io.BytesIO(b"PRIVATE_SENTINEL")
    requests = []
    headers = Message()
    headers["Server"] = "cloudflare"
    headers["CF-Ray"] = "PRIVATE_SENTINEL"
    headers["Content-Type"] = "text/html"

    class Reader:
        def open(self, path, timeout):
            requests.append(path)
            raise HTTPError("https://PRIVATE_SENTINEL", 403, "PRIVATE_SENTINEL", headers, response)

    monkeypatch.setattr("runpod_ci_broker.controller.RunPodResultReader", lambda *args: Reader())
    result = controller.collect()
    assert result["state"] == "REJECTED"
    assert result["facts"]["pod_deleted_confirmed"]
    assert not result["facts"]["results_saved"]
    assert requests == ["/v1/manifest"] and response.closed
    assert result["collector_proxy_environment"]["https_proxy_configured"] is True
    sample = result["startup_diagnostics"]["samples"][0]
    assert sample["result_port_configured"] is True
    assert sample["pod_status"] == "RUNNING"
    assert sample["worker_readiness"] == "unverified"
    assert result["result_http_failures"][0]["header_hints"]["server_family"] == "cloudflare"
    assert result["result_http_failures"][0]["response_origin"] == "unverified"
    write_report(journal.read(), tmp_path / "report")
    for name in ["summary.md", "report.json"]:
        assert "PRIVATE_SENTINEL" not in (tmp_path / "report" / name).read_text()
    assert "Startup observations" in (tmp_path / "report" / "summary.md").read_text()
    controller.start(confirmed=True)
    assert cloud.creates == 1


def test_result_403_preserves_diagnostics_without_retry_or_second_create(
    setup, monkeypatch, tmp_path
):
    controller, cloud, journal = setup
    started = controller.start(confirmed=True)
    assert "No GPU was executed" not in started["notice"]

    def denied(*args, **kwargs):
        raise ResultHTTPError(403, "manifest")

    monkeypatch.setattr("runpod_ci_broker.controller.collect_https", denied)
    result = controller.collect()
    assert result["state"] == "REJECTED"
    assert result["facts"]["pod_deleted_confirmed"]
    assert not result["facts"]["results_saved"]
    assert result["collect_attempts"] == 1
    assert result["result_http_failures"] == [
        {
            "code": "result_permission_denied",
            "http_status": 403,
            "stage": "manifest",
            "response_origin": "unverified",
        }
    ]
    controller.start(confirmed=True)
    assert cloud.creates == 1
    assert cloud.log_calls == [("abc123", 3)]
    assert result["startup_logs"]["signals"] == ["worker_refusal_hint"]
    write_report(journal.read(), tmp_path / "report")
    summary = (tmp_path / "report" / "summary.md").read_text()
    assert "result_permission_denied" in summary and '"http_status": 403' in summary
    assert "not verified" in summary


@pytest.mark.parametrize("diagnostic_failure", [True, False])
def test_failed_startup_diagnostic_cannot_block_cleanup(setup, monkeypatch, diagnostic_failure):
    controller, cloud, _ = setup
    controller.start(confirmed=True)

    def denied(*args, **kwargs):
        raise ResultHTTPError(403, "manifest")

    def logs(*args):
        cloud.log_calls.append(args)
        if diagnostic_failure:
            raise ProviderError("SECRET")
        return {"status": "http_error", "http_status": 403, "root_cause": "unverified"}

    cloud.startup_logs = logs
    monkeypatch.setattr("runpod_ci_broker.controller.collect_https", denied)
    result = controller.collect()
    assert result["facts"]["pod_deleted_confirmed"] and cloud.deletes == ["abc123"]
    assert result["collect_attempts"] == 1 and len(cloud.log_calls) == 1
    assert "SECRET" not in json.dumps(result)


def test_diagnostics_skip_near_deadline_and_preserve_original_budget(setup, monkeypatch):
    controller, cloud, journal = setup
    started = controller.start(confirmed=True)
    original_campaign = copy.deepcopy(started["campaign"])
    cloud.clock = original_campaign["collection_deadline"] - 2
    monkeypatch.setattr(
        "runpod_ci_broker.controller.collect_https",
        lambda *a, **kw: (_ for _ in ()).throw(ResultHTTPError(403, "manifest")),
    )
    result = controller.collect()
    assert result["startup_logs"]["status"] == "skipped_deadline"
    assert not cloud.log_calls and result["facts"]["pod_deleted_confirmed"]
    assert result["campaign"]["wall_deadline"] == original_campaign["wall_deadline"]
    assert result["campaign"]["reserved_usd"] == original_campaign["reserved_usd"]
    assert journal.read()["attempts"] == 1


def test_diagnostic_intent_survives_lost_runner_without_second_read(setup, monkeypatch):
    controller, cloud, journal = setup
    controller.start(confirmed=True)
    r = journal.read()
    r["startup_logs"] = {"status": "attempted_unconfirmed", "root_cause": "unverified"}
    with journal.lock():
        journal.write(r)
    monkeypatch.setattr(
        "runpod_ci_broker.controller.collect_https",
        lambda *a, **kw: (_ for _ in ()).throw(ResultHTTPError(403, "manifest")),
    )
    result = controller.collect()
    assert not cloud.log_calls and result["facts"]["pod_deleted_confirmed"]


def test_foreign_pod_never_reads_startup_logs(setup, monkeypatch):
    controller, cloud, _ = setup
    controller.start(confirmed=True)
    cloud.pod["env"] = {}
    result = controller.collect()
    assert not cloud.log_calls and not cloud.deletes
    assert result["cleanup_verification"] != "direct_404_and_complete_inventory_twice"


class FakeCloud:
    def __init__(self):
        self.pod = None
        self.creates = 0
        self.deletes = []
        self.lost = False
        self.clock = 1000.0
        self.log_calls = []

    def startup_logs(self, pod_id, timeout):
        self.log_calls.append((pod_id, timeout))
        return {"status": "eof", "signals": ["worker_refusal_hint"], "root_cause": "unverified"}

    def now(self):
        return self.clock

    def sleep(self, seconds):
        self.clock += seconds

    def inventory(self):
        return [copy.deepcopy(self.pod)] if self.pod else []

    def create(self, body):
        self.creates += 1
        self.pod = {
            **copy.deepcopy(body),
            "id": "abc123",
            "status": "RUNNING",
            "cost": 0.5,
            "dataCenterId": body["dataCenterIds"][0],
        }
        if self.lost:
            raise ProviderError("create_response_unknown", ambiguous=True)
        return "abc123"

    def get(self, pod_id, timeout):
        return copy.deepcopy(self.pod)

    def delete(self, pod_id, timeout):
        self.deletes.append(pod_id)
        self.pod = None

    def active_ids(self, deadline):
        return {"abc123"} if self.pod else set()


@pytest.fixture
def smoke():
    return Task.model_validate_json(Path("examples/approved-broker-smoke.json").read_text())


@pytest.fixture
def setup(tmp_path, smoke, monkeypatch):
    cloud = FakeCloud()
    journal = ControllerJournal(
        SQLiteStore(tmp_path / "state.db"), SCOPE, CAMPAIGN, IDENTITY, lambda _: False
    )

    def snapshot(task):
        return [
            Candidate(
                gpu_id=g,
                data_center="EU-RO-1",
                memory_gb=32,
                hourly_usd=0.5,
                capacity="available",
                cuda_versions=["12.9"],
                evidence="read_only",
            )
            for g in task.gpu_allowlist
        ], {
            "inventory": "complete",
            "query_status": "ok",
            "scope_active_pod_count": 0,
            "datacenter_status": "ok",
        }

    c = Controller(
        smoke, journal, cloud, snapshot, tmp_path / "export", now=cloud.now, sleep=cloud.sleep
    )
    # Keep cleanup's bounded waits virtual, while exercising its real ownership logic.
    from runpod_ci_broker.cleanup import cleanup_owned

    monkeypatch.setattr(
        "runpod_ci_broker.controller.cleanup_owned",
        lambda *a, **kw: cleanup_owned(*a, **kw, now=cloud.now, sleep=cloud.sleep),
    )
    return c, cloud, journal


def bundle(task, pod_id, passed=True):
    metrics = {"gpu_memory_smoke_passed": int(passed), "worker_exit_code": 0 if passed else 2}
    observation = {
        "device_name": "NVIDIA GeForce RTX 3090",
        "device_uuid": "a" * 8 + "-" + "a" * 4 + "-" + "a" * 4 + "-" + "a" * 4 + "-" + "a" * 12,
        "device_count": 1,
        "memory_bytes": 24 * 1024**3,
        "driver_cuda_version": 13000,
        "architecture": "sm_86",
        "memory_roundtrip_bytes": 4096,
        "memory_roundtrip_passed": passed,
        "probe": "cuda_driver_memory_roundtrip_v1",
        "framework_runtime": "not_tested",
    }
    files = {
        "metrics.json": json.dumps(metrics).encode(),
        "worker.log": b"fixed smoke fixture\n",
        "environment.json": json.dumps({"observed": observation}).encode(),
    }
    manifest = {
        "config_fingerprint": task.fingerprint(),
        "source_sha": task.source_sha,
        "image": task.image,
        "pod_id": pod_id,
        "metrics": metrics,
        "files": {
            k: {"bytes": len(v), "sha256": hashlib.sha256(v).hexdigest()} for k, v in files.items()
        },
    }
    return manifest, files


def exporter(monkeypatch, *, passed=True, fail=0):
    calls = []

    def collect(task, pod_id, reader, dest, **kwargs):
        calls.append(reader)
        if len(calls) <= fail:
            raise ProviderError("result_transport_failed", retryable=True)
        manifest, files = bundle(task, pod_id, passed)
        return save_bundle(task, pod_id, manifest, files, dest)

    monkeypatch.setattr("runpod_ci_broker.controller.collect_https", collect)
    return calls


def receipt(c, monkeypatch, *, mismatch=False):
    meta = {
        "id": 99,
        "expired": False,
        "digest": "sha256:" + "d" * 64,
        "name": "smoke-bundle-1234-1",
        "workflow_run": {"id": 1234, "head_sha": "b" * 40 if mismatch else "a" * 40},
    }
    github = GitHubEvidence()
    monkeypatch.setattr(github, "get", lambda _: meta)
    return c.receipt("99", "d" * 64, github)


def test_lifecycle_receipt_and_cleanup_are_separate_facts(setup, monkeypatch):
    c, cloud, _j = setup
    exporter(monkeypatch)
    assert c.start(confirmed=True)["state"] == "STARTING"
    r = c.collect()
    assert r["gpu_verified"] and not r["facts"]["results_saved"]
    assert not r["facts"]["task_completed"] and cloud.pod is not None
    r = receipt(c, monkeypatch)
    assert r["facts"]["results_saved"] and not r["facts"]["pod_deleted_confirmed"]
    r = c.cleanup()
    assert r["state"] == "ACCEPTED" and all(r["facts"].values())
    assert cloud.creates == 1 and cloud.deletes == ["abc123"]
    c.start(confirmed=True)
    assert cloud.creates == 1
    assert r["campaign"]["supplier_bill_usd"] is None


def test_missing_authorization_creates_nothing(setup):
    c, cloud, j = setup
    with pytest.raises(ValueError, match="confirmation"):
        c.start()
    assert cloud.creates == 0 and j.read() is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("total_budget_usd", None),
        ("total_budget_usd", 21.0),
        ("runtime_seconds", None),
        ("max_gpu_hourly_usd", 4.0),
        ("storage_hourly_usd", 200.0),
        ("max_start_attempts", 2),
    ],
)
def test_unapproved_or_insufficient_budget_refused(smoke, field, value):
    setattr(smoke.limits, field, value)
    with pytest.raises(ValueError):
        validate_task(smoke)


def test_unknown_create_is_adopted_not_recreated(setup, monkeypatch):
    c, cloud, _j = setup
    cloud.lost = True
    assert c.start(confirmed=True)["state"] == "CREATE_UNKNOWN"
    c.start(confirmed=True)
    exporter(monkeypatch)
    assert c.collect()["pod_id"] == "abc123"
    assert cloud.creates == 1
    assert c.cleanup()["facts"]["pod_deleted_confirmed"]


def test_unknown_create_empty_inventory_remains_unresolved(setup):
    c, cloud, _j = setup
    cloud.lost = True
    c.start(confirmed=True)
    cloud.pod = None
    r = c.cleanup()
    assert not r["facts"]["pod_deleted_confirmed"]
    assert r["cleanup_verification"] == "creation_unresolved"
    c.start(confirmed=True)
    assert cloud.creates == 1


def test_create_intent_checkpoint_failure_cannot_issue_post(setup, monkeypatch):
    c, cloud, j = setup
    original = j.write

    def write(r):
        if r.get("create_intent"):
            raise LedgerError("ledger_write_unconfirmed")
        original(r)

    monkeypatch.setattr(j, "write", write)
    with pytest.raises(LedgerError):
        c.start(confirmed=True)
    assert cloud.creates == 0


@pytest.mark.parametrize(
    "query_failure,expected", [(False, "NO_CAPACITY"), (True, "PREFLIGHT_BLOCKED")]
)
def test_query_failure_not_no_capacity(setup, query_failure, expected):
    c, cloud, _j = setup
    old = c.snapshot

    def snapshot(task):
        candidates, info = old(task)
        for item in candidates:
            item.capacity = "none"
        if query_failure:
            info["query_status"] = "query_failed"
        return candidates, info

    c.snapshot = snapshot
    assert c.start(confirmed=True)["state"] == expected
    assert cloud.creates == 0


def test_foreign_scope_resource_blocks_create(setup):
    c, cloud, _j = setup
    cloud.pod = {"name": "foreign", "env": {"RCB_TOOL": "runpod-ci-broker/v1", "RCB_SCOPE": SCOPE}}
    with pytest.raises(ProviderError, match="existing_scope"):
        c.start(confirmed=True)
    assert cloud.creates == 0 and not cloud.deletes


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "foreign"),
        ("image", "foreign"),
        ("env", {}),
        ("mounts", {"global": [{"volumeId": "user"}]}),
    ],
)
def test_recovery_refuses_foreign_or_changed_resource(setup, field, value):
    c, cloud, _j = setup
    c.start(confirmed=True)
    cloud.pod[field] = value
    assert c.cleanup()["state"] == "CLEANUP_PENDING"
    assert not cloud.deletes


def test_capability_recovery_and_no_plaintext_in_state_or_report(setup, monkeypatch):
    c, cloud, j = setup
    c.start(confirmed=True)
    token = cloud.pod["env"]["RCB_RESULT_TOKEN"]
    assert token not in json.dumps(j.load()[1])
    assert "RUNPOD_API_KEY" not in cloud.pod["env"]
    assert "RCB_LEDGER_TOKEN" not in cloud.pod["env"]
    calls = exporter(monkeypatch)
    c.collect()
    assert calls[0]._token == token  # privately recovered from owned Pod
    assert token not in json.dumps(j.read())


def test_collection_and_delete_failures_can_resume_without_new_create(setup, monkeypatch):
    c, cloud, _j = setup
    c.start(confirmed=True)
    exporter(monkeypatch, fail=1)
    assert c.collect()["state"] == "COLLECTED"
    original = cloud.delete

    def unavailable(*a):
        raise ProviderError("cleanup_request_unknown", retryable=True)

    cloud.delete = unavailable
    assert not c.cleanup()["facts"]["pod_deleted_confirmed"]
    cloud.delete = original
    assert c.cleanup()["facts"]["pod_deleted_confirmed"]
    assert cloud.creates == 1


def test_upload_failure_does_not_keep_gpu_or_claim_saved(setup, monkeypatch):
    c, _cloud, _j = setup
    c.start(confirmed=True)
    exporter(monkeypatch)
    c.collect()
    with pytest.raises(ValueError, match="receipt_unverified"):
        receipt(c, monkeypatch, mismatch=True)
    r = c.cleanup()
    assert r["state"] == "REJECTED" and r["facts"]["pod_deleted_confirmed"]
    assert not r["facts"]["results_saved"]


def test_expired_budget_skips_collection_and_still_cleans(setup, monkeypatch):
    c, cloud, _j = setup
    c.start(confirmed=True)
    cloud.clock += 18001
    calls = exporter(monkeypatch)
    r = c.collect()
    assert r["facts"]["pod_deleted_confirmed"] and not calls
    assert any(e["to"] == "TIMED_OUT" for e in r["events"])
    r = c.cleanup()
    assert r["facts"]["pod_deleted_confirmed"]
    assert r["campaign"]["aggregate_resource_seconds"] >= 18001
    assert r["campaign"]["first_intent_at"] == 1000


def test_observed_overprice_immediately_abandons_work(setup, monkeypatch):
    c, cloud, _j = setup
    c.start(confirmed=True)
    cloud.pod["cost"] = 3.1
    calls = exporter(monkeypatch)
    r = c.collect()
    assert r["facts"]["pod_deleted_confirmed"] and not calls
    assert any(e["to"] == "TIMED_OUT" for e in r["events"])
    assert c.cleanup()["facts"]["pod_deleted_confirmed"]


def test_crash_after_local_export_before_checkpoint_recovers(setup, monkeypatch):
    c, _cloud, _j = setup
    c.start(confirmed=True)
    manifest, files = bundle(c.task, "abc123")
    save_bundle(c.task, "abc123", manifest, files, c.output / "bundle")
    assert c.collect()["manifest_sha256"]
    assert receipt(c, monkeypatch)["facts"]["results_saved"]


def test_campaign_reservation_cannot_reset(smoke):
    r = {"attempts": 0}
    reserve(r, smoke, 1000)
    assert 1 <= r["campaign"]["reserved_usd"] < 1.01
    with pytest.raises(ValueError):
        reserve(r, smoke, 2000)
    observe(r, smoke, 1300)
    with pytest.raises(ValueError, match="clock_regressed"):
        observe(r, smoke, 1299)
    assert not may_collect(r, smoke, 1420)


def test_takeover_requires_completed_exact_attempt_and_preserves_intent(setup):
    c, _cloud, j = setup
    c.start(confirmed=True)
    version, value = j.load()
    value["holder"] = "f" * 32
    j.store.swap(j.key, version, value)
    newer = {**IDENTITY, "run_id": "9999", "path": ".github/workflows/recover-smoke.yml"}
    checked = []
    recover = ControllerJournal(
        j.store, SCOPE, CAMPAIGN, newer, lambda ctx: checked.append(ctx) or False, recovery=True
    )
    with pytest.raises(LedgerError, match="not_confirmed"), recover.lock():
        pass
    assert recover.load()[1]["holder"] == "f" * 32
    recover.completed = lambda ctx: ctx == IDENTITY
    with recover.lock():
        assert recover.read()["create_intent"]
        assert recover.read()["attempts"] == 1
    assert recover.load()[1]["holder"] is None and len(checked) == 1
    assert recover.load()[1]["handovers"][0]["from"] == IDENTITY


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "in_progress"),
        ("run_attempt", 2),
        ("head_sha", "b" * 40),
        ("head_branch", "pr"),
        ("event", "pull_request"),
        ("path", "foreign.yml"),
    ],
)
def test_completion_proof_rejects_wrong_run_fields(monkeypatch, field, value):
    data = {
        "id": 1234,
        "run_attempt": 1,
        "status": "completed",
        "head_sha": "a" * 40,
        "head_branch": "main",
        "event": "workflow_dispatch",
        "path": IDENTITY["path"],
        "repository": {"full_name": "jasonjiang8866/runpod-ci-broker"},
        "head_repository": {"full_name": "jasonjiang8866/runpod-ci-broker"},
    }
    g = GitHubEvidence()
    monkeypatch.setattr(g, "get", lambda _: data)
    assert g.completed(IDENTITY)
    data[field] = value
    assert not g.completed(IDENTITY)


def test_fork_and_nonmain_context_refused(monkeypatch):
    for k, v in {
        "GITHUB_REPOSITORY": "jasonjiang8866/runpod-ci-broker",
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_SHA": "a" * 40,
        "GITHUB_RUN_ID": "1234",
        "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_WORKFLOW_REF": "jasonjiang8866/runpod-ci-broker/.github/workflows/gpu-smoke.yml@refs/heads/main",
    }.items():
        monkeypatch.setenv(k, v)
    assert context() == IDENTITY
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    with pytest.raises(LedgerError):
        context()


@pytest.mark.parametrize(
    "status,ambiguous",
    [(401, False), (403, False), (402, False), (422, False), (429, True), (500, True), (503, True)],
)
def test_create_api_no_retry_and_sanitized_unknown(monkeypatch, status, ambiguous):
    monkeypatch.setenv("RUNPOD_API_KEY", "account-secret")
    calls = []

    class Opener:
        def open(self, *a, **kw):
            calls.append(a)
            raise HTTPError("secret-url", status, "secret-response", {}, io.BytesIO(b"secret"))

    monkeypatch.setattr("runpod_ci_broker.live.build_opener", lambda *a: Opener())
    with pytest.raises(ProviderError) as error:
        LiveAPI(allow_create=True).create({})
    assert error.value.ambiguous is ambiguous and len(calls) == 1
    assert "secret" not in str(error.value)


def test_paid_and_recovery_workflows_have_no_pr_secret_path():
    for name in ["gpu-smoke.yml", "recover-smoke.yml"]:
        text = Path(".github/workflows", name).read_text()
        w = yaml.load(text, Loader=yaml.BaseLoader)
        assert not {"pull_request", "pull_request_target", "push", "schedule"} & set(w["on"])
        job = w["jobs"]["controller"]
        assert job["timeout-minutes"] == "12"
        assert job["permissions"] == {"contents": "write", "actions": "read"}
        assert w["concurrency"]["cancel-in-progress"] == "false"
        assert "RUNPOD_API_KEY" not in job["env"]
        assert all(
            step.get("with", {}).get("ref", "${{ github.sha }}") == "${{ github.sha }}"
            for step in job["steps"]
        )
        if name.startswith("recover"):
            assert "start --confirm-paid" not in text
            assert w["on"]["workflow_run"]["types"] == ["completed"]
            assert "workflow_run.event == 'workflow_dispatch'" in job["if"]


def test_duplicate_owned_resources_are_not_deleted(setup):
    c, cloud, _j = setup
    cloud.lost = True
    c.start(confirmed=True)
    cloud.inventory = lambda: [
        copy.deepcopy(cloud.pod),
        {**copy.deepcopy(cloud.pod), "id": "second"},
    ]
    assert c.cleanup()["state"] == "CLEANUP_PENDING"
    assert not cloud.deletes


def test_wrong_capability_cleans_without_export(setup, monkeypatch):
    c, cloud, _j = setup
    c.start(confirmed=True)
    cloud.pod["env"]["RCB_RESULT_TOKEN"] = "0" * 64
    calls = exporter(monkeypatch)
    r = c.collect()
    assert r["facts"]["pod_deleted_confirmed"] and not calls
    assert "result_capability_unrecoverable" in r["errors"]


def test_primary_controller_cannot_take_over_even_completed_lock(setup):
    _c, _cloud, j = setup
    version, value = j.load()
    value["holder"], value["controller"] = "b" * 32, IDENTITY
    j.store.swap(j.key, version, value)
    j.completed = lambda _: True
    with pytest.raises(LedgerError, match="manual_recovery"), j.lock():
        pass


def test_live_create_success_and_malformed_response_are_distinct(monkeypatch):
    monkeypatch.setenv("RUNPOD_API_KEY", "account-secret")

    class Response(io.BytesIO):
        status = 201

    bodies = [b'{"id":"pod_abc123"}', b'{"unexpected":"secret"}']

    class Opener:
        def open(self, request, **kwargs):
            assert request.method == "POST"
            assert request.full_url == "https://api.runpod.io/v2/pods"
            return Response(bodies.pop(0))

    monkeypatch.setattr("runpod_ci_broker.live.build_opener", lambda *a: Opener())
    assert LiveAPI(allow_create=True).create({}) == "pod_abc123"
    with pytest.raises(ProviderError) as error:
        LiveAPI(allow_create=True).create({})
    assert error.value.ambiguous
    with pytest.raises(ProviderError, match="not_enabled"):
        LiveAPI().create({})


def test_slow_stream_deadline_does_not_wait_for_whole_api_body(monkeypatch):
    from runpod_ci_broker.provider import bounded_body

    clock = [0.0]
    monkeypatch.setattr("runpod_ci_broker.provider.time.monotonic", lambda: clock[0])

    class Slow:
        def read(self, size):
            clock[0] += 0.6
            return b"x"

    with pytest.raises(TimeoutError):
        bounded_body(Slow(), 1000000, 1.0)
    assert clock[0] < 2


def test_oversized_create_body_response_keeps_unknown_intent(monkeypatch):
    monkeypatch.setenv("RUNPOD_API_KEY", "secret")

    class Response(io.BytesIO):
        status = 201

    class Opener:
        def open(self, *a, **kw):
            return Response(b"x" * 1000001)

    monkeypatch.setattr("runpod_ci_broker.live.build_opener", lambda *a: Opener())
    with pytest.raises(ProviderError) as error:
        LiveAPI(allow_create=True).create({})
    assert error.value.ambiguous


@pytest.fixture
def retry_setup(setup, monkeypatch):
    from runpod_ci_broker.campaign import DIAGNOSTIC_RETRY

    c, cloud, journal = setup
    c.start(confirmed=True)
    monkeypatch.setattr(
        "runpod_ci_broker.controller.collect_https",
        lambda *a, **kw: (_ for _ in ()).throw(ResultHTTPError(403, "manifest")),
    )
    c.collect()
    previous = journal.read()
    cloud.clock = previous["campaign"]["wall_deadline"] + 86400
    identity = {**IDENTITY, "run_id": "5678"}
    successor = ControllerJournal(journal.store, SCOPE, DIAGNOSTIC_RETRY, identity, lambda _: True)
    task = Task.model_validate_json(Path("examples/diagnostic-retry-smoke.json").read_text())
    retry = Controller(
        task, successor, cloud, c.snapshot, c.output, now=cloud.now, sleep=cloud.sleep
    )
    return retry, cloud, journal, previous


def test_diagnostic_retry_preserves_parent_budget_and_stops_after_second_denial(retry_setup):
    retry, cloud, original, previous = retry_setup
    result = retry.start(confirmed=True, window_end=cloud.clock + 3600)
    assert cloud.creates == 2
    assert result["selected"]["gpu_id"] == "NVIDIA GeForce RTX 4090"
    assert result["name"] != previous["name"]
    assert result["campaign"]["id"] == CAMPAIGN
    assert result["campaign"]["reserved_usd"] == 2.000334
    assert result["campaign"]["reserved_seconds"] == 1200
    assert result["campaign"]["predecessor"]["reserved_usd"] == 1.000167
    assert (
        result["campaign"]["predecessor"]["record_sha256"]
        == hashlib.sha256(
            json.dumps(previous, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
    )
    cloud.clock += 10
    rejected = retry.collect()
    assert rejected["facts"]["pod_deleted_confirmed"]
    assert rejected["state"] == "REJECTED"
    assert rejected["collect_attempts"] == 1
    assert rejected["campaign"]["estimated_elapsed_cost_usd"] >= 1.500167
    assert rejected["campaign"]["aggregate_resource_seconds"] >= (
        previous["campaign"]["aggregate_resource_seconds"] + 10
    )
    assert original.read() == previous
    retry.start(confirmed=True, window_end=cloud.clock + 3600)
    assert cloud.creates == 2  # a later window never grants a third attempt
    assert original.read() == previous


@pytest.mark.parametrize("window", [None, -1, 0, 599, 3601, float("nan"), float("inf")])
def test_retry_requires_explicit_bounded_window(retry_setup, window):
    retry, cloud, original, previous = retry_setup
    end = None if window is None else cloud.clock + window
    with pytest.raises(ValueError, match="explicit_retry_execution_window_required"):
        retry.start(confirmed=True, window_end=end)
    assert cloud.creates == 1 and retry.j.read() is None
    assert original.read() == previous


@pytest.mark.parametrize(
    "field,value",
    [
        ("reserved_usd", 1.1),
        ("reserved_seconds", 601),
        ("aggregate_resource_seconds", 601),
        ("estimated_elapsed_cost_usd", 1.1),
    ],
)
def test_retry_refuses_changed_accounting(retry_setup, field, value):
    retry, cloud, original, previous = retry_setup
    previous["campaign"][field] = value
    with original.lock():
        original.write(previous)
    with pytest.raises(ValueError, match="retry_parent_not_eligible"):
        retry.start(confirmed=True, window_end=cloud.clock + 3600)
    assert cloud.creates == 1 and retry.j.read() is None


@pytest.mark.parametrize("which", ["absent", "unresolved", "accepted", "no_completion", "recovery"])
def test_retry_rejects_unverified_predecessor_or_recovery(retry_setup, which):
    retry, cloud, original, previous = retry_setup
    if which == "absent":
        with original.lock():
            version, document = original.owned()
            del document["records"][CAMPAIGN]
            original.checkpoint(version, document)
    elif which == "no_completion":
        retry.j.completed = lambda _: False
    elif which == "recovery":
        retry.j.recovery = True
    else:
        if which == "unresolved":
            previous["facts"]["pod_deleted_confirmed"] = False
        else:
            previous["state"] = "ACCEPTED"
        with original.lock():
            original.write(previous)
    with pytest.raises(ValueError):
        retry.start(confirmed=True, window_end=cloud.clock + 3600)
    assert cloud.creates == 1 and retry.j.read() is None


def test_retry_lost_create_response_is_recovered_without_new_allowance(retry_setup):
    retry, cloud, original, previous = retry_setup
    cloud.lost = True
    result = retry.start(confirmed=True, window_end=cloud.clock + 3600)
    assert result["state"] == "CREATE_UNKNOWN"
    retry.start(confirmed=True, window_end=cloud.clock + 3600)
    assert cloud.creates == 2
    result = retry.collect()
    assert result["facts"]["pod_deleted_confirmed"]
    assert result["campaign"]["reserved_usd"] == 2.000334
    assert original.read() == previous


def test_retry_does_not_create_without_paid_confirmation(retry_setup):
    retry, cloud, _, _ = retry_setup
    with pytest.raises(ValueError, match="explicit_paid_confirmation_required"):
        retry.start(window_end=cloud.clock + 3600)
    assert cloud.creates == 1 and retry.j.read() is None


def test_recovery_cli_follows_successor_without_creating(retry_setup, monkeypatch):
    import runpod_ci_broker.controller as module

    retry, cloud, original, previous = retry_setup
    retry.start(confirmed=True, window_end=cloud.clock + 3600)
    real_controller = Controller
    monkeypatch.setattr(
        module,
        "context",
        lambda: {
            **IDENTITY,
            "run_id": "9012",
            "path": ".github/workflows/recover-smoke.yml",
            "event": "workflow_run",
        },
    )
    monkeypatch.setattr(module, "GitHubContentsStore", lambda _: original.store)
    monkeypatch.setattr(module, "LiveAPI", lambda **kw: cloud)
    monkeypatch.setattr(module, "ReadOnlyRunPod", lambda: retry)
    monkeypatch.setattr(
        module,
        "Controller",
        lambda task, journal, api, snapshot, output: real_controller(
            task, journal, api, snapshot, retry.output, now=cloud.now, sleep=cloud.sleep
        ),
    )
    monkeypatch.setattr("sys.argv", ["controller", "collect"])
    assert module.main() == 2  # diagnostic 403 remains rejected; cleanup still completes
    assert cloud.creates == 2
    assert retry.j.read()["facts"]["pod_deleted_confirmed"]
    assert original.read() == previous


def test_retry_reserves_before_ambiguous_ledger_failure_without_post(retry_setup, monkeypatch):
    retry, cloud, original, previous = retry_setup
    save = retry.save

    def fail_after_intent(record, state=None):
        save(record, state)
        if state == "CREATING":
            raise LedgerError("ledger_write_unknown")

    monkeypatch.setattr(retry, "save", fail_after_intent)
    with pytest.raises(LedgerError):
        retry.start(confirmed=True, window_end=cloud.clock + 3600)
    assert cloud.creates == 1
    assert retry.j.read()["campaign"]["reserved_usd"] == 2.000334
    retry.start(confirmed=True, window_end=cloud.clock + 3600)
    assert cloud.creates == 1  # an uncertain checkpoint cannot cause a later POST
    assert original.read() == previous
