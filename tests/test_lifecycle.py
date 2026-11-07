import pytest

from runpod_ci_broker.fake import FakeProvider
from runpod_ci_broker.lifecycle import Engine
from runpod_ci_broker.models import Task
from runpod_ci_broker.storage import Journal


def setup(tmp_path, task, candidates, faults=None):
    p = FakeProvider(candidates, tmp_path / "provider.json", faults)
    j = Journal(tmp_path / "state", task.scope, task.experiment_id)
    e = Engine(task, p, j, tmp_path / "results", "1234")
    return p, j, e


def test_success_and_repeated_workflow_does_not_create_again(tmp_path, task, candidates, auth):
    p, j, e = setup(tmp_path, task, candidates)
    r = e.run(auth)
    assert r["state"] == "ACCEPTED"
    assert all(r["facts"].values())
    assert r["mode"] == "mock" and r["gpu_verified"] is False
    assert not p.list_pods()
    Engine(task, p, j, tmp_path / "results", "5678").run(auth)
    assert p.data["create_calls"] == 1


def test_no_auth_never_creates(tmp_path, task, candidates):
    p, _j, e = setup(tmp_path, task, candidates)
    with pytest.raises(ValueError):
        e.run(None)
    assert p.data["create_calls"] == 0


def test_lost_response_reconciles_before_any_create(tmp_path, task, candidates, auth):
    p, _j, e = setup(tmp_path, task, candidates, {"create_lost": 1})
    r = e.run(auth)
    assert r["state"] == "ACCEPTED" and p.data["create_calls"] == 1
    assert any(x["to"] == "CREATE_UNKNOWN" for x in r["events"])


def test_empty_inventory_after_lost_response_never_retries(tmp_path, task, candidates, auth):
    p, j, e = setup(tmp_path, task, candidates, {"create_lost": 1})
    e.run(auth, max_steps=1)
    p.data["faults"]["inventory_hidden"] = 10
    assert e.run(auth)["state"] == "CREATE_UNKNOWN"
    assert Engine(task, p, j, tmp_path / "results").run(auth)["state"] == "CREATE_UNKNOWN"
    assert p.data["create_calls"] == 1
    p.data["faults"]["inventory_hidden"] = 0
    r = Engine(task, p, j, tmp_path / "results").reconcile(cleanup_only=True)
    assert r["facts"]["pod_deleted_confirmed"] and r["state"] == "REJECTED"


def test_crash_between_intent_and_response(tmp_path, task, candidates, auth):
    p, j, e = setup(tmp_path, task, candidates)
    e.run(auth, max_steps=1)
    r = j.read()
    r["state"] = "CREATING"
    r["pod_id"] = None
    j.write(r)
    r = Engine(task, p, j, tmp_path / "results").run(auth)
    assert r["state"] == "ACCEPTED" and p.data["create_calls"] == 1


@pytest.mark.parametrize(
    "fault,expected",
    [
        ("execution_error", "REJECTED"),
        ("image_pull_error", "REJECTED"),
        ("collect_error", "ACCEPTED"),
        ("corrupt_result", "ACCEPTED"),
        ("poll_error", "ACCEPTED"),
    ],
)
def test_recoverable_failures(tmp_path, task, candidates, auth, fault, expected):
    p, _j, e = setup(tmp_path, task, candidates, {fault: 1})
    r = e.run(auth)
    assert r["state"] == expected
    assert r["facts"]["pod_deleted_confirmed"]
    assert p.data["create_calls"] == 1


def test_collection_expiry_terminates_instead_of_billing_forever(tmp_path, task, candidates, auth):
    _p, _j, e = setup(tmp_path, task, candidates, {"collect_error": 999})
    r = e.run(auth)
    assert r["state"] == "REJECTED" and not r["facts"]["results_saved"]
    assert r["facts"]["pod_deleted_confirmed"]
    assert r["collect_attempts"] == task.limits.max_collect_attempts


def test_delete_unavailable_then_independent_recovery(tmp_path, task, candidates, auth):
    p, j, e = setup(tmp_path, task, candidates, {"delete_error": 3})
    r = e.run(auth)
    assert r["state"] == "CLEANUP_PENDING" and not r["facts"]["pod_deleted_confirmed"]
    assert r["facts"]["experiment_passed"] and r["facts"]["results_saved"]
    r = Engine(task, p, j, tmp_path / "results").reconcile()
    assert r["state"] == "ACCEPTED" and r["facts"]["pod_deleted_confirmed"]


def test_delete_request_alone_is_not_proof(tmp_path, task, candidates, auth):
    p, _j, e = setup(tmp_path, task, candidates, {"delete_noop": 9})
    r = e.run(auth)
    assert r["state"] == "CLEANUP_PENDING" and p.list_pods()


def test_differentiate_query_failure_and_no_capacity(tmp_path, task, candidates, auth):
    p, _j, e = setup(tmp_path / "a", task, candidates, {"query_error": 99})
    assert e.run(auth)["state"] == "PREFLIGHT_BLOCKED"
    p, _j, e = setup(tmp_path / "b", task, candidates, {"capacity_none": 99})
    assert e.run(auth)["state"] == "NO_CAPACITY"
    assert p.data["create_calls"] == 0


def test_capacity_race_is_bounded_and_refreshes(tmp_path, task, candidates, auth):
    p, _j, e = setup(tmp_path, task, candidates, {"capacity_race": 1})
    r = e.run(auth)
    assert r["state"] == "ACCEPTED" and p.data["create_calls"] == 2
    assert r["query_count"] == 2


@pytest.mark.parametrize("fault", ["auth_error", "permission_error", "account_error"])
def test_nonretryable_errors(tmp_path, task, candidates, auth, fault):
    p, _j, e = setup(tmp_path, task, candidates, {fault: 9})
    assert e.run(auth)["state"] == "PREFLIGHT_BLOCKED"
    assert p.data["create_calls"] == 1


def test_hung_start_is_bounded(tmp_path, task, candidates, auth):
    task = Task.model_validate(
        {**task.model_dump(), "limits": {**task.limits.model_dump(), "startup_seconds": 2}}
    )
    auth = auth.model_copy(update={"config_fingerprint": task.fingerprint()})
    _p, _j, e = setup(tmp_path, task, candidates, {"hang": 99})
    r = e.run(auth)
    assert r["state"] == "REJECTED" and any(x["to"] == "TIMED_OUT" for x in r["events"])
    assert r["facts"]["pod_deleted_confirmed"]


def test_cleanup_refuses_ambiguous_owner_and_leaves_foreign_pods(tmp_path, task, candidates, auth):
    p, _j, e = setup(tmp_path, task, candidates)
    e.run(auth, max_steps=1)
    pod = p.data["pods"][e.r["pod_id"]]
    pod["env"]["RCB_SOURCE"] = "f" * 40
    p.data["pods"]["foreign"] = {"id": "foreign", "name": "user-pod", "env": {}}
    r = e.reconcile(cleanup_only=True)
    assert r["state"] == "CLEANUP_PENDING"
    assert p.data["deletions"] == [] and len(p.list_pods()) == 2


def test_unrelated_pod_survives_success(tmp_path, task, candidates, auth):
    p, _j, e = setup(tmp_path, task, candidates)
    p.data["pods"]["foreign"] = {"id": "foreign", "name": "user-pod", "env": {}}
    assert e.run(auth)["state"] == "ACCEPTED"
    assert [x["id"] for x in p.list_pods()] == ["foreign"]


def test_same_experiment_changed_config_is_refused(tmp_path, task, candidates, auth):
    p, j, e = setup(tmp_path, task, candidates)
    e.run(auth, max_steps=1)
    changed = task.model_copy(update={"entrypoint": ["python", "different.py"]})
    with pytest.raises(ValueError):
        Engine(changed, p, j, tmp_path / "results")


def test_scope_lock_and_unresolved_journal_prevent_parallel_pods(tmp_path, task, candidates, auth):
    p, j, e = setup(tmp_path, task, candidates)
    with j.lock(), pytest.raises(ValueError), j.lock():
        pass
    e.run(auth, max_steps=1)
    other = task.model_copy(update={"experiment_id": "other"})
    other_auth = auth.model_copy(
        update={"experiment_id": "other", "config_fingerprint": other.fingerprint()}
    )
    r = Engine(other, p, Journal(tmp_path / "state", task.scope, "other"), tmp_path / "other").run(
        other_auth
    )
    assert r["state"] == "PREFLIGHT_BLOCKED" and p.data["create_calls"] == 1


@pytest.mark.parametrize("steps", [1, 2, 3, 4, 5])
def test_resume_from_intermediate_states(tmp_path, task, candidates, auth, steps):
    _p, j, e = setup(tmp_path, task, candidates)
    e.run(auth, max_steps=steps)
    restored_provider = FakeProvider(candidates, tmp_path / "provider.json")
    r = Engine(task, restored_provider, j, tmp_path / "results").run(auth)
    assert r["state"] == "ACCEPTED" and restored_provider.data["create_calls"] == 1


def test_missing_artifact_invalidates_acceptance_without_relaunch(tmp_path, task, candidates, auth):
    p, j, e = setup(tmp_path, task, candidates)
    assert e.run(auth)["state"] == "ACCEPTED"
    (tmp_path / "results/worker.log").unlink()
    r = Engine(task, p, j, tmp_path / "results").run(auth)
    assert r["state"] == "REJECTED" and r["facts"]["results_saved"] is False
    assert p.data["create_calls"] == 1


@pytest.mark.parametrize(
    "field,value",
    [("image", "different@sha256:" + "1" * 64), ("mounts", {"persistent": {"size": 100}})],
)
def test_cleanup_rejects_changed_resource_contract(tmp_path, task, candidates, auth, field, value):
    p, _j, e = setup(tmp_path, task, candidates)
    e.run(auth, max_steps=1)
    p.data["pods"][e.r["pod_id"]][field] = value
    assert e.reconcile(cleanup_only=True)["state"] == "CLEANUP_PENDING"
    assert not p.data["deletions"]


def test_duplicate_owned_resources_are_not_blindly_deleted(tmp_path, task, candidates, auth):
    import copy

    p, _j, e = setup(tmp_path, task, candidates)
    e.run(auth, max_steps=1)
    duplicate = copy.deepcopy(p.data["pods"][e.r["pod_id"]])
    duplicate["id"] = "duplicate"
    p.data["pods"]["duplicate"] = duplicate
    assert e.reconcile(cleanup_only=True)["state"] == "CLEANUP_PENDING"
    assert not p.data["deletions"]


def test_runtime_gpu_mismatch_does_not_pass_as_gpu_success(tmp_path, task, candidates, auth):
    p, _j, e = setup(tmp_path, task, candidates)
    e.run(auth, max_steps=1)
    p.data["pods"][e.r["pod_id"]]["candidate"]["gpu_id"] = "CPU fallback"
    r = e.run(auth)
    assert r["state"] == "REJECTED" and r["facts"]["experiment_passed"] is False
    assert r["facts"]["pod_deleted_confirmed"]


def test_reconcile_after_deletion_before_acceptance(tmp_path, task, candidates, auth):
    p, j, e = setup(tmp_path, task, candidates)
    r = e.run(auth, max_steps=5)
    assert r["state"] == "ACCEPTING"
    assert Engine(task, p, j, tmp_path / "results").reconcile()["state"] == "ACCEPTED"
    assert p.data["create_calls"] == 1
