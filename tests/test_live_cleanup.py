import copy
import hashlib
import io
from urllib.error import HTTPError

import pytest

from runpod_ci_broker.cleanup import CleanupAPI, cleanup_owned
from runpod_ci_broker.lifecycle import owner
from runpod_ci_broker.provider import ProviderError
from runpod_ci_broker.storage import Journal


class Client:
    def __init__(self, pod):
        self.pod, self.deletes, self.clock = pod, [], 0

    def get(self, pod_id, timeout):
        return copy.deepcopy(self.pod)

    def active_ids(self, deadline):
        return {"abc123"} if self.pod else set()

    def delete(self, pod_id, timeout):
        self.deletes.append(pod_id)
        self.pod = None

    def now(self):
        return self.clock

    def sleep(self, seconds):
        self.clock += seconds


def setup(tmp_path, task):
    name = (
        "rcb-" + hashlib.sha256((task.scope + ":" + task.experiment_id).encode()).hexdigest()[:24]
    )
    r = {
        "mode": "live",
        "request": task.model_dump(),
        "config_fingerprint": task.fingerprint(),
        "create_intent": True,
        "name": name,
        "pod_id": "abc123",
        "actions_run_id": "12345",
        "selected": {"gpu_id": task.gpu_allowlist[0], "data_center": "EU-RO-1"},
        "facts": {"pod_deleted_confirmed": False},
    }
    j = Journal(tmp_path, task.scope, task.experiment_id)
    j.write(r)
    p = {
        "id": "abc123",
        "name": name,
        "image": task.image,
        "env": owner(task, "12345"),
        "mounts": {},
        "gpu": {"id": task.gpu_allowlist[0], "count": task.gpu_count},
        "cloud": task.cloud,
        "dataCenterId": "EU-RO-1",
    }
    return j, Client(p)


def test_known_owned_cleanup_needs_two_direct_and_inventory_absences(tmp_path, task):
    j, p = setup(tmp_path, task)
    r = cleanup_owned(task, j, p, allow_termination=True, now=p.now, sleep=p.sleep)
    assert p.deletes == ["abc123"] and r["facts"]["pod_deleted_confirmed"]
    assert r["cleanup_verification"] == "direct_404_and_complete_inventory_twice"


def test_termination_disabled_by_default(tmp_path, task):
    j, p = setup(tmp_path, task)
    r = cleanup_owned(task, j, p, now=p.now, sleep=p.sleep)
    assert not p.deletes and not r["facts"]["pod_deleted_confirmed"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "user-pod"),
        ("id", "another"),
        ("image", "changed"),
        ("env", {}),
        ("gpu", {"id": "foreign", "count": 1}),
        ("dataCenterId", "unexpected"),
        ("mounts", {"persistent": {"size": 1}}),
    ],
)
def test_ownership_mismatch_never_deletes(tmp_path, task, field, value):
    j, p = setup(tmp_path, task)
    p.pod[field] = value
    r = cleanup_owned(task, j, p, allow_termination=True, now=p.now, sleep=p.sleep)
    assert not p.deletes and "ownership_ambiguous" in r["errors"]


def test_unknown_create_cannot_be_cleared_by_empty_inventory(tmp_path, task):
    j, p = setup(tmp_path, task)
    r = j.read()
    r["pod_id"] = None
    j.write(r)
    r = cleanup_owned(task, j, p, allow_termination=True, now=p.now, sleep=p.sleep)
    assert not p.deletes and r["cleanup_verification"] == "creation_unresolved"


def test_delete_receipt_without_disappearance_is_not_confirmation(tmp_path, task):
    j, p = setup(tmp_path, task)
    p.delete = lambda pod_id, timeout: p.deletes.append(pod_id)
    r = cleanup_owned(task, j, p, allow_termination=True, now=p.now, sleep=p.sleep)
    assert len(p.deletes) == 3 and not r["facts"]["pod_deleted_confirmed"]


def test_ambiguous_delete_response_is_reconciled(tmp_path, task):
    j, p = setup(tmp_path, task)

    def delete(pod_id, timeout):
        p.deletes.append(pod_id)
        p.pod = None
        raise ProviderError("cleanup_request_unknown", retryable=True, ambiguous=True)

    p.delete = delete
    assert cleanup_owned(task, j, p, allow_termination=True, now=p.now, sleep=p.sleep)["facts"][
        "pod_deleted_confirmed"
    ]
    assert len(p.deletes) == 1


def test_404_alone_cannot_verify_deletion(tmp_path, task):
    j, p = setup(tmp_path, task)
    p.pod = None

    def failed(deadline):
        raise ProviderError("inventory_incomplete")

    p.active_ids = failed
    r = cleanup_owned(task, j, p, allow_termination=True, now=p.now, sleep=p.sleep)
    assert not r["facts"]["pod_deleted_confirmed"]


@pytest.mark.parametrize("mode", ["mock", "read_only", "offline"])
def test_nonlive_record_cannot_authorize_deletion(tmp_path, task, mode):
    j, p = setup(tmp_path, task)
    r = j.read()
    r["mode"] = mode
    j.write(r)
    with pytest.raises(ValueError):
        cleanup_owned(task, j, p, allow_termination=True)
    assert not p.deletes


def test_cleanup_transport_paths_and_explicit_enable(monkeypatch):
    monkeypatch.setenv("RUNPOD_API_KEY", "secret")
    api = CleanupAPI()
    with pytest.raises(ProviderError, match="not_enabled"):
        api.delete("abc123", 1)
    for path in ["/network-volumes/abc123", "/pods/abc123/stop", "/pods/../abc123"]:
        with pytest.raises(ProviderError):
            api.request(path, 1, delete=True)


def test_cleanup_http_error_sanitized(monkeypatch):
    monkeypatch.setenv("RUNPOD_API_KEY", "secret")

    class Opener:
        def open(self, *a, **kw):
            raise HTTPError("secret-url", 403, "secret-body", {}, io.BytesIO(b"secret"))

    monkeypatch.setattr("runpod_ci_broker.cleanup.build_opener", lambda *a: Opener())
    with pytest.raises(ProviderError) as exc:
        CleanupAPI(allow_termination=True).delete("abc123", 1)
    assert str(exc.value) == "permission_denied"


def test_complete_inventory_pagination_and_malformed_status(monkeypatch):
    monkeypatch.setenv("RUNPOD_API_KEY", "secret")
    api = CleanupAPI(now=lambda: 0)
    responses = [
        {
            "pods": [{"id": "x", "status": "RUNNING", "env": {}}],
            "pagination": {"hasNextPage": True, "nextCursor": "next"},
        },
        {
            "pods": [{"id": "y", "status": "TERMINATED", "env": {}}],
            "pagination": {"hasNextPage": False},
        },
    ]
    api.request = lambda *a, **kw: responses.pop(0)
    assert api.active_ids(100) == {"x"}
    api.request = lambda *a, **kw: {
        "pods": [{"id": "x", "status": "weird", "env": {}}],
        "pagination": {"hasNextPage": False},
    }
    with pytest.raises(ProviderError):
        api.active_ids(100)
