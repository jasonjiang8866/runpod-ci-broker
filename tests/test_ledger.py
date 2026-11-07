import base64
import copy
import hashlib
import io
import json
import multiprocessing
import os
from pathlib import Path
from urllib.error import HTTPError, URLError

import pytest

from runpod_ci_broker.fake import FakeProvider
from runpod_ci_broker.ledger import (
    GitHubContentsStore,
    LedgerError,
    SharedJournal,
    SQLiteStore,
    encode,
)
from runpod_ci_broker.lifecycle import Engine


class MemoryStore:
    def __init__(self):
        self.data = {}
        self.lost = 0
        self.drop = 0
        self.writes = 0

    def read(self, key):
        value = self.data.get(key)
        return (
            (hashlib.sha256(encode(value)).hexdigest(), copy.deepcopy(value))
            if value
            else (None, None)
        )

    def swap(self, key, version, value):
        self.writes += 1
        if self.read(key)[0] != version:
            raise LedgerError("ledger_conflict")
        if self.drop:
            self.drop -= 1
            raise LedgerError("ledger_write_unknown")
        self.data[key] = copy.deepcopy(value)
        if self.lost:
            self.lost -= 1
            raise LedgerError("ledger_write_unknown")


def test_competing_scope_and_unlocked_write(task):
    store = MemoryStore()
    a = SharedJournal(store, task.scope, task.experiment_id)
    b = SharedJournal(store, task.scope, "other")
    with pytest.raises(LedgerError, match="lock_lost"):
        a.owned()
    with a.lock():
        with pytest.raises(LedgerError, match="scope_busy"), b.lock():
            pass
        with pytest.raises(LedgerError, match="scope_busy"), a.lock():
            pass
    with b.lock():
        assert not b.other_unresolved()


def test_lost_commit_response_is_read_back_without_resubmission(task):
    store = MemoryStore()
    store.lost = 1
    with SharedJournal(store, task.scope, task.experiment_id).lock():
        assert store.writes == 1
    assert store.writes == 2


def test_unconfirmed_intent_never_calls_create(tmp_path, task, candidates, auth):
    store = MemoryStore()
    journal = SharedJournal(store, task.scope, task.experiment_id)
    provider = FakeProvider(candidates)
    engine = Engine(task, provider, journal, tmp_path)
    original = store.swap

    def drop_intent(key, version, value):
        if any(r["create_intent"] for r in value["records"].values()):
            raise LedgerError("ledger_write_unknown")
        original(key, version, value)

    store.swap = drop_intent
    with pytest.raises(LedgerError, match="unconfirmed"):
        engine.run(auth)
    assert provider.data["create_calls"] == 0


def test_create_response_lost_restores_on_new_runner_and_never_duplicates(
    tmp_path, task, candidates, auth
):
    store = SQLiteStore(tmp_path / "ledger.db")
    j = SharedJournal(store, task.scope, task.experiment_id)
    p = FakeProvider(candidates, tmp_path / "provider.json", {"create_lost": 1})
    e = Engine(task, p, j, tmp_path / "results", "first")
    assert e.run(auth, max_steps=1)["state"] == "CREATE_UNKNOWN"
    j2 = SharedJournal(SQLiteStore(tmp_path / "ledger.db"), task.scope, task.experiment_id)
    p2 = FakeProvider(candidates, tmp_path / "provider.json")
    r = Engine(task, p2, j2, tmp_path / "results", "replacement").run(auth)
    assert r["state"] == "ACCEPTED" and p2.data["create_calls"] == 1
    assert r["resume_run_ids"] == ["first", "replacement"]


@pytest.mark.parametrize(
    "fault,state", [("collect_error", "COLLECTION_FAILED"), ("delete_error", "CLEANUP_PENDING")]
)
def test_resume_collection_or_cleanup_with_new_journal(
    tmp_path, task, candidates, auth, fault, state
):
    store = SQLiteStore(tmp_path / "ledger.db")
    p = FakeProvider(candidates, faults={fault: 10})
    e = Engine(task, p, SharedJournal(store, task.scope, task.experiment_id), tmp_path / "results")
    r = e.run(auth, max_steps=4 if fault == "collect_error" else 20)
    assert r["state"] == state
    p.data["faults"][fault] = 0
    restored = Engine(
        task, p, SharedJournal(store, task.scope, task.experiment_id), tmp_path / "results"
    )
    r = restored.run(auth) if fault == "collect_error" else restored.reconcile()
    assert r["state"] == "ACCEPTED" and p.data["create_calls"] == 1


def test_unresolved_other_task_still_blocks_when_inventory_is_empty(
    tmp_path, task, candidates, auth
):
    store = MemoryStore()
    p = FakeProvider(candidates)
    e = Engine(task, p, SharedJournal(store, task.scope, task.experiment_id), tmp_path / "a")
    e.run(auth, max_steps=1)
    p.data["faults"]["inventory_hidden"] = 5
    other = task.model_copy(update={"experiment_id": "other"})
    other_auth = auth.model_copy(
        update={"experiment_id": "other", "config_fingerprint": other.fingerprint()}
    )
    r = Engine(other, p, SharedJournal(store, task.scope, "other"), tmp_path / "b").run(other_auth)
    assert r["state"] == "PREFLIGHT_BLOCKED" and p.data["create_calls"] == 1


def crash_owner(path):
    with SharedJournal(SQLiteStore(Path(path)), "scope", "task").lock():
        os._exit(0)


def test_killed_controller_does_not_expire_into_second_owner(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    path = str(tmp_path / "ledger.db")
    process = ctx.Process(target=crash_owner, args=(path,))
    process.start()
    process.join(10)
    assert process.exitcode == 0
    j = SharedJournal(SQLiteStore(Path(path)), "scope", "task")
    with pytest.raises(LedgerError, match="manual_recovery"), j.lock():
        pass


def cas_racer(path, barrier, queue):
    s = SQLiteStore(Path(path))
    key = "a" * 64
    version, _ = s.read(key)
    barrier.wait(timeout=10)
    try:
        s.swap(key, version, {"winner": os.getpid()})
        queue.put("won")
    except LedgerError:
        queue.put("lost")


def test_actual_process_cas_race_has_one_winner(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    path = str(tmp_path / "ledger.db")
    SQLiteStore(Path(path))
    barrier, queue = ctx.Barrier(2), ctx.Queue()
    children = [ctx.Process(target=cas_racer, args=(path, barrier, queue)) for _ in range(2)]
    for p in children:
        p.start()
    for p in children:
        p.join(10)
        assert p.exitcode == 0
    assert sorted([queue.get(timeout=2), queue.get(timeout=2)]) == ["lost", "won"]


@pytest.mark.parametrize("mutation", ["scope", "revision", "records"])
def test_corrupt_document_fails_closed(task, mutation):
    store = MemoryStore()
    j = SharedJournal(store, task.scope, task.experiment_id)
    with j.lock():
        pass
    store.data[j.key][mutation] = "invalid"
    with pytest.raises(LedgerError, match="invalid"):
        j.read()


def test_changed_lease_refuses_stale_write_and_release(task):
    store = MemoryStore()
    j = SharedJournal(store, task.scope, task.experiment_id)
    with pytest.raises(LedgerError, match="lock_lost"), j.lock():
        store.data[j.key]["holder"] = "b" * 32
        j.owned()
    assert store.data[j.key]["holder"] == "b" * 32


class Response(io.BytesIO):
    pass


class Opener:
    def __init__(self, result):
        self.result, self.requests = result, []

    def open(self, request, **kwargs):
        self.requests.append(request)
        if isinstance(self.result, Exception):
            raise self.result
        return Response(json.dumps(self.result).encode())


def github_store(monkeypatch, response):
    monkeypatch.setenv("RCB_LEDGER_TOKEN", "test-secret-DO-NOT-PRINT")
    opener = Opener(response)
    monkeypatch.setattr("runpod_ci_broker.ledger.build_opener", lambda *a: opener)
    return GitHubContentsStore("owner/broker"), opener


def test_github_conditional_payload_and_fixed_destination(monkeypatch):
    store, opener = github_store(monkeypatch, {})
    store.swap("a" * 64, "b" * 40, {"revision": 1})
    req = opener.requests[0]
    payload = json.loads(req.data)
    assert req.method == "PUT" and req.full_url.startswith(
        "https://api.github.com/repos/owner/broker/contents/scopes/"
    )
    assert payload["sha"] == "b" * 40 and payload["branch"] == "broker-state"
    assert b"test-secret" not in req.data
    with pytest.raises(LedgerError):
        GitHubContentsStore("owner/broker", "main")
    with pytest.raises(LedgerError):
        GitHubContentsStore("owner/broker?redirect=evil")


def test_github_reads_and_verifies_blob_sha(monkeypatch):
    raw = encode({"revision": 1})
    sha = hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest()
    store, opener = github_store(
        monkeypatch,
        {
            "type": "file",
            "encoding": "base64",
            "sha": sha,
            "content": base64.b64encode(raw).decode(),
        },
    )
    assert store.read("a" * 64) == (sha, {"revision": 1})
    opener.result["sha"] = "f" * 40
    with pytest.raises(LedgerError, match="response_invalid"):
        store.read("a" * 64)


@pytest.mark.parametrize(
    "status,code",
    [
        (401, "authentication_failed"),
        (403, "permission_denied"),
        (409, "conflict"),
        (422, "conflict"),
        (500, "request_failed"),
        (302, "request_failed"),
    ],
)
def test_github_errors_never_echo_response_or_token(monkeypatch, status, code):
    error = HTTPError("https://sensitive", status, "test-secret", {}, io.BytesIO(b"test-secret"))
    store, _ = github_store(monkeypatch, error)
    with pytest.raises(LedgerError, match=code) as exc:
        store.swap("a" * 64, None, {})
    assert "test-secret" not in str(exc.value)


def test_github_missing_read_is_not_successful_acquisition(monkeypatch):
    store, _ = github_store(monkeypatch, HTTPError("url", 404, "missing", {}, None))
    assert store.read("a" * 64) == (None, None)
    with pytest.raises(LedgerError), SharedJournal(store, "scope", "task").lock():
        pytest.fail("must not enter")


def test_github_network_write_unknown_and_no_retry(monkeypatch):
    store, opener = github_store(monkeypatch, URLError("secret"))
    with pytest.raises(LedgerError, match="write_unknown"):
        store.swap("a" * 64, None, {})
    assert len(opener.requests) == 1
