"""Explicit manual GitHub state-only integration probe. Never imports RunPod clients."""

from __future__ import annotations

import hashlib
import json
import os

from .ledger import GitHubContentsStore, LedgerError, SharedJournal


def main() -> int:
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if (
        repository != "jasonjiang8866/runpod-ci-broker"
        or os.environ.get("GITHUB_REF") != "refs/heads/main"
        or os.environ.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
        or not run_id.isdecimal()
    ):
        print('{"error":"trusted_manual_main_context_required"}')
        return 2
    try:
        store = GitHubContentsStore(repository)
        scope = "state-probe-" + run_id + "-" + os.environ.get("GITHUB_RUN_ATTEMPT", "1")
        journal = SharedJournal(store, scope, "probe")
        with journal.lock():
            version, document = journal.owned()
            assert version is not None
            journal.checkpoint(version, document)
            try:
                store.swap(journal.key, version, document)
            except LedgerError as exc:
                if str(exc) != "ledger_conflict":
                    raise
            else:
                raise LedgerError("conditional_write_not_enforced")
        _, final = journal.load()
        assert final["holder"] is None and not final["records"]
        print(
            json.dumps(
                {
                    "mode": "github_state_only",
                    "scope_hash": hashlib.sha256(scope.encode()).hexdigest(),
                    "conditional_write_verified": True,
                    "released": True,
                    "runpod_operations": 0,
                }
            )
        )
        return 0
    except (LedgerError, ValueError, AssertionError):
        print('{"error":"state_probe_failed","runpod_operations":0}')
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
