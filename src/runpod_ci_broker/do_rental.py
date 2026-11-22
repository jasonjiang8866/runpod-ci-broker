"""One campaign-bound on-demand provisioning probe, with independent cleanup."""

from __future__ import annotations

import hmac
import json
import os
import re
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

from .campaign import REPOSITORY
from .digitalocean import CreateRejected, DigitalOcean
from .handover import GitHubEvidence, context
from .ledger import GitHubContentsStore, LedgerError, Store
from .market import MarketPolicy, do_quotes
from .optimization import digest
from .provider import ProviderError
from .queue import TaskQueue, task_key
from .queue_workflow import LABELS, POLICY, QUEUE, tasks
from .sealed import SealedStore

LAUNCH = ".github/workflows/do-rental.yml"
RECOVERY = ".github/workflows/recover-do-rental.yml"
KEY = digest(["digitalocean", "gpu-rental-pilot-v1"])
OPENING = 16_003_502
# 15 minutes at USD 5 plus USD 0.50 reserve, retained even after cleanup.
RESERVE = 1_750_000
SIZES = {"gpu-h100x1-80gb", "gpu-h200x1-141gb"}
PROFILE = Path("examples/global-gpu-market.json")


def check_budget(store: Store) -> None:
    q = TaskQueue(store, QUEUE).status()
    specs = tasks()
    if (
        q.policy != POLICY
        or q.slots_used
        or q.reserved_microusd != OPENING
        or {k: e.task for k, e in q.entries.items()}
        != {task_key(QUEUE, specs[k].experiment_id): specs[k] for k in LABELS}
        or any(
            e.release != "verified_deleted" or (e.execution_status or {}).get("state") != "ACCEPTED"
            for e in q.entries.values()
        )
        or OPENING + RESERVE > q.policy.budget_microusd
    ):
        raise LedgerError("predecessor_unsettled")


def team(client: DigitalOcean) -> str:
    value = client.request("GET", "/account").get("account", {}).get("team", {}).get("uuid")
    if not isinstance(value, str) or not value:
        raise ProviderError("team_unverified")
    return value


def owned(row: dict[str, Any], state: dict[str, Any]) -> int:
    identifier = row.get("id")
    if (
        type(identifier) is not int
        or identifier <= 0
        or state.get("id") not in (None, identifier)
        or row.get("name") != state["name"]
        or row.get("size_slug") != state["sku"]
        or row.get("region", {}).get("slug") != state["region"]
        or (
            "tag" in state
            and (not isinstance(row.get("tags"), list) or state["tag"] not in row["tags"])
        )
    ):
        raise ProviderError("droplet_ownership_unverified")
    return identifier


def ssh_key_reference(configured: str) -> int | str:
    if not configured:
        raise ProviderError("registered_ssh_key_required")
    if re.fullmatch(r"[1-9][0-9]{0,18}", configured):
        return int(configured)
    if re.fullmatch(r"(?:[0-9a-fA-F]{2}:){15}[0-9a-fA-F]{2}", configured):
        return configured.lower()
    raise ProviderError("invalid_configured_ssh_key")


def credential_binding(client: DigitalOcean, name: str) -> str:
    if not client.token:
        raise ProviderError("credentials_missing")
    return hmac.digest(client.token.encode(), ("rcb/do-rental/v2/" + name).encode(), "sha256").hex()


def launch(
    store: Store, client: DigitalOcean, identity: dict[str, Any], configured_key: str = ""
) -> None:
    check_budget(store)
    if store.read(KEY)[1] is not None:
        raise LedgerError("rental_already_reserved")
    key = ssh_key_reference(configured_key)
    client.listing("droplets")  # Confirm inventory access before the first POST.
    policy = MarketPolicy.model_validate_json(PROFILE.read_text())
    if policy.gpu_count != 1:
        raise ValueError("single_gpu_required")
    refused: set[tuple[str, str]] = set()
    deadline = time.monotonic() + 180
    marker = uuid.uuid4().hex
    version = None
    for attempt in range(1, 4):
        options = [
            o
            for o in do_quotes(client, policy)
            if not o["reasons"] and o["sku"] in SIZES and (o["sku"], o["region"]) not in refused
        ]
        if not options or time.monotonic() >= deadline:
            raise ProviderError("no_eligible_droplet")
        previous_regions = {region for _, region in refused}
        best = min(
            options,
            key=lambda o: (
                o["region"] in previous_regions,
                Decimal(o["instance_hourly_usd"]),
                o["region"],
                o["sku"],
            ),
        )
        state = {
            "launch": identity,
            "name": "rcb-" + marker,
            "credential_binding": credential_binding(client, "rcb-" + marker),
            "sku": best["sku"],
            "region": best["region"],
            "instance_hourly_usd": best["instance_hourly_usd"],
            "status": "create_intent",
            "active": False,
            "id": None,
            "attempt": attempt,
            "refused": sorted([list(pair) for pair in refused]),
            "reserved_total_microusd": OPENING + RESERVE,
            "reserved_total_seconds": 12_600 + 900,
        }
        store.swap(KEY, version, state)
        version, saved = store.read(KEY)
        if saved != state or version is None:
            raise LedgerError("create_intent_unverified")
        try:
            row = client.request(
                "POST",
                "/droplets",
                body={
                    "name": state["name"],
                    "size": state["sku"],
                    "region": state["region"],
                    "image": "gpu-h100x1-base",
                    "ssh_keys": [key],
                    "backups": False,
                },
            ).get("droplet", {})
        except CreateRejected as exc:
            rejected = {**state, "status": "create_rejected", "reason": exc.code}
            store.swap(KEY, version, rejected)
            if exc.code != "capacity_refused" or attempt == 3:
                raise
            version, saved = store.read(KEY)
            if saved != rejected or version is None:
                raise LedgerError("rejection_unverified")
            refused.add((best["sku"], best["region"]))
            continue
        state["id"] = owned(row, state)
        store.swap(KEY, version, state)
        break
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        row = client.request("GET", f"/droplets/{state['id']}")["droplet"]
        owned(row, state)
        if row.get("status") == "active":
            version, saved = store.read(KEY)
            if saved != state:
                raise LedgerError("rental_state_changed")
            store.swap(KEY, version, {**state, "active": True})
            print(
                json.dumps(
                    {
                        "status": "active",
                        "gpu_size": state["sku"],
                        "region": state["region"],
                        "instance_hourly_usd": state["instance_hourly_usd"],
                    }
                )
            )
            return
        time.sleep(5)
    raise ProviderError("active_deadline_elapsed")


def cleanup(store: Store, client: DigitalOcean) -> str:
    version, state = store.read(KEY)
    if state is None:
        return "no_intent"
    if state["status"] == "create_rejected":
        return "no_create"
    if state["status"] == "deleted":
        return "verified_deleted"
    if "credential_binding" in state:
        if not hmac.compare_digest(
            state["credential_binding"], credential_binding(client, state["name"])
        ):
            raise ProviderError("rental_credential_mismatch")
    elif team(client) != state["team"]:
        raise ProviderError("team_mismatch")

    def matches() -> list[int]:
        if "tag" in state:  # Preserve recovery for pre-v2 journals.
            return [owned(row, state) for row in client.listing("droplets", tag_name=state["tag"])]
        return [
            owned(row, state) for row in client.listing("droplets") if row["name"] == state["name"]
        ]

    found = matches()
    if len(found) > 1:
        raise ProviderError("multiple_owned_matches")
    if state["id"] is None:
        for _ in range(5):
            if found:
                break
            time.sleep(3)
            found = matches()
        if len(found) != 1:
            raise ProviderError("ambiguous_create_unresolved")
        state["id"] = found[0]
        store.swap(KEY, version, state)
        version, observed = store.read(KEY)
        if observed != state:
            raise LedgerError("recovered_id_unverified")
    path = f"/droplets/{state['id']}"

    def exists() -> bool:
        try:
            row = client.request("GET", path)["droplet"]
        except ProviderError as exc:
            if exc.code == "not_found":
                return False
            raise
        owned(row, state)
        return True

    if exists():
        client.request("DELETE", path)
    absent = 0
    for _ in range(10):
        absent = absent + 1 if not exists() and not matches() else 0
        if absent == 2:
            store.swap(KEY, version, {**state, "status": "deleted"})
            return "verified_deleted"
        time.sleep(3)
    raise ProviderError("deletion_unverified")


def main() -> int:
    action = sys.argv[1:]
    try:
        if action == ["preflight"]:
            client = DigitalOcean()
            results = {}
            for stage, check in (
                (
                    "ssh_key_reference_format",
                    lambda: ssh_key_reference(os.environ.get("DO_SSH_KEY_ID", "")),
                ),
                ("droplet_inventory", lambda: client.listing("droplets")),
            ):
                try:
                    check()
                    results[stage] = "ok"
                except ProviderError as exc:
                    results[stage] = exc.code
            print(json.dumps(results))
            return 0 if all(value == "ok" for value in results.values()) else 2
        identity = context()
        store = SealedStore(GitHubContentsStore(REPOSITORY))
        if action == ["start"]:
            if (
                identity["path"] != LAUNCH
                or identity["event"] != "workflow_dispatch"
                or os.environ.get("RCB_CONFIRM_PAID") != "true"
            ):
                raise LedgerError("manual_launch_required")
            client = DigitalOcean(mode="allocate")
            launch(store, client, identity, os.environ.get("DO_SSH_KEY_ID", ""))
            print("GPU Droplet reached active state")
        elif action == ["cleanup"]:
            _, state = store.read(KEY)
            if identity["path"] not in {LAUNCH, RECOVERY}:
                raise LedgerError("cleanup_context_invalid")
            if state:
                original = state["launch"]
                if identity["path"] == LAUNCH:
                    if identity != original:
                        raise LedgerError("launch_mismatch")
                elif not GitHubEvidence().completed(original):
                    raise LedgerError("original_run_not_completed")
                elif identity["event"] == "workflow_run":
                    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())[
                        "workflow_run"
                    ]
                    if (str(event["id"]), event["run_attempt"], event["head_sha"]) != (
                        original["run_id"],
                        original["attempt"],
                        original["sha"],
                    ):
                        raise LedgerError("recovery_source_mismatch")
                print(cleanup(store, DigitalOcean(mode="cleanup")))
            else:
                print("no_intent")
        else:
            return 2
        return 0
    except (LedgerError, ProviderError) as exc:
        print(json.dumps({"error": str(exc) if str(exc).isidentifier() else "rental_failed"}))
    except (ValueError, TypeError, KeyError, AttributeError, OSError):
        print(json.dumps({"error": "invalid_config_or_response"}))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
