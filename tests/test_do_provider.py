import json
import sys
from decimal import Decimal
from io import BytesIO
from urllib.error import HTTPError

import pytest
from pydantic import ValidationError

from runpod_ci_broker import cli, market
from runpod_ci_broker import do_rental as rental
from runpod_ci_broker.digitalocean import CreateRejected, DigitalOcean
from runpod_ci_broker.ledger import LedgerError, SQLiteStore
from runpod_ci_broker.provider import ProviderError

H100 = "gpu-h100x1-80gb"
H200 = "gpu-h200x1-141gb"


def size(slug=H100, price=5, count=1, regions=("ams3",), available=True):
    return {
        "slug": slug,
        "price_hourly": price,
        "gpu_info": {"count": count},
        "regions": list(regions),
        "available": available,
    }


class Cloud:
    def __init__(self):
        self.rows = [size(), size(H200, 5, regions=("atl1",))]
        self.account = "test-team"
        self.vm = None
        self.posts = []
        self.deletes = []
        self.behavior = []
        self.lists = 0

    def listing(self, name, **filters):
        if name == "sizes":
            assert filters == {"gpu_minimum": 1}
            return self.rows
        assert name == "droplets" and filters["tag_name"].startswith("rcb-owner-")
        self.lists += 1
        return [self.vm] if self.vm else []

    def request(self, method, path, body=None):
        if path == "/account":
            return {"account": {"team": {"uuid": self.account}}}
        if method == "POST":
            self.posts.append(body)
            action = self.behavior.pop(0) if self.behavior else None
            if isinstance(action, CreateRejected):
                raise action
            self.vm = {
                "id": 22,
                "name": body["name"],
                "size_slug": body["size"],
                "region": {"slug": body["region"]},
                "tags": body["tags"],
                "status": "active",
            }
            if action:
                raise action
        else:
            assert path == "/droplets/22"
            if not self.vm:
                raise ProviderError("not_found")
            if method == "DELETE":
                self.deletes.append(path)
                self.vm = None
                return {}
        return {"droplet": self.vm}


@pytest.fixture
def policy():
    return market.MarketPolicy.model_validate_json(rental.PROFILE.read_text())


def test_quotes_count_price_boundary_and_market(policy):
    c = Cloud()
    c.rows += [
        size("gpu-h200x8-1128gb", Decimal(32), 8),
        size(H200, Decimal("5.000001")),
        size(H100, None),
        size(H100, True),
        size(H100, available=False),
        size(H100, regions=()),
        size(H100 + "-spot"),
    ]
    rows = market.do_quotes(c, policy)
    assert rows[0]["gpu_hourly_usd"] == "5" and rows[0]["reasons"] == []
    assert (rows[2]["gpu_hourly_usd"], rows[2]["instance_hourly_usd"], rows[2]["reasons"]) == (
        "4",
        "32",
        ["gpu_count_mismatch"],
    )
    assert [r["reasons"] for r in rows[3:]] == [
        ["unit_price_above_cap"],
        ["price_unverified"],
        ["price_unverified"],
        ["no_capacity_signal"],
        ["no_capacity_signal"],
    ]
    with pytest.raises(ValidationError):
        policy.max_gpu_hourly_usd = 5.01


def test_cli_ranks_globally_and_reports_provider_outage(tmp_path, monkeypatch):
    c = Cloud()
    monkeypatch.setattr(market, "DigitalOcean", lambda: c)

    class RP:
        def get(self, path, params):
            return {
                "gpus": [
                    {
                        "id": "NVIDIA H200 NVL",
                        "price": {"secure": 3.79},
                        "dataCenters": [
                            {"id": "EU-1", "availability": "HIGH"},
                            {"id": "US-1", "availability": "NONE"},
                        ],
                    }
                ]
            }

    monkeypatch.setattr(market, "Transport", RP)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "broker",
            "--action",
            "offers",
            "--config",
            str(rental.PROFILE),
            "--output",
            str(tmp_path),
        ],
    )
    assert cli.main() == 0

    def read():
        return json.loads((tmp_path / "offers.json").read_text())

    assert [o["provider"] for o in read()["ranked"]] == ["runpod", "digitalocean", "digitalocean"]

    def missing():
        raise ProviderError("permission_denied")

    monkeypatch.setattr(market, "Transport", missing)
    assert cli.main() == 2
    assert len(read()["ranked"]) == 2 and read()["provider_status"]["runpod"] == "permission_denied"


@pytest.mark.parametrize("duplicate", [False, True])
def test_pagination_checks_completeness_and_never_follows_links(monkeypatch, duplicate):
    monkeypatch.setenv("DIGITALOCEAN_ACCESS_TOKEN", "fixture")
    client = DigitalOcean()

    def request(method, path, params):
        assert method == "GET" and path == "/sizes"
        sku = H100 if params["page"] == 1 or duplicate else H200
        return {
            "sizes": [size(sku)],
            "meta": {"total": 2},
            "links": {"pages": {"next": "https://untrusted.invalid"}},
        }

    monkeypatch.setattr(client, "request", request)
    if duplicate:
        with pytest.raises(ProviderError, match="duplicate"):
            client.listing("sizes")
    else:
        assert len(client.listing("sizes")) == 2


@pytest.mark.parametrize(
    ("status", "message", "code", "definitive"),
    [
        (422, "insufficient capacity in region", "capacity_refused", True),
        (403, "sensitive provider details", "permission_denied", True),
        (503, "out of capacity", "provider_unavailable", False),
    ],
)
def test_post_classification(monkeypatch, status, message, code, definitive):
    monkeypatch.setenv("DIGITALOCEAN_ACCESS_TOKEN", "fixture")

    class Reject:
        def open(self, req, timeout):
            raise HTTPError(
                req.full_url,
                status,
                "hidden",
                {},
                BytesIO(json.dumps({"message": message}).encode()),
            )

    monkeypatch.setattr("runpod_ci_broker.digitalocean.build_opener", lambda *args: Reject())
    with pytest.raises(ProviderError, match="endpoint_not_allowed"):
        DigitalOcean().request("POST", "/droplets", body={})
    with pytest.raises(ProviderError) as err:
        DigitalOcean(mode="allocate").request("POST", "/droplets", body={"name": "fixture"})
    assert err.value.code == code and isinstance(err.value, CreateRejected) == definitive


@pytest.fixture
def pilot(tmp_path, monkeypatch):
    store = SQLiteStore(tmp_path / "state")
    cloud = Cloud()
    monkeypatch.setattr(rental.time, "sleep", lambda _: None)
    with pytest.raises(LedgerError, match="queue_missing"):
        rental.launch(store, cloud, {}, 1)
    assert cloud.posts == []
    monkeypatch.setattr(rental, "check_budget", lambda s: None)
    return store, cloud


def test_capacity_fallback_and_cleanup(pilot):
    store, cloud = pilot
    cloud.behavior = [CreateRejected("capacity_refused")]
    rental.launch(store, cloud, {}, 1)
    assert [p["region"] for p in cloud.posts] == ["ams3", "atl1"]
    assert store.read(rental.KEY)[1]["active"] is True
    assert rental.cleanup(store, cloud) == "verified_deleted"
    assert cloud.deletes == ["/droplets/22"] and cloud.lists >= 3
    assert rental.cleanup(store, cloud) == "verified_deleted"
    with pytest.raises(LedgerError):
        rental.launch(store, cloud, {}, 1)
    assert len(cloud.posts) == 2


def test_lost_create_is_recovered_by_ownership_and_never_retried(pilot):
    store, cloud = pilot
    cloud.behavior = [ProviderError("response_unverified")]
    with pytest.raises(ProviderError):
        rental.launch(store, cloud, {}, 1)
    assert store.read(rental.KEY)[1]["id"] is None and len(cloud.posts) == 1
    assert rental.cleanup(store, cloud) == "verified_deleted"


def test_no_guess_or_cross_team_delete(pilot):
    store, cloud = pilot
    cloud.behavior = [ProviderError("response_unverified")]
    with pytest.raises(ProviderError):
        rental.launch(store, cloud, {}, 1)
    cloud.account = "different"
    with pytest.raises(ProviderError, match="team_mismatch"):
        rental.cleanup(store, cloud)
    cloud.account = "test-team"
    cloud.vm["name"] = "different"
    with pytest.raises(ProviderError, match="ownership"):
        rental.cleanup(store, cloud)
    cloud.vm = None
    with pytest.raises(ProviderError, match="ambiguous_create"):
        rental.cleanup(store, cloud)
    assert not cloud.deletes
    assert store.read(rental.KEY)[1]["status"] == "create_intent"


def test_over_budget_and_permission_denied_do_not_fallback(pilot):
    store, cloud = pilot
    cloud.rows = [size(price=Decimal("5.0001"))]
    with pytest.raises(ProviderError, match="no_eligible"):
        rental.launch(store, cloud, {}, 1)
    assert not cloud.posts and store.read(rental.KEY)[1] is None
    cloud.rows = [size()]
    cloud.behavior = [CreateRejected("permission_denied")]
    with pytest.raises(CreateRejected):
        rental.launch(store, cloud, {}, 1)
    assert rental.cleanup(store, cloud) == "no_create"
    assert len(cloud.posts) == 1


def test_verified_queue_balance_is_inherited(tmp_path):
    from runpod_ci_broker.optimization import digest
    from runpod_ci_broker.queue import TaskQueue

    store = SQLiteStore(tmp_path / "queue")
    queue = TaskQueue(store, rental.QUEUE)
    queue.initialize(rental.POLICY)
    for task in rental.tasks().values():
        queue.submit(task)
    with pytest.raises(LedgerError, match="predecessor_unsettled"):
        rental.check_budget(store)
    version, state = store.read(queue.key)
    for entry in state["entries"].values():
        identity = {"run_id": "123"}
        entry.update(
            owner=digest(identity),
            release="verified_deleted",
            execution_status={"state": "ACCEPTED"},
            dispatch={
                "queue_name": rental.QUEUE,
                "snapshot_id": "snapshot",
                "gpu_id": "gpu",
                "data_center": "dc",
                "hourly_microusd": 1000000,
                "permit_digest": "permit",
                "launch_identity": identity,
                "latest_start": None,
            },
        )
    store.swap(queue.key, version, state)
    rental.check_budget(store)


def test_failed_delete_keeps_reservation_and_can_be_recovered(pilot, monkeypatch):
    store, cloud = pilot
    rental.launch(store, cloud, {}, 1)
    request = cloud.request

    def deny_delete(method, path, **kwargs):
        if method == "DELETE":
            raise ProviderError("permission_denied")
        return request(method, path, **kwargs)

    monkeypatch.setattr(cloud, "request", deny_delete)
    with pytest.raises(ProviderError, match="permission_denied"):
        rental.cleanup(store, cloud)
    assert cloud.vm and store.read(rental.KEY)[1]["status"] != "deleted"
    monkeypatch.setattr(cloud, "request", request)
    assert rental.cleanup(store, cloud) == "verified_deleted"
    assert len(cloud.posts) == 1


def test_cleanup_transport_cannot_create(monkeypatch):
    monkeypatch.setenv("DIGITALOCEAN_ACCESS_TOKEN", "fixture")
    with pytest.raises(ProviderError, match="endpoint_not_allowed"):
        DigitalOcean(mode="cleanup").request("POST", "/droplets")
    with pytest.raises(ProviderError, match="endpoint_not_allowed"):
        DigitalOcean(mode="allocate").request("DELETE", "/droplets/22")
