"""Read-only cross-provider quotes. An offer is never an execution permit."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import Field, ValidationError

from .digitalocean import DigitalOcean
from .models import Identifier, Model
from .provider import ProviderError, Transport
from .wire import GPU


class MarketPolicy(Model):
    gpu_count: Annotated[int, Field(ge=1, le=8)]
    max_gpu_hourly_usd: Annotated[float, Field(gt=0, le=5, allow_inf_nan=False)]
    runpod_gpu_ids: Annotated[list[str], Field(min_length=1)]
    digitalocean_sizes: Annotated[list[Identifier], Field(min_length=1)]


def quote(
    provider: str,
    sku: str,
    region: str,
    count: int,
    total: Decimal | None,
    stock: str,
    policy: MarketPolicy,
) -> dict[str, Any]:
    unit = total / count if total is not None else None
    reasons = []
    if count != policy.gpu_count:
        reasons.append("gpu_count_mismatch")
    if unit is None or not unit.is_finite() or unit <= 0:
        reasons.append("price_unverified")
        total = unit = None
    elif unit > Decimal(str(policy.max_gpu_hourly_usd)):
        reasons.append("unit_price_above_cap")
    if stock not in {"LOW", "MEDIUM", "HIGH", "catalog_offered"} or not region:
        reasons.append("no_capacity_signal")
    return {
        "provider": provider,
        "sku": sku,
        "region": region,
        "gpu_count": count,
        "gpu_hourly_usd": str(unit) if unit is not None else None,
        "instance_hourly_usd": str(total) if total is not None else None,
        "signal": stock,
        "reasons": reasons,
    }


def do_quotes(client: DigitalOcean, policy: MarketPolicy) -> list[dict[str, Any]]:
    offers = []
    for row in client.listing("sizes", gpu_minimum=1):
        sku = row["slug"]
        if sku not in policy.digitalocean_sizes or sku.endswith(("-spot", "-contracted")):
            continue
        info = row.get("gpu_info")
        count = info.get("count") if isinstance(info, dict) else None
        regions = row.get("regions")
        if (
            type(count) is not int
            or count < 1
            or type(row.get("available")) is not bool
            or not isinstance(regions, list)
            or any(not isinstance(r, str) or not r for r in regions)
        ):
            raise ProviderError("size_schema_invalid")
        price = row.get("price_hourly")
        total = Decimal(str(price)) if type(price) in (Decimal, float, int) else None
        for region in sorted(set(regions)) or [""]:
            offers.append(
                quote(
                    "digitalocean",
                    sku,
                    region,
                    count,
                    total,
                    "catalog_offered" if row["available"] and region else "NONE",
                    policy,
                )
            )
    return offers


def runpod_quotes(client: Transport, policy: MarketPolicy) -> list[dict[str, Any]]:
    data = client.get(
        "/catalog/gpus",
        {"include": "AVAILABILITY", "product": "POD", "cloud": "SECURE", "count": policy.gpu_count},
    )
    if not isinstance(data.get("gpus"), list):
        raise ProviderError("catalog_schema_invalid")
    offers = []
    seen = set()
    for raw in data["gpus"]:
        if not isinstance(raw, dict):
            raise ProviderError("catalog_schema_invalid")
        if raw.get("id") not in policy.runpod_gpu_ids:
            continue
        gpu = GPU.model_validate(raw)
        if gpu.id in seen:
            raise ProviderError("catalog_duplicate")
        seen.add(gpu.id)
        unit = gpu.price.get("secure")
        total = Decimal(str(unit)) * policy.gpu_count if unit is not None else None
        if not gpu.dataCenters:
            offers.append(
                quote(
                    "runpod",
                    gpu.id,
                    "",
                    policy.gpu_count,
                    total,
                    "NONE" if gpu.availability == "NONE" else "UNKNOWN",
                    policy,
                )
            )
        if len({dc.id for dc in gpu.dataCenters}) != len(gpu.dataCenters):
            raise ProviderError("catalog_duplicate")
        for dc in gpu.dataCenters:
            stock = dc.availability or "UNKNOWN"
            if gpu.maxCount.get("secure", policy.gpu_count) < policy.gpu_count:
                stock = "NONE"
            offers.append(quote("runpod", gpu.id, dc.id, policy.gpu_count, total, stock, policy))
    return offers


def report(
    policy: MarketPolicy, provider: Literal["all", "digitalocean", "runpod"]
) -> dict[str, Any]:
    offers = []
    status = {}
    for name in ("digitalocean", "runpod"):
        if provider not in ("all", name):
            continue
        try:
            rows = (
                do_quotes(DigitalOcean(), policy)
                if name == "digitalocean"
                else runpod_quotes(Transport(), policy)
            )
            status[name] = "ok"
            offers.extend(rows)
        except ProviderError as exc:
            status[name] = exc.code
        except ValidationError:
            status[name] = "catalog_schema_invalid"
    ranked = sorted(
        (o for o in offers if not o["reasons"]),
        key=lambda o: (Decimal(o["gpu_hourly_usd"]), o["provider"], o["sku"], o["region"]),
    )
    return {
        "observed_at": datetime.now(UTC).isoformat(),
        "provider_status": status,
        "max_gpu_hourly_usd": policy.max_gpu_hourly_usd,
        "capacity_reserved": False,
        "ranked": ranked,
        "excluded": [o for o in offers if o["reasons"]],
    }
