"""Report DigitalOcean's public hourly Spot GPU capacity snapshot."""

import json
import re
import sys
from datetime import UTC, datetime
from urllib.error import URLError
from urllib.request import urlopen

SOURCE = "https://docs.digitalocean.com/products/droplets/details/spot-gpu-capacity/index.html.md"
MARKERS = {"✓": "available", "✗": "no_capacity", "": "not_offered"}


def parse_capacity(markdown: str) -> list[dict[str, str]]:
    lines = markdown.splitlines()
    start = lines.index("## Capacity by Datacenter") + 1
    section = []
    for line in lines[start:]:
        if line.startswith("#"):
            break
        if line.strip().startswith("|"):
            section.append(
                [
                    cell.strip()
                    for cell in line.strip().removeprefix("|").removesuffix("|").split("|")
                ]
            )
    if len(section) < 3 or section[0][0] != "Spot Capacity":
        raise ValueError("missing capacity table")
    regions = section[0][1:]
    if (
        not regions
        or len(set(regions)) != len(regions)
        or not all(re.fullmatch(r"[A-Z0-9]+", region) for region in regions)
    ):
        raise ValueError("invalid datacenter columns")
    if len(section[1]) != len(regions) + 1 or not all(
        re.fullmatch(r":?-{3,}:?", cell) for cell in section[1]
    ):
        raise ValueError("invalid table separator")
    rows = []
    models = set()
    for model, *cells in section[2:]:
        if not model or model in models or len(cells) != len(regions):
            raise ValueError("invalid GPU row")
        models.add(model)
        for region, cell in zip(regions, cells, strict=True):
            if cell not in MARKERS:
                raise ValueError("unrecognized capacity marker")
            rows.append({"model": model, "region": region, "capacity": MARKERS[cell]})
    return rows


def main() -> int:
    try:
        with urlopen(SOURCE, timeout=15) as response:
            body = response.read(256 * 1024 + 1)
        if len(body) > 256 * 1024:
            raise ValueError("capacity page too large")
        rows = parse_capacity(body.decode("utf-8"))
    except (OSError, URLError, ValueError) as exc:
        print(f"DigitalOcean Spot capacity unknown: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "provider": "digitalocean",
                "market": "spot",
                "source": SOURCE,
                "retrieved_at": datetime.now(UTC).isoformat(),
                "refresh_interval": "hourly",
                "placements": rows,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
