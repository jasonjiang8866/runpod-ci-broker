import pytest

from runpod_ci_broker.do_spot import parse_capacity

TABLE = """## Capacity by Datacenter

| Spot Capacity | ATL1 | MKC1 | MEM1 |
|---|---|---|---|
| Example GPU | ✗ | ✓ | |
## Capacity by GPU Model
| Unrelated | table |
"""


def test_preserves_all_three_states() -> None:
    assert parse_capacity(TABLE) == [
        {"model": "Example GPU", "region": "ATL1", "capacity": "no_capacity"},
        {"model": "Example GPU", "region": "MKC1", "capacity": "available"},
        {"model": "Example GPU", "region": "MEM1", "capacity": "not_offered"},
    ]


@pytest.mark.parametrize(
    "page",
    [
        "",
        TABLE.replace("✓", "?"),
        TABLE.replace("MKC1", "ATL1"),
        TABLE.replace("| ✓ | |", "| ✓ |"),
        TABLE.replace("| Example GPU |", "| |"),
    ],
)
def test_unreadable_snapshot_fails_closed(page: str) -> None:
    with pytest.raises(ValueError):
        parse_capacity(page)
