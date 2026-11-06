import json
from pathlib import Path

import pytest

from runpod_ci_broker.models import Authorization, Candidate, Task


@pytest.fixture
def task():
    return Task.model_validate_json(Path("examples/simplexlora-flexible.json").read_text())


@pytest.fixture
def candidates():
    return [
        Candidate.model_validate(x)
        for x in json.loads(Path("examples/mock-catalog.json").read_text())
    ]


@pytest.fixture
def auth(task):
    return Authorization(
        experiment_id=task.experiment_id,
        config_fingerprint=task.fingerprint(),
        approved_budget_usd=task.limits.total_budget_usd,
        gpu_allowlist=task.gpu_allowlist,
        runtime_seconds=task.limits.runtime_seconds,
        explicitly_approved=True,
    )
