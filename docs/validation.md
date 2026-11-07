# Phase-one delivery and validation

Historical delivery record. For the subsequent 110-test hardening change and
current phase-2 dispatch status, see [phase-2 plan](phase-2-plan.md).

Date: 2026-11-06 (client date, Asia/Singapore).

## Local evidence

- **85 pytest cases passed** on Python 3.12.14 with the committed uv lockfile.
- Strict mypy passed for all 10 package modules.
- Ruff lint and formatting passed for package and tests.
- `uv build` produced a wheel and source distribution.
- CLI strict-reproduction and independent hardware-group simulations reached ACCEPTED.
- CLI injected lost-create/collection/delete failures reached CLEANUP_PENDING;
  a separately invoked reconcile reached ACCEPTED with confirmed fake absence.
- Missing-credential preflight emitted JSON/Markdown with `credentials_missing`,
  unknown inventory and no selected GPU. No authenticated RunPod call occurred.

| First-round requirement | Evidence |
|---|---|
| Deterministic explainable selection | `test_deterministic_preference_before_price`, `test_flexible_fallback`, candidate reason tests |
| Strict reproduction never changes GPU | `test_strict_never_falls_back`; strict schema rejects multiple GPUs |
| Missing budget/authority refuses launch | Budget/admission parameterized tests; CLI run refusal even with an approval document |
| Query failure differs from no capacity | Fake lifecycle and live-adapter response tests, missing-key CLI report |
| Lost create response does not duplicate | Lost response, hidden inventory, crash-after-intent and repeated workflow tests |
| Execution/collection/cleanup recover | Intermediate-state resume, worker failure, corrupt bundle, bounded collection and independent cleanup tests |
| Foreign resources untouched | Foreign Pod survival, changed ownership/image/mount, duplicate resource refusal |
| Evidence modes distinct | Offline/mock/read_only report tests; fake execution blocks sockets; live adapter has no mutations |

## Reproduce

```bash
uv sync --locked --group dev --python 3.12
uv run --locked ruff check src tests
uv run --locked ruff format --check src tests
uv run --locked mypy src
uv run --locked pytest --junitxml=out/tests.xml
uv build
```

See README for the independent fault/reconcile commands. Provider fake clock values
are simulation seconds, not cloud timestamps. Numerical fixture metrics are synthetic.

## Not verified or not implemented

- Real GPU catalog response, current stock, account permissions, prices, volume or
  private-image access: **not verified**; no local RunPod key was available.
- Real create/start/admission/worker/result transport/termination: **not implemented**
  in this phase. Only fake lifecycle operations exist. No paid workflow exists.
- Off-runner journal durability, multi-runner atomic lease and independent live
  cancellation recovery: required before paid execution, not claimed complete.
- Real GPU numerical/performance correctness, actual spend and supplier billing:
  **not run / unavailable**.
- Existing business project integration: **not attempted**; requires separate PRs.

## Next controlled step

Review/merge the no-cost PR, then use the main-only `read-only-preflight.yml`
workflow with an appropriately scoped existing `RUNPOD_API_KEY` in this repository.
No credentials need to be copied from other repositories. Its only input is
`example` with one of `simplexlora-flexible`, `spleeter-strict`,
`flashattention-hardware`. The example image/worker pins remain illustrative;
preflight does not validate their executability. An existing private registry ID
also remains blocked until access can be established.

Workflow dispatch was not available in the current GitHub connector, `gh` was not
installed and no browser fallback was requested or used. The workflow is prepared,
**not dispatched**. GitHub PR CI is a separate credential-free event.

Phase-three proposed smoke, after the live implementation is reviewed: one allowed
GPU, one small CUDA allocation/matmul, environment manifest, checksum-verified
artifacts, deliberate recovery case and verified termination. Choose exact GPU,
image, region, seconds and an all-in cap from the then-current read-only quote;
request explicit approval before any paid operation. No historical budget applies.
