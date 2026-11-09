# Fixed campaign controller: review before activation

Prepared for the already approved **USD 20 total / five-hour** Broker campaign.
No paid resource was created while developing this change. Tests inject cloud
responses; they are not live GPU evidence. PR merge and activation of the new
credential-bearing workflows require review of the concrete permissions below.

## First launch specification

| Item | Fixed value or behavior |
|---|---|
| Task | `examples/approved-broker-smoke.json`; ID `broker-smoke-20261107` |
| Worker source | `2e6fe6930c02fc2469079a1d8e64025562aff07f` |
| Image | `ghcr.io/jasonjiang8866/runpod-ci-broker-worker@sha256:d1a450f816d71169c29eb53217d869baa88d9eeaa156911f06b83b8a93f9ad11` |
| GPU | One allowed 3090, 4090, 5090 or PRO 6000 MIG 24GB; preference in that order |
| Compatibility | Driver CUDA support >=12.8; no framework/toolkit or numerical-equivalence claim |
| Placement | Fresh catalog filtering/ranking; secure cloud; any compatible data center; no attached volumes |
| Storage | 5 GB ephemeral container disk; no persistent, Network or Global Volume |
| Bounds | One create request; 300s startup, 60s worker, 120s collection/upload, 120s cleanup; planned total 600s |
| Estimate at maximum unit price | USD 0.50 GPU + 0.000167 storage + 0.50 reserves = **USD 1.000167**; this is not a quote or hard cap |
| Acceptance | Real device admission + CUDA memory round trip, verified exported bundle, GitHub upload receipt, confirmed Pod absence |

MIG remains allowed, but the current worker refuses ambiguous profile identity.
It is ranked last because the consumer GPU names have a direct runtime identity
check. No fallback is outside the allowlist, and no benchmark group is mixed.
A fresh anonymous digest pull and source-label check must pass before creation.
The controller refreshes actual GPU prices/capacity; creation may still lose a
capacity race. An API error is never relabeled as empty stock.

## Entry points and permissions

The generic CLI still defaults to dry-run and refuses `--action run`. This narrowly
scoped module is `python -m runpod_ci_broker.controller`; it requires the exact
repository, main ref, workflow path, Actions run/attempt and fixed campaign task.
Arbitrary configs, images, commands, budgets and source SHAs are not launch inputs.

- **`gpu-smoke.yml`**: manual only, `confirm_paid=false` by default. Once reviewed
  and merged, dispatch on main with `confirm_paid=true` using the existing compute
  approval. `start` is the only command with POST enabled.
- **`recover-smoke.yml`**: manual `confirm_cleanup=true`, or `workflow_run` completion
  of the exact main-branch manual smoke workflow. It never calls create. It checks
  out its trusted default-branch SHA, never PR code or downloaded code/artifacts.
- Both use job-level **`contents: write` and `actions: read`**. Contents write is
  repository-wide; the code's state-only path is not token-enforced branch scope.
  Actions read verifies exact run-attempt completion and artifact metadata/digest.
- `RUNPOD_API_KEY` is available only to start/collect/cleanup steps on the controller.
  Receipt/report steps receive no RunPod key. No account key enters the worker.
- One shared GitHub concurrency group queues these workflows; the CAS journal
  serializes the central Broker scope. Neither locks the whole RunPod account.

The owner approved the earlier state-probe/publication jobs, but these new paid
and recovery workflows extend their use and add Actions read. Review/approval is
needed for this PR's merge and their activation, **not a new USD 20 budget**.
No schedules, paid push trigger, `pull_request_target`, account billing changes or
business repository edits are introduced.

Before the first paid dispatch, run the main smoke workflow once with
`confirm_paid=false`. Its controller job must be skipped and its completion must
trigger recovery, which returns `NO_OWNED_INTENT` without a RunPod operation when
no campaign exists. This verifies the new event route without compute; a passed
route check still does not verify real cancellation cleanup.

## Intent, budget and recovery

The fixed campaign record is never replaced on rerun. Its configuration fingerprint,
original run ID, create intent, selected allocation, attempt count, first-intent time,
reserved dollars and elapsed-cost estimate survive runner loss. Only one create
attempt is enabled in this first tranche. Even an explicit rejection consumes that
attempt; an empty inventory after an unknown response never clears the intent.
A failed preflight also remains a recorded result; this version does not silently
restart scheduling on every dispatch. New attempts need a reviewed extension that
retains the original campaign totals, not record deletion or a fresh allowance.

The entire planned attempt is reserved before POST and read back. After a lost
response, complete inventory and exact name/image/mount/owner checks may recover
its Pod ID. Duplicate or conflicting resources stop destructive operations.
`dataCenterIds` is documented as a placement preference: actual allocation is
recorded separately from the requested candidate and checked at cleanup.

The campaign keeps both the five-hour wall deadline and cumulative resource time.
Only one resource is possible in this tranche, so unresolved lifetime is counted
conservatively from intent to confirmed absence. Estimates include startup and
stopped periods. Observed Pod `cost` is an hourly rate, never a final bill; a rate
above USD 3/hour stops work and starts cleanup. Clock regression fails closed.

The controller's sticky lock records the exact run ID **and attempt**, workflow,
source SHA and event. Only the recovery controller may take it over, after GitHub
reports that exact attempt completed with matching repository/main/source/path.
No TTL or user-supplied stopped flag unlocks it. The CAS handover preserves every
unresolved intent and leaves an audit record. API failure or missing proof keeps
it locked. A rerun's different attempt does not validate the original attempt.

## Export and cleanup order

A random result capability is present only in the create request/Pod environment
and private controller memory. Public state stores its SHA256. A replacement
controller retrieves it via authenticated Pod GET **after** ownership checks and
verifies the hash. It does not create a new GitHub Secret or commit the token.
RunPod's automatic Pod-scoped key is discarded by the worker wrapper unused.

On success, the collector verifies the manifest, all checksums, source/image/Pod
identity and actual CUDA observation, then marks the bundle runner-local only.
The workflow uploads it before normal cleanup. The receipt command verifies the
artifact ID, ZIP digest, workflow run/SHA and fixed artifact name through GitHub,
and only then sets `results_saved=true`. A process success alone is insufficient.
Artifacts retain for 30 days; expiration or later deletion is outside this receipt.

Budget expiry, startup failure, corrupt export or exhausted retries invokes
owner-checked cleanup immediately, before any diagnostic upload. Collection must
finish by intent+420s, leaving 60s for upload and 120s for cleanup in the 600s plan.
An upload failure never retains a GPU indefinitely: cleanup proceeds and results
remain unconfirmed. `workflow_run` recovery can attempt bounded recollection only
within the **original** collection deadline, then terminate regardless of export.

Cleanup checks exact task markers, Pod ID, image, mounts, GPU/count and actual DC.
It requires two direct GET 404s plus two complete inventories without the Pod;
DELETE 204 alone is not confirmation. Stop and volume deletion are absent.

## Remaining limits and evidence

Offline tests exercise the real controller logic with injected clients, including
lost create responses, ledger write failure before POST, duplicate/foreign Pods,
old attempt takeover, clock/budget expiry, collection/upload/delete failures and
CPU/GPU admission distinctions. The prior real state probe and anonymous image
publication evidence remain valid preparation evidence.

No real controller POST, CUDA admission, proxy export, cancellation handover or
DELETE has yet been tested. GitHub run-attempt fields and existing artifact digest
metadata were checked against the real completed publication run during development.

GitHub queue delays, runner loss, unavailable APIs and unconfirmed lock handover can
extend billing past the software deadline. `always()` and `workflow_run` are separate
recovery routes, not a supplier-enforced spending cap. If recovery fails, inspect
the trusted record and rerun the manual recovery workflow; do not erase the lock,
force-push the state branch, or delete resources with uncertain ownership.

Official contracts rechecked on 2026-11-09 (Asia/Singapore):
[RunPod create](https://docs.runpod.io/api-reference-v2/pods/create-a-pod),
[Pod inspection](https://docs.runpod.io/api-reference-v2/pods/get-a-pod),
[storage/transfer pricing](https://docs.runpod.io/pods/pricing),
[GitHub run attempt](https://docs.github.com/en/rest/actions/workflow-runs#get-a-workflow-run-attempt),
[artifact metadata](https://docs.github.com/en/rest/actions/artifacts#get-an-artifact),
[workflow_run](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_run).
