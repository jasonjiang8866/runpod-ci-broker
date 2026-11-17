# Single-task allocation controller

`allocation.BoundedController` is a prepared controller API for a new task with its
own budget, execution window and finite create attempts. It searches fresh catalog
snapshots, feeds confirmed capacity refusals back into ranking, and completes the
fixed CUDA memory smoke lifecycle. Its tests inject a fake cloud and SQLite journal;
they do **not** demonstrate live rental success or improved availability.

`allocation_workflow` supplies a main-only manual entry and independent recovery.
The release manifest must be enabled with a published image digest before paid
creation. The default `run` command remains disabled. The historical campaigns use their existing
controller and retain their original records, budgets and recovery rules.

## Admission and budget

The caller supplies a validated `Task`, `ExecutionPermit`, `ControllerJournal`,
`AllocationClient`, GET transport and output directory. The permit binds the exact
task fingerprint and authorization to a controller commit and absolute UTC window
of at most six hours. Both authorization validation and `start(confirmed=True)`
are software gates; neither proves that a person approved spending. A trusted
workflow must independently establish that authorization before activating them.

This version accepts new experiment IDs/scopes in the central broker repository,
but only a permit-bound immutable worker image/source, fixed memory smoke
entrypoint, one GPU, runtime at most 60 seconds and the worker's existing GPU
allowlist. It requires a minimum CUDA version of at least 12.8, fixed result files
and gates, and no persistent volumes or registry credentials. Hard data-center
constraints are rejected: provider data-center IDs are preferences in this path.
Requested and actual placement are recorded separately. Arbitrary training,
strict reproduction and hardware benchmark execution remain future integrations.

Before the first POST, one conditional journal write reserves the conservative
estimate for **all allowed attempts**, including startup, runtime, collection,
cleanup, storage and configured fixed reserves/prior spend. The integer micro-USD
reservation does not reset or refund on fallback, restart or ambiguous creation.
Each new attempt must still fit the original execution window. This is a
single-task reservation; cross-task shared budget accounting is an M2 deliverable.

The same conditional scope lock and unresolved-intent checks prevent this broker
from opening another task in a scope with an unresolved allocation. This is not
an account-wide lock against other clients or repositories. A recovery controller
can take over a held lock only after exact GitHub evidence confirms the prior
controller attempt completed; a timeout alone is insufficient.

## Search, attempts and recovery

Search persists its query count and individual HTTP request count, including
pagination, before issuing reads. It uses one original wait deadline and bounded
exponential intervals; restarts do not replenish either limit. Complete inventory
is checked again before create. Proposals use the existing `PlacementSolver`
interface, independent validation and quote expiry checks. Third-party optimizers
are not enabled; the current ranked baseline remains the executable solver.

Every attempted create has a stable per-attempt intent ID, request placement,
outcome and timestamps. The durable intent and full reservation precede POST.
The Pod receives exact ownership markers and `RCB_ATTEMPT_ID`. The ephemeral result
capability is hashed in the journal; its raw value and the account API key are
never written to reports. Raw provider error bodies are not retained.

| Observation | Controller behavior |
|---|---|
| Classified capacity refusal, HTTP 400/422 | Persist refusal/cooldown, refresh and choose another eligible candidate within original limits |
| Definitive authorization/permission/account rejection | Stop; do not try other cards |
| Timeout, 429/5xx, malformed create response or unclassified adapter failure | Keep unresolved intent; reconcile exact ownership; never repeat POST |
| Process lost after intent, even before POST may have happened | Treat as unknown; empty inventory does not authorize a new create |
| Cancellation | Persist cancellation, prevent further search/create, clean up owned compute |
| Wrong attempt marker or ambiguous ownership | Refuse adoption/deletion and preserve unresolved state |
| Cleanup unavailable or absence unverified | Keep `CLEANUP_PENDING`, reserve the scope for recovery |

The public methods are `start`, `reconcile`, `collect`, `receipt`, `cancel` and
`cleanup`. `start` requires a manual workflow identity and refuses recovery mode.
`reconcile` only identifies an existing allocation. `collect` polls authenticated
readiness and downloads the bounded result bundle; a failed/timed-out collection
attempts cleanup. An acknowledged create is not worker readiness or acceptance.

The intended trusted caller sequence is:

1. Construct the task-bound controller and call `start(confirmed=True)`.
2. Reconcile/collect if an intent exists; retain any unresolved state for recovery.
3. Upload the verified local bundle, then call `receipt` with the upload outputs.
4. Always attempt `cleanup`, including when upload or receipt validation fails.

Recovery calls reconciliation/collection/cleanup, never `start`. A caller must
provide cleanup on normal failure and an independent recovery workflow for runner
loss. The API itself is not a background reaper and cannot survive runner loss.

## Results and cost evidence

Collection verifies hashes, task/source/image/Pod provenance, manifest gates and
the observed CUDA device. Local export alone leaves `results_saved=false`.
`receipt` requires GitHub artifact metadata with the matching exporting run, attempt
in the name (`allocation-bundle-RUN_ID-ATTEMPT`), controller SHA and upload digest.
The trusted workflow must upload this exact verified bundle. Recovery can export a
newly collected verified bundle under its own run/attempt and SHA, after validating
the original dispatch identity and completion. The original allocation identity
remains unchanged; unrelated uploads cannot satisfy the receipt.

Deletion requires exact attempt ownership, direct absence and two complete absent
inventory observations. `ACCEPTED` requires a passing smoke, verified durable
upload receipt, confirmed deletion and no cancellation. A valid late receipt can
complete acceptance after cleanup without retaining a billable Pod for upload.

Reports contain query rounds, per-attempt outcomes, requested/actual placements,
reservation and available runtime observations. Observed compute cost uses elapsed
time from intent through confirmed deletion at the highest observed Pod hourly
rate; it is an estimate, not an invoice. If no rate was observed it stays null.
Supplier billing remains null. Provider outages can exceed software deadlines;
there is no provider-enforced per-task total-dollar cap or zero-overspend guarantee.

## Verification and next activation gate

`tests/test_allocation.py` exercises the controller with the real conditional
journal and fake provider, including capacity fallback through smoke export,
receipt and deletion; crash/restart; stale/window/HTTP/attempt limits; cancellation;
wrong ownership; failed deletion and recovery handover. Existing offline CI runs
these tests without cloud credentials.

The manual `allocation-smoke.yml` and `recover-allocation.yml` share the historical
campaign concurrency group. The reviewed `examples/allocation-smoke.json` carries
all six prior reservations (USD 6.001002 / 3600 seconds) into the USD 20 / 18000-second
campaign. Before starting, the wrapper checks the accepted sixth record's hash,
all predecessor hashes and the previous controller's completed status. It never
writes the historical scope. Three permitted attempts reserve an additional
USD 2.000500 and 1800 lifecycle seconds; total retained reservation is USD 8.001502.
The new manual execution window leaves 600–1800 seconds, and resumes preserve it.

All listed eligible data centers are searched, with no task region restriction.
An explicit capacity refusal cools that GPU/data-center pair for the whole
600-second search window. GPU preference begins with 4090 and never substitutes
an unapproved card. Exact IDs include the consumer GPUs, PRO 6000 Server,
Workstation, Max-Q and MIG 1g.24gb/2g.48gb, as listed in the
[official RunPod GPU table](https://github.com/runpod/docs/blob/main/references/gpu-types.mdx).
Host CUDA is a numeric minimum of 12.8; runtime identity remains independently checked.

Publish the expanded worker, pin its source SHA/digest and enable the manifest,
then run the specific paid smoke and verify independent recovery. A successful run would establish
this controller's lifecycle only. Real fallback needs an observed capacity refusal;
improved rental success rates need a larger, comparable sample.

## First live search and diversity continuation

[Run 44109623354](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/44109623354)
received three definitive capacity refusals: 4090/EU-RO-1, 5090/EU-RO-1 and
4090/US-IL-1. No Pod was created. Independent recovery
[44109871190](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/44109871190)
passed. This exercised real capacity feedback but did not pass GPU acceptance.

The remaining equally LOW stock candidates included untried PRO 6000 and MIG.
The availability policy's preference tie-break could still concentrate attempts
on recently refused models. Optional `diversify_after_refusal` now ranks recent
model and center refusals before stock, price and preference. These are temporary
exploration penalties, not allocation probabilities or new hard exclusions.
Other valid placements of a refused model remain eligible. Without active feedback,
or when this option is disabled, the prior ordering is retained.

The reviewed continuation `broker-allocation-20261117-diverse` verifies the first
search's immutable record hash, completed controller, exactly three definitive
refusals, no Pod/unknown intent and retained USD 8.001502 reservation. It imports
those actual refusals into the new task's history, retains the exact original
UTC deadline **1794929408** (2026-11-17 23:30:08 Singapore), and permits at most
three further attempts. No counters or reservations in the first record are reset.
The total conservative reservation is USD 10.002002 / 7200 seconds, within the
original USD 20 / 18000-second campaign. The unchanged published worker is reused.
