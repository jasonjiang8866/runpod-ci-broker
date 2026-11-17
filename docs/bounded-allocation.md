# Single-task allocation controller

`allocation.BoundedController` is a prepared controller API for a new task with its
own budget, execution window and finite create attempts. It searches fresh catalog
snapshots, feeds confirmed capacity refusals back into ranking, and completes the
fixed CUDA memory smoke lifecycle. Its tests inject a fake cloud and SQLite journal;
they do **not** demonstrate live rental success or improved availability.

No credential-bearing CLI or workflow activates this API yet. The default `run`
command remains disabled. The historical fixed campaigns use their existing
controller and retain their original records, budgets and recovery rules.

## Admission and budget

The caller supplies a validated `Task`, `ExecutionPermit`, `ControllerJournal`,
`AllocationClient`, GET transport and output directory. The permit binds the exact
task fingerprint and authorization to a controller commit and absolute UTC window
of at most six hours. Both authorization validation and `start(confirmed=True)`
are software gates; neither proves that a person approved spending. A trusted
workflow must independently establish that authorization before activating them.

This version accepts new experiment IDs/scopes in the central broker repository,
but only the previously published repair image/source, fixed memory smoke
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
`receipt` requires GitHub artifact metadata with the matching original run, attempt
in the name (`allocation-bundle-RUN_ID-ATTEMPT`), controller SHA and upload digest.
The trusted workflow must upload this exact verified bundle. A recovery workflow
must retrieve the original artifact or preserve that provenance; it cannot claim
an unrelated upload as the original receipt.

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

Next: add reviewed manual and independent recovery workflow integration, pin its
controller version, verify artifact handover, and prepare a specific paid smoke
with an explicit allowlist, budget, window and cleanup criteria. Keep the existing
worker digest unless the worker changes. A real successful run would establish
this controller's lifecycle only. Real fallback needs an observed capacity refusal;
improved rental success rates need a larger, comparable sample.
