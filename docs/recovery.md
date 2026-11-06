# Recovery and result transport contract

## What is implemented now

The local journal is atomically replaced and fsynced before each critical state.
A per-scope flock prevents overlapping controllers sharing that filesystem.
`FakeProvider` persists its independent inventory and fault counters. Restoring the
same journal/provider files reproduces a stopped simulation. `Engine.run` resumes
execution and collection; `reconcile`/`cleanup` never create. No live destructive
adapter exists. The engine explicitly refuses a non-fake provider.

| Interruption / failure | Implemented fake behavior | Remaining live gate |
|---|---|---|
| Before create | fsync identity and intent before provider invocation | Remote durable intent receipt before POST |
| After create request, response lost | CREATE_UNKNOWN; inventory match or remain unresolved; no blind POST | Real inventory pagination, eventual-consistency horizon, durable intent |
| Duplicate workflow | Same experiment/config journal resumes; changed config refused | Shared authoritative ledger across runner replacements |
| Pull/start failure | RUN_FAILED; bounded diagnostic collection; owned cleanup | Real system logs and runtime admission |
| Worker crash / timeout | Exit code distinct from acceptance; collect then terminate | GPU evidence, worker watchdog, external controller deadline |
| Collection failure | Bounded attempts/time; hashes and provenance; diagnostics retained | Authenticated bounded streaming, independently accessible checkpoint |
| DELETE unavailable | CLEANUP_PENDING, false deletion fact; independent reconciliation | Live termination + direct GET/complete inventory confirmation |
| Ambiguous ownership | No destructive operation; fixed diagnostic code | Trusted journal access and account permission review |
| Runner cancelled / killed | Simulated intermediate-state recovery tests | Durable journal and independent recovery runner; not finally-only |

The fake inventory intentionally supports empty reads after successful create.
An unresolved POST is never cleared on absence alone. Recovery with no trusted
record fails closed; raw name-prefix cleanup and account-wide deletion are forbidden.
A known Pod ID and two complete absence observations are required for mock deletion
confirmation. The run ID is attribution; task identity is stable across reruns.

## Phase-three export design (not implemented)

1. A reviewed immutable worker image includes a generic wrapper. Before starting
   user code, capture `nvidia-smi`, CUDA runtime/driver, GPU UUID/model/count/VRAM,
   compute capability, dependency versions, source SHA and input hashes. Reject CPU
   fallback, wrong hardware, unavailable int8/kernel support or identity mismatch.
2. Execute the validated argv directly, with a process-group timeout inside the
   controller's work deadline. A worker exit is not Pod termination.
3. Write logs and metrics locally, publish a final manifest with exact file names,
   byte lengths, SHA256, exit status and source/config/image identity. Add periodic
   snapshots for long experiments. Project-specific metric thresholds stay in config
   or project validators, not the Broker provider.
4. Serve the allowlisted results through a per-task HTTPS bearer capability, or use
   a pre-approved short-lived object upload capability restricted to that task prefix.
   The account RunPod key and GitHub management token must never reach the worker.
   No arbitrary URL, redirect, archive extraction or account credential in a URL.
5. Runner streams within byte/time limits, checks each digest, writes temporary
   local files, fsyncs and records the completion manifest last. Publish to GitHub
   artifacts and, if required, a separately approved persistent result destination.
6. If the collection deadline expires, preserve the sanitized diagnostic record
   and terminate owned compute. Retry export independently from durable storage;
   do not create a recovery Pod or prolong GPU billing without separate authority.

Current `save_bundle` validates a supplied in-memory fake bundle and saves it
outside fake Pod storage. It is **not** the network exporter. The real design must
also protect against process restarts, partial upload and large streaming results.
No durable-storage provisioning is authorized by the design document.

## Cancellation and scope

GitHub may send signals and later kill the runner process tree. Code cannot
reliably run cleanup after SIGKILL or runner loss. An `always()` step and a finally
block help ordinary exits but do not close this gap. Before enabling paid launches:

- Choose one central scheduling repository and a durable journal with conditional
  writes/leases. Document which callers it serializes; do not claim an account lock.
- Persist intent externally **before** create, recover the same identity on rerun,
  and keep failed-create ambiguity active until examined.
- Provide independent main-only manual recovery and a bounded expired-task check,
  both restricted to exact owned records. Review any schedule and permissions
  separately. Do not turn on live account cleanup in this MVP.
- Keep enough time for export and termination inside the budget estimate, then
  report unverified cleanup immediately if provider access fails.

A future account-wide hard cap, server-side lease or new idempotency API must be
verified from the then-current contract before relying on it.
