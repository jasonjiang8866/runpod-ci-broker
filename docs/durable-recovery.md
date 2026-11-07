# Durable recovery and result transport: implementation tranche

This tranche advanced phase 3 preparation without enabling paid execution.
`Engine` still accepts only `FakeProvider`. A separately approved follow-up has
verified real GitHub Contents conditional writes/conflicts and release, and
published the worker image. See [activation evidence](evidence/activation-20261107.md).
Live RunPod proxy export, paid controller integration and lock handover remain
unverified; the separate ownership-checked GET/DELETE adapter is not enabled by
the CLI or workflows. No RunPod resources have been created.

## Shared intent and scope serialization

`SharedJournal` implements the same `TaskJournal` interface as the local journal.
A single scope document contains a revision, process-unique holder and all task
records. A conditional update changes the lock and task records atomically. Every
write is read back before returning, including the write immediately before create.
A lost response is resolved by comparing the exact proposed document; it is never
retried blindly. Read failure, version conflict or uncertain receipt stops progress.

Each task retains config fingerprint, original Actions run ID, resume IDs, source
SHA, image, Pod ID, attempts and state transitions. Malformed or cross-scope records
block the scope. A different task remains blocked by unresolved create intent even
when the provider's inventory is temporarily empty. The original owner markers
are retained when another runner resumes.

There are two storage implementations:

| Backend | Implemented behavior | Verified boundary |
|---|---|---|
| `SQLiteStore` | Transactional compare-and-swap, shared file, independent processes | Real local concurrency and process-crash tests; same shared disk only |
| `GitHubContentsStore` | GET/PUT fixed `api.github.com` Contents path, old blob SHA on update, base64/blob-hash validation, no redirects or write retries | HTTP fixtures plus real manual write/conflict/release probe; stopped-controller handover remains unverified |

The prepared remote backend requires an **existing** `broker-state` branch and
an explicitly supplied repository identifier. It never creates a branch, changes
permissions or writes main/workflows. `RCB_LEDGER_TOKEN` is read from the controller
environment only; it must be a separately approved repository-scoped capability
with Contents write permission. Ordinary PR jobs remain Contents read-only and
receive no token. There is deliberately no remote-backend CLI switch yet.
Do not substitute `RUNPOD_API_KEY` or pass the ledger credential into a worker.

State is limited to 500,000 bytes. Hitting the limit fails closed; there is no
implicit pruning of unresolved tasks. Git history may disclose task metadata if
the state branch is public. Review the target's visibility and retention before
activation. State writers are trusted: malicious edits, resets and rollback by a
repository administrator are outside this lock's protection.

### No expiry-based lock stealing

A holder is a fresh process nonce, not an Actions run ID reusable by concurrent
retries. A killed process leaves a sticky lock. It is **not** automatically stolen
because RunPod does not participate in the journal's fencing protocol: a paused
old controller could otherwise resume and create after a newer owner took over.
A failed release is reported as unresolved, not silently treated as unlocked.

This tranche intentionally provides no remote lock-reset command. Before live
activation, an independent recovery route must verify the old controller has
stopped, retain the exact trusted record and unresolved intent, and audit the
conditional ownership transfer. An arbitrary timeout, empty Pod listing or
`always()` completion is insufficient. Tests demonstrate this fail-closed boundary,
not automatic recovery from every GitHub cancellation scenario.

## Offline recovery drill

```bash
uv sync --locked --group dev --python 3.12
uv run --locked runpod-ci-broker \
  --config examples/simplexlora-flexible.json \
  --action simulate --mock-catalog examples/mock-catalog.json \
  --shared-mock-ledger .broker/shared.db --state-dir .broker/drill --output out/shared

# Independent entry point: never creates a new Pod, even in simulation.
uv run --locked runpod-ci-broker \
  --config examples/simplexlora-flexible.json \
  --action reconcile --mock-catalog examples/mock-catalog.json \
  --shared-mock-ledger .broker/shared.db --state-dir .broker/drill --output out/recovered
```

The CLI option is limited to simulate/reconcile/cleanup. Preserve the fake provider
file and result directory as well as the SQLite file when replaying a mock run.
SQLite is not an off-runner durable destination for independent GitHub machines.
The report identifies `shared_sqlite_mock`; it never relabels this as GPU evidence.

## Result-only transport

`result_server` serves a finalized, immutable in-memory bundle through two GET
routes: `/v1/manifest` and exact `/v1/files/<allowlisted-name>` paths. It has no shell,
filesystem browser, upload or management endpoint. A fresh 256-bit capability
(`secrets.token_hex(32)`) is checked using constant-time comparison. No access-log
output includes headers or request paths. The server defaults to localhost; binding
to a Pod interface requires a reviewed TLS proxy and port configuration.

`RunPodResultReader` derives a single HTTPS origin from a validated Pod ID and port:
`https://<pod-id>-<port>.proxy.runpod.net`. It does not accept arbitrary URLs or
redirects. Tokens are only in the Authorization header. RunPod documents that
this proxy is public and requires application authentication; the capability is
therefore mandatory, not optional protection based on an obscure Pod ID.

`collect_https` accepts the controller's absolute monotonic deadline. It streams
with a maximum five-second socket idle timeout, reads bounded chunks, and checks
time between reads. One in-flight read can overrun the deadline by up to that idle
timeout; this is not a provider spending cap. It performs no retries itself. A
future controller must share one total collection deadline across bounded retries.

Manifest size is limited to 64 KiB, total export to 64 MiB for this small-experiment
implementation, and paths to the task allowlist. Truncated, excessive or corrupt
bodies, wrong provenance, malformed metrics and file-set mismatches fail validation.
The collector verifies in a sibling staging directory, writes the manifest last,
and atomically renames only the complete bundle. Failed staging is removed. Existing
export destinations are refused instead of overwritten. Resume verification now
rechecks Pod/source/image/config identity, sizes, hashes, symlinks and metrics-file
agreement, rather than relying solely on a configuration hash.

A saved bundle is not an accepted experiment: threshold failure retains diagnostics
and returns false. Export on the runner is also not durable artifact publication.
The future paid controller must upload to GitHub artifacts or another approved
durable destination and record that independent receipt before declaring results
safe outside the transient runner. Export failure must not extend GPU billing past
the collection/cleanup reserve.

## Evidence and remaining live gates

- Real local tests: concurrent processes compete for one scope; a killed process
  leaves a lock; loopback HTTP requires the result capability and exports a verified
  bundle. This is CPU/offline evidence, with no RunPod API calls.
- Fault fixtures: ambiguous create plus runner replacement creates only once;
  collection and deletion failures recover through a new journal; foreign resources
  remain protected by the existing ownership checks; lost journal writes reconcile
  by read-back, and unconfirmed intent never reaches create.
- Now verified separately: authenticated remote ledger round trip/conflict behavior
  and immutable public worker publication; see the activation evidence above.
- Still unverified: live lock handover, actual CUDA admission/execution, private
  image pull, live proxy export, GitHub artifact receipt and RunPod deletion checks.

The next implementation tranche is the live recovery/controller integration.
The pinned worker and separate manual state/publication permissions are now approved.
The [campaign approval](approved-smoke.md) is USD 20 total, five hours and
USD 3/GPU-hour, with the four-model allowlist and one active single-GPU Pod.
Use its short first-smoke specification and shared cleanup reserve; do not ask for
the same compute authorization again or reset its allowance on retries.

Official contracts consulted on 2026-11-07:

- [GitHub Contents API: conditional update SHA and permissions](https://docs.github.com/en/rest/repos/contents#create-or-update-file-contents)
- [GitHub concurrency scope](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency)
- [RunPod HTTP proxy: URL, HTTPS, public access and timeout](https://docs.runpod.io/pods/configuration/expose-ports)
