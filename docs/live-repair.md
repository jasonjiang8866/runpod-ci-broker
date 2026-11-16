# Real GPU repair: error reasons and readiness

Prepared 2026-11-17 Singapore time after the user requested fixing the Broker and
running real paid GPU smoke within the existing USD 20 / five-hour campaign.
This code change does not reset historical records or enable another create.
A new immutable worker image and an explicitly pinned successor contract are
required before launch. At most two new ten-minute attempts are planned.

The latest run 43921033128 reached POST /v2/pods and received HTTP 400, before
any Pod ID or result request. The response body was discarded, so the historical
cause cannot be recovered. Runpod's official Go SDK documents that 400 covers
both placement/capacity refusal and request rules:
https://pkg.go.dev/github.com/runpod/go-sdk/v2/lib/pods#Client.CreatePod

Create errors now read at most 16 KiB with a bounded deadline. Only fixed reason
categories leave the reader; no remote detail, credentials, environment or
request content is saved. The exact documented capacity sentence is distinguished
from unknown errors. Authentication and ambiguous outcomes never become retryable
because of a substring. application/problem+json is recognized.

The new worker binds its authenticated status server before running CUDA smoke.
GET /v1/status binds phase to Pod ID, source SHA and task fingerprint. Results stay
unavailable until the complete checksum-verified bundle is published. Legacy
result-server callers remain supported. Repair successors will poll this status
before spending a collection attempt; 401/403 remain terminal and preserved.
This removes an observable readiness gap, but does not claim to explain the old
403. Existing ownership, cost, upload and deletion gates remain in force.

Validation: 364 offline tests, Ruff and strict mypy passed. A real loopback HTTP
test verifies authenticated pre-compute status, 503 for premature result reads,
ready transition, identity mismatch rejection and bundle availability. These are
not cloud/GPU acceptance results.

## Pinned fourth attempt

Worker publication run [43928479656](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/43928479656)
succeeded from source `567edcc32529ad017d13edbc154971aea4d9d624`.
`examples/repair-smoke.json` pins its anonymous GHCR digest. This manual attempt
requires the exact hashes of all three historical records, a completed previous
controller, an explicit 600–3600-second launch window, and fresh complete scope
inventory. It adds one 600-second reservation: cumulative USD 4.000668 / 2400 s,
within the unchanged USD 20 / 18000 s campaign. Reservations are not charges.
Only one new create is enabled here. Reruns cannot create again; recovery follows
this record and never creates. The second planned repair attempt, if needed,
requires reviewing the fourth result before a separate pinned contract is added.
Fresh eligible data centers for the selected GPU are all offered as placement
preferences; actual hardware, price and identity still require runtime admission.

## Observed fourth attempt and one final capacity fallback

[Run 43930077665](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/43930077665)
verified anonymous pull and reached RunPod creation on 2026-11-17 Singapore time.
HTTP 400 carried the exact documented no-instances capacity message. The safe
reason classifier recorded `capacity`; no Pod ID or ambiguous intent remains.
This is direct evidence of a placement refusal, not an API-key authentication error.
The downloadable report artifact 14256259940 was SHA-256 verified:
`28e307570117615581d1060aa028d1a4a28198074c4e02771f61bf26481cb8e1`.

The catalog offered 5090 EU-RO-1 and MIG 24GB US-WA-1; fixed GPU preference chose
the former. The 4090 had an overall LOW signal but no eligible data-center row.
This exposes why repeating the same ranking repeatedly hits a stale placement.
The fifth/final repair contract pins all four record hashes, requires the exact
unambiguous capacity refusal, prefers the already-approved MIG slice and excludes
the refused GPU/data-center pair. Two fresh complete scope inventories separated
by at least five seconds precede its only POST. Unknown create/authentication
failures never qualify. Cumulative reservations become USD 5.000835 / 3000 s,
within USD 20 / 18000 s. This is the second and final new attempt in this repair.

## Final outcome

The fifth attempt [43931230068](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/43931230068)
reached **ACCEPTED** on MIG 24GB EUR-IS-2 with real CUDA verification, downloaded
artifacts and confirmed deletion. Recovery re-entry 43931669505 succeeded without
a new create. [Acceptance results, cost estimate and limitations](evidence/smoke-43931230068.md).
All four predecessor records were rechecked unchanged. No further GPU was launched.
