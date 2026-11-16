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
