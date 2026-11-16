# Startup / HTTP 403 investigation — 2026-11-16

Update: PR #10 and #11 are merged; both main-branch checks passed at
`6e9b35a5553a8a06c76e59cc2b92811f27a5bcfe`. The investigation and executable
[single successor proposal](diagnostic-retry.md) below preserve the original
evidence and require a renewed execution window. No second Pod has been launched.

The first paid Pod was deleted before its raw response or startup logs were
captured. Its precise HTTP 403 source cannot be reconstructed from the saved
report. `RUNNING` alone does not prove that the worker's HTTP server is listening.
The worker returns 401 for a bad capability, but that does not identify who sent
the observed 403. No confirmed transport fix is claimed here.

## Implemented diagnosis

- Project the already ownership-checked Pod GET into a bounded timeline: elapsed
  time, provider state, presence of runtime metadata, and whether `8000/http` is
  declared. Keep the first and latest 15 samples; never retain raw env or addresses.
- Record only presence of proxy environment variables, never their values. Do not
  bypass an environment proxy or change TLS, authentication, User-Agent or routing.
- On HTTP failure, retain fixed header categories and presence flags. Never read
  the error body; close the response. Cloudflare/nginx hints are not authenticated
  evidence of the responder or cause. 401/403 remain non-retryable.
- On terminal Pod startup failure or a non-retryable result HTTP failure, read
  `GET /v2/pods/{id}/logs?tail=100` at most once, only after ownership checks.
  Preserve only event counts and fixed hints (image pull, worker refusal, Python
  traceback, CUDA initialization, port binding). Discard raw text and timestamps.
  A missing hint does not prove absence; a hint is not an acceptance result.
- Log sampling has a three-second deadline, at most one additional one-second
  socket wait, and a 64 KiB cap. Skip when less than four seconds remain in the
  existing collection/window deadline. Persist intent before this GET; a lost
  runner cannot cause repeated reads. 403 on the log API is recorded separately
  from 403 on the result endpoint. No automatic credential/permission changes.
- Diagnostic failure proceeds to existing ownership-checked deletion. Success
  still requires the original CUDA result, manifest hashes, upload receipt and
  two direct 404 / complete-inventory absence checks.

Official contracts inspected on 2026-11-16:
[Pod GET](https://docs.runpod.io/api-reference-v2/pods/get-a-pod),
[SSE log GET](https://docs.runpod.io/api-reference-v2/pods/stream-pod-logs).
The API exposes an ongoing SSE stream, so an idle timeout can leave useful partial
evidence. No raw stream is printed or uploaded.

## Verification and limits

310 tests pass, including a real loopback HTTP SSE stream that sends a partial
record then stalls. Fault cases cover oversized/trickling/malformed logs, 302/401/
403 responses, secret omission, ownership mismatch, deadline exhaustion, resumed
diagnostic intent and deletion after a failed diagnostic. Ruff, strict mypy
(24 modules), wheel and source distribution builds pass. An additional integration
test exercises the real collector/controller path with an injected HTTP 403 stream,
then verifies stream closure, safe JSON/Markdown diagnostics, cleanup and no second
create. Startup hints are persisted through existing journal writes without an
additional write on each Pod observation.

These are local/offline tests; neither the new log API nor new diagnostics have
been exercised on a live Pod. The worker/image pins, workflow permissions,
authentication, one-attempt cap and original campaign record are unchanged.
This revision creates no paid resources and does not enable a second attempt.

## Exact accounting and proposed resumption

The `broker-state` record still shows the original attempt as `REJECTED`, with
verified Pod deletion and a released lock. Original values are preserved:

| Field | Stored value |
|---|---|
| Intent to confirmed deletion | 30.945142 seconds |
| Conservative elapsed cost, including reserves | USD 0.525796 (not an invoice) |
| Entire-attempt reservation retained | USD 1.000167 / 600 seconds |
| Original total caps | USD 20 / 18,000 seconds |
| Original continuous wall deadline | 2026-11-09 22:51:50.875960 Asia/Singapore |
| Remaining dollars using the unrefunded reservation | USD 18.999833 |

The old continuous execution window has expired. Do not erase the record, advance
its timestamp, change its fingerprint in place, or start a new USD 20 allowance.
Current `start` resumes the old terminal record and cannot issue another POST.

Proposed next tranche, pending approval of a renewed execution window and review
of its executable attempt extension: one additional Pod, <=600 seconds including
startup/collection/cleanup, worker <=60 seconds, original four-GPU allowlist and
USD 3/GPU-hour ceiling. Prefer 4090 if compatible and available at a fresh quote;
fallback must stay in that allowlist. Keep the reviewed worker digest. Carry the
original reservation/spend and all history forward; reserve <=USD 1.000167 more
(combined reservation <=USD 2.000334, not a supplier hard cap). No automatic third
attempt. No persistent storage. Require fresh complete inventory, exact source and
passing CI before any new create intent.

If 403 recurs, preserve the new lossy diagnostics, verify deletion, and stop for
analysis. Do not turn the refusal into repeated retries or change identity/routes.
