# Diagnose the result endpoint without resetting the campaign

Prepared on 2026-11-16 (Asia/Singapore). This change adds evidence collection;
it does not identify or repair the 403 from
[the first smoke](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/42596594291).
That Pod was deleted before these diagnostics existed. Historical fields stay
unknown; no new observations may be backfilled into the old report.

## Evidence collected on a subsequent authorized attempt

The collector uses its existing owner-checked Pod GET and existing result request.
There are no new requests, endpoints, permissions, workload commands or image pins.

| Signal | Recorded projection | What it can establish |
|---|---|---|
| Pod startup | Known status, elapsed seconds, whether `8000/http` is configured, runtime-object presence | Provider metadata at collection time; not application readiness |
| HTTP failure | Existing status/stage plus fixed server/media categories and presence flags for CF-Ray, Via, Location and WWW-Authenticate | Clues for a next hypothesis; headers are not authenticated origin evidence |
| Runner proxy settings | Four booleans for HTTP_PROXY, HTTPS_PROXY, ALL_PROXY and NO_PROXY (including lowercase) | Whether settings exist, not the effective route or which hop denied access |

The first and latest 15 Pod samples are retained, along with an observation count.
No raw Pod environment, IP address, proxy address/credentials, header values,
response body or redirect destination is written to the journal or report.
The HTTP error stream is closed without reading its body. The result capability
continues to be sent only to the original HTTPS endpoint in the Authorization header.
401/403 still end collection immediately and enter existing ownership-checked
cleanup. Redirects remain disabled. Diagnostic fields do not affect admission,
retry policy, budget or deletion decisions. A missing field means unknown.

## How to interpret the next failure

1. Compare the port configuration with the fixed creation request. A missing or
   false signal is a configuration lead, not proof that a proxy produced the 403.
2. Check Pod status/runtime evidence alongside HTTP evidence. `RUNNING` alone
   does not prove that the worker finished CUDA or bound its server socket.
3. A recognized server name or CF-Ray presence suggests a possible intermediary;
   it does not prove the denial originated there. A proxy configuration boolean
   also does not justify bypassing that proxy, altering authentication or TLS.
4. Stop and verify cleanup after a denial. Use the recorded distinctions to select
   the next investigation, such as authorized provider-side startup logs; do not
   automatically retry 403, change GPU or create a replacement Pod.

The old image returns 401 for failed worker authentication and has no 403 branch.
Neither that code fact nor these added hints proves whether the worker started.
No new worker build is needed to collect these controller-side observations.

## Retry boundary discovered in the live ledger

Read-only inspection found campaign `broker-smoke-20261107` at revision 32,
unlocked, rejected, one attempt consumed and Pod deletion confirmed. It retains
USD 1.000167 reserved and USD 0.525796 conservative elapsed estimate (not invoices).
The USD 20 total and five-hour limits remain part of the contract.

The implementation's absolute wall deadline is first intent + 18,000 seconds.
It has passed. A rerun cannot reopen it or issue a second POST. No ledger or
authorization document is modified by this change. Before another paid attempt,
define a new execution window and review an explicit attempt extension that keeps
the original campaign identity, attempt history, reservations, cost estimates and
aggregate resource seconds. Do not erase the state, alter its fingerprint or
reinterpret the old deadline as a fresh five-hour allowance.

## Verification

Unit tests inject hostile header, proxy and Pod values; the report retains only
fixed categories. The real collector/controller path is exercised with an injected
HTTP 403 stream, verifying stream closure, rejection, no second result request,
no second create, immediate fake-provider cleanup and secret-free JSON/Markdown.
This is offline validation, not a live RunPod result or proof that 403 is fixed.

Official contracts consulted:
- [HTTP proxy port configuration](https://docs.runpod.io/pods/configuration/expose-ports)
- [Pod GET fields: status, ports and runtime](https://docs.runpod.io/api-reference-v2/pods/get-a-pod)
