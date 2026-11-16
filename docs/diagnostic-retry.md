# One diagnostic successor for the unresolved HTTP 403

## Investigation findings — 2026-11-16

PR [#10](https://github.com/jasonjiang8866/runpod-ci-broker/pull/10) and
[#11](https://github.com/jasonjiang8866/runpod-ci-broker/pull/11) are merged at
`6e9b35a5553a8a06c76e59cc2b92811f27a5bcfe`. Main offline validation and image
build both passed. The [original runner log](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/42596594291/job/129090627563)
and saved report contain no response headers, body or Pod startup logs. The
deleted Pod cannot provide a retrospective observation of those missing fields.

| Check | Observed conclusion |
|---|---|
| Result URL | Uses the documented HTTPS Pod/internal-port proxy format |
| Service configuration | Create requests expose `8000/http`; worker binds `0.0.0.0:8000` |
| Worker HTTP behavior | Invalid capability returns 401; no handler branch emits 403 |
| Startup sequencing | HTTP server starts after CUDA probe; Pod RUNNING alone cannot establish readiness |
| Management credentials | Account key is used for management APIs, not result requests; result token is separate |
| Root cause | Unknown: original evidence cannot distinguish runner intermediary, Cloudflare or Runpod load balancer |

Contracts checked: Runpod's [port guide](https://docs.runpod.io/pods/configuration/expose-ports)
documents the Cloudflare/load-balancer/Pod route and warns that RUNNING can precede
service readiness. Its [SSE log API](https://docs.runpod.io/api-reference-v2/pods/stream-pod-logs)
matches the bounded diagnostic implementation. Cloudflare documents
[`cf-mitigated: challenge`](https://developers.cloudflare.com/cloudflare-challenges/challenge-types/challenge-pages/detect-response/)
as a challenge-page indicator. The collector now stores only a boolean hint for
that value. Header absence does not exclude a WAF block; no original challenge
response has been observed. No authentication, proxy, TLS or User-Agent changes
are made. 401/403 still stop collection and enter cleanup.

## Reviewable launch contract — not yet authorized or executed

The original five-hour continuous window ended on **2026-11-09 22:51:50.875960
Asia/Singapore**. Existing USD 20 and cumulative 18,000-second caps remain; this
proposal needs approval for a new bounded execution window, not a fresh budget.

| Item | Proposed value |
|---|---|
| Workflow | Existing `gpu-smoke.yml`, main, manual dispatch only |
| Inputs | `confirm_paid=true`, `diagnostic_retry=true`, `retry_window_end=<approved UTC epoch seconds>` |
| Window | Explicit absolute end; 600–3600 seconds must remain at creation |
| Runtime | One additional Pod; planned lifecycle <=600s, worker <=60s |
| GPU | Fresh price/capacity check; prefer 4090, then 3090, 5090, MIG 24GB; <=USD 3/GPU-hour |
| Image/source | Original reviewed digest/source from `examples/approved-broker-smoke.json`; no publication needed |
| Prior reservation kept | USD 1.000167 / 600 seconds |
| Additional reservation | USD 1.000167 / 600 seconds |
| Combined reservation | USD 2.000334 / 1200 seconds; software estimate, not a supplier hard cap |
| Persistent storage | None; 5 GB ephemeral container disk |
| Stop | First terminal HTTP denial, deadline, ownership uncertainty or price violation; no third create |
| Acceptance | CUDA admission, complete verified result files, GitHub artifact receipt and two independent absence rounds |

`retry_window_end` is a technical guard, not evidence of human approval. Confirm
the approved window and current source/checks before setting it. Defaults remain
false/empty. No new workflow permission, trigger, registry publication or account
setting is introduced.

## Accounting and recovery

- Keep the original `broker-smoke-20261107` record unchanged. Write the sole
  successor `broker-smoke-20261107-diagnostic-2` in the same CAS scope document.
- Require original REJECTED state, completed task, confirmed deletion, exact
  retained reservation and bounded observed costs/time. Refuse unknown/increased
  accounting rather than refunding or silently changing the prior spend.
- Verify the previous controller's exact Actions attempt completed, even when
  its lock was released. Refuse creation while another scope resource/intent exists.
- Link the original record by canonical SHA256. Its inspected hash is
  `46bda3979ff7559b393bfab767341a44fbc25596a2b15736ea582fff5e4c0511`.
  The live implementation derives and verifies the record; this documented hash
  is evidence of the inspected snapshot, not an alternative state source.
- Reserve both attempts' dollars and time before the one successor POST. Count
  previous observed lifetime plus the new attempt's lifetime; retain the prior
  full cost reservation in the conservative cost estimate.
- Repeated dispatch only resumes the successor; neither a new end time nor a
  lost POST/checkpoint response grants another POST. Failed preflight also stays
  recorded; it does not silently restart on every dispatch.
- Collect/receipt/report and independent recovery select the successor once
  present. Recovery never creates it. Original state and cleanup evidence survive.

## Verification

334 local tests, strict mypy across 24 modules, Ruff and wheel/sdist build passed.
Tests cover missing/expired/nonfinite/overlong windows, immutable original record,
aggregate reservations, 4090 preference, changed prior accounting, unconfirmed
controller completion, missing/unresolved predecessors, recovery creation refusal,
lost create/checkpoint responses, second-denial cleanup and no third create. The
actual recovery CLI was exercised with injected cloud clients. The real original
ledger was also passed through the pure reservation calculation locally without
writing to GitHub or calling Runpod.

No new live Pod, supplier charge, current inventory observation, live log response
or successful GPU result is claimed. The original 403 remains unresolved until
new direct evidence is obtained.
