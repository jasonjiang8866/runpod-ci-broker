# GPU incident reflection and repeat regression

2026-11-17, Singapore time. The user requested a reflection and one further paid
GPU regression after the successful [fifth attempt](evidence/smoke-43931230068.md).

## What the investigation should have done better

1. **Separate stages before assigning causes.** A result-proxy HTTP 403 and a
   Pod-create HTTP 400 are different failures. Working waferVLM, simplexLoRA-bench
   and spleeterpt paths were useful controls, but matching request headers was
   only a hypothesis. The historical 403 response origin remains unverified.
   Keep the operation, status and safe reason classification in every diagnosis.
2. **Preserve useful evidence without publishing remote payloads.** Discarding
   all create-response bodies concealed capacity refusals. Bounded reads and a
   fixed allowlist of reason classes now distinguish a documented capacity
   response without retaining raw error text or credentials.
3. **Treat catalog capacity as advisory.** Repeating the same deterministic GPU
   and data-center ranking did not test a new hypothesis. A definitive placement
   refusal now excludes that pair during the approved successor; ambiguous
   outcomes and authentication errors never qualify for another create.
4. **Verify the application before collecting.** A RUNNING Pod is insufficient.
   The worker now starts its authenticated status endpoint before CUDA work;
   the controller checks the task and Pod identity before collecting results.
   Startup 404s can be transient; 401/403 remain terminal and trigger cleanup.
5. **Separate offline coverage from real acceptance.** Hundreds of tests did not
   prove cloud routing, real CUDA execution or durable result upload. Acceptance
   requires independent GPU, checksummed bundle, upload-receipt and deletion
   evidence. A second real run tests repeatability, not model-training quality.
6. **Avoid growing permanent control flow for every experiment.** The current
   fixed campaign adds a pinned successor for each approval. It is auditable but
   cumbersome. A later change should separate reviewed run manifests from the
   controller while retaining once-only intent, budgets and recovery guarantees.
   This regression deliberately keeps the working image and workload unchanged.

## Sixth attempt contract

`examples/regression-smoke.json` retains the successful worker digest, CUDA
memory-roundtrip workload and MIG-first approved GPU preference. Its only task
changes from attempt five are the experiment ID and cumulative prior reservation.
No worker publication or new GPU type is needed.

The new manual `regression` input permits exactly one additional create after
verifying all five predecessor hashes and the fifth record's ACCEPTED state,
GPU verification, upload receipt and confirmed cleanup. The launch window must
leave 600–3600 seconds. Two complete scope inventories precede reservation and
creation. The failed fourth-attempt placement remains excluded.

Maximum worker compute is 60 seconds; total lifecycle allocation is 600 seconds.
The USD 3/hour ceiling and USD 20 / 18000-second campaign remain unchanged.
Retained reservations become USD 6.001002 / 3600 seconds; these are conservative
allocations, not supplier charges. Re-entry cannot create a seventh Pod. Recovery
follows attempt six once it exists and can only collect or clean up.

The regression must verify fresh CUDA work, authenticated readiness, downloaded
file checksums, GitHub upload receipt and direct-404 plus two-inventory cleanup.
It does not establish a root cause for the old 403 or validate model training.

## Regression preflight finding

[Run 43936964886](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/43936964886)
was blocked before creating anything: MIG, 3090 and 4090 had no eligible capacity;
the only 5090 placement was the explicitly excluded prior refusal. Attempts stayed
zero, create intent false, and no campaign reservation was made. Complete inventory
reported no active Pods. This is not a paid GPU regression result.

This exposed an overly broad rerun guard: a persisted catalog refusal prevented
rechecking even though no POST or budget reservation had occurred. The sixth
contract now allows at most two manual catalog rechecks only for this exact
unspent state. It retains each complete preflight record and still requires the
same verified predecessors, manual window and absence checks. Any reservation,
create intent, Pod identity, other blocker or exhausted recheck allowance keeps
the guard closed. Recovery cannot open a recheck. The total paid-create allowance
remains one.

## Verified regression outcome

The first two manual entries (43936964886 and 43938023026) found no eligible
capacity and created no Pod. Their complete records remain in the sixth record's
`preflight_history`, with attempts zero and no reservation. The independent
[read-only diagnostic](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/43938425393)
subsequently found MIG capacity under the existing minimum-CUDA condition, without
changing admission rules.

[Run 43938726780](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/43938726780)
then reached **ACCEPTED** from controller commit
`0c23858d9cc342e900b6f2fac43d2ddb2e23a5d6`, using the same worker digest as the
previous acceptance. It created exactly one RTX PRO 6000 Blackwell MIG 24GB Pod.
A transient readiness 404 was followed by authenticated readiness; exactly one
bundle collection verified a 4096-byte CUDA memory roundtrip, smoke metric 1,
and worker exit code 0. The downloaded archive and its inner file hashes were
independently checked against the manifest, task and GitHub upload receipt.

Direct GET 404 plus two complete inventory checks confirmed deletion. Lifecycle
from intent to confirmed deletion was 78.176 seconds at USD 0.69/hour, about
**USD 0.015 estimated compute** (not an invoice). The two catalog-only entries
incurred no GPU charge or additional campaign reservation. All five predecessor
hashes remain unchanged; the retained total is USD 6.001002 / 3600 seconds.
[Recovery 43939097647](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/43939097647)
completed successfully and did not create another Pod.

PRs #18 and #19 passed GitHub tests and worker-image offline builds; 390 local
tests, Ruff and strict mypy passed. The regression demonstrates repeatable GPU
smoke, result transport and cleanup, including recovery from an unspent capacity
preflight. Historical 403 attribution and model-training acceptance remain
unverified. Public documentation retains this summary; complete downloaded
artifacts are provided separately.
