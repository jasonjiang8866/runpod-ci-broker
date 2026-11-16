# Authorized live transport retest — 2026-11-17 Singapore

The user requested an actual paid RunPod test after reviewing the cross-project
HTTP client comparison. This renews one bounded execution opportunity within
the existing USD 20 / 18,000-second campaign; it does not reset either prior
attempt or grant an unlimited retry loop.

The live ledger was read again before implementation. Original record hash:
`46bda3979ff7559b393bfab767341a44fbc25596a2b15736ea582fff5e4c0511`.
Second record hash:
`9e66483324cd361af71486d7459b8d2adb22d3e88a7addaa3ce44e3a5ba71916`.
The second record is a definitive create refusal, with no Pod ID or unresolved
create intent. Both records and their reservations remain unchanged.

| Launch field | Bound |
|---|---|
| Workflow / branch | `gpu-smoke.yml`, reviewed and tested `main` |
| Inputs | `confirm_paid=true`, `transport_retest=true`, `diagnostic_retry=false`, explicit `retry_window_end` |
| Execution window | One hour from dispatch preparation; 600–3600 seconds must remain before creation |
| Attempt | `broker-smoke-20261107-transport-3`; at most one POST |
| Hardware | Prefer RTX 4090; existing 3090/5090/MIG 24 GB allowlist; <=USD 3/GPU-hour |
| Runtime | <=600 seconds lifecycle including collection/upload/cleanup; worker <=60 seconds |
| Prior reservations | USD 2.000334 / 1200 seconds, retained in full |
| Added reservation | USD 1.000167 / 600 seconds, including disk and transfer/request/cleanup reserves |
| Combined reservation | USD 3.000501 / 1800 seconds; an estimate, not a supplier hard cap or invoice |
| Storage | Existing 5 GB ephemeral container disk; no persistent volume |
| Image | Same reviewed immutable worker digest and source as the first attempt |
| Acceptance | Real CUDA admission/memory roundtrip, hashed result files, durable Actions upload receipt, two independent absence rounds |
| Cleanup | Existing always-run steps and independent workflow-run recovery, ownership checked |

Only the known second definitive refusal is eligible as predecessor. The code
validates both historical records, their hash linkage, accounting, and completion
of the prior controller before reserving new intent. It refreshes complete
inventory and catalog price/CUDA admission before POST. Unknown creation,
authentication failures or altered accounting cannot authorize this retest.
Recovery follows the newest existing record and never creates one. Reruns of the
new record cannot issue a fourth POST, regardless of a changed window input.

The HTTP correction covers result GETs, create POST, ownership/inventory/cleanup
requests and bounded startup-log requests. The read-only catalog client already
used `runpod-ci-broker/0.1`; the separate live and cleanup adapters had omitted it.
All now use that same project identity. TLS, routing, credentials, 401/403 stopping
behavior and worker/image content are unchanged. No cloud success is claimed
before observing this retest's actual results.

Local validation: 357 tests passed, Ruff and strict mypy passed. Coverage includes
preserved predecessor hashes/reservations, refusal of unresolved or altered
predecessors, authorization/window guards, no fourth POST after a lost response,
and recovery selecting the third record without creating compute. The pure
reservation check was also exercised against the exact live ledger bytes.
