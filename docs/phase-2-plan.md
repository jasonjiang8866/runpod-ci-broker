# Phase 2: authenticated, read-only evidence

**Update, 2026-11-07:** PR #2 and #4 are merged. The exact-only preflight found
no capacity, while the later minimum-CUDA comparison found all four models eligible.
See [latest run evidence](evidence/preflight-42334557432.md) and the
[earlier observation](evidence/preflight-42331880254.md). The sections
below retain the pre-run plan for provenance; the evidence record supersedes
its statements that dispatch and Secret validity were unverified.

Status checked 2026-11-07. PR #1 is merged at
`6de7cd7733c8fc8123bdff1a6fce1a0014704e6b`; main CI run `42330437634` passed.
The owner reports adding repository secret `RUNPOD_API_KEY`. Secret contents are
neither requested nor readable here. Its validity and account permissions remain
unverified until the manual workflow executes. No paid authorization exists.

## Deliverable in this change

- Regional capacity must be present on that region's catalog entry. Overall HIGH
  plus a missing regional signal stays unknown, never available.
- Validate projected catalog, Pod inventory, volume and data-center fields. Malformed
  responses produce fixed diagnostic codes, never raw API bodies or validation input.
- Reject incomplete pagination and duplicate resource IDs. An unknown Pod status or
  invalid environment map cannot be treated as an empty scheduling scope. EXITED
  Pods still block their scope because retained storage may remain billable.
- Publish candidate VRAM, price, capacity, blockers and safe observations in both
  artifact files and the Actions job summary. Prices are per GPU; overall catalog
  signals and machine count ceilings are separately labeled. No capacity reservation.

This change keeps dispatch manual and main-only, with no new triggers or permissions.
The live adapter still has only GET operations. `run`, real `cleanup` and real
`reconcile` remain disabled regardless of a task's illustrative budget values.

## First authenticated observation

After this PR is reviewed and merged:

1. Open [RunPod read-only preflight](https://github.com/jasonjiang8866/runpod-ci-broker/actions/workflows/read-only-preflight.yml).
2. Select **Run workflow**, branch **main**, input **example=simplexlora-flexible**.
   This committed template queries all four permitted models with CUDA 12.8 and one
   GPU. It does not execute SimplexLoRA or validate its placeholder image/entrypoint.
3. Record the workflow run URL, exact workflow SHA, attempt and observation timestamp.
   Download `broker-preflight-<run_id>-<attempt>` containing `report.json` and
   `summary.md`. Keep the evidence mode `read_only`; do not relabel it GPU validation.
4. Inspect formal GPU IDs, VRAM, per-GPU prices, data-center-specific capacity,
   inventory completeness, scope count and every candidate rejection. A successful
   API query can still return no eligible candidate; workflow exit 2 is expected
   for blocked admission. Inspect the report rather than equating red CI with stockout.
5. Distinguish authentication failure, permission denial, account restriction,
   provider/network failure, invalid response, missing model, unknown capacity and
   explicit NONE. Do not automatically widen permissions, GPU lists or budgets.

This first template has no volume. It therefore does **not** establish Network or
Global Volume access. Inspect a specific existing volume only after its ID and
constraints are supplied in a reviewed task. Do not create a volume to test access.
Private-image credentials and runtime driver/architecture remain unverified.
Strict reproduction will stay blocked on missing exact environment facts.

Current control capability: the GitHub connector supports repository/PR operations
and GET workflow inspection, but does not expose dispatch. No authenticated local
`gh` is available. A browser dispatch requires explicit authorization for this
repository; otherwise the steps above are the manual handoff. This is a tool
capability limitation, not a rejection of the user's intent to run read-only checks.
No alternate trigger is added to work around it.

## Development after the observation

The next [implementation tranche](durable-recovery.md) adds the shared journal,
prepared GitHub Contents adapter and bounded result export, verified offline.
Authenticated remote integration and live lock handover remain gates.


| Work | Completion evidence | Paid resources |
|---|---|---|
| Resolve real response/schema differences | Sanitized fixtures, regression cases and a new readonly report | None |
| Durable central task ledger and scope lease | Crash/restart and cross-run races tested; ambiguous create never retried blindly | None |
| Fixed smoke image and worker admission/export | Reviewed digest, no account key in worker, CUDA gate, bounded export/checksums; local contract tests | None; image publication needs its own approval |
| Independent recovery entrypoint | Scope/ownership proof, bounded termination and absent-resource verification, cancellation drill with mocks | None |
| Propose real smoke | Exact GPU/DC/image, duration, storage/export reserves and total USD cap based on fresh observations | Only after separate explicit approval |
| Business integration | Separate small PR in one selected project | Requires separate scope/approval |

The first paid smoke should be a tiny CUDA allocation/matmul and environment
manifest, not a business benchmark. Its acceptance requires a verified remote
artifact bundle and confirmed Pod absence as separate facts. The price and all-in
cap will be proposed after real quotes and the live recovery gates exist. No
historical US$1/2/5/10 or monthly budget carries forward.

## Validation for this change

110 pytest cases pass, including 25 new cases for regional unknown capacity,
malformed API values/collections, duplicate pagination, EXITED scope occupancy,
and complete credential-safe CLI reports. Strict mypy passes all 11 modules;
Ruff lint/format and wheel/sdist build pass. These tests inject HTTP response
fixtures; **they are not authenticated RunPod evidence**.
