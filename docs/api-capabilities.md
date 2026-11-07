# Official API capability review

**Live verification update, 2026-11-07:** all four exact GPU IDs/VRAM and catalog
prices below were returned by the authenticated preflight. Catalog, data centers
and Pod inventory queries succeeded. [Run evidence and exact filters](evidence/preflight-42331880254.md).
The original documentation review below is historical; other permissions and live
create/worker/cleanup capabilities remain unverified.

Checked 2026-11-06 (client date, Asia/Singapore). These are documentation/schema
observations, **not authenticated account queries**. Current source audit is separate.
The implementation uses RunPod REST v2 and no GraphQL or v1 field mixing.

| Capability | Documented interface / fields | MVP behavior and boundary |
|---|---|---|
| GPU catalog | `GET /v2/catalog/gpus`; `id`, `name`, `memory`, per-cloud `price`, `maxCount` | `maxCount` is a machine ceiling, not free inventory. `price` is per GPU; count multiplication belongs in the estimate. |
| Pod capacity | `include=AVAILABILITY&product=POD&count=...&cloud=...`; `availability` and `dataCenters[].availability` | Regional LOW/MEDIUM/HIGH are advisory capacity; overall availability never fills a missing regional signal. NONE is explicit no capacity, missing/failed query is unknown. |
| CUDA filter | `cudaVersions` exact versions or `minCudaVersion`, mutually exclusive | Uses exact configured version set. `cudaVersions[]` contains `version` and **`available: bool`**. No fabricated driver or compute-capability fields. |
| Data centers | `GET /v2/catalog/datacenters` with optional GPU availability/region filters | Reads IDs/regions. Global networking capability is not Global Volume capability. |
| Pod inventory | `GET /v2/pods`, cursor pagination | All pages required; malformed/repeated cursors, duplicate IDs and invalid Pod state/environment shapes are unknown. Raw environments never enter reports. |
| Create / inspect / terminate | `POST /v2/pods`, `GET /v2/pods/{id}`, `DELETE /v2/pods/{id}` | Contract reviewed; only fake lifecycle implemented. No undocumented idempotency-key guarantee is assumed. |
| Pod configuration | `name`, `image`, `gpu`, `disk`, `env`, `entrypoint`, `cmd`, `mounts`, `dataCenterIds`, `registry` | Future exec-form entrypoint must be fixed in a reviewed image; no shell interpolation. `registry` is an existing registry-credential ID. Registry creation is outside this MVP. |
| Pod status / cost | PROVISIONING, STARTING, RUNNING, EXITED, ERROR, TERMINATED; `cost` | Provider state is not experiment acceptance. Hourly compute rate is not accrued cost or final invoice. |
| Network Volume | `GET /v2/network-volumes`, record field **`dataCenter`**; mounts use `volumeId` | Existing volume only. Must match the Pod data center. Never create/resize/delete a volume. |
| Global Volume | `GET /v2/global-volumes`; `mounts.global: [{volumeId, path}]` | Current schema permits one Global Volume on GPU/CPU Pods without a data-center restriction. No assumption of POSIX checkpoint semantics or free operations. No live attachment tested. |
| Mount combinations | Up to four network mounts, one global; persistent disk conflicts with either | MVP models at most one existing volume. Paths/ownership are checked by fake cleanup. |
| Logs | `GET /v2/pods/{id}/logs`, SSE, `source`, bounded `tail`, timestamp cursor | Available in API; not implemented as an artifact filesystem. No documented general file-download endpoint was established. Worker export needs a separate protocol. |
| Billing | `GET /v2/billing/pods`, time-bucketed GPU/CPU/disk totals | Available documentation; no account query, no synthetic final spend. Cost fields remain null where not observed. |
| Error handling | HTTP 401/402/403/429 and 5xx | Fixed safe error categories. Only transient reads get bounded backoff/jitter. 403 never means capacity or permission to try another identity. |

GPU IDs below were verified in the **official GPU reference** and cross-checked
against current project controller strings. They have **not** been confirmed by a
live authenticated catalog response in this task. The adapter selects only exact
returned IDs; it does not translate approximate marketing names.

| Requested GPU | Official reference ID | Listed VRAM |
|---|---|---:|
| PRO 6000 MIG 24GB | `NVIDIA RTX PRO 6000 Blackwell Server Edition MIG 1g.24gb` | 24 GB |
| RTX 4090 | `NVIDIA GeForce RTX 4090` | 24 GB |
| RTX 3090 | `NVIDIA GeForce RTX 3090` | 24 GB |
| RTX 5090 | `NVIDIA GeForce RTX 5090` | 32 GB |

The current pricing page describes per-second Pod billing, no Pod ingress/egress
fees, storage charges, a minimum balance condition and an account hourly spend
limit. None is a verified per-experiment total-dollar cap. Storage, Global Volume
requests and third-party export costs require separate estimates. Software cleanup
can fail during an outage; the estimate is not a provider-enforced spend guarantee.

GitHub reusable workflows use `workflow_call` and explicit inputs/secrets; checkout
context must be handled deliberately. This reusable workflow checks out Broker from
an explicit full SHA, not the caller's project. Concurrency groups are repository
scoped. Cancellation can terminate processes despite cleanup code; `always()` is
not an external resource deletion guarantee. PRs receive no RunPod credential.

## Sources

- [GPU types](https://docs.runpod.io/references/gpu-types)
- [GPU catalog schema](https://docs.runpod.io/api-reference-v2/catalog/list-gpu-types)
- [Data-center catalog](https://docs.runpod.io/api-reference-v2/catalog/list-data-centers)
- [Create Pod](https://docs.runpod.io/api-reference-v2/pods/create-a-pod)
- [List Pods](https://docs.runpod.io/api-reference-v2/pods/list-pods)
- [Get Pod](https://docs.runpod.io/api-reference-v2/pods/get-a-pod)
- [Terminate Pod](https://docs.runpod.io/api-reference-v2/pods/terminate-a-pod)
- [Registry credential create contract](https://docs.runpod.io/api-reference-v2/registries/create-a-container-registry-credential)
- [Network Volumes](https://docs.runpod.io/api-reference-v2/network-volumes/list-network-volumes)
- [Global Volumes](https://docs.runpod.io/api-reference-v2/global-volumes/list-global-volumes)
- [Storage options](https://docs.runpod.io/pods/storage/types)
- [Pod pricing](https://docs.runpod.io/pods/pricing)
- [Pod log SSE](https://docs.runpod.io/api-reference-v2/pods/stream-pod-logs)
- [Pod billing](https://docs.runpod.io/api-reference-v2/billing/get-pod-billing-history)
- [GitHub reusable workflows](https://docs.github.com/en/actions/how-tos/reuse-automations/reuse-workflows)
- [GitHub concurrency](https://docs.github.com/en/actions/concepts/workflows-and-actions/concurrency)
- [GitHub cancellation](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-cancellation)
