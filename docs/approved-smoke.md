# Approved smoke envelope and activation gates

The owner approved **USD 20 total and five hours** on 2026-11-07 (Asia/Singapore).
The [authorization record](authorizations/broker-smoke-20261107.json) retains the
four allowed GPU IDs and the earlier USD 3/GPU-hour ceiling. This is approval for
this Broker experiment, not a budget borrowed from any business repository.

Treat five hours as BOTH the campaign wall-clock limit from first paid create
intent and the cumulative resource lifetime over all attempts. One active Pod,
one GPU. Startup, image pull, failed attempts, temporary storage, applicable transfer,
collection and cleanup share the same USD 20. Reruns must resume the same campaign
and spend/time ledger. At the compute ceiling five GPU-hours alone would cost USD 15;
that arithmetic does not establish the remaining USD 5 is sufficient for every other
cost. No provider-level total-dollar hard cap has been verified.

**Launch status: not started.** The authorization record is not an executable task
or a substitute for trusted manual workflow context. Actual billed cost is unknown;
no GPU expense has been incurred by this conversation's tool actions.

## Implemented worker and cleanup preparation

- A small Python worker image, with a base resolved to its SHA256 before build,
  `uv.lock`, source revision baked into the image, and an exec-form fixed entrypoint.
- `cuda_probe`: CUDA Driver API device identity/count/UUID/VRAM/architecture and a
  4 KiB device allocation, pattern fill, device-to-host copy and byte-level check.
  It releases its allocation/context. There is no torch dependency or CPU fallback.
- `worker`: a fixed subprocess with a maximum 60-second timeout, no shell and an
  allowlisted environment. It rejects GitHub/ledger management credentials, excludes
  the result capability from the child, checks source revision and hardware/software
  admission, then exports metrics, environment evidence and sanitized logs.
- `cleanup_owned`: a separate controller library using real RunPod REST v2 GET and
  DELETE contracts. It requires a trusted live journal, exact task/run/Pod ownership,
  image and mounts. It never stops, creates or deletes volumes. Two direct 404s plus
  two complete inventories without the Pod are required for deletion confirmation.
  Network errors and successful DELETE alone remain unconfirmed. Bounded retries
  preserve CLEANUP_PENDING for independent recovery. Tests use an injected API.

The controller can supply the secret-free task as `RCB_TASK_JSON`, selected GPU as
`RCB_SELECTED_GPU`, and a fresh result capability as `RCB_RESULT_TOKEN`. The worker
uses the documented `RUNPOD_POD_ID`. RunPod also documents an injected Pod-scoped
`RUNPOD_API_KEY`; the wrapper discards it unused before starting the child. The
future create adapter must never copy its account API key into a Pod environment.

The memory probe verifies CUDA driver/device operations only. It does **not** verify
PyTorch, TensorFlow, int8, a CUDA toolkit runtime or numerical reproducibility. Driver
support uses a minimum 12.8 floor, accepting later versions. Exact-software and
hardware-benchmark jobs are refused by this initial worker. An observed parent GPU
name or 24GB memory alone does not prove the selected MIG profile; an ambiguous MIG
identity is refused and recorded. Do not remove MIG from the overall allowlist or
silently claim its identity was checked.

The image was published from main `2e6fe6930c02fc2469079a1d8e64025562aff07f`:
`ghcr.io/jasonjiang8866/runpod-ci-broker-worker@sha256:d1a450f816d71169c29eb53217d869baa88d9eeaa156911f06b83b8a93f9ad11`.
Ordinary PR CI builds it without credentials and checks that a CPU runner returns
a failed GPU probe. Publication and CPU refusal do not verify positive GPU execution.
See [activation evidence](evidence/activation-20261107.md) for verified boundaries.

## Approved manual workflows

| Workflow | New job permission | Exact effect |
|---|---|---|
| `verify-shared-state.yml` | `contents: write` in this repository | Writes isolated test scope records into an existing `broker-state` branch and verifies conditional-write conflict plus release; no RunPod access |
| `publish-worker.yml` | `packages: write`, `contents: read` | Builds the current main SHA, publishes `ghcr.io/jasonjiang8866/runpod-ci-broker-worker:sha-<full-sha>` and records the immutable registry digest; no RunPod access |

Both are manual-only, main-only, constrained to this repository and have an explicit
confirmation input defaulting to false. They do not run from a PR, do not use
`pull_request_target`, and do not access `RUNPOD_API_KEY`. The user-approved compute
budget did not by itself authorize these permissions. The owner separately approved
PR #7 merge, both manual workflows, the state branch and public image publication.
PR #7 is merged and both workflows succeeded. Ordinary image-build/CPU tests retain
Contents read.

**The Contents permission is repository-wide**, not technically branch-scoped.
The prepared client restricts its write path to `broker-state/scopes/<hash>.json`;
that code restriction must not be described as a token-enforced permission boundary.
The branch was created at merge `2e6fe6930c02fc2469079a1d8e64025562aff07f`.
The real probe verified conditional-write conflict and release. Its final record
has `holder: null`, empty `records`, and no task secrets or GPU intent.

The package page reports Public. Anonymous manifest/config/layer verification is
recorded separately from authenticated publishing; neither is evidence of a RunPod
image pull. Never inject a GitHub management token into a Pod.

## First attempt and remaining implementation

Use a short memory smoke, not five hours of work. Initial target: runtime <=60s,
startup <=300s, collection <=120s, cleanup <=120s, and at most two startup attempts
inside the single campaign allowance. This bounds the *planned* first smoke to
20 aggregate minutes, or at most USD 1 compute at the approved ceiling. It is not a
current quote or a hard spend guarantee. Use fresh storage/transfer estimates and
capacity/price observations before launch; retain most of the USD 20 as unused
headroom rather than automatically spending it.

Before creation, finish and verify:

1. Retain the completed state-probe and public immutable image evidence. These
   preparation checks needed no RunPod compute.
2. The paid create/controller integration with the shared journal and campaign-wide
   budget/time accounting. The public `run` command still deliberately refuses live
   execution; this PR does not bypass that guard.
3. Independent takeover of a stopped controller's sticky lock, plus persistence of
   the per-task result capability outside disposable runner memory. Neither capability
   tokens nor account credentials may be committed to the public state branch.
4. Durable GitHub artifact publication receipt, then the owner-verified cleanup
   adapter. Its actual deletion and the live proxy export remain untested.

Once those gates pass, the existing USD 20/five-hour approval can be used without
asking for the same compute authorization again. Changed budget, GPU scope,
permissions or business-repository modifications still need separate approval.

Official interfaces checked:
[RunPod inspect](https://docs.runpod.io/api-reference-v2/pods/get-a-pod),
[terminate](https://docs.runpod.io/api-reference-v2/pods/terminate-a-pod),
[NVIDIA device](https://docs.nvidia.com/cuda/cuda-driver-api/group__CUDA__DEVICE.html),
[memory](https://docs.nvidia.com/cuda/cuda-driver-api/group__CUDA__MEM.html) and
[primary context](https://docs.nvidia.com/cuda/cuda-driver-api/group__CUDA__PRIMARY__CTX.html).

[RunPod-provided environment variables](https://docs.runpod.io/pods/templates/environment-variables)
