# CUDA catalog filtering and no-cost comparison

RunPod's `cudaVersions` query matches an **exact set**, while `minCudaVersion`
is a numeric floor. These are mutually exclusive. Omitting both enumerates
reported versions without a CUDA availability filter. All three scope catalog
availability/prices; none proves the container's runtime environment.

The first authenticated query on 2026-11-07 used exact `12.8`:
[run 42331880254](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/42331880254).
It returned no capacity for the four allowed models under SECURE / 1 GPU / 12.8.
Later user-provided console screenshots showed capacity signals for 3090, 4090,
5090 and PRO 6000 MIG 24GB. These are different observations in time with incomplete
console filter context; CUDA filtering is a plausible explanation, not yet proven
by an authenticated side-by-side comparison.

## Explicit configuration change

At the owner's request, `examples/simplexlora-flexible.json` now uses:

```json
{
  "compatibility": {"min_cuda_version": "12.8"},
  "limits": {"max_gpu_hourly_usd": 3.0}
}
```

This is a partial excerpt, not a complete task. The four-model allowlist already
included RTX 5090; it still includes only the 24GB MIG variant, not the 48GB variant.
The maximum rate is **USD 3 per GPU-hour**; multiple GPUs multiply the rate.
The template's USD 2 total, timing and non-compute reserves remain illustrative,
not a user-approved experiment budget. Its conservative estimate rises with the
unit ceiling. No live execution capability is enabled.

`strict_repro` still requires a single exact CUDA version, driver, architecture,
GPU and data center; a floor is rejected. Hardware benchmark groups retain their
existing explicit constraints. Older exact-only configurations keep their v1
fingerprints; the deliberately revised smoke configuration gets a new fingerprint
and cannot resume a task journal or authorization from its older configuration.

The floor is a host catalog admission requirement. A higher host CUDA capability
may support an older toolkit through driver compatibility, but framework kernels,
PTX/JIT and GPU architecture still require actual runtime validation. Keep the image
digest fixed and record driver, image toolkit/framework and device architecture
separately. Never replace a failed GPU admission with a CPU pass.

## Diagnostic workflow input

After review/merge, manually run `.github/workflows/read-only-preflight.yml` on
**main**, with `example=simplexlora-flexible` and **`diagnose_cuda=true`**.
No new trigger, cloud mutation or token permission is added. Locally:

```bash
uv run runpod-ci-broker --config examples/simplexlora-flexible.json \
  --action preflight --diagnose-cuda --output out/preflight
```

The task's own filter is the primary query and is the **only** source for selection.
With diagnostics enabled, the adapter also queries without a CUDA filter and with
the alternative exact/floor filter. For this template the three modes are:

| Mode | Query | Use |
|---|---|---|
| Requested minimum | `minCudaVersion=12.8` | Candidate admission under the updated task |
| Unfiltered | Neither CUDA parameter | Diagnostics only |
| Exact comparison | `cudaVersions=12.8` | Diagnostics only |

For an exact-set task, the alternative floor is the lowest member of that set;
it does not change the configured task or approve versions outside its set.

There are at most two extra GET requests. They share `limits.max_queries` with
primary catalog attempts (including retries), and no diagnostic read starts after
the original preflight wait deadline. Each HTTP request still has the transport's
15-second timeout: the wait limit bounds starting reads, not a hard wall-clock cap
on an already in-flight read. No diagnostic retries occur. A failed primary query
skips diagnostics; a failed diagnostic stops further probes and remains unknown.
Pod/volume inventories are not queried again for the diagnostic modes.

Reports retain timestamps, actual parameters, overall capacity signals, regional
signals, per-GPU prices and reported CUDA versions. The Markdown comparison table
uses **overall signals**, not proof of an allowed regional placement. Missing rows,
missing signals and query failures are unknown, not no capacity. A broader signal
alongside exact NONE produces `cuda_filter_effect_possible`; sequential reads can
observe inventory changes, so this is an inference, not causal proof.

Diagnostics never feed the planner, loosen thresholds, change image/driver/GPU,
remove blockers or cause a paid operation. An exact task with no qualifying capacity
still exits 2 even if a diagnostic query finds a machine elsewhere.

## Validation and remaining work

132 offline tests pass, including 22 new cases for numeric CUDA floors, retained
legacy fingerprints, strict-task locks, 5090 pricing, shared query caps, failures,
redaction and CLI isolation. Strict mypy passes 12 modules. Ruff and wheel/sdist
builds pass. **The revised live comparison has not been dispatched.**

Sources checked 2026-11-07:

- [RunPod GPU catalog parameters](https://docs.runpod.io/api-reference-v2/catalog/list-gpu-types)
- [NVIDIA CUDA compatibility and PTX caveats](https://docs.nvidia.com/deploy/cuda-compatibility/minor-version-compatibility.html)
