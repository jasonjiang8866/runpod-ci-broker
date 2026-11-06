# Current project integration audit

Inspection date: 2026-11-06 (client date, Asia/Singapore). Only RunPod-related
scripts, workflows, documentation, default-branch metadata, open PR metadata,
recent Actions metadata and release metadata were read. **No business repository
was modified, dispatched, merged, or supplied credentials to this project.**

| Repository | Inspected main SHA | Observed state relevant to discovery |
|---|---|---|
| `jasonjiang8866/runpod-ci-broker` | `d545105c1ba87fb1e35ab0221e603bb5523e07a3` | Already public; initial tree has only MIT LICENSE. No existing open PR or Actions run. Existing LICENSE preserved. |
| `jasonjiang8866/spleeterpt` | `b35c45f398de79c8948ec893efdaada1602978c3` | PR #49 open; recent expired-smoke cleanup Actions succeeded. Release `weights-v1.0.0` exists. These are historical Actions facts, not current Pod inventory. |
| `jasonjiang8866/simplexLoRA-bench` | `6d1de911ffc029fe7876d408f4c745f67b5e871b` | No open PR; latest benchmark checks succeeded, smoke/Global Volume workflow failed. No GitHub Releases returned. Failure cause not investigated because business debugging is outside this task. |
| `jasonjiang8866/flashattentionanycard` | `d53afd9aa6f3ce68ac896db0924558d1fe72ffe7` | No open PR; latest CPU regression/image offline validation succeeded. No GitHub Releases returned. |

## Sources actually inspected

- Spleeter: `AGENTS.md`, `runpod/agent-handoff.md`, `runpod/README.md`,
  `runpod/control.py`, `runpod/five_stem_capacity.py`, `runpod/global_storage.py`,
  `runpod/transfer.py`, `.github/workflows/runpod-five-stem.yml`, and
  `.github/workflows/runpod-cleanup.yml`.
- SimplexLoRA: `runpod/README.md`, `runpod/provider.py`, `runpod/experiment.py`,
  `runpod/inspect_managed.py`, `.github/workflows/runpod-experiment.yml`.
- FlashAttention: `runpod/README.md`, `runpod/provider.py`, `runpod/paid_smoke.py`,
  `.github/workflows/runpod-gpu-smoke.yml`, `.github/workflows/runpod-gpu-cleanup.yml`.

Pinned entry points:

- [Spleeter controller](https://github.com/jasonjiang8866/spleeterpt/blob/b35c45f398de79c8948ec893efdaada1602978c3/runpod/control.py)
- [Spleeter capacity](https://github.com/jasonjiang8866/spleeterpt/blob/b35c45f398de79c8948ec893efdaada1602978c3/runpod/five_stem_capacity.py)
- [SimplexLoRA provider](https://github.com/jasonjiang8866/simplexLoRA-bench/blob/6d1de911ffc029fe7876d408f4c745f67b5e871b/runpod/provider.py)
- [SimplexLoRA experiment](https://github.com/jasonjiang8866/simplexLoRA-bench/blob/6d1de911ffc029fe7876d408f4c745f67b5e871b/runpod/experiment.py)
- [FlashAttention lifecycle](https://github.com/jasonjiang8866/flashattentionanycard/blob/d53afd9aa6f3ce68ac896db0924558d1fe72ffe7/runpod/paid_smoke.py)
- [FlashAttention workflow](https://github.com/jasonjiang8866/flashattentionanycard/blob/d53afd9aa6f3ce68ac896db0924558d1fe72ffe7/.github/workflows/runpod-gpu-smoke.yml)

## Extracted common requirements

All three currently use REST v2 for resource operations and retain the account
key on the Actions controller. Existing budget helpers also use GraphQL. They
share GPU catalog queries, existing-volume discovery, per-run ownership markers,
bounded lifetimes, sanitized errors and independent cleanup checks.

Spleeter distinguishes numeric/FFT experiments, uses an optional explicit 4090
fallback, and has chunked checksum-verified result transport. SimplexLoRA has
image/source provenance, paired-method/seed acceptance and separate recovery
phases. FlashAttention has an immutable image, a one-off authorization marker,
capacity/price preflight and conservative ambiguous-create handling. These patterns
inform the generic journal, configuration and artifact contract; none of their
budgets, account limits, volume IDs, task modes or acceptance code was copied into
the Broker implementation.

## Keep project logic outside Broker

| Project | Broker responsibility | Project adapter responsibility (future separate PR) |
|---|---|---|
| Spleeter | Lock environment, preserve exact artifact identity, record GPU group | FFT policy, TF/PT waveform gate, stem definitions and quality evaluation |
| SimplexLoRA | CUDA-compatible placement, deadline, result manifest, recovery | int8 gate, independent runs, paired seeds, classification metrics |
| FlashAttention | Exact hardware group and separate unavailable record | Triton/kernel validation, tuning search, per-architecture performance analysis |

The example configs contain a reviewed source snapshot but deliberately no claimed
source-to-image mapping. Pinning a real digest requires verifying its build
provenance, wrapper and runtime admission. Existing images were inspected as
references in current documentation/controllers, not republished or reused.

## Access observations

The GitHub connection supports repository/PR read and write operations. The shell
can clone this public repository. No workflow-dispatch tool was exposed and no
`gh` executable was available. No browser session was used. `RUNPOD_API_KEY` was
absent from the local environment (only its presence was checked). Repository
secrets in other projects were neither read nor copied. Live RunPod catalog,
account inventory, registry access and billing remain unverified.
