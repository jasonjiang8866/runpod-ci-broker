# Result HTTP 403: comparison with working GPU projects

Inspected on 2026-11-17 Singapore time (2026-11-16 UTC). This is a source and
existing-run audit, plus a local transport correction. No new Pod was created.

## Finding

Broker's result reader omitted an explicit `User-Agent`; the real Python 3.12
HTTP stack sent `Python-urllib/3.12`. All three working projects identify their
HTTP client. SimplexLoRA previously encountered result-endpoint 403 responses
and added that exact class of correction before successfully retrieving GPU
results. This is the strongest concrete precedent found, but not proof of the
original Broker response's origin or cause: its headers/body were not retained,
and the same live endpoint has not been tested before/after this change.

| Project / inspected successful source | Result transport | Explicit client identity | Existing GPU evidence |
|---|---|---|---|
| [waferVLM](https://github.com/jasonjiang8866/waferVLM/blob/c255bec677ba4b525e4997af60d9020592e15a46/runpod/provider.py) | HTTPS Pod proxy, port 8888, `/result`, ephemeral bearer | `waferVLM-actions/0.1` | [43716864164](https://github.com/jasonjiang8866/waferVLM/actions/runs/43716864164): DINO linear/LoRA work, results and cleanup; successful job |
| [simplexLoRA-bench](https://github.com/jasonjiang8866/simplexLoRA-bench/blob/331ca64fad2b1344e5dab91e3835c94aa0bc59c0/runpod/provider.py) | HTTPS Pod proxy, port 8888, `/result`, ephemeral bearer | `simplexlora-bench-actions/0.1` | [41324992190](https://github.com/jasonjiang8866/simplexLoRA-bench/actions/runs/41324992190): CUDA gate and six smoke workers passed; results verified and GPU deleted; later CPU creation failed |
| [spleeterpt](https://github.com/jasonjiang8866/spleeterpt/blob/e604181127542d134005ef01402234c577d9a1bc/runpod/control.py) | HTTPS Pod proxy, port 8888, `/result`, ephemeral bearer | `spleeterpt-runpod-actions/0.1` | [41990713955](https://github.com/jasonjiang8866/spleeterpt/actions/runs/41990713955): five-stem GPU benchmark/export validation and cleanup; successful job |
| [Broker before this patch](https://github.com/jasonjiang8866/runpod-ci-broker/blob/07c255fb07ee39cd9512baf72ea13a5d88f00fb9/src/runpod_ci_broker/export.py) | HTTPS Pod proxy, port 8000, `/v1/manifest` and `/v1/files/...`, ephemeral bearer | Missing on result requests; management client already identifies itself | [42596594291](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/42596594291): created/deleted a GPU Pod, first result request returned 403; CUDA results unverified |

All four use REST v2 for Pod lifecycle. The working projects do not use a
different GPU permission mechanism or put the RunPod account key into result
requests. Their separate GraphQL budget queries do not explain this result GET
failure. Broker declares `8000/http` and binds the corresponding port; the port
number difference alone is not evidence of a defect.

Audit snapshots of current main were waferVLM `658fa5d1b1e80ce4156609a57ce901c15fe1a66d`,
simplexLoRA-bench `6d1de911ffc029fe7876d408f4c745f67b5e871b`, spleeterpt
`a4c092a01d8308eeecabbc3344bf797573685ad4`, and Broker
`818ec70d4acb15d667c452dddff7e8baa5abed03`. The table pins successful-run source
separately rather than assuming today's main ran those experiments.

## SimplexLoRA's directly relevant precedent

[Commit 771ad7e](https://github.com/jasonjiang8866/simplexLoRA-bench/commit/771ad7efb43488328feea8211c6154036967751e)
changes only HTTP client identification in `provider.py`, `global_smoke.py` and
`inspect_managed.py`. Its
[saved run history](https://github.com/jasonjiang8866/simplexLoRA-bench/blob/6d1de911ffc029fe7876d408f4c745f67b5e871b/docs/results/runpod-smoke-41324992190.md)
records the first run, 41323672930, canceled after result HTTP 403 responses.
The subsequent run 41324992190 downloaded and verified the GPU archive; its
job log independently shows CUDA admission, all six workers and completion.
The overall workflow failed later on a CPU disk/create setting, so it must not
be described as an entirely green workflow. The saved report records final
CPU-only recovery separately as run 41326447241.

This precedent supports reusing the explicit client header. It does not identify
a particular WAF rule, establish that every 403 has this cause, or demonstrate
that the Broker correction works on RunPod yet.

## Startup readiness is a second difference

The working controllers can query a state endpoint while worker computation
runs in another thread. Broker's `worker.main` completes `run_smoke()` before
creating its result server. Provider `RUNNING` therefore cannot establish that
Broker's HTTP service is ready. Some working controllers also catch broad
request errors and poll again, which can conceal startup HTTP failures.

Do not copy broad 401/403 retry behavior. A separate worker/readiness change
could serve authenticated state while CUDA admission runs, then publish results
only after completion. That would require worker/image validation. This patch
does not change the worker, its image pin, polling policy or error classification.
Readiness remains an investigation item, not an established cause of the 403.

## Correction and verification

`RunPodResultReader.open` now uses the same honest project identity as Broker's
management client: `User-Agent: runpod-ci-broker/0.1`. The common reader covers
both manifest and file downloads. Bearer authentication, fixed HTTPS routes,
redirect refusal, TLS and environment proxy behavior are unchanged. There is
no browser impersonation, identity rotation or automatic retry of 401/403.

- Before the production fix, the two route/header cases and a real loopback
  request regression failed (three failures); the loopback observed the default
  Python user agent.
- After the fix, **342 tests passed**. The loopback exercises real urllib
  requests for both routes and checks project identity, capability bearer,
  identity encoding and absence of the account credential. It is not a RunPod
  proxy or GPU acceptance test.
- Ruff and strict mypy passed. No live after-fix result request has been made.

The later [43899066456 create refusal](smoke-43899066456.md) is a separate failure:
one `POST /v2/pods` returned 400/422 before any result request. Adding the result
header does not fix or explain that creation refusal. PR #13 also preserves
numeric create status and safe diagnostic hints for future evidence.

The consumed successor allowance, expired execution window, original ledger
hash, both retained reservations and once-only create guard are untouched.
This comparison and correction neither authorize nor enable a further paid run.
