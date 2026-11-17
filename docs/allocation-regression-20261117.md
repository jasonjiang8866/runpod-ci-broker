# Global allocation regression — 2026-11-17

**Result: allocation succeeded; GPU acceptance failed; Pod deletion confirmed.**
This is a CUDA smoke infrastructure test, not a training benchmark. Historical
accepted smoke runs do not establish acceptance of the new allocation controller.

## Observations

- [First search](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/44109623354):
  three definitive capacity refusals across two GPU models and two data centers;
  no Pod was created. [Recovery](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/44109871190)
  passed without an owned allocation to terminate.
- [Diversity continuation](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/44111598106):
  after importing those refusals, the first create succeeded for an RTX PRO 6000
  Blackwell Server Edition in a third data center. This demonstrates global search
  and live feedback, but is not statistical evidence of improved success rates.
- The result endpoint repeatedly returned `result_not_ready`; authenticated
  worker readiness, CUDA execution and a verified result bundle were not observed.
- Collection and [automatic recovery](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/44111875644)
  both failed while the persistent controller lock remained held. Their generic
  error output did not preserve the underlying failure, so an exact journal or
  worker root cause cannot be established from this evidence.
- [Manual cleanup](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/44114485863)
  succeeded after PR #27 made manual recovery skip collection. Deletion was
  verified by direct absence and two complete inventory checks, and the lock was
  released. Final experiment state is `REJECTED`, with `pod_deleted_confirmed=true`
  and `results_saved=false`.

## Cost and bounds

Intent-to-confirmed-deletion time was about 893.7 seconds (14m54s), at an observed
USD 2.69/hour. Estimated compute cost is **USD 0.668**, excluding storage and other
fees; supplier billing remains unavailable. This exceeded the planned 600-second
lifecycle because recovery itself failed. Software deadlines are not a supplier
billing cap, and a worker process exit does not confirm Pod termination.

The original USD 20 campaign remains unchanged. All prior reservations, including
capacity-refused attempts, were retained: USD 10.002002 / 7200 planned lifecycle
seconds. No unused attempt or reservation is refunded. No further GPU is created
after the unresolved allocation; the release manifest is disabled after cleanup.

## Corrective changes and next gate

PR #27 adds cleanup-only manual recovery and exposes fixed provider/journal error
codes without logging raw responses or credentials. Follow-up fixes preserve the
original journal exception instead of masking it through lock reacquisition,
apply startup deadlines to retryable readiness errors, and report successful
cleanup separately from failed experiment acceptance.

Before the next paid test:

1. Add bounded, safe startup diagnostics to the allocation path so readiness
   failure can distinguish image startup, worker admission and result transport.
2. Reproduce journal failure and recovery under realistic latency and read-back
   behavior; measure write volume. The current evidence does not prove rate
   limiting, caching or permissions as the underlying cause.
3. Exercise independent cleanup when collection or journal updates fail, including
   whether a separate watchdog can terminate an already-owned Pod when the state
   backend is degraded, without enabling new creates or ambiguous deletion.
4. Pass a new complete readiness → CUDA smoke → verified export → receipt →
   confirmed deletion acceptance before advancing to multi-task queues or MILP.

Offline validation after the follow-up: 521 tests, Ruff and strict mypy passed.
Those checks do not substitute for the missing GPU result.
