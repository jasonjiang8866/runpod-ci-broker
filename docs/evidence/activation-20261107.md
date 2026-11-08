# Shared state and worker publication: observed evidence

Observed on 2026-11-07. Evidence modes: **github_state_only** and
**registry_read_only**. No real GPU execution, RunPod create/stop/delete or
supplier billing verification occurred. The USD 20/five-hour allowance is unused
by these operations; preparation duration does not start the paid campaign clock.

## Source and approval

The owner explicitly approved PR #7 merge, repository Contents write for the
manual state probe, Packages write for manual image publication, creation of the
`broker-state` branch, public GHCR publication and browser dispatch. These approvals
are distinct from the previously granted compute allowance.

- [PR #7](https://github.com/jasonjiang8866/runpod-ci-broker/pull/7) merged at
  `2e6fe6930c02fc2469079a1d8e64025562aff07f` after both head checks passed.
- Both manual runs below use that exact main SHA. Their boolean confirmations were
  true, each dispatched once. Neither workflow has a RunPod key.

## Real GitHub state probe

[Run 42358736934](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/42358736934)
completed successfully. The probe's structured log reports:

```json
{
  "mode": "github_state_only",
  "scope_hash": "60ec653c1825d65a0308262137cef4cd56b13ff9c253fc3dfd071e55e5977973",
  "conditional_write_verified": true,
  "released": true,
  "runpod_operations": 0
}
```

An independent read of the committed scope record verified `revision: 3`,
`holder: null`, and `records: {}`. This establishes real GitHub conditional
update/conflict/read-back/release behavior. It does not test stopped-controller
handover, account-global locking or concurrent paid controllers.

## Published immutable worker

[Run 42358807048](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/42358807048)
completed successfully: image build, CPU negative GPU gate, registry publication
and provenance artifact upload. Artifact `worker-publication-42358807048` has ID
`13568278221` (973 bytes at upload). This is image provenance, not experiment output.

The [package page](https://github.com/jasonjiang8866/runpod-ci-broker/pkgs/container/runpod-ci-broker-worker)
reports **Public**. The push log records:

```text
ghcr.io/jasonjiang8866/runpod-ci-broker-worker@sha256:d1a450f816d71169c29eb53217d869baa88d9eeaa156911f06b83b8a93f9ad11
```

The [machine-readable registry evidence](worker-publication-42358807048.json)
records anonymous manifest, config and complete layer-blob download, size and
SHA256 checks, plus source/revision labels. No account credential was supplied;
the registry issued an anonymous pull token. Bytes were hashed without executing
the image. Local Docker unpack/run and RunPod image pull remain unverified.
The publication workflow separately executed the built image on a CPU runner.

## Remaining gates before paid creation

1. Integrate real create/status calls with campaign-wide budget/time accounting,
   persisted create intent, unknown-create reconciliation and the shared journal.
2. Independently recover a stopped controller's lock without unsafe time-based
   takeover; retain the original ownership and unresolved-intent record.
3. Persist/recover the result capability without placing it in public Git state;
   verify durable experiment artifact receipts and the bounded collection policy.
4. Run the approved short GPU smoke, then verify task-owned Pod termination through
   the independent cleanup route. CUDA, MIG profile identity, export and deletion
   are not claimed verified by this document.

The CLI's paid `run` guard remains enabled. No business repository was modified.
