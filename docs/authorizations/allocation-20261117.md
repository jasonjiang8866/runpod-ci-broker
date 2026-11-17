# Global allocation smoke continuation

The owner requested further development and paid GPU acceptance on 2026-11-17,
allowing 3090, 4090, 5090, RTX PRO 6000, Workstation and MIG across global regions.
This continuation retains the project's existing USD 20 total; it does not reset
prior reservations or grant a separate USD 20 allowance.

- Fixed task: `broker-allocation-20261117`, one GPU, CUDA memory smoke only.
- All six historical reservations are retained: USD 6.001002 / 3600 seconds.
- New allowance: at most three attempts, each with 300 seconds startup, 60 seconds
  worker, 120 seconds collection and 120 seconds cleanup. Total reservation including
  history, temporary storage and fixed reserves: USD 8.001502 / 5400 seconds, below
  USD 20 / 18000 seconds. These are estimates, not supplier billing or a hard cap.
- USD 3 per GPU-hour ceiling; catalog and observed prices must fit.
- Current manual launch supplies one UTC deadline leaving 600–1800 seconds;
  re-entry keeps that deadline. This is a new bounded execution window under the
  owner's current paid-acceptance instruction, not reuse of the expired old window.
- Fresh complete catalog/inventory, no region allowlist; explicit capacity refusal
  permits another eligible pair. Ambiguous creates require reconciliation.
- Worker source: `615cf130685fea1ba02173c8b153b4f66388a4c3`.
- Immutable worker: `ghcr.io/jasonjiang8866/runpod-ci-broker-worker@sha256:968681add1078ad7ac9cd39d2887bd5b2948a9356c7eb1f2347af548d2d22291`.
- [Publication 44108210129](https://github.com/jasonjiang8866/runpod-ci-broker/actions/runs/44108210129)
  built, checked and published that source. The paid workflow independently verifies
  anonymous pull and image revision before accessing the RunPod create path.
- Acceptance requires authenticated readiness, CUDA memory validation, verified
  bundle export, GitHub receipt, confirmed Pod absence and independent recovery.

This record captures the approved scope and planned limits. It is not GPU outcome
evidence or proof of authorization from an arbitrary user-supplied JSON flag.
