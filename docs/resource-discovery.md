# Adaptive resource discovery / 智能资源查找

Broker 的核心能力之一是提高用户在预算和兼容性要求内找到可租 GPU 的机会。
库存目录显示可用只是候选信号；真正的容量拒绝应反馈到下一次选择，而不是
反复选择同一卡型、同一机房。当前版本提供只读查找、模拟器反馈闭环及固定
真实控制器的可选规划接入，尚未
验证它对真实租卡成功率的提升，也尚未接入通用付费创建控制器。

## Current behavior

1. Apply existing hard constraints: GPU allowlist/count, VRAM, CUDA, driver and
   architecture when specified, cloud, region, volume locality and total budget.
   Hardware benchmark and strict reproduction tasks retain their original locks.
2. Produce a queue of **GPU/data-center pairs**, using each center's catalog
   stock grade. Aggregate GPU stock is not substituted for location stock.
3. Rank by a selectable objective. `availability` uses stock, then GPU preference,
   then per-GPU hourly price; `preference` uses preference, stock, price;
   `price` uses price, stock, preference. Ties use data-center and GPU identifiers.
   Explicit preferences are honored; with none, 4090 wins a stock-grade tie among
   otherwise eligible cards. HIGH/MEDIUM/LOW are ordinal hints, not measured odds.
4. When no candidate qualifies, refresh with exponential backoff and up to one
   second of jitter. Stop at the task's snapshot/time limit or the policy's total
   HTTP request limit. Pagination counts toward that HTTP limit.
5. Exclude a placement temporarily after a **definitive capacity rejection**.
   Other models/centers can remain eligible. On cooldown expiry, a fresh snapshot
   is required; an old catalog response cannot establish current availability.

`CANDIDATES_FOUND`, `SEARCH_EXHAUSTED` and `SEARCH_BLOCKED` describe discovery,
not experiment acceptance. Each report includes ranked alternatives, rejection
reasons, cooldown remaining, request counts, elapsed time and next-query delays.
The Markdown view shows the first 20 candidates; JSON retains the complete queue.

## Entry points and bounds

```bash
# Real API reads only; obtain the key through the existing secret mechanism.
uv run runpod-ci-broker --config my-task.json --action discover \
  --search-policy examples/resource-search-policy.json --output out/search
```

The manual main-only `read-only-preflight.yml` workflow accepts `mode: discover`.
It uses the default search policy and the chosen example's limits. CUDA diagnostic
comparison remains a separate `preflight` mode. Discovery has no create/delete
method and cannot consume an authorization file.

| Setting | Default | Meaning |
|---|---:|---|
| `objective` | `availability` | Ordering of eligible candidates |
| `initial_interval_seconds` | 5 | First retry delay, plus jitter |
| `max_interval_seconds` | 30 | Exponential delay ceiling, before jitter |
| `cooldown_seconds` | 60 | Exclude this GPU/center after known refusal |
| `max_observation_age_seconds` | 30 | Maximum age of a returned catalog observation |
| `max_http_requests` | 64 | All GET attempts, including inventory pages |

Task `limits.max_queries` bounds snapshots; `limits.max_wait_seconds` bounds the
search session. They do not authorize additional create attempts or paid runtime.
Network timeouts are bounded but are not a provider-side hard deadline.
The candidate expiry is advisory: the paid controller must refresh inventory,
recheck price/budget and reconcile existing intent immediately before creation.

An optional `--search-history history.json` accepts a serialized `SearchHistory`
bound to the exact task fingerprint. It uses UTC epoch timestamps. Wrong-task or
future entries are refused. History can only exclude candidates; it cannot grant
permission to allocate or raise limits. No global reputation or permanent blacklist
is inferred from one capacity failure.

## Capacity feedback and simulation

`capacity_feedback` accepts the create adapter's classified, nonambiguous HTTP
400/422 capacity refusal. Authentication, permission, account errors, timeouts and
unknown create outcomes are not capacity feedback. The simulator additionally
accepts its explicit fake `no_capacity` event.

Pass `--search-policy` to `simulate` to opt into adaptive ranking. The fixture
`examples/resource-search-capacity-race.json` rejects the first selected placement.
The next attempt uses another eligible placement within the original attempt and
cost reservation. The policy and history are journaled; resuming adopts the stored
policy and refuses a policy change. Existing simulations keep their old behavior
unless this option is selected. Simulator evidence is always marked `mock`.

The live finder only reads inventory. The fixed controller now accepts an opt-in
`search_policy` (or `start --search-policy PATH`) and uses the same admission and
planner interface on a fresh snapshot. It persists classified capacity feedback
and allocation timing in its existing journal. Its original one-create contract
still applies: feedback does not enable another paid attempt. Existing paid
workflow inputs do not activate this option, and generic paid allocation is not
enabled. Consequently this milestone does **not** establish better real allocation
rates. See the [three-stage implementation plan](resource-allocation-plan.md).

## Validation and next integration

Offline tests exercise delayed stock, alternate data centers, cooldown expiry,
expired observations, failed refreshes, complete paginated inventory, request/time
limits, authentication stops, constrained fallback and ambiguous-create recovery.
No new paid GPU is required to run these tests.

The next integration should route the queue through one generic controller and
durable scope ledger. Every attempt must preserve create intent, original budget
and total attempt caps; unknown outcomes must reconcile before any alternative is
created. Only an explicit rejected placement can produce capacity feedback.

Evaluate that integration with comparable paid sessions: time to allocation,
eligible placements considered, explicit capacity refusals, allocation success
within the original wait/budget limits, cost and confirmed cleanup. Report the
sample size and workload constraints. Do not equate catalog hits, mock passes or
one successful GPU smoke with a measured success-rate improvement.
