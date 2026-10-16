# Workflow-owned day-base Container cleanup

The day-base Workflow waits with `step.sleep`; an accepted build is not started again on each wake. The Container remains necessary until its result has been copied out of its in-memory cache and canonical R2 readiness is verified. Reducing `sleepAfter` alone can lose that result and force a rebuild.

## Stop before Worker-only processing

For a repair-owner Workflow, the completion sequence is:

1. Confirm durable day-base readiness and heartbeat the repair reservation.
2. Execute `stop-day-base-container` as a durable Workflow step.
3. Release the repair reservation.
4. Fan out predictions and optionally warm heatmaps on Workers.

The stop step uses the existing atomic `consumeContainerStop` protocol directly, rather than waiting for a prediction-queue pickup to finish its Worker-side processing and enqueue cleanup. Its acceptable identities are the canonical and stale-rebuild work keys of the **same date, category and generation**. It never sets `force` or `allowUnowned`.

- Observers and workflows whose readiness is still pending do not stop Containers.
- A newer owner causes a fenced skip, not forced termination.
- Transport or stop-confirmation errors propagate to the Workflow's step retry mechanism.
- Repeated execution uses the existing stopping/destroyed fence and physical-stop confirmation; it does not bypass coordinator ownership.
- Existing queue cleanup remains a compatible fallback and may finish first.

## Queue pickup: stop handoff before Worker-only processing

`completeLandedDayBase` is shared by delayed pickups and the authenticated admin pickup. All callers first verify durable readiness. It now awaits the existing owner-fenced cleanup handoff **before** assembling per-race caches or fanning out predictions. The control queue (or its durable cleanup fallback) can stop the Container while Workers continue that work. No lease is released early and no force/unowned stop is added.

If neither queue accepts cleanup, Worker materialization/fanout does not start and the error propagates for retry. Later materialization or fanout errors still propagate, but their cleanup has already been handed off. The old combined completion/cleanup error is no longer needed because cleanup failure prevents completion work from starting.

Local verification on 2026-10-10: 2,447 tests passed; coverage statements 97.87%, branches 95.32%, functions 99.12%, lines 98.47%; type checking, zero-warning lint and changed-file formatting passed. The regression test verifies handoff acknowledgement before materialization/fanout and preservation of the exact generation fence. The local deploy CLI also accepts current Wrangler's `{ instances, result_info }` response, rejecting unknown/non-final pagination instead of incorrectly treating a partial page as drained.

Production rollout: `3702ed48-db0a-4c56-b782-beac0f92f349` → **`12b293c3-3d23-40d4-9dd9-7ce374a7b3f2`**, verified at 07:52 JST on 2026-10-10. The isolated candidate replaced only the reviewed function in the live module: the pre-edit TypeScript function compiled byte-for-byte to the deployed baseline, and all bytes outside that function remained unchanged. Strict Wrangler dry-run and independent downloaded-module comparison passed. All 54 bindings, runtime/public settings and 10 cron schedules were preserved.

The existing Queue-draining deploy ran with `PREDICTION_WORKER_ONLY=1` and `REQUEUE_FINISH_POSITION_PREDICTIONS=0`. No live Container needed stopping; both start queues resumed. Independent postflight confirmed all five Queue configurations unchanged, four non-DLQ queues unpaused, three Container application image/version pairs unchanged, no unsafe Container instances, and 10/10 prediction readiness 59/59 complete (all 59 still awaiting weight publication). No model/native-writer switch or prediction replay was performed.

Evidence: `.cache/heatmap-recovery-20261010/container-final-*.log`, `container-candidate/patch-manifest.json`, `container-postdeploy/{deployment-audit,operations-audit}.json`; pre-edit files are in `container-change-baseline/` there. Exact saved Container seconds have not been measured: this advances the stop **handoff**, while the control queue still owns physical termination.

## Per-race rescore: stop handoff before shadow and publication

On 2026-10-10, the live `processContainerPerRaceRescore` still awaited Ban-ei shadow/parity and Worker KV/Viewer publication before requesting cleanup. The existing, previously undeployed source change now hands cleanup off after successful Container output and `score_complete` / `neon_complete`, before those Worker-only steps. If both cleanup queues reject the early request, publication still proceeds and the existing terminal cleanup retry remains. Successful handoffs are not sent again on normal or deadline completion. No force/unowned stop, early slot release, model switch or image update was added.

This function alone was rolled out from `12b293c3-3d23-40d4-9dd9-7ce374a7b3f2` to **`fb1c9690-3257-48fc-a5db-6af79a37443e`**, independently verified at 10:16:52 JST. The candidate preserves the live module's identifier disambiguation; reversing only the reviewed ordering/duplicate-stop changes reproduces the old function exactly. All other module bytes remained unchanged. The existing Queue drain procedure found no running prediction Containers or Workflows and performed a Worker-only deployment without replay.

Added regression checks prove cleanup while the Ban-ei shadow is still pending, cleanup acknowledgement before KV publication, and publication plus terminal cleanup retry when the early handoff fails. Local verification: **2,449 tests**, coverage statements 97.87%, branches 95.32%, functions 99.12%, lines 98.47%; type checking, zero-warning lint and formatting passed. Independent postflight preserved all 54 bindings, 10 cron schedules, runtime/public settings, five Queue configurations and three prediction Container image/version pairs. Start queues resumed; prediction readiness was 59/59, including all six published weight generations then available (53 awaiting weights). At 10:18:10 JST all 59 heatmaps, 678 runners, 11,389 numeric cells and 59 locally rendered tables passed again.

Evidence: `.cache/container-runtime-20261010-followup/` (`function.diff`, `coverage.log`, `container-postdeploy/{deployment-audit,operations-audit}.json`) and `.cache/heatmap-recovery-20261010/rescore-postdeploy/content-render-audit.json`.

### Measurement scope

The 21:00–01:00 UTC application/hour metrics and 37 sampled historical log events do not establish a paired per-job runtime reduction. Only the legacy prediction app had metrics in that result: workload CPU sums were 38.42 seconds in the 23:00 bucket and 61.85 seconds in the 00:00 bucket. These are returned aggregate CPU counters, not elapsed Container time, saved seconds, or a global minimum. MLflow's separate application was excluded. Workload and billing-side metrics have different semantics and possible sampling/delay; do not subtract them as a saving.

Bounded pre/post tails initially contained no correlated rescore-and-physical-stop samples. Actual saved seconds remain unmeasured; additional observation must use naturally scheduled work, not artificial model replay. See [Cloudflare's metrics and billing dataset definitions](https://developers.cloudflare.com/analytics/graphql-api/tutorials/querying-container-metrics/).

## Runtime effect and limits

This removes dependence on the prediction queue for cleanup once the owner Workflow observes durable readiness. Worker-only fanout and heatmap work no longer precede this stop. If the pickup queue already stopped the Container, additional savings are zero.

The normal Workflow readiness polling interval remains five minutes; retries and external failures can extend observation latency. Container computation, result pickup latency, cold starts, and the separate race-chain warm-reuse policy are unchanged. There is no claim of a global runtime minimum or a measured production speedup. Production runtime savings require a before/after comparison of build completion, R2 publication and Container stop timestamps after deployment.

Regression tests in `src/race-day-workflow.test.ts` cover ordering, generation-scoped stop messages, observer/pending skips, stop errors and fenced skips. `src/day-base-pickup.test.ts` verifies shared stale-generation keys. Existing coordinator and Container-control tests cover atomic fencing and stop confirmation.

## Post-weight serving: quality-preserving Cloudflare target

This is the migration contract, not a claim that migration or latency validation is complete.

- Preserve the production models, every ensemble component, feature transforms, calibration, and output selection rules. With identical frozen inputs and artifacts, full and post-weight paths must produce equivalent feature vectors and predictions. Fresh weight/odds may change predictions; faster execution must not replace missing calculations with a simpler model. Statistical accuracy still needs evaluation on held-out outcomes.
- Precompute immutable history features and stage model artifacts in R2 before weights arrive. Workflows handle waits/retries; Workers validate snapshots, do equivalent lightweight calculations, and publish/warm display caches. Containers run only work that needs their compute/runtime. Do not keep one running for a Workflow sleep or speculative warm.
- Use the existing dedicated weight queue and race/snapshot deduplication. Measure acquisition, admission, execution, durable result, KV and Viewer timestamps. Initial targets are Viewer-ready by T-10 minutes and acquisition-to-Viewer p95 <=120 seconds when inputs/foundation are available. These targets are unverified; late source delivery must be reported separately. Missing the early target must not silently discard valid predictions or relax quality requirements.
- After a rescore has durably persisted its scores, hand off fenced Container cleanup before Worker-only shadow comparison, KV publication and Viewer warming. An early cleanup failure must not prevent publication; terminal cleanup retries remain required. A stop handoff is not evidence of a physical stop.

### Removing mandatory Neon dependencies

Existing production call sites still require Neon in `_flush_scored` (Container persistence), the default `prediction-kv-writer.ts` path (publication reads), `focused-full-completion.ts` (completion checks), and `prediction-readiness.ts` (readiness). Removing only the write would break publication and recovery.

The target is an immutable per-race result artifact in R2, with a D1/DO generation manifest linking the runner set, frozen inputs, feature generation, model artifacts, results and completion state. Workers publish the existing Viewer schema from that artifact. Stage immutable results first, validate completeness and provenance, then advance the authoritative generation with concurrency fencing; a late older result must not replace newer weights. KV remains a serving cache, not the sole durable record.

Reuse the existing output-mapping rules, including model selection, nullable probabilities and confidence aggregation. Validate artifact-backed reads against current Neon-backed results before switching completion/readiness and publication together. Only then make Neon compatibility synchronization asynchronous and retryable without rerunning inference. Historical feature acquisition and model metadata reads must also be inventoried; moving result writes alone is not Cloudflare-only prediction generation.

Cutover requires frozen-input parity across JRA/NAR/Ban-ei, malformed/partial/stale-generation rejection, repeated-delivery idempotency, recovery between artifact write and manifest commit, and tests proving that Neon unavailability does not prevent prediction, publication or readiness. Workers inference flags remain off unless their full production model composition passes these checks. No lower coverage thresholds, forced live-job termination, or unrelated secret/configuration changes are permitted.

#### Implemented reader slice — not a production cutover

`prediction-result-artifact.ts` validates a specified content-addressed R2 object against its trusted reference: SHA-256, race scope, input fingerprint, generation, selected model, timestamp, exact horse IDs/numbers and complete ordinal ranks. `prediction-kv-writer.ts` accepts that reference explicitly and reuses its existing Viewer projection. Invalid or missing specified artifacts fail closed without reading Neon or overwriting KV; unspecified references retain the existing Neon path. Current production callers do not supply references.

This validates content, **not ownership or freshness**. The producer, authoritative manifest, publication fencing and completion/readiness readers are not connected yet. The existing coordinator's rescore key includes the weight timestamp/count/hash: execution-ID checks protect one weight generation, but do not prevent two different generations from finishing out of order. Before enabling this path:

- Preserve the exact selected model policy (including NAR Transformer preference), frozen input/runner provenance and database-generated timestamps during dual-output validation.
- Order authoritative generations at the race level, independently of completion time, and reject stale commits without cancelling running calculations.
- Serialize publication and recover durable publication intents. A check immediately before an external KV write alone does not prevent an older in-flight write from finishing last. Do not hold the shared coordinator's `blockConcurrencyWhile` across external R2/KV/network I/O.
- Test overlapping generations, duplicate deliveries and crashes between object write, manifest commit and publication. Keep completion/readiness on the old path until their equivalent readers are verified.

#### Image rollout safety

The [documented rollout sequence](https://developers.cloudflare.com/containers/configuration/rollouts/) activates Worker code before image processing and rollout; it is not transactional. `--containers-rollout=none` is **Worker-only**, not image-only. The [Containers CLI reference](https://developers.cloudflare.com/workers/wrangler/commands/containers/) documents building/pushing images, but does not establish an image-only application rollout command. These operations are not a verified workaround for unsafe Worker secret inheritance.

Selected running instances receive SIGTERM and may subsequently receive SIGKILL; a grace period is connection-age based, not proof that work is finished. Consequently, rollout flags alone do not satisfy the no-interruption requirement. Preserve the existing verified drain/ownership procedure, and do not mutate applications through an unverified API to bypass deployment safety.
