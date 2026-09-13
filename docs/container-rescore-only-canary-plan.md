# Rescore-only Container canary

## Deployed, routing disabled — 2026-09-13 02:16 UTC

Steps 1–6 are implemented and deployed through the normal queue-draining workflow
with prediction requeue disabled. Independent API verification confirmed:

- Worker `bae10d4a-8f54-47c1-b2f5-aceda0dc2465` at 100%.
- Legacy v253: 12 GiB / 4 CPU / 20 GB, max 10; 45-minute lease unchanged.
- Race-chain v132: 6 GiB / 2 CPU / 12 GB, max 4; unchanged resources.
- Rescore v1: 3 GiB / 1 CPU / 6 GB, max 1, image tag `bae10d4a`.
  Application ID: `a03d47b2-64c8-47be-9b50-c12280facd8c`.
- All three application health responses had no errors or starting/scheduling
  instances. Both prediction and weight queues had `delivery_paused: false`.
- Production `RESCORE_CONTAINER_ENABLED=0`, `RESCORE_CONTAINER_RACES=""`.
  No small-profile live rescore has been verified yet.

Validation: TypeScript 2,087 tests, coverage 97.73/95.10/98.83/98.30%;
Python 2,320 passed, one existing skip, coverage 97.11%; type/lint gates passed.
Model manifest root remained `4a6963f9ad41b7d11f35485fae9a7561d5ef1052ed0cdbe79c659e7508804332`
with `MATCH` and no findings. Final local image size was 425,464,098 bytes.
Linux 3 GiB/1 CPU, network-disabled checks rejected full/prewarm and failed closed
on an attested cache miss; frozen score output and served signatures remained
exactly equal to the earlier benchmark. These checks do not replace live snapshot,
publication, deadline and cleanup verification in step 7.

Deployment drain discovery already selects all applications by the prediction
name prefix; a regression test now explicitly includes the rescore application.

## First live candidate — prepared 2026-09-13 02:35 UTC

- Exact allowlist: `jra:20260913:06:06`, Nakayama 6R, post 12:55 JST.
  At 02:34:46 UTC, all 16 initial predictions were complete in Neon and KV,
  generation-consistent; horse weights had not yet been delivered. The readiness
  card contained races through 12, confirming the offline card context.
- Candidate R2 object: `feat-cache/catalog-v1/jra/20260913/06/06/features.parquet`,
  98,027 bytes; HEAD identity unchanged before/after the read-only download.
  SHA-256 `ef3c75bbf49d315ec9cf583d11c0f16c2081632f450f3e4874b6866adb837af0`.
- Final-image 12 GiB/4 CPU versus 3 GiB/1 CPU scoring matched exactly across
  cold/warm runs, including served signatures and explicit NaN audit positions.
  All prediction scores were finite. Only the pre-weight auxiliary
  `weight_diff_from_avg` column contained NaN, represented as a tagged NaN in
  comparison JSON rather than imputed or dropped. Canonical output SHA-256:
  `9e6e8faaf6f9078f9ddc9e47061213196de341b13e0254bf1dfb5ac86d379a6e`.
- Frozen routing computed the `jra-joken-703-dirt-intermediate-qsm-gated-v1`
  Stage-2 branch, then selected `jra-cb-stage1-marketfree235-2013` through the
  odds-missing Stage-1 guard. This is not proof of the future fresh-odds route;
  live serving remains the complete authoritative Python pipeline.
- Activated through the normal queue-draining deploy at 02:38–02:39 UTC after
  a fresh eligibility check (77 minutes to post, weights not delivered).
  API verification confirmed Worker `424e7de3-c086-45c9-9578-c14c196b5aa0` at 100%,
  gate `1`, exactly this race key, and both delivery queues unpaused. Container
  code did not change, so applications retained the verified `bae10d4a` image.
  Live canary publication was subsequently verified below.
- A non-canary Nakayama 5R rescore already in flight at the update was reset by
  the DO code update. Its ordinary retry completed scoring/publication at
  02:40:43 UTC and terminal stop at 02:40:48 UTC, ahead of its 12:25 JST post.
  Thus an empty Container drain observation is not proof of zero in-flight
  Worker requests. Retain this deployment-race caveat; do not label the update
  disruption-free. Independent readiness at 02:44:21 UTC confirmed all 11
  post-weight predictions complete in Neon and KV, one matching generation,
  `kvAfterWeight=true`, snapshot 11:38:08 JST, with 41 minutes to post.
  The canary 6R still had no delivered weights at that check. A separate
  Container log query confirmed the 5R retry's resource event: 40.938 seconds,
  162 guest-system samples, peak 570,781,696 bytes (about 544 MiB), guest total
  12,814,610,432 bytes. Native cgroup data was unavailable. This is a legacy
  measurement and not evidence that the small canary has executed.
- After observation, restore gate `0` and empty allowlist, retaining the binding.
  Avoid another rollout while an observed prediction is still in flight.

## Live result — verified 2026-09-13 03:10 UTC

- Natural weight/entry delivery: 12:06:27 JST, all 16 horses active, no exclusions.
  Worker preflight reported a feature HIT; Python accepted cache attestation with
  `exact-entry-and-identity-match` for JRA 06/06. No forced replay was submitted.
- The dedicated namespace completed the rescore in 78.789 seconds as measured
  by the Worker; Python resource telemetry recorded 63.253 seconds, success.
  KV publication was written by 03:08:18 UTC. Control stopped
  `rescore-predict-jra-2` normally at 03:08:21 UTC, before 12:55 JST post.
- Independent readiness at 03:10:00 UTC confirmed all 16 post-weight predictions,
  Neon and KV complete, one matching generation, `kvAfterWeight=true`, and
  snapshot 12:06:27 JST. Neon generation: `2026-09-13 03:08:01.199731+00`.
- Telemetry: 251 samples at 250 ms, guest-system peak **402,571,264 bytes
  (about 384 MiB)**, guest total 3,389,485,056 bytes. Native cgroup fields remain
  unavailable. This sampled guest measurement is not an exact per-job peak or
  a universal sizing bound. Attestation/resource events both reported BOM.
- Small application v1/image `bae10d4a` was independently confirmed, with
  instance `8b71f8764b61d5482304dceb7dbcf7e519f8903f685f50a9d454ad586bef0cc7`
  named `rescore-predict-jra-2`; no health errors, failed or starting instances.
  Lifecycle stop is established by the control log, not merely health counters.
- This verifies a fresh, naturally triggered publication on the small role.
  It does not establish exact live-output equality against a simultaneous
  large-profile replay, representative workload coverage or billed savings.
  No native artifacts were uploaded; Python remained authoritative.
  A read-only Neon check confirmed 16 rows in the live generation using
  `jra-joken-703-dirt-intermediate-qsm-gated-v1`; the older 16-row generation
  used `jra-cb-stage1-marketfree235-2013`. This confirms that the live model
  differed from the frozen pre-weight final model, not equality of outputs.
  A read-only schema check found no served-signature column in the prediction
  table. Stored model IDs and odds/weight audit columns alone do not establish
  the complete routing signature or an immutable set of all live inputs;
  do not reconstruct a purported exact reference from later mutable snapshots.
- The normal no-requeue deployment restored production gate/allowlist to
  `0`/empty. Independent API verification confirmed Worker
  `e6514693-2976-4b51-a3db-d37331af5b98` at 100% (deployment created
  03:20:26 UTC), both queues unpaused and the cleanup binding retained.
  All 2,087 tests and format/type/lint gates passed; coverage remained
  97.73/95.10/98.83/98.30%. A transient D1 7403 error recovered through the
  existing migration retry before queue pausing; no migrations were pending.
  Subsequent Hanshin 7R legacy execution also reached terminal stop at
  03:22:20 UTC; no new update/reset error was observed in the watcher.

## Additional synthetic fresh-board comparison — 2026-09-13 04:08 UTC

`container-rescore-synthetic-20260913.json` records a network-disabled comparison
using fixed synthetic odds/ranks/weights and the production late-binding helpers.
All 16 scoring rows and served signatures matched exactly across 12 GiB/4 CPU
and 3 GiB/1 CPU, cold/warm, with finite scores and no nonfinite audit values.
The signature now covers `stage2=confidence-gate-swap`, `stage1=fresh`, final
`jra-joken-703-dirt-intermediate-qsm-gated-v1`. This extends the earlier
odds-missing comparison, but is not an exact replay of live inputs or a native
Worker comparison. Local cgroup peaks and run times are not Cloudflare bounds
or causal savings estimates. Source parquet contains 422 columns; do not confuse
that physical column count with model feature counts or transformed row fields.

## Evidence and scope

- Production JRA rescore: sampled guest-system peak 562,925,568 bytes, 194 samples;
  successful publication/cleanup observed on the existing large profile.
- Frozen JRA 06/02 scoring on identical images/inputs matched exact output hashes
  and served signatures at 12 GiB/4 CPU and 3 GiB/1 CPU (cold/warm process runs).
- This does not prove arbitrary categories/full builds fit. See the recorded
  limitations in `container-rescore-offline-canary-20260913.json`.
- Current Python routing can differ from the hardcoded native Worker routes;
  keep Python authoritative. Do not restore missing native model artifacts yet.

## Implementation sequence

1. Add an authoritative `rescore` runtime role in Python. Require single-race
   rescore mode, a rescore callable and cache attestation before any work. Reject
   full prediction and unattested compatibility fallback. Retain the existing
   attested-cache-miss error path (never rebuild in this role).
2. Add an optional rescore namespace and an exact race-key allowlist, disabled by
   default. Route only the first Queue attempt; force-legacy and retries use the
   existing namespace. The established weight/deadline/cache preflight remains
   mandatory before starting either namespace.
3. Qualify the new DO names and carry the chosen role/namespace through claims,
   request dispatch, stop handoff and slot release. Preserve the same rescore
   execution identity, snapshot identity, retry budget and publication protocol.
   A failed first attempt must hand off cleanup before the normal retry takes
   the legacy path; do not enqueue copies that reset retry budgets.
4. Add a distinct Container class with 1 vCPU / 3072 MiB / up to 6000 MB disk.
   Verify the complete image fits the disk limit before deployment. Use a bounded
   short idle fallback for this role, without changing legacy's 45-minute lease.
   Reject day-base and full-prediction requests at the DO boundary as well.
5. Update role validation, namespace lookup, admin/control/cleanup, deployment
   drain discovery, class exports/bindings/migration, and role-sensitive tests.
   All active resource roles must be visible to future drain/control operations.
6. Run full Python and TypeScript gates without changing thresholds or scope.
   Initially deploy with routing disabled through the normal draining workflow.
7. Choose a future eligible JRA race after verifying its frozen scoring context,
   then enable an exact bounded allowlist. Verify same snapshot/artifact identity,
   successful scoring, publication, deadline and cleanup on the new profile.
   Collect memory across additional representative branches before expansion.

## Rollback and completion

- Disable the allowlist/gate; ordinary and retried work continues on legacy.
- Do not remove a class/binding while work or cleanup messages still refer to it.
- No changes to day-base size/lease, retry budgets, model accuracy or freshness.
- No broad rollout based on one offline fixture. No full-pipeline cost or deadline
  claim from a score-only benchmark. Actual billing savings remain unmeasured.
