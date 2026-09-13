# Container cost optimization (2026-09-13)

## Constraints

Preserve prediction parity, pre-race deadlines, fresh weight/entry generations,
MLflow UI availability and ten-minute discovery cadence. Do not shorten the
45-minute day-base lease or remove retry/repair safety checks. Unrelated research
working-tree changes are outside this task.

## Implementation sequence

1. MLflow: compute a source fingerprint in the Worker/DO before starting a
   Container; persist checkpoints only after successful sync; fail open to full
   sync on probe failure and periodically reconcile. Sync only changed days.
   Serialize cron syncs, preserve UI inactivity grace, shorten cron-only idle
   grace, and prevent expiry while an exec is in progress.
2. Prediction: preflight required weight snapshot generation before starting
   expensive rescore work; retain in-container generation checks and existing
   superseded-generation/retry handling. Verify existing day-base completed
   artifact recovery paths before altering them.
3. Resource sizing: add measurable per-workload memory/runtime evidence before
   decreasing production memory. Keep day-base sizing unchanged; only use smaller
   rescore instances after peak-memory and prediction parity/deadline validation.
4. Validate changes with package lint, typecheck, format and full coverage gates;
   deploy only validated changes, verify live logs and compare daily billable
   memory/CPU/disk usage over comparable workloads. Keep rollback instructions.

## Baseline and limitations

Billable Usage for 2026-08-24 through 2026-09-11 UTC reported Containers memory
$38.7701975, CPU $7.07098, disk $1.95621013, egress $0. API application configuration
matches prediction 12 GiB, race-chain 6 GiB and MLflow 4 GiB. Other account apps
exist. Log count is not an attribution of billable runtime.

Logs sampled September 11-12 show MLflow startup/shutdown at ten-minute cadence,
rescore completions around 16-53 seconds, a weight-generation mismatch, and
missing-status/day-base pickup exhaustion. Some structured container events did
not expose message bodies. No peak memory measurement was recovered; absence of
OOM log matches is not proof that downsizing is safe.

## Acceptance and rollback

- Identical input does not start MLflow before reconciliation is due; changed,
  deleted or newly inserted source rows trigger sync; failures are not checkpointed.
- UI requests and overlapping syncs must never be stopped by cron cleanup.
- Snapshot mismatch must not invoke the predictor; valid generations retain their
  existing processing and final validation. Transient errors keep retry semantics.
- No reduced coverage thresholds or narrower source measurement.
- Record implementation/test/deployment results below. Do not describe pending
  measurement or deployment as complete.

## Status

Initial optimization/telemetry deployment completed on 2026-09-12 at 19:44–19:47
UTC. Live verification and sizing remain in progress; no peak-based sizing or
billable savings have yet been established. The full optimization rollout is
**not complete**.

### Implemented locally

- MLflow fingerprints complete prediction rows for every requested date (including
  empty dates), syncs changed days serially, and reconciles at least hourly. The
  Neon probe has a five-second timeout and falls back to the whole window on
  failure. Failed syncs never advance checkpoints. The Python preview CLI reads
  only these prediction tables and returns nonzero when its summary has errors;
  nightly result/artifact evaluation remains separate and unchanged.
- MLflow cron-only idle grace is 30 seconds; UI activity retains five minutes.
  Active sync renews activity. An exec exceeding ten minutes is signalled for
  termination (not the UI server) and fails the caller. Writer serialization and
  activity protection remain until exec output settles, including when signalling
  termination fails; timeout alone does not confirm process exit.
- `MLFLOW_SOURCE_GATE_ENABLED=0` bypasses fingerprints/checkpoints and syncs the
  whole window on every cron. This rolls back the source gate only; reverting
  the deployment restores the original lifecycle behavior too.
- Container rescore validates the requested weight generation before exec and
  checks the pre-race deadline again after preflight. Final in-container checks,
  superseded-generation handling and transient retries remain in place.
- Prediction/day-base work emits `container-resource-usage` JSON containing
  category, date, mode, race, elapsed seconds, success, current memory, cgroup
  memory limit and **container-lifetime** peak. Missing cgroup counters are null.
  The lifetime peak includes child processes and earlier jobs, so it must not be
  mislabeled as isolated rescore peak or used as proof of lower memory needs.

### Existing protections retained after review

`day-base-pickup.ts` already checks canonical readiness against current Catalog
and running-style watermarks before touching a Container. A delayed duplicate
pickup can finish without a cold start. `day-base-prewarm.ts` similarly guards
completed foundations and busy generation ownership. These paths passed the
full cron suite. Mere R2 object presence or missing status is not sufficient
proof of fresh completion: do not replace their checks with an existence-only
shortcut, suppress repairs, shorten the 45-minute lease, or reduce retry budgets.
Further missing-status/rebuild changes require a traced failing generation;
aggregate error counts alone do not establish redundant work.

### Local verification

| Package                                  | Tests                         | Coverage                                                           | Other checks                              |
| ---------------------------------------- | ----------------------------- | ------------------------------------------------------------------ | ----------------------------------------- |
| `apps/mlflow-ui-proxy`                   | 52 passed                     | statements 99.39%, branches 97.72%, functions 96.77%, lines 99.37% | tsc, lint, format passed                  |
| `apps/finish-position-cron`              | 2,054 passed                  | statements 97.72%, branches 95.09%, functions 98.83%, lines 98.29% | tsc, lint, format passed                  |
| `apps/finish-position-predict-container` | 2,292 passed, 1 existing skip | combined branch coverage 97.06%; resource module 100%              | Ruff format/check and basedpyright passed |

The local deployment artifact verifier returned `MATCH`, no integrity findings,
144 selected artifacts and 293 observed artifacts. Manifest root SHA-256:
`4a6963f9ad41b7d11f35485fae9a7561d5ef1052ed0cdbe79c659e7508804332`.
It reported unselected manifest/observed artifact warnings; these files were not
removed or promoted. This validates local selected bytes against the manifest,
not equality with the currently deployed image.

Coverage thresholds and measurement scope were not changed. These are local
checks, not live SQL validation, prediction parity certification on a smaller
machine, or production deployment evidence.

### Production rollout baseline (2026-09-12 19:43 UTC)

User authorized production deployment, verification, measurement and safe
right-sizing. Captured through Executor Cloudflare API before any deployment:

| Worker                            | Current Worker version                 | Container application version / image tag | Provisioned resources         |
| --------------------------------- | -------------------------------------- | ----------------------------------------- | ----------------------------- |
| `mlflow-ui-proxy`                 | `5d92b2a4-923d-4d22-b66c-b128881c9fdf` | 2 / `2c742714`                            | 0.5 vCPU, 4 GiB, 8 GB, max 1  |
| `finish-position-cron` prediction | `d20ab9f0-7e55-4661-96e2-96c4768c92e8` | 250 / `d20ab9f0`                          | 4 vCPU, 12 GiB, 20 GB, max 10 |
| `finish-position-cron` race-chain | same Worker                            | 129 / `d20ab9f0`                          | 2 vCPU, 6 GiB, 12 GB, max 4   |

The observed race-chain CPU is **2**, not the previously quoted 1 vCPU; use
actual deployed configuration for comparisons. Keep these images/version IDs
for rollback planning, not an automatic rollback that bypasses queue safety.

### Initial deployment and live verification

- MLflow Worker `bbf68378-3a64-4e29-8981-e49607f86925` and prediction Worker
  `ddc0ca7f-7318-44bb-810d-864266b1f0cb` were deployed and independently confirmed
  at 100% through the Cloudflare API. No D1 migrations were pending. Prediction
  queues were resumed, no model requeue was requested, and Colima shut down.
- Prediction root endpoint returned 200; authenticated prediction readiness
  returned 200. MLflow correctly returned 401 without credentials. Local MLflow
  UI credentials were unavailable, so authenticated UI verification is pending.
- The 19:50 MLflow cron logged a source-probe timeout and took the full-window
  fallback. The original concatenated date predicate could not use the normal
  composite date index. A tuple-bound predicate was implemented and tested,
  including year rollover. Live Neon connectivity took 267 ms and the corrected
  three-day fingerprint query took 116 ms (September 13 nonempty, 14–15 empty).
  The corrected MLflow Worker was deployed as
  `df10fae6-25f9-4e68-a2a5-93f80da698e8` at 19:52 UTC. Subsequent
  unchanged cron checks remain pending; the fallback has not been
  mistaken for successful gating.
- The corrected 20:00 cron produced persisted preview tags in the MLflow backend:
  JRA at 20:01:46 UTC (2 runs), NAR at 20:01:54 (2 runs), ban-ei at 20:01:58
  (1 run), all for September 13. Read-only SQL confirmed these timestamps.
  Container logs showed startup, three HTTP 200 readiness responses and normal
  shutdown at 20:03:27; API status was inactive at 20:03:41. Thus the changed
  sync and server response were verified independently of sampled Worker logs.
  No post-deployment prediction workloads/resource events were observed through
  20:04; listed prediction/race-chain instances were inactive. This is not
  evidence that their memory requirement is low.
- At 20:20:25 UTC, a bounded live tail captured the scheduled invocation with
  outcome `ok`, no exceptions and 710 ms wall time. The DO RPC explicitly logged
  `unchanged ... Container not started` for all three dates (September 13–15),
  outcome `ok`, 1,312 ms wall time and 2 ms CPU time. This directly verifies the
  no-start path. At 20:10 there were also no container log events or preview tag
  rewrites. API `inactive.updated_at` can still change during DO monitoring;
  **do not treat that timestamp as a container boot/runtime counter**.
  The live tail was stopped after capturing the required invocation. This proves
  avoided container execution for an unchanged cron, but not a measured daily
  invoice reduction or a safe smaller prediction memory limit.
- Observability event-view parsing failed in the integration; calculation view
  grouped by `$metadata.message` successfully retrieved log messages. Counts
  may be sampled/scaled and must not be used as exact execution counts.

### Billing measurement checkpoint

At 2026-09-12 20:25 UTC the billing endpoint still returned 613 total records,
including 96 Containers daily service groups, with no missing Container costs.
The latest charge interval ends at **2026-09-12 00:00 UTC exclusive**, so this
contains no post-deployment usage. The full non-secret daily baseline is saved
in `docs/container-cost-baseline-20260913.json`. It is account-wide, not specific
to the three changed applications. Do not attribute other applications' usage
changes to this optimization. Recheck after a complete post-change UTC day has
been ingested; frequent same-day polling cannot establish an invoice reduction.

### Expanded goal: structural reduction, not only smaller instances

The user requested substantially larger savings. A **50%+ reduction in Containers
cost is an investigation target, not a forecast or verified result**. Earlier
$5–10/month MLflow and $10–20/month combined figures were rough scenarios; they
must be revised using attributable workloads, and already-enabled optimizations
must not be counted again.

- `JRA_WORKER_RESCORE_ENABLED=1` is already configured and dispatches to the
  native Worker before Container fallback. Investigate actual fallback reasons
  rather than claiming that simply enabling JRA Worker rescore is a new saving.
- `BANEI_WORKER_RESCORE_SHADOW_ENABLED=1` already runs comparison-only scoring.
  Container output is authoritative. Promotion requires representative parity
  evidence (runners, model/routed branch, rank and score/probability output,
  fresh generations, deadline/publish behavior), not just unit-test success.
- Inspect day-base/race-chain reuse and repeated work while retaining exact
  freshness checks. Existing split, fused race-chain and market foundation
  features are already enabled; do not count them as newly introduced savings.
- A code audit found another prerequisite: the Ban-ei Worker shadow scorer
  currently emits raw CatBoost scores, while the Python serving path applies
  `adjust_prediction_rows_with_prophet` under the enabled frozen cell/branch
  policy when sufficient trend coverage exists. Both Ban-ei base/sim policy
  cells are enabled. Port and verify this serving transformation (and audit the
  other output fields) before considering shadow promotion; equal raw model
  scores do not establish equal served predictions.
- `docs/container-cost-attribution.graphql` contains a bounded read-only
  predeployment usage/max-metric query, approved by the user and executed
  successfully (GraphQL `errors: null`). The result is saved in
  `docs/container-cost-attribution-20260912.json`.
  Schema inspection verified usage sums `allocatedMemory` and `allocatedDisk`
  are byte-seconds and `cpuTimeSec` is seconds. Metrics `max.memory` has no unit
  in its schema description; do not use its numeric value to resize until its
  unit and sampling semantics are established against documentation/cgroup data.
- The first September 10–12 historical event search returned no JRA-native,
  fallback or Ban-ei parity events. This is inconclusive, especially with 10%
  Worker log sampling. Collect actual race-time evidence rather than promoting
  the shadow scorer on the basis of absent failures.
- Keep NAR's Transformer/ensemble model contract intact. Moving scoring into a
  Worker or another runtime is acceptable only if the entire served model path
  and its publication guarantees pass comparison; removing ensemble branches,
  sacrificing freshness or moving production scheduling to a local Mac is not
  an authorized cost shortcut.

### Attributable predeployment usage and first sizing canary

For September 12 00:00–19:40 UTC, GraphQL returned usage for only these three
applications (3 rows, below the query limit 50):

| Application       | Allocated-memory-time share | Memory sum converted to GiB-seconds × rate | GraphQL maximum workload memory    |
| ----------------- | --------------------------- | ------------------------------------------ | ---------------------------------- |
| Legacy prediction | 79.58%                      | $2.66370                                   | 5,103,050,752 (~4.75 GiB as bytes) |
| Race-chain        | 11.41%                      | $0.38205                                   | 1,400,750,080 (~1.30 GiB as bytes) |
| MLflow            | 9.01%                       | $0.30165                                   | 956,948,480 (~0.89 GiB as bytes)   |

The dollar column is an estimate using $0.0000025/GiB-second, not the priced
Billable Usage record. The interval is partial, includes a JRA race day, and
must not be extrapolated into a guaranteed month. The metrics endpoint covers
workload processes; the usage endpoint includes the micro-VM overhead used for
billing. Sampled maxima are not isolated workload cgroup peaks.

Official limits verified at
<https://developers.cloudflare.com/containers/platform/limits/>: custom profiles
require at least 1 vCPU and **3 GiB per vCPU**. Therefore 4 vCPU/8 GiB and
2 vCPU/3 GiB are not valid conservative alternatives. Do not assume CPU can
always remain unchanged when reducing memory.

The first sizing canary is MLflow: 4 GiB/0.5 vCPU/8 GB → **3 GiB/1 vCPU/4 GB**.
Observed workload memory has >3x headroom and observed disk was ~0.98 GB; CPU is
increased rather than reduced. Local checks passed (52 tests and all gates).
Two Wrangler deploy attempts uploaded the unchanged Worker code but failed to
build because Docker Hub timed out; the Container profile was not changed by
those failures. To avoid rebuilding unchanged Python code, the existing image
was retained and a resource-only rollout was created via Executor at 20:57:45
UTC: `c48724c9-df7d-4174-9c36-ae455204d2ad`, target application version 4.
At 20:58 the rollout was still starting; application current version remained 3.
At 21:12, API inspection confirmed application **version 4**, **3072 MiB /
1 vCPU / 4000 MB**, unchanged image/network/logging, no health errors or failed
instances, and no active/starting instances after synchronization.
The most recent Worker upload is `c9c6c455-cc54-4c3c-92b3-d28f52a353dc` at 100%.
Do not call the smaller profile complete until actual application configuration
and workload execution are verified. Validate cold-start periodic reconciliation,
persisted preview values, no OOM/timeouts and subsequent idle shutdown. Rollback profile is
`standard-1` with the same code/checkpoints. This is not evidence for resizing
prediction/day-base; they remain unchanged while collecting per-workload data.

Post-canary verification (September 12 UTC):

- Live tail: periodic reconciliation completed successfully in 92.740 seconds;
  three finish-position MLflow runs and two running-style MLflow runs updated
  (these are tracking runs, not counts of races or new prediction generation).
- Read-only backend SQL confirmed September 13 preview timestamps: JRA
  **21:02:24**, NAR **21:02:31**, Ban-ei **21:02:35**, with 2/2/1 runs respectively.
- Normal inactivity shutdown at **21:04:29**, no sync exceptions. The SDK alarm
  cadence made actual post-sync idle about 90 seconds, despite a 30-second grace;
  do not claim an exact 30-second shutdown time.
- The subsequent cron returned `ok` and all three dates logged `unchanged ...
Container not started`. The bounded watcher was stopped after this evidence.
- Provisioned memory is 25% lower and disk 50% lower. Actual total dollar savings
  still require post-change usage/billing; CPU is larger, and these percentages
  must not be called total cost savings.
- No prediction `container-resource-usage` events were returned for 20:50–21:12;
  this is not evidence of low memory demand or an error-free day-base build.

### Log-query validation at 21:24 UTC

A query using `datasets: ["finish-position-cron"]` used a Worker name rather than
its dataset name and cannot establish absence of events. Repeated with the API's
supported `datasets: []` (all datasets), the narrow day-base/resource search was
still empty. A positive-control query returned known MLflow sync events, proving
that the corrected query path and event parsing work. A corrected September 12
search for native JRA success, Container fallback and Ban-ei shadow parity also
returned no events; with sampling, this remains inconclusive rather than proof
of native success or serving parity. Avoid repeated identical empty queries.

Application instance listing returned 12 legacy and 9 race-chain instances, all
inactive, below the page limit 20. This is a point-in-time state check, not a
history of boots or proof that the 06:00 JST prewarm ran successfully. Normal
weight-driven work later in the morning is still needed for representative
post-deployment cgroup measurements.

### Historical rescore investigation at 21:54 UTC

A broader literal `rescore` search returned historical execution events, including
JRA `Rescore container ... durationMs=66729` and a separate JRA feature-cache HIT
before Container entry. The returned `races=16` is the existing log field, not
independently verified race cardinality. Ban-ei also logged deferrals for
`initial-prediction-or-cache-incomplete` at attempts 56 and 85; queue deferrals do
not establish Container boots or attributable Container cost.

Both current Worker settings and predeployment version
`d20ab9f0-7e55-4661-96e2-96c4768c92e8` explicitly have
`JRA_WORKER_RESCORE_ENABLED=1` and `BANEI_WORKER_RESCORE_SHADOW_ENABLED=1`.
Therefore neither flag may be counted as a newly enabled optimization. A literal
fallback-message search still returned no retained events, so the cause of the
historical JRA Container execution remains unverified. Source inspection also
confirmed terminal Container cleanup on successful and failed rescore paths;
do not shorten the shared 45-minute day-base protection on the assumption that
normal completed rescores always idle until lease expiry.

### Structured Container logs verified at 22:28 UTC

Container JSON fields are returned under `event.source`, not necessarily under
`$metadata.message`. A bounded historical query returned six
`rescore-cache-attestation` events across JRA, NAR and Ban-ei, all `accepted` with
`exact-entry-and-identity-match`. These events establish those specific cache
attestations, not native-Worker parity, full rescore completion, or memory needs.
The verified dataset was `containers`; using `datasets: []` also works.

For new `container-resource-usage` events, extract `source.mode`, `source.race`,
`source.elapsed_seconds`, `source.memory_current_bytes`,
`source.container_lifetime_memory_peak_bytes`, `source.memory_limit_bytes`, and
`source.succeeded`. Retain timestamp and placement identity; placement reuse alone
is not proof of continuous runtime. The postdeployment query through 22:28 UTC
still returned no resource events, so prediction downsizing remains gated.

### Morning live evidence and newly identified gates (September 13 UTC)

At 00:10, the live Worker tail captured a concrete JRA native fallback:
`R2 object not found: finish-position/jra/jra-cb-v9-sim-2013-clean-jockey-pedigree269/model.json`.
The native attempt took 7.399 seconds; Container fallback then completed in
77.300 seconds, published KV, and was explicitly destroyed around 00:11:43.
The watcher was stopped with TERM to its own Wrangler PID 61547; absence was
verified. Next-day NAR/Ban-ei discovery deferrals were also observed, but those
are not evidence of Container starts.

A `container-resource-usage` event for JRA September 13 race 01 reported success
and 45.797 seconds inside the instrumented workload. The returned structured
record did **not** contain memory counters. Treat those counters as unavailable,
not zero; the existing cgroup-v2-only reader is not sufficient evidence for
resource sizing in this runtime. Next implementation must support appropriate
fallback measurement, label its scope and sampling limitations, preserve
best-effort/non-interference behavior, and add deterministic tests before rollout.

The missing JSON and metadata exist locally under `models/finish-position/jra/`.
Do not simply upload them: doing so would enable the currently failing native
serving path. Source inspection found that native JRA `scoreAndWrite` calls the
shadow scorer then persists directly, while Python applies Prophet adjustment
when coverage permits; the `jockey_pedigree_703` cell policy is enabled with
weight 0.14213735231084337. Model-byte equality alone therefore does not establish
served-output equality. Validate complete routing/branch/signature policy,
trend coverage, adjusted scores/ranks and publication behavior before restoring
native artifacts. The successful Container fallback remains authoritative meanwhile.

### Telemetry compatibility implementation and verification

Local implementation now supports cgroup v2 plus nested/root cgroup v1 memory
controllers, labels the selected controller, rejects negative counters and treats
the v1 unlimited-memory sentinel as unavailable. It never combines counters from
different controllers.

A separate `system_memory_sampler.py` samples `/proc/meminfo` every 250 ms,
including initial/final samples, using a daemon thread with a stop event, lock,
and bounded join. Missing data and thread-start failure remain non-fatal.
`sampled_system_memory_peak_bytes` is explicitly a **sampled guest-system**
measurement (including other guest processes), not an exact Container or job peak.
It can miss short peaks; `MemAvailable` also accounts for reclaimable cache.
Native cgroup peak and system sample fields remain separate.

Verification completed locally at 00:37 UTC: Ruff passed, basedpyright **0 errors /
0 warnings**, **2,308 tests passed / 1 existing skip**, total coverage **97.10%**;
both changed telemetry modules have **100%** coverage. An asynchronous routing test
was corrected to wait for its predictor-call event rather than assume acceptance
implies execution; routing tests isolate optional telemetry from host I/O/threads.
No thresholds, measurement scope, suppressions or skip policy were weakened.
Artifact verification again returned `MATCH`, **144 selected / 293 observed**, no
findings, unchanged manifest root `4a6963f9ad41b7d11f35485fae9a7561d5ef1052ed0cdbe79c659e7508804332`.

The production-equivalent Linux image build and network-disabled sampler smoke
test passed before any production queues were paused. In a local 3 GiB-limited
container, five system samples were collected and the native cgroup limit was
3,221,225,472 bytes. This diagnostics-only smoke test does not establish that
prediction workloads fit in 3 GiB.

At 00:41–00:43 UTC, the normal queue-draining deployment completed with an immediate
Container drain, no pending migrations and no prediction requeue. Worker version
`6108c8a7-b39c-4d37-bf3e-50ce2430cabc` was independently confirmed at **100%**.
API checks at 00:45 confirmed **deliveryPaused=false for both prediction and
weight-rescore queues**. Race-chain had advanced to application version 131 /
image `6108c8a7`, still 2 vCPU / 6144 MiB. Legacy was still transitioning: version
251 / image `ddc0ca7f`, six scheduling instances, no health errors, unchanged
4 vCPU / 12288 MiB. Do not equate the completed Worker deploy with completed
legacy image rollout; verify convergence and actual new-image workload telemetry.

At **00:51 UTC**, API verification confirmed legacy application **version 252 /
image `6108c8a7`**, no health errors or active/starting/scheduling instances. A new
JRA September 13 race 02 rescore event reported **success**, **51.002 seconds**,
**194 system samples**, no sampler-start failure, total guest memory
**12,814,610,432 bytes**, and sampled guest-system peak **562,925,568 bytes
(~537 MiB)**. Native cgroup counters explicitly remained `unavailable`.
This proves the new fallback measurement works in production. It is one sampled
JRA rescore, not a bound for all models/categories, full-build fallback, or day-base.
Retain the 12 GiB legacy profile while designing any smaller rescore-only canary.

### Frozen-input offline resource comparison

`docs/container-rescore-offline-canary-20260913.json` records a successful
read-only comparison for JRA September 13 / venue 06 / race 02: **10 runners,
423 columns**, cache SHA-256
`fcddb701d390c5e32934d22f7e2e226f68b4f8a2d4ea7719756e12ad2f6259cd`.
R2 HEAD identity was unchanged before/after download. No production writes were
performed; both containers ran with networking disabled and read-only inputs.

With the identical baked image, 12 GiB/4 CPU and 3 GiB/1 CPU process-cold and
process-warm calls to production `score_races` produced identical rows and served
signatures, output SHA-256
`316afbedee34a94295f14346a26a1376fb6f4d7a0e025254def829507ba99b6c`.
The larger profile's cgroup lifetime peak was **653,336,576 bytes (~623 MiB)**.
Its process-cold score took ~1.000 s, versus ~0.890 s in the small profile;
process-warm calls were ~0.007–0.008 s. Do not interpret the small timing advantage
or lower second-run cgroup charge as a CPU/memory efficiency guarantee: the
larger run came first, host page cache/accounting can differ, the host has 4 GiB,
and amd64 runs under emulation. This tests scoring only, not full request startup,
remote reads, publication, fallback builds or Cloudflare deadline behavior.
Cached late-binding values were frozen; live weight generation was not replayed.
Card maximum 12 was explicit benchmark context, not independently verified.

The served model for this fixture was `jra-joken-703-pooled-yetirank-v2`, showing
why native parity must verify complete current routing, not simply the presence
of the older native model filenames or a Prophet port alone. Next small-route
work must prevent full-build fallback from running inside a rescore-only profile
and preserve ownership, slot accounting, cleanup and deadline checks.

### Remaining rollout and sizing gate

1. Before production deployment, record current Worker versions and application
   configuration; verify selected model artifacts through the existing deploy
   preflight. Use the repository's queue-draining deployment workflow for the
   prediction Worker, not a direct deploy that bypasses its safety checks. Keep
   existing production instance sizes, concurrency and lease settings.
2. Validate the fingerprint query against production data and observe both an
   unchanged cron (no startup) and a changed date (preview updated). Exercise UI
   access during sync. Probe fallback, timeouts or growing preview lag require
   investigation; disable the source gate if necessary.
3. Collect resource events across JRA/NAR/ban-ei, cold and warm starts, day-base
   builds and rescores, including peak-size fields and error runs. Correlate with
   actual container lifetimes and provisioned memory/disk, not log counts.
4. Only after that evidence exists, choose a dedicated rescore size with explicit
   headroom and test the exact same artifacts/inputs/weight generation against
   the existing instance. Require identical published predictions, acceptable
   startup/score latency, completion before the existing deadline, and no OOM.
   Dedicated smaller rescore routing is now implemented. On September 13,
   Worker `424e7de3-c086-45c9-9578-c14c196b5aa0` enabled only
   `jra:20260913:06:06` on 3 GiB/1 CPU after two frozen-input comparisons,
   Linux guards and full gates passed. The natural rescore published all 16
   fresh-weight predictions in one matching Neon/KV generation and stopped
   normally at 03:08:21 UTC, before 12:55 JST post. Python took 63.253 seconds;
   251 guest-system samples peaked at about 384 MiB (not an exact job peak).
   Gate/allowlist returned to `0`/empty on Worker
   `e6514693-2976-4b51-a3db-d37331af5b98`; independent API checks confirmed
   100% deployment and both queues resumed. Exact live-output equality against
   a large-profile replay was not tested. This is not total-cost savings proof.
   See `container-rescore-only-canary-plan.md` for exact deployment evidence and
   the observed update/reset/retry of the separate Nakayama 5R request.
5. Roll out any smaller route as a limited canary with a tested fallback to the
   existing route, then compare complete daily Billable Usage windows normalized
   for comparable race workloads. Roll back on output differences, deadline
   regressions, OOM or increased repair/retry rates. Do not resize day-base from
   rescore measurements.
