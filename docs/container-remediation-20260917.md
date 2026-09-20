# Container remediation — September 17

## Acceptance, not assumptions

- September 17 initial publication: job 247 independently verified 48/48 at 01:00:08 JST. Post-weight was 0/48, with earliest post at 10:40 JST; this is not post-weight completion evidence.
- Feature preparation: remove repeated immutable R2 byte-range reads within one materialization invocation, retaining per-race source precedence, ETag checks, identity/coverage checks, the 24,950 ms handler budget and existing retries. Verify equivalent bytes/outputs and fewer reads. Do not call local tests proof of production recovery.
- Neon mirroring: retrieve the actual persisted error classification before changing retry/transaction behavior. Preserve the durable D1 recovery payload and success-only completed state.
- SIGTERM: installed image and local PID1 exit are verified; production control-queue destroy is not SIGTERM evidence. Obtain a natural activity-expiry trace with exit/lifecycle correlation; do not kill active work to manufacture evidence.
- Cost: retain existing profiles and disabled small routing. Billing job 220 is already scheduled for 12:00 JST. Check complete periods, currency, successful work, preparation work and unrelated deployments. Report whether >=50% is actually established, not assumed from inactive snapshots.

## Verification update (01:00–01:03 JST)

- Job 249: formatting, TypeScript and zero-warning lint passed; 60 focused tests passed.
- Job 250: 2,706 tests passed across 104 files. Coverage: statements 97.56%, branches 95.11%, functions 96.91%, lines 98.05%. No thresholds or coverage scope changed.
- Job 251: production small routing remains disabled (`RESCORE_CONTAINER_ENABLED=0`); small application remains version 2 with the previously installed SIGTERM image. Its inactive snapshot is not lifecycle proof.
- No production deployment, prediction submission, signal, new Worker or cron was performed. A local isolated-probe draft was dry-run only and removed: acceptance remains natural activity-expiry evidence, not a manufactured probe.
- Scoped source deployment, persisted Neon error diagnosis, production lifecycle proof and complete-period cost evidence remain outstanding. Billing job 220 remains the existing scheduled check; do not duplicate it.

## Verification update (01:20–01:27 JST)

- Job 254: read-only D1 query over September 16 and 17 found 96/96 `completed`, no remaining error messages; latest completion 23:56:32 JST. This establishes current recovery, not the historical cause of a transient status.
- Job 255: historical mirror/error needle returned no events. Activity-expiry lookup failed in the observability integration's response decoder (two events had missing `$workers.event` / `outcome`); this is not evidence of absent expiry. Retrieve the narrow query through the documented telemetry API if authorized.
- Job 256: Worker content access returned authentication-scheme error 10405. Do not extract credentials or try alternate authentication to retrieve that content.
- Job 259: metadata lookup succeeded; sync Worker still runs version `29a79146-229c-4b28-9486-0cfab58942bc`, deployed September 16 at 07:29 JST. No relevant committed changes exist after that deployment; concurrent uncommitted edits still require deployment coordination.
- Isolated deployment candidate: `/Users/kkk4oru/Library/Caches/horse-sync-range-stage-37TsF4`, HEAD plus only the six byte-range/materialization source and test files. Shared tracked dependencies are clean. Job 260 passed tsc, zero-warning lint and 2,697 tests, with all coverage metrics >=95%. Different test count excludes unrelated concurrent test additions; coverage configuration is unchanged. Job 262 checks formatting and dry-run bundling.
- Billing job 220 is confirmed live on its dedicated tmux socket, still due at 12:00 JST. No duplicate billing timer was created.

## Natural expiry trace recovered

- Job 264 recovered the two events through the authorized telemetry API. One is MLflow (not predictor evidence). The other is `FinishPositionRaceChainContainer`, September 16 02:07:32.327 JST, DO `64e9a2d2e5b32237115ddf6f07b18f27be299cbdeb3436e2b8e65e644187ad61`, Worker version `e6514693-2976-4b51-a3db-d37331af5b98`, message `Activity expired, signalling container to stop`.
- Job 266 found only alarm success around that event; it predates the fixed-image rollout at 18:49–18:52 UTC, so it cannot validate the fix.
- Job 267 narrowed the post-rollout query to the predictor service (ABR level 1, rather than the earlier sampled level 10) and recovered a relevant expiry at September 16 23:57:41.392 JST: DO `ac19e5391b6e0fa6d0bb26e8f39e6566223e8b4c561d98ca1733cdaca4d3ad42`, race-chain entrypoint, corrected Worker `73b86345-9699-40fa-96d2-750fdaaac674`. Job 268 retrieves its surrounding events and discovers documented Container lifecycle endpoints. Clean exit still needs correlation.
- Deployment coordination is waiting for the user's session identity selection: agmsg reports four registered Pi identities and no unambiguous active sender. No identity was guessed or registered.
- Job 265 passed candidate formatting using the repository's actual `.oxfmtrc.json`; job 262 successfully dry-run bundled the candidate. No production deployment yet.

## Lifecycle evidence gap and logging remediation

- Job 269 identifies the post-fix object as `race-chain-predict-nar-2`, with the correct digest, currently inactive. Its state timestamp is 00:55:23 JST—almost an hour after the expiry—so this snapshot does not establish that expiry caused a clean exit.
- Job 268 also captured ordinary `admin-stop destroyed` before the expiry, and later status polls. Alarm `outcome: ok` is not a process exit code.
- The documented Container hooks expose stop reason/code, but the current class does not log `onStop`; the SDK default is a no-op. The documentation does not promise historical exit-code retrieval. Job 270 found no dedicated Container logs/events endpoint in the OpenAPI catalog. Do not invent an endpoint or infer a zero exit.
- Added observational hooks in `container-class.ts` plus five tests: expiry logs the current runtime-running flag and delegates unchanged to the SDK; stop logs the supplied reason/code. No signal, TTL, cleanup, routing, image or prediction behavior is changed. Job 271 passed tsc, lint and 2,094 tests with coverage 97.73% statements / 95.10% branches / 98.83% functions / 98.30% lines. Job 272 passed all 2,095 tests and the same type/lint/coverage gates after adding an absent-runtime test. Job 276 also passed formatting, tsc, zero-warning lint, 2,095 tests and the same coverage after the final declaration-order cleanup. The two verified files and scoped patch are retained at `/Users/kkk4oru/Library/Caches/horse-lifecycle-observability-20260917/` (source evidence only, not a deployment configuration). Existing coverage scope is unchanged (the Container wrapper was already excluded); direct hook tests exercise its behavior. No deployment yet; coordinate it after resolving the messaging identity. Job 273 discovers read-only indexed history tools to see whether this exact session's prior identity can be recovered without guessing; current session ID is `01a096a9-5ee6-706c-a60e-5f8151f1ffee`. Job 275 reports this session is not present in the existing Core index. No import, refresh, direct transcript read or guessed identity was used; user confirmation remains required.

## Autonomous production release resumed (02:17 JST onward)

- User explicitly requested autonomous Cloudflare work. Messaging identity is no longer the release gate: use the existing shared deployment mutex plus live version/configuration fences, without guessing an agmsg identity.
- Live MCP preflight found a newer sync deployment: `5d1e5132-0f71-4e37-b492-f6bc79eafbec`, September 17 01:50 JST (Catalog readiness caller). Do not overwrite it with the earlier HEAD-based candidate.
- Its retained uploaded artifact and receipt are under `/tmp/horse-readiness-deploy/1789577046782-6c3cbb53-8862-44d4-a4ff-4a76feb47bdd/`. The two target modules match the compiled unmodified HEAD modules byte-for-byte. A release artifact replaces only those two with the tested compiled modules and adds the cache module. Reversing those three changes restores the exact newer production artifact; WASM is unchanged. Thus the concurrent Catalog readiness change is retained.
- New artifact, manifest, unchanged config and MCP/Wrangler release runner: `/Users/kkk4oru/Library/Caches/horse-range-cache-release-20260917/`. No-bundle dry-run passed. The runner checks full settings/Cron/Queue/runtime-resource hashes before and after and independently reads serving source hash through MCP; it does not read credentials or run the Queue reconfiguration command.
- Correct documented `GET /workers/scripts/{name}` successfully downloads source through the same authorized MCP connection. The earlier `/content` request's scheme error does not require a new credential or an authentication workaround. FPC source read confirms lifecycle hooks are not yet present.
- The first oversized preflight response could not be parsed; no upload had started. Its owned mutex was released after verifying no upload-started/receipt files existed. Response handling now returns canonical SHA-256 summaries to avoid truncation. The corrected upload is job 230; reconcile its receipts before taking further action and never blindly repeat an uncertain upload.
- Tmux job numbering restarted at 221 after continuation. Prior important logs were copied to `/Users/kkk4oru/Library/Caches/horse-remediation-evidence-20260917/prior-*.log` to retain earlier evidence. Billing job 220 remains separate/live.

## Cache production activation verified

- Uploaded version `3937bc60-df44-439c-b80e-7a6d1e72706b` passed independent MCP code-hash/runtime-resource checks. The first post-upload guard correctly stopped on a settings hash difference; per-field investigation showed only the explicitly supplied version tag/message annotations changed. All 75 bindings and all behavioral settings remained identical. Read-only reconciliation (job 232) validated those exact metadata values; no second upload occurred.
- Important API semantics: `GET /workers/scripts/{name}` and settings reflect the latest uploaded version, even before it receives traffic. Deployment percentages, not the downloaded script alone, establish activation. The runner now records `latestUploadedModuleHash` and separately validates the active deployment.
- Job 234 activated that version at 100%, independently verified the deployment plus downloaded artifact hash, unchanged behavioral settings, five Queue policies/consumer settings, Cron schedules and DO migration tag, and HTTP health 200/ok. Only its owned shared deployment mutex was released. Receipts: `horse-range-cache-release-20260917/activation-verified.json` and `after-activate.json`.
- This establishes safe activation and basic runtime health, not reduced natural preparation latency or 50% savings. No prediction replay, new timer, model promotion, Container start or forced stop was performed.
- FPC remains `73b86345-9699-40fa-96d2-750fdaaac674` without lifecycle logging. MCP job 233 independently confirmed its latest module hash `f4c06baa5172f14ab24c35615b648f30e9064fb25b9519df94dddcd0950f815f` exactly matches the retained maintenance dry-run artifact at `/Users/kkk4oru/Library/Caches/horse-maintenance-20260916/dry-run/index.js`. That provides a scoped base for adding only the two tested hooks, without rebuilding or changing Container images/profiles.

## Lifecycle Worker-only release preparation

- Job 235 built the two-method patch against the exact retained FPC production artifact, verified byte-for-byte reversal, and exercised five cases against the compiled classes (including inherited race-chain/rescore behavior and unchanged 45m/2m TTLs). No Container business operation was invoked.
- Candidate directory: `/Users/kkk4oru/Library/Caches/horse-lifecycle-release-20260917/`; module SHA-256 `5fa6965b25261112aa6d0495c705596a360d434afe57e3877dd2c83c3b56f1c1`. The local-only image URI typo was corrected from the retained `pinned-image.json` before any upload; config/image account equality is now asserted.
- Job 236 passed no-bundle dry-run with `--containers-rollout none` and produced that exact module hash. Installed Wrangler 4.100.0 supports the flag. Official Containers documentation also explicitly says `versions upload` uploads Worker code only, without publishing images or rolling out instances.
- Job 237 performs read-only preflight. The release runner uses separate upload/activate steps with the shared mutex and guards all configured variable values, small gate 0, active version/source hash, full behavioral settings/runtime resources, six relevant Queue policies/consumers, Cron and three Container application policy hashes/images/versions. It never invokes the image build/deploy or Queue configuration commands. Reconcile errors and receipts rather than repeating a mutation.

## Lifecycle production activation verified — September 17 02:58 JST

- Read-only preflight 237 passed. Worker-only upload 238 produced `33284dc7-fb13-42f1-83c9-0967fe2a66c7`; independent MCP verification confirmed the exact candidate code and unchanged runtime resources before traffic activation.
- Job 239 activated that version at 100%. MCP verified the active deployment, exact uploaded module hash, unchanged settings, Cron, six Queue policies/consumer settings, migration tag, and all three Container application policies/images/versions (small 2, race-chain 133, legacy 254). Public Worker health returned HTTP 200 with `ok: true`. The owned deployment mutex was released.
- Receipt: `/Users/kkk4oru/Library/Caches/horse-lifecycle-release-20260917/activation-verified.json`, timestamp `2026-09-16T17:58:39.064Z`. Both scoped fixes are now in production; earlier notes saying identity-blocked or undeployed are historical and superseded.
- Remaining acceptance evidence: naturally occurring idle-expiry/stop correlation showing clean termination, natural preparation performance, and priced cost comparison demonstrating at least 50% savings. Deployment and health success alone do not establish those outcomes. Do not manufacture probes, stops or prediction replays. Retain the existing noon billing job and morning readiness deadlines rather than duplicating work or repeatedly querying empty logs.

## First post-release observation — September 17 04:00 JST

- Read-only job 240 queried FPC events from `2026-09-16T17:58:39Z` through `18:59:00Z`: both the lifecycle-marker query (limit 15) and general activity query (limit 5) returned empty arrays without tool errors.
- Evidence: `horse-remediation-evidence-20260917/lifecycle-0359.json` and `activity-0359.json`. These are empty query results, not proof of no workload, no errors, or clean termination; sampling and log availability limit interpretation. No additional business workload or mutation was triggered.
- Keep the existing hourly observation pacing and noon billing job. Natural clean-exit and cost-reduction acceptance remain open.

## Natural prewarm observed — September 17 06:00 JST

- Job 241's observability wrapper rejected console events lacking `$workers.outcome`; this is a response-schema error, not a reported Worker failure. Its chained lifecycle query did not run.
- After documented API discovery, job 245 obtained valid results through the existing Cloudflare API MCP connection using the read-only telemetry query (`dry: true`), for `21:00–21:15Z`. The local command later timed out (exit 124), but its saved envelope is complete with `ok: true`, inner `success: true` and both query results; no repeat is necessary.
- `horse-remediation-evidence-20260917/prewarm-0616-api.json` records three events on deployed version `33284dc7-fb13-42f1-83c9-0967fe2a66c7`: day-base prewarm started for `20260917`, dispatched category `nar`, and the scheduled invocation completed with `outcome: ok` (1391 ms wall time, 3 ms CPU). This proves natural scheduler/dispatch operation, not completion of downstream prewarm work.
- Lifecycle query returned zero events, with query ABR level 1. This does not prove clean termination; configured log sampling and the legacy 45-minute idle interval still apply. No Container start/stop or business job was manually triggered. Continue the existing scheduled observation and noon billing check.

## Follow-up observation — September 17 07:16 JST

- Job 246 successfully queried the new `21:15–22:15Z` window (06:15–07:15 JST) using the read-only telemetry API. Both lifecycle markers and messages matching `prewarm|day-base|error|failed` returned zero events; both query ABR levels were 1.
- Evidence: `horse-remediation-evidence-20260917/prewarm-0716-api.json`. No clean exit, downstream completion or absence of errors can be concluded from these filtered empty results. The earlier natural cron/dispatch success remains the only new runtime evidence. Existing hourly pacing and the noon billing job remain in place; no additional workload or deployment was performed.

## Follow-up observation — September 17 08:18 JST

- Read-only job 247 successfully queried the new `22:15–23:15Z` window (07:15–08:15 JST). Lifecycle-marker and general FPC event queries both returned zero events at query ABR level 1. Evidence: `horse-remediation-evidence-20260917/activity-0817-api.json`.
- This adds no evidence of downstream completion, clean exit, or absence of errors. No deployment or extra workload was performed. Keep the 09:40 readiness checkpoint, 10:40 first-post deadline and existing 12:00 billing job; do not repeat these empty historical windows.

## Morning readiness checkpoint — September 17 09:40 JST

- Job 248 queried the independent `pipeline-health-monitor` events from 09:00 through 09:39 JST. The successful API response at `00:39:35.604Z` contains zero events (ABR 1); it does **not** establish current publication or post-weight readiness. Evidence: `horse-remediation-evidence-20260917/readiness-0939-api.json`.
- Consequently, the 09:40 readiness checkpoint is **unverified**, not passed. The historical 48/48 initial publication and 0/48 post-weight figures must not be presented as current. Job 249 is reading only monitor incident key metadata as secondary evidence; incident absence also cannot establish prediction completeness.
- The existing local readiness-attestation script includes healing/requeue behavior, so it was deliberately not executed. No credentials were extracted, no new work triggered, and no foreign deployment mutex touched. Continue read-only diagnosis before the 10:40 first post while preserving the existing noon billing job.

## Readiness diagnosis — September 17 09:41 JST

- Job 249 listed all 86 incident-state keys (empty continuation cursor): none were September 17 readiness incidents. Absence of incidents is not proof of readiness.
- Job 250 read `finish-position-monitor-endpoint:queue-health`: still open, critical, stage `endpoint-failure`, opened August 30, last notified September 17 09:00:47 JST, send count 255. This is a longstanding monitoring-endpoint incident, **not** evidence that the prediction Queue itself stopped or that either new fix caused a regression. Evidence: `queue-health-incident-0941.json`.
- Job 251 independently reads today's published prediction KV values (prefix `pred:fp:v1:20260917:`), summarizing race keys, horse counts, generation timestamps and model versions without starting prediction work. This can establish publication presence, but cannot by itself establish alignment with fresh horse-weight input or all current entrants.

## Fresh publication presence verified — September 17 09:43 JST

- Job 251 completed successfully at `2026-09-17T00:43:31.159Z`: all 48 expected race keys have nonempty published prediction arrays, totaling 486 horse rows. The listing has no continuation cursor, and a local assertion confirms the exact race-key set matches the previously verified 48-race schedule.
- All records use `iter12-nar-xgb-hpo-v8-stage1-marketfree-184`; generation timestamps remain September 16 `14:44:47.889Z`–`15:54:39.559Z` (23:44–00:54 JST). Evidence: `horse-remediation-evidence-20260917/published-predictions-0943.json`.
- This is a fresh read proving publication presence, obtained three minutes after the 09:40 checkpoint. It does not retroactively pass that checkpoint or prove current entrant coverage, latest-weight alignment, post-weight rescore completion, or cache performance. No new predictions or mutations were submitted.

## Natural first-race weight arrival — September 17 09:54 JST

- Job 252's read-only primary D1 snapshot at 09:53:10 JST found 48 scheduled races and zero with positive recorded weights. The first post remains 10:40; first-race fetch attempts were current through 09:51:53. D1 reported zero writes.
- Job 253 then found first-race `nar:2026:0917:50:01` fetch logs explicitly marked `pending:weights-unavailable` / `count=0 upstream-not-published` through 09:51:55, followed by `fetch-weights` status `ok` at **09:54:18 JST**. Thus the earlier absence had a recorded upstream-unpublished classification, and natural fetching has now succeeded. This does not yet establish rescore/publication completion.
- Evidence: `weight-readiness-0952.json`, `first-race-weight-fetch-0954.json` in the retained evidence directory. No manual fetch, requeue or prediction was triggered.
- Job 254 opens a **bounded 600-second**, read-only Wrangler tail filtered to `predict-container-lifecycle` to observe natural follow-on work without relying solely on persisted sampled logs. Files `lifecycle-tail-0955.jsonl` / `.stderr` are private (umask 077). Timeout 124 at the observation-window end is expected, not by itself a Worker failure. Do not duplicate this watcher.

## Production clean process exit observed — September 17 09:55 JST

- Job 254's tail ended early with CLI exit 0 (not the 600-second timeout) and no stderr. It captured one complete event; do not claim a full ten-minute observation window.
- At `2026-09-17T00:55:37.301Z`, `FinishPositionPredictContainer`, DO `a0fc4a4a0f8509597762829cc5a698f19eac2aa78f8191c3b3d33e110a44e188` (`predict-nar-0`), running version `33284dc7-fb13-42f1-83c9-0967fe2a66c7`, logged `[predict-container-lifecycle] stopped` with `{exitCode: 0, reason: "exit"}`. Invocation outcome was `ok`, no exceptions, untruncated.
- This independently demonstrates that the observational hook works in production and records a clean process exit. It **does not** establish a natural idle-expiry/SIGTERM sequence: no preceding `idle-expired` event was captured and the reason is `exit`, not evidence of the initiating signal.
- Job 255 reads the first race's published KV predictions and primary D1 weights to compare horse coverage and generation/fetch timestamps after natural weight arrival. No additional business workload was submitted.

## First-race post-arrival publication verified — September 17 09:57 JST

- Job 255's read-only KV/D1 comparison at `00:57:07Z` found eight positive-weight horse records and eight published prediction rows for `nar:2026:0917:50:01` (first post 10:40 JST).
- Weights were fetched at **09:54:13 JST**; every corresponding horse has a prediction generated at **09:55:12.532 JST** (~59.5 seconds later). The model remains `iter12-nar-xgb-hpo-v8-stage1-marketfree-184`. D1 reported zero rows written. Evidence: `horse-remediation-evidence-20260917/first-race-postweight-0956.json`.
- This establishes fresh publication coverage for all eight recorded weighted horses before the first post, following natural work. Timestamp/horse matching alone does not attest to the exact model feature values, all later races, or the byte-range cache's performance effect. No predictions, fetches or stops were manually triggered.

## Pre-first-post verification — September 17 10:34 JST

- Job 256 at `01:34:58.323Z` verified first-race coverage still 8/8 with post-arrival generation and second-race coverage 7/7, all corresponding predictions newer than recorded weights. Race 2 weights: 10:22:03 JST; prediction generation: 10:22:20.891 JST. Both D1 reads wrote zero rows.
- Race 2's published model is `iter40-nar-settransformer-blend-v1`, unlike race 1's Stage-1 `iter12-nar-xgb-hpo-v8-stage1-marketfree-184`. Do not report model names as unchanged across these races. `apps/finish-position-predict-container/DEPLOY.md` documents exactly these as NAR champion and freshness-gated fallback: per-race variation can occur under existing routing without a promotion. This documentation alone does not prove this run's precise routing cause. Job 257 independently checks current Worker policy/active version and Container image versions without modifying anything.
- The new lifecycle-log window after the previously captured clean exit returned zero events (ABR 1). Natural idle-SIGTERM proof remains open. Evidence: `horse-remediation-evidence-20260917/pre-firstpost-1034.json`.

## Model-routing configuration cross-check — September 17 10:36 JST

- Job 257 and local comparison against the activation snapshot confirmed FPC is still version `33284dc7-fb13-42f1-83c9-0967fe2a66c7` at 100%, with the identical behavioral settings hash, small gate `0`, and unchanged images/application versions (small 2, race-chain 133, legacy 254).
- Evidence: `horse-remediation-evidence-20260917/model-routing-config-1036.json`. The two observed model names are documented champion/freshness-fallback alternatives; no Worker settings or Container image rollout explains their difference. This is not a full audit of external model artifacts or the run-specific gate decision. No model promotion or configuration change was performed by this session.

## Noon contracted-usage result — September 17 12:00 JST

- Existing billing job 220 completed once, without restart or duplicate queries. The authorized billingRead integration returned success at `2026-09-17T03:00:14.815Z`: 733 source records / 123 Container records. Full parsed result and assessment are retained in `horse-remediation-evidence-20260917/billing-1200.json`.
- Latest reported UTC period is September 16 → 17: **$0.02214282**, consisting of disk cost, with memory and CPU costs zero and only three rows. Previous period is **$1.55670896**, six rows. Latest-period completeness is not established; treating their apparent difference as achieved savings would be misleading.
- The latest period ends at September 17 09:00 JST, before the observed morning post-weight workload. These are account-wide daily contracted Container charges, not application-attributed or equal-workload costs and not final invoice totals.
- **At least 50% savings remains unverified.** Do not immediately re-query the same partial period. The existing job has finished; later checks must not describe it as still pending or restart it blindly. A useful later billing check requires a newly matured complete period plus a defensible comparable baseline.

## Billing interpretation / next evidence — September 17 12:06 JST

- Billing job 220 has completed and was already evaluated; no billing job remains pending. Official [Containers pricing documentation](https://developers.cloudflare.com/containers/pricing/) confirms monthly included usage and that memory/disk charge by provisioned resources while CPU charges by active usage. It provides no daily billable-usage finality marker or refresh SLA. Therefore zero monetary CPU/memory charges must not be equated with zero resource usage, and incompleteness is a possibility rather than a proven explanation for the latest record shape.
- No repeat billing request was issued. Revisit only after a new reporting period has had time to appear (provisionally the next day's noon checkpoint, not a promised Cloudflare update time); assess completeness and comparable workload before claiming savings.
- Job 258 performs a bounded, read-only lifecycle query for the new window after `01:34:58.323Z`, covering subsequent natural race work. It does not repeat earlier empty windows or trigger a stop. Proof still requires a correlated running idle-expiry/clean-exit sequence; the previously captured standalone `exitCode: 0` remains valid but narrower evidence.

## Additional clean exits — September 17 12:07 JST

- Job 258's local command reported exit 2, but its retained response is complete and parses successfully: outer `ok: true`, no MCP `isError`, inner `success: true`, two events and statistics (ABR 1). The query succeeded; do not classify the CLI status as a production failure or repeat the query.
- `lifecycle-1206-api.json` records two more `stopped` events on deployed FPC version `33284dc7-fb13-42f1-83c9-0967fe2a66c7`: race-chain DO `64e9…` at **10:39:59.494 JST**, and `ac19…` at **10:55:07.753 JST**. Both report `exitCode: 0`, `reason: "exit"`.
- Together with the earlier legacy-container event, this provides three observed clean process exits across legacy/race-chain execution. No `idle-expired` event appears in this result; none of these standalone exits establishes a correlated idle-triggered SIGTERM sequence. No additional workload, stop or mutation was performed.

## Afternoon idle-expiry check — September 17 14:10 JST

- Read-only job 259 successfully searched only `[predict-container-lifecycle] idle-expired` for the new `03:07:53.828Z–05:10:47.870Z` window (12:07–14:10 JST). It returned zero events at query ABR level 1.
- Evidence: `horse-remediation-evidence-20260917/idle-expiry-1410-api.json`. This adds no idle-SIGTERM proof; persisted log sampling still limits negative conclusions. The three previously observed clean process exits remain valid. No extra workload, stop, deployment or billing query was performed.

## Verification resumed — September 18 07:39 JST

- User explicitly requested continuation after the automation pause. Verification resumed read-only; no release or business operation was repeated. Tmux task numbering restarted at 1 in the same namespace, so new task numbers must not be confused with the earlier run.
- `resume-20260918-0739.json` confirms both original releases still serve 100%: sync `3937bc60-df44-439c-b80e-7a6d1e72706b` and FPC `33284dc7-fb13-42f1-83c9-0967fe2a66c7`.
- New idle-expiry queries cover September 17 14:10–21:15 and 21:15–September 18 07:39 JST; both returned zero events at ABR 1. A separate overnight query for the specific running-style finish-position-day-base HIT marker also returned zero events. These are bounded, sampled-log observations, not proof of absent work or a cache performance result.
- A fresh read-only D1 summary for September 17/18 inference state is in progress. The first statement failed due to shell quoting; subsequent queries parameterize the fallback status. An initial join on raw `race_key` is invalid: `running-style-features.ts` documents compact `source:YYYYMMDD:venue:race` versus realtime/viewer `source:YYYY:MMDD:venue:race`. Its all-`missing-state` result must not be treated as an incident. Task 4 instead joins the source/date/venue/race components; all queries are read-only.
- Billing is not re-queried before the planned September 18 noon checkpoint: the next UTC daily period has not even closed at this local check time. Existing billing job 220 remains completed, not restarted.

## Fresh natural inference completion — September 18 07:42 JST

- Corrected component-key D1 query (new task 4) confirms **September 17: 48/48 completed; September 18: 36/36 completed**, zero stored error messages and expected/written horse counts matching for every race. Primary D1 reports zero writes. Evidence: `horse-remediation-evidence-20260917/natural-inference-20260918-components.json`.
- September 18 inference attempts occurred September 17 20:22–20:53 JST, completing by 20:53:34.613, after the cache release but **before** the initially queried 21:00 overnight window. No fresh inference was manually triggered.
- Task 5 therefore checks the previously unqueried 18:00–21:00 JST feature-HIT window to identify the natural feature source. Successful inference alone does not prove byte-range cache use or quantify its latency/cost effect. The initial raw-key join and late log window must not be interpreted as missing work.

## Natural feature-path evidence — September 18 07:43 JST

- Task 5 successfully retrieved seven sampled feature-HIT events from the corrected September 17 18:00–21:00 JST window. All use deployed sync version `3937bc60-df44-439c-b80e-7a6d1e72706b`.
- Two events explicitly report `HIT finish-position day-base` for September 18 venue 50 races 11/12 at **20:22:06.080 / 20:22:07.395 JST**, sharing request ID `eb03bf15625bac0b1c93ae0224eab7a3`. Other retrieved events report `HIT day foundation`.
- This demonstrates that the relevant day-base read path was exercised naturally on the released version. It is not a direct byte-range cache-hit counter or a before/after performance measurement. Task 6 completed: the same invocation's queue event reports `outcome: ok`, wall time **27,290 ms**, CPU time **2,192 ms**, on the released version. This is whole-invocation timing, not per-race latency or isolated cache-read timing. It confirms successful natural execution of this observed invocation, not absence of all timeouts or a quantified improvement over the previous version. No materialization was manually triggered. Evidence: `feature-path-20260918.json` and `cache-path-request-20260918.json`.

## Noon follow-up — September 18 12:00 JST

- New task 8 queried only the new natural idle-expiry window, September 18 07:39:17.188–12:00:44.798 JST. It returned zero events at ABR 1; this does not establish idle SIGTERM or absence of activity. Evidence: `idle-expiry-sep18-noon.json`.
- The read-only billing integration's current schema was rediscovered (task 7). Its billable-usage endpoint defaults to the current billing period; the schema documents usage quantities and contracted cost but does not provide a daily finality marker. No credentials or settings were changed.
- New task 9 performs a single current-period billing GET, retaining daily Container totals and recent service-level consumed/pricing quantities for comparison with the September 17 snapshot. It completed at September 18 12:01:56.924 JST; the historical billing job 220 was not restarted. Evidence: `billing-sep18-noon-api.json` (768 source / 129 Container records, recent 21 records retained without truncation).
- The September 16 UTC daily total was revised from **$0.02214282 / 3 rows** in yesterday's snapshot to **$0.64528532 / 6 rows** today. CPU and memory charges now appear ($0.15126 / $0.47188250), demonstrating that the earlier snapshot was incomplete. Zero/missing monetary entries were not evidence of zero usage.
- Compared with September 15 UTC ($1.55670896), the reported September 16 account-wide Container total is **58.55% lower**. This is an arithmetic observation, **not verification of this release's ≥50% savings**: September 16 includes both pre- and post-release hours, workload is not normalized, and charges are not application-attributed or marked final.
- September 17 UTC—the first whole UTC day after both releases—currently has only **$0.02029573 / 3 rows**, with CPU and memory rows absent. Do not treat missing services as zero or claim a 98% reduction. Given the demonstrated retrospective additions, the next useful provisional billing checkpoint is September 19 noon JST rather than immediate polling; finality remains unestablished.
- Idle-SIGTERM proof is still missing. Both acceptance items remain open; no production mutation or additional workload was performed.

## Afternoon lifecycle observation — September 18 18:00 JST

- New task 10 completed successfully, querying only September 18 12:00:44.799–18:00:46.116 JST. The bounded idle-expiry query returned zero events at ABR 1. Evidence: `horse-remediation-evidence-20260917/idle-expiry-sep18-1800.json`.
- There is no new expiry event to correlate with a same-instance clean exit. This sampled observation does not prove either absence of activity or successful idle SIGTERM; the acceptance item remains unverified.
- No pending query remains. No stop, probe, replay, configuration change or billing re-query was performed. Retain the September 19 noon provisional billing checkpoint; do not repeat this historical lifecycle window.

## Implementation sequence

1. Add a request-scoped, bounded completed-byte cache (2 MiB / 512 entries); never retain request-owned promises globally. Do not cache rejected reads. Isolate returned buffers from consumer mutation.
2. Share that cache only across the finish-position day-base fallback reads of one running-style date materialization. Keep every HEAD/freshness check and ETag-conditioned miss read. Existing single-race callers retain their behavior.
3. Add cache boundary/concurrency/error/ownership tests and materializer threading tests. Run package type/lint/tests/coverage without exclusions or lowered thresholds.
4. Review the resulting diff against concurrent working-tree edits. Deploy only verified, scoped changes, with no prediction requeue or Container image rebuild. Verify live behavior on natural work; do not promote on unit-test evidence alone.
5. Diagnose remaining mirror and lifecycle evidence independently. Mark incomplete evidence honestly and continue scheduled checks.
