# Container restart audit — 2026-10-03

Scope: repository TypeScript Container entrypoints, prediction Queue producers/consumers, day-base pickup, focused-full watches, coordinator ownership/alarms, and completion cleanup. This is a source audit with deterministic regression reproductions, not an account-wide billing audit or a proof of globally optimal runtime.

## Confirmed defects fixed

### 1. Reading the day-base cache could cold-start an empty process

`container-class.ts` treated `/prewarm-day-base-cache` as an ordinary application request, unlike focused-full cache/status reads. The generic path called `startAndWaitForPorts` and `containerFetch`. A delayed pickup after a stop therefore booted a new process whose in-memory cache could not contain the old result. Failed reads could also enter the generic destroy/retry path.

The endpoint now uses the existing read-only runtime-port path:

- Stopped runtime: return 404 without starting a Container.
- Running runtime: forward to the existing process, preserving query parameters and payload.
- Read error: return 503 without destroying active computation.
- Reading a cache does not renew idle activity; genuine focused-full progress retains its existing renewal policy.

This fix is inherited by legacy, race-chain and rescore Container classes. Generation-scoped and fallback day-base pickups use the same endpoint.

### 2. A durable watch delivery failure could destroy accepted computation

`sendFocusedFullWatchMessageDurably` already persisted an outbox entry, active watch and recovery alarm before Queue delivery. A subsequent Queue send or outbox-clear failure nevertheless escaped into the Container request's destructive error handler. Useful computation was lost and its caller received a retryable 502.

After successful durable registration, fast-path send/clear errors now leave delivery to the coordinator alarm. The accepted response remains successful; no restart is requested. The same shared helper covers watch successors and completion callbacks, preventing duplicate producer retries when the outbox already owns delivery.

Failures **before** successful registration still propagate: an unpersisted watch is never acknowledged as supervised. The coordinator's existing alarm reschedules itself, retains failed entries, and redelivers active watches. Existing ownership/terminal-watch fences handle duplicate delivery. This does not promise exactly-once Queue delivery.

## Other paths inspected; protections retained

- `day-base-prewarm.ts`: capacity/generation ownership gates start; a busy owner does not create a parallel pickup chain.
- `day-base-pickup.ts`: old-date and superseded-generation fences, canonical R2 readiness before Container pickup, bounded retry cadence, and owner-scoped cleanup.
- `race-day-workflow.ts`: accepted builds are observed rather than started on every wake; the previously added Workflow stop step runs after durable readiness and before Worker-only fanout.
- `queue-producer.ts` / coordinator: race enqueue reservations and freshness checks prevent blindly replaying already-current predictions. Genuine input changes and explicit forced repair remain supported.
- `queue-consumer.ts`: warm reuse transfers the lease **before** timestamping the delayed stop. A failed delayed-stop enqueue falls back to ownership-fenced cleanup. This protection existed before this audit.
- `container-control.ts` / coordinator: atomic stop fencing protects newer owners and confirms physical termination before clearing leases.
- `jra-van-datalab-cloudflare-demo/src/worker.ts`: public `/health` is answered by the Worker without entering the Container. Authenticated data/bootstrap requests intentionally start it.
- `mlflow-ui-proxy/src/index.ts` and config: source fingerprint gating is enabled; sync runs serialize and preserve a still-running subprocess. UI activity and actual sync work intentionally start the Container.

No sleep timeout was shortened, no active production process was stopped, and no coverage scope/threshold was reduced. The existing `container-class.ts` coverage exclusion was not added or changed; its behavior is exercised directly by regression tests.

## Verification and deployment boundary

Before implementation, the two targeted regressions failed: stopped day-base cache returned 200 after startup instead of a no-start 404; a durably registered watch with Queue failure returned 502 instead of preserving accepted work. The new tests cover live/stopped/repeated/failed cache pickup, Queue failure, outbox-clear failure, failed registration, and Queue acknowledgement boundaries. Coordinator alarm recovery/fencing tests remain part of the full suite.

Verification: 81 test files / 2,276 tests passed; statements 97.74%, branches 95.21%, functions 98.82%, lines 98.30%. Type checking, type-aware lint with warnings denied, formatting of changed TypeScript files, and `git diff --check` passed. Temporary reproduction and verification logs: `/tmp/keiba-container-audit/`.

Production rollout verified on 2026-10-04 JST: Worker version `3702ed48-db0a-4c56-b782-beac0f92f349` contains these changes and the Workflow owner-fenced stop after durable readiness. An isolated patch preserved the existing live code and settings outside the reviewed fixes; the deployed module was downloaded and matched the candidate byte-for-byte. The rollout drained zero live Containers, skipped image rollout and prediction requeue, and resumed both start queues. All four non-DLQ prediction/control queues were independently confirmed unpaused. Reverification passed 2,278 tests with the same coverage percentages, type checking and zero-warning lint. Evidence: `/tmp/keiba-20261004-validation/`.

The 10/4 audit additionally found overseas sales venue A8 incorrectly included in domestic JRA discovery and fresh-runner queries. This blocked the 24-race domestic foundation and caused readiness API failure. Those discovery/query paths now exclude the overseas venue without removing it from the all-race listing. A single owner-fenced JRA recovery Workflow was accepted after the failed owner's lease expired.

### Additional recomputation defect: incomplete running-style export

Later telemetry disproved the earlier interpretation that the Container was merely advancing through one build: the first build completed at 2026-10-03 16:32:01 UTC, and the same physical Container began another in-process build at 16:33:55 UTC. This is observed recomputation, not proof of a Container reboot. The running-style completion barrier still included unsupported overseas A8, preventing the full domestic export. A fresh-process R2 read found only 28 running-style rows, versus 362 in D1; readiness must not be weakened to accept that mismatch.

A single existing, deduplicated planning job was accepted at 17:35:25 UTC. Its native export restored all 362 R2 rows, independently verified by a fresh-process read. No new manual Container generation or forced stop was requested. The original supervising Workflow errored at 16:53:45 UTC. A later automatically created repair generation, `c3319f6d-273c-4578-93d2-49bb4575dd7d`, published its manifest at 17:52:48.097 UTC; do not confuse it with the failed Workflow's owner `89e4f1e7`. No additional generation was manually requested. Container telemetry recorded success at 17:52:39.342 UTC, pickup logged landed at 17:52:58.216 UTC, and the lifecycle stopped event (exit code 0) followed at 17:53:02.347 UTC. The control consumer confirmed `predict-jra` terminal status stopped at 17:53:03.099 UTC. That is approximately 23 seconds from computation success, or 4 seconds from pickup completion, to the stopped event. This verifies the pickup/control cleanup path, **not** successful execution of the errored Workflow's stop step. Existing queued race predictions subsequently received durable-watch acceptance; prediction completion remains under verification. All four non-DLQ finish queues remained unpaused.

The permanent running-style list filter is locally fixed and verified (2,735 tests; statements 97.55%, branches 95.04%, functions 96.93%, lines 98.05%; type checking and zero-warning lint). It is **not deployed**: the target Worker has a newer inactive version, and the API rejects inheritance pinned to the active version. A temporary Worker with dummy markers proved that `keep_bindings: ["secret_text"]` inherits the inactive version's secret rather than preserving the active value. That diagnostic Worker was deleted and absence verified. Do not blindly deploy or substitute local secrets; visible binding metadata equality does not prove secret-value equality.

Evidence: `rs-r2-after-plan.json`, `container-after-rs-plan.log`, `container-progress-after-full-rs.log`, `jra-workflow-after-full-rs.json`, `jra-manifest-after-rs.json`, `jra-publication-stop-evidence.json`, `queues-after-jra-landed.json`, and `secret-preservation-probe.json` under `/tmp/keiba-20261004-validation/`.

The 15:00–18:00 UTC resource-usage query returned 960.79 CPU-seconds, 57,779.83 allocated-memory GiB-seconds and 96,299.72 allocated-disk GB-seconds for the legacy application. These are `containersUsageAdaptiveGroups` values, which [include the micro VM](https://developers.cloudflare.com/analytics/graphql-api/tutorials/querying-container-metrics/), and cover all application instances rather than only the successful JRA generation. Raw `containerUptime` metrics included zero in a period with verified running activity, so they were not used as a wall-runtime estimate. Evidence: `container-usage-recovery-window.json` and `container-usage-recovery-summary.json`. Without a comparable workload baseline or monthly allowance accounting, these numbers do not establish saved time, reduced charges or globally minimal runtime.

Production occurrence frequency and saved Container seconds have not been measured. The first fix eliminates starts attributable solely to late cache reads; the second avoids losing accepted computation on transient post-registration delivery errors. Build time, external I/O, result pickup cadence, cold-start cost and other legitimate recomputation remain outside those savings.
