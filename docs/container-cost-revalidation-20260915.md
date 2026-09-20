# Containers cost revalidation — 2026-09-15 UTC

User authorized autonomous revalidation and safe additional reductions. Existing
prediction correctness, publication deadlines, ownership, retry and coverage
protections remain mandatory. Unrelated research changes are out of scope.

## Verified billing checkpoint

Executor `cloudflare-billing-mcp.org.billingRead.execute` successfully queried
Billable Usage at 2026-09-15T16:41:21Z: 712 source records, 114 Containers records,
no service errors. Daily account-wide contracted usage is preserved in
`container-cost-daily-20260915.json`. Latest returned interval ends September 15
00:00 UTC exclusive; these are usage charges, not an invoice total.

| UTC date                   | Memory USD | CPU USD | Disk USD | Total USD |
| -------------------------- | ---------: | ------: | -------: | --------: |
| September 6                |    2.74698 | 0.36300 |  0.14275 |   3.25273 |
| September 7                |    2.88080 | 0.37164 |  0.15175 |   3.40418 |
| September 12 (rollout day) |    3.36228 | 0.64462 |  0.16335 |   4.17025 |
| September 13               |    0.52979 | 0.18100 |  0.02446 |   0.73525 |
| September 14               |    1.04305 | 0.16060 |  0.05458 |   1.25823 |

Actual account-wide daily charges are lower on September 13–14 than the prior
same weekdays. This does **not** establish causal optimization savings: race
counts, successful publication, full/day-base work, unrelated applications and
retry rates have not yet been normalized. In particular, September 13's Ban-ei
publication incident must not be counted as cost-efficient successful work.
The 50% optimization target remains unverified.

The first detailed retrieval (job 134) exceeded the integration response limit;
it is not complete evidence. Job 136 corrected this by returning bounded daily
aggregates and validating numeric costs/currency. Job 137 is retrieving current
application configuration using an explicit non-secret field allowlist.

## Execution plan

1. Verify current deployed resources, routing flags and application-attributed
   usage over the returned post-change complete UTC dates.
2. Check current-day pipeline readiness once; do not repeat historical race
   monitoring or trigger historical scoring. Investigate any current blocker
   separately without credential guessing or unauthorized secret changes.
3. Rank remaining costs by attributable memory-time and avoidable execution.
   Retain legacy 12 GiB/4 CPU/45-minute and chain 6 GiB/2 CPU protections; a
   rescore result is not full-build sizing evidence.
4. Implement the best evidence-supported safe reduction with tests and unchanged
   coverage scope/thresholds. Validate before rollout, preserve admission safety,
   and verify production behavior and rollback capability.
5. Report deployed reductions separately from pending changes and billed effects.

## Additional reduction implementation plan

The live September 16 JST readiness check at 01:43 reported all 48 races with
complete initial Neon/KV predictions, no initial deadline failures, and the
first post still 537 minutes away. This is current readiness, not retrospective
resolution of September 13's incident.

Application-attributed usage is in `container-cost-attribution-20260913-14.json`:
race-chain accounts for 68.09% of September 14 allocated-memory-time. Recent
sampled full-mode NAR scoring events took 24–103 seconds with guest-system peaks
around 426–452 MiB; these do not measure the entire feature-building chain or
justify resizing. Live logs show existing warm-reuse grace and ownership-fenced
stale-stop rejection. Those protections must not be disabled as a shortcut.

A separate, bounded reduction is available in MLflow: `syncChangedDays` currently
launches one Python CLI per changed/reconciliation-due day. The existing Python
`sync_production_preview_range` already iterates each calendar day with identical
per-day run identities and reuses one Neon connection and MLflow client.

Implementation:

- Read existing day checkpoints and identify only due days; keep hourly repair,
  clock-reversal handling, empty/deleted fingerprints and fail-open probe behavior.
- Coalesce only consecutive calendar dates in their existing order. Never include
  an unchanged day or fill a missing date between batches.
- Execute each range serially via the existing range CLI. Checkpoint its individual
  source fingerprints only after the entire CLI succeeds. A partially failed range
  stays uncheckpointed and can replay idempotent MLflow updates; no prediction
  scoring or publication is involved.
- Test continuous dates, gaps, unchanged middle dates, month/year/leap boundaries,
  failed batches, checkpoint ordering and no-start behavior; run complete package
  type/lint/format/test coverage checks without weaker gates.
- Deploy only the MLflow Worker change after verification, retaining the Python
  image and 3 GiB profile if the normal deployment tooling supports that safely.
  Verify live grouped execution, unchanged no-start behavior and normal shutdown.

This reduces three adjacent-day Python invocations to one in the tested path;
total billed savings remain to be measured.

### Verification and Worker-only rollout

- Job 142: all 63 TS tests passed; tsc/lint/format clean. Coverage statements
  99.45%, branches 97.91%, functions 97.29%, lines 99.44%.
- Job 145: unchanged MLflow Python suite, 912 passed in 200.30 seconds, 100%
  coverage. 130 existing runtime/deprecation/test warnings were reported; this
  is not a claim of warning-free Python execution.
- Job 143: Wrangler 4.100.0 dry-run succeeded. Installed CLI documentation
  confirms `--containers-rollout=none` deploys the Worker without rebuilding or
  updating Containers.
- Job 148: a fresh MLflow health check found no active/starting/scheduling
  instances; the guard avoided the immediate ten-minute cron boundary. Worker
  `10fccd69-c8ad-4c4f-8f8e-9a05297f8536` was deployed successfully. No Container
  image/resource change, prediction deployment or prediction requeue occurred.
  This point-in-time guard is not a general linearizable admission protocol.
- Job 150: the normal cron successfully ran one CLI for
  `range=20260916..20260918 dates processed: 3`, creating one finish-position run
  and updating one running-style run. Outcome was OK, no exceptions, approximately
  101.936 seconds. At 17:04:11.943 UTC, activity expiry signalled normal shutdown.
- Job 155 at 17:06:01 UTC independently confirmed MLflow active=0, no health errors,
  unchanged application v4/image `bbf68378`/3 GiB/1 CPU and the new Worker at 100%.
  The completed job-150 watcher was identity-checked and explicitly stopped.
  Grouped execution and subsequent stop are verified; unchanged no-start behavior
  after this rollout and incremental billed savings remain separate checks.

### Race-chain long-runtime investigation

Job 146's hourly usage query for September 14 returned approximately one 6-GiB
instance's allocated memory each hour from 15:00 through 23:59 UTC, while CPU
usage was only about 48–67 seconds per hour. This is evidence of low CPU use with
continued allocation, not proof that all runtime was unnecessary.

Job 147 found one currently running instance, `race-chain-predict-nar-1`, started
September 15 13:42:20 UTC, with status timestamp 13:49:07 UTC. The instance-list
API returned inactive entries despite the requested `state=active` filter;
classify by each returned status rather than trusting the filter.

The bounded historical race-chain log query was empty; a current name-specific
query returned only an earlier 13:34 completion callback. Sample absence cannot
prove the current placement is idle. Do not force-stop it based solely on age,
CPU averages or complete Neon rows. Current ownership/watch/process state must
be established first. Existing two-minute idle and stale-stop ownership checks
remain intact.

### PID 1 signal reproduction and corrective plan

Job 156 ran the existing image locally, network disabled, with Python as PID 1
and no SIGTERM handler. It printed `pid 1 sigterm_disposition 0`; after SIGTERM,
Docker reported `running_after_sigterm=true`. The test-owned probe was removed.
This reproduces the signal behavior; it is not yet a complete causal trace of
the particular production placement.

The production Dockerfile likewise uses Python directly as its entrypoint, and
`serve_http` installs no SIGTERM handler. SDK 0.3.7's default idle handler sends
SIGTERM, not SIGKILL. Add an HTTP-server-scoped SIGTERM handler that exits the
main thread with status 0, closes the server via its existing context manager,
and restores the previous handler on scope exit. Do not alter one-shot mode,
model execution, 45-minute day-base protection, two-minute chain/rescore idle
settings, ownership checks or retry budgets.

Test normal/exceptional handler restoration, SIGTERM exit, server lifecycle wiring
and real Linux PID 1 termination. Run complete Python gates and existing artifact
integrity checks. Any prediction rollout still requires safe admission/drain;
never deploy over an unknown active execution to apply this fix.

Job 157 submitted one existing admin stop request for `race-chain-predict-nar-1`
with `overrideActive=false`. The coordinator's atomic check rejects any existing
lease or active focused watch and fences later claims before stopping. No force
flag or budget reset was submitted. Job 160 independently confirmed this same
instance `inactive`, updated at `2026-09-15T17:11:28Z`, four seconds after the
queued response. Job 165 at 17:21:09 UTC confirmed all three prediction apps and
MLflow had zero active/starting/scheduling instances and no health errors.
This confirms stopping, not a general all-path deployment-drain guarantee.

### Local corrective implementation verified; prediction rollout pending

`predict_lib/process_signals.py::exit_on_sigterm()` now installs and restores
an explicit SIGTERM handler around `serve_http`'s existing server context.
It raises `SystemExit(0)`, avoiding the main-thread `shutdown()` deadlock.
One-shot execution and all resource/ownership/retry policies remain unchanged.

- Job 159's first local attempt failed: it sent TERM before observing handler
  readiness. It is not positive verification evidence.
- Job 162 waited for `handler_ready_pid 1` and verified SIGTERM produced
  `running_after_sigterm=false exit_code=0` in an isolated Linux Container.
- Job 168 used the actual `serve_http` function with the changed source mounted
  read-only into the existing image. After observing its listening message,
  SIGTERM produced `actual_http_server_running=false exit_code=0`. Network was
  disabled, callbacks were inert, no predictions were submitted, and the
  test-owned Container was removed. This verifies HTTP lifecycle behavior,
  not a newly built/deployed image or scoring parity.
- Jobs 166/169: 2,325 tests passed, one existing skip, unchanged whole-library
  coverage 97.11% against the retained 95% minimum. Ruff and basedpyright passed.
- The full format gate initially detected 18 pre-existing formatting differences.
  Job 169 formatted them and compared every such file's AST against HEAD:
  zero semantic changes. The complete 125-file format check then passed.
- Job 167: artifact integrity `MATCH`, zero findings, 144 selected / 293 observed,
  manifest root `4a6963f9ad41b7d11f35485fae9a7561d5ef1052ed0cdbe79c659e7508804332`.
  Unselected-artifact warnings remain; this is not a warning-free artifact scan.

Job 171 built local `finish-position-sigterm:20260916` successfully, manifest
`dd29d0d67628c00732fe0629eafc5b57ad768b747af1d74a8322029fd791f3a0`.
The baked artifact scan also matched (140 observed, 144 selected, four remaining
unselected-manifest warnings). Job 174 tested this built image without source
mounts: `/ping` returned 200/`ok`; SIGTERM then stopped the actual HTTP server
with exit code 0. The isolated test Container was removed.

Job 172 passed prediction Worker type/lint/format/coverage gates: statements
97.73%, branches 95.10%, functions 98.83%, lines 98.30%.

Job 173 compared three offline scoring fixtures with archived baselines. Synthetic
fresh JRA QSM and frozen NAR Transformer-blend rows and served signatures matched
exactly, including cold/warm consistency. Frozen JRA differed: the candidate
reported `stage1=fresh`, whereas the archive reported
`stage1=odds-missing:weather-gate-kept-base`. The test supplied
`STAGE1_PRESERVED_ODDS_GATE_ENABLED=1`, but the old result did not record that
setting. Treat the comparison as failed pending isolation, not as a passed gate.
Job 175 could not start the planned new/old image matrix: the old image tag was
absent. Job 176 confirmed its known immutable image ID was absent too; neither
old-image run executed, and no cause of local image removal was established.

Job 177 instead varied only the preserved-odds gate on the immutable candidate
image, with identical frozen inputs and network disabled. Gate 0 exactly reproduced
the archived JRA rows/signature/hash `9e6e8faa…`; gate 1 exactly reproduced job
173's fresh-route rows/signature/hash `632acc9c…`. Thus the observed discrepancy
is reproduced by the test's gate setting alone; no scoring change was made to
obtain equality. This is archived-output plus configuration-isolation evidence,
not a completed simultaneous old/new image comparison. Production routing flags
must remain unchanged. Results: `~/Library/Caches/horse-sigterm-gate-k7tJfN/`.

No prediction Worker/image rollout, resource reduction, native artifact upload,
queue reset or prediction rewrite has been performed in this restart. Empty
inventory alone still cannot authorize deployment over potentially admitted work.

Job 163 checked the current rollout API schema: both `rolling` and
`new_instances` explicitly say they actively replace instances. Do not interpret
`new_instances` as an idle-only rollout or bypass the admission safety requirement.

Job 165's sampled unchanged-sync log query returned no events; this is inconclusive.
Job 170 subsequently captured normal cron success on the deployed MLflow Worker
and all three `unchanged date=20260916/17/18; Container not started` messages at
17:31 UTC, with no exceptions. It reached its bounded observation timeout and
terminated. This confirms live unchanged-source no-start behavior; no manual sync
or benchmark-triggered prediction was submitted. Incremental billed savings still
require a later usage interval and workload normalization.

### Authorized maintenance rollout (in progress)

The user explicitly authorized autonomous maintenance and production deployment.
Job 184 at 18:02:51 UTC confirmed 48/48 initial predictions complete, first post
10:40 JST. Jobs 178/187 found all three prediction apps idle, both start queues
initially unpaused, and saved ten cron entries plus HTTP/preview ingress settings.

The standard deploy CLI now uses tested ownership-guarded stop requests rather
than `overrideActive:true`, and accepts an explicit maintenance config while
retaining its normal queue handling and no-requeue deployment. The temporary
config keeps HTTP/preview ingress and cron triggers closed; resources, models,
routing flags and bindings are retained. Before deployment it is also pinned to
the already-tested image digest, avoiding a rebuild from files that might change
during the maintenance wait.

Job 188 owns the maintenance/deployment process. Recovery journal and exclusive
lock are under `~/Library/Caches/horse-maintenance-20260916/`; `run.mjs` performs
fresh preflight, closes new public/preview HTTP and cron ingress, pauses only the
two start queues, and aborts/restores if any prediction Container activity or
unknown health appears. Completion/control queues remain enabled, though the
public HTTP callback origin is temporarily closed. It allows an observation
window for cron propagation and bounded Queue/Cron invocations, then invokes
`REQUEUE_FINISH_POSITION_PREDICTIONS=0 bun run deploy` with ingress still closed.
A finally block restores and independently verifies HTTP, preview URLs, all
saved crons and both delivery queues. Deployment itself is limited to 15 minutes;
the enclosing tool is limited to one hour. Do not start a duplicate maintenance
job, alter the saved journal, or clear its lock while it is live.

This is an operator-authorized, quiet maintenance rollout, not implementation or
proof of the general all-path admission-epoch design. A fixed observation window
cannot prove absence of arbitrary unbounded HTTP requests. Job 189 checks entry
into the maintenance phase; deployment and restoration still require reading
job 188's final result and independent live verification.

Job 189 confirmed ingress closure at `2026-09-15T18:16:05.269Z`. Job 191 separately
verified both HTTP/preview flags false, cron list empty and both start queues
paused. An Executor settings-sync warning was present, but the actual Cloudflare
service responses succeeded; no authentication/approval policy was changed.

Job 193 pushed the tested image, preserving exact manifest digest
`sha256:dd29d0d67628c00732fe0629eafc5b57ad768b747af1d74a8322029fd791f3a0`, to
`registry.cloudflare.com/78109ec18c7c85b194b19fb32e3bb149/finish-position-sigterm`.
Job 194 atomically pinned all three roles in the temporary maintenance config to
that digest; Wrangler dry-run succeeded and recognized all three digest references.
The standard no-requeue deployment remains in charge of activation. Retained
profiles: legacy standard-4/max10, chain 6144 MiB/2 CPU/12000 MB disk/max4,
small 3072 MiB/1 CPU/6000 MB disk/max1 with its gate still off.
Docker reported its normal CLI registry login was cached unencrypted; job 195
successfully logged out of only `registry.cloudflare.com`, removing that temporary
cached login without reading or printing credentials.

Exclude this deliberately closed-ingress maintenance interval from claims of
steady-state efficiency. Zero allocation while work admission is paused is not
proof of successful-work cost reduction.

### First rollout result — acceptance is not activation

Job 188's standard deployment reported the image-only configuration change
accepted for all three apps and Worker version
`73b86345-9699-40fa-96d2-750fdaaac674`. Prediction requeue was skipped. Its
post-deployment idle-only check then flagged activity or unknown health at
18:48:08 UTC. The finally block independently verified all saved ingress settings,
ten crons and both start queues restored at 18:48:13 UTC; overall command exit
was 1. Do not rerun deployment blindly or stop work to satisfy an idle-only check.

Job 197 at 18:49:12 UTC confirmed initial prediction readiness remained 48/48,
with no incomplete races. Job 199 at 18:50:27 UTC found all three apps healthy
and idle, but only small-role application v2 showed the new image. Legacy v253
and chain v132 still showed `bae10d4a`. Thus the CLI acceptance does not yet prove
main-role activation; job 201 inspects rollout state before any further change.
Job 196's first inventory read had a local response-shape error (`instances`
is nested in an object); job 199 corrected it, with all service calls successful.
Job 198 is a bounded live prediction tail for post-rollout errors/lifecycle proof.

### Progressive rollout completed

Job 201 resolved the activation uncertainty without another deployment:

| Role                  | Target version | Rollout completed (UTC) |                  Target share |
| --------------------- | -------------: | ----------------------- | ----------------------------: |
| Legacy                |            254 | 18:51:42.699            | 100% (7/7 assigned instances) |
| Race-chain            |            133 | 18:49:21.259            |                    100% (4/4) |
| Small, still disabled |              2 | 18:49:12.906            |                    100% (1/1) |

All three rollout records report `completed` and no health errors. These are
rollout-assigned instance counts, not successful prediction-job counts or proof
that each stopped Container executed its new image. Job 200 independently
confirmed Worker `73b86345-9699-40fa-96d2-750fdaaac674` at 100%, all ingress/crons
restored, both queues unpaused, Prophet/focused-watch/race-chain policies retained,
and the small-role gate 0 with empty allowlist.

The maintenance wrapper's idle-only check ran before progressive rollout finished.
Its exact transient health response was not saved, so do not invent a specific
job failure or claim a new prediction caused its exit code. Future deployment
coordination must await rollout completion (not merely CLI acceptance) before
releasing maintenance gates. No forced stop or repeated deployment was used to
resolve this observation.

Job 202's direct per-application GETs at 18:58:56 UTC then confirmed stable
versions 254/133/2 all reference the exact tested image digest. All three are
idle with no health errors. Legacy remains 12288 MiB/4 CPU/20000 MB disk/max10;
chain 6144 MiB/2 CPU/12000 MB/max4; disabled small 3072 MiB/1 CPU/6000 MB/max1.
MLflow remains v4/image `bbf68378`, 3072 MiB/1 CPU/4000 MB/max1, also healthy/idle.

The SIGTERM fix is now rolled out. Normal production workload/idle-stop validation
and post-maintenance normalized billed savings remain pending; no benchmark
predictions have been triggered. See `container-sigterm-validation-20260916.json`.

Job 198 captured the restored normal 04:00 JST running-style cron on the new
Worker: `start` followed by `queued date=20260916`, outcome OK, no exceptions.
This proves cron/queue handoff, not completion of that downstream work or a
prediction Container lifecycle. The watcher reached its 15-minute bound; its
specific process was independently confirmed gone before replacement.

Job 203 is the only new prediction tail, bounded to four hours with a check-in
around the normal 06:00 JST day-base activity. Its purpose is to observe naturally
admitted work and subsequent idle expiry without creating benchmark predictions.
Do not launch duplicate tails, force-stop active work, or interpret quiet output
as lifecycle proof.

Job 203 exited with code 0 after about 15 minutes and captured no events, before
its configured four-hour hard limit. No exit cause was established; it is not
lifecycle evidence and is no longer running. Job 205 instead waits until 05:55
JST before opening one bounded tail around the 06:00 normal workload. No production
prediction is submitted by that observer.

Job 204's successful usage query for 19:00–19:20 UTC (after maintenance/rollout)
returned an empty group list at 19:27 UTC. This may represent inactivity or
reporting delay; do not turn an empty sample into a zero-dollar savings claim.

At the job 205 check-in (07:02 JST), the tail was still live. It captured the
06:00 day-base cron dispatching NAR and its normal prediction-queue consumer
returning `worker-hit category=nar runYmd=20260916 containerStarted=false`.
The consumer took 10,318 ms wall/116 ms CPU, outcome OK, no exceptions. This is
positive evidence of a real scheduled cache hit avoiding a Container start,
not evidence of a newly started Container handling SIGTERM.

At 07:01–07:02 JST, coordinator `/reserve-focused-full-race-enqueue` requests
returned HTTP 200. Their response bodies are not in the tail, so acceptance or
actual Container startup cannot be inferred from these calls alone. Jobs 206/207
check live instance state and prediction readiness. The existing single tail is
retained for this new activity, with its original 12,000-second hard limit
(approximately 07:47 JST); no duplicate watcher or benchmark work was started.

Jobs 206/207 at 07:04 JST confirmed all prediction instances remain inactive on
versions 254/133/2, without health errors, and initial readiness remains 48/48
(post-weight 0). Their last inactive timestamps still predate this image rollout,
so this is not evidence of new-image startup/shutdown. MLflow v4 was healthy and
inactive, with a newer inactive timestamp of 06:52:19 JST; its sync outcome was
not captured by this prediction tail.

Job 205 subsequently exited 0 before its hard limit, with no exceptions in the
captured events. Job 208 requests post-rollout usage attribution over 19:00–22:00
UTC. Job 209 is the sole planned successor tail, starting at 08:55 JST before the
normal realtime cron window, with a four-hour total runtime bound including its
initial wait. No prediction or retry is submitted to manufacture lifecycle proof.

Job 208 succeeded at 07:07 JST. For 19:00–22:00 UTC it returned only MLflow:
1,610,616,724,930.4753 allocated-memory byte-seconds (~1,500.004 GiB-seconds),
2,000,004,953,308 allocated-disk byte-seconds, and 118.22 CPU-seconds. At its
configured 3 GiB, the memory total corresponds to ~500 aggregate instance-seconds,
not necessarily one continuous run. No prediction application usage group was
returned. This is consistent with the observed cache-hit/no-start behavior and
inactive snapshots, but reporting lag and workload differences still prevent a
zero-cost or 50%-savings conclusion. The maintenance/rollout interval is excluded.

Job 210 schedules a separate read-only readiness checkpoint for 09:15 JST, ahead
of the expected first-race initial-publication deadline at 09:40 JST (10:40 post).
It reports incomplete race keys, treats an empty inventory as an error, and exits
nonzero on incomplete initial predictions so the completion notification can
prompt investigation before the reference deadline. It does not submit predictions,
change retry budgets, or start/stop Containers. This complements, rather than
duplicates, the single 08:55 prediction tail in job 209.

Job 210 completed at 09:15:05 JST: initial readiness 48/48, no incomplete race,
post-weight completeness 0. This precedes the reference 09:40 deadline; it is
not a claim that weights are available or fresh-weight publication has completed.
The first 44 complete events captured by job 209 through 09:15 had outcome OK
and no exceptions. They include an externally initiated next-day NAR admin
prewarm (`runYmd=20260917`) repeatedly deferred with
`discovery-race-count-0-of-48`. The implementation checks discovery before claiming
a generation or Container slot. These deferred messages therefore do not prove
new-image startup and must not be forced through or have their retry budgets reset.
The current-date and next-date observations must not be conflated.

Job 211 schedules a read-only 09:35 checkpoint, including earliest upcoming race
start times, Neon/KV phase completeness, and available-but-unpublished weight
snapshots. The existing prediction tail remains the only tail; no extra production
work was submitted.

Job 211 at 09:35:02 JST confirmed 48/48 initial predictions and no
available-but-unpublished weight snapshots. First race `nar:50:01` starts 10:40
JST: all eight runners were complete in Neon/KV with matching single generation,
65 minutes before post. Its weight state was `waiting-for-weight`, fetch/snapshot
absent, so post-weight 0 is not yet evidence of a failed rescore.

Job 209 captured a real 09:30 cron miss
(`running-style-race-count-47-of-48`) and startup of legacy `predict-nar` for
`runYmd=20260916`. Port readiness took 8.263 seconds and the prewarm endpoint
returned HTTP 200/accepted. The cache object was still missing, so pickup was
scheduled; full-build completion was not established. At 09:34, pickup skipped
after complete prediction coverage and the normal control queue invoked
`/__admin/stop-container`; logs report `destroyed` and terminal `stopped`.
Two DO events were canceled at that same stop time, without exceptions. Do not
present this as an error-free full build or as SIGTERM idle-expiry proof:
this stop path uses `destroy()`, not the idle SIGTERM path. No stop was submitted
by this observer. Job 212 independently checks the exact instance state.

Job 212 confirmed `predict-nar` inactive with an updated timestamp of 09:34:21
JST, matching the control-queue stop. Legacy still reports v254/the tested digest,
and no prediction-app health errors were returned. However, the application-list
response reported chain v132/old image despite direct GET confirmations of v133
in jobs 202 and 206. Job 213 rechecks the individual chain application and rollout
history before deciding whether this is inconsistent list data or an actual change.
Do not infer a rollback, trigger redeployment, or claim current chain activation
from that discrepant list response alone.

Job 213 at 09:37:54 JST resolved the discrepancy: direct chain GET still reports
v133 with the tested digest, no health errors and no active/starting/scheduling
instances. Its latest rollout remains the original completed v133 rollout at
100%; v132 is marked replaced, with no newer rollback record returned. The list
response was inconsistent with the individual application/rollout records. Prefer
individual GET plus rollout history for version verification rather than reacting
to list metadata with another deployment. No production mutation was needed.

### First live post-weight publication on the tested image

Job 209 captured the ordinary `nar:50:01` rescore after its 09:53:30 JST weight
snapshot: feature HIT, nine entry records including excluded horse 8, eight active
runner outputs, 59,533 ms Worker rescore duration, and successful KV publication.
Job 214 independently confirmed at 10:07 JST that all eight Neon/KV rows belong
to one matching generation, generated at 09:54:31.723648 JST after the snapshot,
with `kvAfterWeight=true` and post-weight completeness true. This was ~45m28s
before the 10:40 post. Initial completeness remained 48/48; no weight-ready race
was awaiting publication in that snapshot.

Job 215 confirmed legacy v254/the tested digest and exact instance `predict-nar-0`
inactive since 09:54:51 JST, without app health errors. The normal control queue
stopped it through `destroy()`, not idle expiry; a coincident canceled DO event
had no exception. This verifies live rescore/publication and cleanup, but not the
SIGTERM fallback or a successful full day-base build. No observer submitted the
prediction or stop request.

The next-day (`20260917`) discovery-deferred prewarm reached DLQ at 09:53 JST;
additional admin prewarm calls were also observed. Do not reset/replay those
budgets or confuse that unresolved next-day preparation with current-day success.
Job 216 schedules a read-only 10:30 checkpoint ahead of the first post. The single
existing tail remains bounded by its original ~11:07 JST deadline, to observe
subsequent normal updates; no duplicate tail is needed.

Job 216 at 10:30:02 JST confirmed initial completeness 48/48, post-weight
completeness 2, no initial gaps and no weight-ready race awaiting publication.
The first race remained complete ten minutes before post. `nar:50:02` also had
all eight rows in Neon/KV with matching single generation and `kvAfterWeight=true`:
weight snapshot 10:19:29 JST, prediction generation 10:20:33.249308 JST,
post 11:10 JST. Job 209 captured its successful feature-HIT rescore (60,230 ms),
KV write and normal control-queue stop at ~10:20:44 JST. That second stop also
used `destroy()` and produced a coincident canceled DO event without exceptions;
it does not exercise the idle SIGTERM fallback. No observer-driven prediction,
stop, retry reset or configuration change was used.

Job 209 reached its planned hard limit at ~11:07 JST (exit 124); its exact watcher
PID was subsequently absent. The final log record is a successful next-day
discovery deferral followed by GNU timeout's termination notice, not a new
prediction failure. No replacement tail was started.

Before expiry it captured `nar:50:03`: five entry records, feature HIT, five runner
outputs, 18,551 ms rescore duration, KV publication, and normal control-queue
cleanup of `predict-nar-2` at ~10:52:07 JST. Again, the stop used `destroy()` and
not idle SIGTERM. Job 217 independently checks current-day publication and the
next-day data inventory; job 218 requests usage for 09:00–11:00 JST, which now
contains real production work rather than only an idle observation window.

Job 217 at 11:09 JST confirmed initial completeness 48/48 and three post-weight
complete races (8/8/5 runners), with no weight-ready pending publication and no
initial gaps before post. The third generation was 10:51:55.561034 JST after its
10:51:33 snapshot, for an 11:40 post. In contrast, the September 17 readiness API
returned zero races while its prewarm guard expects 48 Catalog races: this is an
unready/inconsistent inventory observation, not proof tomorrow has no work or that
its preparation is complete. No replay/reset was attempted.

Job 218 returned usage for 09:00–11:00 JST: legacy 4,859.894 GiB-seconds and
127.67 CPU-seconds; MLflow 1,485.018 GiB-seconds and 176.68 CPU-seconds. These
correspond to about 405 aggregate 12-GiB instance-seconds and 495 aggregate 3-GiB
instance-seconds respectively. Do not turn these into single-job durations or
full-day savings: prewarm activity is included and the initial 48 predictions were
created before the window. Raw values/limitations are preserved in
`container-post-rollout-observation-20260916.json`.

Job 219 schedules one read-only checkpoint 20 minutes after the latest current-day
post returned by the API, comparing the original race inventory and rechecking the
next day. Job 220 schedules the existing authenticated `billingRead` query for
September 17 12:00 JST to look for a complete post-rollout UTC-day usage record.
It reuses job 136's exact query, does not change approval policies, and must not
interpret absent/partial billing records as zero cost. These are bounded checks,
not new prediction jobs or continuous tail watchers.

### 18:06 JST checkpoint

Jobs 221/222 independently found initial completeness still 48/48, post-weight
completeness 38, no initial gaps and no weight-ready pending publication. All
four application GETs succeeded without health errors and all instances were
inactive at the snapshot; prediction versions remain 254/133/2 and MLflow v4.
Recent legacy shutdown timestamps include 18:01:43 JST and MLflow 18:04:51 JST.
These snapshots do not establish which shutdown signal was used between checks.

The September 17 readiness inventory was still empty. The discovery barrier
compares Catalog race keys with the shared D1 `realtime_race_sources` date/category
count before acquiring any generation/Container slot. Reviewing the producer
shows NAR discovery additionally depends on official race-list links and race-page
fetches; Catalog schedule presence alone is insufficient to establish discovery.
Job 223 reads a bounded, sanitized projection of recent discovery counters/error
classes, without raw error text or credentials. Job 224 requests hourly allocation
for 09:00–18:00 JST to check for prolonged allocation during the observed workload.
No production changes, retry resets or new tail watchers were introduced; jobs
219/220 remain the planned end-of-day/billing checkpoints.

Job 223's D1 read succeeded with 16 rows read, zero rows written and
`changed_db=false`. Its recent discovery records were successful: current-day
09:05/09:10 records, and prior evening `multi-day-prep` (~20:06) and
`running-style-prewarm` (~21:01) records. No classified 401/403/timeout appeared
in that bounded sample. Successful records omit their target date, so those rows
alone cannot prove September 17 discovery succeeded or failed.

The checked-in producer schedules next-day preparation for 20:05 JST
(`5 11 * * *`, offset +1), later than this 18:06 snapshot. Therefore an empty
next-day inventory now is not by itself an overdue publication failure. Job 225
checks deployed schedules and Worker versions before relying on that cadence.
No manual discovery seeding or queue replay is justified by these observations.

Job 224's returned 09:00–18:00 JST hourly groups total legacy 29,039.826
GiB-seconds / 697.62 CPU-seconds and MLflow 17,865.108 GiB-seconds / 2,267.79
CPU-seconds. These correspond to ~40.33 aggregate instance-minutes at 12 GiB and
~99.25 minutes at 3 GiB, respectively. Only these two applications had returned
groups; absence is not an independently verified zero. The 18:06 readiness count
is later than the 18:00 usage cutoff, so it is not used as a per-race denominator.
See `container-post-rollout-usage-20260916-0900-1800.json`; full-day priced and
workload-normalized comparison remains pending.

Job 225 verified the deployed sync schedules include both 20:05 JST
(`5 11 * * *`) and 21:00 JST (`0 12 * * *`). Finish-position Worker
`73b86345-9699-40fa-96d2-750fdaaac674` and MLflow Worker
`10fccd69-c8ad-4c4f-8f8e-9a05297f8536` remain at 100% traffic.
It also found sync-realtime-data Worker `29a79146-229c-4b28-9486-0cfab58942bc`,
created at 07:29:38 JST September 16. That deployment was not submitted by this
observer and is an additional before/after confounder; do not attribute every
subsequent workload or cost change solely to the Container/MLflow patches.
The scheduled 21:10 check follows the normal discovery/prewarm slots, but must
still distinguish discovered inventory from completed next-day predictions.

### End of September 16 race day

Job 219 at 21:10 JST found all original 48 races retained, no races still upcoming,
and initial/post-weight completeness both 48/48. Across 487 expected runners,
all final generations matched Neon/KV and were generated after the latest weight
snapshot/fetch and before post. The smallest generation lead was 1,995.361 seconds
(~33m15s). This is a generation-time check, not an audit of actual KV write times
for all 48 races; do not silently equate those timestamps.

Next-day discovery has now populated all 48 September 17 races, resolving the
earlier zero-inventory observation after the normal evening slots. Initial
predictions were still 0/48 at 21:10, with first post 10:40 the next morning.
The 22:00/23:00 finish-position crons are running-style preparation retries, not
a guarantee of a separate full-prediction launch. Job 226 checks live instances;
job 227 is one new bounded 40-minute tail for night preparation, with a useful
20-minute check-in. Prior tail 209 is closed. No manual prediction submission,
lease override, retry reset or historical rewrite was performed.

Job 226 at 21:15 JST found legacy `predict-nar` running since 21:02:00.556 JST
on v254/the tested image, with no app health errors. All other prediction instances
and MLflow were inactive. This confirms a live night-preparation allocation, not
successful full-build completion. The observer did not stop or replace it. Job
227's initial records show successful cron/queue/readiness invocations and no
exceptions; its bounded check-in remains appropriate while that work progresses.
An Executor settings-sync warning preceded job 226's JSON, but the outer result
and all individual API calls succeeded; no authentication/policy changes were made.

At job 227's 21:36 check-in, the watcher was still alive. Captured events show an
old candidate rejected for `running-style-race-count-0-of-48`, normal generation
preemption/cleanup at 21:21, a superseded pickup dropped (and its stale stop
skipped), then a fresh prewarm at 21:22. Since 21:25, pickups report
`foundation-landed` and enqueue running-style work while retaining the normal
180-second pickup schedule. All these events still use Worker version
`73b86345-9699-40fa-96d2-750fdaaac674`; no observer deployment or stop caused the
transition. A landed foundation is not complete running-style/initial prediction
publication or proof of final enriched full-build success. Job 228 reads grouped
D1 running-style inference state for September 17, without raw error messages.
The single existing prediction tail keeps its original 40-minute hard bound.

Job 228 succeeded at 21:40:25 JST with no September 17 NAR rows in
`running_style_inference_state` (zero writes, `changed_db=false`). This does not
prove inference is running or failed: the queued kicks have not been linked to
persisted inference state by this query. Job 229 therefore observes the distinct
upstream `sync-realtime-data` Worker for at most 20 minutes, with a 10-minute
check-in, to distinguish dispatch/feature readiness from inference execution.
It does not duplicate the prediction Worker tail or submit work.

Job 227 ended at its planned 21:54 hard limit (exit 124); its Wrangler process
was confirmed absent. It captured another generation replacement around 21:41,
followed by foundation-landed pickups through 21:51, not full prediction completion.
The final timeout diagnostic accounts for one non-JSON block in the log parser.
No replacement prediction tail was started.

Job 229 remains bounded by its original 20-minute limit. By approximately 21:55,
it had captured day-base feature HITs for eight distinct September 17 races
(NAR 48:07–12 and 50:01–02). This establishes upstream feature consumption,
not inference completion or published finish-position predictions. In the same
sample it logged ten WIN5 query-read timeouts and ten WIN5 incomplete-runner
errors, despite event outcomes being `ok`; those nested job failures must not be
ignored or causally attributed to the Container changes without further evidence.
Job 230 repeats the read-only inference-state aggregation after these feature HITs.

Job 230 again found zero inference-state rows at 21:55. Job 231 then confirmed
repeated September 17 planner skips through 21:54:55: `running-style foundation
not ready`, with timeout text present. Separate feature-materialization records
at 21:31/21:41/21:51 were errors mentioning timeout; September 16 materialization
records in the same sample were `ok`. Thus the feature HITs are partial progress,
not evidence that tomorrow's preparation is merely waiting normally. This is an
upstream preparation failure requiring diagnosis, not established SIGTERM failure.
The checked-in planner wraps whole-date materialization with the 24,950 ms
`QUEUE_HANDLER_TIMEOUT_MS` before inference dispatch. Job 232 checks for that
specific timeout label versus database-read timeout, bounded to the latest 2,000
log rows. Job 231 read 311,682 rows despite its small result limit; do not repeat
that broader query unnecessarily. At 21:58 job 229's process was alive, and remains
bounded to its original ~22:06 stop, covering the normal 22:00 retry slot. No
retry budgets, timeouts, ownership fences or production configuration were changed.

Job 232's CLI completed but its nested API result failed: Cloudflare 7500,
`LIKE or GLOB pattern too complex: SQLITE_ERROR`. It supplies no diagnostic rows
and is not a successful verification. Job 233 replaces the two wildcard LIKE
checks with literal `instr` checks, keeping the same read-only, 2,000-row bound.
This is an observer-query correction, not a production pipeline change.

Job 233 succeeded at 22:00:07 JST (212 rows read, zero written). The September 17
planner skips and feature-materialization errors in its bounded sample match
`handler timeout: materialize-running-style-features:`; none match `Query read
timeout`. This identifies the recorded failure boundary, not the underlying slow
operation, and does not rule out database latency beneath the wrapper. Checked-in
code applies a 24,950 ms wrapper to whole-date materialization; that materializer
loads/builds per-race caches sequentially and publishes the aggregate foundation
only afterward. The observed finish-position foundation HITs therefore do not
establish completion of the separate running-style foundation. No timeout increase,
retry reset, scoring change or speculative concurrency patch has been applied.

Job 229 ended at its original 22:05 limit with exit 124. Both observation-tail
processes are now absent; no replacement was launched. Its complete parsed sample
contains 154 events and feature HITs for 11 distinct September 17 races (NAR
48:07–12 and 50:01–05), not a complete 48-race preparation result. One final
non-JSON block is the timeout diagnostic. WIN5 nested failures persisted through
22:02, additionally including `WIN5 schedule not found`; no causal link to the
running-style timeout is established. Job 234 takes a single read-only next-day
publication snapshot after the 22:00 preparation slot. Live idle-SIGTERM and
next-day full-build/publication success remain unverified.

Job 234 at 22:06:42 JST still reports September 17 inventory 48, initial-complete
0 and post-weight-complete 0. First post is 10:40 JST September 17; the initial
T−60 deadline has not yet passed. Post-weight absence overnight is not itself an
incident. Source inspection shows per-race feature caches are written before the
aggregate foundation, so partial cold preparation may be reusable by subsequent
normal attempts; this is not a guarantee of recovery. Job 235 schedules one
read-only publication checkpoint at 22:30 JST to check that hypothesis against
actual outputs rather than applying speculative concurrency/timeout changes.
No live tails, duplicate inference submissions or observer-driven stops remain.

Job 235 at 22:30:03 JST still found 0/48 next-day initial predictions. Natural
publication recovery has therefore not been demonstrated by that checkpoint;
partial cache reuse must not be reported as recovery. Job 236 rechecks both
inference-state counts and the bounded planner/materialization failure labels to
distinguish a continued preparation block from a later inference/publication
stage. It launches no additional tail or prediction work.

Job 236 at 22:30:26 JST establishes actual progress beyond the earlier preparation
block: inference-state now contains all 48 September 17 races, with one completed
(12/12 horses), 46 pending (466 expected horses), and one sync-failed (8/8 written
horses, one error). Attempts began around 22:29. The bounded planner log sample
also contains an `ok` entry at 22:29:35 after timeout skips through 22:26; that
entry's target-date projection is null, so the date-specific inference-state is
the stronger evidence. This is running-style progress, not proof of initial
finish-position publication or full recovery. Job 237 classifies the sync-failed
records without printing raw error text. Normal retries/ownership are untouched.

Job 237 at 22:31:32 found one sync-failed race, `nar:20260917:30:04`, with
9/9 written horses. Its error mentions Neon/Postgres, without matching the
selected timeout/401/403 text markers. These string checks neither identify the
specific database error nor rule out other causes. The prior aggregated failure
had eight horses, so persistence of the same failed race is not established.
Source semantics reserve `completed` until the Neon mirror has all rows; 9/9
written here is not proof of successful Neon synchronization. The normal planner
has a mirror-retry path. Job 238 checks publication, inference counts and remaining
sync failures at 22:45 JST, without manual requeue or another live tail.

Job 238 at 22:45 confirms continued natural running-style progress: 15/48 races
completed (157 horses), 33 pending (329 expected horses), and no sync-failed
records in the subsequent filtered query. All nested API results succeeded with
zero database writes. Initial finish-position publication remains 0/48, so this
is upstream recovery/progress rather than end-to-end completion. Job 239 checks
again at 23:25, allowing time for the remaining 33 races based on observed progress
without assuming the current completion rate will persist. No retries were reset,
no predictions submitted and no additional tails started.

Job 239 at 23:25 found 47/48 running-style races completed (474 horses), one
pending (12 expected horses), and no sync-failed records. The remaining pending
record's attempted timestamp was 23:10:23, whereas the latest completion was
23:23:46; it must not be assumed to be actively executing merely because its
status is pending. Initial finish-position publication remains 0/48. Job 240
identifies the outstanding race/state read-only before deciding the next check.
Neither upstream completion nor end-to-end recovery is yet established.

Job 240 at 23:26:19 JST returned no non-completed September 17 NAR inference
records (API success, 48 rows read, zero written). The previously pending race
could not be identified from this later filtered snapshot; the state changed
between observations. Job 241 will positively recount statuses, inspect initial
publication and check Container versions/health at 23:40. An empty filtered result
alone is not substituted for that positive count or downstream publication proof.
No observer intervention accompanied the apparent upstream progress.

Job 241 positively confirms all 48 running-style races completed, 486/486 horses,
zero recorded errors; latest completion was 23:26:08 JST. At 23:40, however,
initial finish-position publication remains 0/48. The only running instance is
legacy `predict-nar`, with status timestamp 23:26:17, still v254/the tested digest.
All applications report no health errors; chain, small and MLflow are inactive.
These snapshots do not prove what work the running instance is executing or
that downstream generation is complete. Job 242 starts a fresh, bounded 15-minute
prediction-Worker observation (10-minute check-in) specifically to inspect the
post-running-style handoff. Previous tails 227/229 are closed; no duplicate live
tail or prediction submission is involved.

At job 242's 23:51 check-in, the watcher process was alive and the trace showed
actual downstream progress: focused-full Python race-chain success with
`daybase-hit source=r2-race-foundation ... reason=contract-match`, followed by
`prediction kv fp publish ... status=written` for September 17 races. Completion
callbacks and stale-stop rejection were observed. Ordinary control cleanup also
used `admin-stop destroyed`, followed by subsequent race starts; these remain
destroy/control-queue observations, not idle-SIGTERM proof. Foundation-HIT runs
are not cold full-build sizing evidence. Job 243 independently checks publication
counts and live Container state. The existing watcher retains its original
15-minute hard limit (~23:55); no duplicate watcher or manual work was started.

Job 243 at 23:51:36 independently confirms 12/48 next-day initial predictions
complete, with post-weight 0 as expected before delivery of next-day weights.
Legacy, small and MLflow instances were inactive at the subsequent API snapshot.
The chain application's health summary reported active=2/assigned=1 while three
instance records said running; these are not an atomic census and should not be
used as exact runtime billing. Versions remain 254/133/2/4 with no reported health
errors. Partial initial publication is now verified, but 36 races remain incomplete.

Job 242 ended at its 23:55 hard bound (exit 124); its process is absent. The full
trace has 1,440 parsed events, no event exceptions, and publication-written logs
for 16 distinct September 17 races. No log strings matched the bounded
failed/error/timeout/exhausted scan; that is not proof of absence of all failures.
The final timeout diagnostic is one non-JSON block. Sixteen logged publications
are not a fresh whole-inventory readiness count. Job 244 independently checks
readiness and Container health at 00:30 JST September 17, based on the observed
ongoing progress. No live tail remains or was replaced; no manual submissions,
stops or retry resets were used.

Job 244 at 00:30:17 JST September 17 confirms 41/48 initial predictions complete.
Missing generation records remain for NAR 48:09, 30:09, 30:10, 44:10, 48:11,
30:12 and 44:12. The 00:30:43 API snapshot found all four applications inactive,
without health errors, retaining versions 254/133/2/4. Inactivity is not evidence
that remaining work is queued, drained, successful, or idle-SIGTERM stopped.
Job 245 observes the prediction Worker for a bounded ten minutes to inspect
remaining-work dispatch/retry behavior rather than assuming the seven races will
finish simply from elapsed time. Prior observation tasks are closed; no active
Container is stopped, no job is manually requeued, and no retry counter is reset.

Job 245 ended at its 00:41 hard bound (exit 124), with its process confirmed gone.
It captured normal focused-full admissions, completion callbacks and written KV
publication for previously missing races, including 30:10, 48:09, 30:09 and 44:12
through 00:39:01, followed by ordinary control-queue destruction. Its 194 parsed
events contained no event exceptions; one trailing timeout diagnostic was not
JSON. One cleanup reported `stopped_with_code`; the actual exit code/signal is
not present in this summary and must not be inferred as successful idle SIGTERM.
Job 246 now independently checks whole-inventory readiness and Container state;
no live tail remains.

Job 246 at 00:42:17 JST confirms 45/48 initial predictions complete. The remaining
three are NAR 44:10, 48:11 and 30:12, each still lacking a generation timestamp.
All Container applications were inactive and error-free at the later 00:42:33
snapshot, with versions unchanged. The four newly completed races corroborate
actual progress since 00:30, but neither the remaining dispatch state nor full
completion is inferred from inactivity. Job 247 checks readiness/health at 01:00
without a new tail, requeue or stop.
