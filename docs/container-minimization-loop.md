# Container runtime minimization — loop tracker (from 2026-09-23)

### 05:07 JST idle full-hour usage (2026-09-25)

19–20Z metrics and billable usage have no predict or race-chain app rows; only unrelated MLflow usage CPU50.78s. No sampled finish-position-cron logs 19:00–20:04Z; predict/DLQ/control Queue backlogs remain 0, completion count2/bytes0/oldest0 remains approximate. There is no reported continued prediction-container billing in this full post-run idle hour, **not** proof of exact zero seconds or universal minimum runtime. Production NAR 9/25 recovery verification is complete; JRA/Ban-ei, numerical parity and full native migration remain distinct planned phases.

### 04:11 JST regression gates rechecked (2026-09-25)

Current dirty worktree finish-position-cron tsc/lint/format:check/test:coverage passed (79 files/2175 tests, 97.68% statements/95.07% branches/98.79% functions/98.28% lines). Root scripts pytest 335/335, coverage 96.79%, ruff check/ty/basedpyright/format-check/bash -n all pass. Not a full-repo/Python Container/numerical parity certification.

### 04:08 JST second complete Container hour (2026-09-25)

18–19Z both Container datasets have no predict app row (not proof of zero runtime). Race-chain usage CPU149.10s/allocatedMemory7.151e12 byte-s (~1.85 GiB-h), workload metrics CPU135.54s/raw uptime1,785,208 (unit unknown). Separate MLflow usage CPU174.97s/~0.588 GiB-h. Sampled last NAR 50:10 focused-full status 18:20:18Z, terminal-watch complete 18:20:33Z and claim 18:22:48Z; no later sampled activity through 19:03:30Z. NAR 35/35 still complete, predict/DLQ/control Queue metrics 0, completion count2/bytes0/oldest0 remains approximate. App/hour aggregation does not prove per-run minimum duration or leak. Do not roll out a speculative idle timeout change.

### 03:38 JST completion Queue metrics caveat (2026-09-25)

18:34Z completion Queue metrics still count=2/bytes=0/oldest=0, predict/control Queues=0, no sampled 18:27–18:34:30Z logs. [Official Queue metrics](https://developers.cloudflare.com/api/resources/queues/methods/get_metrics/) call these best-effort/approximate, with oldest=0 meaning unknown. Neither two live watches nor a leak is proven. Do not pull/ack/clear the Queue. Assess the full 18:00–19:00Z billable Container usage after aggregation rather than equating this counter with Container starts.

### 03:31 JST direct D1 API currently healthy (2026-09-25)

Cloudflare debug API token directly read D1 DB metadata and `POST /query` SELECT 1, then real manifest (published) and realtime race-source count=35 from two D1 databases; no 7403 on these read/query requests now. Historical intermittent 7403 is not disproved; do not infer token write permission or permanence. Container-control Queue backlog=0; focused-full-completion Queue metric count=2/bytes=0 (may be in flight); do not claim all cleanup is drained until separately checked.

### 03:29 JST single-replay NAR verification complete (2026-09-25)

9/25 NAR reached 35/35 preWeight.complete, Neon complete=35, KV complete=35, KV generation matches Neon=35, missing=0. Last nar:50:10 finished at 18:20:14.715Z with 12/12. Predict Queue and DLQ backlogs both 0 (18:25Z), D1 day-base manifest remains published, owner automatically released, Worker-only version 55cc5a6b remains 100%. No second replay/manual clear. This verifies the one-day NAR production data flow, **not** JRA/Ban-ei, numerical Python parity, per-run minimum Container duration, or direct API token D1 permission (7403 still needs admin D1 grant; Wrangler OAuth SELECT works).

### 03:20 JST final NAR race still queued (2026-09-25)

NAR 9/25 preWeight.complete=34/35; predict Queue backlog=1, DLQ=0. Only nar:50:10 remains, Neon/KV 0 of 12 and preWeight.reason=neon-incomplete. Sampled Queue logs at 17:47/18:03/18:14Z show worker R2 foundation HIT before Container for that race, but no publish; 18:14 Queue event outcome=ok, not proof of completion. Original Queue timestamp 17:44:48Z. Sampling cannot distinguish coordinator/slot busy from another retry reason. No duplicate replay/restart; inspect natural attempt later.

### 03:12 JST Queue naturally draining (2026-09-25)

NAR 9/25 preWeight.complete=31/35; predict Queue backlog=3, DLQ backlog=0. Four incomplete race keys: nar:50:08,42:10,50:10,42:12 (metrics can omit in-flight). Both start Queues resumed. Sampled logs include prediction KV written and two race-chain terminal stops; no sampled errors, but sampling cannot prove absence. Do not replay/clear/restart.

### 03:05 JST full-hour usage and publication (2026-09-25)

NAR 9/25 preWeight.complete=27/35, predict Queue backlog=8, published manifest stable, no owner or running Workflow. Sampled prediction KV `status=written,busted=true` for several 9/25 races and per-race Container/stop logs confirm some outputs. Complete 17:00–18:00Z predict app usage CPU266.15s / allocatedMemory1.533e13 byte-s (~3.97 GiB-h, ~1,190s aggregate equivalent at 12GiB), while workload metrics CPU396.32s / raw uptime=0. Race-chain usage CPU273.02s / allocatedMemory1.134e13 byte-s (~2.93 GiB-h, ~1,760s aggregate equivalent at 6GiB). Dataset mismatch and app/hour aggregation prevent per-run or minimum-runtime claims; usage is unequivocally nonzero. Remaining 8 and reference parity need checking.

### 02:50 JST Queue draining (2026-09-25)

NAR 9/25 preWeight.complete rose 3→7 of 35; predict Queue backlog fell 33→28, naturally progressing. No duplicate replay. Check the complete 17:00–18:00Z Container usage bucket after aggregation and prediction completion later; these counts do not establish parity or per-run runtime.

### 02:48 JST first completion sample (2026-09-25)

Authenticated read-only prediction-readiness GET returned 35 NAR races, 3 preWeight.complete at 17:46:31Z (nar:47:01/03/05). Not full publication/parity. Added fixed `bun run prod prediction-readiness <YYYYMMDD>` helper for bounded repeat checks; no new deploy/replay.

### 02:46 JST NAR 9/25 day-base published (2026-09-25)

Same replay Workflow **complete**: prewarm landed, readiness=true, owner release=true, fanout-predictions=35. D1 manifest published at 17:42:56.926Z under generation `9b7559a3...`, key `feat-daybase/catalog-v1/nar/20260925/generations/9b7559a3-3246-44c3-b928-e55f938f5080/features.parquet` / ETag `e7d4f40cee397e3149b9afb6e0c630c8`. Owner row is gone without manual clear. Sampled Container `stopped` at 17:43:12Z, slot-stopped at 17:43:13Z. Predict Queue backlog=33 of 35 fanout messages; publication/parity not yet proven. Wait for drain and full 17:00–18:00Z Container usage (nonzero expected, minimum not proven).

### 02:41 JST read-only follow-up (2026-09-25)

Same Workflow running in cycle3 with owner renewed 17:39:20.667Z, prewarm owned, readiness still missing; D1 manifest absent. Sampled DO logs 17:39:47–48Z show cache route port-ready in 30ms, HTTP 200 in 82ms. The earlier 500 is no longer reproduced in that request, but no response body or final manifest yet; do not infer zero Container start or successful publication.

### 02:36 JST read-only follow-up (2026-09-25)

Same NAR Workflow running in cycle2 (heartbeat=true, prewarm=owned, readiness miss), owner renewed 17:34:13.406Z, manifest absent. Sampled Queue log at 17:33:38Z scheduled attempt4 after 180s; no cause for previous 500 seen. Current Worker bindings: 54 same names/types/non-secret values as 7e6 baseline. Do not replay/deploy again while the build is active.

### 02:31 JST pickup attempt 1 (2026-09-25)

Same NAR 9/25 Workflow running in cycle1 with heartbeat=true/prewarm=owned/readiness miss. Owner renewed to 17:29:10.761Z; manifest still absent. Sampled 17:27:19Z log shows scoped `prewarm-day-base-cache?generationId=9b7559a3...` returning HTTP 500 from `predict-nar`, followed by retry attempt2 scheduled after 180s. Root cause of 500 is not known; generation forwarding works but publication is not demonstrated. No duplicate replay or manual clear.

### 02:26 JST single NAR replay (2026-09-25)

After preflight, sent exactly one authorized `/api/admin/race-day` POST for NAR 20260925; response started `day-nar-20260925-fanout-9b7559a3-3246-44c3-b928-e55f938f5080`, no categories deferred. D1 owner automatically took over the stale generation at 17:23:57Z, so destructive manual clear was unnecessary. Workflow running with heartbeat=true, first pickup-scheduled and readiness miss, then sleeping 5m; manifest still has no row. Queue metrics count=0/bytes=129 are point-in-time and not publication proof. Wait for natural pickup and manifest outcome; do not duplicate replay.

### 02:24 JST Worker-only rollout (2026-09-25)

Isolated candidate tests/coverage/format pass (branches 95.09%) and artifact manifest MATCH (144 selected models). Deployed with `PREDICTION_WORKER_ONLY=1`, Wrangler `--containers-rollout=none` from the isolated candidate after both start Queues paused and Containers drained; migration no-op. New Worker `55cc5a6b-75db-4b05-b87e-7e7d5d73060b` at 100%; both Queues resumed (`delivery_paused=false`), Queue backlog=0, Workflow running=0. Manifest still absent and owner old as expected before a fresh generation. No manual clear/replay; no publication/parity/runtime-minimum claim yet. Rollback version `7e6b224f-b723-4aad-888b-7d6d307af3b8`.

### 02:15 JST read-only and isolated candidate (2026-09-25)

16:00–17:00Z Container usage API returned no rows (not zero-runtime proof). 9/25 NAR manifest=0, source=35, owner timestamp unchanged, active Workflow=0, Queue backlog=0. Wrangler OAuth is scoped `d1(write)` and fixed Wrangler D1 reads work; separate HTTP token still gets 7403. Built an isolated Worker-only candidate at `/tmp/finish-position-pickup-rollout-20260925.0LUrkE` from the prior release tree; only the pickup source/test differ from that tree. Isolated tsc/lint/coverage/format pass (branches 95.09%). No upload/deploy/replay/clear before validating production version/bindings and safe rollback.

### 01:07 JST local root-cause fix (2026-09-25)

A fixed read-only D1 query returned **no** 9/25 NAR day-base manifest row. At 16:00Z Workflow running=0, Queue backlog=0, owner timestamp unchanged; both 15:00–16:00Z Container usage datasets had no rows (not proof of zero runtime). Found `day-base-pickup.ts` incorrectly checked a legacy canonical R2 object for generation-scoped pickups, and dropped `generationId` when calling DO pickup. Local fix skips that legacy shortcut for scoped work and passes generation into pickup; added regression test. Cron tsc/lint/test/coverage/format pass (branches 95.07%); scripts pytest 331 passed and test file formatted. Not deployed; publishing/parity/minimum nonzero runtime are still unverified. D1 direct HTTP 7403 and isolated rollout remain blockers.

### 00:11 JST read-only follow-up (2026-09-25)

Workflow running=0, 9/25 NAR source count=35, predict Queue backlog=0 at 15:10Z. Owner row still has old `requested_at=12:56:29.947Z` (row presence is not proof of active lease). Both Container usage APIs returned no rows for 14:00–15:00Z; this is not proof of runtime=0. Sampled logs showed no 9/25 readiness/publication success.

### 23:07 JST read-only follow-up (2026-09-24)

The NAR 9/25 fanout errored at 13:01:35Z; active Workflow=0 at 14:05, source count=35, and D1 owner row still shows old request 12:56:29. Queue pickup logs at 13:16 and 13:36 said `foundation-landed` then same-attempt `foundation-not-ready`, with attempts 7/12 rescheduled; queue backlog was 0 at 14:08Z, but no publication evidence. Container app usage for 13:00–14:00Z is nonzero: predict metrics CPU170.21s / raw uptime 9,507,938 / usage CPU243.66s. Metrics differ and aggregate by hour/app; no per-Workflow or minimum-runtime claim. A `mark-container-slot-stopped` call at 13:40 does not establish zero usage.

### 22:02 JST read-only follow-up (2026-09-24)

The NAR 9/25 fanout naturally errored at 13:01:35Z after cycle 7's final wait; reason remains `day-base-missing-or-invalid`, active race-day Workflows=0. D1 source count=35. Owner row still exists with `requested_at=12:56:29.947Z`; do not clear manually. Sampled logs show prewarm-day-base-cache reached Container (HTTP 200, DO wall time 5.341s) but do not show artifact/readiness/publication. For 12:00–13:00Z, predict app had CPU 429.18s (metrics; uptime=0) and 498.49s (usage API); those hourly app aggregates disagree and cannot be assigned to one Workflow. Nonzero activity, minimum runtime unproven.

### 21:57 JST read-only follow-up (2026-09-24)

The same NAR 9/25 fanout remains running. Cycle 7: heartbeat true, prewarm `owned`, readiness missing; owner naturally renewed at 12:56:29.947Z. Nine readiness misses including initial check. Current sleep ends 13:01:35.047Z, then the Workflow reaches its configured final attempt boundary. No artifact/output/parity, no manual repair/replay/deploy.

### 21:53 JST read-only follow-up (2026-09-24)

The same NAR 9/25 fanout remains running (one active instance). Cycle 6: heartbeat true, prewarm `owned`, readiness missing; owner naturally renewed at 12:51:25.266Z. Eight readiness misses including the initial check; next sleep ends 12:56:28.479Z. No day-base artifact or prediction output/parity. No manual intervention, replay, or deploy.

### 21:48 JST read-only follow-up (2026-09-24)

The same NAR 9/25 fanout remains running. Cycle 5 is another heartbeat=true / prewarm `owned` / readiness missing; owner `requested_at` naturally advanced to 12:46:21.660Z. Seven readiness misses including the initial check; next sleep ends 12:51:23.812Z. 12:41–12:48Z sampled finish-position-cron logs show `/run` work for 9/24 NAR, not 9/25, so no next-day prediction evidence. No day-base artifact or publication; no manual repair/replay/deploy.

### 21:43 JST read-only follow-up (2026-09-24)

Existing NAR 9/25 fanout remains running. Cycle 4 again has heartbeat true, prewarm `owned`, readiness `day-base-missing-or-invalid`; next 5-minute wait ends 12:46:20Z. Six readiness checks including the initial one have missed. Owner is still present; no evidence of day-base artifact or prediction output/parity. Do not intervene; inspect this same Workflow after its natural sleep.

### 21:38 JST read-only follow-up (2026-09-24)

The same 9/25 fanout Workflow is still running. Cycle 3: heartbeat true, prewarm `owned`, readiness `day-base-missing-or-invalid`; owner renewal at 12:36:14Z. Five readiness checks including the initial check have missed. It is sleeping until 12:41:16Z. 12:31–12:39Z sampled finish-position-cron logs show a successful manifest fetch/queue event but no response body or artifact publication. No output/parity evidence. No manual intervention; inspect after the existing sleep expires.

### 21:32 JST read-only follow-up (2026-09-24)

After the 12:31Z natural retry, source count remains 35. Owner `b4c1f6b2-70ff-4d47-af53-d8c9271486d2` renewed at 12:31:10Z. Its fanout remains running; cycle 2 heartbeat is true, prewarm returned `owned`, but readiness still reports `day-base-missing-or-invalid`; the Workflow sleeps until 12:36:13Z. Three readiness checks have not found a valid artifact. No prediction output/publication evidence; no manual write/replay/deploy. Check once after the next scheduled retry.

### 21:30 JST read-only follow-up (2026-09-24)

`race-source-count nar 20260925` is 35. The stale owner was replaced naturally: generation `b4c1f6b2-70ff-4d47-af53-d8c9271486d2`, `requested_at=12:26:07Z`. Its fanout Workflow `day-nar-20260925-fanout-b4c1f6b2-70ff-4d47-af53-d8c9271486d2` started at 12:20:55Z and is still running. Instance detail: heartbeat true; cycle 0 readiness miss → `pickup-scheduled` → second readiness miss → 5m sleep; cycle 1 heartbeat true → `prewarm-day-base=owned` → readiness miss → sleeping until 12:31:09Z. Logs show Container DO `prewarm-day-base-cache` fetch duration 148.425s at 12:29:59Z and an alarm at 180.021s. This confirms prewarm activity but not day-base readiness, output, parity, or per-instance runtime. Do not intervene; next check after 12:31Z for a natural next cycle and artifact/publication evidence.

### 21:16 JST read-only follow-up (2026-09-24)

The narrow read-only query `bun run prod race-source-count nar 20260925` returned 0 around 11:12Z, then 35 on retry at 12:16Z. Expected NAR discovery is now present, but the 10%-sampled sync-realtime-data logs did not capture the `5 11 * * *` cron, so do not attribute exact cause/time. `day-base-owner nar 20260925` still shows generation `2c89f976-2938-43ce-acfa-1b68d3f7f58b`, requested 03:36:10Z (stale >45m); do not clear manually. Running RaceDayWorkflow=0; complete instances list has no 9/25 run (only an unrelated 9/23 run). A day-base-manifest internal fetch returned HTTP 200 at 12:10:57Z, but response body is absent from logs; no proof of day-base publication, fanout, or prediction/parity. The complete 11:00–12:00Z predict app usage group is small but nonzero: metrics uptime 0 / CPU 0.0156s / allocatedMemory 1.031e12 byte-s; usage CPU 0.62s / allocatedMemory 1.289e11 byte-s. These app/hour aggregates disagree and cannot establish exact instance runtime or minimum. Do not manually repair/replay; next useful evidence is whether the normal coordinator sees discovery=35, clears/replaces the stale owner through its normal lease path, and publishes output.

### 20:10 JST read-only follow-up (2026-09-24)

After the scheduled 11:05 UTC multi-day-prep time, `bun run prod race-source-count nar 20260925` first failed with Cloudflare API 7403, then the repository's narrow read-only D1 helper succeeded and returned 0. `day-base-owner nar 20260925` still returns generation `2c89f976-2938-43ce-acfa-1b68d3f7f58b`, requested 03:36:10Z, older than its 45-minute lease; do not clear manually. `finish-position-race-day running` is 0. The 10:55–11:10 UTC sync-realtime-data query returned 21 sampled events but did not contain the 5 11 cron event; configured head sampling is 10%, so absence is inconclusive. finish-position-cron logs show an NAR `admin-race-day` request at 11:00:51Z and an internal day-base-manifest fetch returning 200 at 11:06:02Z, after the cron time. The response body and discovery count are not in Observability logs, so this does not prove prediction fanout/output. The 10:00–11:00 UTC predict app aggregate is nonzero (metrics CPU 6.39s / uptime 0 / allocatedMemory 1.289e11 byte-s; usage CPU 2.28s / allocatedMemory 3.865e11 byte-s); differing aggregates do not establish exact Container runtime or minimum duration. No write, deploy, replay, or manual owner cleanup was done. Next: verify NAR discovery and owner transition/output read-only, using the narrow helper if API 7403 recurs; preserve fail-closed behavior and do not trigger a rebuild.

### 17:06 JST read-only follow-up (2026-09-24)

`bun run prod race-source-count nar 20260925` succeeds through the repository's narrow read-only D1 helper and returns 0; `day-base-owner nar 20260925` returns the existing generation `2c89f976-2938-43ce-acfa-1b68d3f7f58b`, requested 03:36:10Z. This reservation is older than its 45-minute lease; no manual clear was performed. `finish-position-race-day running` returns 0, and the 06:30–08:06 UTC sampled finish-position-cron logs return 0 events (absence is not proof). The direct D1 API 7403 restriction remains distinct from the successful narrow read-only query path. Predict Container app `a0348266` has nonzero usage in both complete hourly groups: 06:00Z metrics uptime 254.272s / CPU 82.40s and usage CPU 84.52s; 07:00Z metrics uptime 51.468s / CPU 50.24s and usage CPU 73.26s. Hourly app aggregates and metrics/usage differences cannot attribute exact runtime to the stale-owner Workflow or prove minimum runtime. Wait for the scheduled 11:05 UTC multi-day discovery, then recheck source count, owner generation, Workflow/output evidence and mature usage read-only. Do not manually clear/replay/deploy.

Goal: run JRA / NAR / Ban-ei running-style and finish-position predictions on
Cloudflare Workflows + Workers and keep production Container runtime minimal,
with production verification for every change. Parity rules from
`container-native-parity-review.md` still apply: a native path may publish only
after full published-value parity against the Python reference on frozen
inputs, with fail-open to Python.

### 11:15 JST follow-up

Read-only: the 9/25 NAR fanout Workflow `day-nar-20260925-fanout-68f66510-bdf7-416e-877d-0df9d38031ce` ended errored at 00:41:24Z after attempts 0–7 all returned `busy` (all owner heartbeats including initial remained `true`); the final error was `day-base not ready category=nar reason=day-base-missing-or-invalid`. A second naturally started 9/25 fanout `day-nar-20260925-fanout-8eb5e145-c1bf-407b-8ea7-e2793060dfef` also errored at 02:11:34Z after 8 `busy` prewarms and readiness misses; heartbeats stayed `true`. A matching 01:51:17Z cron log explicitly reports `discovery deferred ... reason=discovery-race-count-0-of-35`, showing that at least one retry was blocked because the Catalog had 0 of 35 expected NAR races discovered. Other `busy` attempts may have other causes. There are now no running/waiting RaceDayWorkflow instances and 8 retained errors. Code can return `busy` for discovery-not-ready, generation preemption, or Container-slot contention; only discovery-not-ready is directly evidenced for one attempt. A naturally started NAR 9/24 `prewarm-only` Workflow `day-nar-20260924-prewarm-only-11cb96fa-10a7-483f-ac05-9f560057d4f2` errored `day-base repair owner lost` at 00:35:59Z: prewarm had returned `pickup-scheduled`, then after the 5-minute wait heartbeat was `false`. Historical 00:30–00:37 UTC Worker logs show the pickup Queue message was skipped after complete pre-weight coverage and the Container DO alarm logged `admin-stop destroyed` at 00:34:13Z. The queue handler explicitly clears that generation's reservation and stops its matching day-base work on this coverage-complete path, so this owner-loss appears consistent with intentional cleanup, not evidence of a competing generation; exact D1 mutation still unavailable (7403). This run confirms pickup/start followed by stop, but not a landed artifact or exact Container duration. Prediction and weight-rescore Queue backlogs are both 0. Logs 00:30–00:37 UTC: finish-position-cron 6 events, including the coverage skip and stop; sync-realtime-data 8 routine events. No prediction/output or inference completion evidence. The full 00:00–00:42:30 finish-position-cron log query contained 16 events and no prediction/inference completion. Mature 23:00–00:00 UTC Container usage returned only MLflow app `a03d0f99…` (metrics CPU46.70s/uptime426s; usage CPU47.82s); predict app absent in both tables, which does not prove zero runtime. The mature 00:00–01:00 UTC bucket now includes predict app `a0348266…`: metrics CPU72.19s, `containerUptime=410026` (unit/aggregation unreliable), allocated memory 2.7703e12 byte-s; usage CPU140.35s and allocated memory 3.5434e12 byte-s (0.916 GiB-h, equivalent to ~275s at configured 12 GiB). This establishes nonzero app-level resource use and is temporally consistent with the 00:34:13Z admin-stop, but hourly app aggregates do not attribute exact seconds to that Workflow; the metrics-vs-usage discrepancy remains. Logs 00:00–00:42:30 UTC have no prediction/inference completion. The next mature 01:00–02:00 UTC bucket also has predict app activity: metrics CPU6.83s, `containerUptime=64344` (not a trusted per-run duration), allocatedMemory 1.289e11 byte-s; usage CPU34.17s, allocatedMemory 1.0951e12 byte-s (0.284 GiB-h, ~85s at configured 12GiB). This is nonzero app-level activity, but the per-hour dataset cannot attribute it to a specific Workflow. During this interval the 9/25 fanout ran 01:30:57–02:11:34Z and logs show one discovery-not-ready attempt; no prediction/inference completed. Local owner-protection fix passes gates but is undeployed; no replay, repair, or production write.

### 12:15 JST follow-up

Read-only (2026-09-24 02:00–03:00 UTC): Container usage still shows nonzero predict app `a0348266` (metrics CPU 53.27s, raw uptime 200032, allocated memory 1.675e12 byte-s; usage CPU 14.55s, allocated memory 7.087e11 byte-s ≈0.183 GiB-h ≈55s at 12 GiB). Do not treat either aggregate/raw uptime as a per-instance duration; the two datasets materially differ. Finish-position Worker logs independently show a NAR rescore (`20260924`, venue 47, race 04, 8 races, `durationMs=16249`), result publication `status=written`, and Container DO alarm at 02:58:18Z followed by explicit `predict-nar-0` stop at 02:58:35Z. This is concrete nonzero Container-backed rescore activity and output, not proof of full reference parity; rescore remains an intentionally retained Container path pending native parity. A new NAR 9/25 fanout Workflow `day-nar-20260925-fanout-2c89f976-2938-43ce-acfa-1b68d3f7f58b` was running at 03:10Z. Its first three cycles each showed readiness `day-base-missing-or-invalid`, prewarm `busy`, owner heartbeat `true`, then a durable 5-minute wait; latest observed wait ends 03:15:52Z. The instance itself does not prove why prewarm returned busy; a 01:51Z cron independently showed `discovery-race-count-0-of-35`. Workflow was not complete at the check. No D1 API access/write attempted. Local owner-protection fix remains undeployed; do not replay or manually start processing.

### 12:55 JST deployment follow-up (2026-09-24)

The 9/25 NAR fanout Workflow `day-nar-20260925-fanout-2c89f976-2938-43ce-acfa-1b68d3f7f58b` ended `errored` at 03:41:16Z with `day-base not ready ... day-base-missing-or-invalid`; its bounded prewarm/readiness retries did not produce a ready day-base. Running race-day Workflows are now 0. This does not establish the exact cause of each `busy` response; do not replay or force a prediction.

The isolated release candidate passed the finish-position TS/Python gates and artifact-integrity check. `bun run deploy` completed successfully: D1 migration check reported `No migrations to apply`, Worker version `7e6b224f-b723-4aad-888b-7d6d307af3b8` is at 100% (confirmed by Wrangler deployment listing), prediction/rescore Queue delivery was paused for drain and resumed in `finally`, and both Queue backlogs are 0. The deployment explicitly skipped prediction requeue. The earlier direct D1 API 7403 still blocks the row-level audit; it did not block this Wrangler deploy.

This was a Worker-only image rollout. Wrangler reported `no changes` for all three Container applications; their deployed image tags remain predict/race-chain `0e13f756` and rescore `0398dbbb` (the prior compatibility release). A point-in-time instance query at 03:49Z showed every listed finish-position instance `inactive`. A partial 03:00–03:46Z usage bucket nevertheless contains nonzero predict-app aggregates (metrics CPU 265.02s, raw `containerUptime=137680`; billing usage CPU 85.66s); it straddles the deploy and cannot establish postdeploy per-run duration or zero/minimum runtime. The 03:44–03:54Z Worker log query returned 0 events, which is not proof of no failures.

Next: get authorized read-only evidence for the 9/25 owner/discovery state, verify the next natural day-base publication and actual prediction outputs, and only then enable strict manifest mode. Complete published-value parity for each native scorer before routing traffic; M2–M7 remain open. Keep measuring complete hourly/daily Container usage and retain the Container fallback until runtime and parity are demonstrated. Do not interpret an inactive snapshot or absent analytics row as zero runtime.

## Baseline (predict Container `a0348266`, standard-4 / 12 GiB)

| JST day            | Type           | predict GiB-h | race-chain GiB-h | CPU util |
| ------------------ | -------------- | ------------- | ---------------- | -------- |
| 9/16, 9/17         | NAR-only       | 28–36         | 9–11             | ~6%      |
| 9/18–9/22          | JRA/NAR/Ban-ei | 220–562       | 5–108            | ~6%      |
| 9/23 (after 06:42) | NAR-only       | ~2–4 per hour | 0 after 06h      | ~3%      |

Hourly profile on 9/20: ~2 predict instances alive 09–24h at ~6% CPU, i.e.
the cost was held-idle memory, not compute. The 9/22–9/23 fixes (fenced
day-base rebuilds, Workflow-committed day-base) cut the daytime rate by ~85%.

## Running-style Workflow route (9/24 02:00 JST, deployed compatibility mode)

`sync-realtime-data` no longer invokes the direct `/api/admin/prewarm-day-base` Container path when a running-style feature foundation is missing. Its existing planner now invokes authenticated `/api/admin/race-day` on `finish-position-cron`, which reuses the D1-fenced `RaceDayWorkflow` (readiness → single-owner Container prewarm only on miss → durable wait/recheck → fanout). Existing independent `feat-running-style-base` and attested immutable `feat-daybase` HITs still skip that call; the per-race LightGBM inference itself remains Worker/Queue-native. The shared Workflow is also used by finish-position scheduled prewarm, so no duplicate Container role was added. A clean-HEAD isolated sync release excluded unrelated experiments; an isolated test initially caught a real dependency skew (`hyparquet` 1.26 locally vs pinned 1.30.1): the newer decoder rejects absent optional `bamei` columns. `running-style-finish-feature-hit.ts` now filters requested optional columns against Parquet metadata and reuses that metadata for reads; missing required race/feature columns fail closed. Both root and pinned-dependency isolated suites pass 2,720 tests, statements 97.55%, branches **95.02%**, functions 96.93%, lines 98.05%, plus tsc/lint/format. Worker-only dry-run passed; at 02:00 JST version `1ff894fe-8a8d-4bf7-bc8a-d4f1243c52d0` deployed **100%**, prior version `58128983` rollback target. Predict/running-style Queue backlogs 0, race-day Workflows running/waiting 0, and sampled logs returned without API errors (7 events, not prediction evidence). The 9/24 06:00 natural prewarm/real prediction path has **not** happened; do not assert runtime savings or complete parity. Per-race inference still uses Queue rather than a dedicated running-style Workflow; introducing one would require a separately tested D1 generation/ownership contract, not a one-step wrapper around inference.

### 03:04 JST follow-up (before 06:00 natural prewarm)

Read-only: predict and running-style Queue backlogs 0; race-day Workflow queued/running/waiting 0. The 9/24 and 9/25 day-base manifest and repair-owner D1 queries return **no rows**. NAR 9/24 running-style and finish-position D1 states remain 47/47 `completed`, with their last timestamps still 2026-09-23 11:26 and 11:25 UTC respectively (old generation). Last-15m Observability sample: finish-position-cron 0 events, sync-realtime-data 7 routine GET events, no API error; not proof of no events. `container-usage` 9/23 15:00–18:00 UTC returned only the MLflow application at the 17:00 hour (125s workload `containerUptime`, unreliable metric), no prediction/race-chain app groups; delayed/grouped telemetry **does not establish zero Container runtime or savings**. No replay or proactive Container start.

### 03:36 JST follow-up

Queues remain empty; race-day Workflow running/waiting 0. Manifest and repair-owner reads still have no rows for 9/24. Inference D1 still reports only NAR 47 `completed` rows in each table, timestamps unchanged at 2026-09-23 11:26/11:25 UTC. Thus there is no 9/24 JST generation evidence yet. A read-only query against `race_running_styles` using a guessed `source` column failed (`no such column`), and a guessed repair-request `status` projection failed; neither mutated production. Use actual schema before any further row-level queries. Observability has 7 routine finish-position calls and zero sampled cron events; `container-usage` through 19:00 UTC still has no prediction/race-chain application group (telemetry gap, not zero-runtime proof). Per-race heatmap persistence is not independently verified by this D1 state; the RaceDayWorkflow's `warm-heatmaps` step/log or authoritative cache artifact must be checked after natural fanout.

### 03:53 JST follow-up

No state change: predict/running-style Queues 0; RaceDayWorkflow running/waiting 0; 9/24 manifest/repair request empty. The inference state still shows just 47 NAR races for each model, latest completions 11:26/11:25 UTC on 9/23. `race_running_styles` date query (correct schema has category encoded in `race_key`, not a `source` column) returned no 9/24 rows. Finish-position-cron log sample 0 events; sync-realtime-data sample contains routine GETs and its 2-minute cron, no evidence of 9/24 prediction dispatch. One analytics command accidentally used a future UTC end (`20:00Z`, while it was 18:53Z) and was rejected before querying; no production action occurred. Earlier valid Container usage snapshots remain incomplete/delayed and cannot establish runtime. Natural run at 06:00 JST is still pending.

### 04:09 JST follow-up

No change in D1 completion: still only 47 NAR `completed` per inference table, latest timestamps 2026-09-23 11:26/11:25 UTC; 9/24 `race_running_styles` query, manifest, and repair request empty. Predict and running-style Queues backlog 0; RaceDayWorkflow running/waiting 0. Last-15m finish-position-cron sample 0 events; sync sample 5 mixed requests, still for 9/23 NAR and no 9/24 dispatch. Valid `container-usage` through 19:00 UTC contains only MLflow 17:00/18:00 groups, no prediction or race-chain group (insufficient evidence to infer zero). Natural schedule remains ahead; no replay or manually started work.

### 04:25 JST follow-up

No change: predict/running-style Queues 0; RaceDayWorkflow running/waiting 0; inference-state D1 still NAR 47+47 completed with 9/23 UTC timestamps; `race_running_styles` has 0 rows matching `:2026:0924:`; day-base manifest and repair request both return no rows. Finish-position-cron Observability sample 0 events; sync sample 2 HorseWeightDO reads, no 9/24 prediction dispatch. Valid usage through 19:00 UTC still reports only MLflow 17:00/18:00 groups, not an authoritative zero for other apps. A first manifest probe used nonexistent `state` instead of schema's `status`; it failed read-only and was rerun correctly, both queries empty. No replay/container trigger was issued.

### 04:41 JST follow-up

Same state: prediction/running-style Queue backlog 0, RaceDayWorkflow running/waiting 0, D1 still 47 NAR rows `completed` each at previous-day UTC timestamps, no `race_running_styles` for the date, and 9/24 manifest/repair request empty. Sampled finish-position-cron events 0; sync-realtime-data 12 events are previous-day NAR/horse-weight traffic, no target-day dispatch. Valid Cloudflare Container usage through 19:00 UTC remains MLflow-only (17:00 and 18:00 hourly groups); missing prediction/race-chain groups are an analytics gap, not proof of no runtime. No manual recovery was triggered; next check 04:56 JST, shorten to 10 minutes after 05:30.

### 04:57 JST follow-up

No target-day output changes: prediction/running-style Queues 0; RaceDayWorkflow running/waiting 0; inference D1 still NAR 47 completed each with 9/23 UTC timestamps. `race_running_styles` has 0 matching rows; manifest and repair request empty. Last-15m Cron Observability 0 events; sync has 7 routine requests for 9/23 only. Usage query now includes UTC 19:00 but still only the MLflow app across 17:00–19:00 hours; prediction/race-chain are absent from results, which remains insufficient to assert zero execution. No recovery action taken.

### 05:08 JST correction and race-set baseline

The earlier `race_running_styles` checks used the wrong key pattern (`%:2026:0924:%`); compact keys are `${source}:${YYYYMMDD}:${venue}:${race}`. Therefore the prior 03:36–04:57 statements that the style table had zero 9/24 rows were false; read-only queries with the correct key show 47 distinct NAR race keys, latest style artifact timestamp `2026-09-23T11:10:31.758Z`, and running-style inference state `completed=47`, latest `2026-09-23T11:26:23.804Z`. Finish-position inference state also has `completed=47`, all 47 with non-null `predictions_r2_key`, latest `2026-09-23T11:25:49.721Z`. A `realtime_race_sources` left join by exact compact race key shows all currently registered target races accounted for: 9/24 NAR venues 30=12, 42=12, 47=11, 50=12; 47 total, style completed 47/47 and finish artifact pointers 47/47. These are real existing output/state evidence, but timestamps precede the 02:00 JST rollout and do not validate postdeploy behavior. The 9/24 day-base manifest/repair request remain empty. Heatmap Workflow `pc-keiba-viewer-heatmap-warm` has no queued/running/errored 9/24 instance as of 05:08; last 50 completed are dated 9/23 and total retained count is 316. The 06:00 JST scheduled viewer sweep is the next heatmap verification point. Do not repeat the old incorrect date-key query.

### 05:22 JST check

Correct-key exact-join status unchanged: 47/47 target races have style-completed rows and finish-position state with R2 pointers; latest output/status timestamps remain 9/23 UTC and predate the rollout. The day-base manifest/repair tables remain empty (compat mode). Both Queues are empty and race-day Workflow has no running/waiting instance. Viewer heatmap Workflow has queued=0, running=0, errored=0; its completed listing has 316 retained instances but the returned first page is all dated 9/23, no 9/24 venue run. Viewer/cron recent logs contain no 9/24 warm execution; the next scheduled JST 06:00 run is not due yet. Existing 9/24 style/prediction outputs are present, but heatmap and post-rollout steady-state remain unconfirmed.

### 05:33 JST check

Correct exact race join still confirms 47/47 running-style completed and 47/47 finish-position artifacts, unchanged pre-rollout timestamps; categories/venues remain NAR 30=12, 42=12, 47=11, 50=12. Predict/running-style Queues 0; finish-position RaceDayWorkflow running/waiting 0; day-base manifest/repair tables empty. Heatmap Workflow queued/running/errored all 0; complete listing's 316 retained instances still 9/23-only. Sampled viewer and finish-position-cron logs show no date-9/24 workflow dispatch. Next scheduled heatmap sweep is in ~27 minutes (06:00 JST / 21:00 UTC); no manual invocation.

### 05:44 JST check

The exact D1 join remains 47/47 style completed + 47/47 finish artifact pointers; latest dates remain unchanged pre-rollout values. Both prediction Queues are at 0 and finish-position race-day running/waiting instances are 0. Manifest/repair rows remain empty. Heatmap workflow queued/running/errored are 0; completed instance listing remains 316, with newest entries 9/23 only. Logs sampled: viewer has 6 regular 9/23 race-page requests; finish-position-cron has 0 events. The 06:00 JST (21:00 UTC) scheduled sweep has not run yet. No manual workflow invocation.

### 05:53 JST check

No change before the natural run: Queue backlogs 0; heatmap queued/running/errored 0; completed list 316, still only 9/23. Exact race-set join remains 47/47 style and 47/47 finish artifacts. Viewer log sample includes routine prior-day race traffic only; finish-position-cron sample 0. The 06:00 JST heatmap cron has not fired yet; next check is shortly after its scheduled time.

### 06:01 JST scheduled-sweep check

The natural `0 21 * * *` UTC run did fire: all four 9/24 NAR venues started heatmap workflows at ~21:00:45 UTC, IDs `heatmap-20260924-{30,42,47,50}-16mt0` (4 running, 0 queued/errored at first poll). Read-only step details: venue 47 finished successfully with 11/11 `hit`; venues 30,42,50 were still progressing serially at race 10/10/11 respectively, preceding completed race steps all `hit` and success=true. Need final status and all 47 race results after completion. No manual trigger was issued.

The same cron started finish-position day-base Workflow `day-nar-20260924-prewarm-only-36fad21a-1146-4e59-bcae-d0cbaddf2eba`. Its readiness step says `day-base-missing-or-invalid`; prewarm returned `pickup-scheduled`; confirmation remains false and Workflow is in scheduled 5-minute wait. This is not day-base success. finish-position queue metrics: backlog_count=0, backlog_bytes=129 (snapshot); queue should be rechecked after wake. Direct D1 read-only check failed with API 7403 `account not valid or not authorized to access this service`, so no refreshed exact D1 race join at this timestamp; last successful exact join was 47/47 before 06:00. Viewer logs show only routine 9/23 traffic in sampled events (absence is not proof). Record D1 auth limitation and use permitted production path/credentials only; do not alter credentials or trigger processing.

### 06:07 JST post-cron verification

All four heatmap Workflows are now `complete` with `success=true`, no errors, and output status `hit` for every race: venue 30=12/12, 42=12/12, 47=11/11, 50=12/12, total 47/47. IDs are `heatmap-20260924-{30,42,47,50}-16mt0`; start ~21:00:45Z, final completion latest 21:02:15Z. Queued/running/errored counts are all zero. This verifies today's heatmap cache warming for all registered races.

The finish-position day-base prewarm Workflow did **not** succeed: instance `day-nar-20260924-prewarm-only-36fad21a-1146-4e59-bcae-d0cbaddf2eba` ended errored at 21:05:58Z with `day-base repair owner lost`. Readiness was `day-base-missing-or-invalid`, pickup step said `pickup-scheduled`, but after its 5-minute wait the heartbeat returned false. Both finish-related Queue metrics now show backlog_count=0, backlog_bytes=0; finish-position RaceDay running/waiting are 0. No manifest/repair record can be checked because D1 remote query returned API 7403 authorization error at 06:01. Keep the day-base / finish path unresolved; do not infer successful output from heatmap success.

Style and finish prediction D1 evidence remains last successfully confirmed 47/47 each at 05:53, before this day's deploy-triggered workflow; no post-06:00 D1 verification is available due access error. Next verify once more after ≥15 min: read-only exact D1 join if access recovers, heatmap remains complete, inspect finish-position errored instances/logs and day-base repair state. Do not manually rerun or repair absent explicit authorization.

### 06:08 JST observability follow-up

`finish-position-race-day errored` lists the current 9/24 prewarm failure plus four existing 9/23 race-day errors; no new running/waiting instances. Observability logs show the 9/24 `/prewarm-day-base` request was delivered successfully (HTTP 200, ~6.8s to port-ready), then `FinishPositionPredictContainer` had a 180,012ms alarm execution at ~21:00:49–21:03:49Z and was stopped/destroyed around 21:04:09Z. This is real nonzero Container runtime (~3 minutes for that scheduled prewarm); it was not zero-runtime. A post-cron viewer Queue log shows successful cache warm batches for 9/24, requests returning HTTP 200. One sampled slow D1 query warning was 3.6s; not an error. Queue metrics at 06:07 are both zero bytes/count, no race-day Workflow running/waiting. Do not infer prediction generation from prewarm HTTP 200 or cache warm success.

### 06:24 JST stability + usage check

At a second read-only check 17 minutes after the first, the same four heatmap IDs remain complete and successful with all 47 output statuses `hit` (30=12, 42=12, 47=11, 50=12); queued/running/errored=0. This establishes stable heatmap output across the required ≥15-minute interval. Both prediction Queues remain count/bytes 0; race-day running/waiting remain 0. The current day-base errored instance is still present; error count 5 includes this 9/24 prewarm and four older 9/23 failures. Logs in this sampling window show no finish-position/sync-realtime-data events indicating new inference completion; absence is not proof. The direct exact D1 check remains unavailable (previous API 7403), thus finish/style artifacts have only the predeploy 47/47 check, not a postdeploy second confirmation.

Read-only `container-usage 2026-09-23T21:00:00Z 2026-09-23T21:15:00Z` returned one hourly group (`2026-09-23T21:00:00Z`, app `a0348266-3050-47d4-9bad-b04086c1a02b`): metrics `containerUptime=0`, `cpuTimeSec=21.69`, `allocatedMemory=1.9328e12`, usage dataset `cpuTimeSec=185.79`, `allocatedMemory=3.6078e12`; bytes / allocated fields are sums and platform overhead differs, datasets are delayed/sampled and hourly-granularity (not a precise per-Workflow charge). Despite metrics containerUptime=0, DO logs show its scheduled alarm executed 180,012ms and the Container was stopped/destroyed; treat measured execution as nonzero, prefer event timeline for per-run duration. The alerting/reporting discrepancy requires a later full-hour/day aggregation or billing verification, not converting a zero uptime row to zero runtime.

### 06:40 JST follow-up

Heatmap queued/running/errored remain 0; all four 9/24 complete records remain present from the prior detailed read (47/47 hit). The day-base prewarm is still the single new 9/24 errored finish-position Workflow; race-day running/waiting=0. Both Queue metrics remain count/bytes=0. Recent sampled logs: finish-position-cron 0 events, sync-realtime-data 13 events with no inference-completion message, viewer 8 events. D1 exact inference-state recheck still not available due API 7403; don't claim fresh predictions.

Expanded read-only Container query (UTC 21:00–21:40) returned two hourly application rows, both still hourly (`2026-09-23T21:00Z`) and reported `containerUptime=0`, despite nonzero CPU/allocation metrics. Predict app `a0348266-3050-47d4-9bad-b04086c1a02b`: metrics CPU 21.69s, allocated memory 1.9328e12; usage CPU 185.79s, allocated memory 3.6078e12. Other app `a03d0f99-9d0d-46da-a9a8-1e04d235dda8`: metrics CPU 84.37s / allocated memory 7.0864e11; usage CPU 22.14s / allocated memory 8.3753e11. Treat these as hourly sampled/aggregate raw fields, not exact run durations; there is still a discrepancy against explicit 180s Container alarm wall time. Wait for full-hour/billing aggregation before concluding. Heatmap stability has second confirmation; prediction + style remain unverified postdeploy.

### 07:06 JST full-hour usage follow-up

Workflow states unchanged: heatmap queued/running/errored=0; day-base failure remains the sole new 9/24 finish-position error, with no finish-position running/waiting instance. Both Queue metrics remain 0 count/bytes. Recent logs: finish-position-cron 0 events, sync-realtime-data 7 (no inference completion), viewer 3. No postdeploy D1 state refresh; the exact data join remains unverified after deploy.

Full-hour `container-usage 2026-09-23T21:00:00Z 2026-09-23T22:00:00Z` now reports two application IDs. `a03d0f99-9d0d-46da-a9a8-1e04d235dda8`: `containerUptime=98s`, metrics CPU 84.37s, usage CPU 22.14s. `a0348266-3050-47d4-9bad-b04086c1a02b`: `containerUptime=0`, metrics CPU 21.69s, usage CPU 185.79s. Allocation sums are unchanged from 06:40. The full hour updated one app uptime from 0 to 98s, evidence of aggregation delay; the a034 uptime still conflicts with usage CPU and the explicit 180,012ms DO Container alarm. Keep individual fields distinct; do not declare zero runtime. Need next day's billed allocation/usage reconciliation if this remains a success criterion.

### 07:22 JST follow-up

No changes since 07:06: heatmap queued/running/errored=0; finish-position day-base prewarm remains errored; race-day running/waiting=0; both Queues count/bytes=0. No new inference completion in sampled recent logs (finish-position 0, sync-realtime-data 7); viewer 17 events. The full-hour 21:00–22:00 usage values are unchanged from 07:06: a03d metrics uptime98s; a034 uptime0 with usage CPU185.79s; explicit prewarm container alarm duration180,012ms still conflicts with the a034 uptime field. The full-hour result has not reconciled the discrepancy. Do not lower certainty or treat runtime as zero. D1 authorization 7403 remains the blocker to checking postdeploy finish/style output state. Need user/admin to restore an authorized read-only D1 query path before claiming that third output class verified.

## Fresh read-only measurement (9/24 01:38 JST; **before** the 01:08 rollout's first natural prewarm)

Cloudflare `bun run prod container-usage` hourly application groups for JST 9/23 (UTC 9/22 15:00–9/23 15:00): prediction app `a0348266` **84.78 billable GiB-h**, race-chain `a0371eee` **18.32**, MLflow `a03d0f99` **11.39**. Prediction app consumed **46.70 GiB-h during JST 00:00–04:59** (five 8.13–10.62 GiB-h hour buckets), despite no imminent first race. In a bounded 00:00–05:00 JST finish-position-cron Observability sample (686 events, not the full event population), repeated `focused-full day-base deferred before claim` (142), old day-base queue payload skips (37), and pickup/prewarm calls to prediction DO appeared; this suggests repair/retry pressure, but application-hour aggregates cannot assign the 46.70 GiB-h solely to these events. Source-readiness vs running-style completeness is a potentially necessary bootstrap dependency; do **not** prevent an initial build just from 0-of-N running-style rows without proving running-style can bootstrap independently. A separate 291-event `sync-realtime-data` sample for the same window included repeated Neon `timeout exceeded when trying to connect` errors for earlier 9/22 NAR rows; `running-style-queue.ts` marks D1 state `sync-failed` but the day barrier explicitly accepts `completed` **or** `sync-failed` when row counts are complete. The sampled Neon errors alone do not prove they held up parquet readiness; check the D1 state counts/source data before attributing Container waste. For the preceding mixed JRA/NAR/Ban-ei JST 9/22, application totals were prediction 236.10, race-chain 97.34, MLflow 14.30 GiB-h; workloads differ and the decrease is **not** a causal rollout comparison. UTC 9/23 15:00–16:36 returned no usage groups yet. A read-only REALTIME_DB snapshot at 2026-09-24 ~01:41 JST already showed 20260924 NAR running-style D1 `completed=47/47 races`, `written=expected=464 horses` (previous 20260923 NAR 59/59, 619/619); this is inference-state/row-count evidence, **not** manifest publication or final prediction parity. Record the first post-rollout like-for-like live day and delayed billing before claiming savings.

## Where Container time goes now (9/23)

1. Day-base DuckDB build: once per category/day via `RaceDayWorkflow`
   (`sync-day-base`), Container not detached.
2. Initial per-race predictions: race-chain Container, ~05–06h JST.
3. Post-weight rescores: NAR and Ban-ei always use the predict Container;
   JRA rescores also reached the Container on 9/20 (no `Rescore Worker` log in
   7 days) despite `JRA_WORKER_RESCORE_ENABLED=1`.
4. Night 00–04h JST: ~10 GiB-h/h predict with ~2% CPU on a NAR-only night.

Running-style inference is already Worker-native (LightGBM trees in
`sync-realtime-data`); its only Container dependency is the day-base parquet.

## Milestones (cheapest and safest first)

| #   | Item                                                        | Status                |
| --- | ----------------------------------------------------------- | --------------------- |
| M1  | Day-base rebuild loop on partial RS (night/day idle holder) | deployed 9/23, verify |
| M2  | JRA rescore reaches Container despite Worker flag — fix     | open                  |
| M3  | Ban-ei rescore Worker-native (shadow parity → enable)       | open                  |
| M4  | NAR rescore Worker-native (scorer + parity harness)         | open                  |
| M5  | Initial per-race predictions Worker-native                  | open                  |
| M6  | Running-style independent of Container day-base             | open                  |
| M7  | Day-base DuckDB layers ported to R2 SQL / Workers           | open                  |

JRA and Ban-ei changes need verification on their race days (next JRA:
9/26–27). A NAR-only day does not verify JRA or Ban-ei.

## Log

- 2026-09-23 16:30 JST — M1 root cause: `running-style-race-count-66-of-71`
  kept NAR day-base "stale" on 9/22 and rebuilt the 12 GiB day-base every few
  minutes until midnight; each rebuild reproduced the same bytes. Readiness now
  waives the race-count check only when the artifact holds exactly the current
  RS rows (content hash + row count). Commit 011f9f1f, Worker-only version
  `b424bae0` (Container images untouched). Verify: predict GiB-h tonight and on
  9/26–27 JRA days versus the baseline above.
- 2026-09-23 21:15 JST — read-only follow-up: predict queue backlog 41 → 31 → 26
  across snapshots from 11:44 to 12:02 UTC; oldest message still ~11:44 UTC;
  metrics approximate and payload type unknown. Weight-rescore backlog 0; no
  active RaceDayWorkflow instances. Recent Worker logs show future-NAR
  `worker-r2-warm` feature-foundation HITs before Container, but Worker wall time
  is not Container runtime/billing. Locally removed one duplicate D1 statement
  per JRA/NAR running-style readiness watermark probe by calculating
  `MAX(predicted_at) OVER ()` alongside fingerprint rows. Gates pass (77 files,
  2,150 tests, 97.74/95.05/98.79/98.32 coverage); not deployed and runtime/query
  savings are not yet measured. See `container-full-migration-handoff-plan.md` §7.6.
- 2026-09-23 21:33 JST — Queue fell to 8 (1,704 bytes), still oldest 11:44:36 UTC;
  weight-rescore backlog 0 and active Workflows 0. Historical Workers Logs +
  GraphQL helpers were added to `bun run prod`, both bounded/read-only, to test
  old Workflow/Container correlation. For failed NAR `mud1i9dh`, its 1,197.7s
  `sync-day-base` step overlaps a 19:00–19:22 UTC window where the standard-4
  prediction app has 1,200s of 12-GiB allocated-memory equivalent in workload
  metrics and 1,174.9s in billing usage. Strong app/time correlation, not
  instance-level proof; Container `containerUptime` reported 0 despite nonzero
  CPU/memory, and Workers logs are sampled at 10%. Raw response:
  `container-runtime-correlation-20260922.json`. At 12:39 UTC both predict and
  weight-rescore Queue metrics were 0 and all six nonterminal Workflow statuses
  were empty, but recent NAR 20260924 logs still show queue work: 51.84s wall /
  12ms CPU, `worker-r2-warm` HIT before Container, then Neon 520 completion-guard
  failure; race-chain lifecycle stops exit 0. Current 12:15–12:35 GraphQL shows
  nonzero race-chain Container CPU (114.2s workload /129.75s billing). Empty
  backlog is not idle, uptime metric is inconsistent, and per-instance attribution
  remains needed. Scripts gates pass (312 tests, 96.79% coverage, Ruff/type
  checks/shellcheck). See handoff plan §7.6.
- 2026-09-23 21:54 JST — fresh read-only check: both prediction/rescore Queue
  backlogs remain 0 and all six nonterminal Workflows remain 0, while 12:40 UTC
  NAR `/run` was accepted. 12:25–12:45 Container app usage: race-chain 5.14s
  workload CPU /25.92s billable CPU; predict app had no row; MLflow 130.47s /
  67s. Added `bun run prod d1-insights` from Cloudflare's 31-day D1 GraphQL
  query-insights dataset. 9/22 sync-realtime-data D1 baseline: the aggregate
  query targeted by the local change ran 25,067x (avg40.67ms, 2.785B rows read) and fingerprint
  query 26,360x (avg21.74ms, 2.929B rows read). Cumulative SQL time is ~26.5m
  for the pair, not wall time; local change remains undeployed, so savings are
  unmeasured. 9/23 partial insights still show old query shape. CLI tests/gates
  pass (322 tests, 96.79% coverage, Ruff/type/shell checks). See handoff §7.6.
- 2026-09-23 22:18 JST — read-only recheck: both Queues and all six nonterminal
  Workflows still report zero. `/run` for NAR 20260923 was accepted; readiness
  GET then took 1.237s Worker wall/38ms CPU. Container GraphQL 12:58–13:18 UTC
  returned no app rows; not proof of zero runtime. Same-day D1 Insights snapshot
  was unchanged at 2,775 aggregate / 2,457 fingerprint executions, still showing
  old SQL; window-query change remains undeployed. See handoff §7.6.
- 2026-09-23 22:40 JST — user authorized production deployment. Isolated the
  four-file D1 query change on clean HEAD, full package gates passed (2,136 tests,
  97.72/95.06/98.78/98.31 coverage, tsc/lint/format). Worker-only version
  `80df2113` deployed at 100% (deployment `a6af318e`), incumbent `b424bae0`
  retained for rollback. No Container image, D1 migration or Queue purge.
  Initial Queue backlogs both 0; 59/59 postrace coverage already complete.
  D1 insights still show the old 9/23 counts without a new query shape, so
  query/Container savings cannot yet be claimed. Single sampled Neon HTTP 520
  on 12:26 UTC was caught by existing fail-open guard, and Worker warm HIT
  followed; verify race completion and monitor recurrence. See handoff §7.6.
- 2026-09-23 ~22:44 JST — **rolled back** candidate `80df2113` to incumbent
  `b424bae0` at 100%: direct D1 read-only probe measured window query
  408.57ms vs prior SELECT 71.55ms + aggregate 39.90ms (619 rows, NAR 9/23).
  One unpaired execution, not an aggregate saving; query consolidation regressed
  badly despite removing a statement. Three query-only files restored to HEAD,
  related readiness tests restored without dropping owner-generation work.
  Never redeploy the slow candidate. Details/rollback evidence in handoff §7.6.
- 2026-09-24 local-only — immutable day-base manifest protocol is implemented through D1 `0011`, owner-scoped R2 writes and `/prewarm-day-base` generation IDs; Python full/rescore now accepts an explicit generation-scoped day-base key, but production has **not** received this migration, code, or Container image. Finish-position Worker readers (readiness, race materializer, market-signal foundation, rescore attestation, enqueue freshness) now resolve the D1 pointer and R2 ETag rather than the legacy alias when a manifest is present; an authenticated service-binding endpoint exposes that pointer to sync-realtime-data. Its running-style feature reader and cron readiness use the same pointer, with an explicit pre-cutover legacy mode (`DAY_BASE_MANIFEST_REQUIRED=0`) and a strict mode (`1`). Pending, malformed, or ETag-mismatched pointers never read the alias. Local gates: finish-position-cron 2,166 tests / 97.68% statements, 95.05% branches before the latest queue-producer test; sync-realtime-data 95.04% branches, all four thresholds pass; Python 2,846 tests passed / 1 skipped, 97.35% coverage. Admin direct category prewarm now acquires the shared D1 owner before Container entry (busy/old-date do not start a build); stale-restart pickup forwards that owner generation. The controlled old-PUT-after-new-publication SQLite/R2 test confirms readers select only the new pointer. Immutable-key use in Python `pipeline_runner.ensure_day_base`, actual cross-package rollout/read parity, and live runtime measurement are **still required**; do not deploy this partial cutover.
- 2026-09-24 ~00:58 JST — isolated finish-position-cron/Container release candidate in a detached clean-HEAD worktree: overlaid only manifest/owner/Workflow/immutable-source changes; intentionally omitted uncommitted PyTorch-MPX/ONNX/Dockerfile/lookup experiments. Copied and SHA-256 verified the 144 manifest-selected model files. Isolated Worker gates: 2,172 tests, branch 95.06%, tsc/lint/format; isolated Python: 2,835 tests/1 skipped, 97.28% coverage, Ruff/basedpyright and artifact integrity MATCH. Docker-backed `wrangler deploy --dry-run` completed successfully at 01:04 JST with three configured images; bounded Colima wrapper shut down the VM afterward. Incumbent finish-position-cron version `9ec530a0-4b20-4555-bcde-dad92d9cea14` is deployed 100% (rollback target). At ~01:06–01:08 JST the same isolated candidate deployed under `deploy-with-queue-drain.ts` and bounded Docker wrapper: predict/rescore Queue delivery paused; all prediction Containers drained; three images and Worker uploaded; delivery resumed in the `finally` block and Colima shut down. New version `0e13f756-f8eb-4666-ac84-0d2da379acd4` (deployment `ab45c63f-b4bc-488e-9020-0077349f4d8f`) was independently confirmed at **100%**; incumbent `9ec530a0` retained for rollback. Postdeploy prediction/rescore Queue backlog 0, both `delivery_paused=false` with one consumer each (independent Queue API), race-day Workflow running/waiting 0; sampled finish-position-cron logs had 0 events (not proof of no errors). D1 day-base manifest rows remain **empty**; authenticated production `/api/internal/day-base-manifest?category=nar&runYmd=20260924` returned `{"state":"missing"}` (HTTP 200). `DAY_BASE_MANIFEST_REQUIRED=0`; this is a compatibility rollout, not validated generation cutover or measured Container runtime reduction.
- 2026-09-24 ~00:48 JST — deployed **sync-realtime-data Worker only** version `58128983-5224-4466-9f6b-cacd7254ba45`, `DAY_BASE_MANIFEST_REQUIRED=0`; `wrangler deploy --dry-run` and gates (2,719 tests, branch 95.07%, tsc/lint/format) passed. It reads published immutable pointers when available, uses the legacy alias only for manifest-absent/old endpoint 404 in compatibility mode, and fails closed for pending/mismatched pointers. The incumbent finish-position-cron Worker/Container writer is still unchanged; production manifest table remains empty. This stage does **not** cut over production predictions nor establish runtime reduction. `wrangler deploy` retained all four existing consumer bindings; postdeploy running-style and prediction Queue backlogs both 0. Five sampled sync-realtime-data log events returned without API errors, but they predated/overlapped deployment and are not serving parity evidence. Verify natural race output before strict flag.
- 2026-09-24 ~00:43 JST — applied **only additive D1 schema** `0010` (nullable repair generation) and `0011` (new day-base manifest table) to `finish-position-cron-db` after confirming two pending migrations and recording the pre-migration Time Travel bookmark `00002606-00000000-000050ef-eb721587ef382e32edacf0f6121d8d29` (D1 time travel retains at most 30 days). The incumbent Worker/Container code, Queue bindings and `DAY_BASE_MANIFEST_REQUIRED=0` were **not deployed/changed**; empty manifest does not redirect any incumbent reads. Remote verification: no pending migrations; manifest table and `repair_generation_id` column exist; both prediction/rescore Queue backlogs remain 0. Never restore the entire live D1 database to the bookmark as a casual rollback: that would also discard later production writes.
- 2026-09-24 00:38 JST — read-only production check: prediction and weight-rescore Queue backlogs both 0, `finish-position-race-day` running instances 0. Container metrics/usage for 00:00–00:30 JST returned no rows (delayed/sampled data; **not** proof of zero runtime). No local migration or strict manifest flag deployed, so none of these observations measure the local cutover.
- 2026-09-24 local-only — finished the focused `/predict` immutable-source handoff: the prediction Queue resolves a published D1 generation in both compatibility and strict mode before claiming a Container slot, retries pending pointers (and missing pointers when `DAY_BASE_MANIFEST_REQUIRED=1`), and sends its key/ETag; only manifest-absent compatibility mode retains the alias. Python validates their category/date/generation shape, checks the exact R2 HEAD ETag and watermark, isolates the local parquet by key+ETag, and validates race/market foundations against the same key. A pinned miss never falls back to the legacy alias. Gates: finish-position-cron 2,172 tests, 97.68/95.06/98.79/98.28 coverage, tsc/lint/format pass; Python 2,851 tests (1 skipped), 97.36% coverage, Ruff/basedpyright pass. sync-realtime-data: 2,718 tests, 97.55/95.04/96.92/98.05 coverage, tsc/lint/format pass. **Not deployed**: flag remains `0`, D1 migrations 0010/0011 and cross-package cutover still need a safe rollout and live proof; no runtime reduction is claimed.
- 2026-09-23 22:50 JST — indexed range instead of LIKE over `race_key` used
  SQLite's index; direct D1 read-only run had exact 619-row parity, a uniform
  maximum timestamp, 9.89ms / 2,478 rows-read vs the old pair's 111.45ms /
  224,208 rows-read. One unpaired sample, not a live Worker saving. Isolated
  clean-HEAD gates passed; corrected Worker-only version `9ec530a0` deployed
  100% (deployment `18241450`), original dirty-tree gates pass (2,150 tests,
  97.74/95.06/98.79/98.32 coverage, tsc/lint/format). Both Queue backlogs
  were 0; recent sampled logs showed no error. D1 Insights has only the manual
  literal indexed query so far; wait for natural parameterized calls. See §7.6.
