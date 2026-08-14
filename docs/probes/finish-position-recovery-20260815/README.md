# 2026-08-15 finish-position recovery preparation

This document began as a read-only snapshot taken around 2026-08-15 00:03
JST. The snapshot sections are retained as incident history, but the recovery
was subsequently executed and completed.

## Final outcome (updated 2026-08-15 07:03 JST)

- Running-style prerequisites: **68/68 complete**.
- Finish-position predictions: **68/68 races and 822/822 runners complete**:
  JRA 36/487, ordinary NAR 20/216, and ban-ei 12/119.
- Quality gates: zero null rank/score, invalid rank range, all-equal/all-zero,
  or NaN failures in the final fixed-inventory check.
- The final reconciliation tracked nine transient failed writes: eight
  `ReadOnlySqlTransaction` and one `pg_closed`. Every named race subsequently
  passed the fixed-inventory check; none remains failed.
- Viewer cache warm: **68/68 complete**, with zero warm failures. Details and
  production cache-hit measurements are recorded below.

Do not use the historical 00:03 and 00:10 counts below as current status.

## Fixed race inventory

`races-20260815.tsv` is the input for the submission script. It includes `raceStartAtJst` and is sorted by post time, earliest first (09:40 through 20:50 JST), so submission prioritizes races closest to post.

| Category             | Races | Expected runners |
| -------------------- | ----: | ---------------: |
| JRA                  |    36 |              487 |
| NAR excluding ban-ei |    20 |              216 |
| ban-ei (venue 83)    |    12 |              119 |
| Total                |    68 |              822 |

The remote `sync-realtime-data` D1 has exactly 68 distinct `(source, keibajo_code, race_bango)` rows for `kaisai_nen=2026, kaisai_tsukihi=0815`. Neon `race_entry_corner_features` also has exactly 68 races and 822 runners. The reported Neon split of JRA 36 / NAR 20 / ban-ei 12 agrees with D1: Neon stores both ordinary NAR and ban-ei with `source='nar'`, so venue `83` must be split out explicitly.

Important join trap: D1 `realtime_race_sources.race_key` uses `jra:2026:0815:01:01`, while `running_style_inference_state.race_key` uses `jra:20260815:01:01`. Join RS state by date/source/venue/race fields, not `race_key`.

## Historical prerequisite snapshot

At approximately 00:10 JST, `running_style_inference_state` was:

- completed: 49
- processing: 1 (`jra 07/03`, attempted 00:10:13 JST)
- pending: 17
- failed: 1 (`jra 04/02`, attempted 00:05:07 JST)

Incomplete races at that snapshot:

- JRA: `04/02` failed; `07/03` processing
- NAR: `55/01`, `55/02`, `55/08`-`55/10` pending
- ban-ei: `83/01`-`83/12` pending

The queue is progressing: races have continued moving through processing to completed. Failures have been transient `PC_KEIBA_R2_CATALOG ... HTTP 502: r2_sql_unavailable`; retries moved `55/01` and `55/02` back to pending and completed `55/06`. The `04/02` retry failed with the same error. Re-query D1 immediately before submission.

A direct read-only Catalog comparison ruled out a race-specific 04/02 load problem. The same 04/02 query subsequently returned HTTP 200 in 38.96 seconds with 15 rows and 155 features; 04/04 returned 200 in 48.25 seconds (15 rows), and 04/05 returned 200 in 54.20 seconds (18 rows). In the same probe, already-completed 04/03 returned HTTP 502 after 300.57 seconds. The failures are intermittent request-level R2 SQL timeouts, not a persistent 04/02 cardinality issue. A spaced retry is justified; the successful 04/02 GET may also have warmed its edge cache.

All 49 completed states had `expected_horse_count = written_horse_count`, and `race_running_styles` had the same row count.

A read-only GET inventory of the expected full-pipeline per-race feature-cache keys found 0/68 objects at:

```text
feat-cache/catalog-v1/{category}/20260815/{keibajoCode}/{raceBango}/features.parquet
```

This is not, by itself, a full-generation blocker. The focused `mode=full` pipeline builds the features and seeds this object; the container code explicitly treats absent R2 cache credentials/objects as degraded-but-functional for full generation. It does mean all 68 currently lack the fast rescore cache and must take the full path. Raw entrant availability was separately confirmed by the 68-race Neon query above.

Historical classification at the snapshot time:

- Ready from RS perspective: 49 races
- Blocked on RS: 19 races (the failed/processing/pending list above)
- Existing fast per-race R2 rescore cache: 0 races
- Full generation possible after RS completion: all races with completed RS and raw entries; the full path creates its own feature cache

All 19 historical RS blockers subsequently recovered; final RS state is 68/68.

## Submission

`submit-focused-races.sh` targets only the admin per-race endpoint. It:

- requires `keibajoCode` and `raceBango` for every POST;
- sends a non-default `User-Agent` to avoid WAF rule 1010;
- has configurable concurrency (default 9);
- records per-race UTC start/end, HTTP status, curl exit, and response in JSONL;
- supports retrying only the latest failed/busy races with `--retry-failed`;
- is dry-run by default and requires explicit `--execute`;
- keeps verbose container logging off by default; use `--debug` only for a focused diagnostic run.

Example for the production operator (not executed during preparation):

```bash
export FINISH_POSITION_CRON_TRIGGER_TOKEN='...'
docs/probes/finish-position-recovery-20260815/submit-focused-races.sh \
  --execute --concurrency 9 --log-dir /tmp/fp-20260815-attempt-1

docs/probes/finish-position-recovery-20260815/submit-focused-races.sh \
  --execute --concurrency 9 \
  --retry-failed /tmp/fp-20260815-attempt-1 \
  --log-dir /tmp/fp-20260815-retry-1
```

The script considers HTTP 2xx successful except response status `busy` or `error`. Operators should still run `check-completion.sql`; an accepted detached pipeline is not yet complete.

## Completion and quality check

Run `check-completion.sql` repeatedly against Neon. It reports:

- races with any predictions and fully healthy races out of 68;
- prediction row count versus expected runner count;
- rank coverage `1..field size`;
- all-equal, all-zero, and numeric NaN scores;
- generation timestamps restricted to the 2026-08-15 JST day.

At preparation time it returned 0/68 races with any finish-position prediction.
The final fixed-inventory result was 68/68 races, 822/822 runners, with the
category split and zero quality failures shown in the final-outcome section.
An accepted submission was not used as evidence; completion was verified from
Neon rows and then checked by named race.

The emergency local-generation artifacts also preserve operational failure
evidence. Among 68 base log files, six races contain local PostgreSQL
`server closed the connection unexpectedly` errors (8.82%) and eight contain
Neon `ReadOnlySqlTransaction` errors (11.76%). Fourteen `retry2` logs have no
Traceback, although two stop before their completion JSON. Therefore the final
68/68 Neon inventory—not an assumption that every retry log completed—is the
recovery proof. The six connection-close log events are a different artifact
count from the final reconciliation's single `pg_closed` failed write; do not
merge those denominators. The underlying local PostgreSQL reset cause and the
read-only transaction cause remain unresolved. The recovery owner observed an
approximately 16% ReadOnly rate in the affected local-write cohort.

The target list is fixed and does not infer result finality. If a future query adds a result-code predicate, use both `btrim(code) <> ''` and `btrim(code) !~ '^0+$'`; do not treat `IS NOT NULL` as confirmed.

## Queue delivery readiness and race-day checks

`wrangler queues info` does not display the queue's `delivery_paused` field. Use the read-only API probe instead; it exits 0 for active, 2 for paused, and 3 when readiness cannot be determined:

```bash
docs/probes/finish-position-recovery-20260815/check-queue-delivery.sh
```

For the 2026-08-15 meeting, the active operator or agent must run it at approximately 08:30 JST before the first race, then at 10:30, 12:30, 14:30, and 16:30 while weight-triggered rescoring is active. Also run it immediately when prediction timestamps fail to advance after a weight update.

If it reports `PAUSED`:

```bash
bunx wrangler queues resume-delivery finish-position-predict-queue
docs/probes/finish-position-recovery-20260815/check-queue-delivery.sh
```

After resuming, verify that queue-consumer events return in `wrangler tail` and that `prediction_generated_at` advances in Neon. Identify races whose expected rescore did not arrive and re-enqueue only those races through the per-race producer; do not launch an unscoped day-wide production run. The 2026-08-14 recovery required purging stale backlog and reconstructing focused messages from `races-20260815.tsv`, but purge is an incident-only action, not part of the routine resume procedure.

Cloudflare's pause documentation describes explicit control-plane pause/resume operations and does not document an automatic pause threshold. Local agent history contained no pause command after 2026-08-09, and the current OAuth token lacks Audit Logs Read permission, so the actor that caused the 2026-08-12 to 2026-08-14 outage remains unconfirmed. Do not attribute a future pause to deploy, error rate, or Cloudflare automation without audit evidence.

## Viewer cache warm after prediction completion

Cache warm completed before the first race. The initial process started around
04:30 JST after its model-feature preflight passed all 68 races, but its outer
process stopped around 04:40 with no recorded request failure: three races had
all major sections complete and five had page warm/verify artifacts. The empty
`failures.tsv`, final timing, and absence of a live process indicate external
process termination rather than a script-classified HTTP failure; the exact
external cause is not proven.

The resumed run at concurrency 2 skipped the three fully completed races,
skipped only the already-completed page step for two more, and processed the
remaining work from 06:07:32 to 06:30:04 JST:

- `tmp/race-detail-cache-warm-resume/20260815/tasks.tsv`: 65 tasks;
- `results.tsv`: 65 successful tasks;
- `failures.tsv`: empty;
- `timings.tsv`: all 906 requests returned HTTP 200.

Together with the original three complete races, this proves **68/68 warm
completion**. Major-section verify averages included 0.089 seconds for
`overall-score`. Access-token production GET checks after completion returned:

| Category | Race                         |          Page |                      `overall-score` |
| -------- | ---------------------------- | ------------: | -----------------------------------: |
| JRA      | 04/01 (09:40, earliest post) | 200 / 1.696 s | 200 / 0.071 s; repeats 0.062/0.061 s |
| NAR      | 44/05                        | 200 / 2.007 s |        200 / 1.336 s; repeat 0.060 s |
| ban-ei   | 83/12                        | 200 / 1.899 s |        200 / 0.070 s; repeat 0.072 s |

Race 83/12 had returned `section_unavailable` with HTTP 503 in 15.997 seconds
before warming, so its HTTP 200 response in 0.070 seconds is the decisive
cold-to-warm recovery check.

After the subsequent viewer pedigree deployment (`74f9030b`), the numeric-venue
cache was checked again rather than assumed to persist. JRA 04/01, NAR 44/05,
and ban-ei 83/12 returned HTTP 200 with valid 9-, 5-, and 10-row payloads. Final
repeat times were 0.065, 0.092, and 0.075 seconds with no 503, so another
68-race warm was not required. The reusable command remains:

```bash
EXPECTED_RACE_COUNT=68 CONCURRENCY=2 \
  bash docs/probes/pc-keiba-viewer-cache-warm/warm-race-detail-cache.sh 20260815
```

The script performs its own non-empty model-feature preflight and aborts before
warming other sections if any race is incomplete. See
`docs/probes/pc-keiba-viewer-cache-warm/README.md` for safety behavior. Do not
repeat it merely because queue submissions were accepted; require a healthy
fixed-inventory check and a demonstrated need to refresh the cache.

## Historical duration estimate

Production has three deterministic shards per category, nine total, and one full pipeline slot per shard. For this exact race list, the FNV shard distribution is:

| Shard    | Races | At 7 min/race |
| -------- | ----: | ------------: |
| jra-0    |    15 |       105 min |
| jra-1    |    10 |        70 min |
| jra-2    |    11 |        77 min |
| nar-0    |     7 |        49 min |
| nar-1    |     4 |        28 min |
| nar-2    |     9 |        63 min |
| ban-ei-0 |     3 |        21 min |
| ban-ei-1 |     4 |        28 min |
| ban-ei-2 |     5 |        35 min |

The critical path is `jra-0`: approximately 105 minutes, plus queue/startup/retry overhead. The theoretical latest start for 07:00 is 05:15 JST. Use 05:00 JST or earlier to preserve at least 15 minutes of operational margin.

Increasing client submission concurrency beyond 9 does not increase pipeline throughput: queue consumer concurrency is 9, container `max_instances` is 10, and static routing caps each category at three shards. It only increases queued/busy requests. Reducing client concurrency below 9 can lengthen the run. Shortening the 105-minute critical path requires changing shard routing/count and jointly revalidating queue concurrency plus the shared container instance cap; that is a production configuration change outside this preparation task.
