# 2026-08-15 race-day handoff

Last updated: 2026-08-15 07:56 JST

This document is the standalone handoff for the finish-position prediction
incident, the 2026-08-15 recovery, and the overseas-race viewer work. Automated
prediction outage monitoring is implemented in Git but is **not deployed**.
During the 2026-08-15 meetings, a human operator must perform the checks below.

## 1. Incident summary

After the 2026-08-12 17:29 deploy, finish-position prediction output was absent
for three days:

- 2026-08-13: 8 races affected;
- 2026-08-14: 19 races affected;
- 2026-08-15: all 68 scheduled races initially had no prediction rows.

The immediate cause was Cloudflare Queue `finish-position-predict-queue` having
`delivery_paused=true`. Producers still accepted enqueue calls, but the primary
consumer received no messages. Existing self-heal code enqueued work back onto
the same paused queue and logged escalation only to sampled console output, so
it could neither recover nor reliably notify a person.

The actor or mechanism that paused delivery is **not known**. Repository and
local-agent history contain no pause command after 2026-08-09, and the available
Cloudflare OAuth token does not have Audit Logs Read permission. Do not claim
that a deploy, an error-rate threshold, Cloudflare automation, or a specific
person caused the pause without audit evidence. Determining the actor remains
an open incident item.

## 2. Current state

### Queue and race data

- Queue delivery was resumed and the read-only control-plane probe currently
  reports `ACTIVE`.
- Running-style prediction prerequisites are complete for all 68 races
  (`68/68`).
- The fixed inventory is 36 JRA, 20 ordinary NAR, and 12 ban-ei races, with 822
  expected runners total. It is recorded in
  `docs/probes/finish-position-recovery-20260815/races-20260815.tsv`.
- Finish-position recovery is **complete**. The final fixed-inventory check
  verified 68/68 races and 822/822 runners: JRA 36/487, ordinary NAR 20/216,
  and ban-ei 12/119. Null rank/score, invalid rank range, all-equal/all-zero,
  and NaN quality failures were zero. This result came from Neon row checks and
  named-race verification, not from HTTP `accepted` responses.
- The final reconciliation tracked nine transient failed writes: eight
  `ReadOnlySqlTransaction` and one `pg_closed`. All nine named races recovered
  and passed the final inventory check.
- The emergency local path was validated on JRA 04/01: 9 expected entries, 9
  rows, ranks 1-9, 9 distinct horses, and non-degenerate scores. This proves the
  emergency path can write valid rows; it does not make local generation an
  approved recurring architecture.

### Emergency local-generation operational evidence

The 68 base log artifacts contain more first-attempt process failures than the
nine-item final reconciliation, because they count a different execution
stage and denominator:

- local PostgreSQL `server closed the connection unexpectedly`: 6/68 base logs
  (8.82%);
- Neon `ReadOnlySqlTransaction`: 8/68 base logs (11.76%);
- total base logs with either terminal error: 14/68 (20.59%).

There are 14 `retry2` logs with no Traceback, but only 12 end with a completion
JSON; the remaining two stop mid-run. Do not claim 14/14 retry-log completion.
The definitive recovery evidence is the later 68/68, 822/822 Neon inventory.
The six base-log connection closes are not the same metric as the one
`pg_closed` item in the final failed-write reconciliation. The recovery owner
also observed an approximately 16% ReadOnly rate in the affected local-write
cohort. The causes of both local PostgreSQL resets and read-only transactions
remain unresolved.

### Running-style repair for venue 55

Venue 55 races 01 and 02 had missing running-style inputs. Between 01:51 and
02:00 JST, the recovery operator read Neon primary
`race_entry_corner_features` through the existing running-style feature SQL,
verified the official NAR flat model (`nar-running-style-lgbm-prod-v3`, 146
features), and non-destructively wrote:

- `running-style/features-parquet/raw-iceberg-v1/nar/20260815/nar:20260815:55:01.parquet`
  — 6 rows, 42,529 bytes, 43.8-second source query;
- `running-style/features-parquet/raw-iceberg-v1/nar/20260815/nar:20260815:55:02.parquet`
  — 7 rows, 43,019 bytes, 23.2-second source query.

The objects are in R2 bucket `pc-keiba-finish-position-models`. After Worker
retry/cache fallback, D1 running-style state completed with written=expected
`6/6` and `7/7`. The NAR daily archive then verified 32 races and 335 rows,
including 6 and 7 rows for these races. This was a one-shot temporary probe,
not committed production code. No source row or existing object was deleted.

### Viewer deployments

The viewer deployment chain is:

1. Primary overseas identity fix:
   - version: `d687b68b-f033-4888-afea-14e92e3f14e0`;
   - rollback anchor at that time: `aea68ad3-b901-452c-b47b-3c0240074905`.
2. Placeholder correction and identity-display follow-up (approximately 01:25
   JST):
   - version: `5e7c8a7b-c7e0-42e1-91ca-ebf7a83dc3a9`;
   - rollback anchor at that time: `d687b68b-f033-4888-afea-14e92e3f14e0`.
3. Mapped overseas-history/time-score correction and removal of 4,451
   incorrect rows from commit `a0a573be`:
   - version: `e924f7ee-da84-4f7b-86e9-22f74453b3dd`;
   - rollback anchor at that time:
     `5e7c8a7b-c7e0-42e1-91ca-ebf7a83dc3a9`.
4. Overseas pedigree migration/seed and viewer support from commit `d015055e`:
   - version: `74f9030b-3a90-48e7-8f77-92e50f54c4a6`;
   - rollback anchor at that time:
     `e924f7ee-da84-4f7b-86e9-22f74453b3dd`.
5. Overseas person JV fallback and precomputed snapshot from commits `e69c9023`
   and `3b2575e7`:
   - **current active version**:
     `06fd3c24-9ed2-4ee5-9bb4-80583c010198`;
   - **current rollback anchor**:
     `74f9030b-3a90-48e7-8f77-92e50f54c4a6`.

The short race-night chain is therefore `5e7c8a7b` (placeholder and identity
display) -> `e924f7ee` (mapped history/time score and removal of 4,451 incorrect
rows) -> `74f9030b` (pedigree) -> `06fd3c24` (person win rates and snapshot).
Rolling the current version back one step removes only the person-rate/snapshot
changes while retaining pedigree;
rolling 74f back to e924 also removes pedigree; rolling e924 back to 5e7 removes
the mapped-history/time-score correction.

The e924 version loads A8 mapped netkeiba history and time scores and uses cache
v4 only for alphabetic overseas venues. Production checks returned 104 A8/04
result rows covering 10 horses and time scores for all 10; representative JRA,
ordinary NAR, and ban-ei response bodies remained byte-identical.

For the 74f pedigree version, the Neon migration and ten-row seed committed in
one successful transaction. Raw verification returned 10 rows for 10 horses
with zero missing IDs. The A8 page returned HTTP 200 with pedigree fallback
active: six placeholder-derived scores, neutral handling for horses 2 and 9,
and the ambiguous Kizuna mapping excluded. Pedigree cache v6 applies only to
alphabetic overseas venues; numeric domestic venues retain their prior cache
version. Normal 2026-08-15 JRA 01/01, NAR 44/01, and ban-ei 83/01
`overall-score` bodies were byte-identical before and after that deploy.

For the current 06fd person-rate/snapshot version, the deploy owner's postcheck
found byte-identical pre/post bodies for the `similar`, `time-score`, and
`overall-score` sections of JRA 01/01, NAR 44/05, and ban-ei 83/12. The three
`overall-score` requests returned in 0.040, 0.027, and 0.032 seconds. A8 warm
checks returned `similar` in 0.072 seconds, `time-score` in 0.071 seconds, and
`overall-score` in 0.033 seconds.

Production checks passed for the Jacques le Marois page: ten English horse
names, full jockey/trainer/owner names, eight external JRA-VAN profile links,
two real JV horse links, paddock and prediction/statistical sections, and a 404
for `/horses/0000000000`. A normal domestic JRA race with no supplemental
identity also passed, proving the JV fallback remains intact.

Viewer cache warm is also **complete for 68/68 races**. An initial process
completed three races before external termination around 04:40 JST; it recorded
no request failure. A resumed concurrency-2 run skipped completed work and
finished 65/65 remaining tasks from 06:07:32 to 06:30:04, with an empty
`failures.tsv` and all 906 timing rows at HTTP 200. Artifacts are under
`tmp/race-detail-cache-warm-resume/20260815/`.

Access-token GET checks demonstrated the resulting behavior:

- JRA 04/01, the 09:40 earliest-post race: page 200/1.696 s;
  `overall-score` 200/0.071 s and repeats 0.062/0.061 s;
- NAR 44/05: page 200/2.007 s; `overall-score` 200/1.336 s and repeat
  0.060 s;
- ban-ei 83/12: page 200/1.899 s; `overall-score` 200/0.070 s and repeat
  0.072 s.

Before warming, 83/12 `overall-score` returned `section_unavailable` with HTTP
503 in 15.997 seconds. Its later 200 in 0.070 seconds is the decisive cache-hit
proof. The page responses intentionally remain `private, no-cache, no-store`;
the warm primarily prepares the application detail-section caches. The
`section_unavailable` fallback itself has existed since commit `c7defe8d`
(2026-05-28).

The cache was independently rechecked after both later viewer deployments. For
version `74f9030b`, JRA 04/01, NAR 44/05, and ban-ei 83/12 returned HTTP 200
with valid 9-, 5-, and 10-row `overall-score` payloads; final repeated times
were 0.065, 0.092, and 0.075 seconds.

After the current `06fd3c24` deploy, the same three races again returned HTTP
200 with valid 9-, 5-, and 10-row payloads in 0.078, 0.066, and 0.073 seconds.
Repeated body SHA-256 values were stable within each race. There was no 503 or
multi-second cold response. Numeric-venue caches therefore remained warm and no
second 68-race warm was required.

### Open incident items

- The actor or mechanism that set queue delivery to PAUSED remains unknown.
- The cause of intermittent local PostgreSQL connection resets remains unknown.
- The cause of the approximately 16% ReadOnly rate in the affected local-write
  cohort remains unknown.
- The first warm process appears to have been externally terminated because it
  stopped with an empty failure file; the exact external cause is not proven.

## 3. Required checks during the 2026-08-15 meetings

### Important limitation

Automatic outage detection in commits `f0f8be62` and `3bf60bfb` is **not
running in production yet**. Until the meetings end and that code is reviewed,
migrated, configured, and deployed, human checks are the only operational
defense.

### Queue delivery check

Run the production control-plane probe at approximately:

- 08:30 JST, before the first race;
- 10:30 JST;
- 12:30 JST;
- 14:30 JST;
- 16:30 JST;
- immediately whenever expected prediction timestamps stop advancing after a
  horse-weight update.

Command:

```bash
docs/probes/finish-position-recovery-20260815/check-queue-delivery.sh
```

Exit status meanings:

- `0`: delivery active;
- `2`: delivery paused;
- `3`: status unknown, including API/auth failure. Treat unknown as an incident,
  not as healthy.

### Verify prediction and rescore progress

Use the fixed-inventory Neon query:

```bash
set -a
. ./.env
set +a
container exec -i horse-racing-local-postgresql \
  psql "$NEON_PRIMARY_URL" -v ON_ERROR_STOP=1 \
  < docs/probes/finish-position-recovery-20260815/check-completion.sql
```

Confirm:

- complete race count and expected runner count match;
- ranks span 1 through field size;
- score-quality failures and stale-generation failures are zero;
- after horse weights are published, `prediction_generated_at` advances for
  that race. A queue status of `ACTIVE` does not prove that rescore completed.

Run the query after each scheduled queue check and whenever the viewer does not
show a weight-triggered update within the expected processing window.

### If delivery is paused

1. Resume primary delivery:

   ```bash
   bunx wrangler queues resume-delivery finish-position-predict-queue
   ```

2. Re-run `check-queue-delivery.sh`; require `ACTIVE`.
3. Verify consumer events resume with `bunx wrangler tail` and verify
   `prediction_generated_at` advances in Neon.
4. Use `check-completion.sql` to create an exact missing-race list.
5. Re-enqueue only those per-race targets. Do not start an unscoped/day-wide
   prediction run. Prepare a TSV containing only missing races and use:

   ```bash
   export FINISH_POSITION_CRON_TRIGGER_TOKEN='...'
   docs/probes/finish-position-recovery-20260815/submit-focused-races.sh \
     --execute --concurrency 9 --input /path/to/missing-races.tsv \
     --log-dir /tmp/fp-recovery-$(date +%Y%m%dT%H%M%S)
   ```

6. An HTTP `accepted` response is not completion. Continue the Neon check until
   row count, rank range, distinct-score quality, and timestamps pass.

Do not routinely purge a queue. The overnight purge was an incident-specific
response to stale backlog; routine recovery is resume, identify exact missing
races, and enqueue only those races.

### Viewer cache warm

The pre-race warm is already complete for all 68 races; do not rerun it merely
as a routine check. Use the representative GET results in the current-state
section as the completed baseline. If a later cache invalidation makes a
carefully targeted rerun necessary, first require all 68 races to remain
healthy, then use:

```bash
EXPECTED_RACE_COUNT=68 CONCURRENCY=2 \
  bash docs/probes/pc-keiba-viewer-cache-warm/warm-race-detail-cache.sh 20260815
```

The script independently aborts if any race lacks non-empty model prediction
features. Keep concurrency low and retain its failure/timing artifacts.

## 4. Work required after all meetings end

### Deploy outage monitoring

Commits:

- `f0f8be62` — end-to-end readiness, delivery canary, self-heal lifecycle,
  direct durable incident/outbox, acknowledgement, resend, and recovery;
- `3bf60bfb` — daily healthy heartbeat, warning noise control, and control-plane
  probe prioritization in the design.

Before deploy:

1. apply `apps/finish-position-cron/migrations/0006_create_prediction_monitoring.sql`;
2. configure `FINISH_POSITION_CRON_TOKEN` and `ALERT_ACK_TOKEN`;
3. record the private Discord channel and named primary/backup responders;
4. witness forced critical, 10/30-minute resend, acknowledgement, hourly
   reminder, recovery, and daily healthy-heartbeat tests;
5. deploy only after the day's final meeting and perform rollback-aware checks.

End-to-end readiness remains the primary signal. Queue API
`delivery_paused`, the queue canary, and monitor heartbeat are independent
supplements, not replacements.

### Deploy container startup hardening

Commit `0f13c58a` adds PostgreSQL `connect_timeout=10`, bounded retry behavior,
and credential-safe startup diagnostics. It was intentionally not deployed on
the incident night because the actual outage cause was paused queue delivery.
Deploy and validate it only after all meetings end.

### Resolve prediction quality/data issues

- Complete and deploy the market-feature gate correction only after the
  meetings. Commit `5cabe484` preserves odds behind a flag, but race-day
  coverage does not prove the market-feature semantics are correct.
- Investigate why canonical `tansho_ninkijun`/odds values are not reaching the
  feature rows. Odds/popularity features directly affect finish-position
  quality; coverage can be complete while these inputs are wrong.
- Address intermittent R2 SQL requests that stall until approximately 300
  seconds and return 502. Successful retries for the same races show this is an
  intermittent service/request failure, not race cardinality. Add bounded
  retries/caching without hiding persistent failures.
- Automate the now-proven Cloudflare Queue API `delivery_paused` check as signal
  C. It is an immediate and inexpensive cause signal, but API/token failure must
  itself alert and must not suppress readiness or canary checks.
- Obtain Cloudflare Audit Logs Read access if identifying who or what paused the
  queue remains required.

### Make viewer cache invalidation durable

The warm recovered today's cache, but it is not the permanent cache lifecycle.
After the meetings, implement and validate an explicit invalidation/versioning
mechanism for detail sections when overseas mappings, histories, scores, or
prediction inputs change. Preserve the deployment-specific, alphabetic-venue
cache-version precaution (currently pedigree cache v6) until that mechanism is
proven. Do not solve this by blanket race-day cache deletion.

### Repository publication

No `git push` was performed. Before adding this handoff, local `main` was 27 commits ahead
of `origin/main`. Review the complete local commit range, decide whether to
split or publish it, and push only with explicit owner approval. Never use
`--no-verify`.

## 5. Prohibited shortcuts and cautions

- Do not make emergency local finish-position generation the permanent path.
  The intended architecture is Cloudflare queue, Worker/Container, and Neon.
- Do not treat enqueue success, HTTP `accepted`, queue `ACTIVE`, or an empty
  alert stream as proof of prediction readiness.
- Do not deploy monitoring or container changes during the meetings.
- Do not change or synthesize JV fixed-width identity keys.
- Do not delete prediction, identity, queue-audit, or lifecycle data as a normal
  repair step. Use additive/upsert behavior and targeted per-race recovery.
- Do not expose DSNs, webhook URLs, tokens, or database hostnames in logs,
  responses, or handoff artifacts.
- Do not push the local commit stack until explicitly approved.
