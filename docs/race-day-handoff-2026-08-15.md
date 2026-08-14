# 2026-08-15 race-day handoff

Last updated: 2026-08-15 02:20 JST

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
- Finish-position recovery is running through both the production per-race path
  and an emergency local generation path. At 02:20 JST, 28/68 races were fully
  healthy: JRA 16, ordinary NAR 6, and ban-ei 6. Null rank/score failures were
  zero and generation was still advancing. The final production/local split
  and final `68/68` completion result must be inserted here after the recovery
  owner gives completion GO. Do not infer completion from HTTP `accepted`;
  verify the Neon rows with `check-completion.sql`.
- The emergency local path was validated on JRA 04/01: 9 expected entries, 9
  rows, ranks 1-9, 9 distinct horses, and non-degenerate scores. This proves the
  emergency path can write valid rows; it does not make local generation an
  approved recurring architecture.

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

The viewer was deployed twice:

1. Primary overseas identity fix:
   - version: `d687b68b-f033-4888-afea-14e92e3f14e0`;
   - rollback anchor at that time: `aea68ad3-b901-452c-b47b-3c0240074905`.
2. Presentation follow-up:
   - current active version: `5e7c8a7b-c7e0-42e1-91ca-ebf7a83dc3a9`;
   - current rollback anchor: `d687b68b-f033-4888-afea-14e92e3f14e0`.

Production checks passed for the Jacques le Marois page: ten English horse
names, full jockey/trainer/owner names, eight external JRA-VAN profile links,
two real JV horse links, paddock and prediction/statistical sections, and a 404
for `/horses/0000000000`. A normal domestic JRA race with no supplemental
identity also passed, proving the JV fallback remains intact.

The first cold `overall-score` request returned a transient 503 and immediate
retries succeeded. This is not a regression: the `section_unavailable` fallback
has existed since commit `c7defe8d` (2026-05-28). A reusable low-concurrency
cache warm script is available at
`docs/probes/pc-keiba-viewer-cache-warm/warm-race-detail-cache.sh`. Run it only
after all 68 prediction races are verified complete.

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

After, and only after, all 68 races are healthy:

```bash
EXPECTED_RACE_COUNT=68 CONCURRENCY=2 \
  bash docs/probes/pc-keiba-viewer-cache-warm/warm-race-detail-cache.sh 20260815
```

The script independently aborts if any race lacks non-empty model prediction
features, then warms the race page and overall/pace/finish/similar/bloodline/
time/premium sections. Keep concurrency low.

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
