# Migration 0006 apply prep (do not run today)

Read-only. No apply. No deploy. Clock of the remote reads:
**2026-08-16 ~10:50 JST**.

Remote `finish-position-cron-db`
(`9475ef83-d1da-4b47-9251-0cb431176aa8`):

| `d1_migrations`                        | applied_at          |
| -------------------------------------- | ------------------- |
| `0001_create_audit.sql`                | 2026-06-03 09:37:46 |
| `0002_create_dlq_events.sql`           | 2026-07-11 15:46:48 |
| `0003_create_old_date_skip_events.sql` | 2026-07-11 19:13:38 |
| `0004_create_coverage_gap_events.sql`  | 2026-07-11 19:31:38 |
| `0005_add_dlq_error_details.sql`       | 2026-08-09 15:01:04 |

`sqlite_master` has no `finish_position_delivery_canaries` and no
`finish_position_delivery_lifecycle`. **0006 is the only unapplied
file in this app.**

`sync-realtime-data` remote: files `0001`–`0034`, remote rows **34**.
None missing there.

## 1. Command (tomorrow, not now)

```sh
cd apps/finish-position-cron
bunx wrangler d1 migrations apply finish-position-cron-db --remote
```

Wrangler will offer only `0006_create_prediction_monitoring.sql`.
Do not pass `--yes` until the printed file list is that one file.

Before:

```sh
bunx wrangler d1 execute finish-position-cron-db --remote --command \
  "select id, name from d1_migrations order by id"
bunx wrangler d1 execute finish-position-cron-db --remote --command \
  "select name from sqlite_master where name like 'finish_position_delivery_%'"
```

Expect 0001–0005 and **zero** delivery tables.

After:

```sh
bunx wrangler d1 execute finish-position-cron-db --remote --command \
  "select id, name from d1_migrations order by id"
bunx wrangler d1 execute finish-position-cron-db --remote --command \
  "select name, type from sqlite_master where name like 'finish_position_delivery_%' order by 1"
bunx wrangler d1 execute finish-position-cron-db --remote --command \
  "select count(*) as n from finish_position_delivery_canaries"
bunx wrangler d1 execute finish-position-cron-db --remote --command \
  "select count(*) as n from finish_position_delivery_lifecycle"
```

Expect a sixth migrations row, two tables + two indexes, **0 rows**.

Do not apply during 09:40–20:50. See risk below.

## 2. What the tables would record — and what they would not

SQL is `CREATE TABLE IF NOT EXISTS` + indexes only. Columns:

- `finish_position_delivery_canaries`: `id`, `enqueued_at`,
  `consumed_at`, `delivery_lag_ms`
- `finish_position_delivery_lifecycle`: `tracking_id`, `run_ymd`,
  `category`, `keibajo_code`, `race_bango`, `detected_at`,
  `enqueued_at`, `consumed_at`, `prediction_completed_at`,
  `notified_at`

Writers exist on **HEAD** (`f0f8be62`, 2026-08-15 01:58 JST). Apply
does not deploy them.

| path                                     | writes a row?                                           |
| ---------------------------------------- | ------------------------------------------------------- |
| Cron `*/5 0-13` canary                   | **yes**, if the live Worker has `enqueueDeliveryCanary` |
| `coverage-self-heal` → `enqueuePredict`  | **yes**, it sets `deliveryTrackingId`                   |
| POST `/run` → `enqueuePredict`           | **no** — Worker never passes an id                      |
| Weight `POST /api/internal/rescore-race` | **no** — `sendRescoreRaceMessage` has no id             |
| Coordinator `PREDICT_QUEUE.send`         | **no** — no id                                          |
| `processContainerPerRaceRescore` success | **no** `prediction_completed_at` even when an id exists |

`lifecycleIdentity` returns null without `deliveryTrackingId`.
`recordConsumedBestEffort` then no-ops. Today’s three JRA R1s used
the weight path. **0006 would not have labeled them.**

### Today’s proof-gaps

| gap                                            | 0006 apply alone                            | 0006 + current HEAD deploy                            |
| ---------------------------------------------- | ------------------------------------------- | ----------------------------------------------------- |
| Queue paused vs consumer alive                 | maybe canaries, if live already writes      | canaries every 5 min                                  |
| Self-heal / tracked `enqueuePredict` consume   | no (no writer on live unless already there) | detected / enqueued / consumed                        |
| Weight trigger consume started?                | **no**                                      | **no** (no tracking id)                               |
| Rescore reached Python / Neon                  | **no**                                      | **no** (rescore never sets `prediction_completed_at`) |
| HIT vs MISS                                    | **no**                                      | **no**                                                |
| Which GraphQL `internalError` is which message | **no**                                      | **no**                                                |
| Isolate / held-fetch timeout                   | **no**                                      | **no**                                                |
| Late Neon landing                              | **no**                                      | **no**                                                |

Applying 0006 does **not** replace the durable per-message outcome
described in `nine-commits-do-not-fix-weight-stall-20260816.md`.
The table exists. The weight path does not write it.

## 3. Risk

File contents: two `CREATE TABLE IF NOT EXISTS`, two
`CREATE INDEX IF NOT EXISTS`. No `ALTER`, no `DROP`, no `UPDATE`,
no `DELETE`. Existing tables and prediction rows are untouched.

The only runtime change: if the **live** Worker already runs
`enqueueDeliveryCanary` (insert **then** `PREDICT_QUEUE.send`),
today the insert hits a missing table and the send never happens.
After apply, a tiny canary message would start landing on
`finish-position-predict-queue` every 5 minutes. No container, no
Neon. Still a new producer during the card. That is why this waits
until after 20:50.

If the live Worker does not have that function, apply leaves two
empty tables. Same “built, unused” shape as today.

## 4. Other apps

`finish-position-cron` migrations: **only 0006** unapplied.
`sync-realtime-data` migrations: **0** unapplied (34/34).
No other D1 in this stall’s write path.

No production change.
