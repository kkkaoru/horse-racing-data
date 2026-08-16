# Tomorrow morning runbook (2026-08-17)

Default window from `deploy-windows-tonight-20260816.md`.
Do this **before first NAR generate / 08:00 Neon count**.
08-17 has **no JRA**. First post 35/01 **11:45**.
If you wake after 10:00 with Neon 0, **do not expect 35/01**.
**pi-fix-developer will not be the 08:00 operator.** Handoff this
page. Anyone with repo access can run it.

Tonight’s nine commits **do not fix** the stall
(`nine-commits-do-not-fix-weight-stall-20260816.md`).
This page is order, not a promise.

Rollback: `0c76062e`. Emergency previous: `953d086b`.
Split stays off.

## 0. Before anything

```sh
cd apps/finish-position-cron
bunx wrangler deployments list --name finish-position-cron | head
```

Must still be `0c76062e` 100%. If not, stop.

```sh
bunx wrangler d1 execute finish-position-cron-db --remote --command \
  "select id, name from d1_migrations order by id"
```

Must be 0001–0005 only. If 0006 is already there, skip step 1.

## 1. D1 0006 (tables only)

```sh
cd apps/finish-position-cron
bunx wrangler d1 migrations apply finish-position-cron-db --remote
```

Print must list **only** `0006_create_prediction_monitoring.sql`.
Then `--yes`.

Check: sixth migrations row; two `finish_position_delivery_*`
tables; **0 rows**. Detail: `migration-0006-apply-prep-20260816.md`.

0006 does **not** label weight messages (no `deliveryTrackingId`).
Canaries start after the **next** Worker deploy that has
`enqueueDeliveryCanary`.

## 2. Image + Worker (one deploy)

```sh
cd apps/finish-position-predict-container
bun run artifact:verify -- --artifact-root models --system finish-position
cd ../finish-position-cron
bun run deploy -- --containers-rollout immediate
```

Do **not** set `DAY_BASE_SPLIT_ENABLED`.
Do **not** POST `/run`. Do **not** PUT feat-cache.

After: new 100% version recorded; keep `0c76062e` as rollback.
Neon 0816 count must not drop if that card is still there.

This ships whatever is on `main` (the nine plus later commits).
Stall-relevant are only `85bfba82` and maybe `67440b8b`.
`41676f7c` is visibility on MISS layers, not a Neon write.

## 3. Boot check (not the stall)

One already-scored race, HTTP 200, same runner count.
`SHOW transaction_read_only` after `SET TRANSACTION READ WRITE`
is `off` if you can see a writable probe. That is **boot**, not
“weight rescore works”.

## 4. Stall clock (~09:10)

Primary: D1 `fetch-weights` status=ok, then
`weight-rescore-trigger` ok. Not `jvd_se`.

Same minute: `wrangler containers list` LIVE INSTANCES.

Then Neon `prediction_generated_at` for that race vs the morning
baseline. Pass = new stamp **before post**. Fail = unchanged at
post (same sentence as 08-16).

A fail after this deploy does **not** mean the nine were buggy.
It means they were the wrong object. Next work is still the
per-message consume row, not another pile of commits.

## 5. Do not

- Deploy after first post, or during 09:40–20:50
- Apply 0006 during a live card (canary pollutes the queue)
- Judge HIT/MISS from Last-Modified
- Flip split
- Ship only the seven non-stall commits and call it a test

Copy-paste commands: `observe-commands-20260816.md`.
If Neon 0817 is still 0 at 08:00, host NAR by 08:15
(`tomorrow-host-fallback-deadline-20260817.md`). First post is
**11:45**, not 09:40. Ban-ei 08-16 13:54 is tonight, not this page.
