# finish-position container deploy window (2026-08-16)

Do not run this between 09:40 and 20:50 JST. The 07-18 outage was a
mid-card deploy. Window = user confirmation only. Default is tomorrow
morning, not tonight.

Confirmed live at 06:50 JST via `wrangler deployments list --name
finish-position-cron` (read-only):

| when (UTC)           | version                                    | source        | note                                                                                            |
| -------------------- | ------------------------------------------ | ------------- | ----------------------------------------------------------------------------------------------- |
| 2026-08-14T15:00:58Z | **`0c76062e-03c6-4b8b-9a25-a501a4f4c9cc`** | restore       | current 100%. "Restore current image after identifying paused queue root cause; keep split off" |
| 2026-08-14T14:45:43Z | `953d086b-4342-42ce-a146-0a5061d51575`     | rollback      | last emergency rollback (08-11 image)                                                           |
| 2026-08-14T14:18:14Z | `0c76062e-03c6-4b8b-9a25-a501a4f4c9cc`     | secret change | same id as current                                                                              |
| 2026-08-12T08:29:40Z | `aab128a1-9b7a-468c-b976-11b540a61a55`     | upload        | last code upload before the secret-change line                                                  |

Primary rollback target after any new deploy: **`0c76062e`**.
If that version itself is bad: **`953d086b`** (used on 08-14).

Local image already built, not shipped:
`finish-position-predict-local:duckdb-1.5.5`.

Would ride a future deploy (do not ship tonight): `85bfba82` writable
txn, `2139645b` `PIPELINE_DIR`, `67440b8b` DuckDB 1.5.5, `cd90cb73` /
`3cd71358` PREWARM logging. `9007a6e6` FORCE env is viewer-side; the
container DO `envVars` map does not forward it yet.

Do not put `DAY_BASE_SPLIT_ENABLED` on the same deploy. The 08-14
restore said `keep split off`. Enabling it is a separate investigation
(`day-base-split-0812-lineage-20260816.md`), not a follow-on to DuckDB
1.5.5.

## When the window opens

```sh
cd apps/finish-position-predict-container
bun run artifact:verify -- --artifact-root models --system finish-position
cd ../finish-position-cron
bun run deploy -- --containers-rollout immediate
```

`package.json` `deploy` already runs artifact verify + docker-compat
before `wrangler deploy`.

## Success checks (read-only)

A deploy is successful only if all of these hold:

1. `wrangler deployments list` shows a new 100% version. Record it.
   Keep `0c76062e` as the rollback id.
2. Neon still has 80 distinct 0816 races / 940 rows. UPSERT must not
   drop coverage.
3. Probe one already-scored race (`04/01` is fine). HTTP 200 and the
   same runner count as before the deploy.
4. After `BEGIN; SET TRANSACTION READ WRITE`, `SHOW transaction_read_only`
   is `off`.
5. `DAY_BASE_SPLIT_ENABLED` is still unset / empty.

## Rollback

```sh
cd apps/finish-position-cron
bunx wrangler rollback --name finish-position-cron --version-id 0c76062e-03c6-4b8b-9a25-a501a4f4c9cc
```

If Wrangler asks for a message, use the 08-14 wording: restore known
image; keep split off. Prediction writes are UPSERT-only. Do not
DELETE / TRUNCATE Neon rows.
