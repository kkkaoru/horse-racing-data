# Day-base PREWARM status (2026-08-16 05:22 JST)

Read-only. No implement. NAR host generation not touched.

## Cron

`wrangler.jsonc` `30 0 * * *` is Cloudflare UTC = **09:30 JST**, not 00:30 JST.
Clock at check: 05:22 JST / 20:22 UTC. **Today's 08-16 prewarm has not fired yet.**

`runDayBasePrewarm` writes no D1 row. Evidence is Worker logs + R2 object.

Prewarm targets `GET /prewarm-day-base` on the **category-scoped** DO
(`predict-{category}`), not a race shard. Comment in `day-base-prewarm.ts`:
sharded processes still lazy-build on first race.

## Artifacts

Expected key: `feat-daybase/catalog-v1/{category}/{runDate}/features.parquet`
(`build_r2_day_base_key`).

Signed HEAD via container `r2_client` against `R2_BUCKET` (name not printed):

| key date | jra | nar | ban-ei | use as evidence?                               |
| -------- | --- | --- | ------ | ---------------------------------------------- |
| 20260816 | 404 | 404 | 404    | **No** — 09:30 JST not fired yet at check time |
| 20260815 | 404 | 404 | 404    | **Yes** — past 09:30                           |
| 20260814 | 404 | 404 | 404    | **Yes** — past 09:30                           |

**Cite 6/6 past-day 404s, not 9/9.**

Local: no `/tmp/predict-upcoming/daybase-*`.
08-15 emergency logs: no `daybase` / `racechain` lines.
Neon `_debug_finish_position_layer_timing`: 0 rows in last 48h.

## Reuse tonight?

No. A hit would be an 0816 object. There is none.

Independently: `is_day_base_split_enabled` is an allowlist. The secret
`DAY_BASE_SPLIT_ENABLED` exists; its value was not read. Empty/unset means
every focused-full race runs full `LAYER_CHAIN`. Even if the allowlist were
on, `ensure_day_base` would miss (no local dir, R2 404) and either
inline-build or fall back to the full chain.

## Cron fire vs artifact (separate)

`wrangler` has no cron-history command. Read-only GraphQL
`workersInvocationsAdaptive` (`scriptName=finish-position-cron`,
`CLOUDFLARE_DEBUG_TOKEN` Analytics:Read), same method as
`jra-serving-audit-jun-jul-2026-07-17.md` §12.1a.

Windows 00:25–00:35 UTC (= 09:25–09:35 JST):

| local date | 00:25 (`25 0`, Neon pre-wake) | 00:30 (`30 0`, day-base PREWARM)                                                  |
| ---------- | ----------------------------- | --------------------------------------------------------------------------------- |
| 08-14      | success req=1 sub=1           | **internalError req=1 err=1 sub=1**                                               |
| 08-15      | success ×2                    | **success req=2 sub=0** and **success req=1 sub=3**; then 00:33 **internalError** |
| 08-16      | (empty — future at 05:33 JST) | empty                                                                             |

So this is **not** “cron not registered” (case 1).
08-14: fired, platform `internalError` (case 3 / crash).
08-15: fired `success` but **no surviving R2 object** (early return and/or put failed). `sub=0` on one 00:30 row is compatible with a handler that returned without container/R2 work; that is not proven.

No config change. No deploy.

## Implication for Phase 4

The 08-15 per-race p50 ~9.9 min is **not** "RACE_CHAIN only after a warm
day-base". It is consistent with paying DAY+RACE every race. Connecting
existing prewarm → `ensure_day_base` is still the largest policy-compatible
win; it is not already happening for these three dates.
