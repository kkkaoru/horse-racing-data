# Weight-rescore observation (2026-08-16)

Read-only. Optimize owns cache HIT / rank change. This note owns
trigger fire, Neon `prediction_generated_at`, and whether the healthy
host 80/80 rows get overwritten worse.

## Baseline at 08:02 JST

Neon 0816 still **80 races / 940 rows**. Model on all JRA rows:
`jra-cb-stage1-marketfree235-2013`.

JRA `prediction_generated_at` (UTC):

- Venue 01 all 12, venue 07 all 12, venue 04 races 04–12:
  **20:03:55–20:04:14 UTC = 05:03–05:04 JST** (host one-shot).
- Already overwritten before weight publish:
  - `04/03` 21:14:03 UTC = 06:14 JST
  - `04/02` 21:33:28 UTC = 06:33 JST
  - `04/01` 22:07:10 UTC = 07:07 JST

Those three are **not** a clean "healthy host row" control anymore.
Use `04/04` (05:04:03) and `01/01` (05:03:55) as the degradation
controls. First post is `04/01` 09:40; weight lead is ~30–60 min so
**09:09 is the first likely publish**, not a guarantee.

D1 `fetch_logs` last 8h (remote, 08:02 JST):

| job_type                                     | latest                 | note                    |
| -------------------------------------------- | ---------------------- | ----------------------- |
| `fetch-weights` `skip:weights-empty`         | **08:02:07 JST**       | still empty now         |
| `fetch-weights` `queued:weights-empty-retry` | 08:02:02               | watchdog retrying       |
| `weight-watchdog` ok                         | 08:00:33               | alive                   |
| `fetch-weights` ok                           | 22:47:46 **yesterday** | last real write         |
| `weight-rescore-trigger` ok                  | 20:08:24 **yesterday** | **no 0816 trigger yet** |
| `weight-rescore-trigger` skip:not-claimed    | 15:39 yesterday        | 2 rows                  |

So at 08:02 the trigger path exists in code
(`triggerRescoreAfterWeights` → POST
`/api/internal/rescore-race`) but has **not** fired for today's
empty-weight polls. Fire condition is a successful weight **write**,
not a skip.

## What I will measure after 09:09

1. Trigger: `fetch_logs` where `job_type='weight-rescore-trigger'`
   and `created_at >= '2026-08-16 09:00:00+09:00'`. Count ok /
   skip:not-claimed / error. Pair with `fetch-weights` status=ok for
   the same `race_key`.
2. Neon clock: for each JRA race, new `prediction_generated_at` vs
   the 08:02 baseline above. HIT rescore should move the timestamp
   by seconds–a few minutes. MISS / focused-full can take ~10 min
   or never move (04/01 last night).
3. Degradation: row count must stay 80 / 940. `model_version` must
   not silently become a worse family without a note. Pedigree
   quality is optimize's HIT parquet check; here we only refuse a
   coverage drop or a mass rewrite of the 05:04 host rows onto a
   later stamp **without** a matching trigger row.

Do not PUT cache. Do not deploy. Do not POST `/run`.
