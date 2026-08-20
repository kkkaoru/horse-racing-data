# Weight-triggered per-race add-on (2026-08-16 05:41 JST)

Read-only measurements. No deploy. Ban-ei / host generation not touched.

Purpose (user): after pre-race horse-weight arrives, **add** a per-race
prediction on top of the morning no-weight generate. Same problem as
DAY-share speedup: 9.9 min full chain vs a 31–50 min lead.

## 1. How many minutes before post does weight arrive?

Source: D1 `sync-realtime-data` `fetch_logs` (`job_type='fetch-weights'`,
`status='ok'`) joined to `realtime_race_sources.race_start_at_jst`.
Date **2026-08-15**, n=68 races (all of that day’s inventory).

First successful weight fetch vs post:

|      | lead (minutes) |
| ---- | -------------: |
| min  |         **31** |
| p50  |         **40** |
| mean |           39.6 |
| max  |             50 |

JRA first posts (04/01, 07/01, 01/01): **36 minutes**.
Shortest: NAR 44/04 **31 minutes**.

This is D1 fetch time, not `jvd_se.bataiju` column arrival. Advisor’s
local PG tonight still has `bataiju` empty for 08-16 (950 entries). Those
are different stores.

**Deadline for the add-on generate: ~31–40 minutes before post.**

## 2. Does weight write start a rescore?

**Yes. Path exists and fired yesterday.**

`apps/sync-realtime-data/src/worker.ts`:
after `insertHorseWeightSnapshot` → `triggerRescoreAfterWeights(env, raceKey)`
→ service binding POST `/api/internal/rescore-race`
(`keibajoCode` + `raceBango` + category).

`finish-position-cron` `handleInternalRescoreRace`:

- `RESCORE_ENABLED` must be `"1"` (it is, wrangler vars)
- this path **does not** read `RESCORE_CATEGORIES` (JRA included)
- DO claim: at most one enqueue per race per day

D1 `fetch_logs` `job_type='weight-rescore-trigger'`:

| date         |                  ok | skip:not-claimed |
| ------------ | ------------------: | ---------------: |
| 08-15        | **68** (every race) |                2 |
| 08-16 so far |               **0** |                0 |

Trigger timestamp equals first `fetch-weights` ok (same second).

Why 08-16 Neon rows have no weight: **weights have not been fetched yet**
(advisor local PG 0/950; trigger count 0). Not a missing hook.

Why 08-15 triggers did not always leave a same-day Neon UPDATE: see §3.

## 3. Is rescore 17 layers or a light path?

Code (`predict_upcoming._make_rescore_fn`):

- Catalog source + single-race scope: try watermarked **per-race**
  `feat-cache/catalog-v1/{cat}/{date}/{keibajo}/{bango}/features.parquet`
- HIT: no DuckDB / no 21y scan. Refresh late-binding columns (odds +
  bataiju) and score.
- MISS: `CacheMissError` → full `LAYER_CHAIN` (JRA 17 / NAR 10 / Ban-ei 7)

R2 HEAD tonight (`build_r2_per_race_feat_cache_key`):

| key               | result                |
| ----------------- | --------------------- |
| jra 0816 04/01    | 404                   |
| jra 0816 07/01    | HIT 73 KB (18:37:13Z) |
| nar 0816 35/01    | 404                   |
| nar 0815 44/10    | 404                   |
| ban-ei 0816 83/10 | HIT 49 KB (19:53:29Z) |
| ban-ei 0815 83/12 | 404                   |

So cache is **sparse**. Many weight triggers will take the full chain.

Neon `prediction_generated_at` after 08-15 09:03 JST trigger for `jra 04/01`:
last_gen `09:21:08Z` = **17 min after trigger**, **19 min before 09:40 post**.
That UPDATE exists. Other races’ last_gen is hours later or still the
08-14 first_served — **cannot attribute those to the weight trigger**.
Per-trigger latency is mostly **unmeasurable** (no start audit; cache
miss looks like any other full generate).

Measurable bound: one JRA race **17 min** trigger→write (04/01), which
still beat a 36 min lead. p50 9.9 min full-chain would also beat 31 min
**if the slot is free**. A cache miss behind a busy shard / vanished
accept (04/01 72 min pattern) will not.

## 4. Does it make post?

| path                                | time                                          | vs 31 min min-lead | vs 36 min JRA first |
| ----------------------------------- | --------------------------------------------- | ------------------ | ------------------- |
| rescore cache HIT                   | seconds–low minutes (code; not timed tonight) | yes                | yes                 |
| full LAYER_CHAIN p50                | 9.9 min (08-15 local)                         | yes if not queued  | yes                 |
| full worst 08-15                    | 14.6 min                                      | yes                | yes                 |
| vanished accept (08-16 04/01)       | 72 min                                        | **no**             | **no**              |
| DAY+RACE every time on a busy shard | 9.9 × queue depth                             | **no**             | **no**              |

**Same-problem statement:** the weight add-on only makes post if the
per-race job is the **light** path (feat-cache HIT) or a short RACE_CHAIN
on a warm day-base. Paying 17 layers on every weight event, plus
unobservable failures, is why “trigger ok=68” ≠ “prediction saw bataiju
before post”.

Required together (policy unchanged: one race enqueue/UPSERT):

1. Seed per-race `feat-cache` (or day-base + RACE_CHAIN) at morning generate
   so 09:03 weight HIT is seconds.
2. Always-on layer / HTTP audit so a miss is visible.
3. Do not flip `RESCORE_ENABLED` / `COORDINATOR_ENABLED` without a gate.
   Weight path is already enabled.

08-16 action until first weights (~08:50–09:03 JST if same as 08-15):
keep Ban-ei recovery with fix-developer; land observability in repo;
do **not** deploy during 09:40–20:50.
