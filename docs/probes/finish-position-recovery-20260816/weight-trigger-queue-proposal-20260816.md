# Weight-triggered per-race prediction: why it misses betting time (2026-08-16)

Written 06:13 JST by `pi-optimize-developer`. **Proposal only. No deploy, no
flag change, no further focused-full POSTs, no remaining-MISS expansion.**

Related measurements in this directory:

- `weight-triggered-per-race-20260816.md` — weight lead times, trigger path
- `feat-cache-seed-blocked-20260816.md` — why local seed was refused
- `focused-full-0401-timeout-20260816.md` — 04/01 accept+20min fail
- `day-base-prewarm-status-20260816.md` — PREWARM fire vs R2 404
- `per-race-latency-phase4-proposal-20260816.md` — DAY/RACE split
- `measure-builtin-split-0401.md` — built-in `build_day_base` + `build_pipeline_from_day_base` timing (host)

## 1. Requirement

Horse weights arrive **31–50 minutes before post** (08-15 D1, n=68,
p50 **40 min**, JRA R1 **36 min**). The user then **bets from the
prediction**. Finishing at post time is worthless. The add-on generate
after weight write must leave **usable slack** (tens of minutes), not
just beat the post clock.

Policy unchanged: enqueue and UPSERT stay **one race** (`keibajo` +
`raceBango`). Shared objects may only be **features**, never prediction
rows.

## 2. Why tonight does not meet that (measurements only)

| fact                                                                                                                                                   | source                       |
| ------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------- |
| Today’s `jvd_se`/`nvd_se` bataiju: **0 / 950** entries (advisor local PG)                                                                              | advisor 05:39                |
| Neon 08-16 predictions exist for **80/80** races (940 rows)                                                                                            | advisor 05:53 / 06:12        |
| Those rows were scored **without weights**                                                                                                             | logs `bataiju=0/…`; PG 0/950 |
| Per-race feat-cache HEAD of 73 Neon races: **HIT 10 / MISS 63**; JRA **2/36**                                                                          | this track 05:47             |
| Weight→rescore **hook exists** and fired **68× ok on 08-15**; **0× on 08-16** so far                                                                   | D1 `fetch_logs`              |
| Full `LAYER_CHAIN` p50 **9.9 min** JRA (08-15 local, n=33)                                                                                             | phase 1                      |
| Cache HIT rescore: no DuckDB / no 21y scan (**seconds–low minutes**, not timed tonight)                                                                | `_make_rescore_fn`           |
| **04/01** admin focused-full: accept **05:51:12 JST**, still R2 **404** and Neon **13 rows / last_gen 20:04:02Z** at **+1213 s** and again **+1326 s** | this track                   |
| In that window Neon completed **55/10, 55/09, 55/03, 07/09, 01/01** (01/01 at 06:04:47, **after** our accept)                                          | advisor Neon                 |

So: the morning generate ran; the weight add-on did not (no weights yet);
most races have no feat-cache; a requested full rebuild sat **>20 min**
while other races finished.

## 3. Structural defects and how each bites

### (a) Day-base PREWARM (pi-fix-developer’s four defects)

Cron `30 0 * * *` = **09:30 JST**. GraphQL: 08-14 **00:30Z internalError**;
08-15 **00:30Z success** but **6/6 past-day R2 keys 404**. 08-16 404 at
05:22 is **not** evidence (cron not yet fired).

Fix-developer listed: swallowed exceptions, treating missing key as
success, timeout, `DAY_BASE_SPLIT_ENABLED` unset. This track confirmed
the **put/get key** is `build_r2_day_base_key` =
`feat-daybase/catalog-v1/{cat}/{date}/features.parquet` (not a guessed
prefix). Even a healthy 09:30 prewarm is **10 min before first post**
and cannot feed 09:40 R1.

Effect: DAY_CHAIN is not reused. Focused-full pays **17/10/7 layers**
per race.

### (b) Feat-cache writer is HTTP focused-full only

Write path: NDJSON `parquetKey` → Worker `FEATURES_CACHE.put`.
`_score_and_flush_races` and CLI `main()` **do not** PUT.
Emergency host generate (tonight) therefore leaves cache empty.

Rejected shortcut: split `feat-jra-layer-16` (490×390) and PUT.
Vs production HIT `04/12`: **names/order match**, **16 dtypes differ**,
**values on the same race differ** (e.g. pedigree rank 14 vs 1).
`feat-jra-v7-final` is **1×8** and is not features. Seeding would create
a **wrong HIT**. MISS is safer.

### (c) Queue has no priority; consume is not FIFO

All producers `PREDICT_QUEUE.send` with **no priority**.
Consumer `max_concurrency: 9`. `RACE_SHARDED_DO=1` → up to 3 shards ×
category. Focused-full returns `accepted` then
`message.retry({ delaySeconds })` — the request is **not held at the
head**. Per-process lock is one pipeline per container.

Overtake is **structural**. 04/01 sat 20+ min while five other races
completed, including `01/01` after our accept.

### (d) DAY / RACE split unused — and must not be flipped tonight

JRA 17 = DAY 12 + RACE 5; NAR 10 = 7+3; Ban-ei 7 = 6+1.
`DAY_BASE_SPLIT_ENABLED` is a secret name; value unread; vars empty.
Without a live day-base object, split still rebuilds or falls back.

08-14 restore message on the live image is **“keep split off”**
(`container-deploy-window-20260816.md`). Whether that was because split
caused the 08-12 outage, or because restore refused extra flags, is
**owned by pi-fix-developer** and is **not resolved here**. This
proposal therefore does **not** say “enable split”.

### (e) Weight rescore shares the same queue

`triggerRescoreAfterWeights` → `/api/internal/rescore-race` → same
`PREDICT_QUEUE` (`mode=rescore`). `RESCORE_ENABLED=1`. This path does
**not** read `RESCORE_CATEGORIES`. Dedup: one claim per race per day.

Coordinator **enqueue** uses `race_start_at_jst` (T-X window) but
`COORDINATOR_ENABLED=0` tonight. **Consume order does not** use
time-to-post. Shard hash is `keibajo:bango` only.

## 4. Proposed fixes and expected effect

Do not implement tonight. Numbers below are **measured** unless marked
**estimate**.

### 4.0 Primary reason to enable split later: JRA pedigree quality

**Not tonight.** Spec still requires team-lead confirmation before flipping
`DAY_BASE_SPLIT_ENABLED`. This section records why the **main** expected
effect is no longer “save 2.45 min”.

**Harm of split off (measured 07:29 JST):** production per-race `mode=full`
runs the DuckDB **base** with `--target-race`. That sets `horse_filter` to
the race’s own `ketto_toroku_bango` list. JRA pedigree is
`rec INNER JOIN jra_um`; sire aggregates then lack other progeny and miss
`PEDIGREE_MIN_RACES = 5`. Experiment
(`pedigree-target-race-experiment-0401-20260816.md`):

| 04/01 13 horses                                    | 5 sire/damsire components pos |  score live |
| -------------------------------------------------- | ----------------------------: | ----------: |
| `--target-race 04:01` base                         |         **0 / 13** (all NULL) |  **0 / 13** |
| same 13 horses, full-day base (`target_race=None`) |                    11–13 / 13 | **13 / 13** |

Production HIT `04/12` is the same **degeneracy** (no positive scores;
mix of 0.0 and NULL, not byte-equal to the experiment). 08-16 JRA HITs
**2 / 2** degenerate; 08-15 JRA sample **4 / 5**. NAR HITs live.
RACE_CHAIN `--target-race` does **not** kill these columns when they were
already computed on a full-day day-base.

Tonight’s Neon 80/80 was a **full-day host** generate (`target_race=None`),
so those JRA pedigree columns are live. That is better than the usual
production per-race path, and it was an accident of the outage rebuild.

**Speed is secondary.** Host split: day-base 1333.8 s + whole-day RACE_CHAIN
447.0 s vs 08-15 full-chain p50 9.9 min. 447 / 594 ≈ **25% less compute
after a day-base exists**. Advisor ~07:00 judged that 25% “too small to
enable”. That judgment used **only** the time axis and is **wrong** given
§4.0. Keep the 25% number; do not use it as the go/no-go.

**Why a flip can still do nothing (07-15 lineage, keep this):** 2026-07-15
`e6111ca6` made `ensure_day_base()` always return `None` for catalog
sources, so `DAY_BASE_SPLIT_ENABLED=jra` still rebuilt DAY every race
(confirmed 2026-07-17 `9da2f5eb` / serving-latency probe). Watermark reuse
landed later (`216a2fc5` / `3d75c0d1`). Enabling the secret **without** a
working `ensure_day_base` hit would pay DAY every race **and** still run
base with `--target-race` on the first race of a shard — pedigree would
stay dead on that first race. Flip is not “set the secret and walk away”.

**08-12 / keep split off:** outage cause recorded as paused queue, not
split (`day-base-split-0812-lineage-20260816.md`). Still do not flip
tonight.

### 4.1 Measured layer times (host, JRA 20260816, 04:01, FORCE 8G/4t)

Built-in `build_day_base` + `build_pipeline_from_day_base`.
Work dir `/tmp/fp-builtin-split`. Existing `/tmp/predict-upcoming/feat-jra-*`
untouched. No Neon / R2 write. Host-only lookup rebind for
`add-course-numerical-features.py` (container path `/app/lookups/...`
does not exist on the Mac). Details:
`measure-builtin-split-0401.md`.

**DAY (once per category+day): 1333.8 s = 22.2 min**

| step | script                                    | seconds |
| ---- | ----------------------------------------- | ------: |
| base | `finish_position_features_duckdb.py`      | 337.292 |
| 1    | `add-race-internal-features.py`           |   0.182 |
| 2    | `add-sectional-and-weight-features.py`    | 118.687 |
| 3    | `add-futan-juryo-features.py`             | 126.948 |
| 4    | `add-workout-features.py`                 |  65.296 |
| 5    | `add-grade-race-lineage-features.py`      | 142.512 |
| 6    | `add-head-to-head-features.py`            | 203.410 |
| 7    | `add-trainer-stable-affinity-features.py` | 122.751 |
| 8    | `add-pacestyle-features.py`               |   0.745 |
| 9    | `add-course-numerical-features.py`        |   0.086 |
| 10   | `add_kohan3f_going_features.py`           |  56.139 |
| 11   | `add-similar-race-features.py`            |  89.339 |
| 12   | `add-sire-venue-bias-features.py`         |  70.428 |

Tonight’s earlier full-day host base was **612 s** (03:44:53 pid →
03:55:05 `feat-jra-base`). This run’s base **337 s** was faster, not stuck.

Layers are **not uniform**. Fastest DAY layer 0.086 s vs slowest 337 s
is about **3900×**. Sharing “all 12 DAY layers” is a larger change than
attacking the few heavy ones (base / h2h / lineage / futan / trainer /
sectional).

**RACE_CHAIN (`target_race=04:01` in the log): 447.0 s = 7.45 min**

This is **not** “1-race RACE cost”. It is **whole-day RACE cost**.

Evidence:

- every `racechain-layer` line has `target_race=04:01`
  (`/tmp/fp-builtin-split-0401.out`)
- output `/tmp/fp-builtin-split/feat-jra-v7-final` is **490 rows / 36 races
  / 391 cols**, same shape as tonight’s `feat-jra-layer-16`
- 04/01 is **13 of those 490 rows**; the other 35 races are present
- day-base `final` was already 490×321; RACE_CHAIN appended columns, it did
  not drop races

So `--target-race` **did pass**, and it **did not shrink the parquet**. It
only narrows PG history staging. A 13-row input was **not** measured.

| step | script                                     | seconds |
| ---- | ------------------------------------------ | ------: |
| 1    | `add-market-signal-features.py`            |  57.891 |
| 2    | `add-near-miss-features.py`                | 112.357 |
| 3    | `add-baba-pedigree-affinity-features.py`   | 118.184 |
| 4    | `add-relationship-r1-features.py`          |  92.877 |
| 5    | `add-jra-jockey-pedigree-cell-features.py` |  65.652 |

Compare: 08-15 local **full** `LAYER_CHAIN` p50 **9.9 min** (JRA, n=33).
Whole-day RACE_CHAIN **7.45 min** is **measured** 2.45 min less than that
p50. near-miss 112 s vs the earlier day-wide ~5 min is the same order of
magnitude, not 1/36 — **fixed-cost-ish** on the RACE layers themselves.

**Expected effect of skipping DAY, written exactly:** DAY (measured
1333.8 s = base + 12 DAY layers) can be skipped on later races if a
day-base already exists. The RACE layers themselves cost about the same
for one race as for the whole day in this harness. Per-race speedup from
split is therefore **the skipped DAY time**, not “RACE becomes seconds”.

### 4.2 Changes

| change                                                                                                                                                                | grounded in                                                                                                           | expected effect                                                                                                                                                                                                                                                         |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A. Seed feat-cache from the same HTTP writer** after morning generate (or CLI PUT that emits the **identical** parquet the Worker already stores)                   | HIT 10 objects exist; CLI path wrote 0 of tonight’s JRA 34 MISS                                                       | Weight rescore **HIT** → seconds–low minutes **after dequeue** (HIT path not wall-timed tonight; **estimate**)                                                                                                                                                          |
| **B. After team-lead confirmation (not tonight): enable split so JRA **base** pedigree runs `target_race=None` once per day**                                         | experiment: `--target-race` base kills 5/5 components (0/13 live); full-day base 13/13 live; prod JRA HITs degenerate | **Primary:** 2nd+ races reuse a day-base whose pedigree is live. **Secondary:** ~25% less compute (447 s vs 9.9 min p50) after that day-base exists. First race/shard still pays DAY + a `--target-race` base unless prewarm wrote the day-base without `--target-race` |
| **B2. If a future owner attacks compute without flipping split:** the heavy DAY layers are base 337 / h2h 203 / lineage 143 / futan 127 / trainer 123 / sectional 119 | table in §4.1                                                                                                         | Smaller than “share all 12”. Effect on 9.9 min is **estimate** until those layers are changed                                                                                                                                                                           |
| **C. Near-post / weight-trigger consume priority** (or a separate high-priority queue)                                                                                | 04/01 +20 min with five overtakes; no priority field                                                                  | HIT or 7.45 / 9.9 min compute can actually **start** inside the 31 min lead. Without C, A and B can still miss betting time                                                                                                                                             |
| **D. Always-on layer timing + HTTP per-race start/end audit**                                                                                                         | 04/01 had no start clock; debug gate hid internals                                                                    | Next 20 min wait is diagnosable. Not a speedup                                                                                                                                                                                                                          |
| **E. Keep ≥8 GB on h2h**                                                                                                                                              | tonight RSS 6.95 GB; 2.7 GB OOM                                                                                       | Prevents vanished accepts. Not the 9.9 min itself                                                                                                                                                                                                                       |

**Estimate (say so):** if A+C hold, slack after weight ≈ 31–40 min minus
queue wait minus seconds of HIT rescore. If only A, slack ≈ lead minus
whatever wait 04/01 showed (20+ min) → **can be zero**. If only C and
MISS, slack ≈ lead minus **measured 7.45 min** (split path) or 9.9 min
p50 (full path) minus residual wait.

## 5. What we did not do tonight, and why

- **No production deploy / COORDINATOR / RESCORE / queue config change** —
  first post 09:40; 80/80 already on Neon; changing consume order mid-diagnosis
  is a new failure mode.
- **No layer-16 / v7-final seed** — schema and values disagreed with
  production HIT.
- **No second 04/01 POST** — retry goes to the tail of the same queue.
- **No flood of remaining 63 MISS** — would add work to the same
  non-priority queue and make overtake worse.
- **No Ban-ei / host kill** — Ban-ei finished exit=0 at 20:50:20Z.

## 6. What to measure next (to test the proposal)

1. **One** focused-full on an **idle** queue (document no other Neon writes
   in the window). Record accept→R2 HEAD HIT elapsed and schema vs an
   existing HIT (`04/12`). If this is ~p50 9.9 min, tonight’s 20 min is
   wait/overtake, not compute.
2. After a confirmed HIT, fire **one** `mode=rescore` for that race and
   time Neon `prediction_generated_at` − trigger. That is the HIT-path
   number (currently **estimate**).
3. Compare `predicted_rank` before/after that rescore on the same 13
   horses (04/01 snapshot last_gen 20:04:02Z). Large rank moves need
   explanation before expanding.
4. PREWARM: after fix-developer’s four defects land, HEAD
   `feat-daybase/.../YYYYMMDD` the day **after** 09:30 JST; do not use
   same-morning 404 as evidence.
5. Queue: if a priority or separate weight queue is added, repeat (1)+(2)
   **while** other full jobs are in flight. Success = weight rescore
   `generated_at` still inside the 31 min lead with slack.

## 7. Rejected hypotheses (do not repeat)

| tried / assumed                                                               | outcome                                                                                                          |
| ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| 08-16 PREWARM 404 means PREWARM is broken                                     | **No** — cron is 09:30 JST; check was 05:22. Evidence is **08-14/15 6/6 404**                                    |
| PREWARM key was guessed wrong                                                 | **No** — same `build_r2_day_base_key` as Worker PUT                                                              |
| Seed cache from `feat-jra-v7-final`                                           | **1×8**, not features                                                                                            |
| Seed from `feat-jra-layer-16`                                                 | 390 names match; **16 dtypes** and **values** ≠ R2 `04/12`                                                       |
| Weight trigger missing                                                        | **Exists**; 68 ok on 08-15                                                                                       |
| Compute 9.9 min alone blows the 31 min budget                                 | **No** if the job **starts immediately**                                                                         |
| 04/01 20 min = slow DuckDB                                                    | **No** — five other races completed in the same window                                                           |
| Re-POST 04/01                                                                 | **No** — tail of the same queue                                                                                  |
| FIFO queue                                                                    | **No** — no priority; concurrency 9; shard 3; delayed retry                                                      |
| Enable `DAY_BASE_SPLIT_ENABLED` tonight because RACE_CHAIN is 7.45 min / ~25% | **No** — spec needs team-lead; 07:00 “25% too small” used the wrong axis; quality is the reason, and not tonight |
| `--target-race` on RACE_CHAIN kills JRA pedigree                              | **No** — split final 04/01 and 04/12 scores live; those cols come from day-base                                  |
| `--target-race` on DuckDB **base** leaves JRA pedigree live                   | **No** — experiment 04/01 all five components NULL, score 0/13                                                   |
| Flip `DAY_BASE_SPLIT_ENABLED` always changes runtime (07-15)                  | **No** — `e6111ca6` made catalog `ensure_day_base()` always `None` until later watermark work                    |
| Host `build_day_base` works with container `COURSE_LOOKUP_PATH`               | **No** — `/app/lookups/...` missing on Mac; rebound in the measure script only                                   |
| Hand-assemble RACE 5 from DAY-complete `feat-jra-layer-6`                     | **No** — DAY_CHAIN is not a prefix of `LAYER_CHAIN`; market-signal needs `tansho_ninkijun`                       |

## 8. 04/01 fail record (copy)

```
05:51:12  focused-full accept (jra 04/01)
06:11+    R2 404 / Neon unchanged (13 rows, last_gen 20:04:02Z)
meanwhile 55/09 05:56:46 / 55/10 05:56:19 / 07/09 06:00:38
          55/03 06:00:31 / 01/01 06:04:47 completed
+1326s    still MISS (06:13:17 JST re-HEAD)
```

Container was live. The explicit request did not complete in 20 minutes
while other races did.
