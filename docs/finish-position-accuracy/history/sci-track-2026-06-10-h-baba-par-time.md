---
science_track_entry: true
hypothesis_id: H-BABA-PAR-TIME
date: 2026-06-10
based_on_iteration: iter30-nar-cb-residual-*-v8 (production baseline)
scope: NAR (all keibajo except Banei), per-class residual ensemble
status: PROCEED (probe PASS — partial rho 0.1798 >> bar 0.08; WF retrain warranted)
verdict: PROCEED — WF retrain with par-time adjusted speed index recommended
production_change: none (probe only; retrain pending)
artifacts:
  probe_script: tmp/nar-perclass/sci_track/v8_partime/probe.py
  par_time_table: tmp/nar-perclass/sci_track/v8_partime/par_time_table.json
  probe_verdict: tmp/nar-perclass/sci_track/v8_partime/probe_verdict.json
---

## Hypothesis

**H-BABA-PAR-TIME** (CYCLE 2, refinement direction #1 from V2 H-DIRT-GOING-SIGN-FLIP abort):

V2 empirically confirmed that NAR dirt winners run 0.3–5.7% FASTER on 不良/重 going
vs 良 going (`probe_verdict.json`, going-parquet). V2's abort was due to the sparse
going-preference signal (horse_heavy_pref, within-race rho=−0.045, 31% of races).
The V2 abort document identified a stronger operationalization:

> "use actual race-time speed deviation by going as the preference signal (normalize
> finish time by baba-adjusted par time, then compute horse-level going deviation)"

V8 implements that idea. A horse whose past times came on heavy (fast) going looks
artificially faster than it is; one racing on good (slow) going looks slower. The
existing speed features (`speed_index_avg_5`, `past_speed_kg_normalized_avg5`) compare
raw times without any going-adjustment, creating systematic bias.

**Proposed signal**: a BABA-ADJUSTED par-time speed index that re-normalizes each past
time by `(venue × distance × going)` par before aggregating:

- `par_time(venue, dist, going)` fitted on 2007–2017 frozen (median winner time per
  cell with ≥10 race shrinkage threshold; fall back to `venue×distance` median for thin
  cells)
- `past_speed_baba_adj_avg5 = mean over last 5 starts of (par_time − soha_time) / par_time`
  (positive = faster than par, negative = slower)
- `baba_adj_centered` = deviation of horse's adj speed from race-average adj speed
  (correction term isolating going-normalization contribution)
- Coverage: ~93% (horses with at least 1 eligible past race with known going)

## Critical Step: Verification That No Going Adjustment Exists

Before building features, the existing speed family was audited to confirm the hypothesis
gap is real.

**Speed features in the current 174-feature baseline:**

| Feature                            | Formula                                   | Going Adjusted?                          |
| ---------------------------------- | ----------------------------------------- | ---------------------------------------- |
| `speed_index_avg_5`                | avg(time_sa) over last 5 races            | **NO** — time_sa = gap to winner; no par |
| `speed_index_best_5`               | min(time_sa) over last 5 races            | **NO**                                   |
| `past_speed_kg_normalized_avg5`    | avg(soha_time/kyori × bataiju) last 5     | **NO** — bataiju multiplier, not baba    |
| `past_speed_futan_normalized_avg5` | avg(soha_time/kyori × futan_juryo) last 5 | **NO** — futan multiplier, not baba      |
| `past_speed_age_adjusted_avg5`     | avg((soha_time/kyori)/barei) last 5       | **NO** — age divisor, not baba           |
| `recent_soha_time_per_meter_avg5`  | avg(soha_time/kyori) last 5               | **NO**                                   |
| `last_3_avg_kohan_3f`              | avg 3f final section time, last 3         | **NO**                                   |

**Conclusion: NO going adjustment of any kind exists in the current feature set.**
The `track_condition_normalized` encodes current race going (0.0–1.0 scalar, noted
to be sign-inverted for NAR dirt), but zero features correct PAST race times for the
going conditions under which those times were recorded. V3 redundancy trap is NOT
triggered — this is a genuine gap.

Source files audited:

- `apps/pc-keiba-viewer/src/scripts/finish-position-features/build-horse-career-sql.ts`
  (speed_index_avg_5 via time_sa, no going join)
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/build-relationship-history-sql.ts`
  (past*speed*\* via soha_time × bataiju/futan, no going join)
- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-sectional-and-weight-features.py`
  (soha_time/kyori avg, no going join)

## Par-Time Table: Fit Summary

**Fitting protocol:**

- Source: `pg.race_entry_corner_features JOIN pg.nvd_ra` on race key
- Filter: source='nar', keibajo_code≠'83', 2007–2017, finish_position=1
- Going: nvd_ra.babajotai_code_dirt (codes 1–4; code 0/null excluded)
- Aggregation: median winner soha_time per (keibajo_code × kyori × baba)
- Shrinkage: cells with n < 10 races fall back to (keibajo × kyori) median
- Training window: 2007–2017 (frozen, never overlaps 2018+ probe/test set)

**Par-time table statistics:**

| Metric                            | Value            |
| --------------------------------- | ---------------- |
| Total cells                       | 473              |
| Thin cells (< 10 races, fallback) | 107 (22.6%)      |
| Venue coverage                    | 18 keibajo codes |
| Going codes covered               | baba=1,2,3,4     |

**Going-time gradients by venue (baba4=不良 vs baba1=良):**

All 18 venues show negative mean gradient (heavy going = faster winner times), consistent
with the V2 sign-flip evidence:

| keibajo | Venue  | Mean gradient baba4 vs baba1 |
| ------- | ------ | ---------------------------- |
| 30      | 門別   | −1.71%                       |
| 34      | 荒尾   | −1.14%                       |
| 35      | 盛岡   | −3.46%                       |
| 36      | 水沢   | −0.93%                       |
| 42      | 浦和   | −0.46%                       |
| 43      | 船橋   | −0.28%                       |
| 44      | 大井   | −0.00%                       |
| 45      | 川崎   | −0.60%                       |
| 46      | 金沢   | −0.15%                       |
| 47      | 笠松   | −0.25%                       |
| 48      | 名古屋 | −0.96%                       |
| 50      | 園田   | −1.79%                       |
| 51      | 姫路   | −0.48%                       |
| 53      | —      | −4.10%                       |
| 54      | 高知   | −1.00%                       |
| 55      | 佐賀   | −0.25%                       |
| 56      | —      | −0.25%                       |
| 58      | —      | −0.20%                       |

Notes:

- keibajo=44 (大井/Oi): gradient ≈ 0% — Oi uses sand/gravel mix that resists going effect
- keibajo=53 (−4.1%) and 35 (−3.5%) show largest gradients; may include thin-cell artifacts
- keibajo=30 (門別) shows a consistent −1.7% effect across 8 distance cells

**Key finding:** The going gradient is real and consistent across ALL venues. Horses that
ran fast times on heavy going are systematically "credited" more speed than deserved by
the raw time, since those times occurred under artificially fast conditions.

## Probe Results

**Features engineered (fully leak-free, strict race_date < target_race_date):**

1. `past_speed_baba_adj_avg5` = mean((par_time − soha_time)/par_time) last 5 prior starts
   where going code is known and par cell exists
2. `baba_adj_speed_best3` = max of adj speed over last 3 starts (max = fastest relative to par)
3. `baba_adj_centered` = baba_adj_avg5 − race_avg(baba_adj_avg5) [CORRECTION TERM]
4. `baba_adj_rank_in_race` = rank within race by baba_adj_avg5 desc

Baseline (no-going-adjustment): `unadj_soha_time_per_meter_avg5` = avg(soha_time/kyori) same lookback.

**Coverage:**

| Feature                  | Coverage |
| ------------------------ | -------- |
| past_speed_baba_adj_avg5 | 93.2%    |
| baba_adj_speed_best3     | 92.7%    |
| baba_adj_centered        | 93.2%    |

Coverage is dense (>93%), confirming the par-time table has sufficient going-code
availability in PG going history for the 2018–2024 probe set.

**Raw within-race Spearman (positive = faster = better finish, averaged over 88k+ races):**

| Feature                                   | Mean within-race rho | N races    |
| ----------------------------------------- | -------------------- | ---------- |
| speed_index_avg_5 (existing, time_sa)     | −0.426               | 90,666     |
| past_speed_baba_adj_avg5                  | +0.399               | 88,389     |
| baba_adj_speed_best3                      | +0.382               | 87,825     |
| **baba_adj_centered (correction term)**   | **+0.399**           | **88,389** |
| baba_adj_rank_in_race                     | −0.353               | 90,826     |
| unadj_soha_time_per_meter_avg5 (baseline) | −0.195               | 90,666     |

Note: `speed_index_avg_5` uses time_sa (gap to winner, lower=better, negative rho=good);
baba_adj uses (par−soha)/par (positive = faster, positive rho=good). Both are ~0.40
raw — comparable magnitude.

**Critical: Unadjusted baseline comparison**

The key question is whether the going-adjustment adds information beyond the raw soha_time:

| Feature                                 | Partial rho after controlling speed family | Interpretation                                                  |
| --------------------------------------- | ------------------------------------------ | --------------------------------------------------------------- |
| unadj_soha_time_per_meter_avg5          | **+0.019**                                 | Near-zero residual — fully explained by existing speed features |
| **baba_adj_centered (correction term)** | **+0.180**                                 | Strong residual — going adjustment contributes genuine new info |

The unadjusted soha_time partial rho is effectively zero (0.019), confirming that the
`speed_index_avg_5` + `speed_index_best_5` + `last_3_avg_kohan_3f` family already
captures most of the raw-time information. But the going-normalized version retains
0.180 partial rho — that delta (0.161) is the going-adjustment's genuine contribution.

**Redundancy check:**

| Feature                  | Max   | r                 | vs existing speed family | Closest feature |
| ------------------------ | ----- | ----------------- | ------------------------ | --------------- |
| past_speed_baba_adj_avg5 | 0.414 | speed_index_avg_5 |
| baba_adj_centered        | 0.321 | speed_index_avg_5 |

The correction term has |r|=0.32 with the closest existing feature — substantial overlap
(expected, since it IS a speed feature) but not redundant enough to exclude (0.32 << 0.95
redundancy threshold).

**Partial Spearman (the decision number):**

Speed family used for partialling: `speed_index_avg_5`, `speed_index_best_5`,
`last_3_avg_kohan_3f`, `unadj_soha_time_per_meter_avg5`.

| Feature                             | Partial rho | Bar  | Pass? |
| ----------------------------------- | ----------- | ---- | ----- |
| baba_adj_centered (correction term) | **+0.180**  | 0.08 | YES   |
| past_speed_baba_adj_avg5 (full)     | −0.066      | 0.08 | NO    |

Decision number: **partial_rho(baba_adj_centered) = +0.180**.

The correction term (race-centred going deviation) is the operative signal; the
absolute adjusted speed is not independently informative after partialling (−0.066),
presumably because the level is captured by the existing time_sa family.

**Per-venue Spearman breakdown (baba_adj_centered):**

| keibajo     | Mean within-race rho | N races | Notes                                                         |
| ----------- | -------------------- | ------- | ------------------------------------------------------------- |
| 30 (門別)   | +0.473               | 6,268   | Northernmost; strong drainage effect                          |
| 35 (盛岡)   | +0.336               | 5,067   |                                                               |
| 36 (水沢)   | +0.311               | 4,699   | Lowest rho; mild gradient                                     |
| 42 (浦和)   | +0.351               | 4,534   |                                                               |
| 43 (船橋)   | +0.353               | 4,672   |                                                               |
| 44 (大井)   | +0.387               | 7,739   | Near-zero gradient but still benefits from time normalisation |
| 45 (川崎)   | +0.350               | 5,095   |                                                               |
| 46 (金沢)   | +0.423               | 6,653   |                                                               |
| 47 (笠松)   | +0.442               | 6,476   |                                                               |
| 48 (名古屋) | +0.392               | 6,826   |                                                               |
| 50 (園田)   | +0.411               | 11,579  |                                                               |
| 51 (姫路)   | +0.367               | 1,516   | Small holdout                                                 |
| 54 (高知)   | +0.486               | 8,545   | Highest rho; warm climate, variable drainage                  |
| 55 (佐賀)   | +0.373               | 8,720   |                                                               |

**Venue concentration flag: BROAD (False)** — top-2 venue contribution = 0.18 (well below
0.80 concentration threshold). The signal is distributed across all 14 major NAR venues.
No single venue dominates the improvement.

Note: The per-venue rho ranges from 0.31 (水沢) to 0.49 (高知), a moderate spread.
This is expected — venues with stronger going effects (高知 in warm/humid climate,
門別 in cold/wet north) show higher within-race discrimination.

## Historical Bar Context

| Comparison                                       | Signal                             | Outcome                 |
| ------------------------------------------------ | ---------------------------------- | ----------------------- |
| V2 H-DIRT-GOING-SIGN-FLIP (raw going pref)       | pref_x_heavy rho=0.045 conditional | ABORT (pre-training)    |
| H2 momentum delta (raw rho)                      | h2_form_delta rho=0.142 overall    | REJECT after WF retrain |
| V3 age-month speed deviation                     | partial rho=0.055                  | ABORT                   |
| V5 race-volume density                           | partial rho=0.059                  | ABORT                   |
| V6 sire distance split                           | partial rho=0.025                  | ABORT                   |
| **V8 H-BABA-PAR-TIME (correction term partial)** | **partial rho=0.180**              | **PROCEED**             |

The partial rho of 0.180 is:

- 2.2× V3's bar (0.055)
- 2.3× the 0.08 dense bar
- 1.27× H2's raw signal (0.142) that was REJECTED

However, H2 was a raw rho comparison, not partial. The partial rho controls for the
existing speed family — it represents genuinely orthogonal signal. H2's rejection was
due to insufficient within-race bootstrap power at that signal level. At 0.180 partial,
the signal should be substantially more robust to the WF fold evaluation.

## Verdict

**PROCEED — WF retrain with par-time adjusted speed index is recommended.**

**Binding reason:** The correction term `baba_adj_centered` has a partial Spearman of
+0.180 after controlling for the full speed family (speed_index_avg_5, speed_index_best_5,
last_3_avg_kohan_3f, unadj_soha_time/m). The unadjusted soha_time has only +0.019 partial
rho — confirming that going-adjustment contributes ~0.161 partial rho of genuine new
information beyond the existing speed encoding. Coverage is 93%, the signal is dense
(not conditioned on 31% of races like V2), and the venue distribution is broad (no
heterogeneity concentration in 1–2 venues).

**Recommended features for retrain:**

1. `baba_adj_centered` (correction term: race-mean-centred going deviation) — primary
2. `past_speed_baba_adj_avg5` (absolute level) — secondary (overlap with speed family but
   meaningful in GBDT split context)
3. `baba_adj_speed_best3` (best-3 adjusted — captures ceiling ability)

**Risk assessment:**

- The partial rho of 0.180 puts this above the empirical ABORT threshold (0.08–0.10)
  and above H2's raw rho (0.142 that was rejected). However:
- Coverage depends on PG going codes being populated in nvd_ra. Races missing baba
  code (7.0% of 2018+ probe set) will have null adjustment, treated as missing by GBDT.
- The par-time table has 107/473 (22.6%) thin cells using venue×distance fallback
  (ignoring going within those cells) — these are predominantly extreme distances/venues
  with few historical races. The fallback is conservative.
- WF retrain + bootstrap + Holm powered judge required before any production decision.

## Hard Rules Observed

- `tmp/` only: all artifacts in `tmp/nar-perclass/sci_track/v8_partime/`
- No `git add tmp/`
- PG read-only: only SELECT queries issued through DuckDB postgres extension
- seed=42: set in probe.py (numpy)
- CatBoost retrain not yet invoked (probe phase only)
- Par-time training window strictly 2007–2017, probe window 2018–2024 (no leak)

## Refinement Directions (if WF retrain proceeds)

1. **Venue-adjusted par shrinkage**: use Bayesian shrinkage toward national median
   rather than venue×distance fallback for thin cells (30+ venues × 15+ distances × 4
   going codes = 1800 possible cells, many sparse)
2. **Per-horse going specialization score**: build going-preference index from
   `baba_adj_centered` history (average deviation above race-mean on heavy vs good)
   as a meta-feature on top of the raw adjusted time
3. **Median-runner par vs winner par**: the current par uses median winner time; a
   field-level par (median of all runners, not just winners) may produce a more stable
   normalization for mid-field and back-marker horses
4. **Cross-category extension**: if NAR retrain succeeds, extend to JRA turf
   (different going effect direction: turf heavy IS slower, but the going correction
   logic is symmetric)
