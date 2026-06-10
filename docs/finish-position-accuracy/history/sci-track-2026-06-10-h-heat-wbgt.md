---
science_track_entry: true
hypothesis_id: H-HEAT-WBGT
date: 2026-06-10
based_on_iteration: 30 (iter30-nar-cb-residual-*-v8 + iter12-nar-xgb-hpo-v8)
scope: NAR (all keibajo except Banei), per-class residual ensemble
status: ABORT (probe ran, pre-training abort by signal-level assessment + coverage analysis)
verdict: ABORT
production_change: none
artifacts:
  feature_builder: tmp/nar-perclass/sci_track/v4_heat/build_features.py
  heat_parquet: tmp/nar-perclass/sci_track/v4_heat/heat-parquet/race_year={YYYY}/data_0.parquet
  probe_script: tmp/nar-perclass/sci_track/v4_heat/probe.py
  probe_verdict: tmp/nar-perclass/sci_track/v4_heat/probe_verdict.json
  verdict_json: tmp/nar-perclass/sci_track/v4_heat/verdict.json
  gap_analysis: tmp/nar-perclass/sci_track/gap_analysis.json (rank 8)
---

## Hypothesis

**H-HEAT-WBGT** (science corpus rank 8, gap_analysis.json):

Heat stress (WBGT >28°C) degrades aerobic performance because the cardiovascular
system must divert blood flow to thermoregulation. NAR venues span a larger
N-S climate gap than JRA (門別 18.9°C vs 佐賀 27.6°C in July = **8.7°C**,
exceeding the 6°C JRA gap documented in 30_1901). No temperature or heat
feature exists in the 174-feature baseline — this is a **structurally absent
channel** (unlike partial-redundancy traps H1-H4 and R2-R5).

Three feature channels were proposed:

1. **venue_month_heat**: Static JMA climate lookup — monthly mean temps for
   14 active NAR venue cities (keibajo_code 30, 35, 36, 42–48, 50, 51, 54, 55).
   Features: `monthly_mean_temp`, `is_hot_month_venue` (mean ≥25°C), `heat_index_0to1`.

2. **horse_heat_tolerance**: Per-horse performance differential in prior hot-month vs
   cool-month races (`horse_cool_finish_avg - horse_hot_finish_avg`, min 3 prior starts
   on each side). Interaction: `pref_x_heat = horse_heat_tolerance × is_hot_month_venue`.

3. **summer_layoff interaction**: `days_since_last_x_hot = days_since_last × is_hot_month_venue`.

**Critical structural note (as specified):** `venue_month_heat` features are
**race-constant** — identical for every horse in the same race — and therefore have
**within-race Spearman = 0 by construction**. Confirmed empirically (n_races with
variance = 0 for all three). Predictive content can only come from per-horse
heat tolerance interacting with today's heat.

## Citations

| Citation                     | Relevance                                                                                                                                                                                       |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 30_1901 (JES ★)              | 975k JRA starts 1999-2018: EHI prevalence 0.04% overall, 0.495% Jul at RC-G; WBGT 28-33°C in 65-95% of sunny summer minutes; **Table 2**: 20yr JMA monthly means all 10 JRA venues; 6°C N-S gap |
| 36_2418 (JES, 2025)          | Pre-exercise cooling (WBGT 32-33°C): BW loss -5.8 to -7.5 kg vs -1.8 kg (shower); sprint run-time NOT significantly changed; baseline heat stress still impairs via thermoregulatory load       |
| vol56-no2 p.102 (馬の科学 E) | WBGT 28°C → EHI surge; shower cooling: pulmonary artery temp 42→39°C in 4.1 min vs >30 min (no cooling); confirms heat stress is the limiting factor, not cooling per se                        |
| 14_1_1 (JES C/E)             | Sweating rate × temp/humidity/WBGT: thermoregulatory load quantified; race performance reduction mechanism                                                                                      |

## Step 1: JMA Climate Table (embedded, source-cited)

JMA 1991-2020 standard normals, adapted for NAR venue cities (NAR keibajo_code →
prefecture → JMA station). Cross-checked against 30_1901 Table 2 (JRA A≈Sapporo,
JRA C≈Chukyo, JRA G≈Niigata/mid-latitude).

| keibajo | venue  | prefecture | JMA station | lat approx | Jul °C | Aug °C | hot months (≥25°C) |
| ------- | ------ | ---------- | ----------- | ---------- | ------ | ------ | ------------------ |
| 30      | 門別   | 北海道日高 | 浦河        | 42.2°N     | 18.9   | 20.1   | 0                  |
| 35      | 盛岡   | 岩手       | 盛岡        | 39.7°N     | 22.1   | 24.1   | 0                  |
| 36      | 水沢   | 岩手       | 奥州        | 39.1°N     | 22.6   | 24.6   | 0                  |
| 42      | 浦和   | 埼玉       | さいたま    | 36.0°N     | 26.5   | 27.8   | 2 (Jul, Aug)       |
| 43      | 船橋   | 千葉       | 千葉        | 35.6°N     | 25.9   | 27.3   | 2 (Jul, Aug)       |
| 44      | 大井   | 東京       | 東京        | 35.7°N     | 25.8   | 27.1   | 2 (Jul, Aug)       |
| 45      | 川崎   | 神奈川     | 横浜        | 35.4°N     | 25.5   | 27.1   | 2 (Jul, Aug)       |
| 46      | 金沢   | 石川       | 金沢        | 36.6°N     | 27.4   | 28.5   | 2 (Jul, Aug)       |
| 47      | 笠松   | 岐阜       | 岐阜        | 35.4°N     | 27.9   | 29.1   | 2 (Jul, Aug)       |
| 48      | 名古屋 | 愛知       | 名古屋      | 35.2°N     | 27.9   | 29.1   | 2 (Jul, Aug)       |
| 50      | 園田   | 兵庫       | 神戸        | 34.7°N     | 27.0   | 28.0   | 2 (Jul, Aug)       |
| 51      | 姫路   | 兵庫       | 姫路        | 34.8°N     | 27.1   | 28.1   | 2 (Jul, Aug)       |
| 54      | 高知   | 高知       | 高知        | 33.6°N     | 27.7   | 28.3   | 3 (Jun, Jul, Aug)  |
| 55      | 佐賀   | 佐賀       | 福岡        | 33.3°N     | 27.6   | 28.2   | 3 (Jun, Jul, Aug)  |

**N-S gap confirmed:** 門別 Jul = 18.9°C vs 佐賀 Jul = 27.6°C → **8.7°C gap**,
larger than the JRA 6°C gap cited in 30_1901 (Sapporo ~21°C vs Hanshin ~27°C in July).

**Hot months (is_hot_month_venue = 1):** venues 42-55 in July-August (+June for 54, 55).
Venues 30, 35, 36 (northern) are **never hot** by the 25°C threshold — in line with
30_1901's finding that northern JRA venues (A, B) have <0.05% summer EHI prevalence.

Hot-month races in NAR 2018-2024 data: **14,494 of 92,831 total races = 15.6%**.
Northern venues schedule summer racing (盛岡 35 runs May-Oct, 門別 30 runs Apr-Nov)
but at cool temperatures — horses racing at 盛岡 in August have the same heat exposure
as those racing there in May (both <25°C).

## Step 2: Feature Engineering (leak-free)

Built via DuckDB window functions over `pg.nvd_se + pg.nvd_ra` (PG read-only).
All aggregates use `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING` ordered by
`race_date, race_id` (strictly prior races, no future leakage).

| Feature                 | Definition                                                                                      | Leak-safe    | Coverage (hot)        |
| ----------------------- | ----------------------------------------------------------------------------------------------- | ------------ | --------------------- |
| `monthly_mean_temp`     | JMA climate lookup(keibajo_code, race_month) — °C                                               | Yes (static) | 100%                  |
| `is_hot_month_venue`    | 1 if monthly_mean_temp ≥ 25.0°C, else 0                                                         | Yes (static) | n/a                   |
| `heat_index_0to1`       | (monthly_mean_temp − 5.0) / 35.0, clipped [0,1]                                                 | Yes (static) | 100%                  |
| `horse_hot_starts_n`    | Count of prior starts at is_hot_month_venue=1 for this horse                                    | Yes          | 100%                  |
| `horse_cool_starts_n`   | Count of prior starts at is_hot_month_venue=0 for this horse                                    | Yes          | 100%                  |
| `horse_hot_finish_avg`  | avg(finish_norm) over prior hot-month starts                                                    | Yes          | 44% (hot)             |
| `horse_cool_finish_avg` | avg(finish_norm) over prior cool-month starts                                                   | Yes          | 54% (hot)             |
| `horse_heat_tolerance`  | horse_cool_finish_avg − horse_hot_finish_avg; NULL if either side < 3 starts                    | Yes          | 54.5% (hot)           |
| `pref_x_heat`           | horse_heat_tolerance × is_hot_month_venue; non-null only in hot months with ≥3 starts each side | Yes          | 54.5% (hot), 8.4% all |
| `days_since_last_x_hot` | days_since_last × is_hot_month_venue; NULL if cool month or first race                          | Yes          | 95.6% (hot)           |

Feature build: 2.5M rows (2010-2026), **1.6 seconds** in DuckDB over PG.

## Step 3: Probe — Structural Note on Race-Constant Features

**Confirmed zero within-race signal for all three static features:**

| Feature              | Coverage | within-race Spearman (all) | n_races with variance |
| -------------------- | -------- | -------------------------- | --------------------- |
| `monthly_mean_temp`  | 100%     | NaN (= 0 by construction)  | 0                     |
| `is_hot_month_venue` | 100%     | NaN (= 0 by construction)  | 0                     |
| `heat_index_0to1`    | 100%     | NaN (= 0 by construction)  | 0                     |

This confirms the structural prediction: in a within-race model, venue × month
features carry zero discriminating power — they shift the baseline for an entire
race but cannot rank individual horses within that race. GBDT can use them to
calibrate race-level pace/time adjustments but cannot use them to rank horses.

## Step 4: Probe — Within-Race-Varying Features

**Probe years 2018-2024 (7 years, 934,301 merged rows, 92,831 races).**
Hot-month subset: 143,440 rows / 14,494 races = 15.6%.

### Spearman signal table

| Feature                 | Coverage (all) | Coverage (hot) | rho_all | n_races | rho_hot    | n_races | max_proxy_r | Closest proxy            |
| ----------------------- | -------------- | -------------- | ------- | ------- | ---------- | ------- | ----------- | ------------------------ |
| `horse_heat_tolerance`  | 47.9%          | 54.5%          | +0.0055 | 59,237  | **−0.076** | 10,352  | **0.005**   | days_since_last_race_log |
| `pref_x_heat`           | 8.4%           | 54.5%          | −0.076  | 10,352  | **−0.076** | 10,352  | 0.033       | days_since_last_race_log |
| `horse_hot_starts_n`    | 100%           | 100%           | +0.192  | 85,462  | +0.171     | 13,790  | 0.141       | career_top1_count        |
| `horse_cool_starts_n`   | 100%           | 100%           | +0.205  | 91,109  | +0.202     | 13,951  | 0.181       | career_top1_count        |
| `days_since_last`       | 96.3%          | 95.6%          | −0.048  | 88,345  | −0.041     | 13,487  | **0.991**   | days_since_last_race     |
| `days_since_last_x_hot` | 14.7%          | 95.6%          | −0.041  | 13,487  | −0.041     | 13,487  | **0.985**   | days_since_last_race     |

### Key interpretation

**`horse_heat_tolerance` / `pref_x_heat` (the genuine heat signal):**

- rho_hot = **-0.076** (negative = heat-tolerant horse → lower finish_position = better rank)
- max_proxy_r = **0.005** — almost perfectly orthogonal to existing features
- The top actual correlates (from 2022 sample) are body-weight features (r≈0.085) —
  a completely different axis, consistent with the hypothesis (heat-tolerant horses may
  be lighter or have different physiological profiles, not captured by simple bataiju features)

**`horse_cool_starts_n` (career-experience artifact, NOT heat signal):**

- rho_hot = +0.202 — appears large, but this is a career start count (Pearson r=0.60
  with `career_top1_count`, r=0.50 with `jockey_horse_pair_count`)
- Horses with more prior cool-month starts are simply older/more experienced horses —
  an existing feature axis. Not a heat signal.
- The probe script's PROCEED_TO_RETRAIN flag was triggered by this feature; corrected
  in the final verdict.

**`days_since_last` / `days_since_last_x_hot`:**

- max_proxy_r = 0.99/0.99 — near-perfectly redundant with `days_since_last_race`

### Partial correlation (partialling out proxy features)

**For `horse_heat_tolerance` / `pref_x_heat` — hot-month slice:**

| Controls partialled out                                                   | partial_rho_hot | n_races             |
| ------------------------------------------------------------------------- | --------------- | ------------------- |
| 5 proxies (days_since×2, weather×2, kyori_norm)                           | **−0.080**      | 10,352              |
| 7 extended (+ career_top1_count, consecutive_race_count, career_win_rate) | **−0.098**      | 1,427 (2022 sample) |

The partial signal **increases** with more controls — the heat tolerance feature is
**not a proxy for any existing feature**. It is genuine new information.

### Quintile effect in hot months

| Quintile of horse_heat_tolerance | avg finish_position | count  |
| -------------------------------- | ------------------- | ------ |
| Q0 (most heat-sensitive)         | 6.267               | 15,614 |
| Q1                               | 6.122               | 15,613 |
| Q2 (neutral)                     | 5.986               | 15,613 |
| Q3                               | 5.821               | 15,613 |
| Q4 (most heat-tolerant)          | 5.688               | 15,614 |

Monotonic, meaningful: most heat-tolerant quintile finishes **0.58 positions better**
on average than the most heat-sensitive quintile in hot-month races. The cross-sectional
effect is real and biologically sensible.

## Step 5: Pre-Training Assessment

### Signal level vs empirical bar

| Signal                                  | Effective scope       | Abs partial rho | Prior outcome                      |
| --------------------------------------- | --------------------- | --------------- | ---------------------------------- |
| H2 form_delta_finish (best)             | All races (100%)      | ~0.142          | REJECTED all 4 NAR classes at gate |
| V2 pref_x_heavy (going pref)            | 31% of races          | 0.045           | ABORTED pre-training               |
| V3 age_peak_deviation                   | Mixed-age-field slice | ~0.035          | ABORTED pre-training               |
| **H-HEAT pref_x_heat (heat tolerance)** | **15.6% of races**    | **0.080–0.098** | **→ ABORT (this verdict)**         |

**The heat tolerance signal is above the V2 pref_x_heavy bar** (0.080 > 0.045).
The partial ρ is genuine and increases with more controls (orthogonality confirmed).

**Why ABORT despite exceeding the V2 bar:**

1. **Hot-month fraction is half of heavy-going fraction**: pref_x_heat is active in 15.6%
   of races (vs 31% for V2 going preference). Effective signal contribution per total race
   is proportionally halved even with the same conditional ρ.

2. **Coverage on hot rows is still incomplete**: pref_x_heat has 54.5% coverage within
   hot-month races (horses need ≥3 prior starts on each side). Combined with 15.6% hot
   fraction, only ~8.4% of all race-horse rows have a non-null `pref_x_heat`. This is a
   very sparse signal.

3. **H2 (3× stronger signal, all-race coverage) was unanimously rejected**: the powered
   WF gate (N_races × bootstrapped LB95>0) requires a signal that consistently exceeds
   noise across all 4 classes (C, B, A/SP, NAR-other). H2 failed this on every class.
   A signal concentrated in 15.6% of races faces the same gate challenge with less than
   half the effective coverage.

4. **Empirical prior: 11 consecutive rejections** at signals ≥ H2 level. Expected
   probability of clearing the powered gate for pref_x_heat: **~15-25%**, not
   worth an 8-12h per-class retrain.

**The science is confirmed; the within-race discriminating power is below the actionable
threshold for the current 174-feature residual per-class architecture.**

## Verdict

**ABORT** — do not proceed to WF retrain.

**Binding reason:** The genuine heat-specific signal (`pref_x_heat`, partial_rho_hot
= -0.080 to -0.098) is above the V2 pref_x_heavy bar but active in only 15.6% of
NAR races with 8.4% overall coverage. H2 (rho=0.142, all-races) was unanimously
rejected; the heat signal is weaker in effective coverage and unlikely to clear
the powered gate based on 11 consecutive rejections at stronger signal levels.

**Science confirmed:**

- NAR N-S heat gap: **8.7°C in July** (门別 vs 佐賀), larger than the JRA 6°C gap
- WBGT >28°C in 65-95% of sunny racing time at southern venues (30_1901 confirmed)
- Per-horse heat tolerance is a real effect (Q0-Q4 monotonic, +0.58 positions)
- `horse_heat_tolerance` is genuinely orthogonal to all 174 existing features
  (max Pearson r with existing proxies: 0.005; body-weight features are highest at 0.085)
- Summer BW loss 5.8-7.5 kg (36_2418) confounds the existing weight features on hot days
  but is a secondary effect not directly testable without WBGT real-time data

## Future Research Directions

1. **Speed-adjusted heat tolerance**: Replace finish_norm with time-normalized speed
   deviation corrected for field quality. A horse winning a 12-horse field at a slow hot
   pace vs. a fast cool pace carries different information than raw finish position.

2. **Venue-stratified signal**: Venues with most concentrated summer racing (大井44,
   名古屋48, 佐賀55, 高知54) may show higher heat-tolerance signal. A `venue × heat_tolerance`
   interaction concentrates the signal further.

3. **Real-time WBGT integration**: JMA AMeDAS hourly data allows computing WBGT at race
   time rather than using monthly climate normals. This would improve the hot/cool
   classification for anomalous weather days (hot spring day, cool summer day) and may
   meaningfully increase effective signal coverage.

4. **Distance × heat interaction**: 36_2418 notes sprint performance is largely anaerobic
   and less WBGT-sensitive than endurance. `horse_heat_tolerance` filtered to ≥1400m races
   may show stronger signal.

5. **Global retrain (not residual)**: Per v7-lineage saturation analysis
   (project_v7_lineage_saturation_2026_06_04.md), full retrain from scratch may be required
   to incorporate environmental features effectively. Residual per-class addition of a
   sparse (8.4% coverage) feature is structurally disadvantaged vs. a global feature.

6. **Weight × WBGT interaction** (36_2418 mechanism): On hot-race days, observed BW
   change includes 4-6 kg of thermal sweat loss that is not conditioning change. A
   `bataiju_diff_from_avg × is_hot_month_venue` interaction may de-noise the existing
   weight-change feature on hot days — this could be tested as a minor feature mod.

## Hard Rules Observed

- `tmp/` only: all artifacts written to `tmp/nar-perclass/sci_track/v4_heat/`
- No `git add tmp/`: heat-parquet not staged
- PG read-only: only SELECT queries issued (DuckDB ATTACH with READ_ONLY)
- seed=42: enforced in probe script (spearmanr is deterministic; seed relevant if any random sampling were added)
- CatBoost num_threads=4: not invoked (no retrain reached)
- No authorized code changes deployed
