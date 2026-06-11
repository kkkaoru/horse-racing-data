# Off-the-Card Data Feasibility — 2026-06-11

**Scope**: Off-card signal candidates for finish-position accuracy improvement.  
**Method**: PG read-only schema audit (127 tables, 15432 port) + feature pipeline review + web research.  
**Investigator**: Feasibility SubAgent (claude-sonnet-4-6)

---

## Executive Summary

| Rank | Domain                          | In PG?          | History | JRA cov     | NAR cov | Ban-ei cov | Signal  | Feasibility | Decision    |
| ---- | ------------------------------- | --------------- | ------- | ----------- | ------- | ---------- | ------- | ----------- | ----------- |
| 1    | **調教タイム `jvd_hc`**         | YES             | 23yr    | ~100%       | ~42%    | ~0%        | HIGH    | HIGH        | **GO**      |
| 2    | **騎手×調教師コンボ**           | YES (derivable) | 23yr    | ~100%       | ~100%   | ~100%      | MEDIUM  | HIGH        | **GO**      |
| 3    | **血統深度拡張 (grandsire)**    | YES             | 23yr    | ~100%       | ~100%   | ~100%      | LOW-MED | HIGH        | **COND GO** |
| 4    | **jvd_wc 長距離調教**           | YES             | 5yr     | ~70%        | ~0%     | ~0%        | LOW-MED | MED         | **COND GO** |
| 5    | **JRA yoso speed figure**       | YES             | 23yr    | ~40%        | 0%      | 0%         | LOW-MED | HIGH        | **COND GO** |
| 6    | **含水率/クッション値**         | NO              | 8yr     | JRA only    | 0%      | 0%         | LOW     | LOW-MED     | **NO-GO**   |
| 7    | **パドック/馬体コンディション** | NO              | 0yr     | scrape only | minimal | minimal    | UNKNOWN | VERY LOW    | **NO-GO**   |

**Highest expected-signal × feasibility candidate**: 調教タイム `jvd_hc` — data already warehoused (11.8M rows), implementation script already written, zero ingestion cost, completely absent from current 103-feature model.

---

## Domain 1: 調教 / Workout Data (jvd_hc + jvd_wc)

### Situation in PG

Two workout tables exist in PG:

**`jvd_hc`** — short-distance training sectionals (4F, 3F, 2F, 1F lap/cumulative times):

- Rows: **11,807,140**
- Date range: 20030102 – 20260609 (23 years)
- `tracen_kubun`: `0` = Ritto (West), `1` = Miho (East) (Zed tracen)
- Format: zero-padded varchar, e.g. `lap_time_1f = '166'` = 16.6s

**`jvd_wc`** — extended workout sectionals (10F, 9F, 8F, 7F, 6F, 5F):

- Rows: **740,838**
- Date range: 20210727 – 20260609 (5 years only)
- 13,310 unique horses with data in 2025

### Coverage

| Category | `jvd_hc` coverage                                                              |
| -------- | ------------------------------------------------------------------------------ |
| JRA      | ~100% (all JRA horses train at JRA-managed tracen)                             |
| NAR      | ~42% (14,532 / 34,793 horses since 2022 — NAR horses that train at JRA tracen) |
| Ban-ei   | ~0% (Obihiro trains independently, no JVD feed)                                |

No NVD equivalent to `jvd_hc` exists. NAR has no dedicated workout record in the NVD feed.

### Feature Script Status

`add-workout-features.py` exists at:  
`apps/pc-keiba-viewer/src/scripts/finish-position-features/add-workout-features.py`

It is **complete and tested** but **NOT integrated** into the main `finish_position_features_duckdb.py` pipeline. The script computes:

- `workout_lap_1f_avg5`, `workout_lap_3f_avg5`, `workout_lap_4f_avg5`
- `workout_gokei_4f_avg5`, `workout_gokei_3f_avg5`
- `workout_lap_1f_best5`, `workout_lap_3f_best5`
- `workout_count_recent` (last 10 workouts), `workout_count_30d`
- `days_since_last_workout`
- `workout_pace_progression` (gokei_4f − lap_1f = stamina/finish indicator)

### Assessment

- **Horse-level, within-race-varying**: YES — each horse has unique workout history
- **Orthogonal to existing features**: YES — zero overlap with current 103-feature set
- **Ingestion effort**: NONE (already in PG)
- **Integration effort**: LOW — post-processing call to existing script + include columns in training
- **V8 lesson**: Does NOT apply — workout features are horse-level, not race-level constants

**Decision: GO (Priority 1)**. The implementation gap between "script exists" and "used in training" is the lowest-effort high-signal fix available.

---

## Domain 2: 馬体 / Paddock / Body Condition

### What Exists in PG

- `jvd_se.bataiju` / `nvd_se.bataiju`: horse weight (kg), already in features as `weight_avg_5` / `weight_diff_from_avg`
- `jvd_se.zogen_fugo` / `zogen_sa`: weight change sign and magnitude — already used
- `jvd_wh.bataiju_joho_01..18`: pre-race weight announcement per horse (realtime feed) — 0 rows (realtime only, not persisted)

### What Is Missing

No structured paddock condition (coat quality, muscle tone, sweat, walking score) data exists anywhere in the JVD/NVD feed. JRA publishes paddock video but no structured scores. Netkeiba.com has crowd-sourced paddock comments (unstructured text). Commercial services (Keibalab, Rakuten Keiba) have paddock observer ratings but require paid scraping.

**Decision: NO-GO** — infrastructure infeasible; bataiju already covered.

---

## Domain 3: 厩舎/騎手 Microdata — Jockey-Trainer Combo

### Current State

Existing features include `jockey_career_win_rate`, `jockey_horse_pair_win_rate`, `trainer_career_win_rate`, `trainer_horse_win_rate`, jockey running-style rates, trainer grade/hiraba affinity. **Missing: `trainer_jockey_combo_win_rate`** — the pair win rate for a specific jockey × trainer combination.

### Data Availability

`jvd_se` and `nvd_se` both have `kishu_code` + `chokyoshi_code` per entry, enabling full historical pair statistics. Example top JRA pair (since 2020): kishu `01088` × chokyo `01137` — 504 races, 158 wins = 31.3% win rate. NAR also fully available.

### Why This Adds Signal

The combo rate is not simply `jockey_win_rate × trainer_win_rate` — it captures the systematic synergy of a specific partnership (specific training style, communication patterns, horse selection). Literature from UK/AU horse racing ML (e.g., geegeez.co.uk metrics, EquinEdge partnership stats) consistently finds this adds marginal signal beyond individual rates.

**Decision: GO (Priority 2)** — zero ingestion cost, new SQL builder needed, 23yr history, all 3 categories.

---

## Domain 4: 天候/馬場含水率/クッション値

### JVD Availability

**含水率 and クッション値 are NOT available via JV-Link/JVD** (confirmed: no record type covers these; search of all 127 PG table columns for `gansui`, `cushion`, `moisture`, `fukumi` returns 0 results).

JRA publishes these on [jra.go.jp/keiba/baba/archive/](https://www.jra.go.jp/keiba/baba/archive/) as PDFs and HTML (含水率 from 2018-07-27, クッション値 from 2020-09-11). Third-party CSV compilations exist (e.g., ヨニゲ競馬倶楽部 on note.com).

### V8 Lesson Assessment

Applying the V8 lesson directly: moisture% and cushion value are **race-level constants** — every horse in the same race gets the same value. This means:

1. The signal is identical to existing `track_condition_normalized` (babajotai_code) in terms of what it varies on: race × venue × date.
2. The finer scale (float vs 4-category code) could in principle help, but in practice the babajotai_code already captures the 4-band going information that drives the main horse-selection effect.
3. The V8 baba-par-time experiment confirmed this: probe partial rho was 0.180 >> baseline 0.08 (PROCEED), yet it was ultimately race-level selection bias not horse-level ability — same pattern applies here.

**Decision: NO-GO** — race-level constant, short history (8yr), JRA-only, scraping required, marginal over existing `track_condition_normalized`.

### Nuance: Interaction Terms

The one valid use case: `sire × current_moisture_pct` interaction (e.g., certain sire lines outperform on soft going at fine-grained moisture levels). This is horse-level (different sires, different horses). Revisit only after grandsire × baba affinity (domain 3) is validated. Not a first-priority candidate.

---

## Domain 5: 血統深度 — Pedigree Depth

### Current State

`jvd_um.ketto_joho_01b` = sire (used), `ketto_joho_05b` = damsire (used).  
**NOT used**: `ketto_joho_03b` (paternal grandsire), `ketto_joho_04b` (paternal granddam's sire), `ketto_joho_07b`–`14b` (gen3).

### Coverage

`jvd_um` has 14 ancestor fields (3 complete generations), all **100% populated** (212,988 horses). `nvd_um` has the same schema (29 ketto columns, 100% coverage for NAR horses).

### What Could Be Added

1. **Paternal grandsire stats** (`ketto_joho_03b`): win rate by distance/track for the father's father — captures male-line affinity not captured by sire alone.
2. **Gen3 stats** (`ketto_joho_07b`–`14b`): 8 ancestors at depth-3, useful for distance/surface aptitude of the maternal side.
3. **Inbreeding coefficient**: requires cross-referencing repeated ancestor codes across all 14 fields — computable in DuckDB but complex.

### Assessment

Marginal gain is likely small. Current `pedigree_score_for_race` already aggregates sire + damsire monthly stats. Gen3 is more diluted. Inbreeding coefficient is theoretically motivated but computationally complex and literature evidence for horse racing specifically is weak.

**Decision: CONDITIONAL GO** — paternal grandsire rate (`ketto_joho_03b × distance_band`) is worth testing as it is entirely zero-effort (data in PG, same pattern as sire stats). Full inbreeding coefficient is deprioritized.

---

## Domain 6: Other High-Value Unused Source Fields

### JRA yoso_soha_time / yoso_juni (in jvd_se, unused)

`jvd_se` contains `yoso_soha_time` (JRA DataLab predicted finish time in 1/100s) and `yoso_juni` (predicted rank 1–18) for every JRA entry since 2002. **1,188,363 non-zero prediction rows**.

Statistics:

- `yoso_juni` top-1 win rate: 23.1% (vs popularity #1 = 39.2% → JRA prediction weaker than market)
- Pearson r(yoso_juni, tansho_ninkijun) = 0.687 → ~53% residual variance = partial orthogonal signal
- **NOT available for NAR** (nvd_se.yoso_juni always '00')

This is a JRA-internal speed figure model output, pre-race, horse-level. The rank within race and deviation from average could contribute marginally. But since `odds_score` and `popularity_score` already encode much of the market consensus, the incremental gain is likely small.

**Decision: CONDITIONAL GO (low priority, JRA only)** — zero ingestion effort; compute `yoso_rank_in_race` and `yoso_time_diff_from_avg` as rank-within-race features; test in JRA ablation.

### Blinker First-Time / Equipment Change (jvd_se.blinker_shiyo_kubun, unused)

`blinker_shiyo_kubun = 1` for ~5% of entries (143,201 of 2,858,667). Blinker use is correlated with trainer intervention, but the signal is binary and sparse. Worth including as a categorical feature but expected marginal gain is low.

### Official Running Style (jvd_se.kyakushitsu_hantei, partially used)

`kyakushitsu_hantei` (JRA's labeled running style: 0=unknown, 1=nige, 2=senkou, 3=sashi, 4=oikomi) is currently only used to evaluate our running-style model accuracy, not as a direct feature. The agree/disagree between JRA's label and our model prediction could be a derived feature for the finish-position model.

---

## Ranked Go/No-Go Table

| #   | Domain                                      | Decision    | Expected Signal | Effort    | Notes                                              |
| --- | ------------------------------------------- | ----------- | --------------- | --------- | -------------------------------------------------- |
| 1   | `jvd_hc` workout (4F/3F sectionals)         | **GO**      | HIGH            | ZERO      | Script exists, call as post-processor              |
| 2   | Jockey × trainer combo win rate             | **GO**      | MEDIUM          | LOW       | New SQL builder, 23yr, all categories              |
| 3   | Paternal grandsire (`ketto_joho_03b`) stats | **COND GO** | LOW-MED         | LOW       | Same pattern as sire, extend build-pedigree-sql.ts |
| 4   | `jra yoso_soha_time` rank feature           | **COND GO** | LOW-MED         | ZERO      | JRA only; test orthogonality to odds_score         |
| 5   | `jvd_wc` extended workout (5F+)             | **COND GO** | LOW-MED         | LOW       | 5yr history only; supplement to jvd_hc             |
| 6   | 含水率/クッション値                         | **NO-GO**   | LOW             | HIGH      | Race-level constant; not in JVD; scraping needed   |
| 7   | Paddock/body condition (unstructured)       | **NO-GO**   | UNKNOWN         | VERY HIGH | No structured data source                          |

---

## Highest Expected-Signal × Feasibility Candidate

**`jvd_hc` workout data (調教タイム)**

- Data: 11.8M rows, 23 years, already warehoused in PG
- Script: `add-workout-features.py` complete and verified
- Gap: not called from main pipeline + workout columns not in training feature set
- Coverage: JRA ~100%, NAR ~42%, Ban-ei ~0%
- Orthogonality: complete (zero overlap with current 103 features)
- Integration path: (1) add post-processing call in feature generation, (2) extend training feature lists, (3) full retrain → walk-forward eval

---

## Sources

- JRA 含水率アーカイブ: [https://www.jra.go.jp/keiba/baba/archive/](https://www.jra.go.jp/keiba/baba/archive/)
- 含水率基礎知識 JRA: [https://www.jra.go.jp/keiba/baba/moist/](https://www.jra.go.jp/keiba/baba/moist/)
- クッション値基礎知識 JRA: [https://www.jra.go.jp/keiba/baba/cushion/](https://www.jra.go.jp/keiba/baba/cushion/)
- クッション値 CSV (ヨニゲ競馬倶楽部): [https://note.com/pixykeiba/n/nef59b460677c](https://note.com/pixykeiba/n/nef59b460677c)
- netkeiba 馬場情報まとめ: [https://dir.netkeiba.com/keibamatome/special/babadata/index.html](https://dir.netkeiba.com/keibamatome/special/babadata/index.html)
- JRA-VAN Developer Community JV-Link: [https://developer.jra-van.jp/t/topic/49](https://developer.jra-van.jp/t/topic/49)
- JRA-VAN 調教タイム活用講座: [https://jra-van.jp/smartphone/howto/15.html](https://jra-van.jp/smartphone/howto/15.html)
- EquinEdge Jockey-Trainer Stats: [https://equinedge.com/metrics/jockey-trainer-stats](https://equinedge.com/metrics/jockey-trainer-stats)
- geegeez Trainer-Jockey Combo: [https://www.geegeez.co.uk/reports/trainer-jockey-combo-stats/](https://www.geegeez.co.uk/reports/trainer-jockey-combo-stats/)
- ABYSS競馬 クッション値ガイド: [https://abyss-keiba.com/cushion-value-guide/](https://abyss-keiba.com/cushion-value-guide/)
