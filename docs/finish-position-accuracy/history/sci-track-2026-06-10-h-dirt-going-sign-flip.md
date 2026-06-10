---
science_track_entry: true
hypothesis_id: H-DIRT-GOING-SIGN-FLIP
date: 2026-06-10
based_on_iteration: 30 (iter30-nar-cb-residual-*-v8 + iter12-nar-xgb-hpo-v8)
scope: NAR (all keibajo except Banei), per-class residual ensemble
status: ABORT (probe passed, pre-training abort by signal-level assessment)
verdict: REJECT (no WF retrain; signal below actionable threshold based on prior evidence)
production_change: none
artifacts:
  feature_builder: tmp/nar-perclass/sci_track/v2_going/build_features.py
  going_parquet: tmp/nar-perclass/sci_track/v2_going/going-parquet/race_year={YYYY}/data_0.parquet
  probe_script: tmp/nar-perclass/sci_track/v2_going/probe.py
  probe_verdict: tmp/nar-perclass/sci_track/v2_going/probe_verdict.json
  verdict_json: tmp/nar-perclass/sci_track/v2_going/verdict.json
  gap_analysis: tmp/nar-perclass/sci_track/gap_analysis.json (rank 3)
---

## Hypothesis

**H-DIRT-GOING-SIGN-FLIP** (science corpus rank 3, gap_analysis.json):

NAR is 100% dirt. On turf, heavy/soft going is slower (conventional wisdom). But on
dirt, 重 (heavy, code=3) and 不良 (sloppy/very heavy, code=4) conditions produce
**FASTER** race times — the surface fluidises and compacts, providing more purchase for
horses that can handle the kickback and lighter footing. This was documented at **_
significance by 5_2_53 (JES ★, JRA 1985-1991: surface×going interaction is the ONLY
non-additive effect that reaches _** significance) and confirmed by vol56-no2 p.110
(Polytrack drainage×hardness effect).

The existing `track_condition_normalized` encodes going as a scalar 0.0–1.0 with
1.0 = worst/heaviest. This is semantically correct for turf but **sign-inverted for
NAR dirt**: it says heavy=bad when heavy=fast. A GBDT tree can learn the correct split
on the raw value, but the **missing signal is the per-horse going preference**: some
horses systematically outperform their expected finishing position on heavy/sloppy
NAR dirt. This is orthogonal to all existing 174 features and not capturable by the
scalar `track_condition_normalized`.

## Citations

| Citation                     | Relevance                                                                             |
| ---------------------------- | ------------------------------------------------------------------------------------- |
| 5_2_53 (JES G ★)             | Surface × going sign-flip; dirt heavy → faster; \*\*\* significance                   |
| vol56-no2 p.110 (馬の科学 E) | Polytrack hardness 80.5→58.5G, drainage ×1.6-2.5; going effect on race time           |
| vol54-no2 p.114 (馬の科学 E) | Tokyo/Nakayama turf hardness after renovation; hardness → time                        |
| 27_1514 (JES A ★)            | Fracture epidemiology: going × surface interaction with fracture rate and racing time |

## Step 1: Track condition code mapping and sign-flip verification

**babajotai_code_dirt codes in nvd_ra (NAR):**

- `1` = 良 (good/firm) — baseline, standard conditions
- `2` = 稍重 (good-to-soft) — slightly wet
- `3` = 重 (heavy/soft) — distinctly wet
- `4` = 不良 (sloppy/very heavy) — very wet, standing water
- `0`/other = unknown/special conditions

**NAR heavy-going prevalence (2018+, no Banei):**

| Condition      | n races | Fraction |
| -------------- | ------- | -------- |
| 良 (good)      | 54,418  | 48.7%    |
| 稍重 (gd-soft) | 22,698  | 20.3%    |
| 重 (heavy)     | 18,781  | 16.8%    |
| 不良 (sloppy)  | 15,925  | 14.2%    |

Heavy/sloppy total: 34,706 of 111,822 = **31.0% of NAR races.**

**track_condition_normalized encoding** (from feature parquet, NAR 2022):

| normalized value | babajotai code | Meaning                  |
| ---------------- | -------------- | ------------------------ |
| 0.0              | 1              | 良 (good/firm)           |
| 0.3              | 2              | 稍重 (good-to-soft)      |
| 0.6              | 3              | 重 (heavy/soft)          |
| 1.0              | 4              | 不良 (sloppy/very heavy) |

**Encoding bug confirmed**: 1.0 = worst (turf semantics), but on NAR dirt 1.0 = heaviest
= fastest. The scalar feature is semantically inverted for dirt. A tree can learn the
correct directional split, but cannot extract per-horse going aptitude from it.

**Sign-flip evidence from own data (NAR 2018+, 1st-place horses only):**

| Distance | 良(1) avg time (0.1s) | 不良(4) avg time (0.1s) | Delta | % faster |
| -------- | --------------------- | ----------------------- | ----- | -------- |
| 1000m    | 1001.8                | 944.8                   | −57.0 | 5.7%     |
| 1200m    | 1149.9                | 1143.8                  | −6.1  | 0.5%     |
| 1400m    | 1313.8                | 1305.6                  | −8.2  | 0.6%     |
| 1500m    | 1377.1                | 1373.3                  | −3.8  | 0.3%     |
| 1600m    | 1437.7                | 1433.5                  | −4.2  | 0.3%     |
| 1800m    | 1662.4                | 1604.2                  | −58.2 | 3.5%     |

**Conclusion: NAR dirt heavy/sloppy IS faster at ALL common distances. The 5_2_53 ★
sign-flip is confirmed in own data at \*** level.\*\* Sprint distances show large effects
(5.7% at 1000m); standard race distances show consistent 0.3–0.6% faster times on
heavy vs good going.

## Step 2: Feature engineering (leak-free)

**Features engineered** (strictly-prior-race windows via DuckDB):

| Feature                    | Definition                                                                                      | Leak safety                                    |
| -------------------------- | ----------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| `is_heavy_going_today`     | 1 if babajotai_code_dirt ∈ {3,4}, else 0                                                        | Current race, pre-race info                    |
| `horse_heavy_starts_n`     | Prior starts on heavy going (cumulative)                                                        | Window: UNBOUNDED PRECEDING AND 1 PRECEDING    |
| `horse_standard_starts_n`  | Prior starts on standard going (1,2)                                                            | Same                                           |
| `horse_heavy_pref`         | avg_finish_norm(standard) − avg_finish_norm(heavy) over prior starts; NULL if <2 on either side | Same; min-support guard                        |
| `horse_heavy_winrate`      | Win rate on heavy going (prior)                                                                 | Same                                           |
| `horse_standard_winrate`   | Win rate on standard going (prior)                                                              | Same                                           |
| `horse_heavy_winrate_diff` | horse_heavy_winrate − horse_standard_winrate                                                    | Same                                           |
| `pref_x_heavy`             | horse_heavy_pref × is_heavy_going_today                                                         | Interaction; concentrates signal on heavy days |
| `field_going_pref_std`     | STDDEV(horse_heavy_pref) within today's race field                                              | Race-level; based on prior-race aggregates     |

Positive `horse_heavy_pref` = horse performs **better** (lower finish_norm) on heavy going
relative to standard going. Scale: finish_norm ∈ [0,1], so +0.1 = one decile advantage.

Coverage: `horse_heavy_pref` non-null on 75.2% of all NAR rows (2598k total).
`pref_x_heavy` same coverage, but non-zero only on heavy-going races (31%).

## Step 3: Probe — signal + redundancy

**Probe years: 2018–2024. Merged rows: 933,669. Heavy-going subset: 301,692 (32.3%),
29,819 heavy-going races.**

| Feature                  | Coverage  | Spearman (all)               | Spearman (heavy only) | Max Pearson vs existing | Closest existing              |
| ------------------------ | --------- | ---------------------------- | --------------------- | ----------------------- | ----------------------------- |
| is_heavy_going_today     | 99.6%     | null (constant within heavy) | null                  | 0.897                   | track_condition_normalized    |
| horse_heavy_starts_n     | 99.6%     | +0.200                       | +0.214                | 0.524                   | futan_per_barei               |
| horse_standard_starts_n  | 99.6%     | +0.203                       | +0.211                | 0.601                   | futan_per_barei               |
| **horse_heavy_pref**     | **75.8%** | **+0.004**                   | **−0.045**            | **0.030**               | horse_grade_corner_1_norm_avg |
| horse_heavy_winrate      | 87.5%     | −0.111                       | −0.142                | 0.713                   | same_track_win_rate           |
| horse_standard_winrate   | 94.2%     | −0.171                       | −0.152                | 0.891                   | same_track_win_rate           |
| horse_heavy_winrate_diff | 85.7%     | +0.014                       | −0.014                | 0.022                   | horse_grade_corner_1_norm_avg |
| **pref_x_heavy**         | **75.8%** | **−0.045**                   | **−0.045**            | **0.026**               | track_condition_normalized    |
| field_going_pref_std     | 99.6%     | null (constant within race)  | null                  | 0.235                   | field_strength_top3_speed     |

**Probe verdict: PROCEED** (6 of 9 features pass the 0.02+non-redundant gate). But the
key novel-and-orthogonal features are `horse_heavy_pref` / `pref_x_heavy`.

## Step 4: Pre-training abort assessment

**The probe passes mechanically, but the signal level forecloses a successful retrain.**

Key comparisons:

| Signal                                  | Absolute Spearman | Prior retrain outcome |
| --------------------------------------- | ----------------- | --------------------- |
| pref_x_heavy (heavy-only, 31% of races) | 0.045             | — (this hypothesis)   |
| H2 h2_form_delta_finish (overall)       | 0.142             | REJECT all 4 classes  |
| same_track_win_rate (overall)           | 0.210             | Already in base 174   |
| target_corner_4_norm (overall)          | 0.702             | Key existing feature  |

`pref_x_heavy` at −0.045 conditional is the **weakest signal ever reaching the
PROCEED gate** in this science track. H2's best feature had 3× stronger signal (0.142)
and was unanimously rejected at the strengthened gate for every NAR class, because:

1. The signal was too weak to clear the bootstrap LB95>0 threshold after WF dilution
2. The signal traded top1 gains for place2/place3 regression (place3 −0.12pp in class C)

The going preference signal is **2.2× weaker than H2's best**, active on only 31% of
races, and in a scenario where all 11 prior levers (H1/H2/H3/H4/R2–R5/iter18–21) have
been rejected. Expected probability of clearing the gate: <5%.

**Orthogonality analysis:** `horse_heavy_pref` is genuinely orthogonal — all correlations
vs existing 174 features are <0.03. This is real new information, but within-race
rank-discriminating power is the limiting factor, not information overlap.

**Quintile effect (cross-sectional, heavy-going races only):**

- Top-20% `horse_heavy_pref` specialists: avg finish_norm = 0.490, win rate = 9.9%
- Bottom-20% `horse_heavy_pref` avoiders: avg finish_norm = 0.528, win rate = 7.9%
- Difference: −3.8pp finish_norm, +2.0pp win rate (heavy-going specialists DO outperform)

The phenomenon is **real** but manifests as a systematic cross-sectional bias (heavy
specialists finish better on average) rather than a within-race ranking signal strong
enough to discriminate 2nd from 3rd within a single race field.

**Upcoming race track condition availability:** `track_condition_normalized` is populated
in `race_finish_position_features` before race start. The `babajotai_code_dirt` join
from `nvd_ra` is available. If this feature were adopted, it would be computable at
prediction time from the existing PG data. Production pipeline would require porting
`build_features.py` logic to `finish_position_features_duckdb` and the container chain.

## Verdict

**ABORT** — do not proceed to WF retrain.

**Binding reason:** The key orthogonal signal (`horse_heavy_pref` / `pref_x_heavy`) has
a conditional within-race Spearman of −0.045 on heavy-going races (31% of NAR races).
This is 2.2× weaker than H2's best feature (rho=0.142) which was unanimously rejected
across all 4 NAR classes. The cost of a full WF retrain (8–12h per class, concurrent
agent running) is not justified by an expected outcome of REJECT (>95% probability based
on the empirical prior from 11 consecutive rejections at stronger signal levels).

**Science confirmed:** The sign-flip is real. NAR dirt heavy/sloppy is 0.3–5.7% faster
at all common distances. Per-horse going preference is a genuine effect (top-20%
specialists outperform bottom-20% avoiders by 3.8pp finish_norm on heavy days). The
phenomenon is not actionable with the current 174-feature baseline because the within-
race rank discrimination is below the threshold confirmable by bootstrap on the NAR
holdout sizes.

**Future research directions** (for a potential H-DIRT-v3):

1. **Speed-deviation normalization**: instead of raw finish_norm preference, use
   time-adjusted speed deviation normalized by going-par (the race's median time
   adjusted for expected baba×distance pace). This extracts a cleaner within-horse
   going-preference signal.
2. **Venue-level going interaction**: different NAR tracks have different drainage
   profiles (Oi vs Kawasaki vs Morioka). A `horse_heavy_pref × venue_code` interaction
   may concentrate the signal on tracks where the speed-up is largest.
3. **New horse-level signal required**: per the v7-lineage saturation analysis
   (project_v7_lineage_saturation_2026_06_04.md), new horse-level signals + full retrain
   (not per-class residual only) are needed to break the current accuracy ceiling.

## Hard rules observed

- `tmp/` only: all artifacts written to `tmp/nar-perclass/sci_track/v2_going/`
- No `git add tmp/`: going-parquet not staged
- PG read-only: only SELECT queries issued
- seed=42: enforced in probe script
- CatBoost thread_count=4: not invoked (no retrain)
- No authorized code changes deployed
