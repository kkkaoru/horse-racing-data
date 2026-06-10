---
science_track_entry: true
hypothesis_id: H-JOINT-WEAK-ORTHOGONAL
date: 2026-06-10
based_on_iteration: 30 (iter30-nar-cb-residual-*-v8 + iter12-nar-xgb-hpo-v8)
scope: NAR (all keibajo except Banei), per-class residual ensemble (C, B, other, A)
status: ABORT (joint probe failed; partial composite rho=0.005 << abort threshold=0.08)
verdict: ABORT (no WF retrain warranted)
production_change: none
artifacts:
  probe_script: tmp/nar-perclass/sci_track/v7_joint/joint_probe.py
  probe_verdict: tmp/nar-perclass/sci_track/v7_joint/probe_verdict.json
  verdict_json: tmp/nar-perclass/sci_track/v7_joint/verdict.json
  v1_parquets: tmp/nar-perclass/sci_track/v1_seasonal_bw/bw-parquet/race_year={YYYY}/data_0.parquet
  v2_parquets: tmp/nar-perclass/sci_track/v2_going/going-parquet/race_year={YYYY}/data_0.parquet
  v4_parquets: tmp/nar-perclass/sci_track/v4_heat/heat-parquet/race_year={YYYY}/data_0.parquet
  v5_parquets: tmp/nar-perclass/sci_track/v5_volume/vol-parquet/race_year={YYYY}/data_0.parquet
---

## Hypothesis

**H-JOINT-WEAK-ORTHOGONAL**: individually-weak but mechanistically-independent
science-backed signals, when combined in a single joint probe, may collectively
exceed the within-race rank-discriminating threshold where each alone could not.

### Scientific rationale

Five independent physiological/developmental mechanisms were hypothesised to act on
non-overlapping race subsets:

1. **Footing aptitude (V2)**: Per-horse going preference on dirt heavy/sloppy conditions.
   Mechanism: NAR dirt is faster on heavy going (sign-flip confirmed; 5_2_53 ★); horses
   that have previously performed better on heavy going carry a genuine physiological
   advantage that cannot be a proxy for general ability. Active on 31% of NAR races.

2. **Thermoregulation (V4)**: Per-horse heat tolerance differential (hot vs cool venues).
   Mechanism: WBGT > 28°C at southern NAR venues in July–September degrades aerobic
   performance (30_1901 ★, 36_2418). North-south venue temperature gap = 8.7°C (confirmed
   in own data). Active on 15.6% of NAR races.

3. **Conditioning/load (V5)**: races_in_90d — count of prior starts in 90-day rolling window.
   Mechanism: vol56-no4 p.372 ★ documents gastric-ulcer risk as a function of race density;
   adequate recent load = conditioned tendons = maintained performance. Full coverage.

4. **Sex / gelding physiology (V1)**: is_gelding, sex one-hots, gelding × high-BW interactions.
   Mechanism: gelding OR=3.09 vs female for SDFT tendinopathy (30_1909 ★); sex effects are
   genuinely new (sex code was completely absent from the 174-feature baseline).

5. **Seasonal body-weight deviation (V1)**: bataiju_seasonal_dev.
   Mechanism: male/gelding horses peak body weight in autumn–winter, trough in summer;
   fillies peak autumn, trough spring (~30kg growth through age 5). A horse below its
   sex × age × month seasonal norm is physiologically below expected peak.
   (vol56-no3 p.194 ★).

**Joint hypothesis logic**: because mechanisms 1–5 fire on different race subsets
(heavy-going days, hot months, any day, any race, any race), their combination
might fire on more races than any single mechanism and the Ridge composite would
aggregate the signals without redundancy. The marginal mechanisms have proven
orthogonal to the existing 174 features (max Pearson r < 0.03–0.04 for going/heat
preference features).

## Joint Feature Set

All 12 features were pre-computed and existed as parquets before this probe.
No new feature engineering was required.

| Feature                     | Source | Coverage (eval) | Individual raw rho | Mechanism                     |
| --------------------------- | ------ | --------------- | ------------------ | ----------------------------- |
| `horse_heavy_pref`          | V2     | 73.9%           | +0.013             | Going preference (overall)    |
| `pref_x_heavy`              | V2     | 73.9%           | −0.042             | Going pref × is heavy today   |
| `horse_heat_tolerance`      | V4     | 46.0%           | +0.009             | Heat tolerance (overall)      |
| `pref_x_heat`               | V4     | 7.9%            | −0.074             | Heat pref × hot month         |
| `races_in_90d`              | V5     | 100%            | +0.120             | Race density / conditioning   |
| `is_gelding`                | V1     | 100%            | −0.020             | Gelding flag                  |
| `sex_code_male`             | V1     | 100%            | −0.047             | Male flag                     |
| `sex_code_female`           | V1     | 100%            | +0.057             | Female flag                   |
| `is_gelding_high_bw`        | V1     | 100%            | −0.036             | Gelding × BW ≥ 470kg          |
| `gelding_bw_risk_composite` | V1     | 100%            | −0.038             | Gelding × heavy × weight drop |
| `gelding_bw_risk_score`     | V1     | 100%            | −0.036             | Gelding BW risk continuous    |
| `bataiju_seasonal_dev`      | V1     | 99.96%          | −0.094             | Seasonal BW deviation (kg)    |

All features were verified leak-free in their respective per-track builds:
all per-horse aggregates use ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
ordered by race date / race_id. No current-race outcome used.

## Citations

| Citation                       | Relevance                                                                                                                                 |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 5_2_53 (JES G ★)               | Surface×going interaction: NAR dirt heavy IS faster; going sign-flip the only non-additive effect at \*\*\* significance                  |
| 30_1901 (JES ★, 975k starts)   | WBGT 28–33°C at hot NAR venues; N-S gap 6°C+ (8.7°C confirmed in own data); 65–95% of sunny summer minutes above WBGT 28°C risk threshold |
| 36_2418 (JES 2025)             | Hot-day BW loss 5.8–7.5kg; baseline heat stress degrades performance; aerobic capacity impaired by thermoregulatory load                  |
| vol56-no4 p.372 (馬の科学 ★)   | Race density → gastric-ulcer risk; races in 30/60/90d as EGGD proxy; ≥3 races in 30d = elevated EGGD risk                                 |
| 30_1909 (JES A ★)              | Gelding OR=3.09 vs female for SDFT tendinopathy; BW ≥470kg OR=1.55; weight drop ≥5kg OR=1.59; interaction compounded                      |
| vol56-no3 p.194 (馬の科学 F ★) | JRA 632,540 horses: colt/gelding BW peaks autumn–winter / troughs summer; filly peaks autumn / troughs spring; ~30kg growth through age 5 |

## Method

### Step 1: Merge existing parquets

The four per-track parquets (V1 BW, V2 going, V4 heat, V5 volume) were merged
on (race_id, ketto_toroku_bango) via LEFT JOIN onto the base feature table
(feat-nar-v8-iter26-relationships). No feature was recomputed; all parquets were
taken as-is from their respective builds.

Total merged rows (2018–2024): 934,301. Ridge fit years: 2018–2021 (525,733 rows).
Evaluation years: 2022–2024 (408,568 rows).

### Step 2: Joint composite probe

A Ridge regression (alpha=1.0, numpy-based, no sklearn) was fit on the complete
rows (all 12 features non-null simultaneously) to predict finish_position.
Complete rows in fit set: **45,254** — only 8.6% of the fit period, driven by the
coverage bottleneck of pref_x_heat (7.9% of all rows).

The joint composite score was then applied in two modes:

- **All-non-null**: NaN where any of the 12 features is missing. Coverage: 7.7%.
- **NaN-filled**: missing features imputed with 0 (mean proxy). Coverage: 100%.

### Ridge coefficients (standardised)

| Feature                | Coef (standardised) | Interpretation                                   |
| ---------------------- | ------------------- | ------------------------------------------------ |
| `pref_x_heavy`         | −0.163              | Going specialist on heavy day: lower finish rank |
| `horse_heat_tolerance` | −0.100              | Heat-tolerant horse: lower finish rank           |
| `pref_x_heat`          | −0.100              | Duplicates heat_tolerance in hot months          |
| `bataiju_seasonal_dev` | −0.079              | Below seasonal norm → worse performance          |
| `is_gelding`           | −0.083              | Gelding: lower rank in field (injury risk)       |
| `races_in_90d`         | −0.060              | More recent races → better conditioning          |
| `is_gelding_high_bw`   | +0.040              | Suppressed by gelding main effect                |
| `horse_heavy_pref`     | +0.042              | Absorbed into pref_x_heavy interaction           |

## Step 3: Probe results

### Within-race Spearman of composite

| Composite mode          | Coverage | Within-race rho | n races |
| ----------------------- | -------- | --------------- | ------- |
| All-non-null (12 feats) | 7.7%     | **+0.074**      | 4,291   |
| NaN filled (0-imputed)  | 100%     | **+0.006**      | 40,685  |

The all-non-null composite rho of 0.074 is misleadingly elevated: it applies only to
the 4,291 races where all 12 features are simultaneously non-null — a highly selected
subset dominated by races where pref_x_heat fires (hot-month, heat-tolerant horses,
sufficient hot + cool starts, on a heavy day). This subset is not representative of the
full NAR schedule and the rho is not comparable to the all-race rho used in prior
science-track comparisons.

### Partial Spearman after existing feature proxies

11 existing feature proxies were regressed out (OLS) before computing the within-race
Spearman of the composite residual:

| Partialled-out proxy           | Why included                                       |
| ------------------------------ | -------------------------------------------------- |
| `weight_avg_5`                 | Body weight history (proxies bataiju_seasonal_dev) |
| `weight_diff_from_avg`         | Weight deviation                                   |
| `bataiju_futan_ratio`          | Weight/burden ratio                                |
| `past_speed_age_adjusted_avg5` | Age-adjusted speed                                 |
| `consecutive_race_count`       | Race schedule (proxies races_in_90d)               |
| `days_since_last_race`         | Schedule freshness                                 |
| `same_track_win_rate`          | Venue/going ability proxy                          |
| `career_win_rate`              | Overall quality                                    |
| `speed_index_avg_5`            | Recent speed                                       |
| `finish_trend_5`               | Form trend                                         |
| `barei_diff_from_race_mean`    | Age within field                                   |

**Partial composite rho (NaN-filled): +0.005** (n_races = 38,351).

This is consistent with noise. The partial rho is **16x below the abort threshold of
0.08** and 19x below what would be needed to match the V1 single-track level that
already failed full retrain (bataiju_seasonal_dev raw rho = −0.093).

## Verdict

**ABORT** — joint probe, pre-training.

**Binding reason**: the joint composite partial rho after partialling out existing
feature proxies is **+0.005** — indistinguishable from zero (38,351 races, 2022–2024).
This is far below the 0.08 abort threshold and represents no incremental signal.

### Why combining orthogonal sparse signals fails here

The core structural problem exposed by this probe is a **coverage intersection collapse**:

- pref_x_heat: non-null for 7.9% of rows
- horse_heavy_pref: non-null for 73.9% of rows
- Intersection (all 12 features non-null): 7.7% of rows

A Ridge composite that requires all signals simultaneously applies only to races where
all mechanism-specific conditions fire at once (hot month + heavy going + heat-tolerant
horse + sufficient going history + all V1 features present). This is not a representative
sample of NAR racing; it is an anomalously selected subset where the model happens to
have all signals active.

When NaN features are filled with zero (mean imputation, treating "no signal" as neutral),
the composite collapses to rho=0.006 — effectively zero — because 92% of rows receive
a NaN-filled composite that carries no real signal for those features.

The orthogonality of the mechanisms does not compensate for their sparsity: two
independent signals that fire on 8% and 31% of races respectively produce a joint
signal that fires on 2–8% of races (their intersection), not on 39% (their union).
A GBDT can exploit this in principle, but the probe shows the joint partial information
after the existing 174 features is near zero even on the concentrated intersection.

### Comparison to empirical prior

| Signal                           | Coverage  | Partial rho | Scope      | Outcome                  |
| -------------------------------- | --------- | ----------- | ---------- | ------------------------ |
| H2 h2_form_delta (best feat)     | all races | —           | rho=0.142  | REJECT all 4 NAR classes |
| V1 bataiju_seasonal_dev          | 99.96%    | rho=−0.093  | all races  | REJECT all 4 NAR classes |
| V4 pref_x_heat (partial)         | 15.6%     | 0.080–0.098 | hot months | ABORT pre-training       |
| V2 pref_x_heavy (partial)        | 31%       | 0.045       | heavy days | ABORT pre-training       |
| V5 races_in_90d (partial)        | 100%      | 0.059       | all races  | ABORT pre-training       |
| V3 age features (partial)        | all races | 0.040–0.055 | all races  | ABORT pre-training       |
| V6 sire distance split (best)    | 89.7%     | 0.025       | all races  | ABORT pre-training       |
| **V7 joint composite (partial)** | 7.7% (nn) | **0.005**   | mixed      | **ABORT pre-training**   |

The joint composite produces the _weakest_ partial rho of any signal in the entire
science track — weaker even than V6's 0.025. Combining orthogonal signals that each
individually sit below the actionable threshold does not add their rank-discriminating
power; it dilutes it through coverage fragmentation.

## Science-Track Synthesis: 8-Hypothesis Verdicts Table

This section documents the complete V1–V7 science track (plus the joint V7 hypothesis)
and draws the overall conclusion about the 174-feature accuracy ceiling vs science-
literature channels.

| #   | Hypothesis ID                                 | Mechanism                                                               | Status                                      | Binding reason (one line)                                                                                                                                                                          |
| --- | --------------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| V1  | H-SEASONAL-BW + H-GELDING-BW                  | Seasonal body weight deviation; gelding injury risk                     | **REJECT** (full WF retrain, all 4 classes) | Strongest new signal (bataiju_seasonal_dev rho=−0.093) below actionable threshold after existing BW representation; 0/4 axes positive in B/other/A, 1/4 in C                                       |
| V2  | H-DIRT-GOING-SIGN-FLIP                        | Per-horse going preference on NAR heavy dirt                            | **ABORT** (pre-training)                    | Conditional partial rho=0.045 on 31% of races — ~3x weaker than H2 (rho=0.142) which was rejected; not worth compute                                                                               |
| V3  | H-AGE-MONTH-SURFACE                           | Age-peak deviation by sex × distance type                               | **ABORT** (pre-training)                    | Raw rho=0.123 collapses to partial rho=0.040–0.055 after existing futan_per_barei + past_speed_age_adjusted_avg5 already capture the age development curve                                         |
| V4  | H-HEAT-WBGT                                   | Per-horse heat tolerance differential (hot vs cool venues)              | **ABORT** (pre-training)                    | Conditional partial rho=0.080–0.098 on hot-month races (15.6% of starts); probe script erroneously used career-count proxy; genuine heat signal below actionable threshold at this coverage        |
| V5  | H-RECENT-RACE-VOLUME + H-GASTRIC-RACE-DENSITY | Cumulative distance / race density in 30–90d windows                    | **ABORT** (pre-training)                    | races_in_30d Pearson=0.9848 vs consecutive_race_count (redundant); races_in_90d partial rho=0.059 — marginal miss of 0.06 bar; not actionable given 11-rejection prior                             |
| V6  | H-SIRE-DISTANCE-SPLIT                         | Sprint vs route sire aptitude mismatch                                  | **ABORT** (pre-training)                    | Best partial rho=0.025 (sire_today_mismatch_abs); sire_sprint_wr 82% correlated with existing sire_track_win_rate; all features well below 0.06 abort bar                                          |
| V7a | H-JOINT-WEAK-ORTHOGONAL (this entry)          | Ridge composite of V1+V2+V4+V5 joint features                           | **ABORT** (joint probe, pre-training)       | Partial composite rho=0.005 after 11 existing proxies; coverage intersection collapses to 7.7% (driven by pref_x_heat 8%); combining sparse orthogonal signals produces weaker not stronger signal |
| V7b | (Wave 1 prior: H1–H5)                         | Field-relative ranks, form delta, calibration stacking, HPO, multiclass | **REJECT** (multiple)                       | H1: score-Spearman vs existing residual 0.9991 (duplicate); H2: rho=0.142 rejected all 4 classes; H3: stacking sub-noise members; H4: signal ceiling not capacity; H5: no additional benefit       |

### Overall conclusion: the 174-feature ceiling and science-literature channels

**The 174-feature NAR baseline has reached an accuracy ceiling that the entire
science-literature signal family cannot break with current data and model architecture.**

Six independent science-backed mechanisms were tested across V1–V6. Each was
confirmed as a real physiological phenomenon (the underlying biology is valid):

- Seasonal BW deviation: confirmed by probe Spearman −0.093 and vol56-no3 ★ paper.
- Going sign-flip: confirmed at \*\*\* significance in own data (34,706 heavy-going races).
- Age peak curve: confirmed in NAR data with 8-month sprint vs route peak spread.
- Heat tolerance: confirmed by quintile effect (q0 avg_finish=6.27 vs q4=5.69).
- Race density: gastric-ulcer risk mechanism confirmed by literature.
- Sire distance split: r=0.35 between sprint and route EPD confirmed in literature.

Yet every mechanism failed to produce actionable within-race rank-discriminating signal
above the existing 174-feature representation. The joint probe now confirms that this
is not a failure of any individual mechanism but a **structural ceiling**:

**The existing 174 features already represent the extractable rank-discriminating
information from the available data sources at NAR inter-race scales.**

Three structural reasons explain why science-backed signals fail to lift the ceiling:

1. **Coverage-signal tradeoff**: The strongest individual partial rho values (heat 0.080–0.098,
   going 0.045) apply to subsets (15.6% and 31% of races) where the signal actively fires.
   The remaining 69–84% of races receive no signal. The holdout covers 2023–2026 (all races);
   the powered gate on the full holdout averages over the zero-signal majority.

2. **Existing feature representation is nearly complete for available data types**: Every
   science-track signal involves a feature type (body weight, going, race schedule, pedigree,
   sex) that the existing 174 features already partially encode. The marginal partial
   information — after conditioning on what the existing features already know — is too
   small to move holdout accuracy given NAR sample sizes.

3. **Within-race ranking vs absolute performance**: Scientific mechanisms like heat stress
   and going preference affect absolute performance (speed). But finish position is
   determined by _relative_ performance within a single race field. When all horses in a
   race experience the same heat stress (they are at the same venue on the same day), the
   within-race discriminating signal comes only from the _differential_ heat tolerance —
   which is inherently weak because most horses in a field have similar career heat exposure
   patterns.

**What is needed to break the ceiling:**

Per the v7-lineage saturation analysis (project_v7_lineage_saturation_2026_06_04.md),
the accuracy ceiling requires:

- **Genuinely new horse-level signals not derivable from race outcomes**: genomic features
  (MSTN genotype, performance gene variants — not in nvd_se/nvd_ra), biomechanical
  telemetry (wearable sensor data — not collected for NAR public data), veterinary records
  (lameness history, treatment records — not accessible).

- **Full retrain** (not per-class residual) on a newly constructed feature set that
  cannot be proxied by the existing 174 features.

- **Longer holdout or venue-stratified analysis**: given the sparse signal in hot-month
  venues (15.6% of races), a 4-year holdout may be underpowered for the conditional
  improvements even if the mechanism is real. A 10-year holdout at a single hot-venue
  (e.g., 名古屋48, 高知54) might reveal lift that is invisible in the full NAR average.

**The science track exploration has been scientifically productive**: six mechanisms were
confirmed, six were operationally infeasible at the 174-feature ceiling. The log is
permanent evidence of what the existing feature representation already captures.

## Hard Rules Observed

- `tmp/` only: all model artifacts, parquets, and prediction files in `tmp/`
- No `git add tmp/`: no tmp/ files staged or committed
- PG read-only: only SELECT queries issued (DuckDB postgres attach READ_ONLY)
- seed=42: enforced in joint_probe.py
- CatBoost thread_count=6 (spec), seed=42: not invoked (ABORT before retrain)
- No authorized code changes deployed
- Null permutation not run after all-ABORT (moot per task spec)
