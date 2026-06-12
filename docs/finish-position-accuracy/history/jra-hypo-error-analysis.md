# JRA Bottom-Up Error Analysis — Hypothesis Generation

**Date:** 2026-06-13
**Model:** iter14-jra-cb-pacestyle-course-v8 (CatBoost, 241 features, 20-fold LOYO)
**Holdout:** 2023–2026 JRA (11,703 races / 160,347 horse-race rows)
**Purpose:** Bottom-up failure-mode analysis to derive genuinely novel, testable hypotheses.
Prior top-down feature search is exhausted (jra-unused-data-scan.md verdict: "space closed").

---

## Baseline (2023–2026 holdout)

| metric  | value                                    |
| ------- | ---------------------------------------- |
| top1    | 44.76%                                   |
| place2  | 64.26% (predicted rank ≤2 finishes ≤2nd) |
| place3  | 74.86%                                   |
| n_races | 11,703                                   |

The confusion structure is: 44.8% correct top1; of misses, 30% finish 2nd/3rd (near-miss), 4% finish 10th+.
65.9% of races are upsets (market favourite loses) — the bulk of error is structurally irreducible.

---

## Method

1. Joined iter14 WF prediction parquets (`tmp/bucket-eval/…/predictions/category=jra/race_year=*/`) with
   feature parquets (`apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course/race_year=*/`) on
   (race_id, ketto_toroku_bango, umaban, race_year). 160,347 matched rows.
2. Computed per-race "top1 hit" as: (predicted_rank=1 AND actual_finish_position=1).
3. Sliced on 15+ dimensions. All slice accuracy numbers are percent of races where top-pick wins.
4. Novelty-checked each finding against docs/finish-position-accuracy/history/ corpus.

---

## Slice Results Summary

### By Distance Band

| dist_band      | n_races   | top1_pct  | Δ vs global |
| -------------- | --------- | --------- | ----------- |
| <1400m         | 2,578     | 43.1%     | −1.7pp      |
| **1400-1599m** | **1,582** | **38.6%** | **−6.2pp**  |
| 1600-1799m     | 2,081     | 41.8%     | −3.0pp      |
| 1800-1999m     | 2,942     | 43.9%     | −0.9pp      |
| 2000-2399m     | 1,609     | 45.7%     | +1.0pp      |
| 2400m+         | 911       | 67.9%     | +23.2pp     |

1400–1599m is the single worst non-trivial distance band (−6.2pp, n=1,582). The 2400m+ outlier reflects
predictable small-field long-distance races with high-odds favourites (avg odds 5.0x).

### By Grade Code

| grade_code | n_races | top1_pct | note                  |
| ---------- | ------- | -------- | --------------------- |
| C          | 231     | 32.9%    | open stakes           |
| B          | 133     | 33.1%    | graded race (lower)   |
| L          | 223     | 35.4%    | listed                |
| E          | 2,447   | 40.9%    | conditional/allowance |
| (blank)    | 8,555   | 46.5%    | maiden/unclassified   |
| A          | 80      | 47.5%    | graded race (upper)   |

Grade C/B/L: 32.9–35.4%, roughly 9–12pp below the majority bucket (blank).
Within 1400-1599m specifically, grade=E on dirt tracks (codes 17/18) is worst: 31.4–33.3%.

### By Field Size

| field_size | n_races | top1_pct | Δ vs global |
| ---------- | ------- | -------- | ----------- |
| ≤8         | 679     | 53.5%    | +8.7pp      |
| 9–12       | 2,952   | 49.2%    | +4.4pp      |
| 13–14      | 2,116   | 46.0%    | +1.2pp      |
| 15–16      | 4,935   | 41.7%    | −3.1pp      |
| 17+        | 1,021   | 38.7%    | −6.1pp      |

Clean monotone degradation. The 15–16 bucket (42% of all races) depresses the global average by ~1.5pp on
its own. 17+ is the acute tail (−6.1pp). Already documented in I2 as "partially reducible."

### By Track Condition (normalized)

| cond                    | n_races | top1_pct |
| ----------------------- | ------- | -------- |
| 0.0 (firm)              | 8,332   | 43.2%    |
| 0.3 (good-yielding)     | 1,813   | 45.8%    |
| 0.6 (soft)              | 901     | 45.5%    |
| 1.0 (heavy)             | 358     | 44.1%    |
| NULL (all-weather/dirt) | 299     | 80.6%    |

Track condition shows minimal differential (43–46%). The NULL bucket is artifactual: these 299 races are
all-weather or dirt-surface races (track_codes 52/54/56) where track_condition_normalized is structurally
absent — not a real accuracy gain.

### By Season

Season band 0–3: 44.2–46.0%. Flat. No seasonal bias.

### By Model Confidence (score gap rank-1 vs rank-2)

| conf_bucket         | n_races | top1_pct  | place2_pct |
| ------------------- | ------- | --------- | ---------- |
| high_conf (>0.5)    | 5,879   | **57.3%** | **74.4%**  |
| med_conf (0.2–0.5)  | 3,075   | 34.3%     | 56.5%      |
| low_conf (0.05–0.2) | 2,008   | 29.4%     | 51.7%      |
| very_low (<0.05)    | 741     | 30.5%     | 49.9%      |

This is the strongest single discriminator found: when the rank-1 horse's predicted score exceeds rank-2
by >0.5, accuracy is 57.3% (50% of all races). When the gap is <0.2, accuracy is 29–31%. The model's own
score distribution contains strong metacognitive signal about when it will be right. This is previously
undocumented.

### By Running Style (rs_predicted_class of predicted-rank-1 horse)

| rs_class | meaning | times picked rank-1 | win_pct when picked | avg odds |
| -------- | ------- | ------------------- | ------------------- | -------- |
| 0        | nige    | 451                 | 45.9%               | 3.3x     |
| 1        | senkou  | 4,138               | 46.8%               | 3.5x     |
| 2        | sashi   | 2,030               | 43.4%               | 4.2x     |
| 3        | oikomi  | 244                 | 37.3%               | 6.5x     |
| NULL     | no RS   | 4,840               | 43.9%               | 3.9x     |

Class 3 (oikomi/late-closers) is picked as rank-1 only 244 times (0.92% of races) vs 13% for nige/senkou.
When picked, they win only 37.3% vs 46–47% for front-running styles. Oikomi horses have median odds 93.7x
— they are structural longshots with 2.13% oracle win rate. The model correctly avoids them but slightly
under-performs when it does pick them (37% vs expected ~38% at 6.5x odds — minor miscalibration).

The RS NULL bucket (41.7% of horses, 4,840 times picked) behaves like the global average (43.9%).
Track code 10 (jump races) is 100% NULL for rs_predicted_class.

### By Days Since Last Race (freshness of predicted-rank-1 horse)

| freshness       | n_races | top1_pct | Δ      |
| --------------- | ------- | -------- | ------ |
| layoff (>90d)   | 1,445   | 42.6%    | −2.2pp |
| 15–28d normal   | 3,465   | 44.2%    | −0.6pp |
| ≤14d very fresh | 1,118   | 45.3%    | +0.5pp |
| 29–56d rested   | 2,547   | 45.5%    | +0.7pp |
| 57d+ rest       | 2,154   | 45.3%    | +0.5pp |
| debut/null      | 974     | 46.0%    | +1.2pp |

**The short-rest hypothesis does NOT hold at the race level.** Very-fresh horses (≤14d) are actually
slightly above average (45.3%) because the model picks freshness-signal favourites. The main penalty is
for layoff (>90d flag): −2.2pp. No severe short-rest failure mode.

### By Year (training era)

| year           | top1_pct                 |
| -------------- | ------------------------ |
| 2007–2016      | 36.3–38.6% (mean ~37.8%) |
| 2017–2022      | 40.2–43.5%               |
| 2023–2024      | 43.9–46.0%               |
| 2025           | 44.5%                    |
| 2026 (partial) | 44.5%                    |

Clear upward trend from 2017 onwards. 2024 was peak (46.0%). 2025 is slightly below 2024 but well
above pre-2017. No deterioration signal. The 7–8pp pre-2017 gap is the leave-one-out "cold start"
effect (training on post-year data, early years have less data).

### By H2H Encounter Count (per-race average)

| h2h_bucket    | n_races | top1_pct |
| ------------- | ------- | -------- |
| high (avg 4+) | 1,606   | 38.7%    |
| med (avg 2–4) | 2,210   | 43.3%    |
| no h2h        | 3,223   | 46.3%    |
| low (avg <2)  | 4,664   | 46.4%    |

Inverse monotone: more H2H history = worse accuracy. Races with rich H2H data (avg 4+ prior meetings
per horse) have −6pp vs no-H2H races. These are high-frequency competitive circuits where genuine
race uncertainty is higher, not a feature failure. The H2H features may be adding noise (or: the H2H
features partially capture horse-level rivalry but the confound is race type).

### Track Condition × Baba Experience (artifact)

The baba_exp × track_condition analysis showed an inverted pattern (experienced horses in soft/heavy
conditions score near 0%) but this is a measurement artifact: `horse_baba_career_starts` is not
horse-level experience at that surface — it counts at a specific condition code, creating selection
bias. Not a genuine signal gap.

---

## Failure Mode Diagnosis

### Failure Mode A: 1400–1599m Systematic Weakness (−6.2pp, n=1,582)

The 1400–1599m band is the worst well-populated distance band. Within it:

- Worst subset: grade=E on dirt tracks 17/18 at 31–33% (n=30–80 each).
- The weakness is NOT pace-specific: both fast_pace (38.4%) and normal_pace (39.6%) within
  this band are equally weak.
- Average field size is 14.7 — the largest of any distance band (larger fields correlate with
  lower accuracy; this is partly a field-size confound).
- The actual winner was the model's predicted rank 4+ in 25.9% of 1400–1599m races vs 22.8%
  for 1800–1999m — more "chaos" upsets.
- Root cause candidates: (1) JRA sprint-to-mile transition races where pace tactics are most
  variable (front-runner vs closer balance is neutral at this distance, maximising uncertainty);
  (2) the largest field sizes in JRA cluster here; (3) no distance-specific pace shaping feature.

### Failure Mode B: Model Over-Confidence Suppression (ECE=0.062, high-decile gap −14pp)

From I2 calibration analysis: when the model assigns the rank-1 horse 70% implied win probability,
the horse actually wins 85% of the time. The score is systematically compressed. The score gap
between rank-1 and rank-2 is a stronger accuracy predictor than previously exploited:

- Top 50% of races by score gap: 57.3% top1.
- Bottom 50%: 31–34% top1.

This is not a training failure — it is a known CatBoost/GBDT behaviour where cross-entropy
regularisation shrinks scores toward the mean. Post-hoc isotonic recalibration on the score
distribution has been discussed (I2 sec 8) but never implemented.

### Failure Mode C: Grade C/B/L Stakes Races (32.9–35.4%, n=133–231 each)

Stakes races (grade B/C) are 9–12pp below average. These are even-quality fields where horse
quality differences are smallest. The model's primary signal (odds, speed index, pedigree) all
compress toward zero within such a field, making upset probability higher. No specific feature
has been probed for this stratum. The per-class LGBm residual approach was rejected on statistical
power grounds (n too small), but per-stratum calibration or reweighting has not been attempted.

### Failure Mode D: Large Field Size ≥15 (−3 to −6pp, 5,956 races = 51% of holdout)

This is the highest-volume failure mode. Large fields are the majority of JRA racing. The model
degrades monotonically with field size (53% at ≤8, 39% at 17+). No field-interaction feature
specifically addresses crowding effects, post-position advantage in large fields, or the
combinatorial noise of 17-horse fields. This was labeled "mostly irreducible" in I2 but pace
variance features in large fields have not been tested.

### Failure Mode E: Winner Predicted Rank 4+ in 26% of Misses

26% of races where the model's top pick loses, the actual winner was predicted rank 4 or lower.
These are "complete surprises" — the model has no structural signal on these horses. This fraction
is consistent across distance and class buckets. It likely represents genuine market failures
(late-scratched rivals, track bias events, form-reversal events) that no pre-race feature can
capture.

---

## Novelty Check Results

| Hypothesis                                                   | Prior status                                                                                                 |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| Score gap rank-1 vs rank-2 as calibration/confidence feature | **NEVER TRIED** — zero mentions in corpus                                                                    |
| 1400-1599m distance weakness                                 | Documented in iter0 + I2; labeled "partially reducible via pace/track features"; no iteration targeted it    |
| Grade C/B/L stakes race calibration                          | iter0 documented weakness; per-class LGBm residual REJECTED for stat power; stratum-level isotonic not tried |
| Field size 17+ features                                      | Documented I2; no feature engineering attempted beyond existing field_size_normalized                        |
| Oikomi (class 3) miscalibration as cross-model propagation   | Calibration shipped for RS model; propagation to FP model not studied                                        |
| Post-hoc isotonic recalibration of FP model score            | Discussed in I2 as "highest ROI lever" but never implemented                                                 |

---

## Hypotheses

### Hypothesis 1: Post-Hoc Score Recalibration Directly Lifts Top1 by 0.5–2pp

**Failure slice:** JRA ECE=0.062. Model decile 10 (top confidence picks): predicted win prob 0.71,
actual win rate 0.85 — gap of +14pp. This shrinkage is most severe in races the model already
gets right (high confidence). I2 estimated 0.5–2pp top1 lift from isotonic recalibration.

**Evidence:**

- Score gap >0.5 (50% of races): 57.3% top1 hit.
- Score gap 0.05–0.2 (17% of races): 29.4% top1 hit.
- At calibration decile 10 (n=1,170), the model's predicted rank ordering is mostly correct
  but the score values are compressed. Recalibration would shift rank borderline cases.

**Mechanism:** CatBoost uses cross-entropy with L2 regularisation; scores are shrunk toward the
prior (uniform 1/n). Post-hoc isotonic regression on holdout softmax scores would unfold this
compression, directly increasing rank-separation near the decision boundary.

**NOT a duplicate of:** calibration/rerank in DO-NOT-RETEST list. That list refers to
_secondary model rescoring_ (Platt/logistic on top-1 binary output). This is isotonic calibration
of the raw CatBoost NDCG scores to a probability scale, used only for rank decision-boundary
sharpening. The technique was flagged as "highest ROI" in I2 sec 8 but never tested.

**Test design:**

- Probe: Fit isotonic regression (or Platt) on 2021–2022 holdout softmax scores → apply to
  2023–2026. Measure Δtop1 / Δplace2.
- Expected gain: +0.5 to +1.5pp top1 (conservative), up to +2pp (I2 estimate). Place2 gain
  from rank reordering: +0.3–0.8pp.
- No retraining required. Zero training risk.

**Caveats:** This is strictly a post-processing fix. It does not improve the underlying model;
it improves the precision of rank-boundary decisions. WF gate must measure on served predictions
(after calibration applied), not raw scores.

---

### Hypothesis 2: Score-Gap Confidence Feature in Serve Filtering / Bet Sizing

**Failure slice:** Score gap <0.2 (25% of races): 29–31% top1. Score gap >0.5 (50% of races):
57.3% top1. The difference is 26pp — larger than any single feature's importance.

**Evidence:**

- This signal is implicit in the model (the gap IS the model's uncertainty) but has never been
  materialised as a feature or served metadata.
- The confidence gap correctly identifies low-precision races: in races where the model picks
  weakly (gap <0.05), accuracy is 30.5% — barely better than random in a 14-horse field (7.1%).
- The gap is NOT an artefact of field size: large-field races tend to have compressed gaps, but
  the relationship holds within each field-size bucket.

**Mechanism:** The score gap quantifies the model's certainty about which horse it most
distinguishes. When gap is small, two horses are nearly tied — the model's ranking is essentially
a coin flip between them, and the actual outcome is equally noisy. When gap is large, the model
has identified a dominant horse.

**NOT a duplicate of:** Confidence-based filtering has not appeared in any iteration doc or probe.
The score_gap is derivable directly from existing prediction artifacts without retraining.

**Test design:**

- Probe: Split 2023–2026 races by score gap quintiles. Compute top1 within each quintile
  and verify monotone relationship persists after odds-control (partial Spearman ρ of score_gap
  with top1 binary, controlling for tansho_odds_raw).
- If ρ ≥ 0.08 after odds-control: implement as a serve-time metadata field ("prediction
  confidence class"). Use in dashboard display ("high confidence" / "low confidence" picks).
- Expected gain to served accuracy: 0pp (this is not a model feature) but substantial lift to
  _user-actionable accuracy_ when filtering on high-confidence races. Could inform bet sizing.
- Incremental model test: include score_gap as a meta-feature in a lightweight calibration
  model (logistic regression on top1 binary, features: [score_gap, predicted_score, odds_score]).
  Test if this meta-model outperforms isotonic-only calibration.

**Caveats:** Score gap is computed post-prediction; it is derived from existing model outputs
and cannot improve the base model. Its value is in (a) meta-calibration, (b) serve UX.

---

### Hypothesis 3: Distance-Specific Pace Structure Feature for 1400–1599m

**Failure slice:** 1400–1599m: 38.6% top1 (−6.2pp vs global, n=1,582). This persists across
pace buckets (fast: 38.4%, normal: 39.6%) and is not explained by field size alone (avg 14.7
is the largest but the gap persists within field-size-controlled buckets).

**Evidence:**

- 1400–1599m has the highest fraction of "rank 4+ surprise winners" (25.9% vs 22.8% at
  1800–1999m). The actual winner is a genuine upset more often.
- Worst subsets: grade=E on dirt tracks 17/18 at 31–33%. These are conditional/allowance
  races on dirt at middle distances — "maiden sprint-to-mile step-up" category.
- The distance band straddles JRA's tactical transition zone: at 1200–1399m, pace is
  uniform (sprint, all-speed); at 1800m+, pace is settled (long-distance gallop). At
  1400–1599m, tactics are least predictable (mixed front-runner vs mid-field tactics).
- Available features already include `field_nige_pressure`, `field_pace_index`,
  `course_final_straight_m`, and `rs_p_*` probabilities. None directly encode whether a
  horse's historical performance at 1400–1599m specifically differs from its overall distance
  record.

**Mechanism:** A horse that has only raced at 1200m and 1600m races will have extrapolated
speed/pace indices for 1400–1599m, but its actual tactical preference at this "transition zone"
may differ from either extreme. The model uses `same_distance_win_rate` and
`same_distance_place2_rate` but these features require actual prior starts at that distance
— and with distance bands, many horses have zero or one prior start in the 1400–1599m band.

**NOT a duplicate of:** `h-laplevel-speed-fade.md` tested sectional deceleration from lap
splits (B4 probe, ρ=0.0725, ABORT). That is a different concept (within-race pace shape from
historical lap times). This hypothesis is about per-horse distance-band transition features —
specifically whether a horse is entering the 1400–1599m band from a shorter or longer distance,
and how its historical rate changes at this transition. The `last_race_distance_diff` feature
exists but is a raw delta, not a win-rate differential.

**Test design:**

- Probe: Compute within-horse partial Spearman ρ for a new feature:
  `horse_1400_1599_win_rate` (win rate specifically in 1400–1599m races) vs
  `career_win_rate`. The DIFFERENCE `horse_1400_1599_win_rate − career_win_rate` captures
  whether a horse over-/under-performs at this distance.
  Gate: partial ρ ≥ 0.08 controlling for odds and career_win_rate.
- If gate passes: add distance-band-specific win rates for the 3 worst bands
  (1400–1599, 1600–1799, 1200–1399) to the feature store. Retrain with
  these 3 features added to iter14 base.
- Expected gain: +0.2–0.8pp top1 globally (1400–1599m is 13.5% of races; recovering 4pp
  of the 6pp gap within that slice yields 0.54pp global — realistic upper bound ~0.5pp).
- Risk: coverage. Horses with <3 prior starts in the 1400–1599m band will have NULL or
  unreliable rate estimates. Need NULL-routing (CatBoost handles natively).

**Caveats:** The `same_distance_win_rate` feature already exists at exact-distance level
(e.g., 1400m or 1600m separately). If a horse has run 5 races at 1400m and 5 at 1600m,
the band-level feature will aggregate correctly. But it adds marginal signal on top of what
`same_distance_win_rate` already captures. The probe must confirm incremental partial ρ.

---

### Hypothesis 4: Grade-Stratum Post-Hoc Calibration (B/C/L Stakes Races)

**Failure slice:** Grade B: 33.1% top1 (−11.6pp, n=133). Grade C: 32.9% (−11.8pp, n=231).
Grade L: 35.4% (−9.4pp, n=223). Total: n=587 races = 5% of holdout.

**Evidence:**

- All three grades are "even-quality fields" where horse quality differences compress.
  The model's primary discriminators (odds, speed index, pedigree score) all compress toward
  zero within a stakes field.
- The average field size for grade B is 14.2 and for C is 14.8 — close to the overall average.
  Field size alone does not explain the gap.
- Grade A (best races): 47.5% top1 — better than average despite being competitive.
  This is because grade A races (G1/G2) have efficient market odds that the model follows
  correctly; the odds-signal is strong when the best horse is clearly dominant.
- In grade B/C/L, market odds are more ambiguous (field is balanced), and the model
  lacks additional signal to discriminate.
- The per-class LGBm residual approach (jra-perclass-residual-feasibility.md) was REJECTED
  for statistical power reasons (n too small for full retrain). But post-hoc calibration
  at the grade level does not require retraining.

**Mechanism:** Grade B/C/L races have the most even quality fields. The model's rank-1
pick is more often "marginally best" rather than "clearly best," so calibrated confidence is
lower and rank inversions are more frequent. A grade-stratified isotonic calibrator would
learn separate score→win-probability mappings for each grade stratum.

**NOT a duplicate of:** "calibration/rerank" DO-NOT-RETEST refers to secondary-model
rescoring experiments (I7, per-class specialist heads). Grade-stratified isotonic calibration
is a lighter post-hoc approach that has never been applied at the grade level.
The per-class LGBm residual (rejected) is a different approach (LGBm on residuals) vs
isotonic correction of CatBoost raw scores.

**Test design:**

- Probe: Within the 2021–2022 calibration window, fit a separate isotonic regressor for
  each grade_code (B, C, L, E, blank). Apply to 2023–2026. Measure per-grade top1 Δ
  and global Δ.
- Expected gain: +0.3–0.7pp global (5% of races recovering ~4pp → 0.2pp; more if E also improves).
- This is strictly post-hoc and can be deployed without retraining.

**Caveats:** n=133–231 per grade in the calibration window (2021–2022 only) is small.
The isotonic calibrator may overfit to noise at this sample size. Use Platt scaling
(2-parameter logistic) rather than isotonic for small-n grades.

---

### Hypothesis 5: Large-Field Pace Variance Feature (15+ horses)

**Failure slice:** Field size 15–16: 41.7% top1 (−3.1pp, n=4,935 = 42% of all races).
Field size 17+: 38.7% (−6.1pp, n=1,021). Combined: ~51% of all JRA races are in these buckets.

**Evidence:**

- The existing `field_size_normalized` feature captures raw head count. The model degrades
  monotonically as field size grows.
- But the degradation is not uniform: 17+ fields at grade C (n~80) hit 35% — 3pp below the
  17+ average. 17+ fields at distance 2400m+ remain high (67%+ due to distance effect).
- The key failure mechanism for large fields is not just combinatorics but **pace variance**:
  in a 16-horse field, the probability of an atypically fast early pace (many nige/senkou
  horses competing for the lead) is higher. The existing `field_nige_pressure` feature captures
  the number of front-runners but does not capture the _volatility_ of expected pace, nor the
  benefit accruing to horses positioned mid-field who benefit from chaos.
- Current features: `field_nige_pressure`, `field_pace_index`, `field_spread_past_corner_1_norm`,
  `field_has_pure_nige_horse`, `field_style_diversity`. These are first-order field-level
  aggregates.
- Missing: a feature for "expected pace volatility in large fields" — e.g., the variance of
  `past_corner_1_norm` across all horses in the field, weighted by field size.

**Mechanism:** In large fields, the pace scenario is harder to predict ex ante because more
horses have incentive to contest the lead. The model's pace features were designed at field
sizes of 8–12 (typical sprint distances) and may not capture emergent dynamics at 15–17+.
A large-field-specific pace variance signal could help identify races where the pace structure
itself is predictable vs chaotic.

**NOT a duplicate of:** Pace features were tested in iter9 (L4/L9 levers) at the global level.
This hypothesis is specifically about large-field pace variance — a conditional feature
(`field_size × pace_variance` or similar). No prior probe has targeted this interaction.

**Test design:**

- Probe: Compute `large_field_pace_entropy` = Shannon entropy of `rs_p_nige` across all
  horses in the race, computed only when `shusso_tosu_1 ≥ 15`. Partial Spearman ρ against
  `actual_finish_norm` (controlling for `odds_score`, `field_nige_pressure`, `field_pace_index`).
  Gate: partial ρ ≥ 0.08 in 2023–2026 holdout.
- If gate passes: add 3 features to the store: `large_field_pace_entropy`,
  `large_field_nige_clash_intensity` (max(rs_p_nige) × second_max(rs_p_nige) for field≥15),
  `large_field_style_balance_index`. Retrain on iter14 base.
- Expected gain: +0.2–0.5pp global (51% of races, recovering ~1.5pp → 0.75pp at 50% coverage —
  realistic estimate 0.3pp accounting for partial ρ being threshold-limited).
- Risk: CatBoost already handles field-size non-linearity through depth-8 trees. Explicit
  interaction features may be redundant (iter15 lesson: pre-computed products are often
  redundant for GBDT). Probe partial ρ is mandatory before retrain.

**Caveats:** rs*p*\* features are 42% NULL across all horses (structural: pre-2024 coverage
and horses with insufficient history). For a field-level entropy feature, races where >50%
of horses have NULL rs_p_nige will have unreliable entropy estimates. Coverage analysis
on the 2023–2026 holdout window is needed before building.

---

## Summary: Ranked Hypothesis List

| rank | hypothesis                                             | evidence (slice, n, gap)                                                     | mechanism                                                                                         | expected gain                                         | novelty                     |
| ---- | ------------------------------------------------------ | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ----------------------------------------------------- | --------------------------- |
| 1    | Post-hoc isotonic score recalibration                  | ECE=0.062; decile 10: −14pp gap; score gap strong discriminator (57% vs 29%) | GBDT score shrinkage toward mean; unfold with isotonic fit                                        | +0.5–2.0pp top1, +0.3–0.8pp place2                    | Never tried                 |
| 2    | Score-gap as served confidence metadata                | Score gap >0.5 → 57.3% (n=5,879); gap <0.2 → 29–31% (n=2,749)                | Model's own uncertainty is a strong metacognitive signal                                          | 0pp model accuracy; high UX value; enables bet-sizing | Never tried                 |
| 3    | Distance-band-specific win rate features (1400–1599m)  | 1400–1599m: 38.6% (−6.2pp, n=1,582); rank-4+ surprise winners 25.9% vs 22.8% | Horse tactical preference at transition distance not captured by exact-distance rate              | +0.2–0.5pp global (gate-dependent)                    | Documented but never probed |
| 4    | Grade-stratum post-hoc calibration (B/C/L)             | Grade B/C/L: 32.9–35.4% (−9 to −12pp, n=587); grade A: 47.5%                 | Even-quality fields compress model scores; grade-stratified isotonic corrects this                | +0.3–0.7pp global                                     | Never tried                 |
| 5    | Large-field pace variance entropy feature (≥15 horses) | 15–16: −3.1pp (n=4,935=42%); 17+: −6.1pp (n=1,021); 51% of races affected    | Pace unpredictability scales non-linearly with field size; not captured by first-order aggregates | +0.2–0.5pp global (probe-dependent)                   | Never tried                 |

---

## DO-NOT-RETEST Confirmation

All hypotheses above have been verified NOT to reduce to the following rejected items:

- Relationship/per-class features (nige_vs_field, log_odds_z, futan_rank): NOT covered here.
- Exotic/fukusho odds: NOT covered here.
- rs*p*\* backfill or NAR rs expansion: NOT covered here.
- Graded relevance objective (NDCG@2, set-membership): NOT covered here.
- Per-class LGBm residual ensemble: Hypothesis 4 uses grade-stratified ISOTONIC (not LGBm residual).
- Calibration/rerank via secondary classifier model: Hypothesis 1 uses post-hoc isotonic on
  raw scores (not a secondary classifier predicting rank-1 binary).
- Decision rules / hand-crafted filters: NOT covered here.
- par-time, sectional-z, workout, yoso, scratch, gate-draw, age-sex-bw: NOT covered here.
- Condition-aware routing: NOT covered here.
- Jockey-trainer combo: NOT covered here.
- Odds-decoupling: NOT covered here.
- Class-weight serve: NOT covered here.
- Speed-fade index (sectional deceleration from lap times): H3 is distance-band win rate,
  NOT per-furlong deceleration.

---

## Appendix: Data Artifacts Identified

1. **track_condition_normalized = NULL** (n=299, 80.6% top1): These are all-weather/dirt-surface
   races (track codes 52/54/56/57 at Mombetsu, Kawasaki, Oi, Funabashi). The NULL means the
   standard baba condition encoding does not apply, not a data error.

2. **is_newcomer_race = 1** never appears in 2023–2026 joined table: The flag is either always 0
   or filtered out upstream. Verified that the column exists in the feature schema but no rows
   have value 1 in the 2023–2026 JRA holdout.

3. **rs_predicted_class coverage**: 42.3% NULL (68,768 of 162,471 horses). Track code 10
   (jump races) is 100% NULL. Other tracks cluster 37–45% NULL. The NULL distribution is
   grade/distance agnostic (spread 39–48%). Structural: rs model coverage limited by historical
   corner-position data availability.

4. **YoY accuracy step change at 2017**: Pre-2017 top1 ~37–38%, post-2017 ~40–46%. This is the
   leave-one-year-out cold-start effect (early years trained on post-year data only, not a
   temporal drift in horse racing itself).
