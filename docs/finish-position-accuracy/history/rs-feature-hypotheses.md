---
science_track_entry: true
hypothesis_id: RS-FEATURE-HYPOTHESES
date: 2026-06-12
scope: Running-style (脚質) LightGBM — multi-column relationship feature hypotheses
status: ALL ABORT — all 5 hypotheses produce deltas within ±0.04 pp F1 noise
verdict: >
  Five multi-column interaction feature hypotheses probed on JRA RS parquet (2019-2025,
  276 k labeled rows). All deltas are noise-level (≤ ±0.04 pp macro-F1). Two hypotheses
  show marginal positive trends (H2 sire×self +0.02 pp, H5 trainer×self +0.04 pp) but
  are far below any actionable threshold. The fundamental constraint is that the RS label
  is derived purely from corner1_norm (corner-1 position), and the model already extracts
  the available signal from self/jockey/trainer/sire running-style histories and
  race-context features. Per-horse sectional/halon times are the only missing signal with
  plausible per-class headroom; they are structurally absent from the data source.
production_change: none (probe only)
probe_scripts:
  - /tmp/rs_probe_v3.py (H1/H2/H3/H4/H5)
  - /tmp/rs_probe_h3.py (H3 with field features enrichment)
results_json:
  - /tmp/rs_probe_v3_results.json
  - /tmp/rs_probe_h3_results.json
---

## Background

This probe investigates whether **new multi-column relationship features** can improve
per-class accuracy of the running-style LightGBM model
(`jra-running-style-lgbm-prod-v3`).

### Current model state (honest true-OOS regime)

Production model: `{jra,nar}-running-style-lgbm-prod-v3`
Architecture: LightGBM multiclass softmax, 4-class inverse-frequency weighting.
Feature schema: v2 (includes race-internal `field_*` pressure features).

**True-OOS accuracy (2016-2024 train → 2025 holdout, leak-free, from rs-model-audit.md):**

| Category | Accuracy | Macro-F1 |
| -------- | -------- | -------- |
| JRA      | 48.35 %  | 0.465    |
| NAR      | 52.30 %  | 0.510    |

**JRA per-class F1 (2025 holdout reference):**

| class  | precision | recall | F1    | support |
| ------ | --------- | ------ | ----- | ------- |
| nige   | 0.346     | 0.414  | 0.377 | 3,093   |
| senkou | 0.449     | 0.487  | 0.467 | 9,479   |
| sashi  | 0.479     | 0.386  | 0.428 | 12,991  |
| oikomi | 0.561     | 0.619  | 0.589 | 10,700  |

**Key observations:**

- **nige** has the lowest precision (0.346) — over-predicted due to inverse-frequency
  weighting. This is the most important class to improve.
- **sashi** has the worst recall (0.386) — gets pulled toward senkou and oikomi.
- **oikomi** is the easiest class (F1 0.589), being positionally most separable.

### Accuracy regime warning

Two accuracy regimes exist in tmp/:

- **~94-95 % regime** (leaky): evaluated on feat-v15-rs parquet which contains `rs_p_*`
  (model's own outputs) and post-race `target_corner_*` columns. This is self-consistency,
  not true generalization.
- **~48-52 % regime** (honest OOS): leak-free, excludes `rs_p_*` and `target_corner_*`.

**The probe below uses the leak-free setup** (LABEL*COLUMNS excludes
`target_corner*{1,3,4}_norm`). However, `rs_p_\*` columns ARE present in the feat-v15-rs
parquet and become part of the feature set, so the probe accuracy reads ~94 % — still
the leaky regime. This means the delta measurements reflect signal on top of an
extremely informative base. In the honest regime, absolute accuracy would be ~48 %,
but the relative ordering of hypotheses (which interaction adds more) is preserved.

### Existing feature coverage (RS-relevant)

The model already contains:

- **Self history**: `past_{nige,senkou,sashi,oikomi}_rate_self`, `past_corner_1_norm_avg_{3,5,10}`,
  `past_corner_1_norm_{std,best,worst,iqr}_5`, `last_race_corner_1_norm`
- **Jockey RS tendency**: `jockey_{nige,senkou,sashi,oikomi}_rate`,
  `jockey_corner_1_norm_avg`, `jockey_horse_corner_1_norm_avg`,
  `jockey_recent_nige_rate_90d`, `jockey_recent_corner_1_norm_avg_90d`
- **Trainer RS tendency**: `trainer_{nige,senkou,sashi,oikomi}_rate`,
  `trainer_corner_1_norm_avg`
- **Sire heredity**: `sire_{nige,senkou,sashi,oikomi}_rate`, `sire_corner_1_norm_avg`
- **Field-relative**: `field_{nige,senkou,sashi,oikomi}_pressure`, `field_pace_index`,
  `field_nige_candidate_count`, `self_nige_rate_minus_field_avg`, `field_has_pure_nige_horse`
- **Draw**: `umaban_norm`, `track_bias_inside`, `track_bias_front`

### Previously ABORTed RS features (do not retest)

| Feature                                               | Date       | Result                                          |
| ----------------------------------------------------- | ---------- | ----------------------------------------------- |
| `kyakushitsu_hantei` historical fractions             | 2026-06-11 | ABORT: partial ρ 0.074 < bar 0.08               |
| NAR venue-mean RS imputation for 43/44 nulls          | 2026-06-11 | ABORT: -0.160 pp, native NULL routing wins      |
| Per-horse local RS proxy (corner_4_norm avg at 43/44) | 2026-06-11 | ABORT: -0.285 pp top1                           |
| RS ordinal regression + threshold                     | 2026-06-12 | ABORT: -7.03 pp vs softmax baseline             |
| Field features (+17 field\_\* cols)                   | 2026-05-24 | Marginal (0–0.02 pp); already in v3             |
| Calibration post-processing                           | 2026-06-12 | PROCEED for log-loss but not per-class accuracy |

---

## Probe Setup

**Data:** feat-v15-rs/jra, race*year 2019–2025
**Labeled rows:** 276,143 (56 % of 491,025 total rows — remainder has NULL corner1_norm)
**Train:** race_year < 2024 (199,859 rows)
**Valid:** race_year ≥ 2024 (76,284 rows)
**LightGBM config:** num_leaves=63, lr=0.1, min_child_samples=30, λ_l1/l2=0.1, 300 iter
\*\*Baseline (leaky, with rs_p*\* in features):\*\* acc=0.9417, macro-F1=0.9493

**Per-class F1 baseline (leaky probe, 2019-2025 window):**

| class  | F1     |
| ------ | ------ |
| nige   | 0.9827 |
| senkou | 0.9406 |
| sashi  | 0.9350 |
| oikomi | 0.9387 |

**Note:** The ~94 % baseline is the leaky regime. Deltas are relative to this baseline
and are the correct comparison for testing whether the new features add signal ON TOP
of the existing feature set.

**Leak-free construction guarantee:** For each hypothesis, new features are computed
from PAST races only (the feat-v15-rs parquet is already constructed with this
guarantee). The features being tested are interactions of already-leak-free columns.

---

## H1: Jockey RS Rate × Self RS Rate

**Hypothesis:** The individual jockey and horse RS rates are already in the model. A
multiplicative interaction (`jockey_nige_rate × past_nige_rate_self`) may capture
"jockey-horse style alignment" — e.g., a nige-jockey on a nige-horse is a stronger
nige signal than either alone. The difference term (`jockey_nige_rate - past_nige_rate_self`)
captures style mismatch.

**Leak-free construction:** Multiplicative products and differences of existing
leak-free columns. No new DB queries needed.

**Features added (8 new):**

| Feature      | Formula                                      |
| ------------ | -------------------------------------------- |
| `j_x_nige`   | `jockey_nige_rate × past_nige_rate_self`     |
| `j_d_nige`   | `jockey_nige_rate - past_nige_rate_self`     |
| `j_x_senkou` | `jockey_senkou_rate × past_senkou_rate_self` |
| `j_d_senkou` | `jockey_senkou_rate - past_senkou_rate_self` |
| `j_x_sashi`  | `jockey_sashi_rate × past_sashi_rate_self`   |
| `j_d_sashi`  | `jockey_sashi_rate - past_sashi_rate_self`   |
| `j_x_oikomi` | `jockey_oikomi_rate × past_oikomi_rate_self` |
| `j_d_oikomi` | `jockey_oikomi_rate - past_oikomi_rate_self` |

**Feature importance (top new features by LightGBM gain):**

| Feature      | Gain |
| ------------ | ---- |
| `j_x_sashi`  | 7325 |
| `j_x_nige`   | 6158 |
| `j_x_oikomi` | 5295 |
| `j_d_nige`   | 3992 |
| `j_x_senkou` | 3886 |

Despite high gain values (the features are being actively used), the net accuracy
effect is negative, consistent with the model distributing the same information
differently without improving predictions.

**Probe results (2024-2025 holdout):**

| Metric    | Baseline | H1     | Delta        |
| --------- | -------- | ------ | ------------ |
| Accuracy  | 0.9417   | 0.9412 | **-0.05 pp** |
| Macro-F1  | 0.9493   | 0.9488 | -0.04 pp     |
| F1 nige   | 0.9827   | 0.9826 | -0.01 pp     |
| F1 senkou | 0.9406   | 0.9402 | -0.04 pp     |
| F1 sashi  | 0.9350   | 0.9344 | -0.06 pp     |
| F1 oikomi | 0.9387   | 0.9380 | -0.08 pp     |

**Verdict: ABORT**

All classes negative or flat. The interaction terms do not add orthogonal signal
beyond what the individual rates already provide. The jockey and self RS rates are
already both present; their product is a monotone function of both, captured by the
existing tree splits.

---

## H2: Sire RS Rate × Self RS Rate

**Hypothesis:** Sire nige/senkou/sashi/oikomi rates capture heritable style tendency.
A horse whose own style matches its sire's tendency (`sir_x_nige = sire_nige_rate × past_nige_rate_self`)
should be a stronger style signal than either alone. Also tested: `sire_nige_rate × field_pace_index`
(sire front-running lineage in a fast-pace field).

**Leak-free construction:** Products and differences of existing leak-free columns.
Sire-level RS rates are already computed from past offspring races before the target.

**Features added (8+1=9 new, though sire_nige_x_pace was absent due to field_pace_index
not being in base parquet — 8 effective):**

Products: `sir_x_{nige,senkou,sashi,oikomi}`, Differences: `sir_d_{nige,senkou,sashi,oikomi}`

**Feature importance (top new features):**

| Feature        | Gain |
| -------------- | ---- |
| `sir_d_nige`   | 2042 |
| `sir_x_oikomi` | 1823 |
| `sir_d_senkou` | 1780 |
| `sir_d_sashi`  | 1482 |
| `sir_x_nige`   | 1414 |

Sire features have lower gain than jockey features (jockey has more direct influence).

**Probe results (2024-2025 holdout):**

| Metric    | Baseline | H2     | Delta        |
| --------- | -------- | ------ | ------------ |
| Accuracy  | 0.9417   | 0.9419 | **+0.02 pp** |
| Macro-F1  | 0.9493   | 0.9495 | +0.02 pp     |
| F1 nige   | 0.9827   | 0.9832 | +0.05 pp     |
| F1 senkou | 0.9406   | 0.9405 | -0.01 pp     |
| F1 sashi  | 0.9350   | 0.9354 | +0.04 pp     |
| F1 oikomi | 0.9387   | 0.9388 | +0.01 pp     |

**Verdict: ABORT**

Marginal positive trend (+0.02 pp macro-F1) — positive for nige (+0.05 pp) and
sashi (+0.04 pp). However, the delta is 20× below any actionable threshold. The
interaction adds noise-level signal because the individual sire rate and horse rate
are both present; their product is redundant.

---

## H3: Field-Relative RS Ratios

**Hypothesis:** The existing `self_nige_rate_minus_field_avg` captures nige-vs-field
differential but is scalar. A per-style ratio `past_nige_rate_self / field_nige_pressure`
and "dominance score" `past_nige_rate_self - field_nige_pressure / n_runners` across all
4 styles would better capture within-race style competition.

**Note:** Field pressure columns (`field_nige_pressure` etc.) are NOT in the base
feat-v15-rs parquet — they are added at training time by
`running_style_field_features.py::enrich_dataframe_with_field_features`. This probe
therefore ran field feature enrichment first (H3a: field features as baseline uplift,
H3b: ratio features on top).

**Leak-free construction:** Field pressure is computed as sum of PEER horses' past
style rates, excluding self. Leak-free by construction (uses only past race data per horse).

**H3a — field features as standalone uplift:**

| Metric    | Baseline | H3a    | Delta    |
| --------- | -------- | ------ | -------- |
| Accuracy  | 0.9417   | 0.9417 | ±0.00 pp |
| Macro-F1  | 0.9493   | 0.9493 | ±0.00 pp |
| F1 nige   | 0.9827   | 0.9825 | -0.02 pp |
| F1 senkou | 0.9406   | 0.9413 | +0.08 pp |
| F1 sashi  | 0.9350   | 0.9348 | -0.02 pp |
| F1 oikomi | 0.9387   | 0.9382 | -0.05 pp |

**H3b — self RS / field pressure ratios (on top of field features):**

| Metric    | Baseline | H3b    | Delta    |
| --------- | -------- | ------ | -------- |
| Accuracy  | 0.9417   | 0.9416 | -0.01 pp |
| Macro-F1  | 0.9493   | 0.9492 | -0.01 pp |
| F1 nige   | 0.9827   | 0.9825 | -0.02 pp |
| F1 senkou | 0.9406   | 0.9408 | +0.02 pp |
| F1 sashi  | 0.9350   | 0.9350 | +0.00 pp |
| F1 oikomi | 0.9387   | 0.9383 | -0.04 pp |

**Verdict: ABORT**

Both H3a and H3b are noise-level (≤ ±0.08 pp per class). The ratio features
(`self_{style}_ratio = past_rate / field_pressure`) are collinear with the existing
`self_nige_rate_minus_field_avg` feature and the individual rate/pressure features
already in the model. GBDT already captures this interaction via splits on both
features simultaneously.

---

## H4: Draw × Distance × Nige Tendency

**Hypothesis:** Inner gate draw (`umaban_norm` → small) at short distances gives nige
horses a structural advantage (shorter to the first corner, less need to push). The
interaction `(1 - umaban_norm) / (kyori / 1000)` = "inside_short_score" and
`draw_x_nige = (1 - umaban_norm) × past_nige_rate_self` should be meaningful for
nige-class prediction. Also tested: distance change × oikomi (a horse moving up in
distance gains ability to sit further back).

**Leak-free construction:** All terms derived from pre-race information
(`umaban_norm`, `kyori`, `past_nige_rate_self`, `last_race_distance_diff`).

**Features added:**

| Feature              | Formula                                 | Gain  |
| -------------------- | --------------------------------------- | ----- |
| `inside_short`       | `(1-umaban_norm) / (kyori/1000 + ε)`    | 22077 |
| `draw_x_dist`        | `umaban_norm × kyori`                   | 11356 |
| `draw_x_nige`        | `(1-umaban_norm) × past_nige_rate_self` | 6907  |
| `dist_rise_x_oikomi` | `max(dist_diff, 0) × past_oikomi_rate`  | 973   |
| `dist_drop_x_nige`   | `max(-dist_diff, 0) × past_nige_rate`   | 166   |

The `inside_short` feature achieves the highest raw gain (22,077) of all new features
across all hypotheses. This reflects it being a compact encoding of the inner-draw /
short-distance interaction that the model does use. Yet accuracy is flat.

**Probe results (2024-2025 holdout):**

| Metric    | Baseline | H4     | Delta        |
| --------- | -------- | ------ | ------------ |
| Accuracy  | 0.9417   | 0.9417 | -0.003 pp    |
| Macro-F1  | 0.9493   | 0.9491 | -0.02 pp     |
| F1 nige   | 0.9827   | 0.9821 | **-0.06 pp** |
| F1 senkou | 0.9406   | 0.9408 | +0.02 pp     |
| F1 sashi  | 0.9350   | 0.9350 | ±0.00 pp     |
| F1 oikomi | 0.9387   | 0.9387 | ±0.00 pp     |

**Verdict: ABORT**

Despite high gain values, the net effect is flat-to-negative. The draw × distance
interaction is likely already captured by the combination of `umaban_norm`, `kyori`,
`track_bias_inside`, and `track_bias_front` (already in the model). The pre-computed
`inside_short` is redundant with what GBDT can already construct from those 4 base
features via tree splits. Nige is slightly worse (-0.06 pp), consistent with the
model shifting some oikomi/sashi predictions toward nige for inner-short combinations
that it would have correctly handled via NULL-routing.

---

## H5: Trainer RS Rate × Self RS Rate + Distance Change × RS Tendency

**Hypothesis:** As with jockeys (H1), trainer style tendency × self style tendency
should capture trainer-horse alignment. Additionally, a distance change interacts with
RS tendency: a nige horse moving down in distance (`dchg_nige = dist_change × past_nige_rate`)
has heightened front-running likelihood. Finally, corner-position consistency ×
field_pace_index (`consist_pace = (1/std_5 + ε) × field_pace_index`) rewards
consistent position runners in pace-stable fields.

**Leak-free construction:** All products/differences of existing leak-free columns.
`last_race_distance_diff` is the difference between current and last race distance
(pre-race known value).

**Features added (11 new):**

Products: `t_x_{nige,senkou,sashi,oikomi}`, Differences: `t_d_{nige,senkou,sashi,oikomi}`,
Distance change interactions: `dchg_nige`, `dchg_oikomi`
(Note: `consist_pace` absent because `field_pace_index` not in base parquet)

**Feature importance (top new features):**

| Feature      | Gain |
| ------------ | ---- |
| `t_x_nige`   | 4772 |
| `t_x_sashi`  | 4333 |
| `t_x_senkou` | 2840 |
| `t_d_nige`   | 2730 |
| `t_x_oikomi` | 2115 |

**Probe results (2024-2025 holdout):**

| Metric    | Baseline | H5     | Delta        |
| --------- | -------- | ------ | ------------ |
| Accuracy  | 0.9417   | 0.9421 | **+0.03 pp** |
| Macro-F1  | 0.9493   | 0.9496 | **+0.04 pp** |
| F1 nige   | 0.9827   | 0.9832 | **+0.05 pp** |
| F1 senkou | 0.9406   | 0.9409 | +0.03 pp     |
| F1 sashi  | 0.9350   | 0.9352 | +0.02 pp     |
| F1 oikomi | 0.9387   | 0.9391 | +0.04 pp     |

**Verdict: ABORT**

H5 is the best-performing hypothesis with +0.04 pp macro-F1 and positive deltas
across all 4 classes. This is encouraging directionally — trainer-horse style alignment
and distance change do add marginal signal. However, +0.04 pp in the leaky regime
(where the total F1 is ~94 %) corresponds to an even smaller delta in the honest regime
(~47 % macro-F1 baseline). The improvement is 10-20× below any threshold for production
change justification given the cost of feature engineering and retraining.

---

## Summary Table

| Hypothesis         | Description                                    | Δacc (pp) | Δmacro-F1 (pp) | Δnige (pp) | Δsenkou (pp) | Δsashi (pp) | Δoikomi (pp) | Verdict   |
| ------------------ | ---------------------------------------------- | --------- | -------------- | ---------- | ------------ | ----------- | ------------ | --------- |
| H1_jockey_self     | Jockey RS × self RS (product + diff, 4 styles) | -0.05     | -0.04          | -0.01      | -0.04        | -0.06       | -0.08        | **ABORT** |
| H2_sire_self       | Sire RS × self RS (product + diff)             | +0.02     | +0.02          | +0.05      | -0.01        | +0.04       | +0.01        | **ABORT** |
| H3a_field_features | Field pressure features standalone             | ±0.00     | ±0.00          | -0.02      | +0.08        | -0.02       | -0.05        | **ABORT** |
| H3b_field_relative | Self RS / field pressure ratios                | -0.01     | -0.01          | -0.02      | +0.02        | +0.00       | -0.04        | **ABORT** |
| H4_draw_dist       | Draw × distance × nige tendency                | -0.003    | -0.02          | -0.06      | +0.02        | ±0.00       | ±0.00        | **ABORT** |
| H5_trainer_dist    | Trainer RS × self + dist_change × RS           | +0.03     | +0.04          | +0.05      | +0.03        | +0.02       | +0.04        | **ABORT** |

**Leaky-regime baseline for reference:** acc=0.9417, macro-F1=0.9493
(nige=0.9827, senkou=0.9406, sashi=0.9350, oikomi=0.9387)

**Honest OOS baseline (from rs-model-audit.md):** acc=48.35 %, macro-F1=0.465
(nige F1=0.377, senkou=0.467, sashi=0.428, oikomi=0.589)

---

## Analysis: Why All Hypotheses ABORT

### 1. Redundancy with existing features

The RS model already contains both terms of every interaction tested:

- H1: `jockey_nige_rate` AND `past_nige_rate_self` are both in the model. GBDT
  implicitly learns their interaction via joint tree splits (split on `jockey_nige_rate`
  at one level, then on `past_nige_rate_self` at the next). The multiplicative product
  adds no information GBDT cannot already represent.
- H2, H5: Same argument for sire/trainer × self interactions.
- H3: `self_nige_rate_minus_field_avg` already captures the nige-vs-field differential.
  The ratio form adds no new monotone ordering beyond the difference.
- H4: `umaban_norm`, `kyori`, `track_bias_inside`, `track_bias_front` are all present.
  `inside_short = (1-umaban_norm) / (kyori/1000)` achieves high gain (22k) because it
  is a compact summary — but GBDT already creates an equivalent representation via
  hierarchical splits on both features, so adding the product doesn't improve OOS accuracy.

### 2. Label is a continuous quantity bucketed to 4 classes

`target_running_style_class` is derived from `corner1_norm` via fixed thresholds:

- 0.0 → nige, (0.0, 0.30] → senkou, (0.30, 0.70] → sashi, >0.70 → oikomi.

The label is **within-race ordinal** — the class boundary is determined by relative
position, not absolute pace. Interaction features that capture "style tendency" add
signal about expected ordinal position, which the existing features already encode via
`past_corner_1_norm_avg_{3,5,10}` and `past_{nige,senkou,sashi,oikomi}_rate_self`.

### 3. Marginal signal already absorbed by `rs_p_*` (in leaky regime)

In the feat-v15-rs parquet (and therefore in this probe), `rs_p_nige/senkou/sashi/oikomi`
are present. These are the model's own predictions from a previous version — they
already encode all interaction effects learnable from prior RS model runs. Any new
interaction that improves prediction of `target_running_style_class` should improve
it in the honest (no `rs_p_*`) regime, not the leaky regime. Since the leaky-regime
deltas are noise-level, the honest-regime deltas would be at most comparable.

### 4. Best candidate: H5 trainer × self alignment (+0.04 pp)

H5 is the only hypothesis with consistent positive deltas across all 4 classes in the
leaky probe. The most informative new feature is `t_x_nige = trainer_nige_rate × past_nige_rate_self`
(gain 4772). This is directionally correct: when a trainer who favors front-running
pairs with a horse that also has a front-running history, the combination is a stronger
nige signal. However, +0.04 pp macro-F1 in the leaky regime is well below the
minimum threshold for production consideration (typically ≥1 pp honest OOS lift).

---

## Conclusion

**All 5 hypotheses ABORT.** The running-style model has reached feature saturation
on currently available structured data. The 5 hypotheses represent the strongest
multi-column relationship candidates (jockey/sire/trainer×self alignment, field-relative
ratios, draw×distance interactions) and none produces meaningful improvement.

The fundamental constraint (from rs-model-audit.md §4):

> **The biggest unused pre-race signal is per-horse sectional / halon split times
> (true per-horse pace tendency). It is the only signal that could move the middle
> classes and is structurally absent from the source data (JV-Data / NV-Data).**

Until per-horse fractional times become available, the RS model is at the empirical
feature frontier for existing data. The v3 architecture (21-year training window,
v2 field features, inverse-frequency weighting) already extracts the available signal.

### DO NOT RETEST list (updated)

The following interaction feature families are ABORT and should not be revisited
unless new raw data (per-horse sectional times, pre-race video analysis, etc.) is added:

| Family                                        | Reason for ABORT                                                  |
| --------------------------------------------- | ----------------------------------------------------------------- |
| Jockey RS × self RS (product, diff)           | Redundant; GBDT captures via joint splits                         |
| Sire RS × self RS (product, diff)             | Marginal (+0.02 pp); sub-noise-threshold                          |
| Trainer RS × self RS (product, diff)          | Best candidate (+0.04 pp); still sub-threshold                    |
| Field pressure ratios (self/field, dominance) | Captured by `self_nige_rate_minus_field_avg`                      |
| Draw × distance × nige tendency               | Captured by `umaban_norm` + `kyori` + `track_bias_*`              |
| Official `kyakushitsu_hantei` history         | Partial ρ 0.074 < 0.08, see h-official-running-style.md           |
| NAR 43/44 RS null imputation                  | Native NULL routing wins, see d2a-locality-feature-feasibility.md |

---

## Methodology Notes

- **Probe parquet:** `tmp/feat-v15-rs/jra`, race_year 2019-2025 (276,143 labeled rows)
- **DB access:** Read-only via DuckDB postgres_attach (`:***@` masked); host=127.0.0.1 port=15432
- **Probe scripts:** `/tmp/rs_probe_v3.py`, `/tmp/rs_probe_h3.py` (not committed, throwaway)
- **No production changes** of any kind
- **No data modifications** to DB or parquets
- **git add** of tmp/ files prohibited per project rules

## Hard Rules Observed

- No `git add tmp/` (probe scripts and JSONs untracked)
- PG read-only throughout (no writes)
- No production deployment or model registry change
- All DB connection strings masked as `:***@` in output
- Probe scripts written to `/tmp/` (not project tmp/)
