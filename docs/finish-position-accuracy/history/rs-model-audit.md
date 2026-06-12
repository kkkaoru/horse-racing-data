---
title: Running-Style (脚質) Model — Definitive Baseline + What's-Been-Tried Audit
date: 2026-06-12
scope: running-style v3 LightGBM classifier (jra/nar-running-style-lgbm-prod-v3)
judge_metric: PER-CLASS (per running-style) accuracy / precision / recall / F1
status: AUDIT (read-only). Establishes the leak-free baseline + DO-NOT-RETEST inventory
  so future RS-improvement hypotheses do not re-test dead ends.
---

## TL;DR

- **The model is a 4-class softmax LightGBM** predicting running style (nige / senkou /
  sashi / oikomi) from **pre-race** features. Label is derived from `corner1_norm`
  (normalized corner-1 position) of the CURRENT race.
- **Two accuracy regimes exist in tmp/ and they are NOT comparable.** The widely-cited
  **~94–95 %** numbers (`tmp/running-style-eval-{baseline,improved,...}`) are
  **LEAKY self-consistency** evals that score on features containing the realized
  corner positions / the model's own `rs_p_*` outputs. The **honest true-OOS** accuracy
  is **~48 % (JRA) / ~52 % (NAR)** — confirmed twice (stored v3 metadata + a fresh
  leak-free retrain). **Always quote the OOS regime.**
- **True-OOS per-class accuracy (2025 holdout, leak-free retrain):**
  - **JRA: 48.3 % overall, macro-F1 0.465.** Hardest class **nige** (P 0.346 / F1 0.377);
    easiest **oikomi** (F1 0.589).
  - **NAR: 52.3 % overall, macro-F1 0.510.** Hardest **nige by precision** (P 0.379) and
    **senkou by F1** (0.457); easiest **oikomi** (F1 0.660).
- **Biggest unused pre-race signal:** per-horse **sectional / halon split times** (true
  per-horse pace tendency). It is the only signal that could move the middle classes and
  is **structurally absent from the source** (confirmed multiple times). Everything
  derivable from `corner1_norm` is already in the model.
- **Top already-tried (DO NOT RETEST):** (1) official JRA running-style label
  `kyakushitsu_hantei` history → ABORT, ρ 0.074<0.08, 79.6 % redundant with
  `past_nige_rate_self`; (2) 21-year window extension → already shipped as v3 (the +9pp
  gain is spent); (3) race-internal field-pressure features (`field_*`) → already in
  production (v2 schema), and the 2024 single-fold field-only/constraint variants were
  net-neutral.

---

## 1. Architecture + Full Feature List

### Pipeline

```
finish_position_features_duckdb.py   →  Phase-A feature parquet (per race, per horse)
                                          incl. target_running_style_class label
running_style_field_features.py      →  enrich_dataframe_with_field_features()
                                          adds race-internal field_* pressure features
running_style_lightgbm.py            →  4-class softmax LightGBM (train / walk-forward)
score_running_style_local.py         →  raw 4-class softmax probs (no argmax)
running-style-*.ts (sync-realtime)   →  Worker inference (mirrors field_* at serve time)
insert_running_style_bucket_evaluation_row.py → PG bucket-sliced 4×4 confusion store
```

Production model version: `jra-running-style-lgbm-prod-v3` / `nar-running-style-lgbm-prod-v3`.
Inference is **R2-parquet-first** (happy path), PG build-and-put-back on miss.

### Model config (LightGBM, production v3)

| param                   | value                                                                     |
| ----------------------- | ------------------------------------------------------------------------- |
| objective               | `multiclass` (softmax), `num_class=4`                                     |
| metric                  | `multi_logloss`                                                           |
| num_leaves              | 63                                                                        |
| learning_rate           | 0.05                                                                      |
| min_child_samples       | 30 (metadata variant: 50)                                                 |
| lambda_l1 / l2          | 0.1 / 0.1 (metadata variant: 0.2 / 0.2)                                   |
| feature_fraction        | 0.8 (metadata variant: 0.85)                                              |
| bagging_fraction / freq | 0.8 / 1 (metadata variant: 0.6 / 5)                                       |
| num_iterations          | 2000–3000, early_stopping 100                                             |
| class weights           | **inverse-frequency** per sample (train only; valid uniform)              |
| training span           | JRA 2006-01-01→2026 (836,427 rows) · NAR 2005-01-01→2026 (2,240,086 rows) |
| feature_schema_version  | `v2` (with `field_*` features; `v1` = without, CLI fallback)              |

Transformer variant (`running_style_transformer.py`, race-set attention, padded to
MAX_RUNNERS, AdamW, 20 epochs) exists for **walk-forward only** and **lost to GBDT** on
JRA (per `project_mlx_transformer_status.md`). Not in production.

### Feature inventory — what the RS model trains on

**Categorical (LightGBM `category` dtype):**
`track_code`, `grade_code`, `keibajo_code`, `kyori_band` (sprint/mile/intermediate/long),
`season_band`, `is_newcomer_race`, `tenko_code`, `babajotai_code_shiba`,
`babajotai_code_dirt`, `seibetsu_code`, plus NAR-only `nar_subclass`, `kyoso_joken_code`.

**Field / race-internal pressure (`FIELD_FEATURE_COLUMNS`, computed from peers' PAST rates):**
`field_nige_pressure`, `field_senkou_pressure`, `field_sashi_pressure`,
`field_oikomi_pressure`, `field_pace_index`, `field_nige_candidate_count`,
`field_max/min/spread_past_corner_1_norm`, `field_has_pure_nige_horse`,
`field_avg_speed_index`, `field_top_speed_index`, `field_avg_past_first_3f`,
`field_avg_past_kohan_3f`, `field_avg_career_win_rate`,
`self_nige_rate_minus_field_avg`, `self_speed_index_vs_field_top`.

**Numeric — self running-style history (the dominant signal for this task):**
`past_corner_1_norm_avg_{3,5,10}`, `past_corner_progression_avg_5`,
`past_corner_1_norm_{std,best,worst,iqr}_5`,
`past_{nige,senkou,sashi,oikomi}_rate_self`,
`past_{nige,senkou,sashi,oikomi}_win_rate_self`,
`last_race_corner_1_norm`, `last_race_corner_progression`,
`horse_{distance,track,keibajo,grade}_corner_1_norm_avg`,
`past_dominant_label_consistency_5`.

**Numeric — career / weight / recent form:**
`speed_index_avg_5`, `speed_index_best_5`, `kohan3f_avg_5`, `corner_pass_avg_5`,
`career_win_rate`, `career_place_rate`, `career_top1_count`,
`same_{keibajo,distance,track,grade}_win_rate`, `days_since_last_race`,
`consecutive_race_count`, `weight_avg_5`, `weight_diff_from_avg`,
`last_race_finish_norm`, `last_race_margin_to_winner`, `finish_trend_5`,
`avg_finish`, `recent_finish`, `popularity_score`, `odds_score`, ... (full list in §App).

**Numeric — jockey / trainer style tendencies:**
`jockey_{nige,senkou,sashi,oikomi}_rate`, `jockey_corner_1_norm_avg`,
`jockey_horse_corner_1_norm_avg`, `jockey_recent_{nige_rate,corner_1_norm_avg}_90d`,
`trainer_{nige,senkou,sashi,oikomi}_rate`, `trainer_corner_1_norm_avg`, + win-rate slices.

**Numeric — pedigree (gated ≥5 races, else NULL):**
`sire_{nige,senkou,sashi,oikomi}_rate`, `sire_corner_1_norm_avg`,
`sire_{distance,track}_win_rate`, `dam_sire_distance_win_rate`,
`pedigree_score_for_race`, ... .

**Numeric — race-level window ranks (added in final SELECT):**
`speed_index_avg_5_rank_in_race`, `jockey_recent_win_rate_rank_in_race`,
`pedigree_score_for_race_rank_in_race`, `same_distance_win_rate_rank_in_race`, and
`*_diff_from_race_avg` companions, plus `umaban_norm`, `shusso_tosu`,
`field_size_normalized`, `kyori`, `track_bias_inside`, `track_bias_front`.

### Pre-race signals NOT used / not available

| signal                                             | status                                                                                |
| -------------------------------------------------- | ------------------------------------------------------------------------------------- |
| **per-horse sectional / halon split times**        | **source absent** (biggest gap; only signal that could plausibly move middle classes) |
| odds time-series for NAR                           | source absent (NAR history zero)                                                      |
| race-level "1 nige per race" cap                   | **prohibited** by rule `feedback_no_race_level_nige_constraint` — do not add          |
| `nar_subclass` for JRA/Ban-ei                      | NULL by design                                                                        |
| current-race corner / finish / `target_*` columns  | **excluded as label-leak** (META + LABEL columns dropped)                             |
| current-race `kyakushitsu_hantei` (official style) | excluded as leak; historical fraction probed and ABORTed (see §4)                     |

---

## 2. Label & Class Balance

### `target_running_style_class` definition (from `corner1_norm`)

`corner1_norm` = position at corner 1 normalized by field size (0.0 = leader … 1.0 = last).

```
corner1_norm == 0.0                 → 0  nige   (逃げ, pure front)
0.0  < corner1_norm <= 0.30         → 1  senkou (先行, RUNNING_STYLE_SENKOU_THRESHOLD)
0.30 < corner1_norm <= 0.70         → 2  sashi  (差し,  RUNNING_STYLE_SASHI_THRESHOLD)
corner1_norm >  0.70                → 3  oikomi (追込, closer)
```

Rows with NULL `corner1_norm` (no corner data) → label NULL → dropped before training.

### Class balance (true-OOS 2025 holdout)

| class  | JRA true % | NAR true % |
| ------ | ---------- | ---------- |
| nige   | 8.5 %      | 10.1 %     |
| senkou | 26.1 %     | 23.6 %     |
| sashi  | 35.8 %     | 35.3 %     |
| oikomi | 29.5 %     | 31.0 %     |

The label is **ordinal** (running position), so errors concentrate in adjacent classes.
`sashi` (the largest middle class) is the natural majority-baseline; nige is the rarest.

---

## 3. Current Per-Class Accuracy (TRUE-OOS)

> **Method (leak-free, reproducible):** fresh 4-class softmax LightGBM trained on labeled
> rows **2016–2024**, scored on the **held-out 2025** rows the model never saw.
> Field features rebuilt via `enrich_dataframe_with_field_features` exactly as in
> production. Features restricted to the production metadata `feature_columns` with a
> **hard leak-guard** that drops `rs_p_*` (model's own outputs) and `target_corner_*`
> (realized current-race positions). Overall accuracy (JRA 48.3 % / NAR 52.3 %)
> reproduces the stored v3 walk-forward metadata (JRA ~48 % / NAR ~52 %), confirming
> validity. This is a single-fold lower bound (prod used a 21-y window); the per-class
> shape is the deliverable.

### JRA — overall acc **48.35 %**, macro-F1 **0.465** (n=36,263)

Confusion (rows = TRUE, cols = PRED, order nige/senkou/sashi/oikomi):

```
              pred:nige senkou  sashi oikomi
true nige        1279    1131    465    218
true senkou      1370    4613   2385   1111
true sashi        749    3379   5019   3844
true oikomi      302     1157   2618   6623
```

| class  | precision | recall | F1        | support |
| ------ | --------- | ------ | --------- | ------- |
| nige   | 0.346     | 0.414  | **0.377** | 3,093   |
| senkou | 0.449     | 0.487  | 0.467     | 9,479   |
| sashi  | 0.479     | 0.386  | 0.428     | 12,991  |
| oikomi | 0.561     | 0.619  | **0.589** | 10,700  |

### NAR — overall acc **52.30 %**, macro-F1 **0.510** (n=99,824)

```
              pred:nige senkou  sashi  oikomi
true nige        6272    2448    968    392
true senkou      5764   10765   5065   1982
true sashi       3404    8080  13956   9777
true oikomi      1091    2293   6351  21216
```

| class  | precision | recall | F1        | support |
| ------ | --------- | ------ | --------- | ------- |
| nige   | 0.379     | 0.622  | 0.471     | 10,080  |
| senkou | 0.456     | 0.457  | **0.457** | 23,576  |
| sashi  | 0.530     | 0.396  | 0.453     | 35,217  |
| oikomi | 0.636     | 0.685  | **0.660** | 30,951  |

### Reading the matrices

- **nige is the hardest class** (lowest precision in both: 0.346 JRA / 0.379 NAR). The
  model **over-predicts nige** (JRA predicts nige 3,700× vs 3,093 true; NAR 16,531× vs
  10,080 true) — inverse-freq class weighting inflates nige recall at the cost of
  precision. This is the single clearest per-class failure mode.
- **Middle classes confuse with their neighbours**: senkou↔sashi and sashi↔oikomi are the
  big off-diagonal masses. `sashi` has the worst recall (0.386 JRA / 0.396 NAR) — it gets
  pulled toward both neighbours. Far-corner confusion (nige↔oikomi) is small (the label is
  effectively ordinal).
- **oikomi is easiest** (F1 0.589 JRA / 0.660 NAR) — closers are the most positionally
  separable from the front.
- **NAR > JRA on every class** (more data, plus more determinate short-dirt front-running).

### Calibration (predicted-class prob, mean-pred vs empirical accuracy)

| bin       | JRA mean-pred / acc | NAR mean-pred / acc |
| --------- | ------------------- | ------------------- |
| [0.2,0.4) | 0.361 / 0.356       | 0.360 / 0.372       |
| [0.4,0.6) | 0.491 / 0.452       | 0.490 / 0.474       |
| [0.6,0.8) | 0.681 / 0.580       | 0.686 / 0.623       |
| [0.8,1.0) | 0.867 / 0.795       | 0.875 / 0.845       |

Well-calibrated at low confidence; **over-confident above 0.6** (~10 pp gap on JRA, milder
on NAR). A post-hoc temperature/isotonic calibration would tighten the high-prob bins —
**but note calibration does not change argmax, so it cannot raise per-class accuracy**
(it only fixes the probabilities, which matter for the downstream finish-position consumer).

---

## 4. WHAT'S BEEN TRIED — RS DO-NOT-RETEST Inventory

### Model lineage (shipped)

| version | category      | train rows              | span               | schema | note                                                                                             |
| ------- | ------------- | ----------------------- | ------------------ | ------ | ------------------------------------------------------------------------------------------------ |
| v1.5    | JRA / NAR     | 404,536 / 1,008,857     | 2016–2025          | v1     | no field features                                                                                |
| v2      | JRA           | 403,840                 | 2016–2025          | v2     | added `field_*`                                                                                  |
| **v3**  | **JRA / NAR** | **836,427 / 2,240,086** | **2006/2005–2026** | **v2** | **production**; +9.27pp JRA / +6.98pp NAR WF vs prior — gain came from the 21-y window extension |

### Experiments — outcomes

| #   | experiment                                                                      | artifact                                                            | what was tested                                                    | result                                                                                                                                                                                                                                  |
| --- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| E1  | **Official JRA running-style label** `kyakushitsu_hantei` (historical fraction) | `h-official-running-style.md`, `tmp/kyakushitsu/`                   | leak-free prior-race fraction of judge labels as orthogonal signal | **ABORT.** Best partial ρ 0.074 full / 0.068 holdout (bar 0.08). 79.6 % corr with `past_nige_rate_self`; official ≈ inferred (corner1_norm) label.                                                                                      |
| E2  | **21-year window extension**                                                    | v3 metadata                                                         | extend 2016- to 2005/6-                                            | **SHIPPED as v3** — this IS the +9pp gain. Re-extending is spent; no future-year headroom (trained through 2026).                                                                                                                       |
| E3  | **Field / race-internal features** (`field_*`)                                  | v2 schema, `tmp/running-style-eval-v2`                              | add peer-pressure features                                         | **SHIPPED (v2 schema).** WF v2 vs v1 baseline: ±0.02pp (self-consistency regime) — neutral-to-tiny on the leaky eval; kept for the finish-position consumer.                                                                            |
| E4  | **2024 single-fold variants**: `field-only`, `constraint-only`, `improved`      | `tmp/running-style-eval-{field-only,constraint-only,improved}`      | nige post-hoc constraint pushing nige precision to ~99.9 %         | **Net-neutral.** `improved`/`constraint-only` trade nige recall −1.3pp for precision; overall acc 94.80–94.85 % (leaky regime), no real gain. Constraint approach overlaps the **prohibited** race-level nige cap idea — do not pursue. |
| E5  | **Transformer (race-set attention)**                                            | `running_style_transformer.py`, `project_mlx_transformer_status.md` | set-attention over runners                                         | **LOST to GBDT** on JRA (Metal-fast but lower accuracy). NAR untried = the only transformer remnant of interest, low priority.                                                                                                          |
| E6  | **Leaky self-consistency eval**                                                 | `tmp/running-style-eval-{baseline,v1,improved}/report.json`         | (artifact, not an experiment)                                      | **DO NOT CITE as accuracy.** 94–95 % comes from scoring on `rs_p_*` / realized-corner features. A naive `resolve_feature_columns()` auto-pulls `rs_p_*` and yields a bogus 95.3 %. Always use the OOS protocol in §3.                   |

### Cross-project priors that bound RS improvement

- `feedback_no_race_level_nige_constraint` — any "1 nige per race" hard cap is **forbidden**
  (diverges from racing reality). Excludes the E4 constraint family from production.
- `project_science_track_saturation_2026_06_11` / `project_finish_position_frontier_2026_06_11`
  — per-horse halon/sectional source **absent**; odds-decoupling REJECTED; the finish-position
  frontier explicitly names "v3 running-style model extension (separate PJ)" as the **only**
  remaining avenue → this audit is that PJ's baseline.
- Lesson (finish-position D-phase): **imputing/flagging train-time-NULL features is
  counter-productive** — applies directly to the gated pedigree / NAR-NULL columns here.

### RS DO-NOT-RETEST (one-line)

1. Official `kyakushitsu_hantei` historical fraction (ABORT, redundant with corner1_norm).
2. Re-extending the training window (already v3; no future holdout left).
3. Race-level nige cap / nige-precision post-hoc constraint (prohibited + net-neutral).
4. Re-running the leaky self-consistency eval as if it were accuracy (it isn't).
5. Transformer on JRA (lost to GBDT) — NAR transformer is the only untried variant.

### Where the real headroom is (for future hypotheses, NOT yet tried)

- The error mass is **adjacent-class** (senkou↔sashi↔oikomi). Any new signal must
  **separate middle classes** — base-rate/style-history features are already saturated
  (E1 proved the corner1_norm bundle absorbs even the official label).
- **nige over-prediction** is a class-weight artifact: revisiting the inverse-frequency
  weighting / a nige-specific cost or an **ordinal** objective (vs flat softmax) is
  un-tried and directly targets the worst cells. (Hypothesis only — not yet evaluated.)
- A genuinely new **per-horse pace signal** (sectional times) would be the highest-value
  input but is **blocked on data availability**, not modeling.

---

## App. Method & reproducibility notes

- True-OOS computation: throwaway `tmp/rs_oos_confusion/run_fold.py` (NOT git-added),
  inputs `tmp/feat-v15-rs/{jra,nar}/race_year=*/...parquet`, outputs
  `tmp/rs_oos_confusion/result_{jra,nar}_2025.json`. 134 leak-free features
  (of 146 prod `feature_columns`; 12 absent from this older parquet build, handled by
  LightGBM as not-present — hence a slight lower bound vs the full prod feature set).
- Leak-guard: explicitly excluded `rs_p_{nige,senkou,sashi,oikomi}` and
  `target_corner_{1,3,4}_norm`. Without the guard, accuracy jumps to a bogus 95.3 %.
- Stored v3 metadata (`tmp/models/{jra,nar}-running-style-lgbm-prod-v3/metadata.json`)
  kept only nige P/R per walk-forward fold; this audit adds the full 4-class breakdown.
- All work read-only / tmp-only. No PG / registry / production writes.
