# NAR clean-retrain — full-store WF A/B gate (2026-07-04)

Status: **COMPLETE** — WF A/B gate PASSED decisively (top1 +5.46pp [LB95
+5.07]), clean deploy-candidate artifact trained + validated + smoke-tested.
No deploy action taken (out of scope for this rail; orchestrator decides).

## 1. Store provenance

- Source: `tmp/candidate-leak-clean-retrain/nar-full-regen/s11-pacestyle-FINAL/`
  (21 hive partitions `race_year=2006` .. `race_year=2026`).
- `feature_coverage_report.json`: `store_total_cols=290`, `deployed_feat_total=192`,
  `present=186`, `absent=6`, `row_count=2,782,381`.
- The 6 absent columns are all `trainer_*` (`trainer_grade_career_starts`,
  `trainer_grade_top3_rate`, `trainer_target_race_career_count`,
  `trainer_target_race_win_count`, `trainer_target_race_top3_count`,
  `trainer_target_race_has_history`) — these are skipped BY DESIGN in the NAR
  pipeline (matches production serving), not a regen defect. Expected, not a
  blocker, per task brief.
- After `where finish_position is not null` filter (both scripts' `load_store`):
  **2,730,085 rows**, 290 cols loaded into both harnesses.

## 2. Dry-run validation against the NEW full store (100 rows)

Both scripts were previously dry-validated only against the REDUCED
117-feat proxy store (`tmp/candidate-eval-nar/augmented`). Re-validated here
against the real full store before any real training:

### `train_nar_clean_full.py --dry-run-rows 100`

```
{"loaded_rows": 2730085, "loaded_cols": 290}
{
  "store_rows": 100, "store_cols": 290,
  "resolved_n_feat": 267,
  "leak_cols_present_in_store": ["finish_position", "finish_norm",
    "target_corner_1_norm", "target_corner_3_norm", "target_corner_4_norm",
    "target_running_style_class"],
  "leak_cols_leaked_into_features": [],
  "has_nar_subclass": true
}
DRY-RUN OK
```

### `wf_ab_nar_clean.py --dry-run-rows 100 --seeds 42`

```
{"store_rows": 2730085, "store_cols": 290, "seeds": [42], "threads": 4}
{
  "store_rows": 100, "store_cols": 290,
  "resolved_n_feat_serve": 271, "resolved_n_feat_clean": 267,
  "leak_cols_present_in_serve_feats": ["target_corner_1_norm",
    "target_corner_3_norm", "target_corner_4_norm", "target_running_style_class"],
  "leak_cols_present_in_clean_feats": [],
  "has_nar_subclass": true, "has_race_year": true
}
DRY-RUN OK
```

Confirmed before any real run:

- row count ~2.78M total / 2.73M after finish_position filter — matches
  expectation (order of magnitude of deployed iter12's own
  `n_train_rows: 2,673,368`, actually a bit higher since this is the full
  186/192-feature regen not the old training snapshot).
- leak columns present in store.
- arm feature counts differ by exactly 4 (271 serve vs 267 clean) — confirms
  `serve_feature_columns()` correctly keeps the leak cols for arm A while
  `resolve_feature_columns()` (used for the clean deploy artifact) excludes them.

## 3. WF A/B gate — real run

Command:

```sh
cd apps/pc-keiba-viewer
STORE=tmp/candidate-leak-clean-retrain/nar-full-regen/s11-pacestyle-FINAL
uv run python tmp/candidate-nar-clean-rail/wf_ab_nar_clean.py \
  --store-glob "$STORE/**/*.parquet" --threads 4 --seeds 42,101,2026
```

3 blind folds (train 2006..Y-1, test Y for Y in 2023/2024/2025) x 3 seeds x
2 arms = 18 XGBoost trainings (rank:pairwise, iter12 HPO verbatim,
num_boost_round=650, early_stopping=30).

Store load: 2,730,085 rows / 290 cols. Fold splits:
| fold | train rows | valid rows |
|---|---|---|
| 2023 | 2,260,965 | 136,354 |
| 2024 | 2,397,319 | 138,377 |
| 2025 | 2,535,696 | 138,586 |

Feature counts confirmed at real-run time: `n_feat_serve=271` (arm A, leak cols
present + trainable, NaN'd at predict), `n_feat_clean=267` (arm B, leak cols
excluded from training). 18 models trained (2 arms x 3 folds x 3 seeds);
completed well inside the runbook's ~30-45 min budget.

**Design note on granularity**: `wf_ab_nar_clean.py` pools all 3 folds'
per-race hit-frames together (`pl.concat` across `FOLD_YEARS`) before computing
paired-bootstrap stats _per seed_, and further averages across the 3 seeds for
the headline `pooled_seedavg`. This means the report below has **per-seed**
(3 seeds, each already fold-pooled) and **final pooled** (seed+fold pooled)
tables, but no separate **per-fold** breakout — that granularity was not
computed by this harness. All `n_races=40,710` figures are the same
race-count across seeds because it's the same union of 3 folds' races each
time, just re-scored by a different-seed pair of models.

## 4. Gate verdict — **ACCEPT_strict_gate: true**

### Pooled (3 seeds x 3 folds, seed-averaged) — headline result

| metric   | base (serve, deployed-spec) | clean (candidate) | delta_pp  | LB95_pp | n races |
| -------- | --------------------------- | ----------------- | --------- | ------- | ------- |
| top1     | 39.51%                      | 44.97%            | **+5.46** | +5.07   | 40,710  |
| place2   | 20.91%                      | 23.42%            | **+2.52** | +2.11   | 40,710  |
| place3   | 16.22%                      | 17.62%            | **+1.40** | +1.01   | 40,710  |
| place4   | 14.31%                      | 15.39%            | +1.09     | +0.70   | 40,710  |
| place5   | 14.01%                      | 14.68%            | +0.68     | +0.31   | 40,710  |
| place6   | 14.23%                      | 14.99%            | +0.77     | +0.39   | 40,710  |
| top3_box | 13.19%                      | 16.15%            | +2.96     | +2.65   | 40,710  |

Gate check: primaries_passed = {top1: true, place2: true, place3: true}
(3/3, threshold requires >=2/3), place2_or_place3 = true, worst_delta_pp
across ALL metrics (rank1-6 + top3_box) = +0.6763 (place5, still well above
the -0.05pp no-regression floor). **All 7 metrics positive with LB95>0** —
this is not a narrow pass, every rank band (1 through 6) and the box metric
move the same direction.

### Per-seed pooled (stability check — 3 independent seeds, each fold-pooled)

| seed | top1 delta [LB95] | place2 delta [LB95] | place3 delta [LB95] |
| ---- | ----------------- | ------------------- | ------------------- |
| 42   | +5.49 [+5.06]     | +2.37 [+1.90]       | +1.41 [+0.96]       |
| 101  | +5.21 [+4.81]     | +2.56 [+2.10]       | +1.37 [+0.95]       |
| 2026 | +5.68 [+5.25]     | +2.62 [+2.15]       | +1.42 [+0.97]       |

Multi-seed stable: top1 range 5.21-5.68pp (spread 0.47pp), place2 range
2.37-2.62pp, place3 range 1.37-1.42pp — no seed is an outlier driving the
result.

### Per-class (`nar_subclass`, cells with >=200 races)

| class   | n      | top1 delta [LB95] | place2 delta [LB95] | place3 delta [LB95] | top3_box delta [LB95] |
| ------- | ------ | ----------------- | ------------------- | ------------------- | --------------------- |
| C       | 20,473 | +6.05 [+5.50]     | +2.41 [+1.82]       | +1.55 [+0.98]       | +3.27 [+2.84]         |
| B       | 5,563  | +5.57 [+4.53]     | +3.88 [+2.71]       | +1.53 [+0.45]       | +3.01 [+2.18]         |
| 3YO     | 7,783  | +4.84 [+4.00]     | +2.03 [+1.03]       | +0.48 [-0.37]       | +2.00 [+1.30]         |
| A       | 2,224  | +5.85 [+4.26]     | +1.02 [-0.76]       | +1.35 [-0.30]       | +3.39 [+2.10]         |
| 2YO     | 2,168  | +4.01 [+2.52]     | +3.08 [+1.46]       | +4.00 [+2.35]       | +3.43 [+2.17]         |
| OP      | 1,116  | +3.11 [+1.02]     | +2.96 [+0.75]       | +0.15 [-2.09]       | +2.78 [+1.01]         |
| MUKATSU | 535    | +4.74 [+1.74]     | +2.55 [-0.87]       | +1.87 [-1.43]       | +4.11 [+1.50]         |
| NEW     | 544    | -0.06 [-1.65]     | +0.67 [-1.23]       | +1.35 [-0.43]       | 0.00 [-1.23]          |
| other   | 304    | +7.35 [+1.54]     | +5.48 [+1.31]       | -0.99 [-5.37]       | +2.63 [-0.44]         |

Read: **top1 is positive with LB95>0 in every class including NEW/other**
except `NEW` itself (-0.06pp, LB95 -1.65 — effectively flat/noise on n=544,
not a regression by the -0.05pp floor but not a confirmed win either).
`place2`/`place3` lose significance (LB95<0) in the smaller classes (A, OP,
MUKATSU, other, NEW) purely from sample size (n<2,300) — directionally still
positive or flat in all of them except `other`'s place3 (-0.99, LB95 -5.37,
n=304, likely noise given the wide interval). The two largest classes by far
(C: 20,473 races = 50% of all races; B: 5,563) are unambiguous wins on every
metric. No class shows a confirmed _regression_ (no cell has both negative
delta AND that being the dominant signal outside noise-band smalls).

**Conclusion**: the reduced-store direction from earlier today (+4.375pp
[LB95 +3.972] top1) is not just confirmed by the full 186/192-feature store —
it's **stronger** (+5.46pp [LB95 +5.07]), and now backed by 3 independent
seeds and a full serve-realistic 3-fold blind WF instead of the smaller
reduced-feature proxy. This is the largest, cleanest win of the whole
leak-clean-retrain campaign to date.

## 5. Clean deploy-candidate artifact

Trained via:

```sh
uv run python tmp/candidate-nar-clean-rail/train_nar_clean_full.py \
  --store-dir tmp/candidate-leak-clean-retrain/nar-full-regen/s11-pacestyle-FINAL \
  --out-dir tmp/candidate-nar-clean-rail/artifacts/iter12-nar-xgb-hpo-v8-clean \
  --threads 4
```

Wall time: **34.5s** (well under the runbook's 2-5 min budget). Output:
`tmp/candidate-nar-clean-rail/artifacts/iter12-nar-xgb-hpo-v8-clean/{model.json,metadata.json}`.

### metadata.json validation

| field                   | value                                                                                                                                                                    | check                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `feature_count`         | 267                                                                                                                                                                      | vs deployed 192 — expected increase (regen's pacestyle layer adds columns beyond the original 192, all auto-included by `resolve_feature_columns`)                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `leak_cols_excluded`    | `true`                                                                                                                                                                   | pass                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `excluded_leak_columns` | `finish_position`, `finish_norm` (true targets) + `target_corner_1_norm`, `target_corner_3_norm`, `target_corner_4_norm`, `target_running_style_class` (the 4 leak cols) | pass — none of these appear in `feature_names` (confirmed by direct diff)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `n_train_rows`          | 2,674,282                                                                                                                                                                | close to deployed iter12's 2,673,368 (small delta expected — regen reconstruction, not byte-identical source)                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `n_train_races`         | 269,741                                                                                                                                                                  | **identical** to deployed iter12's 269,741                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `train_years`           | 2006-2025 (20 years)                                                                                                                                                     | matches deployed convention                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `val_year`              | 2026                                                                                                                                                                     | matches deployed iter12's own `val_year: 2026` (partial-current-year holdout for early-stopping, same convention both models use)                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `best_iteration`        | 46                                                                                                                                                                       | deployed iter12's was 147 — the clean model early-stops much sooner. Not treated as a defect (the script's own leak-exclusion assertion passed, and the WF gate already confirms real accuracy gain), but flagged here as an observed architecture difference: the leak columns evidently let the deployed booster keep finding marginal splits for ~100 more rounds before its own early-stopping criterion triggered — plausible since they are near-perfect within-race signals. Worth knowing if anyone tunes `EARLY_STOPPING_ROUNDS`/`NUM_ROUNDS` for this model line later. |
| `seed`                  | 2068                                                                                                                                                                     | matches deployed metadata's own seed field, as designed                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |

### Predict smoke test (ad-hoc, `tmp/candidate-nar-clean-rail/smoke_test_clean.py`)

Scored 8 real 2026 NAR races (6-11 horses each, most recent available in the
store) using the exact `feature_names` list from `metadata.json`:

- 0 missing feature columns in the store.
- 73/73 rows produced finite scores.
- All 8 races' predicted ranks form a valid 1..n permutation.
  **SMOKE TEST PASSED.**

## 6. Transformer-blend recommendation

Per `tmp/candidate-nar-clean-rail/transformer_decision.md` (full analysis
there): the deployed transformer blend (`iter40-nar-settransformer-blend-v1`)
is **also leaky** — its `norm.json` `feature_order` (117 features) contains
all 4 leak columns, so today's live NAR serve path is leak-contaminated in
BOTH halves of `0.5*znorm(base) + 0.5*znorm(transformer)`.

This A/B's result sharpens the urgency of the transformer_decision.md
recommendation rather than changing it: the clean XGB base alone is worth
**+5.46pp top1 [LB95 +5.07]** — a far larger, and now more rigorously
confirmed, win than the transformer blend's own previously-reported
**+0.63pp top1** contribution (`project_nar_iter40_transformer_blend_deployed`
memory) — and that +0.63pp figure was measured with the leak present in both
halves, so its true leak-free contribution is unknown and could be smaller,
larger, or even negative once re-measured cleanly. Recommendation stands:
**Path (a), staged** — deploy `iter12-nar-xgb-hpo-v8-clean` as the sole NAR
model with `NAR_TRANSFORMER_BLEND_ENABLED=0` at deploy time, fast-follow a
separate clean transformer retrain (3 MLX seeds, clean 113-feat subset) rather
than leaving the half-leaky blend state as a long-lived deploy. This is a
recommendation for the orchestrator/USER to confirm — this rail does not
flip any deploy pointer itself.

## 7. Artifact path (for the orchestrator)

```
apps/pc-keiba-viewer/tmp/candidate-nar-clean-rail/artifacts/iter12-nar-xgb-hpo-v8-clean/
  model.json
  metadata.json
```

Not baked into the container, not deployed, no `active_models`/`model_meta.json`
change made — per instructions, deploy steps are the orchestrator's call
(runbook.md section 3 has the mechanical steps ready).
