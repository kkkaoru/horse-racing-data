# NAR clean-retrain — full-store WF A/B gate (2026-07-04)

Status: **COMPLETE** — two rounds. Round 1 (section 5) trained a 267-feature
clean artifact using every column the offline regen store happened to
contain; team-lead review caught a **serve-parity defect** before deploy: the
production predict-container only ever builds the deployed 192-feature NAR
schema at real serve time, so a 267-feature model would see ~75-79 features
go NaN in production — the same class of defect this whole campaign exists to
eliminate. Round 2 (section 5b) fixes this: **`iter12-nar-xgb-hpo-v8-clean188`**
uses exactly the deployed model's own 192 `feature_names` minus the 4 leak
columns (188 features, verified a byte-for-byte subset of the deployed list),
re-ran the WF A/B gate on this exact schema (PASSED, magnitude unchanged from
round 1), and is the artifact recommended for deploy. Round 1's 267-feature
artifact is kept in section 5 as a documented upper-bound / future
serve-chain-extension reference, NOT a deploy candidate. No deploy action
taken by this rail (out of scope; orchestrator decides).

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

## 5. [NOT a deploy candidate — see 5b] 267-feature artifact / store-schema upper bound

**Superseded by section 5b below.** This artifact and its WF A/B (section 4)
prove the leak-fix direction and magnitude are real, but the feature set
(267 = every numeric column the offline regen store contains) is **wider than
the deployed model's 192-name schema that production's predict-container
actually constructs at serve time**. Deploying this artifact as-is would
leave roughly 75-79 features NaN in real serving (the container's matrix
builder never grows past the 192 names in `model_meta.json`) — the identical
failure mode as the original leak defect this campaign exists to fix. Kept
here as evidence of the achievable ceiling if the serve pipeline is ever
extended to build a wider feature schema; not to be baked into the container.

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

## 5b. DEPLOY-GRADE serve-parity variant — `iter12-nar-xgb-hpo-v8-clean188` (THE deploy candidate)

### Why a second round was needed

Section 5's 267-feature artifact and its WF A/B (section 4) used
`serve_feature_columns()` / `resolve_feature_columns()`, which both take
**every numeric non-meta column present in the store** as the candidate
feature set. The full-regen store reconstructs the pacestyle layer with ~79
more engineered columns than the original deployed 192-name spec. Production's
predict-container never sees those extra columns — it builds exactly the
`feature_counts.nar` (192) schema from `model_meta.json` at serve time. A
model trained on 267 features would therefore get ~75-79 always-NaN features
in real serving, i.e. the same "trained on serve-unavailable features" defect
class as the original leak.

### Deploy-grade design

New harness `tmp/candidate-nar-clean-rail/wf_ab_nar_deploygrade.py` +
`train_nar_clean188_full.py` (parallel to the round-1 scripts, round-1 left
untouched for the record):

- **Feature list source**: the deployed `iter12-nar-xgb-hpo-v8` model's own
  `feature_names` (192, read directly from its `metadata.json`), not
  store-derived.
- **Arm A (`A_serve`)**: exactly those 192 names (leak cols present + trained
  on, NaN'd at predict time — matches today's live serving reality exactly).
- **Arm B (`B_clean188`)**: the same 192 names minus the 4 leak columns = 188.
  Verified by assertion at run time: `len(feats["serve"])==192`,
  `len(feats["clean188"])==188`, `clean188` is a strict subset of `serve`, no
  leak columns in `clean188`.
- **The 6 `trainer_*` names** (present in the deployed 192 list but absent
  from this regen store) are added to BOTH arms as literal always-null
  `Float64` columns rather than dropped. Per `feature_coverage_report.json`
  these are skipped by NAR's pipeline BY DESIGN — i.e. always-NaN at genuine
  serve too — so filling them null at train time is not an approximation, it
  IS the real serve condition. This also means `feats188` stays exactly
  192-4=188 (not 182), preserving strict schema parity with the deployed
  model's matrix builder.

Dry-run (100 rows) before the real run confirmed: `n_feat_A_serve=192`,
`n_feat_B_clean188=188`, `B_subset_of_deployed192=true`, leak cols present in
A / absent from B, the 6 `trainer_*` names correctly identified as
missing-from-store and null-filled.

### Deploy-grade WF A/B gate — **ACCEPT_strict_gate: true**

Same folds (2023/2024/2025), same 3 seeds (42/101/2026), same bootstrap
(paired, race-level, 2000 iters) as section 4 — only the feature-list
construction changed.

| metric   | base (serve, exact deployed-192) | clean188 (candidate) | delta_pp  | LB95_pp | n races |
| -------- | -------------------------------- | -------------------- | --------- | ------- | ------- |
| top1     | 39.23%                           | 44.73%               | **+5.50** | +5.10   | 40,710  |
| place2   | 20.92%                           | 23.28%               | **+2.36** | +1.92   | 40,710  |
| place3   | 16.18%                           | 17.54%               | **+1.36** | +0.97   | 40,710  |
| place4   | 14.25%                           | 15.43%               | +1.19     | +0.80   | 40,710  |
| place5   | 13.98%                           | 14.90%               | +0.92     | +0.56   | 40,710  |
| place6   | 14.18%                           | 14.91%               | +0.74     | +0.36   | 40,710  |
| top3_box | 13.10%                           | 16.06%               | +2.95     | +2.64   | 40,710  |

**Magnitude is essentially unchanged from the round-1 267-feature result**
(top1 +5.46 -> +5.50, place2 +2.52 -> +2.36, place3 +1.40 -> +1.36) — this
confirms the extra ~79 store-only columns in round 1 were NOT meaningfully
driving the win; the leak removal itself is the entire effect, and it
survives fully intact when restricted to the exact deployed 192-minus-leak
schema. Gate: primaries_passed = {top1: true, place2: true, place3: true},
worst_delta_pp across all 7 metrics = +0.7369 (place6), well above the
-0.05pp floor.

### Per-seed pooled

| seed | top1 delta [LB95] | place2 delta [LB95] | place3 delta [LB95] |
| ---- | ----------------- | ------------------- | ------------------- |
| 42   | +5.53 [+5.09]     | +2.36 [+1.87]       | +1.38 [+0.93]       |
| 101  | +5.47 [+5.06]     | +2.52 [+2.05]       | +1.35 [+0.90]       |
| 2026 | +5.50 [+5.07]     | +2.20 [+1.73]       | +1.35 [+0.90]       |

Multi-seed stable, tighter spread than round 1 (top1 range 5.47-5.53pp vs
round 1's 5.21-5.68pp).

### Per-class (`nar_subclass`, cells with >=200 races) — deploy-grade

| class   | n      | top1 delta [LB95] | place2 delta [LB95] | place3 delta [LB95] | top3_box delta [LB95] |
| ------- | ------ | ----------------- | ------------------- | ------------------- | --------------------- |
| C       | 20,473 | +6.04 [+5.50]     | +2.26 [+1.63]       | +1.19 [+0.63]       | +3.21 [+2.77]         |
| B       | 5,563  | +5.54 [+4.45]     | +3.24 [+2.09]       | +1.44 [+0.40]       | +3.20 [+2.34]         |
| 3YO     | 7,783  | +4.72 [+3.90]     | +2.24 [+1.31]       | +0.67 [-0.18]       | +1.83 [+1.15]         |
| A       | 2,224  | +5.98 [+4.36]     | +1.20 [-0.66]       | +3.07 [+1.39]       | +3.91 [+2.53]         |
| 2YO     | 2,168  | +4.04 [+2.64]     | +2.37 [+0.80]       | +3.40 [+1.77]       | +3.15 [+1.85]         |
| OP      | 1,116  | +4.75 [+2.54]     | +2.81 [+0.54]       | +2.21 [-0.21]       | +3.02 [+1.05]         |
| MUKATSU | 535    | +3.86 [+1.12]     | +1.87 [-1.43]       | +2.18 [-1.18]       | +4.61 [+1.87]         |
| NEW     | 544    | +0.92 [-0.37]     | +1.23 [-0.49]       | -0.25 [-1.84]       | +0.37 [-0.86]         |
| other   | 304    | +8.77 [+3.51]     | +5.59 [+1.10]       | 0.00 [-4.39]        | +2.74 [-0.33]         |

Same read as round 1: the two largest classes (C = 50% of races, B) are
unambiguous wins on every metric; smaller classes lose statistical
significance on place2/place3 purely from sample size, not from a sign
flip; `NEW` (n=544) is the weakest cell but now even top1 direction is
positive (+0.92, though LB95 still crosses zero) — slightly better than
round 1's flat -0.06 on the same class, likely just noise given both are
inside each other's interval on n=544.

### `iter12-nar-xgb-hpo-v8-clean188` artifact — full training + validation

Trained via:

```sh
uv run python tmp/candidate-nar-clean-rail/train_nar_clean188_full.py \
  --store-dir tmp/candidate-leak-clean-retrain/nar-full-regen/s11-pacestyle-FINAL \
  --deployed-metadata apps/finish-position-predict-container/models/finish-position/nar/iter12-nar-xgb-hpo-v8/metadata.json \
  --out-dir tmp/candidate-nar-clean-rail/artifacts/iter12-nar-xgb-hpo-v8-clean188 \
  --threads 4
```

Wall time: 29.9s. Output:
`tmp/candidate-nar-clean-rail/artifacts/iter12-nar-xgb-hpo-v8-clean188/{model.json,metadata.json}`.

| field                                        | value                                                                   | check                                                                                                                                                                                                                                                                                                            |
| -------------------------------------------- | ----------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `feature_count`                              | 188                                                                     | = deployed 192 minus 4 leak cols, exactly as designed                                                                                                                                                                                                                                                            |
| `leak_cols_excluded`                         | `true`                                                                  | pass                                                                                                                                                                                                                                                                                                             |
| `serve_parity_strict_subset_of_deployed_192` | `true`                                                                  | pass — script asserts this before writing the artifact (refuses to train otherwise)                                                                                                                                                                                                                              |
| feature_names ⊆ deployed 192                 | verified via direct set diff: `new_feats - deployed_feats = {}` (empty) | **PASS — confirmed independently, not just via the training script's own internal assertion**                                                                                                                                                                                                                    |
| leak cols in feature_names                   | `[]`                                                                    | pass                                                                                                                                                                                                                                                                                                             |
| `n_train_rows`                               | 2,674,282                                                               | same as round-1's 267-feat artifact (same store, same window)                                                                                                                                                                                                                                                    |
| `n_train_races`                              | 269,741                                                                 | **identical** to deployed iter12's 269,741                                                                                                                                                                                                                                                                       |
| `train_years`                                | 2006-2025 (20 years)                                                    | matches deployed convention                                                                                                                                                                                                                                                                                      |
| `val_year`                                   | 2026                                                                    | matches deployed iter12's own convention                                                                                                                                                                                                                                                                         |
| `best_iteration`                             | 62                                                                      | vs deployed iter12's 147 and round-1's 267-feat artifact's 46 — sits between the two, consistent with "leak columns let the model keep finding marginal splits longer, and more available features (188 vs 267) shifts this figure further" as a plausible but unconfirmed explanation. Not treated as a defect. |
| `seed`                                       | 2068                                                                    | matches deployed metadata's own seed field                                                                                                                                                                                                                                                                       |

### Predict smoke test (`tmp/candidate-nar-clean-rail/smoke_test_clean188.py`)

Scored 8 real 2026 NAR races (8-11 horses each) using the exact
`feature_names` from `metadata.json`, with the 6 `trainer_*` columns filled
null (matching real serve):

- `serve_parity_feature_names_subset_of_deployed_192`: **true** (independently
  re-verified inside the smoke test against the deployed metadata.json, not
  just trusted from training time).
- 79/79 rows produced finite scores.
- All 8 races' predicted ranks form a valid 1..n permutation.
  **SMOKE TEST PASSED.**

## 6. Transformer-blend recommendation

Per `tmp/candidate-nar-clean-rail/transformer_decision.md` (full analysis
there): the deployed transformer blend (`iter40-nar-settransformer-blend-v1`)
is **also leaky** — its `norm.json` `feature_order` (117 features) contains
all 4 leak columns, so today's live NAR serve path is leak-contaminated in
BOTH halves of `0.5*znorm(base) + 0.5*znorm(transformer)`.

This A/B's result sharpens the urgency of the transformer_decision.md
recommendation rather than changing it: the clean188 deploy-grade base alone
is worth **+5.50pp top1 [LB95 +5.10]** — a far larger, and now more rigorously
confirmed (serve-parity-exact), win than the transformer blend's own
previously-reported **+0.63pp top1** contribution
(`project_nar_iter40_transformer_blend_deployed` memory) — and that +0.63pp
figure was measured with the leak present in both halves, so its true
leak-free contribution is unknown and could be smaller, larger, or even
negative once re-measured cleanly. Recommendation stands: **Path (a),
staged** — deploy `iter12-nar-xgb-hpo-v8-clean188` as the sole NAR model with
`NAR_TRANSFORMER_BLEND_ENABLED=0` at deploy time, fast-follow a separate clean
transformer retrain (3 MLX seeds, clean 113-feat subset) rather than leaving
the half-leaky blend state as a long-lived deploy. This is a recommendation
for the orchestrator/USER to confirm — this rail does not flip any deploy
pointer itself.

## 7. Artifact paths (for the orchestrator)

**Deploy this one:**

```
apps/pc-keiba-viewer/tmp/candidate-nar-clean-rail/artifacts/iter12-nar-xgb-hpo-v8-clean188/
  model.json       (188 features, serve-parity-verified subset of deployed 192)
  metadata.json     (leak_cols_excluded=true, serve_parity_strict_subset_of_deployed_192=true)
```

**Reference only, do NOT bake into the container** (section 5's superseded
267-feature upper-bound artifact, kept for the record):

```
apps/pc-keiba-viewer/tmp/candidate-nar-clean-rail/artifacts/iter12-nar-xgb-hpo-v8-clean/
  model.json        (267 features — NOT deployable, serve-parity broken)
  metadata.json
```

Neither is baked into the container, neither is deployed, no
`active_models`/`model_meta.json` change made — per instructions, deploy
steps are the orchestrator's call (runbook.md section 3 has the mechanical
steps ready; substitute `-clean188` for `-clean` and `feature_counts.nar: 188`
for `192` throughout, since the runbook was written before the serve-parity
issue was found).
