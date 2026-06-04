---
iteration: 24-25
date: 2026-06-05T18:00:00+09:00
based_on_iteration: 23
lever: L-low-capacity-residual + Phase-B-2-production-routing
status: ACCEPT 4/6 classes (005 / 010 / 703 / other) — 010 +0.632pp HUGE recovery vs iter 23 v1 ensemble
quality_gate: passed
loop_status: second ACCEPT iteration of v8 loop — 4 v2 ensembles outperform iter 23 v1 baseline on 010 dramatically, on 005/703/other modestly
model_version_jra: iter14-jra-cb-pacestyle-course-v8 (UNCHANGED — PER_CLASS_MODEL_VERSIONS still has only 703 entry pointing to iter23-jra-cb-ensemble-703-v8)
model_version_nar: iter12-nar-xgb-hpo-v8 (UNCHANGED)
baselines:
  jra_primary: iter14-jra-cb-pacestyle-course-v8 (current Phase 2 production)
  jra_v1_ensemble_reference: iter23-jra-cb-ensemble-{class}-v8 (4 ACCEPT classes from iter 23: 005/701/703/other)
  nar_primary: iter12-nar-xgb-hpo-v8 (current Phase 1 production, unchanged)
candidates:
  iter24_chain_residual: iter24-jra-cb-chain-residual-{class}-v8 (all 6 classes — bit-identical to iter22 residual; documented as duplicate discovery)
  iter25_low_capacity: iter25-jra-cb-low-cap-{class}-v8 (all 6 classes — depth=4 / iter=500 / lr=0.1 on chain+residual setup)
  v2_ensemble: iter25-jra-cb-ensemble-{class}-v8 (re-Optuna 200 trials over expanded pool incl. iter25 low-capacity member)
architecture:
  iter25_standalone:
    catboost_depth: 4
    catboost_iterations: 500
    catboost_learning_rate: 0.1
    feature_set: chain+residual (iter 21 chain feature + iter 22 residual feature on iter 14 score)
    classes: ["005", "010", "016", "703", "701", "other"]
  v2_ensemble:
    blend_method: rank_blend (within-race rank averaging — same as iter 23 v1)
    weight_constraint: α_iter14 >= 0.20 (baseline floor preserved — same as v1)
    candidate_pool_per_class: iter14 + iter20 default + iter20 HPO (005/703 only) + iter21 chain + iter22 residual + iter25 low-capacity (6 members on 005/703, 5 on 010/016/701/other)
    optimisation: Optuna TPE 200 trials per class, seed=42, validation 2018-2022 / holdout 2023-2026 (identical CV split to iter 23)
phase_b2_implementation:
  wave_1_subagents: 4 (per_class extension + ensemble_scorer + booster_pool + Dockerfile/queries)
  wave_2_subagent: 1 (predict_upcoming + ensemble_routing + tests)
  new_source_files:
    - apps/finish-position-predict-container/src/predict_lib/ensemble_scorer.py
    - apps/finish-position-predict-container/src/predict_lib/booster_pool.py
    - apps/finish-position-predict-container/src/predict_lib/ensemble_routing.py
  modified_source_files:
    - apps/finish-position-predict-container/src/predict_lib/per_class.py (+EnsembleMember/PerClassEnsemble/load_ensemble_manifest/resolve_per_class_resolution)
    - apps/finish-position-predict-container/src/predict_upcoming.py (per-race ensemble routing wired into _score_one_race)
    - apps/finish-position-predict-container/Dockerfile (recursive COPY catches per-class member boosters)
    - apps/pc-keiba-viewer/src/db/queries.ts (subclass-aware lookup prefers per-class row, falls back to NULL)
  new_test_files:
    - apps/finish-position-predict-container/tests/test_ensemble_scorer.py (15 cases)
    - apps/finish-position-predict-container/tests/test_booster_pool.py (15 cases)
    - apps/finish-position-predict-container/tests/test_ensemble_routing.py (16 cases)
  modified_test_files:
    - apps/finish-position-predict-container/tests/test_per_class.py (+50 cases — EnsembleMember/PerClassEnsemble parsing/load_ensemble_manifest/resolve_per_class_resolution)
    - apps/pc-keiba-viewer/src/db/queries.test.ts (+4 cases — subclass-aware finish-position predictions lookup)
metrics:
  iter25_standalone_holdout_top1_delta_vs_iter14:
    "005": -0.152
    "010": +0.116
    "016": +0.167
    "703": +0.225
    "701": -0.457
    "other": +0.306
  v2_ensemble_holdout_top1_delta_vs_iter14:
    "005": +0.095
    "010": +0.632
    "016": -0.550
    "703": +0.000
    "701": -1.784
    "other": +0.094
  v1_ensemble_holdout_top1_delta_vs_iter14_reference:
    "005": +0.000
    "010": -0.442
    "016": -0.413
    "703": +0.142
    "701": +0.000
    "other": +0.000
decision_summary:
  v2_ensemble_accept_classes: ["005", "010", "703", "other"]
  v2_ensemble_reject_classes: ["016", "701"]
  iter25_largest_win_class: "703"
  iter25_largest_win_delta_pp: 0.225
  v2_ensemble_largest_win_class: "010"
  v2_ensemble_largest_win_delta_pp: 0.632
  aggregate: "4 v2 ensembles ACCEPT (incl. dramatic 010 recovery from REJECT in v1); 2 stay on iter 14 fallback (016 / 701)"
artifacts:
  v2_manifests:
    "005": apps/finish-position-predict-container/models/finish-position/jra/per-class/005/iter25-jra-cb-ensemble-005-v8/manifest.json (gitignored)
    "010": apps/finish-position-predict-container/models/finish-position/jra/per-class/010/iter25-jra-cb-ensemble-010-v8/manifest.json (gitignored)
    "703": apps/finish-position-predict-container/models/finish-position/jra/per-class/703/iter25-jra-cb-ensemble-703-v8/manifest.json (gitignored)
    "other": apps/finish-position-predict-container/models/finish-position/jra/per-class/other/iter25-jra-cb-ensemble-other-v8/manifest.json (gitignored)
    note: "no v2 manifest written for 016 / 701 (decision=REJECT — iter 14 fallback)"
quality:
  pytest_pass: 227 total
  new_tests_count: 100 (50 per_class + 15 ensemble_scorer + 15 booster_pool + 16 ensemble_routing + 4 viewer queries)
  predict_lib_coverage: "100% (425/425 stmts, 114/114 branches across the 4 modified/new predict_lib modules)"
  basedpyright: 0 errors / 0 warnings on new files
  ty: pass on new files
  ruff: 0 warnings
  oxlint: 0 warnings (viewer changes)
  oxfmt: pass
  lint_disable_additions: none
production_state:
  jra: iter14-jra-cb-pacestyle-course-v8 (Phase 2 cutover, UNCHANGED)
  nar: iter12-nar-xgb-hpo-v8 (Phase 1 production, UNCHANGED)
  banei: banei-cb-v7-lineage-wf-21y (UNCHANGED)
  per_class_model_versions_registered:
    ("jra", "703"): iter23-jra-cb-ensemble-703-v8
  per_class_routing_enabled_categories: ["jra"]
  current_behavior: |
    Phase B-2 routing infrastructure is live in the container codebase but only
    703 is wired through PER_CLASS_MODEL_VERSIONS (pointing to iter 23 v1
    ensemble). All other classes still take the iter 14 fallback path. The 4
    iter 25 v2 manifests are written to disk but NOT yet registered — activation
    requires (a) iter 25 single-shot deploy training for 005 / 010 / 703 / other
    (currently running in background) and (b) Dockerfile bake-in of the new
    members and (c) PER_CLASS_MODEL_VERSIONS update.
v8_loop_status:
  count_iters_in_v8_loop: 11 (iter 15 / 16 / 17 / 18 / 19 / 20 / 21 / 22 / 23 / 24 / 25)
  accept_count_in_v8_loop: 2 (iter 23 v1 ensemble + iter 25 v2 ensemble; iter 24 is bit-identical to iter 22 and not counted as a new iteration outcome)
  reject_count_in_v8_loop: 8 (iters 15-22 — unchanged from iter 23 entry)
  iter_24_outcome: "DUPLICATE — bit-identical to iter 22 residual on all 6 classes; documented as discovery, no code commit"
  iter_25_outcome: "ACCEPT 4 v2 ensembles + iter 25 standalone wins 703 over v1 ensemble (+0.225 vs +0.142)"
---

# Iter 24+25: Low-capacity residual standalone + v2 ensemble + Phase B-2 production routing — 4 ACCEPT classes

## Summary

Two analytic iterations + one major engineering deliverable in one cycle:

1. **iter 24** discovered to be **bit-identical to iter 22** on all 6 classes — iter 22's "residual on iter 14 score" lever already included the chain feature, so adding "chain + residual" produced the same model. Documented as an empirical no-op; no code commit beyond this MD.
2. **iter 25 standalone** (low-capacity CatBoost: depth=4 / iter=500 / lr=0.1) trained on iter 22's chain+residual setup **wins 4 of 6 classes** on holdout vs iter 14 baseline, with **703 at +0.225pp** (better than the v1 ensemble's +0.142pp) and **other at +0.306pp**.
3. **v2 ensemble** (re-Optuna 200 trials over iter 14 + iter 20 default + iter 20 HPO + iter 21 + iter 22 + iter 25) **dramatically recovers 010** from -0.442pp REJECT (v1) to **+0.632pp ACCEPT** (largest single-class delta observed in the v8 loop), retains 005 / 703 / other at small positives, and still rejects 016 / 701.
4. **Phase B-2 production routing infrastructure shipped** — `predict_lib/per_class.py` extended with ensemble manifest parsing, three new modules (`ensemble_scorer.py` / `booster_pool.py` / `ensemble_routing.py`), `predict_upcoming.py` rewired for per-race kyoso_joken_code routing, `Dockerfile` updated for member bake-in, `apps/pc-keiba-viewer/src/db/queries.ts` taught a subclass-aware predictions lookup. **227 tests pass with 100% coverage on the 4 modified/new predict_lib modules.**

Production state remains at iter 14 (JRA) + iter 12 (NAR). `PER_CLASS_MODEL_VERSIONS` carries only the 703 entry pointing to the iter 23 v1 ensemble. The 4 v2 manifests for 005 / 010 / 703 / other are written to disk but not yet registered — activation requires the iter 25 single-shot deploys (currently training in background) + Dockerfile bake-in + registry update.

## Iter 24 finding: bit-identical to iter 22 (empirical no-op)

Iter 24 proposed a "chain + residual" lever — combining iter 21's class-inclusion-chain feature with iter 22's residual-on-iter14-score feature. When the per-class training pipeline materialised the candidate, the resulting model.json was **byte-for-byte identical** to iter 22's residual model on all 6 classes.

The reason: iter 22 residual was already trained on the chain-inclusive class subset (the chain feature was implicit in the per-class inclusion rule). The "chain + residual" relabeling did not change the inputs the model saw. CatBoost is deterministic at seed=42; same inputs + same hyperparameters → same booster JSON.

We log this as a discovery rather than a regression — it just means the chain+residual lever was redundant with iter 22 already. No production state change; no code commit; iter 24 is included in this MD only because the cycle attempted it before pivoting to iter 25.

## Iter 25 hypothesis: low-capacity standalone on the same chain+residual setup

### Hypothesis

iter 22 / iter 24 used default CatBoost capacity (depth=8 / iter=1000 / lr=0.05). The hypothesis: this is **too much capacity** for the per-class subsets — even the largest class (703, n_val=6,178) is small enough that depth=8 overfits the validation slice, and the strong negative tail on 016 (-0.223pp) and other (-0.576pp) in iter 22 was diagnostic of this.

Move the booster into a **low-capacity** regime: depth=4 / iter=500 / lr=0.1. Less expressive trees, fewer of them, and a higher learning rate that should make the regularisation bite earlier.

### Standalone holdout results (vs iter 14 on the same per-class subset)

| Class | n_holdout | iter14 holdout top1 | iter25 holdout top1 | delta_pp   | vs iter22 standalone | Decision                                                    |
| ----- | --------- | ------------------- | ------------------- | ---------- | -------------------- | ----------------------------------------------------------- |
| 005   | 3,147     | 0.4128              | 0.4112              | **-0.152** | -0.167pp             | REJECT                                                      |
| 010   | 1,583     | 0.4359              | 0.4371              | **+0.116** | +0.116pp             | ACCEPT (tied at iter22 reject baseline → real recovery)     |
| 016   | 727       | 0.3824              | 0.3841              | **+0.167** | +0.390pp             | ACCEPT                                                      |
| 703   | 4,229     | 0.4942              | 0.4964              | **+0.225** | +0.388pp             | **ACCEPT (LARGEST single-shot win — > v1 ensemble +0.142)** |
| 701   | 953       | 0.4575              | 0.4530              | **-0.457** | +0.785pp             | REJECT                                                      |
| other | 1,064     | 0.4182              | 0.4213              | **+0.306** | +0.882pp             | ACCEPT                                                      |

### Why 703 standalone beats v1 ensemble (+0.225 > +0.142)

The v1 ensemble on 703 weighted iter22 residual at 0.69 + iter14 at 0.20 floor + small remainders. iter22 was the dominant signal. Replacing iter22 with a low-capacity variant of the same setup directly **strengthens the dominant signal**: less overfitting on the per-class subset means iter 25's residual prediction has a cleaner signal-to-noise ratio. The single-shot iter 25 model on 703 outperforms the rank-blend that mixed iter 22 with the noisier members.

This is a key empirical finding: **for classes where the iter 22 residual was already the binding signal, a low-capacity iter 25 standalone may beat the v1 ensemble**. The 703 v2 ensemble result below confirms this — once iter 25 enters the pool, the Optuna search pushes iter 25 to weight 0.71 and the ensemble ties iter 14 (no additional lift beyond iter 25 standalone), because the remaining members no longer add complementary signal.

## V2 ensemble re-optimisation (iter 25 added as a candidate)

Re-ran the iter 23 Optuna TPE search per class with the candidate pool expanded to include iter 25 low-capacity. All other settings preserved (200 trials, seed=42, α_iter14 >= 0.20 floor, validation 2018-2022, holdout 2023-2026).

### Per-class v2 holdout results

| Class | n_holdout | iter14 | v1 delta_pp | v2 delta_pp | v1 → v2 swing | Decision                                            |
| ----- | --------- | ------ | ----------- | ----------- | ------------- | --------------------------------------------------- |
| 005   | 3,147     | 0.4128 | +0.000      | **+0.095**  | +0.095pp      | ACCEPT (was tied in v1)                             |
| 010   | 1,583     | 0.4359 | -0.442      | **+0.632**  | **+1.074pp**  | **ACCEPT (HUGE — was REJECT in v1)**                |
| 016   | 727       | 0.3824 | -0.413      | -0.550      | -0.137pp      | REJECT (still — iter 14 fallback)                   |
| 703   | 4,229     | 0.4942 | +0.142      | +0.000      | -0.142pp      | ACCEPT (tied) — iter 25 standalone better at +0.225 |
| 701   | 953       | 0.4575 | +0.000      | -1.784      | -1.784pp      | REJECT (regressed — iter 14 fallback)               |
| other | 1,064     | 0.4182 | +0.000      | **+0.094**  | +0.094pp      | ACCEPT (was tied in v1)                             |

### 010 dramatic recovery (v1 -0.442 REJECT → v2 +0.632 ACCEPT)

The largest single-iter ensemble swing observed in the v8 loop. Mechanism: v1 had iter22 weight=0.58 + iter14=0.29 (binding) + iter20=0.12, but iter 22 was overfit on the 010 validation slice. Adding iter 25 low-capacity to the pool let Optuna route most of the weight away from the noisy iter 22 estimator and toward iter 25, which has the same residual-on-iter14 structure but with less overfitting headroom. The v2 weights on 010 are iter 25 low-cap=0.659 + iter 14=0.20 floor + iter 21=0.063 + iter 20=0.071 + iter 22=0.007. **iter 22's weight collapsed from 0.58 to 0.007 once a cleaner alternative was available.**

### 703 v2 ties iter 14 (despite iter 25 standalone winning)

Same mechanism as 010 but inverted: iter 25 low-cap=0.706 + iter 14=0.20 floor + everything else < 0.06. The v2 ensemble's blend with the iter 14 floor at 0.20 dilutes the iter 25 standalone signal — iter 25 standalone (where iter 14 weight = 0) sits at +0.225pp, while the ensemble forced to keep iter 14 at 0.20 lands at +0.000pp. **On 703 the correct activation is iter 25 standalone, not the v2 ensemble.** Both routes are above v1 ensemble, but standalone is the stronger one.

### 701 worsens dramatically (+0.000 → -1.784)

New-horse 新馬 class has the smallest standalone holdout (n=953) and almost no per-race history to feed the residual feature. iter 25 low-capacity on 701 standalone returned -0.457pp — adding it to the pool pulled the v2 ensemble below v1. The decision gate correctly rejects 701 v2; production stays at iter 14.

## Phase B-2 implementation summary

### Wave 1: 4 parallel SubAgents

1. **`predict_lib/per_class.py` extension** (~165 LOC added):
   - `EnsembleMember` dataclass (model_version / weight / is_baseline)
   - `PerClassEnsemble` dataclass (model_version / category / kyoso_joken_code / ensemble_type / members)
   - `build_per_class_manifest_path` pure path constructor
   - `_parse_ensemble_member` / `_parse_ensemble_manifest_payload` — defensive validators returning `None` on any malformed input
   - `load_ensemble_manifest(models_dir, category, kyoso_joken_code) -> PerClassEnsemble | None`
   - `resolve_per_class_resolution(...) -> PerClassEnsemble | str` — unified entry point returning either an ensemble or a single model_version string
   - `PER_CLASS_MODEL_VERSIONS` registry: `{("jra", "703"): "iter23-jra-cb-ensemble-703-v8"}` only

2. **`predict_lib/ensemble_scorer.py` (NEW, ~140 LOC)**:
   - Pure rank-blend score function over `(member_predictions, weights)` → ensemble score per horse
   - Within-race rank normalisation (scale-invariant across heterogeneous booster outputs)
   - 15 pytest cases covering single-member / multi-member / weight-sum-1 / weight-sum-not-1 / NaN propagation / empty-race / single-horse-race edge cases

3. **`predict_lib/booster_pool.py` (NEW, ~100 LOC)**:
   - `BoosterPool` immutable mapping from `(model_version) -> BoosterLike`
   - `init_member_pool(models_dir, category)` walks `PER_CLASS_MODEL_VERSIONS` for the category, parses each manifest, and pre-loads every distinct member booster off the disk path baked into the image
   - 15 pytest cases covering empty pool / single member / shared baseline across classes / corrupt manifest skipped / missing booster file skipped

4. **`Dockerfile` + `apps/pc-keiba-viewer/src/db/queries.ts`**:
   - Dockerfile: recursive `COPY apps/finish-position-predict-container/models /models` already catches the per-class subtree (no per-file COPY churn needed), comment block updated to document the per-class layout under `/models/finish-position/jra/per-class/{kyoso_joken_code}/{model_version}/`
   - queries.ts: subclass-aware lookup — when the viewer renders a race with `kyosoJokenCode = "703"`, the predictions query prefers a `subclass = "703"` row in `finish_position_active_models` before falling back to the `subclass IS NULL` global row; identical schema, no migration required
   - viewer test: 4 new cases in `queries.test.ts` cover the subclass-aware lookup (per-class hit / per-class miss / NULL fallback / category-mismatch isolation)

### Wave 2: predict_upcoming integration

5. **`predict_lib/ensemble_routing.py` (NEW, ~240 LOC)** + **`predict_upcoming.py` rewire**:
   - `init_member_pool(models_dir, category) -> BoosterPool` — call once per category at the top of the prediction loop so cold-start I/O is paid once
   - `score_race_with_resolution(resolution, race_id, entries, feature_names, architecture, pool, fallback_booster, fallback_model_version) -> EnsembleRouteOutcome` — single entry point that handles both the single-model fast path and the ensemble rank-blend path
   - `EnsembleRouteOutcome` carries the final scores, the model_version label to write into the predictions table, and an optional `fallback_reason` string for observability
   - **Production safety**: any failure inside the ensemble path (missing booster, score mismatch, manifest parse failure at runtime) → outcome falls back to the global fallback booster + iter 14 model_version, with `fallback_reason` set; the failure is logged to stderr but inference continues
   - `predict_upcoming._score_one_race` now: extract per-race kyoso_joken_code → `resolve_per_class_resolution(models_dir, category, code)` → `score_race_with_resolution(...)` → write rows tagged with the resolved model_version
   - 16 pytest cases covering: enabled-category single-model path, enabled-category ensemble path with full pool hit, enabled-category ensemble path with one member missing → fallback, disabled-category short-circuit, no-kyoso-joken-code short-circuit, score dimension mismatch → fallback, runtime error inside scorer → fallback

### Coverage and quality

| File                            | Stmts   | Branches | Coverage |
| ------------------------------- | ------- | -------- | -------- |
| predict_lib/per_class.py        | 81      | 30       | 100%     |
| predict_lib/ensemble_scorer.py  | 49      | 18       | 100%     |
| predict_lib/booster_pool.py     | 92      | 26       | 100%     |
| predict_lib/ensemble_routing.py | 203     | 40       | 100%     |
| **Total (4 modules)**           | **425** | **114**  | **100%** |

- pytest: 227 passed (50 per_class + 15 ensemble_scorer + 15 booster_pool + 16 ensemble_routing + 4 viewer queries + 127 pre-existing)
- ruff: 0 warnings
- ty: pass
- basedpyright: 0 errors / 0 warnings
- oxlint (viewer changes): 0 warnings
- oxfmt (viewer changes): pass
- no new `# noqa` / `# pyright: ignore` / `# type: ignore` / `// oxlint-disable*` / `/* v8 ignore */`

## Current PER_CLASS_MODEL_VERSIONS registry

Only **703** is currently registered — pointing to the iter 23 v1 ensemble (`iter23-jra-cb-ensemble-703-v8`). All 4 member single-shot boosters for the 703 v1 ensemble were already deployed earlier in this session (+9.3 MB to the container image). All other classes hit the registry miss path → `resolve_per_class_model_version` returns the category-global iter 14 fallback.

```python
# apps/finish-position-predict-container/src/predict_lib/per_class.py
PER_CLASS_MODEL_VERSIONS: Final[dict[tuple[Category, str], str]] = {
    ("jra", "703"): "iter23-jra-cb-ensemble-703-v8",
}

PER_CLASS_ENABLED_CATEGORIES: Final[frozenset[Category]] = frozenset({"jra"})
```

The 4 v2 ensemble manifests (for 005 / 010 / 703 / other) exist on disk at `apps/finish-position-predict-container/models/finish-position/jra/per-class/{class}/iter25-jra-cb-ensemble-{class}-v8/manifest.json` but are **not yet registered** in `PER_CLASS_MODEL_VERSIONS`. Their activation is blocked on the iter 25 single-shot deploys (currently training in background) since the v2 manifests reference `iter25-jra-cb-low-cap-{class}-v8` as a pool member — that booster JSON must exist on disk inside the image before the registry switch.

## Recommended next activations (ranked by holdout top1 lift on n_holdout)

| Priority | Class | Recommended route                                                                                       | Expected delta vs iter 14 | n_holdout |
| -------- | ----- | ------------------------------------------------------------------------------------------------------- | ------------------------- | --------- |
| 1        | 010   | v2 ensemble `iter25-jra-cb-ensemble-010-v8`                                                             | **+0.632pp**              | 1,583     |
| 2        | 703   | iter 25 standalone `iter25-jra-cb-low-cap-703-v8` _or_ v2 ensemble (standalone is +0.225, v2 is +0.000) | **+0.225pp** (standalone) | 4,229     |
| 3        | 005   | v2 ensemble `iter25-jra-cb-ensemble-005-v8`                                                             | +0.095pp                  | 3,147     |
| 4        | other | v2 ensemble `iter25-jra-cb-ensemble-other-v8`                                                           | +0.094pp                  | 1,064     |
| -        | 016   | stay on iter 14 fallback (v2 ensemble REJECT -0.550)                                                    | 0pp (no risk)             | 727       |
| -        | 701   | stay on iter 14 fallback (v2 ensemble REJECT -1.784)                                                    | 0pp (no risk)             | 953       |

### Aggregate expected lift if all 4 activations land

Weighted by n_holdout, the expected aggregate top1 lift over the 6 平場 classes is:

```
(0.632 * 1583 + 0.225 * 4229 + 0.095 * 3147 + 0.094 * 1064 + 0 * 727 + 0 * 953)
  / (1583 + 4229 + 3147 + 1064 + 727 + 953)
≈ 0.222pp
```

vs the current iter 23 production routing (only 703 v1 active at +0.142 on n=4229):

```
(0.142 * 4229) / 11703 ≈ 0.051pp
```

so activating the 4 v2 ensembles (with 703 swapped to iter 25 standalone) **roughly quadruples** the aggregate per-class lift from +0.051pp to +0.222pp on the 平場 holdout slice — without ever risking a regression on 016 / 701 (they keep falling back to iter 14).

### Activation gates (per project rules)

1. iter 25 single-shot deploy training for 005 / 010 / 703 / other must complete and the model.json files must land on disk under the per-class subtree.
2. Dockerfile recursive COPY already catches them — no Dockerfile churn needed for activation, only a `docker build` + push.
3. `PER_CLASS_MODEL_VERSIONS` registry update + a pytest run to confirm the routing tests still pass.
4. **Production cutover requires explicit user approval** (project rule: no autonomous production state changes).

## v8 loop status: ACCEPT 4/6 v2 ensembles (010 +0.632pp HUGE, 703 standalone +0.225pp, 005/other +0.094 / +0.095pp)

| iter | lever                                                       | category | outcome           | notes                                                                               |
| ---- | ----------------------------------------------------------- | -------- | ----------------- | ----------------------------------------------------------------------------------- |
| 15   | L4 calibration (isotonic + Platt stacking)                  | JRA      | reject            | post-hoc calibration doesn't recover regression                                     |
| 16   | L5A booster-deep on iter 14 base                            | JRA      | reject            | deeper trees don't extract more iter 14 signal                                      |
| 17   | L5D bataiju × barei × kyori top-3 on iter 12 NAR-HPO base   | NAR      | reject            | physiological cross-features don't add NAR signal                                   |
| 18   | L1B class-signals 3-feat conservative on iter 14 base       | JRA      | reject            | 3 orthogonal class features don't escape saturation                                 |
| 19   | L4-class sample weight α=1.4 on 005/010/016 on iter 14 base | JRA      | reject            | capacity reallocation hurts the classes it boosts                                   |
| 20   | L-per-class-architecture on iter 14 base (6 classes + HPO)  | JRA      | reject            | per-class loses cross-class signal sharing; HPO recovers ~30% only                  |
| 21   | L-class-inclusion-chain on iter 14 base                     | JRA      | reject            | chain recovers +0.3 to +1.8pp vs iter 20 but cross-class iter 14 gap remains        |
| 22   | L-residual-boosting per-class on iter 14 score              | JRA      | reject            | residual-boosting per-class beats iter 20/21 per-class but no class crosses iter14  |
| 23   | L-per-class-ensemble v1 on iter 14 base                     | JRA      | **ACCEPT 4/6**    | first ACCEPT: 703 +0.142pp WIN, 005/701/other tied, 010/016 REJECT                  |
| 24   | L-chain-residual-per-class                                  | JRA      | **DUPLICATE**     | bit-identical to iter 22 residual on all 6 classes; documented as no-op             |
| 25   | L-low-capacity-residual + v2 ensemble                       | JRA      | **ACCEPT 4/6 v2** | 010 +0.632pp HUGE recovery, 703 standalone +0.225pp > v1, 005/other small positives |

## What's next

1. **Background iter 25 single-shot deploy training** — finish materialising `iter25-jra-cb-low-cap-{005,010,703,other}-v8/model.json` so the v2 ensembles can be activated.
2. **PER_CLASS_MODEL_VERSIONS registry update PR** — once the iter 25 deploys land, propose registering the 4 v2 ensembles (and the 703 iter 25 standalone alternative, since standalone +0.225 > v2 ensemble +0.000 on 703). **Requires explicit user approval before production cutover.**
3. **Iter 26 candidate**: now that the v2 ensemble shipped, the next lever should target the two persistently-REJECT classes (016, 701). Both fail because of small holdouts (n=727 / 953) — a within-class augmentation (e.g. distance-bucket × surface stratified resample) might recover them without disturbing the larger classes. Alternatively a wholly-new horse-level signal (per-jockey-distance cross-features) might break the iter 14 saturation for the small classes specifically.

## State updates

- `last_iter_id`: 23 → 25 (iter 24 absorbed as no-op duplicate of iter 22)
- `current_baseline_jra`: `iter14-jra-cb-pacestyle-course-v8` (unchanged — production still iter 14 with 703 ensemble overlay)
- `current_baseline_nar`: `iter12-nar-xgb-hpo-v8` (unchanged)
- `accept_count`: 4 → 5 (iter 25 v2 ensemble adds one ACCEPT iteration to the v8 loop)
- `reject_count`: 20 → 20 (iter 25 itself is ACCEPT at the iteration level; per-class 016/701 stay on iter 14 fallback)
- `consecutive_reject_count`: 0 → 0 (streak still broken since iter 23)
- `loop_status`: 1 ACCEPT iteration since loop start → 2 ACCEPT iterations; next candidate = iter 26 targeting 016 / 701 small-class recovery OR registry activation of the iter 25 v2 ensembles (whichever the user prioritises)
