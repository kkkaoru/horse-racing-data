# Relevance-Scheme Audit — Finish-Position Trainers

| Field   | Value                                                                                                                                                          |
| ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Date    | 2026-06-12                                                                                                                                                     |
| Status  | REFERENCE                                                                                                                                                      |
| Purpose | Inventory every trainer's relevance definition, verify I3-overlap scope, and enumerate implementable alternative schemes per framework for an experiment agent |

---

## 1. Per-Model Relevance Inventory

### 1-A. CatBoost — `finish_position_catboost.py` + `train_finish_position_catboost_walk_forward.py`

**Objective:** `YetiRank`  
**Eval metric:** `NDCG:top=3`  
**Label type used in training:** `np.int32`

**Relevance mapping (production defaults):**

| finish_position | relevance |
| --------------- | --------- |
| 1               | 3         |
| 2               | 2         |
| 3               | 1         |
| 4+, NULL, NaN   | 0         |

Implemented via `make_to_relevance(rank1, rank2, rank3)` which builds a `{1: rank1, 2: rank2, 3: rank3}` dict and returns `0` for all other positions.  
The three values are individually overridable at CLI via `--relevance-rank1/2/3`.

**Sub-4th ordering:** All positions >= 4 receive relevance = 0; the relative ordering of 4th, 5th, ... Nth is structurally discarded.

**Float relevance support:** CatBoost `YetiRank` accepts float labels in Pool. The trainer currently casts labels to `np.int32`, but removing the cast would enable float schemes without any framework-level blocker.

---

### 1-B. XGBoost — `finish_position_xgboost.py` + `train_finish_position_xgboost_walk_forward.py`

**Objective:** `rank:pairwise` (production default); `rank:ndcg` available via `--objective ndcg` in the walk-forward trainer (configures `lambdarank_pair_method=topk`, `lambdarank_num_pair_per_sample=3`)  
**Eval metric:** `ndcg@3`  
**Label type used in training:** `np.int32`

**Relevance mapping (production defaults):**

| finish_position | relevance                                              |
| --------------- | ------------------------------------------------------ |
| 1               | 3                                                      |
| 2               | 2                                                      |
| 3               | 1 (walk-forward trainer hardcodes `relevance_rank3=2`) |
| 4+, NULL, NaN   | 0                                                      |

Note: `finish_position_xgboost.py` (base module) defaults to `{1:3, 2:2, 3:1}` via `DEFAULT_RELEVANCE_RANK1/2/3`. The walk-forward orchestrator `train_finish_position_xgboost_walk_forward.py` overrides with `relevance_rank3=2` (line: `relevance_rank3=2` in `build_fold_namespace`), making the walk-forward training scheme `{1:3, 2:2, 3:2, else:0}`. **This means 2nd and 3rd place share the same relevance in the production walk-forward trainer.**

**Sub-4th ordering:** All positions >= 4 receive relevance = 0; relative ordering discarded.

**Float relevance support:** XGBoost `rank:pairwise` treats labels as relevance grades and accepts floats natively. `rank:ndcg` also accepts floats.

---

### 1-C. LightGBM (full model) — `finish_position_lightgbm.py` + `train_finish_position_lightgbm_walk_forward.py`

**Objective:** `lambdarank` (default); also supports binary variants (`binary-top1`, `binary-top3`, `binary-place2`, `binary-place3`)  
**Eval metric:** `ndcg@3`  
**Label type used in training:** `np.int64`

**Relevance mapping — named presets (in `RELEVANCE_TIER_PRESETS`):**

| preset name      | pos=1 | pos=2 | pos=3 | pos>=4 |
| ---------------- | ----- | ----- | ----- | ------ |
| `default`        | 3     | 2     | 1     | 0      |
| `place_weighted` | 2     | 3     | 2     | 0      |
| `sequence_aware` | 1     | 2     | 3     | 0      |

`default` is the production preset (`DEFAULT_RELEVANCE_TIER_NAME = "default"`).

The walk-forward trainer (`train_finish_position_lightgbm_walk_forward.py`) hard-codes `RELEVANCE_RANK1=3, RELEVANCE_RANK2=2, RELEVANCE_RANK3=1` and uses `lambdarank_truncation_level` (default 3, CLI-configurable via `--lambdarank-truncation-level`).

Also supports `rank_xendcg` objective via `--objective rank_xendcg`.

**Sub-4th ordering:** All positions >= 4 receive relevance = 0; relative ordering discarded (same as CatBoost and XGBoost).

**Float relevance support:** LightGBM `lambdarank` requires **non-negative integer** labels by default. Float grades are NOT natively supported with lambdarank; a `label_gain` parameter table can extend the integer grade set but has a maximum length constraint (the number of distinct integer relevance grades must fit within `label_gain`). See Section 3.

---

### 1-D. LightGBM Lite — `finish_position_lite_lightgbm.py`

**Objective:** `lambdarank`  
**Eval metric:** `ndcg@3`  
**Label type:** `np.int64`

**Relevance mapping:**

| finish_position | relevance |
| --------------- | --------- |
| 1               | 3         |
| 2               | 2         |
| 3               | 1         |
| 4+, NULL, NaN   | 0         |

Hard-coded `RELEVANCE_BY_RANK = {1: 3, 2: 2, 3: 1}`. No CLI override. This is a 25-feature inference-only model; not the primary accuracy target.

---

### 1-E. Race Set Transformer — `finish_position_transformer/training.py`

**Loss function:** Multi-task composite:
`L = w_top1*BCE(top1) + w_top3*BCE(top3) + w_place2*BCE(place2) + w_place3*BCE(place3) + w_cond_place2*BCE(cond_place2) + w_cond_place3*BCE(cond_place3) + w_pairwise*pairwise_ranking + w_listnet*listnet`

The `listnet` term uses `relevance = max(0, LISTNET_RELEVANCE_TIER_BASE - finish_position)` where `LISTNET_RELEVANCE_TIER_BASE = 4`, giving `{1→3, 2→2, 3→1, 4→0, 5+→0}` continuous-decay via softmax over scores.

The pairwise term directly compares raw `finish_position` values (smaller = better = higher label), not a discretized relevance grade.

**Sub-4th ordering:** The pairwise term respects raw `finish_position` for all positions > 0, so 4th vs 5th is NOT discarded in the loss — only the listnet component collapses pos>=4 to relevance 0. This makes the Transformer the **only** trainer that partially preserves sub-4th pairwise ordering.

**Float relevance:** N/A — the Transformer uses raw float `finish_position` directly.

**Deployment status:** SUPERSEDED by GBDT (MLX Set Transformer underperforms active GBDT on JRA; NAR untested but deprioritized per project memory `project_mlx_transformer_status`).

---

## 2. I3 Overlap Verdict

**I3 reference doc:** `rootcause-i3-objective-alignment.md` (2026-06-11, CLOSED)

### What I3 actually tested

I3 tested four objective variants on NAR XGBoost with the following relevance schemes:

| ID  | Objective                           | Relevance scheme          |
| --- | ----------------------------------- | ------------------------- |
| A   | rank:pairwise (production baseline) | `{1→3, 2→2, 3→1, else→0}` |
| B   | rank:pairwise + place-boosted       | `{1→2, 2→3, 3→3, else→0}` |
| C   | rank:ndcg, eval_metric=ndcg@2       | `{1→3, 2→3, 3→1, else→0}` |
| D   | rank:pairwise, set-membership       | `{1→1, 2→1, 3→1, else→0}` |

All four schemes share the following property: **positions >= 4 uniformly receive relevance 0**.

None of the four I3 schemes assigned any non-zero relevance to positions 4, 5, ..., N. The only variation across schemes was how the top-3 grades `{1st, 2nd, 3rd}` were weighted relative to each other.

### Is sub-4th graded relevance genuinely untested?

**Yes. Sub-4th graded relevance is genuinely untested.**

I3 conclusively exhausted the search space of re-weighting grades within `{3, 2, 1, 0}` (the existing 4-tier scheme). It did NOT test:

- Assigning relevance > 0 to positions 4, 5, 6, ..., N (scheme B or C in Section 3)
- Assigning distinct non-zero relevance values to 4th vs 5th vs 6th (graded tail)
- Continuous decay relevance `f(pos, field_size)` that extends beyond position 3

I3's conclusion ("production NDCG@3 relevance scheme is already optimal among tested alternatives") applies only to **re-weighting of top-3 tiers**. The structural question of whether adding signal to positions >= 4 would lift accuracy — specifically by narrowing the gap between "correctly ranked 3rd" and "correctly ranked 4th through Nth" in the pairwise loss — was not addressed and remains empirically open.

---

## 3. Per-Framework Implementable Schemes and Risks

Three candidate alternative schemes are defined here for an experiment agent:

### Scheme B — Extended Integer Grades

```
pos=1 → 7
pos=2 → 6
pos=3 → 5
pos=4 → 4
pos=5 → 3
pos in [6,8] → 2
pos in [9,12] → 1
pos >= 13 → 0
```

### Scheme C — Continuous (N-normalised) Decay

```
relevance(pos, N) = max(0, (N - pos + 1) / N)
```

where N = `shusso_tosu` (field size). This is a continuous float in [0, 1].

### Scheme D — Hybrid (top-3 preserved + epsilon graded tail)

```
pos=1 → 3
pos=2 → 2
pos=3 → 1
pos=4 → 0.1
pos=5 → 0.08
pos in [6,8] → 0.05
pos in [9,12] → 0.02
pos >= 13 → 0
```

Preserves the existing top-3 structure and adds small epsilon relevance to tail positions to break the flat zero plateau.

---

### 3-A. CatBoost YetiRank

| Scheme               | Implementable? | Notes                                                                                                                                   |
| -------------------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| B (extended integer) | YES            | Remove `np.int32` cast in `make_to_relevance`; pass `np.float32` or integer values. YetiRank accepts non-negative float labels in Pool. |
| C (continuous float) | YES            | YetiRank accepts float labels natively. Relevance must be non-negative. Field size `shusso_tosu` is available in the parquet.           |
| D (hybrid float)     | YES            | Same as C. No integer-only constraint.                                                                                                  |

**Risk:** YetiRank optimises a stochastic approximation of NDCG. Extending relevance to positions >= 4 increases gradient signal for tail positions. This may hurt top-3 metrics if the gradient energy shifts from top-3 pairs to lower pairs. Recommend using `eval_metric=NDCG:top=3` to detect this.

---

### 3-B. XGBoost rank:pairwise / rank:ndcg

| Scheme               | Implementable? | Notes                                                                                                                                                                      |
| -------------------- | -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| B (extended integer) | YES            | `DMatrix` label accepts float/int. `rank:pairwise` uses label as relevance grade for pair direction.                                                                       |
| C (continuous float) | YES            | Both `rank:pairwise` and `rank:ndcg` accept float labels. Note: `rank:pairwise` treats label differences as tie-break signals; continuous float makes every pair relevant. |
| D (hybrid float)     | YES            | Same.                                                                                                                                                                      |

**Constraint for `rank:ndcg`:** The NDCG computation uses the label values directly as relevance grades. With continuous float, the ideal DCG denominator is well-defined but may produce different optimisation dynamics than the production scheme. `lambdarank_pair_method=topk` + `lambdarank_num_pair_per_sample=3` is recommended for efficiency.

**Risk specific to `rank:pairwise`:** The objective treats every pair (i, j) where label(i) != label(j) as a training signal. With Scheme C, nearly every pair has a distinct label, multiplying the gradient cost by O(N^2) rather than O(top-k pairs). Memory and compute budget should be verified. Use `rank:ndcg` with topk pairing to control this.

---

### 3-C. LightGBM lambdarank

| Scheme                     | Implementable?                                | Notes                                                                                                                                                                                                                                                                                |
| -------------------------- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| B (extended integer)       | YES, but requires `label_gain` extension      | LightGBM lambdarank treats labels as integer grade indices into a `label_gain` lookup table. The default table covers grades 0–4. With scheme B (grades 0–7), the `label_gain` parameter must be set to a list of 8 values. The parameter length must exactly match `max_label + 1`. |
| C (continuous float)       | NO for lambdarank directly                    | LightGBM lambdarank requires non-negative integer labels. Float labels cause a silent truncation to int or an error depending on LGB version.                                                                                                                                        |
| C via `rank_xendcg`        | POTENTIALLY YES                               | `rank_xendcg` is an alternative XE-NDCG objective that may accept float relevance. Requires empirical verification; not currently documented in the codebase.                                                                                                                        |
| D (hybrid with float tail) | NO for lambdarank; needs integer-only version | If epsilon values are rounded to small distinct integers (e.g. `{1→6, 2→5, 3→4, 4→3, 5→2, 6–12→1, 13+→0}`) this becomes scheme B with a 7-tier table — valid for lambdarank.                                                                                                         |

**`label_gain` constraint detail:**  
With grades 0–7 (Scheme B), the parameter must be:

```python
"label_gain": [0, 1, 3, 7, 15, 31, 63, 127]  # DCG-style; or any monotone sequence
```

The length MUST equal the number of distinct grade levels (8 for grades 0–7). If `label_gain` is shorter than `max_label + 1`, LightGBM raises a runtime error. The existing `eval_at=[3]` NDCGeval is independent of `label_gain` and continues to work correctly.

**`lambdarank_truncation_level` interaction:**  
The current default is `lambdarank_truncation_level=3`. With extended grades, increasing this to 5 or 7 may be beneficial — the truncation level limits which positions contribute to the lambdarank gradient. Increasing it with sub-4 relevance enables the gradient to include 4th and 5th place pairs.

---

## 4. Recommended Scheme Order for the Experiment Agent

Priority ranking based on (a) lack of tested overlap with I3, (b) framework compatibility without major refactoring, (c) expected gradient signal:

1. **CatBoost Scheme B (extended integer, YetiRank)** — Highest priority.  
   YetiRank + integer grades {7,6,5,4,3,2,1,0} requires only changing the `make_to_relevance` return dict and removing the `int32` cast. No framework constraint. Tests the genuinely untested "sub-4 graded" hypothesis on the JRA production model (iter14-jra-cb). Expected: moderate gradient shift; low regression risk on top-3 because 1st/2nd/3rd grades still dominate by gap (7 vs 4 for 4th).

2. **XGBoost Scheme B (extended integer, rank:pairwise, NAR production model)** — Second priority.  
   NAR uses XGBoost as the base model (`finish_position_xgboost.py`). Same change as CatBoost Scheme B but against `train_xgboost_ranker`. Confirms whether sub-4 grading helps across both architectures.

3. **LightGBM Scheme B (extended integer with `label_gain`, lambdarank)** — Third.  
   Requires adding `label_gain` parameter. The LGB walk-forward trainer is already used for NAR class-C ensembles (iter36). Test at class level first. The `lambdarank_truncation_level` should be increased to 5 or 7 simultaneously. Higher complexity / more moving parts than CatBoost or XGBoost.

4. **CatBoost Scheme D (hybrid float with epsilon tail, YetiRank)** — Fourth.  
   Scheme D is a softer version of B: preserves `{3,2,1}` for top-3, adds small float epsilons for 4–12. Float labels are natively supported. This is a lower-risk variant of Scheme B, useful if Scheme B causes top-1 regression and the team wants to probe whether a smaller sub-4 relevance signal suffices.

5. **XGBoost Scheme C (continuous float, rank:ndcg + topk pairing)** — Fifth, exploratory.  
   Fully continuous decay via `(N - pos + 1) / N`. Most novel scheme; tests whether field-size-normalised relevance distributes gradient more equitably. Requires switching to `rank:ndcg` to control pairing cost. Risk: changes the effective gradient weight on every pair; harder to interpret.

6. **LightGBM `rank_xendcg` with float relevance** — Lowest priority, requires feasibility check.  
   It is unclear from the current codebase whether `rank_xendcg` accepts float labels without truncation. Requires a quick empirical test (20k rows, single fold) before committing to a full walk-forward run.

---

## 5. Appendix — Scheme Applicability Matrix

| Framework             | Scheme B int                | Scheme C float     | Scheme D hybrid   | Notes                                                   |
| --------------------- | --------------------------- | ------------------ | ----------------- | ------------------------------------------------------- |
| CatBoost YetiRank     | YES                         | YES                | YES               | Remove int32 cast; float labels native                  |
| XGBoost rank:pairwise | YES                         | YES (risk O(N²))   | YES               | No constraint; float native                             |
| XGBoost rank:ndcg     | YES                         | YES (topk pairing) | YES               | Preferred over pairwise for float schemes               |
| LightGBM lambdarank   | YES (need label_gain table) | NO                 | NO (round to int) | Integer-only; label_gain length == max_grade+1          |
| LightGBM rank_xendcg  | LIKELY YES                  | UNKNOWN            | UNKNOWN           | Requires feasibility test                               |
| Transformer (MLX)     | N/A                         | N/A                | N/A               | Uses raw finish_position; not a relevance-scheme target |

---

## 6. Key Constraints Checklist for the Experiment Agent

- **Do not lower `lambdarank_truncation_level` when adding sub-4 relevance in LightGBM.** Increase it to >= 5 so the gradient actually reaches 4th and 5th place pairs.
- **Baseline must be walk-forward, not a single held-out year.** I3's false-positive lesson (`rootcause-i3-objective-alignment.md`, "single-test-year transfer guard false-positive") applies here: a single held-out year is too small to distinguish noise from signal.
- **Accept gate is 4-axis multi-metric (`{top1, place2, place3, top3_box}`) with >= 2 positive and >= 1 of `{place2, place3}` positive.** No-regression threshold: -0.05 pp per axis.
- **Coverage 95% must be maintained** on any source files modified in `apps/pc-keiba-viewer/`.
- **Do not add `label_gain` with an incorrect length** — LightGBM will raise a runtime error at training time.
