# Graded Sub-4 Relevance Scheme Experiments

| Field   | Value                                                                                                                                |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Date    | 2026-06-13                                                                                                                           |
| Status  | COMPLETE — **NAR ADOPT superseded by full-system deploy judge REJECT** (see `nar-schemeD-deploy-judge.md`, commit 5e6186e)           |
| Purpose | Test whether extending relevance labels beyond rank 3 (sub-4 ordering) improves finish-position prediction across NAR / JRA / Banei. |

> **SUPERSESSION NOTE (2026-06-13, post-experiment)**: The NAR scheme-D ADOPT below measured the **base model alone**. The follow-up full-production-system judge (scheme-D base + retrained iter30/36 per-class residual ensembles, TRUE production baseline, 45,573 races 2023–2026) returned **REJECT**: f2p LB95 = −0.00481, 0/4 positive axes, veto-floor fail (top1 −0.40pp, place3 −0.43pp). Root cause: per-class residuals + blend weights are calibrated to iter12's score distribution; a scheme-D base shifts that distribution and the blended system regresses. **Production unchanged** (iter12-nar-xgb-hpo-v8 + iter30/36). Lesson re-confirmed: adoption decisions must be judged against the full serving system, not the base model in isolation.

---

## 1. Schemes Tested

| Scheme             | Definition                                                 | Framework compatibility           |
| ------------------ | ---------------------------------------------------------- | --------------------------------- |
| current (NAR)      | {1:3, 2:2, 3:2, 4+:0}                                      | XGBoost rank:pairwise             |
| current (CB)       | {1:3, 2:2, 3:1, 4+:0}                                      | CatBoost YetiRank                 |
| **D (hybrid)**     | {1:3, 2:2, 3:1, 4:0.1, 5:0.08, 6–8:0.05, 9–12:0.02, 13+:0} | Both (float labels)               |
| **C (continuous)** | rel(pos, N) = (N − pos + 1) / N                            | CatBoost only (NAR skip — see §2) |
| **B (extended)**   | {1:7, 2:6, 3:5, 4:4, 5:3, 6–8:2, 9–12:1, 13+:0}            | Both                              |

---

## 2. Protocol

- **Cheap filter**: single OOT split (train ≤ split_year, holdout = fold_years) — screens out clear failures
- **Walk-forward**: 3 folds (2023 / 2024 / 2025), retrain each fold, pool predictions
- **Accept gate**: pooled fukusho_2p LB95 (paired bootstrap, 10k iters, seed=42) > 0 AND ≥ 2 of 4 axes positive AND ≥ 1 of {place2, place3} positive AND veto floor ≥ −0.05pp
- **Serial execution**: NAR → JRA → Banei

**Scheme C skip for NAR**: XGBoost `rank:pairwise` with all-unique float labels generates O(N²) pairs. On 3M-row NAR data, training was infeasible within compute budget. Scheme C was tested only for JRA and Banei (CatBoost YetiRank handles float labels natively without the pairing explosion).

---

## 3. NAR Results (XGBoost rank:pairwise, iter12 params)

### 3-A. Cheap filter (train ≤ 2022, holdout 2023–2025, n=3,426,629 rows)

| Scheme  | top1   | place2 | place3 | top3_box | f2p    | Δtop1   | Δplace2 | Δplace3 | Δbox    | Filter       |
| ------- | ------ | ------ | ------ | -------- | ------ | ------- | ------- | ------- | ------- | ------------ |
| current | 0.5853 | 0.3526 | 0.2695 | 0.3466   | 0.8796 | —       | —       | —       | —       | —            |
| D       | 0.5861 | 0.3531 | 0.2745 | 0.3487   | 0.8832 | +0.0008 | +0.0004 | +0.0049 | +0.0022 | **PASS**     |
| C       | —      | —      | —      | —        | —      | —       | —       | —       | —       | SKIP (O(N²)) |
| B       | 0.5849 | 0.3522 | 0.2738 | 0.3477   | 0.8825 | −0.0005 | −0.0005 | +0.0042 | +0.0011 | **PASS**     |

Survivors: **[D, B]**

### 3-B. Walk-forward per-fold (folds 2023 / 2024 / 2025)

**Scheme D** (3 folds × ~230s training):

| Fold | Baseline top1 | D top1 | Δtop1   | Δplace2 | Δplace3 | Δbox    | Δf2p    |
| ---- | ------------- | ------ | ------- | ------- | ------- | ------- | ------- |
| 2023 | 0.5879        | 0.5901 | +0.0022 | +0.0026 | +0.0082 | +0.0062 | +0.0025 |
| 2024 | 0.5874        | 0.5857 | −0.0017 | −0.0003 | +0.0015 | −0.0012 | +0.0018 |
| 2025 | 0.5827        | 0.5845 | +0.0018 | −0.0012 | +0.0022 | +0.0012 | +0.0050 |

**Scheme B** (3 folds × ~165s training):

| Fold | Baseline top1 | B top1 | Δtop1   | Δplace2 | Δplace3 | Δbox    | Δf2p    |
| ---- | ------------- | ------ | ------- | ------- | ------- | ------- | ------- |
| 2023 | 0.5879        | 0.5887 | +0.0008 | +0.0011 | +0.0095 | +0.0049 | +0.0023 |
| 2024 | 0.5874        | 0.5863 | −0.0011 | +0.0004 | +0.0011 | −0.0007 | +0.0003 |
| 2025 | 0.5827        | 0.5845 | +0.0018 | −0.0013 | −0.0005 | +0.0012 | +0.0038 |

### 3-C. Pooled delta + bootstrap verdict

| Scheme | Δtop1   | Δplace2 | Δplace3 | Δtop3_box | Δf2p    | f2p LB95     | Verdict   |
| ------ | ------- | ------- | ------- | --------- | ------- | ------------ | --------- |
| D      | +0.0008 | +0.0004 | +0.0040 | +0.0021   | +0.0031 | **+0.00174** | **ADOPT** |
| B      | +0.0005 | +0.0000 | +0.0034 | +0.0018   | +0.0021 | **+0.00091** | **ADOPT** |

Both schemes improve **place3 (+0.0034–+0.0040)** and **top3_box** the most. The epsilon tail (sub-4 ordering) gives the model richer pairwise signal about mid-field placement, lifting place-accuracy without hurting top1.

D is the stronger scheme: larger place3/top3_box gains and higher f2p LB95. B produces slightly lower gains but still passes all gate conditions with positive LB95.

**Top-3 dilution check**: Both D and B keep top-3 labels at {3, 2, 1} unchanged (only sub-4 gets epsilon). The model sees the same top-3 reward structure; the sub-4 tail provides ordering signal without diluting the primary ranking objective. ✓

---

## 4. JRA Results (CatBoost YetiRank, iter14 params, depth=8, lr=0.05, l2=3, iter=1000)

### 4-A. Cheap filter (train ≤ 2022, holdout 2023–2025, n=936,044 rows)

| Scheme     | top1   | place2 | place3 | top3_box | f2p    | Δtop1   | Δplace2 | Δplace3 | Δbox    | Filter   |
| ---------- | ------ | ------ | ------ | -------- | ------ | ------- | ------- | ------- | ------- | -------- |
| current_cb | 0.4501 | 0.2325 | 0.1661 | 0.1577   | 0.6830 | —       | —       | —       | —       | —        |
| D          | 0.4473 | 0.2345 | 0.1676 | 0.1561   | 0.6863 | −0.0028 | +0.0020 | +0.0014 | −0.0016 | **PASS** |
| C          | 0.4189 | 0.2302 | 0.1711 | 0.1542   | 0.6863 | −0.0312 | −0.0023 | +0.0049 | −0.0036 | FAIL     |
| B          | 0.4278 | 0.2304 | 0.1728 | 0.1543   | 0.6898 | −0.0223 | −0.0021 | +0.0067 | −0.0035 | FAIL     |

Survivors: **[D]** — C and B fail because top1 drops too severely (−0.0312 / −0.0223), leaving only 1 positive axis.

### 4-B. Walk-forward + bootstrap for scheme D

| Fold | current_cb top1 | D top1 | Δtop1   | Δplace2 | Δplace3 | Δbox    | Δf2p    |
| ---- | --------------- | ------ | ------- | ------- | ------- | ------- | ------- |
| 2023 | 0.4395          | 0.4294 | −0.0101 | −0.0023 | −0.0052 | −0.0055 | −0.0046 |
| 2024 | 0.4525          | 0.4606 | +0.0081 | +0.0087 | −0.0017 | 0.0000  | +0.0064 |
| 2025 | 0.4498          | 0.4507 | +0.0009 | −0.0012 | −0.0052 | −0.0006 | −0.0037 |

| Scheme | Δtop1   | Δplace2 | Δplace3 | Δtop3_box | Δf2p    | f2p LB95     | Verdict    |
| ------ | ------- | ------- | ------- | --------- | ------- | ------------ | ---------- |
| D      | −0.0004 | +0.0017 | −0.0040 | −0.0020   | −0.0007 | **−0.00357** | **REJECT** |

JRA scheme D is **REJECT**: only 1 positive axis (place2), f2p_lb95 is negative (−0.00357). The fold-level results are highly inconsistent — fold 2023 regresses broadly while fold 2024 gains. The pooled signal is too weak and directionally mixed.

---

## 5. Banei Results (CatBoost YetiRank, v7 params, depth=8, lr=0.05, iter=300)

### 5-A. Cheap filter (train ≤ 2022, holdout 2023–2025, n=157,129 rows)

| Scheme     | top1   | place2 | place3 | top3_box | f2p    | Δtop1   | Δplace2 | Δplace3 | Δbox    | Filter |
| ---------- | ------ | ------ | ------ | -------- | ------ | ------- | ------- | ------- | ------- | ------ |
| current_cb | 0.3481 | 0.2107 | 0.1528 | 0.1112   | 0.6357 | —       | —       | —       | —       | —      |
| D          | 0.3470 | 0.2094 | 0.1494 | 0.1080   | 0.6361 | −0.0011 | −0.0013 | −0.0034 | −0.0032 | FAIL   |
| C          | 0.3330 | 0.2115 | 0.1524 | 0.1091   | 0.6300 | −0.0152 | +0.0008 | −0.0004 | −0.0021 | FAIL   |
| B          | 0.3396 | 0.2094 | 0.1521 | 0.1097   | 0.6298 | −0.0085 | −0.0013 | −0.0008 | −0.0015 | FAIL   |

Survivors: **none** — all schemes fail cheap filter. D has all-negative deltas (except f2p +0.0004), C and B also regress or have only 1 positive axis.

**No WF run for Banei.**

---

## 6. Summary Table

| Category | Scheme | Filter | WF Verdict (base model only)            | Best signal                           |
| -------- | ------ | ------ | --------------------------------------- | ------------------------------------- |
| NAR      | D      | PASS   | ADOPT → **REJECT at full-system judge** | place3 +4.0pp, f2p_lb95 +0.00174      |
| NAR      | B      | PASS   | ADOPT (base only; not deploy-judged)    | place3 +3.4pp, f2p_lb95 +0.00091      |
| NAR      | C      | SKIP   | n/a                                     | O(N²) XGB rank:pairwise infeasible    |
| JRA      | D      | PASS   | **REJECT**                              | f2p_lb95 −0.00357, inconsistent folds |
| JRA      | C      | FAIL   | n/a                                     | top1 −3.1pp                           |
| JRA      | B      | FAIL   | n/a                                     | top1 −2.2pp                           |
| Banei    | D      | FAIL   | n/a                                     | all-negative deltas                   |
| Banei    | C      | FAIL   | n/a                                     | top1 −1.5pp                           |
| Banei    | B      | FAIL   | n/a                                     | all-negative deltas                   |

---

## 7. Verdict per Category

### NAR — best scheme at base-model level: D (B also passed) — **but full-system judge REJECTED D**

At the **base-model level**, both D and B passed the gate. The epsilon sub-4 tail improves place3/top3_box without hurting top1 when measuring the base XGBoost alone. The mechanism: XGBoost `rank:pairwise` uses all horse pairs within a race; adding a small positive relevance to ranks 4–12 creates additional ordering signal for mid-field horses, which cascades into better discrimination of the 3rd-place finisher.

Top-3 labels unchanged ({3, 2, 1}) → no dilution of the top-3 precision signal at the base level.

**However**, the production system is not the base model alone — it is iter12 + per-class residual ensembles (iter30/36) whose residual features and blend weights are calibrated to iter12's score distribution. The full-system deploy judge (`nar-schemeD-deploy-judge.md`) retrained the entire chain on scheme-D and measured **regression**: f2p LB95 = −0.00481, top1 −0.40pp, place3 −0.43pp. **Final verdict: REJECT — production unchanged.**

### JRA — no adoption

Scheme D passes the cheap filter but fails WF: fold-level results are inconsistent (2023 regresses, 2024 gains), and the pooled f2p_lb95 is negative. CatBoost YetiRank with its list-wise training formulation may be less sensitive to sub-4 epsilon labels than XGBoost's pairwise approach. JRA's larger race fields (16–18 horses) and more competitive market efficiency may reduce the signal-to-noise ratio of tail labels.

### Banei — no adoption

All schemes fail the cheap filter — the small dataset (157k rows) combined with Banei's unusual distance/weight structure means the additional tail labels add noise rather than signal. The current {3,2,1} scheme is already well-tuned for Banei.

---

## 8. Implications and Next Steps

1. **NAR production**: ~~Adopt scheme D~~ — **superseded**. The full-system deploy judge REJECTED scheme-D (f2p LB95 = −0.00481 vs TRUE production baseline). Production stays on iter12 + iter30/36 with the current {3, 2, 2} relevance. See `nar-schemeD-deploy-judge.md`.
2. **JRA and Banei**: Retain current schemes ({1:3, 2:2, 3:1}). No change warranted.
3. **NAR scheme B**: Passed base-model gate (weaker than D). Given D's full-system REJECT and the shared root cause (any relevance change shifts the base score distribution that per-class residuals are calibrated to), a B deploy judge is expected to fail the same way — not pursued.
4. **CatBoost+YetiRank with float labels**: Schemes C and B showed the YetiRank objective is sensitive to label inflation — large extended integer labels (B: 7/6/5/4/3/2/1) cause severe top1 regression in JRA/Banei. D's epsilon tail (0.1/0.08/0.05/0.02) avoids this by keeping sub-4 labels small.
5. **Key lesson (re-confirmed at scale)**: a base-model WF gain does not transfer to the production blend when downstream per-class residuals/blend weights are calibrated to the old base score distribution. Adoption gates must run against the full serving system — same conclusion as the NAR G-1+F1 retrain judge (#262) and the D-phase NULL-routing lesson.

---

## 9. Technical Notes

- **Bootstrap implementation**: Original row-level isin-based bootstrap (O(N_rows × n_iters)) was replaced with vectorized race-level precomputation (O(N_races + n_iters)). For 412k NAR races × 10k iterations, this reduced bootstrap runtime from ~27 min to ~29 sec with identical statistical properties.
- **CatBoost grade_code issue**: JRA features include a `grade_code` column stored as PyArrow `str` with whitespace `' '` values. The experiment script was fixed to use `astype("string").fillna("__missing__").astype(str)` for categorical columns, mirroring the production `_prepare_feature_matrix` approach.
- **Run history**: Original attempt (PID 24452) died during slow bootstrap; run3 used old 27-min bootstrap; run4 completed NAR with fast bootstrap; run5 completed JRA+Banei (31.2 min total).
- **Results file**: `/tmp/graded_relevance_results.json` (not git-tracked per project rules)
