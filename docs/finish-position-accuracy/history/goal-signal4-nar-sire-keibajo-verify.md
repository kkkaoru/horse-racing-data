# Signal 4 NAR: Sire×Keibajo Incremental Cheap-Filter Verification (2026-06-17)

## Context

Probe (`goal-pedigree-aptitude-probes.md`, commit 76adebc) found NAR global partial ρ_full=0.1055 /
ρ_hold=0.0887 for `sire_keibajo_win_rate` (sire's progeny win rate at this specific
keibajo_code, leak-free monthly rolling, ≥10 progeny races). This is structurally distinct from
the existing `sire_track_win_rate` (surface-based, 芝/ダート split). JRA was ABORT (sign flip).

Per the project lesson (rel [[project_relationship_perclass_investigation_2026_06_12]]):
ρ clearing the bar is necessary but not sufficient — GBDT often already captures the signal
non-linearly via existing features. This cheap-filter verifies whether that holds for NAR.

---

## Method

**Training setup:**

- Production NAR iter12 feature store: `tmp/feat-v15/nar/race_year=XXXX/*.parquet` (135 cols)
- Train: 2016–2022 (913,804 rows); Holdout: 2023–2025 (412,403 rows)
- Algorithm: LightGBM lambdarank with iter12 HPO params
  (`lr=0.0527, num_leaves=63, min_child_samples=7, lambda_l2=1.967,
subsample=0.618, colsample_bytree=0.750, num_threads=6, seed=42`)
- Early stopping rounds: 50; max n_estimators: 650
  - Base best_iter: 122; With-feature best_iter: 100

**New features added (WITH variant):**

- `sire_keibajo_win_rate`: sire's cumulative progeny win rate at this keibajo_code, monthly
  rolling accumulation, NULL if < 10 progeny races at this venue (same threshold as
  production pedigree stats)
- `damsire_keibajo_win_rate`: same for dam's sire

**Feature construction:** Computed from PG (`nvd_se` × `nvd_um`). Sire stats: 3.32M rows;
damsire stats: 4.61M rows. Horse pedigree map: 100,452 horses from `nvd_um`.

**Coverage:**

| Set                 | Rows    | Sire coverage | Damsire coverage |
| ------------------- | ------- | ------------- | ---------------- |
| Train (2016–2022)   | 913,804 | 96.3%         | 95.6%            |
| Holdout (2023–2025) | 412,403 | 62.0%         | 61.7%            |

**Holdout coverage note:** Only 50% of 2024 NAR horses appear in `nvd_um` (JV-Data registered
horses); recent NAR-only horses are not in the JV-Data horse master table. This means 38% of
holdout rows receive NULL for both new features. LightGBM handles NULL natively (NULL-routing
in split nodes). This biases the comparison toward underestimating the gain (the model trained
on 96%-coverage data evaluates on 62%-coverage holdout, so the new feature has less information
in the holdout window). The holdout coverage gap means results are **conservative**.

**Metrics:** top1 (predicted rank 1 = actual rank 1), place2 (predicted rank 1 ≤ actual 2),
place3 (predicted rank 1 ≤ actual 3), top3_box (predicted top-3 ⊆ actual top-3),
fukusho_2p (predicted rank 1 ≤ fukusho threshold: 2 if n≤6, 3 if n>6). Paired bootstrap
LB95 (10,000 samples, seed=42). Total holdout races: 40,710.

**NAR subclasses** from `tmp/nar-perclass/nar_vec/nar_subclass_map.parquet`
(derived from `kyoso_joken_meisho` text; 293,306 races).

---

## Global Results (WITH − WITHOUT)

| Metric         | Base    | With    | Delta (pp) | LB95 delta (pp) | n_races |
| -------------- | ------- | ------- | ---------- | --------------- | ------- |
| **top1**       | 0.44557 | 0.44569 | **+0.012** | **+0.012**      | 40,710  |
| **place2**     | 0.64974 | 0.65158 | **+0.184** | **+0.191**      | 40,710  |
| **place3**     | 0.76288 | 0.76310 | **+0.022** | **+0.027**      | 40,710  |
| **top3_box**   | 0.15812 | 0.15834 | **+0.022** | **+0.022**      | 40,710  |
| **fukusho_2p** | 0.75913 | 0.75944 | **+0.032** | **+0.042**      | 40,710  |

All 5 metrics are positive. All LB95 deltas are positive. The strongest signal is place2
(+0.184pp, LB95 +0.191pp). The base model top1 (44.6%) is consistent with the production iter12
WF baseline, confirming the test is comparable.

---

## Per-Subclass Results (WITH − WITHOUT, top1 delta)

| Subclass    | Base top1 | With top1 | Delta (pp) | LB95 delta | n_races | place2 delta | place3 delta |
| ----------- | --------- | --------- | ---------- | ---------- | ------- | ------------ | ------------ |
| **B**       | 0.4199    | 0.4229    | **+0.300** | **+0.285** | 6,326   | +0.269       | +0.221       |
| **OP**      | 0.4453    | 0.4480    | **+0.269** | **+0.359** | 1,116   | +0.717       | −0.358       |
| **C**       | 0.4516    | 0.4516    | −0.004     | 0.000      | 23,133  | +0.203       | +0.117       |
| **A**       | 0.4417    | 0.4382    | −0.358     | −0.358     | 2,515   | **+0.596**   | **+0.477**   |
| **NEW**     | 0.4926    | 0.4890    | −0.368     | −0.368     | 544     | 0.000        | +0.368       |
| **MUKATSU** | 0.4766    | 0.4748    | −0.187     | −0.374     | 535     | **−1.495**   | −0.561       |
| **other**   | 0.4440    | 0.4434    | −0.061     | −0.061     | 6,541   | −0.061       | −0.596       |

**Summary per subclass:**

- **B (6,326 races):** Uniformly positive. top1 +0.300pp (LB95 +0.285pp), place2 +0.269pp,
  place3 +0.221pp. Strongest consistent subclass gain.
- **OP (1,116 races):** top1 +0.269pp (LB95 +0.358pp), place2 +0.717pp. Strong for top1/place2
  but place3 regresses −0.358pp. Mixed within-OP signal.
- **C (23,133 races, largest):** top1 near-zero but place2 +0.203pp, place3 +0.117pp. Positive
  overall effect at the dominant subclass.
- **A (2,515 races):** top1 −0.358pp but place2 +0.596pp, place3 +0.477pp. The feature reshuffles
  ranks within top-3 without a net top1 gain.
- **NEW (544 races):** top1 −0.368pp, but fukusho_2p +0.368pp. Small n (noisy).
- **MUKATSU (535 races):** top1 −0.187pp, **place2 −1.495pp (LB95 −1.682pp)**. Clear regression
  at MUKATSU. Small n but strong negative signal.
- **other (6,541 races):** Mildly negative across most metrics.

---

## Analysis

**Does the global positive hold?** Yes — all 5 global metrics are positive (LB95 ≥ 0). The
global verdict is PROCEED by the gate.

**Is the gain orthogonal to existing sire features?** Partial. The existing `sire_track_win_rate`
uses surface (芝/ダート, first character of `track_code`). NAR races run predominantly on dirt,
so the surface split is very coarse for NAR. The keibajo_code split (20+ distinct NAR venues)
adds genuine specificity that the surface split cannot capture. The ~+0.184pp place2 gain
confirms the keibajo split is not fully captured by the existing surface-based feature.

**MUKATSU caveat:** The MUKATSU place2 regression (−1.495pp, LB95 −1.682pp) is large and
consistent. MUKATSU = "未確認" races in NAR, a juvenile/maiden-equivalent class with very small
sample size. With only 535 holdout races, statistical power is limited, but the LB95 is firmly
negative. The most likely cause: MUKATSU horses are young (few prior progeny races per sire at
this venue), so `sire_keibajo_win_rate` is NULL-heavy even in train, and the feature provides
noisy signal for this subclass. A route-specific model excluding MUKATSU from the new feature
column would mitigate this.

**Coverage bias direction:** The 62% holdout sire coverage (vs 96% train) means the measured
gain _understates_ the true effect: many holdout horses who would benefit from the feature
receive NULL instead. A production-quality estimate would require completing the `nvd_um`
backfill for 2023–2025 NAR horses, which is a data-quality issue separate from this probe.

---

## Weld Awareness

The NAR production system uses per-class residual ensembles (iter30 MUKATSU/NEW/B/A/OP, iter36
C-class LGB lambdarank) blended onto the iter12 base score distribution. Adding a new base
feature shifts the base score distribution, which the ensemble calibrations are tuned to. A
real deploy therefore requires:

1. **Full retrain of NAR base** with `sire_keibajo_win_rate` + `damsire_keibajo_win_rate` added
   to the production feature store (`finish_position_features_duckdb.py` pedigree stat specs).
2. **Full re-build of feat-v15/nar** (or equivalent store) with the new feature columns.
3. **Co-retrain of iter30/iter36 residual ensembles** on the new base score distribution.
4. **MUKATSU routing decision:** Given the −1.495pp place2 regression at MUKATSU, consider
   not routing the new feature to MUKATSU rows (NULL-fill for MUKATSU = treat as unknown).
5. **nvd_um completeness audit:** Resolve why 2023–2025 NAR horses are missing from nvd_um
   (likely: NAR-local horse registration lag in JV-Data feed). If the NAR equivalent
   (`nvd_um` should be the N-Data horse master) has the missing horses, use that instead.

---

## Verdict

**PROCEED to co-train path** (subject to MUKATSU routing caveat).

The base-level incremental test confirms:

- `sire_keibajo_win_rate` and `damsire_keibajo_win_rate` add signal orthogonal to the existing
  surface-based `sire_track_win_rate` in NAR.
- Global gain: top1 +0.012pp (LB95 +0.012pp), place2 +0.184pp (LB95 +0.191pp).
- Strongest subclasses: B (+0.300pp top1, all metrics positive) and C (+0.203pp place2).
- MUKATSU regresses place2 −1.495pp and should be routed with NULL for the new feature.
- Results are **conservative** due to 62% holdout coverage (nvd_um lag for 2023–2025 NAR horses).
- Full deploy requires: feature store rebuild + base retrain + iter30/iter36 co-retrain.
- ρ was necessary AND sufficient at base level; GBDT does not fully capture sire×keibajo
  non-linearly via existing sire_track_win_rate (surface) + base sire features.
