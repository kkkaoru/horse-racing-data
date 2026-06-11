# F1 Pedigree Fix — Retrain + Judge (NAR + Ban-ei)

**Date:** 2026-06-12
**Branch:** docs/jes-journal-collection
**Status:** REJECT (both categories)

---

## Summary

Regenerated NAR and Ban-ei feature parquets with the pedigree fix from commit 65ad49e
(pedigree_staging.py now UNIONs `pg.jvd_um` + `pg.nvd_um`). Re-trained walk-forward
models and ran paired-bootstrap judge. Both REJECT.

---

## Step 1: NULL Rate Before / After Fix

### NAR (feat-nar-v7-baba-21y)

| Column                  | Before fix | After fix |
| ----------------------- | ---------- | --------- |
| sire_baba_win_rate      | 44.8% null | 3.9% null |
| damsire_baba_win_rate   | 44.8% null | 3.9% null |
| sire_baba_career_starts | 44.8% null | 3.9% null |

The residual 3.9% null represents genuine data absence — horses with no historical
track condition records, not a pipeline bug.

**Root cause of partial coverage in the old parquet:** NAR horses that also appear
in `pg.jvd_um` (typically dual-registered horses with JRA history) were already
getting pedigree from jvd_um, hence 44.8% instead of 100% null.

### Ban-ei (feat-ban-ei-v7-grade-21y-parity)

| Column                | Before fix | After fix  |
| --------------------- | ---------- | ---------- |
| sire_baba_win_rate    | 100% null  | 23.1% null |
| damsire_baba_win_rate | 100% null  | 23.1% null |

The residual 23.1% null is from recently registered horses not yet in `pg.nvd_um`:

- 2024-registered horses: 273 absent from nvd_um (8,084 null rows)
- 2025-registered horses: 314 absent
- 2026-registered horses: 124 absent
- 24 horses from 2023 also missing

This is a data sync lag in nvd_um for recently registered Ban-ei horses — not a
pipeline bug. For horses present in nvd_um, sire_id fill rate is ~100%.

---

## Step 2: Pipeline Execution

### NAR pipeline (starting from feat-nar-v6-21y intermediate)

| Step    | Script                                 | Input → Output                                      | Status | Time |
| ------- | -------------------------------------- | --------------------------------------------------- | ------ | ---- |
| Lineage | add-grade-race-lineage-features.py     | feat-nar-v6-21y → feat-nar-v7-lineage-21y-f1        | OK     | ~14s |
| H2H     | add-head-to-head-features.py           | feat-nar-v7-lineage-21y-f1 → feat-nar-v7-h2h-21y-f1 | OK     | ~51s |
| Baba    | add-baba-pedigree-affinity-features.py | feat-nar-v7-h2h-21y-f1 → feat-nar-v7-baba-21y-f1    | OK     | ~17s |

Output: `tmp/feat-nar-v7-baba-21y-f1` — 192 cols total, 175 features (same as production).

### Ban-ei pipeline (starting from feat-ban-ei-v7-h2h-21y)

| Step       | Script                                 | Input → Output                                                               | Status | Time |
| ---------- | -------------------------------------- | ---------------------------------------------------------------------------- | ------ | ---- |
| Baba       | add-baba-pedigree-affinity-features.py | feat-ban-ei-v7-h2h-21y → feat-ban-ei-v7-baba-21y-f1                          | OK     | ~14s |
| Futan      | add-banei-futan-class-features.py      | feat-ban-ei-v7-baba-21y-f1 → feat-ban-ei-v7-futan-21y-f1                     | OK     | ~9s  |
| Grade      | add-banei-grade-career-features.py     | feat-ban-ei-v7-futan-21y-f1 → feat-ban-ei-v7-grade-21y-parity-f1             | OK     | ~2s  |
| Projection | pandas select                          | feat-ban-ei-v7-grade-21y-parity-f1 → feat-ban-ei-v7-grade-21y-parity-f1-proj | OK     | ~10s |

**Note on Ban-ei projection:** The `feat-ban-ei-v7-h2h-21y` intermediate already contained
55 extra running-style columns that were absent from the original production pipeline.
A column-projection step was applied to match the production 111-feature schema (128 file
columns excluding race_year partition col).

Output: `tmp/feat-ban-ei-v7-grade-21y-parity-f1-proj` — 129 cols (w/ partition), 111 features.

---

## Step 3: Walk-Forward Training

### NAR: XGBoost rank:pairwise

```
namespace:  nar-xgb-v7-f1pedigree-wf-21y
features:   tmp/feat-nar-v7-baba-21y-f1  (175 features, parity guard passed)
folds:      2007-2026 (20 folds)
hyperparams: num_rounds=450, max_depth=6, lr=0.1
```

All 20 folds completed. NDCG@3 representative: 2022=0.77221, 2023=0.77602, 2024=0.77709, 2025=0.77185.

### Ban-ei: CatBoost YetiRank

```
namespace:  banei-cb-v7-f1pedigree-wf-21y
features:   tmp/feat-ban-ei-v7-grade-21y-parity-f1-proj  (111 features, parity guard passed)
folds:      2008-2026 (19 folds)
hyperparams: iterations=300, depth=8, l2_leaf_reg=3.0, lr=0.05
```

All 19 folds completed. OMP_NUM_THREADS=4.

---

## Step 4: Judge Results

**Method:** Paired bootstrap (10,000 iterations, race-level resampling).
Baseline: `nar-xgb-v7-lineage-wf-21y` / `banei-cb-v7-lineage-wf-21y` (production walk-forward).

### NAR (n_races = 258,966)

| Metric     | Baseline | F1      | Diff     | LB95         | UB95     |
| ---------- | -------- | ------- | -------- | ------------ | -------- |
| top1       | 58.055%  | 58.218% | +0.163pp | +0.105pp     | +0.219pp |
| place2     | 79.929%  | 79.983% | +0.054pp | +0.005pp     | +0.100pp |
| place3     | 90.125%  | 90.177% | +0.052pp | +0.017pp     | +0.087pp |
| fukusho_2p | 44.045%  | 44.080% | +0.035pp | **−0.023pp** | +0.092pp |
| top3_box   | 37.176%  | 37.296% | +0.120pp | +0.065pp     | +0.174pp |

**DECISION: REJECT**
Reason: `fukusho_2p LB95 = −0.023pp ≤ 0` (Gate 1 fails). All other axes are
robustly positive with LB95 > 0. The fukusho_2p improvement (+0.035pp) is real
but its 95% CI crosses zero narrowly.

### Ban-ei (n_races = 31,969)

| Metric     | Baseline | F1      | Diff         | LB95     | UB95     |
| ---------- | -------- | ------- | ------------ | -------- | -------- |
| top1       | 33.507%  | 33.579% | +0.072pp     | −0.109pp | +0.247pp |
| place2     | 53.768%  | 54.012% | +0.244pp     | +0.069pp | +0.416pp |
| place3     | 68.416%  | 68.338% | −0.078pp     | −0.235pp | +0.081pp |
| fukusho_2p | 14.855%  | 14.883% | +0.028pp     | −0.100pp | +0.156pp |
| top3_box   | 12.606%  | 12.462% | **−0.144pp** | −0.275pp | −0.009pp |

**DECISION: REJECT**
Reason: `fukusho_2p LB95 = −0.100pp ≤ 0` (Gate 1) | `top3_box = −0.144pp < −0.05pp`
(veto floor).

---

## Step 5: Analysis of Reject

### NAR: Near-ADOPT

NAR is a borderline case. The fukusho_2p LB95 of −0.023pp is just below the gate.
Four of five axes are strongly positive (all LB95 > 0), and the observed fukusho_2p
diff (+0.035pp) is positive. The gate1 failure reflects statistical uncertainty at
the fukusho_2p metric specifically.

**Why the pedigree fix helped top1/place2/place3/top3_box but not fukusho_2p:**
fukusho_2p measures coverage of both actual top-2 finishers by predicted top-2 — a
stricter "exact set" criterion. This is harder to improve with sire×baba affinity
features since these help rank the best horse within a race more accurately (top1),
but the second-best horse is less determined by pedigree.

**The improvement is real:** top1 +0.163pp with LB95 +0.105pp over 258,966 races is
a robust finding. The pedigree fix did provide genuine signal for NAR — it was just
not enough to clear the fukusho_2p gate.

### Ban-ei: Mixed signal

Ban-ei place2 improved substantially (+0.244pp, LB95 +0.069pp), but top3_box
regressed (−0.144pp, statistically significant with UB95 −0.009pp). This
suggests the pedigree features improved prediction of the single best horse (place2)
but introduced noise that disrupted the top-3 clustering (top3_box). The top3_box
regression triggers the veto floor.

**Probable cause of top3_box regression:** Ban-ei has 23.1% residual null for
sire_baba_win_rate (recently registered horses). The model trained on partially-null
data may have learned inconsistent patterns for the pedigree features, hurting
multi-horse ensemble accuracy (top3_box) while still improving single-horse selection
(place2).

---

## Step 6: Decision

**REJECT for both NAR and Ban-ei.**

Production models remain:

- NAR: `nar-xgb-v7-lineage-wf-21y` from `tmp/feat-nar-v7-baba-21y`
- Ban-ei: `banei-cb-v7-lineage-wf-21y` from `tmp/feat-ban-ei-v7-grade-21y-parity`

---

## Step 7: Per-Class Ensemble Rebuild Recommendation

**Not warranted** at this stage. The F1 models are REJECT, so rebuilding per-class
ensembles on top of rejected base models would not improve production.

If the fukusho_2p gap closes in a future iteration (e.g., after nvd_um data completes
for 2024-2026 registrations), a re-evaluation would be warranted.

---

## Lessons Learned

1. **fukusho_2p is the binding gate for NAR.** The metric is a strict "exact set"
   criterion and is harder to move than top1/place3. Future improvements should
   track fukusho_2p separately and with more bootstrap power (wider CI than ±0.05pp
   requires).

2. **Partial NULL (23.1%) hurts Ban-ei top3_box.** The residual null for recent
   Ban-ei horses (2024-2026 registrations not yet in nvd_um) introduces train/serve
   inconsistency. A re-run after nvd_um sync completes would be cleaner.

3. **The fix is additive, not free.** NAR top1/place2/place3/top3_box all improved
   robustly (+0.052–+0.163pp, all LB95 > 0). The pedigree signal is real. It did not
   pass the strict gate, but the signal is there for future ensembles.

---

## Artifacts

| Artifact              | Path                                          | Status                   |
| --------------------- | --------------------------------------------- | ------------------------ |
| NAR F1 features       | `tmp/feat-nar-v7-baba-21y-f1`                 | Kept (for re-evaluation) |
| Ban-ei F1 features    | `tmp/feat-ban-ei-v7-grade-21y-parity-f1-proj` | Kept                     |
| NAR F1 predictions    | `tmp/wf-nar-f1pedigree-predictions`           | Kept                     |
| Ban-ei F1 predictions | `tmp/wf-banei-f1pedigree-predictions`         | Kept                     |
| Judge script          | `tmp/f1-pedigree-judge-v4.py`                 | Kept                     |
| Production registry   | unchanged                                     | No flip                  |
