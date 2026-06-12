# JRA kohan3f going-conditional verify — iter19-kohan3f (2026-06-13)

**VERDICT: ADOPT-pending-deploy (config = iter19-base-only)** — the new 244-feature base
beats the production iter25/26 per-class ensemble system end-to-end on holdout 2023-2026
with all 3 pooled axes LB95 > 0. Deciding number: **pooled fukusho_2p LB95 = +0.10pp**
(0.6871 → 0.6907, Δ=+0.36pp, paired bootstrap 10k seed42, n=11703 races).

Verification #1 of the synthesized JRA hypothesis plan (rank-1 lever from
`jra-hypo-rejection-gaps.md`: probe-PASSED, model-test never run). Disambiguation: the
model id is `iter19-jra-cb-kohan3f-going-v8` — unrelated to the 2026-06-04
`iter19-jra-cb-l4class-v8` (L4 class-weight REJECT) which happens to share the iter number.

## Candidate features (3, store 241 → 244)

Built on top of the iter14 production store (`feat-jra-v8-iter14-course`), per horse,
strictly prior history (`race_date < target`), no imputation. Precise window semantics
(as trained & judged): rank ALL going-coded prior starts newest-first, keep the last 5,
then split that window by going — NOT "last 5 firm starts ever":

| feature              | definition                                                                      |
| -------------------- | ------------------------------------------------------------------------------- |
| `kohan3f_firm_avg5`  | avg `jvd_se.kohan_3f` over FIRM starts (babajotai 1-2) among last 5 going-coded |
| `kohan3f_soft_avg5`  | same for SOFT starts (babajotai 3-4) among the same last-5 window               |
| `kohan3f_going_diff` | firm_avg5 − soft_avg5 (NULL if either side NULL)                                |

Going source: `jvd_ra.babajotai_code_shiba` for turf (track_code 10-29),
`babajotai_code_dirt` for dirt (51-69); code 0 excluded. Probe ρ (from triplet-verify-p1):
firm +0.1286, soft +0.0977.

NULL rates (train): firm 27.6%, soft 84.2%, diff 85.3% — soft sparsity expected (most JRA
races are on firm going); CatBoost routes NULLs natively.

## Base-level equal-footing judge (iter19 base vs iter14 base)

Both CatBoost YetiRank, identical params (depth 8, lr 0.05, l2 3.0, 1000 iter, Bayesian,
od_wait 30, seed 2068), train 2007-2025 / val 2026, scored on holdout 2023-2026 from each
model's own store. iter19 best_iter=315. Paired bootstrap LB95 10k seed42.

| class      | n_races   | top1 iter14→iter19 (Δ / LB95)         | fukusho_2p iter14→iter19 (Δ / LB95)   |
| ---------- | --------- | ------------------------------------- | ------------------------------------- |
| 005        | 3147      | 0.4147→0.4210 (+0.64pp / +0.16pp)     | 0.6540→0.6571 (+0.32pp / −0.19pp)     |
| 010        | 1583      | 0.4340→0.4371 (+0.32pp / −0.38pp)     | 0.6469→0.6526 (+0.57pp / −0.13pp)     |
| 016        | 727       | 0.3796→0.3920 (+1.24pp / +0.00pp)     | 0.5695→0.5860 (+1.65pp / +0.28pp)     |
| 703        | 4229      | 0.4956→0.5018 (+0.61pp / +0.21pp)     | 0.7531→0.7567 (+0.35pp / +0.00pp)     |
| 701        | 953       | 0.4565→0.4659 (+0.94pp / +0.21pp)     | 0.7219→0.7335 (+1.15pp / +0.42pp)     |
| other      | 1064      | 0.4164→0.4267 (+1.03pp / +0.09pp)     | 0.6118→0.6175 (+0.56pp / −0.38pp)     |
| **POOLED** | **11703** | **0.4479→0.4548 (+0.68pp / +0.43pp)** | **0.6853→0.6907 (+0.54pp / +0.28pp)** |

top3_box pooled LB95 = +0.23pp. Gate: PROCEED (pooled LB95 positive on all 3 axes).
Every class improves top1; signal is broad, not class-localized.

## Full-system judge (vs production = iter25/26 per-class ensembles)

WELD-aware: production baseline is the real serving system — per-class rank-blend
ensembles (005/016/703 = iter26 7-6-7 members, 010/other = iter25 5 members, 701 = iter14
fallback), reproduced offline from manifests with each member scored on its own
metadata feature_names (iter26 members need the 12 relationship features →
`feat-jra-v8-iter26-relationships` store; iter22/25/26 members need `iter14_score`).
Race sets aligned: 11703.

Two candidate configurations judged:

### Candidate A — iter19 base + fresh per-class low-cap (0.5/0.5 rank blend): **FAIL**

Fresh per-class members = iter25 low-cap params (depth 4, lr 0.1, 500 cap) trained on the
iter19 store + leak-free WF `iter19_score` residual (per-fold 2008-2026 walk-forward),
chain-filtered, train 2007-2024 / val 2025. best_iter 28-73 — very shallow.

| pooled axis | prod→blend (Δ)          | LB95             |
| ----------- | ----------------------- | ---------------- |
| top1        | 0.4500→0.4503 (+0.03pp) | **−0.26pp** veto |
| fukusho_2p  | 0.6871→0.6878 (+0.07pp) | **−0.21pp** veto |
| top3_box    | (+0.36pp)               | +0.14pp          |

The weak 1-member blend drags the strong base down (iter19 base alone is 0.4548 top1).
Confirms the WELD lesson in reverse: a per-class layer only helps with mature,
weight-optimized members; a naive equal-weight residual member is harmful.

### Candidate B — iter19 base only (drop per-class layer): **PASS**

| class      | n_races   | top1 prod→iter19 (Δ / LB95)           | fukusho_2p prod→iter19 (Δ / LB95)     |
| ---------- | --------- | ------------------------------------- | ------------------------------------- |
| 005        | 3147      | 0.4163→0.4210 (+0.48pp / +0.03pp)     | 0.6581→0.6571 (−0.10pp / −0.64pp)     |
| 010        | 1583      | 0.4346→0.4371 (+0.25pp / −0.44pp)     | 0.6507→0.6526 (+0.19pp / −0.51pp)     |
| 016        | 727       | 0.3865→0.3920 (+0.55pp / −0.69pp)     | 0.5681→0.5860 (+1.79pp / +0.55pp)     |
| 703        | 4229      | 0.4987→0.5018 (+0.31pp / −0.14pp)     | 0.7538→0.7567 (+0.28pp / −0.09pp)     |
| 701        | 953       | 0.4565→0.4659 (+0.94pp / +0.21pp)     | 0.7219→0.7335 (+1.15pp / +0.42pp)     |
| other      | 1064      | 0.4164→0.4267 (+1.03pp / +0.09pp)     | 0.6118→0.6175 (+0.56pp / −0.38pp)     |
| **POOLED** | **11703** | **0.4500→0.4548 (+0.48pp / +0.22pp)** | **0.6871→0.6907 (+0.36pp / +0.10pp)** |

top3_box pooled: Δ=+0.47pp, LB95=+0.26pp.

**Strengthened gate: PASS** — pooled fukusho_2p LB95 = +0.10pp > 0; all 3 pooled axes
LB95 > 0 (so ≥2-axes and the −0.05pp veto floors are satisfied with margin). Per-class
top1 Δ is positive in all 6 classes; the negative per-class LB95 cells are small-sample
width, and the gate is pooled by design.

## Verdict & deploy notes

**ADOPT-pending-deploy, config = iter19-base-only.** The 3 going-conditional kohan3f
features produce a base that single-handedly beats the entire production per-class
ensemble stack (+0.48pp top1, +0.36pp fukusho_2p pooled).

Deploy phase (separate, NOT done here) requires:

1. Serve-path feature builder for the 3 columns (same jvd_se×jvd_ra going-conditional
   aggregation; data exists in the PG mirror — serve can compute it).
2. Per-class registry flip: route all JRA classes to the new base
   (`iter19-jra-cb-kohan3f-going-v8`), dropping iter25/26 ensembles — OR rebuild the
   full multi-member weight-optimized per-class suite on the iter19 base (unproven;
   quick proxy failed, but production-grade rebuild could add on top of +0.48pp).
3. **Serve-skew check before flip** (2026-06-11/12 lessons): validate on the serve
   distribution. Risk is lower than the NAR G-1/F1 case — existing 241 features keep
   identical build pipeline + NULL patterns (only 3 columns appended), unlike the NAR
   retrain which _changed_ NULL patterns of existing features — but the WF-vs-serve
   gap must still be measured before production traffic moves.

Caveat: two configs were tested against the same holdout (blend, base-only); base-only
was the precommitted WELD fallback, not a post-hoc selection, and its margins survive
the conservative LB95. fukusho_2p margin (+0.10pp LB95) is thin; the serve-skew check
in step 3 is the real gate before flip.

## Artifacts

- Feature store: `tmp/v8/feat-jra-v8-iter19-kohan3f-going/` (2007-2026, 263 cols)
- Base model: `tmp/v8/models/iter19-jra-cb-kohan3f-going-v8/` (244 feats, best_iter 315)
- WF preds: `tmp/v8/iter19-wf-predictions/` (folds 2008-2026)
- Per-class probes: `tmp/v8/models/iter19-perclass/` (judge candidate A only — not adopted)
- Scripts: `tmp/v8/iter19_kohan3f_going_verify.py`, `tmp/v8/iter19_kohan3f_fullsystem_judge.py`
- Results: `tmp/v8/iter19-kohan3f-verify-result.json`, `tmp/v8/iter19-fullsystem-result.json`
- Committed feature builder: `apps/pc-keiba-viewer/src/scripts/finish-position-features/add_kohan3f_going_features.py` (+ tests)
