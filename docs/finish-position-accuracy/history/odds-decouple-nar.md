# Odds-Decoupling Experiment: NAR Finish-Position Prediction

**Date**: 2026-06-11  
**Branch**: docs/jes-journal-collection  
**Status**: COMPLETE — market-efficiency hypothesis confirmed; no decoupling deployed

---

## Hypothesis

The betting market is efficient: odds/popularity absorb pre-race signals, and the
current models DEPEND HEAVILY on odds (favorite-trackers). A model with reduced
reliance on odds_score / popularity_score might do BETTER in upset-prone races and
improve place2/place3 even where top1 is neutral.

---

## Setup

### Odds features audited

Three features were identified as odds/market-derived:

- `odds_score` — normalized inverse tansho odds (0=best)
- `popularity_score` — normalized popularity rank within race (0=favorite)
- `horse_popularity_vs_field` — all-NaN in every year (no signal; retained in list for completeness)

### Model architecture

Current production ensemble = `iter12-nar-xgb-hpo-v8` (XGB, ~50% weight) +
`h4-residual-{C}` / per-class LightGBM lambdarank residual (~50% weight).  
The residual is the only component we can retrain here; iter12 is treated as fixed.

- **model_O** = LGB lambdarank residual WITH odds features (3 features in, same HPO params as H4)
- **model_F** = LGB lambdarank residual WITHOUT odds features (171 instead of 174 features)
- **Final blend** = `(1-λ) * model_F_score + λ * model_O_score` combined with iter12 at
  production weights (C: 0.498/0.502; B/other: 0.5/0.5)

### Splits (nested, holdout scored ONCE)

| Window            | Years           | Purpose                       |
| ----------------- | --------------- | ----------------------------- |
| Inner WF training | 2006–2020       | model_F/model_O fold training |
| Tuning            | valid=2021,2022 | λ selection                   |
| Holdout           | valid=2023–2026 | final eval (scored once)      |

### Lambda sweep

λ ∈ {0.0, 0.1, 0.2, ..., 1.0}. λ=1.0 = odds-aware (current). λ=0.0 = pure fundamentals.

---

## Step 1: Odds-Dependence Audit (fold valid=2022)

| Class         | Odds gain importance | odds_score gain% | popularity_score gain% | Spearman vs odds_score | Spearman vs popularity_score |
| ------------- | -------------------- | ---------------- | ---------------------- | ---------------------- | ---------------------------- |
| **C**         | **2.56%**            | 2.406%           | 0.147%                 | **-0.755**             | -0.723                       |
| **B**         | **0.43%**            | 0.399%           | 0.034%                 | **-0.752**             | -0.726                       |
| **other**     | **1.78%**            | 1.699%           | 0.080%                 | **-0.754**             | -0.728                       |
| A (ref)       | 2.51%                | —                | —                      | -0.742                 | -0.709                       |
| OP (ref)      | 1.91%                | —                | —                      | -0.784                 | -0.751                       |
| MUKATSU (ref) | 3.59%                | —                | —                      | -0.817                 | -0.791                       |
| NEW (ref)     | 2.81%                | —                | —                      | -0.794                 | -0.767                       |

**Key observation**: The Spearman correlation of model score vs odds_score is -0.75 across all
classes — but this is almost entirely driven by the **iter12 component** (XGB, 50% weight),
which itself relies heavily on odds. Removing odds from the LGB residual (0.43–2.56% gain share)
barely changes the overall odds-dependence of the blended ensemble.

`horse_popularity_vs_field` is all-NaN in every year of the feature store; its gain share is
effectively 0% in all classes. Removal has no effect.

---

## Step 2: Lambda-vs-Axes Curves (tuning split 2021–2022)

### Class C (n=15,094 races, tuning)

| λ       | top1       | place2     | place3 | top3_box   |
| ------- | ---------- | ---------- | ------ | ---------- |
| 0.0     | 0.5929     | 0.7997     | —      | 0.7383     |
| 0.1     | 0.5918     | 0.7994     | —      | 0.7391     |
| 0.5     | 0.5918     | 0.7995     | —      | 0.7392     |
| **1.0** | **0.5930** | **0.8001** | —      | **0.7391** |

Flat-to-slightly-worse as λ decreases. No benefit from decoupling at any level.

### Class B (n=4,166 races, tuning)

| λ       | top1       | place2     | place3 | top3_box   |
| ------- | ---------- | ---------- | ------ | ---------- |
| **0.0** | 0.5658     | 0.7775     | 0.8802 | **0.7290** |
| **0.1** | **0.5667** | **0.7782** | 0.8802 | 0.7288     |
| 0.5     | 0.5663     | 0.7777     | 0.8800 | 0.7284     |
| 1.0     | 0.5670     | 0.7765     | 0.8802 | 0.7283     |

λ=0.1 marginally best top1+place2 on tuning. λ_c=0.1 selected by algorithm.

### Class other (n=3,780 races, tuning)

All λ values produce **identical scores**: top1=0.6100, place2=0.8131, top3_box=0.7546.  
The residual's odds features have zero marginal effect for this class on the tuning window.

---

## Step 3 + 4: Holdout Evaluation (2023–2026, scored once)

### Class C — λ_c = 1.0 (no improvement found)

| Model          | top1   | place2 | place3 | top3_box | n_races |
| -------------- | ------ | ------ | ------ | -------- | ------- |
| λ=1 (current)  | 0.5914 | 0.7994 | 0.8904 | 0.7403   | 26,060  |
| λ=0.0 (pure-F) | 0.5909 | 0.7995 | 0.8903 | 0.7406   | 26,060  |

**Deltas (lam0 vs current, pp)**: top1=-0.054, place2=+0.004, place3=-0.012, top3_box=+0.027

Noise-level differences. No systematic improvement from removing odds.

### Class B — λ_c = 0.1 (tuning win, holdout regression)

| Model          | top1   | place2 | place3 | top3_box   | n_races |
| -------------- | ------ | ------ | ------ | ---------- | ------- |
| λ=1 (current)  | 0.5821 | 0.7887 | 0.8829 | 0.7280     | 7,124   |
| λ=0.1 (chosen) | 0.5817 | 0.7865 | 0.8818 | **0.7296** | 7,124   |
| λ=0.0 (pure-F) | 0.5825 | 0.7872 | 0.8821 | **0.7295** | 7,124   |

**Deltas λ=0.1 vs current (pp)**: top1=-0.042, place2=-0.225, place3=-0.112, top3_box=+0.154  
**Deltas λ=0.0 vs current (pp)**: top1=+0.042, place2=-0.154, place3=-0.084, top3_box=+0.150

Bootstrap (λ=0.1 vs λ=1): delta_mean=-0.0004, LB95=-0.0025, p(Δ>0)=0.36 — **not significant**.  
The tuning-time improvement (λ=0.1 was best on 2021-22) did NOT generalize to holdout 2023-26.

### Class other — λ_c = 1.0 (no improvement found)

| Model          | top1   | place2 | place3 | top3_box | n_races |
| -------------- | ------ | ------ | ------ | -------- | ------- |
| λ=1 (current)  | 0.5957 | 0.8070 | 0.8969 | 0.7500   | 7,217   |
| λ=0.0 (pure-F) | 0.5958 | 0.8070 | 0.8969 | 0.7500   | 7,217   |

**Deltas (lam0 vs current, pp)**: top1=+0.014, place2=0.000, place3=0.000, top3_box=0.000

Effectively zero delta across all axes. The "other" class is so heterogeneous that odds
features contribute nothing the fundamentals don't already capture.

---

## Upset-Subset Analysis

Races where the odds-favorite did NOT win (upset subset):

### Class C — upset subset (n=14,269 holdout races)

**Chosen λ=1.0**, so upset deltas = 0.000 by construction. For reference, λ=0.0 on
upset races: not separately computed (same as overall lam0 delta: tiny).

### Class B — upset subset (n=4,063 holdout races)

| Model          | top1       | place2     | place3     | top3_box   |
| -------------- | ---------- | ---------- | ---------- | ---------- |
| λ=1 (current)  | 0.3953     | 0.6911     | 0.8282     | 0.7076     |
| λ=0.1 (chosen) | 0.3948     | 0.6877     | 0.8265     | **0.7098** |
| λ=0.0 (pure-F) | **0.3995** | **0.6914** | **0.8287** | 0.7096     |

Upset deltas (λ=0.0 vs current): top1=+0.418pp, place2=+0.025pp, place3=+0.049pp, top3_box=+0.197pp.

**λ=0.0 (pure fundamentals) is marginally better on upsets for class B** — but the effect
size (+0.4pp top1) is within noise given the bootstrap p=0.36 finding on the full holdout.

**Strict upset subset (favorite outside top-3, n=1,828):**

| Model          | top1   | place2 | place3     | top3_box |
| -------------- | ------ | ------ | ---------- | -------- |
| λ=1 (current)  | 0.4469 | 0.6176 | 0.7040     | 0.6225   |
| λ=0.0 (pure-F) | 0.4458 | 0.6154 | **0.7057** | 0.6244   |

Deltas lam0 vs current: top1=-0.109pp, place2=-0.219pp, place3=+0.164pp, top3_box=+0.182pp.  
Mixed results even on the strictest upset subset — no consistent advantage from decoupling.

### Class other — upset subset (n=4,045 holdout races)

λ_c=1.0 chosen; δ=0.000 by construction. λ=0.0 gives essentially the same scores.

---

## Per-Class Recommendations

| Class     | λ_c     | Deploy? | Key finding                                                                        |
| --------- | ------- | ------- | ---------------------------------------------------------------------------------- |
| **C**     | **1.0** | **NO**  | No improvement at any λ. Odds gain=2.56% but zero net effect from removal.         |
| **B**     | **1.0** | **NO**  | λ=0.1 won tuning but regressed -0.042pp top1 on holdout. Not significant (p=0.36). |
| **other** | **1.0** | **NO**  | λ is irrelevant — all values produce identical predictions.                        |

_Note_: The recommendation algorithm flagged class B λ=0.1 as `deploy=True` based on tuning
improvement, but the holdout evaluation (scored once, 2023-26) shows regression. Correcting
to `deploy=False` following the holdout evidence.

---

## Structural Finding: Why Decoupling Doesn't Help

The ensemble's odds-dependence is ~0.73–0.75 Spearman vs odds_score regardless of λ.
This occurs because:

1. **iter12 component is odds-correlated**: The XGB base model (50% ensemble weight)
   was trained with all features including odds. Its predictions remain correlated
   with the market. Removing odds from only the residual component (50% weight)
   cannot change the overall ensemble's dependence by more than ~50%.

2. **Odds features have low residual gain**: After iter12 absorbs much of the market
   signal, the LGB residual only extracts 0.43–2.56% of its gain from odds features.
   These features are redundant given what iter12 already captures.

3. **True decoupling requires retraining iter12 without odds** — not in scope for this
   experiment, and would require a full retrain of the base XGB model.

---

## Summary

**The market-efficiency hypothesis is confirmed for NAR at all class levels**: the
current models' heavy correlation with odds (-0.73 Spearman) is a structural property
of the ensemble architecture (iter12 base), not something addressable by removing
odds features from the LGB residual. The 3 odds features contribute only 0.43–2.56%
of LGB gain importance. Removing them yields no sustained improvement on holdout data.

**λ_c = 1.0 for all NAR classes (no decoupling deployed).**

A future avenue would be to retrain the iter12 base model without odds features
and rebuild the full ensemble — a larger undertaking outside the scope of this probe.

---

## Artifacts

- `tmp/odds-decouple/nar_results.json` — full per-class JSON: odds-dependence audit,
  λ-curves, chosen λ_c, holdout 4-axis + LB95, upset-subset deltas
- `tmp/odds-decouple/run_odds_decouple_nar.py` — experiment script
- `tmp/odds-decouple/run.err` / `run.out` — logs

_Total experiment time: 941s (15.7 min), threads=4, single process._
