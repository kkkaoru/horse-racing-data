# NAR + Ban-ei Finish-Position Accuracy Improvement Roadmap

**Date**: 2026-06-19  
**Scope**: Genuine remaining levers for NAR and Ban-ei after the per-class campaign completed  
**Horizon**: Ordered by expected-value; serve-side #1 dwarfs every model lever

---

## Context: What Is Already at the Frontier

This roadmap assumes the following exhausted levers are **DO-NOT-RETEST**:

- **NAR model levers**: NAR B per-class ML (ABORT), all 7 per-class CB residuals (iter30/36 ADOPTED or REJECT),
  window-ablation (net-neutral), Signal4 sire×keibajo (genuine WF +0.18pp place2 but SERVE-BLOCKED by nvd_um
  ~21% coverage, tied to #254), NAR 3YO/2YO age residual (iter37 REJECT — residual, not features), NAR C
  place-preserving objective (ABORT), NAR G1+F1 retrain (−0.63pp serve regression — NULL-routing trap).
- **Ban-ei model levers**: window-ablation (ABORT, fold-unstable), sectional/race-internal features (no gain),
  exotic odds (fukusho +1.69pp but top1 trade → REJECT), relationship features (PARTIAL — P-class only, n=25
  too small), odds-decoupling (top1 −7.95pp → REJECT).
- **Cross-category**: pgvector kNN, RL, meta-learning stacking, partial-ρ probes (log*odds_z, futan_rank,
  rs_p*\*, nige_vs_field) — all ABORT or REJECT for NAR/Ban-ei.

The NAR model is at its empirical frontier. Ban-ei global is saturated at market level (+0.15pp top1 vs market,
place2/3 within noise). The single biggest remaining lever is **serve-side accuracy**, not model-side.

---

## Lever 1 (HIGHEST EV): NAR Serve-Side Odds Timing Fix

### Hypothesis

NAR predictions are chronically served with median-fallback odds because the 03:00 JST cron fires before
NAR race odds open in D1. The I4 serve-skew diagnosis (`rootcause-i4-serve-skew-tax.md`) measured:

| Condition                        | NAR top1 |
| -------------------------------- | -------: |
| WF (true final odds)             |   57.62% |
| Full median fallback             |   47.78% |
| Observed (~40% success / 60% fb) |   51.93% |
| **Skew tax (full)**              |  −9.84pp |
| **Skew tax (observed mix)**      |  −5.68pp |

The race-prediction-guard already fires every 20 min during JST 10:00–20:40 (freshness re-prediction),
and the finish-position-predict plist has the 09:30 JST JRA run. However, the **6/18 NAR incident**
(diagnosed in task #43) reveals NAR races are still being served with pre-odds median fallback at 03:04
JST — meaning the guard-refresh cycle is not guaranteeing final odds reach every NAR prediction before race.

### Feasibility

Very high. The infrastructure (guard, plist, PREDICT_CATEGORIES env) is already built. The 03:00 cron
correctly scopes to `nar,ban-ei`. The guard fires at 20-min cadence during race hours. The gap is:

1. **Guard refresh path**: whether the guard's race-hours re-kick actually lands a fresh prediction with
   real odds before each NAR race post. NAR races start as early as 10:00 JST; the guard fires at 10:00,
   10:20, 10:40, etc. If the pipeline takes ~4–5 min and the guard fires just before post, the prediction
   may be the 03:00 run (median odds) not the guard-kicked run (real odds).

2. **Last-known-odds KV cache** (`rootcause-i4-serve-skew-tax.md` §5, option 2): for early-day guard runs
   before odds are open, reuse the most recently fetched `{race_key → (odds, rank)}` from D1 with a 2-hour
   TTL. Currently early-day fallback is OOD median; the cached last-known odds are significantly better.

3. **NAR D1 odds TTL**: the DO-Tier 4h TTL + retry fix (commit 5770360) was applied for NAR but the 6/18
   incident suggests the guard refresh post-TTL is not always sufficient.

### Expected gain

Recovering from observed ~5.68pp skew tax to near-WF (the 40% → 90%+ success path): **+3–9pp top1**
(lower end if coverage only improves from 40% to 70%; upper end if near-full coverage on all races).
This is 100–1000× larger than any model lever accepted in the past.

### Cost

Low: ops/scripting work only. No model training. No feature engineering. The fix is improving when and
how often the prediction pipeline runs with real odds before each race post time.

### Gate

Measure the pre- and post-fix odds coverage rate (success fraction from the pipeline logs), and spot-check
a sample of served predictions to confirm `odds_score ≠ 0.5048` (the NAR OOD median) for the served record
closest to race post time.

### Specific design actions needed

- **Diagnose 6/18 incident fully** (task #43 in progress): trace why NAR predictions on that date were
  served with 03:04 JST median-fallback. Was the guard's race-hours kicks not firing? Was the D1 query
  returning empty for those races? Was the lock held?
- **Ensure guard-race-hour window aligns with NAR post times**: NAR races can start at 10:00 JST. The
  guard fires at 10:00, 10:20, 10:40. With a ~4-min pipeline, the 10:00 fire finishes ~10:04. If the
  race is at 10:05, the updated prediction arrived in time. If 10:00 guard fires and the race is at 10:03,
  there is no margin. Consider whether the 20-min cadence is sufficient for the earliest NAR races.
- **Last-known-odds KV cache**: the early-day runs (before odds open) currently use median fallback.
  Implementing a KV cache or a "use prior snapshot if odds > median" heuristic would eliminate the 60%
  early-day fallback rate described in rootcause-i4.

---

## Lever 2 (MEDIUM EV): Ban-ei E-top2-Style XGBoost Override

### Hypothesis

JRA E-top2 (deployed iter22, commit b92a7ce) overrides CB rank-1 with XGB rank-1 when they point to
different horses and XGB rank-1 == CB rank-2. This yielded +1.42pp top1 / +0.75pp place2 on the JRA
blind holdout. The same structural pattern (CatBoost base + XGBoost probe) might transfer to Ban-ei.

### Feasibility Assessment

**Marginal — proceed only if a Ban-ei XGB can be shown to have non-trivial complementarity with the CB base.**

The key considerations:

1. **Ban-ei is already saturated at market level** (`ban-ei/README.md`). The CB model provides +0.15pp
   top1 vs market. There is essentially no headroom beyond odds in the Ban-ei signal space. An XGB
   might not learn anything the CB doesn't already know.

2. **n is very small**: Ban-ei holdout is ~5,928 races / ~55k entries over 2023–2026, vs JRA 11,703
   holdout races. Override fraction at JRA was 13.1% (453/3,455 races). At Ban-ei with the same 13.1%
   rate, that's ~776 overridden races. A +1.42pp lift on 776 races corresponds to ~11 additional
   correct top1 hits over the full holdout — statistically detectable (LB95 > 0) but marginal given
   Ban-ei saturation.

3. **The JRA E-top2 win depended on XGB having genuine diversity from CB**: the XGB model trained on
   `rank:ndcg` (244 features, same feature set, different objective + hyperparameters) learned
   complementary ranking priorities. Ban-ei's smaller corpus (~40k training entries) may not be enough
   to train a reliably distinct XGB. Overfit risk is higher.

4. **Ban-ei feature coverage**: Ban-ei has futan_juryo (weight load) as a distinctive feature not in
   JRA/NAR. An XGB trained to weight futan_rank more heavily than the CB might find a complementary
   angle on winner identification, particularly in P-class and E_named.

### Experiment design

If proceeded:

1. Train a Ban-ei XGB with `rank:ndcg`, same 174 feature columns as the CB base, on the same 2016–2022
   training window (matching the CB training window). Use seed diversity (3 seeds).
2. Measure: on the 2023–2026 holdout, what fraction of races have XGB rank-1 == CB rank-2 AND XGB rank-1
   != CB rank-1? (override candidate races)
3. Measure: within that override-candidate subset, does swapping CB rank-1/rank-2 improve top1 and
   preserve place3 (same gate as JRA E-top2)?
4. Gate: top1 delta LB95 > 0 AND place3 non-negative across all holdout years (Holm-adjusted).

### Expected gain

**0–1.5pp top1 (wide CI due to small n and saturation)**. The JRA E-top2 win was +1.42pp top1 with
n=3,455 races per fold. Ban-ei has only ~1,480 holdout races/year. If the same 13% override rate holds
and the override is equally precise, gain is ~+0.6–1.5pp top1 — but could be zero if saturation
eliminates the CB/XGB complementarity.

### Cost

Medium: one XGB training run on Ban-ei corpus (~40k entries, fast), plus eval script adaptation.
Lower than a full retrain. No feature engineering required.

### Verdict

**LOW-MEDIUM priority.** Worth a single cheap probe (train Ban-ei XGB, check override-fraction and
within-override accuracy) but with realistic expectations that saturation likely kills the signal.
Do not invest significant time if the probe shows override-accuracy < CB accuracy in the override
subset.

---

## Lever 3 (LOW-MEDIUM EV): NAR CatBoost-Place Override (Inverse E-top2)

### Hypothesis

NAR base is XGBoost (iter12), which is top1-strong but place2/place3-weak vs oracle. JRA E-top2
promotes XGB rank-1 over CB rank-1 for top1 preservation. The inverse for NAR: a CB model trained
with a place-aware objective (YetiRank@3 or NDCG@3 with graded place relevance) might be a better
place2 ranker than the XGB base, allowing a "CB rank-2 override" when CB rank-2 is not the same
as XGB rank-2.

### Feasibility

The iter30 CB residual ensembles (A/OP/NEW/MUKATSU/other) and iter36 LGB (C) were trained as residuals
over iter12 XGB and improve overall ranking. These residuals are already active in production. The
question is whether a FULL CatBoost retrain (not a residual) — trained purely on place2/place3 relevance
— would identify a complementary ranking for place that the XGB base misses.

**Key constraint**: the existing iter30 CB residuals already represent the CB-over-XGB complement. If
iter30 residuals saturated the CB-complement signal, a fresh CB standalone would not add further gain.
The iter30 approach was an additive blend (XGB score + CB residual), not a swap. An E-top2-style swap
(where CB rank-2 overrides XGB rank-2) is structurally different — it requires the CB model to predict
a DIFFERENT horse at rank-2 than XGB in a meaningful fraction of races.

**Evidence for feasibility**: NAR B class (ABORT) and all other NAR per-class REJECT results suggest the
CB residual approach is saturated. However, the SWAP pattern (like E-top2) is different from the blend
pattern (iter30). The direct comparison between blend results and swap potential has not been tested
for NAR.

### Design

1. Train a full NAR CatBoost model with `NDCG@3` (graded relevance: pos=1 → rel=3, pos=2 → rel=2,
   pos=3 → rel=1) on the same 2016–2022 window. This is a new standalone CB, not a residual.
2. Measure: fraction of races where CB rank-2 != XGB rank-2 (override candidates).
3. For those races: swap XGB rank-2 with CB rank-2. Measure exact place2 delta.
4. Gate: place2 delta LB95 > 0 AND top1 non-negative (no top1 trade).

### Expected gain

**0–0.5pp place2**. NAR place2 oracle ceiling is ~23% (from rootcause-i1-ceiling-market.md), and the
model is at 35.26% (already EXCEEDS oracle on exact-ordinal because the oracle is computed differently
— this is a known artifact of the oracle calculation). Real headroom for CB-place override is uncertain.
The G1/F1 retrain lesson applies: any modification to the CB feature distribution at serve time can
cause regression. A fresh CB standalone avoids the NULL-routing trap of a retrain but introduces
architecture divergence.

**More honest assessment**: the iter30 CB residual blends are already a form of this experiment — they
weight the CB signal additively. If those blends are saturated (they are, per the ABORT/REJECT results),
a CB swap is unlikely to break new ground unless it targets the specific case where CB rank-2 ≠ XGB
rank-2 AND CB is right. That rate-of-correctly-different-horses is the key unknown.

### Verdict

**LOW priority.** Worth consideration only after Lever 1 (serve-side) is confirmed, and only if:
(a) a standalone CB model can be trained at reasonable cost, and (b) a cheap probe shows the
CB rank-2 override rate is ≥ 10% with accuracy > random in those races.

---

## Lever 4 (EXTERNAL DEPENDENCY): Signal4 — NAR Sire×Keibajo on nvd_um Resolution (#254)

### Status

DEFER-SERVE-BLOCKED as of 2026-06-17 (`goal-signal4-serve-coverage-gate.md`).

The WF cheap-filter result (+0.18pp place2, LB95 +0.191pp) is genuine and still stands. The feature
(`sire_keibajo_win_rate`, `damsire_keibajo_win_rate`) cannot be deployed until `nvd_um` achieves ≥80%
coverage for 2024–2026 NAR horses.

Current serve coverage: **~22% (median last 30 days)**. Train coverage: **~99.7%**. Gap: −77.7pp.
This is 2.3× the g1f1 gap that caused −0.63pp serve regression.

### What needs to happen

Once `nvd_um` is resolved (either by a full N-Data backfill or an alternative NAR horse master with
≥80% 2024-26 coverage), re-run the serve-coverage gate. If coverage ≥80%:

1. Proceed to feature store rebuild (add `sire_keibajo_win_rate`, `damsire_keibajo_win_rate`).
2. Base retrain (NAR) with the new columns.
3. Residual retrains for all active per-class ensembles (C/A/OP/NEW/MUKATSU/other).
4. Apply MUKATSU zero-out (NAR MUKATSU was place2 −1.495pp for this feature — zero it at build time).

Expected gain (conditioned on ≥80% serve coverage): **+0.18pp place2 confirmed by WF, LB95 > 0.**
Actual serve gain likely in the same range. This is small but real and accumulated on top of iter12.

**This lever is currently not actionable.** Note it as a blocking external dependency.

---

## Summary: Ranked NAR + Ban-ei Roadmap

| Rank | Lever                                          | Category | Type | EV         | Cost   | Status      |
| ---- | ---------------------------------------------- | -------- | ---- | ---------- | ------ | ----------- |
| 1    | NAR serve-side odds timing / guard refresh fix | NAR      | OPS  | VERY HIGH  | Low    | ACTIONABLE  |
| 2    | Ban-ei E-top2 XGB override probe               | Ban-ei   | ML   | LOW-MEDIUM | Medium | ACTIONABLE  |
| 3    | NAR CB-place override (inverse E-top2)         | NAR      | ML   | LOW        | Medium | CONDITIONAL |
| 4    | Signal4 sire×keibajo (nvd_um #254)             | NAR      | FEAT | LOW-MEDIUM | High   | BLOCKED     |

### Top candidate to run first: Lever 1 — NAR serve-side fix

The 6/18 NAR incident (task #43, in-progress diagnosis) is the immediate trigger. Completing that
diagnosis and implementing the guard-refresh improvement is the single highest-EV action. The
~5–10pp top1 recovery from eliminating median-fallback at race time dwarfs every model lever ever
accepted (+0.05pp gate threshold). It requires no model training, no feature engineering, no
data infrastructure changes — only ensuring the prediction pipeline runs with real odds before
each NAR race post time.

---

## Supplementary: Why Ban-ei E-top2 Is Marginal

The JRA E-top2 result (+1.42pp top1, LB95 +0.58pp) was driven by CatBoost and XGBoost having
different ranking priorities on the same feature set — CB tended to weigh pace/form features
more conservatively, while XGB weighted market-signal more aggressively. When XGB's top pick
was CB's second pick, the XGB was right more often than the CB.

Ban-ei lacks this precondition:

1. **Saturation**: Ban-ei CB model is already at market level (+0.15pp top1 vs market). There is no
   structural "CB systematic error on winner" that XGB can correct. The CB model's ranking IS
   essentially the market ranking.

2. **futan_juryo asymmetry**: The one feature where Ban-ei differs from market odds is futan (weight
   load). But futan's signal is limited to P-class (partial ρ ≈ 0.18) and P-class has only ~25 races
   in the holdout — too few to measure any override effect reliably.

3. **Small corpus**: Training a reliable XGB on ~40k entries is feasible, but the diversity that
   made JRA XGB complement JRA CB depended on 700k+ training rows. Ban-ei XGB may overfit or learn
   the same ranking as CB.

The experiment is still worth a single training run and probe (the cost is low), but the prior
probability of a clean E-top2-style win for Ban-ei is materially lower than for JRA. Report the
override-accuracy within-override-subset as the go/no-go signal.

---

## References

- `rootcause-i4-serve-skew-tax.md` — NAR serve-skew tax measurement (~5.68pp top1 at observed 40% coverage)
- `history/serve-combined-recovery-measurement.md` — JRA serve-fix definitive measurement (+21.74pp with all 3 fixes)
- `per-class/ban-ei/README.md` — Ban-ei saturation, class structure, futan probe results
- `per-class/nar/README.md` — NAR per-class baseline table, all prior experiment refs
- `per-class/ROADMAP.md` — full per-class campaign status (B/3YO/999/016 completed)
- `history/goal-signal4-serve-coverage-gate.md` — Signal4 serve-blocked verdict, nvd_um gap
- `per-class/jra/etop2-deploy.md` — JRA E-top2 deploy doc (reference implementation)
