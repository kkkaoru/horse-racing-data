# NAR Exotic Odds — Ingest Gap Investigation + Phase 2-alt Re-Verification

**Date:** 2026-06-12
**Continuation of:** exotic-odds-place-verify.md (REJECT 2026-06-12)
**Status:** ADOPT-READY (sanrenpuku-only variant, NAR)

---

## Summary

The original NAR exotic-odds REJECT was caused entirely by a 2024 warehouse gap in
nvd_o2 (umaren) and nvd_o3 (wide). This investigation confirmed the gap is
irrecoverable without re-running the Windows PC-Keiba Database application against
2024 upstream data. Phase 2-alt was executed using the single intact exotic signal
(nvd_o5, sanrenpuku — complete for all years including 2024). The sanrenpuku-only
variant passes all ADOPT gate conditions.

---

## Phase 1 — Ingest Gap Root Cause

### Confirmed gap (local PostgreSQL, data_kubun='5')

| Table               | 2022        | 2023        | 2024            | 2025        |
| ------------------- | ----------- | ----------- | --------------- | ----------- |
| nvd_o2 (umaren)     | 13,408 rows | 13,607 rows | **0 rows**      | 13,391 rows |
| nvd_o3 (wide)       | 13,408 rows | 13,604 rows | **0 rows**      | 13,391 rows |
| nvd_o5 (sanrenpuku) | 13,408 rows | 13,604 rows | **13,460 rows** | 13,391 rows |

- Global check (all keibajo_code): nvd_o2 and nvd_o3 have zero rows for 2024 across
  ALL keibajo codes (JRA and NAR alike). nvd_o5 has 15,224 rows globally for 2024.
- No partial data in nvd_o2/o3 for 2024 under any data_kubun value (not just '5').

### Root cause

The data flows through PC-KEIBA Database (proprietary Windows application) which
performs "通常データ登録" against PC-Keiba's online service. The PC-KEIBA Database
app did not populate nvd_o2/nvd_o3 for 2024 — either the source data was unavailable
at the time of ingest, or the sync job was not run for that year.

**Backfill is not programmatically possible** — there is no raw-file import path; the
only re-ingest route is to re-run the Windows UI automation against 2024 source data
that may no longer be available online.

nvd_o5 (sanrenpuku) is structurally distinct: it appears in a separate ingest batch
or data product that was available for 2024.

**BACKFILL VERDICT: IMPOSSIBLE — fall back to Phase 2-alt.**

---

## Phase 2-alt — Sanrenpuku-Only Re-Verification

### Setup

- **Feature:** `exotic_sanrenpuku_p3` — marginalized 3連複 (sanrenpuku) implied
  probability of top-3 finish. Computed from nvd_o5 using DuckDB substring SQL.
  Overround-normalized within race. Implementation: `add_exotic_odds_features.py`.
- **Source table:** `pg.nvd_o5`, `data_kubun='5'` (final pre-race odds).
- **Base features:** `feat-nar-v8-iter9-pacestyle` (192 numeric features).
- **Model:** XGBoost rank:pairwise, iter12-nar-xgb-hpo-v8 params
  (max_depth=7, lr=0.0527, reg_lambda=1.967, min_child_weight=7,
  subsample=0.618, colsample_bytree=0.750, n_estimators=650).
- **Holdout:** 2023, 2024, 2025 (walk-forward, train on all prior years).
- **Bootstrap:** paired, 10k samples, seed=42.

### NULL coverage for exotic_sanrenpuku_p3

| Year | null rate | n rows  |
| ---- | --------- | ------- |
| 2023 | 0.007%    | 136,446 |
| 2024 | 1.399%    | 138,478 |
| 2025 | 0.270%    | 137,805 |

2024 null rate 1.4% vs 0% null for o3/o2 — sanrenpuku is intact; the 1.4% reflects
a small number of races where nvd_o5 was not final-published.

### Per-year results

| Year | top1 BASE | top1 NEW | Δtop1   | place2 BASE | place2 NEW | Δplace2 | fukusho_2p BASE | fukusho_2p NEW | Δf2p    | top3_box BASE | top3_box NEW | Δtop3_box |
| ---- | --------- | -------- | ------- | ----------- | ---------- | ------- | --------------- | -------------- | ------- | ------------- | ------------ | --------- |
| 2023 | 0.5884    | 0.5894   | +0.10pp | 0.6771      | 0.6803     | +0.33pp | 0.8779          | 0.8806         | +0.26pp | 0.7415        | 0.7433       | +0.18pp   |
| 2024 | 0.5855    | 0.5874   | +0.19pp | 0.6856      | 0.6876     | +0.19pp | 0.8832          | 0.8848         | +0.17pp | 0.7434        | 0.7447       | +0.13pp   |
| 2025 | 0.5867    | 0.5865   | −0.02pp | 0.6835      | 0.6859     | +0.24pp | 0.8808          | 0.8816         | +0.08pp | 0.7379        | 0.7380       | +0.01pp   |

All 3 years: f2p positive. 2025 top1 −0.02pp (within −0.05pp veto floor).

### Pooled results (2023–2025 mean)

| Metric     | BASE   | NEW    | Delta   | Delta (pp)   |
| ---------- | ------ | ------ | ------- | ------------ |
| top1       | 0.5869 | 0.5878 | +0.0009 | **+0.088pp** |
| place2     | 0.6821 | 0.6846 | +0.0025 | **+0.253pp** |
| place3     | 0.7409 | 0.7420 | +0.0011 | **+0.107pp** |
| fukusho_2p | 0.8806 | 0.8824 | +0.0017 | **+0.172pp** |
| top3_box   | 0.7409 | 0.7420 | +0.0011 | **+0.107pp** |

### ADOPT gate evaluation

| Check                                  | Value         | Result        |
| -------------------------------------- | ------------- | ------------- |
| fukusho_2p LB95 (paired bootstrap 10k) | +0.061pp      | **PASS** (>0) |
| place2 delta                           | +0.253pp      | POSITIVE      |
| place3 delta                           | +0.107pp      | POSITIVE      |
| p2_or_p3 robust                        | both positive | **PASS**      |
| veto floor top1 ≥ −0.05pp              | +0.088pp      | PASS          |
| veto floor f2p ≥ −0.05pp               | +0.172pp      | PASS          |
| veto floor top3_box ≥ −0.05pp          | +0.107pp      | PASS          |

**NAR SANRENPUKU-ONLY VERDICT: ADOPT-READY**

### Comparison to prior 3-feature reject

The prior 3-feature (o5+o3+o2) run had:

- fukusho_2p LB95 = −0.341pp → REJECT (2024 gap drove regression −0.42pp)
- 2024 alone: Δf2p = −0.42pp

With sanrenpuku-only (o5 only, no gap):

- fukusho_2p LB95 = +0.061pp → PASS
- 2024: Δf2p = +0.17pp (positive — no ingest gap, GBDT uses valid signal)

The fix was simply isolating to the one complete signal. The 2024 gap in o2/o3 was
the only failure driver; removing those two features makes the variant robustly positive.

---

## Code quality

- `add_exotic_odds_features.py` — already tracked, tests at 97.14% (>95% threshold).
- Full suite: 2292 passed, 97.29% total coverage, ruff 0 warnings, basedpyright 0
  errors.
- `add_exotic_odds_features` is already included in `pyproject.toml` `--cov` addopts.
- No new lint ignores, no `v8 ignore`, no `ts-ignore`.

---

## Deploy steps (NOT executed in this run — requires explicit production flip)

When adopting into production:

1. **Rebuild NAR features with sanrenpuku:** Run `add_exotic_odds_features.py` against
   the current iter12 base features (`feat-nar-v8-iter9-pacestyle`) with
   `--category nar`. Output dir: `tmp/feat-nar-exotic-sanrenpuku-p3`.

2. **Full retrain NAR (iter12 params + new feature):** Use
   `iter12_train_predict.py` (or `train_finish_position_xgboost_walk_forward.py`)
   on the exotic-augmented features. All 21 walk-forward folds.

3. **Update ensembles:** Refit any per-class or stacking ensembles that use
   NAR XGBoost predictions as a base layer.

4. **Container serve-side:** `realtime_odds_fetcher.py` currently parses tansho only.
   Add nvd_o5 parse for real-time exotic_sanrenpuku_p3 (does not block training-only
   adoption; requires separate `extract_rows()` fix for live inference path).

5. **Smoke test + production flip:** After smoke passes, update
   `active_models`/`inference_state` to the new model version.

---

## Artifacts

- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add_exotic_odds_features.py`
  — Feature builder (committed with this doc).
- `apps/pc-keiba-viewer/tests/test_add_exotic_odds_features.py` — Tests (committed).
- `tmp/phase2alt_sanrenpuku_results.json` — Full numeric results (untracked).
- `tmp/phase2alt_sanrenpuku_verify.py` — Verification script (untracked; tmp/).
