# JRA FP Stage-1 market-free fallback — graceful-degradation feasibility probe (2026-07-22)

**Author:** c1*jra_newvenue · **Status:** PROBE COMPLETE — FEASIBLE / positive. **NOT deployed, no adoption, no serving-path change.** Records a measured feasibility result for a \_gated* two-stage graceful-degradation architecture for JRA finish-position (FP).
**MLflow:** run `0bae46a693ab452f869e62347b1e1ef4`, experiment `finish-position/wf-eval` (raw-SQL-verified durable in Neon, `register=false`/`champion=false`).
**Verification:** triple-anchored — sanctioned `jra_v9sim_wf.py` harness + independent `retest_wf.py`-core reproduction (c1_banei_recheck, decimal-exact) + advisor sign-off.
**Related prior art:** `serve-contamination-and-coordinator-disabled-2026-07-22.md` (the odds-serving incident conditions — Cluster-B, odds-freeze, `COORDINATOR_ENABLED=0` — that this fallback insures against, and the USER-gated root-cause fix it must sequence behind) and `serve-skew-campaign-consolidation-2026-07-18.md` (the market-feature look-ahead / serve-degradation mechanism).

## TL;DR

The JRA FP champion (`jra-cb-v9-sim-2013-clean`, 250 feat, ~59.7% market gain-share) **collapses to ~9.4% top1 when its market/odds features go NaN/median at serve** during an odds-serving incident (Cluster-B 2026-07-12, odds-freeze 2026-07-18, coordinator off since 07-18). A **genuinely retrained market-free model** (235 non-market features) reaches **28.89% top1** on blind WF and **recovers +19.45pp [LB95 +18.44] over the collapsed champion**, uniform across all 22 cells. It costs −4.75pp vs the full champion in the healthy (odds-fresh) regime — so it is a **gated fallback, never a primary replacement**: serve the champion when odds are fresh, the Stage-1 model only when a freshness gate fails.

**This mitigates the incident tail; it does NOT replace the root-cause fix** (odds-serving reliability / `COORDINATOR_ENABLED=0`, USER-gated), which restores the champion's full ~33.6% during incident windows and is the upper-bound-better lever.

## Why a market-free MODEL is the right floor (not a heuristic)

During a full odds-serving collapse the **market-favorite identity itself is unavailable** — the broken/stale odds snapshot is exactly what's missing — so "just bet the favorite" is NOT an available fallback. A model that ranks from serve-robust non-market features (form/speed/running-style/corner/pedigree) is the only thing that can produce a ranking when odds are gone. This is why the collapse is so severe (the champion delegates ~all within-race ranking to the 15 market features) and why the fallback must be a market-free model.

## Method (sanctioned, self-validated)

Reused the exact harness that produced the champion: `apps/pc-keiba-viewer/tmp/candidate-leak-clean-retrain/jra_v9sim_wf.py` — DuckDB store (`tmp/candidate-eval-jra/augmented`), `split_train_valid` blind folds (train ≤ Y-1; folds 2023/2024/2025), CatBoost YetiRank identical hyperparams (iterations 300, lr 0.05, depth 8, l2 3.0, relevance 3/2/1, `no_cat_features`, seed 42+fold), `learning.subgroup_diagnostics` rank-1-6 hit metrics, paired race-bootstrap LB95. n = 10,365 blind races / 141,523 rows.

**Arms:**

- `B_pop` — champion `jra-cb-v9-sim-2013-clean` (250 feat), market features POPULATED = healthy regime.
- `C_pop` — **Stage-1 market-free**: champion feature set minus the 15 market features = **235 feat, GENUINELY RETRAINED** through the harness (fresh CatBoost `.fit()` per blind fold). Confirmed from saved models: `models_jra_v9sim/armC/fold-{2023,2024,2025}` = 235 feats, ZERO market features, mtimes 2026-07-22 00:45/00:46/00:47 JST (sequential per-fold training tonight); champion `armB` = 250 feats with all market features, mtimes 2026-07-04 (reused, untouched). This is a real retrain, not the champion re-scored with columns dropped.
- `B_null` — champion with the 15 market features → NaN = the OOD / incident-collapse regime.
- `MKT` — bet-the-favorite baseline (`predicted_score = -tansho_ninkijun_raw`), i.e. FINAL-odds favorite.

**15 market features removed for Stage-1:** `popularity_score`, `odds_score`, `tansho_odds_raw`, `tansho_ninkijun_raw`, `inverse_odds_implied_prob`, `inverse_odds_market_share`, `inverse_odds_rank_in_race`, `popularity_rank_in_race`, `odds_score_diff_from_race_avg`, `popularity_score_diff_from_race_avg`, `popularity_odds_disagreement`, `field_dominant_favorite_indicator`, `horse_popularity_vs_field`, `sim_odds_rank_correlation`, `sim_odds_correlation_variance`.

**Self-validation:** `B_pop` = 33.63% reproduces the cached champion `B_clean` (`jra_v9sim_wf_report.json` `pooled_raw.B_clean`) EXACTLY; `MKT` = 33.60% matches the independently-measured JRA favorite win-rate.

## Results (pooled top1, blind WF 2023-2025)

| arm                                                   | top1       | meaning                 |
| ----------------------------------------------------- | ---------- | ----------------------- |
| `B_pop` champion (market populated)                   | **33.63%** | healthy regime          |
| `C_pop` Stage-1 market-free (235 feat, fresh retrain) | **28.89%** | the incident floor      |
| `B_null` champion market→NaN                          | **9.44%**  | incident / OOD collapse |
| `MKT` bet-the-favorite (final odds)                   | **33.60%** | naive baseline          |

**Paired bootstrap (top1, delta [LB95]):**

| comparison                                                        | delta_pp   | LB95       | note                               |
| ----------------------------------------------------------------- | ---------- | ---------- | ---------------------------------- |
| **`C_pop` vs `B_null` (incident recovery — THE decision metric)** | **+19.45** | **+18.44** | GATE ACCEPT                        |
| `C_pop` vs `B_pop` (healthy-regime cost as primary)               | −4.75      | −5.56      | expected; why it must be gated     |
| `C_pop` vs `MKT`                                                  | −4.72      | −5.54      | see caveat below                   |
| `B_pop` vs `MKT` (champion ≈ favorite)                            | +0.03      | −0.41      | edge ~0 (JRA analog of NAR +1.4pp) |

**Independent confirmation (c1_banei_recheck, canonical `retest_wf.py` sort-before-mask core, same 4 pred parquets):** recovery `B_null`→`C_pop`: top1 +19.45 [+18.39], place2 +8.02 [+7.11], place3 +4.38 [+3.54], top3_box +5.93 [+5.43], fukusho_2p +34.63 [+33.37] — **GATE ACCEPT (3/3 primaries, all LB95>0)**; **cells @0.08pp: ALL 22 pass** (every venue 01-10, kyori_band 0-3, season 0-3, baba 1-4). Healthy cost `B_pop`→`C_pop`: −4.75 [−5.59], matches. (LB95 +18.39 vs +18.44 = different bootstrap seed only; point estimates identical.)

**Per-fold (stable):** `C_pop` 28.33 / 29.99 / 28.34; recovery +18.0 / +21.8 / +18.5 (2023/24/25).
**Per-venue:** Stage-1 25.6–32.5%; incident recovery +13.7 to +23.5pp UNIFORM across all 10 venues (FOCUS 01/04/07 +16.9/+19.3/+20.6 ≈ estab 02/03/10). **Rank 2-6:** `C_pop` is within ~1pp of the champion on place2-6 (market features mostly help TOP1 favorite-ID, not deeper ranks).

## Interpretation

1. **Resolves the earlier "Stage-1 floor" uncertainty in the GOOD direction.** Prior training-free proxies on the champion were degenerate/uninformative: market→within-race-mean neutralization scored 6.01% (SUB-random vs the 7.77% [7.26,8.29] random-within-field baseline), NaN-collapse 9-12%. A GENUINE retrain lands at 28.89% — a real, usable floor. Do not cite the 6% mean-arm as a floor; it is a degenerate artifact.
2. **The −4.72pp vs `MKT` is not damning.** `MKT` needs FINAL odds — exactly what's gone during the incident Stage-1 insures against — so Stage-1 "loses" only to an option that is equally unavailable in that scenario. The decision-relevant comparison is `C_pop` vs `B_null` = +19.45pp.
3. **Serve-robustness of the 235 non-market features is credible for JRA** (not merely "bounded"): the near-miss aggregate serve-skew family was already shadow-scored at ZERO accuracy impact (108 cell×fold, earlier this campaign), and the weight/`bataiju` hex-decode bug found 2026-07-21 is Ban-ei-ONLY (JRA stores weight as decimal, confirmed in code). So 28.89% is a trustworthy serve floor for JRA.

## Lineage-mismatch correction (recorded for the record)

An interim addendum reported `B_pop` 33.84% / `B_null` 11.60%. That accidentally scored an EARLIER 231-feature iter26-lineage arm (`models_jra/armB`) — both per-fold blind (NOT a leakage issue), just the wrong lineage. This run uses the correct 250-feature champion lineage (`models_jra_v9sim/armB`), whose `B_pop` reproduces the cached champion `B_clean` exactly. **The 231-feat numbers (33.84/11.60) are superseded by the 250-feat numbers (33.63/9.44).**

## Proposed architecture (design only — NOT built)

- **Stage 1 (floor):** the 235-feature market-free model, serves whenever the freshness gate fails.
- **Stage 2 (default/healthy):** the current champion, serves whenever the gate passes. Steady-state accuracy unchanged — this ADDS a fallback, does not replace the primary. (Contrast RS v1, which REPLACED the primary with a market-dropped retrain and lost −3.40pp — a gated fallback is a different decision in kind and is NOT that.)
- **Freshness gate (reuse existing signals):** at prediction time, did late-binding receive a fresh, valid, non-median odds snapshot for this race? (`predict_lib/late_binding.py` already determines present-vs-median-fallback.) Optional post-hoc safety net = within-race `predicted_score` stddev ≥ 0.3 (`serve_health_check.py`'s Cluster-B quality signature).
- **Alternative single-model variant (worth comparing):** train the champion with odds-feature DROPOUT so it degrades gracefully instead of OOD-collapsing — simpler, no routing, but risks anchoring to the training-time missingness pattern; an architectural gate can swap in a model never asked to guess at absent features.

## Explicit flags (per team-lead)

- **(a) UPDATE 2026-07-22 (later same night) — BUILT AND DEPLOYED, no longer probe-only.** Following explicit user authorization, this probe was built out into a real production artifact + serving-path change. A genuine full-history retrain (`jra-cb-stage1-marketfree235-2013`, 235 feat) was trained with the exact same recipe/window/seed as the champion (CatBoost YetiRank, iterations 300, lr 0.05, depth 8, l2 3.0, relevance 3/2/1, no_cat_features, seed 20260519, full 2013-01-01..2025-12-31 single fit, 626,798 rows / 44,907 races — identical population to the champion's own build), registered in `production-artifacts.json` per this app's artifact-integrity convention (`predict_lib.artifact_integrity`), and wired live via a new `predict_lib.stage1_routing` gate. The probe's `register=false`/`champion=false` MLflow run `0bae46a693ab452f869e62347b1e1ef4` remains the measurement-only run and is unchanged; the production training run is logged separately with `register=true`.
- **(b) UPDATE 2026-07-22 (later same night) — done, not separate/future work.** The freshness gate (`predict_lib.stage1_routing.race_has_fresh_odds`, reading `late_binding`'s raw `tansho_ninkijun` — never itself median-substituted, unlike the derived `odds_score`/`popularity_score`) and the stddev safety net (`is_score_spread_degraded`) are both wired into the shared `_score_and_flush_races` core in `predict_upcoming.py`, so both `mode=full` and `mode=rescore` get the identical gated fallback with zero duplicated logic. The stddev threshold was independently re-validated against real Neon prediction data rather than adopted as-is from this doc's proposal: 2026-07-12 Cluster B (36/36 races, stddev 0.048-0.160), 2026-07-18 odds-freeze (36 races, stddev 0.114-0.354, a CONTINUOUS gradient — consistent with `COORDINATOR_ENABLED=0` staleness rather than total odds absence, so the originally-proposed 0.3 would have missed 7/36 (~19%) of that day's degraded races), 2026-07-19 healthy (36 races, stddev 0.749-1.870) and 2026-07-11 (a second, independently documented mixed-incident day, stddev down to 0.044). Final tracked value: **0.4** (`predict_lib/stage1_routing.json`, tunable without a code change) — covers the full observed incident range with margin while keeping a >0.3 margin below the observed/documented healthy floor. Default-Stage-2 / fallback-to-Stage-1-on-gate-fail is unit-tested in `tests/test_stage1_routing.py` (52 cases, 100% coverage of the new module).
- **(c) STILL TRUE — sequence behind or ALONGSIDE the root-cause fix, not instead of it.** Graceful degradation mitigates the incident tail; restoring odds-serving reliability (`COORDINATOR_ENABLED=0`, USER-gated; see `project_coordinator_disabled_2026_07_21`) restores the champion's full ~33.6% in incident windows and is the upper-bound-better lever. This gated fallback and the coordinator fix are complementary, not substitutes: the fallback bounds the downside whenever odds serving degrades for ANY reason (not only this one root cause), while the root-cause fix is what actually recovers the champion's full accuracy during a known incident window.

## Artifacts

- Probe: `apps/pc-keiba-viewer/tmp/candidate-leak-clean-retrain/models_jra_v9sim/armC/fold-{2023,2024,2025}/model.json` (235-feat Stage-1 models).
- Scratch scripts + report: session scratchpad (`stage1_market_free_probe.py`, `stage1_probe_report.json`, `preds/{B_pop,C_pop,B_null,MKT}.parquet`).
- Independent scoring: `apps/pc-keiba-viewer/tmp/probe-scoring-2026-07-22/reports/jra_recovery_Bnull_vs_Cpop.json` + `jra_healthycost_Bpop_vs_Cpop.json`.
- MLflow run `0bae46a693ab452f869e62347b1e1ef4`.
