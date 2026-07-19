# NAR served-vs-WF gaps → the unified market-null mechanism (2026-07-19)

**Investigator:** inv-2026-nar. **Scope:** NAR (Saga=55, Kochi=54, + all NAR venues) RS + FP served-vs-WF, 2026 real data. **Status:** both tasks closed; findings unify with JRA RS (#75), NAR FP (#69), Ban-ei FP (#70/#72).
**MLflow:** run `a39cb294d4fa491d8970fe0b9dccc0e1` in `finish-position/wf-eval` (note: the RS "calibration" tag in that run is SUPERSEDED — see below).

## Headline

One mechanism explains every served-vs-WF gap measured this campaign: **market/odds features are populated from settled/final data in training+WF but NULL (RS) or stale/median (FP) at serve.** Feature gain establishes exposure, not an additive recovery estimate: direct ablation measured the NAR HPF+FDF extension at only ~0.2pp. No matview train/serve SQL-dialect divergence, no inference-engine bug, no calibration (that was a reverted artifact, #65/#67).

| Task                | Champion                       | null-at-serve market dependence                                                         | served-vs-WF gap |
| ------------------- | ------------------------------ | --------------------------------------------------------------------------------------- | ---------------- |
| NAR RS              | nar-running-style-lgbm-prod-v3 | odds_score 2.59% + popularity_score 2.06% = **4.64%**                                   | **~2.3pp**       |
| JRA RS (#75)        | jra-running-style-lgbm-prod-v3 | odds+popularity+track_condition_normalized (bigger; track_cond null at serve for JRA)   | **~8.93pp**      |
| NAR FP (#69)        | iter12-nar-xgb-hpo-v8-clean188 | popularity_score 17.7% + horse_popularity_vs_field 16.7% + odds_score 13.9% = **49.3%** | **~16pp**        |
| Ban-ei FP (#70/#72) | banei champion                 | high (similar)                                                                          | **~16pp**        |

## NAR RS (task #51) — measurements

- **Served** (genuine, real `race_running_style_model_predictions`, prod-v3, 2026, generated within 3d of race, n=1653 labeled horse-rows): 4-class accuracy **0.4973**; nige recall 0.307, precision 0.536 (under-calls nige: predicted 8.3% vs actual 14.6%).
- **WF** (champion metadata `walk_forward_results`, 2026 held-out, raw, full features): accuracy **0.5202**, recall_nige 0.585.
- **Mechanism:** RS serve path (`running-style-feature-ctes.ts:451-468`, `running-style-sql.ts:171-172`) computes popularity_score/odds_score from `se.tansho_ninkijun`/`tansho_odds`, which are NULL for upcoming races; there is **no realtime-odds injection** in the RS path (unlike FP's `realtime_odds_rt`). Training/WF gets the settled values. `track_condition_normalized` is only 0.05% gain for NAR and is populated at serve (from babajotai), so it is not a driver here (contrast JRA RS #75).
- **Retraction:** the served nige-recall collapse (0.585→0.307) initially looked like calibration cost; it is the same regime-mismatch artifact reverted in #65/#67. The serve path does apply the OvR-isotonic calibrator (`running-style-calibration.ts`), but it is not the gap driver. NAR calibrator content is byte-equivalent to tracked `docs/.../nar-rs-v3-calibrators.json` (no wrong-artifact bug like JRA had). TS-tree inference parity is clean (#63).

## NAR FP (task #52) — measurements

- **WF-2026** (deployed clean188 scored on its own 2026 store partition `s11-pacestyle-FINAL`, cumulative metric): top1 **0.4627** (n=5409 races).
- **Served** (genuine live, Neon, 2026, current champions): iter40-blend top1 **0.3063** (n=271), clean188 **0.299** (n=147). Venue-matched (per-venue WF reweighted to served venue mix, n=418): expected WF 0.4645 vs served 0.3038 = **16.07pp gap**; every venue below WF 7–34pp. Steady-state across May+July (not an incident artifact).
- **Mechanism:** clean188 draws **49.3%** of gain from popularity_score/horse_popularity_vs_field/odds_score, built in the store from FINAL-CONFIRMED `nvd_se.tansho_ninkijun`/`tansho_odds` (`finish_position_features_duckdb.py`), but at serve COALESCEd from a realtime snapshot over hardcoded medians. **NaN-ablating those 3 features on the store collapses WF top1 to 0.25–0.28**, which brackets served 0.30 → at serve the odds features carry ~no signal.
- **#43 timing (corrected):** NAR had no coordinator odds rescore for the entire served window (07-08..18). The `RESCORE_CATEGORIES` widening to NAR/Ban-ei (`4b826464`, deployed 07-19 01:07 JST, NAR time-box 14:00-21:00 JST) had not fired yet at analysis time. (An earlier `07-11` per-race rescore commit `dc11a74a` was JRA-only.) So served ≈ 0.30 throughout is uniformly the pre-NAR-rescore regime; there is no real step at 07-11 (an earlier "+7pp at 07-11" note was a spurious arbitrary-boundary artifact and is retracted). Recovery after the new window remained a timestamped live-verification item, not an established result at the 08:00 cutoff.
- **Upstream producer coverage correction:** the later category-support audit refuted the claim that the odds producer omits NAR/Ban-ei. The producer and catalog event path are category-neutral; discovery explicitly covers `nvd_ra`, with Ban-ei venue 83 mapped to source `nar`. The apparent early-morning emptiness was before the intentional NAR sale gates (10:00 JST; generally 12:00 for night meetings). At matched ~T-2h35 on 2026-07-18, JRA/NAR/Ban-ei histories had 19/21/18 snapshots, and D1 coverage was 36/36, 22/22, 12/12. Therefore the NAR FP gap is a **stale/invalid pre-race odds serving** problem downstream, not missing producer category support.
- **Late-binding recompute (answers whether #43 re-derives the dominant features):** `predict_lib/rescore.py::apply_fresh_snapshots` recomputes five columns using helpers in `predict_lib/late_binding.py`, including `odds_score` (`compute_odds_score`) + `popularity_score` (`compute_popularity_score`) from the fresh snapshot — 13.9%+17.7% = 31.6% of gain. It does NOT re-derive `horse_popularity_vs_field` (16.7%) or `field_dominant_favorite_indicator`, but those are REDUNDANT: nulling {HPF,FDF} while POP+ODDS fresh costs only 0.2pp (0.4605 vs 0.4627). A fresh, valid direct-odds snapshot is therefore the high-impact prerequisite, while #74's HPF+FDF scope expansion is a ~0.2pp red herring. Recovery magnitude after #43 was not measured at the 08:00 cutoff and remains HOLD.
- The matview `race_entry_corner_features` does NOT steal fresh odds (its NAR 2026 odds are NULL → the COALESCE falls through to the parquet realtime odds).

## Capstone: the NAR FP model is a market-favorite identifier

Anchoring the FP gap against the naive market baseline (clean188, 2026):

| quantity                                                       | value     |
| -------------------------------------------------------------- | --------- |
| Market-favorite (tansho_ninkijun==1) WIN rate (n=8044 settled) | **0.449** |
| WF predicted-top1 == market favorite                           | **0.957** |
| WF top1                                                        | 0.463     |
| SERVED predicted-top1 == market favorite                       | **0.465** |
| SERVED top1                                                    | 0.30      |

With fresh odds (WF) the model picks the favorite ~~96% of the time and scores ≈ the favorite's own win rate (0.463 vs 0.449 — genuine edge over "just bet the favorite" is only **~~+1.4pp**). At serve, degraded odds (median fallback when the fresh snapshot is missing, per #26/#68) let it identify the favorite only 47% of the time → top1 collapses to 0.30. So the entire 16pp gap = **losing favorite-identification when serve odds degrade**, and served NAR FP (0.30) is ~15pp BELOW the naive "bet the favorite" baseline (0.449) — production currently does worse than betting the favorite.

Consequences: (1) the only lever that matters for NAR/Ban-ei FP served accuracy is odds-serving freshness (recompute popularity_score/odds_score from a present fresh snapshot — `late_binding.py` already does this when the snapshot exists; the bug is the missing-snapshot → median path, #26/#68). (2) WF-gated model/feature work on NAR FP is mostly measuring favorite-reproduction, not edge (+1.4pp) — near-worthless until serving is fixed. (3) #74's HPF+FDF scope expansion is a ~0.2pp red herring; they're redundant with the direct odds features.

## Cross-category natural experiment

The measured contrast is consistent with the serving path: JRA FP was healthy in its measured window, while NAR+Ban-ei had not yet entered the newly widened #43 rescore window and showed ~16pp gaps. The later producer audit proves NAR/Ban-ei odds are collected once their sale gates open; it does **not** by itself prove that the predictor receives a fresh, valid snapshot at the needed timestamp. The supported conclusion is a downstream pre-race serving-freshness problem. Exact recovery remains a timestamped live-verification question, not an automatic consequence of category widening.

## Implications / fix

1. WF numbers overstate achievable served accuracy in proportion to null-at-serve market-feature dependence. FP deploy/adopt gates (built on ~0.46 WF) are chasing an unreachable ceiling unless odds serving is fixed.
2. The high-impact NAR/Ban-ei FP lever is getting a fresh, valid pre-race odds snapshot from the already-supported producer path into `popularity_score`/`odds_score` at prediction time. Expanding #74 to `horse_popularity_vs_field` + `field_dominant_favorite_indicator` is bounded by direct ablation at **~0.2pp**, not ~15pp; treat it as hygiene/low priority.
3. For RS, realtime-odds injection remains unevaluated. **Do not retrain without market features:** JRA RS v1 already tested that exact construction and lost **-3.40pp** (UB95 -2.96; 18/19 cells) to prod-v3. NAR RS impact is small (~2pp) given 4.64% dependence.

**Open (not run):** exact NAR RS ablation (null odds_score+popularity_score on the WF, expected ~-2pp) — needs a v2 RS feature build to score prod-v3.
