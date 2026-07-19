# JRA RS served-vs-WF gap — mechanism, ablation, and retrain-team handoff (2026-07-19)

**Author:** inv-2026-jra · **Status:** CLOSED — gap explained; retrain fix rejected; live-odds candidate unevaluated/HOLD.

## TL;DR

The JRA running-style (RS) "served (~38-42%) vs WF-capability (~45-48%)" gap is **explained** by a
market-feature train/serve availability skew: the champion `jra-running-style-lgbm-prod-v3` uses
`popularity_score` (dominant) / `odds_score` / `track_condition_normalized`, **populated in the
offline WF/eval store but NULL at genuine serve time** (the settled `jvd_se` odds/`ninkijun` are
still `'00'`/`'0000'` placeholders when RS predicts pre-race). The offline capability number is
therefore **inflated** by features serve lacks. Two fix directions:

- **A) retrain without market features — DEAD. DO-NOT-RETRY.** = RS v1, already lost −3.40pp to
  prod-v3 (`train-all + null-at-serve + refit calibrator`) on 2026-07-11.
- **B) populate market features at serve from LIVE odds — UNEVALUATED / HOLD.**
  The RS serve build has no realtime-odds join. Live odds are earlier than the final odds used in
  training, so no recovery magnitude or adoption claim is supported until a serve-real held-out gate.

## Mechanism + ablation (measured)

Method: exact prod flatbin (`latest.flatbin`, sha256 `278690ed…`, model_version
`jra-running-style-lgbm-prod-v3`, 146 feats), real TS serve inference
(`run-running-style-inference-local.ts` → `predictFlatRunningStyle`, confirmed clean vs Python
booster per task #63), raw softmax → argmax. Store: `tmp/inv-jra-rs-wf/store-raw-fixed`.

Per-feature ablation (null the feature everywhere → argmax accuracy), 2026 JRA held-out n=11,403:

| nulled                       | acc        | Δ         |
| ---------------------------- | ---------- | --------- |
| baseline (populated)         | 47.72%     | —         |
| `odds_score`                 | 47.86%     | +0.14     |
| `track_condition_normalized` | 47.72%     | 0.00      |
| **`popularity_score`**       | **41.00%** | **−6.72** |
| all three (serve reality)    | 38.79%     | **−8.93** |

`popularity_score` (normalized `ninkijun` rank) is essentially the whole effect. Per-surface:
dirt −10.60pp, turf −4.42pp. Confirmed the serve path returns these NULL via a 30-horse live
R2-SQL serve dump (all null) and the recent-race store slices (null).

## Reconciliation with the 2026-07-11 RS v1 rollback (decisive for the verdict)

The 2026-07-11 investigation (`project_rs_v1_rollback` memory; `tmp/candidate-rs-jra/serve-path-fix/`)
already tested the two candidate fixes and settled the current champion:

- **Retrain-without-serve-missing-columns = RS v1 → LOST −3.40pp** (UB95 −2.96, 18/19 cells) to
  the current prod-v3 ("train-all-cols + null-at-serve + refit calibrator"). Same-day rollback,
  zero races served on v1.
- On their clean population (n=62,092), nulled ≈ 51.59%. This **reconciles** with our re-run on
  2024-2025 (n=41,543 JRA): populated 59.49% → nulled 50.44% (−9.05pp, same magnitude as 2026).
  Their "97.76% nulled-vs-native match" was nulled-vs-serve(already-null), a different comparison
  than our nulled-vs-populated (65% match) — no real conflict once decomposed.

So the −9pp is real as an _offline-vs-serve_ gap. Two distinct fix directions remain — one dead, one unevaluated/HOLD:

## Verdict + handoff

**Gap = eval optimism** from scoring serve-null market features (`popularity_score` dominant).
The serve number is near RS's achievable ceiling _under the current serve pipeline_.

- **Fix A — retrain WITHOUT market features: DEAD. DO-NOT-RETRY.** Already tried as RS v1
  (2026-07-11), lost −3.40pp (UB95 −2.96, 18/19 cells) to prod-v3 (`train-all + null-at-serve +
refit calibrator`). The nulled-original control established that this construction regresses;
  do not reopen it from a new ablation result.

- **Fix B — populate market features at serve from LIVE odds: UNEVALUATED / HOLD.** Structurally confirmed:
  both train and serve read `tansho_ninkijun`/`tansho_odds` from the SETTLED `jvd_se`/`nvd_se` record
  (serve: `pc-keiba-r2-catalog/src/r2-sql.ts:196-197`, `try_cast(nullif(se.tansho_ninkijun,'00'))` /
  `nullif(se.tansho_odds,'0000')`), and there is **no realtime/live-odds join anywhere in the RS
  feature build**. Wiring it to REALTIME_HOT/odds_snapshots (as the viewer already reads odds)
  would populate `popularity_score` at serve, but the −6.72pp final-odds ablation is not a valid
  live-odds recovery estimate.
  - **Why not tonight:** (1) new production code (RS has zero live-odds join today) → needs the
    standard branch+tests+coverage-gate discipline, not an incident-style deploy; (2) genuine
    train/serve distribution question — training uses FINAL settled odds, live serve odds are
    earlier/preliminary — that needs real held-out live-odds data to evaluate (not testable locally
    tonight); (3) it must be a **proper WF-loop evaluation** that BEATS prod-v3's train-with +
    null-at-serve baseline with the **nulled-original as the control arm** (07-11 lesson), not an
    ad-hoc check.
  - **Broader structural link — task #68:** #68 established that RS has **zero late-binding refresh
    mechanism for ANY feature** (found via `current_bataiju`/weight never refreshed at serve), not
    just odds. Fix B is one unevaluated candidate instance of that gap; any engineering project needs a
    general RS late-binding refresh path (odds/popularity + weight + other serve-fresh inputs), same
    tier as the speed levers (#46). Also ties to #74/#76 (NAR odds-snapshot-freshness).
- **Contrast with FP:** NAR/Ban-ei FP run nearer post time and their upstream producer/category
  coverage exists. Their high-impact issue is whether a fresh, valid direct-odds snapshot reaches
  late binding; #74's extra relative columns are only ~0.2pp by direct ablation. RS runs earlier,
  and its live-odds alternative remains unmeasured.

## Calibrator note (separate red herring, resolved)

The deployed JRA `calibrators.json` differs from the tracked `docs/.../jra-rs-v3-calibrators.json`
(deployed = raw isotonic breakpoints; docs = 100-knot linspace). docs-v3 looked +2.52pp — but ONLY
in the populated-odds regime. Under serve-real NULL-odds, the ranking flips: the original wins
+4.26pp (its oikomi-heavy reshape is serendipitously robust to the market-feature collapse).
Deployed docs-v3, caught the regime error, **reverted** (R2 byte-verified back to original). No net
production change. **Lesson: evaluate serving-path calibrators/features under serve-real feature
availability, not the offline store.** Detail: `rs-calibration-implementation.md` (2026-07-19
section) + `apps/pc-keiba-viewer/tmp/inv-jra-rs-wf/calibrator-deploy-bug-2026-07-19.md`.

## Follow-up (hygiene, post-campaign)

CI/health hash-guard: compare the deployed R2 `{jra,nar}/calibrators.json` hash against the tracked
`docs/.../{jra,nar}-rs-v3-calibrators.json` — the JRA mis-upload was silent (serving degraded but
didn't error, and `tryLoadCalibrators` accepts any valid-but-wrong file).
