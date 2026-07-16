# Task #37 execution — volatility-tiered family sub-score fusion (2026-07-11)

- **Date**: 2026-07-11
- **Category**: JRA finish-position architecture lever, executing task #37 from
  the 2026-07-04 lever bank (`tmp/frontier-scout/lever_bank.md` #1,
  "highest-value, UNTESTED"). This doc is the execution record the bank entry
  and both precursor docs explicitly called for.
- **Precursors** (2026-07-04, both read in full before executing):
  `docs/probes/race-volatility-model-2026-07-04.md` (odds-free race-level
  volatility classifier, AUC 0.58-0.61, `n_horses` the dominant feature) and
  `docs/probes/jra-nonconforming-signal-decomposition-2026-07-04.md` (on
  market-non-conforming races, `physical`/`style_pace`/`speed_time` are the
  best-retaining and least market-redundant non-market feature families).
- **Design under test**: keep the champion's own 250-feature score
  (`base_score`) untouched on ordinary races; on races the volatility
  classifier tags as high-volatility, blend in a second score
  (`boost_score`) trained only on the `physical`/`style_pace`/`speed_time`
  columns. Score-fusion, not routing/gating and not a post-hoc rank swap
  (confirmed against the session's prior REJECTs: confidence-shrinkage,
  upset-gate cascade, label reweighting, odds-blind override — none of those
  blend two model scores conditioned on a continuous tier).

## Stage 1 — field-size-only ablation (pre-registered first step)

Both precursor docs explicitly flagged that the volatility classifier's
single largest feature by gain is `n_horses` (field size), and recommended
checking whether the classifier is materially more than a field-size proxy
before building anything on top of it. Reused the exact walk-forward fold
structure from `train_volatility_model.py` (same train windows, same
date-cutoff early-stop split, same label) but fit a single-feature LightGBM
(`n_horses` only) instead of the 82-feature set, then compared to the
existing `volatility_scores.parquet` on the same OOS races (2023/2024/2025).

| check                                                          | 2023  | 2024  | 2025  |
| -------------------------------------------------------------- | ----- | ----- | ----- |
| Full-model AUC                                                 | 0.577 | 0.611 | 0.607 |
| Field-size-only AUC                                            | 0.553 | 0.578 | 0.573 |
| AUC-lift-above-chance captured by field size alone             | 69.1% | 70.1% | 68.4% |
| Spearman(full score, `n_horses`)                               | 0.602 | 0.648 | 0.630 |
| Quintile exact-tier-match rate (chance=20%)                    | 37.4% | 37.8% | 39.3% |
| Quintile adjacent-tier rate (chance≈52%)                       | 75.2% | 77.0% | 76.2% |
| Top-volatility-tier vs largest-field-tier overlap (chance=20%) | 34.7% | 33.3% | 33.4% |

**Verdict: does not collapse to `field_band`.** Field size is real
signal — it explains roughly two-thirds of the classifier's AUC lift — but a
third of the AUC lift and 60%+ of the tier assignments are genuinely
something else. This is "partial overlap, real residual signal," not "≈"
field size. Cleared the stop condition; proceeded to stage 2. Artifacts:
`tmp/ms-volatility-fusion/{field_size_ablation.py,field_size_ablation.json,field_size_scores.parquet}`.

## Stage 2 — fusion construction and evaluation

### Sub-score

`boost_score` = a second CatBoost YetiRank model, **identical hyperparameters
to the live champion** (`iterations=300, depth=8, lr=0.05, l2_leaf_reg=3.0,
loss=YetiRank`, `no_cat_features`, same relevance labels 3/2/1), trained on
the CLEAN leak-free armB feature set (`tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`)
**restricted to the union of the `physical` + `style_pace` + `speed_time`
family columns** from the decomposition doc's `families.py` (83 nominal
columns, 78 present in armB — the 5 missing, `weight_zscore`/`seibetsu_code`/
`barei`/`zogen_sa`/`shusso_tosu`, are all previously-REJECTed or
categorical-excluded columns not in the champion's feature set at all).
`base_score` = the unmodified champion, reusing the **already-cached** 250-feat
models under `tmp/candidate-masked-lever-retest/models/base/` (3 seeds x 3
folds, trained by an earlier lever's harness on this exact CLEAN 250-feat
spec) — no retraining, no risk of the two scores diverging on anything but
the feature-set restriction. 9 new `boost_score` fits (3 seeds x 3 folds,
same seed convention `seed_base+fold_year`) trained under
`tmp/ms-volatility-fusion/models/boost/`.

### Fusion

Both scores z-scored within race
(`(score - race_mean)/race_std`). Fused score = `z_base + w * z_boost` for
races whose volatility tier is in the selected high-volatility tier set,
`z_base` unchanged otherwise (soft, continuous nudge — not a hard rank-K
swap). Volatility tiers = quintiles of the existing `volatility_scores.parquet`,
cut points **fit on pooled 2023+2024 only and frozen** before touching 2025.

### Selection sweep (2023+2024 pooled, seed-averaged over 3 seeds) — ALL 10 combos fail

| config             | top1 Δpp | place2 Δpp | place3 Δpp | primaries passed |
| ------------------ | -------- | ---------- | ---------- | ---------------- |
| top1 (Q5) w0.15    | +0.043   | -0.019     | +0.019     | 0/3              |
| top1 (Q5) w0.30    | -0.010   | -0.072     | +0.024     | 0/3              |
| top1 (Q5) w0.50    | +0.039   | -0.092     | +0.043     | 0/3              |
| top1 (Q5) w0.75    | -0.072   | -0.154     | -0.010     | 0/3              |
| top1 (Q5) w1.00    | -0.183   | -0.121     | +0.005     | 0/3              |
| top2 (Q4+Q5) w0.15 | -0.005   | -0.019     | +0.063     | 0/3              |
| top2 (Q4+Q5) w0.30 | -0.077   | -0.072     | +0.053     | 0/3              |
| top2 (Q4+Q5) w0.50 | -0.077   | -0.106     | +0.068     | 0/3              |
| top2 (Q4+Q5) w0.75 | -0.294   | -0.164     | +0.029     | 0/3              |
| top2 (Q4+Q5) w1.00 | -0.492   | -0.183     | +0.053     | 0/3              |

No combo clears the 0.08pp/LB95>0 gate on any primary. Effect sizes are all
inside ±0.5pp with inconsistent signs across adjacent weights — the signature
of noise, not a monotonic dose-response a real effect would produce. Best by
selection score (`n_primaries_passed`, then top1 Δ) = `top1(Q5-only) w=0.15`
(the gentlest touch: only the top volatility quintile, small weight).

### Blind 2025 confirm (selected config)

| metric     | base    | fused   | Δpp        | LB95pp |
| ---------- | ------- | ------- | ---------- | ------ |
| top1       | 11.076% | 11.124% | +0.048     | -0.048 |
| place2     | 6.667%  | 6.628%  | -0.039     | -0.116 |
| place3     | 4.341%  | 4.235%  | **-0.106** | -0.203 |
| place4     | 3.599%  | 3.560%  | -0.039     | -0.145 |
| place5     | 2.682%  | 2.827%  | +0.145     | +0.039 |
| place6     | 2.151%  | 2.113%  | -0.039     | -0.125 |
| top3_box   | 0.000%  | 0.000%  | 0.000      | 0.000  |
| fukusho_2p | 35.195% | 35.301% | +0.106     | -0.077 |

0/3 primaries pass, `place2_or_place3=false`, worst metric -0.106pp (within
the -0.05pp no-regression floor but negative). Pooled 3-fold top1 Δ = **+0.045pp**,
below the 0.4pp noise floor and well below the campaign's own measured
pure-retraining-seed-noise level (+0.309pp, `tmp/dead-lever-retest/noise_floor/seed_noise_report.json`,
same features/folds, only the CatBoost seed differs). **Noise-suspect.**

## Per-cell / per-tier breakdown (2026-07-11 directive: pooled numbers alone forbidden as decision basis)

Per an updated evaluation directive mid-session, adoption is assessed
per-cell/per-tier on the **blind 2025 fold**, not just pooled — a cell or
tier that clears the gate is a conditional-ADOPT candidate even if the
pooled number fails. Ran venue / surface / distance_band / class / volatility-tier
marginal cells (n≥50) plus the explicitly-requested volatility-tier × venue
cross and summer-3-venue (Hakodate/Fukushima/Kokura) pooled cell, all on
2025 only, using the selected config (tier=Q5-only, w=0.15).

- **Tiers 0-3 show exactly 0.000pp delta everywhere by construction** — the
  selected config only touches the top volatility quintile, so races in the
  other 4 tiers get an identical `z_fused == z_base` and identical predicted
  ranks. This also mechanically dilutes every venue/surface/distance*band/class
  marginal cell toward ~0 (each mixes ~80% untouched tier-0-3 races with ~20%
  tier-4 races), so those four marginal tables are not very informative for
  \_this* design — reported per the directive, but the volatility-tier cut is
  the only cell where the design does anything.
- **Tier 4 (the only active tier), 2025, n=616**: top1 +0.271pp[LB95 -0.271],
  place2 -0.216pp[LB95 -0.704], **place3 -0.595pp[LB95 -1.082]**. Directionally
  positive on top1 but not significant, and a real regression on place3.
- **Volatility-tier 4 × venue** (n≥50 cells, 8 venues qualify): top1 deltas
  range from -0.844pp (venue 10) to +2.260pp (venue 05, n=59) — no cell's
  LB95 clears 0 on any primary. The one large point estimate (venue 05,
  +2.260pp) has LB95 -0.565pp, i.e. not distinguishable from noise at n=59.
- **Fold-consistency check on tier 4** (the natural design cell, all 3 folds):
  top1 Δ = 2023 **-0.046pp**, 2024 **+0.510pp**, 2025 **+0.271pp** — 2/3
  folds positive but not sign-consistent across all 3, and the magnitude
  swings by more than 10x between adjacent folds with no monotonic trend.
- **Summer-3-venue pooled** (02/03/10, n=624): top1 -0.053pp[LB95 -0.320],
  place2 +0.160pp[LB95 -0.107], place3 -0.214pp[LB95 -0.427] — no win.
- **Systematic scan** over every reported cell/tier (venue, surface,
  distance_band, class, vol_tier, vol_tier×venue, summer-3-venue) for any
  row with LB95>0 on ≥1 primary and Δ≥0.08pp on ≥1 primary: **0 candidates
  found.** Full table: `tmp/ms-volatility-fusion/blind_cell_report.json`.

## Verdict: REJECT

Every level of evidence points the same way: the full 2x5 selection sweep is
flat/noisy with no dose-response, the blind 2025 confirm fails all 3
primaries with a real place3 regression, the pooled top1 signal sits below
both the noise floor and the campaign's own measured pure-seed-noise level,
and the exhaustive per-cell/per-tier scan required by the updated evaluation
directive found zero conditional-ADOPT candidates anywhere — including the
one tier the design actually touches, which is fold-inconsistent. This is
**not** a `COLLAPSED-TO-FIELD-SIZE` result (stage 1 cleared that bar cleanly)
— the volatility classifier carries real residual signal beyond field size,
but that residual signal did not translate into a usable re-ranking edge
when fused with a physical/style_pace/speed_time sub-score at any weight or
tier threshold tested.

**DO-NOT-RETEST this exact design** (z-score additive fusion of a
family-restricted CatBoost sub-score, gated by volatility quintile, on the
CLEAN armB-250 champion). Untested variants that remain open if a future
session wants to revisit the underlying idea: (a) a stronger boost sub-model
(the 78-feature model is necessarily weaker than the 250-feature champion,
which may dilute rather than sharpen the blend — a stacked meta-learner
instead of hand-tuned weights, consistent with this campaign's established
GBDT-stacking preference, was not tried here for compute-budget reasons);
(b) decile instead of quintile tiering, given the honest-assessment doc's
own caution that the volatility score is "a coarse dial, not a fine one";
(c) restricting the blend to only re-order within the market's own top-5/6
contender set (the doc's literal proposal) rather than a smooth
race-wide z-score nudge, which this design approximates but does not
implement exactly.

## 日本語まとめ

タスク#37(lever bank最上位案件)を実行した。まず両先行ドキュメントが推奨する
field-size-onlyアブレーションを実施——volatility分類器のAUC上昇分の約2/3・
quintile層の3〜4割はfield_size(出走頭数)だけで説明できるが、残り約3割の
AUC上昇と6割超の層再割当は説明できず、「field_bandへの縮退」は否定された。
続けて段階2として、チャンピオンモデル(250特徴量、キャッシュ済・再学習なし)
と、物理(physical)・脚質展開(style_pace)・スピード(speed_time)の3ファミリー
のみ(78列)で学習した第2モデルをz-score加算融合し、volatility上位quintileの
レースのみ融合重みを有効化する設計を9モデル学習(3seed×3fold)して検証した。
2023+2024選択foldでの重み・層しきい値スイープ(10通り)は全て不合格、2025
blind confirmも3主要指標すべて不合格(place3 -0.106pp)、プールtop1変化
(+0.045pp)はnoise floor(0.4pp)未満かつ本キャンペーン実測のseed再学習
ノイズ(+0.309pp)を下回る。セッション中に追加されたユーザー指示(pooled数値
のみでの判断禁止、cell/tier単位で採否を評価)に従い、venue/surface/
distance_band/class/volatility-tier/tier×venue/夏季3場のcellをblind 2025
foldで網羅的に走査したが、LB95>0かつΔ≥0.08ppをクリアするcell/tierは
0件——設計が唯一作用するtier(volatility最上位quintile)自体も3fold間で
符号一致しない。**結論: REJECT**(field_size縮退ではないが、融合設計として
は不採用)。同一設計の再テストは禁止、別変種(stacked meta-learner・decile
分割・market contender-set限定reorder)は今後の余地として残す。

## Artifacts

- Stage 1: `tmp/ms-volatility-fusion/{field_size_ablation.py,field_size_ablation.json,field_size_scores.parquet}`
- Stage 2 training + selection + blind confirm: `tmp/ms-volatility-fusion/{family_boost_train_and_fuse.py,fusion_report.json}`, boost models under `tmp/ms-volatility-fusion/models/boost/`
- Per-cell/per-tier breakdown: `tmp/ms-volatility-fusion/{blind_cell_analysis.py,blind_cell_report.json}`
- Noise-floor cross-reference: `tmp/dead-lever-retest/noise_floor/task37_volatility_tiered_fusion.json`
- Reused (not modified): `tmp/candidate-masked-lever-retest/models/base/` (champion base predictions), `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` (armB feature list), `tmp/candidate-nonconform-decomp/families.py` (family column groupings), `tmp/candidate-race-volatility/volatility_scores.parquet` (volatility classifier scores)
