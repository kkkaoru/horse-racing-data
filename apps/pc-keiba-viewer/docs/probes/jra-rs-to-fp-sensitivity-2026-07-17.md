# JRA running-style (RS) → finish-position (FP) causal-ceiling sensitivity study (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position — oracle-substitution sensitivity study,
  not a lever-development probe. Team-lead-directed (wave4-1), genuinely
  open, no DO-NOT-RETEST conflict.
- **Question**: JRA's running-style (RS) model has a known, unrepaired
  defect (oikomi recall collapse, `project_jra_rs_calibrator_drift_2026_07_03`,
  ~2.9pp headroom on the RS task itself). Champion consumes `rs_p_*`-derived
  features. **Before building an RS v2**, measure the _causal ceiling_: if
  the RS model that feeds these features were perfect, how much would JRA
  finish-position accuracy actually improve? If real, RS v2 is justified as
  the next major lever. If ~zero, the RS→FP linkage can be closed as a lead.
- **Training performed**: none. Predict-only re-scoring of the same 9
  cached champion CatBoost artifacts (3 seed × 3 fold) used throughout
  today's campaign, under 3 feature-substitution arms.
- **⚠️ The oracle arm (b) is a leak-based ceiling-measurement device only —
  it is never a production candidate.** It substitutes the actual, ex-post
  observed running style (`target_running_style_class`, one of the 5
  columns in `model_meta.py`'s `WITHIN_RACE_LEAK_COLUMNS`) as if it were a
  perfect pre-race prediction. This is stated here, at the top, and again
  in §2 — nothing in this doc proposes deploying the oracle arm or anything
  resembling it.

---

## 0. Headline result

**The RS→FP causal ceiling is essentially zero everywhere tested, including
the one stratum specifically targeting the known RS defect.** Team-lead's
own pre-registered threshold (≥+0.3pp-class ceiling, considering the noise
floor, to justify RS v2) is not met anywhere — the largest point estimate
in the entire study (any cell, any metric, summer4 or JRA-wide) is
**+0.198pp** (Sapporo, top1, LB95 sitting exactly at the zero boundary),
and the specific "oikomi-heavy" stratum built to target the known defect
(JRA-wide, n=354, winner ran as oikomi) tops out at **+0.188pp [LB95
+0.000]** — the largest ceiling estimate found, still not robustly
significant. **Verdict: RS→FP linkage is saturated. Closed, per team-lead's
own pre-registered fallback. No RS v2 spec is produced by this doc** (that
branch of the decision tree was not reached).

---

## 1. Method

### 1.1 The RS-dependent feature set, established by direct code read, not assumed

The only script that writes RS-model-derived columns into the JRA
finish-position feature store is
`src/scripts/finish-position-features/add-pacestyle-features.py`. Its own
docstring lists 13 output columns; cross-checked directly against armB
(`tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`), only
**8 are actually consumed by champion**:

| Column                                   | Type                                                   |
| ---------------------------------------- | ------------------------------------------------------ |
| `rs_p_nige`/`_senkou`/`_sashi`/`_oikomi` | direct RS model output (4-class softmax probabilities) |
| `rs_predicted_class`                     | direct RS model output (argmax integer class)          |
| `rs_confidence_entropy`                  | derived: `-Σ p·ln(p+1e-9)` over the 4 probabilities    |
| `rs_p_nige_x_field_pace`                 | derived: `rs_p_nige × field_pace_index`                |
| `rs_sire_style_match`                    | derived: `Σ_k rs_p_k × sire_k_rate`                    |

Three more of the script's 13 outputs
(`rs_predicted_corner_front_score`/`_rank`/`_rank_pct`) are confirmed **not**
in armB and don't even exist as columns in this store (a direct
column-not-found query, not an assumption) — champion never sees them, so
they're out of scope. Two further armB columns
(`past_style_x_field_pace_match`, `sire_x_field_pace_score`) are **not**
RS-model-dependent — they're built from the horse's own historical
running-style _usage_ rates and sire pedigree tendency, not the RS model's
live prediction — and were left untouched in every arm.

Every derived column above was recomputed from its substituted `rs_p_*`
inputs using the **exact formula** read directly from
`add-pacestyle-features.py`'s `append_features_sql` — not re-derived from
memory.

### 1.2 Coverage, measured directly

| Fact                                                       | Value                                      |
| ---------------------------------------------------------- | ------------------------------------------ |
| `rs_p_nige` non-null, race_year=2023                       | **0.00%** — RS model didn't score pre-2024 |
| `rs_p_nige` non-null, race_year=2024                       | 99.18%                                     |
| `rs_p_nige` non-null, race_year=2025                       | 99.26%                                     |
| `target_running_style_class` non-null, overall (all years) | **43.76%**                                 |

Both facts materially shape the design: **2023 is excluded from the
headline analysis** (arm (a) is already ~100% null that year, so
`(a)−(c)`/`(b)−(a)` would be measuring nothing) and kept only as a
methodology-validation check (§1.4). The oracle arm (b) can only build a
genuine "perfect" substitution where `target_running_style_class` is known
(~44% of rows) — where it's not, arm (b) falls back to arm (a)'s own value
rather than fabricating a ceiling from nothing.

### 1.3 The 3 arms

- **(a) as-is**: the store's real `rs_p_*`/etc values, unmodified.
- **(b) oracle** (leak, ceiling-only): where `target_running_style_class`
  is known, a perfect one-hot probability vector on the true class (verified
  identical class encoding between `target_running_style_class` and
  `rs_p_*`: `CLASS_LABELS = (nige, senkou, sashi, oikomi)` at indices 0-3,
  `running_style_lightgbm.py:94`, the same script that trains
  `target_running_style_class` as its own `TARGET_COLUMN` — same encoding
  by construction, not assumed) plus every derived column recomputed from
  that one-hot via the §1.1 formulas. Where the true class is unknown,
  falls back to arm (a).
- **(c) null**: all 8 RS-dependent columns set to missing for every row,
  relying on CatBoost's native missing-value handling — already proven
  working throughout today's entire campaign (2023's 0%-populated `rs_p_*`
  has flowed through this exact predict path all day without incident).

`(b)−(a)` = the ceiling from a perfect RS. `(a)−(c)` = current RS's actual
serve-time contribution.

### 1.4 Methodology validation: the 2023 consistency check

Since arm (a) is ~100% null for 2023 already, arm (a) and arm (c) should be
**numerically identical** that year — a strong internal check that the
substitution mechanism itself works correctly. Result, 2023, n=3456 races,
3-seed pooled: **top1/place2/place3 deltas all exactly 0.0000pp, LB95=UB95=0.0000**
— bit-for-bit identical predicted rankings between the two arms, confirming
the pipeline's substitution and predict-only re-scoring mechanism behaves
exactly as designed before trusting it on the 2024-2025 headline comparison.

### 1.5 Population, cells, and gate discipline

Headline comparison: **race_year ∈ {2024, 2025} only** (2 blind fold-years,
justified by §1.2's measured 2023 coverage gap), 3 seed × 2 fold = 6
predict-only runs per arm, seed-averaged per race. Cells: summer-4-venue
restricted (mandatory per task) — pooled, venue, venue×distance*band — plus
a **winner-style stratification directly targeting the known defect**:
races whose \_actual winner* ran as oikomi (`target_running_style_class==3`)
vs other-known-style winners vs unknown-style winners. JRA-wide (all 10
venues) versions of every cut are also reported for context and statistical
power (the oikomi-heavy stratum needs more than summer4's ~1632 races to
clear `n≥200`). `n≥200` enforced everywhere; paired bootstrap (n_boot=2000,
seed=20260717); sort-before-mask discipline (`.filter()` on race-id-keyed
frames only, confirmed no positional mask anywhere in the pipeline).

---

## 2. Results

### 2.1 Pooled (headline, summer4-restricted, n=1632)

| Comparison                     | top1 Δ   | top1 LB95 | top1 UB95 |
| ------------------------------ | -------- | --------- | --------- |
| Ceiling `(b)−(a)`              | +0.020pp | −0.041    | +0.102    |
| Current contribution `(a)−(c)` | +0.000pp | −0.102    | +0.122    |

Both are indistinguishable from zero. For reference, JRA-wide pooled
(n=6909) is equally flat: ceiling +0.019pp [−0.005,+0.048], current
contribution −0.005pp [−0.048,+0.043].

### 2.2 Venue breakdown (summer4, top1)

| Venue        | n   | Ceiling `(b)−(a)` [LB95,UB95] | Current contribution `(a)−(c)` [LB95,UB95] |
| ------------ | --- | ----------------------------- | ------------------------------------------ |
| 01 Sapporo   | 336 | +0.198 [+0.000, +0.496]       | +0.000 [−0.397, +0.496]                    |
| 02 Hakodate  | 288 | +0.000 [+0.000, +0.000]       | +0.116 [+0.000, +0.347]                    |
| 03 Fukushima | 480 | +0.000 [+0.000, +0.000]       | −0.069 [−0.208, +0.000]                    |
| 10 Kokura    | 528 | −0.063 [−0.189, +0.000]       | +0.000 [+0.000, +0.000]                    |

Several cells show a bootstrap CI of exactly `[0.000, 0.000]` — this is not
a formatting artifact, it means the substituted arm produced the **literal
identical top1 pick, race-by-race, for every race in that cell**, not just
a statistically indistinguishable average. `venue×distance_band` fragmented
below `n≥200` everywhere within the smaller summer4/2-year population (0
cells reported) — matches the same fragmentation pattern seen in today's
earlier cell-model-selection ledger at similar cross-cut granularity.

### 2.3 The targeted test: winner-style stratification (JRA-wide, for power)

The stratum built specifically to target the known RS defect (does a
perfect RS help most exactly where the real winner was a closer the
current RS misreads?):

| Stratum (JRA-wide)   | n    | Ceiling `(b)−(a)` top1 [LB95,UB95] | Current contribution `(a)−(c)` top1 [LB95,UB95] |
| -------------------- | ---- | ---------------------------------- | ----------------------------------------------- |
| **oikomi_winner**    | 354  | **+0.188 [+0.000, +0.471]**        | **−0.282 [−0.659, +0.000]**                     |
| other_winner_style   | 2812 | +0.024 [−0.036, +0.083]            | −0.024 [−0.083, +0.024]                         |
| unknown_winner_style | 3743 | +0.000 [+0.000, +0.000]            | +0.036 [−0.027, +0.107]                         |

`oikomi_winner` (n=354, comfortably above the gate floor) is the single
largest ceiling estimate found **anywhere in this entire study** — and its
current-contribution point estimate is _negative_ (−0.282pp), consistent
with the known defect actively misleading rather than helping in exactly
this stratum. **Neither number clears LB95>0 / UB95<0 robustly** — both
sit with one bound exactly at the zero boundary. Full rank1-6+top3_box
breakdown for this cell (all metrics, not just top1) shows the same
pattern throughout: every metric's delta is ≤0.19pp in magnitude, and every
CI touches or straddles zero (place4/place5 are exact `[0,0]` — again,
literal race-level identity, not rounding).

**Direct answer to the brief's targeted question**: even in the cell built
specifically to give the known defect its best chance to show up, the
ceiling from perfecting RS tops out under 0.2pp and does not clear
significance.

---

## 3. Why the ceiling is this low — a mechanistic reading, not just a null result

This is not simply "RS doesn't matter" — the 2026-07-04 family-decomposition
work (`jra-nonconforming-signal-decomposition-2026-07-04.md`) found the
`style_pace` **family** (49 columns, of which the 8 RS-model-output columns
tested here are a small subset) is one of the **best-retaining, least
market-redundant** families on upset races — genuinely informative as a
family. The two findings are compatible, not contradictory: `style_pace`'s
other 41 columns (`past_corner_1_norm_avg_5`, `past_nige_rate_self`,
`field_pace_index`, and similar raw historical/pace-context features — none
of them touched by this study's substitution, present at their real values
in all 3 arms) already let CatBoost triangulate "this horse's running-style
tendency" from the horse's own history and the race's pace context,
independent of what the dedicated RS model predicts. The RS model's
probabilistic read is **largely redundant with information champion
already has from other columns** — so even making it perfect adds little
incremental signal. This is the same qualitative pattern already
established today in a different context (`docs/probes/jra-contender-set-meta-reorder-2026-07-17.md`:
`z_base` dominates any secondary-score blend by 19-49×, meaning the
champion's own tree structure has already absorbed most of what a
secondary signal could add) — a champion-scale GBDT tends to saturate
redundant secondary signal sources rather than leave visible headroom for
them.

---

## 4. Verdict and closure

**RS→FP linkage is saturated. No RS v2 justification from this study.**
Per team-lead's own pre-registered decision criterion (≥+0.3pp-class
ceiling required, considering the noise floor), and even setting that
threshold aside entirely, **no cell tested — summer4 or JRA-wide, pooled
or targeted at the known defect — shows a robust (LB95>0) ceiling of any
magnitude.** The RS v2 investment case, evaluated purely on FP-side payoff,
does not clear the bar this study set out to measure. (This says nothing
about whether RS v2 would be worth building for the RS _task itself_ — the
2.9pp RS-side headroom cited in the background is a separate, real, and
unaffected fact; this doc only closes the _FP-side_ justification.)

**DO-NOT-RETEST**: the RS→FP causal-ceiling question via this oracle-
substitution method, on the 2024-2025 WF population — tested at reasonable
power (n=354 in the single most-targeted stratum) and closed. Re-opening
would need either a materially different population/method or genuinely
new evidence (e.g. an actual trained RS v2 artifact, tested the ordinary
way — WF blind, gated — rather than an oracle proxy).

No RS v2 spec is produced (the branch of the task that would have required
one — a ≥0.3pp-class robust ceiling somewhere — was not reached).

---

## 5. Caveats

- The oracle arm can only be "perfect" where `target_running_style_class`
  is known (43.76% of rows) — for the other ~56%, arm (b) silently equals
  arm (a), which _understates_ the theoretical ceiling if a real RS v2
  could generalize correctly to those rows too from patterns learned
  elsewhere. This doc measures the _verifiable_ ceiling, not a fully
  unconstrained theoretical one — stated plainly rather than glossed over.
- `venue×distance_band` fragmented below the `n≥200` gate everywhere in the
  summer4 population (2 years only) — not reported, not papered over with
  a sub-200 table.
- This is a sensitivity/ceiling study, not a WF gate evaluation of a real
  candidate model — no ADOPT/REJECT gate mechanics apply here (there's
  nothing being adopted).
- `cell_training_evaluations` was not used anywhere in this study (all
  numbers are predict-only re-scores of already-cached WF artifacts).

---

## 6. 日本語まとめ

RS(脚質)モデルの既知未修理defect(oikomi recall崩壊、project_jra_rs_calibrator_drift_2026_07_03)
がFP(着順)精度にどれだけ伝播しているか、RS v2着手前に**oracle-substitution
による因果天井**を測定した(学習なし、既存9 cached champion artifactの
predict-onlyのみ)。champion armB-250のうちRSモデル出力に依存する列は
直接コード確認で正確に8列(rs_p_nige/senkou/sashi/oikomi、rs_predicted_class、
rs_confidence_entropy、rs_p_nige_x_field_pace、rs_sire_style_match)と確定
(rs_predicted_corner_front_score/rank/rank_pctはarmBに不在かつstore自体に
非存在)。3 arm: (a)現状serve相当、(b)oracle(実際の脚質=target_running_style_class
から構築した完璧確率、**leakであり天井測定専用、本番候補では絶対にない**)、
(c)全NULL(完全ablation)。(b)−(a)=RS完璧化の上限、(a)−(c)=現状RSの寄与。

**結果: 天井はほぼゼロ、既知defectを直撃する層別でも同様**。夏4場restricted
pooled(n=1632)は(b)−(a)=+0.020pp[LB95−0.041]、(a)−(c)=+0.000pp[LB95−0.102]
——いずれも実質ゼロ。事前登録閾値(≥+0.3pp級)超えは全セルでゼロ件。既知defect
を最も直撃する層別(JRA全体、勝ち馬が実際にoikomi脚質だったレース、n=354)
でも(b)−(a)=+0.188pp[LB95+0.000、境界]——本研究全体で最大の点推定値だが、
それでも有意性は確保できず。(a)−(c)はこの層でむしろ**負**(−0.282pp)——現状RS
がこの層でむしろ足を引っ張っている可能性と整合的だが、これも非有意。

**機序的解釈**: 07-04家族分解ドキュメントはstyle_pace家族(49列、本研究が
触れたRS由来8列はその一部)自体はmarket非冗長で保持力の高い有力家族と結論
していたが、これは矛盾しない——style_pace家族の残り41列(過去脚質使用率・
コーナー通過履歴・pace文脈など、本研究では一切変更していない)が既に
「この馬の脚質傾向」をRSモデルの予測なしに三角測量できているため、RSモデル
の確率出力を完璧にしても増分情報がほぼ無い、という一貫した機序。同日の
contender-set meta-reorder REJECT(z_baseがboost信号を19-49倍支配)と同型の
「champion規模のGBDTは冗長な副次シグナルを既に飽和吸収する」パターン。

**結論: RS→FP linkageは飽和——CLOSE。RS v2のFP側投資根拠なし**(RS単体タスク
側の2.9pp headroomは別問題として無傷で残る、本ドキュメントはFP側正当化のみ
クローズする)。事前登録基準の「正なら v2 spec 化」分岐には到達しなかった
ため、v2要件のdocは作成していない。DO-NOT-RETEST登録。

---

## Artifacts

- `apps/pc-keiba-viewer/tmp/candidate-jra-rs-fp-sensitivity-2026-07-17/oracle_substitution.py`
  — full pipeline (3-arm predict-only re-score, exact formula reproduction, cell/gate logic)
- `.../run.log`, `.../oracle_substitution_result.json` (full nested result:
  spec, 2023 consistency check, ceiling + current-contribution tables for
  summer4 and JRA-wide scopes, all 7 metrics per cell)
- Ground truth read directly (not assumed):
  `src/scripts/finish-position-features/add-pacestyle-features.py` (RS
  column lineage + exact derived-column formulas),
  `src/scripts/running_style_lightgbm.py` (class-label encoding),
  `apps/finish-position-predict-container/src/predict_lib/model_meta.py`
  (`WITHIN_RACE_LEAK_COLUMNS`)
- Reused unchanged: `tmp/candidate-masked-lever-retest/models/base/**` (9
  champion artifacts), `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`,
  `tmp/candidate-eval-jra/augmented/**`
- Cited: `docs/probes/jra-nonconforming-signal-decomposition-2026-07-04.md`
  (style_pace family retention finding, contextualizes rather than
  contradicts this doc's near-zero marginal-column ceiling),
  `docs/probes/jra-contender-set-meta-reorder-2026-07-17.md` (same-day
  precedent for the "champion GBDT saturates redundant secondary signal"
  pattern), memory `project_jra_rs_calibrator_drift_2026_07_03` (the RS-side
  defect this study's FP-side ceiling question was scoped from)
