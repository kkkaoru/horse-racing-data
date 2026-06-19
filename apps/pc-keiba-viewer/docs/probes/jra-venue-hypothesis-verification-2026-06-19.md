# JRA Probe — 競馬場別精度向上仮説の検証結果

**Date**: 2026-06-19
**Category**: JRA finish-position (iter14-jra-cb-pacestyle-course-v8)
**Target venues**: 東京(05), 函館(02), 阪神(09)
**Verdict**: H1 REJECT / H2 REJECT / H3 INCONCLUSIVE / H4 PENDING

## H1 — 阪神芝 1400m 特化補正: REJECT

**Hypothesis**: 阪神芝 1400m 内回りは JRA 全体で最悪の top1 (27.38% recent) → 標的補正で改善可能

**Refutation**:

- 27.38% は 84 レースの小標本 (2025 年 27 レースで 18.52% — 純粋なノイズ)
- **1400m は全場で最弱の芝距離**: 全 JRA turf top1 by distance — 1200m 37.60%, **1400m 33.80%**, 1600m 36.02%, 1800m 37.88%
- 場別 1400m turf: 東京 31.67 < **阪神 32.88** < 中京 33.77 < 京都 34.69 — 阪神は mid-pack
- 原因 = field-size dilution (59% が 15+頭立て) — 普遍的、非場固有
- **モデルはオッズに +3.56pp 勝っている** (model 32.88% vs fav 29.32%)
- 脚質 bias なし (阪神 1400m nige overpick = +0.4pp、全 JRA turf +2.4pp より良好)

**Conclusion**: 場固有でない → 補正対象外

## H2 — 逃げ/先行 pick rate 較正: REJECT

**Hypothesis**: モデルが逃げ/先行を系統的に過剰選択 (+4-6pp) → post-scoring 較正で改善

**Data**:

- 全 JRA: 逃げ actual 19.03% vs picked 23.92% (+4.90pp overpick)
- 先行: actual 13.43% vs picked 15.24% (+1.80pp)
- 差し: actual 4.99% vs picked 3.48% (−1.51pp underpick)
- 追込: actual 1.50% vs picked 0.51% (−0.99pp)
- Target venues: 東京 +3.92pp / 函館 +5.75pp / 阪神 +3.82pp (nige overpick)
- ダートで悪化: dirt +7.14pp vs turf +2.38pp
- 2020-2026 trend: stable (悪化も改善もしていない)

**Refutation**:

- **Rank-1 に選ばれた時の precision**: 逃げ 45.36%, 先行 40.19%, 差し 34.44%, 追込 29.00%
- 逃げは per-capita で genuinely 最も勝つ → overpick は optimal な base-rate 集中
- **較正シミュレーション**: front-runner demote → closer promote
  - Global: 40.30% → **36.11% (−4.19pp)** — 精度が破壊される
  - Dirt nige-only (最大 bias): 42.95% → **41.32% (−1.63pp)**
  - 全 variant negative
- RS は既に深い feature set: target_running_style_class + jockey/trainer/sire nige/senko/sashi/oikomi rates
- 残差 = base-rate concentration、exploitable error ではない

**Conclusion**: 補正は逆効果 — モデルの RS 利用は既に最適

## H3 — Per-venue E-top2 gating: INCONCLUSIVE

**Hypothesis**: E-top2 override (XGB#1==CB#2) の効果が場ごとに異なる → venue 別 ON/OFF で改善

**Finding**:

- iter22-jra-etop2 / iter20-jra-cb-2013-v8 の walk-forward predictions が **PG に不在**
- Proxy simulation (iter14 + win5-xgb): **構造的に無効** — 全年・全場で negative sign (deployed E-top2 は positive)
- Proxy の iter14 CB#2 と deployed iter20 CB#2 は異なるモデル → coincidence が異なる信号を検出
- Fire rate は uniform (~11-12%) across venues

**Conclusion**: offline 検証不可 — 以下いずれかが必要:

1. iter20-cb-2013 + xgb-2013 walk-forward predictions を PG に store
2. CF Container serve path で E-top2 fire + outcome を keibajo_code 付きで log

**Action**: DEFER — live data 蓄積まで保留

## H4 — Venue × distance interaction feature: PENDING

結果待ち。

## Cross-hypothesis 総括

3/4 仮説が REJECT/INCONCLUSIVE。パターン:

1. **場固有に見える弱点は距離 or field-size の普遍的効果** (H1)
2. **系統的 bias に見える脚質 overpick は optimal な base-rate 集中** (H2)
3. **E-top2 のオフライン検証は deployed model pair の predictions なしでは不可能** (H3)

これは [[project_finish_position_frontier_2026_06_11]] / [[project_science_track_saturation_2026_06_11]] の結論と完全に一致:
**市場効率の壁により、tactical lever での改善は枯渇済み。改善は新情報 (新データ or 新 signal) からのみ。**

## DO-NOT-RETEST 追加

- 阪神 1400m 特化 (場固有でない — 1400m 普遍)
- 脚質 pick rate post-scoring 較正 (optimal base-rate concentration — 補正で −4.19pp)
- Per-venue E-top2 gating (offline 検証不可、live data 不足)
- 場別 RS threshold tuning (H2 refutation の延長)

## Method

- Local PostgreSQL: `race_finish_position_model_predictions` JOIN `jvd_se`/`jvd_ra`
- Model: iter14-jra-cb-pacestyle-course-v8 (full-history 2007-2026)
- 4 parallel SubAgents (H1-H4)
- RS column: `jvd_se.kyakushitsu_hantei` (NOT kyakushitsu_kubun)
- E-top2 proxy: iter14 (CB) + win5-xgb-v7-lineage-v1 (XGB) — structurally invalid for deployed pair
