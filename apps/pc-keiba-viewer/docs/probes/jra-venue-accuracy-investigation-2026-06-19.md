# JRA Probe — 東京/函館/阪神 競馬場別精度調査

**Date**: 2026-06-19
**Category**: JRA finish-position (iter14-jra-cb-pacestyle-course-v8)
**Verdict**: INVESTIGATION COMPLETE — 4 仮説を形成、検証 loop 開始

## 全 10 場精度比較 (Global baseline: 40.30%)

| Rank | Code | 競馬場 | Races  | top1   | Δ global | place2 | place3 |
| ---- | ---- | ------ | ------ | ------ | -------- | ------ | ------ |
| 1    | 02   | 函館   | 2,928  | 45.25% | +4.95    | 24.49% | 18.24% |
| 2    | 01   | 札幌   | 3,096  | 42.64% | +2.34    | 22.38% | 18.41% |
| 3    | 10   | 小倉   | 5,495  | 42.06% | +1.76    | 23.28% | 17.71% |
| 4    | 03   | 福島   | 4,608  | 41.49% | +1.19    | 21.98% | 17.32% |
| 5    | 06   | 中山   | 9,504  | 41.41% | +1.11    | 22.81% | 16.22% |
| 6    | 08   | 京都   | 9,212  | 39.92% | -0.38    | 21.75% | 16.13% |
| 7    | 09   | 阪神   | 9,661  | 39.48% | -0.82    | 21.96% | 16.61% |
| 8    | 07   | 中京   | 6,060  | 39.49% | -0.81    | 20.87% | 15.50% |
| 9    | 05   | 東京   | 10,117 | 38.36% | -1.94    | 20.42% | 14.99% |
| 10   | 04   | 新潟   | 6,286  | 38.48% | -1.82    | 20.28% | 14.49% |

Spread: 6.89pp (函館 45.25% ↔ 東京 38.36%)

## 東京 (05) — JRA 2nd hardest

- **Overall**: top1 38.36% (全期間), 47.12% (2025), 45.64% (2024)
- **Surface**: turf 37.30% / dirt 38.69% (+1.39pp — JRA 最小 gap)
- **Distance**: sprint 36.94% / mile 37.52% / intermediate 41.63% / long 38.60% / extended 50.99%
- **Grade**: GII 30.86% / Listed 32.14% (最弱), GI 40.51%
- **Error**: 52% of misses finish 2nd/3rd (near-miss)
- **Odds corr**: rank↔odds 0.904 (全場最高 = 市場追従最大)

## 函館 (02) — JRA easiest

- **Overall**: top1 45.25% (全期間), 46.53% (2025)
- **Surface**: turf 42.08% / dirt 49.13% (+7.05pp)
- **洋芝劣化**: Jun 45.27% → Jul 40.09% → Aug 40.85% (turf のみ、dirt は flat)
- **Distance**: sprint 46.15% / mile 45.28% / intermediate 35.84% / long 54.43%
- **Weak spot**: 中距離 (2000m) turf = 35.84% (venue avg -10pp)
- **Field size**: small 53.21% → large 39.44% (-13.8pp, 全場最大 gradient)

## 阪神 (09) — below average

- **Overall**: top1 39.48% (全期間), 43.80% (2025), 47.47% (2026 YTD best)
- **Surface**: turf 36.78% / dirt 40.12% (+3.34pp)
- **Inner vs Outer (turf)**:
  - 1200m inner: 35.75%, **1400m inner: 32.88% (全期間) / 27.38% (2023-26)**
  - 1600m outer: 38.15%, 1800m outer: 37.15%
  - 2000m inner: 38.01% → 47.93% (2023-26)
  - 2200m outer: 32.23%, 2400m outer: 38.78% → 55.56% (2023-26)
- **Weak spot**: 芝 1400m = 27.38% (recent, venue avg -16pp) — THE worst config in all JRA
- **Obstacle**: 61-63% top1 (small field dominant favorites)
- **Field size**: small 48.34% → large 33.15% (-15pp)

## Cross-venue systematic bias

### 逃げ/先行 過剰選択 (全場共通)

- 逃げ: actual win rate 14-25% vs model pick rate 18-31% (+4-6pp 過剰)
- 差し: actual 4-6% vs model 2.4-5.3% (過少)
- 函館が最悪 (+5.75pp nige over-pick on tight track)
- これは calibration gap (model pick distribution ≠ base rate) であり、feature correlation とは異なるロバストな signal class

### ダート > 芝 (全場)

- 全 10 場で dirt > turf (+1.39 ~ +8.76pp)
- 東京が最小 gap、新潟が最大

### Near-miss error pattern

- エラーの 52-72% が 2-3 着 (近い順位の逆転)
- 「正しい馬を選んでいるが、top-2-3 の順序が不正確」

### Draw/枠順: well-calibrated (補正不要)

### Jockey blind spot: 阪神の中堅騎手で miscalibration あり

## 形成した仮説 (検証 loop 対象)

| ID  | 仮説                                                               | 手法                                                     | 推定改善幅      | リスク                                      |
| --- | ------------------------------------------------------------------ | -------------------------------------------------------- | --------------- | ------------------------------------------- |
| H1  | 阪神芝 1400m 特化: 内回り chaos で pace 不安定、逃げ過剰選択が最大 | Post-scoring rank adjustment (E-top2 pattern)            | +2-5pp @1400m   | thin n, seed noise                          |
| H2  | 逃げ/先行 pick rate 較正: base rate に近づける                     | Statistical calibration (isotonic/Bayesian per RS class) | +0.3-1pp global | 既存 RS×venue REJECT あり — 異なる approach |
| H3  | Per-venue E-top2 gating: venue 別 ON/OFF                           | Programmatic routing                                     | +0.2-0.5pp      | place3 downside-protected                   |
| H4  | Venue × distance interaction feature                               | ML feature engineering                                   | +0-0.3pp        | GBDT already captures?                      |

## DO-NOT-RETEST (この調査で確認)

- Per-venue specialist 訓練 (= Ōi REJECT)
- Track-bias / draw-bias 補正 (2026-06-19 全 REJECT)
- Running-style × venue feature (2026-06-19 REJECT)
- Odds 補正 (odds-decoupling campaign REJECT)
- Per-venue HPO (iter21 HPO = selection bias REJECT)
- RL / bandit (MLX RL ABORT)

## Container routing 設計 (venue-model-routing agent)

- `per_venue.py` (新規): `per_class.py` と並列軸、`(category, keibajo_code) → model_version` registry
- E-top2 は venue base の上に unchanged layer として compose
- Fallback: venue-specific → global → error
- 初期は EMPTY registry (全 race → global fallback) — eval gate 通過後のみ populate
- Option A (eval first) → Option C (global venue features) → Option B (subset training, 最後の手段)

## Method

- Local PostgreSQL: `race_finish_position_model_predictions` JOIN `jvd_se`/`jvd_ra`
- Model: iter14-jra-cb-pacestyle-course-v8 (full-history 2007-2026)
- Per-race bool_or scoring (duplicate-safe)
- 7 parallel SubAgents (per-venue accuracy × 3 + global baseline + error patterns + routing design + improvement methods)
