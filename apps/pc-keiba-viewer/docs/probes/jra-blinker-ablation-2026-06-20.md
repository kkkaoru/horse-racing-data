# JRA ブリンカー (blinker A-H) ablation — REJECT (2026-06-20)

## 概要

CatBoost YetiRank にブリンカー装着パターン 8 one-hot (A-H) を追加する ablation。
**2 独立訓練実行で結果が不一致 = training noise 確定。REJECT。**

## パターン定義

| Code | パターン            | N       | 比率  |
| ---- | ------------------- | ------- | ----- |
| A    | 初装着              | 13,415  | 2.0%  |
| B    | デビュー+装着       | 944     | 0.14% |
| C    | 再装着 (gap 1-2)    | 2,042   | 0.31% |
| D    | 再装着 (gap 3+)     | 1,081   | 0.16% |
| E    | 外し                | 461     | 0.07% |
| F    | 継続装着            | 45,833  | 7.0%  |
| G    | 未装着 (装着歴なし) | 485,763 | 73.7% |
| H    | デビュー未装着      | 68,865  | 10.5% |

## 2 独立訓練実行の比較 (同一 CB params / WF folds)

| Metric   | Run 1 (ablation)       | Run 2 (train_save_preds) |
| -------- | ---------------------- | ------------------------ |
| Δ top1   | −0.116pp (LB95 −0.376) | +0.02pp (LB95 +0.00)     |
| Δ place2 | +0.367pp (LB95 +0.010) | +0.04pp (LB95 −0.06)     |
| Δ place3 | +0.260pp (LB95 −0.097) | +0.14pp (LB95 −0.04)     |

**place2 が +0.367 → +0.04 に変動**: 再現不能 = training noise。

## Feature importance

全 8 パターンで CatBoost importance ≈ **0.0000** (avg over 3 folds)。
→ CatBoost はブリンカー特徴を完全に無視。per-class/per-venue の差異は model の stochastic variation。

## Per-class 評価 (Run 2 bootstrap CI)

| Class     | N     | Δ top1 [LB95] | Δ place2 [LB95] | Δ place3 [LB95] |
| --------- | ----- | ------------- | --------------- | --------------- |
| 2勝       | 640   | 0.00 [0.00]   | +0.31 [+0.00]   | +0.47 [−0.31]   |
| 1勝       | 931   | 0.00 [0.00]   | +0.32 [+0.00]   | +0.32 [−0.21]   |
| 新馬      | 908   | 0.00 [0.00]   | +0.11 [+0.00]   | +0.22 [−0.33]   |
| 3勝       | 1,400 | +0.07 [0.00]  | +0.07 [−0.29]   | +0.36 [−0.29]   |
| OP/Listed | 2,776 | 0.00 [0.00]   | 0.00 [−0.22]    | 0.00 [−0.40]    |
| 未勝利    | 3,710 | +0.03 [0.00]  | −0.08 [−0.19]   | +0.05 [−0.13]   |

2勝/1勝/新馬 の place2 LB95=+0.00 だが、Run 1→Run 2 で global place2 が 10 倍縮小 → per-class も noise。

## Per distance × surface

| Cell        | N     | Δ top1 | Δ place2 [LB95] | Δ place3 |
| ----------- | ----- | ------ | --------------- | -------- |
| sprint_turf | 1,480 | +0.07  | +0.27 [−0.07]   | +0.74    |
| mile_dirt   | 2,442 | 0.00   | +0.04 [+0.00]   | +0.12    |
| sprint_dirt | 2,168 | 0.00   | −0.05 [−0.28]   | 0.00     |

## Per venue (上位)

| Venue | N     | Δ place2 [LB95] | Δ place3 |
| ----- | ----- | --------------- | -------- |
| 函館  | 432   | +0.46 [+0.00]   | +0.69    |
| 中山  | 1,500 | +0.13 [−0.13]   | −0.07    |
| 小倉  | 792   | +0.13 [+0.00]   | +0.25    |

## REJECT 根拠

1. **Feature importance = 0**: CatBoost が 8 特徴を完全に無視 → model は同一
2. **2 独立訓練で不一致**: place2 delta が 10 倍変動 = 訓練乱数依存
3. **絶対差が極小**: 2勝 Δp2=+0.31pp = 640 レース中 2 レース差
4. **既存特徴で捕捉済み**: past_nige_rate_self / running-style / jockey-trainer aggregates がブリンカー効果を間接的に encode
5. **NAR データなし**: blinker_shiyo_kubun が NAR で 100% zeros

## 結論

ブリンカー one-hot パターンは CatBoost に情報を追加しない。
per-class/per-venue の差異は training noise であり、class routing を実装しても次回再訓練で消失する。
本番モデル (iter22-jra-etop2) は変更なし。
