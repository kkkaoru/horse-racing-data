# ブリンカーパターン A-F CatBoost Ablation — REJECT (2026-06-20)

## 概要

ブリンカー着用の 6 パターン (A-F) + 2 補助パターン (G: 未着用継続, H: 初出走未着用) を
one-hot 特徴量として CatBoost ablation で評価。全 3 folds で top1 negative、feature importance ≈ 0。

## パターン定義 (データリーク無し: 出走前レースのみ使用)

| Pattern | 説明                | N       | %     |
| ------- | ------------------- | ------- | ----- |
| A       | 初装着 (初出走以外) | 13,415  | 2.03% |
| B       | 初出走+初装着       | 944     | 0.14% |
| C       | 再装着 (1-2走休止)  | 2,042   | 0.31% |
| D       | 再装着 (3走+休止)   | 1,081   | 0.16% |
| E       | 全走着用→解除       | 461     | 0.07% |
| F       | 継続着用            | 45,833  | 6.96% |
| G       | 未着用継続          | 485,763 | 73.7% |
| H       | 初出走・未着用      | 68,865  | 10.5% |

## 結果 (pooled 2023-2025, n=10,365 races)

| 指標       | Base   | Candidate | Δ (pp) | LB95   |
| ---------- | ------ | --------- | ------ | ------ |
| top1       | 39.10% | 38.99%    | −0.116 | −0.376 |
| place2     | 21.46% | 21.82%    | +0.367 | +0.010 |
| place3     | 16.57% | 16.83%    | +0.260 | −0.097 |
| fukusho_2p | 67.25% | 67.11%    | −0.135 | −0.425 |

Per-fold: Fold1 top1 −0.145pp, Fold2 −0.145pp, Fold3 −0.058pp (全 negative)

## Feature importance

全 8 パターン: 0.0000〜0.0001 (CatBoost が完全に無視)

## 根因

1. ブリンカー情報は市場 (odds) で完全に織込み済み (partial ρ = 0.018, threshold 0.08 未満)
2. パターン A-F は極めてスパース (B=0.14%, D=0.16%, E=0.07%)
3. Prior investigation (goal-blinker-signal-probe.md) で first_blinker win −0.21pp/place3 −1.53pp を確認済み
4. NAR はブリンカー未記録 (100% '0') → JRA only signal

## 結論

ブリンカーパターン A-F は着順予測精度に寄与しない。  
DO-NOT-RETEST を CatBoost ablation で最終確認。  
本番モデル変更なし。
