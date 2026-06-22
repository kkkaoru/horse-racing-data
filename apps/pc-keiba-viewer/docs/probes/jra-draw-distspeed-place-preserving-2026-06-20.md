# Draw + Dist-speed place-preserving implementation — 全 6 手法 REJECT/ABORT (2026-06-20)

## 概要

ablation で draw +0.077pp / dist-speed +0.116pp の top1 改善を確認したが、LB95 < 0 で有意でない。  
place2/place3 に影響を与えない実装手法を 6 種検討し、全て REJECT/ABORT。

## 手法別結果

| #   | 手法                            | Δ top1 (pp)     | Δ place2      | Δ place3      | LB95 top1     | Verdict        |
| --- | ------------------------------- | --------------- | ------------- | ------------- | ------------- | -------------- |
| 1   | Place-preserving rule swap      | −0.78           | —             | 0.00          | —             | REJECT         |
| 2   | LR win-specialist ensemble      | +0.029          | —             | 0.00          | −0.029        | REJECT (noise) |
| 3   | Score additive α×draw+β×speed   | all ≤ 0         | —             | —             | —             | REJECT         |
| 4   | E-top2 + Layer2 draw+speed      | +0.05           | —             | 0.00          | −0.10         | ABORT          |
| 5   | Dual CatBoost (ext→R1/base→R2+) | ≤ +0.077\*      | —             | 0.00          | −0.183\*      | REJECT\*       |
| 6   | Raw ablation (参考)             | +0.077 / +0.116 | −0.10 / −0.11 | −0.28 / −0.16 | −0.18 / −0.14 | REJECT         |

\*手法 5 は理論的上限分析。Extended model の top1 上限 = ablation の +0.077pp (LB95=−0.183)。  
place3 は base model 採用で構成上 0.000pp だが、top1 gain が統計的に有意でないため REJECT。

## 核心

- CatBoost (YetiRank) は wakuban / umaban_norm / kyori 等の既存特徴量で枠順・距離の signal を**非線形に既捕捉**
- LR specialist の draw_advantage 係数は**負** (−0.006 〜 −0.013)：GBDT 外では逆方向
- Score additive は全 36 組合せ (α,β) で負 delta
- E-top2 Layer2 は best 構成 (draw=0.015, speed=0.2, gap=0.5) で +0.05pp だが LB95=−0.10
- place-preserving override は confirmed signal でのみ有効 (E-top2: XGB#1==CB#2 の +1.36pp)  
  draw/speed の partial ρ ~0.02 では override 対象が見つからない

## 結論

draw + dist-speed signal の post-scoring 抽出は、いかなる place-preserving 手法でも統計的に有意な改善を達成できない。  
CatBoost が既存特徴量で同等以上の情報を内部的に活用済み。  
本番モデル (iter22-jra-etop2) は変更なし。
