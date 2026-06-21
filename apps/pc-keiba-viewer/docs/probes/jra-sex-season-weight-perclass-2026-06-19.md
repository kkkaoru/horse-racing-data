# JRA 性別×季節×体重 per-class / per-season partial ρ probe (2026-06-19)

## 結論 (TL;DR)

**全 class / 全 season / 交差セルで採用に値する分類は無し。REJECT。**

- グローバル評価では partial ρ 最大 +0.048 で REJECT 済み。
- per-class / per-season / cross に細分化しても、**PASS した 10 セルはすべて |ρ| ≤ 0.080、大半は ≤ 0.03** で、グローバル値すら下回る。
- 1.5%〜8% の partial ρ は per-class GBDT routing を正当化する信号量に達していない。サンプルが大きいセルで「LB95>0」が出るのは効果量ではなく n 由来。
- `partial ρ は必要だが十分でない`（[[project-relationship-perclass-investigation-2026-06-12]]）に整合。**本番無変更を推奨。**

## 方法

- Feature store: `tmp/v8/feat-jra-v8-iter19-kohan3f-going/race_year=2019..2025`（JRA、330,844 行 JOIN 後）
- 性別: PG `jvd_um.seibetsu_code`（1=牡, 2=牝, 3=せん）を `ketto_toroku_bango` で JOIN
- target = `-finish_norm`（finish_norm は 0=1着…1=最下位なので符号反転して higher=better）
- control = `odds_score`（OLS で y と x を残差化した後 Pearson = partial ρ）
- 候補特徴: `is_mare`(牝=1) / `bataiju`(=bataiju_avg5, 欠損0除外を z 化) / `sex_x_bataiju`(is_mare × weight_z)
- bootstrap: n_boot=5000, seed=42, 片側下側 5%tile = LB95(one-sided)
- セル条件: cross は n ≥ 500 のみ
- スクリプト: `tmp/probes/sex_season_weight_probe.py`

NOTE: `futan_weight_class` は当該 feature store で全行 0 のため不使用。重みは `bataiju_avg5` を採用。

## PASS したセル（LB95 one-sided > 0）— 全 10 件

| scope  | class       | season | feat          |      n |       ρ | LB95(one) |
| ------ | ----------- | ------ | ------------- | -----: | ------: | --------: |
| class  | L           | all    | bataiju       |  5,955 | +0.0218 |   +0.0008 |
| season | all         | 春     | is_mare       | 82,506 | +0.0158 |   +0.0100 |
| cross  | '' (未勝利) | 春     | is_mare       | 60,756 | +0.0112 |   +0.0047 |
| cross  | A           | 冬     | sex_x_bataiju |  1,054 | +0.0548 |   +0.0089 |
| cross  | E (新馬)    | 冬     | bataiju       | 16,324 | +0.0241 |   +0.0111 |
| cross  | E (新馬)    | 冬     | sex_x_bataiju | 16,324 | +0.0248 |   +0.0112 |
| cross  | E (新馬)    | 春     | is_mare       | 18,260 | +0.0126 |   +0.0003 |
| cross  | E (新馬)    | 秋     | sex_x_bataiju | 16,569 | +0.0263 |   +0.0134 |
| cross  | L           | 秋     | bataiju       |  1,657 | +0.0804 |   +0.0376 |
| cross  | L           | 秋     | sex_x_bataiju |  1,657 | +0.0606 |   +0.0190 |

最大は L クラス×秋の bataiju ρ=+0.080（n=1,657）。グローバル REJECT 基準 +0.048 を超えるのは L×秋 の 2 件のみだが、いずれも小 n（1,657）で安定性に乏しく、隣接セル（L×冬/春/夏）では全て REJECT のため再現性なし＝ノイズと判断。

## 観察

- `is_mare` の符号は季節で反転（春 +、秋/冬 −）。牝馬の相対力は春に僅かに高く秋に低い傾向だが ρ≤0.027 で実用域外。
- `bataiju` の主効果は概ね負（重い馬がやや劣る）だが、新馬・L クラスの一部で正。クラス・季節で符号が安定せず、GBDT が既存 263 特徴（`bataiju_avg5`, `weight_trend_5`, `field_*`, `futan_*` 等）で非線形に既に捕捉済みと整合。
- `sex_x_bataiju`(牝×体重) は新馬冬/秋で +0.025 前後。最も「らしい」交互作用だが、やはり効果量が小さく per-class routing コストに見合わない。

## 推奨

1. **本番モデル無変更。** per-class でも性別×季節×体重は frontier を動かさない。
2. 採否は probe の ρ ではなく incremental model 検証（per-class/serve 分布）で最終判断すべき。今回 ρ 自体が閾値未達なのでモデル検証フェーズに進める価値なし。
3. もし将来再検討するなら、L×秋・新馬×冬の sex_x_bataiju に限った狭い routing 実験のみ。ただし優先度は低（[[project-perclass-campaign-complete-2026-06-17]] の他レバー枯渇状況と同様）。
