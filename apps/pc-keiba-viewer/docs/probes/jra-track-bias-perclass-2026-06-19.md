# JRA トラックバイアス交互作用 — 細かい分類別 partial ρ probe (2026-06-19)

## 目的

グローバル評価では枠番×バイアス系の交互作用は partial ρ < 0.02 で ABORT されたが、
USER 指示により **細かい分類セル**(クラス × 芝ダ × 馬場状態 × 距離帯)ごとに評価し、
改善が確認できたセルでのみ model/logic を分岐させる方針。

## 手法

- Store: `tmp/v8/feat-jra-v8-iter19-kohan3f-going/race_year={2019..2025}/data_0.parquet`
- DuckDB(memory_limit=4GB, threads=4)で turf(track_code 10–22)/ dirt(23–29)を抽出。
  jump/障害(52–57)は除外。null(odds_score/umaban/bias/baba)行は除外。**321,098 行**。
- アウトカム = **win**(finish_position == 1)。
  ※ `odds_score` は値が大きいほど人気薄(corr(odds_score, finish)=+0.567)なので、
  人気上位ほど低 odds_score。control に含めて市場効率を吸収する。
- 候補交互作用(`umaban_norm` 0–1 正規化):
  - **a. umaban × current_baba_condition**(枠番 × 馬場状態)
  - **b. umaban × kyori_band**(枠番 × 距離帯)
  - **d. umaban × track_bias_inside**(枠番 × 既存内枠バイアス)
- partial Spearman ρ: rank-z 変換 → control `[odds_score, track_bias_inside, umaban_norm]` を
  最小二乗で残差化 → 残差同士の相関。**正方向(win 寄り)に符号固定**。
- Bootstrap 95% CI(n_boot=5000, seed=42)を残差 Pearson 相関で算出(ベクトル化)。
- セルは **n ≥ 300** のみ採用。**PASS = LB95 > 0**。

## 結果サマリ

**176 セル評価 → 14 セル PASS(LB95 > 0)。ただし全 PASS の効果量は ρ ≈ 0.004–0.028 と極小。**
高 ρ(0.06–0.07)のセルは n が小さく CI が 0 をまたぎ、全て非 PASS。

### PASS セル一覧(LB95 降順)

| candidate        | grouping           | cell                                  | ρ       | CI95              | n       |
| ---------------- | ------------------ | ------------------------------------- | ------- | ----------------- | ------- |
| b_umaban_x_kyori | grade×surface      | grade= \| turf                        | +0.0131 | [+0.0070,+0.0191] | 99,978  |
| b_umaban_x_kyori | surface×baba       | turf \| baba=1(良)                    | +0.0112 | [+0.0057,+0.0169] | 119,742 |
| b_umaban_x_kyori | surface            | turf                                  | +0.0106 | [+0.0054,+0.0154] | 155,915 |
| d_umaban_x_bias  | surface×baba×kyori | dirt \| baba=3(重) \| kyori=0(sprint) | +0.0280 | [+0.0003,+0.0532] | 5,082   |
| d_umaban_x_bias  | surface×baba       | dirt \| baba=1(良)                    | +0.0081 | [+0.0021,+0.0139] | 103,943 |
| d_umaban_x_bias  | surface            | dirt                                  | +0.0077 | [+0.0029,+0.0123] | 165,183 |
| d_umaban_x_bias  | grade×surface      | grade= \| dirt                        | +0.0074 | [+0.0023,+0.0125] | 135,525 |
| d_umaban_x_bias  | surface×baba×kyori | dirt \| baba=1 \| kyori=1(mile)       | +0.0114 | [+0.0011,+0.0209] | 37,159  |
| d_umaban_x_bias  | surface×kyori      | dirt \| kyori=0(sprint)               | +0.0099 | [+0.0010,+0.0186] | 46,767  |
| d_umaban_x_bias  | surface×kyori      | dirt \| kyori=2(middle)               | +0.0082 | [+0.0004,+0.0161] | 56,002  |
| b_umaban_x_kyori | grade              | grade=E(新馬)                         | +0.0076 | [+0.0001,+0.0154] | 66,214  |
| d_umaban_x_bias  | grade              | grade=(平場)                          | +0.0044 | [+0.0004,+0.0084] | 235,503 |
| a_umaban_x_baba  | surface            | turf                                  | +0.0051 | [+0.0001,+0.0101] | 155,915 |

## 注目仮説の検証

- **重馬場 × 芝 × 内枠**(d, turf baba=3/4): **全セル CI が 0 をまたぎ非 PASS**。
  baba=3×kyori=3 で ρ=+0.069 と最大値だが n=506 で CI[-0.017,+0.148]。シグナル不在ではないが統計的に確証できず。
- **短距離 × ダート × 外枠**:
  - 候補 a(umaban×baba)は **構造上 ρ≈0**(セルを baba で固定すると umaban×const となり control の umaban と共線 → 残差化で消える)。a が PASS するのは baba が変動する粗い `surface=turf` のみ。
  - 候補 d(umaban×bias)は dirt sprint で **PASS**(全体 ρ=+0.0099、baba=3 で +0.0280)。重ダート短距離で内枠バイアスと枠番の相互作用がわずかに残る。
- **新馬(E) × track_bias**: d(umaban×bias)は ρ=+0.0042 で非 PASS。
  PASS は b(umaban×kyori、ρ=+0.0076、LB95=+0.0001)のみで、新馬特有の bias 読み替えは検出されず。

## 判定

**ADOPT 不可(全 PASS が経験的に無視可能な効果量)。**

- 統計的に有意(LB95>0)なセルは存在するが、最大でも ρ≈0.028、大半は ρ<0.013。
  これは control に既に `track_bias_inside`/`umaban_norm` が入っており、GBDT が
  これらの非線形交互作用を既に捕捉済みであることと整合する。
- グローバル ABORT(ρ<0.02)と同じ結論が細かい分類でも成立。例外的に大きく見えるセル
  (重馬場×芝×内枠、ダート短距離×外枠)は **小 n で CI が 0 をまたぎ**、別個の
  model 分岐を正当化しない。
- USER 方針(改善セルのみ分岐)に照らしても、**incremental model 検証に進める価値のある
  セルは無い**。partial ρ が「必要だが十分でない」原則
  ([[project_relationship_perclass_investigation_2026_06_12]])どおり、
  この ρ 水準では per-class routing を実装しても serve 精度向上は期待できない。

## 成果物

- probe: `apps/pc-keiba-viewer/tmp/probe_track_bias_perclass.py`
- raw 結果: `apps/pc-keiba-viewer/tmp/probe_track_bias_results.json`(176 セル全件)
