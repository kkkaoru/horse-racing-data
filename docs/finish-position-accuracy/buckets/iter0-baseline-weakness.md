---
date: 2026-06-04
iteration: 0
purpose: Baseline (v7-lineage) weak-spot audit — Iter 1 lever 選定の根拠
---

# v7-lineage baseline weak-point audit

## Active models

| category | model_version      | activated_at         |
| -------- | ------------------ | -------------------- |
| jra      | jra-cb-v7-lineage  | 2026-05-24T04:53:09Z |
| nar      | nar-xgb-v7-lineage | 2026-05-22T21:28:37Z |

備考: production active な model_version は `-wf-21y` 接尾なし。 これらは v7-lineage WF parquet (`apps/pc-keiba-viewer/tmp/bucket-eval/finish-position/v7-lineage-wf-21y/predictions/`) から WF 推論結果を直接読み、 `model_prediction_bucket_evaluations` を populate する Stage 4 `evaluate-bucket-21y-v7lineage.ts` は本 audit 時点で未実行 (table empty)。 本 audit は SQL-direct fallback (`tmp/v8/identify_weak_buckets_fallback.py`) で WF predictions parquet + PG `race_entry_corner_features` + `jvd_ra` / `nvd_ra` を直接 join し、 `evaluate-bucket-predictions-sql.ts` と等価な aggregate を計算した。

## Category overall mean (race-weighted, keibajo_code dim total)

| cat | races   | top1   | place2 | place3 | top3_box |
| --- | ------- | ------ | ------ | ------ | -------- |
| JRA | 66,964  | 0.4014 | 0.2173 | 0.1620 | 0.1424   |
| NAR | 258,966 | 0.5805 | 0.3609 | 0.2861 | 0.3718   |

これらは production `model_prediction_evaluations` の active 行 (`jra-cb-v7-lineage`: top1=0.5251, ...) とは **異なる** 値である。 active 行は本番 score 経路の累積 (再 score 重複 / 別ウィンドウ) を含み、 本 audit は WF 厳密 walk-forward の honest fold predictions のみを集計しているため。 lever 比較は本 audit の WF mean を baseline とする。

## JRA worst buckets (composite_gap descending、 n >= 100、 top 5 per dim)

`composite_gap` = (top1_gap + place2_gap + place3_gap + top3_box_gap) / 4. 正は category mean より accuracy が低いことを意味する (= 改善余地大)。

### dim: keibajo_code

| rank | bucket    | n     | top1  | place2 | place3 | top3_box | composite_gap |
| ---- | --------- | ----- | ----- | ------ | ------ | -------- | ------------- |
| 1    | 05 (東京) | 10115 | 0.379 | 0.200  | 0.149  | 0.117    | +0.0194       |
| 2    | 04 (新潟) | 6286  | 0.386 | 0.203  | 0.149  | 0.123    | +0.0156       |
| 3    | 07 (阪神) | 6060  | 0.394 | 0.208  | 0.155  | 0.127    | +0.0097       |
| 4    | 08 (京都) | 9212  | 0.395 | 0.218  | 0.161  | 0.142    | +0.0017       |
| 5    | 09 (中京) | 9660  | 0.394 | 0.221  | 0.165  | 0.140    | +0.0008       |

### dim: kyori

| rank | bucket | n     | top1  | place2 | place3 | top3_box | composite_gap |
| ---- | ------ | ----- | ----- | ------ | ------ | -------- | ------------- |
| 1    | 1400m  | 8561  | 0.356 | 0.187  | 0.147  | 0.111    | +0.0306       |
| 2    | 1600m  | 7631  | 0.361 | 0.197  | 0.141  | 0.105    | +0.0298       |
| 3    | 2200m  | 1247  | 0.363 | 0.208  | 0.145  | 0.118    | +0.0223       |
| 4    | 1300m  | 558   | 0.387 | 0.194  | 0.152  | 0.118    | +0.0180       |
| 5    | 1200m  | 12946 | 0.390 | 0.211  | 0.150  | 0.128    | +0.0110       |

### dim: track_code

| rank | bucket      | n     | top1  | place2 | place3 | top3_box | composite_gap |
| ---- | ----------- | ----- | ----- | ------ | ------ | -------- | ------------- |
| 1    | 10 (障害)   | 481   | 0.306 | 0.175  | 0.096  | 0.071    | +0.0691       |
| 2    | 12 (障害)   | 1459  | 0.319 | 0.171  | 0.140  | 0.077    | +0.0541       |
| 3    | 11 (障害)   | 8909  | 0.365 | 0.189  | 0.148  | 0.109    | +0.0280       |
| 4    | 18 (ダート) | 6391  | 0.371 | 0.209  | 0.146  | 0.111    | +0.0215       |
| 5    | 17 (ダート) | 14916 | 0.385 | 0.205  | 0.157  | 0.129    | +0.0117       |

### dim: grade_code

| rank | bucket         | n     | top1  | place2 | place3 | top3_box | composite_gap |
| ---- | -------------- | ----- | ----- | ------ | ------ | -------- | ------------- |
| 1    | C (1勝クラス)  | 1302  | 0.321 | 0.141  | 0.115  | 0.065    | +0.0703       |
| 2    | B (2勝クラス)  | 696   | 0.355 | 0.175  | 0.138  | 0.096    | +0.0397       |
| 3    | A (3勝クラス)  | 448   | 0.395 | 0.172  | 0.127  | 0.074    | +0.0388       |
| 4    | L (リステッド) | 474   | 0.331 | 0.192  | 0.152  | 0.112    | +0.0341       |
| 5    | E (一般)       | 14872 | 0.363 | 0.196  | 0.148  | 0.106    | +0.0273       |

### dim: condition_key

JRA は `condition_key` を null として bucket 化しているため、 n >= 100 の bucket は 0 件。 JRA 側の condition_key 加味は Iter 1 の対象外 (現状 `evaluate-bucket-predictions-sql.ts` の category 分岐に従う)。

## NAR worst buckets (composite_gap descending、 n >= 100、 top 5 per dim)

### dim: keibajo_code

| rank | bucket      | n     | top1  | place2 | place3 | top3_box | composite_gap |
| ---- | ----------- | ----- | ----- | ------ | ------ | -------- | ------------- |
| 1    | 44 (笠松)   | 22447 | 0.430 | 0.239  | 0.172  | 0.169    | +0.1473       |
| 2    | 43 (名古屋) | 12560 | 0.465 | 0.266  | 0.202  | 0.225    | +0.1104       |
| 3    | 58 (姫路)   | 279   | 0.466 | 0.301  | 0.262  | 0.258    | +0.0781       |
| 4    | 30 (門別)   | 16117 | 0.511 | 0.290  | 0.229  | 0.269    | +0.0750       |
| 5    | 45 (金沢)   | 13958 | 0.507 | 0.301  | 0.242  | 0.293    | +0.0644       |

### dim: kyori

| rank | bucket | n     | top1  | place2 | place3 | top3_box | composite_gap |
| ---- | ------ | ----- | ----- | ------ | ------ | -------- | ------------- |
| 1    | 1200m  | 23919 | 0.479 | 0.269  | 0.202  | 0.220    | +0.1072       |
| 2    | 2200m  | 154   | 0.474 | 0.325  | 0.240  | 0.260    | +0.0752       |
| 3    | 1000m  | 8151  | 0.539 | 0.309  | 0.236  | 0.284    | +0.0580       |
| 4    | 1800m  | 5510  | 0.521 | 0.311  | 0.250  | 0.306    | +0.0528       |
| 5    | 1230m  | 3368  | 0.523 | 0.319  | 0.239  | 0.309    | +0.0524       |

### dim: track_code

| rank | bucket          | n      | top1  | place2 | place3 | top3_box | composite_gap |
| ---- | --------------- | ------ | ----- | ------ | ------ | -------- | ------------- |
| 1    | 26 (ダート右内) | 24887  | 0.467 | 0.262  | 0.195  | 0.205    | +0.1176       |
| 2    | 11 (芝右)       | 1153   | 0.435 | 0.260  | 0.225  | 0.251    | +0.1071       |
| 3    | 23 (ダート左)   | 50215  | 0.528 | 0.317  | 0.247  | 0.309    | +0.0496       |
| 4    | 24 (ダート右)   | 182711 | 0.611 | 0.387  | 0.310  | 0.412    | -0.0303       |

(24 が最大 sample で mean を引き上げており、 上位 3 が relative weakness)

### dim: grade_code

| rank | bucket     | n     | top1  | place2 | place3 | top3_box | composite_gap |
| ---- | ---------- | ----- | ----- | ------ | ------ | -------- | ------------- |
| 1    | A (重賞 A) | 189   | 0.497 | 0.312  | 0.196  | 0.275    | +0.0797       |
| 2    | R (重賞 R) | 726   | 0.528 | 0.282  | 0.242  | 0.274    | +0.0682       |
| 3    | E (一般)   | 51005 | 0.546 | 0.329  | 0.262  | 0.331    | +0.0327       |
| 4    | C (重賞 C) | 358   | 0.534 | 0.330  | 0.249  | 0.369    | +0.0297       |
| 5    | B (重賞 B) | 211   | 0.502 | 0.336  | 0.284  | 0.370    | +0.0266       |

### dim: condition_key

| rank | bucket          | n   | top1  | place2 | place3 | top3_box | composite_gap |
| ---- | --------------- | --- | ----- | ------ | ------ | -------- | ------------- |
| 1    | Ｃ１ 二 …選抜   | 209 | 0.383 | 0.177  | 0.134  | 0.115    | +0.1977       |
| 2    | Ｃ２ 三 …選抜   | 124 | 0.339 | 0.218  | 0.153  | 0.121    | +0.1922       |
| 3    | Ｃ１ 三 …選抜   | 179 | 0.369 | 0.223  | 0.123  | 0.123    | +0.1903       |
| 4    | Ｃ２ 二 …選抜   | 222 | 0.410 | 0.203  | 0.131  | 0.117    | +0.1847       |
| 5    | Ｃ２ 一 Ｃ２ 二 | 165 | 0.394 | 0.218  | 0.127  | 0.188    | +0.1680       |

NAR の低 class 「Ｃ１ / Ｃ２ 選抜」系で全 metric が大幅に mean を下回る。 n >= 100 の condition_key bucket は 470 件存在し、 condition_key 全体として体系的に弱い (mean を引き上げているのは小規模な無条件混走 / Ｂ級 race 群)。

## Per-metric weakness summary (n >= 100 で最大 gap)

| cat | metric   | dim           | value         | gap     | n    |
| --- | -------- | ------------- | ------------- | ------- | ---- |
| JRA | top1     | track_code    | 10 (障害平地) | +0.0958 | 481  |
| JRA | place2   | grade_code    | C (1勝クラス) | +0.0767 | 1302 |
| JRA | place3   | track_code    | 10 (障害平地) | +0.0664 | 481  |
| JRA | top3_box | grade_code    | C (1勝クラス) | +0.0771 | 1302 |
| NAR | top1     | condition_key | Ｃ２ 三 …選抜 | +0.2418 | 124  |
| NAR | place2   | condition_key | Ｂ３ …選抜馬  | +0.1943 | 108  |
| NAR | place3   | condition_key | Ｃ１ 三 …選抜 | +0.1632 | 179  |
| NAR | top3_box | condition_key | Ｃ１ 二 …選抜 | +0.2569 | 209  |

## Iter 1 lever recommendation

### JRA

- Worst metric: **top3_box (grade C)** + **place2 (grade C)** — composite_gap 1 位 (障害 + 1 勝クラス) は集中型、 worst metric が「中位 / 下位 grade で 2 着 + 3 着組合せ全体」を取り損ねている。
- 推奨 lever:
  1. **L2 (isotonic calibration、 train-free、 SubAgent 並列 OK)** — place2 / place3 / top3_box は確率の順位だけでなく cutoff 校正で改善余地が大きく、 plan default を JRA にそのまま適用。
  2. **L1A (CB+XGB ensemble、 train-free)** — top1 (障害) と top3_box (grade C) の同時改善に train-free で寄与しうる、 L2 と直交。
- 不採用: L4 (bucket-aware sample weight) は retrain 必須で本 iter に不適合、 worst bucket (障害) が単独 4.8% 程度の race share しか持たないため、 grade-C と障害は Iter 3+ の queue。

### NAR

- Worst metric: **top3_box (Ｃ１ 二 選抜) + top1 (Ｃ２ 三 選抜)** — `condition_key` 軸で「下位 class の選抜混走」全体が体系的に弱い、 worst の絶対 gap は +0.19 〜 +0.26pp と巨大。
- 推奨 lever:
  1. **L2 (isotonic calibration)** — top1 / place2 / place3 / top3_box の 4 軸を condition_key 別に校正すれば最も gain が見込める (plan v7.1 「place2 / place3 重視」とも合致)。
  2. **L1A (CB+XGB ensemble)** — NAR は XGB 単体なので CB をアンサンブル先として加える train-free 余地が大きい。
- 不採用 / queue:
  - L4 (sample weight per condition_key) は retrain 必須だが、 worst bucket が n=100 〜 200 と小さく gain が薄いため Iter 3+ で再評価。
  - L8 (stacking)、 L15 (sectional)、 L16 (r431 asymmetric) は L2 / L1A で gain 余地が露呈してから着手 (overfit / train-cost のリスクが残る)。

### 結論 (両 cat 共通)

- **Iter 1 は plan default の L2 (isotonic calibration) + L1A (CB+XGB ensemble) を JRA / NAR の両方で並列実施**。
- どちらも train-free で 1 iter 内に完走可能、 worst metric (top3_box + place2) は確率校正で gain しやすく、 audit 結果と整合。
- accept されない場合 (Step F gate fail) の次 iter 候補: L4 (NAR condition_key bucket weight) → L8 (stacking) の順で priority queue。

## 参考: 出力 file

- 数値 source: `tmp/v8/weak-buckets-iter0.json` (3,370 entries、 schema_version=1)
- 生成 script (fallback): `tmp/v8/identify_weak_buckets_fallback.py`
- active models snapshot: `tmp/v8/stage-0d-active-models.json`
