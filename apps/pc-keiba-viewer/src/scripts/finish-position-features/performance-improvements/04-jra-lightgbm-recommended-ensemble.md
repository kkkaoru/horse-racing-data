# JRA LightGBM おすすめ構成

対象: JRAの着順予測モデル

## 基本方針

現在の表形式特徴量では、LightGBMを主軸にした複数モデル構成が最も費用対効果が高いです。

単一モデルの精度改善だけを狙うより、目的や学習条件が少し異なるLightGBMを複数作り、レース内正規化後に加重平均する構成を推奨します。

## 推奨モデル構成

### 1. LambdaRank モデル

現行の主力モデルです。

目的:

- レース内の順位全体を最適化する。
- `ndcg_at_3` や `pair_score` を安定させる。

推奨設定:

```text
objective = lambdarank
metric = ndcg
eval_at = 3
boosting_type = gbdt
num_iterations = 500-1500
learning_rate = 0.02-0.08
num_leaves = 31-255
min_child_samples = 20-200
lambda_l2 = 0.0-20.0
```

役割:

- ensembleの中心。
- 初期重みは `0.5-0.7`。

### 2. 1着分類モデル

1着馬の取りこぼしを減らすための補助モデルです。

目的:

- `finish_position = 1` を二値分類する。
- `top1_accuracy` と `top3_winner_capture` を押し上げる。

推奨設定:

```text
objective = binary
metric = binary_logloss, auc
label = finish_position == 1
num_iterations = 500-1500
learning_rate = 0.02-0.08
num_leaves = 31-127
min_child_samples = 50-300
lambda_l2 = 1.0-30.0
```

注意:

- 1着は少数ラベルなので、過学習に注意する。
- レース内で予測確率を正規化して使う。

役割:

- ensembleの補助。
- 初期重みは `0.15-0.25`。

### 3. 3着内分類モデル

馬券圏内の安定性を見る補助モデルです。

目的:

- `finish_position <= 3` を二値分類する。
- `top3_box_accuracy` と `top3_place_relation` を押し上げる。

推奨設定:

```text
objective = binary
metric = binary_logloss, auc
label = finish_position <= 3
num_iterations = 500-1500
learning_rate = 0.02-0.08
num_leaves = 31-127
min_child_samples = 50-300
lambda_l2 = 1.0-30.0
```

役割:

- ensembleの補助。
- 初期重みは `0.15-0.25`。

### 4. 条件別サブモデル

全JRAモデルに加えて、条件別モデルを追加します。

候補:

- 芝モデル
- ダートモデル
- 短距離モデル
- マイルモデル
- 中距離モデル
- 長距離モデル
- 2歳・3歳限定戦モデル
- 重賞モデル

注意:

- データ量が少ない条件は専用モデルにしない。
- 条件別モデル単体で使うより、全体モデルとブレンドする。

例:

```text
final_score =
  0.75 * all_jra_model_score
+ 0.25 * condition_model_score
```

## スコア正規化

モデルごとのスコア尺度が違うため、加重平均前にレース内正規化します。

候補:

```text
rank_normalized = 1 - (rank - 1) / (runner_count - 1)
z_score = (score - race_mean) / race_std
softmax_score = exp(score / temperature) / sum(exp(score / temperature))
```

最初は `rank_normalized` が安全です。

分類モデルの確率は、レース内で合計1になるように正規化する方法も比較します。

## 初期アンサンブル案

最初の実装は以下を推奨します。

```text
ensemble_score =
  0.60 * lambdarank_score_norm
+ 0.20 * top1_probability_norm
+ 0.20 * top3_probability_norm
```

次に、walk-forwardで以下のように重み探索します。

```text
lambdarank: 0.40-0.80
top1:      0.05-0.35
top3:      0.05-0.35
```

制約:

```text
lambdarank + top1 + top3 = 1.0
```

## 検証手順

1. 現行LambdaRank単体の基準値を保存する。
2. 1着分類モデルを追加し、単体評価する。
3. 3着内分類モデルを追加し、単体評価する。
4. 3モデルの加重平均を実装する。
5. validation yearごとに最適重みを探索する。
6. 年別で過剰にブレる重みは採用しない。
7. 最終重みは、全validation yearの平均性能と安定性で決める。

## 採用基準

採用候補:

- `ndcg_at_3` が改善する。
- `top3_winner_capture` が改善する。
- `pair_score` が悪化しない。
- 年別で改善が安定している。

不採用候補:

- 1年だけ大きく改善し、他の年で悪化する。
- `top1_accuracy` だけ改善し、`ndcg_at_3` や `pair_score` が落ちる。
- 条件別モデルがデータ不足で不安定。

## 実装メモ

- 予測保存テーブルには、モデル別の `model_version` を分けて保存する。
- ensembleは別の `model_version` として保存する。
- 例:
  - `jra-lgbm-rank-v1`
  - `jra-lgbm-top1-v1`
  - `jra-lgbm-top3-v1`
  - `jra-lgbm-ensemble-v1`
- 既存の評価SQLを使えるよう、最終的なensemble結果は通常の `predicted_rank` と `predicted_score` に変換する。
