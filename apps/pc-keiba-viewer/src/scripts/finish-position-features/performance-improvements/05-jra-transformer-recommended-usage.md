# JRA Transformer 活用方針

対象: JRAの着順予測モデル

## 結論

Transformerは、現在の1行1馬の集計済み特徴量をそのまま入力するだけでは優先度が低いです。

活用するなら、1レース内の全出走馬を同時に入力し、馬同士の相対関係を学習するモデルとして設計します。

## 使う価値があるケース

Transformerを検討する価値があるのは、以下のような構造を学習したい場合です。

- レース内の全馬の相対比較
- 出走頭数による順位難易度の違い
- 枠順と脚質の組み合わせ
- 人気やオッズの分布
- 同型馬の多さ
- 逃げ馬が多いレース、差し馬が多いレースなどの展開構造
- 同じ騎手・調教師・血統傾向を持つ馬同士の比較

## 推奨入力形式

1レースを1サンプルとして扱います。

```text
race = [
  horse_1_features,
  horse_2_features,
  horse_3_features,
  ...
  horse_N_features
]
```

各horse featureには、現在の表形式特徴量を使います。

追加で入れると良い特徴:

- 馬番
- 枠番
- 出走頭数
- レース内人気rank
- レース内オッズrank
- レース内偏差値化した能力特徴
- 芝/ダート
- 距離
- 競馬場
- 馬場状態
- グレード

## モデル構成案

### Race Set Transformer

レース内の出走馬集合をTransformer Encoderに通します。

```text
horse_features
  -> numeric embedding
  -> categorical embedding
  -> transformer encoder
  -> per-horse score head
  -> race内 ranking
```

出力:

- 各馬のscore
- scoreをレース内で順位化

損失関数候補:

- ListNet系のlistwise loss
- pairwise ranking loss
- 1着分類 loss
- 3着内分類 loss

最初は実装しやすい `1着分類 + 3着内分類 + pairwise ranking` の組み合わせが現実的です。

## LightGBMとの併用

Transformer単体でLightGBMを置き換えるのではなく、補助モデルとして使います。

推奨:

```text
final_score =
  0.75 * lightgbm_ensemble_score
+ 0.25 * transformer_score
```

初期重みはTransformerを低めにします。

```text
transformer_weight = 0.10-0.30
```

理由:

- 表形式特徴量ではLightGBMが強い。
- Transformerは過学習しやすい。
- データ量と時系列分割への感度が高い。

## 実装前に必要な検証

Transformer実装前に、以下を確認します。

- JRAの学習レース数が十分あるか。
- validation yearごとの性能が安定しているか。
- 1レース内の全出走馬を欠損なく揃えられるか。
- race_id単位でtrain/valid/testを分割できるか。
- 同一レースの馬がtrainとvalidに分かれないようにできるか。

## 過学習対策

Transformerを使う場合は、モデルを小さく始めます。

初期設定候補:

```text
embedding_dim = 64-128
num_layers = 2-4
num_heads = 4
dropout = 0.1-0.3
batch_size = 64-256 races
early_stopping = enabled
weight_decay = 1e-4 - 1e-2
```

避ける設定:

- 最初から大きな層数にする。
- validation yearをランダム分割にする。
- レース内の馬を別サンプルとして独立に扱う。

## LSTMとの違い

LSTMは馬ごとの過去走シーケンス向けです。

Transformerはレース内の相対関係向けです。

現在の特徴量パイプラインに追加するなら、LSTMよりもTransformerの方が自然です。ただし、現在の1行1馬CSVをそのまま使うのではなく、レース単位の入力へ変換する必要があります。

## 推奨実装順

1. LightGBM ensembleを先に完成させる。
2. race_id単位でTransformer用データセットを作る。
3. 小さいRace Set Transformerを作る。
4. Transformer単体をwalk-forwardで評価する。
5. LightGBM ensembleに低い重みでブレンドする。
6. 年別に安定して改善する場合だけ採用する。

## 採用基準

採用候補:

- LightGBM ensembleに混ぜたとき、`ndcg_at_3` が安定して改善する。
- `pair_score` が改善する。
- `top3_winner_capture` が改善する。
- validation year間で改善が偏らない。

不採用候補:

- Transformer単体は良く見えるが、LightGBM ensembleに混ぜると改善しない。
- 1年だけ改善し、他年で悪化する。
- 学習が不安定で再現性が低い。
- 推論・運用コストに見合う改善がない。
