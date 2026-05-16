# JRA finish position 追加特徴量案

対象: JRAの着順予測モデル

## 結論

まだ増やせる特徴量はあります。

現在の特徴量は、馬の近走、騎手、調教師、血統、馬場、天候、馬体重、人気、オッズをある程度カバーしています。次に優先するなら、新しい外部データを足す前に、既存特徴量のレース内相対化を増やすのが最も費用対効果が高いです。

## 1. レース内 rank / percentile 特徴

最優先候補です。

現在の特徴量は絶対値が多いため、同じレース内での相対的な強さを明示的に渡します。

候補:

- `speed_index_avg_5_rank_in_race`
- `speed_index_avg_5_percentile_in_race`
- `speed_index_best_5_rank_in_race`
- `jockey_career_win_rate_rank_in_race`
- `jockey_recent_win_rate_rank_in_race`
- `trainer_career_win_rate_rank_in_race`
- `pedigree_score_for_race_rank_in_race`
- `same_distance_win_rate_rank_in_race`
- `weight_diff_from_avg_rank_in_race`
- `odds_score_rank_in_race`
- `popularity_score_rank_in_race`

期待効果:

- LightGBM LambdaRankと相性が良い。
- 絶対値より「このレースの中で何番目に強いか」を直接表現できる。
- データ追加なしで実装できる。

注意点:

- label由来の値は使わない。
- targetレースの全出走馬が揃った後に計算する。
- nullが多い特徴は `nulls last` のrankと、欠損フラグを併用する。

## 2. オッズ・人気の相対特徴

既存の `popularity_score` と `odds_score` をさらにレース内で相対化します。

候補:

- `odds_implied_probability`
- `odds_implied_probability_share_in_race`
- `odds_gap_to_favorite`
- `odds_ratio_to_favorite`
- `odds_gap_to_median`
- `popularity_gap_to_favorite`
- `model_base_rank_minus_popularity_rank`
- `model_base_score_minus_odds_rank`

期待効果:

- 市場評価とモデル特徴量のズレを拾える。
- 過剰人気、過小人気の検出に使える。

注意点:

- 予測時点で取得できるオッズだけを使う。
- 学習データの確定オッズと、運用時の途中オッズを混ぜると分布がずれる。
- オッズを使うモデルと使わないモデルを分けて評価する。

## 3. 斤量・馬齢・性別・馬体重増減

JRAでは基本的な説明力を持つ可能性が高い特徴です。

候補:

- `futan_juryo`
- `futan_juryo_rank_in_race`
- `futan_juryo_diff_from_race_avg`
- `futan_juryo_diff_from_last_race`
- `horse_age`
- `horse_sex_code`
- `current_bataiju`
- `bataiju_diff_from_last_race`
- `bataiju_change_rate`
- `bataiju_rank_in_race`
- `sex_age_bucket`

期待効果:

- ハンデ戦や牝馬、若駒、休み明けの状態差を拾いやすい。
- 馬体重の変化が近走平均との差より直接的に効く可能性がある。

注意点:

- 馬体重は発表後にしか使えないため、予測タイミング別にモデルを分ける。
- 斤量や性別はJRA SE/UMなどから取得できるか確認する。

## 4. 騎手・調教師の複勝率 / 平均着順

現状は勝率中心です。着順予測では、勝率だけでなく3着内率や平均着順が安定する可能性があります。

候補:

- `jockey_career_place_rate`
- `jockey_recent_place_rate`
- `jockey_keibajo_place_rate`
- `jockey_distance_place_rate`
- `jockey_avg_finish_norm`
- `jockey_recent_avg_finish_norm`
- `trainer_career_place_rate`
- `trainer_keibajo_place_rate`
- `trainer_distance_place_rate`
- `trainer_avg_finish_norm`
- `jockey_trainer_pair_win_rate`
- `jockey_trainer_pair_place_rate`
- `jockey_trainer_pair_count`

期待効果:

- 1着だけでなく、馬券圏内や順位全体の安定性を拾える。
- `ndcg_at_3` や `pair_score` 改善に寄与する可能性がある。

注意点:

- 勝率、複勝率、平均着順は相関が高いので、追加後はfeature importanceとwalk-forwardで確認する。
- 騎手名・調教師名の表記揺れがある場合は先に正規化する。

## 5. レース内の相手関係特徴

既存の `field_strength_*` を拡張し、自馬と相手の差を特徴量化します。

候補:

- `speed_index_avg_5_diff_from_race_avg`
- `speed_index_avg_5_diff_from_top3_avg`
- `speed_index_best_5_diff_from_top3_avg`
- `jockey_win_rate_diff_from_race_avg`
- `trainer_win_rate_diff_from_race_avg`
- `pedigree_score_diff_from_race_avg`
- `stronger_speed_horse_count`
- `stronger_jockey_horse_count`
- `stronger_trainer_horse_count`
- `stronger_pedigree_horse_count`
- `race_speed_stddev`
- `race_jockey_win_rate_stddev`

期待効果:

- 同じ能力値でも、相手が強いレースか弱いレースかを表現できる。
- レース単位の難易度を学習しやすくなる。

注意点:

- 同一レース内のtarget特徴だけで計算する。
- 出走取消や欠損がある場合のrank/count処理を統一する。

## 6. 距離・馬場適性の細分化

既存の距離・馬場特徴を、JRA向けにより細かくします。

候補:

- `distance_bucket_fine`
- `distance_bucket_standard`
- `surface_distance_win_rate`
- `surface_distance_place_rate`
- `surface_distance_avg_finish_norm`
- `track_condition_win_rate`
- `track_condition_place_rate`
- `track_condition_avg_finish_norm`
- `left_right_turn_fit`
- `course_shape_fit`
- `straight_length_fit`
- `hill_course_fit`

期待効果:

- JRAは競馬場・コース形状の差が大きいため、単純な競馬場別成績より効く可能性がある。
- 芝/ダート、距離帯、馬場状態の組み合わせ適性を表現できる。

注意点:

- コース形状データがない場合は、まず競馬場コードと距離、芝/ダートの組み合わせで代替する。
- 細かくしすぎるとデータ不足になるため、最低出走数でguardする。

## 7. ローテーション特徴

`days_since_last_race` はありますが、カテゴリ化したローテ特徴を追加します。

候補:

- `weeks_since_last_race`
- `is_first_after_layoff`
- `is_second_after_layoff`
- `is_third_after_layoff`
- `is_racing_back_to_back`
- `is_short_rest`
- `is_normal_rest`
- `is_long_rest`
- `last_race_to_current_distance_change_bucket`
- `is_distance_extension`
- `is_distance_shortening`
- `is_surface_switch`
- `is_venue_switch`

期待効果:

- 休み明け、叩き2走目、連闘などの状態変化を拾える。
- `days_since_last_race` の連続値だけでは拾いにくい非線形な影響を補える。

注意点:

- 閾値はJRAの慣習に合わせて固定し、後から変えた場合はschema versionを上げる。

## 8. クラス・条件の変化

`last_race_class_diff` をさらに分解します。

候補:

- `is_class_up`
- `is_class_down`
- `is_same_class`
- `is_from_grade_to_allowance`
- `is_from_allowance_to_grade`
- `last_race_grade_code`
- `current_grade_code`
- `last_to_current_grade_diff`
- `kyoso_joken_code_bucket`
- `is_handicap_race`
- `is_maiden_race`
- `is_age_limited_race`

期待効果:

- 昇級戦、降級戦、格上挑戦などを明示できる。
- 近走成績の意味を補正しやすくなる。

注意点:

- 条件コードの解釈を間違えるとノイズになるため、コード体系を確認してから実装する。

## 9. 展開・脚質特徴

近走の通過順から脚質とレース展開を推定します。

候補:

- `running_style_front_rate`
- `running_style_stalker_rate`
- `running_style_closer_rate`
- `avg_corner1_norm_5`
- `avg_corner3_norm_5`
- `avg_corner4_norm_5`
- `front_runner_candidate_count`
- `pace_pressure_score`
- `same_style_rival_count`
- `track_bias_match_score`

期待効果:

- 逃げ馬過多、先行馬有利、差し有利などの展開を拾える。
- `track_bias_front` と組み合わせると効果が出やすい。

注意点:

- 通過順が欠損するレースを考慮する。
- 展開予測は不安定になりやすいため、単体で過信しない。

## 10. リアルタイム系特徴

予測タイミングを分けられるなら効果がある可能性があります。

候補:

- 直前オッズ
- オッズ推移
- 人気変動
- 馬体重発表後の増減
- パドック評価
- 返し馬評価
- 馬場状態の直前更新

期待効果:

- レース直前の市場評価や状態変化を取り込める。

注意点:

- 学習時点で未来情報にならないようにする。
- 予測タイミング別にモデルを分ける。
  - 前日予測モデル
  - 当日朝モデル
  - 馬体重発表後モデル
  - 直前オッズモデル

## 推奨実装順

1. レース内 rank / percentile 特徴。
2. レース内の相手関係特徴。
3. 騎手・調教師の複勝率 / 平均着順。
4. ローテーション特徴のカテゴリ化。
5. 斤量・馬齢・性別・馬体重増減。
6. オッズ・人気の相対特徴。
7. 距離・馬場適性の細分化。
8. クラス・条件の変化。
9. 展開・脚質特徴。
10. リアルタイム系特徴。

## 最初に実装するなら

最初は以下だけでよいです。

- `speed_index_avg_5_rank_in_race`
- `speed_index_best_5_rank_in_race`
- `jockey_recent_win_rate_rank_in_race`
- `trainer_career_win_rate_rank_in_race`
- `pedigree_score_for_race_rank_in_race`
- `same_distance_win_rate_rank_in_race`
- `speed_index_avg_5_diff_from_race_avg`
- `jockey_recent_win_rate_diff_from_race_avg`
- `pedigree_score_diff_from_race_avg`

理由:

- 既存特徴量だけで作れる。
- データソース追加が不要。
- リークリスクが低い。
- LightGBM LambdaRankに合いやすい。

## 計算コストを考慮した実装手順

追加特徴量は、重い履歴joinを増やすものと、既存の最終特徴量から軽く派生できるものに分けます。

最初は、既存の `final_query` 相当の結果から作れる派生特徴を優先してください。履歴テーブル `rec` への追加join、血統集計の追加、騎手・調教師履歴の再集計は後回しにします。

### Phase 1. 最終特徴量からレース内相対特徴を一括生成する

対象:

- レース内 rank / percentile 特徴
- レース内平均との差分
- レース内上位平均との差分
- レース内標準偏差
- 既存 `odds_score` / `popularity_score` の相対化

実装方針:

- `assemble_final_select_from_temp_tables()` の結果を一度 `base_features` 一時テーブルにmaterializeする。
- その後、`base_features` に対してwindow関数で相対特徴を追加する。
- 履歴テーブルへ戻らない。
- `partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango` を共通化する。

SQLイメージ:

```sql
create or replace temp table base_features as
select *
from (... existing final select ...);

select
  b.*,
  percent_rank() over race_by_speed_avg as speed_index_avg_5_percentile_in_race,
  rank() over race_by_speed_avg as speed_index_avg_5_rank_in_race,
  b.speed_index_avg_5
    - avg(b.speed_index_avg_5) over race_partition as speed_index_avg_5_diff_from_race_avg,
  b.jockey_recent_win_rate
    - avg(b.jockey_recent_win_rate) over race_partition as jockey_recent_win_rate_diff_from_race_avg
from base_features b
window
  race_partition as (
    partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
  ),
  race_by_speed_avg as (
    partition by source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango
    order by speed_index_avg_5 asc nulls last
  );
```

注意:

- `speed_index_*` は小さいほうが良い値なのか、大きいほうが良い値なのかを列ごとに確認する。
- rankの向きは特徴量ごとに統一する。
- nullが多い列は、rankだけでなく欠損フラグを追加する。

期待コスト:

- 既存の出力行数に対するwindow計算だけなので比較的軽い。
- 履歴joinを増やさないため、最初の追加として安全。

### Phase 2. 既存履歴CTEの集計項目だけを増やす

対象:

- 騎手・調教師の複勝率
- 騎手・調教師の平均着順
- 馬のローテーションカテゴリ
- 展開・脚質の一部

実装方針:

- すでに作っている `horse_history_base`、`jockey_history`、`trainer_history` を再利用する。
- 新しいCTEやjoinを増やさず、既存の `select` 集計列を増やす。
- 既存のgroup by粒度を変えない。

追加例:

```sql
avg(case when finish_position between 1 and 3 then 1 else 0 end) as jockey_career_place_rate,
avg(finish_norm) as jockey_avg_finish_norm
```

期待コスト:

- 既存CTEのスキャン中に集計列が増えるだけなので、履歴joinを増やすより安い。
- 列数が増える分のメモリと出力サイズは増える。

注意:

- `filter` 付き集計を大量に増やしすぎるとCPUコストが増える。
- まずは勝率に対応する複勝率と平均着順だけ追加する。

### Phase 3. SE/UM/RAなど別テーブルから取れる軽い静的特徴を追加する

対象:

- 斤量
- 馬齢
- 性別
- 馬体重増減
- ハンデ戦フラグ
- 条件コード分解

実装方針:

- すでにstageしている `jra_se`、`jra_um`、`jra_ra` に列を追加して取り込む。
- 追加joinは避け、既存のstage tableとtarget joinに列を足す。
- 取得列を増やす前に、PostgreSQL側の元テーブルに必要列があるか確認する。

期待コスト:

- staging時の転送列数と一時テーブルサイズは増える。
- ただし履歴joinよりは軽い。

注意:

- 斤量、馬体重は予測時点で利用可能か確認する。
- 馬体重増減は発表後モデルと事前モデルを分ける必要がある。

### Phase 4. 新しい履歴集計を増やす

対象:

- 距離・馬場適性の細分化
- コース形状適性
- クラス・条件変化の詳細
- 展開・脚質の詳細

実装方針:

- 既存の `horse_history_base` に必要な列を足してから、同じ履歴joinを再利用する。
- 新しい履歴joinを別途作らない。
- 最低出走数guardを入れる。
- 粒度を細かくしすぎない。

期待コスト:

- Phase 2より重い。
- 距離帯や条件を細かくするとgroup数が増え、メモリと実行時間が増える。

注意:

- `distance_bucket_fine` のような細かいbucketは、データ不足でノイズになる可能性がある。
- 最初は粗いbucketで試し、walk-forwardで効く場合だけ細分化する。

### Phase 5. リアルタイム系特徴を別モデルとして追加する

対象:

- 直前オッズ
- オッズ推移
- 馬体重発表後の増減
- パドック評価

実装方針:

- 既存の事前予測モデルには混ぜない。
- 予測タイミング別にdatasetとmodel_versionを分ける。
- 前日モデル、当日朝モデル、馬体重発表後モデル、直前モデルを区別する。

期待コスト:

- データ取得と履歴保存の設計コストが大きい。
- 特徴量計算よりも、学習時点と推論時点の整合性管理が難しい。

注意:

- 確定後の値を学習に入れるとリークになる。
- まずはLightGBM ensembleが安定してから着手する。

## コスト優先の推奨順

1. `base_features` materialize後のwindow特徴。
2. 既存履歴CTEに複勝率・平均着順を追加。
3. 既存stage tableへ斤量・馬齢・性別を追加。
4. ローテーションカテゴリを既存 `horse_history_base` から追加。
5. 距離・馬場・クラスの細分化。
6. 展開・脚質の詳細化。
7. リアルタイム系特徴。

この順で進めると、最初の改善では重いjoinを増やさず、既存の計算結果を再利用できます。
