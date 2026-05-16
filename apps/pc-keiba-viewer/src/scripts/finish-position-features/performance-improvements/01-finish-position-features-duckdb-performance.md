# finish_position_features_duckdb.py 高速化案

対象: `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`

このスクリプトは、PostgreSQL の競馬データを DuckDB に取り込み、着順予測 LightGBM 用の特徴量を集計して、年別 Parquet に出力するバッチである。主なボトルネック候補は、PostgreSQL からの読み込み量、同種の履歴 join の重複、日付変換の繰り返し、最後の Parquet 再スキャンである。

## 優先度高

### 1. `rec` の取得列を絞る

現状は `pg.race_entry_corner_features` から `select *` で temp table `rec` を作っている。実際に使う列だけを明示すれば、PostgreSQL から DuckDB への転送量、DuckDB のメモリ使用量、temp table 作成時間を削減できる。

同様に、`jra_um` / `nar_um` も血統特徴で使う `ketto_toroku_bango`, `ketto_joho_01b`, `ketto_joho_05b` だけで足りる可能性が高い。

期待効果: 大。最初の読み込みと以後の全 CTE のスキャンが軽くなる。

### 2. 履歴期間の指定を日付演算に直す

現状の `history_start = str(int(from_date) - HISTORY_LOOKBACK_YYYYMMDD)` は YYYYMMDD 文字列を数値として引いている。これは暦上の「10年前」とは一致しない。

`race_date` を date 型に変換した列、または PostgreSQL 側の日付式を使って、`from_date - interval '10 years'` 相当の範囲にする。これにより、不要な履歴読み込みを避けつつ、期間指定の意味も明確になる。

期待効果: 中から大。読み込み対象が過剰な場合に効く。正確性の改善にもなる。

### 3. `strptime()` の繰り返しをなくす

複数の CTE で `strptime(race_date, '%Y%m%d')::date` を繰り返している。`rec` と `target` 作成時に `race_dt` のような date 型列を作っておけば、日数差や期間 filter で毎回文字列変換をしなくてよい。

対象例:

- `days_since_last_race`
- `consecutive_race_count`
- `jockey_recent_win_rate`
- `track_bias`

期待効果: 中。履歴 join 後の大きな中間結果に対して繰り返されるため、CPU 削減が見込める。

### 4. 馬の過去走 join を共通化する

`horse_career_cte()`, `weight_cte()`, `recent_form_cte()`, `legacy_five_cte()` は、いずれも対象馬と過去走を `source`, `ketto_toroku_bango`, `race_date < target` で join している。これを `horse_history_base` のような temp table として一度 materialize し、各特徴量 CTE から使い回す。

共通化候補の列:

- target の主キー列
- target race date / date 型列
- history race date / date 型列
- `recent_rank`
- `finish_position`, `finish_norm`, `time_sa`, `kohan_3f`, `corner3_norm`, `corner4_norm`
- `kyori`, `track_code`, `grade_code`, `kyoso_joken_code`

期待効果: 大。最も重いと想定される自己履歴 join を複数回実行しなくて済む。

### 5. 最終出力後の Parquet 再カウントをオプション化する

`count_output_rows()` は出力済み Parquet を `read_parquet` で再読み込みして `count(*)` している。大規模データでは、最後に全出力を再スキャンするコストが発生する。

対応案:

- `--skip-count` を追加する
- デフォルトでは `target` 行数を `rows_written` の近似値として返す
- 厳密な行数確認が必要なときだけ Parquet を再カウントする

期待効果: 中。処理末尾の待ち時間を削減できる。

## 優先度中

### 6. `rec` に前処理済み列を持たせる

`race_dt`, `surface`, `kyori_band`, `class_level`, 数値化済み `time_sa` / `kohan_3f` / corner 系などを `rec` 作成時に持たせる。各 CTE で同じ cast や `left(coalesce(...))` を繰り返すより、スキャン時の計算を減らせる。

期待効果: 中。SQL の見通しも改善する。

### 7. 血統統計をキャッシュする

`sire_distance_stats`, `sire_track_stats`, `damsire_distance_stats`, `damsire_track_stats` は target 依存ではなく、カテゴリと履歴期間に依存する集計である。同じ期間・カテゴリで繰り返し実行するなら、Parquet または DuckDB 永続 DB にキャッシュできる。

期待効果: 中。再実行やチューニング試行が多い場合に特に効く。

### 8. `all` カテゴリの血統処理を分割する

現状の `pedigree_cte("all")` は `jra_um` を使うため、NAR 側の血統参照としては不自然である。`jra`, `nar`, `ban-ei` を別々に集計して union する形にすると、正確性と最適化の両面で扱いやすくなる。

期待効果: 性能は中、正確性面の価値が大きい。

### 9. PostgreSQL 側 index を確認する

DuckDB temp table 上には index を作っているが、最初に PostgreSQL から読む段階では PostgreSQL 側 index が効く。少なくとも以下の index があるか確認したい。

- `race_entry_corner_features(race_date)`
- `race_entry_corner_features(source, race_date)`
- `race_entry_corner_features(source, ketto_toroku_bango, race_date)`

期待効果: 中。PostgreSQL からの staging が遅い場合に効く。

### 10. 中間 staging を永続化する

開発中に同じ期間で何度も実行するなら、`rec` や血統統計を DuckDB ファイル、または Parquet に保存しておく。PostgreSQL attach と全量 staging を毎回行わずに済む。

期待効果: 中。反復実行の高速化に向く。

## 優先度低または要実測

### 11. 直近特徴と通算特徴を分離する

直近5走だけ必要な特徴と、全履歴が必要な勝率系が混在している。直近特徴だけは `recent_rank <= 5` に早めに絞ったテーブルから作ると、後段の集計メモリを抑えられる可能性がある。

期待効果: 小から中。実測次第。

### 12. Parquet 出力設定を調整する

現状は `partition_by (race_year)` のみ。下流の読み方に応じて、以下を検討する。

- `category` や `source` で追加分割する
- compression を明示する
- row group size を調整する

期待効果: 小から中。LightGBM 側の読み込み条件に依存する。

### 13. DuckDB profiling を入れる

改善前に `PRAGMA enable_profiling='json'` などで、CTE ごとの実行時間、スキャン量、join コストを確認する。推測では、自己履歴 join の重複、`select *` staging、Parquet 再カウントが主要候補である。

期待効果: 直接の高速化ではないが、優先順位を確定できる。

## 推奨する実施順

1. `select *` を廃止し、PostgreSQL からの取得列を絞る。
2. `rec` / `target` に `race_dt` など前処理済み列を追加する。
3. 馬の過去走 join を `horse_history_base` として共通化する。
4. Parquet 出力後の再カウントを `--skip-count` 可能にする。
5. profiling を取り、次に重い CTE に絞って追加改善する。
