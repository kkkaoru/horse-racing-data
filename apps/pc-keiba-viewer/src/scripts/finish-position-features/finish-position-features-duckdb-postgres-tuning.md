# finish_position_features_duckdb.py PostgreSQL/DuckDB 接続まわりの追加高速化案

対象: `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`

このメモは、`finish-position-features-duckdb-performance.md` に書いた改善案と重複しない、PostgreSQL 設定・PostgreSQL 側データ整備・DuckDB PostgreSQL extension 接続まわりの追加案をまとめる。特徴量 SQL 自体の書き換えよりも、`source.stage` の待ち時間や PostgreSQL から DuckDB への staging を短くするための案である。

## 1. DuckDB PostgreSQL extension の filter pushdown 設定を明示する

DuckDB の PostgreSQL extension には、PostgreSQL 側へ filter を押し込む設定がある。現状の staging SQL は `race_date between ...` などの条件を持つため、この pushdown が無効または期待どおり効いていないと、DuckDB 側へ余計な行が流れる可能性がある。

確認・検討する設定:

- `pg_experimental_filter_pushdown`
- 使用中 DuckDB バージョンでの PostgreSQL extension のデフォルト値
- `ATTACH` 時に指定できる scan/connection option

期待効果: 中。PostgreSQL から DuckDB への転送行数が減る場合に効く。

注意点: experimental 扱いの設定は DuckDB バージョン差が出やすいため、結果件数の一致確認をセットにする。

## 2. PostgreSQL 側で `ANALYZE` を定期実行する

PostgreSQL 側の統計が古いと、DuckDB から発行される単純な filter query でも不利な実行計画になる可能性がある。特に、大量投入後や `race_entry_corner_features` の再構築後は `ANALYZE` を明示的に実行する。

対象候補:

- `race_entry_corner_features`
- `jvd_se`
- `nvd_se`
- `jvd_ra`
- `nvd_ra`
- `jvd_um`
- `nvd_um`

実行例:

```sql
analyze race_entry_corner_features;
analyze jvd_se;
analyze nvd_se;
analyze jvd_ra;
analyze nvd_ra;
```

期待効果: 小から中。PostgreSQL 側 staging が遅い場合の前提整備として有効。

## 3. `effective_cache_size` を実メモリに合わせる

`effective_cache_size` はメモリを確保する設定ではなく、PostgreSQL planner への cache 見積もりである。値が小さすぎると、index scan より sequential scan を選びやすくなるケースがある。

確認すること:

```sql
show effective_cache_size;
```

期待効果: 小から中。PostgreSQL 側で date/source filter に index を使わせたい場合に効く可能性がある。

## 4. SSD/NVMe 前提なら `random_page_cost` と `effective_io_concurrency` を見直す

PostgreSQL が SSD/NVMe 上で動いているのに HDD 寄りの cost 設定のままだと、planner が index scan を過小評価する場合がある。

確認すること:

```sql
show random_page_cost;
show effective_io_concurrency;
```

期待効果: 小から中。staging query が index を使うべき条件なのに sequential scan を選ぶ場合に検討する。

注意点: 全 DB ワークロードに影響するため、`EXPLAIN (ANALYZE, BUFFERS)` で staging query の計画を見てから変更する。

## 5. 開催日文字列結合 filter 用の式 index または生成列を検討する

`jvd_se`, `nvd_se`, `jvd_ra`, `nvd_ra` は `(kaisai_nen || kaisai_tsukihi) between ...` で絞っている。この式に通常の複合 index が効かない場合、式 index または生成列を用意すると staging が軽くなる。

案:

```sql
create index concurrently if not exists jvd_se_kaisai_date_expr_idx
  on jvd_se ((kaisai_nen || kaisai_tsukihi));

create index concurrently if not exists nvd_se_kaisai_date_expr_idx
  on nvd_se ((kaisai_nen || kaisai_tsukihi));
```

より堅い案として、`kaisai_date` のような生成列を追加して、その列に index を張る。

期待効果: 中。SE/RA テーブルの対象期間抽出が遅い場合に効く。

## 6. `source.stage` の内訳をさらに分けて計測する

既存ログでは `source.stage` 全体の時間は分かるが、どの PostgreSQL テーブル取り込みが遅いかは見えにくい。各 temp table 作成ごとにログを出すと、PostgreSQL 設定や index の効果を判断しやすくなる。

分けたい単位:

- `rec`
- `jra_se`
- `nar_se`
- `jra_um`
- `nar_um`
- `jra_ra`
- `nar_ra`

期待効果: 直接の高速化ではないが、PostgreSQL 側の改善対象を特定できる。

## 推奨する確認順

1. `source.stage` の内訳ログを追加し、PostgreSQL 由来の遅さが全体の何割か確認する。
2. `ANALYZE` と PostgreSQL planner 設定の現状を確認する。
3. DuckDB PostgreSQL extension の filter pushdown が有効か確認する。
4. SE/RA テーブルの `(kaisai_nen || kaisai_tsukihi)` filter に index が効くか `EXPLAIN` で確認する。
