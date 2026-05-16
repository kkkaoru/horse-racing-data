# finish_position_features_duckdb.py 追加高速化指示

対象: `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`

このファイルは、直近レビューで残った速度改善案のうち、ステージごとの `count(*)` ログ任意化を除いた追加指示です。

## 1. 血統用 `rec_um` を一度だけ materialize する

現在の血統統計生成で、各血統種別ごとに `rec_um` 相当の抽出・正規化が再利用されず、同じ重い入力を複数回読む可能性があります。

対応方針:

- `target_months` 作成後、血統統計専用の一時テーブルを作る。
- 一時テーブル名の候補は `pedigree_rec_um`。
- 列は血統集計に必要なものだけに絞る。
  - `source`
  - `race_date`
  - `race_year_month`
  - `ketto_toroku_bango`
  - `kyori`
  - `track_code`
  - `finish_position`
  - `finish_norm`
  - `keibajo_code`
  - `ketto_joho_01b`
  - `ketto_joho_05b`
- 既存の `pedigree_rec_um_subquery` と同じフィルタ条件を使い、挙動を変えない。
- `PEDIGREE_STAT_SPECS` 側のSQLは、毎回サブクエリを埋め込むのではなく `pedigree_rec_um` を参照する形へ寄せる。

期待効果:

- 血統4系統の月次集計で、同じ `rec_um` 抽出を繰り返すコストを減らせる。
- 実装リスクが比較的低く、最初に入れる価値が高い。

## 2. `jra_se` / `nar_se` / `jra_ra` / `nar_ra` に DuckDB 側 index を張る

特徴量生成中の結合で `se` と `ra` のキー参照が多いため、ステージング後にDuckDB内の一時テーブルへindexを張る余地があります。

候補:

```sql
CREATE INDEX IF NOT EXISTS idx_jra_se_race_entry
ON jra_se (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango);

CREATE INDEX IF NOT EXISTS idx_nar_se_race_entry
ON nar_se (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango);

CREATE INDEX IF NOT EXISTS idx_jra_ra_race
ON jra_ra (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango);

CREATE INDEX IF NOT EXISTS idx_nar_ra_race
ON nar_ra (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango);
```

注意点:

- index作成にも時間がかかるため、必ず実測で判断する。
- 小さい期間では遅くなる可能性がある。
- まずはオプション化せず、ローカル実測で明確に効く場合だけ固定導入する。

## 3. 血統累積統計を ASOF/cumulative 方式にする

月次粒度の血統統計は精度面では良い一方、`target_months` と過去月次統計を結合して集計すると、対象月数に比例して中間行が増えます。

血統処理がまだボトルネックの場合の改善案:

- 月次統計を先に作る。
- 血統キーごとに月次累積値を作る。
- targetの `race_year_month` に対して、その月より前の最新累積行を参照する。
- DuckDBで可能なら `ASOF JOIN`、難しければ window関数で累積テーブルを作ってから通常joinする。

期待効果:

- `target_months × monthly_stats` の中間行増加を避けられる。
- 10年分など対象月が多いケースで効きやすい。

注意点:

- SQLの構造変更が大きいため、最初の改善ではなく、profileで血統が残ボトルネックと分かった場合に実施する。
- `race_year_month` の境界条件が変わらないよう、現行の「対象月より前だけを使う」条件を維持する。

## 4. 騎手・調教師履歴の年別処理を再評価する

騎手・調教師の履歴特徴量は、年別処理によりメモリ使用量を抑えられる一方、年ごとに入力を再走査している場合は総実行時間が伸びる可能性があります。

対応方針:

- 内部実装またはCLIで `yearly` と `all` の2モードを比較できるようにする。
- 例: `--partner-build-mode yearly|all`
- このMac上で、同じ期間・同じ入力に対して以下を比較する。
  - 総実行時間
  - 最大メモリ使用量
  - DuckDB spillの有無
  - 出力行数と主要特徴量の一致

判断基準:

- メモリに収まるなら `all` が速い可能性がある。
- メモリやspillが厳しいなら `yearly` を維持する。
- 実装変更前に、現行の年別処理が本当に再走査コストを持っているか確認する。

## 5. DuckDB temp directory を明示する

DuckDBが大きな中間結果をspillする場合、temp directoryの場所で実行時間が変わります。

対応方針:

- CLIオプションとして `--temp-dir` を追加する。
- 指定された場合だけ、接続後に以下を設定する。

```sql
SET temp_directory = '/path/to/duckdb-temp';
```

- ディレクトリがなければ作成する。
- デフォルトでリポジトリ直下などへ大きな一時ファイルを作らない。

運用例:

- Macの内蔵SSD上の十分空き容量がある場所を指定する。
- 例: `/tmp/finish-position-duckdb`

期待効果:

- spillが発生する大規模実行で安定しやすい。
- 外部ディスクや遅い場所へ一時ファイルが出る事故を避けられる。

## 6. PostgreSQL staging の内訳ログを細分化する

PostgreSQLからDuckDBへ取り込むステージング処理のどこが遅いかを把握するため、入力別に時間を出す。

対象候補:

- `rec`
- `jra_se`
- `nar_se`
- `jra_um`
- `nar_um`
- `jra_ra`
- `nar_ra`

対応方針:

- `stage_source_tables` または `stage_source` の中で、各テーブル作成前後の経過時間をログ出力する。
- ログだけで出力データやSQLの意味は変えない。
- PostgreSQL側が遅いのか、DuckDB側の変換が遅いのかを切り分けられる粒度にする。

期待効果:

- 次の高速化対象を実測で選べる。
- PostgreSQLの設定、ネットワーク、DuckDB変換のどれを見るべきか判断しやすくなる。

## 推奨実装順

1. 血統用 `pedigree_rec_um` の一時テーブル化。
2. PostgreSQL staging の内訳ログ追加。
3. DuckDB `--temp-dir` オプション追加。
4. `se` / `ra` indexを実測し、効果がある場合だけ採用。
5. 騎手・調教師履歴の `yearly` / `all` 比較。
6. 血統がまだ重い場合のみ、ASOF/cumulative方式へ変更。
