# PC-KEIBA PostgreSQL Table Reference

MotherDuck agent skill などから参照しやすいように、`PC-KEIBAテーブル定義書.xlsx` の定義とローカル PostgreSQL の実 schema を統合した参照データです。実レコード由来の名前や値は公開用に含めません。

## Source

- Generated at: 2026-05-06 00:42:59 JST
- Database: `horse_racing` on local PostgreSQL 18
- Excel definition: `apps/local-postgresql/docs/PC-KEIBAテーブル定義書.xlsx`
- Scope: `public` schema base tables present in PostgreSQL

## Overview

- PostgreSQL tables: 94
- Tables with records: 48
- Empty tables: 46
- Excel table definitions parsed: 93
- Present in PostgreSQL but missing in Excel definition: `apd_tohyo_python`

## MotherDuck Agent Check

The MotherDuck MCP server can read the local PostgreSQL database through DuckDB's `postgres` extension:

```sql
INSTALL postgres;
LOAD postgres;
ATTACH 'host=127.0.0.1 port=5432 dbname=horse_racing user=<user> password=<password>' AS pg (TYPE postgres);
SELECT count(*) AS nvd_se_rows FROM pg.public.nvd_se;
```

Verified result: query succeeded; row counts are intentionally omitted from this public reference.

## Table Catalog

| table              | logical name                 |     rows | columns |       size | primary key                                                                                                                                  |
| ------------------ | ---------------------------- | -------: | ------: | ---------: | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `apd_kaime_log_1`  | 買い目ログ1                  |        0 |      13 | 8192 bytes |                                                                                                                                              |
| `apd_kaime_log_2`  | 買い目ログ2                  |        0 |      15 | 8192 bytes |                                                                                                                                              |
| `apd_kaime_sim_1`  | 買い目シミュレーション1      |        0 |      13 | 8192 bytes |                                                                                                                                              |
| `apd_kaime_sim_2`  | 買い目シミュレーション2      |        0 |      15 | 8192 bytes |                                                                                                                                              |
| `apd_kaime_tmp_1`  | 買い目一時表1                |        0 |      13 | 8192 bytes |                                                                                                                                              |
| `apd_kaime_tmp_2`  | 買い目一時表2                |        0 |      15 | 8192 bytes |                                                                                                                                              |
| `apd_se_jv`        | 馬毎払戻過去走               |        0 |      40 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban                                                                                 |
| `apd_se_nv`        | 馬毎払戻過去走               |        0 |      40 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban                                                                                 |
| `apd_shisu_1`      | 指数1                        |        0 |       3 | 8192 bytes | shisu_no                                                                                                                                     |
| `apd_shisu_2`      | 指数2                        |        0 |       9 | 8192 bytes | shisu_no, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban                                                                       |
| `apd_shisu_3`      | 指数3                        |        0 |       3 | 8192 bytes | column_no                                                                                                                                    |
| `apd_sokuho_o1`    | 速報系データ オッズ1(単複枠) |        0 |      22 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun                                                                   |
| `apd_sokuho_o2`    | 速報系データ オッズ2(馬連)   |        0 |      15 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun                                                                   |
| `apd_sokuho_o3`    | 速報系データ オッズ3(ワイド) |        0 |      15 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun                                                                   |
| `apd_sokuho_o4`    | 速報系データ オッズ4(馬単)   |        0 |      15 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun                                                                   |
| `apd_sokuho_o5`    | 速報系データ オッズ5(3連複)  |        0 |      15 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun                                                                   |
| `apd_sokuho_o6`    | 速報系データ オッズ6(3連単)  |        0 |      15 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun                                                                   |
| `apd_sokuho_oa`    | 速報系データ オッズA(枠単)   |        0 |      15 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun                                                                   |
| `apd_sokuho_ra`    | 速報系データ レース詳細      |        0 |      14 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `apd_sokuho_se`    | 速報系データ 馬毎レース情報  |        0 |      30 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban                                                                                 |
| `apd_tohyo_python` |                              |        0 |       7 | 8192 bytes | toroku_no                                                                                                                                    |
| `apd_tohyo_race`   | 投票レース                   |        0 |       6 |      16 kB |                                                                                                                                              |
| `apd_tohyo_settei` | 投票設定                     |        0 |      33 | 8192 bytes | tohyo_no                                                                                                                                     |
| `jvd_av`           | 出走取消･競走除外            |        0 |      13 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban                                                                                 |
| `jvd_bn`           | 馬主マスタ                   |     8682 |      11 |    6528 kB | banushi_code                                                                                                                                 |
| `jvd_br`           | 生産者マスタ                 |    10722 |      11 |    8344 kB | seisansha_code                                                                                                                               |
| `jvd_bt`           | 系統情報                     |       92 |       7 |     216 kB | hanshoku_toroku_bango                                                                                                                        |
| `jvd_cc`           | コース変更                   |        0 |      15 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_ch`           | 調教師マスタ                 |     1474 |      21 |    2512 kB | chokyoshi_code                                                                                                                               |
| `jvd_ck`           | 出走別着度数                 |        0 |     106 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango                                                                     |
| `jvd_cs`           | コース情報                   |      119 |       8 |     248 kB | keibajo_code, kyori, track_code, course_kaishu_nengappi                                                                                      |
| `jvd_dm`           | タイム型データマイニング予想 |    83847 |      28 |      34 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_h1`           | 票数1                        |        0 |      43 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_h6`           | 票数6(3連単)                 |        0 |      16 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_hc`           | 坂路調教                     | 11752406 |      14 |    2251 MB | tracen_kubun, chokyo_nengappi, chokyo_jikoku, ketto_toroku_bango                                                                             |
| `jvd_hn`           | 繁殖馬マスタ                 |   161273 |      19 |      63 MB | hanshoku_toroku_bango                                                                                                                        |
| `jvd_hr`           | 払戻                         |   138297 |     158 |     140 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_hs`           | 競走馬市場取引価格           |    52900 |      14 |      20 MB | ketto_toroku_bango, shusaisha_shijo_code, kaisai_kikan_min                                                                                   |
| `jvd_hy`           | 馬名の意味由来情報           |   174042 |       6 |      46 MB | ketto_toroku_bango                                                                                                                           |
| `jvd_jc`           | 騎手変更                     |        0 |      20 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun, umaban                                                           |
| `jvd_jg`           | 競走馬除外情報               |   817276 |      14 |     181 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango, shutsuba_tohyo_uketsuke                                            |
| `jvd_ks`           | 騎手マスタ                   |     1559 |      30 |    3136 kB | kishu_code                                                                                                                                   |
| `jvd_o1`           | オッズ1(単複枠)              |   113692 |      22 |     115 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_o2`           | オッズ2(馬連)                |   111029 |      15 |     131 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_o3`           | オッズ3(ワイド)              |    90790 |      15 |     134 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_o4`           | オッズ4(馬単)                |    82449 |      15 |     182 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_o5`           | オッズ5(3連複)               |    82485 |      15 |     380 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_o6`           | オッズ6(3連単)               |    66009 |      15 |    1520 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_ra`           | レース詳細                   |   237713 |      62 |     386 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_rc`           | レコードマスタ               |     2125 |      24 |    1888 kB | record_shikibetsu_kubun, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, tokubetsu_kyoso_bango, kyoso_shubetsu_code, kyori, track_code |
| `jvd_se`           | 馬毎レース情報               |  2851051 |      70 |    2798 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, ketto_toroku_bango                                                             |
| `jvd_sk`           | 産駒マスタ                   |        0 |      26 |      56 kB | ketto_toroku_bango                                                                                                                           |
| `jvd_tc`           | 発走時刻変更                 |        0 |      12 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_tk`           | 特別登録馬                   |      171 |     336 |    2016 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_tm`           | 対戦型データマイニング予想   |    53430 |      28 |      13 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_um`           | 競走馬マスタ                 |   212353 |      89 |     466 MB | ketto_toroku_bango                                                                                                                           |
| `jvd_wc`           | ウッドチップ調教             |   725838 |      29 |     189 MB | tracen_kubun, chokyo_nengappi, chokyo_jikoku, ketto_toroku_bango                                                                             |
| `jvd_we`           | 天候馬場状態                 |        0 |      16 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, happyo_tsukihi_jifun, henko_shikibetsu                                                             |
| `jvd_wf`           | 重勝式(WIN5)                 |      861 |     266 |      27 MB | kaisai_nen, kaisai_tsukihi                                                                                                                   |
| `jvd_wh`           | 馬体重                       |        0 |      28 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `jvd_ys`           | 開催スケジュール             |     7817 |      12 |    4936 kB | kaisai_nen, kaisai_tsukihi, keibajo_code                                                                                                     |
| `nvd_av`           | 出走取消･競走除外            |        0 |      13 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban                                                                                 |
| `nvd_bn`           | 馬主マスタ                   |     9485 |      11 |      13 MB | banushi_code, banushimei                                                                                                                     |
| `nvd_cd`           | コード変更情報               |        0 |       7 | 8192 bytes | henkotaisho_id, code_new                                                                                                                     |
| `nvd_ch`           | 調教師マスタ                 |     1384 |      21 |    3256 kB | chokyoshi_code                                                                                                                               |
| `nvd_ck`           | 出走別着度数                 |        0 |     106 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango                                                                     |
| `nvd_h1`           | 票数1                        |        0 |      43 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_h6`           | 票数6(3連単)                 |        0 |      16 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_ha`           | 票数A(枠単)                  |        0 |      17 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_hr`           | 払戻                         |   324651 |     159 |     346 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_jc`           | 騎手変更                     |        0 |      20 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun, umaban                                                           |
| `nvd_ks`           | 騎手マスタ                   |     1871 |      30 |    6088 kB | kishu_code                                                                                                                                   |
| `nvd_nb`           | 生産者マスタ地方             |     5542 |      11 |    9480 kB | seisansha_code, seisanshamei                                                                                                                 |
| `nvd_nc`           | 調教師マスタ地方             |     1809 |      21 |    4728 kB | chokyoshi_code                                                                                                                               |
| `nvd_nd`           | 出走別着度数地方             |        0 |     110 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango                                                                     |
| `nvd_nk`           | 騎手マスタ地方               |     2355 |      32 |    9536 kB | kishu_code                                                                                                                                   |
| `nvd_nn`           | 馬主マスタ地方               |     9485 |      13 |      20 MB | banushi_code, banushimei                                                                                                                     |
| `nvd_nr`           | 能力試験詳細                 |     3576 |      31 |    1952 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_ns`           | 馬毎能力試験情報             |    17264 |      47 |    8856 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban                                                                                 |
| `nvd_nu`           | 競走馬マスタ地方             |   120159 |      81 |     496 MB | ketto_toroku_bango                                                                                                                           |
| `nvd_o1`           | オッズ1(単複枠)              |   290592 |      22 |     303 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_o2`           | オッズ2(馬連)                |   280497 |      15 |     166 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_o3`           | オッズ3(ワイド)              |   243289 |      15 |     175 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_o4`           | オッズ4(馬単)                |   290592 |      15 |     263 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_o5`           | オッズ5(3連複)               |   239829 |      15 |     400 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_o6`           | オッズ6(3連単)               |   240005 |      15 |    2036 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_oa`           | オッズA(枠単)                |    81319 |      15 |      60 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_ra`           | レース詳細                   |   326807 |      62 |     523 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_se`           | 馬毎レース情報               |  1878251 |      70 |    1753 MB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango                                                                     |
| `nvd_tc`           | 発走時刻変更                 |        0 |      12 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |
| `nvd_um`           | 競走馬マスタ                 |        0 |      89 |      64 kB | ketto_toroku_bango                                                                                                                           |
| `nvd_we`           | 天候馬場状態                 |        0 |      16 | 8192 bytes | kaisai_nen, kaisai_tsukihi, keibajo_code, happyo_tsukihi_jifun, henko_shikibetsu                                                             |
| `nvd_wf`           | 重勝式(WIN5)                 |        0 |     267 |      16 kB | kaisai_nen, kaisai_tsukihi                                                                                                                   |
| `nvd_wh`           | 馬体重                       |        0 |      28 |      16 kB | kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango                                                                                         |

## Table Details

### `apd_kaime_log_1`

- Logical name: 買い目ログ1
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「買い目ログ1」。
- Rows: 0
- Columns: 13
- Total size: 8192 bytes
- Primary key: none
- Excel indexes: apd_kaime_log_1_idx1(tohyo_no,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column             | logical name | db type                       | Excel type                       | nullable | key | reference / note                                                    |
| ------------------ | ------------ | ----------------------------- | -------------------------------- | -------- | --- | ------------------------------------------------------------------- |
| `update_timestamp` | 更新日時     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                                                     |
| `data_kubun`       | データ区分   | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:テスト                                                   |
| `tohyo_no`         | 投票No       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                                                     |
| `kaisai_nen`       | 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     | yyyy形式で設定                                                      |
| `kaisai_tsukihi`   | 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     | mmdd形式で設定                                                      |
| `keibajo_code`     | 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                                        |
| `race_bango`       | レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                                                     |
| `hasso_jikoku`     | 発走時刻     | `character varying(4)`        | `character varying(4)`           | YES      |     | HHmm形式で設定                                                      |
| `shikibetsu_kubun` | 式別区分     | `character varying(1)`        | `character varying(1)`           | YES      |     | 1:単勝 2:複勝 3:枠連 4:馬連 5:ワイド 6:馬単 7:3連複 8:3連単 9:枠単  |
| `hoshiki_kubun`    | 方式区分     | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:通常 1:ボックス 2:フォーメーション 3:流し 4:軸1頭流し 5:軸2頭流し |
| `multi_flag`       | マルチフラグ | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:マルチ                                                   |
| `gosei_odds`       | 合成オッズ   | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `zandaka`          | 残高         | `numeric(19,0)`               | `numeric(19)`                    | YES      |     |                                                                     |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_kaime_log_2`

- Logical name: 買い目ログ2
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「買い目ログ2」。
- Rows: 0
- Columns: 15
- Total size: 8192 bytes
- Primary key: none
- Excel indexes: apd_kaime_log_2_idx1(tohyo_no,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column             | logical name | db type                       | Excel type                       | nullable | key | reference / note                     |
| ------------------ | ------------ | ----------------------------- | -------------------------------- | -------- | --- | ------------------------------------ |
| `update_timestamp` | 更新日時     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                      |
| `data_kubun`       | データ区分   | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:テスト 9:該当レコード削除 |
| `tohyo_no`         | 投票No       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                      |
| `kaisai_nen`       | 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     | yyyy形式で設定                       |
| `kaisai_tsukihi`   | 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     | mmdd形式で設定                       |
| `keibajo_code`     | 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照         |
| `race_bango`       | レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                      |
| `kumiban_1`        | 組番1        | `character varying(36)`       | `character varying(36)`          | YES      |     |                                      |
| `kumiban_2`        | 組番2        | `character varying(36)`       | `character varying(36)`          | YES      |     |                                      |
| `kumiban_3`        | 組番3        | `character varying(36)`       | `character varying(36)`          | YES      |     |                                      |
| `odds`             | オッズ       | `numeric(7,1)`                | `numeric(7, 1)`                  | YES      |     |                                      |
| `ninkijun`         | 人気順       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                      |
| `tohyo_maisu`      | 投票枚数     | `numeric(8,0)`                | `numeric(8)`                     | YES      |     |                                      |
| `tekichu_flag`     | 的中フラグ   | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:的中                      |
| `haraimodoshi`     | 払戻         | `numeric(19,0)`               | `numeric(19)`                    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_kaime_sim_1`

- Logical name: 買い目シミュレーション1
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「買い目シミュレーション1」。
- Rows: 0
- Columns: 13
- Total size: 8192 bytes
- Primary key: none
- Excel indexes: apd_kaime_sim_1_idx1(tohyo_no,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column             | logical name | db type                       | Excel type                       | nullable | key | reference / note                                                    |
| ------------------ | ------------ | ----------------------------- | -------------------------------- | -------- | --- | ------------------------------------------------------------------- |
| `update_timestamp` | 更新日時     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                                                     |
| `data_kubun`       | データ区分   | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:テスト                                                   |
| `tohyo_no`         | 投票No       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                                                     |
| `kaisai_nen`       | 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     | yyyy形式で設定                                                      |
| `kaisai_tsukihi`   | 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     | mmdd形式で設定                                                      |
| `keibajo_code`     | 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                                        |
| `race_bango`       | レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                                                     |
| `hasso_jikoku`     | 発走時刻     | `character varying(4)`        | `character varying(4)`           | YES      |     | HHmm形式で設定                                                      |
| `shikibetsu_kubun` | 式別区分     | `character varying(1)`        | `character varying(1)`           | YES      |     | 1:単勝 2:複勝 3:枠連 4:馬連 5:ワイド 6:馬単 7:3連複 8:3連単 9:枠単  |
| `hoshiki_kubun`    | 方式区分     | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:通常 1:ボックス 2:フォーメーション 3:流し 4:軸1頭流し 5:軸2頭流し |
| `multi_flag`       | マルチフラグ | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:マルチ                                                   |
| `gosei_odds`       | 合成オッズ   | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `zandaka`          | 残高         | `numeric(19,0)`               | `numeric(19)`                    | YES      |     |                                                                     |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_kaime_sim_2`

- Logical name: 買い目シミュレーション2
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「買い目シミュレーション2」。
- Rows: 0
- Columns: 15
- Total size: 8192 bytes
- Primary key: none
- Excel indexes: apd_kaime_sim_2_idx1(tohyo_no,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column             | logical name | db type                       | Excel type                       | nullable | key | reference / note             |
| ------------------ | ------------ | ----------------------------- | -------------------------------- | -------- | --- | ---------------------------- |
| `update_timestamp` | 更新日時     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                              |
| `data_kubun`       | データ区分   | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:テスト            |
| `tohyo_no`         | 投票No       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                              |
| `kaisai_nen`       | 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     | yyyy形式で設定               |
| `kaisai_tsukihi`   | 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     | mmdd形式で設定               |
| `keibajo_code`     | 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照 |
| `race_bango`       | レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                              |
| `kumiban_1`        | 組番1        | `character varying(36)`       | `character varying(36)`          | YES      |     |                              |
| `kumiban_2`        | 組番2        | `character varying(36)`       | `character varying(36)`          | YES      |     |                              |
| `kumiban_3`        | 組番3        | `character varying(36)`       | `character varying(36)`          | YES      |     |                              |
| `odds`             | オッズ       | `numeric(7,1)`                | `numeric(7, 1)`                  | YES      |     |                              |
| `ninkijun`         | 人気順       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                              |
| `tohyo_maisu`      | 投票枚数     | `numeric(8,0)`                | `numeric(8)`                     | YES      |     |                              |
| `tekichu_flag`     | 的中フラグ   | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:的中              |
| `haraimodoshi`     | 払戻         | `numeric(19,0)`               | `numeric(19)`                    | YES      |     |                              |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_kaime_tmp_1`

- Logical name: 買い目一時表1
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「買い目一時表1」。
- Rows: 0
- Columns: 13
- Total size: 8192 bytes
- Primary key: none
- Excel indexes: apd_kaime_tmp_1_idx1(tohyo_no,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column             | logical name | db type                       | Excel type                       | nullable | key | reference / note                                                    |
| ------------------ | ------------ | ----------------------------- | -------------------------------- | -------- | --- | ------------------------------------------------------------------- |
| `update_timestamp` | 更新日時     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                                                     |
| `data_kubun`       | データ区分   | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:テスト                                                   |
| `tohyo_no`         | 投票No       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                                                     |
| `kaisai_nen`       | 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     | yyyy形式で設定                                                      |
| `kaisai_tsukihi`   | 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     | mmdd形式で設定                                                      |
| `keibajo_code`     | 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                                        |
| `race_bango`       | レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                                                     |
| `hasso_jikoku`     | 発走時刻     | `character varying(4)`        | `character varying(4)`           | YES      |     | HHmm形式で設定                                                      |
| `shikibetsu_kubun` | 式別区分     | `character varying(1)`        | `character varying(1)`           | YES      |     | 1:単勝 2:複勝 3:枠連 4:馬連 5:ワイド 6:馬単 7:3連複 8:3連単 9:枠単  |
| `hoshiki_kubun`    | 方式区分     | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:通常 1:ボックス 2:フォーメーション 3:流し 4:軸1頭流し 5:軸2頭流し |
| `multi_flag`       | マルチフラグ | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:マルチ                                                   |
| `gosei_odds`       | 合成オッズ   | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `zandaka`          | 残高         | `numeric(19,0)`               | `numeric(19)`                    | YES      |     |                                                                     |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_kaime_tmp_2`

- Logical name: 買い目一時表2
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「買い目一時表2」。
- Rows: 0
- Columns: 15
- Total size: 8192 bytes
- Primary key: none
- Excel indexes: apd_kaime_tmp_2_idx1(tohyo_no,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column             | logical name | db type                       | Excel type                       | nullable | key | reference / note                     |
| ------------------ | ------------ | ----------------------------- | -------------------------------- | -------- | --- | ------------------------------------ |
| `update_timestamp` | 更新日時     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                      |
| `data_kubun`       | データ区分   | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:テスト 9:該当レコード削除 |
| `tohyo_no`         | 投票No       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                      |
| `kaisai_nen`       | 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     | yyyy形式で設定                       |
| `kaisai_tsukihi`   | 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     | mmdd形式で設定                       |
| `keibajo_code`     | 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照         |
| `race_bango`       | レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                      |
| `kumiban_1`        | 組番1        | `character varying(36)`       | `character varying(36)`          | YES      |     |                                      |
| `kumiban_2`        | 組番2        | `character varying(36)`       | `character varying(36)`          | YES      |     |                                      |
| `kumiban_3`        | 組番3        | `character varying(36)`       | `character varying(36)`          | YES      |     |                                      |
| `odds`             | オッズ       | `numeric(7,1)`                | `numeric(7, 1)`                  | YES      |     |                                      |
| `ninkijun`         | 人気順       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                      |
| `tohyo_maisu`      | 投票枚数     | `numeric(8,0)`                | `numeric(8)`                     | YES      |     |                                      |
| `tekichu_flag`     | 的中フラグ   | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:初期値 1:的中                      |
| `haraimodoshi`     | 払戻         | `numeric(19,0)`               | `numeric(19)`                    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_se_jv`

- Logical name: 馬毎払戻過去走
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「馬毎払戻過去走」。
- Rows: 0
- Columns: 40
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban`
- Excel indexes: apd_se_jv_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban), apd_se_jv_idx1(expr)

| column                     | logical name       | db type                       | Excel type                       | nullable | key | reference / note                            |
| -------------------------- | ------------------ | ----------------------------- | -------------------------------- | -------- | --- | ------------------------------------------- |
| `update_timestamp`         | 更新日時           | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                             |
| `kaisai_nen`               | 開催年             | `character varying(4)`        | `character varying(4)`           | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_tsukihi`           | 開催月日           | `character varying(4)`        | `character varying(4)`           | NO       | PK  | レースを一意に識別するための複合キー        |
| `keibajo_code`             | 競馬場コード       | `character varying(2)`        | `character varying(2)`           | NO       | PK  | レースを一意に識別するための複合キー        |
| `race_bango`               | レース番号         | `character varying(2)`        | `character varying(2)`           | NO       | PK  | レースを一意に識別するための複合キー        |
| `umaban`                   | 馬番               | `character varying(2)`        | `character varying(2)`           | NO       | PK  |                                             |
| `ketto_toroku_bango`       | 血統登録番号       | `character varying(10)`       | `character varying(10)`          | YES      |     | 競走馬マスタ.血統登録番号への外部キーリンク |
| `chakukaisu_1`             | 1着回数            | `numeric(1,0)`                | `numeric(1)`                     | YES      |     |                                             |
| `chakukaisu_2`             | 2着回数            | `numeric(1,0)`                | `numeric(1)`                     | YES      |     |                                             |
| `chakukaisu_3`             | 3着回数            | `numeric(1,0)`                | `numeric(1)`                     | YES      |     |                                             |
| `haraimodoshi_tansho`      | 単勝払戻           | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_fukusho`     | 複勝払戻           | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_umaren`      | 馬連払戻           | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_wide`        | ワイド払戻         | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_umatan_1`    | 馬単払戻1着        | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_umatan_2`    | 馬単払戻2着        | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_sanrenpuku`  | 3連複払戻          | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_sanrentan_1` | 3連単払戻1着       | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_sanrentan_2` | 3連単払戻2着       | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_sanrentan_3` | 3連単払戻3着       | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `kako1_kaisai_nen`         | 過去1 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako1_kaisai_tsukihi`     | 過去1 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako1_keibajo_code`       | 過去1 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                |
| `kako1_race_bango`         | 過去1 レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                             |
| `kako2_kaisai_nen`         | 過去2 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako2_kaisai_tsukihi`     | 過去2 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako2_keibajo_code`       | 過去2 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                |
| `kako2_race_bango`         | 過去2 レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                             |
| `kako3_kaisai_nen`         | 過去3 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako3_kaisai_tsukihi`     | 過去3 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako3_keibajo_code`       | 過去3 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                |
| `kako3_race_bango`         | 過去3 レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                             |
| `kako4_kaisai_nen`         | 過去4 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako4_kaisai_tsukihi`     | 過去4 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako4_keibajo_code`       | 過去4 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                |
| `kako4_race_bango`         | 過去4 レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                             |
| `kako5_kaisai_nen`         | 過去5 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako5_kaisai_tsukihi`     | 過去5 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako5_keibajo_code`       | 過去5 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                |
| `kako5_race_bango`         | 過去5 レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_se_nv`

- Logical name: 馬毎払戻過去走
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「馬毎払戻過去走」。
- Rows: 0
- Columns: 40
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban`
- Excel indexes: apd_se_nv_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban), apd_se_nv_idx1(expr)

| column                     | logical name       | db type                       | Excel type                       | nullable | key | reference / note                            |
| -------------------------- | ------------------ | ----------------------------- | -------------------------------- | -------- | --- | ------------------------------------------- |
| `update_timestamp`         | 更新日時           | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                             |
| `kaisai_nen`               | 開催年             | `character varying(4)`        | `character varying(4)`           | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_tsukihi`           | 開催月日           | `character varying(4)`        | `character varying(4)`           | NO       | PK  | レースを一意に識別するための複合キー        |
| `keibajo_code`             | 競馬場コード       | `character varying(2)`        | `character varying(2)`           | NO       | PK  | レースを一意に識別するための複合キー        |
| `race_bango`               | レース番号         | `character varying(2)`        | `character varying(2)`           | NO       | PK  | レースを一意に識別するための複合キー        |
| `umaban`                   | 馬番               | `character varying(2)`        | `character varying(2)`           | NO       | PK  |                                             |
| `ketto_toroku_bango`       | 血統登録番号       | `character varying(10)`       | `character varying(10)`          | YES      |     | 競走馬マスタ.血統登録番号への外部キーリンク |
| `chakukaisu_1`             | 1着回数            | `numeric(1,0)`                | `numeric(1)`                     | YES      |     |                                             |
| `chakukaisu_2`             | 2着回数            | `numeric(1,0)`                | `numeric(1)`                     | YES      |     |                                             |
| `chakukaisu_3`             | 3着回数            | `numeric(1,0)`                | `numeric(1)`                     | YES      |     |                                             |
| `haraimodoshi_tansho`      | 単勝払戻           | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_fukusho`     | 複勝払戻           | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_umaren`      | 馬連払戻           | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_wide`        | ワイド払戻         | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_umatan_1`    | 馬単払戻1着        | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_umatan_2`    | 馬単払戻2着        | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_sanrenpuku`  | 3連複払戻          | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_sanrentan_1` | 3連単払戻1着       | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_sanrentan_2` | 3連単払戻2着       | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `haraimodoshi_sanrentan_3` | 3連単払戻3着       | `numeric(9,0)`                | `numeric(9)`                     | YES      |     |                                             |
| `kako1_kaisai_nen`         | 過去1 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako1_kaisai_tsukihi`     | 過去1 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako1_keibajo_code`       | 過去1 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                |
| `kako1_race_bango`         | 過去1 レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                             |
| `kako2_kaisai_nen`         | 過去2 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako2_kaisai_tsukihi`     | 過去2 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako2_keibajo_code`       | 過去2 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                |
| `kako2_race_bango`         | 過去2 レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                             |
| `kako3_kaisai_nen`         | 過去3 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako3_kaisai_tsukihi`     | 過去3 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako3_keibajo_code`       | 過去3 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                |
| `kako3_race_bango`         | 過去3 レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                             |
| `kako4_kaisai_nen`         | 過去4 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako4_kaisai_tsukihi`     | 過去4 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako4_keibajo_code`       | 過去4 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                |
| `kako4_race_bango`         | 過去4 レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                             |
| `kako5_kaisai_nen`         | 過去5 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako5_kaisai_tsukihi`     | 過去5 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                                             |
| `kako5_keibajo_code`       | 過去5 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照                |
| `kako5_race_bango`         | 過去5 レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_shisu_1`

- Logical name: 指数1
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「指数1」。
- Rows: 0
- Columns: 3
- Total size: 8192 bytes
- Primary key: `shisu_no`
- Excel indexes: apd_shisu_1_pk(shisu_no)

| column             | logical name | db type                       | Excel type                       | nullable | key | reference / note |
| ------------------ | ------------ | ----------------------------- | -------------------------------- | -------- | --- | ---------------- |
| `update_timestamp` | 更新日時     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                  |
| `shisu_no`         | 指数No       | `numeric(4,0)`                | `numeric(4)`                     | NO       | PK  |                  |
| `shisu_name`       | 指数名       | `character varying(30)`       | `character varying(30)`          | YES      |     |                  |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_shisu_2`

- Logical name: 指数2
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「指数2」。
- Rows: 0
- Columns: 9
- Total size: 8192 bytes
- Primary key: `shisu_no, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban`
- Excel indexes: apd_shisu_2_pk(shisu_no,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban)

| column             | logical name | db type                       | Excel type                       | nullable | key | reference / note                     |
| ------------------ | ------------ | ----------------------------- | -------------------------------- | -------- | --- | ------------------------------------ |
| `update_timestamp` | 更新日時     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                      |
| `shisu_no`         | 指数No       | `numeric(4,0)`                | `numeric(4)`                     | NO       | PK  |                                      |
| `kaisai_nen`       | 開催年       | `character varying(4)`        | `character varying(4)`           | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`   | 開催月日     | `character varying(4)`        | `character varying(4)`           | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`     | 競馬場コード | `character varying(2)`        | `character varying(2)`           | NO       | PK  | レースを一意に識別するための複合キー |
| `race_bango`       | レース番号   | `character varying(2)`        | `character varying(2)`           | NO       | PK  | レースを一意に識別するための複合キー |
| `umaban`           | 馬番         | `character varying(2)`        | `character varying(2)`           | NO       | PK  |                                      |
| `shisu`            | 指数         | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                      |
| `juni`             | 順位         | `numeric(2,0)`                | `numeric(2)`                     | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_shisu_3`

- Logical name: 指数3
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「指数3」。
- Rows: 0
- Columns: 3
- Total size: 8192 bytes
- Primary key: `column_no`
- Excel indexes: apd_shisu_3_pk(column_no)

| column             | logical name | db type                       | Excel type                       | nullable | key | reference / note |
| ------------------ | ------------ | ----------------------------- | -------------------------------- | -------- | --- | ---------------- |
| `update_timestamp` | 更新日時     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                  |
| `column_no`        | 列No         | `numeric(2,0)`                | `numeric(2)`                     | NO       | PK  |                  |
| `shisu_no`         | 指数No       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                  |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_sokuho_o1`

- Logical name: 速報系データ オッズ1(単複枠)
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「速報系データ オッズ1(単複枠)」。
- Rows: 0
- Columns: 22
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun`
- Excel indexes: apd_sokuho_o1_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,happyo_tsukihi_jifun)

| column                   | logical name     | db type                  | Excel type               | nullable | key | reference / note                     |
| ------------------------ | ---------------- | ------------------------ | ------------------------ | -------- | --- | ------------------------------------ |
| `record_id`              | レコード種別ID   | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `data_kubun`             | データ区分       | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `data_sakusei_nengappi`  | データ作成年月日 | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`             | 開催年           | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`         | 開催月日         | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`           | 競馬場コード     | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`             | 開催回[第N回]    | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `kaisai_nichime`         | 開催日目[N日目]  | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `race_bango`             | レース番号       | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`   | 発表月日時分     | `character varying(8)`   | `character varying(8)`   | NO       | PK  | mmddHHmm形式で設定                   |
| `toroku_tosu`            | 登録頭数         | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `shusso_tosu`            | 出走頭数         | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `hatsubai_flag_tansho`   | 発売フラグ　単勝 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `hatsubai_flag_fukusho`  | 発売フラグ　複勝 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `hatsubai_flag_wakuren`  | 発売フラグ　枠連 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `fukusho_chakubarai_key` | 複勝着払キー     | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `odds_tansho`            | 単勝オッズ       | `character varying(224)` | `character varying(224)` | YES      |     |                                      |
| `odds_fukusho`           | 複勝オッズ       | `character varying(336)` | `character varying(336)` | YES      |     |                                      |
| `odds_wakuren`           | 枠連オッズ       | `character varying(324)` | `character varying(324)` | YES      |     |                                      |
| `hyosu_gokei_tansho`     | 単勝票数合計     | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |
| `hyosu_gokei_fukusho`    | 複勝票数合計     | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |
| `hyosu_gokei_wakuren`    | 枠連票数合計     | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_sokuho_o2`

- Logical name: 速報系データ オッズ2(馬連)
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「速報系データ オッズ2(馬連)」。
- Rows: 0
- Columns: 15
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun`
- Excel indexes: apd_sokuho_o2_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,happyo_tsukihi_jifun)

| column                  | logical name     | db type                   | Excel type                | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ------------------------- | ------------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`    | `character varying(8)`    | NO       | PK  | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `hatsubai_flag_umaren`  | 発売フラグ　馬連 | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `odds_umaren`           | 馬連オッズ       | `character varying(1989)` | `character varying(1989)` | YES      |     |                                      |
| `hyosu_gokei_umaren`    | 馬連票数合計     | `character varying(11)`   | `character varying(11)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_sokuho_o3`

- Logical name: 速報系データ オッズ3(ワイド)
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「速報系データ オッズ3(ワイド)」。
- Rows: 0
- Columns: 15
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun`
- Excel indexes: apd_sokuho_o3_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,happyo_tsukihi_jifun)

| column                  | logical name       | db type                   | Excel type                | nullable | key | reference / note                     |
| ----------------------- | ------------------ | ------------------------- | ------------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID     | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `data_kubun`            | データ区分         | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日   | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年             | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日           | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード       | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]      | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]    | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `race_bango`            | レース番号         | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分       | `character varying(8)`    | `character varying(8)`    | NO       | PK  | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数           | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数           | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `hatsubai_flag_wide`    | 発売フラグ　ワイド | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `odds_wide`             | ワイドオッズ       | `character varying(2601)` | `character varying(2601)` | YES      |     |                                      |
| `hyosu_gokei_wide`      | ワイド票数合計     | `character varying(11)`   | `character varying(11)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_sokuho_o4`

- Logical name: 速報系データ オッズ4(馬単)
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「速報系データ オッズ4(馬単)」。
- Rows: 0
- Columns: 15
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun`
- Excel indexes: apd_sokuho_o4_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,happyo_tsukihi_jifun)

| column                  | logical name     | db type                   | Excel type                | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ------------------------- | ------------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`    | `character varying(8)`    | NO       | PK  | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `hatsubai_flag_umatan`  | 発売フラグ　馬単 | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `odds_umatan`           | 馬単オッズ       | `character varying(3978)` | `character varying(3978)` | YES      |     |                                      |
| `hyosu_gokei_umatan`    | 馬単票数合計     | `character varying(11)`   | `character varying(11)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_sokuho_o5`

- Logical name: 速報系データ オッズ5(3連複)
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「速報系データ オッズ5(3連複)」。
- Rows: 0
- Columns: 15
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun`
- Excel indexes: apd_sokuho_o5_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,happyo_tsukihi_jifun)

| column                     | logical name      | db type                    | Excel type                 | nullable | key | reference / note                     |
| -------------------------- | ----------------- | -------------------------- | -------------------------- | -------- | --- | ------------------------------------ |
| `record_id`                | レコード種別ID    | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `data_kubun`               | データ区分        | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `data_sakusei_nengappi`    | データ作成年月日  | `character varying(8)`     | `character varying(8)`     | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`               | 開催年            | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`           | 開催月日          | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`             | 競馬場コード      | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`               | 開催回[第N回]     | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `kaisai_nichime`           | 開催日目[N日目]   | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `race_bango`               | レース番号        | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`     | 発表月日時分      | `character varying(8)`     | `character varying(8)`     | NO       | PK  | mmddHHmm形式で設定                   |
| `toroku_tosu`              | 登録頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `shusso_tosu`              | 出走頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `hatsubai_flag_sanrenpuku` | 発売フラグ　3連複 | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `odds_sanrenpuku`          | 3連複オッズ       | `character varying(12240)` | `character varying(12240)` | YES      |     |                                      |
| `hyosu_gokei_sanrenpuku`   | 3連複票数合計     | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_sokuho_o6`

- Logical name: 速報系データ オッズ6(3連単)
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「速報系データ オッズ6(3連単)」。
- Rows: 0
- Columns: 15
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun`
- Excel indexes: apd_sokuho_o6_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,happyo_tsukihi_jifun)

| column                    | logical name      | db type                    | Excel type                 | nullable | key | reference / note                     |
| ------------------------- | ----------------- | -------------------------- | -------------------------- | -------- | --- | ------------------------------------ |
| `record_id`               | レコード種別ID    | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `data_kubun`              | データ区分        | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `data_sakusei_nengappi`   | データ作成年月日  | `character varying(8)`     | `character varying(8)`     | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`              | 開催年            | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`          | 開催月日          | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`            | 競馬場コード      | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`              | 開催回[第N回]     | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `kaisai_nichime`          | 開催日目[N日目]   | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `race_bango`              | レース番号        | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`    | 発表月日時分      | `character varying(8)`     | `character varying(8)`     | NO       | PK  | mmddHHmm形式で設定                   |
| `toroku_tosu`             | 登録頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `shusso_tosu`             | 出走頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `hatsubai_flag_sanrentan` | 発売フラグ　3連単 | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `odds_sanrentan`          | 3連単オッズ       | `character varying(83232)` | `character varying(83232)` | YES      |     |                                      |
| `hyosu_gokei_sanrentan`   | 3連単票数合計     | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_sokuho_oa`

- Logical name: 速報系データ オッズA(枠単)
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「速報系データ オッズA(枠単)」。
- Rows: 0
- Columns: 15
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun`
- Excel indexes: apd_sokuho_oa_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,happyo_tsukihi_jifun)

| column                  | logical name     | db type                    | Excel type                 | nullable | key | reference / note                     |
| ----------------------- | ---------------- | -------------------------- | -------------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`     | `character varying(8)`     | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`     | `character varying(8)`     | NO       | PK  | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数         | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数         | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `hatsubai_flag_wakutan` | 発売フラグ　枠単 | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `odds_wakutan`          | 枠単オッズ       | `character varying(83232)` | `character varying(83232)` | YES      |     |                                      |
| `hyosu_gokei_wakutan`   | 枠単票数合計     | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_sokuho_ra`

- Logical name: 速報系データ レース詳細
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「速報系データ レース詳細」。
- Rows: 0
- Columns: 14
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: apd_sokuho_ra_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name         | db type                       | Excel type                       | nullable | key | reference / note                     |
| ----------------------- | -------------------- | ----------------------------- | -------------------------------- | -------- | --- | ------------------------------------ |
| `update_timestamp`      | 更新日時             | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                      |
| `kaisai_nen`            | 開催年               | `character varying(4)`        | `character varying(4)`           | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日             | `character varying(4)`        | `character varying(4)`           | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード         | `character varying(2)`        | `character varying(2)`           | NO       | PK  | レースを一意に識別するための複合キー |
| `race_bango`            | レース番号           | `character varying(2)`        | `character varying(2)`           | NO       | PK  | レースを一意に識別するための複合キー |
| `kyori`                 | 距離                 | `character varying(4)`        | `character varying(4)`           | YES      |     |                                      |
| `kyori_henkomae`        | 変更前距離           | `character varying(4)`        | `character varying(4)`           | YES      |     |                                      |
| `track_code`            | トラックコード       | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.トラックコード を参照       |
| `track_code_henkomae`   | 変更前トラックコード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.トラックコード を参照       |
| `hasso_jikoku`          | 発走時刻             | `character varying(4)`        | `character varying(4)`           | YES      |     | HHmm形式で設定                       |
| `hasso_jikoku_henkomae` | 変更前発走時刻       | `character varying(4)`        | `character varying(4)`           | YES      |     | HHmm形式で設定                       |
| `tenko_code`            | 天候コード           | `character varying(1)`        | `character varying(1)`           | YES      |     | コード表.天候コード を参照           |
| `babajotai_code_shiba`  | 芝馬場状態コード     | `character varying(1)`        | `character varying(1)`           | YES      |     | コード表.馬場状態コード を参照       |
| `babajotai_code_dirt`   | ダート馬場状態コード | `character varying(1)`        | `character varying(1)`           | YES      |     | コード表.馬場状態コード を参照       |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_sokuho_se`

- Logical name: 速報系データ 馬毎レース情報
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「速報系データ 馬毎レース情報」。
- Rows: 0
- Columns: 30
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban`
- Excel indexes: apd_sokuho_se_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban)

| column                        | logical name                 | db type                       | Excel type                       | nullable | key | reference / note                        |
| ----------------------------- | ---------------------------- | ----------------------------- | -------------------------------- | -------- | --- | --------------------------------------- |
| `update_timestamp`            | 更新日時                     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                         |
| `kaisai_nen`                  | 開催年                       | `character varying(4)`        | `character varying(4)`           | NO       | PK  | レースを一意に識別するための複合キー    |
| `kaisai_tsukihi`              | 開催月日                     | `character varying(4)`        | `character varying(4)`           | NO       | PK  | レースを一意に識別するための複合キー    |
| `keibajo_code`                | 競馬場コード                 | `character varying(2)`        | `character varying(2)`           | NO       | PK  | レースを一意に識別するための複合キー    |
| `race_bango`                  | レース番号                   | `character varying(2)`        | `character varying(2)`           | NO       | PK  | レースを一意に識別するための複合キー    |
| `umaban`                      | 馬番                         | `character varying(2)`        | `character varying(2)`           | NO       | PK  |                                         |
| `futan_juryo`                 | 負担重量                     | `character varying(3)`        | `character varying(3)`           | YES      |     |                                         |
| `futan_juryo_henkomae`        | 変更前負担重量               | `character varying(3)`        | `character varying(3)`           | YES      |     |                                         |
| `kishu_code`                  | 騎手コード                   | `character varying(5)`        | `character varying(5)`           | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク |
| `kishu_code_henkomae`         | 変更前騎手コード             | `character varying(5)`        | `character varying(5)`           | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク |
| `kishumei_ryakusho`           | 騎手名略称                   | `character varying(8)`        | `character varying(8)`           | YES      |     |                                         |
| `kishumei_ryakusho_henkomae`  | 変更前騎手名略称             | `character varying(8)`        | `character varying(8)`           | YES      |     |                                         |
| `kishu_minarai_code`          | 騎手見習コード               | `character varying(1)`        | `character varying(1)`           | YES      |     | コード表.騎手見習コード を参照          |
| `kishu_minarai_code_henkomae` | 変更前騎手見習コード         | `character varying(1)`        | `character varying(1)`           | YES      |     | コード表.騎手見習コード を参照          |
| `bataiju`                     | 馬体重                       | `character varying(3)`        | `character varying(3)`           | YES      |     |                                         |
| `zogen_fugo`                  | 増減符号                     | `character varying(1)`        | `character varying(1)`           | YES      |     |                                         |
| `zogen_sa`                    | 増減差                       | `character varying(3)`        | `character varying(3)`           | YES      |     |                                         |
| `jiyu_kubun`                  | 事由区分                     | `character varying(3)`        | `character varying(3)`           | YES      |     |                                         |
| `tansho_odds`                 | 単勝オッズ                   | `character varying(4)`        | `character varying(4)`           | YES      |     |                                         |
| `tansho_ninkijun`             | 単勝人気順                   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                         |
| `fukusho_odds_min`            | 複勝最低オッズ               | `character varying(4)`        | `character varying(4)`           | YES      |     |                                         |
| `fukusho_odds_max`            | 複勝最高オッズ               | `character varying(4)`        | `character varying(4)`           | YES      |     |                                         |
| `fukusho_ninkijun`            | 複勝人気順                   | `character varying(2)`        | `character varying(2)`           | YES      |     |                                         |
| `mining_kubun`                | マイニング区分               | `character varying(1)`        | `character varying(1)`           | YES      |     |                                         |
| `yoso_soha_time`              | マイニング予想走破タイム     | `character varying(5)`        | `character varying(5)`           | YES      |     | 9分99.99秒で設定                        |
| `yoso_gosa_plus`              | マイニング予想誤差(信頼度)＋ | `character varying(4)`        | `character varying(4)`           | YES      |     | 99.99秒で設定                           |
| `yoso_gosa_minus`             | マイニング予想誤差(信頼度)－ | `character varying(4)`        | `character varying(4)`           | YES      |     | 99.99秒で設定                           |
| `yoso_juni`                   | マイニング予想順位           | `character varying(2)`        | `character varying(2)`           | YES      |     |                                         |
| `yosoku_score`                | 予測スコア                   | `character varying(4)`        | `character varying(4)`           | YES      |     |                                         |
| `yosoku_juni`                 | 予測スコア順位               | `character varying(2)`        | `character varying(2)`           | YES      |     |                                         |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_tohyo_python`

- Logical name:
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「」。
- Rows: 0
- Columns: 7
- Total size: 8192 bytes
- Primary key: `toroku_no`

| column                    | logical name | db type                       | Excel type | nullable | key | reference / note |
| ------------------------- | ------------ | ----------------------------- | ---------- | -------- | --- | ---------------- |
| `update_timestamp`        |              | `timestamp without time zone` | ``         | YES      |     |                  |
| `toroku_no`               |              | `numeric(4,0)`                | ``         | NO       | PK  |                  |
| `objective`               |              | `character varying(1)`        | ``         | YES      |     |                  |
| `class_su`                |              | `numeric(1,0)`                | ``         | YES      |     |                  |
| `train_table`             |              | `character varying(63)`       | ``         | YES      |     |                  |
| `pred_table`              |              | `character varying(63)`       | ``         | YES      |     |                  |
| `python_source_code_path` |              | `character varying(260)`      | ``         | YES      |     |                  |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_tohyo_race`

- Logical name: 投票レース
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「投票レース」。
- Rows: 0
- Columns: 6
- Total size: 16 kB
- Primary key: none
- Excel indexes: apd_tohyo_race_idx1(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,tohyo_no), apd_tohyo_race_idx2(expr)

| column             | logical name | db type                       | Excel type                       | nullable | key | reference / note             |
| ------------------ | ------------ | ----------------------------- | -------------------------------- | -------- | --- | ---------------------------- |
| `update_timestamp` | 更新日時     | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                              |
| `kaisai_nen`       | 開催年       | `character varying(4)`        | `character varying(4)`           | YES      |     |                              |
| `kaisai_tsukihi`   | 開催月日     | `character varying(4)`        | `character varying(4)`           | YES      |     |                              |
| `keibajo_code`     | 競馬場コード | `character varying(2)`        | `character varying(2)`           | YES      |     | コード表.競馬場コード を参照 |
| `race_bango`       | レース番号   | `character varying(2)`        | `character varying(2)`           | YES      |     |                              |
| `tohyo_no`         | 投票No       | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                              |

Sample records: omitted from public reference; do not publish actual record values.

### `apd_tohyo_settei`

- Logical name: 投票設定
- Purpose: PC-KEIBA アプリケーション/速報/投票/指数系の補助テーブル。 論理名は「投票設定」。
- Rows: 0
- Columns: 33
- Total size: 8192 bytes
- Primary key: `tohyo_no`
- Excel indexes: apd_tohyo_settei_pk(tohyo_no)

| column                | logical name   | db type                       | Excel type                       | nullable | key | reference / note                                                    |
| --------------------- | -------------- | ----------------------------- | -------------------------------- | -------- | --- | ------------------------------------------------------------------- |
| `update_timestamp`    | 更新日時       | `timestamp without time zone` | `timestamp(6) without time zone` | YES      |     |                                                                     |
| `tohyo_no`            | 投票No         | `numeric(4,0)`                | `numeric(4)`                     | NO       | PK  |                                                                     |
| `tohyo_name`          | 投票名         | `character varying(50)`       | `character varying(50)`          | YES      |     |                                                                     |
| `shikibetsu_kubun`    | 式別区分       | `character varying(1)`        | `character varying(1)`           | YES      |     | 1:単勝 2:複勝 3:枠連 4:馬連 5:ワイド 6:馬単 7:3連複 8:3連単 9:枠単  |
| `hoshiki_kubun`       | 方式区分       | `character varying(1)`        | `character varying(1)`           | YES      |     | 0:通常 1:ボックス 2:フォーメーション 3:流し 4:軸1頭流し 5:軸2頭流し |
| `shisu_no_1`          | 指数No1        | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                                                     |
| `shisu_min_1`         | 最低指数1      | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `shisu_max_1`         | 最高指数1      | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `juni_min_1`          | 最低順位1      | `numeric(2,0)`                | `numeric(2)`                     | YES      |     |                                                                     |
| `juni_max_1`          | 最高順位1      | `numeric(2,0)`                | `numeric(2)`                     | YES      |     |                                                                     |
| `shisu_no_2`          | 指数No2        | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                                                     |
| `shisu_min_2`         | 最低指数2      | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `shisu_max_2`         | 最高指数2      | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `juni_min_2`          | 最低順位2      | `numeric(2,0)`                | `numeric(2)`                     | YES      |     |                                                                     |
| `juni_max_2`          | 最高順位2      | `numeric(2,0)`                | `numeric(2)`                     | YES      |     |                                                                     |
| `shisu_no_3`          | 指数No3        | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                                                     |
| `shisu_min_3`         | 最低指数3      | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `shisu_max_3`         | 最高指数3      | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `juni_min_3`          | 最低順位3      | `numeric(2,0)`                | `numeric(2)`                     | YES      |     |                                                                     |
| `juni_max_3`          | 最高順位3      | `numeric(2,0)`                | `numeric(2)`                     | YES      |     |                                                                     |
| `shisu_gokei_min`     | 最低指数合計   | `numeric(9,2)`                | `numeric(9, 2)`                  | YES      |     |                                                                     |
| `shisu_gokei_max`     | 最高指数合計   | `numeric(9,2)`                | `numeric(9, 2)`                  | YES      |     |                                                                     |
| `odds_min`            | 最低オッズ     | `numeric(7,1)`                | `numeric(7, 1)`                  | YES      |     |                                                                     |
| `odds_max`            | 最高オッズ     | `numeric(7,1)`                | `numeric(7, 1)`                  | YES      |     |                                                                     |
| `ninkijun_min`        | 最低人気順     | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                                                     |
| `ninkijun_max`        | 最高人気順     | `numeric(4,0)`                | `numeric(4)`                     | YES      |     |                                                                     |
| `shikin_haibun_kubun` | 資金配分区分   | `character varying(1)`        | `character varying(1)`           | YES      |     | 1:均等買い 2:合成オッズ                                             |
| `gosei_odds_min`      | 最低合成オッズ | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `gosei_odds_max`      | 最高合成オッズ | `numeric(8,2)`                | `numeric(8, 2)`                  | YES      |     |                                                                     |
| `kingaku_kubun`       | 金額区分       | `character varying(1)`        | `character varying(1)`           | YES      |     | 1:金額 2:残高                                                       |
| `kingaku`             | 金額           | `numeric(8,0)`                | `numeric(8)`                     | YES      |     |                                                                     |
| `zandaka`             | 残高(％)       | `numeric(5,2)`                | `numeric(5, 2)`                  | YES      |     |                                                                     |
| `check_box1`          | 選択           | `character varying(1)`        | `character varying(1)`           | YES      |     |                                                                     |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_av`

- Logical name: 出走取消･競走除外
- Purpose: JRA 系データ。 論理名は「出走取消･競走除外」。
- Rows: 0
- Columns: 13
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban`
- Excel indexes: jvd_av_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban)

| column                  | logical name     | db type                 | Excel type              | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`  | `character varying(8)`  | YES      |     | mmddHHmm形式で設定                   |
| `umaban`                | 馬番             | `character varying(2)`  | `character varying(2)`  | NO       | PK  |                                      |
| `bamei`                 | 馬名             | `character varying(36)` | `character varying(36)` | YES      |     |                                      |
| `jiyu_kubun`            | 事由区分         | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_bn`

- Logical name: 馬主マスタ
- Purpose: JRA 系データ。 論理名は「馬主マスタ」。
- Rows: 8682
- Columns: 11
- Total size: 6528 kB
- Primary key: `banushi_code`
- Excel indexes: jvd_bn_pk(banushi_code)

| column                    | logical name       | db type                  | Excel type               | nullable | key | reference / note                 |
| ------------------------- | ------------------ | ------------------------ | ------------------------ | -------- | --- | -------------------------------- |
| `record_id`               | レコード種別ID     | `character varying(2)`   | `character varying(2)`   | YES      |     |                                  |
| `data_kubun`              | データ区分         | `character varying(1)`   | `character varying(1)`   | YES      |     |                                  |
| `data_sakusei_nengappi`   | データ作成年月日   | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定               |
| `banushi_code`            | 馬主コード         | `character varying(6)`   | `character varying(6)`   | NO       | PK  | 馬主を一意に識別するためのコード |
| `banushimei_hojinkaku`    | 馬主名(法人格有)   | `character varying(64)`  | `character varying(64)`  | YES      |     |                                  |
| `banushimei`              | 馬主名(法人格無)   | `character varying(64)`  | `character varying(64)`  | YES      |     |                                  |
| `banushimei_hankaku_kana` | 馬主名半角ｶﾅ       | `character varying(50)`  | `character varying(50)`  | YES      |     |                                  |
| `banushimei_eur`          | 馬主名欧字         | `character varying(100)` | `character varying(100)` | YES      |     |                                  |
| `fukushoku_hyoji`         | 服色標示           | `character varying(60)`  | `character varying(60)`  | YES      |     |                                  |
| `seiseki_joho_1`          | 本年･累計成績情報1 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                  |
| `seiseki_joho_2`          | 本年･累計成績情報2 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                  |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_br`

- Logical name: 生産者マスタ
- Purpose: JRA 系データ。 論理名は「生産者マスタ」。
- Rows: 10722
- Columns: 11
- Total size: 8344 kB
- Primary key: `seisansha_code`
- Excel indexes: jvd_br_pk(seisansha_code)

| column                        | logical name       | db type                  | Excel type               | nullable | key | reference / note                   |
| ----------------------------- | ------------------ | ------------------------ | ------------------------ | -------- | --- | ---------------------------------- |
| `record_id`                   | レコード種別ID     | `character varying(2)`   | `character varying(2)`   | YES      |     |                                    |
| `data_kubun`                  | データ区分         | `character varying(1)`   | `character varying(1)`   | YES      |     |                                    |
| `data_sakusei_nengappi`       | データ作成年月日   | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                 |
| `seisansha_code`              | 生産者コード       | `character varying(8)`   | `character varying(8)`   | NO       | PK  | 生産者を一意に識別するためのコード |
| `seisanshamei_hojinkaku`      | 生産者名(法人格有) | `character varying(72)`  | `character varying(72)`  | YES      |     |                                    |
| `seisanshamei`                | 生産者名(法人格無) | `character varying(72)`  | `character varying(72)`  | YES      |     |                                    |
| `seisanshamei_hankaku_kana`   | 生産者名半角ｶﾅ     | `character varying(72)`  | `character varying(72)`  | YES      |     |                                    |
| `seisanshamei_eur`            | 生産者名欧字       | `character varying(168)` | `character varying(168)` | YES      |     |                                    |
| `seisansha_jusho_jichishomei` | 生産者住所自治省名 | `character varying(20)`  | `character varying(20)`  | YES      |     |                                    |
| `seiseki_joho_1`              | 本年･累計成績情報1 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                    |
| `seiseki_joho_2`              | 本年･累計成績情報2 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                    |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_bt`

- Logical name: 系統情報
- Purpose: JRA 系データ。 論理名は「系統情報」。
- Rows: 92
- Columns: 7
- Total size: 216 kB
- Primary key: `hanshoku_toroku_bango`
- Excel indexes: jvd_bt_pk(hanshoku_toroku_bango)

| column                  | logical name     | db type                   | Excel type                | nullable | key | reference / note                 |
| ----------------------- | ---------------- | ------------------------- | ------------------------- | -------- | --- | -------------------------------- |
| `record_id`             | レコード種別ID   | `character varying(2)`    | `character varying(2)`    | YES      |     |                                  |
| `data_kubun`            | データ区分       | `character varying(1)`    | `character varying(1)`    | YES      |     |                                  |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定               |
| `hanshoku_toroku_bango` | 繁殖登録番号     | `character varying(10)`   | `character varying(10)`   | NO       | PK  | 繁殖馬を一意に識別するための番号 |
| `keito_id`              | 系統ID           | `character varying(30)`   | `character varying(30)`   | YES      |     |                                  |
| `keito_mei`             | 系統名           | `character varying(36)`   | `character varying(36)`   | YES      |     |                                  |
| `keito_setsumei`        | 系統説明         | `character varying(6800)` | `character varying(6800)` | YES      |     |                                  |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_cc`

- Logical name: コース変更
- Purpose: JRA 系データ。 論理名は「コース変更」。
- Rows: 0
- Columns: 15
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: jvd_cc_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name          | db type                | Excel type             | nullable | key | reference / note                     |
| ----------------------- | --------------------- | ---------------------- | ---------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID        | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `data_kubun`            | データ区分            | `character varying(1)` | `character varying(1)` | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日      | `character varying(8)` | `character varying(8)` | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年                | `character varying(4)` | `character varying(4)` | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日              | `character varying(4)` | `character varying(4)` | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード          | `character varying(2)` | `character varying(2)` | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]         | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]       | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `race_bango`            | レース番号            | `character varying(2)` | `character varying(2)` | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分          | `character varying(8)` | `character varying(8)` | YES      |     | mmddHHmm形式で設定                   |
| `kyori`                 | 変更後 距離           | `character varying(4)` | `character varying(4)` | YES      |     |                                      |
| `track_code`            | 変更後 トラックコード | `character varying(2)` | `character varying(2)` | YES      |     | コード表.トラックコード を参照       |
| `kyori_henkomae`        | 変更前 距離           | `character varying(4)` | `character varying(4)` | YES      |     |                                      |
| `track_code_henkomae`   | 変更前 トラックコード | `character varying(2)` | `character varying(2)` | YES      |     | コード表.トラックコード を参照       |
| `jiyu_kubun`            | 事由区分              | `character varying(1)` | `character varying(1)` | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_ch`

- Logical name: 調教師マスタ
- Purpose: JRA 系データ。 論理名は「調教師マスタ」。
- Rows: 1474
- Columns: 21
- Total size: 2512 kB
- Primary key: `chokyoshi_code`
- Excel indexes: jvd_ch_pk(chokyoshi_code)

| column                      | logical name            | db type                   | Excel type                | nullable | key | reference / note                   |
| --------------------------- | ----------------------- | ------------------------- | ------------------------- | -------- | --- | ---------------------------------- |
| `record_id`                 | レコード種別ID          | `character varying(2)`    | `character varying(2)`    | YES      |     |                                    |
| `data_kubun`                | データ区分              | `character varying(1)`    | `character varying(1)`    | YES      |     |                                    |
| `data_sakusei_nengappi`     | データ作成年月日        | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `chokyoshi_code`            | 調教師コード            | `character varying(5)`    | `character varying(5)`    | NO       | PK  | 調教師を一意に識別するためのコード |
| `massho_kubun`              | 調教師抹消区分          | `character varying(1)`    | `character varying(1)`    | YES      |     |                                    |
| `menkyo_kofu_nengappi`      | 調教師免許交付年月日    | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `menkyo_massho_nengappi`    | 調教師免許抹消年月日    | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `seinengappi`               | 生年月日                | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `chokyoshimei`              | 調教師名                | `character varying(34)`   | `character varying(34)`   | YES      |     |                                    |
| `chokyoshimei_hankaku_kana` | 調教師名半角ｶﾅ          | `character varying(30)`   | `character varying(30)`   | YES      |     |                                    |
| `chokyoshimei_ryakusho`     | 調教師名略称            | `character varying(8)`    | `character varying(8)`    | YES      |     |                                    |
| `chokyoshimei_eur`          | 調教師名欧字            | `character varying(80)`   | `character varying(80)`   | YES      |     |                                    |
| `seibetsu_kubun`            | 性別区分                | `character varying(1)`    | `character varying(1)`    | YES      |     |                                    |
| `tozai_shozoku_code`        | 調教師東西所属コード    | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.東西所属コード を参照     |
| `shotai_chiikimei`          | 招待地域名              | `character varying(20)`   | `character varying(20)`   | YES      |     |                                    |
| `jushoshori_joho_1`         | 最近重賞勝利情報1       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                    |
| `jushoshori_joho_2`         | 最近重賞勝利情報2       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                    |
| `jushoshori_joho_3`         | 最近重賞勝利情報3       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                    |
| `seiseki_joho_1`            | 本年･前年･累計成績情報1 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                    |
| `seiseki_joho_2`            | 本年･前年･累計成績情報2 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                    |
| `seiseki_joho_3`            | 本年･前年･累計成績情報3 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                    |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_ck`

- Logical name: 出走別着度数
- Purpose: JRA 系データ。 論理名は「出走別着度数」。
- Rows: 0
- Columns: 106
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango`
- Excel indexes: jvd_ck_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango)

| column                        | logical name             | db type                   | Excel type                | nullable | key | reference / note                            |
| ----------------------------- | ------------------------ | ------------------------- | ------------------------- | -------- | --- | ------------------------------------------- |
| `record_id`                   | レコード種別ID           | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `data_kubun`                  | データ区分               | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `data_sakusei_nengappi`       | データ作成年月日         | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `kaisai_nen`                  | 開催年                   | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_tsukihi`              | 開催月日                 | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `keibajo_code`                | 競馬場コード             | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_kai`                  | 開催回[第N回]            | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `kaisai_nichime`              | 開催日目[N日目]          | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `race_bango`                  | レース番号               | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `ketto_toroku_bango`          | 血統登録番号             | `character varying(10)`   | `character varying(10)`   | NO       | PK  | 競走馬マスタ.血統登録番号への外部キーリンク |
| `bamei`                       | 馬名                     | `character varying(36)`   | `character varying(36)`   | YES      |     |                                             |
| `heichi_honshokin_ruikei`     | 平地本賞金累計           | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `shogai_honshokin_ruikei`     | 障害本賞金累計           | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `heichi_fukashokin_ruikei`    | 平地付加賞金累計         | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `shogai_fukashokin_ruikei`    | 障害付加賞金累計         | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `heichi_shutokushokin_ruikei` | 平地収得賞金累計         | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `shogai_shutokushokin_ruikei` | 障害収得賞金累計         | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `sogo`                        | 総合着回数               | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `chuo_gokei`                  | 中央合計着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_choku`                 | 芝直・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_migi`                  | 芝右・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_hidari`                | 芝左・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_choku`                  | ダ直・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_migi`                   | ダ右・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_hidari`                 | ダ左・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai`                      | 障害・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_ryo`                   | 芝良・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_yayaomo`               | 芝稍・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_omo`                   | 芝重・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_furyo`                 | 芝不・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_ryo`                    | ダ良・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_yayaomo`                | ダ稍・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_omo`                    | ダ重・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_furyo`                  | ダ不・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_ryo`                  | 障良・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_yayaomo`              | 障稍・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_omo`                  | 障重・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_furyo`                | 障不・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1200_ika`              | 芝1200以下・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1201_1400`             | 芝1201-1400・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1401_1600`             | 芝1401-1600・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1601_1800`             | 芝1601-1800・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1801_2000`             | 芝1801-2000・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_2001_2200`             | 芝2001-2200・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_2201_2400`             | 芝2201-2400・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_2401_2800`             | 芝2401-2800・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_2801_ijo`              | 芝2801以上・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1200_ika`               | ダ1200以下・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1201_1400`              | ダ1201-1400・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1401_1600`              | ダ1401-1600・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1601_1800`              | ダ1601-1800・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1801_2000`              | ダ1801-2000・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_2001_2200`              | ダ2001-2200・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_2201_2400`              | ダ2201-2400・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_2401_2800`              | ダ2401-2800・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_2801_ijo`               | ダ2801以上・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_sapporo`               | 札幌芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_hakodate`              | 函館芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_fukushima`             | 福島芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_niigata`               | 新潟芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_tokyo`                 | 東京芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_nakayama`              | 中山芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_chukyo`                | 中京芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_kyoto`                 | 京都芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_hanshin`               | 阪神芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_kokura`                | 小倉芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_sapporo`                | 札幌ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_hakodate`               | 函館ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_fukushima`              | 福島ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_niigata`                | 新潟ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_tokyo`                  | 東京ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_nakayama`               | 中山ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_chukyo`                 | 中京ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kyoto`                  | 京都ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_hanshin`                | 阪神ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kokura`                 | 小倉ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_sapporo`              | 札幌障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_hakodate`             | 函館障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_fukushima`            | 福島障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_niigata`              | 新潟障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_tokyo`                | 東京障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_nakayama`             | 中山障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_chukyo`               | 中京障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_kyoto`                | 京都障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_hanshin`              | 阪神障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_kokura`               | 小倉障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `kyakushitsu_keiko`           | 脚質傾向                 | `character varying(12)`   | `character varying(12)`   | YES      |     |                                             |
| `toroku_race_su`              | 登録レース数             | `character varying(3)`    | `character varying(3)`    | YES      |     |                                             |
| `kishu_code`                  | 騎手コード               | `character varying(5)`    | `character varying(5)`    | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク     |
| `kishumei`                    | 騎手名                   | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `seiseki_joho_kishu_1`        | 騎手本年･累計成績情報1   | `character varying(1220)` | `character varying(1220)` | YES      |     |                                             |
| `seiseki_joho_kishu_2`        | 騎手本年･累計成績情報2   | `character varying(1220)` | `character varying(1220)` | YES      |     |                                             |
| `chokyoshi_code`              | 調教師コード             | `character varying(5)`    | `character varying(5)`    | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei`                | 調教師名                 | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `seiseki_joho_chokyoshi_1`    | 調教師本年･累計成績情報1 | `character varying(1220)` | `character varying(1220)` | YES      |     |                                             |
| `seiseki_joho_chokyoshi_2`    | 調教師本年･累計成績情報2 | `character varying(1220)` | `character varying(1220)` | YES      |     |                                             |
| `banushi_code`                | 馬主コード               | `character varying(6)`    | `character varying(6)`    | YES      |     | 馬主マスタ.馬主コードへの外部キーリンク     |
| `banushimei_hojinkaku`        | 馬主名(法人格有)         | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `banushimei`                  | 馬主名(法人格無)         | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `seiseki_joho_banushi_1`      | 馬主本年･累計成績情報1   | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `seiseki_joho_banushi_2`      | 馬主本年･累計成績情報2   | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `seisansha_code`              | 生産者コード             | `character varying(8)`    | `character varying(8)`    | YES      |     | 生産者マスタ.生産者コードへの外部キーリンク |
| `seisanshamei_hojinkaku`      | 生産者名(法人格有)       | `character varying(72)`   | `character varying(72)`   | YES      |     |                                             |
| `seisanshamei`                | 生産者名(法人格無)       | `character varying(72)`   | `character varying(72)`   | YES      |     |                                             |
| `seiseki_joho_seisansha_1`    | 生産者本年･累計成績情報1 | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `seiseki_joho_seisansha_2`    | 生産者本年･累計成績情報2 | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_cs`

- Logical name: コース情報
- Purpose: JRA 系データ。 論理名は「コース情報」。
- Rows: 119
- Columns: 8
- Total size: 248 kB
- Primary key: `keibajo_code, kyori, track_code, course_kaishu_nengappi`
- Excel indexes: jvd_cs_pk(keibajo_code,kyori,track_code,course_kaishu_nengappi)

| column                   | logical name     | db type                   | Excel type                | nullable | key | reference / note               |
| ------------------------ | ---------------- | ------------------------- | ------------------------- | -------- | --- | ------------------------------ |
| `record_id`              | レコード種別ID   | `character varying(2)`    | `character varying(2)`    | YES      |     |                                |
| `data_kubun`             | データ区分       | `character varying(1)`    | `character varying(1)`    | YES      |     |                                |
| `data_sakusei_nengappi`  | データ作成年月日 | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定             |
| `keibajo_code`           | 競馬場コード     | `character varying(2)`    | `character varying(2)`    | NO       | PK  | コード表.競馬場コード を参照   |
| `kyori`                  | 距離             | `character varying(4)`    | `character varying(4)`    | NO       | PK  |                                |
| `track_code`             | トラックコード   | `character varying(2)`    | `character varying(2)`    | NO       | PK  | コード表.トラックコード を参照 |
| `course_kaishu_nengappi` | コース改修年月日 | `character varying(8)`    | `character varying(8)`    | NO       | PK  | yyyymmdd形式で設定             |
| `course_setsumei`        | コース説明       | `character varying(6800)` | `character varying(6800)` | YES      |     |                                |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_dm`

- Logical name: タイム型データマイニング予想
- Purpose: JRA 系データ。 論理名は「タイム型データマイニング予想」。
- Rows: 83847
- Columns: 28
- Total size: 34 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_dm_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango), jvd_dm_idx1(expr)

| column                  | logical name     | db type                 | Excel type              | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `data_sakusei_jifun`    | データ作成時分   | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `mining_yoso_01`        | マイニング予想1  | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_02`        | マイニング予想2  | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_03`        | マイニング予想3  | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_04`        | マイニング予想4  | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_05`        | マイニング予想5  | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_06`        | マイニング予想6  | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_07`        | マイニング予想7  | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_08`        | マイニング予想8  | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_09`        | マイニング予想9  | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_10`        | マイニング予想10 | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_11`        | マイニング予想11 | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_12`        | マイニング予想12 | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_13`        | マイニング予想13 | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_14`        | マイニング予想14 | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_15`        | マイニング予想15 | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_16`        | マイニング予想16 | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_17`        | マイニング予想17 | `character varying(15)` | `character varying(15)` | YES      |     |                                      |
| `mining_yoso_18`        | マイニング予想18 | `character varying(15)` | `character varying(15)` | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_h1`

- Logical name: 票数1
- Purpose: JRA 系データ。 論理名は「票数1」。
- Rows: 0
- Columns: 43
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: jvd_h1_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                          | logical name             | db type                    | Excel type                 | nullable | key | reference / note                     |
| ------------------------------- | ------------------------ | -------------------------- | -------------------------- | -------- | --- | ------------------------------------ |
| `record_id`                     | レコード種別ID           | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `data_kubun`                    | データ区分               | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `data_sakusei_nengappi`         | データ作成年月日         | `character varying(8)`     | `character varying(8)`     | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`                    | 開催年                   | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`                | 開催月日                 | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`                  | 競馬場コード             | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`                    | 開催回[第N回]            | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `kaisai_nichime`                | 開催日目[N日目]          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `race_bango`                    | レース番号               | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `toroku_tosu`                   | 登録頭数                 | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `shusso_tosu`                   | 出走頭数                 | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `hatsubai_flag_tansho`          | 発売フラグ　単勝         | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_fukusho`         | 発売フラグ　複勝         | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_wakuren`         | 発売フラグ　枠連         | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_umaren`          | 発売フラグ　馬連         | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_wide`            | 発売フラグ　ワイド       | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_umatan`          | 発売フラグ　馬単         | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_sanrenpuku`      | 発売フラグ　3連複        | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `fukusho_chakubarai_key`        | 複勝着払キー             | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `henkan_umaban_joho`            | 返還馬番情報(馬番01～28) | `character varying(28)`    | `character varying(28)`    | YES      |     |                                      |
| `henkan_wakuban_joho`           | 返還枠番情報(枠番1～8)   | `character varying(8)`     | `character varying(8)`     | YES      |     |                                      |
| `henkan_dowaku_joho`            | 返還同枠情報(枠番1～8)   | `character varying(8)`     | `character varying(8)`     | YES      |     |                                      |
| `hyosu_tansho`                  | 単勝票数                 | `character varying(420)`   | `character varying(420)`   | YES      |     |                                      |
| `hyosu_fukusho`                 | 複勝票数                 | `character varying(420)`   | `character varying(420)`   | YES      |     |                                      |
| `hyosu_wakuren`                 | 枠連票数                 | `character varying(540)`   | `character varying(540)`   | YES      |     |                                      |
| `hyosu_umaren`                  | 馬連票数                 | `character varying(2754)`  | `character varying(2754)`  | YES      |     |                                      |
| `hyosu_wide`                    | ワイド票数               | `character varying(2754)`  | `character varying(2754)`  | YES      |     |                                      |
| `hyosu_umatan`                  | 馬単票数                 | `character varying(5508)`  | `character varying(5508)`  | YES      |     |                                      |
| `hyosu_sanrenpuku`              | 3連複票数                | `character varying(16320)` | `character varying(16320)` | YES      |     |                                      |
| `hyosu_gokei_tansho`            | 単勝票数合計             | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_fukusho`           | 複勝票数合計             | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_wakuren`           | 枠連票数合計             | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_umaren`            | 馬連票数合計             | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_wide`              | ワイド票数合計           | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_umatan`            | 馬単票数合計             | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_sanrenpuku`        | 3連複票数合計            | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_tansho`     | 単勝返還票数合計         | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_fukusho`    | 複勝返還票数合計         | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_wakuren`    | 枠連返還票数合計         | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_umaren`     | 馬連返還票数合計         | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_wide`       | ワイド返還票数合計       | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_umatan`     | 馬単返還票数合計         | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_sanrenpuku` | 3連複返還票数合計        | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_h6`

- Logical name: 票数6(3連単)
- Purpose: JRA 系データ。 論理名は「票数6(3連単)」。
- Rows: 0
- Columns: 16
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: jvd_h6_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                         | logical name             | db type                     | Excel type                  | nullable | key | reference / note                     |
| ------------------------------ | ------------------------ | --------------------------- | --------------------------- | -------- | --- | ------------------------------------ |
| `record_id`                    | レコード種別ID           | `character varying(2)`      | `character varying(2)`      | YES      |     |                                      |
| `data_kubun`                   | データ区分               | `character varying(1)`      | `character varying(1)`      | YES      |     |                                      |
| `data_sakusei_nengappi`        | データ作成年月日         | `character varying(8)`      | `character varying(8)`      | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`                   | 開催年                   | `character varying(4)`      | `character varying(4)`      | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`               | 開催月日                 | `character varying(4)`      | `character varying(4)`      | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`                 | 競馬場コード             | `character varying(2)`      | `character varying(2)`      | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`                   | 開催回[第N回]            | `character varying(2)`      | `character varying(2)`      | YES      |     |                                      |
| `kaisai_nichime`               | 開催日目[N日目]          | `character varying(2)`      | `character varying(2)`      | YES      |     |                                      |
| `race_bango`                   | レース番号               | `character varying(2)`      | `character varying(2)`      | NO       | PK  | レースを一意に識別するための複合キー |
| `toroku_tosu`                  | 登録頭数                 | `character varying(2)`      | `character varying(2)`      | YES      |     |                                      |
| `shusso_tosu`                  | 出走頭数                 | `character varying(2)`      | `character varying(2)`      | YES      |     |                                      |
| `hatsubai_flag_sanrentan`      | 発売フラグ　3連単        | `character varying(1)`      | `character varying(1)`      | YES      |     |                                      |
| `henkan_umaban_joho`           | 返還馬番情報(馬番01～18) | `character varying(18)`     | `character varying(18)`     | YES      |     |                                      |
| `hyosu_sanrentan`              | 3連単票数                | `character varying(102816)` | `character varying(102816)` | YES      |     |                                      |
| `hyosu_gokei_sanrentan`        | 3連単票数合計            | `character varying(11)`     | `character varying(11)`     | YES      |     |                                      |
| `henkan_hyosu_gokei_sanrentan` | 3連単返還票数合計        | `character varying(11)`     | `character varying(11)`     | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_hc`

- Logical name: 坂路調教
- Purpose: JRA 系データ。 論理名は「坂路調教」。
- Rows: 11752406
- Columns: 14
- Total size: 2251 MB
- Primary key: `tracen_kubun, chokyo_nengappi, chokyo_jikoku, ketto_toroku_bango`
- Excel indexes: jvd_hc_pk(tracen_kubun,chokyo_nengappi,chokyo_jikoku,ketto_toroku_bango), jvd_hc_idx1(ketto_toroku_bango)

| column                  | logical name                | db type                 | Excel type              | nullable | key | reference / note                            |
| ----------------------- | --------------------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`             | レコード種別ID              | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`            | データ区分                  | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi` | データ作成年月日            | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `tracen_kubun`          | トレセン区分                | `character varying(1)`  | `character varying(1)`  | NO       | PK  |                                             |
| `chokyo_nengappi`       | 調教年月日                  | `character varying(8)`  | `character varying(8)`  | NO       | PK  | yyyymmdd形式で設定                          |
| `chokyo_jikoku`         | 調教時刻                    | `character varying(4)`  | `character varying(4)`  | NO       | PK  | HHmm形式で設定                              |
| `ketto_toroku_bango`    | 血統登録番号                | `character varying(10)` | `character varying(10)` | NO       | PK  | 競走馬マスタ.血統登録番号への外部キーリンク |
| `time_gokei_4f`         | 4ハロンタイム合計(800M～0M) | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_4f`           | ラップタイム(800M～600M)    | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `time_gokei_3f`         | 3ハロンタイム合計(600M～0M) | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_3f`           | ラップタイム(600M～400M)    | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `time_gokei_2f`         | 2ハロンタイム合計(400M～0M) | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_2f`           | ラップタイム(400M～200M)    | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `lap_time_1f`           | ラップタイム(200M～0M)      | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_hn`

- Logical name: 繁殖馬マスタ
- Purpose: JRA 系データ。 論理名は「繁殖馬マスタ」。
- Rows: 161273
- Columns: 19
- Total size: 63 MB
- Primary key: `hanshoku_toroku_bango`
- Excel indexes: jvd_hn_pk(hanshoku_toroku_bango), jvd_hn_idx1(ketto_toroku_bango)

| column                  | logical name     | db type                 | Excel type              | nullable | key | reference / note                            |
| ----------------------- | ---------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`             | レコード種別ID   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`            | データ区分       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `hanshoku_toroku_bango` | 繁殖登録番号     | `character varying(10)` | `character varying(10)` | NO       | PK  | 繁殖馬を一意に識別するための番号            |
| `yobi_1`                | 予備             | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `ketto_toroku_bango`    | 血統登録番号     | `character varying(10)` | `character varying(10)` | YES      |     | 競走馬マスタ.血統登録番号への外部キーリンク |
| `yobi_2`                | 予備             | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `bamei`                 | 馬名             | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `bamei_hankaku_kana`    | 馬名半角ｶﾅ       | `character varying(40)` | `character varying(40)` | YES      |     |                                             |
| `bamei_eur`             | 馬名欧字         | `character varying(80)` | `character varying(80)` | YES      |     |                                             |
| `seinen`                | 生年             | `character varying(4)`  | `character varying(4)`  | YES      |     |                                             |
| `seibetsu_code`         | 性別コード       | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.性別コード を参照                  |
| `hinshu_code`           | 品種コード       | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.品種コード を参照                  |
| `moshoku_code`          | 毛色コード       | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.毛色コード を参照                  |
| `mochikomi_kubun`       | 繁殖馬持込区分   | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `yunyu_nen`             | 輸入年           | `character varying(4)`  | `character varying(4)`  | YES      |     |                                             |
| `sanchimei`             | 産地名           | `character varying(20)` | `character varying(20)` | YES      |     |                                             |
| `ketto_joho_01a`        | 父馬繁殖登録番号 | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_02a`        | 母馬繁殖登録番号 | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_hr`

- Logical name: 払戻
- Purpose: JRA 系データ。 論理名は「払戻」。
- Rows: 138297
- Columns: 158
- Total size: 140 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_hr_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                       | logical name             | db type                 | Excel type              | nullable | key | reference / note                     |
| ---------------------------- | ------------------------ | ----------------------- | ----------------------- | -------- | --- | ------------------------------------ |
| `record_id`                  | レコード種別ID           | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `data_kubun`                 | データ区分               | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `data_sakusei_nengappi`      | データ作成年月日         | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`                 | 開催年                   | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`             | 開催月日                 | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`               | 競馬場コード             | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`                 | 開催回[第N回]            | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `kaisai_nichime`             | 開催日目[N日目]          | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `race_bango`                 | レース番号               | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `toroku_tosu`                | 登録頭数                 | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `shusso_tosu`                | 出走頭数                 | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `fuseiritsu_flag_tansho`     | 不成立フラグ　単勝       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_fukusho`    | 不成立フラグ　複勝       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_wakuren`    | 不成立フラグ　枠連       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_umaren`     | 不成立フラグ　馬連       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_wide`       | 不成立フラグ　ワイド     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `yobi_1`                     | 予備                     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_umatan`     | 不成立フラグ　馬単       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_sanrenpuku` | 不成立フラグ　3連複      | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_sanrentan`  | 不成立フラグ　3連単      | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_tansho`      | 特払フラグ　単勝         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_fukusho`     | 特払フラグ　複勝         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_wakuren`     | 特払フラグ　枠連         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_umaren`      | 特払フラグ　馬連         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_wide`        | 特払フラグ　ワイド       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `yobi_2`                     | 予備                     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_umatan`      | 特払フラグ　馬単         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_sanrenpuku`  | 特払フラグ　3連複        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_sanrentan`   | 特払フラグ　3連単        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_tansho`         | 返還フラグ　単勝         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_fukusho`        | 返還フラグ　複勝         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_wakuren`        | 返還フラグ　枠連         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_umaren`         | 返還フラグ　馬連         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_wide`           | 返還フラグ　ワイド       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `yobi_3`                     | 予備                     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_umatan`         | 返還フラグ　馬単         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_sanrenpuku`     | 返還フラグ　3連複        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_sanrentan`      | 返還フラグ　3連単        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_umaban_joho`         | 返還馬番情報(馬番01～28) | `character varying(28)` | `character varying(28)` | YES      |     |                                      |
| `henkan_wakuban_joho`        | 返還枠番情報(枠番1～8)   | `character varying(8)`  | `character varying(8)`  | YES      |     |                                      |
| `henkan_dowaku_joho`         | 返還同枠情報(枠番1～8)   | `character varying(8)`  | `character varying(8)`  | YES      |     |                                      |
| `haraimodoshi_tansho_1a`     | 単勝払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_tansho_1b`     | 単勝払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_tansho_1c`     | 単勝払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_tansho_2a`     | 単勝払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_tansho_2b`     | 単勝払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_tansho_2c`     | 単勝払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_tansho_3a`     | 単勝払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_tansho_3b`     | 単勝払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_tansho_3c`     | 単勝払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_1a`    | 複勝払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_1b`    | 複勝払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_1c`    | 複勝払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_2a`    | 複勝払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_2b`    | 複勝払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_2c`    | 複勝払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_3a`    | 複勝払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_3b`    | 複勝払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_3c`    | 複勝払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_4a`    | 複勝払戻4                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_4b`    | 複勝払戻4                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_4c`    | 複勝払戻4                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_5a`    | 複勝払戻5                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_5b`    | 複勝払戻5                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_5c`    | 複勝払戻5                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_1a`    | 枠連払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_1b`    | 枠連払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_1c`    | 枠連払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_2a`    | 枠連払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_2b`    | 枠連払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_2c`    | 枠連払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_3a`    | 枠連払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_3b`    | 枠連払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_3c`    | 枠連払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_umaren_1a`     | 馬連払戻1                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umaren_1b`     | 馬連払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umaren_1c`     | 馬連払戻1                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umaren_2a`     | 馬連払戻2                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umaren_2b`     | 馬連払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umaren_2c`     | 馬連払戻2                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umaren_3a`     | 馬連払戻3                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umaren_3b`     | 馬連払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umaren_3c`     | 馬連払戻3                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_1a`       | ワイド払戻1              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_1b`       | ワイド払戻1              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_1c`       | ワイド払戻1              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_2a`       | ワイド払戻2              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_2b`       | ワイド払戻2              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_2c`       | ワイド払戻2              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_3a`       | ワイド払戻3              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_3b`       | ワイド払戻3              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_3c`       | ワイド払戻3              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_4a`       | ワイド払戻4              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_4b`       | ワイド払戻4              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_4c`       | ワイド払戻4              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_5a`       | ワイド払戻5              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_5b`       | ワイド払戻5              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_5c`       | ワイド払戻5              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_6a`       | ワイド払戻6              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_6b`       | ワイド払戻6              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_6c`       | ワイド払戻6              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_7a`       | ワイド払戻7              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_7b`       | ワイド払戻7              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_7c`       | ワイド払戻7              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `yobi_4_1a`                  | 予備1                    | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `yobi_4_1b`                  | 予備1                    | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `yobi_4_1c`                  | 予備1                    | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `yobi_4_2a`                  | 予備2                    | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `yobi_4_2b`                  | 予備2                    | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `yobi_4_2c`                  | 予備2                    | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `yobi_4_3a`                  | 予備3                    | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `yobi_4_3b`                  | 予備3                    | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `yobi_4_3c`                  | 予備3                    | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_1a`     | 馬単払戻1                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_1b`     | 馬単払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_1c`     | 馬単払戻1                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_2a`     | 馬単払戻2                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_2b`     | 馬単払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_2c`     | 馬単払戻2                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_3a`     | 馬単払戻3                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_3b`     | 馬単払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_3c`     | 馬単払戻3                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_4a`     | 馬単払戻4                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_4b`     | 馬単払戻4                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_4c`     | 馬単払戻4                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_5a`     | 馬単払戻5                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_5b`     | 馬単払戻5                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_5c`     | 馬単払戻5                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_6a`     | 馬単払戻6                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_6b`     | 馬単払戻6                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_6c`     | 馬単払戻6                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_1a` | 3連複払戻1               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_1b` | 3連複払戻1               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_1c` | 3連複払戻1               | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_2a` | 3連複払戻2               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_2b` | 3連複払戻2               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_2c` | 3連複払戻2               | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_3a` | 3連複払戻3               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_3b` | 3連複払戻3               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_3c` | 3連複払戻3               | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_1a`  | 3連単払戻1               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_1b`  | 3連単払戻1               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_1c`  | 3連単払戻1               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_2a`  | 3連単払戻2               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_2b`  | 3連単払戻2               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_2c`  | 3連単払戻2               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_3a`  | 3連単払戻3               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_3b`  | 3連単払戻3               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_3c`  | 3連単払戻3               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_4a`  | 3連単払戻4               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_4b`  | 3連単払戻4               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_4c`  | 3連単払戻4               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_5a`  | 3連単払戻5               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_5b`  | 3連単払戻5               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_5c`  | 3連単払戻5               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_6a`  | 3連単払戻6               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_6b`  | 3連単払戻6               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_6c`  | 3連単払戻6               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_hs`

- Logical name: 競走馬市場取引価格
- Purpose: JRA 系データ。 論理名は「競走馬市場取引価格」。
- Rows: 52900
- Columns: 14
- Total size: 20 MB
- Primary key: `ketto_toroku_bango, shusaisha_shijo_code, kaisai_kikan_min`
- Excel indexes: jvd_hs_pk(ketto_toroku_bango,shusaisha_shijo_code,kaisai_kikan_min)

| column                  | logical name           | db type                 | Excel type              | nullable | key | reference / note                            |
| ----------------------- | ---------------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`             | レコード種別ID         | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`            | データ区分             | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi` | データ作成年月日       | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `ketto_toroku_bango`    | 血統登録番号           | `character varying(10)` | `character varying(10)` | NO       | PK  | 競走馬を一意に識別するための番号            |
| `ketto_joho_01a`        | 父馬 繁殖登録番号      | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_02a`        | 母馬 繁殖登録番号      | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `seinen`                | 生年                   | `character varying(4)`  | `character varying(4)`  | YES      |     |                                             |
| `shusaisha_shijo_code`  | 主催者・市場コード     | `character varying(6)`  | `character varying(6)`  | NO       | PK  |                                             |
| `shusaisha_meisho`      | 主催者名称             | `character varying(40)` | `character varying(40)` | YES      |     |                                             |
| `shijo_meisho`          | 市場の名称             | `character varying(80)` | `character varying(80)` | YES      |     |                                             |
| `kaisai_kikan_min`      | 市場の開催期間(開始日) | `character varying(8)`  | `character varying(8)`  | NO       | PK  |                                             |
| `kaisai_kikan_max`      | 市場の開催期間(終了日) | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `torihikiji_nenrei`     | 取引時の競走馬の年齢   | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `torihiki_kakaku`       | 取引価格               | `character varying(10)` | `character varying(10)` | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_hy`

- Logical name: 馬名の意味由来情報
- Purpose: JRA 系データ。 論理名は「馬名の意味由来情報」。
- Rows: 174042
- Columns: 6
- Total size: 46 MB
- Primary key: `ketto_toroku_bango`
- Excel indexes: jvd_hy_pk(ketto_toroku_bango)

| column                  | logical name     | db type                 | Excel type              | nullable | key | reference / note                 |
| ----------------------- | ---------------- | ----------------------- | ----------------------- | -------- | --- | -------------------------------- |
| `record_id`             | レコード種別ID   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                  |
| `data_kubun`            | データ区分       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                  |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定               |
| `ketto_toroku_bango`    | 血統登録番号     | `character varying(10)` | `character varying(10)` | NO       | PK  | 競走馬を一意に識別するための番号 |
| `bamei`                 | 馬名             | `character varying(36)` | `character varying(36)` | YES      |     |                                  |
| `bamei_imi_yurai`       | 馬名の意味由来   | `character varying(64)` | `character varying(64)` | YES      |     |                                  |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_jc`

- Logical name: 騎手変更
- Purpose: JRA 系データ。 論理名は「騎手変更」。
- Rows: 0
- Columns: 20
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun, umaban`
- Excel indexes: jvd_jc_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,happyo_tsukihi_jifun,umaban)

| column                        | logical name         | db type                 | Excel type              | nullable | key | reference / note                        |
| ----------------------------- | -------------------- | ----------------------- | ----------------------- | -------- | --- | --------------------------------------- |
| `record_id`                   | レコード種別ID       | `character varying(2)`  | `character varying(2)`  | YES      |     |                                         |
| `data_kubun`                  | データ区分           | `character varying(1)`  | `character varying(1)`  | YES      |     |                                         |
| `data_sakusei_nengappi`       | データ作成年月日     | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                      |
| `kaisai_nen`                  | 開催年               | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー    |
| `kaisai_tsukihi`              | 開催月日             | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー    |
| `keibajo_code`                | 競馬場コード         | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー    |
| `kaisai_kai`                  | 開催回[第N回]        | `character varying(2)`  | `character varying(2)`  | YES      |     |                                         |
| `kaisai_nichime`              | 開催日目[N日目]      | `character varying(2)`  | `character varying(2)`  | YES      |     |                                         |
| `race_bango`                  | レース番号           | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー    |
| `happyo_tsukihi_jifun`        | 発表月日時分         | `character varying(8)`  | `character varying(8)`  | NO       | PK  | mmddHHmm形式で設定                      |
| `umaban`                      | 馬番                 | `character varying(2)`  | `character varying(2)`  | NO       | PK  |                                         |
| `bamei`                       | 馬名                 | `character varying(36)` | `character varying(36)` | YES      |     |                                         |
| `futan_juryo`                 | 負担重量             | `character varying(3)`  | `character varying(3)`  | YES      |     |                                         |
| `kishu_code`                  | 騎手コード           | `character varying(5)`  | `character varying(5)`  | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク |
| `kishumei`                    | 騎手名               | `character varying(34)` | `character varying(34)` | YES      |     |                                         |
| `kishu_minarai_code`          | 騎手見習コード       | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.騎手見習コード を参照          |
| `futan_juryo_henkomae`        | 変更前負担重量       | `character varying(3)`  | `character varying(3)`  | YES      |     |                                         |
| `kishu_code_henkomae`         | 変更前騎手コード     | `character varying(5)`  | `character varying(5)`  | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク |
| `kishumei_henkomae`           | 変更前騎手名         | `character varying(34)` | `character varying(34)` | YES      |     |                                         |
| `kishu_minarai_code_henkomae` | 変更前騎手見習コード | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.騎手見習コード を参照          |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_jg`

- Logical name: 競走馬除外情報
- Purpose: JRA 系データ。 論理名は「競走馬除外情報」。
- Rows: 817276
- Columns: 14
- Total size: 181 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango, shutsuba_tohyo_uketsuke`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_jg_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango,shutsuba_tohyo_uketsuke)

| column                    | logical name     | db type                 | Excel type              | nullable | key | reference / note                            |
| ------------------------- | ---------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`               | レコード種別ID   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`              | データ区分       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi`   | データ作成年月日 | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `kaisai_nen`              | 開催年           | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_tsukihi`          | 開催月日         | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `keibajo_code`            | 競馬場コード     | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_kai`              | 開催回[第N回]    | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `kaisai_nichime`          | 開催日目[N日目]  | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `race_bango`              | レース番号       | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `ketto_toroku_bango`      | 血統登録番号     | `character varying(10)` | `character varying(10)` | NO       | PK  | 競走馬マスタ.血統登録番号への外部キーリンク |
| `bamei`                   | 馬名             | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `shutsuba_tohyo_uketsuke` | 出馬投票受付順番 | `character varying(3)`  | `character varying(3)`  | NO       | PK  |                                             |
| `shusso_kubun`            | 出走区分         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `jogai_jotai_kubun`       | 除外状態区分     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_ks`

- Logical name: 騎手マスタ
- Purpose: JRA 系データ。 論理名は「騎手マスタ」。
- Rows: 1559
- Columns: 30
- Total size: 3136 kB
- Primary key: `kishu_code`
- Excel indexes: jvd_ks_pk(kishu_code)

| column                   | logical name            | db type                   | Excel type                | nullable | key | reference / note                            |
| ------------------------ | ----------------------- | ------------------------- | ------------------------- | -------- | --- | ------------------------------------------- |
| `record_id`              | レコード種別ID          | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `data_kubun`             | データ区分              | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `data_sakusei_nengappi`  | データ作成年月日        | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `kishu_code`             | 騎手コード              | `character varying(5)`    | `character varying(5)`    | NO       | PK  | 騎手を一意に識別するためのコード            |
| `massho_kubun`           | 騎手抹消区分            | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `menkyo_kofu_nengappi`   | 騎手免許交付年月日      | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `menkyo_massho_nengappi` | 騎手免許抹消年月日      | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `seinengappi`            | 生年月日                | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `kishumei`               | 騎手名                  | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `yobi_1`                 | 予備                    | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `kishumei_hankaku_kana`  | 騎手名半角ｶﾅ            | `character varying(30)`   | `character varying(30)`   | YES      |     |                                             |
| `kishumei_ryakusho`      | 騎手名略称              | `character varying(8)`    | `character varying(8)`    | YES      |     |                                             |
| `kishumei_eur`           | 騎手名欧字              | `character varying(80)`   | `character varying(80)`   | YES      |     |                                             |
| `seibetsu_kubun`         | 性別区分                | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `kijo_shikaku_code`      | 騎乗資格コード          | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.騎乗資格コード を参照              |
| `kishu_minarai_code`     | 騎手見習コード          | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.騎手見習コード を参照              |
| `tozai_shozoku_code`     | 騎手東西所属コード      | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.東西所属コード を参照              |
| `shotai_chiikimei`       | 招待地域名              | `character varying(20)`   | `character varying(20)`   | YES      |     |                                             |
| `chokyoshi_code`         | 所属調教師コード        | `character varying(5)`    | `character varying(5)`    | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei_ryakusho`  | 所属調教師名略称        | `character varying(8)`    | `character varying(8)`    | YES      |     |                                             |
| `hatsukijo_joho_1`       | 初騎乗情報1             | `character varying(67)`   | `character varying(67)`   | YES      |     |                                             |
| `hatsukijo_joho_2`       | 初騎乗情報2             | `character varying(67)`   | `character varying(67)`   | YES      |     |                                             |
| `hatsushori_joho_1`      | 初勝利情報1             | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `hatsushori_joho_2`      | 初勝利情報2             | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `jushoshori_joho_1`      | 最近重賞勝利情報1       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                             |
| `jushoshori_joho_2`      | 最近重賞勝利情報2       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                             |
| `jushoshori_joho_3`      | 最近重賞勝利情報3       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                             |
| `seiseki_joho_1`         | 本年･前年･累計成績情報1 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                             |
| `seiseki_joho_2`         | 本年･前年･累計成績情報2 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                             |
| `seiseki_joho_3`         | 本年･前年･累計成績情報3 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_o1`

- Logical name: オッズ1(単複枠)
- Purpose: JRA 系データ。 論理名は「オッズ1(単複枠)」。
- Rows: 113692
- Columns: 22
- Total size: 115 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_o1_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                   | logical name     | db type                  | Excel type               | nullable | key | reference / note                     |
| ------------------------ | ---------------- | ------------------------ | ------------------------ | -------- | --- | ------------------------------------ |
| `record_id`              | レコード種別ID   | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `data_kubun`             | データ区分       | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `data_sakusei_nengappi`  | データ作成年月日 | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`             | 開催年           | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`         | 開催月日         | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`           | 競馬場コード     | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`             | 開催回[第N回]    | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `kaisai_nichime`         | 開催日目[N日目]  | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `race_bango`             | レース番号       | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`   | 発表月日時分     | `character varying(8)`   | `character varying(8)`   | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`            | 登録頭数         | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `shusso_tosu`            | 出走頭数         | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `hatsubai_flag_tansho`   | 発売フラグ　単勝 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `hatsubai_flag_fukusho`  | 発売フラグ　複勝 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `hatsubai_flag_wakuren`  | 発売フラグ　枠連 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `fukusho_chakubarai_key` | 複勝着払キー     | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `odds_tansho`            | 単勝オッズ       | `character varying(224)` | `character varying(224)` | YES      |     |                                      |
| `odds_fukusho`           | 複勝オッズ       | `character varying(336)` | `character varying(336)` | YES      |     |                                      |
| `odds_wakuren`           | 枠連オッズ       | `character varying(324)` | `character varying(324)` | YES      |     |                                      |
| `hyosu_gokei_tansho`     | 単勝票数合計     | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |
| `hyosu_gokei_fukusho`    | 複勝票数合計     | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |
| `hyosu_gokei_wakuren`    | 枠連票数合計     | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_o2`

- Logical name: オッズ2(馬連)
- Purpose: JRA 系データ。 論理名は「オッズ2(馬連)」。
- Rows: 111029
- Columns: 15
- Total size: 131 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_o2_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name     | db type                   | Excel type                | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ------------------------- | ------------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`    | `character varying(8)`    | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `hatsubai_flag_umaren`  | 発売フラグ　馬連 | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `odds_umaren`           | 馬連オッズ       | `character varying(1989)` | `character varying(1989)` | YES      |     |                                      |
| `hyosu_gokei_umaren`    | 馬連票数合計     | `character varying(11)`   | `character varying(11)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_o3`

- Logical name: オッズ3(ワイド)
- Purpose: JRA 系データ。 論理名は「オッズ3(ワイド)」。
- Rows: 90790
- Columns: 15
- Total size: 134 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_o3_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name       | db type                   | Excel type                | nullable | key | reference / note                     |
| ----------------------- | ------------------ | ------------------------- | ------------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID     | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `data_kubun`            | データ区分         | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日   | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年             | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日           | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード       | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]      | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]    | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `race_bango`            | レース番号         | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分       | `character varying(8)`    | `character varying(8)`    | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数           | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数           | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `hatsubai_flag_wide`    | 発売フラグ　ワイド | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `odds_wide`             | ワイドオッズ       | `character varying(2601)` | `character varying(2601)` | YES      |     |                                      |
| `hyosu_gokei_wide`      | ワイド票数合計     | `character varying(11)`   | `character varying(11)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_o4`

- Logical name: オッズ4(馬単)
- Purpose: JRA 系データ。 論理名は「オッズ4(馬単)」。
- Rows: 82449
- Columns: 15
- Total size: 182 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_o4_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name     | db type                   | Excel type                | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ------------------------- | ------------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`    | `character varying(8)`    | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `hatsubai_flag_umatan`  | 発売フラグ　馬単 | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `odds_umatan`           | 馬単オッズ       | `character varying(3978)` | `character varying(3978)` | YES      |     |                                      |
| `hyosu_gokei_umatan`    | 馬単票数合計     | `character varying(11)`   | `character varying(11)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_o5`

- Logical name: オッズ5(3連複)
- Purpose: JRA 系データ。 論理名は「オッズ5(3連複)」。
- Rows: 82485
- Columns: 15
- Total size: 380 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_o5_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                     | logical name      | db type                    | Excel type                 | nullable | key | reference / note                     |
| -------------------------- | ----------------- | -------------------------- | -------------------------- | -------- | --- | ------------------------------------ |
| `record_id`                | レコード種別ID    | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `data_kubun`               | データ区分        | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `data_sakusei_nengappi`    | データ作成年月日  | `character varying(8)`     | `character varying(8)`     | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`               | 開催年            | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`           | 開催月日          | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`             | 競馬場コード      | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`               | 開催回[第N回]     | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `kaisai_nichime`           | 開催日目[N日目]   | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `race_bango`               | レース番号        | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`     | 発表月日時分      | `character varying(8)`     | `character varying(8)`     | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`              | 登録頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `shusso_tosu`              | 出走頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `hatsubai_flag_sanrenpuku` | 発売フラグ　3連複 | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `odds_sanrenpuku`          | 3連複オッズ       | `character varying(12240)` | `character varying(12240)` | YES      |     |                                      |
| `hyosu_gokei_sanrenpuku`   | 3連複票数合計     | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_o6`

- Logical name: オッズ6(3連単)
- Purpose: JRA 系データ。 論理名は「オッズ6(3連単)」。
- Rows: 66009
- Columns: 15
- Total size: 1520 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_o6_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                    | logical name      | db type                    | Excel type                 | nullable | key | reference / note                     |
| ------------------------- | ----------------- | -------------------------- | -------------------------- | -------- | --- | ------------------------------------ |
| `record_id`               | レコード種別ID    | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `data_kubun`              | データ区分        | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `data_sakusei_nengappi`   | データ作成年月日  | `character varying(8)`     | `character varying(8)`     | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`              | 開催年            | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`          | 開催月日          | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`            | 競馬場コード      | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`              | 開催回[第N回]     | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `kaisai_nichime`          | 開催日目[N日目]   | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `race_bango`              | レース番号        | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`    | 発表月日時分      | `character varying(8)`     | `character varying(8)`     | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`             | 登録頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `shusso_tosu`             | 出走頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `hatsubai_flag_sanrentan` | 発売フラグ　3連単 | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `odds_sanrentan`          | 3連単オッズ       | `character varying(83232)` | `character varying(83232)` | YES      |     |                                      |
| `hyosu_gokei_sanrentan`   | 3連単票数合計     | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_ra`

- Logical name: レース詳細
- Purpose: JRA 系データ。 論理名は「レース詳細」。
- Rows: 237713
- Columns: 62
- Total size: 386 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_ra_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango), jvd_ra_idx1(expr), jvd_ra_idx2(tokubetsu_kyoso_bango)

| column                      | logical name               | db type                  | Excel type               | nullable | key | reference / note                     |
| --------------------------- | -------------------------- | ------------------------ | ------------------------ | -------- | --- | ------------------------------------ |
| `record_id`                 | レコード種別ID             | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `data_kubun`                | データ区分                 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `data_sakusei_nengappi`     | データ作成年月日           | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`                | 開催年                     | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`            | 開催月日                   | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`              | 競馬場コード               | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`                | 開催回[第N回]              | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `kaisai_nichime`            | 開催日目[N日目]            | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `race_bango`                | レース番号                 | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `yobi_code`                 | 曜日コード                 | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.曜日コード を参照           |
| `tokubetsu_kyoso_bango`     | 特別競走番号               | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `kyosomei_hondai`           | 競走名本題                 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyosomei_fukudai`          | 競走名副題                 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyosomei_kakkonai`         | 競走名カッコ内             | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyosomei_hondai_eur`       | 競走名本題欧字             | `character varying(120)` | `character varying(120)` | YES      |     |                                      |
| `kyosomei_fukudai_eur`      | 競走名副題欧字             | `character varying(120)` | `character varying(120)` | YES      |     |                                      |
| `kyosomei_kakkonai_eur`     | 競走名カッコ内欧字         | `character varying(120)` | `character varying(120)` | YES      |     |                                      |
| `kyosomei_ryakusho_10`      | 競走名略称10文字           | `character varying(20)`  | `character varying(20)`  | YES      |     |                                      |
| `kyosomei_ryakusho_6`       | 競走名略称6文字            | `character varying(12)`  | `character varying(12)`  | YES      |     |                                      |
| `kyosomei_ryakusho_3`       | 競走名略称3文字            | `character varying(6)`   | `character varying(6)`   | YES      |     |                                      |
| `kyosomei_kubun`            | 競走名区分                 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `jusho_kaiji`               | 重賞回次[第N回]            | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `grade_code`                | グレードコード             | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.グレードコード を参照       |
| `grade_code_henkomae`       | 変更前グレードコード       | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.グレードコード を参照       |
| `kyoso_shubetsu_code`       | 競走種別コード             | `character varying(2)`   | `character varying(2)`   | YES      |     | コード表.競走種別コード を参照       |
| `kyoso_kigo_code`           | 競走記号コード             | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走記号コード を参照       |
| `juryo_shubetsu_code`       | 重量種別コード             | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.重量種別コード を参照       |
| `kyoso_joken_code_2sai`     | 競走条件コード 2歳条件     | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code_3sai`     | 競走条件コード 3歳条件     | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code_4sai`     | 競走条件コード 4歳条件     | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code_5sai_ijo` | 競走条件コード 5歳以上条件 | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code`          | 競走条件コード 最若年条件  | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_meisho`        | 競走条件名称               | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyori`                     | 距離                       | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `kyori_henkomae`            | 変更前距離                 | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `track_code`                | トラックコード             | `character varying(2)`   | `character varying(2)`   | YES      |     | コード表.トラックコード を参照       |
| `track_code_henkomae`       | 変更前トラックコード       | `character varying(2)`   | `character varying(2)`   | YES      |     | コード表.トラックコード を参照       |
| `course_kubun`              | コース区分                 | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `course_kubun_henkomae`     | 変更前コース区分           | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `honshokin`                 | 本賞金                     | `character varying(56)`  | `character varying(56)`  | YES      |     |                                      |
| `honshokin_henkomae`        | 変更前本賞金               | `character varying(40)`  | `character varying(40)`  | YES      |     |                                      |
| `fukashokin`                | 付加賞金                   | `character varying(40)`  | `character varying(40)`  | YES      |     |                                      |
| `fukashokin_henkomae`       | 変更前付加賞金             | `character varying(24)`  | `character varying(24)`  | YES      |     |                                      |
| `hasso_jikoku`              | 発走時刻                   | `character varying(4)`   | `character varying(4)`   | YES      |     | HHmm形式で設定                       |
| `hasso_jikoku_henkomae`     | 変更前発走時刻             | `character varying(4)`   | `character varying(4)`   | YES      |     | HHmm形式で設定                       |
| `toroku_tosu`               | 登録頭数                   | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `shusso_tosu`               | 出走頭数                   | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `nyusen_tosu`               | 入線頭数                   | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `tenko_code`                | 天候コード                 | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.天候コード を参照           |
| `babajotai_code_shiba`      | 芝馬場状態コード           | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.馬場状態コード を参照       |
| `babajotai_code_dirt`       | ダート馬場状態コード       | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.馬場状態コード を参照       |
| `lap_time`                  | ラップタイム               | `character varying(75)`  | `character varying(75)`  | YES      |     |                                      |
| `shogai_mile_time`          | 障害マイルタイム           | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `zenhan_3f`                 | 前3ハロン                  | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `zenhan_4f`                 | 前4ハロン                  | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `kohan_3f`                  | 後3ハロン                  | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `kohan_4f`                  | 後4ハロン                  | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `corner_tsuka_juni_1`       | コーナー通過順位1          | `character varying(72)`  | `character varying(72)`  | YES      |     |                                      |
| `corner_tsuka_juni_2`       | コーナー通過順位2          | `character varying(72)`  | `character varying(72)`  | YES      |     |                                      |
| `corner_tsuka_juni_3`       | コーナー通過順位3          | `character varying(72)`  | `character varying(72)`  | YES      |     |                                      |
| `corner_tsuka_juni_4`       | コーナー通過順位4          | `character varying(72)`  | `character varying(72)`  | YES      |     |                                      |
| `record_koshin_kubun`       | レコード更新区分           | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_rc`

- Logical name: レコードマスタ
- Purpose: JRA 系データ。 論理名は「レコードマスタ」。
- Rows: 2125
- Columns: 24
- Total size: 1888 kB
- Primary key: `record_shikibetsu_kubun, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, tokubetsu_kyoso_bango, kyoso_shubetsu_code, kyori, track_code`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_rc_pk(record_shikibetsu_kubun,kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,tokubetsu_kyoso_bango,kyoso_shubetsu_code,kyori,track_code)

| column                    | logical name         | db type                  | Excel type               | nullable | key | reference / note                     |
| ------------------------- | -------------------- | ------------------------ | ------------------------ | -------- | --- | ------------------------------------ |
| `record_id`               | レコード種別ID       | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `data_kubun`              | データ区分           | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `data_sakusei_nengappi`   | データ作成年月日     | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                   |
| `record_shikibetsu_kubun` | レコード識別区分     | `character varying(1)`   | `character varying(1)`   | NO       | PK  |                                      |
| `kaisai_nen`              | 開催年               | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`          | 開催月日             | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`            | 競馬場コード         | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`              | 開催回[第N回]        | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `kaisai_nichime`          | 開催日目[N日目]      | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `race_bango`              | レース番号           | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `tokubetsu_kyoso_bango`   | 特別競走番号         | `character varying(4)`   | `character varying(4)`   | NO       | PK  |                                      |
| `kyosomei_hondai`         | 競走名本題           | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `grade_code`              | グレードコード       | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.グレードコード を参照       |
| `kyoso_shubetsu_code`     | 競走種別コード       | `character varying(2)`   | `character varying(2)`   | NO       | PK  | コード表.競走種別コード を参照       |
| `kyori`                   | 距離                 | `character varying(4)`   | `character varying(4)`   | NO       | PK  |                                      |
| `track_code`              | トラックコード       | `character varying(2)`   | `character varying(2)`   | NO       | PK  | コード表.トラックコード を参照       |
| `record_kubun`            | レコード区分         | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `record_time`             | レコードタイム       | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `tenko_code`              | 天候コード           | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.天候コード を参照           |
| `babajotai_code_shiba`    | 芝馬場状態コード     | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.馬場状態コード を参照       |
| `babajotai_code_dirt`     | ダート馬場状態コード | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.馬場状態コード を参照       |
| `record_hojiuma_joho_1`   | レコード保持馬情報1  | `character varying(130)` | `character varying(130)` | YES      |     |                                      |
| `record_hojiuma_joho_2`   | レコード保持馬情報2  | `character varying(130)` | `character varying(130)` | YES      |     |                                      |
| `record_hojiuma_joho_3`   | レコード保持馬情報3  | `character varying(130)` | `character varying(130)` | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_se`

- Logical name: 馬毎レース情報
- Purpose: JRA 系データ。 論理名は「馬毎レース情報」。
- Rows: 2851051
- Columns: 70
- Total size: 2798 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban, ketto_toroku_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_se_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban,ketto_toroku_bango), jvd_se_idx1(expr), jvd_se_idx2(ketto_toroku_bango)

| column                        | logical name                 | db type                 | Excel type              | nullable | key | reference / note                            |
| ----------------------------- | ---------------------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`                   | レコード種別ID               | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`                  | データ区分                   | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi`       | データ作成年月日             | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `kaisai_nen`                  | 開催年                       | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_tsukihi`              | 開催月日                     | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `keibajo_code`                | 競馬場コード                 | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_kai`                  | 開催回[第N回]                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `kaisai_nichime`              | 開催日目[N日目]              | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `race_bango`                  | レース番号                   | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `wakuban`                     | 枠番                         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `umaban`                      | 馬番                         | `character varying(2)`  | `character varying(2)`  | NO       | PK  |                                             |
| `ketto_toroku_bango`          | 血統登録番号                 | `character varying(10)` | `character varying(10)` | NO       | PK  | 競走馬マスタ.血統登録番号への外部キーリンク |
| `bamei`                       | 馬名                         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `umakigo_code`                | 馬記号コード                 | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.馬記号コード を参照                |
| `seibetsu_code`               | 性別コード                   | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.性別コード を参照                  |
| `hinshu_code`                 | 品種コード                   | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.品種コード を参照                  |
| `moshoku_code`                | 毛色コード                   | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.毛色コード を参照                  |
| `barei`                       | 馬齢                         | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `tozai_shozoku_code`          | 東西所属コード               | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.東西所属コード を参照              |
| `chokyoshi_code`              | 調教師コード                 | `character varying(5)`  | `character varying(5)`  | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei_ryakusho`       | 調教師名略称                 | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `banushi_code`                | 馬主コード                   | `character varying(6)`  | `character varying(6)`  | YES      |     | 馬主マスタ.馬主コードへの外部キーリンク     |
| `banushimei`                  | 馬主名(法人格無)             | `character varying(64)` | `character varying(64)` | YES      |     |                                             |
| `fukushoku_hyoji`             | 服色標示                     | `character varying(60)` | `character varying(60)` | YES      |     |                                             |
| `yobi_1`                      | 予備                         | `character varying(60)` | `character varying(60)` | YES      |     |                                             |
| `futan_juryo`                 | 負担重量                     | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `futan_juryo_henkomae`        | 変更前負担重量               | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `blinker_shiyo_kubun`         | ブリンカー使用区分           | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `yobi_2`                      | 予備                         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `kishu_code`                  | 騎手コード                   | `character varying(5)`  | `character varying(5)`  | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク     |
| `kishu_code_henkomae`         | 変更前騎手コード             | `character varying(5)`  | `character varying(5)`  | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク     |
| `kishumei_ryakusho`           | 騎手名略称                   | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `kishumei_ryakusho_henkomae`  | 変更前騎手名略称             | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `kishu_minarai_code`          | 騎手見習コード               | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.騎手見習コード を参照              |
| `kishu_minarai_code_henkomae` | 変更前騎手見習コード         | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.騎手見習コード を参照              |
| `bataiju`                     | 馬体重                       | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `zogen_fugo`                  | 増減符号                     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `zogen_sa`                    | 増減差                       | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `ijo_kubun_code`              | 異常区分コード               | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.異常区分コード を参照              |
| `nyusen_juni`                 | 入線順位                     | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `kakutei_chakujun`            | 確定着順                     | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `dochaku_kubun`               | 同着区分                     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `dochaku_tosu`                | 同着頭数                     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `soha_time`                   | 走破タイム                   | `character varying(4)`  | `character varying(4)`  | YES      |     | 9分99.9秒で設定                             |
| `chakusa_code_1`              | 着差コード                   | `character varying(3)`  | `character varying(3)`  | YES      |     | コード表.着差コード を参照                  |
| `chakusa_code_2`              | ＋着差コード                 | `character varying(3)`  | `character varying(3)`  | YES      |     | コード表.着差コード を参照                  |
| `chakusa_code_3`              | ＋＋着差コード               | `character varying(3)`  | `character varying(3)`  | YES      |     | コード表.着差コード を参照                  |
| `corner_1`                    | 1コーナーでの順位            | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `corner_2`                    | 2コーナーでの順位            | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `corner_3`                    | 3コーナーでの順位            | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `corner_4`                    | 4コーナーでの順位            | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `tansho_odds`                 | 単勝オッズ                   | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9倍で設定                               |
| `tansho_ninkijun`             | 単勝人気順                   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `kakutoku_honshokin`          | 獲得本賞金                   | `character varying(8)`  | `character varying(8)`  | YES      |     | 単位：百円                                  |
| `kakutoku_fukashokin`         | 獲得付加賞金                 | `character varying(8)`  | `character varying(8)`  | YES      |     | 単位：百円                                  |
| `yobi_3`                      | 予備                         | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `yobi_4`                      | 予備                         | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `kohan_4f`                    | 後4ハロンタイム              | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `kohan_3f`                    | 後3ハロンタイム              | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `aiteuma_joho_1`              | 1着馬(相手馬)情報1           | `character varying(46)` | `character varying(46)` | YES      |     |                                             |
| `aiteuma_joho_2`              | 1着馬(相手馬)情報2           | `character varying(46)` | `character varying(46)` | YES      |     |                                             |
| `aiteuma_joho_3`              | 1着馬(相手馬)情報3           | `character varying(46)` | `character varying(46)` | YES      |     |                                             |
| `time_sa`                     | タイム差                     | `character varying(4)`  | `character varying(4)`  | YES      |     |                                             |
| `record_koshin_kubun`         | レコード更新区分             | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `mining_kubun`                | マイニング区分               | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `yoso_soha_time`              | マイニング予想走破タイム     | `character varying(5)`  | `character varying(5)`  | YES      |     | 9分99.99秒で設定                            |
| `yoso_gosa_plus`              | マイニング予想誤差(信頼度)＋ | `character varying(4)`  | `character varying(4)`  | YES      |     | 99.99秒で設定                               |
| `yoso_gosa_minus`             | マイニング予想誤差(信頼度)－ | `character varying(4)`  | `character varying(4)`  | YES      |     | 99.99秒で設定                               |
| `yoso_juni`                   | マイニング予想順位           | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `kyakushitsu_hantei`          | 今回レース脚質判定           | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_sk`

- Logical name: 産駒マスタ
- Purpose: JRA 系データ。 論理名は「産駒マスタ」。
- Rows: 0
- Columns: 26
- Total size: 56 kB
- Primary key: `ketto_toroku_bango`
- Excel indexes: jvd_sk_pk(ketto_toroku_bango), jvd_sk_idx1(seinengappi), jvd_sk_idx2(ketto_joho_01a), jvd_sk_idx3(ketto_joho_02a), jvd_sk_idx4(ketto_joho_06a), jvd_sk_idx5(ketto_joho_14a), jvd_sk_idx6(seisansha_code)

| column                  | logical name     | db type                 | Excel type              | nullable | key | reference / note                            |
| ----------------------- | ---------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`             | レコード種別ID   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`            | データ区分       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `ketto_toroku_bango`    | 血統登録番号     | `character varying(10)` | `character varying(10)` | NO       | PK  |                                             |
| `seinengappi`           | 生年月日         | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `seibetsu_code`         | 性別コード       | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.性別コード を参照                  |
| `hinshu_code`           | 品種コード       | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.品種コード を参照                  |
| `moshoku_code`          | 毛色コード       | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.毛色コード を参照                  |
| `mochikomi_kubun`       | 産駒持込区分     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `yunyu_nen`             | 輸入年           | `character varying(4)`  | `character varying(4)`  | YES      |     |                                             |
| `seisansha_code`        | 生産者コード     | `character varying(8)`  | `character varying(8)`  | YES      |     | 生産者マスタ.生産者コードへの外部キーリンク |
| `sanchimei`             | 産地名           | `character varying(20)` | `character varying(20)` | YES      |     |                                             |
| `ketto_joho_01a`        | 3代血統情報1     | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_02a`        | 3代血統情報2     | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_03a`        | 3代血統情報3     | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_04a`        | 3代血統情報4     | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_05a`        | 3代血統情報5     | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_06a`        | 3代血統情報6     | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_07a`        | 3代血統情報7     | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_08a`        | 3代血統情報8     | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_09a`        | 3代血統情報9     | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_10a`        | 3代血統情報10    | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_11a`        | 3代血統情報11    | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_12a`        | 3代血統情報12    | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_13a`        | 3代血統情報13    | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_14a`        | 3代血統情報14    | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_tc`

- Logical name: 発走時刻変更
- Purpose: JRA 系データ。 論理名は「発走時刻変更」。
- Rows: 0
- Columns: 12
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: jvd_tc_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name     | db type                | Excel type             | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ---------------------- | ---------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)` | `character varying(1)` | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)` | `character varying(8)` | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)` | `character varying(4)` | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)` | `character varying(4)` | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)` | `character varying(2)` | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)` | `character varying(2)` | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)` | `character varying(8)` | YES      |     | mmddHHmm形式で設定                   |
| `hasso_jikoku`          | 変更後 発走時刻  | `character varying(4)` | `character varying(4)` | YES      |     | HHmm形式で設定                       |
| `hasso_jikoku_henkomae` | 変更前 発走時刻  | `character varying(4)` | `character varying(4)` | YES      |     | HHmm形式で設定                       |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_tk`

- Logical name: 特別登録馬
- Purpose: JRA 系データ。 論理名は「特別登録馬」。
- Rows: 171
- Columns: 336
- Total size: 2016 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_tk_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                      | logical name               | db type                  | Excel type               | nullable | key | reference / note                     |
| --------------------------- | -------------------------- | ------------------------ | ------------------------ | -------- | --- | ------------------------------------ |
| `record_id`                 | レコード種別ID             | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `data_kubun`                | データ区分                 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `data_sakusei_nengappi`     | データ作成年月日           | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`                | 開催年                     | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`            | 開催月日                   | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`              | 競馬場コード               | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`                | 開催回[第N回]              | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `kaisai_nichime`            | 開催日目[N日目]            | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `race_bango`                | レース番号                 | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `yobi_code`                 | 曜日コード                 | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.曜日コード を参照           |
| `tokubetsu_kyoso_bango`     | 特別競走番号               | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `kyosomei_hondai`           | 競走名本題                 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyosomei_fukudai`          | 競走名副題                 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyosomei_kakkonai`         | 競走名カッコ内             | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyosomei_hondai_eur`       | 競走名本題欧字             | `character varying(120)` | `character varying(120)` | YES      |     |                                      |
| `kyosomei_fukudai_eur`      | 競走名副題欧字             | `character varying(120)` | `character varying(120)` | YES      |     |                                      |
| `kyosomei_kakkonai_eur`     | 競走名カッコ内欧字         | `character varying(120)` | `character varying(120)` | YES      |     |                                      |
| `kyosomei_ryakusho_10`      | 競走名略称10文字           | `character varying(20)`  | `character varying(20)`  | YES      |     |                                      |
| `kyosomei_ryakusho_6`       | 競走名略称6文字            | `character varying(12)`  | `character varying(12)`  | YES      |     |                                      |
| `kyosomei_ryakusho_3`       | 競走名略称3文字            | `character varying(6)`   | `character varying(6)`   | YES      |     |                                      |
| `kyosomei_kubun`            | 競走名区分                 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `jusho_kaiji`               | 重賞回次[第N回]            | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `grade_code`                | グレードコード             | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.グレードコード を参照       |
| `kyoso_shubetsu_code`       | 競走種別コード             | `character varying(2)`   | `character varying(2)`   | YES      |     | コード表.競走種別コード を参照       |
| `kyoso_kigo_code`           | 競走記号コード             | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走記号コード を参照       |
| `juryo_shubetsu_code`       | 重量種別コード             | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.重量種別コード を参照       |
| `kyoso_joken_code_2sai`     | 競走条件コード 2歳条件     | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code_3sai`     | 競走条件コード 3歳条件     | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code_4sai`     | 競走条件コード 4歳条件     | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code_5sai_ijo` | 競走条件コード 5歳以上条件 | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code`          | 競走条件コード 最若年条件  | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyori`                     | 距離                       | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `track_code`                | トラックコード             | `character varying(2)`   | `character varying(2)`   | YES      |     | コード表.トラックコード を参照       |
| `course_kubun`              | コース区分                 | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `handicap_happyobi`         | ハンデ発表日               | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                   |
| `toroku_tosu`               | 登録頭数                   | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `torokuba_joho_001`         | 登録馬毎情報1              | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_002`         | 登録馬毎情報2              | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_003`         | 登録馬毎情報3              | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_004`         | 登録馬毎情報4              | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_005`         | 登録馬毎情報5              | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_006`         | 登録馬毎情報6              | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_007`         | 登録馬毎情報7              | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_008`         | 登録馬毎情報8              | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_009`         | 登録馬毎情報9              | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_010`         | 登録馬毎情報10             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_011`         | 登録馬毎情報11             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_012`         | 登録馬毎情報12             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_013`         | 登録馬毎情報13             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_014`         | 登録馬毎情報14             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_015`         | 登録馬毎情報15             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_016`         | 登録馬毎情報16             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_017`         | 登録馬毎情報17             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_018`         | 登録馬毎情報18             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_019`         | 登録馬毎情報19             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_020`         | 登録馬毎情報20             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_021`         | 登録馬毎情報21             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_022`         | 登録馬毎情報22             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_023`         | 登録馬毎情報23             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_024`         | 登録馬毎情報24             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_025`         | 登録馬毎情報25             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_026`         | 登録馬毎情報26             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_027`         | 登録馬毎情報27             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_028`         | 登録馬毎情報28             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_029`         | 登録馬毎情報29             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_030`         | 登録馬毎情報30             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_031`         | 登録馬毎情報31             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_032`         | 登録馬毎情報32             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_033`         | 登録馬毎情報33             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_034`         | 登録馬毎情報34             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_035`         | 登録馬毎情報35             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_036`         | 登録馬毎情報36             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_037`         | 登録馬毎情報37             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_038`         | 登録馬毎情報38             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_039`         | 登録馬毎情報39             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_040`         | 登録馬毎情報40             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_041`         | 登録馬毎情報41             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_042`         | 登録馬毎情報42             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_043`         | 登録馬毎情報43             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_044`         | 登録馬毎情報44             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_045`         | 登録馬毎情報45             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_046`         | 登録馬毎情報46             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_047`         | 登録馬毎情報47             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_048`         | 登録馬毎情報48             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_049`         | 登録馬毎情報49             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_050`         | 登録馬毎情報50             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_051`         | 登録馬毎情報51             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_052`         | 登録馬毎情報52             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_053`         | 登録馬毎情報53             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_054`         | 登録馬毎情報54             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_055`         | 登録馬毎情報55             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_056`         | 登録馬毎情報56             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_057`         | 登録馬毎情報57             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_058`         | 登録馬毎情報58             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_059`         | 登録馬毎情報59             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_060`         | 登録馬毎情報60             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_061`         | 登録馬毎情報61             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_062`         | 登録馬毎情報62             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_063`         | 登録馬毎情報63             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_064`         | 登録馬毎情報64             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_065`         | 登録馬毎情報65             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_066`         | 登録馬毎情報66             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_067`         | 登録馬毎情報67             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_068`         | 登録馬毎情報68             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_069`         | 登録馬毎情報69             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_070`         | 登録馬毎情報70             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_071`         | 登録馬毎情報71             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_072`         | 登録馬毎情報72             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_073`         | 登録馬毎情報73             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_074`         | 登録馬毎情報74             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_075`         | 登録馬毎情報75             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_076`         | 登録馬毎情報76             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_077`         | 登録馬毎情報77             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_078`         | 登録馬毎情報78             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_079`         | 登録馬毎情報79             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_080`         | 登録馬毎情報80             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_081`         | 登録馬毎情報81             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_082`         | 登録馬毎情報82             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_083`         | 登録馬毎情報83             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_084`         | 登録馬毎情報84             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_085`         | 登録馬毎情報85             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_086`         | 登録馬毎情報86             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_087`         | 登録馬毎情報87             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_088`         | 登録馬毎情報88             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_089`         | 登録馬毎情報89             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_090`         | 登録馬毎情報90             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_091`         | 登録馬毎情報91             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_092`         | 登録馬毎情報92             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_093`         | 登録馬毎情報93             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_094`         | 登録馬毎情報94             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_095`         | 登録馬毎情報95             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_096`         | 登録馬毎情報96             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_097`         | 登録馬毎情報97             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_098`         | 登録馬毎情報98             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_099`         | 登録馬毎情報99             | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_100`         | 登録馬毎情報100            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_101`         | 登録馬毎情報101            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_102`         | 登録馬毎情報102            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_103`         | 登録馬毎情報103            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_104`         | 登録馬毎情報104            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_105`         | 登録馬毎情報105            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_106`         | 登録馬毎情報106            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_107`         | 登録馬毎情報107            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_108`         | 登録馬毎情報108            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_109`         | 登録馬毎情報109            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_110`         | 登録馬毎情報110            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_111`         | 登録馬毎情報111            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_112`         | 登録馬毎情報112            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_113`         | 登録馬毎情報113            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_114`         | 登録馬毎情報114            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_115`         | 登録馬毎情報115            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_116`         | 登録馬毎情報116            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_117`         | 登録馬毎情報117            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_118`         | 登録馬毎情報118            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_119`         | 登録馬毎情報119            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_120`         | 登録馬毎情報120            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_121`         | 登録馬毎情報121            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_122`         | 登録馬毎情報122            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_123`         | 登録馬毎情報123            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_124`         | 登録馬毎情報124            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_125`         | 登録馬毎情報125            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_126`         | 登録馬毎情報126            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_127`         | 登録馬毎情報127            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_128`         | 登録馬毎情報128            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_129`         | 登録馬毎情報129            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_130`         | 登録馬毎情報130            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_131`         | 登録馬毎情報131            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_132`         | 登録馬毎情報132            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_133`         | 登録馬毎情報133            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_134`         | 登録馬毎情報134            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_135`         | 登録馬毎情報135            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_136`         | 登録馬毎情報136            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_137`         | 登録馬毎情報137            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_138`         | 登録馬毎情報138            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_139`         | 登録馬毎情報139            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_140`         | 登録馬毎情報140            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_141`         | 登録馬毎情報141            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_142`         | 登録馬毎情報142            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_143`         | 登録馬毎情報143            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_144`         | 登録馬毎情報144            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_145`         | 登録馬毎情報145            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_146`         | 登録馬毎情報146            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_147`         | 登録馬毎情報147            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_148`         | 登録馬毎情報148            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_149`         | 登録馬毎情報149            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_150`         | 登録馬毎情報150            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_151`         | 登録馬毎情報151            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_152`         | 登録馬毎情報152            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_153`         | 登録馬毎情報153            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_154`         | 登録馬毎情報154            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_155`         | 登録馬毎情報155            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_156`         | 登録馬毎情報156            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_157`         | 登録馬毎情報157            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_158`         | 登録馬毎情報158            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_159`         | 登録馬毎情報159            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_160`         | 登録馬毎情報160            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_161`         | 登録馬毎情報161            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_162`         | 登録馬毎情報162            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_163`         | 登録馬毎情報163            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_164`         | 登録馬毎情報164            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_165`         | 登録馬毎情報165            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_166`         | 登録馬毎情報166            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_167`         | 登録馬毎情報167            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_168`         | 登録馬毎情報168            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_169`         | 登録馬毎情報169            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_170`         | 登録馬毎情報170            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_171`         | 登録馬毎情報171            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_172`         | 登録馬毎情報172            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_173`         | 登録馬毎情報173            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_174`         | 登録馬毎情報174            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_175`         | 登録馬毎情報175            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_176`         | 登録馬毎情報176            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_177`         | 登録馬毎情報177            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_178`         | 登録馬毎情報178            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_179`         | 登録馬毎情報179            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_180`         | 登録馬毎情報180            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_181`         | 登録馬毎情報181            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_182`         | 登録馬毎情報182            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_183`         | 登録馬毎情報183            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_184`         | 登録馬毎情報184            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_185`         | 登録馬毎情報185            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_186`         | 登録馬毎情報186            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_187`         | 登録馬毎情報187            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_188`         | 登録馬毎情報188            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_189`         | 登録馬毎情報189            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_190`         | 登録馬毎情報190            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_191`         | 登録馬毎情報191            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_192`         | 登録馬毎情報192            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_193`         | 登録馬毎情報193            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_194`         | 登録馬毎情報194            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_195`         | 登録馬毎情報195            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_196`         | 登録馬毎情報196            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_197`         | 登録馬毎情報197            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_198`         | 登録馬毎情報198            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_199`         | 登録馬毎情報199            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_200`         | 登録馬毎情報200            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_201`         | 登録馬毎情報201            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_202`         | 登録馬毎情報202            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_203`         | 登録馬毎情報203            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_204`         | 登録馬毎情報204            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_205`         | 登録馬毎情報205            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_206`         | 登録馬毎情報206            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_207`         | 登録馬毎情報207            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_208`         | 登録馬毎情報208            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_209`         | 登録馬毎情報209            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_210`         | 登録馬毎情報210            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_211`         | 登録馬毎情報211            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_212`         | 登録馬毎情報212            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_213`         | 登録馬毎情報213            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_214`         | 登録馬毎情報214            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_215`         | 登録馬毎情報215            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_216`         | 登録馬毎情報216            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_217`         | 登録馬毎情報217            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_218`         | 登録馬毎情報218            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_219`         | 登録馬毎情報219            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_220`         | 登録馬毎情報220            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_221`         | 登録馬毎情報221            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_222`         | 登録馬毎情報222            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_223`         | 登録馬毎情報223            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_224`         | 登録馬毎情報224            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_225`         | 登録馬毎情報225            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_226`         | 登録馬毎情報226            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_227`         | 登録馬毎情報227            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_228`         | 登録馬毎情報228            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_229`         | 登録馬毎情報229            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_230`         | 登録馬毎情報230            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_231`         | 登録馬毎情報231            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_232`         | 登録馬毎情報232            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_233`         | 登録馬毎情報233            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_234`         | 登録馬毎情報234            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_235`         | 登録馬毎情報235            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_236`         | 登録馬毎情報236            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_237`         | 登録馬毎情報237            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_238`         | 登録馬毎情報238            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_239`         | 登録馬毎情報239            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_240`         | 登録馬毎情報240            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_241`         | 登録馬毎情報241            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_242`         | 登録馬毎情報242            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_243`         | 登録馬毎情報243            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_244`         | 登録馬毎情報244            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_245`         | 登録馬毎情報245            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_246`         | 登録馬毎情報246            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_247`         | 登録馬毎情報247            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_248`         | 登録馬毎情報248            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_249`         | 登録馬毎情報249            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_250`         | 登録馬毎情報250            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_251`         | 登録馬毎情報251            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_252`         | 登録馬毎情報252            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_253`         | 登録馬毎情報253            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_254`         | 登録馬毎情報254            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_255`         | 登録馬毎情報255            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_256`         | 登録馬毎情報256            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_257`         | 登録馬毎情報257            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_258`         | 登録馬毎情報258            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_259`         | 登録馬毎情報259            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_260`         | 登録馬毎情報260            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_261`         | 登録馬毎情報261            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_262`         | 登録馬毎情報262            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_263`         | 登録馬毎情報263            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_264`         | 登録馬毎情報264            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_265`         | 登録馬毎情報265            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_266`         | 登録馬毎情報266            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_267`         | 登録馬毎情報267            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_268`         | 登録馬毎情報268            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_269`         | 登録馬毎情報269            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_270`         | 登録馬毎情報270            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_271`         | 登録馬毎情報271            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_272`         | 登録馬毎情報272            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_273`         | 登録馬毎情報273            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_274`         | 登録馬毎情報274            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_275`         | 登録馬毎情報275            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_276`         | 登録馬毎情報276            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_277`         | 登録馬毎情報277            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_278`         | 登録馬毎情報278            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_279`         | 登録馬毎情報279            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_280`         | 登録馬毎情報280            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_281`         | 登録馬毎情報281            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_282`         | 登録馬毎情報282            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_283`         | 登録馬毎情報283            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_284`         | 登録馬毎情報284            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_285`         | 登録馬毎情報285            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_286`         | 登録馬毎情報286            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_287`         | 登録馬毎情報287            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_288`         | 登録馬毎情報288            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_289`         | 登録馬毎情報289            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_290`         | 登録馬毎情報290            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_291`         | 登録馬毎情報291            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_292`         | 登録馬毎情報292            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_293`         | 登録馬毎情報293            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_294`         | 登録馬毎情報294            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_295`         | 登録馬毎情報295            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_296`         | 登録馬毎情報296            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_297`         | 登録馬毎情報297            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_298`         | 登録馬毎情報298            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_299`         | 登録馬毎情報299            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |
| `torokuba_joho_300`         | 登録馬毎情報300            | `character varying(70)`  | `character varying(70)`  | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_tm`

- Logical name: 対戦型データマイニング予想
- Purpose: JRA 系データ。 論理名は「対戦型データマイニング予想」。
- Rows: 53430
- Columns: 28
- Total size: 13 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_tm_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango), jvd_tm_idx1(expr)

| column                  | logical name     | db type                | Excel type             | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ---------------------- | ---------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)` | `character varying(1)` | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)` | `character varying(8)` | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)` | `character varying(4)` | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)` | `character varying(4)` | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)` | `character varying(2)` | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)` | `character varying(2)` | NO       | PK  | レースを一意に識別するための複合キー |
| `data_sakusei_jifun`    | データ作成時分   | `character varying(4)` | `character varying(4)` | YES      |     |                                      |
| `mining_yoso_01`        | マイニング予想1  | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_02`        | マイニング予想2  | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_03`        | マイニング予想3  | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_04`        | マイニング予想4  | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_05`        | マイニング予想5  | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_06`        | マイニング予想6  | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_07`        | マイニング予想7  | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_08`        | マイニング予想8  | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_09`        | マイニング予想9  | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_10`        | マイニング予想10 | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_11`        | マイニング予想11 | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_12`        | マイニング予想12 | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_13`        | マイニング予想13 | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_14`        | マイニング予想14 | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_15`        | マイニング予想15 | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_16`        | マイニング予想16 | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_17`        | マイニング予想17 | `character varying(6)` | `character varying(6)` | YES      |     |                                      |
| `mining_yoso_18`        | マイニング予想18 | `character varying(6)` | `character varying(6)` | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_um`

- Logical name: 競走馬マスタ
- Purpose: JRA 系データ。 論理名は「競走馬マスタ」。
- Rows: 212353
- Columns: 89
- Total size: 466 MB
- Primary key: `ketto_toroku_bango`
- Excel indexes: jvd_um_pk(ketto_toroku_bango), jvd_um_idx1(seinengappi), jvd_um_idx2(ketto_joho_01a), jvd_um_idx3(ketto_joho_02a), jvd_um_idx4(ketto_joho_06a), jvd_um_idx5(ketto_joho_14a), jvd_um_idx6(chokyoshi_code), jvd_um_idx7(seisansha_code), jvd_um_idx8(banushi_code)

| column                        | logical name          | db type                 | Excel type              | nullable | key | reference / note                            |
| ----------------------------- | --------------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`                   | レコード種別ID        | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`                  | データ区分            | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi`       | データ作成年月日      | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `ketto_toroku_bango`          | 血統登録番号          | `character varying(10)` | `character varying(10)` | NO       | PK  | 競走馬を一意に識別するための番号            |
| `massho_kubun`                | 競走馬抹消区分        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `toroku_nengappi`             | 競走馬登録年月日      | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `massho_nengappi`             | 競走馬抹消年月日      | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `seinengappi`                 | 生年月日              | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `bamei`                       | 馬名                  | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `bamei_hankaku_kana`          | 馬名半角ｶﾅ            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `bamei_eur`                   | 馬名欧字              | `character varying(60)` | `character varying(60)` | YES      |     |                                             |
| `zaikyu_flag`                 | JRA施設在きゅうフラグ | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `yobi_1`                      | 予備                  | `character varying(19)` | `character varying(19)` | YES      |     |                                             |
| `umakigo_code`                | 馬記号コード          | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.馬記号コード を参照                |
| `seibetsu_code`               | 性別コード            | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.性別コード を参照                  |
| `hinshu_code`                 | 品種コード            | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.品種コード を参照                  |
| `moshoku_code`                | 毛色コード            | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.毛色コード を参照                  |
| `ketto_joho_01a`              | 3代血統情報1          | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_01b`              | 3代血統情報1          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_02a`              | 3代血統情報2          | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_02b`              | 3代血統情報2          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_03a`              | 3代血統情報3          | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_03b`              | 3代血統情報3          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_04a`              | 3代血統情報4          | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_04b`              | 3代血統情報4          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_05a`              | 3代血統情報5          | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_05b`              | 3代血統情報5          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_06a`              | 3代血統情報6          | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_06b`              | 3代血統情報6          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_07a`              | 3代血統情報7          | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_07b`              | 3代血統情報7          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_08a`              | 3代血統情報8          | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_08b`              | 3代血統情報8          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_09a`              | 3代血統情報9          | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_09b`              | 3代血統情報9          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_10a`              | 3代血統情報10         | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_10b`              | 3代血統情報10         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_11a`              | 3代血統情報11         | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_11b`              | 3代血統情報11         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_12a`              | 3代血統情報12         | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_12b`              | 3代血統情報12         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_13a`              | 3代血統情報13         | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_13b`              | 3代血統情報13         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_14a`              | 3代血統情報14         | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_14b`              | 3代血統情報14         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `tozai_shozoku_code`          | 東西所属コード        | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.東西所属コード を参照              |
| `chokyoshi_code`              | 調教師コード          | `character varying(5)`  | `character varying(5)`  | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei_ryakusho`       | 調教師名略称          | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `shotai_chiikimei`            | 招待地域名            | `character varying(20)` | `character varying(20)` | YES      |     |                                             |
| `seisansha_code`              | 生産者コード          | `character varying(8)`  | `character varying(8)`  | YES      |     | 生産者マスタ.生産者コードへの外部キーリンク |
| `seisanshamei`                | 生産者名(法人格無)    | `character varying(72)` | `character varying(72)` | YES      |     |                                             |
| `sanchimei`                   | 産地名                | `character varying(20)` | `character varying(20)` | YES      |     |                                             |
| `banushi_code`                | 馬主コード            | `character varying(6)`  | `character varying(6)`  | YES      |     | 馬主マスタ.馬主コードへの外部キーリンク     |
| `banushimei`                  | 馬主名(法人格無)      | `character varying(64)` | `character varying(64)` | YES      |     |                                             |
| `heichi_honshokin_ruikei`     | 平地本賞金累計        | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `shogai_honshokin_ruikei`     | 障害本賞金累計        | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `heichi_fukashokin_ruikei`    | 平地付加賞金累計      | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `shogai_fukashokin_ruikei`    | 障害付加賞金累計      | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `heichi_shutokushokin_ruikei` | 平地収得賞金累計      | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `shogai_shutokushokin_ruikei` | 障害収得賞金累計      | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `sogo`                        | 総合着回数            | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `chuo_gokei`                  | 中央合計着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_choku`                 | 芝直・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_migi`                  | 芝右・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_hidari`                | 芝左・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_choku`                  | ダ直・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_migi`                   | ダ右・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_hidari`                 | ダ左・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shogai`                      | 障害・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_ryo`                   | 芝良・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_yayaomo`               | 芝稍・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_omo`                   | 芝重・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_furyo`                 | 芝不・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_ryo`                    | ダ良・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_yayaomo`                | ダ稍・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_omo`                    | ダ重・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_furyo`                  | ダ不・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shogai_ryo`                  | 障良・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shogai_yayaomo`              | 障稍・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shogai_omo`                  | 障重・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shogai_furyo`                | 障不・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_short`                 | 芝16下・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_middle`                | 芝22下・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_long`                  | 芝22超・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_short`                  | ダ16下・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_middle`                 | ダ22下・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_long`                   | ダ22超・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `kyakushitsu_keiko`           | 脚質傾向              | `character varying(12)` | `character varying(12)` | YES      |     |                                             |
| `toroku_race_su`              | 登録レース数          | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_wc`

- Logical name: ウッドチップ調教
- Purpose: JRA 系データ。 論理名は「ウッドチップ調教」。
- Rows: 725838
- Columns: 29
- Total size: 189 MB
- Primary key: `tracen_kubun, chokyo_nengappi, chokyo_jikoku, ketto_toroku_bango`
- Excel indexes: jvd_wc_pk(tracen_kubun,chokyo_nengappi,chokyo_jikoku,ketto_toroku_bango), jvd_wc_idx1(ketto_toroku_bango)

| column                  | logical name                  | db type                 | Excel type              | nullable | key | reference / note                            |
| ----------------------- | ----------------------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`             | レコード種別ID                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`            | データ区分                    | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi` | データ作成年月日              | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `tracen_kubun`          | トレセン区分                  | `character varying(1)`  | `character varying(1)`  | NO       | PK  |                                             |
| `chokyo_nengappi`       | 調教年月日                    | `character varying(8)`  | `character varying(8)`  | NO       | PK  | yyyymmdd形式で設定                          |
| `chokyo_jikoku`         | 調教時刻                      | `character varying(4)`  | `character varying(4)`  | NO       | PK  | HHmm形式で設定                              |
| `ketto_toroku_bango`    | 血統登録番号                  | `character varying(10)` | `character varying(10)` | NO       | PK  | 競走馬マスタ.血統登録番号への外部キーリンク |
| `course`                | コース                        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `babamawari`            | 馬場周り                      | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `yobi_1`                | 予備                          | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `time_gokei_10f`        | 10ハロンタイム合計(2000M～0M) | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_10f`          | ラップタイム(2000M～1800M)    | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `time_gokei_9f`         | 9ハロンタイム合計(1800M～0M)  | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_9f`           | ラップタイム(1800M～1600M)    | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `time_gokei_8f`         | 8ロンタイム合計(1600M～0M)    | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_8f`           | ラップタイム(1600M～1400M)    | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `time_gokei_7f`         | 7ハロンタイム合計(1400M～0M)  | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_7f`           | ラップタイム(1400M～1200M)    | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `time_gokei_6f`         | 6ハロンタイム合計(1200M～0M)  | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_6f`           | ラップタイム(1200M～1000M)    | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `time_gokei_5f`         | 5ハロンタイム合計(1000M～0M)  | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_5f`           | ラップタイム(1000M～800M)     | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `time_gokei_4f`         | 4ハロンタイム合計(800M～0M)   | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_4f`           | ラップタイム(800M～600M)      | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `time_gokei_3f`         | 3ハロンタイム合計(600M～0M)   | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_3f`           | ラップタイム(600M～400M)      | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `time_gokei_2f`         | 2ハロンタイム合計(400M～0M)   | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9秒で設定                               |
| `lap_time_2f`           | ラップタイム(400M～200M)      | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `lap_time_1f`           | ラップタイム(200M～0M)        | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_we`

- Logical name: 天候馬場状態
- Purpose: JRA 系データ。 論理名は「天候馬場状態」。
- Rows: 0
- Columns: 16
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, happyo_tsukihi_jifun, henko_shikibetsu`
- Excel indexes: jvd_we_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,happyo_tsukihi_jifun,henko_shikibetsu)

| column                          | logical name            | db type                | Excel type             | nullable | key | reference / note             |
| ------------------------------- | ----------------------- | ---------------------- | ---------------------- | -------- | --- | ---------------------------- |
| `record_id`                     | レコード種別ID          | `character varying(2)` | `character varying(2)` | YES      |     |                              |
| `data_kubun`                    | データ区分              | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `data_sakusei_nengappi`         | データ作成年月日        | `character varying(8)` | `character varying(8)` | YES      |     | yyyymmdd形式で設定           |
| `kaisai_nen`                    | 開催年                  | `character varying(4)` | `character varying(4)` | NO       | PK  |                              |
| `kaisai_tsukihi`                | 開催月日                | `character varying(4)` | `character varying(4)` | NO       | PK  |                              |
| `keibajo_code`                  | 競馬場コード            | `character varying(2)` | `character varying(2)` | NO       | PK  | コード表.競馬場コード を参照 |
| `kaisai_kai`                    | 開催回[第N回]           | `character varying(2)` | `character varying(2)` | YES      |     |                              |
| `kaisai_nichime`                | 開催日目[N日目]         | `character varying(2)` | `character varying(2)` | YES      |     |                              |
| `happyo_tsukihi_jifun`          | 発表月日時分            | `character varying(8)` | `character varying(8)` | NO       | PK  | mmddHHmm形式で設定           |
| `henko_shikibetsu`              | 変更識別                | `character varying(1)` | `character varying(1)` | NO       | PK  |                              |
| `tenko_code`                    | 天候状態                | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `babajotai_code_shiba`          | 馬場状態・芝            | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `babajotai_code_dirt`           | 馬場状態・ダート        | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `tenko_code_henkomae`           | 変更前 天候状態         | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `babajotai_code_shiba_henkomae` | 変更前 馬場状態・芝     | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `babajotai_code_dirt_henkomae`  | 変更前 馬場状態・ダート | `character varying(1)` | `character varying(1)` | YES      |     |                              |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_wf`

- Logical name: 重勝式(WIN5)
- Purpose: JRA 系データ。 論理名は「重勝式(WIN5)」。
- Rows: 861
- Columns: 266
- Total size: 27 MB
- Primary key: `kaisai_nen, kaisai_tsukihi`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_wf_pk(kaisai_nen,kaisai_tsukihi)

| column                  | logical name             | db type                 | Excel type              | nullable | key | reference / note   |
| ----------------------- | ------------------------ | ----------------------- | ----------------------- | -------- | --- | ------------------ |
| `record_id`             | レコード種別ID           | `character varying(2)`  | `character varying(2)`  | YES      |     |                    |
| `data_kubun`            | データ区分               | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `data_sakusei_nengappi` | データ作成年月日         | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定 |
| `kaisai_nen`            | 開催年                   | `character varying(4)`  | `character varying(4)`  | NO       | PK  | yyyy形式で設定     |
| `kaisai_tsukihi`        | 開催月日                 | `character varying(4)`  | `character varying(4)`  | NO       | PK  | mmdd形式で設定     |
| `yobi_1`                | 予備                     | `character varying(2)`  | `character varying(2)`  | YES      |     |                    |
| `race_joho_1`           | 重勝式対象レース情報1    | `character varying(8)`  | `character varying(8)`  | YES      |     |                    |
| `race_joho_2`           | 重勝式対象レース情報2    | `character varying(8)`  | `character varying(8)`  | YES      |     |                    |
| `race_joho_3`           | 重勝式対象レース情報3    | `character varying(8)`  | `character varying(8)`  | YES      |     |                    |
| `race_joho_4`           | 重勝式対象レース情報4    | `character varying(8)`  | `character varying(8)`  | YES      |     |                    |
| `race_joho_5`           | 重勝式対象レース情報5    | `character varying(8)`  | `character varying(8)`  | YES      |     |                    |
| `yobi_2`                | 予備                     | `character varying(6)`  | `character varying(6)`  | YES      |     |                    |
| `win5_hyosu_gokei`      | 重勝式発売票数           | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `yuko_hyosu_1`          | 有効票数情報1            | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `yuko_hyosu_2`          | 有効票数情報2            | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `yuko_hyosu_3`          | 有効票数情報3            | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `yuko_hyosu_4`          | 有効票数情報4            | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `yuko_hyosu_5`          | 有効票数情報5            | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `henkan_flag`           | 返還フラグ               | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `fuseiritsu_flag`       | 不成立フラグ             | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `tekichu_nashi_flag`    | 的中無フラグ             | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `carry_over`            | キャリーオーバー金額初期 | `character varying(15)` | `character varying(15)` | YES      |     |                    |
| `carry_over_zandaka`    | キャリーオーバー金額残高 | `character varying(15)` | `character varying(15)` | YES      |     |                    |
| `haraimodoshi_win5_001` | 重勝式払戻情報1          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_002` | 重勝式払戻情報2          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_003` | 重勝式払戻情報3          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_004` | 重勝式払戻情報4          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_005` | 重勝式払戻情報5          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_006` | 重勝式払戻情報6          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_007` | 重勝式払戻情報7          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_008` | 重勝式払戻情報8          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_009` | 重勝式払戻情報9          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_010` | 重勝式払戻情報10         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_011` | 重勝式払戻情報11         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_012` | 重勝式払戻情報12         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_013` | 重勝式払戻情報13         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_014` | 重勝式払戻情報14         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_015` | 重勝式払戻情報15         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_016` | 重勝式払戻情報16         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_017` | 重勝式払戻情報17         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_018` | 重勝式払戻情報18         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_019` | 重勝式払戻情報19         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_020` | 重勝式払戻情報20         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_021` | 重勝式払戻情報21         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_022` | 重勝式払戻情報22         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_023` | 重勝式払戻情報23         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_024` | 重勝式払戻情報24         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_025` | 重勝式払戻情報25         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_026` | 重勝式払戻情報26         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_027` | 重勝式払戻情報27         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_028` | 重勝式払戻情報28         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_029` | 重勝式払戻情報29         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_030` | 重勝式払戻情報30         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_031` | 重勝式払戻情報31         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_032` | 重勝式払戻情報32         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_033` | 重勝式払戻情報33         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_034` | 重勝式払戻情報34         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_035` | 重勝式払戻情報35         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_036` | 重勝式払戻情報36         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_037` | 重勝式払戻情報37         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_038` | 重勝式払戻情報38         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_039` | 重勝式払戻情報39         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_040` | 重勝式払戻情報40         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_041` | 重勝式払戻情報41         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_042` | 重勝式払戻情報42         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_043` | 重勝式払戻情報43         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_044` | 重勝式払戻情報44         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_045` | 重勝式払戻情報45         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_046` | 重勝式払戻情報46         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_047` | 重勝式払戻情報47         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_048` | 重勝式払戻情報48         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_049` | 重勝式払戻情報49         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_050` | 重勝式払戻情報50         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_051` | 重勝式払戻情報51         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_052` | 重勝式払戻情報52         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_053` | 重勝式払戻情報53         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_054` | 重勝式払戻情報54         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_055` | 重勝式払戻情報55         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_056` | 重勝式払戻情報56         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_057` | 重勝式払戻情報57         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_058` | 重勝式払戻情報58         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_059` | 重勝式払戻情報59         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_060` | 重勝式払戻情報60         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_061` | 重勝式払戻情報61         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_062` | 重勝式払戻情報62         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_063` | 重勝式払戻情報63         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_064` | 重勝式払戻情報64         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_065` | 重勝式払戻情報65         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_066` | 重勝式払戻情報66         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_067` | 重勝式払戻情報67         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_068` | 重勝式払戻情報68         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_069` | 重勝式払戻情報69         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_070` | 重勝式払戻情報70         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_071` | 重勝式払戻情報71         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_072` | 重勝式払戻情報72         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_073` | 重勝式払戻情報73         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_074` | 重勝式払戻情報74         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_075` | 重勝式払戻情報75         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_076` | 重勝式払戻情報76         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_077` | 重勝式払戻情報77         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_078` | 重勝式払戻情報78         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_079` | 重勝式払戻情報79         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_080` | 重勝式払戻情報80         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_081` | 重勝式払戻情報81         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_082` | 重勝式払戻情報82         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_083` | 重勝式払戻情報83         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_084` | 重勝式払戻情報84         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_085` | 重勝式払戻情報85         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_086` | 重勝式払戻情報86         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_087` | 重勝式払戻情報87         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_088` | 重勝式払戻情報88         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_089` | 重勝式払戻情報89         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_090` | 重勝式払戻情報90         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_091` | 重勝式払戻情報91         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_092` | 重勝式払戻情報92         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_093` | 重勝式払戻情報93         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_094` | 重勝式払戻情報94         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_095` | 重勝式払戻情報95         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_096` | 重勝式払戻情報96         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_097` | 重勝式払戻情報97         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_098` | 重勝式払戻情報98         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_099` | 重勝式払戻情報99         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_100` | 重勝式払戻情報100        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_101` | 重勝式払戻情報101        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_102` | 重勝式払戻情報102        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_103` | 重勝式払戻情報103        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_104` | 重勝式払戻情報104        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_105` | 重勝式払戻情報105        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_106` | 重勝式払戻情報106        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_107` | 重勝式払戻情報107        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_108` | 重勝式払戻情報108        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_109` | 重勝式払戻情報109        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_110` | 重勝式払戻情報110        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_111` | 重勝式払戻情報111        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_112` | 重勝式払戻情報112        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_113` | 重勝式払戻情報113        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_114` | 重勝式払戻情報114        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_115` | 重勝式払戻情報115        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_116` | 重勝式払戻情報116        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_117` | 重勝式払戻情報117        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_118` | 重勝式払戻情報118        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_119` | 重勝式払戻情報119        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_120` | 重勝式払戻情報120        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_121` | 重勝式払戻情報121        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_122` | 重勝式払戻情報122        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_123` | 重勝式払戻情報123        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_124` | 重勝式払戻情報124        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_125` | 重勝式払戻情報125        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_126` | 重勝式払戻情報126        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_127` | 重勝式払戻情報127        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_128` | 重勝式払戻情報128        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_129` | 重勝式払戻情報129        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_130` | 重勝式払戻情報130        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_131` | 重勝式払戻情報131        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_132` | 重勝式払戻情報132        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_133` | 重勝式払戻情報133        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_134` | 重勝式払戻情報134        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_135` | 重勝式払戻情報135        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_136` | 重勝式払戻情報136        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_137` | 重勝式払戻情報137        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_138` | 重勝式払戻情報138        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_139` | 重勝式払戻情報139        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_140` | 重勝式払戻情報140        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_141` | 重勝式払戻情報141        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_142` | 重勝式払戻情報142        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_143` | 重勝式払戻情報143        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_144` | 重勝式払戻情報144        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_145` | 重勝式払戻情報145        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_146` | 重勝式払戻情報146        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_147` | 重勝式払戻情報147        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_148` | 重勝式払戻情報148        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_149` | 重勝式払戻情報149        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_150` | 重勝式払戻情報150        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_151` | 重勝式払戻情報151        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_152` | 重勝式払戻情報152        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_153` | 重勝式払戻情報153        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_154` | 重勝式払戻情報154        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_155` | 重勝式払戻情報155        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_156` | 重勝式払戻情報156        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_157` | 重勝式払戻情報157        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_158` | 重勝式払戻情報158        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_159` | 重勝式払戻情報159        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_160` | 重勝式払戻情報160        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_161` | 重勝式払戻情報161        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_162` | 重勝式払戻情報162        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_163` | 重勝式払戻情報163        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_164` | 重勝式払戻情報164        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_165` | 重勝式払戻情報165        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_166` | 重勝式払戻情報166        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_167` | 重勝式払戻情報167        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_168` | 重勝式払戻情報168        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_169` | 重勝式払戻情報169        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_170` | 重勝式払戻情報170        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_171` | 重勝式払戻情報171        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_172` | 重勝式払戻情報172        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_173` | 重勝式払戻情報173        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_174` | 重勝式払戻情報174        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_175` | 重勝式払戻情報175        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_176` | 重勝式払戻情報176        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_177` | 重勝式払戻情報177        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_178` | 重勝式払戻情報178        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_179` | 重勝式払戻情報179        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_180` | 重勝式払戻情報180        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_181` | 重勝式払戻情報181        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_182` | 重勝式払戻情報182        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_183` | 重勝式払戻情報183        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_184` | 重勝式払戻情報184        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_185` | 重勝式払戻情報185        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_186` | 重勝式払戻情報186        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_187` | 重勝式払戻情報187        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_188` | 重勝式払戻情報188        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_189` | 重勝式払戻情報189        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_190` | 重勝式払戻情報190        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_191` | 重勝式払戻情報191        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_192` | 重勝式払戻情報192        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_193` | 重勝式払戻情報193        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_194` | 重勝式払戻情報194        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_195` | 重勝式払戻情報195        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_196` | 重勝式払戻情報196        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_197` | 重勝式払戻情報197        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_198` | 重勝式払戻情報198        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_199` | 重勝式払戻情報199        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_200` | 重勝式払戻情報200        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_201` | 重勝式払戻情報201        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_202` | 重勝式払戻情報202        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_203` | 重勝式払戻情報203        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_204` | 重勝式払戻情報204        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_205` | 重勝式払戻情報205        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_206` | 重勝式払戻情報206        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_207` | 重勝式払戻情報207        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_208` | 重勝式払戻情報208        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_209` | 重勝式払戻情報209        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_210` | 重勝式払戻情報210        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_211` | 重勝式払戻情報211        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_212` | 重勝式払戻情報212        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_213` | 重勝式払戻情報213        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_214` | 重勝式払戻情報214        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_215` | 重勝式払戻情報215        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_216` | 重勝式払戻情報216        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_217` | 重勝式払戻情報217        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_218` | 重勝式払戻情報218        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_219` | 重勝式払戻情報219        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_220` | 重勝式払戻情報220        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_221` | 重勝式払戻情報221        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_222` | 重勝式払戻情報222        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_223` | 重勝式払戻情報223        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_224` | 重勝式払戻情報224        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_225` | 重勝式払戻情報225        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_226` | 重勝式払戻情報226        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_227` | 重勝式払戻情報227        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_228` | 重勝式払戻情報228        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_229` | 重勝式払戻情報229        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_230` | 重勝式払戻情報230        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_231` | 重勝式払戻情報231        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_232` | 重勝式払戻情報232        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_233` | 重勝式払戻情報233        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_234` | 重勝式払戻情報234        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_235` | 重勝式払戻情報235        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_236` | 重勝式払戻情報236        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_237` | 重勝式払戻情報237        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_238` | 重勝式払戻情報238        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_239` | 重勝式払戻情報239        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_240` | 重勝式払戻情報240        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_241` | 重勝式払戻情報241        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_242` | 重勝式払戻情報242        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_243` | 重勝式払戻情報243        | `character varying(29)` | `character varying(29)` | YES      |     |                    |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_wh`

- Logical name: 馬体重
- Purpose: JRA 系データ。 論理名は「馬体重」。
- Rows: 0
- Columns: 28
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: jvd_wh_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name     | db type                 | Excel type              | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`  | `character varying(8)`  | YES      |     | mmddHHmm形式で設定                   |
| `bataiju_joho_01`       | 馬体重情報1      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_02`       | 馬体重情報2      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_03`       | 馬体重情報3      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_04`       | 馬体重情報4      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_05`       | 馬体重情報5      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_06`       | 馬体重情報6      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_07`       | 馬体重情報7      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_08`       | 馬体重情報8      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_09`       | 馬体重情報9      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_10`       | 馬体重情報10     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_11`       | 馬体重情報11     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_12`       | 馬体重情報12     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_13`       | 馬体重情報13     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_14`       | 馬体重情報14     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_15`       | 馬体重情報15     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_16`       | 馬体重情報16     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_17`       | 馬体重情報17     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_18`       | 馬体重情報18     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `jvd_ys`

- Logical name: 開催スケジュール
- Purpose: JRA 系データ。 論理名は「開催スケジュール」。
- Rows: 7817
- Columns: 12
- Total size: 4936 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: jvd_ys_pk(kaisai_nen,kaisai_tsukihi,keibajo_code)

| column                  | logical name     | db type                  | Excel type               | nullable | key | reference / note             |
| ----------------------- | ---------------- | ------------------------ | ------------------------ | -------- | --- | ---------------------------- |
| `record_id`             | レコード種別ID   | `character varying(2)`   | `character varying(2)`   | YES      |     |                              |
| `data_kubun`            | データ区分       | `character varying(1)`   | `character varying(1)`   | YES      |     |                              |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定           |
| `kaisai_nen`            | 開催年           | `character varying(4)`   | `character varying(4)`   | NO       | PK  | yyyy形式で設定               |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`   | `character varying(4)`   | NO       | PK  | mmdd形式で設定               |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`   | `character varying(2)`   | NO       | PK  | コード表.競馬場コード を参照 |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`   | `character varying(2)`   | YES      |     |                              |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`   | `character varying(2)`   | YES      |     |                              |
| `yobi_code`             | 曜日コード       | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.曜日コード を参照   |
| `jusho_joho_1`          | 重賞案内1        | `character varying(118)` | `character varying(118)` | YES      |     |                              |
| `jusho_joho_2`          | 重賞案内2        | `character varying(118)` | `character varying(118)` | YES      |     |                              |
| `jusho_joho_3`          | 重賞案内3        | `character varying(118)` | `character varying(118)` | YES      |     |                              |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_av`

- Logical name: 出走取消･競走除外
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「出走取消･競走除外」。
- Rows: 0
- Columns: 13
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban`
- Excel indexes: nvd_av_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban)

| column                  | logical name     | db type                 | Excel type              | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`  | `character varying(8)`  | YES      |     | mmddHHmm形式で設定                   |
| `umaban`                | 馬番             | `character varying(2)`  | `character varying(2)`  | NO       | PK  |                                      |
| `bamei`                 | 馬名             | `character varying(36)` | `character varying(36)` | YES      |     |                                      |
| `jiyu_kubun`            | 事由区分         | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_bn`

- Logical name: 馬主マスタ
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「馬主マスタ」。
- Rows: 9485
- Columns: 11
- Total size: 13 MB
- Primary key: `banushi_code, banushimei`
- Excel indexes: nvd_bn_pk(banushi_code,banushimei)

| column                    | logical name       | db type                  | Excel type               | nullable | key | reference / note                 |
| ------------------------- | ------------------ | ------------------------ | ------------------------ | -------- | --- | -------------------------------- |
| `record_id`               | レコード種別ID     | `character varying(2)`   | `character varying(2)`   | YES      |     |                                  |
| `data_kubun`              | データ区分         | `character varying(1)`   | `character varying(1)`   | YES      |     |                                  |
| `data_sakusei_nengappi`   | データ作成年月日   | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定               |
| `banushi_code`            | 馬主コード         | `character varying(6)`   | `character varying(6)`   | NO       | PK  | 馬主を一意に識別するためのコード |
| `banushimei_hojinkaku`    | 馬主名(法人格有)   | `character varying(64)`  | `character varying(64)`  | YES      |     |                                  |
| `banushimei`              | 馬主名(法人格無)   | `character varying(64)`  | `character varying(64)`  | NO       | PK  |                                  |
| `banushimei_hankaku_kana` | 馬主名半角ｶﾅ       | `character varying(50)`  | `character varying(50)`  | YES      |     |                                  |
| `banushimei_eur`          | 馬主名欧字         | `character varying(100)` | `character varying(100)` | YES      |     |                                  |
| `fukushoku_hyoji`         | 服色標示           | `character varying(60)`  | `character varying(60)`  | YES      |     |                                  |
| `seiseki_joho_1`          | 本年･累計成績情報1 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                  |
| `seiseki_joho_2`          | 本年･累計成績情報2 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                  |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_cd`

- Logical name: コード変更情報
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「コード変更情報」。
- Rows: 0
- Columns: 7
- Total size: 8192 bytes
- Primary key: `henkotaisho_id, code_new`
- Excel indexes: nvd_cd_pk(henkotaisho_id,code_new)

| column                  | logical name     | db type                 | Excel type              | nullable | key | reference / note   |
| ----------------------- | ---------------- | ----------------------- | ----------------------- | -------- | --- | ------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`  | `character varying(2)`  | YES      |     |                    |
| `data_kubun`            | データ区分       | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定 |
| `henkotaisho_id`        | 変更対象種別ID   | `character varying(2)`  | `character varying(2)`  | NO       | PK  |                    |
| `code_new`              | 新コード         | `character varying(6)`  | `character varying(6)`  | NO       | PK  |                    |
| `code_old`              | 旧コード         | `character varying(6)`  | `character varying(6)`  | YES      |     |                    |
| `meisho`                | 名称             | `character varying(64)` | `character varying(64)` | YES      |     |                    |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_ch`

- Logical name: 調教師マスタ
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「調教師マスタ」。
- Rows: 1384
- Columns: 21
- Total size: 3256 kB
- Primary key: `chokyoshi_code`
- Excel indexes: nvd_ch_pk(chokyoshi_code)

| column                      | logical name            | db type                   | Excel type                | nullable | key | reference / note                   |
| --------------------------- | ----------------------- | ------------------------- | ------------------------- | -------- | --- | ---------------------------------- |
| `record_id`                 | レコード種別ID          | `character varying(2)`    | `character varying(2)`    | YES      |     |                                    |
| `data_kubun`                | データ区分              | `character varying(1)`    | `character varying(1)`    | YES      |     |                                    |
| `data_sakusei_nengappi`     | データ作成年月日        | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `chokyoshi_code`            | 調教師コード            | `character varying(5)`    | `character varying(5)`    | NO       | PK  | 調教師を一意に識別するためのコード |
| `massho_kubun`              | 調教師抹消区分          | `character varying(1)`    | `character varying(1)`    | YES      |     |                                    |
| `menkyo_kofu_nengappi`      | 調教師免許交付年月日    | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `menkyo_massho_nengappi`    | 調教師免許抹消年月日    | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `seinengappi`               | 生年月日                | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `chokyoshimei`              | 調教師名                | `character varying(34)`   | `character varying(34)`   | YES      |     |                                    |
| `chokyoshimei_hankaku_kana` | 調教師名半角ｶﾅ          | `character varying(30)`   | `character varying(30)`   | YES      |     |                                    |
| `chokyoshimei_ryakusho`     | 調教師名略称            | `character varying(8)`    | `character varying(8)`    | YES      |     |                                    |
| `chokyoshimei_eur`          | 調教師名欧字            | `character varying(80)`   | `character varying(80)`   | YES      |     |                                    |
| `seibetsu_kubun`            | 性別区分                | `character varying(1)`    | `character varying(1)`    | YES      |     |                                    |
| `tozai_shozoku_code`        | 調教師東西所属コード    | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.東西所属コード を参照     |
| `shotai_chiikimei`          | 招待地域名              | `character varying(20)`   | `character varying(20)`   | YES      |     |                                    |
| `jushoshori_joho_1`         | 最近重賞勝利情報1       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                    |
| `jushoshori_joho_2`         | 最近重賞勝利情報2       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                    |
| `jushoshori_joho_3`         | 最近重賞勝利情報3       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                    |
| `seiseki_joho_1`            | 本年･前年･累計成績情報1 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                    |
| `seiseki_joho_2`            | 本年･前年･累計成績情報2 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                    |
| `seiseki_joho_3`            | 本年･前年･累計成績情報3 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                    |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_ck`

- Logical name: 出走別着度数
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「出走別着度数」。
- Rows: 0
- Columns: 106
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango`
- Excel indexes: nvd_ck_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango)

| column                        | logical name             | db type                   | Excel type                | nullable | key | reference / note                            |
| ----------------------------- | ------------------------ | ------------------------- | ------------------------- | -------- | --- | ------------------------------------------- |
| `record_id`                   | レコード種別ID           | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `data_kubun`                  | データ区分               | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `data_sakusei_nengappi`       | データ作成年月日         | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `kaisai_nen`                  | 開催年                   | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_tsukihi`              | 開催月日                 | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `keibajo_code`                | 競馬場コード             | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_kai`                  | 開催回[第N回]            | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `kaisai_nichime`              | 開催日目[N日目]          | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `race_bango`                  | レース番号               | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `ketto_toroku_bango`          | 血統登録番号             | `character varying(10)`   | `character varying(10)`   | NO       | PK  | 競走馬マスタ.血統登録番号への外部キーリンク |
| `bamei`                       | 馬名                     | `character varying(36)`   | `character varying(36)`   | YES      |     |                                             |
| `heichi_honshokin_ruikei`     | 平地本賞金累計           | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `shogai_honshokin_ruikei`     | 障害本賞金累計           | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `heichi_fukashokin_ruikei`    | 平地付加賞金累計         | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `shogai_fukashokin_ruikei`    | 障害付加賞金累計         | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `heichi_shutokushokin_ruikei` | 平地収得賞金累計         | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `shogai_shutokushokin_ruikei` | 障害収得賞金累計         | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `sogo`                        | 総合着回数               | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `chuo_gokei`                  | 中央合計着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_choku`                 | 芝直・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_migi`                  | 芝右・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_hidari`                | 芝左・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_choku`                  | ダ直・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_migi`                   | ダ右・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_hidari`                 | ダ左・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai`                      | 障害・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_ryo`                   | 芝良・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_yayaomo`               | 芝稍・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_omo`                   | 芝重・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_furyo`                 | 芝不・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_ryo`                    | ダ良・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_yayaomo`                | ダ稍・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_omo`                    | ダ重・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_furyo`                  | ダ不・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_ryo`                  | 障良・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_yayaomo`              | 障稍・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_omo`                  | 障重・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_furyo`                | 障不・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1200_ika`              | 芝1200以下・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1201_1400`             | 芝1201-1400・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1401_1600`             | 芝1401-1600・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1601_1800`             | 芝1601-1800・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1801_2000`             | 芝1801-2000・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_2001_2200`             | 芝2001-2200・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_2201_2400`             | 芝2201-2400・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_2401_2800`             | 芝2401-2800・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_2801_ijo`              | 芝2801以上・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1200_ika`               | ダ1200以下・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1201_1400`              | ダ1201-1400・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1401_1600`              | ダ1401-1600・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1601_1800`              | ダ1601-1800・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1801_2000`              | ダ1801-2000・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_2001_2200`              | ダ2001-2200・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_2201_2400`              | ダ2201-2400・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_2401_2800`              | ダ2401-2800・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_2801_ijo`               | ダ2801以上・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_sapporo`               | 札幌芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_hakodate`              | 函館芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_fukushima`             | 福島芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_niigata`               | 新潟芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_tokyo`                 | 東京芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_nakayama`              | 中山芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_chukyo`                | 中京芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_kyoto`                 | 京都芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_hanshin`               | 阪神芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_kokura`                | 小倉芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_sapporo`                | 札幌ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_hakodate`               | 函館ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_fukushima`              | 福島ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_niigata`                | 新潟ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_tokyo`                  | 東京ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_nakayama`               | 中山ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_chukyo`                 | 中京ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kyoto`                  | 京都ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_hanshin`                | 阪神ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kokura`                 | 小倉ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_sapporo`              | 札幌障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_hakodate`             | 函館障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_fukushima`            | 福島障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_niigata`              | 新潟障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_tokyo`                | 東京障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_nakayama`             | 中山障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_chukyo`               | 中京障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_kyoto`                | 京都障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_hanshin`              | 阪神障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shogai_kokura`               | 小倉障・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `kyakushitsu_keiko`           | 脚質傾向                 | `character varying(12)`   | `character varying(12)`   | YES      |     |                                             |
| `toroku_race_su`              | 登録レース数             | `character varying(3)`    | `character varying(3)`    | YES      |     |                                             |
| `kishu_code`                  | 騎手コード               | `character varying(5)`    | `character varying(5)`    | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク     |
| `kishumei`                    | 騎手名                   | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `seiseki_joho_kishu_1`        | 騎手本年･累計成績情報1   | `character varying(1220)` | `character varying(1220)` | YES      |     |                                             |
| `seiseki_joho_kishu_2`        | 騎手本年･累計成績情報2   | `character varying(1220)` | `character varying(1220)` | YES      |     |                                             |
| `chokyoshi_code`              | 調教師コード             | `character varying(5)`    | `character varying(5)`    | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei`                | 調教師名                 | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `seiseki_joho_chokyoshi_1`    | 調教師本年･累計成績情報1 | `character varying(1220)` | `character varying(1220)` | YES      |     |                                             |
| `seiseki_joho_chokyoshi_2`    | 調教師本年･累計成績情報2 | `character varying(1220)` | `character varying(1220)` | YES      |     |                                             |
| `banushi_code`                | 馬主コード               | `character varying(6)`    | `character varying(6)`    | YES      |     | 馬主マスタ.馬主コードへの外部キーリンク     |
| `banushimei_hojinkaku`        | 馬主名(法人格有)         | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `banushimei`                  | 馬主名(法人格無)         | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `seiseki_joho_banushi_1`      | 馬主本年･累計成績情報1   | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `seiseki_joho_banushi_2`      | 馬主本年･累計成績情報2   | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `seisansha_code`              | 生産者コード             | `character varying(6)`    | `character varying(6)`    | YES      |     | 生産者マスタ.生産者コードへの外部キーリンク |
| `seisanshamei_hojinkaku`      | 生産者名(法人格有)       | `character varying(70)`   | `character varying(70)`   | YES      |     |                                             |
| `seisanshamei`                | 生産者名(法人格無)       | `character varying(70)`   | `character varying(70)`   | YES      |     |                                             |
| `seiseki_joho_seisansha_1`    | 生産者本年･累計成績情報1 | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `seiseki_joho_seisansha_2`    | 生産者本年･累計成績情報2 | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_h1`

- Logical name: 票数1
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「票数1」。
- Rows: 0
- Columns: 43
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: nvd_h1_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                          | logical name             | db type                    | Excel type                 | nullable | key | reference / note                     |
| ------------------------------- | ------------------------ | -------------------------- | -------------------------- | -------- | --- | ------------------------------------ |
| `record_id`                     | レコード種別ID           | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `data_kubun`                    | データ区分               | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `data_sakusei_nengappi`         | データ作成年月日         | `character varying(8)`     | `character varying(8)`     | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`                    | 開催年                   | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`                | 開催月日                 | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`                  | 競馬場コード             | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`                    | 開催回[第N回]            | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `kaisai_nichime`                | 開催日目[N日目]          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `race_bango`                    | レース番号               | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `toroku_tosu`                   | 登録頭数                 | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `shusso_tosu`                   | 出走頭数                 | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `hatsubai_flag_tansho`          | 発売フラグ　単勝         | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_fukusho`         | 発売フラグ　複勝         | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_wakuren`         | 発売フラグ　枠連         | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_umaren`          | 発売フラグ　馬連         | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_wide`            | 発売フラグ　ワイド       | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_umatan`          | 発売フラグ　馬単         | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `hatsubai_flag_sanrenpuku`      | 発売フラグ　3連複        | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `fukusho_chakubarai_key`        | 複勝着払キー             | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `henkan_umaban_joho`            | 返還馬番情報(馬番01～28) | `character varying(28)`    | `character varying(28)`    | YES      |     |                                      |
| `henkan_wakuban_joho`           | 返還枠番情報(枠番1～8)   | `character varying(8)`     | `character varying(8)`     | YES      |     |                                      |
| `henkan_dowaku_joho`            | 返還同枠情報(枠番1～8)   | `character varying(8)`     | `character varying(8)`     | YES      |     |                                      |
| `hyosu_tansho`                  | 単勝票数                 | `character varying(420)`   | `character varying(420)`   | YES      |     |                                      |
| `hyosu_fukusho`                 | 複勝票数                 | `character varying(420)`   | `character varying(420)`   | YES      |     |                                      |
| `hyosu_wakuren`                 | 枠連票数                 | `character varying(540)`   | `character varying(540)`   | YES      |     |                                      |
| `hyosu_umaren`                  | 馬連票数                 | `character varying(2754)`  | `character varying(2754)`  | YES      |     |                                      |
| `hyosu_wide`                    | ワイド票数               | `character varying(2754)`  | `character varying(2754)`  | YES      |     |                                      |
| `hyosu_umatan`                  | 馬単票数                 | `character varying(5508)`  | `character varying(5508)`  | YES      |     |                                      |
| `hyosu_sanrenpuku`              | 3連複票数                | `character varying(16320)` | `character varying(16320)` | YES      |     |                                      |
| `hyosu_gokei_tansho`            | 単勝票数合計             | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_fukusho`           | 複勝票数合計             | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_wakuren`           | 枠連票数合計             | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_umaren`            | 馬連票数合計             | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_wide`              | ワイド票数合計           | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_umatan`            | 馬単票数合計             | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `hyosu_gokei_sanrenpuku`        | 3連複票数合計            | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_tansho`     | 単勝返還票数合計         | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_fukusho`    | 複勝返還票数合計         | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_wakuren`    | 枠連返還票数合計         | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_umaren`     | 馬連返還票数合計         | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_wide`       | ワイド返還票数合計       | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_umatan`     | 馬単返還票数合計         | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |
| `henkan_hyosu_gokei_sanrenpuku` | 3連複返還票数合計        | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_h6`

- Logical name: 票数6(3連単)
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「票数6(3連単)」。
- Rows: 0
- Columns: 16
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: nvd_h6_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                         | logical name             | db type                     | Excel type                  | nullable | key | reference / note                     |
| ------------------------------ | ------------------------ | --------------------------- | --------------------------- | -------- | --- | ------------------------------------ |
| `record_id`                    | レコード種別ID           | `character varying(2)`      | `character varying(2)`      | YES      |     |                                      |
| `data_kubun`                   | データ区分               | `character varying(1)`      | `character varying(1)`      | YES      |     |                                      |
| `data_sakusei_nengappi`        | データ作成年月日         | `character varying(8)`      | `character varying(8)`      | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`                   | 開催年                   | `character varying(4)`      | `character varying(4)`      | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`               | 開催月日                 | `character varying(4)`      | `character varying(4)`      | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`                 | 競馬場コード             | `character varying(2)`      | `character varying(2)`      | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`                   | 開催回[第N回]            | `character varying(2)`      | `character varying(2)`      | YES      |     |                                      |
| `kaisai_nichime`               | 開催日目[N日目]          | `character varying(2)`      | `character varying(2)`      | YES      |     |                                      |
| `race_bango`                   | レース番号               | `character varying(2)`      | `character varying(2)`      | NO       | PK  | レースを一意に識別するための複合キー |
| `toroku_tosu`                  | 登録頭数                 | `character varying(2)`      | `character varying(2)`      | YES      |     |                                      |
| `shusso_tosu`                  | 出走頭数                 | `character varying(2)`      | `character varying(2)`      | YES      |     |                                      |
| `hatsubai_flag_sanrentan`      | 発売フラグ　3連単        | `character varying(1)`      | `character varying(1)`      | YES      |     |                                      |
| `henkan_umaban_joho`           | 返還馬番情報(馬番01～18) | `character varying(18)`     | `character varying(18)`     | YES      |     |                                      |
| `hyosu_sanrentan`              | 3連単票数                | `character varying(102816)` | `character varying(102816)` | YES      |     |                                      |
| `hyosu_gokei_sanrentan`        | 3連単票数合計            | `character varying(11)`     | `character varying(11)`     | YES      |     |                                      |
| `henkan_hyosu_gokei_sanrentan` | 3連単返還票数合計        | `character varying(11)`     | `character varying(11)`     | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_ha`

- Logical name: 票数A(枠単)
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「票数A(枠単)」。
- Rows: 0
- Columns: 17
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: nvd_ha_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                       | logical name           | db type                  | Excel type               | nullable | key | reference / note                     |
| ---------------------------- | ---------------------- | ------------------------ | ------------------------ | -------- | --- | ------------------------------------ |
| `record_id`                  | レコード種別ID         | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `data_kubun`                 | データ区分             | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `data_sakusei_nengappi`      | データ作成年月日       | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`                 | 開催年                 | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`             | 開催月日               | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`               | 競馬場コード           | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`                 | 開催回[第N回]          | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `kaisai_nichime`             | 開催日目[N日目]        | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `race_bango`                 | レース番号             | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `toroku_tosu`                | 登録頭数               | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `shusso_tosu`                | 出走頭数               | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `hatsubai_flag_wakutan`      | 発売フラグ　枠単       | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `henkan_wakuban_joho`        | 返還枠番情報(枠番1～8) | `character varying(8)`   | `character varying(8)`   | YES      |     |                                      |
| `henkan_dowaku_joho`         | 返還同枠情報(枠番1～8) | `character varying(8)`   | `character varying(8)`   | YES      |     |                                      |
| `hyosu_wakutan`              | 枠単票数               | `character varying(960)` | `character varying(960)` | YES      |     |                                      |
| `hyosu_gokei_wakutan`        | 枠単票数合計           | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |
| `henkan_hyosu_gokei_wakutan` | 枠単返還票数合計       | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_hr`

- Logical name: 払戻
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「払戻」。
- Rows: 324651
- Columns: 159
- Total size: 346 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_hr_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                       | logical name             | db type                 | Excel type              | nullable | key | reference / note                     |
| ---------------------------- | ------------------------ | ----------------------- | ----------------------- | -------- | --- | ------------------------------------ |
| `record_id`                  | レコード種別ID           | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `data_kubun`                 | データ区分               | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `data_sakusei_nengappi`      | データ作成年月日         | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`                 | 開催年                   | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`             | 開催月日                 | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`               | 競馬場コード             | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`                 | 開催回[第N回]            | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `kaisai_nichime`             | 開催日目[N日目]          | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `race_bango`                 | レース番号               | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `toroku_tosu`                | 登録頭数                 | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `shusso_tosu`                | 出走頭数                 | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `fuseiritsu_flag_tansho`     | 不成立フラグ　単勝       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_fukusho`    | 不成立フラグ　複勝       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_wakuren`    | 不成立フラグ　枠連       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_umaren`     | 不成立フラグ　馬連       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_wide`       | 不成立フラグ　ワイド     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_wakutan`    | 不成立フラグ　枠単       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_umatan`     | 不成立フラグ　馬単       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_sanrenpuku` | 不成立フラグ　3連複      | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `fuseiritsu_flag_sanrentan`  | 不成立フラグ　3連単      | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_tansho`      | 特払フラグ　単勝         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_fukusho`     | 特払フラグ　複勝         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_wakuren`     | 特払フラグ　枠連         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_umaren`      | 特払フラグ　馬連         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_wide`        | 特払フラグ　ワイド       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_wakutan`     | 特払フラグ　枠単         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_umatan`      | 特払フラグ　馬単         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_sanrenpuku`  | 特払フラグ　3連複        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `tokubarai_flag_sanrentan`   | 特払フラグ　3連単        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_tansho`         | 返還フラグ　単勝         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_fukusho`        | 返還フラグ　複勝         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_wakuren`        | 返還フラグ　枠連         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_umaren`         | 返還フラグ　馬連         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_wide`           | 返還フラグ　ワイド       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_wakutan`        | 返還フラグ　枠単         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_umatan`         | 返還フラグ　馬単         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_sanrenpuku`     | 返還フラグ　3連複        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_flag_sanrentan`      | 返還フラグ　3連単        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `henkan_umaban_joho`         | 返還馬番情報(馬番01～28) | `character varying(28)` | `character varying(28)` | YES      |     |                                      |
| `henkan_wakuban_joho`        | 返還枠番情報(枠番1～8)   | `character varying(8)`  | `character varying(8)`  | YES      |     |                                      |
| `henkan_dowaku_joho`         | 返還同枠情報(枠番1～8)   | `character varying(8)`  | `character varying(8)`  | YES      |     |                                      |
| `haraimodoshi_tansho_1a`     | 単勝払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_tansho_1b`     | 単勝払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_tansho_1c`     | 単勝払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_tansho_2a`     | 単勝払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_tansho_2b`     | 単勝払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_tansho_2c`     | 単勝払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_tansho_3a`     | 単勝払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_tansho_3b`     | 単勝払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_tansho_3c`     | 単勝払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_1a`    | 複勝払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_1b`    | 複勝払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_1c`    | 複勝払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_2a`    | 複勝払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_2b`    | 複勝払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_2c`    | 複勝払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_3a`    | 複勝払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_3b`    | 複勝払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_3c`    | 複勝払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_4a`    | 複勝払戻4                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_4b`    | 複勝払戻4                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_4c`    | 複勝払戻4                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_5a`    | 複勝払戻5                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_5b`    | 複勝払戻5                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_fukusho_5c`    | 複勝払戻5                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_1a`    | 枠連払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_1b`    | 枠連払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_1c`    | 枠連払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_2a`    | 枠連払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_2b`    | 枠連払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_2c`    | 枠連払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_3a`    | 枠連払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_3b`    | 枠連払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wakuren_3c`    | 枠連払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_umaren_1a`     | 馬連払戻1                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umaren_1b`     | 馬連払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umaren_1c`     | 馬連払戻1                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umaren_2a`     | 馬連払戻2                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umaren_2b`     | 馬連払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umaren_2c`     | 馬連払戻2                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umaren_3a`     | 馬連払戻3                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umaren_3b`     | 馬連払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umaren_3c`     | 馬連払戻3                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_1a`       | ワイド払戻1              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_1b`       | ワイド払戻1              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_1c`       | ワイド払戻1              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_2a`       | ワイド払戻2              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_2b`       | ワイド払戻2              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_2c`       | ワイド払戻2              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_3a`       | ワイド払戻3              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_3b`       | ワイド払戻3              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_3c`       | ワイド払戻3              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_4a`       | ワイド払戻4              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_4b`       | ワイド払戻4              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_4c`       | ワイド払戻4              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_5a`       | ワイド払戻5              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_5b`       | ワイド払戻5              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_5c`       | ワイド払戻5              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_6a`       | ワイド払戻6              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_6b`       | ワイド払戻6              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_6c`       | ワイド払戻6              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wide_7a`       | ワイド払戻7              | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_wide_7b`       | ワイド払戻7              | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wide_7c`       | ワイド払戻7              | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_wakutan_1a`    | 枠単払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakutan_1b`    | 枠単払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wakutan_1c`    | 枠単払戻1                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakutan_2a`    | 枠単払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakutan_2b`    | 枠単払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wakutan_2c`    | 枠単払戻2                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakutan_3a`    | 枠単払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `haraimodoshi_wakutan_3b`    | 枠単払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_wakutan_3c`    | 枠単払戻3                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `yobi_1`                     | 予備                     | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_1a`     | 馬単払戻1                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_1b`     | 馬単払戻1                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_1c`     | 馬単払戻1                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_2a`     | 馬単払戻2                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_2b`     | 馬単払戻2                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_2c`     | 馬単払戻2                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_3a`     | 馬単払戻3                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_3b`     | 馬単払戻3                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_3c`     | 馬単払戻3                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_4a`     | 馬単払戻4                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_4b`     | 馬単払戻4                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_4c`     | 馬単払戻4                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_5a`     | 馬単払戻5                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_5b`     | 馬単払戻5                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_5c`     | 馬単払戻5                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_umatan_6a`     | 馬単払戻6                | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_umatan_6b`     | 馬単払戻6                | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_umatan_6c`     | 馬単払戻6                | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_1a` | 3連複払戻1               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_1b` | 3連複払戻1               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_1c` | 3連複払戻1               | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_2a` | 3連複払戻2               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_2b` | 3連複払戻2               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_2c` | 3連複払戻2               | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_3a` | 3連複払戻3               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_3b` | 3連複払戻3               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrenpuku_3c` | 3連複払戻3               | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_1a`  | 3連単払戻1               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_1b`  | 3連単払戻1               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_1c`  | 3連単払戻1               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_2a`  | 3連単払戻2               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_2b`  | 3連単払戻2               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_2c`  | 3連単払戻2               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_3a`  | 3連単払戻3               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_3b`  | 3連単払戻3               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_3c`  | 3連単払戻3               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_4a`  | 3連単払戻4               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_4b`  | 3連単払戻4               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_4c`  | 3連単払戻4               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_5a`  | 3連単払戻5               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_5b`  | 3連単払戻5               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_5c`  | 3連単払戻5               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_6a`  | 3連単払戻6               | `character varying(6)`  | `character varying(6)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_6b`  | 3連単払戻6               | `character varying(9)`  | `character varying(9)`  | YES      |     |                                      |
| `haraimodoshi_sanrentan_6c`  | 3連単払戻6               | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_jc`

- Logical name: 騎手変更
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「騎手変更」。
- Rows: 0
- Columns: 20
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, happyo_tsukihi_jifun, umaban`
- Excel indexes: nvd_jc_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,happyo_tsukihi_jifun,umaban)

| column                        | logical name         | db type                 | Excel type              | nullable | key | reference / note                        |
| ----------------------------- | -------------------- | ----------------------- | ----------------------- | -------- | --- | --------------------------------------- |
| `record_id`                   | レコード種別ID       | `character varying(2)`  | `character varying(2)`  | YES      |     |                                         |
| `data_kubun`                  | データ区分           | `character varying(1)`  | `character varying(1)`  | YES      |     |                                         |
| `data_sakusei_nengappi`       | データ作成年月日     | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                      |
| `kaisai_nen`                  | 開催年               | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー    |
| `kaisai_tsukihi`              | 開催月日             | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー    |
| `keibajo_code`                | 競馬場コード         | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー    |
| `kaisai_kai`                  | 開催回[第N回]        | `character varying(2)`  | `character varying(2)`  | YES      |     |                                         |
| `kaisai_nichime`              | 開催日目[N日目]      | `character varying(2)`  | `character varying(2)`  | YES      |     |                                         |
| `race_bango`                  | レース番号           | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー    |
| `happyo_tsukihi_jifun`        | 発表月日時分         | `character varying(8)`  | `character varying(8)`  | NO       | PK  | mmddHHmm形式で設定                      |
| `umaban`                      | 馬番                 | `character varying(2)`  | `character varying(2)`  | NO       | PK  |                                         |
| `bamei`                       | 馬名                 | `character varying(36)` | `character varying(36)` | YES      |     |                                         |
| `futan_juryo`                 | 負担重量             | `character varying(3)`  | `character varying(3)`  | YES      |     |                                         |
| `kishu_code`                  | 騎手コード           | `character varying(5)`  | `character varying(5)`  | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク |
| `kishumei`                    | 騎手名               | `character varying(34)` | `character varying(34)` | YES      |     |                                         |
| `kishu_minarai_code`          | 騎手見習コード       | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.騎手見習コード を参照          |
| `futan_juryo_henkomae`        | 変更前負担重量       | `character varying(3)`  | `character varying(3)`  | YES      |     |                                         |
| `kishu_code_henkomae`         | 変更前騎手コード     | `character varying(5)`  | `character varying(5)`  | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク |
| `kishumei_henkomae`           | 変更前騎手名         | `character varying(34)` | `character varying(34)` | YES      |     |                                         |
| `kishu_minarai_code_henkomae` | 変更前騎手見習コード | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.騎手見習コード を参照          |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_ks`

- Logical name: 騎手マスタ
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「騎手マスタ」。
- Rows: 1871
- Columns: 30
- Total size: 6088 kB
- Primary key: `kishu_code`
- Excel indexes: nvd_ks_pk(kishu_code)

| column                   | logical name            | db type                   | Excel type                | nullable | key | reference / note                            |
| ------------------------ | ----------------------- | ------------------------- | ------------------------- | -------- | --- | ------------------------------------------- |
| `record_id`              | レコード種別ID          | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `data_kubun`             | データ区分              | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `data_sakusei_nengappi`  | データ作成年月日        | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `kishu_code`             | 騎手コード              | `character varying(5)`    | `character varying(5)`    | NO       | PK  | 騎手を一意に識別するためのコード            |
| `massho_kubun`           | 騎手抹消区分            | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `menkyo_kofu_nengappi`   | 騎手免許交付年月日      | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `menkyo_massho_nengappi` | 騎手免許抹消年月日      | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `seinengappi`            | 生年月日                | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `kishumei`               | 騎手名                  | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `yobi_1`                 | 予備                    | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `kishumei_hankaku_kana`  | 騎手名半角ｶﾅ            | `character varying(30)`   | `character varying(30)`   | YES      |     |                                             |
| `kishumei_ryakusho`      | 騎手名略称              | `character varying(8)`    | `character varying(8)`    | YES      |     |                                             |
| `kishumei_eur`           | 騎手名欧字              | `character varying(80)`   | `character varying(80)`   | YES      |     |                                             |
| `seibetsu_kubun`         | 性別区分                | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `kijo_shikaku_code`      | 騎乗資格コード          | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.騎乗資格コード を参照              |
| `kishu_minarai_code`     | 騎手見習コード          | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.騎手見習コード を参照              |
| `tozai_shozoku_code`     | 騎手東西所属コード      | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.東西所属コード を参照              |
| `shotai_chiikimei`       | 招待地域名              | `character varying(20)`   | `character varying(20)`   | YES      |     |                                             |
| `chokyoshi_code`         | 所属調教師コード        | `character varying(5)`    | `character varying(5)`    | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei_ryakusho`  | 所属調教師名略称        | `character varying(8)`    | `character varying(8)`    | YES      |     |                                             |
| `hatsukijo_joho_1`       | 初騎乗情報1             | `character varying(67)`   | `character varying(67)`   | YES      |     |                                             |
| `hatsukijo_joho_2`       | 初騎乗情報2             | `character varying(67)`   | `character varying(67)`   | YES      |     |                                             |
| `hatsushori_joho_1`      | 初勝利情報1             | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `hatsushori_joho_2`      | 初勝利情報2             | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `jushoshori_joho_1`      | 最近重賞勝利情報1       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                             |
| `jushoshori_joho_2`      | 最近重賞勝利情報2       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                             |
| `jushoshori_joho_3`      | 最近重賞勝利情報3       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                             |
| `seiseki_joho_1`         | 本年･前年･累計成績情報1 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                             |
| `seiseki_joho_2`         | 本年･前年･累計成績情報2 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                             |
| `seiseki_joho_3`         | 本年･前年･累計成績情報3 | `character varying(1052)` | `character varying(1052)` | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_nb`

- Logical name: 生産者マスタ地方
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「生産者マスタ地方」。
- Rows: 5542
- Columns: 11
- Total size: 9480 kB
- Primary key: `seisansha_code, seisanshamei`
- Excel indexes: nvd_nb_pk(seisansha_code,seisanshamei)

| column                        | logical name       | db type                  | Excel type               | nullable | key | reference / note                   |
| ----------------------------- | ------------------ | ------------------------ | ------------------------ | -------- | --- | ---------------------------------- |
| `record_id`                   | レコード種別ID     | `character varying(2)`   | `character varying(2)`   | YES      |     |                                    |
| `data_kubun`                  | データ区分         | `character varying(1)`   | `character varying(1)`   | YES      |     |                                    |
| `data_sakusei_nengappi`       | データ作成年月日   | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                 |
| `seisansha_code`              | 生産者コード       | `character varying(8)`   | `character varying(8)`   | NO       | PK  | 生産者を一意に識別するためのコード |
| `seisanshamei_hojinkaku`      | 生産者名(法人格有) | `character varying(72)`  | `character varying(72)`  | YES      |     |                                    |
| `seisanshamei`                | 生産者名(法人格無) | `character varying(72)`  | `character varying(72)`  | NO       | PK  |                                    |
| `seisanshamei_hankaku_kana`   | 生産者名半角ｶﾅ     | `character varying(72)`  | `character varying(72)`  | YES      |     |                                    |
| `seisanshamei_eur`            | 生産者名欧字       | `character varying(168)` | `character varying(168)` | YES      |     |                                    |
| `seisansha_jusho_jichishomei` | 生産者住所自治省名 | `character varying(20)`  | `character varying(20)`  | YES      |     |                                    |
| `seiseki_joho_1`              | 本年･累計成績情報1 | `character varying(116)` | `character varying(116)` | YES      |     |                                    |
| `seiseki_joho_2`              | 本年･累計成績情報2 | `character varying(116)` | `character varying(116)` | YES      |     |                                    |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_nc`

- Logical name: 調教師マスタ地方
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「調教師マスタ地方」。
- Rows: 1809
- Columns: 21
- Total size: 4728 kB
- Primary key: `chokyoshi_code`
- Excel indexes: nvd_nc_pk(chokyoshi_code)

| column                      | logical name            | db type                   | Excel type                | nullable | key | reference / note                   |
| --------------------------- | ----------------------- | ------------------------- | ------------------------- | -------- | --- | ---------------------------------- |
| `record_id`                 | レコード種別ID          | `character varying(2)`    | `character varying(2)`    | YES      |     |                                    |
| `data_kubun`                | データ区分              | `character varying(1)`    | `character varying(1)`    | YES      |     |                                    |
| `data_sakusei_nengappi`     | データ作成年月日        | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `chokyoshi_code`            | 調教師コード            | `character varying(5)`    | `character varying(5)`    | NO       | PK  | 調教師を一意に識別するためのコード |
| `massho_kubun`              | 調教師抹消区分          | `character varying(1)`    | `character varying(1)`    | YES      |     |                                    |
| `menkyo_kofu_nengappi`      | 調教師免許交付年月日    | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `menkyo_massho_nengappi`    | 調教師免許抹消年月日    | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `seinengappi`               | 生年月日                | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                 |
| `chokyoshimei`              | 調教師名                | `character varying(34)`   | `character varying(34)`   | YES      |     |                                    |
| `chokyoshimei_hankaku_kana` | 調教師名半角ｶﾅ          | `character varying(30)`   | `character varying(30)`   | YES      |     |                                    |
| `chokyoshimei_ryakusho`     | 調教師名略称            | `character varying(8)`    | `character varying(8)`    | YES      |     |                                    |
| `chokyoshimei_eur`          | 調教師名欧字            | `character varying(80)`   | `character varying(80)`   | YES      |     |                                    |
| `seibetsu_kubun`            | 性別区分                | `character varying(1)`    | `character varying(1)`    | YES      |     |                                    |
| `tozai_shozoku_code`        | 調教師東西所属コード    | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.東西所属コード を参照     |
| `shotai_chiikimei`          | 招待地域名              | `character varying(20)`   | `character varying(20)`   | YES      |     |                                    |
| `jushoshori_joho_1`         | 最近重賞勝利情報1       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                    |
| `jushoshori_joho_2`         | 最近重賞勝利情報2       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                    |
| `jushoshori_joho_3`         | 最近重賞勝利情報3       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                    |
| `seiseki_joho_1`            | 本年･前年･累計成績情報1 | `character varying(1464)` | `character varying(1464)` | YES      |     |                                    |
| `seiseki_joho_2`            | 本年･前年･累計成績情報2 | `character varying(1464)` | `character varying(1464)` | YES      |     |                                    |
| `seiseki_joho_3`            | 本年･前年･累計成績情報3 | `character varying(1464)` | `character varying(1464)` | YES      |     |                                    |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_nd`

- Logical name: 出走別着度数地方
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「出走別着度数地方」。
- Rows: 0
- Columns: 110
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango`
- Excel indexes: nvd_nd_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango)

| column                        | logical name             | db type                   | Excel type                | nullable | key | reference / note                            |
| ----------------------------- | ------------------------ | ------------------------- | ------------------------- | -------- | --- | ------------------------------------------- |
| `record_id`                   | レコード種別ID           | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `data_kubun`                  | データ区分               | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `data_sakusei_nengappi`       | データ作成年月日         | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `kaisai_nen`                  | 開催年                   | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_tsukihi`              | 開催月日                 | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `keibajo_code`                | 競馬場コード             | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_kai`                  | 開催回[第N回]            | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `kaisai_nichime`              | 開催日目[N日目]          | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `race_bango`                  | レース番号               | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー        |
| `ketto_toroku_bango`          | 血統登録番号             | `character varying(10)`   | `character varying(10)`   | NO       | PK  | 競走馬マスタ.血統登録番号への外部キーリンク |
| `bamei`                       | 馬名                     | `character varying(36)`   | `character varying(36)`   | YES      |     |                                             |
| `heichi_honshokin_ruikei`     | 平地本賞金累計           | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `heichi_fukashokin_ruikei`    | 平地付加賞金累計         | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `heichi_shutokushokin_ruikei` | 平地収得賞金累計         | `character varying(9)`    | `character varying(9)`    | YES      |     | 単位：百円                                  |
| `sogo`                        | 総合着回数               | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `chiho_gokei`                 | 地方合計着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_choku`                 | 芝直・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_migi`                  | 芝右・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_hidari`                | 芝左・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_choku`                  | ダ直・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_migi`                   | ダ右・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_hidari`                 | ダ左・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_ryo`                   | 芝良・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_yayaomo`               | 芝稍・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_omo`                   | 芝重・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_furyo`                 | 芝不・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_ryo`                    | ダ良・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_yayaomo`                | ダ稍・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_omo`                    | ダ重・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_furyo`                  | ダ不・着回数             | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1000_ika`              | 芝1000以下・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1001_1200`             | 芝1001-1200・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1201_1300`             | 芝1201-1300・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1301_1400`             | 芝1301-1400・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1401_1500`             | 芝1401-1500・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1501_1600`             | 芝1501-1600・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1601_1700`             | 芝1601-1700・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1701_1800`             | 芝1701-1800・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_1801_2000`             | 芝1801-2000・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_2001_2200`             | 芝2001-2200・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_2201_ijo`              | 芝2201以上・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1000_ika`               | ダ1000以下・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1001_1200`              | ダ1001-1200・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1201_1300`              | ダ1201-1300・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1301_1400`              | ダ1301-1400・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1401_1500`              | ダ1401-1500・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1501_1600`              | ダ1501-1600・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1601_1700`              | ダ1601-1700・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1701_1800`              | ダ1701-1800・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_1801_2000`              | ダ1801-2000・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_2001_2200`              | ダ2001-2200・着回数      | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_2201_ijo`               | ダ2201以上・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_morioka`               | 盛岡芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_sapporo`               | 札幌芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_hakodate`              | 函館芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_niigata`               | 新潟芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `shiba_chukyo`                | 中京芝・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_mombetsu`               | 門別ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kitami`                 | 北見ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_iwamizawa`              | 岩見沢ダ・着回数         | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_obihiro`                | 帯広ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_asahikawa`              | 旭川ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_morioka`                | 盛岡ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_mizusawa`               | 水沢ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kaminoyama`             | 上山ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_sanjo`                  | 三条ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_ashikaga`               | 足利ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_utsunomiya`             | 宇都宮ダ・着回数         | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_takasaki`               | 高崎ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_urawa`                  | 浦和ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_funabashi`              | 船橋ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_oi`                     | 大井ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kawasaki`               | 川崎ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kanazawa`               | 金沢ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kasamatsu`              | 笠松ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_nagoya`                 | 名古屋ダ・着回数         | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kimiidera`              | 紀三井寺ダ・着回数       | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_sonoda`                 | 園田ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_himeji`                 | 姫路ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_masuda`                 | 益田ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_fukuyama`               | 福山ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_kochi`                  | 高知ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_saga`                   | 佐賀ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_arao`                   | 荒尾ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_nakatsu`                | 中津ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_sapporo`                | 札幌ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_hakodate`               | 函館ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_niigata`                | 新潟ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_chukyo`                 | 中京ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `dirt_haruki`                 | 春木ダ・着回数           | `character varying(18)`   | `character varying(18)`   | YES      |     |                                             |
| `kyakushitsu_keiko`           | 脚質傾向                 | `character varying(12)`   | `character varying(12)`   | YES      |     |                                             |
| `toroku_race_su`              | 登録レース数             | `character varying(3)`    | `character varying(3)`    | YES      |     |                                             |
| `kishu_code`                  | 騎手コード               | `character varying(5)`    | `character varying(5)`    | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク     |
| `kishumei`                    | 騎手名                   | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `seiseki_joho_kishu_1`        | 騎手本年･累計成績情報1   | `character varying(1524)` | `character varying(1524)` | YES      |     |                                             |
| `seiseki_joho_kishu_2`        | 騎手本年･累計成績情報2   | `character varying(1524)` | `character varying(1524)` | YES      |     |                                             |
| `chokyoshi_code`              | 調教師コード             | `character varying(5)`    | `character varying(5)`    | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei`                | 調教師名                 | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `seiseki_joho_chokyoshi_1`    | 調教師本年･累計成績情報1 | `character varying(1524)` | `character varying(1524)` | YES      |     |                                             |
| `seiseki_joho_chokyoshi_2`    | 調教師本年･累計成績情報2 | `character varying(1524)` | `character varying(1524)` | YES      |     |                                             |
| `banushi_code`                | 馬主コード               | `character varying(6)`    | `character varying(6)`    | YES      |     | 馬主マスタ.馬主コードへの外部キーリンク     |
| `banushimei_hojinkaku`        | 馬主名(法人格有)         | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `banushimei`                  | 馬主名(法人格無)         | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `seiseki_joho_banushi_1`      | 馬主本年･累計成績情報1   | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `seiseki_joho_banushi_2`      | 馬主本年･累計成績情報2   | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `seisansha_code`              | 生産者コード             | `character varying(8)`    | `character varying(8)`    | YES      |     | 生産者マスタ.生産者コードへの外部キーリンク |
| `seisanshamei_hojinkaku`      | 生産者名(法人格有)       | `character varying(72)`   | `character varying(72)`   | YES      |     |                                             |
| `seisanshamei`                | 生産者名(法人格無)       | `character varying(72)`   | `character varying(72)`   | YES      |     |                                             |
| `seiseki_joho_seisansha_1`    | 生産者本年･累計成績情報1 | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `seiseki_joho_seisansha_2`    | 生産者本年･累計成績情報2 | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_nk`

- Logical name: 騎手マスタ地方
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「騎手マスタ地方」。
- Rows: 2355
- Columns: 32
- Total size: 9536 kB
- Primary key: `kishu_code`
- Excel indexes: nvd_nk_pk(kishu_code)

| column                   | logical name            | db type                   | Excel type                | nullable | key | reference / note                            |
| ------------------------ | ----------------------- | ------------------------- | ------------------------- | -------- | --- | ------------------------------------------- |
| `record_id`              | レコード種別ID          | `character varying(2)`    | `character varying(2)`    | YES      |     |                                             |
| `data_kubun`             | データ区分              | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `data_sakusei_nengappi`  | データ作成年月日        | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `kishu_code`             | 騎手コード              | `character varying(5)`    | `character varying(5)`    | NO       | PK  | 騎手を一意に識別するためのコード            |
| `massho_kubun`           | 騎手抹消区分            | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `menkyo_kofu_nengappi`   | 騎手免許交付年月日      | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `menkyo_massho_nengappi` | 騎手免許抹消年月日      | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `seinengappi`            | 生年月日                | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                          |
| `kishumei`               | 騎手名                  | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `yobi_1`                 | 予備                    | `character varying(34)`   | `character varying(34)`   | YES      |     |                                             |
| `kishumei_hankaku_kana`  | 騎手名半角ｶﾅ            | `character varying(30)`   | `character varying(30)`   | YES      |     |                                             |
| `kishumei_ryakusho`      | 騎手名略称              | `character varying(8)`    | `character varying(8)`    | YES      |     |                                             |
| `kishumei_eur`           | 騎手名欧字              | `character varying(80)`   | `character varying(80)`   | YES      |     |                                             |
| `seibetsu_kubun`         | 性別区分                | `character varying(1)`    | `character varying(1)`    | YES      |     |                                             |
| `kijo_shikaku_code`      | 騎乗資格コード          | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.騎乗資格コード を参照              |
| `kishu_minarai_code`     | 騎手見習コード          | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.騎手見習コード を参照              |
| `tozai_shozoku_code`     | 騎手東西所属コード      | `character varying(1)`    | `character varying(1)`    | YES      |     | コード表.東西所属コード を参照              |
| `shotai_chiikimei`       | 招待地域名              | `character varying(20)`   | `character varying(20)`   | YES      |     |                                             |
| `chokyoshi_code`         | 所属調教師コード        | `character varying(5)`    | `character varying(5)`    | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei_ryakusho`  | 所属調教師名略称        | `character varying(8)`    | `character varying(8)`    | YES      |     |                                             |
| `fukushoku_hyoji_gazo`   | 服色標示（画像取得用）  | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `fukushoku_hyoji_moji`   | 服色標示（文字表示用）  | `character varying(60)`   | `character varying(60)`   | YES      |     |                                             |
| `hatsukijo_joho_1`       | 初騎乗情報1             | `character varying(67)`   | `character varying(67)`   | YES      |     |                                             |
| `hatsukijo_joho_2`       | 初騎乗情報2             | `character varying(67)`   | `character varying(67)`   | YES      |     |                                             |
| `hatsushori_joho_1`      | 初勝利情報1             | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `hatsushori_joho_2`      | 初勝利情報2             | `character varying(64)`   | `character varying(64)`   | YES      |     |                                             |
| `jushoshori_joho_1`      | 最近重賞勝利情報1       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                             |
| `jushoshori_joho_2`      | 最近重賞勝利情報2       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                             |
| `jushoshori_joho_3`      | 最近重賞勝利情報3       | `character varying(163)`  | `character varying(163)`  | YES      |     |                                             |
| `seiseki_joho_1`         | 本年･前年･累計成績情報1 | `character varying(1464)` | `character varying(1464)` | YES      |     |                                             |
| `seiseki_joho_2`         | 本年･前年･累計成績情報2 | `character varying(1464)` | `character varying(1464)` | YES      |     |                                             |
| `seiseki_joho_3`         | 本年･前年･累計成績情報3 | `character varying(1464)` | `character varying(1464)` | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_nn`

- Logical name: 馬主マスタ地方
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「馬主マスタ地方」。
- Rows: 9485
- Columns: 13
- Total size: 20 MB
- Primary key: `banushi_code, banushimei`
- Excel indexes: nvd_nn_pk(banushi_code,banushimei)

| column                    | logical name                        | db type                  | Excel type               | nullable | key | reference / note                 |
| ------------------------- | ----------------------------------- | ------------------------ | ------------------------ | -------- | --- | -------------------------------- |
| `record_id`               | レコード種別ID                      | `character varying(2)`   | `character varying(2)`   | YES      |     |                                  |
| `data_kubun`              | データ区分                          | `character varying(1)`   | `character varying(1)`   | YES      |     |                                  |
| `data_sakusei_nengappi`   | データ作成年月日                    | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定               |
| `banushi_code`            | 馬主コード                          | `character varying(6)`   | `character varying(6)`   | NO       | PK  | 馬主を一意に識別するためのコード |
| `banushimei_hojinkaku`    | 馬主名(法人格有)                    | `character varying(64)`  | `character varying(64)`  | YES      |     |                                  |
| `banushimei`              | 馬主名(法人格無)                    | `character varying(64)`  | `character varying(64)`  | NO       | PK  |                                  |
| `banushimei_hankaku_kana` | 馬主名半角ｶﾅ                        | `character varying(50)`  | `character varying(50)`  | YES      |     |                                  |
| `banushimei_eur`          | 馬主名欧字                          | `character varying(100)` | `character varying(100)` | YES      |     |                                  |
| `fukushoku_hyoji`         | 服色標示 JRA                        | `character varying(60)`  | `character varying(60)`  | YES      |     |                                  |
| `fukushoku_hyoji_gazo`    | 服色標示 ホッカイドウ（画像取得用） | `character varying(60)`  | `character varying(60)`  | YES      |     |                                  |
| `fukushoku_hyoji_moji`    | 服色標示 ホッカイドウ（文字表示用） | `character varying(60)`  | `character varying(60)`  | YES      |     |                                  |
| `seiseki_joho_1`          | 本年･累計成績情報1                  | `character varying(116)` | `character varying(116)` | YES      |     |                                  |
| `seiseki_joho_2`          | 本年･累計成績情報2                  | `character varying(116)` | `character varying(116)` | YES      |     |                                  |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_nr`

- Logical name: 能力試験詳細
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「能力試験詳細」。
- Rows: 3576
- Columns: 31
- Total size: 1952 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_nr_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name         | db type                 | Excel type              | nullable | key | reference / note                     |
| ----------------------- | -------------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID       | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `data_kubun`            | データ区分           | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日     | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年               | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日             | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード         | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `race_bango`            | レース番号           | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `yobi_code`             | 曜日コード           | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.曜日コード を参照           |
| `kyori`                 | 距離                 | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `kyori_henkomae`        | 変更前距離           | `character varying(4)`  | `character varying(4)`  | YES      |     |                                      |
| `track_code`            | トラックコード       | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.トラックコード を参照       |
| `track_code_henkomae`   | 変更前トラックコード | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.トラックコード を参照       |
| `course_kubun`          | コース区分           | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `course_kubun_henkomae` | 変更前コース区分     | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `hasso_jikoku`          | 発走時刻             | `character varying(4)`  | `character varying(4)`  | YES      |     | HHmm形式で設定                       |
| `hasso_jikoku_henkomae` | 変更前発走時刻       | `character varying(4)`  | `character varying(4)`  | YES      |     | HHmm形式で設定                       |
| `toroku_tosu`           | 登録頭数             | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数             | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `nyusen_tosu`           | 入線頭数             | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `tenko_code`            | 天候コード           | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.天候コード を参照           |
| `babajotai_code_shiba`  | 芝馬場状態コード     | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.馬場状態コード を参照       |
| `babajotai_code_dirt`   | ダート馬場状態コード | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.馬場状態コード を参照       |
| `lap_time`              | ラップタイム         | `character varying(75)` | `character varying(75)` | YES      |     |                                      |
| `zenhan_3f`             | 前3ハロン            | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `zenhan_4f`             | 前4ハロン            | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `kohan_3f`              | 後3ハロン            | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `kohan_4f`              | 後4ハロン            | `character varying(3)`  | `character varying(3)`  | YES      |     |                                      |
| `corner_tsuka_juni_1`   | コーナー通過順位1    | `character varying(72)` | `character varying(72)` | YES      |     |                                      |
| `corner_tsuka_juni_2`   | コーナー通過順位2    | `character varying(72)` | `character varying(72)` | YES      |     |                                      |
| `corner_tsuka_juni_3`   | コーナー通過順位3    | `character varying(72)` | `character varying(72)` | YES      |     |                                      |
| `corner_tsuka_juni_4`   | コーナー通過順位4    | `character varying(72)` | `character varying(72)` | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_ns`

- Logical name: 馬毎能力試験情報
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「馬毎能力試験情報」。
- Rows: 17264
- Columns: 47
- Total size: 8856 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, umaban`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_ns_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,umaban)

| column                       | logical name       | db type                 | Excel type              | nullable | key | reference / note                            |
| ---------------------------- | ------------------ | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`                  | レコード種別ID     | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`                 | データ区分         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi`      | データ作成年月日   | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `kaisai_nen`                 | 開催年             | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_tsukihi`             | 開催月日           | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `keibajo_code`               | 競馬場コード       | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `race_bango`                 | レース番号         | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `umaban`                     | 馬番               | `character varying(2)`  | `character varying(2)`  | NO       | PK  |                                             |
| `ketto_toroku_bango`         | 血統登録番号       | `character varying(10)` | `character varying(10)` | YES      |     | 競走馬マスタ.血統登録番号への外部キーリンク |
| `bamei`                      | 馬名               | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `seibetsu_code`              | 性別コード         | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.性別コード を参照                  |
| `hinshu_code`                | 品種コード         | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.品種コード を参照                  |
| `moshoku_code`               | 毛色コード         | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.毛色コード を参照                  |
| `barei`                      | 馬齢               | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `chokyoshi_code`             | 調教師コード       | `character varying(5)`  | `character varying(5)`  | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei_ryakusho`      | 調教師名略称       | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `futan_juryo`                | 負担重量           | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `kishu_code`                 | 騎手コード         | `character varying(5)`  | `character varying(5)`  | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク     |
| `kishu_code_henkomae`        | 変更前騎手コード   | `character varying(5)`  | `character varying(5)`  | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク     |
| `kishumei_ryakusho`          | 騎手名略称         | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `kishumei_ryakusho_henkomae` | 変更前騎手名略称   | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `bataiju`                    | 馬体重             | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `zogen_fugo`                 | 増減符号           | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `zogen_sa`                   | 増減差             | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `ijo_kubun_code`             | 異常区分コード     | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.異常区分コード を参照              |
| `juni`                       | 順位               | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `dochaku_kubun`              | 同着区分           | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `dochaku_tosu`               | 同着頭数           | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `soha_time`                  | 走破タイム         | `character varying(4)`  | `character varying(4)`  | YES      |     | 9分99.9秒で設定                             |
| `chakusa_code_1`             | 着差コード         | `character varying(3)`  | `character varying(3)`  | YES      |     | コード表.着差コード を参照                  |
| `chakusa_code_2`             | ＋着差コード       | `character varying(3)`  | `character varying(3)`  | YES      |     | コード表.着差コード を参照                  |
| `chakusa_code_3`             | ＋＋着差コード     | `character varying(3)`  | `character varying(3)`  | YES      |     | コード表.着差コード を参照                  |
| `noryoku_shiken_code`        | 能力試験種類コード | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.能力試験種類コード を参照          |
| `gohi_code`                  | 合否コード         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `riyu_code`                  | 不合格理由コード   | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.不合格理由コード を参照            |
| `gohi_nengappi`              | 合否年月日         | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `ashiiro_code`               | 脚色コード         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `corner_1`                   | 1コーナーでの順位  | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `corner_2`                   | 2コーナーでの順位  | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `corner_3`                   | 3コーナーでの順位  | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `corner_4`                   | 4コーナーでの順位  | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `kohan_4f`                   | 後4ハロンタイム    | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `kohan_3f`                   | 後3ハロンタイム    | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `aiteuma_joho_1`             | 1着馬(相手馬)情報1 | `character varying(46)` | `character varying(46)` | YES      |     |                                             |
| `aiteuma_joho_2`             | 1着馬(相手馬)情報2 | `character varying(46)` | `character varying(46)` | YES      |     |                                             |
| `aiteuma_joho_3`             | 1着馬(相手馬)情報3 | `character varying(46)` | `character varying(46)` | YES      |     |                                             |
| `kyakushitsu_hantei`         | 今回レース脚質判定 | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_nu`

- Logical name: 競走馬マスタ地方
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「競走馬マスタ地方」。
- Rows: 120159
- Columns: 81
- Total size: 496 MB
- Primary key: `ketto_toroku_bango`
- Excel indexes: nvd_nu_pk(ketto_toroku_bango), nvd_nu_idx1(seinengappi), nvd_nu_idx2(ketto_joho_01a), nvd_nu_idx3(ketto_joho_02a), nvd_nu_idx4(ketto_joho_06a), nvd_nu_idx5(ketto_joho_14a), nvd_nu_idx6(chokyoshi_code), nvd_nu_idx7(seisansha_code), nvd_nu_idx8(banushi_code)

| column                  | logical name                            | db type                 | Excel type              | nullable | key | reference / note                            |
| ----------------------- | --------------------------------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`             | レコード種別ID                          | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`            | データ区分                              | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi` | データ作成年月日                        | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `ketto_toroku_bango`    | 血統登録番号                            | `character varying(10)` | `character varying(10)` | NO       | PK  | 競走馬を一意に識別するための番号            |
| `massho_kubun`          | 競走馬抹消区分                          | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `toroku_nengappi`       | 競走馬登録年月日                        | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `massho_nengappi`       | 競走馬抹消年月日                        | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `seinengappi`           | 生年月日                                | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `bamei`                 | 馬名                                    | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `bamei_hankaku_kana`    | 馬名半角ｶﾅ                              | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `bamei_eur`             | 馬名欧字                                | `character varying(60)` | `character varying(60)` | YES      |     |                                             |
| `zaikyu_flag`           | 在厩フラグ                              | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `yobi_1`                | 予備                                    | `character varying(19)` | `character varying(19)` | YES      |     |                                             |
| `umakigo_code`          | 馬記号コード                            | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.馬記号コード を参照                |
| `seibetsu_code`         | 性別コード                              | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.性別コード を参照                  |
| `hinshu_code`           | 品種コード                              | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.品種コード を参照                  |
| `moshoku_code`          | 毛色コード                              | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.毛色コード を参照                  |
| `ketto_joho_01a`        | 3代血統情報1                            | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_01b`        | 3代血統情報1                            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_02a`        | 3代血統情報2                            | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_02b`        | 3代血統情報2                            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_03a`        | 3代血統情報3                            | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_03b`        | 3代血統情報3                            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_04a`        | 3代血統情報4                            | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_04b`        | 3代血統情報4                            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_05a`        | 3代血統情報5                            | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_05b`        | 3代血統情報5                            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_06a`        | 3代血統情報6                            | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_06b`        | 3代血統情報6                            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_07a`        | 3代血統情報7                            | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_07b`        | 3代血統情報7                            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_08a`        | 3代血統情報8                            | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_08b`        | 3代血統情報8                            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_09a`        | 3代血統情報9                            | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_09b`        | 3代血統情報9                            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_10a`        | 3代血統情報10                           | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_10b`        | 3代血統情報10                           | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_11a`        | 3代血統情報11                           | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_11b`        | 3代血統情報11                           | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_12a`        | 3代血統情報12                           | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_12b`        | 3代血統情報12                           | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_13a`        | 3代血統情報13                           | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_13b`        | 3代血統情報13                           | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_14a`        | 3代血統情報14                           | `character varying(10)` | `character varying(10)` | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_14b`        | 3代血統情報14                           | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `tozai_shozoku_code`    | 東西所属コード                          | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.東西所属コード を参照              |
| `chokyoshi_code`        | 調教師コード                            | `character varying(5)`  | `character varying(5)`  | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei_ryakusho` | 調教師名略称                            | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `shotai_chiikimei`      | 招待地域名                              | `character varying(20)` | `character varying(20)` | YES      |     |                                             |
| `seisansha_code`        | 生産者コード                            | `character varying(8)`  | `character varying(8)`  | YES      |     | 生産者マスタ.生産者コードへの外部キーリンク |
| `seisanshamei`          | 生産者名(法人格無)                      | `character varying(72)` | `character varying(72)` | YES      |     |                                             |
| `sanchimei`             | 産地名                                  | `character varying(20)` | `character varying(20)` | YES      |     |                                             |
| `banushi_code`          | 馬主コード                              | `character varying(6)`  | `character varying(6)`  | YES      |     | 馬主マスタ.馬主コードへの外部キーリンク     |
| `banushimei`            | 馬主名(法人格無)                        | `character varying(64)` | `character varying(64)` | YES      |     |                                             |
| `honshokin_ruikei`      | 本賞金累計                              | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `fukashokin_ruikei`     | 付加賞金累計                            | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `shutokushokin_ruikei`  | 収得賞金累計                            | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `sogo`                  | 総合着回数                              | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `chiho_gokei`           | 地方合計着回数                          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_choku`           | 芝直・着回数                            | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_migi`            | 芝右・着回数                            | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_hidari`          | 芝左・着回数                            | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_choku`            | ダ直・着回数                            | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_migi`             | ダ右・着回数                            | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_hidari`           | ダ左・着回数                            | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_ryo`             | 芝良・着回数（馬場水分0.0～0.9%着回数） | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_yayaomo`         | 芝稍・着回数（馬場水分1.0～1.9%着回数） | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_omo`             | 芝重・着回数（馬場水分2.0～2.9%着回数） | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_furyo`           | 芝不・着回数（馬場水分3.0～3.9%着回数） | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_ryo`              | ダ良・着回数（馬場水分4.0～4.9%着回数） | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_yayaomo`          | ダ稍・着回数（馬場水分5.0～5.9%着回数） | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_omo`              | ダ重・着回数（馬場水分6.0%以上着回数）  | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_furyo`            | ダ不・着回数                            | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_short`           | 芝16下・着回数                          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_middle`          | 芝22下・着回数                          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_long`            | 芝22超・着回数                          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_short`            | ダ16下・着回数                          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_middle`           | ダ22下・着回数                          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_long`             | ダ22超・着回数                          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `kyakushitsu_keiko`     | 脚質傾向                                | `character varying(12)` | `character varying(12)` | YES      |     |                                             |
| `toroku_race_su`        | 登録レース数                            | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_o1`

- Logical name: オッズ1(単複枠)
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「オッズ1(単複枠)」。
- Rows: 290592
- Columns: 22
- Total size: 303 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_o1_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                   | logical name     | db type                  | Excel type               | nullable | key | reference / note                     |
| ------------------------ | ---------------- | ------------------------ | ------------------------ | -------- | --- | ------------------------------------ |
| `record_id`              | レコード種別ID   | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `data_kubun`             | データ区分       | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `data_sakusei_nengappi`  | データ作成年月日 | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`             | 開催年           | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`         | 開催月日         | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`           | 競馬場コード     | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`             | 開催回[第N回]    | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `kaisai_nichime`         | 開催日目[N日目]  | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `race_bango`             | レース番号       | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`   | 発表月日時分     | `character varying(8)`   | `character varying(8)`   | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`            | 登録頭数         | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `shusso_tosu`            | 出走頭数         | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `hatsubai_flag_tansho`   | 発売フラグ　単勝 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `hatsubai_flag_fukusho`  | 発売フラグ　複勝 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `hatsubai_flag_wakuren`  | 発売フラグ　枠連 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `fukusho_chakubarai_key` | 複勝着払キー     | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `odds_tansho`            | 単勝オッズ       | `character varying(224)` | `character varying(224)` | YES      |     |                                      |
| `odds_fukusho`           | 複勝オッズ       | `character varying(336)` | `character varying(336)` | YES      |     |                                      |
| `odds_wakuren`           | 枠連オッズ       | `character varying(324)` | `character varying(324)` | YES      |     |                                      |
| `hyosu_gokei_tansho`     | 単勝票数合計     | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |
| `hyosu_gokei_fukusho`    | 複勝票数合計     | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |
| `hyosu_gokei_wakuren`    | 枠連票数合計     | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_o2`

- Logical name: オッズ2(馬連)
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「オッズ2(馬連)」。
- Rows: 280497
- Columns: 15
- Total size: 166 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_o2_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name     | db type                   | Excel type                | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ------------------------- | ------------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`    | `character varying(8)`    | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `hatsubai_flag_umaren`  | 発売フラグ　馬連 | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `odds_umaren`           | 馬連オッズ       | `character varying(1989)` | `character varying(1989)` | YES      |     |                                      |
| `hyosu_gokei_umaren`    | 馬連票数合計     | `character varying(11)`   | `character varying(11)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_o3`

- Logical name: オッズ3(ワイド)
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「オッズ3(ワイド)」。
- Rows: 243289
- Columns: 15
- Total size: 175 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_o3_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name       | db type                   | Excel type                | nullable | key | reference / note                     |
| ----------------------- | ------------------ | ------------------------- | ------------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID     | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `data_kubun`            | データ区分         | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日   | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年             | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日           | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード       | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]      | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]    | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `race_bango`            | レース番号         | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分       | `character varying(8)`    | `character varying(8)`    | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数           | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数           | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `hatsubai_flag_wide`    | 発売フラグ　ワイド | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `odds_wide`             | ワイドオッズ       | `character varying(2601)` | `character varying(2601)` | YES      |     |                                      |
| `hyosu_gokei_wide`      | ワイド票数合計     | `character varying(11)`   | `character varying(11)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_o4`

- Logical name: オッズ4(馬単)
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「オッズ4(馬単)」。
- Rows: 290592
- Columns: 15
- Total size: 263 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_o4_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name     | db type                   | Excel type                | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ------------------------- | ------------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`    | `character varying(8)`    | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`    | `character varying(4)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`    | `character varying(2)`    | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`    | `character varying(8)`    | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数         | `character varying(2)`    | `character varying(2)`    | YES      |     |                                      |
| `hatsubai_flag_umatan`  | 発売フラグ　馬単 | `character varying(1)`    | `character varying(1)`    | YES      |     |                                      |
| `odds_umatan`           | 馬単オッズ       | `character varying(3978)` | `character varying(3978)` | YES      |     |                                      |
| `hyosu_gokei_umatan`    | 馬単票数合計     | `character varying(11)`   | `character varying(11)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_o5`

- Logical name: オッズ5(3連複)
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「オッズ5(3連複)」。
- Rows: 239829
- Columns: 15
- Total size: 400 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_o5_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                     | logical name      | db type                    | Excel type                 | nullable | key | reference / note                     |
| -------------------------- | ----------------- | -------------------------- | -------------------------- | -------- | --- | ------------------------------------ |
| `record_id`                | レコード種別ID    | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `data_kubun`               | データ区分        | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `data_sakusei_nengappi`    | データ作成年月日  | `character varying(8)`     | `character varying(8)`     | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`               | 開催年            | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`           | 開催月日          | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`             | 競馬場コード      | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`               | 開催回[第N回]     | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `kaisai_nichime`           | 開催日目[N日目]   | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `race_bango`               | レース番号        | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`     | 発表月日時分      | `character varying(8)`     | `character varying(8)`     | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`              | 登録頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `shusso_tosu`              | 出走頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `hatsubai_flag_sanrenpuku` | 発売フラグ　3連複 | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `odds_sanrenpuku`          | 3連複オッズ       | `character varying(12240)` | `character varying(12240)` | YES      |     |                                      |
| `hyosu_gokei_sanrenpuku`   | 3連複票数合計     | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_o6`

- Logical name: オッズ6(3連単)
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「オッズ6(3連単)」。
- Rows: 240005
- Columns: 15
- Total size: 2036 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_o6_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                    | logical name      | db type                    | Excel type                 | nullable | key | reference / note                     |
| ------------------------- | ----------------- | -------------------------- | -------------------------- | -------- | --- | ------------------------------------ |
| `record_id`               | レコード種別ID    | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `data_kubun`              | データ区分        | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `data_sakusei_nengappi`   | データ作成年月日  | `character varying(8)`     | `character varying(8)`     | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`              | 開催年            | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`          | 開催月日          | `character varying(4)`     | `character varying(4)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`            | 競馬場コード      | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`              | 開催回[第N回]     | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `kaisai_nichime`          | 開催日目[N日目]   | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `race_bango`              | レース番号        | `character varying(2)`     | `character varying(2)`     | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`    | 発表月日時分      | `character varying(8)`     | `character varying(8)`     | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`             | 登録頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `shusso_tosu`             | 出走頭数          | `character varying(2)`     | `character varying(2)`     | YES      |     |                                      |
| `hatsubai_flag_sanrentan` | 発売フラグ　3連単 | `character varying(1)`     | `character varying(1)`     | YES      |     |                                      |
| `odds_sanrentan`          | 3連単オッズ       | `character varying(83232)` | `character varying(83232)` | YES      |     |                                      |
| `hyosu_gokei_sanrentan`   | 3連単票数合計     | `character varying(11)`    | `character varying(11)`    | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_oa`

- Logical name: オッズA(枠単)
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「オッズA(枠単)」。
- Rows: 81319
- Columns: 15
- Total size: 60 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_oa_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name     | db type                  | Excel type               | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ------------------------ | ------------------------ | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`   | `character varying(8)`   | YES      |     | mmddHHmm形式で設定                   |
| `toroku_tosu`           | 登録頭数         | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `shusso_tosu`           | 出走頭数         | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `hatsubai_flag_wakutan` | 発売フラグ　枠単 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `odds_wakutan`          | 枠単オッズ       | `character varying(640)` | `character varying(640)` | YES      |     |                                      |
| `hyosu_gokei_wakutan`   | 枠単票数合計     | `character varying(11)`  | `character varying(11)`  | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_ra`

- Logical name: レース詳細
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「レース詳細」。
- Rows: 326807
- Columns: 62
- Total size: 523 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_ra_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango), nvd_ra_idx1(expr)

| column                      | logical name                               | db type                  | Excel type               | nullable | key | reference / note                     |
| --------------------------- | ------------------------------------------ | ------------------------ | ------------------------ | -------- | --- | ------------------------------------ |
| `record_id`                 | レコード種別ID                             | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `data_kubun`                | データ区分                                 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `data_sakusei_nengappi`     | データ作成年月日                           | `character varying(8)`   | `character varying(8)`   | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`                | 開催年                                     | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`            | 開催月日                                   | `character varying(4)`   | `character varying(4)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`              | 競馬場コード                               | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`                | 開催回[第N回]                              | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `kaisai_nichime`            | 開催日目[N日目]                            | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `race_bango`                | レース番号                                 | `character varying(2)`   | `character varying(2)`   | NO       | PK  | レースを一意に識別するための複合キー |
| `yobi_code`                 | 曜日コード                                 | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.曜日コード を参照           |
| `tokubetsu_kyoso_bango`     | 特別競走番号                               | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `kyosomei_hondai`           | 競走名本題                                 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyosomei_fukudai`          | 競走名副題                                 | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyosomei_kakkonai`         | 競走名カッコ内                             | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyosomei_hondai_eur`       | 競走名本題欧字                             | `character varying(120)` | `character varying(120)` | YES      |     |                                      |
| `kyosomei_fukudai_eur`      | 競走名副題欧字                             | `character varying(120)` | `character varying(120)` | YES      |     |                                      |
| `kyosomei_kakkonai_eur`     | 競走名カッコ内欧字                         | `character varying(120)` | `character varying(120)` | YES      |     |                                      |
| `kyosomei_ryakusho_10`      | 競走名略称10文字                           | `character varying(20)`  | `character varying(20)`  | YES      |     |                                      |
| `kyosomei_ryakusho_6`       | 競走名略称6文字                            | `character varying(12)`  | `character varying(12)`  | YES      |     |                                      |
| `kyosomei_ryakusho_3`       | 競走名略称3文字                            | `character varying(6)`   | `character varying(6)`   | YES      |     |                                      |
| `kyosomei_kubun`            | 競走名区分                                 | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |
| `jusho_kaiji`               | 重賞回次[第N回]                            | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `grade_code`                | グレードコード                             | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.グレードコード を参照       |
| `grade_code_henkomae`       | 変更前グレードコード（二重グレードコード） | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.グレードコード を参照       |
| `kyoso_shubetsu_code`       | 競走種別コード                             | `character varying(2)`   | `character varying(2)`   | YES      |     | コード表.競走種別コード を参照       |
| `kyoso_kigo_code`           | 競走記号コード                             | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走記号コード を参照       |
| `juryo_shubetsu_code`       | 重量種別コード                             | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.重量種別コード を参照       |
| `kyoso_joken_code_2sai`     | 競走条件コード 2歳条件                     | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code_3sai`     | 競走条件コード 3歳条件                     | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code_4sai`     | 競走条件コード 4歳条件                     | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code_5sai_ijo` | 競走条件コード 5歳以上条件                 | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_code`          | 競走条件コード 最若年条件                  | `character varying(3)`   | `character varying(3)`   | YES      |     | コード表.競走条件コード を参照       |
| `kyoso_joken_meisho`        | 競走条件名称                               | `character varying(60)`  | `character varying(60)`  | YES      |     |                                      |
| `kyori`                     | 距離                                       | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `kyori_henkomae`            | 変更前距離                                 | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `track_code`                | トラックコード                             | `character varying(2)`   | `character varying(2)`   | YES      |     | コード表.トラックコード を参照       |
| `track_code_henkomae`       | 変更前トラックコード                       | `character varying(2)`   | `character varying(2)`   | YES      |     | コード表.トラックコード を参照       |
| `course_kubun`              | コース区分                                 | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `course_kubun_henkomae`     | 変更前コース区分                           | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `honshokin`                 | 本賞金                                     | `character varying(56)`  | `character varying(56)`  | YES      |     |                                      |
| `honshokin_henkomae`        | 変更前本賞金                               | `character varying(40)`  | `character varying(40)`  | YES      |     |                                      |
| `fukashokin`                | 付加賞金                                   | `character varying(40)`  | `character varying(40)`  | YES      |     |                                      |
| `fukashokin_henkomae`       | 変更前付加賞金                             | `character varying(24)`  | `character varying(24)`  | YES      |     |                                      |
| `hasso_jikoku`              | 発走時刻                                   | `character varying(4)`   | `character varying(4)`   | YES      |     | HHmm形式で設定                       |
| `hasso_jikoku_henkomae`     | 変更前発走時刻                             | `character varying(4)`   | `character varying(4)`   | YES      |     | HHmm形式で設定                       |
| `toroku_tosu`               | 登録頭数                                   | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `shusso_tosu`               | 出走頭数                                   | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `nyusen_tosu`               | 入線頭数                                   | `character varying(2)`   | `character varying(2)`   | YES      |     |                                      |
| `tenko_code`                | 天候コード                                 | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.天候コード を参照           |
| `babajotai_code_shiba`      | 芝馬場状態コード                           | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.馬場状態コード を参照       |
| `babajotai_code_dirt`       | ダート馬場状態コード                       | `character varying(1)`   | `character varying(1)`   | YES      |     | コード表.馬場状態コード を参照       |
| `lap_time`                  | ラップタイム                               | `character varying(75)`  | `character varying(75)`  | YES      |     |                                      |
| `shogai_mile_time`          | 障害マイルタイム                           | `character varying(4)`   | `character varying(4)`   | YES      |     |                                      |
| `zenhan_3f`                 | 前3ハロン                                  | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `zenhan_4f`                 | 前4ハロン                                  | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `kohan_3f`                  | 後3ハロン                                  | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `kohan_4f`                  | 後4ハロン                                  | `character varying(3)`   | `character varying(3)`   | YES      |     |                                      |
| `corner_tsuka_juni_1`       | コーナー通過順位1                          | `character varying(72)`  | `character varying(72)`  | YES      |     |                                      |
| `corner_tsuka_juni_2`       | コーナー通過順位2                          | `character varying(72)`  | `character varying(72)`  | YES      |     |                                      |
| `corner_tsuka_juni_3`       | コーナー通過順位3                          | `character varying(72)`  | `character varying(72)`  | YES      |     |                                      |
| `corner_tsuka_juni_4`       | コーナー通過順位4                          | `character varying(72)`  | `character varying(72)`  | YES      |     |                                      |
| `record_koshin_kubun`       | レコード更新区分                           | `character varying(1)`   | `character varying(1)`   | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_se`

- Logical name: 馬毎レース情報
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「馬毎レース情報」。
- Rows: 1878251
- Columns: 70
- Total size: 1753 MB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango`
- `kaisai_nen || kaisai_tsukihi` range: omitted from public reference
- Excel indexes: nvd_se_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango,ketto_toroku_bango), nvd_se_idx1(expr), nvd_se_idx2(ketto_toroku_bango)

| column                        | logical name                 | db type                 | Excel type              | nullable | key | reference / note                            |
| ----------------------------- | ---------------------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`                   | レコード種別ID               | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`                  | データ区分                   | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi`       | データ作成年月日             | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `kaisai_nen`                  | 開催年                       | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_tsukihi`              | 開催月日                     | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `keibajo_code`                | 競馬場コード                 | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `kaisai_kai`                  | 開催回[第N回]                | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `kaisai_nichime`              | 開催日目[N日目]              | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `race_bango`                  | レース番号                   | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー        |
| `wakuban`                     | 枠番                         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `umaban`                      | 馬番                         | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `ketto_toroku_bango`          | 血統登録番号                 | `character varying(10)` | `character varying(10)` | NO       | PK  | 競走馬マスタ.血統登録番号への外部キーリンク |
| `bamei`                       | 馬名                         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `umakigo_code`                | 馬記号コード                 | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.馬記号コード を参照                |
| `seibetsu_code`               | 性別コード                   | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.性別コード を参照                  |
| `hinshu_code`                 | 品種コード                   | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.品種コード を参照                  |
| `moshoku_code`                | 毛色コード                   | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.毛色コード を参照                  |
| `barei`                       | 馬齢                         | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `tozai_shozoku_code`          | 東西所属コード               | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.東西所属コード を参照              |
| `chokyoshi_code`              | 調教師コード                 | `character varying(5)`  | `character varying(5)`  | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei_ryakusho`       | 調教師名略称                 | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `banushi_code`                | 馬主コード                   | `character varying(6)`  | `character varying(6)`  | YES      |     |                                             |
| `banushimei`                  | 馬主名(法人格無)             | `character varying(64)` | `character varying(64)` | YES      |     |                                             |
| `fukushoku_hyoji`             | 服色標示                     | `character varying(60)` | `character varying(60)` | YES      |     |                                             |
| `yobi_1`                      | 予備                         | `character varying(60)` | `character varying(60)` | YES      |     |                                             |
| `futan_juryo`                 | 負担重量                     | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `futan_juryo_henkomae`        | 変更前負担重量               | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `blinker_shiyo_kubun`         | ブリンカー使用区分           | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `yobi_2`                      | 予備                         | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `kishu_code`                  | 騎手コード                   | `character varying(5)`  | `character varying(5)`  | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク     |
| `kishu_code_henkomae`         | 変更前騎手コード             | `character varying(5)`  | `character varying(5)`  | YES      |     | 騎手マスタ.騎手コードへの外部キーリンク     |
| `kishumei_ryakusho`           | 騎手名略称                   | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `kishumei_ryakusho_henkomae`  | 変更前騎手名略称             | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `kishu_minarai_code`          | 騎手見習コード               | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.騎手見習コード を参照              |
| `kishu_minarai_code_henkomae` | 変更前騎手見習コード         | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.騎手見習コード を参照              |
| `bataiju`                     | 馬体重                       | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `zogen_fugo`                  | 増減符号                     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `zogen_sa`                    | 増減差                       | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `ijo_kubun_code`              | 異常区分コード               | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.異常区分コード を参照              |
| `nyusen_juni`                 | 入線順位                     | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `kakutei_chakujun`            | 確定着順                     | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `dochaku_kubun`               | 同着区分                     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `dochaku_tosu`                | 同着頭数                     | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `soha_time`                   | 走破タイム                   | `character varying(4)`  | `character varying(4)`  | YES      |     | 9分99.9秒で設定                             |
| `chakusa_code_1`              | 着差コード                   | `character varying(3)`  | `character varying(3)`  | YES      |     | コード表.着差コード を参照                  |
| `chakusa_code_2`              | ＋着差コード                 | `character varying(3)`  | `character varying(3)`  | YES      |     | コード表.着差コード を参照                  |
| `chakusa_code_3`              | ＋＋着差コード               | `character varying(3)`  | `character varying(3)`  | YES      |     | コード表.着差コード を参照                  |
| `corner_1`                    | 1コーナーでの順位            | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `corner_2`                    | 2コーナーでの順位            | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `corner_3`                    | 3コーナーでの順位            | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `corner_4`                    | 4コーナーでの順位            | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `tansho_odds`                 | 単勝オッズ                   | `character varying(4)`  | `character varying(4)`  | YES      |     | 999.9倍で設定                               |
| `tansho_ninkijun`             | 単勝人気順                   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `kakutoku_honshokin`          | 獲得本賞金                   | `character varying(8)`  | `character varying(8)`  | YES      |     | 単位：百円                                  |
| `kakutoku_fukashokin`         | 獲得付加賞金                 | `character varying(8)`  | `character varying(8)`  | YES      |     | 単位：百円                                  |
| `yobi_3`                      | 予備                         | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `yobi_4`                      | 予備                         | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |
| `kohan_4f`                    | 後4ハロンタイム              | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `kohan_3f`                    | 後3ハロンタイム              | `character varying(3)`  | `character varying(3)`  | YES      |     | 99.9秒で設定                                |
| `aiteuma_joho_1`              | 1着馬(相手馬)情報1           | `character varying(46)` | `character varying(46)` | YES      |     |                                             |
| `aiteuma_joho_2`              | 1着馬(相手馬)情報2           | `character varying(46)` | `character varying(46)` | YES      |     |                                             |
| `aiteuma_joho_3`              | 1着馬(相手馬)情報3           | `character varying(46)` | `character varying(46)` | YES      |     |                                             |
| `time_sa`                     | タイム差                     | `character varying(4)`  | `character varying(4)`  | YES      |     |                                             |
| `record_koshin_kubun`         | レコード更新区分             | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `mining_kubun`                | マイニング区分               | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `yoso_soha_time`              | マイニング予想走破タイム     | `character varying(5)`  | `character varying(5)`  | YES      |     | 9分99.99秒で設定                            |
| `yoso_gosa_plus`              | マイニング予想誤差(信頼度)＋ | `character varying(4)`  | `character varying(4)`  | YES      |     | 99.99秒で設定                               |
| `yoso_gosa_minus`             | マイニング予想誤差(信頼度)－ | `character varying(4)`  | `character varying(4)`  | YES      |     | 99.99秒で設定                               |
| `yoso_juni`                   | マイニング予想順位           | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `kyakushitsu_hantei`          | 今回レース脚質判定           | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_tc`

- Logical name: 発走時刻変更
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「発走時刻変更」。
- Rows: 0
- Columns: 12
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: nvd_tc_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name     | db type                | Excel type             | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ---------------------- | ---------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)` | `character varying(1)` | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)` | `character varying(8)` | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)` | `character varying(4)` | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)` | `character varying(4)` | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)` | `character varying(2)` | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)` | `character varying(2)` | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)` | `character varying(2)` | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)` | `character varying(8)` | YES      |     | mmddHHmm形式で設定                   |
| `hasso_jikoku`          | 変更後 発走時刻  | `character varying(4)` | `character varying(4)` | YES      |     | HHmm形式で設定                       |
| `hasso_jikoku_henkomae` | 変更前 発走時刻  | `character varying(4)` | `character varying(4)` | YES      |     | HHmm形式で設定                       |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_um`

- Logical name: 競走馬マスタ
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「競走馬マスタ」。
- Rows: 0
- Columns: 89
- Total size: 64 kB
- Primary key: `ketto_toroku_bango`
- Excel indexes: nvd_um_pk(ketto_toroku_bango), nvd_um_idx1(seinengappi), nvd_um_idx2(ketto_joho_01a), nvd_um_idx3(ketto_joho_02a), nvd_um_idx4(chokyoshi_code), nvd_um_idx5(seisansha_code), nvd_um_idx6(banushi_code)

| column                        | logical name          | db type                 | Excel type              | nullable | key | reference / note                            |
| ----------------------------- | --------------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------------- |
| `record_id`                   | レコード種別ID        | `character varying(2)`  | `character varying(2)`  | YES      |     |                                             |
| `data_kubun`                  | データ区分            | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `data_sakusei_nengappi`       | データ作成年月日      | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `ketto_toroku_bango`          | 血統登録番号          | `character varying(10)` | `character varying(10)` | NO       | PK  | 競走馬を一意に識別するための番号            |
| `massho_kubun`                | 競走馬抹消区分        | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `toroku_nengappi`             | 競走馬登録年月日      | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `massho_nengappi`             | 競走馬抹消年月日      | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `seinengappi`                 | 生年月日              | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                          |
| `bamei`                       | 馬名                  | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `bamei_hankaku_kana`          | 馬名半角ｶﾅ            | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `bamei_eur`                   | 馬名欧字              | `character varying(60)` | `character varying(60)` | YES      |     |                                             |
| `zaikyu_flag`                 | JRA施設在きゅうフラグ | `character varying(1)`  | `character varying(1)`  | YES      |     |                                             |
| `yobi_1`                      | 予備                  | `character varying(19)` | `character varying(19)` | YES      |     |                                             |
| `umakigo_code`                | 馬記号コード          | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.馬記号コード を参照                |
| `seibetsu_code`               | 性別コード            | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.性別コード を参照                  |
| `hinshu_code`                 | 品種コード            | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.品種コード を参照                  |
| `moshoku_code`                | 毛色コード            | `character varying(2)`  | `character varying(2)`  | YES      |     | コード表.毛色コード を参照                  |
| `ketto_joho_01a`              | 3代血統情報1          | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_01b`              | 3代血統情報1          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_02a`              | 3代血統情報2          | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_02b`              | 3代血統情報2          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_03a`              | 3代血統情報3          | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_03b`              | 3代血統情報3          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_04a`              | 3代血統情報4          | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_04b`              | 3代血統情報4          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_05a`              | 3代血統情報5          | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_05b`              | 3代血統情報5          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_06a`              | 3代血統情報6          | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_06b`              | 3代血統情報6          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_07a`              | 3代血統情報7          | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_07b`              | 3代血統情報7          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_08a`              | 3代血統情報8          | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_08b`              | 3代血統情報8          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_09a`              | 3代血統情報9          | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_09b`              | 3代血統情報9          | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_10a`              | 3代血統情報10         | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_10b`              | 3代血統情報10         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_11a`              | 3代血統情報11         | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_11b`              | 3代血統情報11         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_12a`              | 3代血統情報12         | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_12b`              | 3代血統情報12         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_13a`              | 3代血統情報13         | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_13b`              | 3代血統情報13         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `ketto_joho_14a`              | 3代血統情報14         | `character varying(8)`  | `character varying(8)`  | YES      |     | 繁殖馬マスタ.繁殖登録番号への外部キーリンク |
| `ketto_joho_14b`              | 3代血統情報14         | `character varying(36)` | `character varying(36)` | YES      |     |                                             |
| `tozai_shozoku_code`          | 東西所属コード        | `character varying(1)`  | `character varying(1)`  | YES      |     | コード表.東西所属コード を参照              |
| `chokyoshi_code`              | 調教師コード          | `character varying(5)`  | `character varying(5)`  | YES      |     | 調教師マスタ.調教師コードへの外部キーリンク |
| `chokyoshimei_ryakusho`       | 調教師名略称          | `character varying(8)`  | `character varying(8)`  | YES      |     |                                             |
| `shotai_chiikimei`            | 招待地域名            | `character varying(20)` | `character varying(20)` | YES      |     |                                             |
| `seisansha_code`              | 生産者コード          | `character varying(6)`  | `character varying(6)`  | YES      |     | 生産者マスタ.生産者コードへの外部キーリンク |
| `seisanshamei`                | 生産者名(法人格無)    | `character varying(70)` | `character varying(70)` | YES      |     |                                             |
| `sanchimei`                   | 産地名                | `character varying(20)` | `character varying(20)` | YES      |     |                                             |
| `banushi_code`                | 馬主コード            | `character varying(6)`  | `character varying(6)`  | YES      |     |                                             |
| `banushimei`                  | 馬主名(法人格無)      | `character varying(64)` | `character varying(64)` | YES      |     |                                             |
| `heichi_honshokin_ruikei`     | 平地本賞金累計        | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `shogai_honshokin_ruikei`     | 障害本賞金累計        | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `heichi_fukashokin_ruikei`    | 平地付加賞金累計      | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `shogai_fukashokin_ruikei`    | 障害付加賞金累計      | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `heichi_shutokushokin_ruikei` | 平地収得賞金累計      | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `shogai_shutokushokin_ruikei` | 障害収得賞金累計      | `character varying(9)`  | `character varying(9)`  | YES      |     | 単位：百円                                  |
| `sogo`                        | 総合着回数            | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `chuo_gokei`                  | 中央合計着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_choku`                 | 芝直・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_migi`                  | 芝右・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_hidari`                | 芝左・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_choku`                  | ダ直・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_migi`                   | ダ右・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_hidari`                 | ダ左・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shogai`                      | 障害・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_ryo`                   | 芝良・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_yayaomo`               | 芝稍・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_omo`                   | 芝重・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_furyo`                 | 芝不・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_ryo`                    | ダ良・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_yayaomo`                | ダ稍・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_omo`                    | ダ重・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_furyo`                  | ダ不・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shogai_ryo`                  | 障良・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shogai_yayaomo`              | 障稍・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shogai_omo`                  | 障重・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shogai_furyo`                | 障不・着回数          | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_short`                 | 芝16下・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_middle`                | 芝22下・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `shiba_long`                  | 芝22超・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_short`                  | ダ16下・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_middle`                 | ダ22下・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `dirt_long`                   | ダ22超・着回数        | `character varying(18)` | `character varying(18)` | YES      |     |                                             |
| `kyakushitsu_keiko`           | 脚質傾向              | `character varying(12)` | `character varying(12)` | YES      |     |                                             |
| `toroku_race_su`              | 登録レース数          | `character varying(3)`  | `character varying(3)`  | YES      |     |                                             |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_we`

- Logical name: 天候馬場状態
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「天候馬場状態」。
- Rows: 0
- Columns: 16
- Total size: 8192 bytes
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, happyo_tsukihi_jifun, henko_shikibetsu`
- Excel indexes: nvd_we_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,happyo_tsukihi_jifun,henko_shikibetsu)

| column                          | logical name            | db type                | Excel type             | nullable | key | reference / note             |
| ------------------------------- | ----------------------- | ---------------------- | ---------------------- | -------- | --- | ---------------------------- |
| `record_id`                     | レコード種別ID          | `character varying(2)` | `character varying(2)` | YES      |     |                              |
| `data_kubun`                    | データ区分              | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `data_sakusei_nengappi`         | データ作成年月日        | `character varying(8)` | `character varying(8)` | YES      |     | yyyymmdd形式で設定           |
| `kaisai_nen`                    | 開催年                  | `character varying(4)` | `character varying(4)` | NO       | PK  |                              |
| `kaisai_tsukihi`                | 開催月日                | `character varying(4)` | `character varying(4)` | NO       | PK  |                              |
| `keibajo_code`                  | 競馬場コード            | `character varying(2)` | `character varying(2)` | NO       | PK  | コード表.競馬場コード を参照 |
| `kaisai_kai`                    | 開催回[第N回]           | `character varying(2)` | `character varying(2)` | YES      |     |                              |
| `kaisai_nichime`                | 開催日目[N日目]         | `character varying(2)` | `character varying(2)` | YES      |     |                              |
| `happyo_tsukihi_jifun`          | 発表月日時分            | `character varying(8)` | `character varying(8)` | NO       | PK  | mmddHHmm形式で設定           |
| `henko_shikibetsu`              | 変更識別                | `character varying(1)` | `character varying(1)` | NO       | PK  |                              |
| `tenko_code`                    | 天候状態                | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `babajotai_code_shiba`          | 馬場状態・芝            | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `babajotai_code_dirt`           | 馬場状態・ダート        | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `tenko_code_henkomae`           | 変更前 天候状態         | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `babajotai_code_shiba_henkomae` | 変更前 馬場状態・芝     | `character varying(1)` | `character varying(1)` | YES      |     |                              |
| `babajotai_code_dirt_henkomae`  | 変更前 馬場状態・ダート | `character varying(1)` | `character varying(1)` | YES      |     |                              |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_wf`

- Logical name: 重勝式(WIN5)
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「重勝式(WIN5)」。
- Rows: 0
- Columns: 267
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi`
- Excel indexes: nvd_wf_pk(kaisai_nen,kaisai_tsukihi)

| column                  | logical name             | db type                 | Excel type              | nullable | key | reference / note   |
| ----------------------- | ------------------------ | ----------------------- | ----------------------- | -------- | --- | ------------------ |
| `record_id`             | レコード種別ID           | `character varying(2)`  | `character varying(2)`  | YES      |     |                    |
| `data_kubun`            | データ区分               | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `data_sakusei_nengappi` | データ作成年月日         | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定 |
| `kaisai_nen`            | 開催年                   | `character varying(4)`  | `character varying(4)`  | NO       | PK  | yyyy形式で設定     |
| `kaisai_tsukihi`        | 開催月日                 | `character varying(4)`  | `character varying(4)`  | NO       | PK  | mmdd形式で設定     |
| `yobi_1`                | 予備                     | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `shikibetsu_code`       | 式別コード               | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `race_joho_1`           | 重勝式対象レース情報1    | `character varying(8)`  | `character varying(8)`  | YES      |     |                    |
| `race_joho_2`           | 重勝式対象レース情報2    | `character varying(8)`  | `character varying(8)`  | YES      |     |                    |
| `race_joho_3`           | 重勝式対象レース情報3    | `character varying(8)`  | `character varying(8)`  | YES      |     |                    |
| `race_joho_4`           | 重勝式対象レース情報4    | `character varying(8)`  | `character varying(8)`  | YES      |     |                    |
| `race_joho_5`           | 重勝式対象レース情報5    | `character varying(8)`  | `character varying(8)`  | YES      |     |                    |
| `yobi_2`                | 予備                     | `character varying(6)`  | `character varying(6)`  | YES      |     |                    |
| `win5_hyosu_gokei`      | 重勝式発売票数           | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `yuko_hyosu_1`          | 有効票数情報1            | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `yuko_hyosu_2`          | 有効票数情報2            | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `yuko_hyosu_3`          | 有効票数情報3            | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `yuko_hyosu_4`          | 有効票数情報4            | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `yuko_hyosu_5`          | 有効票数情報5            | `character varying(11)` | `character varying(11)` | YES      |     |                    |
| `henkan_flag`           | 返還フラグ               | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `fuseiritsu_flag`       | 不成立フラグ             | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `tekichu_nashi_flag`    | 的中無フラグ             | `character varying(1)`  | `character varying(1)`  | YES      |     |                    |
| `carry_over`            | キャリーオーバー金額初期 | `character varying(15)` | `character varying(15)` | YES      |     |                    |
| `carry_over_zandaka`    | キャリーオーバー金額残高 | `character varying(15)` | `character varying(15)` | YES      |     |                    |
| `haraimodoshi_win5_001` | 重勝式払戻情報1          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_002` | 重勝式払戻情報2          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_003` | 重勝式払戻情報3          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_004` | 重勝式払戻情報4          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_005` | 重勝式払戻情報5          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_006` | 重勝式払戻情報6          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_007` | 重勝式払戻情報7          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_008` | 重勝式払戻情報8          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_009` | 重勝式払戻情報9          | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_010` | 重勝式払戻情報10         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_011` | 重勝式払戻情報11         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_012` | 重勝式払戻情報12         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_013` | 重勝式払戻情報13         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_014` | 重勝式払戻情報14         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_015` | 重勝式払戻情報15         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_016` | 重勝式払戻情報16         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_017` | 重勝式払戻情報17         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_018` | 重勝式払戻情報18         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_019` | 重勝式払戻情報19         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_020` | 重勝式払戻情報20         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_021` | 重勝式払戻情報21         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_022` | 重勝式払戻情報22         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_023` | 重勝式払戻情報23         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_024` | 重勝式払戻情報24         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_025` | 重勝式払戻情報25         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_026` | 重勝式払戻情報26         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_027` | 重勝式払戻情報27         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_028` | 重勝式払戻情報28         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_029` | 重勝式払戻情報29         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_030` | 重勝式払戻情報30         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_031` | 重勝式払戻情報31         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_032` | 重勝式払戻情報32         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_033` | 重勝式払戻情報33         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_034` | 重勝式払戻情報34         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_035` | 重勝式払戻情報35         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_036` | 重勝式払戻情報36         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_037` | 重勝式払戻情報37         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_038` | 重勝式払戻情報38         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_039` | 重勝式払戻情報39         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_040` | 重勝式払戻情報40         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_041` | 重勝式払戻情報41         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_042` | 重勝式払戻情報42         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_043` | 重勝式払戻情報43         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_044` | 重勝式払戻情報44         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_045` | 重勝式払戻情報45         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_046` | 重勝式払戻情報46         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_047` | 重勝式払戻情報47         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_048` | 重勝式払戻情報48         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_049` | 重勝式払戻情報49         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_050` | 重勝式払戻情報50         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_051` | 重勝式払戻情報51         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_052` | 重勝式払戻情報52         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_053` | 重勝式払戻情報53         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_054` | 重勝式払戻情報54         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_055` | 重勝式払戻情報55         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_056` | 重勝式払戻情報56         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_057` | 重勝式払戻情報57         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_058` | 重勝式払戻情報58         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_059` | 重勝式払戻情報59         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_060` | 重勝式払戻情報60         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_061` | 重勝式払戻情報61         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_062` | 重勝式払戻情報62         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_063` | 重勝式払戻情報63         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_064` | 重勝式払戻情報64         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_065` | 重勝式払戻情報65         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_066` | 重勝式払戻情報66         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_067` | 重勝式払戻情報67         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_068` | 重勝式払戻情報68         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_069` | 重勝式払戻情報69         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_070` | 重勝式払戻情報70         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_071` | 重勝式払戻情報71         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_072` | 重勝式払戻情報72         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_073` | 重勝式払戻情報73         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_074` | 重勝式払戻情報74         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_075` | 重勝式払戻情報75         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_076` | 重勝式払戻情報76         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_077` | 重勝式払戻情報77         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_078` | 重勝式払戻情報78         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_079` | 重勝式払戻情報79         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_080` | 重勝式払戻情報80         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_081` | 重勝式払戻情報81         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_082` | 重勝式払戻情報82         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_083` | 重勝式払戻情報83         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_084` | 重勝式払戻情報84         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_085` | 重勝式払戻情報85         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_086` | 重勝式払戻情報86         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_087` | 重勝式払戻情報87         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_088` | 重勝式払戻情報88         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_089` | 重勝式払戻情報89         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_090` | 重勝式払戻情報90         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_091` | 重勝式払戻情報91         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_092` | 重勝式払戻情報92         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_093` | 重勝式払戻情報93         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_094` | 重勝式払戻情報94         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_095` | 重勝式払戻情報95         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_096` | 重勝式払戻情報96         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_097` | 重勝式払戻情報97         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_098` | 重勝式払戻情報98         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_099` | 重勝式払戻情報99         | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_100` | 重勝式払戻情報100        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_101` | 重勝式払戻情報101        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_102` | 重勝式払戻情報102        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_103` | 重勝式払戻情報103        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_104` | 重勝式払戻情報104        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_105` | 重勝式払戻情報105        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_106` | 重勝式払戻情報106        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_107` | 重勝式払戻情報107        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_108` | 重勝式払戻情報108        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_109` | 重勝式払戻情報109        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_110` | 重勝式払戻情報110        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_111` | 重勝式払戻情報111        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_112` | 重勝式払戻情報112        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_113` | 重勝式払戻情報113        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_114` | 重勝式払戻情報114        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_115` | 重勝式払戻情報115        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_116` | 重勝式払戻情報116        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_117` | 重勝式払戻情報117        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_118` | 重勝式払戻情報118        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_119` | 重勝式払戻情報119        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_120` | 重勝式払戻情報120        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_121` | 重勝式払戻情報121        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_122` | 重勝式払戻情報122        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_123` | 重勝式払戻情報123        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_124` | 重勝式払戻情報124        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_125` | 重勝式払戻情報125        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_126` | 重勝式払戻情報126        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_127` | 重勝式払戻情報127        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_128` | 重勝式払戻情報128        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_129` | 重勝式払戻情報129        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_130` | 重勝式払戻情報130        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_131` | 重勝式払戻情報131        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_132` | 重勝式払戻情報132        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_133` | 重勝式払戻情報133        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_134` | 重勝式払戻情報134        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_135` | 重勝式払戻情報135        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_136` | 重勝式払戻情報136        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_137` | 重勝式払戻情報137        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_138` | 重勝式払戻情報138        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_139` | 重勝式払戻情報139        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_140` | 重勝式払戻情報140        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_141` | 重勝式払戻情報141        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_142` | 重勝式払戻情報142        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_143` | 重勝式払戻情報143        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_144` | 重勝式払戻情報144        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_145` | 重勝式払戻情報145        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_146` | 重勝式払戻情報146        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_147` | 重勝式払戻情報147        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_148` | 重勝式払戻情報148        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_149` | 重勝式払戻情報149        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_150` | 重勝式払戻情報150        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_151` | 重勝式払戻情報151        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_152` | 重勝式払戻情報152        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_153` | 重勝式払戻情報153        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_154` | 重勝式払戻情報154        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_155` | 重勝式払戻情報155        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_156` | 重勝式払戻情報156        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_157` | 重勝式払戻情報157        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_158` | 重勝式払戻情報158        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_159` | 重勝式払戻情報159        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_160` | 重勝式払戻情報160        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_161` | 重勝式払戻情報161        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_162` | 重勝式払戻情報162        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_163` | 重勝式払戻情報163        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_164` | 重勝式払戻情報164        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_165` | 重勝式払戻情報165        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_166` | 重勝式払戻情報166        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_167` | 重勝式払戻情報167        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_168` | 重勝式払戻情報168        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_169` | 重勝式払戻情報169        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_170` | 重勝式払戻情報170        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_171` | 重勝式払戻情報171        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_172` | 重勝式払戻情報172        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_173` | 重勝式払戻情報173        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_174` | 重勝式払戻情報174        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_175` | 重勝式払戻情報175        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_176` | 重勝式払戻情報176        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_177` | 重勝式払戻情報177        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_178` | 重勝式払戻情報178        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_179` | 重勝式払戻情報179        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_180` | 重勝式払戻情報180        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_181` | 重勝式払戻情報181        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_182` | 重勝式払戻情報182        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_183` | 重勝式払戻情報183        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_184` | 重勝式払戻情報184        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_185` | 重勝式払戻情報185        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_186` | 重勝式払戻情報186        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_187` | 重勝式払戻情報187        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_188` | 重勝式払戻情報188        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_189` | 重勝式払戻情報189        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_190` | 重勝式払戻情報190        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_191` | 重勝式払戻情報191        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_192` | 重勝式払戻情報192        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_193` | 重勝式払戻情報193        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_194` | 重勝式払戻情報194        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_195` | 重勝式払戻情報195        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_196` | 重勝式払戻情報196        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_197` | 重勝式払戻情報197        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_198` | 重勝式払戻情報198        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_199` | 重勝式払戻情報199        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_200` | 重勝式払戻情報200        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_201` | 重勝式払戻情報201        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_202` | 重勝式払戻情報202        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_203` | 重勝式払戻情報203        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_204` | 重勝式払戻情報204        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_205` | 重勝式払戻情報205        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_206` | 重勝式払戻情報206        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_207` | 重勝式払戻情報207        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_208` | 重勝式払戻情報208        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_209` | 重勝式払戻情報209        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_210` | 重勝式払戻情報210        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_211` | 重勝式払戻情報211        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_212` | 重勝式払戻情報212        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_213` | 重勝式払戻情報213        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_214` | 重勝式払戻情報214        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_215` | 重勝式払戻情報215        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_216` | 重勝式払戻情報216        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_217` | 重勝式払戻情報217        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_218` | 重勝式払戻情報218        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_219` | 重勝式払戻情報219        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_220` | 重勝式払戻情報220        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_221` | 重勝式払戻情報221        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_222` | 重勝式払戻情報222        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_223` | 重勝式払戻情報223        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_224` | 重勝式払戻情報224        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_225` | 重勝式払戻情報225        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_226` | 重勝式払戻情報226        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_227` | 重勝式払戻情報227        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_228` | 重勝式払戻情報228        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_229` | 重勝式払戻情報229        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_230` | 重勝式払戻情報230        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_231` | 重勝式払戻情報231        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_232` | 重勝式払戻情報232        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_233` | 重勝式払戻情報233        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_234` | 重勝式払戻情報234        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_235` | 重勝式払戻情報235        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_236` | 重勝式払戻情報236        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_237` | 重勝式払戻情報237        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_238` | 重勝式払戻情報238        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_239` | 重勝式払戻情報239        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_240` | 重勝式払戻情報240        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_241` | 重勝式払戻情報241        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_242` | 重勝式払戻情報242        | `character varying(29)` | `character varying(29)` | YES      |     |                    |
| `haraimodoshi_win5_243` | 重勝式払戻情報243        | `character varying(29)` | `character varying(29)` | YES      |     |                    |

Sample records: omitted from public reference; do not publish actual record values.

### `nvd_wh`

- Logical name: 馬体重
- Purpose: 地方競馬系データ。現在の実データは主にこの系統に入っている。 論理名は「馬体重」。
- Rows: 0
- Columns: 28
- Total size: 16 kB
- Primary key: `kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango`
- Excel indexes: nvd_wh_pk(kaisai_nen,kaisai_tsukihi,keibajo_code,race_bango)

| column                  | logical name     | db type                 | Excel type              | nullable | key | reference / note                     |
| ----------------------- | ---------------- | ----------------------- | ----------------------- | -------- | --- | ------------------------------------ |
| `record_id`             | レコード種別ID   | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `data_kubun`            | データ区分       | `character varying(1)`  | `character varying(1)`  | YES      |     |                                      |
| `data_sakusei_nengappi` | データ作成年月日 | `character varying(8)`  | `character varying(8)`  | YES      |     | yyyymmdd形式で設定                   |
| `kaisai_nen`            | 開催年           | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_tsukihi`        | 開催月日         | `character varying(4)`  | `character varying(4)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `keibajo_code`          | 競馬場コード     | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `kaisai_kai`            | 開催回[第N回]    | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `kaisai_nichime`        | 開催日目[N日目]  | `character varying(2)`  | `character varying(2)`  | YES      |     |                                      |
| `race_bango`            | レース番号       | `character varying(2)`  | `character varying(2)`  | NO       | PK  | レースを一意に識別するための複合キー |
| `happyo_tsukihi_jifun`  | 発表月日時分     | `character varying(8)`  | `character varying(8)`  | YES      |     | mmddHHmm形式で設定                   |
| `bataiju_joho_01`       | 馬体重情報1      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_02`       | 馬体重情報2      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_03`       | 馬体重情報3      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_04`       | 馬体重情報4      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_05`       | 馬体重情報5      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_06`       | 馬体重情報6      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_07`       | 馬体重情報7      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_08`       | 馬体重情報8      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_09`       | 馬体重情報9      | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_10`       | 馬体重情報10     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_11`       | 馬体重情報11     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_12`       | 馬体重情報12     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_13`       | 馬体重情報13     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_14`       | 馬体重情報14     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_15`       | 馬体重情報15     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_16`       | 馬体重情報16     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_17`       | 馬体重情報17     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |
| `bataiju_joho_18`       | 馬体重情報18     | `character varying(45)` | `character varying(45)` | YES      |     |                                      |

Sample records: omitted from public reference; do not publish actual record values.

## Code Tables From Excel

### グレードコード

| code | meaning  |
| ---- | -------- |
| `A`  | G1       |
| `B`  | G2       |
| `C`  | G3       |
| `D`  | 重賞     |
| `E`  | 特別競走 |
| `F`  | J･G1     |
| `G`  | J･G2     |
| `H`  | J･G3     |
| `L`  | L        |
| `P`  | S1       |
| `Q`  | S2       |
| `R`  | S3       |
| `S`  | 重賞     |
| `T`  | 準重賞   |

### トラックコード

| code | meaning           |
| ---- | ----------------- |
| `10` | 平地 芝・直線     |
| `11` | 平地 芝・左       |
| `12` | 平地 芝・左外     |
| `13` | 平地 芝・左内→外  |
| `14` | 平地 芝・左外→内  |
| `15` | 平地 芝・左内２周 |
| `16` | 平地 芝・左外２周 |
| `17` | 平地 芝・右       |
| `18` | 平地 芝・右外     |
| `19` | 平地 芝・右内→外  |
| `20` | 平地 芝・右外→内  |
| `21` | 平地 芝・右内２周 |
| `22` | 平地 芝・右外２周 |
| `23` | 平地 ダート・左   |
| `24` | 平地 ダート・右   |
| `25` | 平地 ダート・左内 |
| `26` | 平地 ダート・右外 |
| `27` | 平地 サンド・左   |
| `28` | 平地 サンド・右   |
| `29` | 平地 ダート・直線 |
| `51` | 障害 芝・襷       |
| `52` | 障害 芝→ダート    |
| `53` | 障害 芝・左       |
| `54` | 障害 芝           |
| `55` | 障害 芝・外       |
| `56` | 障害 芝・外→内    |
| `57` | 障害 芝・内→外    |
| `58` | 障害 芝・内２周   |
| `59` | 障害 芝・外２周   |
| `90` | 障害 ばんえい     |

### 不合格理由コード

| code | meaning        |
| ---- | -------------- |
| `1`  | 発走調教不良   |
| `2`  | 競走調教不良   |
| `3`  | タイムオーバー |
| `4`  | 不明(調査中)   |
| `5`  | 不明(調査中)   |

### 品種コード

| code | meaning                    |
| ---- | -------------------------- |
| `1`  | サラブレッド               |
| `2`  | サラブレッド系種           |
| `3`  | 準サラブレッド             |
| `4`  | 軽半血種                   |
| `5`  | アングロアラブ             |
| `6`  | アラブ系種                 |
| `7`  | アラブ                     |
| `8`  | 中半血種チュウハンケッシュ |
| `A`  | 半血輓系種                 |
| `B`  | 日本輓系種                 |
| `C`  | ペルシュロン               |
| `D`  | ペルシュロン系種           |
| `E`  | ベルジャン                 |
| `F`  | ベルジャン系種             |
| `G`  | ブルトン                   |
| `H`  | ブルトン系種               |
| `I`  | アングロノルマン           |

### 天候コード

| code | meaning |
| ---- | ------- |
| `1`  | 晴      |
| `2`  | 曇      |
| `3`  | 雨      |
| `4`  | 小雨    |
| `5`  | 雪      |
| `6`  | 小雪    |

### 性別コード

| code | meaning |
| ---- | ------- |
| `1`  | 牡      |
| `2`  | 牝      |
| `3`  | ｾﾝ      |

### 曜日コード

| code | meaning |
| ---- | ------- |
| `1`  | 土      |
| `2`  | 日      |
| `3`  | 祝      |
| `4`  | 月      |
| `5`  | 火      |
| `6`  | 水      |
| `7`  | 木      |
| `8`  | 金      |

### 東西所属コード

| code | meaning |
| ---- | ------- |
| `1`  | 美浦    |
| `2`  | 栗東    |
| `3`  | 地方    |
| `4`  | 外国    |

### 毛色コード

| code | meaning          |
| ---- | ---------------- |
| `01` | 栗毛クリゲ       |
| `02` | 栃栗毛トチクリゲ |
| `03` | 鹿毛カゲ         |
| `04` | 黒鹿毛クロカゲ   |
| `05` | 青鹿毛アオカゲ   |
| `06` | 青毛アオゲ       |
| `07` | 芦毛アシケ       |
| `08` | 栗粕毛クリカスケ |
| `09` | 鹿粕毛シカカスゲ |
| `10` | 青粕毛アオカスケ |
| `11` | 白毛シロゲ       |
| `30` | 栗駁毛           |
| `31` | 鹿駁毛           |
| `32` | 青駁毛           |
| `33` | 駁栗毛           |
| `34` | 駁鹿毛           |
| `35` | 駁青毛           |
| `36` | 月毛             |
| `37` | 川原毛           |
| `38` | 佐目毛           |
| `39` | 薄墨毛           |

### 異常区分コード

| code | meaning    |
| ---- | ---------- |
| `1`  | 出走取消   |
| `2`  | 発走除外   |
| `3`  | 競走除外   |
| `4`  | 競走中止   |
| `5`  | 失格       |
| `6`  | 落馬再騎乗 |
| `7`  | 降着       |

### 着差コード

| code  | meaning |
| ----- | ------- |
| `12`  | 1/2     |
| `34`  | 3/4     |
| `1`   | 1       |
| `112` | 1 1/2   |
| `114` | 1 1/4   |
| `134` | 1 3/4   |
| `2`   | 2       |
| `212` | 2 1/2   |
| `3`   | 3       |
| `312` | 3 1/2   |
| `4`   | 4       |
| `5`   | 5       |
| `6`   | 6       |
| `7`   | 7       |
| `8`   | 8       |
| `9`   | 9       |
| `A`   | ｱﾀﾏ     |
| `D`   | 同着    |
| `H`   | ﾊﾅ      |
| `K`   | ｸﾋﾞ     |
| `T`   | 大差    |
| `Z`   | 10      |

### 競走条件コード

| code  | meaning   |
| ----- | --------- |
| `000` |           |
| `001` |           |
| `002` |           |
| `003` |           |
| `004` |           |
| `005` | 1勝クラス |
| `006` |           |
| `007` |           |
| `008` |           |
| `009` |           |
| `010` | 2勝クラス |
| `014` |           |
| `015` |           |
| `016` | 3勝クラス |
| `701` | 新馬      |
| `702` | 未出走    |
| `703` | 未勝利    |
| `999` | オープン  |

### 競走種別コード

| code | meaning         |
| ---- | --------------- |
| `01` | 混合2歳         |
| `02` | 混合3歳         |
| `03` | 混合4歳         |
| `04` | 混合3歳以上     |
| `05` | 混合4歳以上     |
| `06` | 混合5歳以上     |
| `07` | 混合2・3歳      |
| `08` | 混合3・4歳      |
| `09` | 混合一般        |
| `11` | サラ系2歳       |
| `12` | サラ系3歳       |
| `13` | サラ系3歳以上   |
| `14` | サラ系4歳以上   |
| `18` | サラ障害3歳以上 |
| `19` | サラ障害4歳以上 |
| `21` | アラブ系2歳     |
| `22` | アラブ系3歳     |
| `23` | アラブ系3歳以上 |
| `24` | アラブ系4歳以上 |
| `43` | サラ系4歳       |
| `46` | サラ系5歳以上   |
| `47` | サラ系2・3歳    |
| `48` | サラ系3・4歳    |
| `49` | サラ系一般      |
| `53` | アラ系4歳       |
| `56` | アラブ系5歳上   |
| `57` | アラ系2・3歳    |
| `58` | アラ系3・4歳    |
| `59` | アラブ系一般    |

### 競走記号コード

| code  | meaning                                               |
| ----- | ----------------------------------------------------- |
| `001` | (指定)                                                |
| `002` | 若手騎手                                              |
| `003` | [指定]                                                |
| `004` | (特指)                                                |
| `00a` | 南関東・北陸・東海・近畿・中国・四国・九州交流        |
| `00A` | 地方全国交流                                          |
| `00B` | 北海道・東北交流                                      |
| `00e` | 北陸・東海交流                                        |
| `00f` | 北陸・近畿交流                                        |
| `00g` | 北陸・中国交流                                        |
| `00h` | 北陸・東海・近畿交流                                  |
| `00i` | 北陸・東海・近畿・中国交流                            |
| `00j` | 北陸・東海・近畿・中国・四国・九州交流                |
| `00k` |                                                       |
| `00L` |                                                       |
| `00m` | 東海・近畿交流                                        |
| `00M` | 東北交流                                              |
| `00q` | 近畿・中国交流                                        |
| `00s` | 近畿・中国・四国交流                                  |
| `00v` | 中国交流                                              |
| `00w` | 中国・四国交流                                        |
| `00X` | 南関東交流                                            |
| `00y` | 四国・九州交流                                        |
| `00Y` | 南関東・北陸・東海・近畿・交流                        |
| `00z` | 九州交流                                              |
| `00Z` | 南関東・北陸・東海・近畿・中国交流                    |
| `010` | 牡                                                    |
| `020` | 牝                                                    |
| `021` | 牝 (指定)                                             |
| `023` | 牝 [指定]                                             |
| `024` | 牝 (特指)                                             |
| `02A` | 牝 南関東・北陸・東海・近畿・中国・四国・九州交流     |
| `02f` | 牝 北陸・近畿交流                                     |
| `02h` | 牝 北陸・東海・近畿交流                               |
| `02j` | 牝 北陸・東海・近畿・中国・四国・九州交流             |
| `02k` | 牝                                                    |
| `02M` | 牝 東北交流                                           |
| `02v` | 牝 中国交流                                           |
| `02w` | 牝 中国・四国交流                                     |
| `02y` | 牝 四国・九州交流                                     |
| `030` | 牡・ｾﾝ                                                |
| `03A` | 牡・ｾﾝ 南関東・北陸・東海・近畿・中国・四国・九州交流 |
| `040` | 牡・牝                                                |
| `041` | 牡・牝 (指定)                                         |
| `04h` | 牡・牝 北陸・東海・近畿交流                           |
| `04j` | 牡・牝 北陸・東海・近畿・中国・四国・九州交流         |
| `0A0` | 選抜牝馬 (指定)                                       |
| `0A1` | 選抜牝馬 南関東交流                                   |
| `0B0` | 選定牝馬                                              |
| `0B1` | 選定牝馬 (指定)                                       |
| `0BX` | 選定牝馬 南関東交流                                   |
| `a00` | 奨励馬・抽選馬                                        |
| `A00` | (混合)                                                |
| `A01` | (混合)(指定)                                          |
| `A02` | (混合) 若手騎手                                       |
| `A03` | (混合)[指定]                                          |
| `A04` | (混合)(特指)                                          |
| `A20` | (混合) 牝                                             |
| `A21` | (混合) 牝 (指定)                                      |
| `A23` | (混合) 牝 [指定]                                      |
| `A24` | (混合) 牝 (特指)                                      |
| `A30` | (混合) 牡・ｾﾝ                                         |
| `A31` | (混合) 牡・ｾﾝ (指定)                                  |
| `A34` | (混合) 牡・ｾﾝ (特指)                                  |
| `A41` | (混合) 牡・牝 (指定)                                  |
| `b00` | 補助馬                                                |
| `B00` | (父)                                                  |
| `B01` | (父)(指定)                                            |
| `B03` | (父)[指定]                                            |
| `B04` | (父)(特指)                                            |
| `B20` | (父) 牝                                               |
| `c00` | 北海道産馬                                            |
| `C00` | (市)                                                  |
| `C03` | (市)[指定]                                            |
| `C04` | (市)(特指)                                            |
| `C0A` | (市) 地方全国交流                                     |
| `C0C` | (市) 北海道・東北・北関東交流                         |
| `C0D` | (市) 北海道・東北・南関東交流                         |
| `C0E` | (市) 北海道・東北・九州交流                           |
| `C0F` | (市) 北海道・東北・北関東・南関東交流                 |
| `C0G` | (市) 北海道・東北・北関東・南関東・九州交流           |
| `C0H` | (市)                                                  |
| `C0I` | (市)                                                  |
| `C0J` | (市)                                                  |
| `C0K` | (市)                                                  |
| `C0O` | (市) 東北・九州交流                                   |
| `C0P` | (市) 東北・南関東・東海交流                           |
| `C0Q` | (市) 東北・北陸・東海交流                             |
| `C0R` | (市)                                                  |
| `C0S` | (市)                                                  |
| `C0T` | (市)                                                  |
| `C20` | (市) 牝 地方全国交流                                  |
| `C2H` | (市) 牝                                               |
| `C2O` | (市) 牝 東北・九州交流                                |
| `C2S` | (市) 牝                                               |
| `C30` | (市) 牡・ｾﾝ                                           |
| `C3I` | (市) 牡・ｾﾝ                                           |
| `C3K` | (市) 牡・ｾﾝ                                           |
| `C3P` | (市) 牡・ｾﾝ 東北・南関東・東海交流                    |
| `D00` | (抽)                                                  |
| `D01` | (抽)(指定)                                            |
| `D03` | (抽)[指定]                                            |
| `E00` | [抽]                                                  |
| `E01` | [抽](指定)                                            |
| `E03` | [抽][指定]                                            |
| `E20` | [抽] 牝                                               |
| `E30` | [抽] 牡・ｾﾝ                                           |
| `F00` | (市)(抽)                                              |
| `F01` | (市)(抽)(指定)                                        |
| `F03` | (市)(抽)[指定]                                        |
| `F04` | (市)(抽)(特指)                                        |
| `F20` | (市)(抽) 牝                                           |
| `G00` | (抽) 関西配布馬                                       |
| `G01` | (抽) 関西配布馬 (指定)                                |
| `G20` | (抽) 関西配布馬 牝                                    |
| `G30` | (抽) 関西配布馬 牡・ｾﾝ                                |
| `H00` | (抽) 関東配布馬                                       |
| `I00` | [抽] 関西配布馬                                       |
| `j00` | 新冠産馬                                              |
| `J00` | [抽] 関東配布馬                                       |
| `J01` | [抽] 関東配布馬 (指定)                                |
| `k00` | 青毛・青鹿毛・黒鹿毛                                  |
| `k0X` | 青毛・青鹿毛・黒鹿毛 南関東交流                       |
| `l00` | 栗毛                                                  |
| `l0X` | 栗毛 南関東交流                                       |
| `M00` | 九州産馬                                              |
| `M01` | 九州産馬 (指定)                                       |
| `M03` | 九州産馬 [指定]                                       |
| `M04` | 九州産馬 (特指)                                       |
| `n00` | 芦毛                                                  |
| `N00` | (国際)                                                |
| `N01` | (国際)(指定)                                          |
| `N03` | (国際)[指定]                                          |
| `N04` | (国際)(特指)コクサイトクユビ                          |
| `N20` | (国際) 牝                                             |
| `N21` | (国際) 牝 (指定)                                      |
| `N23` | (国際) 牝 [指定]                                      |
| `N24` | (国際) 牝 (特指)トクユビ                              |
| `N41` | (国際) 牡・牝 (指定)                                  |
| `N44` | (国際) 牡・牝 (特指)オストクユビ                      |
| `p00` | 地元デビュー馬                                        |
| `X00` | 認定競走                                              |
| `X0A` | 認定競走 地方全国交流                                 |
| `X0B` | 認定競走 北海道・東北交流                             |
| `X10` | 認定競走 牡                                           |
| `X20` | 認定競走 牝                                           |
| `X2A` | 認定競走 牝 地方全国交流                              |
| `XA0` | 認定競走 選抜牝馬                                     |
| `XB0` | 認定競走 選定牝馬                                     |
| `Y00` | 指定競走                                              |
| `Y01` | 指定競走 (指定)                                       |

### 競馬場コード

| code | meaning          |
| ---- | ---------------- |
| `01` | 札幌             |
| `02` | 函館             |
| `03` | 福島             |
| `04` | 新潟             |
| `05` | 東京             |
| `06` | 中山             |
| `07` | 中京             |
| `08` | 京都             |
| `09` | 阪神             |
| `10` | 小倉             |
| `30` | 門別             |
| `31` | 北見             |
| `32` | 岩見沢           |
| `33` | 帯広             |
| `34` | 旭川             |
| `35` | 盛岡             |
| `36` | 水沢             |
| `37` | 上山             |
| `38` | 三条             |
| `39` | 足利             |
| `40` | 宇都宮           |
| `41` | 高崎             |
| `42` | 浦和             |
| `43` | 船橋             |
| `44` | 大井             |
| `45` | 川崎             |
| `46` | 金沢             |
| `47` | 笠松             |
| `48` | 名古屋           |
| `49` | 紀三井寺         |
| `50` | 園田             |
| `51` | 姫路             |
| `52` | 益田             |
| `53` | 福山             |
| `54` | 高知             |
| `55` | 佐賀             |
| `56` | 荒尾             |
| `57` | 中津             |
| `58` | 札幌(地方)       |
| `59` | 函館(地方)       |
| `60` | 新潟(地方)       |
| `61` | 中京(地方)       |
| `80` | 春木             |
| `81` | 北見(ばんえい)   |
| `82` | 岩見沢(ばんえい) |
| `83` | 帯広(ばんえい)   |
| `84` | 旭川(ばんえい)   |
| `A0` | その他の外国     |
| `A2` | 日本             |
| `A4` | アメリカ         |
| `A6` | イギリス         |
| `A8` | フランス         |
| `B0` | インド           |
| `B2` | アイルランド     |
| `B4` | ニュージーランド |
| `B6` | オーストラリア   |
| `B8` | カナダ           |
| `C0` | イタリア         |
| `C2` | ドイツ           |
| `C5` | オマーン         |
| `C6` | イラク           |
| `C7` | アラブ首長国連邦 |
| `C8` | シリア           |
| `D0` | スウェーデン     |
| `D2` | ハンガリー       |
| `D4` | ポルトガル       |
| `D6` | ロシア           |
| `D8` | ウルグアイ       |
| `E0` | ペルー           |
| `E2` | アルゼンチン     |
| `E4` | ブラジル         |
| `E6` | ベルギー         |
| `E8` | トルコ           |
| `F0` | 韓国             |
| `F1` | 中国             |
| `F2` | チリ             |
| `F8` | パナマ           |
| `G0` | 香港             |
| `G2` | スペイン         |
| `H0` | 西ドイツ         |
| `H2` | 南アフリカ       |
| `H4` | スイス           |
| `H6` | モナコ           |
| `H8` | フィリピン       |
| `I0` | プエルトリコ     |
| `I2` | コロンビア       |
| `I4` | チェコスロバキア |
| `I6` | チェコ           |
| `I8` | スロバキア       |
| `J0` | エクアドル       |
| `J2` | ギリシャ         |
| `J4` | マレーシア       |
| `J6` | メキシコ         |
| `J8` | モロッコ         |
| `K0` | パキスタン       |
| `K2` | ポーランド       |
| `K4` | パラグアイ       |
| `K6` | サウジアラビア   |
| `K8` | キプロス         |
| `L0` | タイ             |
| `L2` | ウクライナ       |
| `L4` | ベネズエラ       |
| `L6` | ユーゴスラビア   |
| `L8` | デンマーク       |
| `M0` | シンガポール     |
| `M2` | マカオ           |
| `M4` | オーストリア     |
| `M6` | ヨルダン         |
| `M8` | カタール         |
| `N0` | 東ドイツヒガシ   |
| `N2` | バーレーン       |
| `N4` | カザフスタン     |
| `N6` | モーリシャス     |

### 能力試験種類コード

| code | meaning      |
| ---- | ------------ |
| `1`  | 能力調教試験 |
| `2`  | 馬検査       |
| `3`  | その他       |

### 重量種別コード

| code | meaning        |
| ---- | -------------- |
| `1`  | ハンデ         |
| `2`  | 別定ベツテイ   |
| `3`  | 馬齢バレイ     |
| `4`  | 定量テイリョウ |
| `6`  | 騎手ハンデ     |
| `7`  | 賞金ハンデ     |
| `8`  | 規定           |

### 馬場状態コード

| code | meaning      |
| ---- | ------------ |
| `1`  | 良リョウ     |
| `2`  | 稍重ヤヤオモ |
| `3`  | 重オモ       |
| `4`  | 不良フリョウ |

### 馬記号コード

| code | meaning      |
| ---- | ------------ |
| `01` | (抽)         |
| `02` | [抽]         |
| `03` | (父)         |
| `04` | (市)         |
| `05` | (地)         |
| `06` | (外)         |
| `07` | (父)(抽)     |
| `08` | (父)(市)     |
| `09` | (父)(地)     |
| `10` | (市)(地)     |
| `11` | (外)(地)     |
| `12` | (父)(市)(地) |
| `15` | (招)         |
| `16` | (招)(外)     |
| `17` | (招)(父)     |
| `18` | (招)(市)     |
| `19` | (招)(父)(市) |
| `20` | (父)(外)     |
| `21` | [地]         |
| `22` | (外)[地]     |
| `23` | (父)[地]     |
| `24` | (市)[地]     |
| `25` | (父)(市)[地] |
| `26` | [外]         |
| `27` | (父)[外]     |
| `31` | (持)モ       |
| `40` | (父)(外)(地) |
| `41` | (父)(外)[地] |

### 騎乗資格コード

| code | meaning        |
| ---- | -------------- |
| `1`  | 平地・障害     |
| `2`  | 平地ヘイチ     |
| `3`  | 障害ショウガイ |

### 騎手見習コード

| code | meaning |
| ---- | ------- |
| `1`  | ☆1Kg減  |
| `2`  | △2Kg減  |
| `3`  | ▲3Kg減  |
| `4`  | ★4Kg減  |
| `9`  | ◇2Kg減  |
