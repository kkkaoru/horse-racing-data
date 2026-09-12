# R2 Data Catalog / R2 SQL 利用量・費用レポート

- 作成日時: 2026-08-23 03:08 JST
- 利用量の集計期間: 2026-08-01 00:00 JST〜2026-08-23 03:08 JST
- 費用の集計基準: USD、1 GB = 10 億 bytes
- データソース: Cloudflare GraphQL Analytics API

## 結論

8月の現時点のストレージ・クエリ費用見積もりは **$10.72** です。内訳は R2 SQL のスキャン料金が **$6.21**、対象バケットへ按分した標準 R2 Class B 料金が **$3.99**、対象R2 storageが **$0.53**、R2 Data CatalogとPipelinesの固有料金が **$0.00** です。

これに直接の query gateway である `pc-keiba-r2-catalog` Worker の変動費を加えると、直接帰属できる合計は **$10.72〜$10.76** です。Catalog/SQLに関係する呼び出し元・取り込み元Workerの全処理を広く按分する管理会計上の合計は **$12.45** ですが、各WorkerにはCatalogと無関係な処理も含まれるため、これを直接費とは扱いません。

月末の直接帰属合計は現在のペースが続くと **$17.53〜$18.87** を見込みます。Cloudflare の請求開始日を考慮せず、8月1日以降の利用量に現行単価を機械的に適用した現時点の上限寄りの値は **$11.78** です。

| 区分                                |                         現在の利用量 |                                    無料枠 | 現在の費用見積もり |                       月末予測 |
| ----------------------------------- | -----------------------------------: | ----------------------------------------: | -----------------: | -----------------------------: |
| Catalog API operations              |                          749,404 ops |                          1,000,000 ops/月 |              $0.00 | 1.04M〜1.10M ops、$0.39〜$0.93 |
| Catalog compaction data             |                                 0 GB |                                  10 GB/月 |              $0.00 |                          $0.00 |
| Catalog compaction objects          |                            0 objects |                      1,000,000 objects/月 |              $0.00 |                          $0.00 |
| R2 SQL scan（請求開始後）           |                          2,492.95 GB |                                  10 GB/月 |              $6.21 |  3,659〜3,944 GB、$9.12〜$9.83 |
| 標準 R2 Class B（対象バケット按分） |                         全体の97.98% |                アカウント全体で10M ops/月 |              $3.99 |                          $7.18 |
| 対象R2 storage                      |                       35.00 GB-month |               アカウント全体で10 GB-month |              $0.53 |                   $0.83〜$0.87 |
| `pc-keiba-r2-catalog` Worker変動費  | 152,516 invocations / 296,727 CPU-ms | アカウント全体で10M requests / 30M CPU-ms |       $0.00〜$0.04 |                   $0.01〜$0.06 |
| **直接帰属合計**                    |                                      |                                           | **$10.72〜$10.76** |             **$17.53〜$18.87** |

Catalog と R2 SQL の従量課金は 2026-08-03 に開始されたため、固有料金は同日 00:00 UTC から発生したものとして保守的に計算した。実際の請求開始時刻、無料枠の適用、請求期間は Cloudflare の Billable Usage / invoice が最終的な正となる。

## R2 Data Catalog

### 利用量

Catalog API は 749,404 operations で、無料枠の 74.9% を使用しています。

| operation         | operations | 構成比 |
| ----------------- | ---------: | -----: |
| `load-table`      |    518,251 |  69.2% |
| `get-config`      |    165,272 |  22.1% |
| `list-tables`     |     28,136 |   3.8% |
| `list-namespaces` |     27,975 |   3.7% |
| `update-table`    |      8,451 |   1.1% |
| その他            |      1,319 |   0.2% |

`load-table` と `get-config` だけで 91.2% を占めます。warehouse 別では `pc-keiba-r2-catalog` が 719,032 operations（96.0%）、`pc-keiba-odds-archive` が 29,243 operations（3.9%）です。

レスポンスは 748,089 件が HTTP 200、1,232 件が 204 でした。エラー系は 409 が59件、404が21件、500が3件です。

### Compaction

観測された maintenance job は256件です。

- compaction: 128件、すべて `success = 0`
- snapshot expiration: 128件、すべて成功
- compaction input/output bytes: 0 bytes
- compaction input files: 0 files

したがって compaction の従量料金は $0.00 です。これは「機能を使う必要がなかった」のではなく、**処理に成功したdata fileが1件もないため**です。

初回調査時点のCatalog設定と、2026-08-23 03:41 JSTの対応結果は次のとおりです。

| bucket                           | compaction | target | credential（調査時→現在） | snapshot expiration | 現在の状態            |
| -------------------------------- | ---------- | -----: | ------------------------- | ------------------- | --------------------- |
| `pc-keiba-r2-catalog`            | enabled    | 128 MB | **absent → present**      | disabled            | compaction実行可能    |
| `pc-keiba-odds-archive`          | enabled    | 128 MB | **absent → present**      | disabled            | compaction実行可能    |
| `pc-keiba-venue-weather-archive` | enabled    | 128 MB | **absent → present**      | disabled            | compaction実行可能    |
| `horse-racing-data`              | enabled    | 256 MB | present（再登録）         | enabled             | 次回jobの成功確認待ち |

Cloudflareの自動compactionはR2のdata fileを読み、書き直すためのservice credentialを必要とします。最初の3 bucketは調査時、設定上 `enabled` でも `credential_status = absent` だったため、実質的にはcompactionできない状態でした。

2026-08-23 03:41 JSTにWranglerで4 bucketのcatalog-level compactionを再度有効化し、service credentialを登録しました。Control Plane APIで全bucketが `compaction.state = enabled` かつ `credential_status = present` になったことを確認済みです。target sizeは既存値を維持し、`pc-keiba-r2-catalog`、`pc-keiba-odds-archive`、`pc-keiba-venue-weather-archive` は128 MB、`horse-racing-data` は256 MBです。snapshot expirationの設定は変更していません。

変更後に `pc-keiba-r2-catalog` warehouseへR2 SQLの `SHOW TABLES IN pc_keiba` を実行し、20テーブルを正常に列挙できることも確認しました。このmetadata queryは0 bytes / 0 files scannedで、既存のCatalog/SQL読み取り機能に影響がないことを確認するsmoke testです。

`horse-racing-data` は調査時からcredentialが存在していましたが、8月UTC集計では8テーブルに対するcompactionが120/120件失敗し、`filesProcessed = 0`、`inputBytes = 0` でした。上記128件との差は、上段がJST月初、こちらがUTC月初を使っているためです。対象は `jockey_info`、`horse_racing_records`、`breeder_info`、`horse_info`、`owner_info`、`race_info`、`race_records`、`trainer_info` です。credentialを再登録しましたが、Analytics APIは過去jobの失敗理由を返さないため、次回自動jobで `success = 1` になるか確認が必要です。

一方、snapshot expirationは `horse-racing-data` で120/120件成功しています。ただし処理ファイルは0件でした。snapshot expiration自体にはCatalog固有の追加料金はありません。

小さいファイルが残ると R2 SQL の scanned files と Class B read が増えるため、これは現在のSQL/R2費用に直接関係する運用問題です。

### Catalog 料金

- API operations: 1M/月まで無料、超過分は $9/M operations
- compaction data: 10 GB/月まで無料、超過分は $0.005/GB
- compaction objects: 1M/月まで無料、超過分は $2/M objects

8月3日の請求開始以降の Catalog API は 710,830 operations です。月末予測は、期間全体の直線予測で 1.04M operations、直近ペースで 1.10M operations となり、固有料金は $0.39〜$0.93 程度です。

## R2 SQL

### 利用量

8月 MTD 全体では、72,127 queries、成功したデータクエリは71,542件でした。

「どれだけ計算したか」をR2 SQLで観測できる単位にすると、**71,542件の成功data queryが、7,194,391 filesをscanし、圧縮済みfileを累計2,421.05 GB読み取った**という意味です。R2 SQL AnalyticsはCPU時間や演算回数、展開後のrow数を公開していないため、それ以上の「計算量」は測定できません。

query対象の `pc-keiba-r2-catalog` bucketは現在9.76 GBです。単純比較では2,421.05 GBはbucket全体の約248倍に相当しますが、実際にはpartition pruning、tableごとのサイズ、期間中のデータ増加があるため、これは「同じデータを繰り返し読んでいる規模感」を示す参考値です。課金上は最低10 MB/queryにより2,914.33 GB、約298 bucket相当まで増えます。

| 指標                                                    |         利用量 |
| ------------------------------------------------------- | -------------: |
| 成功したデータクエリが読み取った圧縮済みファイルのbytes |    2,421.05 GB |
| 1クエリ最低10 MBを反映した推計                          |    2,914.33 GB |
| scanned files                                           |      7,194,391 |
| R2 reads                                                |     23,510,267 |
| cold reads                                              |        238,419 |
| hot/cache reads                                         |     23,271,848 |
| 合計 latency                                            | 649,863,972 ms |

成功クエリ1件あたりの平均は、約100.6 files scanned、約328.6 reads、約9.1秒です。

ここでいう「圧縮済みファイルのbytes」とは、R2 SQLがR2上のParquetなどの圧縮済みdata fileから実際に読み取った量（GraphQLの `r2BytesRead`）です。SQLのレスポンスを圧縮したサイズでも、展開後の行データ量でもありません。たとえば100 MBのParquetを読み、展開後に500 MB相当の列データになっても、scan料金の基準は原則として読み取った圧縮済みbytesです。

その実測 2,421.05 GB に対し、最低10 MB/成功クエリの影響で推計課金量は 493.28 GB（20.4%）増えています。現行単価ではこの差だけで約 $1.23 です。

テーブル別の圧縮 bytes read は `pc_keiba.nvd_ra` が約87.6%、`pc_keiba.jvd_ra` が約11.4%で、この2テーブルがほぼ全量を占めます。

失敗は585件（約0.81%）で、うち434件は HTTP 408 / error 40005 でした。失敗クエリは R2 SQL scan 料金の対象外ですが、タイムアウト削減は再実行と待ち時間の削減につながります。

### R2 SQL 料金

- 10 GB scanned/月まで無料
- 超過分は $0.0025/GB（$2.50/TB）
- 成功したデータクエリは最低10 MBとして課金
- failed query と metadata-only query は scan 料金の対象外

8月3日の請求開始以降では、最低10 MBを反映した推計 scan は 2,492.95 GB です。

```text
(2,492.95 GB - 10 GB) × $0.0025/GB = $6.21
```

最低10 MBは秒単位の query event group ごとに適用して推計しています。同じ秒・同じ属性で複数クエリが集約された場合は個々の bytes 分布を復元できないため、実際の請求量よりわずかに低くなる可能性があります。

### 月間scanが10 TBになった場合

ここでの10 TBは、保存量ではなく、R2 SQLの1か月の課金対象scan量が累計10,000 GBになるケースです。

```text
(10,000 GB - 10 GB) × $0.0025/GB = $24.98
```

請求開始後の現状2,492.95 GBから10,000 GBまで増える場合、追加scanは7,507.05 GB、R2 SQLの追加費用は **$18.77**、R2 SQL合計は **$24.98** です。「10 TBまで」ではなく「現状からさらに10 TB追加」の意味なら、R2 SQLは追加 **$25.00**、合計 **$31.21** です。

標準R2 Class Bはbytesではなくread request数で決まります。8月MTDの課金対象scan 2,914.33 GBと対象bucketの20,873,380 readsの比率がそのまま続くと仮定すると、10 TB時点の対象readsは約71.62M、他bucketの現在値を固定したアカウント全体は約72.05Mです。Class Bは概算 **$22.34**、100万request単位の切り上げを保守的に反映すると **$22.68** です。

| 10 TB scan時の直接費              |                  概算 |
| --------------------------------- | --------------------: |
| R2 SQL scan                       |                $24.98 |
| 標準 R2 Class B                   |        $22.34〜$22.68 |
| R2 storage（171.0〜209.5 GB）     |          $2.41〜$2.99 |
| Catalog operations（約2.57M ops） |                $14.14 |
| Catalog compaction                |                 $0.62 |
| Pipelines                         |                 $0.00 |
| query gateway Worker              |          $0.01〜$0.14 |
| **直接帰属合計**                  | **$64.51〜$65.56/月** |

Class Bはfile数の影響を強く受けるため、この総額は「現在と同じquery形状・小file構成・read効率」を置いた比例試算です。compactionでClass Bが無料枠内まで減れば、直接帰属合計は **$42.17〜$42.88/月** まで下がります。

## Neon / BigQuery / AWS S3 Tablesとの比較

### 比較条件

ここでの2 TB / 10 TBは、前節と同じく**1か月の課金対象scan量**です。8月MTDの22.13日間を月換算すると、課金対象scanは約4,082.33 GB/月、成功queryは約100,214件/月です。前版は月途中の2,914.33 GBをそのまま月間10 TBと比較して保存量を過大推定していたため修正しました。

関連4 bucketは現在69.79 GB、現在の増加ペースが続く月末推定は85.54 GBです。月間scan 4,082.33 GBとの比率を維持すると次の保存容量になります。

| 月間scan |       成功query |     全query | query間隔 |          推定保存容量 |
| -------- | --------------: | ----------: | --------: | --------------------: |
| 2 TB     |  **約49,097件** |  約49,498件 |  約54.1秒 |   **34.19〜41.91 GB** |
| 10 TB    | **約245,484件** | 約247,491件 |  約10.8秒 | **170.96〜209.55 GB** |

```text
月間scan実績 = 2,914.33 GB × 31日 / 22.13日 = 4,082.33 GB
2 TB時保存量 = 69.79〜85.54 GB × 2,000 / 4,082.33 = 34.19〜41.91 GB
10 TB時保存量 = 69.79〜85.54 GB × 10,000 / 4,082.33 = 170.96〜209.55 GB
query数 = 71,542件 × target scan / 2,914.33 GB
```

次の利用特性も現在値から比例させています。

- 1 queryあたり課金scanは実績約40.74 MBとして、scan量とquery数を比例
- 現在の小file状態では、2 TB時に約14.32M、10 TB時に約71.62Mのdata-object read
- 月間純増保存量は2 TB時26.97 GB、10 TB時134.85 GB
- 月間新規objectは2 TB時約58,265件、10 TB時約291,326件
- Catalog operationsは1成功queryあたり実績約10.47 opsとして、2 TB時約0.51M、10 TB時約2.57M
- 合計latencyは1成功queryあたり実績約9.08秒として、2 TB時約123.9時間、10 TB時約619.4時間
- outbound transferはNeon CLIの実測14.25 GB MTDをquery数で按分し、2 TB時9.78 GB、10 TB時48.90 GB
- BigQueryはus-central1のon-demand/native logical storage、S3 Tablesは公式例と同じOregon、Neonは実プロジェクトのSingapore/Launchプラン
- 税、為替、移行作業費は除外。client outboundはNeon実測量をproxyとして含め、それを超えるbulk exportは除外

### データ容量と転送量

scanはquery engine内部で読み取る量であり、clientへ転送する量とは異なります。登録量とclient outboundは実績比から次のように推定しました。

| データフロー         |       2 TB scan |        10 TB scan | 算定根拠                               |
| -------------------- | --------------: | ----------------: | -------------------------------------- |
| 保存容量             | 34.19〜41.91 GB | 170.96〜209.55 GB | 現在〜月末予測のstorage / 月換算scan比 |
| 月間登録・更新量     |        26.97 GB |         134.85 GB | 8月の純増39.30 GBを月換算して比例      |
| query engine内部scan |        2,000 GB |         10,000 GB | 比較条件                               |
| clientへのoutbound   |         9.78 GB |          48.90 GB | Neon実測14.25 GB MTDをquery数で按分    |

| 転送料金                    |  2 TB | 10 TB | 理由                                                    |
| --------------------------- | ----: | ----: | ------------------------------------------------------- |
| R2 / R2 SQL                 | $0.00 | $0.00 | R2 egressは無料                                         |
| BigQuery API query results  | $0.00 | $0.00 | 通常のBigQuery APIでquery resultsへaccessする転送は無料 |
| AWS S3 / Athena results     | $0.00 | $0.00 | AWS全体の最初の100 GB/月DTO無料枠内                     |
| Neon Launch public transfer | $0.00 | $0.00 | 100 GB/月まで含まれる                                   |

AWSでは転送費が$0でも、Athena resultをS3へ保存・取得するPUT/GETとstorageは別途発生するため総額へ含めています。BigQuery Storage Read APIで大量のrow dataをstreaming取得する構成に変える場合は、通常query-result APIとは異なるStorage Read API/network料金を再計算する必要があります。

### 月額比較

| 構成                                          |          2 TB scan |           10 TB scan | 主な費用要因                                               |
| --------------------------------------------- | -----------------: | -------------------: | ---------------------------------------------------------- |
| **Cloudflare R2（compaction安定後）**         |   **$5.43〜$5.57** |   **$42.17〜$42.88** | R2 SQL、storage、Catalog、compaction、Worker、transfer     |
| Cloudflare R2（現在の小file比率）             |       $7.14〜$7.37 |       $64.51〜$65.56 | 上記 + 大量Class B read                                    |
| **Google BigQuery native**                    |   **$5.62〜$5.81** |   **$54.04〜$54.99** | on-demand query、active logical storage、gateway、transfer |
| **AWS S3 Tables + Athena（compaction後）**    | **$10.76〜$11.16** |   **$55.35〜$57.36** | Athena、storage、data/result PUT/GET、Glue、compaction     |
| AWS S3 Tables + Athena（現在の小file比率）    |     $18.91〜$19.31 |       $96.12〜$98.12 | 大量GETとobject monitoringが追加                           |
| **Neon Launch（実プロジェクト消費量で補正）** | **$62.12〜$64.85** | **$233.23〜$246.86** | Postgres storage、実測CU、PITR、gateway、transfer          |

結論として、2 TBではcompaction後R2とBigQueryがほぼ同水準です。10 TBではcompaction後R2が約$42で最安、BigQueryとcompaction後S3 Tablesが約$54〜57です。R2の10 TBではquery増加によりCatalog operationsが約2.57Mとなり、Catalog費約$14.14が無視できません。compactionが効かず現在の小file比率が続く場合はR2も約$65まで増えます。

### Cloudflare R2 / R2 Data Catalog

2 TBの場合の中間値は次のとおりです。

```text
R2 SQL       = (2,000 - 10) GB × $0.0025 = $4.98
R2 storage   = (34.19〜41.91 - 10) GB × $0.015 = $0.36〜$0.48
compaction   = (26.97 - 10) GB × $0.005 = $0.08
Catalog      = 0.51M ops < 1M free = $0.00
Worker       = $0.00〜$0.03
transfer     = 9.78 GB × $0 = $0.00
合計         = $5.43〜$5.57
```

10 TBではR2 SQL $24.98、storage $2.41〜$2.99、compaction $0.62、Catalog $14.14、Worker $0.01〜$0.14、transfer $0で、compaction安定後は$42.17〜$42.88です。Pipelinesの月間transform/sink量も現在比で比例させましたが、10 TBケースでも50 GB無料枠内のため$0です。

compaction後は128 MB前後のfileへ集約され、推定Class Bが10M無料枠内へ収まるものとして$0としました。現在のread/file比率が変わらない悲観ケースでは、Class Bは2 TBで$1.71〜$1.80、10 TBで$22.34〜$22.68です。[R2 SQL pricing](https://developers.cloudflare.com/r2-sql/platform/pricing/)、[R2 pricing](https://developers.cloudflare.com/r2/pricing/)、[R2 Data Catalog pricing](https://developers.cloudflare.com/r2-data-catalog/platform/pricing/)、[Pipelines pricing](https://developers.cloudflare.com/pipelines/platform/pricing/)

### Google BigQuery

BigQuery on-demandは最初の1 TiB/月が無料、その後$6.25/TiBです。BigQueryはTiB（2^40 bytes）基準なので、10,000 GBは9.0949 TiBです。

```text
2 TB query  = (1.8190 - 1) TiB × $6.25 = $5.12
10 TB query = (9.0949 - 1) TiB × $6.25 = $50.59
```

native active logical storageは$0.023/GiB-month、最初の10 GiB無料として、保存費は2 TBケースで$0.50〜$0.67、10 TBケースで$3.43〜$4.26です。通常のBigQuery APIでquery resultsを取得するdata transferは無料なので、推定9.78 GB / 48.90 GBのtransfer費も$0です。gatewayを含む総額は$5.62〜$5.81 / $54.04〜$54.99です。

BigQuery native tableではmetadata catalogやcompactionの独立請求はありません。batch loadは無料です。現在の登録量から比例した26.97 GB / 134.85 GBをStorage Write APIでstreaming登録しても、最初の2 TiB/月無料枠内なので$0です。BigQueryは約49,498 / 247,491 jobs自体へのrequest単価もありません。query result cacheがhitしたqueryは無料なので、同一queryがcache可能なら実費はさらに下がります。[BigQuery pricing](https://cloud.google.com/bigquery/pricing)

注意点として、R2 SQLはR2上の圧縮済みParquet bytesを数えますが、BigQuery nativeは列のlogical sizeを数えます。本比較は「各サービスで請求画面に現れるscanが2 TB / 10 TB」と揃えたもので、同じSQLを移植したときのbillable bytesが完全に一致する保証はありません。

### AWS S3 Tables + Athena

S3 Tables単体にはSQL engineがないため、Athenaを追加しています。Athenaは$5/TiB scannedで無料scan枠はありません。

```text
2 TB query  = 1.8190 TiB × $5 = $9.09
10 TB query = 9.0949 TiB × $5 = $45.47
```

S3 TablesのOregon公式単価はstorage $0.0265/GiB-month、PUT $0.005/1,000、GET $0.0004/1,000、object monitoring $0.025/1,000 objectsです。binpack compactionは$0.002/1,000 objects + $0.005/GiB processedです。

| S3 Tables追加要素                 |               2 TB |              10 TB |
| --------------------------------- | -----------------: | -----------------: |
| table storage                     |       $0.84〜$1.03 |       $4.22〜$5.17 |
| data PUT                          |              $0.29 |              $1.46 |
| binpack compaction                |              $0.24 |              $1.21 |
| GET（100 MB/fileへcompaction後）  |              $0.01 |              $0.04 |
| object monitoring（compaction後） |              $0.01 |       $0.04〜$0.05 |
| Glue Data Catalog                 |              $0.00 |              $1.57 |
| Athena result PUT/GET             |              $0.27 |              $1.34 |
| Athena result storage推定         |       $0.00〜$0.21 |       $0.00〜$1.05 |
| internet transfer                 |              $0.00 |              $0.00 |
| Athena                            |              $9.09 |             $45.47 |
| **合計**                          | **$10.76〜$11.16** | **$55.35〜$57.36** |

Athenaは各queryのresultを通常のS3 bucketへ書くため、約49,498 / 247,491 query分のPUT/GETを追加しました。result容量はNeonの実測transfer量9.78 GB / 48.90 GBを上限proxyとして保存費へ含めています。internet transferはAWS全体の最初の100 GB/月無料枠内なので$0です。

現在の小file数とread比率をそのまま移すと、10 TB時にはGET $28.65、object monitoring約$12.16が追加され、合計$96.12〜$98.12になります。このためS3 Tablesでは自動compactionを有効にすることが費用上も必須です。Glue Data Catalog requestも約2.57Mまで増える想定のため、10 TBでは無料1M超過分を$1.57として含めました。[S3 Tables pricing](https://aws.amazon.com/s3/pricing/)、[Athena pricing](https://aws.amazon.com/athena/pricing/)、[AWS Glue pricing](https://aws.amazon.com/glue/pricing/)

### Neon

Neonはscan bytes課金ではなく、storageとCU-hour課金です。Neon CLIで実プロジェクトを確認した結果は、Launch、Singapore、0.25〜8 CU、auto-suspend 300秒、1 branch、論理data約70.22 GBでした。Launch公式単価は$0.106/CU-hour、$0.35/GB-month、PITR historyは$0.20/GB-monthです。

実績から比例した全queryは約49,498 / 247,491件、平均間隔は54.1秒 / 10.8秒です。どちらも300秒auto-suspendより短いため、queryをまとめない限りcomputeは事実上常時起動します。最小0.25 CUの月間floorは次のとおりです。

```text
0.25 CU × 730 hours × $0.106 = $19.35/月
```

| Neon追加要素                          |               2 TB |                10 TB |
| ------------------------------------- | -----------------: | -------------------: |
| data storage                          |     $11.97〜$14.67 |       $59.84〜$73.34 |
| 実プロジェクト消費量で補正したcompute |             $50.11 |              $173.16 |
| 6時間PITR history推計                 |              $0.04 |                $0.22 |
| public network transfer               |   $0.00（9.78 GB） |    $0.00（48.90 GB） |
| gateway                               |       $0.00〜$0.03 |         $0.01〜$0.14 |
| **実績補正合計**                      | **$62.12〜$64.85** | **$233.23〜$246.86** |

実プロジェクトではMTD `compute_time_seconds = 1,991,447`、data transfer 14.25 GB、論理data 70.22 GBでした。computeを月換算すると約$82.14です。このうち0.25 CUの常時起動floor $19.35を固定し、残りをquery/scan量に比例させました。

```text
target compute = $19.35 + ($82.14 - $19.35) × target scan / 4,082.33 GB
```

このcomputeには既存Neon projectのR2代替以外の処理も含まれるため、単独システムの純増額としては上限寄りです。参考として、R2 SQLの実測latency 9.08秒/queryと平均0.5〜2 CUを使う独立modelでは、総額は2 TBで$34.64〜$57.07、10 TBで$95.83〜$207.95です。ただしPostgreSQLのjoin、sort、index、autovacuum特性はR2 SQLと異なるため、最終的にはproduction相当queryのbenchmarkが必要です。

Public network transferは100 GB/月まで含まれ、今回の9.78 GB / 48.90 GBは$0です。scan bytesはNeon内部I/Oであり、そのままnetwork transferにはなりません。[Neon pricing](https://neon.com/pricing)、[Neon compute sizes](https://neon.com/docs/manage/endpoints/)、[Neon network transfer](https://neon.com/docs/introduction/network-transfer)

### 機能と追加課金の対応

| R2 / R2 Data Catalogの機能 | Neon                            | BigQuery                                                  | S3 Tables + Athena                             |
| -------------------------- | ------------------------------- | --------------------------------------------------------- | ---------------------------------------------- |
| object storage             | Postgres storage $0.35/GB-month | native logical storage $0.023/GiB-month                   | table storage $0.0265/GiB-month                |
| serverless SQL scan        | CU-hour。bytes単価なし          | $6.25/TiB、1 TiB無料                                      | Athena $5/TiB、無料scanなし                    |
| Iceberg catalog            | PostgreSQL system catalog内     | native metadata内                                         | Glue Catalog。2 TBは無料、10 TBは約$1.57       |
| automatic compaction       | autovacuum等のCUへ内包          | native管理に内包                                          | objects + bytesの明示課金                      |
| object read requests       | 独立課金なし                    | 独立課金なし                                              | GETを別途課金                                  |
| object monitoring          | 独立課金なし                    | 独立課金なし                                              | object数に比例して別途課金                     |
| streaming ingest           | DB writeのCU + WAL/PITR         | Storage Write API、2 TiB無料                              | PUT + compaction。ETL engineは別料金になり得る |
| egress（R2は無料）         | 100 GB超$0.10/GB                | 通常query resultsは無料。Storage Read/bulk exportは条件別 | S3 DTOは100 GB超から課金                       |

## データ登録・保存量と費用

### 現在の登録量

R2 storageのdaily peakから、Catalogに使っている4 bucketの8月1日と8月22日を比較しました。

| bucket                           |       8月1日 |      8月22日 |       8月純増 | 現在objects | objects純増 |
| -------------------------------- | -----------: | -----------: | ------------: | ----------: | ----------: |
| `pc-keiba-odds-archive`          |     23.10 GB |     59.96 GB | **+36.86 GB** |     171,586 |     +77,419 |
| `pc-keiba-r2-catalog`            |      7.34 GB |      9.76 GB |  **+2.43 GB** |      18,877 |      +4,101 |
| `pc-keiba-venue-weather-archive` |     0.047 GB |     0.066 GB |     +0.019 GB |       8,008 |      +3,382 |
| `horse-racing-data`              |   0.00024 GB |   0.00024 GB |          0 GB |         119 |           0 |
| **合計**                         | **30.49 GB** | **69.79 GB** | **+39.30 GB** | **198,590** | **+84,902** |

これは現在R2に保存されているpayload全体で、Parquet data fileだけでなくIceberg metadata、manifest、snapshotに関連するobjectも含みます。「今月登録した論理rowのサイズ」とは一致しません。

### Pipelines経由の登録

2026-08-03の課金開始以降、oddsとweatherのPipelinesは次の量を処理しました。

| Pipelines指標                       |                   実績 |             無料枠 |  費用 |
| ----------------------------------- | ---------------------: | -----------------: | ----: |
| Streams ingress                     |                6.19 GB |          unlimited | $0.00 |
| SQL transforms                      |                6.19 GB |           50 GB/月 | $0.00 |
| Iceberg sinks（課金基準の非圧縮量） |               0.462 GB |           50 GB/月 | $0.00 |
| R2へ書かれた圧縮済みdata files      | 0.224 GB / 6,513 files | 課金基準外の参考値 |     — |

Pipelinesのsink料金はR2へ保存された圧縮後サイズではなく、sinkへ渡した**圧縮前データ量**に対して$0.06/GBです。現在はSQL transform、sinkとも50 GB無料枠内なので、登録時のPipelines費用は **$0.00** です。

### Catalog metadataとR2 write

| 登録時の課金要素           |               実績 |          現在の費用 |
| -------------------------- | -----------------: | ------------------: |
| Catalog `update-table`     |   8,451 operations |               $0.00 |
| Catalog `create-table`     |       9 operations |               $0.00 |
| Catalog operations全体     | 749,404 operations | $0.00（1M無料枠内） |
| 対象4 bucketのR2 Class A   | 139,400 operations |               $0.00 |
| アカウント全体のR2 Class A | 674,020 operations | $0.00（1M無料枠内） |

R2 Class Aの139,400 operationsへ無料枠を無視して単価だけを掛けると$0.63相当ですが、アカウント全体でも1M無料枠内なので実費は$0です。Catalogのmetadata更新も読み取りと同じ1M operations無料枠を共有します。

### R2 storage

4 bucketのdaily peakを8月31日までのGB-monthへ按分すると、8月22日までに約35.00 GB-monthを消費しており、無料枠を無視したstorage費用は約 **$0.53** です。アカウントの10 GB-month無料枠は他bucketを含む全体で既に消費されているため、対象bucketの限界費用としては概ねこの$0.53が発生すると考えるのが安全です。

現在の69.79 GBを1か月保持した場合のrun rateは約 **$1.05/月** です。これは登録時の一回限りの料金ではなく、削除するまで毎月発生する保存料金です。

### 登録費用の結論

- Pipelines transform / sink: **$0.00**
- Catalog metadata write: **$0.00**
- R2 Class A write: **$0.00**
- 8月22日までの対象R2 storage限界費用: **約$0.53**
- 取り込みWorker全処理の広い按分: `sync-realtime-data-hot` 約$0.53、`venue-weather` <$0.01

したがってCloudflare側へデータを登録・保存する直接費は現時点で **約$0.53**、取り込みWorkerの全処理を広く含めると上限寄りで **約$1.06** です。Mac上で動くPyIceberg同期処理の電力・端末compute費用は含みません。

odds Pipelineの圧縮済みdata outputが約0.22 GBなのに、`pc-keiba-odds-archive` のR2 payloadが8月に36.86 GB増えている点は重要です。差分には既存dataの増加も含まれるため断定はできませんが、増加object 77,419件の多くがIceberg metadata、manifest、過去snapshot関連である可能性が高いです。このbucketはsnapshot expirationが無効なので、compactionだけでなくsnapshot retentionを確認しないとstorage増加が続く可能性があります。

## 標準 R2 request 料金

R2 SQL は scan 料金とは別に標準 R2 Class B request の対象です。8月1日 00:00 UTC 以降のアカウント全体の利用量は次のとおりです。

| 指標                              |           operations |                    費用 |
| --------------------------------- | -------------------: | ----------------------: |
| Class A                           |              673,980 |     $0.00（1M無料枠内） |
| Class B                           |           21,302,850 |                   $4.07 |
| うち `pc-keiba-r2-catalog` bucket | 20,873,380（97.98%） | $3.99（利用比率で按分） |

```text
(21,302,850 - 10,000,000) ÷ 1,000,000 × $0.36 = $4.07
$4.07 × 97.98% = $3.99
```

この按分には対象バケットを読む R2 SQL 以外のクライアントが含まれる可能性があります。そのため、$3.99 は R2 SQL への厳密な帰属額ではなく、バケット利用比率による管理会計上の配賦額です。

## 関連Workerの利用量・費用

Workers Standardはアカウント全体で月10M requestsと30M CPU-msが含まれ、超過分はrequestsが$0.30/M、CPUが$0.02/M CPU-msです。Workers Paid planの最低料金 $5/月はアカウント全体の固定費です。

8月1日 00:00 UTC以降のアカウント全体では49,078,101 invocations、108,020,978 CPU-msでした。Analyticsから機械的に算出した変動費はrequests $11.72、CPU $1.56で、固定費を含むWorkers全体は **$18.28** です。請求APIは現在のtokenにBilling read権限がなく取得できなかったため、これはinvoice値ではなくAnalyticsベースの推計です。

Catalog/SQLに直接または間接的に関係するWorkerは次のとおりです。

| Worker                        | 関係                          |   invocations |         CPU-ms | アカウント超過額の按分 |
| ----------------------------- | ----------------------------- | ------------: | -------------: | ---------------------: |
| `sync-realtime-data`          | Catalog queryの呼び出し元     |       968,334 |     38,314,468 |                  $0.78 |
| `sync-realtime-data-hot`      | odds Catalogへの取り込み元    |     1,461,125 |     12,280,512 |                  $0.53 |
| `pc-keiba-viewer`             | Catalog queryの呼び出し元     |       592,486 |     11,716,272 |                  $0.31 |
| `sync-realtime-data-features` | Catalog queryの呼び出し元     |        78,925 |      1,741,513 |                  $0.04 |
| `pc-keiba-r2-catalog`         | R2 SQL query gateway          |       152,516 |        296,727 |                  $0.04 |
| `finish-position-cron`        | Catalog queryの呼び出し元     |        88,942 |        409,679 |                  $0.03 |
| `venue-weather`               | weather Catalogへの取り込み元 |         2,249 |          8,359 |                 <$0.01 |
| **関連Worker合計**            |                               | **3,344,577** | **64,767,528** |              **$1.73** |

按分額は、アカウント全体のrequest超過額を各Workerのinvocation比率、CPU超過額をCPU比率で配賦したものです。各Workerの全機能が対象なので、Catalog/SQLだけの厳密な増分費用ではありません。

特に `pc-keiba-r2-catalog` はService Binding経由で呼ばれています。Workers StandardではWorker-to-WorkerのService Binding subrequestに追加request料金はかからず、CPU時間は呼び出し元と呼び出し先を通算します。このため直接gatewayに帰属する実費は、CPUのみとみなす約$0.004から、Analytics invocationも比例配賦した上限寄りの$0.041までのレンジで示しています。subrequests自体はrequest料金へ加算していません。

Workers固定費 $5/月はこのシステム専用ではなく、他の多数のWorkerと共有しているため、直接帰属合計には含めていません。

## 月末予測

次の2方式をレンジとして使いました。

1. 直線予測: 請求開始後の平均利用量を月末まで延長
2. 直近ペース: SQL は直近6完了UTC日の平均 156.96 GB/日、Catalog は直近7日グループの平均を残日数へ適用

| 区分                         |           直線予測 |     直近ペース予測 |
| ---------------------------- | -----------------: | -----------------: |
| Catalog 固有料金             |              $0.39 |              $0.93 |
| R2 SQL scan                  |              $9.12 |              $9.83 |
| 標準 R2 Class B 按分         |              $7.18 |              $7.18 |
| 対象R2 storage               |       $0.83〜$0.87 |       $0.83〜$0.87 |
| `pc-keiba-r2-catalog` Worker |       $0.01〜$0.06 |       $0.01〜$0.06 |
| **直接帰属合計**             | **$17.53〜$17.63** | **$18.78〜$18.87** |

関連Worker全体の広い按分額は現時点で$1.73、月末直線予測で$2.71です。これを含める管理会計上の合計は、現時点 **$12.45**、月末 **$20.23〜$21.52** です。

Class B の直近ペースは日別帰属を正確に分離できないため、両ケースとも MTD の直線予測を使用しています。

## コスト削減の優先順位

1. **compaction 失敗を直す**
   現在は料金ゼロですが、128/128件失敗しています。小さいファイルをまとめられれば scanned files と Class B read の両方を下げられる可能性があります。

2. **`nvd_ra` のクエリ回数・対象期間を減らす**
   scan bytes の約87.6%を占めています。partition pruning、期間条件、同一条件の結果キャッシュ、複数小クエリの集約が第一候補です。

3. **小さい R2 SQL query をまとめる**
   10 MB minimum により推計課金量が20.4%増えています。ポーリングや馬・レース単位の細分化されたクエリをまとめると、scan と Class B の双方に効きます。

4. **Catalog metadata call をキャッシュする**
   `load-table` と `get-config` が91.2%です。短いTTLでも同一プロセス・同一リクエスト内の重複取得を避ければ、月末の1M無料枠超過を抑えられます。

## 算定上の注意

- Catalog Analytics は adaptive sampling を使うため、operations と sums は推計値を含みます。今回の最大 sample interval は4でした。
- R2 SQL の query-level minimum は Analytics API の秒単位集約から再計算した推計です。
- 無料枠はアカウント単位です。他バケットや他サービスの利用状況で最終的な配賦額は変わります。
- Service Bindingは追加request課金されないため、Worker invocation数と請求対象request数は一致しない場合があります。
- Billing APIは現在のtokenでは `insufficient_permissions` だったため、Workers費用はGraphQL Analyticsからの推計です。
- 標準 R2 storage 料金はqueryだけへ厳密に帰属できないため、関連4 bucket全体を直接費へ含めています。サービス比較では現在のscan対storage比で比例させています。
- Cloudflare の Billable Usage は請求書に合わせた期間・丸めを使うため、本レポートとの差異があり得ます。

## 参照した公式資料

- [R2 Data Catalog pricing](https://developers.cloudflare.com/r2-data-catalog/platform/pricing/)
- [R2 Data Catalog metrics](https://developers.cloudflare.com/r2-data-catalog/observability/metrics/)
- [R2 Data Catalog billing enabled — 2026-08-03](https://developers.cloudflare.com/changelog/post/2026-08-03-r2-data-catalog-billing-enabled/)
- [R2 SQL pricing](https://developers.cloudflare.com/r2-sql/platform/pricing/)
- [R2 SQL changelog](https://developers.cloudflare.com/changelog/product/r2-sql/)
- [R2 pricing](https://developers.cloudflare.com/r2/pricing/)
- [Pipelines pricing](https://developers.cloudflare.com/pipelines/platform/pricing/)
- [Pipelines metrics](https://developers.cloudflare.com/pipelines/observability/metrics/)
- [Pipelines billing enabled — 2026-08-03](https://developers.cloudflare.com/changelog/post/2026-08-03-pipelines-billing-enabled/)
- [Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)
- [R2 Data Catalog table maintenance](https://developers.cloudflare.com/r2-data-catalog/table-maintenance/)
- [Manage R2 Data Catalogs](https://developers.cloudflare.com/r2-data-catalog/manage-catalogs/)
- [Cloudflare Billable Usage](https://developers.cloudflare.com/billing/manage/billable-usage/)
- [Neon pricing](https://neon.com/pricing)
- [Neon compute sizes and autoscaling](https://neon.com/docs/manage/endpoints/)
- [Neon network transfer](https://neon.com/docs/introduction/network-transfer)
- [BigQuery pricing](https://cloud.google.com/bigquery/pricing)
- [Amazon S3 Tables pricing](https://aws.amazon.com/s3/pricing/)
- [Amazon Athena pricing](https://aws.amazon.com/athena/pricing/)
- [AWS Glue pricing](https://aws.amazon.com/glue/pricing/)
