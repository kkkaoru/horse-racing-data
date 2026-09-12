# 天気特徴量パイプライン (ローカル学習 → CF Worker 特徴量収集 → CF Container 着順予測)

最終調査日: 2026-06-24

このドキュメントは、Open-Meteo 由来の会場別天気データが「ローカル学習 → 本番特徴量収集 → 本番着順予測」のどこをどう流れるかを、コード上の行番号を添えて記述する。

---

## 0. 30 秒サマリ

- **天気の生データ**: Open-Meteo API → `venue-weather` Worker が D1 (`venue_weather` テーブル) に時間別で保存。学習用には同じデータが `apps/venue-weather/data/venue_weather_YYYY.duckdb` (2013-2026, 25 会場) として配布される。
- **天気から作られる特徴量列** (`finish_position_features_duckdb.py` が生成):
  - 集約 4 列: `venue_temperature` / `venue_precipitation_total` / `venue_wind_speed_max` / `venue_wind_gusts_max` (9-17 時で集約)
  - 交互作用 5 列: `rain_x_speed_decay` / `wind_x_front_runner` / `wind_x_field_size` / `rain_x_track_condition` / `cold_x_speed_effect`
  - レコード由来 2 列 (Open-Meteo 非依存): `weather_normalized` (tenko_code 由来) / `track_condition_normalized` (babajotai_code 由来)
- **重要な現状 (2026-06-24)**: **本番デプロイ済みの 3 モデル (JRA `iter20-jra-cb-2013-v8` / NAR `iter12-nar-xgb-hpo-v8` / Ban-ei `banei-cb-v8-window2011-wf-15y`) は、Open-Meteo 由来の `venue_*` 列・交互作用 5 列を `feature_names` に含んでいない**。含むのはレコード由来の `weather_normalized` / `track_condition_normalized` のみ。つまり venue-weather Worker → Container の天気フェッチ経路は本番で稼働するが、scorer は `feature_names` だけを射影するため、`venue_*` 列は「計算されてから捨てられている」。
- **これが効いてくるのは v9 系**: `banei-cb-v9-weather-2011` (学習済み、`apps/pc-keiba-viewer/tmp/models/`、feature_count=138) が **初めて 4 集約列 + 交互作用 5 列を `feature_names` に取り込んだモデル**。このモデルをデプロイすると、初めて Container の天気フェッチ経路が「実際に着順スコアに効く」状態になる。パリティ確認 (§5) はこの v9 デプロイを安全にするためのもの。

---

## 1. ローカル学習フロー

### 1.1 フィーチャストア構築 — `finish_position_features_duckdb.py`

ファイル: `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`

#### `--venue-weather-dir` の処理

- CLI 定義: 行 267-279。`venue_weather_YYYY.duckdb` を含むディレクトリを受け取る。未指定なら天気列は NULL。
- 競馬時間帯定数: `VENUE_WEATHER_RACE_HOUR_MIN = 9` / `VENUE_WEATHER_RACE_HOUR_MAX = 17` (行 87-90)。
- 読み込み + 集約: `materialize_venue_weather()` (行 1656-1707)。
  - 年ごとの DuckDB を read-only で ATTACH し `keibajo_code, weather_date, weather_hour, temperature, precipitation, wind_speed, wind_gusts` を UNION (行 1683-1690)。
  - `weather_hour BETWEEN 9 AND 17` で絞り、`(keibajo_code, weather_date)` で集約 (行 1691-1703):

    ```sql
    avg(temperature)   AS venue_temperature
    sum(precipitation) AS venue_precipitation_total
    max(wind_speed)    AS venue_wind_speed_max
    max(wind_gusts)    AS venue_wind_gusts_max
    ```

  - 空のスタブテーブル定義 (天気なし時): 行 1635-1642。
  - 対象レース行への LEFT JOIN: `keibajo_code` と `weather_date` でマッチ (行 1728 付近)。

#### 天気から生成される特徴量列 (正確な列名)

| 列名                         | 由来                                                    | 行               |
| ---------------------------- | ------------------------------------------------------- | ---------------- |
| `venue_temperature`          | `avg(temperature)` 9-17時                               | 1695, 1719, 1828 |
| `venue_precipitation_total`  | `sum(precipitation)` 9-17時                             | 1696, 1720, 1829 |
| `venue_wind_speed_max`       | `max(wind_speed)` 9-17時                                | 1697, 1721, 1830 |
| `venue_wind_gusts_max`       | `max(wind_gusts)` 9-17時                                | 1698, 1722, 1831 |
| `rain_x_speed_decay`         | `venue_precipitation_total * speed_index_avg_5`         | 1832             |
| `wind_x_front_runner`        | `venue_wind_speed_max * past_nige_rate_self`            | 1833             |
| `wind_x_field_size`          | `venue_wind_speed_max * (shusso_tosu / MAX_FIELD_SIZE)` | 1844             |
| `rain_x_track_condition`     | 降水量 × 路面状態                                       | 1845-1851        |
| `cold_x_speed_effect`        | `(20.0 - venue_temperature) * speed_index_avg_5`        | 1852             |
| `weather_normalized`         | **tenko_code 由来** (Open-Meteo 非依存)                 | 1823-1827        |
| `track_condition_normalized` | **babajotai_code 由来** (Open-Meteo 非依存)             | 1853-1858        |

> 注: `weather_normalized` / `track_condition_normalized` はレース成績レコードの天気・馬場コードから計算されるため、`--venue-weather-dir` 未指定でも値が入る。`venue_*` と交互作用 5 列は Open-Meteo データがないと NULL (→ COALESCE で 0 / 既定値)。

#### 出力先

- `--output-dir` (既定 `tmp/finish-position-features-parquet`) 配下に `race_year=YYYY/data_0.parquet` で年パーティション出力 (`write_parquet`, 行 2033-2038)。

#### 主な CLI 引数

`--category` / `--from-date` / `--to-date` / `--target-date` / `--days-ahead` / `--output-dir` / `--pg-url` / `--threads` / `--memory-limit` / `--venue-weather-dir` / `--realtime-odds` / `--allow-empty-targets` ほか (行 211-305)。

### 1.2 学習スクリプト

- walk-forward 学習: `train_finish_position_catboost_walk_forward.py` / `train_finish_position_xgboost_walk_forward.py` / `train_finish_position_lightgbm_walk_forward.py` (いずれも `apps/pc-keiba-viewer/src/scripts/`)。
- 推論/評価用: `finish_position_catboost.py` / `finish_position_xgboost.py` / `finish_position_lightgbm_walk_forward.py`。
- フィーチャストア読み込み: `load_parquet_dir()` が `path.glob("race_year=*/*.parquet")` を `pd.concat` (例 `finish_position_catboost.py` 行 79-83)。
- 特徴列の解決: `resolve_feature_columns()` が META/LABEL 列を除いた数値列を全採用 (行 86-100)。**学習に渡されるフィーチャストア parquet に `venue_*` 列が含まれていても、モデルがそれを採用するかは META/LABEL 除外後の数値列に入るか次第。v8 系は採用済みだったが metadata の `feature_names` には現れていない (= 学習時に列が存在しなかった旧ストアで学習された)。**
- 出力先: `{model_root}/{category}/iter{id}/fold-{year}/` に `model.json` + `metadata.json` (`build_per_fold_model_dir` 行 205-206, 行 337-350)。
- `metadata.json` 構造: `model_version` / `architecture` / `category` / `feature_count` / **`feature_names` (列順を保持した JSON 配列)** / `hyperparams` ほか。**scorer が参照する唯一のソース・オブ・トゥルースは `feature_names`。**

### 1.3 モデル配置 — `apps/finish-position-predict-container/models/finish-position/{category}/{version}/`

- 各バージョンディレクトリに `model.json` (CatBoost/XGBoost JSON) または `model.txt` (LightGBM) + `metadata.json`。
- 本番が読むバージョンは **`apps/finish-position-predict-container/src/predict_lib/model_meta.json`** が決める (下記 §1.4)。`models/` 配下にはデプロイ候補・旧モデルも同居する。

### 1.4 本番モデルのソース・オブ・トゥルース — `model_meta.json`

ファイル: `apps/finish-position-predict-container/src/predict_lib/model_meta.json`

```json
{
  "model_versions": {
    "jra": "iter20-jra-cb-2013-v8",
    "nar": "iter12-nar-xgb-hpo-v8",
    "ban-ei": "banei-cb-v8-window2011-wf-15y"
  },
  "feature_counts": { "jra": 244, "nar": 192, "ban-ei": 111 }
}
```

- ローダ: `model_meta.py::load_model_meta()` (行 38-66)。起動時に検証付きで読み込み、`MODEL_VERSION_BY_CATEGORY` / `FEATURE_COUNT_BY_CATEGORY` を確定。
- アーキテクチャ: JRA=catboost / NAR=xgboost / Ban-ei=catboost (`ARCHITECTURE_BY_CATEGORY`, 行 76-80)。
- R2 オブジェクトキー: `finish-position/{category}/{modelVersion}/{file}` (`build_r2_object_key`, 行 258-260)。
- E-top2 オーバーレイ: JRA は `JRA_ETOP2_ENABLED=True` で `xgb-jra-2013-v8` を追加ロードし `iter22-jra-etop2` として書き込む (行 88-105)。NAR は `NAR_ETOP2_ENABLED=False` で待機 (行 107-135)。

---

## 2. 本番特徴量収集フロー (venue-weather Worker + Container)

### 2.1 venue-weather Worker

ディレクトリ: `apps/venue-weather/`

- **Cron** (`wrangler.jsonc` 行 8-9):
  - `"30 21 * * *"` — 予報 (forecast) 用
  - `"0 11 * * *"` — 実況 (actual) 用
- **Cron 処理** (`src/scheduled.ts`): `handleScheduled()` → `buildWeatherJobs()` が 25 会場分のジョブを生成 → Queue `WEATHER_JOBS` (`venue-weather-jobs`) に投入。`FORECAST_CRON` 一致なら `weather_type="forecast"`、それ以外は `"actual"` (行 32-36)。
- **Open-Meteo 取得** (`src/weather-api.ts`):
  - Archive `https://archive-api.open-meteo.com/v1/archive` / Forecast `https://api.open-meteo.com/v1/forecast` (行 5-8)。
  - `HOURLY_VARS = "weather_code,temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m"`、`TIMEZONE="Asia/Tokyo"`。
  - 過去 5 日以上前は Archive を使用 (`isArchiveDate`, 行 39-42)。Cloudflare Cache API で 1800/86400 秒キャッシュ。
- **D1 保存** (`migrations/0001_create_venue_weather.sql` + `src/weather-d1.ts`):
  - テーブル `venue_weather` PK = `(keibajo_code, race_date, weather_hour, weather_type)`。列: `venue_name, latitude, longitude, weather_code, temperature, precipitation, wind_speed, wind_gusts, fetched_at` (全て NULL 許可)。
  - binding 名 `WEATHER_DB` (`wrangler.jsonc` 行 27-32)。`INSERT OR REPLACE` で upsert。
- **HTTP GET エンドポイント** (`src/weather-handler.ts`):
  - パス `/weather`、クエリ `?race_date=YYYYMMDD` (8 桁正規表現検証)。
  - レスポンス: `{ "rows": [ { keibajo_code, race_date, weather_hour, temperature, precipitation, wind_speed, wind_gusts, ... } ], "source": "kv" | "d1" }`。
  - 読み順: KV (`WEATHER_KV`, TTL 1800s) → ミス時 D1 (`readWeatherByDate`) → 結果を KV に書き戻し。
- **会場座標**: `src/venue-coords.ts` の `VENUE_COORDS` に 25 会場 (JRA 01-10 / NAR 30,35,36,42-48,50-51,54-55 / Ban-ei 83)。
- **学習用データ配布**: `apps/venue-weather/data/venue_weather_2013.duckdb` 〜 `venue_weather_2026.duckdb`。`venue_weather` テーブルを含み、学習時に `--venue-weather-dir apps/venue-weather/data` で渡す。

### 2.2 Container 側の天気フェッチ

ディレクトリ: `apps/finish-position-predict-container/`

- **service binding は無い**。Container は HTTP fetch のみで Worker を叩く。URL は環境変数 `VENUE_WEATHER_URL` (既定 `https://venue-weather.kaoru.workers.dev`)。
- **`src/weather_fetcher.py`**:
  - `build_weather_url(target_date)` → `GET {base}/weather?race_date=YYYYMMDD` (行 77-79)。
  - `fetch_weather_json()`: `User-Agent: horse-racing-data-predict/1.0` を付けて GET。失敗 (URLError/timeout/JSON decode/`rows` 欠如) は stderr ログ + `[]` を返す graceful fallback (行 82-116)。
  - `write_weather_duckdb()`: 必須キー (`keibajo_code, race_date, weather_hour`) 欠落行を除外し、`{work_dir}/venue-weather/venue_weather_{year}.duckdb` に書く。テーブル列は **builder が probe する名前と完全一致** (`keibajo_code, weather_date, weather_hour, temperature, precipitation, wind_speed, wind_gusts`) (行 64-73, 119-167)。
  - `fetch_venue_weather_dir()`: 上記をオーケストレートし `venue-weather` ディレクトリを返す。usable row 無しなら `None` (= NULL-weather フォールバック) (行 170-184)。
  - **注**: このモジュールはカバレッジ対象外 (live HTTP I/O + DuckDB sidecar、`realtime_odds_fetcher` と同枠。デプロイ時検証)。
- **Builder 呼び出し連鎖**:
  - `pipeline_runner.build_upcoming_feature_rows()` (行 202-249) が `fetch_venue_weather_dir(target_date, WORK_DIR)` を呼び (行 233)、`build_pipeline()` に `venue_weather_dir` を渡す (行 234-242)。
  - `pipeline_args.build_base_argv()` が `venue_weather_dir is not None` のとき `--venue-weather-dir <dir>` を argv に付ける (行 265-266)。
  - 以降は §1.1 と同じ `finish_position_features_duckdb.py` が `venue_*` 集約 + 交互作用列を生成し parquet 出力。
- **scorer** (`src/predict_lib/scorer.py`):
  - `build_feature_row()` が `entry` を **`feature_names` の順序で射影**。`entry.get(name)` が無ければ `0.0`、XGBoost のみ float32 量子化 (行 36-65)。
  - `feature_names` は `metadata.json` から読む (`_load_model_metadata`, `predict_upcoming.py` 行 263-268)。`assert_feature_count` で `model_meta.json` の `feature_counts` と照合。

> **ここがパリティの肝**: Container は常に `venue_*` 列を _生成_ するが、scorer は _モデルの `feature_names` にある列だけ_ を採点行に入れる。`feature_names` に `venue_*` が無いモデル (現本番 3 モデル) では、フェッチした天気は最終スコアに寄与しない。`feature_names` に `venue_*` があるモデル (v9 系) で初めて寄与する。

---

## 3. 本番着順予測フロー (full / rescore)

### 3.1 Mac launchd 日次予測

- plist: `scripts/launchd/com.kkk4oru.finish-position-predict.plist`
  - JST 03:00: NAR + Ban-ei のみ (JRA mirror 未到着のため)。
  - JST 09:30: 全カテゴリ (JRA mirror 利用可)。
- wrapper: `scripts/launchd/finish-position-predict-daily.sh` が `docker run --network=host ... finish-position-predict-local:split2` を起動 (行 199-215)。`PREDICT_SERVE_MODE=""` (= full バッチモード)。

### 3.2 cron coordinator + per-race rescore

- Worker: `apps/finish-position-cron/src/worker.ts` (行 156-208)。Cron で分岐:
  - Warm cron: Neon 接続ウォーム。
  - Coordinator cron: `runRaceCoordinatorTick()` が post time T-X 分のレースを per-race rescore メッセージとしてキュー投入 (行 161-171)。
  - Feature-build cron: 全カテゴリ `mode=full` を `enqueuePredict()` (行 173-185)。
  - Rescore cron: 全カテゴリ `mode=rescore, daysAhead=0` (行 187-198)。
- producer: `src/queue-producer.ts::enqueuePredict()` が `PREDICT_QUEUE` に投入 (行 31-48)。
- consumer: `src/queue-consumer.ts` (行 166-220)。per-race rescore で JRA は Worker-native (`rescoreJraRace()`)、NAR/Ban-ei は Container `/predict` を呼ぶ。

### 3.3 full モードと rescore モードの違い

| 項目              | full                               | rescore                                                                                   |
| ----------------- | ---------------------------------- | ----------------------------------------------------------------------------------------- |
| DuckDB ビルド     | 実行 (21y Neon scan)               | スキップ (R2/ローカルキャッシュ読み込み)                                                  |
| 天気フェッチ      | `fetch_venue_weather_dir()` を呼ぶ | **呼ばない**                                                                              |
| 天気データ源      | venue-weather Worker → builder     | **full 時に書いた R2 parquet 内の列をそのまま再利用**                                     |
| late-binding 更新 | なし                               | odds_score / popularity_score / tansho_odds / tansho_ninkijun / weight_diff_from_avg のみ |

- モード定義: `src/predict_lib/serve.py` の `PredictMode = Literal["full", "rescore"]` (行 64-70)。rescore はキャッシュが無ければ自動で full にフォールバック。
- full の R2 put: `predict_upcoming.py::_try_r2_put` がキー `feat-cache/{category}/{runDate}/features.parquet` に **全 early-binding 特徴量列を含む parquet** を書く。per-race 分割キーは `feat-cache/{category}/{runDate}/{keibajoCode}/{raceBango}/features.parquet` (`_split_parquet_by_race`, 行 846-906)。
- rescore fn: `predict_upcoming.py::_make_rescore_fn()` (行 1134-1206)。`_ensure_cached_parquet` → `_load_cached_races` でキャッシュを読み、`_fetch_fresh_snapshots` で **odds/weight のみ** を取得、`apply_fresh_snapshots` で late-binding 列だけ更新 → 再採点。

---

## 4. rescore で天気特徴量は利用可能か (核心)

**結論: 利用可能。ただし「full ビルド時の天気値をそのまま保持」する形で。**

### 4.1 R2 キャッシュ parquet には天気列が含まれる

- キャッシュは builder の最終 parquet をそのまま put したもの。builder が `venue_*` + 交互作用列 + `weather_normalized` / `track_condition_normalized` を出力するので (§1.1)、**parquet にこれらの列は物理的に存在する**。
- Worker 側 read 経路 `apps/finish-position-cron/src/scoring/feature-cache.ts`:
  - 冒頭コメント (行 1-13) 明記: 「Each row carries the full early-binding feature set」。`parquetReadObjects` が列名 = `feature_names` のプレーン行オブジェクトを返し、`projectFeatureRow` が位置で射影。
  - 天気列を _特別扱いしない_ ── 全列が `FeatureEntry` Map に入り、`feature_names` にある列だけが採点に使われる。late-binding 上書き対象は odds/weight 系 5 列のみ (行 23-31)。

### 4.2 天気は early-binding (rescore で再計算しない) で正しい

- 天気はレース日に紐づく early-binding 特徴量。rescore は「直前のオッズ/馬体重の更新」だけが目的なので、天気を再フェッチしないのは設計通り。
- full ビルドが「予報 (forecast)」値で天気を入れた場合、その値が rescore まで引き継がれる。実況 (actual) 値で上書きしたいなら full の再ビルドが必要 (rescore では起きない)。これは現状の仕様上の制約として認識しておく。

### 4.3 天気が欠損した場合の対処

3 段の防御がすべて「NULL → 0.0」に収束する:

1. **full ビルド時**: Worker 不通なら `fetch_venue_weather_dir` が `None` → builder は空集約テーブル (`venue_weather_empty_agg_sql`) で続行 → `venue_*` 列は NULL → SQL 側 `COALESCE(..., 0/既定)`。
2. **R2 parquet 内**: NULL 列はそのまま格納。
3. **採点時**: Python `scorer._coerce` (行 54-65) / TS `feature-projection.coerceFeature` (行 36-45) がともに `None/null/空文字 → 0.0`。

モデルは天気欠損 (= 0.0) を含む分布で walk-forward 学習されているため、欠損しても安全に劣化する (壊れない)。

### 4.4 v9 weather モデルをデプロイするときの含意

- 現本番 3 モデルは `venue_*` を `feature_names` に持たないので、上記は「無害だが無意味」(天気は採点に寄与しない)。
- **`banei-cb-v9-weather-2011` をデプロイすると `venue_*` + 交互作用 5 列が `feature_names` に入る**。このとき:
  - full モードでは Container の天気フェッチが必須経路になる (フェッチ失敗 = 全部 0.0 で採点 → 天気シグナルが死ぬが、クラッシュはしない)。
  - rescore モードでは R2 parquet にこれらの列があれば従来通り効く。**ただし full ビルドが天気付きで走っていることが前提** (full が天気無しでビルドしていれば rescore も 0.0 のまま)。
  - デプロイ手順 (§6) で `model_meta.json` の `feature_counts["ban-ei"]` を **111 → 138** に更新し、`metadata.json` の `feature_names` と builder の出力列が一致することを **必ず** §5 のパリティスクリプトで確認する。

---

## 5. 特徴量パリティの確認

新モデル (特に v9 weather) をデプロイする前に、3 者の特徴量集合が整合するかを機械的にチェックする。

- スクリプト: `scripts/verify_feature_parity.py`
- 照合する 3 ソース:
  1. **ローカルのフィーチャストア parquet** の列一覧 (`race_year=*/*.parquet` の schema)。
  2. **モデルの `metadata.json`** の `feature_names` (+ `feature_count`)。
  3. **Container builder が生成しうる列** (`finish_position_features_duckdb.py` の出力列の参照リスト)。
- 判定:
  - `feature_names` の各列が parquet と builder の双方に存在するか (= 欠落していれば本番で常に 0.0 になる危険)。
  - `feature_count` が `feature_names` の長さ・`model_meta.json` の `feature_counts` と一致するか。
  - parquet/builder にしか無い列 (= 学習で未採用) は情報レベルで報告。
- ミスマッチは WARNING として出力し、致命的不整合 (`feature_names` の列が builder で生成不能) は非ゼロ終了で CI / デプロイ前ゲートにできる。

使い方の例:

```bash
cd scripts
uv run python verify_feature_parity.py \
  --features-parquet ../apps/pc-keiba-viewer/tmp/finish-position-features-parquet \
  --metadata ../apps/pc-keiba-viewer/tmp/models/banei-cb-v9-weather-2011/metadata.json
```

> 詳細な引数は `verify_feature_parity.py --help` を参照。builder が生成しうる列名は `finish_position_features_duckdb.py` の最終 SELECT を真とする。

---

## 6. 新モデルデプロイフロー

1. `model.json` (または `model.txt`) + `metadata.json` を `apps/finish-position-predict-container/models/finish-position/{category}/{version}/` に配置。
2. **§5 のパリティスクリプトで `feature_names` ⊆ builder 生成列 を確認** (特に v9 weather は `venue_*` が builder にあることを保証)。
3. `apps/finish-position-predict-container/src/predict_lib/model_meta.json` の `model_versions[{category}]` と `feature_counts[{category}]` を更新。
4. `wrangler deploy` で Container イメージ再構築 (Mac launchd 用には `finish-position-predict-local:split2` を `docker build` し直す)。
5. `finish_position_active_models` テーブルを **local PG + Neon** の両方でフリップ。
6. 次開催で serve 実測 (skew チェック) を監視。

> commit と deploy は別操作。push は明示指示まで行わない (リポジトリ運用ルール)。

---

## 付録 A: 天気のデータフロー全体図

```
Open-Meteo API
   │  (HOURLY: weather_code,temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m / Asia/Tokyo)
   ▼
venue-weather Worker (cron 21:30 forecast / 11:00 actual)
   ├─ Queue venue-weather-jobs (25 venues) → processWeatherJob
   ├─ D1 venue_weather  (PK keibajo_code,race_date,weather_hour,weather_type)
   └─ GET /weather?race_date=YYYYMMDD → { rows:[...], source:"kv"|"d1" }
        │                                         ▲
        │ (学習)                                  │ (本番 full)
        ▼                                         │ HTTP GET (service binding なし)
apps/venue-weather/data/venue_weather_YYYY.duckdb │
        │                              finish-position-predict-container
        │ --venue-weather-dir                 weather_fetcher.py
        ▼                                    → {work}/venue-weather/venue_weather_YYYY.duckdb
finish_position_features_duckdb.py  ◀───────────── --venue-weather-dir
   materialize_venue_weather() : 9-17時集約
   ├─ venue_temperature / venue_precipitation_total
   ├─ venue_wind_speed_max / venue_wind_gusts_max
   └─ rain_x_* / wind_x_* / cold_x_speed_effect
        │
        ▼ parquet (race_year=YYYY)
   ┌────────────┴─────────────┐
   │ (学習)                    │ (本番)
   ▼                          ▼
fit_*/train_*_walk_forward   full: R2 put feat-cache/{cat}/{date}/features.parquet (天気列含む)
   metadata.json                │
   feature_names ──────────────┐│
                               ▼▼
                        scorer.build_feature_row : feature_names 順に射影
                          ├─ feature_names に venue_* あり → 天気が効く (v9)
                          └─ feature_names に venue_* なし → 計算後に捨てる (現本番)
                               │
                        rescore: R2 parquet 読み込み → odds/weight のみ late-binding 更新 → 再採点
                               (天気は full 時の値を保持・再フェッチしない)
```

## 付録 B: 主要ファイル一覧 (絶対パス)

- builder: `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`
- 学習: `apps/pc-keiba-viewer/src/scripts/train_finish_position_{catboost,xgboost,lightgbm}_walk_forward.py`
- モデル配置: `apps/finish-position-predict-container/models/finish-position/{category}/{version}/`
- 本番モデル定義: `apps/finish-position-predict-container/src/predict_lib/model_meta.json` (+ `model_meta.py`)
- Container 天気フェッチ: `apps/finish-position-predict-container/src/weather_fetcher.py`
- Container パイプライン: `apps/finish-position-predict-container/src/pipeline_runner.py` / `src/predict_lib/pipeline_args.py`
- Container scorer: `apps/finish-position-predict-container/src/predict_lib/scorer.py`
- Container serve/rescore: `apps/finish-position-predict-container/src/predict_lib/serve.py` / `src/predict_upcoming.py`
- venue-weather Worker: `apps/venue-weather/src/{scheduled,weather-queue,weather-api,weather-d1,weather-handler,venue-coords}.ts`
- venue-weather 学習データ: `apps/venue-weather/data/venue_weather_YYYY.duckdb`
- cron coordinator: `apps/finish-position-cron/src/{worker,queue-producer,queue-consumer}.ts`
- rescore キャッシュ: `apps/finish-position-cron/src/scoring/{feature-cache,feature-projection,late-binding}.ts`
- launchd: `scripts/launchd/com.kkk4oru.finish-position-predict.plist` / `finish-position-predict-daily.sh`
- パリティ確認: `scripts/verify_feature_parity.py`

```

```
