# 着順予測システム 設計仕様

最終更新: 2026-05-19 (kkkaoru)

## 設計の大前提

**予測精度を犠牲にする実装は、いかなる場合でも禁止。**

軽量化や Worker 移植を理由に精度が下がる変更は採用しない。フル特徴量モデルを mac で学習・予測することを原則とし、Cloudflare 上で動かす範囲は「mac で計算できないもの (= 未来のレースで脚質ラベルがまだ確定していない出走馬の脚質推定)」に限定する。

## アーキテクチャ

```
┌──────────────────────────────────────────────────────────────────────┐
│ mac (ローカル開発機)                                                  │
│                                                                       │
│  PG (horse_racing)                                                    │
│   ├─ jvd_se / nvd_se / ...   (raw 過去20年走行成績)                  │
│   ├─ race_entry_corner_features  (特徴量テーブル)                    │
│   ├─ race_finish_position_features  (full feature parquet と同形)    │
│   ├─ race_finish_position_model_predictions  (予測結果)              │
│   ├─ finish_position_active_models  (active model 一覧)              │
│   └─ model_prediction_evaluations  (1着/2着/3着 精度)                │
│           │                                                            │
│           ▼                                                            │
│  DuckDB feature pipeline  (finish_position_features_duckdb.py)        │
│           │                                                            │
│           ▼                                                            │
│  parquet (tmp/feat-v20-rs/{jra,nar})                                  │
│           │                                                            │
│           ▼                                                            │
│  enrich_parquet_with_running_style.py                                 │
│   (mac で saved running-style booster を回し rs_p_* 4列を付与)        │
│           │                                                            │
│           ▼                                                            │
│  parquet (tmp/feat-v20-rs-final/{jra,nar})                            │
│           │                                                            │
│           ▼                                                            │
│  フル特徴量モデル訓練・予測 (mac)                                     │
│   - LGBM lambdarank (finish_position_lightgbm.py)                     │
│   - Transformer (finish_position_transformer/)                        │
│   - Ensemble (ensemble-finish-predictions.py)                         │
│           │                                                            │
│           ├──→ tmp/models/finish-position-rs/{src}-rs-*               │
│           │                                                            │
│           ├──→ walk-forward 評価 (compare-model-metrics.py)            │
│           │     → tmp/finish-position-eval/...                        │
│           │     → PG model_prediction_evaluations に upsert            │
│           │       (insert-evaluation-row.py)                          │
│           │                                                            │
│           └──→ 出走予定レース予測                                       │
│                 → PG race_finish_position_model_predictions に書込    │
│                                                                        │
│  Neon push (apps/local-postgresql/scripts/push-neon-sync.ts)          │
│   REPLICA_SYNC_TABLES で下記を Neon へレプリケート:                    │
│   - finish_position_active_models                                     │
│   - model_prediction_evaluations                                      │
│   - race_finish_position_model_predictions                            │
│   - race_entry_corner_features                                        │
│           │                                                            │
└───────────┼────────────────────────────────────────────────────────────┘
            │
            ▼
┌────────────────────────────────────────────────────────────────────┐
│ Neon (managed PG, Cloudflare からも読める)                          │
│                                                                     │
│   ├─ race_finish_position_model_predictions  (viewer/Worker 参照用) │
│   ├─ model_prediction_evaluations                                  │
│   ├─ finish_position_active_models                                 │
│   └─ race_entry_corner_features                                    │
└─────────────┬───────────────────────┬───────────────────────────────┘
              │                        │
              │                        │
              ▼                        ▼
┌─────────────────────────┐  ┌─────────────────────────────────────────┐
│ pc-keiba-viewer         │  │ Cloudflare Worker (sync-realtime-data)  │
│ (Next.js, Cloudflare    │  │                                          │
│  Pages)                 │  │  役割は脚質推定のみ:                       │
│                         │  │  - 未来のレースで rs_p_* が未確定の出走馬 │
│  Neon を直接読み込み    │  │    に対し、保存済 running-style booster   │
│  予測結果を表示         │  │    (R2) を回して rs_p_* を計算            │
│                         │  │  - 結果を PG (Hyperdrive 経由) の        │
│                         │  │    race_entry_corner_features 等に書込    │
│                         │  │                                          │
│                         │  │  finish-position-lite-* は廃止           │
│                         │  │  (Worker でフル予測はしない)              │
└─────────────────────────┘  └─────────────────────────────────────────┘
```

## 責務分担 (誰がどこで何をするか)

### mac (ローカル開発機 / scheduled job)

- 全 raw データの PG 取り込み
- DuckDB pipeline で特徴量 parquet を生成 (20 年分)
- 保存済 running-style booster で rs*p*\* を parquet に付与
- フル特徴量モデル (LGBM / Transformer / Ensemble) の訓練
- walk-forward 評価で 1着/2着/3着 精度を計算
- 出走予定レースに対する予測の生成
- 予測 + 評価 + active model 情報を PG に書込
- PG → Neon push sync

### Cloudflare Worker (sync-realtime-data)

- **唯一の責務**: 未来のレースで脚質 (rs*p*_) ラベルが必要だがまだ計算されていない出走馬に対し、R2 から running-style booster をロードして per-horse rs*p*_ を計算し、PG (Hyperdrive) に書き込むこと。
- finish-position 予測そのものは Worker 上では行わない (= `finish-position-lite-*` パイプラインは廃止)。

### Cloudflare R2

- running-style booster の JSON 形式 model artifact のみ保管 (Worker から fetch)。
- finish-position model JSON は保管不要 (Worker で評価しないため)。

### Neon

- viewer + 必要に応じ Worker から読み込まれる読み取り専用レプリカ。
- `push-neon-sync.ts` の `REPLICA_SYNC_TABLES` で対象テーブルを限定。

### pc-keiba-viewer (Next.js)

- Neon を直接読み込み、予測結果と評価メトリクスを表示。

## モデル選定方針

1. **比較対象は同一 walk-forward 窓 (例: 2024-2025) の 1着/2着/3着 精度** で `model_prediction_evaluations` を参照する。
2. 既存ベスト (top1) を下回るモデルは production に昇格させない。
3. 既存ベスト (例: `jra-trans-lgbm-ensemble-v3` 45.20%) を上回るモデルが訓練できた場合のみ `finish_position_active_models` を切り替え。
4. 軽量モデル (lite) の評価行は記録としては残してよいが、Worker 上で active にはしない。

## 「予測精度を犠牲にしない」を運用で担保するためのチェックリスト

- [ ] 新モデルを deploy する前に walk-forward eval を `model_prediction_evaluations` に upsert する
- [ ] 既存 active model と top1/place2/place3 を比較し、いずれかが劣る場合は deploy しない
- [ ] active model 切り替えは PG `finish_position_active_models` を update し、Neon へ sync (Worker は脚質しか触らないので Worker 再 deploy は原則不要)
- [ ] viewer は `finish_position_active_models` に従って表示する model_version を切り替える
- [ ] 5/19 の今回の `*-style20-lgbm-v1.0` は既存ベストより劣るため active にしない (記録として残すのみ)

## やってはいけないこと (anti-patterns)

- Worker での finish-position 推論 (lite 25 特徴量モデル) を production に組み込むこと
- 精度比較なしに active model を新版に切り替えること
- フル特徴量モデルを使えるのに lite で代用すること
- 「Mac が介在しない設計」を理由に精度を下げること

## ロードマップ (2026-05-19 時点)

- [x] Phase 1: 過去 20 年分 (2006-2026) の parquet 再生成 (JRA 1.6M / NAR 2.77M)
- [x] Phase 2.5: parquet に field-style 特徴量を追加 (self_style_dominant_rate / field_avg_style_concentration / field_style_diversity)
- [x] Phase 3 (lite, 参考値): `*-style20-lgbm-v1.0` の 1/2/3着 精度を PG + Neon に保存
- [ ] **Phase A**: 20 年 parquet を使ったフル特徴量モデル (LGBM lambdarank / Transformer / Ensemble) の訓練
- [ ] **Phase B**: 既存ベスト (`jra-trans-lgbm-ensemble-v3` 等) と walk-forward 比較
- [ ] **Phase C**: 上回った場合のみ active model 切り替え、PG/Neon 反映
- [ ] **Phase D**: Worker `finish-position-lite-*` パイプラインの廃止 (cron / queue / state テーブル / inference / features / tree / pool / inference 関連の admin route)
- [ ] **Phase E**: 出走予定レースに対するフル特徴量モデル予測の定期 (mac cron) 投入と Neon sync
