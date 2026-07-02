# 着順・脚質予測システム 仕様書

最終更新: 2026-07-03

本書は、競馬の着順予測システム（finish position prediction system）と、その前段で着順特徴量を供給する脚質予測システム（running-style prediction system）の全体仕様を記述する。学習基盤・特徴量パイプライン・本番推論基盤（Cloudflare Worker / Cloudflare Container）・評価方法・アンチパターンを網羅する。

---

## 1. アーキテクチャ概要

本システムは「学習」と「本番推論」を物理的に分離し、本番では **feature generation → running-style generation → finish-position full generation** の順序を守る。

- **Mac はモデル学習・モデル artifact 生成専用**である。本番の特徴量生成・脚質予測・着順予測を Mac 上で実行することは禁止する。
- **本番生成の authority は Cloudflare 側**である。Cloudflare Cron / Queue / Worker / Container が feature generation → running-style generation → finish-position full generation をレース単位で実行する。
- **ローカル常駐プロセスは本番構成要素ではない**。手元の scheduler、shell wrapper、Docker process、trainer process を本番 trigger / fallback / ordering dependency にしてはならない。
- **本番の着順予測は Cloudflare Container 上でレース単位（per-race）で実行**する。
- **本番の脚質予測は `sync-realtime-data` Worker 上でレース単位（per-race）で実行**し、完了後に `FINISH_POSITION_PREDICT_QUEUE` へ focused per-race full message を enqueue する。service binding / API は queue binding が無い環境の fallback に限る。
- 対象は 3 カテゴリ。各カテゴリは独立したモデル・学習窓・アーキテクチャを持つ。
  - **JRA（中央競馬）**
  - **NAR（地方競馬）**
  - **Ban-ei（ばんえい競馬）**

```mermaid
flowchart TB
    subgraph MAC["Mac（学習・artifact 生成 専用）"]
        TRAIN["continuous_learner.py /<br/>feature_explorer.py /<br/>train_*_walk_forward.py /<br/>running_style_lightgbm.py train-cells"]
        ARTI["model.json + metadata.json<br/>（CatBoost / XGBoost）<br/>running-style flatbin + routing JSON"]
        TRAIN --> ARTI
    end

    subgraph BUILD["Docker build"]
        IMG["Cloudflare Container image"]
    end

    subgraph RT["sync-realtime-data（脚質 Worker）"]
        FEAT["feature generation<br/>race_entry_corner_features"]
        RSPLAN["running-style planner"]
        RSQUEUE["running-style Queue"]
        RSINFER["per-race LightGBM inference"]
        FEAT --> RSPLAN --> RSQUEUE --> RSINFER
    end

    subgraph CF["Cloudflare（本番推論）"]
        CRON["finish-position-cron<br/>（Cron Worker）"]
        QUEUE["Cloudflare Queues"]
        DO["FinishPositionPredictContainer<br/>（Container Durable Object）"]
        CRON --> QUEUE --> DO
    end

    D1[("D1<br/>realtime_race_sources /<br/>race_running_styles")]
    R2RS[("R2<br/>RUNNING_STYLE_MODELS /<br/>running-style feature Parquet")]
    NEONRS[("Neon PostgreSQL<br/>race_running_style_<br/>model_predictions")]
    NEON[("Neon PostgreSQL<br/>race_finish_position_<br/>model_predictions")]
    VIEWER["pc-keiba-viewer<br/>（表示）"]

    D1 --> FEAT
    R2RS --> RSINFER
    RSINFER -->|"D1 write"| D1
    RSINFER -->|"Neon mirror"| NEONRS
    RSINFER -->|"enqueue<br/>mode=full, skipDedup=true"| QUEUE
    RSINFER -.->|"fallback POST /run"| CRON
    ARTI --> IMG
    IMG --> DO
    DO -->|"feature build + score"| DO
    DO -->|"UPSERT"| NEON
    NEONRS --> VIEWER
    NEON --> VIEWER
```

### 1.1 本番 ownership と実行順序

本番の生成責務は Cloudflare 側に閉じる。順序の authority は `sync-realtime-data` と `finish-position-cron` / Container の連携であり、ローカル端末・手元 scheduler は本番の trigger / fallback / ordering dependency を持たない。

| stage                           | production owner                                                                        | 完了条件 / output                                                                                                       |
| ------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| feature generation              | `sync-realtime-data` の Cloudflare cron / queue / Worker                                | D1 race source / corner feature と脚質 feature Parquet が対象 race で利用可能                                           |
| running-style generation        | `sync-realtime-data` の `running-style-cron.ts` / `running-style-queue.ts`              | D1 `race_running_styles`、Neon `race_running_style_model_predictions`、R2 daily prediction Parquet、viewer cache を更新 |
| finish-position full generation | `finish-position-cron` Worker / Cloudflare Queues / `finish-position-predict-container` | Container が per-race DuckDB feature build と scoring を実行し、Neon `race_finish_position_model_predictions` へ UPSERT |

`sync-realtime-data` は feature generation と running-style generation の完了を race scope で確認してから、`FINISH_POSITION_PREDICT_QUEUE` に focused per-race full message を enqueue する。`FINISH_POSITION_CRON` service binding への `POST /run` は queue binding が無い環境の fallback であり、同じ message を `finish-position-cron` 経由で enqueue する。Container が着順 feature build と scoring を完了させる。

---

## 2. 本番モデル（2026-07-03 時点）

着順本番モデルのバージョンと特徴量数は、Container 内の `apps/finish-position-predict-container/src/predict_lib/model_meta.json` を single source of truth とする。

| カテゴリ | model_version                        | アーキテクチャ                                      | 特徴量数                        | 学習窓        | ランキング loss                                    |
| -------- | ------------------------------------ | --------------------------------------------------- | ------------------------------- | ------------- | -------------------------------------------------- |
| JRA      | `jra-cb-v9-sim-2013`                 | CatBoost                                            | 263                             | 2013+         | YetiRank                                           |
| NAR      | `iter40-nar-settransformer-blend-v1` | XGBoost base × Set Transformer score-level z-fusion | 192（base）＋117（transformer） | full（2006+） | rank:pairwise ＋ listnet（0.5/0.5 score-z fusion） |
| Ban-ei   | `banei-cb-v9-sim-2011`               | CatBoost                                            | 130                             | 2011+         | YetiRank                                           |

> **NAR iter40 transformer blend = Cloudflare production ENABLED（2026-07-03）**: NAR は 2026-07-03 に iter40 transformer blend（`iter40-nar-settransformer-blend-v1`）を Cloudflare Worker / Queue / Container の production path で有効化済み。表の NAR 行は iter12 base（192 feat XGBoost）に 117-feat listwise Set Transformer を **score-level z-fusion（0.5/0.5、2026-07-03 に rank-fusion から更新＝ deployed win #2、commit `a90161f4`、model_version は iter40 維持）** した現行 serving 構成を指す（fallback は iter12）。有効化方式は **CF worker path**——`container-class.ts` が env を container へ forward（commit `28c89abf`）＋ wrangler **secret** `NAR_TRANSFORMER_BLEND_ENABLED=1`（redeploy で消えない）＋ worker deploy（Version `0d2d99c7-cbe1-4989-a497-255e73e77388`）。Neon-write smoke は CLEAN（iter40 行が本番 Neon に landing・fusion が iter12 と異なる・fallback 確認済み）。rollback は CF secret を 0 に戻す。ローカル `.env` の flag は診断・backfill wrapper 用で、本番 authority ではない。**初回 genuine 本番 write 確認済み（2026-07-03、production LIVE）**——Cloudflare production path の per-race full serve が feature build → iter12 base → transformer fusion → iter40 Neon write を完走し、48 NAR レース（499 頭行）を 03:05 JST に本番 Neon `race_finish_position_model_predictions` へ書き込んだ（`model_version=iter40-nar-settransformer-blend-v1`、`prediction_generated_at` 18:05:32-18:05:47 UTC ＝ 03:05 JST）。viewer は当日 48 NAR レース全てで iter40 を表示（latest-for-race resolution、iter40 が 48/48 で勝ち）。詳細・検証・rollback は §2.5 / §5.9、統制・機構は §11。

> **重要な留保・更新（2026-07-02 investigation → 同日中に 3 カテゴリすべてで解決、詳細は §5.9）**: 本欄は当初、「本番の per-race パイプラインが実際にこれらのモデルで着順予測を完走し Neon に書き込んだという確認済みの証跡が、テーブル観測可能な履歴の範囲（2026-05-15 以降、約 1.5 ヶ月）で 3 カテゴリいずれについても存在しない」という critical finding を記録していた。**同日中の root-cause 特定と 4 段階の fix（commit `2d3535be` instrumentation → commit `af1ca40e` queue-consumer hold time 分離 → commit `f4b3ea7a` same-category slot starvation 修正 → commit `c3a48694` per-race work-dir cleanup）により、JRA / NAR / Ban-ei の 3 カテゴリすべてで live smoke test で genuine な per-race Neon 書き込みを確認済み**。現行 production の着順 routing は **per-cell を最優先**し、non-default cell に一致しない race は category default / 明示的に有効な category-level path（例: JRA E-top2、NAR transformer blend）へ進む。`nar_subclass` による per-class ensemble routing は production dispatch から外し、履歴・オフライン検証用の記録としてのみ扱う。

### 2.1 脚質予測モデル

脚質予測は `apps/sync-realtime-data/` の Worker が R2 binding `RUNNING_STYLE_MODELS` から flatbin LightGBM model を読み、per-race で `nige` / `senkou` / `sashi` / `oikomi` を推論する。未設定時の既定は source 単位の latest model（`buildRunningStyleFlatModelKey(source)`, `variantId = latest`）で、既存運用と後方互換である。

cell-level routing を使う場合は `RUNNING_STYLE_CELL_ROUTING_JSON` に data-driven routing config を入れる。routing config が存在しないカテゴリ、または config 自体が未設定の場合は、必ず source 単位 latest model に fallback する。

脚質 cell model はローカルで `running_style_lightgbm.py train-cells` により **cell 単位で** 学習・評価・promotion plan 生成を行う。採用された variant は header metadata 込みの flatbin を `RUNNING_STYLE_MODELS` の R2 object として promotion し、`RUNNING_STYLE_CELL_ROUTING_JSON` はその R2 key を指す。production は Cloudflare Worker / Queue / R2 / D1 / Neon のみを参照し、ローカル端末上の model path や process に依存しない。

### 2.2 学習窓が 3 カテゴリで異なる点（重要）

学習窓は ablation 検証の結果としてカテゴリごとに最適値が異なることが確定している。一律化してはならない。

- **JRA = 厳密に 2013+**。pre-2013 は非定常で希釈要因。2012+（広）も 2014+（狭）も 2013+ に劣後（DO-NOT-RETEST）。
- **NAR = full 2006+**。NAR は長い履歴を必要とし、窓を絞ると全 metric 悪化（JRA と真逆）。
- **Ban-ei = 2011+**。pre-2011 非定常で希釈、2013+/2016+ は切りすぎ。2011+ が sweet spot。

### 2.3 E-top2 override（無効・2026-07-02 復活検証）

E-top2 は「XGB の 1 着予測が CatBoost の 2 着予測と一致するレースのみ rank-1 を上書きし、rank-3 以降は CB のまま残すことで exact place3 構成を保存する」place-preserving override 手法で、2026-06-18 には当時の baseline に対し top1 +1.36pp の deployed win だった（`project_etop2_place_preserving_win_2026_06_18`）。v9-sim 移行後に特徴量数が非互換となり無効化されていたが、2026-07-02 に 3 カテゴリで復活を検証した。

- **JRA E-top2 override: DISABLED（2026-07-02 復活検証で REJECT、DO-NOT-RETEST）**。旧構成は override が前提とする XGB が 244 特徴量、本番 CatBoost が 263 特徴量で非互換だった。v9-sim store（254/263 feat 一致）で XGB（`xgb-jra-2013-v8` の HPO を verbatim 移植、`rank:ndcg`、HPO 追加なし）を再学習して非互換を解消した上で、override を 3-fold WF blind（2023/2024/2025、bootstrap LB95 2000 iterations）で正式再検証した結果、REJECT。pooled（n=10,365 races）top1 Δ-0.183pp[LB95 -0.618]／place2 Δ-0.058[LB95 -0.434]、place3〜6・top3_box・fukusho_2p は設計通り Δ±0.000（place 保存は完全に動作）。fired したレース（966/10,365、9.3%）に限ると top1 Δ-1.97pp——**昇格させた新 #1（CB の 2 着予測）の実勝率 27.33% が、降格させた旧 #1（CB の 1 着予測）の 29.30% を下回る**。per-fold も非頑健（2024 Δ-0.926[LB95 -1.679]）。唯一 LB95>0 の robust pocket は distance_band=long の place2（Δ+1.13[LB95 +0.14]）のみで孤立し採用不可。**機序**: v9-sim の sim\_\* 特徴量（+1.34pp、`project_sim_features_v9_deployed_2026_06_26`）が E-top2 の signal を既に吸収しており、強化された baseline 上では swap が冗長どころか有害（score-additive draw+speed REJECT と同型）。2026-06-18 の +1.36pp win は当時の弱い baseline に対する成立であり、v9-sim 世代では再現しない。DO-NOT-RETEST。artifact: `apps/pc-keiba-viewer/tmp/candidate-etop2-restore/result_etop2.json`。
- **NAR E-top2: DISABLED（2026-07-02 復活検証で REJECT、DO-NOT-RETEST、`NAR_ETOP2_ENABLED=False` が正しい本番状態として確定）**。復活作業（phantom feature 除去 → 193-feat 再学習 → 本番 iter12 base に対する per-class gate 再検証）を完遂した結果 REJECT。**歴史的訂正（重要）**: 2026-06-19 の 4-class ADOPT（A/B/NEW/other、top1 +0.24pp、`project_nar_etop2_perclass_routing_2026_06_19`）は **base-selection artifact** だった——当時の gate は本番 serving base（HPO 済みで強い `iter12-nar-xgb-hpo-v8`）ではなく、**再学習した xgb-2013 base に対して** +0.219 を測っていた（今回 harness で当時値 +0.219 を正確に再現し検証）。真の本番 iter12 base に対しては、override モデル `cb-nar-2013-v8`（196 feat）が要求する 3 phantom feature（`bataiju_x_kyori_log`／`bataiju_z_in_barei_kyori_stratum`／`horse_bataiju_trajectory_deviation`——本番 `finish_position_features_duckdb.py` が出力せず iter17 履歴ドキュメントにのみ存在）を含む原 196-feat override でも OVR-ALL top1 +0.125pp[LB95 -0.084]、4 ADOPT class 全てで LB95<0 で、当時から gate は成立していなかった。phantom 3 列を除いた 193-feat で `cb-nar-2013-v8` の hyperparameters を verbatim（YetiRank、iter500／depth8／lr0.05／l2 3.0）に再学習し blind 2023/2024/2025 で再検証: CB-193 standalone top1 58.808% は原 196-feat の 58.884% とほぼ同一（-0.076pp、幻特徴量はほぼ無信号）。iter12 base に対する per-class override gate は POOLED top1 Δ+0.017pp[LB95 -0.106]／place2 Δ-0.047[LB95 -0.160]、A（n=2,515）Δ+0.358[LB95 -0.398]／B（6,326）Δ-0.047[-0.569]／NEW（544）Δ+2.022[+0.184 だが n 極小]／other（6,541）Δ-0.153[-0.688]、place3〜6・top3_box・fukusho_2p は Δ±0.000（place 保存は設計通り）。LB95>0 は NEW（n=544）のみで adoption 不成立。**副次的発見（store 品質）**: 再学習用の原 eval store `tmp/feat-nar-v8-iter17-bataiju` は corrupted（`shusso_tosu` が 2016 年以降で欠落、2016/2017 partition に非同一 dupes 5.5x/2.3x）で、dedupe + augmented store からの `shusso_tosu` join で clean 化して使用した。**機序**: JRA（sim\_\* が signal を吸収）とは別の理由で NAR も REJECT——原 ADOPT は弱い再学習 base に対する成立に過ぎず、真の本番 iter12（HPO 済み）に対しては override が冗長。**これで E-top2 ライン（override 手法）は JRA / NAR / Ban-ei（既 ABORT）全カテゴリでクローズ**。**教訓**: override/ensemble 系 lever の gate は必ず本番 serving base（正確な model artifact）に対して行う——再学習した base に対する gate は base-selection artifact を生む（serve-path 比較の教訓の override 版）。本番無変更（flag False のまま、コード変更なし）。artifact: 検証 agent の session scratchpad（`feat-nar-clean-193` / `cb-193-report.json` / `gate_result_193.json` / `gate_result_orig196_iter12.json`）。
- E-top2 の override helper は履歴・検証用に残るが、現行 production dispatch は per-cell を先に解決し、NAR E-top2 / per-class route は呼ばない。maiden 除外は store 上 `kyoso_joken_code=="701"` を使う（`is_newcomer_race` は store で全ゼロ）という実装注意がある。

### 2.4 NAR per-class ensemble routing（廃止・履歴）

NAR の `nar_subclass`（DuckDB base build が `kyoso_joken_meisho` から regex で導出）に応じた per-class ensemble routing は、2026-06 の一時的な production 手法として存在したが、現行 production dispatch からは除外する。現在の `predict_upcoming.py::score_races` は per-class pool を初期化せず、`resolve_per_class_resolution` / `score_race_with_resolution` も呼ばない。解決順は **non-default cell variant → JRA E-top2（有効時のみ）→ NAR transformer blend（有効時のみ）→ category default** であり、NAR の通常 fallback は `iter12-nar-xgb-hpo-v8` の category default である。

廃止済み per-class の履歴配線は Container 内に残るが、production authority ではない：

- `src/predict_lib/per_class.py` の `PER_CLASS_MODEL_VERSIONS`（historical registry）
- `src/predict_lib/ensemble_routing.py`（履歴用 member pool / rank-blend scorer）
- `src/predict_lib/booster_pool.py`（architecture-aware な member loader）
- `src/predict_upcoming.py::score_races`（現行 production は `cell_routing.json` の non-default variant を先に解決し、該当しない場合も per-class path へは進まない）

過去に各 NAR class が serve していた model_version と blend member（`ensemble_type=rank_blend`、weight は manifest 由来）は以下。これは履歴・再検証用の記録であり、現在の production routing では採用しない。member の architecture は model_version 内の token（`-xgb-` / `-cb-` / `-lgb-`／`-lambdarank-`）で dispatch される設計だった。baseline member（`iter12-nar-xgb-hpo-v8`、192-feat XGBoost）を先に scoring し、その raw score を `iter12_score` として注入した上で residual member（≈174-feat の chain model）を scoring する two-pass 構成だった。

| class（nar_subclass） | serve model_version                                        | blend members（architecture、weight）                                                                                 | holdout Δpp vs iter12 base |
| --------------------- | ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| NEW                   | `iter30-nar-cb-ensemble-NEW-v8`                            | `iter12-nar-xgb-hpo-v8`（XGBoost, 0.690）＋ `iter30-nar-cb-residual-NEW-v8`（CatBoost, 0.310）                        | +0.000（tied で採用）      |
| MUKATSU               | `iter30-nar-cb-ensemble-MUKATSU-v8`                        | `iter12-nar-xgb-hpo-v8`（XGBoost, 0.690）＋ `iter30-nar-cb-residual-MUKATSU-v8`（CatBoost, 0.310）                    | +0.000（tied で採用）      |
| C                     | `iter36-nar-lgb-ensemble-C-v8`                             | `iter12-nar-xgb-hpo-v8`（XGBoost, 0.4976）＋ `iter36-nar-lgb-lambdarank-residual-C-v8`（LightGBM LambdaRank, 0.5024） | +0.342                     |
| A                     | `iter30-nar-cb-ensemble-A-v8`                              | `iter12-nar-xgb-hpo-v8`（XGBoost, 0.345）＋ `iter30-nar-cb-residual-A-v8`（CatBoost, 0.655）                          | +0.178                     |
| OP                    | `iter30-nar-cb-ensemble-OP-v8`                             | `iter12-nar-xgb-hpo-v8`（XGBoost, 0.2）＋ `iter30-nar-cb-residual-OP-v8`（CatBoost, 0.8）                             | +0.325（NAR 最大）         |
| other                 | `iter30-nar-cb-ensemble-other-v8`                          | `iter12-nar-xgb-hpo-v8`（XGBoost, 0.2）＋ `iter30-nar-cb-residual-other-v8`（CatBoost, 0.8）                          | +0.152                     |
| B                     | `iter12-nar-xgb-hpo-v8`（ensemble 未登録 → base fallback） | —（single-model path）                                                                                                | -0.014（REJECT）           |

- `other` は virtual code である。過去設計では未登録の real code と NULL を `normalize_class_code` で `other` に畳み、`iter30-nar-cb-ensemble-other-v8` に routing していた。
- 現行 production で書き込まれる NAR model_version は、cell 一致時は `cell_routing.json` の variant model_version、不一致時は category default / 明示的に有効な category-level path の model_version である。subclass active row や per-class registry を model_version 選択の authority にしない。

**Fallback 設計（現行）**: per-cell variant の artifact / feature contract は起動時に検証し、`feature_count`、`feature_names`、`feature_set_hash` が一致しない場合は fail-fast する。non-default cell に一致しない race は category default booster で scoring する。per-class artifact の欠落や registry は production fallback 判定に使わない。

**Cell routing override**: NAR の `dirt / mile / E / summer / venue 54` は `cell_routing.json` の non-default variant `cell-a957d8b4-nar-xgb-cell-a957d8b4-v1-56559522` に routing され、`nar-xgb-cell-a957d8b4-v1`（XGBoost, 10 features）で直接 scoring する。この path では per-class ensemble を fallback 入力として使わず、Neon へ書き込む `model_version` も `nar-xgb-cell-a957d8b4-v1` になる。該当しない NAR race も per-class へ戻さず、category default / 明示的に有効な category-level path を使う。

**deploy 経緯と gate 記録**（記録は存在する）:

- 初回 deploy: commit `869c223d`（2026-06-05）"NAR per-class ensemble routing — 6 ACCEPT classes"。採用判定は同 commit message に per-class holdout（2023-2026）で記録されている（各 manifest の `holdout_top1` / `baseline_holdout_top1` / `delta_pp` と一致）: NEW/MUKATSU +0.000（tied、iter12 weight 0.69 で採用）、C +0.019 ACCEPT、A +0.178、OP +0.325（NAR 最大）、other +0.152、**B -0.014 REJECT → iter12 base fallback**。関連する scoring fix（per-member 行列・baseline raw-score 注入・column-gap guard）は commit `b62169d4`（2026-06-06）／`6b21e03f`／`decbfc14`。
- class C の flip: commit `3669b6df`（2026-06-10）"adopt LightGBM LambdaRank residual for NAR class C"。C を iter30 CatBoost ensemble から `iter36-nar-lgb-ensemble-C-v8` に切替。holdout top1 +0.342pp、**place3 / top3_box の tradeoff を user が win-priority で承認**（manifest の `note` にも同旨）。LightGBM member は `model.txt`（native text dump）を `lightgbm_adapter` で load し、Dockerfile は `libgomp1` を install する。
- **検証上の留保（正直に記載）**: 上記 gate は 2026-06 の holdout 測定であり、現在の production selection では per-class routing 自体を採用しない。2026-07-02 の live smoke（§5.9 item 19）は当時の per-class path が実際に動いたことの履歴証跡であり、現行仕様の authority ではない。今後は class 単位ではなく cell 単位の routing / evaluation を採用判断の単位にする。

**全カテゴリ共通の現行方針**:

- JRA: per-class の infrastructure と on-disk artifact（`models/finish-position/jra/per-class/{005,010,016,701,703}/iter2x-jra-cb-ensemble-*`）は残存するが、iter19 base-only 化（commit `55629612`）で `PER_CLASS_MODEL_VERSIONS` から JRA entry が全削除され、現在 registry の JRA entry は 0 件。production `score_races` は per-class pool を初期化せず、JRA は全 class で category-global base（現行 `jra-cb-v9-sim-2013`）を single-model で serve する（残存 artifact は dead）。
- Ban-ei: `PER_CLASS_ENABLED_CATEGORIES` に含まれず per-class dir も無い。production `score_races` は per-class resolution を呼ばず、cell routing の `grade_code == E` rule と default variant だけで切り替える。
- NAR: production `score_races` は per-class resolution を呼ばない。non-default cell が一致する場合は cell model、不一致時は category default / 明示的に有効な category-level path を使う。

### 2.5 NAR Set-Transformer blend（2026-07-03 production LIVE・iter40・score-level z-fusion）

NAR の iter40 transformer blend（`iter40-nar-settransformer-blend-v1`）は 2026-07-03 に Cloudflare Worker / Queue / Container の production path で ENABLED——**NAR serving は iter40 transformer blend に切替済み**（§2 本番モデル表の NAR 行がこれを指す）。有効化は Neon-write smoke が CLEAN で完了（iter40 行が本番 Neon に landing・fusion が iter12 と異なる place-refining・fallback → iter12 を direct prod-Neon query で確認）したことを受けて実施した。有効化方式は **CF worker** = `container-class.ts` が env を container へ forward（commit `28c89abf`）＋ wrangler **secret** `NAR_TRANSFORMER_BLEND_ENABLED=1`（redeploy 耐性）＋ worker deploy（Version `0d2d99c7-cbe1-4989-a497-255e73e77388`）。iter40 は serve-exact gate ADOPT 済み。**初回 genuine 本番 write 確認済み（2026-07-03、production LIVE）**——Cloudflare production path の per-race full serve が iter40 で 48 NAR レース（499 頭行）を 03:05 JST に本番 Neon `race_finish_position_model_predictions` へ書き込み（`model_version=iter40-nar-settransformer-blend-v1`、`prediction_generated_at` 18:05:32-18:05:47 UTC ＝ 03:05 JST）、viewer は当日 48 NAR レース全てで iter40 を表示（latest-for-race、iter40 が 48/48 で勝ち）。rollback は CF secret を 0 に戻す（redeploy 不要）。ローカル `.env` は診断・backfill wrapper 用で、本番 trigger / fallback / ordering dependency ではない。アーキテクチャ lever が accept gate を通過した経緯・統制・機構は §11、deploy 記録・検証・rollback は §5.9 の 2026-07-03 ブロックを参照。**2026-07-03 追記——fusion 方式を rank-fusion → score-level z-fusion に更新（deployed win #2、commit `a90161f4`、model_version は iter40 維持）**: rank が捨てる confidence magnitude を保持する score-z が 5-fold pooled（66,883 races）で rank-fusion を上回り（top1 +0.253[LB95 +0.120]／place2 +0.341[+0.166]／place3 +0.230[+0.051]、全 3 primary LB95>0）、Cloudflare Worker / Container path で稼働。過去日 NAR race（`kb55` r3、20260627）を production image で実行し本番 Neon に score-z 11 行が landing（旧 rank-fusion 行と rank 3/11 相違）で LIVE 確認済み。詳細は §11 score-z ブロック / §5.9。

- **手法（2026-07-03 現在 = score-level z-fusion）**: 本番 category-default fallback `iter12-nar-xgb-hpo-v8`（XGBoost・192 feat）の within-race score と、117-feat の 3-seed listwise Set Transformer（全 history 学習・レース内 cross-horse self-attention ＋ listnet loss）の seed-score を、それぞれ race 内で z-正規化してから **0.5 / 0.5 で score-level z-fusion**（2026-07-03 deploy で rank-fusion から更新、commit `a90161f4`、variant `score_z_55`。同順位は `ketto`＝血統登録番号の昇順で tie-break）する。base / fallback は iter12 category-default model。per-class ensemble（iter12 + iter30 + iter36）は import されるが `predict_upcoming.py:640` で unwired ＝ score_races は単一 iter12 で fuse するため、5-fold gate の iter12 base と実 serving base は完全一致する。
- **serve**: `apps/finish-position-predict-container/src/predict_lib/transformer_scorer.py`（pure numpy float64・MLX-eager 実装に byte-exact ＝順位 flip 0 で Container 側は MLX 不要）。scoring は per-race（`score_races` の per-race ループ内 `_score_one_race_nar_blend`・docs §9 の per-race 原則を遵守）。**3-tier fail-closed fallback**（field<2 頭 / feature 欠損 / 例外 のいずれかで iter12 ensemble に落ちる）。R2 weights: `pc-keiba-finish-position-models/finish-position/nar/iter40-nar-settransformer-blend-v1/`（`weights_s1/2/3.npz` ＋ `norm.json`）。
- **flag と有効化（Cloudflare production path で ENABLED=1）**: `NAR_TRANSFORMER_BLEND_ENABLED`（env override）。**CF production path で env=1 有効化済み**——wrangler **secret** `NAR_TRANSFORMER_BLEND_ENABLED=1` として設定（redeploy で消えない drop-proof、live-rollbackable）、`container-class.ts`（commit `28c89abf`）が env を container へ forward、worker deploy Version `0d2d99c7-cbe1-4989-a497-255e73e77388`（container image 再利用、crons/queue/observability intact）。次回 per-race container start で有効。**rollback**（redeploy 不要）: `printf 0 | bunx wrangler secret put NAR_TRANSFORMER_BLEND_ENABLED`。即時に pure iter12 ensemble へ戻る。**注意**: base image は default=False を bake している（HEAD からの rebuild + redeploy でも transformer は default OFF）ため、有効化は上記 secret / env=1 の明示で成立している。詳細は §5.9。
- **精度**: serve-exact（ketto tie-break）gate で ADOPT。pooled blind 2023 / 2024 / 2025 で **top1 Δ+0.629pp[LB95 +0.467]／place2 Δ+0.432[+0.199]／place3 Δ+0.499[+0.260]**、全 fold 全 primary が正。**本キャンペーン初の「アーキテクチャ lever」による deployable win**（30+ の特徴量 lever は市場効率の壁で REJECT だった）。

---

## 3. 特徴量パイプライン（DuckDB feature builder）

特徴量は DuckDB ベースの builder が PostgreSQL から構築する。本番 Container は Neon / production PostgreSQL を読む。local PG は local 学習・検証・artifact 生成・明示的な operator repair 用であり、本番 feature generation の依存先ではない。

- メインビルダー: `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`
- DuckDB の postgres extension 経由で PostgreSQL を読む（Container 内は native libpq、Hyperdrive 不要）。

```mermaid
flowchart LR
    PG[("PostgreSQL<br/>production Neon<br/>local PG = training/repair only")]
    subgraph DUCK["DuckDB feature builder"]
        BASE["base build<br/>se / um / ra テーブル走査"]
        L1["class features"]
        L2["futan"]
        L3["market signals"]
        L4["pacestyle"]
        L5["relationship"]
        L6["similar-race（sim_*）"]
        L7["kohan3f"]
        L8["exotic odds"]
        BASE --> L1 --> L2 --> L3 --> L4 --> L5 --> L6 --> L7 --> L8
    end
    PARQUET["per-race Parquet<br/>（R2）"]

    PG -->|"postgres extension"| BASE
    L8 --> PARQUET
```

### 3.1 per-race モード（`--target-race`）

Container のレース単位予測のため、`--target-race keibajo_code:race_bango` で単一レースのみの特徴量を構築できる（`finish_position_features_duckdb.py:253`）。指定時は rec history scan を当該レースの馬・騎手に絞り込む。

履歴 join は `h.race_date < t.race_date` を用いるため、対象レースが未確定（未走）の段階でも window が計算可能で、対象レースの結果が leak しない（`finish_position_features_duckdb.py:219`）。

### 3.2 エンティティフィルタ（Neon max_stack_depth 対策）

- **se / um テーブル（馬単位）**: `postgres_query()` を用い、`ketto_toroku_bango`（血統登録番号）による horse-level の `IN` フィルタを push down する（`finish_position_features_duckdb.py:441`, `:490`）。
- **ra テーブル（レース単位）**: エンティティフィルタを掛けない。compound tuple の `IN` は Neon の `max_stack_depth` を超過するため。

この非対称性は意図的であり、Neon のスタック制約を回避しつつ馬単位の履歴走査を限定する設計である。

### 3.3 レイヤチェーンと特徴量数

base DuckDB build に v7 由来の enrichment レイヤを積層する。最終的な特徴量数はカテゴリごとに異なる。

| カテゴリ | 最終特徴量数 |
| -------- | ------------ |
| JRA      | 263          |
| NAR      | 192          |
| Ban-ei   | 130          |

similar-race 特徴量（`sim_*`、19 列）は JRA / Ban-ei で ADOPT（v9-sim）、NAR では REJECT。このため NAR の特徴量数（192）は sim\_\* を含まず、JRA（263）・Ban-ei（130）とレイヤ構成が異なる。

### 3.4 脚質予測の特徴量契約

脚質予測の per-race feature builder は `apps/sync-realtime-data/src/running-style-feature-sql.ts` / `running-style-feature-parquet.ts` が担う。基本方針は着順特徴量と揃え、`race_entry_corner_features` と過去履歴から馬・騎手・距離・コーナー・ペース・馬体重などを構築する。ただし target は脚質専用の `target_running_style_class`（`corner1_norm` 由来）であり、実際に scoring へ渡す列は選択された LightGBM model の `feature_names` に従う。

脚質 feature Parquet は `running-style/features-parquet/{source}/{YYYYMMDD}/{raceKey}.parquet` に置く。Worker は R2 を先に読み、miss の場合だけ PostgreSQL / Hyperdrive から再構築する。

routing と後段着順生成のため、脚質 feature rows は以下の metadata を必ず保持する。

| metadata                                                                      | 用途                                            |
| ----------------------------------------------------------------------------- | ----------------------------------------------- |
| `raceKey`, `source`, `kaisaiNen`, `kaisaiTsukihi`, `keibajoCode`, `raceBango` | race identity                                   |
| `category`                                                                    | `jra` / `nar` / `ban-ei` の routing             |
| `kyori`, `trackCode`, `gradeCode`, `shussoTosu`                               | distance / surface / class / field-size routing |
| `kyosoJokenCode`, `narSubClass`                                               | subgroup routing                                |
| `kettoTorokuBango`, `umaban`, `bamei`                                         | runner identity                                 |

この metadata は feature そのものではなく routing contract である。`kyori` / `trackCode` / `gradeCode` / `shussoTosu` / `kyosoJokenCode` / `narSubClass` を削ると、脚質 cell routing と着順 full trigger の整合性が壊れる。

---

## 4. 脚質予測システム（running-style）

脚質予測の本番 owner は `apps/sync-realtime-data/` の `running-style-cron.ts` / `running-style-queue.ts` である。`sync-realtime-data-features` は本番脚質推論の owner ではない。

### 4.1 入力・ラベル・用語

- **source**: `jra` / `nar`。`race_key` と R2 daily prediction Parquet は source で分かれる。
- **category**: `jra` / `nar` / `ban-ei`。source と `keibajo_code` から導出する routing / log 用の分類であり、source と同一ではない。Ban-ei は source=`nar` 由来の特殊カテゴリで、`add-pacestyle-features.py` の通常カテゴリではない。
- **class label**: `nige` / `senkou` / `sashi` / `oikomi`。class id は順に `0` / `1` / `2` / `3`。
- **`target_running_style_class`**: 学習用 target。historical/current-result の `corner1_norm` から作る label であり、pre-race feature から作るものではない。未走レースの scoring では target として使わない。
- **`predicted_label` / `predicted_class`**: 推論結果。`predicted_class` は `predicted_label` の class id であり、`target_running_style_class` とは別物である。
- **version 名**: `running_style_feature_version`（学習・postproc 側の脚質特徴量版）、`feature_schema_version`（per-race feature schema 版）、`model_version`（予測モデル版）は別概念であり、混同しない。

race key は用途で形式が異なる。

| 用途                                            | 形式                                                                   |
| ----------------------------------------------- | ---------------------------------------------------------------------- |
| D1 `race_running_styles.race_key` / Worker 内部 | `{source}:{YYYYMMDD}:{keibajo}:{race_bango}`                           |
| viewer cache / realtime race key                | `{source}:{YYYY}:{MMDD}:{keibajo}:{race_bango}`                        |
| `add-pacestyle-features.py` の `race_id`        | `{category}:{kaisai_nen}:{kaisai_tsukihi}:{keibajo_code}:{race_bango}` |
| finish-position `/predict` race scope           | `category` + `runDate=YYYYMMDD` + `keibajoCode` + `raceBango`          |

### 4.2 本番フロー

```mermaid
flowchart LR
    D1R[("D1<br/>realtime_race_sources")]
    PLAN["running-style-cron<br/>planRunningStylePredictionsForDate"]
    Q["RUNNING_STYLE_JOBS"]
    H["running-style-queue<br/>handleRunningStylePredictionJob"]
    FEAT["R2 feature Parquet<br/>or PostgreSQL rebuild"]
    MODEL["R2 RUNNING_STYLE_MODELS<br/>flatbin LightGBM + optional calibrators"]
    D1RS[("D1<br/>race_running_styles")]
    NEONRS[("Neon<br/>race_running_style_model_predictions")]
    R2DAY[("R2<br/>running-style/predictions/by-day")]
    CACHE["viewer cache<br/>Cache API + D1 query cache"]
    FPQ["finish-position-predict-queue"]
    FPF["finish-position-cron<br/>POST /run fallback"]

    D1R --> PLAN --> Q --> H
    FEAT --> H
    MODEL --> H
    H --> D1RS
    H --> NEONRS
    D1RS --> R2DAY
    D1RS --> CACHE
    H -->|"D1 + Neon expected count OK"| FPQ
    H -.->|"queue binding absent"| FPF
```

`planRunningStylePredictionsForDate()` は D1 の race list と既存 prediction count を見て未完了 race を enqueue する。planner 自体が corner feature の存在を hard gate するのではなく、queue handler が feature Parquet を R2 から読み、miss 時に PostgreSQL / Hyperdrive から再構築する。calibrator は R2 にあれば適用し、読めない場合は uncalibrated prediction に fallback する。

### 4.3 出力

脚質予測は同一 race の結果を複数の読み先へ配る。

| 出力先                                      | 内容                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D1 `race_running_styles`                    | per-horse `p_nige` / `p_senkou` / `p_sashi` / `p_oikomi`、`predicted_label`、`predicted_corner_front_score`、`predicted_corner_rank`、`model_version`、`cell_model_key`、`cell_variant_id`。Worker 内の source of truth                                                                                                             |
| Neon `race_running_style_model_predictions` | viewer / 着順 layer が読む mirror。D1 と同じ `cell_model_key` / `cell_variant_id` と `predicted_corner_front_score` / `predicted_corner_rank` を永続化し、どの cell model で予測した行か、脚質確率から導出したコーナー通過順予測が何かを後段・監査で追跡できるようにする。write は `DATABASE_URL_NEON` → `NEON_DATABASE_URL` を優先 |
| R2 daily prediction Parquet                 | `running-style/predictions/by-day/{YYYY}/{MM}/{DD}/{source}/{model_version}.parquet`。`add-pacestyle-features.py` の R2 path                                                                                                                                                                                                        |
| viewer cache                                | `/api/races/{YYYY}/{MM}/{DD}/{keibajo}/{race}/running-styles?source=...` の Cache API と D1 query cache                                                                                                                                                                                                                             |

R2 daily prediction Parquet は source=`jra|nar` 単位で export する。1 つの現行 model version をドキュメントで固定しない。複数 `model_version` が同日に存在する場合は model version ごとの Parquet になる。

D1 は migration `0032_running_style_predicted_corner.sql` で `predicted_corner_front_score` / `predicted_corner_rank` を持つ。Neon mirror は Worker upsert と D1→Neon sync CLI の双方が `add column if not exists` を先行実行し、列未追加の環境でも同じ永続列へ安定して書き込む。旧 D1 行に rank が無い場合は読み出し時に `p_senkou + 2*p_sashi + 3*p_oikomi` の昇順、`p_nige` 降順、`ketto_toroku_bango` 昇順、馬番昇順で race 内 `row_number()` を補完する。

### 4.4 cell-level routing

脚質も着順と同様に cell-level routing を持つ。`RUNNING_STYLE_CELL_ROUTING_JSON` が設定されている場合、`running-style-cell-router.ts` が race metadata から cell を導出し、variant を選ぶ。未設定時、または該当カテゴリの config が無い場合は source 単位の latest model key に fallback する。

脚質 cell dimensions は以下。

| dimension                                      | 元データ / 派生                                           |
| ---------------------------------------------- | --------------------------------------------------------- |
| `category`, `source`                           | source と keibajo から導出                                |
| `venue`, `racetrack`, `keibajo_code`           | `keibajoCode`                                             |
| `surface`, `trackCode`                         | JRA `trackCode` 先頭で turf / dirt / other、NAR 系は dirt |
| `distance_band`, `kyori`                       | `<1200`, `<1600`, `<2000`, `<2400`, `>=2400`              |
| `season`                                       | `kaisaiTsukihi` の月                                      |
| `class`, `grade_code`                          | `gradeCode`                                               |
| `subgroup`, `kyoso_joken_code`, `nar_subclass` | NAR は `narSubClass`、それ以外は `kyosoJokenCode`         |
| `shusso_tosu`                                  | field size                                                |

routing のログ・summary では `cellModelKey` と `cellVariantId` を確認する。これは採用された flatbin model key と variant id であり、model の `model_version` とは別である。prediction row には snake_case の `cell_model_key` / `cell_variant_id` として D1 `race_running_styles` と Neon `race_running_style_model_predictions` の両方へ永続化する。これらをログだけに残して DB に保存しない運用は禁止する。

`RUNNING_STYLE_CELL_ROUTING_JSON` は production routing の唯一の data-driven control plane である。構造は category（`jra` / `nar` / `ban-ei`）ごとに `defaultVariantId`、`rules`、`variants` を持つ。

```json
{
  "jra": {
    "defaultVariantId": "latest",
    "rules": [
      {
        "conditions": [{ "dimension": "venue", "values": ["05"] }],
        "variantId": "tokyo-turf"
      }
    ],
    "variants": {
      "latest": { "modelKey": "running-style/models/jra/latest.flatbin" },
      "tokyo-turf": { "modelKey": "running-style/models/jra/cells/tokyo-turf.flatbin" }
    }
  }
}
```

`variantId` は routing の識別子で、`modelKey` は `RUNNING_STYLE_MODELS` R2 binding 内の flatbin object key である。rule が一致しない場合は `defaultVariantId` を使う。category config が無い場合は `buildRunningStyleFlatModelKey(source)` の source 単位 latest に fallback する。存在しない `variantId` を rule が参照する JSON は production に入れてはならない。

### 4.5 脚質 cell 学習・評価・promotion（ローカル）

脚質 cell model の学習・評価・promotion plan はローカルの `apps/pc-keiba-viewer/src/scripts/running_style_lightgbm.py train-cells` で行う。これは model artifact を作るための作業であり、本番推論をローカルで実行するものではない。学習・評価・採用判定の単位は category や source の粗い集計ではなく **cell variant 単位**であり、cell 外の平均で改善・回帰を判断してはならない。異なる cell の行を混ぜて単一の「脚質 cell 評価」として扱うことは禁止する。

実行単位は `source` / `category` / cell variant で、入力は脚質 feature Parquet と `target_running_style_class` を持つ labeled rows である。`target_running_style_class` は `nige=0` / `senkou=1` / `sashi=2` / `oikomi=3` の 4-class softmax target であり、着順の rank metric では評価しない。

標準の流れ:

1. `running_style_lightgbm.py train-cells` で候補 cell variant を学習し、cell ごとの `model.txt` / `metadata.json`、Worker routing 候補 JSON、`cell_metrics.json` を出力する。このコマンド自体は walk-forward prediction parquet を出力しない。
2. `cell_metrics.json` の各 `trained_cells[*].metrics` は rate だけでなく、`prediction_count`、`top2_hit_count`、`race_level`、`race_level_pair_metrics`、`confusion_matrix`、`per_class_log_loss_sum/count` を含む。`race_level_pair_metrics` は `corner1` / `corner3` / `corner4` / `finish` ごとの pair score `sum` / `count` / `score` を保持し、生成された脚質確率のレース内順序が実コーナー通過順と最終着順にどれだけ合うかを cell ごとに比較する。cell 間・期間間の集計は rate 平均ではなく raw count / sum から再計算する。
3. `trained_cells[*].cell_training_evaluation` は `cell_training_evaluations.prediction_target = 'running_style'` として保存する互換 mapping を持つ。永続化する場合は `train-cells --save-cell-metrics-to-postgres --pg-url <postgres-url>` を使い、`CellAccuracyStore` と同じ upsert 経路で保存する。`feature_set_hash` が同じ cell は `feature_names_array` と合わせて保存され、着順と脚質は `prediction_target` で分離される。
4. walk-forward / local prediction parquet は `apply-running-style-postproc.ts` で `predicted_corner_front_score = p_senkou + 2*p_sashi + 3*p_oikomi` と race 内 `predicted_corner_rank` を生成する。これは脚質確率から作るコーナー通過順予測であり、actual corner 列を scoring input にしてはならない。
5. `evaluate-running-style-bucket-21y.ts` は `load_running_style_predictions.py` 経由で `predicted_corner_front_score` を PostgreSQL temp table に読み、`running_style_model_bucket_evaluations` へ cell provenance（`cell_model_key` / `cell_variant_id`）付きで `corner1_pair_score_sum/count`、`corner3_pair_score_sum/count`、`corner4_pair_score_sum/count`、`finish_pair_score_sum/count` を upsert する。これが新方式の cell 単位コーナー順序評価の永続化経路である。metrics JSON を 1 行ずつ反映する補助経路 `insert_running_style_bucket_evaluation_row.py` も同じ 51 列構成で保存する。
   - NAR の既定評価年は **2005〜2026 の連続年**である。2018〜2025 を飛ばすと、blind 2023 / 2024 / 2025 と 2024+ の production 脚質 prediction coverage を評価から落とすため、cell 採用判断が歪む。JRA / NAR の category window は `CATEGORY_YEAR_WINDOWS` のテストで固定し、欠落年を許さない。
6. `build_cell_models.py --prediction-target running_style` で baseline variant と候補 variant を同じ cell 定義・同じ holdout window で比較する。
7. adoption gate を通過した候補が同一 cell に複数ある場合、cell ごとに target-specific score が最良の 1 candidate（method / `model_version` / `feature_set_hash`）へ絞る。routing JSON へ出すのは gate 通過候補すべてではなく、この per-cell winner のみである。
8. 採用 cell だけを feature-selection routing JSON に残す。`build_cell_models.py --prediction-target running_style` が出力する JSON は `type = running_style_cell_feature_selection_routing` / `worker_production_routing = false` のローカル学習用 control plane であり、Worker production routing JSON ではない。variant には `feature_set_hash` と `feature_names` を含める。
9. `running_style_lightgbm.py train-cells --cell-feature-selection-json <routing.json>` で採用 cell ごとの `feature_names` を読み、cell ごとに最良だった特徴量セットで local model artifact を作る。未採用 cell は全体特徴量または source latest に fallback する。
10. 採用 variant の LightGBM artifact を Worker が読む header metadata 込み flatbin へ変換し、`RUNNING_STYLE_MODELS` R2 に upload する。
11. upload 済み R2 key だけを `RUNNING_STYLE_CELL_ROUTING_JSON` の `variants[*].modelKey` に反映し、Cloudflare Worker の設定として promote する。

`train-cells` の LightGBM resource control は `--num-threads auto` が既定である。auto は macOS の load average、available memory（free / inactive / speculative / purgeable）、compressor 使用量から fit ごとの thread 数を決め、さらに `/tmp` の slot lock で同時 fit 数を制御する。明示的な固定値が必要な検証時だけ `--num-threads 1` のように指定する。

脚質 / 着順の local feature generation / bucket evaluation は固定 thread / concurrency / `work_mem` / `memory_limit` を標準既定にしない。`generate-running-style-local.ts` は Colima capacity と、その時点の macOS resource snapshot（load average、available memory = free / inactive / speculative / purgeable、compressor 使用量）から DuckDB `--threads`、`--memory-limit`、Phase A chunk concurrency、category concurrency を実行時に解決する。`generate-finish-position-local.ts` は同じ resource snapshot から DuckDB `--threads` と `--memory-limit` を解決する。Python の direct wrapper も `_resource_defaults.py` で同じ macOS pressure を見て DuckDB threads と `memory_limit` を縮退させる。`evaluate-running-style-bucket-21y.ts` の `--chunk-concurrency` / `--work-mem-mb` も既定は `auto` で、同じ resource snapshot から chunk concurrency、category concurrency、PostgreSQL session `work_mem` を解決する。macOS load が高い、free 系 memory が少ない、または compressor pressure が高い場合は、`memory_limit` と `work_mem` も現在の空きリソースに合わせて小さくする。明示指定した場合だけ固定値を使う。kernel panic / swap pressure 再発防止のため、手元の空きリソースを見ずに `8 threads` / `4 parallel` / `256MB work_mem` / `6GB memory_limit` のような固定値を標準運用に戻してはならない。

production prediction では、routing 結果の `modelKey` / `variantId` を各 prediction row の `cell_model_key` / `cell_variant_id` として D1 と Neon に保存する。同時に、脚質確率から導出した `predicted_corner_front_score` / `predicted_corner_rank` も D1 と Neon に保存する。D1 と Neon のどちらか片方だけに保存する、または `model_version` だけで cell variant / predicted corner order を復元しようとする運用は禁止する。

ローカル実行 wrapper:

```bash
bun run --filter pc-keiba-viewer dev:running-style-train-cells -- \
  --csv <feature-parquet> \
  --model-version <version> \
  --output-root <output-dir> \
  --output-routing-json <output-dir>/cell_routing.json \
  --save-cell-metrics-to-postgres \
  --pg-url postgresql://horse_racing:horse_racing@127.0.0.1:<POSTGRES_PORT>/horse_racing
```

local PostgreSQL に保存する場合、`--pg-url` の host port は `apps/local-postgresql/.env` の `POSTGRES_PORT` を使う。container 内部の `PGPORT` と取り違えると、学習後の `cell_training_evaluations` 保存だけが接続拒否で失敗する。

脚質評価で使う metric は running-style 固有であり、finish-position の top1 / place2〜place6 / NDCG gate を流用しない。

| metric                             | 用途                                                                                                                                   |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `accuracy`                         | 4-class predicted class と `target_running_style_class` の一致率                                                                       |
| `macro_f1`                         | `nige` / `senkou` / `sashi` / `oikomi` を4クラス固定で平均する class-balance 指標。未定義 class は 0.0 として扱う                      |
| `per_class_accuracy` / `f1`        | `nige` / `senkou` / `sashi` / `oikomi` の actual class 内での的中率と F1。true negative で水増しされる one-vs-rest accuracy は使わない |
| `per_class_precision` / `recall`   | class ごとの採否確認。特に `nige` は過剰 positive を監視する                                                                           |
| `per_class_support`                | cell 内で評価可能な class 分布。support が薄い cell は採用しない                                                                       |
| `precision_nige` / `recall_nige`   | 逃げ class の precision / recall。production skew の主監視対象                                                                         |
| `log_loss_nige` / `multi_log_loss` | 確率品質。calibrator や class-weight 変更時の過信を検出する                                                                            |
| `confusion_matrix`                 | actual class × predicted class の raw count。集計時はこの count を加算してから precision / recall / F1 を再計算する                    |
| `top2_hit_count`                   | top2 的中数。`top2_accuracy` の再集計では count を加算してから `prediction_count` で割る                                               |
| `race_level`                       | race 単位で脚質分布・脚質 count・corner passage order・finish position との整合性を見る派生 metric 群                                  |
| `race_level_pair_metrics`          | race 内の horse pair ごとに、予測 front score の順序が corner 通過順・最終着順から導出した順序と一致するかを見る pair metric 群        |
| `race_level_per_class_metrics`     | race 内で導出した actual class ごとの precision / recall / F1 / support。`nige` / `senkou` / `sashi` / `oikomi` を4クラス固定で見る    |

`race_level` は generated running-style prediction を horse 単位の class 一致だけでなく、同一 race 内の並びとして評価する。評価対象は生成された脚質予測割合だけではない。`style_distribution_mae` / `style_count_mae` / `style_count_bias` / `nige_count_mae` / `front_group_count_mae` は race 内の脚質構成のずれを測る。`corner_rank_spearman` は予測 front score と `corner1_norm` の順位相関で、通過順と脚質予測の順序整合を確認する。加えて、corner 通過順および `finish_position` から race-level pair metrics を導出し、同一 race 内の horse pair の前後関係が予測 front score と整合しているかを見る。per-class metrics は `nige` / `senkou` / `sashi` / `oikomi` の4クラス固定で計算し、特定 class の崩れを race-level aggregate に埋もれさせない。`finish_weighted_accuracy` / `top1_finish_style_accuracy` / `top3_finish_style_accuracy` は `finish_position` で上位馬を重く見た脚質 class 一致率であり、着順 rank metric ではなく、脚質予測が最終着順上位の馬で崩れていないかを見る補助指標である。

`cell_training_evaluations` の共有列へ保存する場合、running-style profile は `top1_accuracy = accuracy`、`place2_accuracy = top2_accuracy`、`place3_accuracy = macro_f1` として扱う。`build_cell_models.py --prediction-target running_style` は `top1_accuracy` の改善を必須とし、`top2_accuracy` または `macro_f1` のどちらかも改善した cell だけを採用対象にする。

running-style の新方式 cell 精度は、共有列に加えて `metric_payload` JSONB に保存する。payload は `metric_schema_version = running_style_cell_v2`、`prediction_count`、`top2_hit_count`、`accuracy`、`top2_accuracy`、`macro_f1`、`multi_log_loss`、`per_class_*`、`confusion_matrix`、`per_class_log_loss_*`、`race_level.corner_rank_spearman`、`race_level.finish_weighted_accuracy`、`race_level_pair_metrics.corner1/corner3/corner4/finish` などを持つ。`NaN` は JSONB に保存できないため `null` に正規化する。採用比較では互換列を使い、分析・再集計では `metric_payload` の class 別 / race-level 指標を使う。

promotion は「metrics が良い」だけでは完了しない。production で参照される object は flatbin だけであり、R2 に upload されていない local artifact、または `RUNNING_STYLE_CELL_ROUTING_JSON` に反映されていない variant は production に存在しないものとして扱う。

Cloudflare 側で確認する項目:

- `RUNNING_STYLE_MODELS` に `running-style/models/{source}/.../*.flatbin` が存在し、flatbin header の `model_version` / `feature_names` / `class_labels` が期待値と一致する。
- `RUNNING_STYLE_CELL_ROUTING_JSON` の `variants[*].modelKey` が upload 済み flatbin object key を指す。
- `generate-running-style-predictions` の summary に期待した `cellVariantId` / `cellModelKey` が出る。
- D1 `race_running_styles`、Neon `race_running_style_model_predictions`、R2 daily prediction Parquet の件数が expected horse count 以上で揃う。
- D1 `race_running_styles.cell_model_key` / `cell_variant_id` と Neon `race_running_style_model_predictions.cell_model_key` / `cell_variant_id` が summary の `cellModelKey` / `cellVariantId` と一致する。

### 4.6 着順予測との結合

着順特徴量は `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-pacestyle-features.py` で脚質予測を読む。通常の pacestyle layer は `--category jra|nar` を対象とし、Ban-ei を通常の RS/pacestyle 対象として扱わない。

`add-pacestyle-features.py` は actual corner 通過順を特徴量として読まない。脚質予測確率から `rs_predicted_corner_front_score = rs_p_senkou + 2 * rs_p_sashi + 3 * rs_p_oikomi` を生成し、race 内で `rs_predicted_corner_rank` と `rs_predicted_corner_rank_pct` を作る。これらは脚質モデルが生成した事前予測だけを使うため、本番の特徴量生成でも local 学習でも同じロジックで利用できる。

`add-pacestyle-features.py` は R2 daily prediction Parquet を優先でき、R2 が使えない場合は Neon `race_running_style_model_predictions` を読む。join は same race + `ketto_toroku_bango` で行い、missing row は `rs_p_*` / `rs_predicted_class` などの nullable `rs_*` として残る。missing running-style を reason に着順 feature build 全体を落としてはいけない。

一方、本番の per-race full trigger は脚質完了を強く要求する。`handleRunningStylePredictionJob()` は D1 write count と Neon mirror count が expected horse count 以上であることを確認してから、まず `FINISH_POSITION_PREDICT_QUEUE` へ focused per-race full message を enqueue する。`FINISH_POSITION_CRON` service binding への `POST /run` は queue binding が無い環境の fallback である。

### 4.7 本番 ordering guard

本番の ordering guard は `sync-realtime-data` の queue handler が担う。対象 race の feature generation が完了していない場合は脚質 prediction を完了扱いにせず、脚質 D1 write と Neon mirror が expected horse count 以上になるまで `FINISH_POSITION_PREDICT_QUEUE` への enqueue を発行しない。service binding fallback でも同じ guard を通り、guard 未達の race で `FINISH_POSITION_CRON` への `POST /run` を発行しない。

この guard は Cloudflare Worker / Queue と、fallback 時の service binding の中で完結する。ローカル scheduler、手元 shell script、ローカル Docker process で feature → running-style → finish-position の順序を補完・代替してはならない。

---

## 5. 着順推論アーキテクチャ（finish-position）

```mermaid
flowchart TB
    subgraph CRONW["finish-position-cron（Worker）"]
        TRIG["POST /run"]
        COORD["race-coordinator<br/>（JST 10:00-20:59, 10 分毎）"]
        PRODUCER["queue-producer"]
        NEONWARM["neon-warm<br/>（pre-wake / keep-warm）"]
        TRIG --> PRODUCER
        COORD --> PRODUCER
    end
    QUEUE["Cloudflare Queues<br/>（per-category / per-race）"]
    subgraph DOBJ["Container DO"]
        SERVER["Python HTTP server"]
        EP1["GET /predict"]
        EP2["GET /health"]
        SERVER --> EP1
        SERVER --> EP2
    end
    DUCKBUILD["DuckDB feature build"]
    SCORE["scoring<br/>（CatBoost / XGBoost booster）"]
    FPNEON[("Neon<br/>race_finish_position_<br/>model_predictions")]
    R2FP[("R2<br/>finish-position feature Parquet")]

    UPSTREAM["sync-realtime-data<br/>running-style complete"]
    UPSTREAM -->|"enqueue<br/>mode=full, skipDedup=true"| QUEUE
    UPSTREAM -.->|"fallback POST /run"| TRIG
    PRODUCER --> QUEUE
    QUEUE -->|"queue consumer"| SERVER
    EP1 --> DUCKBUILD --> SCORE --> FPNEON
    EP1 <-->|"full/rescore cache"| R2FP
    NEONWARM -.->|"SELECT 1"| FPNEON
```

### 5.1 構成要素

- **Upstream trigger（`sync-realtime-data`）**: 脚質完了後に `FINISH_POSITION_PREDICT_QUEUE` へ focused per-race full を enqueue する。queue binding が無い環境だけ `FINISH_POSITION_CRON` service binding / API の `POST /run` に fallback する。脚質推論自体の詳細は §4。
- **Cron Worker（`finish-position-cron`）**: Cloudflare Queues 経由で Container をトリガーする。`apps/finish-position-cron/wrangler.jsonc` に cron / queue / container binding を定義。
- **Container DO（`FinishPositionPredictContainer`）**: Python HTTP server を内包し、DuckDB ビルドと scoring を実行する。`instance_type: standard-4`, `max_instances: 10`。Queue consumer からの Container DO 名は `predict-{category}` に集約する。per-race identity は `/predict` query の `keibajoCode` / `raceBango` で渡し、DO 名を race scope にしてはならない。
- **PredictRunCoordinator（DO）**: run の dedup / state を strong-consistency で管理（旧 KV `PREDICT_STATE` を置換）。eventual consistency の KV では二重実行を防げないため DO に移行した。

### 5.2 HTTP エンドポイント

- **`GET /predict`** — 特徴量ビルド + scoring。chunked NDJSON（`Transfer-Encoding: chunked`, `application/x-ndjson`）でストリーム返却（`serve.py:11`）。
- **`GET /health`** — ヘルスチェック。

`/predict` のクエリパラメータ（`serve.py:121-176` で parse・validate）:

| パラメータ    | 必須 | 既定               | 説明                            |
| ------------- | ---- | ------------------ | ------------------------------- |
| `category`    | 必須 | —                  | `jra` / `nar` / `ban-ei`        |
| `runDate`     | 必須 | —                  | YYYYMMDD（8 桁 ASCII 数字）     |
| `daysAhead`   | 任意 | `0`                | 非負整数                        |
| `mode`        | 任意 | `full`             | `full` / `rescore`              |
| `keibajoCode` | 任意 | `None`（全レース） | per-race scope 用の競馬場コード |
| `raceBango`   | 任意 | `None`（全レース） | per-race scope 用のレース番号   |

- 日単位バッチ例: `/predict?category=jra&runDate=20260619&daysAhead=0`
- レース単位例（per-race）: `/predict?mode=full&category=nar&runDate=20260628&keibajoCode=35&raceBango=01`
- `keibajoCode` / `raceBango` を両方指定すると単一レースに scope される。R2 特徴量キャッシュキーは `feat-cache/{category}/{runDate}/{keibajoCode}/{raceBango}/features.parquet`（`serve.py:342-349`）。

> 注: 実装上のパラメータ名は `runDate` であり、`targetDate` ではない。日付は `runDate` で渡す。

### 5.3 予測モード

- **`full`** — DuckDB でゼロから特徴量を構築して scoring する。
- **`rescore`** — R2 にキャッシュ済みの特徴量を読み込み、late-binding refresh（直前のオッズ・馬体重など遅延確定値の差し替え）を行って再 scoring する。

### 5.4 本番順序と per-race full 生成

本番 full 生成の順序は固定である。

```
feature generation -> running-style generation -> finish-position full generation
```

`sync-realtime-data` は feature generation 後に脚質予測を per-race で実行し、D1 `race_running_styles` と Neon `race_running_style_model_predictions` の書き込みを確認してから `FINISH_POSITION_PREDICT_QUEUE` へ message を送る。message は `mode: "full"`、`skipDedup: true`、`category`、`runDate` / `runDateIso` / `runYmd`、`keibajoCode`、`raceBango` を含む。queue binding が無い環境では、同等の body を `finish-position-cron` の service binding `FINISH_POSITION_CRON` へ `POST /run` する fallback を使う。

上記以外の順序は本番仕様ではない。finish-position full generation を feature generation より前、または running-style generation 完了前に走らせる fallback は作らない。ローカル wrapper はこの順序の authority ではなく、障害時の再実行境界も Cloudflare Queue retry / DLQ と Worker state に置く。

`skipDedup: true` は「脚質完了に連動した focused per-race full」を意味する。`finish-position-cron` の queue consumer はこの message では category-level の `claimRun` / `completeRun` を通らず、category complete や category cache warm を汚さない。Container の NDJSON final line が `status:error` の場合は成功扱いせず、queue retry / DLQ に回す。

Container の `sleepAfter` 中も Cloudflare の live instance count には残る。Queue の `max_concurrency: 1` は consumer invocation を直列化するだけで、race-scoped DO 名が作る複数の sleeping Container instance を抑制しない。NAR のように同日に多数レースがある場合、`predict-nar-{runYmd}-{keibajoCode}-{raceBango}` のような DO 名は `max_instances: 10` を超過し得るため禁止する。

```mermaid
sequenceDiagram
    participant RT as sync-realtime-data
    participant D1 as D1<br/>(race_running_styles)
    participant RSN as Neon<br/>(running-style)
    participant Q as finish-position-predict-queue
    participant CRON as finish-position-cron<br/>POST /run fallback
    participant DO as Container DO
    participant DUCK as DuckDB
    participant FPN as Neon<br/>(finish-position)

    RT->>RT: feature generation / feature Parquet
    RT->>D1: running-style inference rows を write
    RT->>RSN: race_running_style_model_predictions へ mirror
    RT->>Q: focused per-race full message を enqueue<br/>mode=full, skipDedup=true
    RT-->>CRON: fallback POST /run<br/>queue binding absent
    CRON-->>Q: fallback enqueue
    Q->>DO: /predict?mode=full&category=nar&keibajoCode=35&raceBango=01&runDate=...
    DO->>DUCK: DuckDB feature build（--target-race 35:01）
    DUCK->>DO: v7 layers → 特徴量
    DO->>DO: CatBoost / XGBoost scoring
    DO->>FPN: race_finish_position_model_predictions へ UPSERT
```

ステップ詳細:

1. `sync-realtime-data` が当該 race の脚質 feature Parquet を読み、必要なら PostgreSQL / Hyperdrive から再構築する。
2. `RUNNING_STYLE_CELL_ROUTING_JSON` に基づき脚質モデルを選び、flatbin LightGBM で per-horse prediction を D1 へ write する。
3. D1 の completed state と Neon mirror count が期待頭数以上であり、`cell_model_key` / `cell_variant_id` と `predicted_corner_front_score` / `predicted_corner_rank` が両 store に保存済みであることを確認する。
4. `FINISH_POSITION_PREDICT_QUEUE` へ `mode=full` / `skipDedup=true` の per-race message を enqueue する。queue binding が無い環境だけ `FINISH_POSITION_CRON` service binding / API の `POST /run` に fallback する。
5. Container が DuckDB feature build（`--target-race 35:01`）→ v7 layers → CatBoost/XGBoost scoring → Neon UPSERT を実行する。

### 5.5 per-race rescore は Container 統一

JRA / NAR / Ban-ei の per-race rescore はすべて `finish-position-cron` から Container held `/predict` に渡す。Worker-native JRA scorer は production の model metadata / cell routing / feature contract と乖離しやすいため、queue consumer の production dispatch では使用しない。Container 側が `mode=rescore` と race scope を受け取り、同じ feature build / model routing / Neon UPSERT 経路で再 scoring する。

### 5.6 cron スケジュール（`finish-position-cron/wrangler.jsonc`）

| cron              | JST         | 用途                                                   |
| ----------------- | ----------- | ------------------------------------------------------ |
| `55 17 * * *`     | 02:55       | Neon pre-wake（NAR/Ban-ei）                            |
| `25 0 * * *`      | 09:25       | Neon pre-wake（JRA）                                   |
| `30 0 * * *`      | 09:30       | Cloudflare-side feature generation / per-race planning |
| `*/30 1-11 * * *` | 10:00-20:59 | レース時間帯の Neon keep-warm                          |
| `*/10 1-11 * * *` | 10:00-20:59 | per-race rescore coordinator                           |

`observability.head_sampling_rate: 0.1` を設定済み（請求最適化のため新規 Worker は必須）。

脚質予測は `sync-realtime-data/wrangler.jsonc` の `*/10 0-14 * * *`（JST 09:00-23:50）cron で planner が走る。前日 prewarm / 当日 backfill は daily feature generation と running-style planning を同じ順序で呼ぶ。

### 5.7 Docker / 永続化

- Cloudflare Container image: `apps/finish-position-predict-container/Dockerfile`（build context は repo root）。
- Container は予測結果を Neon の `race_finish_position_model_predictions` へ **UPSERT** で書き込む。
- Container の DuckDB base build は race scope ごとに `--temp-dir /tmp/predict-upcoming/duckdb-spill/{category}-{runDate}-{keibajoCode}-{raceBango}` を渡し、`target_race` 未指定時は末尾を `all` にする。DuckDB の `temp_directory` と table spill はこの run/race 専用ディレクトリに閉じ、並列 per-race full 生成で共有 `/tmp/duckdb-spill` を奪い合わない。
- 本番の authority は `sync-realtime-data` → `FINISH_POSITION_PREDICT_QUEUE` → `finish-position-cron` queue consumer → Container の Cloudflare path であり、Queue retry / DLQ が失敗時の再実行境界である。`FINISH_POSITION_CRON` service binding / API は queue binding が無い環境の fallback に限る。
- production は Cloudflare-only である。ローカル Docker / Python / trainer process は学習・検証・artifact 生成・手元再現用であり、本番の trigger、ordering、retry、fallback、model serving の依存先にしてはならない。local/manual 操作は、operator が明示したデータ修復・再同期に限り、本番 prediction / backfill authority として扱わない。
- Container は `PREDICT_SERVE_MODE=http` を Worker から明示して HTTP server mode で起動する。Dockerfile の ENV だけを本番挙動の根拠にしない。

### 5.8 環境変数・secrets

secret 値はドキュメントに記載しない。運用上必要な名前だけを明示する。

| 所有 Worker / component                      | 名前                                      | 用途                                                                                                               |
| -------------------------------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `sync-realtime-data`                         | `RUNNING_STYLE_D1_WRITE_ENABLED`          | 脚質 D1 write 有効化（`1`）                                                                                        |
| `sync-realtime-data`                         | `RUNNING_STYLE_CELL_ROUTING_JSON`         | 任意の脚質 cell routing config。未設定なら source 単位 latest model                                                |
| `sync-realtime-data`                         | `RUNNING_STYLE_MODELS`                    | 脚質 flatbin model / calibrator / feature Parquet 用 R2 binding                                                    |
| `sync-realtime-data`                         | `FEATURES_ARCHIVE`                        | R2 daily prediction Parquet（`running-style/predictions/by-day/...`）出力先                                        |
| `sync-realtime-data`                         | `FINISH_POSITION_PREDICT_QUEUE`           | `finish-position-predict-queue` producer binding。脚質完了後の per-race full trigger の primary path               |
| `sync-realtime-data`                         | `FINISH_POSITION_CRON`                    | `finish-position-cron` service binding。queue binding が無い環境の `POST /run` fallback と internal rescore API 用 |
| `sync-realtime-data`, `finish-position-cron` | `TRIGGER_TOKEN`                           | service binding / API fallback の `POST /run` / internal API 認証用。queue primary path では使わない               |
| `sync-realtime-data`                         | `DATABASE_URL_NEON`, `NEON_DATABASE_URL`  | 脚質 Neon mirror の writable PostgreSQL 接続先                                                                     |
| `sync-realtime-data`                         | `HYPERDRIVE`                              | feature read pool。production Hyperdrive は read-replica oriented                                                  |
| `finish-position-cron`                       | `NEON_DATABASE_URL`, `PREDICT_DAYS_AHEAD` | Neon warm / Container trigger                                                                                      |

脚質 Neon write は `getFinishPositionWritePool()` が `DATABASE_URL_NEON` → `NEON_DATABASE_URL` → Hyperdrive fallback の順に選ぶ。production Hyperdrive は read-replica oriented なので、writable secret が存在する環境で Hyperdrive を write path の第一候補にしてはならない。read pool は従来通り Hyperdrive first でよい。

### 5.9 運用確認チェックリスト

- D1 `daily_race_entries` / `realtime_race_sources` の対象日 race count が期待値を返す。
- 脚質 feature Parquet が R2 にある、または handler が `race_entry_corner_features` / PostgreSQL から再構築できる。
- `generate-running-style-predictions` の log に `cellModelKey` / `cellVariantId` / `neonWrittenCount` が出る。
- D1 `running_style_inference_state` が対象 race の planning / inference 完了状態を保持し、D1 `fetch_logs` に feature generation / running-style / finish-position trigger の成功または失敗 reason が残る。
- D1 `race_running_styles` と Neon `race_running_style_model_predictions` の件数が期待頭数以上であり、`cell_model_key` / `cell_variant_id` と `predicted_corner_front_score` / `predicted_corner_rank` が同じ routing / predicted corner 結果を保持している。
- `sync-realtime-data` から `FINISH_POSITION_PREDICT_QUEUE` へ `mode=full` / `skipDedup=true` / race scope を含む message が enqueue される。service binding / API fallback の場合だけ `finish-position-cron` への `POST /run` 成功を確認する。
- `finish-position-cron` の queue consumer が category-level `claimRun` / `completeRun` を通らず focused per-race full を Container に渡す。
- focused per-race full / rescore の Container DO 名が `predict-{category}` に集約され、race scope は `/predict` query の `keibajoCode` / `raceBango` にだけ残る。Cloudflare Containers の live instance が `max_instances` で詰まっていないことを確認する。
- Container NDJSON の final status が成功である。`status:error` は失敗として retry される。
- Neon `race_finish_position_model_predictions` に対象 race の着順予測が UPSERT される。

production verification evidence は、Cloudflare 側の D1 `daily_race_entries` / `running_style_inference_state` / `fetch_logs`、D1 `race_running_styles`、Neon `race_running_style_model_predictions`、Neon `race_finish_position_model_predictions` から取得する。手元 scheduler / ローカル Docker process の起動有無を本番完了の証跡にしてはならない。

2026-06-29 の本番 evidence として、`nar:20260629:35:01` の脚質 job は secret / write-pool 修正後に `cellModelKey` / `cellVariantId` と `neonWrittenCount=9` を記録した。着順 end-to-end は、Container log または `race_finish_position_model_predictions` の対象 race 行で確認できるまでは「脚質完了後 trigger まで確認済み」と保守的に扱う。

2026-07-01 の本番確認では、旧 `sync-realtime-data` 経路の D1 `race_running_styles` と Neon `race_running_style_model_predictions` に NAR 48 レース / 535 頭分の脚質予測が反映済みであることを確認した。一方、`sync-realtime-data-features-db` 側は `skeleton-disabled` 状態で、同日の脚質 feature / prediction は未生成だった。したがってこの日の evidence は「旧 `sync-realtime-data` D1 + Neon には反映済み、features-db 経路は未生成」として扱い、features-db を同日の本番生成成功証跡として数えない。

**2026-07-02 investigation: 着順 end-to-end が全カテゴリで未確認と確定 → 同日中に root-cause 特定・fix・JRA で live 確認済み（RESOLVED、残タスクは末尾参照）**

前回セッションが「`jra-cb-v9-sim-2013`（2026-06-26 deploy）で Neon `race_finish_position_model_predictions` の行が 0 件」とフラグした件を本セッションで徹底調査し、以下を確認した。

1. **「レースが無かった」ではない**: D1 `realtime_race_sources` で 06-26 deploy 後の JRA レース実施を確認済み（2026-06-27・2026-06-28、各日 3 場 × 12R = 72 レース、いずれも `result_complete_at` 設定済み）。ギャップは「レース未実施」では説明できない、実在する運用障害である。
2. **想定より遥かに広範囲**: Neon を直接 query したところ、`race_finish_position_model_predictions` の直近 30 日以上（テーブルの観測可能な履歴のほぼ全体、2026-05-15 以降）の row-group はすべて単一時刻・単一時間帯への書き込みクラスタという「one-off の research / backtest script 一括実行」の特徴を示していた。例えば NAR 本番モデル `iter12-nar-xgb-hpo-v8` は 2026-07-01 に 8 レース分 94 行が 10 秒以内に書き込まれており、同じ狭い時間窓に `iter30-nar-cb-ensemble-*` / `iter36-nar-lgb-ensemble-C-v8` という当時の per-class model_version も同居していた。テーブル全履歴中、書き込みが 4 時間帯以上に分散している日は 2026-05-17 の 1 日のみで、その日も 150 万行という規模から多時間にわたる historical bulk load であり、per-race serving ではない。**結論: 本テーブルが可視化する期間（1.5 ヶ月以上）を通じて、live per-race 本番パイプライン（`sync-realtime-data` → `FINISH_POSITION_PREDICT_QUEUE` → `finish-position-cron` queue consumer → Container `/predict` → Neon UPSERT）が JRA / NAR / Ban-ei のいずれか 1 カテゴリでも genuine な live prediction を完走・書き込みした確証は無い。** Ban-ei 本番モデル `banei-cb-v9-sim-2011` も JRA 同様に行数ゼロのままである。 **（2026-07-02 later 訂正、下記「starvation fix 後の low-contention smoke test」節 item 19 参照）**: この解釈のうち「`iter30-nar-cb-ensemble-*` / `iter36-nar-lgb-ensemble-C-v8` は明らかに非本番の model version」「bulk/eval パターン」という判断は、当時の per-class route を前提にすると誤認だった可能性が高い。ただし per-class route は現行 production では廃止され、以後の authority は per-cell / category default である。
3. **根本原因、live smoke test で確認**: 実際の `finish-position-cron` Worker `/run` エンドポイント（`https://finish-position-cron.kaoru.workers.dev/run`）へ focused per-race full message（`category=jra, runDate=20260628, keibajoCode=02, raceBango=01, mode=full, skipDedup=true`）を POST し、Cloudflare GraphQL Analytics API（`finish-position-predict-queue` の `queueMessageOperationsAdaptiveGroups`）で message の生涯を追跡した。`WriteMessage`（enqueue 成功）→ queue consumer による `ReadMessage` → **ack/error 一切無いまま約 17 分後に同一 message が再度 `ReadMessage`**（consumer の held `stub.fetch()` が完了せず、Cloudflare が無言で再配送）→ 約 15 分後に 3 回目の read → `max_retries: 3` を使い切り `DeleteMessage outcome=dlq`（合計約 32 分、Neon 行は一度も書かれず）。同時刻の `wrangler containers info` は failed instance 0・エラー無しで、application-level 例外ではなく Cloudflare プラットフォーム側が held queue-consumer invocation を外部から timeout/kill している挙動と整合する。同じ read/retry/DLQ の約 15-17 分サイクルは、このテスト以前（2026-07-01T15:53 UTC 以降）の同一 queue でも 6 時間以上繰り返し観測されており、テスト message 固有の問題ではなく既存の systemic issue である。
4. **推定される寄与要因（未確証）**: JRA の per-race `full` DuckDB layer chain（`apps/finish-position-predict-container/src/predict_lib/pipeline_args.py::LAYER_CHAIN["jra"]`）は 16 個の逐次 subprocess まで増えており、各々が DuckDB postgres extension 経由で個別に Neon へ接続する。直近の `KOHAN3F_GOING_SCRIPT` / `SIMILAR_RACE_SCRIPT` / `SIRE_VENUE_BIAS_SCRIPT`（2026-06-26 v9-sim deploy）で層が追加され続けてきた。`SIMILAR_RACE_SCRIPT` を持つ Ban-ei（7 layers）も同じ「行数ゼロのまま」の症状を示す。`apps/finish-position-predict-container/src/predict_lib/serve.py` のコメントは「end-to-end 3-8 分」と記載しているが、現在のレイヤ数と Cloudflare 側の実際の queue-consumer processing-duration 上限（今回の read/retry cadence から見て実質 15-17 分程度）に対して、この見積もりは stale である可能性が高い。
5. **本セッションで安全な mitigation を 2 件 deploy 済み（commit `db41b6fd` "fix(finish-position): raise queue max_concurrency + pre-cache DuckDB postgres ext"）**:
   - `apps/finish-position-cron/wrangler.jsonc`: queue consumer の `max_concurrency` を `1` → `3` に引き上げ。DO 名は commit `09dd0755` 以降 `predict-{category}` に集約済みのため、JRA/Ban-ei の遅い/hung なパイプラインが NAR の message を塞き止めなくなる（3 カテゴリ同時でも `max_instances: 10` を十分下回る）。
   - `apps/finish-position-predict-container/Dockerfile`: DuckDB `postgres` extension を image build 時に事前インストールし、cold start ごとの回避可能な network fetch を 1 件削減。
   - 両方とも本番で確認済み（新規 Worker deploy version `017198a3`、新規 Container image tag `017198a3`、`wrangler queues consumer list` で `max_concurrency: 3` を確認）。
6. **これらの修正では根本問題は解決していない**: 修正 deploy 後に同じ live smoke test を再実行（同一 `/run` trigger、`keibajoCode=03, raceBango=01`、同日付）したところ、T+0 read → 約 15 分後に再配送 → 約 30 分後に再配送（`max_retries: 3` により DLQ）という**同一の失敗パターンが再現**した。したがって **JRA（および恐らく Ban-ei）の per-race full パイプラインが Cloudflare の queue-consumer processing window を超過している根本原因は未解決のまま**であり、専用のパフォーマンスプロファイリングが必要である。本セッションでは Container レベルのログ / SSH アクセスが得られなかった（`wrangler containers ssh` は "Web socket error: Unexpected server response: 400" で失敗、`wrangler tail` は親 Worker の DO "Alarm" housekeeping イベントのみを表示し `/predict` 処理の詳細は出ない——`head_sampling_rate: 0.1` が大半の trace event を drop している可能性が高い。Cloudflare dashboard は対話的ログインが必要で本セッションでは完了できなかった）。
   - **推奨フォローアップ**: (a) Container-level ログ可視化を得る（dashboard access、または `pipeline_runner.py` が既に持つ per-layer `step=layer index=.../16 status=done ... elapsed_seconds=...` の timing log を、Worker が `console.log` / `wrangler tail` 経由で既に surface している NDJSON progress stream に載せ、dashboard 無しでボトルネック layer を特定できるようにする）。(b) ボトルネック layer を特定した上で最適化するか、Container の実際の予測完了と Cloudflare Queue の processing-time window を分離する方向でパイプラインを再構成する。
7. **ユーザー影響は無症状（silent degradation）**: `apps/pc-keiba-viewer` は `race_finish_position_model_predictions` を直接読み（`src/db/queries.ts::getFinishPositionLambdarankPredictions`、~2895 行）、行が存在しない場合は orphan な未使用テーブル `race_entry_finish_model_predictions`（repo 内に writer が存在しない）へ fallback し、それも無ければ gracefully degrade する。`apps/pc-keiba-viewer/src/lib/finish-position-prediction.ts` の `getModelCandidates`（~525 行）は model-prediction 行が 0 件の場合、model score の寄与（通常 `modelWeight` ≈ 0.06〜0.08 の小さな重み）を単純に省略し、表示される予測は odds / popularity / jockey / trainer / same-day-jockey シグナルのみの heuristic blend になる。そのため、この観測可能な期間全体を通じて、エンドユーザーは可視エラー無しに model-informed でない劣化した予測を見せられていた——これが今回の調査まで本ギャップが検出されなかった理由である。
8. **根本原因を確定（同日中、commit `2d3535be` の一時計測で確認）**: 上記 4 の推測（layer 数増加が寄与）を、Neon 上の一時テーブル `_debug_finish_position_layer_timing`（writer は `apps/finish-position-predict-container/src/pipeline_runner.py::record_layer_timing_row`）による live per-layer 実測で定量的に確定した。実レース `jra:2026:0628:10:01` で **JRA は base build 96 秒 + 16 layers 累計 1639.6 秒（合計 約 27.5 分）**、NAR は 10 layers 累計 989.8 秒（**約 16.5 分**）を要していた。これは queue consumer が held `stub.fetch()` を維持できる実質上限（read/redelivery/DLQ cadence から観測、約 15-17 分）を確実に超える。パイプライン自体は壊れても hang してもおらず、**Cloudflare が単一の held invocation に許す時間より単純に遅い**ことが、これまで genuine な書き込みが一度も無かった理由だと確定した。
9. **Fix を deploy 済み（commit `af1ca40e` "fix(finish-position): decouple queue consumer hold time from pipeline completion"）**: focused per-race full request に限り、Container `/predict`（`apps/finish-position-predict-container/src/predict_lib/serve.py`）はパイプラインをバックグラウンドスレッドで起動し、即座（秒単位）に NDJSON `status="accepted"` を返して `stub.fetch()` を release する。queue consumer（`apps/finish-position-cron/src/queue-consumer.ts`）は `"accepted"` を受け取ると ack/error にせず `delaySeconds: 150` で再配送し、既存の Neon ベース完了チェック `isFocusedFullPredictionComplete()`（`apps/finish-position-cron/src/focused-full-completion.ts`）を再配送ごとの completion poll として再利用する。`apps/finish-position-cron/wrangler.jsonc` の consumer `max_retries` を `3` → `12` に引き上げ（150 秒 × 12 ≈ 30 分の総 retry 予算、実測 JRA 最悪ケース約 27.5 分を余裕を持って包含）。`container-class.ts` の `sleepAfter` を `30s` → `30m` に引き上げ、Container idle-timeout でバックグラウンドスレッドが kill されないようにした。同一 container process 内で 2 つの focused-full パイプラインが互いの DuckDB work directory（category 単位でキーされ race 単位ではない）を壊さないよう、`serve.py` に process-scoped の single-slot guard を追加した。
10. **Live production smoke test で JRA の genuine 書き込みを確認**（本セッション、deploy 後）: 実際の `finish-position-cron` Worker `POST /run` で JRA `keibajoCode=10, raceBango=01, runDate=20260628` を trigger し、Neon を直接 query して確認した。**`race_finish_position_model_predictions` に本番モデル `model_version='jra-cb-v9-sim-2013'` の行が 18 件**（この model の genuine な書き込みとして初確認）、`prediction_generated_at=2026-07-02T00:09:55Z`（16 番目・最終 layer 完了の約 3 秒後）で存在し、`predicted_score` / `predicted_rank` / `umaban` は妥当な順位付き値だった。NAR / Ban-ei はアーキテクチャ上同じ fix が適用されるが、本セッションでは fix 後の clean smoke test で個別に再確認できていない（`keibajoCode=02, raceBango=02` の 2 本目の smoke test は本タスク時点で layer 1/16 まで進行中だった）。
11. **既知の残存リスク（未解決、フォローアップ推奨）**: 既に完了済みのレースに対する重複・overlap した再配送が、container に到達して layer 途中で失敗するケースを 1 件観測した（同一レース `10:01` の 2 回目の attempt が、1 回目の最終 layer 完了の約 9 秒後に開始し layer 3/16 で失敗）。これは無駄な再処理であり正しさを損なうものではない——1 回目の結果は既に Neon に書かれており、`isFocusedFullPredictionComplete()` の pre-check が Neon の完了を反映すれば以降の再配送は fast-ack されるはずである（実際、既に完了済みレースへの再配送で `DeleteMessage outcome=success` を 2 秒未満で観測済み）。ただし現在の guard は per-process（race-scoped ではない）ため、重複起動を防ぎきれない window が残る。**推奨フォローアップ**: guard を race-scoped にする、または新規 launch 前に completion check を挟む。（2026-07-02 中の追加検証で、このリスクのより深刻な実例が実測された。下記 item 13 を参照。）
12. **Cleanup TODO（未実施、次セッション）**: 本 investigation で導入した一時計測 `_debug_finish_position_layer_timing`（Neon テーブル）と writer `record_layer_timing_row`（`apps/finish-position-predict-container/src/pipeline_runner.py`、commit `2d3535be`）は root-cause 特定・fix 確認に不可欠だったが、diagnostic-only の一時コードであり、次回のクリーンアップ session で削除すること。本 docs-only session では削除しない。

**2026-07-02 追加検証（RESOLVED後のfollow-up）: 同カテゴリ同時実行下でのstarvation/DLQを実測確認**

上記 RESOLVED 更新の直後、NAR の独立確認を得る目的で orchestrating session がさらに 2 本の live smoke test を実行したところ、item 11 で「無駄な再処理、正しさを損なわない」とした残存リスクの、より深刻な実例を実測した。**これは item 1〜10 の RESOLVED 判定を覆すものではない**（元の JRA 確認は無傷であり、以下の通りむしろ本ラウンドで独立に再確認された）が、重要な nuance として追記する。

13. **JRA 2 本目の smoke test で観測した starvation → DLQ**（`keibajoCode=02, raceBango=02, runDate=20260628`、trigger ~2026-07-02T00:17:41Z、`skipDedup=true`）: message は accept され、`_debug_finish_position_layer_timing` で base build + layer 1〜3 までの進行を確認した（00:28:35Z 時点で layer 3/16）ところで進行が停止した。原因はライブで特定できた——同時刻、既に完了済みのレース `keibajoCode=10, raceBango=01`（item 10 の元の RESOLVED 確認と同一レース）に対する別の重複・冗長な再配送が 3 本目のパイプライン実行（run id `078b0a01`）を起動し、00:46Z 頃から `predict-jra` Container の process-scoped single-slot guard（commit `af1ca40e` の設計、item 9 参照——「a single per-process guard ... prevents two concurrent focused-full pipelines in the same container process ... a race whose slot is already claimed still gets 'accepted' without launching a second thread」）を約 27 分間（layer 4→16、00:46Z〜01:07:08Z）占有し続けた。この 3 本目の run は正常完了し、`jra-cb-v9-sim-2013` の同じ 18 行を race `10:01` に対して idempotent に再 UPSERT した（timestamp は `01:07:08Z` に更新、重複行は発生せず）——**これは元の RESOLVED 確認の独立した 2 回目の成功完走であり、fix のコアメカニズムが機能していることを裏づける追加の good evidence である**。しかし競合する run がスロットを占有し続けた結果、2 本目の smoke test 自身の message は redelivery のたびに "accepted" を受け取り続けたが（技術的には正しい——container 内で何かが in-flight ではある）、自身のバックグラウンドスレッドは 00:28:35Z の layer 3/16 から一度も再開しなかった。Cloudflare Queue GraphQL analytics で `DeleteMessage outcome=dlq` が `2026-07-02T00:59:14Z` に確認され、これは `max_retries=12 × delaySeconds=150s = 30 分` の retry 予算が最初の accept（~00:23:52Z）から使い切られたタイミングと整合する。**この message は race `02:02` に対して実質的なワークを一切行わないまま dead-letter され、`race_finish_position_model_predictions` にはこのレースの行は一度も書かれていない。**

14. **NAR の smoke test で観測したゼロ進行 → timeout**（`keibajoCode=35, raceBango=02, runDate=20260630`、trigger ~2026-07-02T00:43:58Z、`skipDedup=true`、`category=nar`）: `_debug_finish_position_layer_timing` に `nar:20260630:35:02:*` パターンの行が一度も現れず、このレースのバックグラウンドパイプラインは一度も開始しなかった。`predict-nar` Container のスロットは、本テストとは無関係に本番システム上で並行して動いていたと見られる他の NAR レース（`keibajoCode` 30/43/44/48/50 等、複数レース番号——恐らく無関係な同時活動 / testing）の処理で占有され続けていた。`race_finish_position_model_predictions` を対象レースについて 25 分間 poll したが行は 0 件のままタイムアウトした。Cloudflare Queue GraphQL analytics では `DeleteMessage outcome=dlq` が `01:01:32Z` / `01:04:21Z` / `01:06:52Z` / `01:06:56Z` の 4 件観測された——このうち少なくとも 1 件はこの NAR test message 自身の 30 分 retry 予算切れ（accept-to-DLQ window は概算 00:44Z〜01:14Z）である可能性が高いが、per-message tracing の tooling が本ラウンドには無く、4 件中どれが該当するかを確実に特定できていない。

15. **残存リスクの是正された理解**: item 11 は当初この現象を「無駄な再処理、正しさを損なわない」と記述したが、本ラウンドの実測により、より深刻な実態が判明した。**同一カテゴリ（jra / nar / ban-ei のいずれか）内で複数レースの message が同時期に in-flight になっている場合——それが genuine な同時開催レースであれ、冗長な再トリガーであれ、（本ラウンドで実際に起きたように）orchestrator 自身の短時間内での連続 manual smoke test によって作り出された人為的な競合であれ——per-process の single-slot guard により、1 カテゴリにつき同時に実質進行できるレースは 1 件のみである。他のレースの message は "accepted" を受け取り続けるだけで自身のワークが一度も開始されず、30 分 / 12 retry の予算を丸ごと空費して dead-letter され、そのレースに対する予測行は一切書き込まれない。**

16. **自己誘発負荷の留保（正直に記載する）**: 今回観測された failure mode は、orchestrator 自身のテスト手順——JRA / NAR 合わせて 3 件以上の異なるレースを ~30 分の窓内でトリガーしたこと、加えて fix deploy 前から残っていた古い message が新アーキテクチャ下で retry を継続していた可能性——によって強く助長された可能性が高く、post time が数分〜数時間離れている通常の本番トラフィックを必ずしも代表するものではない。**しかしこれは再現可能な genuine architectural gap である**: 同一カテゴリの 2 レース以上の per-race full-prediction window が実際に overlap する日（JRA の連続開催が詰まった日、または catch-up / backfill シナリオで複数レースを一括 enqueue する場合等）は、スロットを取れなかった側の message が同じ starvation → DLQ の failure を示し得る。fix 前との比較では regression ではない点も明記する: fix 前は 100% のレースがこの failure mode（プラットフォームのタイムアウトのみで即座に失敗）を示していた。fix 後は、競合ウィンドウごとに少なくとも 1 レースは成功し、DLQ が可視シグナルとして利用可能になった（以前の「行が永遠にゼロのまま」という silent failure ではない）。とはいえこれは capacity / fairness の gap として残っており、**次の優先フォローアップとすべきであり、closed とはみなさない**。

    **推奨フォローアップ（次の優先事項、未実装）**: (a) guard を process-scoped ではなく race-scoped にする（item 11 の推奨と同一方向性だが、本ラウンドはこれが単なる「重複起動防止」だけでなく「他レースの starvation 防止」にも必要であることを示した）、(b) カテゴリごとに複数 Container instance を許容し同時実行可能なレース数を増やす、(c) queue に priority / fairness 機構を導入し、同じスロットを長時間待っている message を優先的に処理する——のいずれか、または組合せを検討する。

17. **NAR 単独・低競合 smoke test を追加実施（同日中の別セッション）: 10/10 layer 完走もNeon書き込み未確認のまま時間切れ（当初 未 CLOSED → work-dir cleanup fix `c3a48694` で CLOSED、末尾節 item 24-26 参照）**: item 13/14 の starvation を避けるため、当日の本番トラフィックと重ならない過去レース（`category=nar, runDate=20260629, keibajoCode=44, raceBango=03`、当日 2026-07-02 の日次バッチ対象外の日付を意図的に選択）を単発でのみ trigger した（`POST /run`、`skipDedup=true`、trigger at `2026-07-02T01:36:48Z`）。`_debug_finish_position_layer_timing` で追跡した結果、`run_id=nar:20260629:44:03:59666aee` は他レースとの競合が一切無く、10 layer 全てが単調に完了した（layer0/base build done `01:39:06Z` → layer10 done `01:55:09Z`、合計約 16 分、既存の NAR ~16.5 分実測と整合。stall や他 race による slot 占有は観測されなかった——item 13/14 の starvation とは異なるクリーンな実行)。**しかし** layer10 完了から約 2 分 35 秒後（`01:57:44Z` 時点、本タスクの time-budget 上の最終確認）まで `race_finish_position_model_predictions` に対象レースの行は 1 件も現れず、同一時間帯（`prediction_generated_at > 2026-07-02T01:36:00Z`）でテーブル全体を見ても他のカテゴリ・レースを含め書き込みは 0 件だった。JRA の確認済みケース（item 10）では最終 layer 完了から Neon 書き込みまで約 3 秒だったのに対し、今回は scoring（CatBoost/XGBoost）+ Neon UPSERT のステップ自体が `_debug_finish_position_layer_timing` の計測範囲外であり、そこで長めの時間を要しているのか、無言で失敗しているのかを本セッションの time budget 内では切り分けられなかった。**結論**: 同一カテゴリの concurrency starvation（item 13-16）は本テストでは再現しなかった（fix のコアメカニズムが競合さえなければ NAR でも 10 layer を完走させられることの追加傍証にはなる）が、NAR の genuine な Neon 書き込みは本ドキュメント時点でも独立に確認できていない。**推奨フォローアップ**: (a) `record_layer_timing_row` の計測を scoring/UPSERT ステップまで拡張し、layer 完了後の空白を可視化する、(b) 同じレース（`nar:20260629:44:03`）に対して十分な時間（30 分超）を空けて再度 poll し、単に書き込みが遅延していただけなのか確認する、(c) `wrangler tail` や Cloudflare Queue GraphQL analytics で同message のその後の redelivery / DLQ 有無を追う。 **（2026-07-02 RESOLVED、下記「starvation fix 後の low-contention smoke test」節 item 19 参照）**: 本 item が「未確認」とした NAR の genuine Neon 書き込みは、starvation fix（commit `f4b3ea7a`）後の clean 過去日（2026-06-27）low-contention smoke で確認された（NAR `54:03` → `model_version='iter36-nar-lgb-ensemble-C-v8'` の 10 行、layer10/10 完了の約 4 秒後 `2026-07-02T06:27:57Z`）。「layer 完了後の空白が単なる遅延か無言失敗か」という切り分けは、十分な time budget があれば genuine に書き込まれると判明したため RESOLVED（今回の 54:03 では layer 完了から 4 秒で書き込み）。 **（2026-07-02 追訂・CLOSED、末尾「item-17 CLOSED」節 item 24-26 参照）**: この「十分な time budget があれば書き込まれる」という切り分けは不完全だった。本 item の `nar:20260629:44:03` が「layer 完走・書き込みゼロ」となった真因は time budget ではなく、`pipeline_runner.py::build_pipeline` の work dir がカテゴリ単位でキーされ per-race cleanup されず、1 プロセスが同カテゴリ複数レースを逐次処理する際に 2 本目以降のレースの最終 `rename(final_dir)` が既存 dir により ENOTEMPTY 失敗する bug（全 layer が done を出した後に落ちるため item 17 の症状と完全一致）だった。commit `c3a48694` で per-race work-dir reset を入れて修正・deploy 済み、同カテゴリ 2 レース連続 re-smoke（`55:05` / `55:06`）で clean write を確認して CLOSED。

**2026-07-02 最終確認（starvation fix `f4b3ea7a` 後の low-contention smoke test）: NAR / Ban-ei の genuine per-race 書き込みを確認 → 3 カテゴリすべて RESOLVED + starvation fix の end-to-end 検証成功**

item 13-16 で確定した same-category concurrency starvation に対し starvation fix を deploy した後、当日の本番トラフィックと重ならない clean 過去日（`race_entry_corner_features` の揃った 2026-06-27）で低競合の smoke test を実施し、以下を確認した。

18. **starvation fix を deploy 済み（commit `f4b3ea7a` "fix(finish-position): stop same-category slot starvation (busy re-enqueue + already-complete guard)"、Worker version `59aaeeba-3262-4dbd-b789-1760ed9b9d86`、Container image tag `59aaeeba`）**: item 13-16 の failure mode（process-scoped single-slot guard により 1 カテゴリにつき同時進行できるレースが 1 件のみで、slot を取れない message は "accepted" を受け取り続けて 30 分予算を空費し DLQ される）に対し、(a) slot が busy の間は message を error にせず **busy re-enqueue** して budget 内で待機させ空き次第 slot を獲得、(b) 既に完了済みのレースへの再配送は launch 前の **already-complete guard** で fast-ack する、の 2 点で対処した。
19. **NAR の genuine per-race 書き込みを初確認**: NAR `54:03` に対し `race_finish_position_model_predictions` へ当時の `model_version='iter36-nar-lgb-ensemble-C-v8'` の 10 行、`prediction_generated_at=2026-07-02T06:27:57Z`（layer10/10 完了の約 4 秒後）を確認した。**履歴上の訂正（item 2 の解釈を更新）**: item 2 が `iter30-nar-cb-ensemble-*` / `iter36-nar-lgb-ensemble-C-v8` を「明らかに非本番の model version」「bulk/eval パターン」と解釈したのは、当時の per-class route を前提にすると誤認だった可能性が高い。現在は per-class route を production dispatch から外しているため、以後の NAR smoke / completion 判定は expected model_version を per-cell / category default から解決する。
20. **Ban-ei の genuine per-race 書き込みを史上初確認**: 同 smoke で Ban-ei `83:03` に対し `model_version='banei-cb-v9-sim-2011'`（期待どおりの本番モデル）の 8 行、`prediction_generated_at=2026-07-02T06:05:38Z`（layer7/7 完了の約 3 秒後）を確認した。テーブル観測可能履歴で Ban-ei 本番モデルの genuine per-race 書き込みが確認されたのは初めてであり、item 2 で「行数ゼロのまま」としていた状態が解消された。
21. **starvation fix の end-to-end 検証成功**: 同一 slot に `54:03` / `54:04` を約 2 秒差で投入したところ、`54:04` は slot が busy のため待機し（fix 前ならこの message は 30 分の accepted 空費 → DLQ となっていたケース）、`54:03` 完走後の `2026-07-02T06:30:22Z` に起動して layer 8/10 まで進行を確認した（完走確認は追ってのフォローアップ）。fix 前の 100% failure（platform timeout で即失敗）や item 13-16 の starvation → DLQ とは異なり、busy re-enqueue が「待機 → slot 獲得 → 進行」のシーケンスを実地で成立させることを確認した。
22. **既知の残存リスク（フォローアップ推奨）**: (a) busy budget（`MAX_BUSY_REQUEUES=40 × 60s = 40 分`）は cold-start transient が多発する状況ではタイト——上記 `54:04` は約 40 分待機してギリギリ slot を獲得した。この budget は commit `b6f7500e` "raise busy-slot budget MAX_BUSY_REQUEUES 40->80"（Worker `97a29f75`）で `40 → 80`（× 60s = 80 分）に引き上げ済みで、残る緩和候補は cold-start warm-up / カテゴリ別同時実行数の増加。(b) 新 image の初回 `/predict` に connection 起因の cold-start transient（初回失敗、retry で回復）が残る。
23. **補足 commit（deploy 済み）**: `50a86c78` "feat(finish-position): warm viewer cache after focused per-race full completion"（focused-full ack の 3 経路で viewer cache を warm する）は当初「未 deploy」としていたが、その後の `finish-position-cron` deploy（Worker `97a29f75`）で本番反映済みである。

**2026-07-02 item-17 CLOSED（per-race work-dir cleanup fix `c3a48694`）: 同カテゴリ複数レース逐次処理で 2 本目以降が書き込みゼロになる真因を特定・修正・end-to-end 確認**

item 17 が「10/10 layer 完走も Neon 書き込み未確認」とした症状（および item 21 で「完走確認は追ってのフォローアップ」とした `54:04`）の真因は time budget ではなく、starvation fix 後の新しい実行構成が露出させた work-dir の per-race cleanup 欠落だった。

24. **root cause 確定**: `apps/finish-position-predict-container/src/pipeline_runner.py::build_pipeline` が使う work dir（`feat-{category}-base` / `-layer-N` / `-v7-final`）はカテゴリ単位でキーされ、レース間で cleanup されていなかった。starvation fix（`f4b3ea7a` の busy re-enqueue）以降は「1 つの long-lived Container プロセスが同カテゴリの複数レースを逐次処理する」構成が常態化したため、2 本目以降のレースの最終 `current.rename(final_dir)` が既存 dir により ENOTEMPTY で失敗する。この失敗は全 layer が `done` を出した**後**に起きるため、`_debug_finish_position_layer_timing` 上は 10/10 完走に見えつつ Neon には 1 行も書かれないという、item 17 の症状（「layer 完走・書き込みゼロ」）と完全に一致する。1 プロセスの最初のレースだけが書けていたことも実測と整合する（`54:03` / `83:03` は書けて、`54:04` / `55:03` / `nar:20260629:44:03` は書けなかった）。duckdb-spill dir は既にレース単位で分離済みのため cleanup 対象外。
25. **fix を deploy 済み（commit `c3a48694` "reset category work dirs per race to unblock 2nd+ race writes"）**: `build_pipeline` 冒頭で対象 work dir を `shutil.rmtree` により per-race リセットしてから materialize する。Container image tag `ec62072a` + Worker Version `ec62072a-bd92-4c7b-9d18-d8e2df781cc2` で deploy 済み。本日の finish-position 関連 deploy を時系列で整理すると: `f4b3ea7a`（same-category slot starvation fix、Worker `59aaeeba`）→ `50a86c78`（focused-full 完了後の viewer cache warm、Worker `97a29f75`）→ `b6f7500e`（busy budget `MAX_BUSY_REQUEUES=40 → 80`、Worker `97a29f75`）→ `c3a48694`（per-race work-dir cleanup、image `ec62072a` + Worker `ec62072a-bd92`）。
26. **CLOSED 証跡（同カテゴリ 2 レース連続 re-smoke、同一 `predict-nar` プロセス）**: fix 後に同一 predict-nar プロセスで NAR 2 レースを連続処理させ、両方が clean write することを確認した——`55:05`（1 本目）は `model_version='iter12-nar-xgb-hpo-v8'` の 9 行を `2026-07-02T09:19:05Z`（layer10/10 完了の約 5 秒後）に、**`55:06`（2 本目）は当時の per-class 廃止前 path による `model_version='iter36-nar-lgb-ensemble-C-v8'` の 12 行を `2026-07-02T09:40:17Z`（layer10/10 完了の約 3 秒後）に**書き込んだ。これは同カテゴリ複数レース逐次処理の修正証跡であり、現行 selection authority ではない。fix 前は 2 本目が必ず書き込みゼロ（例: item 17 / item 21 の `54:04` は layer を 2 回完走しても 0 行）だったのに対し、retry 不要の単発完走で clean write が成立した。これにより item 17 / item 21 の残タスクは CLOSED、§2 冒頭のとおり **JRA（`10:01`）/ NAR（`54:03` / `55:05` / `55:06`）/ Ban-ei（`83:03`）の 3 カテゴリすべてで genuine per-race 書き込み + 同カテゴリ複数レース逐次処理の正常化を確認済み**。**rollback**: image を `59aaeeba` に戻す（`git revert c3a48694` → `finish-position-cron` deploy）。残存リスク（低）は cold-start transient（初回 `/predict` 失敗、queue retry で回復、warm-up は今後検討）と busy budget のタイトさのみで、budget は `b6f7500e` で 80 分に強化済み。さらに同日 19:47 JST までに、当時の serving image `ec62072a` 上で JRA（`jra:2026:0627:02:01`、16 行、`jra-cb-v9-sim-2013`、prediction_generated_at=10:35:56Z、16/16 layer 完了直後）と Ban-ei（`83:04`、10 行、`banei-cb-v9-sim-2011`、10:16:33Z、7/7 layer）の genuine write も確認し、**3 カテゴリ全てが Cloudflare per-race serving で検証済み**となった。

**2026-07-03 NAR per-cell routing: production Neon row confirmation**

Cloudflare production path の NAR focused per-race run（`nar:20260703:54:10`）で、`cell_routing.json` の NAR `dirt / mile / E / summer / venue 54` variant が category-level path より先に選ばれることを Neon write で確認した。`race_finish_position_model_predictions` には `model_version='nar-xgb-cell-a957d8b4-v1'` として `2026-07-04 54:10` が 11 行（`prediction_generated_at=2026-07-02T17:20:38Z`）、`2026-07-05 54:10` が 8 行（`prediction_generated_at=2026-07-02T17:20:39Z`）landing している。元 RA は `grade_code=E`、距離 1300 / 1400、`track_code=24` で、NAR router では dirt / mile / summer / venue54 に解決される。この証跡は CF / Neon based であり、Mac launchd / local wrapper には依存しない。

**2026-07-03 running-style predicted-corner persistence: production LIVE**

`sync-realtime-data` は Worker Version `a48c187d-1d2f-487b-8eb4-37fe3b1ec5b0` で running-style の `predicted_corner_front_score` / `predicted_corner_rank` 永続化を production deploy 済み。D1 remote schema は `race_running_styles.predicted_corner_front_score` / `predicted_corner_rank` の存在を `pragma table_info` で確認済み（`0032_running_style_predicted_corner.sql` は remote で適用済みのため `d1:migrate` は no-op）。当日 NAR `2026-07-03` の既存 48 race / 499 rows は、旧 488 null rows を D1 上で `p_senkou + 2*p_sashi + 3*p_oikomi`、`p_nige desc`、`ketto_toroku_bango asc`、`horse_number asc` の race 内 `row_number()` で補完し、D1 は 499/499 rows で score/rank non-null になった。その後 `sync-running-style-d1-to-neon.ts --from-date 20260703 --to-date 20260703` で D1→Neon 499 rows を upsert し、Neon `race_running_style_model_predictions` でも `source=nar`、48 race / 499 rows すべてで score/rank non-null を確認した。sample `nar:20260703:43:12` は Neon 上で rank 1..11 が score 昇順に並ぶ。completed 済み旧行は `listRaceRunningStylesForRace` が score/rank を導出し、`cacheAndSyncCompletedRunningStyles` が D1 へ再 UPSERT してから viewer cache / Neon sync を行うため、旧 null 行も再同期時に補完される。

**2026-07-03 NAR Set-Transformer blend: production LIVE（iter40、serve-exact gate ADOPT 済み、Neon-write smoke CLEAN → env=1 有効化 → 初回 genuine 本番 write 確認済み）**

§11 で「genuine win だが serve-path parity を閉じる段階（当時 DEPLOY IN PROGRESS）」としていた NAR Set-Transformer × XGBoost rank-fusion blend を、serve-exact な numpy scorer で parity を閉じ本番 image へ deploy し、full 本番 Neon-write smoke を CLEAN で通した上で **Cloudflare Worker / Queue / Container の production path で env=1 有効化済み**。**NAR serving は iter40 transformer blend に切替済み**。2026-07-03 に初回 genuine 本番 write を確認し **production LIVE**——Cloudflare production path の per-race full serve が 48 NAR レース（499 頭行）を iter40 で本番 Neon に書き込み、viewer は当日 48 NAR レース全てで iter40 を表示（下記 item 30 参照）。モデル仕様は §2.5、science / 統制 / 機構は §11 を参照。

27. **deploy 済み ＋ CF production path ENABLED**: transformer コード + R2 artifact を bake した本番 image を deploy 済み（base image は flag default=False を bake）。その上で有効化を実施: **CF worker** = wrangler **secret** `NAR_TRANSFORMER_BLEND_ENABLED=1`（redeploy 耐性の drop-proof、live-rollbackable）＋ `container-class.ts` の env forward（commit `28c89abf`）＋ worker deploy Version `0d2d99c7-cbe1-4989-a497-255e73e77388`（container image 再利用、crons / queue / observability（`head_sampling_rate: 0.1`）は intact）。ローカル wrapper は診断・backfill 用に限定し、本番 authority / 本番 write path としては数えない。viewer は latest-for-race（priority-3）で自動的に iter40 行を拾う（下記 item 28）。
28. **viewer 表示（iter40 表示確認済み）**: finish-position query の priority-3（latest-for-race catch-all、`order by priority, recency desc`）機構は unchanged で、iter40 の書き込みが始まれば同じ priority-3 機構が iter40 の行を latest-for-race として自動的に表示する（active_models の flip は不要）。2026-07-03 の初回 genuine 本番 write（Cloudflare production path、48 NAR レース）以降、viewer は当日 48 NAR レース全てで iter40 行を latest-for-race として表示している（iter40 が 48/48 で勝ち）。pre-enablement の古い iter12（2 レース）/ iter36（3 レース）行は partial write で superseded。
29. **検証済み（enable 前の証跡 ＋ Neon-write smoke）**: (a) serve-exact（ketto tie-break）gate ADOPT（top1 +0.629[LB95 +0.467] 他）、(b) deployed module が canonical scorer に byte-exact（max diff 0、順位 flip 0）、(c) wiring smoke——real image で NAR → iter40 routing、`predicted_score` が `fuse_ensemble_transformer` に bit-exact、両 fallback tier（field<2 / feature 欠損）→ iter12 ensemble を確認、(d) **full 本番 Neon-write smoke CLEAN**——env=1 で NAR `55:03` を trigger し、本番 Neon `race_finish_position_model_predictions` に iter40 の 11 行が landing（generated 01:55 JST、55:03Z 頃 write）、うち 5/11 頭が iter12 と reorder（transformer fusion が place を refine している＝ iter12 と別出力であることを確認）、fallback → iter12 も確認。direct prod-Neon query で検証。**これで §2.5 item 30(a) の環境ブロックは解消し、CF production path での有効化に至った**。
30. **初回 genuine 本番 write 確認済み（2026-07-03、production LIVE）**: (a) full 本番 Neon-write smoke は **CLEAN で完了済み**（item 29(d)——env=1 で trigger した NAR `55:03` の iter40 11 行が本番 Neon に landing、5/11 頭が iter12 と reorder、fallback 確認）。当初 local Colima PG replica に `cell_model_key` / `cell_variant_id` 列が無い環境要因で blocked だったが、本番 Neon 経路の smoke で解消。(b) **CF production path で env=1 有効化済み**（item 27）。(c) **初回の genuine 本番 write を確認**——Cloudflare production path の per-race full serve（feature build → iter12 base → transformer fusion → iter40 Neon write）が iter40 で 48 NAR レース（499 頭行）を 03:05 JST に本番 Neon `race_finish_position_model_predictions` へ書き込み（`model_version=iter40-nar-settransformer-blend-v1`、`prediction_generated_at` 18:05:32-18:05:47 UTC ＝ 03:05 JST）。viewer は当日 48 NAR レース全てで iter40 を表示（latest-for-race、iter40 が 48/48 で勝ち）。pre-enablement の古い iter12（2 レース）/ iter36（3 レース）行は partial write で superseded。Ban-ei は transformer 非対象（NAR-only）。(d) 問題あれば CF secret を 0 に戻して即 rollback（redeploy 不要）。ローカル `.env` は診断・backfill wrapper 用で、本番 authority ではない。
31. **rollback と rebuild 注意（現状 LIVE）**: 現状の本番 authority は CF production path（CF secret=1）である。**無効化 / rollback（redeploy 不要・即時に pure iter12 ensemble へ戻る）**: CF は `printf 0 | bunx wrangler secret put NAR_TRANSFORMER_BLEND_ENABLED`。ローカル `.env` の `NAR_TRANSFORMER_BLEND_ENABLED` は診断・backfill wrapper 専用で、本番 trigger / fallback ではない。**注意**: base image は transformer default=False を bake しているため、有効化は上記 secret / env=1 の明示に依存する（HEAD からの rebuild + redeploy でも default OFF）。iter40 を image から完全に取り除く場合は該当 deploy を revert + image rebuild して旧 image へ deploy。

**2026-07-03 NAR score-level z-fusion: production DEPLOY + LIVE 検証済み（deployed win #2、rank-fusion iter40 → score-level z-fusion、model_version は iter40 維持）**

上記 rank-fusion iter40 を **score-level z-fusion**（base score と transformer seed-score を race 内で z-正規化してから 0.5 / 0.5 で混ぜる ＝ rank が捨てる confidence magnitude を保持）に本番差し替えし、同日中に LIVE 検証まで完了した。**アーキテクチャ lever（cross-horse Set Transformer）由来の 2 つ目の deployable win**（1 つ目＝ rank-fusion iter40 blend の +0.63pp top1、2 つ目＝ rank→score-z fusion の +0.25pp top1）。特徴量 lever 30+ が市場効率の壁で全 REJECT の中、アーキテクチャ ＋ fusion 方式の 2 段で cell 精度を押し上げた。モデル仕様は §2.5、gate / 統制の詳細は §11 の score-z ブロックを参照。

32. **gate（5-fold pooled で CONFIRMED）**: 既存 WF fold predictions を再利用し**再学習ゼロ**で検証（serve-exact な ketto tie-break で eval==serve 保証）。deployed rank-fusion を baseline とした 5-fold pooled（66,883 races、2000-iter LB95）で **top1 Δ+0.253[LB95 +0.120]／place2 Δ+0.341[+0.166]／place3 Δ+0.230[+0.051]**——全 3 primary で LB95>0・regressor ゼロ・全 5 fold 符号一致。当初 3-fold では place3 pooled LB95 が境界（-0.02）で HOLD だったが、2021 / 2022 の blind fold 追加で power が付き CONFIRMED（3-fold の境界は過学習でなく検出力の問題）。
33. **base-selection drift は moot（実 serving base と gate base が一致）**: per-class ensemble（iter12 + iter30 + iter36）は import されるが **`predict_upcoming.py:640` で unwired ＝ `score_races` は単一 `iter12-nar-xgb-hpo-v8` fallback booster で fuse する**。gate が使った iter12 base と実 serving base が完全一致するため、score-fusion は厳密に実 serving base に対して confirmed。§11 に当初記していた「実 ensemble base での plumbing smoke が必要」という scope caveat は解消。
34. **deploy（commit `a90161f4`、CF production path で score-z 稼働）**: container pkg で `transformer_scorer.py::fuse_ensemble_transformer` を rank→score-z に変更（`within_race_zscore` helper 追加、variant `score_z_55`）、`predict_upcoming.py` の NAR blend は `seed_score_mean` + score-z fuse を使用、3-tier fail-closed fallback は維持、coverage 100%。model / norm.json / R2 artifact / Dockerfile / 再学習は無変更で、既存 flag（`NAR_TRANSFORMER_BLEND_ENABLED=1`）の裏で完結。反映は **CF worker** Version `8706eb93-da31-4f97-8a6d-ac9c43a09392`（image `finish-position-cron-finishpositionpredictcontainer:8706eb93`、旧 `6a4d1fa3` を replace）。crons / queue / secret / observability は intact。なお worker / image の version ID は再 deploy ごとに churn するため、docs 記載値は記録時点の point-in-time——正確な現行値は `wrangler deployments` / `docker images` で確認する。
35. **live 検証済み（本番 Neon）**: 過去日 NAR race（`kb55` r3、20260627）を Cloudflare production image / worker path で実行し、本番 Neon `race_finish_position_model_predictions` に score-z の iter40 予測 11 行が landing（05:00:29 JST）。同 race の旧 rank-fusion 保存行と rank が 3/11 相違（同一 3-seed transformer で fuse のみ変更）＝ score-z が本番稼働していることを確認、fallback も intact。model_version は iter40 維持（before/after は timestamp で区別、accuracy 証拠は offline 5-fold gate）。artifact: `apps/pc-keiba-viewer/tmp/candidate-score-fusion-confirm/`（5-fold fusion tables / 2021-2022 base+transformer preds / `eval_confirm.py`）。
36. **rollback**: CF secret を 0 に戻すと pure iter12 ensemble、または commit `a90161f4` revert で rank-fusion iter40 に戻る。model_version は iter40 のまま変わらないため、accuracy 上の rollback は fuse 関数レベル（flag off または revert）で行う。

---

## 6. cell-level 評価（カテゴリ単位評価は禁止）

**精度評価は必ず cell 単位で行う。カテゴリ単位の評価は禁止する。** カテゴリ単位の集計は、特定の class / subgroup での回帰を平均で隠蔽するため。

このルールは着順モデルだけでなく脚質モデルにも適用する。脚質予測では `running_style` target の accuracy / top2_accuracy / macro_f1 / race_level 指標を cell 単位で評価し、採用 variant も cell 単位で決める。source 単位 latest model は fallback であり、cell variant の改善・回帰判定を source/category 集計だけで置き換えてはならない。

### 6.1 cell の定義

```
cell = category × surface × distance_band × class_label × season × venue
```

永続化・採用判定・cell-weighted feature search で使う canonical key は
`{category}_{surface}_{distance_band}_{class_label}_{season}_{venue}` で統一する。
旧実装や一部ドキュメントで `subgroup` / `racetrack` と呼んでいる値は、
この文脈ではそれぞれ `distance_band` / `venue` の旧名であり、新規仕様では使わない。

派生次元は以下から導出する。

- **surface**: `track_code` から turf（JRA `1*`）/ dirt（JRA `2*`、NAR/Ban-ei は常に dirt）/ other を判定。`cell_router.py:81-89`。
- **distance_band**: sprint（< 1200m）/ mile（1200-1599m）/ intermediate（1600-1999m）/ long（2000-2399m）/ extended（>= 2400m）。`cell_router.py:91-100`。
- **season**: spring（3-5 月）/ summer（6-8 月）/ autumn（9-11 月）/ winter（12-2 月）。`cell_router.py:103-110`。
- **class**: `grade_code` から導出（A/B/C/OP/NEW/MUKATSU/other/E/P/Q/R/S/T/unknown）。`cell_router.py:113-114`。
- **venue**: `keibajo_code`（競馬場コード。05=東京、06=中山、08=京都、09=阪神 等）。

### 6.2 cell 精度ストア（`cell_training_evaluations`）

学習パイプラインの `CellAccuracyStore` が Neon PostgreSQL の `cell_training_evaluations` テーブルに cell ごとの精度と target 固有の評価 payload を永続化する。

PRIMARY KEY: `(prediction_target, feature_set_hash, category, surface, distance_band, class_label, season, venue, subgroup)`

| カラム                                 | 説明                                                                                                                                                                                                                                                                                                                                                                      |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `prediction_target`                    | `finish_position` / `running_style`。着順と脚質の cell 評価を分離する                                                                                                                                                                                                                                                                                                     |
| `ndcg_at_3`                            | NDCG@3（relevance: 1着=3.0, 2着=2.0, 3着=1.0）                                                                                                                                                                                                                                                                                                                            |
| `top1_accuracy`                        | 1 着的中率                                                                                                                                                                                                                                                                                                                                                                |
| `place2_accuracy` 〜 `place6_accuracy` | 厳密 2〜6 着的中率                                                                                                                                                                                                                                                                                                                                                        |
| `top3_box_accuracy`                    | 上位 3 頭が順不同で一致した率                                                                                                                                                                                                                                                                                                                                             |
| `accuracy_vector`                      | 全指標を配列化したもの                                                                                                                                                                                                                                                                                                                                                    |
| `feature_names_array`                  | 使用した特徴量名リスト                                                                                                                                                                                                                                                                                                                                                    |
| `cell_vector`                          | cell 次元値の配列                                                                                                                                                                                                                                                                                                                                                         |
| `subgroup`                             | running-style では `kyoso_joken_code` / `nar_subclass` 由来の cell subgroup。finish-position では canonical subgroup key または空文字。`build_cell_models.py` は finish-position の canonical subgroup key を空文字と同等に正規化して、新旧評価行を同じ cell として比較する                                                                                               |
| `metric_payload`                       | target 固有の詳細 JSONB。`finish_position` / `running_style` とも per-cell accuracy / evaluation payload を保存する。payload 未指定の評価は `cell_training_evaluation_scalar_v1` を自動生成し、running-style の新規 cell 学習は `running_style_cell_v2` として class 別指標、confusion matrix、`race_level_pair_metrics` を含む race-level corner / finish 指標を保存する |

着順・脚質とも特徴量セットの hash は `learning.feature_selection_policy.compute_feature_set_hash()` を使う。特徴量名は重複排除・sort 後に SHA-256 化するため、local 探索、cell 評価、本番用 routing artifact で同じ組み合わせを同じ `feature_set_hash` として扱う。

脚質 `train-cells --save-cell-metrics-to-postgres` は `CellAccuracyStore` の DDL / migration / upsert を再利用する。着順・脚質とも `CellAccuracyStore` に渡された target-native な `metric_payload` は `cell_training_evaluations.metric_payload` に保存される。未指定の場合も scalar 指標から `cell_training_evaluation_scalar_v1` を生成して保存する。ローカル PostgreSQL では `apps/local-postgresql/sql/20260630000000_create_cell_training_evaluations.sql` が fresh DB 用の base table を作成し、`20260701000000_add_prediction_target_to_cell_training_evaluations.sql` が既存の target 非対応 primary key を `prediction_target` 付き key に昇格し、`20260702000000_add_subgroup_to_cell_training_evaluations.sql` が full cell key を保持し、`20260702010000_add_metric_payload_to_cell_training_evaluations.sql` が target 固有詳細 metric の JSONB 保存先追加と既存行 backfill を行う。

cell 次元の派生（`cell_training_evaluations` を populate する際の binning。`continuous_learner.py` が `learning/subgroup_diagnostics.py` の `get_distance_band()` / `_distance_band_expr()` で導出する）:

- **surface**: `track_code` 先頭 1 文字で turf（`1*`、JRA のみ）/ dirt（`2*`）/ other。NAR・Ban-ei は常に dirt。
- **distance_band**: `subgroup_diagnostics.get_distance_band()`（`subgroup_diagnostics.py:10-13, 67-75`）。**serve 時の cell routing（`cell_router.py:91-100`、§6.1）と同一の閾値**。
  - sprint: < 1200m
  - mile: 1200〜1599m
  - intermediate: 1600〜1999m
  - long: 2000〜2399m
  - extended: ≥ 2400m
- **season**: spring（3-5 月）/ summer（6-8 月）/ autumn（9-11 月）/ winter（12-2 月）。
- **class_label**: `grade_code` 由来（A/B/C/OP/NEW/MUKATSU/other/E/P/Q/R/S/T/unknown）。
- **venue**: `keibajo_code`。

> 実装上の注意: cell の distance_band は **serve routing（`cell_router.py`）と cell 評価ストア（`subgroup_diagnostics.py`）で同一の閾値（1200 / 1600 / 2000 / 2400）** であり、cell 次元として一貫している。
>
> これとは別に、serve 精度レポート用の bucket-eval 経路（`serve_accuracy_report.py:classify_distance_band` / `aggregate_bucket_eval_duckdb.py:build_distance_band_case_sql`）は **≤1400 / ≤1800 / ≤2200 / ≤2800 / >2800** という独自の binning を用いるが、これは `cell_training_evaluations` の `distance_band` ではなく serve 精度のバケット集計レポート専用である。さらに `finish_position_features_duckdb.py` の数値特徴 `KYORI_BAND*`（sprint ≤1300 / mile ≤1700 / intermediate ≤2200）は cell 次元ではなくモデル入力特徴であり、これも別系統である。

serve 精度レポート用の durable bucket evaluation は `running_style_model_bucket_evaluations` に raw count / sum を保存する。generated running-style predictions は bucket / race 内で actual の通過順・最終着順と照合し、horse pair ごとに予測順序が合っているかを score 化する。永続列は rate ではなく、`corner1_pair_score_sum/count`、`corner3_pair_score_sum/count`、`corner4_pair_score_sum/count`、`finish_pair_score_sum/count` である。集計・比較時は保存済み rate を平均せず、各 bucket の sum / count を加算してから `sum / count` を再計算する。

新方式のコーナー通過順評価では、`apply-running-style-postproc.ts` が脚質確率から生成した `predicted_corner_front_score` を評価の single source とする。`evaluate-running-style-bucket-sql.ts` は同じ式を SQL 内で再計算せず、loader temp table の `predicted_corner_front_score` を読み、同一 race 内の horse pair で actual `corner1_norm` / `corner3_norm` / `corner4_norm` / `finish_position` と比較する。保存単位は `model_version`、`running_style_feature_version`、`category`、`cell_model_key`、`cell_variant_id`、race bucket dimensions の組であり、cell 外の category 平均だけを根拠に採用判断してはならない。

`running_style_model_bucket_evaluations` の DDL は旧 schema の local / Neon にも再実行できる。`cell_model_key` / `cell_variant_id` と pair score 列を `add column if not exists` で追加し、旧 unique / lookup index が cell provenance を含まない場合は drop して新定義で再作成する。旧 index のまま `insert_running_style_bucket_evaluation_row.py` や batch upsert を実行すると `ON CONFLICT` が cell 単位にならないため、DDL bootstrap を先に通す。

`cell_training_evaluations.metric_payload` は target-native metric に加えて optional provenance として `model_version`、`architecture`、`method` を保持できる。専用列は追加しない。`CellAccuracyStore` は caller が provenance を渡した場合だけ JSONB に追記し、payload が既に同名キーを持つ場合は payload 側を優先する。`build_cell_models.py` はこの provenance がある候補では routing variant の `model_version` / `architecture` / `method` に反映する。provenance が無い既存行の `cell-<feature_set_hash prefix>` fallback は `--allow-synthetic-model-version` を明示した local analysis 専用であり、本番 `cell_routing.json` には使わない。

### 6.3 cell_routing.json によるデータ駆動ルーティング

`apps/finish-position-predict-container/src/predict_lib/cell_routing.json` が data-driven なモデルルーティングを駆動する。

production scoring の解決順は固定である。まず non-default cell variant を最優先で解決し、一致すればその variant model で直接 scoring する。一致しない場合は category default / 明示的に有効な category-level path（JRA E-top2、NAR transformer blend）へ進む。per-class / subclass registry は production routing の authority にしない。

```mermaid
flowchart TB
    REQ["予測リクエスト<br/>（race × horse）"]
    DERIVE["cell 次元を導出<br/>（surface / distance_band /<br/>season / class）"]
    LOOKUP{"cell_routing.json<br/>rules にマッチ?"}
    subgraph BANEI["Ban-ei ルーティング例"]
        RULE["grade_code == E ?"]
        BASE["base variant<br/>banei-cb-v8-window2011-wf-15y<br/>（111 feat）"]
        SIM["sim variant（default）<br/>banei-cb-v9-sim-2011<br/>（130 feat）"]
        RULE -->|"yes"| BASE
        RULE -->|"no"| SIM
    end
    SCORE["選択された variant で scoring"]

    REQ --> DERIVE --> LOOKUP
    LOOKUP -->|"rule match"| BASE
    LOOKUP -->|"default_variant"| SIM
    BASE --> SCORE
    SIM --> SCORE
```

Ban-ei では `grade_code == "E"` のレースを `base` variant（v8 window2011）へルーティングし、それ以外は `default_variant = sim`（v9-sim）を用いる。

NAR では `dirt / mile / E / summer / venue 54` の cell を `nar-xgb-cell-a957d8b4-v1` へルーティングする。これは `feature_set_hash = a957d8b4d2bbc7c1ab2a0b320a308b063cf3e4f407240eacbfb21e797a282055`、`architecture = xgboost`、`feature_count = 10` の focused-feature model で、artifact は `apps/finish-position-predict-container/models/finish-position/nar/nar-xgb-cell-a957d8b4-v1/` に置く。`model_meta.json` の NAR default（`iter12-nar-xgb-hpo-v8`, 192 features）は変更せず、該当 cell だけ `cell_routing.json` で切り替える。該当しない NAR race は per-class へ戻さず、category default / 明示的に有効な category-level path を使う。

この NAR cell は baseline `d79657af0d78c116bf4e8458646616ee8501d78a86db291be76874cedcd5223d` に対し、top1 +0.215983pp、place2 +0.215983pp、place3 +0.431965pp、place4 +1.079914pp、place5 +0.431965pp、place6 +1.295896pp、top3_box +0.215983pp と全 gated metrics が改善したため、§7.3 / §8.12 の finish-position LB95 例外で採用する。`cell_training_evaluations.metric_payload` には `model_version = nar-xgb-cell-a957d8b4-v1` / `architecture = xgboost` / `method = focus-features-xgb` を保持し、production routing 生成時は `--model-artifacts-root` で実 artifact の存在と feature count を検証する。

**2026-07-03 JST 本番確認**: Cloudflare production path の NAR focused per-race run（`nar:20260703:54:10`）が layer10 完了後に Neon へ `model_version = nar-xgb-cell-a957d8b4-v1` を書き込んだ。確認行は `2026-07-04 54:10` が 11 頭（`prediction_generated_at = 2026-07-02T17:20:38Z`）、`2026-07-05 54:10` が 8 頭（`prediction_generated_at = 2026-07-02T17:20:39Z`）。元 RA は `grade_code = E`、距離 1300 / 1400、`track_code = 24` で、NAR の cell router では `dirt / mile / E / summer / venue 54` に解決される。これにより、cell hit 時は NAR transformer blend / category default より先に `cell_routing.json` の variant が採用されることを本番 Neon write で確認済み。なお prediction table の `class_code` は NAR subclass 由来であり、router の `grade_code` とは別概念である。

本番反映前の最短検証は次の 3 点である。

1. `build_cell_models.py --prediction-target finish_position --category nar --baseline-hash d79657af0d78c116bf4e8458646616ee8501d78a86db291be76874cedcd5223d --model-artifacts-root apps/finish-position-predict-container/models/finish-position` が `selected_cells=1 variants=1` を出す。
2. `load_cell_router()` が `grade_code=E, keibajo_code=54, kyori=1400, kaisai_tsukihi=0702, track_code=20` を `cell-a957d8b4-nar-xgb-cell-a957d8b4-v1-56559522` に解決する。
3. `models/finish-position/nar/nar-xgb-cell-a957d8b4-v1/metadata.json.feature_names` が 10 件で、`model.json` が同 directory に存在する。

本番 `cell_routing.json` の非 default variant は、必ず実在する model artifact を指す。`model_version` は `apps/finish-position-predict-container/models/finish-position/{category}/{model_version}/` 配下の `model.json` / `metadata.json` と対応し、`metadata.json.feature_names` の件数は routing variant の `feature_count` と一致していなければならない。`build_cell_models.py` はデフォルトで `metric_payload.model_version` と `metric_payload.architecture` を持たない finish-position 候補を拒否する。旧評価行を local で確認する場合だけ `--allow-synthetic-model-version` を使い、本番出力前は `--model-artifacts-root apps/finish-position-predict-container/models/finish-position` で artifact 存在も検証する。

---

## 7. 評価指標（rank 1-6 すべて必須）

### 7.1 順位指標

順位評価は top1 / place2 / place3 だけでなく **1 着〜6 着すべて**を計測する。

- **Primary**: top1, place2, place3, place4, place5, place6
- **Supplementary**: top3_box, fukusho_2p, top3_exact, top3_winner_capture, top5_winner_capture, pair_score

place2 / place3 は exact-ordinal（厳密順位）であり、情報理論的に 40% 到達は不可能であることが確定している。一方、累積指標（fukusho_2p, top3_box 等）は既に 40% を超える。

### 7.2 accept gate

```mermaid
flowchart TB
    EVAL["cell 単位で全指標を評価"]
    G1{"top1 / place2 / place3 の<br/>うち 2 つ以上が positive?"}
    G2{"place2 / place3 の<br/>うち 1 つ以上が positive?"}
    G3{"いずれの primary も<br/>回帰 > -0.05pp なし?"}
    DELTA{"改善 delta >= +0.08pp?"}
    LB{"LB95 > 0<br/>または finish-position の<br/>全 gated metrics が改善?"}
    ADOPT["ADOPT"]
    REJECT["REJECT"]

    EVAL --> G1
    G1 -->|"no"| REJECT
    G1 -->|"yes"| G2
    G2 -->|"no"| REJECT
    G2 -->|"yes"| G3
    G3 -->|"no（回帰あり）"| REJECT
    G3 -->|"yes"| DELTA
    DELTA -->|"no"| REJECT
    DELTA -->|"yes"| LB
    LB -->|"no"| REJECT
    LB -->|"yes"| ADOPT
```

- **gate 条件**: `{top1, place2, place3}` のうち **2 つ以上が positive**、かつ `{place2, place3}` のうち **1 つ以上が positive**、かつ **回帰が -0.05pp を超えない**こと。finish-position cell routing ではこれに加えて LB95 > 0、または全 gated metrics（top1〜place6 / top3_box）の +0.08pp 以上改善が必要である。running-style は §4.5 / §8.12 の target-specific gate を使う。
- **有意改善の閾値**: delta **>= +0.08pp** を実効果ありとみなす。
- per-cell 評価で一部 cell が改善・他 cell が悪化する場合は、global reject せず serve 時の cell routing で「効く cell だけ」新 variant を適用してよい（cell-conditional adoption）。

### 7.3 Walk-forward（WF）検証

```mermaid
flowchart LR
    subgraph WF["Walk-forward 3-fold（時系列 blind）"]
        F1["fold1: train ≤2022 → blind 2023"]
        F2["fold2: train ≤2023 → blind 2024"]
        F3["fold3: train ≤2024 → blind 2025"]
    end
    LB95{"LB95 > 0?<br/>（bootstrap 95% CI 下限<br/>2000 iterations）"}
    HPO["HPO は別個の blind holdout で<br/>確認（選択バイアス回避）"]
    SERVE["serve 分布で必ず突合せ<br/>（WF 精度 ≠ serve 精度）"]

    F1 --> LB95
    F2 --> LB95
    F3 --> LB95
    LB95 --> SERVE
    HPO --> SERVE
```

- WF は時系列の blind fold を **3 つ**用いる（例: 2023 / 2024 / 2025 を blind year とする）。
- 各 fold は **その年より前の年で学習し、当該 fold の年で予測**する（leak-free な chronological 構成）。
- **LB95**（bootstrap 95% 信頼区間下限、2000 iterations）を採否の主指標とする。global / pooled の positive を主張する metric は **LB95 > 0** が必須（点推定が正でも LB95 が 0 を跨ぐ場合は global 採用しない）。
- 例外として、`build_cell_models.py` の finish-position cell routing 採用では、sample / freshness / multi-metric / no-regression を満たし、かつ **着順の全 gated metrics（top1〜place6 / top3_box）が +0.08pp（0.0008）以上改善**している cell は LB95 が 0 を跨いでも採用できる。これは NAR `mile / E / venue 54` のような狭い cell pocket を routing で限定適用するためのルールであり、category/global model の改善主張には使わない。脚質ではこの LB95 例外を使わない。
- **HPO は同一 fold を再利用すると選択バイアスが生じる**ため、deploy 前に**別個の blind holdout**（single-config）で confirm すること（必須、selection bias protection）。
- WF 精度は必ず serve 精度と突合せる。WF が隠した本番劣化（serve-skew）が頭打ちの中核要因となった事例がある。

---

## 8. 学習パイプライン（Mac 専用）

```mermaid
flowchart TB
    LOOP["continuous_learner.py<br/>（iterative loop）"]
    EXPLORE["feature_explorer.py<br/>（Optuna TPE 特徴量探索）"]
    subgraph WFTRAIN["Walk-forward 学習"]
        CB["train_finish_position_<br/>catboost_walk_forward.py<br/>（JRA / Ban-ei, YetiRank）"]
        XGB["train_finish_position_<br/>xgboost_walk_forward.py<br/>（NAR, rank:pairwise）"]
    end
    GATE{"accept gate<br/>（§7.2）"}
    ARTI["model.json + metadata.json"]
    RSTRAIN["running_style_lightgbm.py train-cells<br/>（脚質 cell models）"]
    RSARTI["flatbin（header metadata 込み）<br/>RUNNING_STYLE_CELL_ROUTING_JSON"]
    DEPLOY["model_meta.json 更新<br/>→ docker build<br/>→ deploy"]

    LOOP --> WFTRAIN
    EXPLORE --> WFTRAIN
    WFTRAIN --> GATE
    GATE -->|"ADOPT"| ARTI --> DEPLOY
    GATE -->|"REJECT"| LOOP
    RSTRAIN --> RSARTI
```

### 8.1 主要スクリプト

- **`continuous_learner.py`** — train → predict → verify の iterative loop。ad-hoc fit は禁止し、常に学習ループスクリプトを用いる。
- **`feature_explorer.py`** — Optuna TPE による特徴量組み合わせ探索。
- **Walk-forward 学習**:
  - `train_finish_position_catboost_walk_forward.py`（JRA / Ban-ei）
  - `train_finish_position_xgboost_walk_forward.py`（NAR）— 任意で `--predictions-output-root <root>` を渡すと、fold ごとの per-race `valid_predictions`（`race_id` / `keibajo_code` / `kyori` / `grade_code` / `kaisai_nen` / `kaisai_tsukihi` / `race_bango` / `ketto_toroku_bango` / `finish_position` / `predicted_rank` / `predicted_score` を含む passthrough 列つき）を `<root>/<category>/fold-<fold_year>/predictions.parquet` に書き出す。post-hoc な cell 単位 WF candidate 評価を、passthrough 列を破棄せず再利用できるようにする既存インフラ（未指定時は挙動変更なし）。
- **`running_style_lightgbm.py train-cells`** — 脚質 cell model の local training / evaluation / promotion plan 生成。出力は running-style 固有 metric、flatbin 変換対象 artifact、`RUNNING_STYLE_CELL_ROUTING_JSON` 候補である。

### 8.2 アーキテクチャと loss

- **JRA / Ban-ei**: CatBoost + **YetiRank** loss。categorical features は **`keibajo_code` / `track_code` / `grade_code` / `umaban`**（`train_finish_position_catboost_walk_forward.py:41`、`--focus-features` でも drop されないよう固定）。
- **NAR**: XGBoost + **`rank:pairwise`**（本番既定）。代替として Lever 11 の `--objective ndcg` を選ぶと `rank:ndcg` + `lambdarank_pair_method=topk` + `lambdarank_num_pair_per_sample=3` になるが、本番 NAR は pairwise を採用。
- **LightGBM**（補助 trainer、`train_finish_position_lightgbm_walk_forward.py`）: `objective` 既定 `lambdarank`、代替 `rank_xendcg`（Lever 17 で `lambdarank_truncation_level` を調整可）。
- **NAR listwise Set Transformer（アーキテクチャ lever、2026-07-03 Cloudflare production ENABLED = iter40）**: GBDT 系とは別軸のアーキテクチャ lever。`RaceSetTransformer`——レース内の各馬を token とし multi-head self-attention（cross-horse set attention）で馬同士を相互参照させ、**listnet（listwise）loss** で学習（MLX / Metal GPU、3-seed ensemble）。単体でなく本番 `iter12`（XGBoost）と within-race rank fusion（0.5 / 0.5）で blend する構成が、NAR で prod-base gate を通過した **本キャンペーン初のアーキテクチャ lever win（2026-07-02 深夜、pooled blind top1 Δ+0.641pp[LB95 +0.477]、8 指標すべて LB95>0）**。GBDT の木は per-horse ベクトルしか見ないため cross-horse attention を構造的に表現できず、薄市場 NAR で edge を残す（効率的市場の JRA では転移せず REJECT）。本番反映は CF Container 経由必須だが MLX は Container で動かないため、**serve-exact な numpy scorer での gate 再実行（eval==serve 保証）＋ Container 統合** を経て 2026-07-03 に本番 image へ deploy し、Neon-write smoke CLEAN 後に **Cloudflare production path で ENABLED**、同日 Cloudflare production path の per-race full serve で初回 genuine 本番 write 確認・**production LIVE**（48 NAR レース 499 頭行を iter40 で本番 Neon に書き込み、viewer 48/48 表示）。**同 2026-07-03 に fusion 方式を rank-fusion → score-level z-fusion に更新し LIVE 検証済み（deployed win #2、commit `a90161f4`、model_version は iter40 維持）——アーキテクチャ lever から 2 つの deployable win が本番稼働**（rank-fusion +0.63pp top1 / score-z +0.25pp top1）。手法・統制・数値・deploy 記録の詳細は §11 NAR / §2.5 / §5.9。

### 8.3 学習窓（再掲・カテゴリで異なる）

- JRA: 2013+
- NAR: full 2006+
- Ban-ei: 2011+

### 8.4 サンプル重み

- **時間減衰**: 最古年 0.5 〜 最新年 1.0 の線形重み（`walk_forward_common.py:163-177`）。
- **Bucket-aware mixing**: `w_composed = w_time * (1 + alpha * is_weak_bucket_score)`。`[0.5, 1.75]` にクリップ。`alpha` 上限 0.75。

### 8.5 Walk-forward skip gate（2 段階回帰保護）

fold ごとに以下の 2 条件が同時に成立した場合、その fold をスキップする。

1. NDCG < baseline \* 0.95（5% 劣化）
2. top1 < baseline _ 0.93 **または** place3 < baseline _ 0.90

### 8.6 特徴量グループ（15 semantic groups）

`feature_explorer.py` が Optuna TPE で group-level のカテゴリカル探索を行う。

odds / jockey / pedigree / running_style / corner / speed / similar_race / weather / weight / race_condition / recent_form / career / trainer / horse_identity / other

### 8.7 NDCG 関連性マッピング

| 着順     | 関連度 |
| -------- | ------ |
| 1 着     | 3.0    |
| 2 着     | 2.0    |
| 3 着     | 1.0    |
| 4 着以下 | 0.0    |

### 8.8 主要閾値一覧

| パラメータ              | 値      | 説明                      |
| ----------------------- | ------- | ------------------------- |
| DEPLOY_THRESHOLD        | 0.005   | NDCG delta 最低基準       |
| SATURATION_LOOKBACK     | 50      | 改善なし trial → 予算削減 |
| MIN_RACES（cell）       | 200     | cell 採用の最低レース数   |
| FRESHNESS_DAYS          | 14      | 評価データの鮮度上限      |
| MIN_DELTA               | +0.08pp | 改善とみなす最低 delta    |
| NO_REG_THRESHOLD        | -0.05pp | 許容する最大回帰幅        |
| N_BOOTSTRAP             | 2000    | LB95 CI のリサンプル数    |
| NDCG_SKIP_RATIO         | 0.95    | NDCG 回帰ガード           |
| TOP1_SKIP_RATIO         | 0.93    | top1 回帰ガード           |
| PLACE3_SKIP_RATIO       | 0.90    | place3 回帰ガード         |
| TIME_DECAY_MIN_WEIGHT   | 0.5     | 古いデータの重み下限      |
| TIME_DECAY_MAX_WEIGHT   | 1.0     | 最新データの重み上限      |
| MAX_BUCKET_WEIGHT_ALPHA | 0.75    | bucket 重み mixing 上限   |

### 8.9 Walk-forward fold 構成

各 fold は時系列で train / valid を分割する（`finish_position IS NOT NULL` の行のみ対象）。

- **train**: `train_start` 〜 `(valid_year - 1)/12/31`
- **valid**: `valid_year` の暦年全体（1/1 〜 12/31）
- 関連度（NDCG）: 1 着 3.0 / 2 着 2.0 / 3 着 1.0 / その他 0.0（§8.7）

### 8.10 Continuous Learner オーケストレータ

`continuous_learner.py` は train → predict → verify の loop を統括し、2 つの永続ストアを持つ。

- **`CellAccuracyStore`**: cell ごとの精度を Neon PostgreSQL `cell_training_evaluations` に永続化（§6.2）。
- **`TrialExplorationStore`**: trial の重複排除キャッシュ（DuckDB `trial_exploration_log`、PRIMARY KEY `(feature_set_hash, category, method)`、`continuous_learner.py:225`）。mask / importance vector を `all_features` に整列して保持する。
- **saturation 検知**: 直近 `SATURATION_LOOKBACK`（50）trial で改善が無ければ trial 予算を削減する。

### 8.11 Feature Explorer（Optuna）

`feature_explorer.py` は特徴量グループ（§8.6）レベルのカテゴリカル探索を行う。

- 着順・脚質の特徴量列解決は `learning.feature_selection_policy.resolve_feature_columns_for_target()` を正とする。着順は `finish_position` target、脚質は `running_style` target を指定し、脚質では `rs_p_*` leakage 列と cell 派生列を学習特徴量から除外する。
- routing / evaluation に必要な metadata 列（`source`, `keibajo_code`, `track_code`, `kyori`, `grade_code`, `race_date` 等）は入力 DataFrame に保持するが、target-specific policy で明示的に許可されない限り学習特徴量には含めない。
- local 探索で cell ごとに最良だった特徴量セットは `feature_names_array` と `feature_set_hash` として `cell_training_evaluations` に保存する。候補が実 model artifact を持つ場合は `metric_payload` に `model_version` / `architecture` / `method` も保存する。`build_cell_models.py` は `--prediction-target finish_position|running_style` で対象を分け、adoption gate 後に cell 単位で最良 method / model / feature-set を 1 つ選び、その winner だけを routing JSON に出力する。finish-position の本番routingでは provenance 必須、running-style は別途 flatbin + `RUNNING_STYLE_CELL_ROUTING_JSON` の promotion contract に従う。
- 脚質の `running_style_lightgbm.py train-cells` は `--cell-feature-selection-json` でこの routing JSON を読み、cell ごとの採用特徴量セットを使って model artifact を作る。着順も脚質も「local で cell 精度が良かった特徴量組み合わせ」を本番参照 artifact に反映する。
- **TPESampler**（`multivariate=True`、`feature_explorer.py:1036-1039`）で feature group の joint interaction をモデル化。startup random trial 数は 5。
- **cell-weighted NDCG@3**: canonical cell key（`category_surface_distance_class_season_venue`）ごとに逆精度重み `1 / max(accuracy, 0.01)` を mean 正規化して付与（`compute_cell_weights_from_accuracy` / `weighted_ndcg_at_3`）。弱い cell ほど重みが大きくなり、苦手領域の改善を優先する。`weighted_ndcg_at_3` は `learning.subgroup_diagnostics.assign_subgroup_keys()` を使い、cell 評価・採用と同じキーで per-race の重みを引く。

### 8.12 Cell model adoption gate（`build_cell_models.py`）

`build_cell_models.py` は候補 feature-set の cell 別精度を `cell_training_evaluations` から読み、`--prediction-target` ごとの採用プロファイルで以下の条件を満たした cell のみ採用する。

1. **サンプル数**: `race_count >= 200`（`DEFAULT_MIN_RACES`）。
2. **鮮度**: `evaluated_at` が 14 日以内（`DEFAULT_FRESHNESS_DAYS`）。
3. **多指標改善（着順）**: primary `{top1, place2, place3}` のうち **>= 2 個**が **+0.08pp（0.0008）** 以上改善し、うち **>= 1 個が place2 / place3**（`check_multi_metric_gate`）。
4. **多指標改善（脚質）**: `top1_accuracy = accuracy` の改善を必須とし、さらに `place2_accuracy = top2_accuracy` または `place3_accuracy = macro_f1` のどちらかも改善した cell のみ採用する。
5. **no-regression**: 着順は 7 gated metrics（top1〜place6 / top3_box）すべて、脚質は accuracy / top2_accuracy / macro_f1 が **-0.05pp（-0.0005）** を割り込まない。
6. **bootstrap / all-metric sweep**: primary metric の bootstrap LB95 が **> 0.0**（2000 resamples、`DEFAULT_N_BOOT`）。ただし finish-position では、全 gated metrics が +0.08pp 以上改善している cell は LB95 が 0 を跨いでも採用可能にする。例: NAR `dirt / mile / E / summer / venue 54` は `a957d8b4...` が baseline `d79657af...` に対して top1 +0.215983pp、place2 +0.215983pp、place3 +0.431965pp、place4 +1.079914pp、place5 +0.431965pp、place6 +1.295896pp、top3_box +0.215983pp と全 gated metrics が実効果閾値以上のため、評価上は採用対象にできる。脚質ではこの LB95 例外を使わない。
7. **baseline 存在**: 比較対象 baseline cell が存在すること。

同一 routing cell で複数 candidate が採用条件を満たす場合、`build_cell_models.py` は `--prediction-target` の primary metrics 合計、required metrics 合計、no-regression metrics 合計、`race_count`、`evaluated_at`、`feature_set_hash` の順で deterministic に比較し、cell ごとに最良 candidate を 1 つだけ残す。finish-position は production routing に `cell_subgroup` を出さないため、`class / distance_band / season / surface / venue` が同じ行は同一 routing cell として比較する。running-style は `cell_subgroup` を routing 条件に含めるため、`kyoso_joken_code` / `nar_subclass` 由来の subgroup まで含めて別 cell として扱う。これにより同じ cell 条件を指す routing rule が複数 variant に重複して出ることを防ぎ、cell ごとに最適なモデル / 手法 / feature-set を動的に保持して利用する。

採用された cell をまとめて cell model を構築し、`cell_routing.json`（§6.3）の routing に反映する。ただし finish-position の本番反映は、採用候補が `metric_payload.model_version` / `architecture` を持ち、対応する container model artifact が存在する場合に限る。既存DB行のように provenance が無い候補は `--allow-synthetic-model-version` で local に採用確認できても、本番 promote してはならない。

### 8.13 モデル artifact

- `model.json`（CatBoost JSON tree、または XGBoost）
- `metadata.json`

XGBoost artifact は early stopping の `best_iteration` を production scoring でも尊重する。`xgboost_adapter.py` は `booster.best_iteration` が存在する場合、`predict(..., iteration_range=(0, best_iteration + 1))` で offline 評価と同じ tree range を使う。`best_iteration` が無い artifact だけ全 tree scoring に fallback する。offline gate と production score の tree range をずらしてはならない。

脚質の production artifact は以下。

- `model.txt` / trainer metadata: local training output。production は直接読まない。
- `*.flatbin`: Worker が `RUNNING_STYLE_MODELS` R2 binding から読む唯一の脚質 model format。header に `model_version` / `feature_names` / `class_labels` を含める。
- routing JSON: `RUNNING_STYLE_CELL_ROUTING_JSON` として Cloudflare Worker に設定する。local path ではなく R2 object key を参照する。

脚質 promotion は `bunx wrangler r2 object put ... --remote` 相当の R2 upload と Worker 設定更新が完了して初めて production 反映となる。local training output の作成だけでは production へ反映されない。

---

## 9. アンチパターン（禁止事項）

以下は本システムで明確に禁止する。

```mermaid
flowchart TB
    A["カテゴリ単位評価<br/>→ cell 単位必須"]
    B["coverage 閾値の引き下げ<br/>→ 禁止（ユーザー承認のみ）"]
    C["特徴量カラムの削減<br/>→ 禁止（schema 拡張のみ可）"]
    D["GitHub Workflows での予測<br/>→ CF Cron Trigger 一択"]
    E["データ store からの削除<br/>→ D1/PG/R2/KV すべて禁止"]
    F["Mac での本番予測<br/>→ Mac は学習専用"]
    G["blind holdout なしの HPO<br/>→ 選択バイアスで却下"]
    H["日付単位の一括予測生成<br/>→ per-race 単位必須"]
    I["ローカル scheduler 依存の本番運用<br/>→ CF Queue / Cron / Worker"]

    PROHIBITED(("PROHIBITED"))
    A --> PROHIBITED
    B --> PROHIBITED
    C --> PROHIBITED
    D --> PROHIBITED
    E --> PROHIBITED
    F --> PROHIBITED
    G --> PROHIBITED
    H --> PROHIBITED
    I --> PROHIBITED
```

1. **カテゴリ単位評価の禁止** — 必ず cell 単位（§6）で評価する。
2. **coverage 閾値の引き下げ禁止** — `vitest.config.ts` の thresholds・`pyproject.toml` の `--cov-fail-under` を下げる変更はユーザーの明示承認時のみ。計測対象（include / source）の縮小も禁止。
3. **特徴量カラムの削減禁止** — 部分集合化 / merge / lossy 型変換は全禁止。schema 拡張のみ可。
4. **予測への GitHub Workflows 利用禁止** — スケジュール実行は Cloudflare Cron Trigger 一択。`.github/workflows/` 配下に予測 workflow を追加しない。
5. **データ削除禁止** — D1 / PG / R2 / KV いずれの store からも DELETE / TRUNCATE / DROP / retention 追加を禁止。
6. **Mac での本番予測禁止** — Mac は学習・artifact 生成専用。本番の特徴量生成・脚質予測・着順予測は Cloudflare Worker / Queue / Container。
7. **blind holdout なしの HPO 禁止** — 選択バイアスを避けるため、deploy 前に独立 holdout で confirm する。
8. **日付単位・カテゴリ一括の本番予測生成禁止** — 本番の特徴量生成・脚質予測・着順予測は常にレース単位（per-race）で実行する。日付単位やカテゴリ一括のバッチ処理を新規に構築してはならない。日次 cron であっても内部はレース単位の collect の集約として構成すること（§5.4 参照）。
9. **ローカル scheduler 依存の本番運用禁止** — ローカル scheduler、手元 shell script を本番 trigger / ordering / retry / fallback に使わない。本番順序は Cloudflare Cron / Queue / Worker / Container で担保し、service binding / API は queue primary path が使えない環境の fallback に限る。

---

## 10. 関連ファイル一覧

| 役割                                     | パス                                                                                                                          |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| 特徴量ビルダー                           | `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`                                                         |
| iterative 学習ループ                     | `apps/pc-keiba-viewer/src/scripts/learning/continuous_learner.py`                                                             |
| 特徴量探索                               | `apps/pc-keiba-viewer/src/scripts/learning/feature_explorer.py`                                                               |
| WF 学習（CatBoost）                      | `apps/pc-keiba-viewer/src/scripts/train_finish_position_catboost_walk_forward.py`                                             |
| WF 学習（XGBoost）                       | `apps/pc-keiba-viewer/src/scripts/train_finish_position_xgboost_walk_forward.py`                                              |
| 脚質 LightGBM 学習 / cell promotion      | `apps/pc-keiba-viewer/src/scripts/running_style_lightgbm.py`                                                                  |
| 脚質 flatbin R2 登録                     | `apps/sync-realtime-data/src/running-style-model-register.ts`                                                                 |
| 脚質 flatbin loader / evaluator          | `apps/sync-realtime-data/src/running-style-model-binary.ts`                                                                   |
| 脚質 cron / queue                        | `apps/sync-realtime-data/src/running-style-cron.ts` / `running-style-queue.ts`                                                |
| 脚質 cell routing                        | `apps/sync-realtime-data/src/running-style-cell-router.ts`                                                                    |
| 脚質 feature SQL / Parquet               | `apps/sync-realtime-data/src/running-style-feature-sql.ts` / `running-style-feature-parquet.ts`                               |
| 脚質 R2 daily prediction export          | `apps/sync-realtime-data/src/running-style-parquet-export.ts`                                                                 |
| 脚質 viewer cache                        | `apps/sync-realtime-data/src/viewer-running-style-cache.ts` / `running-style-cache.ts`                                        |
| 着順 pacestyle 結合                      | `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-pacestyle-features.py`                                         |
| Cron Worker                              | `apps/finish-position-cron/`（`wrangler.jsonc`, `src/`）                                                                      |
| Container（推論）                        | `apps/finish-position-predict-container/`                                                                                     |
| 本番モデル定義                           | `apps/finish-position-predict-container/src/predict_lib/model_meta.json`                                                      |
| cell ルーティング（serve 時派生 + 判定） | `apps/finish-position-predict-container/src/predict_lib/cell_routing.json` / `cell_router.py`                                 |
| /predict サーバ                          | `apps/finish-position-predict-container/src/predict_lib/serve.py`                                                             |
| WF 学習（LightGBM、補助）                | `apps/pc-keiba-viewer/src/scripts/train_finish_position_lightgbm_walk_forward.py`                                             |
| per-race rescore dispatch                | `apps/finish-position-cron/src/queue-consumer.ts`（JRA / NAR / Ban-ei すべて Container held `/predict`）                      |
| cell model adoption gate                 | `apps/pc-keiba-viewer/src/scripts/learning/build_cell_models.py`                                                              |
| cell 次元 binning（cell store）          | `apps/pc-keiba-viewer/src/scripts/learning/subgroup_diagnostics.py`（`get_distance_band` 他）                                 |
| trial 重複排除ストア                     | `apps/pc-keiba-viewer/src/scripts/trial_registry.py` / `learning/continuous_learner.py`（`trial_exploration_log`）            |
| serve 精度 bucket-eval（別系統 binning） | `apps/pc-keiba-viewer/src/scripts/serve_accuracy_report.py`（`classify_distance_band` 他）/ `aggregate_bucket_eval_duckdb.py` |

---

## 11. 補足: 各カテゴリのフロンティア状況

各カテゴリは現時点で経験的フロンティアに到達しており、多数の lever が ablation 検証で REJECT 済み（DO-NOT-RETEST）。直近の deployed win は以下。

**2026-07-02〜03 cell 精度 campaign 総括**: 特徴量 lever（similar-race 派生・chemistry・pace-fit・cross-source 転入馬履歴・same-day 系・rotation-fit・draw affinity・trainer / jockey switch・inbreeding・circadian・transport・body-mass・sire-spec・overnight mega-probe 30 候補・文献調査 119 論文由来 6 候補 = すべて REJECT、市場効率の壁を多角的に再確認）は完全に飽和した。唯一の deployable win は **アーキテクチャ lever**——NAR で listwise Set Transformer の cross-horse attention を iter12 XGBoost と blend する新 lever が本キャンペーンで初めて accept gate を通過した（**win #1 = rank-fusion +0.63pp top1**、`iter40` として 2026-07-03 本番稼働 LIVE）。さらに rank ではなく score-level z-fusion で magnitude を保持することで 2 つ目の win を得た（**win #2 = score-z +0.25pp top1**、同日 本番稼働 LIVE）。この lever は NAR 固有（効率的市場の JRA / Ban-ei は listwise CatBoost-YetiRank base に対し冗長で REJECT）。アーキテクチャの全 knob（architecture variant c1 / c2 / c3・fusion 方式 rank / score-z / harmonic / borda・blend weight tw・per-cell routing・seed 数 3 / 5 / 7・transformer 入力への直交特徴量追加）を検証し尽くし、**deployed = 3-seed c2 listwise + score-z 0.5 / 0.5 が最適配置**と確定した。両 win とも fail-closed fallback + instant rollback（env / secret = 0）を完備し、numpy bit-exact serve で eval==serve を保証する。これにより NAR frontier を（アーキテクチャ lever の 1 点のみ更新して）再確定、JRA（2013+ window / sim / E-top2 済）・Ban-ei（2011+ window / sim）も frontier。

- **JRA**: similar-race 特徴量（v9-sim, 263 feat）を 2026-06-26 に deploy。学習窓 2013+ は sweep 完了。
  - **E-top2 override 復活 REJECT（2026-07-02、DO-NOT-RETEST）** — v9-sim store で XGB を再学習し特徴量非互換を解消した上で override を 3-fold WF blind で再検証したが pooled top1 Δ-0.183pp[LB95 -0.618] で REJECT（sim\_\* が signal を既に吸収済み、詳細は §2.3）。本番 `jra-cb-v9-sim-2013` 無変更。
  - **弱 cell 3 件（`jra_dirt_mile_unknown_winter_07` Chukyo / `jra_turf_intermediate_unknown_summer_04` Niigata / `jra_turf_mile_unknown_autumn_03` Fukushima）2 lever 深掘り REJECT（2026-07-02、DO-NOT-RETEST）** — 系統的 per-cell scan（`iter14-jra-cb-pacestyle-course-v8` 予測を proxy として jvd_se/jvd_ra 実結果に join、`learning.subgroup_diagnostics` の canonical binning で独立再確認、3 cell とも race_count/delta 完全一致: Chukyo race_count=452 top1Δ-7.56pp/place2Δ-4.55pp/place3Δ-5.62pp、Niigata race_count=453 top1Δ-5.20pp/place2Δ-3.92pp/place3Δ-1.67pp、Fukushima race_count=261 top1Δ-3.52pp/place2Δ-1.50pp/place3Δ-3.98pp）で発見。market oracle 比較（favorite＝tansho_ninkijun==1 の的中率）は cell ごとに異質: Niigata は市場が cat avg より好調（fav_top1 34.00% vs cat 32.48%）なのにモデルは崩れる（model edge が通常+7.82ppから+1.10ppへ収縮）——唯一 real gap の可能性がある cell。Fukushima は市場も cat avg より悪い（fav_top1 27.20%、Δ-5.28pp）のにモデルの edge はむしろ拡大（+9.58pp）——ほぼ irreducible。Chukyo は中間（edge +3.54pp）。3 cell とも学習窓 2013+ 内の代表率はバラバラ（Chukyo 96%・Niigata 67%・Fukushima 60%）で under-representation と深刻度の相関なし（データ不足仮説を棄却）。3 cell とも avg field size が cat avg（14.6）より高い（15.5〜16.0）——既 REJECT の `project_jra_field_difficulty_reject_2026_06_23`（xl(>16) top1 36.8% vs small 50.4%）と同根の可能性が高い。
    2 lever を `train_finish_position_catboost_walk_forward.py` の 3-fold WF（train≤2022/2023/2024→blind2023/2024/2025、bootstrap LB95 2000 iterations、production の 263 feature_names のうち 254 列が一致する `tmp/candidate-eval-jra/augmented-fixed` store を使用、baseline も同一 harness で再現）で検証、両方 REJECT。
    - **lever (a) — 対象 3 cell のみ `--alpha-bucket-weight` upweight**（NAR venue45/30 upweight の adaptation、対象は全レースの 2.0%=898/45,151 races と NAR の約12%よりはるかに狭い母集団）: CatBoost が全 fold で `"Pairwise losses don't support object weights."` を出す（YetiRank は per-object weight 非対応、NAR の XGBoost `rank:pairwise` とは異なる既知の制約——過去の bucket-upweight lever が NAR でしか試されていない理由と整合。ただし model.json は baseline と非同一・md5 相違で完全 no-op ではない）。alpha=0.75（許容上限）で実 A/B（pooled 3-fold, target cell n=217）: multi-metric gate FAIL（primary 改善 1/3）、target no-regression FAIL（place3 **-3.23pp** / place4 **-1.38pp**）、rest-of-JRA（n=10,148）no-regression も FAIL（place6 -0.38pp / top3_box -0.18pp）、LB95 primary 0/3。上限 alpha で既に負なので低 alpha は既存効果を薄めるだけで符号反転しない。NAR の 2 失敗モード（restrict=fragmentation／upweight=dilution）とは別の第 3 の失敗モード：YetiRank の row-reweight は exact-ordinal 指標に構造的に不向き。DO-NOT-RETEST（JRA/Ban-ei の CatBoost+YetiRank 全般に適用、group_weight へ切り替えない限り）。
    - **lever (b) — 新規特徴量: calendar-month cyclical encoding（`race_month_sin`/`race_month_cos`）を global model に追加**: 本番 263 `feature_names` を監査し season/month 相当の入力が皆無であることを確認（`weather_normalized`/`current_baba_condition` は当日実測値であり暦日情報ではない）——既 REJECT の天気 12 列・kohan3f-going・field-difficulty・barei とは異なる角度。実 A/B（pooled 3-fold, target n=217）: multi-metric gate は一見 PASS（top1+2.30pp/place2+2.76pp）だが target no-regression FAIL（place3 **-3.23pp**/place5-0.46pp/top3_box-0.92pp）、rest-of-JRA（n=10,148）no-regression も広範囲 FAIL（top1-0.07pp/place3-0.08pp/place4-0.39pp/place5-0.35pp/place6-0.34pp/top3_box-0.51pp）、LB95 place3 target -6.91pp。同日並行の Ban-ei season_band 調査（本節上記参照）でも同型の calendar-season 特徴量が同じ理由（GBDT が weather/baba-condition 相関特徴経由で season 相当を既に吸収済み）で REJECT されており、JRA でも同一メカニズムの再現と解釈できる。DO-NOT-RETEST。
    - **総括**: 2 lever とも REJECT、本番 `jra-cb-v9-sim-2013` 無変更。Niigata のみ market 比較上 real gap の可能性が残るが、既存 4 campaign（weather/field-difficulty/kohan3f-going/barei）に続く corroborating failure であり、DO-NOT-RETEST とする。唯一残る未検証 open idea: レース運営側のメタデータ（コース設定・rail position 等、現行データソースに存在しない）を新規に取り込む場合のみ再検討の余地がある。
  - **弱 cell 4 件目: `jra_turf_mile_unknown_autumn_08`（Kyoto）2 lever 深掘り REJECT（2026-07-02、DO-NOT-RETEST）** — 独立再確認: race_count=330、top1Δ-5.45pp/place2Δ+0.62pp/place3Δ-1.69pp/top3_boxΔ-1.29pp で完全一致。market oracle 比較: fav_top1 31.21%（cat avg比 Δ-1.27pp、市場はやや弱い程度）に対しモデル top1 Δ-5.45pp——model edge が+7.82pp（cat avg）から+3.63pp へ縮小、real gap。既 3 cell と異なり avg field size は 14.61（cat avg 14.64 とほぼ同一）で field-difficulty 機序（`project_jra_field_difficulty_reject_2026_06_23`）は非該当。京都競馬場は 2021-2022 年に実在する約 2 年間の改修工事クローズ（ローカル jvd_ra ミラーで keibajo_code=08 の kaisai_nen 2021/2022 が 0 レースと確認、2023 年 4 月 22 日に再開）を経ているが `track_code`（17/18）は改修前後で変化なし——モデルはコース変更を示す入力を一切持たない。年別内訳（n=11-24/年、ノイズ大）は改修前（2007-2020 累積 n=281、top1 11.1-57.1%）に対し改修後（2023-2025、n=49）top1 36.8/30.8/17.6%（平均約28.4%、示唆的だが統計的に頑健ではない）。track condition は cell 内 non-firm turf 比率 23.6% vs category 19.9%（+3.7pp、modest）。
    2 lever を `train_finish_position_catboost_walk_forward.py` の同一 3-fold WF harness（train≤2022/2023/2024→blind2023/2024/2025、bootstrap 2000 resamples、254/263 特徴量一致 store）で検証。**重要な制約**: このcellは3 blind年以内でrace_count=49 しかなく（`MIN_RACES=200` 未達、`min_races_ok: false`）、cell 単独では統計的に頑健な結論に到達できない——先行 3 cell（Chukyo 452/Niigata 453/Fukushima 261、いずれも pooled で >=200）より遥かに希少。
    - **lever (a) — `venue_hiatus_days`（venue非依存の汎用グローバル特徴量、Kyotoハードコードなし）**: 全 JRA venue・全期間（1950年代〜）の実レースカレンダーから「このvenueの前回開催日からの経過日数」を leak-free に算出（2023-04-22 の京都再開レースで確認通り 400日上限に到達、閉鎖が実際に検出可能）。Kyoto cell 単独（n=49、min_races_ok=false）: top1Δ-2.04pp/place6Δ-2.04pp、LB95 全指標負（top1 -6.12pp/place3 -4.08pp）。rest-of-JRA（n=10,148）no-regression も FAIL（top1 -0.21pp/place4 -0.49pp/place5 -0.27pp/place6 -0.18pp）。先行 3 cell プールでも同様 REJECT（target no-regression fail: place3 -2.30pp/place4 -1.38pp、LB95 primary 0/3）。DO-NOT-RETEST。
    - **lever (b) — `baba_x_mile_turf`（track-condition × distance-band interaction、グローバル特徴量）**: `current_baba_condition`（1=良〜4=不良）を turf mile（1200-1599m）でのみ有効化、他は0。multi-metric gate は primary 0/3 改善で即 FAIL、Kyoto cell（n=49）no-regression は top1 -2.04pp/place2 -2.04pp/place3 -4.08pp と 3 primary 全て悪化、rest-of-JRA（n=10,148）も place3/place4/place6 で regression。既 REJECT の天気・kohan3f-going と同型（CatBoost depth=8 は `current_baba_condition`×`kyori` の native tree split で同等の interaction を既に再構成済みで、明示的な積特徴量は redundant noise）。DO-NOT-RETEST。
    - **総括**: 2 lever とも REJECT、本番 `jra-cb-v9-sim-2013` 無変更。Kyoto は market 比較上 real gap の可能性があり、renovation/hiatus 仮説も構造的に妥当だが、3 blind 年以内の race_count=49 という根本的サンプルサイズ制約により、このcell規模でのWF検証は原理的に検出力不足——今回の REJECT は「効果なし」と「検出できない」の両方を反映する。既存 4 campaign（weather/field-difficulty/kohan3f-going/barei）+ 前回 3-cell 深掘りに続く corroborating failure。DO-NOT-RETEST。
  - **per-cell 学習窓 routing REJECT（508 比較・0 採用、2026-07-02 深夜、DO-NOT-RETEST）** — USER 指示（「JRA の年の範囲/レース数を cell ごとに増減できるようにし、固定の最低年数/最低レース数を設定」）に基づく検証。既に確立済みの global 一律窓 2013+ 最適（2012+/2014+ REJECT 済み）は「全 cell 同一窓」の話であり、**cell ごとに異なる窓を routing する構成は未検証**だったため正式に検証した。結論: **JRA 2013+ は global だけでなく per-cell でも frontier**——cell 別に窓を変える余地はない。本番 `jra-cb-v9-sim-2013` 無変更。
    - **固定下限（実装）**: `MIN_TRAIN_YEARS=4`（各 blind fold の学習窓は fold 開始年から遡り最低 4 暦年を含む。2019+ は fold2023 でちょうど 4 年に達する＝生成される最狭窓）、`MIN_RACES=200`（selection 窓内で race_count>=200 の cell のみ routing 候補）。年範囲ベースを採用——レース数ベースより fold 境界の leak 管理が明確、かつ固定開始年は serve で決定的（「最新 N races」は race-day ごとに drift し baked-model 契約を複雑化するため）。
    - **検証設計**: 5 窓 variant（2006+ / 2010+ / 2013+ baseline / 2016+ / 2019+）を `jra-cb-v9-sim-2013` verbatim で 3-fold WF 学習。狭窓（2016/2019）は 254-feat rich store、広窓（2006/2010）は rich store が 2013+ 分しか無いため 126-feat の self-consistent reduced 空間で probe。多重比較防御として **selection（fold 2023+2024）→ blind confirm（fold 2025）** の 2 段構成。
    - **結果**: 狭窓 254 比較（127 cell × 2 variant）＋ 広窓 254 比較 ＝ **508 比較・0 採用**（`n_selection_passed=0` / `n_confirmed=0` / routing rules 0、blind 2025 の routed eval は base==routed の完全 no-op: n=3,455 races、top1 37.74pp Δ0.00）。最も惜しい狭窓 cell（`class_label`=E × summer × 2019 窓）は primary 2 つが LB95>0 だが rank -1.74pp 回帰（top1 -0.74pp）で no-regression fail。global reduced 空間でも広窓は全 fold で baseline 以下（w2010 top1 -0.25 / place2 -0.23 / place3 -0.23、w2006 top1 -0.06 / place2 +0.16 / place3 +0.52 だが全 LB95<0）——確立済みの「2012+ REJECT / 広げると JRA は悪化」を reduced 空間でも再現。広窓 2006+ の global place3 +0.52pp[LB95 -0.015]が唯一の方向性ヒントだが有意未達で、cell 別 blind confirm で消失。
    - **結論と成果物**: 254-feat の高コスト store 再生成（pre-2013 分）は prior 低く非推奨——marginal な reduced-space place3 ヒントは本番 263-feat モデルの追加 137 特徴（course/career/damsire-baba 系）が既に捕捉している可能性が高い。cell_routing.json への JRA window-variant routing 反映機構は Ban-ei と同型で整備済み（採用ゼロなので現状 no-op、将来 signal 用の runbook あり: 本番 263-feat pipeline で full-data 再学習 → R2 bake → cell_routing.json に jra block → docker rebuild → deploy、cell 次元 `class_label` は serve の「class」次元にマップ）。artifact: `apps/pc-keiba-viewer/tmp/candidate-jra-cellwindow/`。本番無変更。
- **NAR**: feature / 学習窓 / 単一アーキ切替（CatBoost）/ venue routing の各 lever はいずれも REJECT で、これらの軸では iter12 XGBoost が frontier。ただし **アーキテクチャ lever（listwise Set Transformer × XGBoost blend）は本物の deployable win（2026-07-02 深夜に gate 通過、2026-07-03 に `iter40-nar-settransformer-blend-v1` を本番 image へ deploy → Neon-write smoke CLEAN → Cloudflare production path で ENABLED → 初回 genuine 本番 write 確認、本キャンペーン初のアーキテクチャ lever win）** であり、この 1 点で従来の「NAR frontier 確定」を更新する。現行 NAR serving = iter40 transformer blend（2026-07-03 production LIVE 確認済み、同日 fusion を rank → score-level z-fusion に更新＝ deployed win #2）。**アーキテクチャ lever から 2 つの deployable win が本番稼働**（rank-fusion +0.63pp top1 / score-z +0.25pp top1、下記 score-z ブロック参照）。アーキテクチャ lever の全 knob（architecture variant c1/c2/c3・fusion 方式 rank/score-z・blend weight tw・per-cell routing・seed 数）を検証完了し、**deployed = 3-seed c2 listwise + score-z fusion が最適配置**と確定（seed 数 sweep は下記 seed 数 sweep ブロック参照、3-SEED-SATURATED）。
  - **listwise Set Transformer × XGBoost blend = genuine deployable win（gate 通過 2026-07-02 深夜、prod-base gate + serve-exact gate 通過・2026-07-03 Cloudflare production ENABLED = iter40、CF env=1、本キャンペーン初のアーキテクチャ lever win）** — 過去の全アンサンブル campaign（`project_ensemble_campaign_complete_2026_06_18`、5-for-5 ABORT）に反し、**単一アーキテクチャの swap ではなくレース内 cross-horse attention を持つ listwise Set Transformer を XGBoost に blend する**という新しい lever が NAR で初めて accept gate を通過した。
    - **手法**: `RaceSetTransformer`——レース内の各馬を token とし multi-head self-attention（cross-horse set attention）で馬同士を相互参照させ、listnet（listwise）loss で学習（MLX / Metal GPU）。これを単体でなく本番 `iter12`（XGBoost）と **within-race rank fusion（0.5 / 0.5 weight）** で blend し、3-seed ensemble を取る。
    - **prod-base gate（deploy 判定用、`gate_prodbase.json`）**: baseline = `iter12-nar-xgb-hpo-v8` の HPO params を verbatim 移植し、150-feat store（`feat-nar-v9-new`=117+33、on-disk で本番 192 特徴量契約に最も近い parity）で学習した XGBoost（＝本番 serve base の忠実な再現）。blend vs この prod-base、pooled blind 2023/2024/2025（n=40,710 races）、2000-boot LB95: **top1 Δ+0.641pp[LB95 +0.477]／place2 Δ+0.445[+0.219]／place3 Δ+0.688[+0.462]／place4 Δ+0.585[+0.354]／place5 Δ+0.486[+0.233]／place6 Δ+0.501[+0.238]／top3_box Δ+0.835[+0.653]／fukusho_2p Δ+0.415[+0.309]——8 指標すべて LB95>0、回帰ゼロ**。per-fold も 3 fold とも 3 primary の点推定が正（2023 +0.63/+0.42/+0.45、2024 +0.77/+0.60/+1.16、2025 +0.52/+0.31/+0.45pp。2025 place2 のみ LB95 が僅かに負 -0.067 だが点推定 +0.313 は正）。transformer 単体（standalone）も勝つ（top1 Δ+0.857[LB95 +0.565]）。cell 単位（n>=200）58 cell 中 top1 改善 45／place3 42／top3_box 41＝broad な system-wide gain（cell 平均 top1 Δ+0.77pp）。
    - **統制 4 種すべて通過（過去の全 ABORT との区別）**: (a) **NULL control**——XGB の seed1+seed2 を同じ within-race rank-fusion した対照は top1 Δ≈+0.00〜+0.08pp・全 LB95<0 でほぼゼロ。したがって blend gain は「2 model を混ぜれば出る汎用アンサンブル多様性」ではなく **transformer 固有の直交 signal**（過去のアンサンブル campaign が全 ABORT だった理由＝汎用多様性では NAR で gain が出ない、との明示的な区別）。(b) **3-seed 頑健**（`final_summary.json` の listwise 3-seed `c2_seedmean3` blend top1 Δ+0.582[+0.423]、単一 seed でも符号安定）。(c) **arch 頑健**——pairwise / listwise / big いずれの transformer variant も seed-mean blend で ADOPT（listwise 3-seed `c2_seedmean3` が全 3 primary ADOPT で最良、big `c3_big` blend は top1+place3 ADOPT、pairwise 2-seed `c1_seedmean2` blend も top1+place3 ADOPT）、**listwise が最良**。(d) **fairness**——race-meta（keibajo / month embedding）を OFF にしても win 維持（`c2nm_seedmean3` blend top1 Δ+0.585[+0.425]／place2 Δ+0.400[+0.194]／place3 Δ+0.381[+0.145]、全 3 primary ADOPT）。よって追加特徴（race メタ）ではなく **アーキテクチャそのものの勝ち**。
    - **機構（なぜ NAR で効くか）**: XGBoost の木は per-horse の特徴量ベクトルしか見えず、Set Transformer の「レース内で馬同士を相互参照する attention」は木が構造的に表現できない。117→150 feature に増やしても edge が縮まらなかった＝ per-horse 特徴を足しても埋まらないギャップで、**薄市場の NAR では cross-horse 構造が exploitable**。これは本節の他 REJECT 群が繰り返し確認してきた「market が織り込み済み」の壁の裏返し——市場効率の低い NAR でのみアーキテクチャ lever が edge を残す。
    - **JRA では転移しない（同夜・別 agent 検証）**: 効率的市場・254-feat CatBoost base の JRA では blend top1 Δ+0.106[LB95 -0.232]／place2 Δ+0.251[-0.164]／place3 Δ+0.183[-0.318] と全 primary LB95<0、transformer 単体は top1 Δ-0.598 と大敗。旧 prior「MLX transformer は JRA で GBDT に劣る（11 iter 実測、`project_mlx_transformer_status`）」と整合。**アーキテクチャ lever は市場効率の低い NAR 固有**という重要な非対称性（JRA で効かないこと自体が「市場効率の壁」を裏側から実証）。Ban-ei も別 base（pairwise-XGB）で検証したが REJECT——arch lever は 3 カテゴリ全て検証完了（NAR deployed / JRA・Ban-ei REJECT。§11 Ban-ei 節の「pairwise-XGBoost base + Set Transformer blend REJECT」を参照）。
    - **memory 訂正**: `project_mlx_transformer_status`（「MLX transformer は GBDT に劣る、NAR 未試行」）は **NAR について覆された**——listwise blend + null-control 手法が有効。JRA での劣後は引き続き有効。
    - **deploy 状況（正確に、2026-07-03 Cloudflare production ENABLED = `iter40-nar-settransformer-blend-v1`、CF env=1）**: 本番着順予測は CF Container 経由必須（§1 / §9）で、当初 **MLX は Container で動かず、ONNX 変換は torch 非在で不可、numpy forward は MLX 出力と corr 0.998 だが bit-exact でなく、接戦レースで順位 flip の serve-skew リスクがある** ことが deploy blocker だった。これを **pure numpy float64 の serve-exact scorer（`predict_lib/transformer_scorer.py`、MLX-eager に byte-exact ＝順位 flip 0）で解消**し、serve-exact な ketto tie-break gate を再実行して ADOPT を確認（eval==serve を保証）した上で本番 image へ deploy、**full 本番 Neon-write smoke CLEAN 後に Cloudflare production path で env=1 有効化**した（CF worker = wrangler secret、commit `28c89abf`、worker Version `0d2d99c7`。**現行 CF production の NAR serving は iter40 transformer blend**。deploy 記録・検証・rollback は §5.9 の 2026-07-03 ブロック、モデル仕様は §2.5 を参照）。過去の phantom deploy（serve と eval の乖離、§2.3 / `project_rs_calibration_deployed_2026_06_12` の教訓）に従い serve-exact 確認を経ているため、eval==serve が保証される。**2026-07-03 に初回 genuine 本番 write 確認済み（production LIVE）**——Cloudflare production path の per-race full serve が 48 NAR レース（499 頭行）を iter40 で本番 Neon に書き込み、viewer は当日 48 NAR レース全てで iter40 を表示（48/48）。**同日、fusion 方式を rank-fusion → score-level z-fusion に更新（deployed win #2、上記 score-z ブロック参照。model_version は iter40 維持）——現行 serving は score-z fuse**。
    - **artifact**: `apps/pc-keiba-viewer/tmp/candidate-mlx-nar/`（`gate_prodbase.json` / `final_summary.json` / `eval_prodbase.py` / `numpy_forward.py` / `v9_feature_list.json` / `ckpt/c2v9_s*`）。serve-path parity（serve-exact numpy scorer）解決 + Neon-write smoke CLEAN 後に `iter40-nar-settransformer-blend-v1` を本番 image へ deploy し Cloudflare production path で env=1 有効化——現行 NAR serving は iter40 transformer blend（2026-07-03 production LIVE 確認済み、同日 fusion を score-level z-fusion に更新＝ deployed win #2、§2.5 / §5.9）。
    - **per-cell routing + blend-weight sweep = GLOBAL-0.5-OPTIMAL（2026-07-03、DO-NOT-RETEST）**: deploy 済み iter40 は全 NAR で global 0.5 / 0.5 rank-fusion。cell 分析で 58 cell 中 45 で top1 改善・13 でわずかに悪化していたため、その refinement として (a) per-cell routing（効く cell だけ blend、悪化 cell は iter12 base に fallback）と (b) blend weight tw sweep を検証した（既存 WF fold predictions を再利用し再学習ゼロ、selection 2023+2024 → blind 2025 confirm の 2 段階、serve-exact な ketto tie-break で eval==serve を保証）。両 refinement とも本番を上回らず、現行 global 0.5 / 0.5 配置が最適と確認。本番 iter40 無変更。artifact: `apps/pc-keiba-viewer/tmp/candidate-nar-tf-percell/`。
      - **per-cell routing REJECT**: selection で「blend が top1-negative」な cell は 5/35 のみで全て LB95 が大幅マイナス＝ノイズ、両 selection 年で安定して負は 1 cell（`nar_dirt_mile_unknown_autumn_46`）のみ、それも blind 2025 で符号反転（selection −0.60 → blind +0.78）。routed（strict 1 cell / lenient 5 cells）vs global-blend は top1 −0.015 / −0.030 で本番を上回らず、全 LB95 が 0 跨ぎ。
      - **tw sweep REJECT（WEIGHT-CHANGE 却下）**: selection（23+24）では tw=0.6 が全 primary LB95>0 に見えたが、blind 2025 confirm で tw=0.6 vs 0.5 = top1 +0.082[LB95 −0.127]／place2 +0.127[−0.157]／place3 +0.223[−0.082]、点推定は全て正・regression ゼロだが primary LB95 が全て 0 跨ぎ → §7.3 の HPO 選択バイアス則で却下。tw=0.5 は意図的に non-tuned として選ばれており、blind がその妥当性を裏づける。（注: tw=0.6 は directional・no-regression・low-risk で、`fuse_ensemble_transformer` の weight 定数 0.5→0.6 のみのコストで serve smoke だけの低リスク bump 候補だが、evidence 上は非推奨。）
    - **score-level z-fusion = 2026-07-03 本番 deploy + LIVE 検証済み（deployed win #2、rank-fusion iter40 → score-level z-fusion に更新、model_version は iter40 維持）** — deploy 済み iter40 の within-race **rank-level** 0.5 / 0.5 fusion（各馬の within-race rank を平均）を、**score-level z-fusion**（base score と transformer seed-score をそれぞれ race 内で z-正規化してから 0.5 / 0.5 で混ぜる ＝ rank が捨てる confidence magnitude を保持）に **本番差し替え済み**。既存 WF fold predictions を再利用し**再学習ゼロ**で gate を通し（serve-exact な ketto tie-break で eval==serve を保証）、当初 3-fold では place3 の pooled LB95 が境界（-0.02）で **HOLD** としていたが、**2021 / 2022 の blind fold を追加して 5-fold pooled（66,883 races）で CONFIRMED**（全 3 primary LB95>0・regressor ゼロ）。**これがアーキテクチャ lever 由来の 2 つ目の deployable win**（1 つ目＝ rank-fusion iter40 の +0.63pp top1、2 つ目＝ rank→score-z の +0.25pp top1）。artifact: `apps/pc-keiba-viewer/tmp/candidate-score-fusion-confirm/`（5-fold fusion tables / 2021-2022 base+transformer preds / `eval_confirm.py`）。deploy 記録・検証・rollback は §5.9 の 2026-07-03 score-z ブロック。
      - **5-fold pooled で全 3 primary が LB95>0**: deployed rank-fusion を baseline とした 5-fold pooled（66,883 races、2000-iter LB95）で top1 Δ+0.253[LB95 +0.120]／place2 Δ+0.341[+0.166]／place3 Δ+0.230[+0.051]、**{top1, place2, place3} 全 3 primary で LB95>0・regressor ゼロ・全 5 fold で符号一致**。place3 が power 追加で 0 を頑健に上抜けした ＝ 3-fold での境界は**過学習ではなく検出力の問題**だったと確定（20 bootstrap seed でも place3 LB95 が全て >0、seed 42 非依存）。`score_z_55`（z-seedmean）/ `score_zperseed_55`（z-perseed）両変種とも CONFIRMED。cell（31 cells、n>=600）でも top1>0 が 22/31・place2>0 が 17/31・place3>0 が 16/31 の broad-thin な改善。
      - **機序**: rank-level fusion は magnitude を捨てるが、score-level z-fusion は transformer の確信度ギャップ（within-race の score 差）を保持する。3 種の正規化方式（z-seedmean / z-perseed / minmax）が全て同方向に改善するのは機序的。tw sweep は §7.3 通り REJECT（0.5 固定が最適、tune は選択バイアス）。borda（等重み rank）は deployed と数値完全一致（rank-fusion 等重み ＝ Borda 等重みを確認）。
      - **deploy 済み（commit `a90161f4`、CF production path で score-z 稼働）**: container pkg で `transformer_scorer.py::fuse_ensemble_transformer` を rank→score-z に変更（`within_race_zscore` helper 追加、variant `score_z_55`）、`predict_upcoming.py` の NAR blend は `seed_score_mean` + score-z fuse を使用、3-tier fail-closed fallback は維持、coverage 100%。model / norm.json / R2 artifact / Dockerfile / 再学習はすべて無変更で、既存 flag（`NAR_TRANSFORMER_BLEND_ENABLED=1`）の裏で完結。反映は **CF worker** Version `8706eb93-da31-4f97-8a6d-ac9c43a09392`（image `finish-position-cron-finishpositionpredictcontainer:8706eb93`、旧 `6a4d1fa3` を replace）、crons / queue / secret / observability は intact。**rollback**: CF secret を 0 に戻すと pure iter12、または commit `a90161f4` revert で rank-fusion iter40 に戻る。
      - **base-selection scope は解消（当初 caveat は moot）**: 5-fold confirm は harness の iter12 base に対してだが、実 serving の per-class ensemble（iter12 + iter30 + iter36）は import されるものの **`predict_upcoming.py:640` で unwired ＝ `score_races` は単一 `iter12-nar-xgb-hpo-v8` fallback booster で fuse する**。よって gate が使った iter12 base と実 serving base が完全一致し、当初の「実 ensemble base での plumbing smoke が必要」という caveat は moot——score-fusion は厳密に実 serving base に対して confirmed。
      - **live 検証済み（本番 Neon）**: 過去日 NAR race（`kb55` r3、20260627）を Cloudflare production image / worker path で実行し、本番 Neon `race_finish_position_model_predictions` に score-z の iter40 予測 11 行が landing（05:00:29 JST）。同 race の旧 rank-fusion 保存行と rank が 3/11 相違（同一 3-seed transformer で fuse のみ変更）＝ score-z が本番稼働していることを確認、fallback も intact。model_version は iter40 維持（before/after は timestamp で区別、accuracy 証拠は offline 5-fold gate）。
    - **transformer seed 数 sweep（3 vs 5 vs 7-seed）= 3-SEED-SATURATED（2026-07-03、DO-NOT-RETEST）** — deployed iter40 は 3-seed listwise Set Transformer ensemble。追加 4 seed を学習し 5-seed / 7-seed の score-z fusion を deployed 3-seed score-z と 5-fold pooled（66,883 races）で比較。**5-seed / 7-seed とも本番を上回らず、seed 数は knob として枯渇**、本番 iter40（3-seed score-z）無変更。artifact: `apps/pc-keiba-viewer/tmp/candidate-nar-tf-seeds/`。
      - **絶対 top1**: 3-seed 59.489% / 5-seed 59.537% / 7-seed 59.540%（微増のみ）。
      - **5-seed vs 3-seed（deployed score-z baseline、5-fold pooled 66,883 races、2000-boot LB95）**: top1 Δ+0.048[LB95 -0.033]／place2 Δ-0.061[-0.175]／place3 Δ+0.039[-0.073]、§7.2 primary LB95>0 = 0/3 で REJECT。
      - **7-seed vs 3-seed（同 baseline / pool）**: top1 Δ+0.051[-0.036]／place2 Δ-0.036[-0.155]／place3 Δ+0.021[-0.103]、primary 0/3 で REJECT。point delta は全 primary で |≤0.06pp|、place2/place6 の微負は per-fold 非再現の bootstrap noise、per-cell は coin-flip（top1>0 15/31）で方向シグナルなし。
      - **fold-std variance の収束確認**: fold-std は top1 が 3→7 seed で 0.424→0.356、place2 0.499→0.391 と僅かに縮むが place3 はむしろ増加（0.253→0.350）、かつ縮小分は pooled 精度 gain に一切変換されない ＝ **3-seed で ensemble variance は既に実質収束**、seed 追加は knob として枯渇。
    - **transformer 入力への直交 2 次モーメント特徴量（finish-variance）追加 = REJECT（2026-07-03、DO-NOT-RETEST）** — 本キャンペーンの特徴量テストは全て GBDT を相手にしていたため、overnight mega-probe（本節下記）が「2 次モーメントの finish-variance（`fn_std_5` / `fn_std_10` / `fn_cv_5`）は place4-6 / top3_box を頑健に改善するが GBDT の exact top1-3 は動かせない」と結論づけた直交特徴量を、今度は **transformer 入力**（117→120 feat、deployed `c2nm` arch と同一構成、3-seed × 3-fold WF）に追加し、deployed 117-feat transformer と比較した。leak-free（serve key 一致 join・prior-rows-only rolling・coverage 88-92%）。
      - **score-z fusion**: top1 Δ-0.015[LB95 -0.169]／place2 Δ-0.138[-0.349]／place3 Δ+0.111[-0.108]、primary LB95>0 = 0/3、place2 / place6 / top3_box が regression、per-fold で符号不安定。**rank-fusion** に切り替えても top1 Δ+0.096[LB95 -0.047]／place2 Δ-0.069 で REJECT——fusion recipe を変えると place3 の符号すら反転する（ノイズ水準）。
      - **結論**: **レース内 cross-horse attention をもってしても 2 次モーメントの finish-variance を exact top1-3 の gain に転換できない**——mega-probe が GBDT 側で確認した「2 次モーメント signal は深い着順（place4-6）だけを tighten し exact top1-3 は不動」という壁は、アーキテクチャ lever（transformer）でも同型に立つ。NAR frontier を補強、本番 iter40 無変更。artifact: `apps/pc-keiba-viewer/tmp/candidate-nar-tf-features/`。
  - venue routing REJECT の内訳（失敗モードが異なる 2 系統、いずれも DO-NOT-RETEST）:
    - **venue-restricted specialist（43/44）** — venue 単体に絞った孤立学習は、その venue 自身の精度でも global model に劣後（fragmentation）。
    - **venue upweighting（bucket-aware mixing, non-restrictive, 45/30、2026-07-02）** — 43/44 の fragmentation を避けるため restrict ではなく `--alpha-bucket-weight 0.75` で venue 45/30 を 2006+ full cross-venue 学習内で upweight する別系統の lever を検証。40,710 race の実 bootstrap paired 評価（`build_cell_models.py` の `compute_deltas` / `check_multi_metric_gate` / `check_no_regression` / `bootstrap_lb95`、2000 resamples）で venue45 Δtop1 -0.75pp（LB95 -3.61）・venue30 Δtop1 +0.03pp（LB95 -2.43）、いずれも primary metric ≥2 改善に届かず gate FAIL。決定打は他 88% の NAR venue（35,469 race）で no-regression 指標が 7/7 悪化（LB95 top1 -0.85pp）——upweight が損失関数の注意を他 venue から奪う、43/44 の fragmentation とは別の失敗モード。venue 45/30 の bucket-aware upweighting は DO-NOT-RETEST。
    - **`H-RS-KEIBAJO-IMPUTE`（venue30=門別）REJECT（2026-07-02、DO-NOT-RETEST）** — venue 30 は oracle top1 が全 NAR venue 中最高（41.23%）かつ locally-anchored-horse rate も高い（0.803）という異常な組合せを持ち、venue-specific class/distance priors で NULL pacestyle / corner-history 特徴量を埋める修正として先行 session から open idea に残っていたが、正式検証して REJECT。venue30 の NULL 構造を実測: `past_nige_rate_self` 系 44.9%（rest 8.2%）、`past_corner_1_norm_avg_5` 59.4%、`target_running_style_class` 74.0% NULL だが raw の `corner_pass_avg_5` は 8.8% しか NULL でない——**門別は corner-1 通過位置／脚質分類を構造的に記録しておらず**（era 効果でも debut artifact でもなく、blind 2023-25 でも高止まり）、欠損は per-horse early-corner 信号そのもの。先行教訓（in-place impute は逆効果）に従い additive companion 4 列（venue30×nar_subclass×kyori_band の expanding-by-year prior、min-support 20、venue30 行のみ populate、leak-free）で検証。3-fold WF（iter12 verbatim、117 reduced baseline）: venue30 cell（n=2,969）で place3 Δ-0.573pp[LB95 -1.314] の明確な回帰、top1 Δ+0.573[LB95 +0.000 で有意でない]、gate 不成立で CELL-CONDITIONAL routing も不可。rest-of-NAR（n=37,741）は全 metric noise 水準。**機構的核心（新教訓）**: venue×class×distance prior はレース内で定数のため、pairwise/YetiRank 系 ranker は当該特徴で within-race の並べ替えを学習できず構造的に不活性。唯一の within-race-varying 代替（`corner_pass_avg_5`、global sire/jockey style rate）は既に baseline に存在＝GBDT の informative-absence routing の実体。venue30 の弱さは leak-free な per-horse fill が存在しない構造的ギャップで、companion 系でも救済不可。artifact: `tmp/candidate-nar-v30impute/`（`null_structure.json` / `pooled_report.json` / `cell_report.json`）。
  - **small-field（`shusso_tosu<=8`、NAR レースの約 20%）CatBoost routing REJECT（2026-07-02 再検証、DO-NOT-RETEST）** — 2026-06-24 に非公式（ephemeral memory のみ、未commit）に top1 +1.02pp / place2 +0.72pp と記録されていた候補を、`train_finish_position_catboost_walk_forward.py` / `train_finish_position_xgboost_walk_forward.py`（XGBoost 側 hyperparameter は `iter12-nar-xgb-hpo-v8` の `metadata.json` から verbatim 移植）で現行 192 特徴量契約に対し正式な 3-fold WF blind test（train ≤2022→blind 2023 / ≤2023→blind 2024 / ≤2024→blind 2025、bootstrap LB95 2000 iterations）で再検証したところ再現しなかった。pooled（n=8,306 races、2023-2025）で top1 delta 0.00pp（LB95 -0.46）、place2 +0.10pp（LB95 -0.53）、place3 -0.81pp（LB95 -1.45、3 fold 全てで単独負: 2023 -0.67 / 2024 -1.13 / 2025 -0.60pp）。§7.2 accept gate の 2 条件（primary の 2/3 が LB95>0、no-regression -0.05pp 以内）を両方外れる明確な REJECT。cell（surface × distance_band × class × season × venue）内訳でも LB95>0 の頑健な positive pocket なし。2026-06-24 の非公式な positive 読みは、現行 192 特徴量契約より少ない（138 または 142 特徴量、旧 feature-store 世代）CatBoost 構成での ad-hoc 評価であったためと考えられる。small-field CatBoost routing は DO-NOT-RETEST。
  - **nvd_nu 血統パイプライン修正の再学習効果 REJECT（2026-07-02、DO-NOT-RETEST）** — `pedigree_staging.py` / `finish_position_features_duckdb.py` が NAR/Ban-ei 血統を `nvd_um`（JV-Data mirror）のみで解決し、2023 年以降 `nvd_um` カバレッジが崩壊（2022=98%→2023=83%→2024=52%→2025=30%→2026=21%）していた一方、`nvd_nu`（N-Data native、121,663 行）は全年 100% カバレッジという実在するデータバグを確認（`nvd_nu` 統合修正自体は commit f8db119d/3f97d0f6/12569219 で既に 2026-06-23 に committed 済み）。iter12 本番モデル（192 feat、2026-06-04 学習）はこの修正**前**の血統データで学習されており、`sire_distance_win_rate` 等の NULL 率が 2024 年 39.1%／2025 年 63.9%／2026 年 76.6% に達していた。同一 192 特徴量・同一ハイパーパラメータ（`iter12-nar-xgb-hpo-v8` の `metadata.json` から verbatim）で、血統由来の約 15 列（`sire_distance_win_rate` 系 10 列 + `pedigree_score_for_race` 系 4 列 + `sire_x_field_pace_score` + `rs_sire_style_match`）だけを現行修正済みパイプラインで再計算し他は一切変更しない厳密な controlled ablation を実施（`sire_baba_*` 系 5 列は対象外、conservative な過小評価）。修正後は NULL 率 2024/2025/2026 とも 3%未満に改善（データ修正自体は確認済み、genuine）。3-fold WF blind test（bootstrap LB95 2000 iterations、pooled n=40,710 races）で top1 diff +0.049pp（LB95 -0.093）、place2 +0.103pp（LB95 -0.093）、place3 +0.029pp（LB95 -0.174）、place4〜6 も全て LB95<=0。primary 3 指標のうち LB95>0 は 0/3 で §7.2 accept gate 不成立、cell 単位（surface×distance_band×class×season×venue、n>=200）でも 22 cell 中 ADOPT 0 件。結論: データバグ自体は本物だが、モデル精度への実効果はゼロ（iter12 は既存の NULL/informative-absence を吸収済み、または血統シグナルが他 177 特徴と冗長）——2026-06-12 の G-1/F1 near-miss 再学習 REJECT（`project_nar_g1f1_combined_adopt_2026_06_12`）と同型の教訓。本番 iter12 は無変更。パイプライン修正（`nvd_um`→`nvd_nu` fallback）自体は correctness fix として既に committed 済みで維持（regression なし）。血統再学習は DO-NOT-RETEST。副産物として `finish_position_xgboost.py` の `train_xgboost_ranker` が `xgboost>=3.2.0` の ranking group 検証（per-row weight 非対応）で crash する既存バグを発見・修正（commit 5bf13197、`group_weights_from_row_weights` で race 単位に reduce）——sample weight を付ける今後の NAR/JRA XGBoost WF 学習すべてに影響する独立した修正。
  - **cross-source 転入馬履歴 companion 列 REJECT（2026-07-02、DO-NOT-RETEST。本日の最有力 near-miss として詳細記録）** — （恒久的な事実確認）`finish_position_features_duckdb.py:2986` の horse-history self-join は `h.source = t.source` で厳密に source-scoped（`horse_career_cte`:1354・`partner_history_cte`:1383 も同様）。その結果 NAR エントリの **57.52%**（2024-2025、n=281,788）が JRA での prior 戦績を特徴量に一切反映しないまま走り、thin-transfer 層（当 source ≤3 走）が 8.44%、**NAR-debut かつ ex-JRA（career 特徴が全 NULL だが JRA フル戦績あり）が 2.28%**（6,432 entries）存在する。JRA 側は 11.60% が NAR 歴持ち、Ban-ei は 0%（別品種で転入経路なし）。この cross-source 断絶は GBDT が再構成不能な真の直交新データの候補として、5 companion 列（`xsrc_career_race_count`/`xsrc_win_rate`/`xsrc_place_rate`/`xsrc_recent_finish_norm`/`xsrc_recent_win_rate`、`race_date < target` かつ 10y lookback の leak-free 設計、当 source と異なる source の履歴のみ集約）で検証。117 conservative reduced baseline + `iter12-nar-xgb-hpo-v8` verbatim（`rank:pairwise`）、3-fold WF blind（2023/2024/2025）+ bootstrap LB95 2000 iterations race-level paired。pooled（n=40,710）: **top1 Δ+0.170pp[LB95 +0.025 で positive]**・fukusho_2p Δ+0.093[+0.010 positive]・**回帰ゼロ（全 rank delta>=0）** だが place2 Δ+0.162[LB95 -0.044]/place3 Δ+0.143[LB95 -0.061] が LB95<0 のため §7.2 gate（primary 2/3 が LB95>0 かつ place2/place3 の 1 以上が positive）不成立で REJECT。決定打は per-fold top1 Δ[LB95] = 2023 +0.625[+0.323] / 2024 -0.117[-0.336] / 2025 +0.000[-0.246]——pooled の正は 2023 単年駆動で `project_jra_field_difficulty_reject_2026_06_23` と同型の regime 依存（多年 blind 頑健性なし、ADOPT すれば 2024 年型の年に本番回帰を招くリスク）。subgroup では仮説の方向性自体は確認——NAR-debut+ex-JRA 層（n=4,994）で top1 Δ+0.300[LB95 -0.100]/place2 Δ+0.581[LB95 +0.000] と pooled の 2-3 倍に集中するが、n 薄で LB95 未達 + top3_box Δ-0.320 回帰。cell pocket（`nar_dirt_mile_unknown` 始まりの cell 系 top1 +0.8〜+1.5pp）は post-hoc かつ per-fold 不安定で不採用。**機序**: cross-source は GBDT が既存特徴から再構成不能な真の直交新データ（機構は実在）だが、**NAR の odds/popularity が移籍馬の JRA 実力を既に価格へ織り込んでおり**、明示列の増分が market の織り込み分を上回れない——science-track saturation と同根だが「informative-absence 既捕捉」型ではなく「odds 既 price-in」型。JRA 横展開は非推奨（NAR 歴持ちが母集団の 11.6% と小さく同じ odds-pricing が効く）。参考: ADOPT だった場合の Container 内 cross-source query は feasible だった（Neon に jvd/nvd 両系統の mirror 存在、`_rec_select_from_corner_features` は既に source 混在 read）。artifact: `apps/pc-keiba-viewer/tmp/candidate-cross-source/`（`phase1_transfer_ratio.py` / `augment.py` / `eval_xsrc.py` / `eval_report.json` / `cell_report.json`）。本番 iter12 無変更。
- **Ban-ei**: 学習窓 2011+ への変更が本物の改善（2026-06-23）、similar-race（v9-sim, 130 feat）を 2026-06-26 に deploy。grade_code=E のみ base variant へ routing。cell 単位 season スキャン + 3 lever 深掘り（2026-07-02）は全て REJECT——distance_band は全レース sprint 固定、class_label は約 90% が unknown で E 以外の grade_code（P/Q/R/T）は season-slice あたり race_count が 1-14 で MIN_RACES=200 gate 未達のため、評価可能な cell 次元は実質 season のみ。
  - **cell ランキング（season, class_label=unknown, race_count>=200、Ban-ei pooled 平均比 delta、pooled 3-fold WF: train≤2022→2023 / ≤2023→2024 / ≤2024→2025、n=5,232）** — pooled baseline top1 35.11% / place2 20.93% / place3 15.33% / place4 13.47% / place5 13.46% / place6 12.90% / top3_box 11.49% / fukusho_2p 78.04%。winter（n=1,327）top1 31.05%（Δ-4.06pp、Δplace2 -2.47pp、Δplace3 -1.16pp、Δfukusho_2p -2.83pp、per-fold top1 33.55/28.35/31.19 で 3/3 fold 弱い）、autumn（n=1,265）top1 32.96%（Δ-2.15pp、Δplace2 +1.28pp、Δplace3 -0.55pp、Δfukusho_2p -2.86pp、per-fold 30.73/33.10/35.08 で 3/3 fold 弱い）、summer（n=1,303）top1 35.92%（Δ+0.81pp、0/3 fold 弱い）、spring（n=852）top1 39.08%（Δ+3.97pp、0/3 fold 弱い）。独立 rest-of-category bootstrap（2000 resamples、全 class_label 込み cell-top1 vs rest-of-Ban-ei-top1）でも winter LB95 -7.20pp（n=1,456）・autumn LB95 -4.67pp（n=1,400）は頑健に負、summer LB95 -0.29pp（n=1,428、有意でない）、spring LB95 +1.48pp（n=948、頑健に強い）。grade_code=='E'（n=386、全 lever で不変の既存 routing）top1=39.90%。
  - **lever 1: sim variant 学習窓 2016→2011 拡張 REJECT（DO-NOT-RETEST）** — pooled top1 Δ+0.06pp（LB95 -1.80）、place2 Δ-0.11pp（LB95 -1.72）、place3 Δ0.00pp（LB95 -1.34）、top3_box Δ-0.54pp（LB95 -1.70）、fukusho_2p Δ-0.02pp（LB95 -1.57）——primary 0/3 が gate 通過、no-regression は 2023/2024 fold で fail・2025 も僅かに fail（place2/place5/top3_box）、winter/autumn を含む season cell 4 種すべてで no-regression fail。副次的に `banei-cb-v9-sim-2011` の `metadata.json` 記載 `train_date_range` が名称と異なり実際は 2016-01-01 始まりであることを確認したが（`tmp/feat-banei-v9-sim` parquet に 2011-2015 分 76,833 行が実在しデータ欠如ではないと確認済み）、本 lever が REJECT となったことで現行 2016 開始は命名との齟齬ではなく経験的に最適な設定と裏付けられた。根本原因: sim\_\* 系特徴量は 2011-2015 の履歴 lookback window が浅く warm-up カバレッジが薄いため、sim 特徴を含む構成では追加 5 年がむしろ希釈方向に働く（sim を含まない v8/base 特徴量での 2011 窓採用とは逆）。DO-NOT-RETEST。
  - **lever 2: season bucket-aware upweighting（winter+autumn 対象、alpha=0.75、non-restrictive full-history）REJECT（DO-NOT-RETEST）** — NAR venue45/30 upweighting と同一系統の `--bucket-membership-parquet` lever を season に適用（対象比率は Ban-ei 全体の約 53.6%、18,638/34,743 レースと NAR の約 12% より大幅に広い）。pooled top1 Δ+0.13pp（LB95 -1.64、有意でない）、no-regression は pooled でも fail（place4 -0.23pp / place5 -0.42pp / top3_box -0.38pp）、対象である winter cell 自身の内部でも fail（place2 -0.53pp / place3 -0.60pp / place4 -1.06pp / place5 -0.75pp / top3_box -0.83pp）——upweight 対象自身にすら robust な改善をもたらさない。NAR venue45/30 upweight REJECT と同型の失敗モード（損失関数の注意を再配分するだけで純増なし）。DO-NOT-RETEST。
  - **lever 3: `season_band` 特徴量追加 REJECT（DO-NOT-RETEST）** — `finish_position_features_duckdb.py`（~2515-2522 行、`kyori_band` と並び算出済み）に既存だが本番 130/111 特徴量契約（metadata.json の feature_names）からは漏れていた calendar-season ordinal を additive に追加。2026-06-24 REJECT 済みの天気 12 列（day-of-race の noisy な実測値、DO-NOT-RETEST）とは機構的に別物（season_band はラベルそのものでノイズなし）として検証。pooled top1 Δ-0.04pp（LB95 -1.82）、primary 1/3（place3 +0.13pp）のみ改善で gate 未達（2/3 必要）、no-regression fail（place5 -0.06pp / top3_box -0.33pp）。cell 別でも winter（place2 +0.53pp / place3 +0.30pp だが place4 -0.90pp / place5 -0.38pp / top3_box -0.83pp で fail）・autumn（top1 +0.40pp だが他指標で fail）・summer（top1 -0.38pp / place2 -0.61pp / top3_box -0.46pp）・spring（top1 -0.82pp）全て no-regression fail。CatBoost は `weather_normalized` / `track_condition_normalized` / `current_baba_condition` / `kohan3f_avg_5` 等の相関特徴から season 相当の情報を既に吸収済みという、JRA/NAR で確認済みの教訓（GBDT は相関シグナルを既に捕捉）が天候特徴とは別機構の特徴でも再現。DO-NOT-RETEST。
  - **総括**: 3 lever 全て REJECT、本番構成（`banei-cb-v9-sim-2011` sim デフォルト + `grade_code=='E'` の `banei-cb-v8-window2011-wf-15y` routing）は無変更。winter/autumn の約 2-4pp top1 劣後は cross-fold 一貫性と独立 rest-of-category bootstrap の両方で real と確認されたが、機構的に異なる 3 lever（学習窓拡張・non-restrictive upweighting・season 識別特徴追加）いずれでも是正不能——JRA の kohan3f-going/field-difficulty REJECT と同型（real だが actionable でない weak subgroup）。本調査は `train_finish_position_catboost_walk_forward.py --predictions-output-root`（commit 56b94937、XGBoost 側 4adaaa39 の flag を踏襲）で得た実 per-race 予測を bootstrap に使用し、`synthesize_hit_vector` の Bernoulli 再構成フォールバックには依らなかった。
  - **馬体重 within-race relativization（文献 12_1_1 由来 Grade A 候補）EARLY-REJECT（2026-07-02 深夜、文献調査由来、DO-NOT-RETEST）** — 上記 3-lever season 調査とは独立の、文献駆動の別 campaign。**文献的動機**: 論文 12_1_1（Kashiwamura 2001、日本の競走競技における体格×成績の唯一の実証研究）は「輓馬はそりに重りを載せて引くため体重が大きいほど強い」（JRA の逆 U 字とは逆の monotone 正相関）を示し、体重は胸囲と r=0.81 の体格 composite proxy。deployed `banei-cb-v9-sim-2011`（130 feat）は `weight_avg_5` / `weight_diff_from_avg` の時系列のみ搭載で `bataiju`（当日絶対体重）/ `barei` / `seibetsu` は非搭載、かつ within-race rank は cross-row 集約のため GBDT が木の分岐で再構成できない——真に新規かつ薄市場という Grade A の事前期待だった。**候補列（4 列）**: `banei_bataiju_rank_in_race`（レース内体重順位）/ `banei_bataiju_diff_from_field_mean`（field 平均からの差）/ `banei_bataiju_per_futan`（power-to-load 比）/ `banei_bataiju_abs`（絶対体重）。**結果 EARLY-REJECT**: 生の順位相関 raw ρ は全 4 列・全 3 年（2023-2025）で仮説符号（例 `banei_bataiju_diff_from_field_mean` raw -0.085/-0.071/-0.079、`banei_bataiju_rank_in_race` raw +0.076/+0.065/+0.073）で方向は文献通りだが、odds（`tansho_ninkijun`）を統制した partial ρ が全列で probe gate（絶対値 0.08）を 3〜15 倍下回る（`banei_bataiju_diff_from_field_mean` partial -0.0193/-0.0233/-0.0268、`banei_bataiju_per_futan` partial -0.0272/-0.0159/-0.0246、`banei_bataiju_rank_in_race` partial +0.0031/+0.0101/+0.0111、`banei_bataiju_abs` は mean partial -0.0055 で最弱）。`weight_avg_5` を追加統制した incremental partial ρ でも最大 0.037（`banei_bataiju_diff_from_field_mean` 2025 の -0.0374 が最大、他は 0.01-0.02 台）。**「薄市場が体重を under-price している」という仮説は棄却**——`tansho_ninkijun` が raw signal の約 70% を吸収しており、Ban-ei でも市場は体重を効率的に価格へ織り込み済み（過去の「Ban-ei odds 単独 gain 56%」観測と整合）。subgroup 増幅仮説も反証: 重ゾリ層（`futan_class>=5`、n=3,441）で partial ρ≈0（rank -0.0179 / diff +0.0039 / per_futan +0.0116）と体重優位がむしろ消失し、逆に light race（`futan_class<=2`、n=20,001）が最強（diff partial -0.0409）——重り負荷で体重が効くはずの層で効かない。grade QRPST（record/heavy、n=901）は partial が gate を名目上超える（diff -0.0728 / per_futan -0.0908）が単年 2024 駆動 + n 過小の noise。**「木は within-race rank を作れない」は真だが moot**: odds + `weight_avg_5` 控除後の残差 ρ≈0.02-0.037 は top1/place を動かす水準になく、model は `popularity_score` / `odds_score` / `weight_avg_5` 経由で同等情報に既到達済み——sim\_\* 家系と同じ market-absorbed 型で、WF 学習まで進める価値なしと probe 段で確定（EARLY-REJECT）。**副次的な申し送り（コード品質）**: `pg.nvd_se` の `bataiju` / `futan_juryo` は 3-char HEX 表現（`'0x'||trim(...)` を int cast して kg）。deployed `add-banei-futan-class-features.py` は正しく hex decode するが、旧 `add-ban-ei-raw-features.py:67` の `/10.0` decimal cast は hex 行を silent に NULL 化する既知バグ——将来この列を wiring する際は hex-decode 済の futan-class 系列を使うこと。artifact: `apps/pc-keiba-viewer/tmp/candidate-banei-bodymass/`（`probe_report.json` / `probe_subgroup_report.json`）。本番 `banei-cb-v9-sim-2011` 無変更。
  - **pairwise-XGBoost base + Set Transformer blend REJECT（2026-07-03、DO-NOT-RETEST）** — NAR で deploy 済みの transformer blend win insight（レース内 cross-horse set-attention は listwise でない base に補完的、§11 NAR 参照）を Ban-ei へ転移検証。以前（2026-07-02）の Ban-ei transformer 検証が本番 CatBoost-YetiRank（listwise）base に blend したための REJECT だった可能性を排除するため、今回は **Ban-ei 専用の pairwise-XGBoost base を新規学習**（`rank:pairwise`、NAR `iter12-nar-xgb-hpo-v8` params verbatim、130 feat、3-fold WF train≤2022/2023/2024→blind 2023/2024/2025、pooled n=5,232）し、その非 listwise base の上に Set Transformer を blend して全 gate を検証。**全 gate REJECT**。
    - **Gate A（transformer on pairwise-XGB base vs pairwise-XGB 単体、tw=0.5）**: nm（race-meta OFF）top1 Δ-0.019pp[LB95 -0.440]／place2 Δ+0.172[-0.382]／place3 Δ-0.019[-0.631]／place6 Δ-0.401[-1.051、回帰]——positives 0 件・regressors[place6] で REJECT。meta（race-meta ON）top1 Δ-0.096[-0.535]／place2 Δ-0.057[-0.631]／place3 Δ+0.038[-0.631]——positives 0 件・regressors[top1/place2/place5/place6] で REJECT。**transformer は Ban-ei では pairwise base の上でも top1/place を一切改善しない**（cell 単位でも top1 2/4・place3 1/4 の弱い散らばりのみ）。
    - **Gate B（pairwise-XGB+transformer blend vs 本番 CatBoost-YetiRank `banei-cb-v9-sim-2011`、tw 0.3-0.6 sweep）**: 全 tw で REJECT。tw=0.3 top1 Δ-0.593[-1.204]／tw=0.4 -0.688[-1.319]／tw=0.5 -0.612[-1.242]／tw=0.6 -0.803[-1.472]——**top1（primary）は全 tw で有意に負、per-fold でも 3/3 fold 負**（tw=0.5: 2023 -0.84／2024 -0.34／2025 -0.66pp）。place3 のみ tw を上げるほど正に振れる（tw=0.6 で +1.204[+0.325]、tw=0.5-meta で +0.841[+0.076]）が、これは Ban-ei 固有の place3+/top1- トレードオフ（下記 diag と同型）に過ぎず、tw=0.5-meta は place3 のみ positive・top1/place2/place5/place6 が regress。§7.2 gate 不成立。
    - **Gate C（null control: XGB seed1+seed2 の within-race rank-fusion vs XGB base、tw=0.5）**: top1 Δ+0.115[-0.306]／place2 Δ+0.248[-0.306]／place3 Δ-0.631[-1.261、回帰]／top3_box Δ-0.420[-0.726、回帰]——positives 0 件で REJECT。**generic な 2-seed diversity も効かない**（NAR の null control が「transformer 固有の直交 signal」を裏づけたのと対照的に、Ban-ei では diversity 自体が無効）。
    - **副次: pairwise-XGB base 単体 vs 本番 CatBoost-YetiRank（`diag_xgb_vs_cb`、tw=0）**: top1 Δ-0.593[-1.204]／place2 Δ-0.076[-0.822]／place3 Δ+0.803[+0.019]——positives[place3]・regressors[top1/place2/place4/place5]、per-fold top1 3/3 fold 負（2023 -0.73／2024 -0.62／2025 -0.42）で REJECT。**Ban-ei の本番 CatBoost-YetiRank は top1（primary）で genuinely 優れ**、base を非 listwise の pairwise-XGB に変えても place3+/top1- の同一トレードオフが出るだけ（base 選択の問題ではない）。
    - **機序/結論**: NAR の transformer blend win は **NAR 固有**であり、base を non-listwise（pairwise-XGB）に変えても Ban-ei には転移しない。Ban-ei の構造（単一 venue・全レース sprint・輓馬）は NAR のような exploitable な cross-horse dynamics を持たないと解釈できる。**これで transformer アーキテクチャ lever は 3 カテゴリ全て検証完了: NAR = deployed（`iter40-nar-settransformer-blend-v1`、本番 LIVE）／JRA = REJECT（listwise CatBoost base で冗長、§11 NAR item 参照）／Ban-ei = REJECT（pairwise base でも転移せず）**。Ban-ei 本番は CatBoost-YetiRank（`banei-cb-v9-sim-2011`）が最適で無変更。artifact: `apps/pc-keiba-viewer/tmp/candidate-banei-tf-pairwise/`（`gates/*.json`・`logs/gates.log`）。DO-NOT-RETEST。

- **相関/interaction 由来の新規 derived feature 3 候補（2026-07-02、ユーザー指示によるセッション）** — 「新規カラム raw 追加ではなく相関/interaction 分析から真に新しい特徴量を作る」という方針で 3 候補を設計・検証。いずれも既存の pacestyle layer（`rs_p_*` x field pace pressure）、within-race rank/diff 特徴、`weight_zscore` とは機構的に別物であることを事前確認した上で着手（restatement ではない）。全カテゴリで本番 CatBoost/XGBoost の verbatim hyperparameters（`model_meta.json` 参照 model_version の `metadata.json` から読み込み）、3-fold WF blind（train≤2022/2023/2024→blind 2023/2024/2025）、bootstrap LB95（2000 iterations）、§7.2 accept gate で評価。JRA は `tmp/candidate-eval-jra/base-store`（254/263 = 96.6% 本番特徴量一致）、Ban-ei は `tmp/candidate-eval-banei/store-full`（130/130 = 100% 一致）、NAR は on-disk に 192 特徴量 full-parity store が存在しないため conservative reduced baseline（117/192 = 61%、`tmp/candidate-eval-nar/feature_lists.json` の `baseline` を踏襲）を使用——同セッション内の別 lever（career-rate rank-in-race 等）と同じ精度上の理由による妥協であり、NAR の REJECT はこの縮小 baseline でも full 192 特徴量契約に対して conclusive（baseline を増やすほど incremental value は下がる方向のみ）、ADOPT の場合のみ追加の full-pipeline regen 確認が必要という既存 methodology を踏襲。
  - **jockey-track chemistry（`jockey_keibajo_chemistry` / `jockey_distance_chemistry` / `jockey_track_chemistry` / `jockey_grade_chemistry` = 各 jockey subgroup 勝率 − jockey career 勝率、raw rate の絶対値ではなく career baseline からの相対差分）REJECT（JRA/NAR/Ban-ei 全て conclusive、2026-07-02 に 3 カテゴリ完了、DO-NOT-RETEST）**。JRA（pooled n=10,365、2023-2025）: top1 Δ-0.27pp（LB95 -0.62）、place2 Δ+0.03pp（LB95 -0.41）、place3 Δ+0.26pp（LB95 -0.19）、place4〜6 も LB95<=0.05、gate `gate_pos_top3_lb95=0` で REJECT、cell-level pocket 0 件。Ban-ei（pooled n=5,232）: top1 Δ-0.15pp（LB95 -0.54）、place2 Δ+0.11pp（LB95 -0.42）、place3 Δ+0.29pp（LB95 -0.29）、pocket 0 件、REJECT。**NAR（2026-07-02 完了、pooled n=40,710、2023-2025）: REJECT**——確立済みの NAR conservative reduced baseline（117 特徴量）を baseline、treatment=121（+4 chemistry diff、coverage 99.7-99.95%）、`iter12-nar-xgb-hpo-v8` metadata verbatim（`rank:pairwise`、max_depth 7、lr 0.0527、seed 2068、650 rounds、early-stop 30 on ndcg@3）、3-fold WF blind 2023/2024/2025、bootstrap LB95 2000 iterations race-level paired で検証。pooled: top1 Δ-0.039pp（LB95 -0.192）、place2 Δ+0.042pp（LB95 -0.157）、place3 Δ+0.118pp（LB95 -0.091）、place4 Δ-0.113pp（LB95 -0.319）、place5 Δ-0.155pp（LB95 -0.369）、place6 Δ-0.231pp（LB95 -0.437）、top3_box Δ-0.128pp（LB95 -0.285）、fukusho_2p Δ-0.098pp（LB95 -0.224）——全 LB95 が 0 未満または 0 をまたぎ、rank4-6 + top3_box + fukusho_2p は回帰。年別も方向不定（top1: 2023 -0.265pp / 2024 +0.219pp / 2025 -0.074pp）で training noise。venue/cell pocket 0 件: 最強は 姫路51（n=933）top1 Δ+1.072pp だが LB95 ちょうど 0.000 で fail・place2/3 LB95 も負、cls=T（n=360）place2 Δ+2.50pp（LB95 +0.278）は 1 指標のみ + place3 Δ-0.833pp 回帰 + N 過小で不採用。機序: 4 diff は 117-set に既在の 6 operand（`jockey_keibajo/distance/track/grade_win_rate` と `jockey_career_win_rate`）の算術変換に過ぎず XGBoost が非線形に既捕捉——JRA REJECT と同一の冗長性結論。artifact: `apps/pc-keiba-viewer/tmp/candidate2-nar/`（`result.json` / `run.log` / `eval_jockey_chemistry_nar.py`）。**JRA/NAR/Ban-ei 3 カテゴリ全て conclusive、DO-NOT-RETEST**。本番 iter12 無変更。注記: harness の distance band（≤1400/≤1800/≤2200）は canonical binning（<1200/<1600/<2000）と異なるが JRA/Ban-ei 完了分との apples-to-apples 維持のため踏襲、gate は binning 非依存の global pool で判定。
  - **weight-change × distance/class interaction（`weight_diff_distance_interaction = weight_diff_from_avg * (1000/max(kyori,800))` および `weight_diff_grade_interaction = weight_diff_from_avg * is_grade_race`）REJECT（JRA/NAR/Ban-ei 全て conclusive、DO-NOT-RETEST）**。JRA（pooled n=10,365）: dist/grade 両 variant とも primary_positives 0 件、regressors に `hit_top1`/`hit_place2`/`hit_place4`/`hit_place5`/`hit_place6`/`hit_top3_box` を含む広範囲 REJECT（top1 Δ-0.21pp LB95-0.56、place2 Δ-0.18pp LB95-0.59、place6 Δ-0.68pp LB95-1.11 で有意に負）。NAR（pooled n=40,710）: top1 Δ+0.16pp（**LB95 +0.007、僅かに正**）だが place2/place3 の LB95 はいずれも負（+0.09pp LB95-0.13／+0.08pp LB95-0.14）のため §7.2 の「place2/place3 のうち 1 つ以上が positive」条件を満たさず gate REJECT。Ban-ei（pooled n=5,232）: 全 rank で delta は微小正だが LB95 は place6 以外すべて負（place6 も -0.02 で僅かに負）、REJECT。distance_band 単一次元セルでも（JRA sprint/mile/intermediate/long/extended 全て、`cells_ge200` は dist/grade 両方 0 件）ADOPT 可能な pocket なし。JRA の regression パターンは、CatBoost が `kyori`（連続）と `weight_diff_from_avg`（連続）の交互作用を明示的な積特徴量なしで既に木の分岐で再構成しており、追加の相関列がノイズとして働くという既存 REJECT 群（score-additive draw+speed、season×sex×weight、field-difficulty）と同型のメカニズムを示唆する。DO-NOT-RETEST（3 カテゴリとも）。
  - **pace-style × distance-band structural fit score（`pace_style_distance_fit_score` / `pace_style_distance_fit_place_score` = 馬自身の `rs_p_nige/senkou/sashi/oikomi` と、fold の train partition のみから再計算した `distance_band`×style 別 historical win/place rate の内積、leak-free 設計）— JRA/NAR とも REJECT（2026-07-02 完遂、DO-NOT-RETEST）、本番無変更**。**JRA REJECT**: 前セッションの 2025-only 部分結果を 3-fold WF まで完走（`tmp/candidate1-jra/base-store`、254 feat baseline、CatBoost YetiRank verbatim depth8/lr0.05/l2 3.0/iter300/seed20260519）。pooled（n=10,365）: top1 Δ+0.116pp[LB95 -0.212]／place2 Δ+0.106[-0.289]／place3 Δ+0.010[-0.377]／`top3_box` Δ+0.251[+0.029]／`fukusho_2p` Δ+0.097[-0.174]——primary の LB95>0 は 0/3。coverage-restricted（2024+2025、n=6,909）でも top1 Δ+0.203[-0.217]／place2 Δ+0.347[-0.159]／place3 Δ+0.275[-0.246] で全 primary LB95 が 0 をまたぐ。**構造的要因**: `rs_p_*` は store で 2024 年以降のみ populated のため、3 fold 中 2 fold（blind 2023/2024）は train partition の候補 coverage 0% で CatBoost が split を学習できず、唯一 trainable な 2025 fold（train coverage 4.98%）でも全 primary LB95<0。cell-level: 完全 6-way cell は blind 3 年で n>=200 が 0 個、marginal cell でも primary LB95>0 は皆無。唯一の positive は補助指標 `top3_box` のみ（dirt +0.502[+0.141]／summer +0.556[+0.148]／intermediate +0.405[+0.045]／函館02 +1.620[+0.463]）で、box 集合を微修正するが exact 順序は改善しない弱効果のため採用不可。serve 反映には point-in-time lookup 機構も要るが、根本 blocker はデータ（`rs_p_*` を複数年 backfill しない限り WF 学習不能）。artifact: `tmp/candidate1-jra/`（`fit_persist_folds.py` / `aggregate_folds.py` / `preds/fold_*.parquet` / `result_3fold.json`）。**NAR REJECT**: 同一設計を NAR に移植（`tmp/candidate1-nar-pacefit/`、117 conservative reduced baseline、iter12 XGBoost verbatim）。`rs_p_*` カバレッジ実測は 2006-2023=0.00%／2024=100%／2025=99.98%（JRA と同一パターン——「~90% NULL」という旧記載よりも「2024 年未満 0%／2024+ ほぼ 100%」が正確、JRA/NAR 共通）。informative fold（2025、n=13,426）で top1 Δ-0.127pp[LB95 -0.380]／place2 Δ-0.164[-0.484]／place3 Δ-0.343[-0.685]／`top3_box` Δ-0.350[-0.611]——全 primary net-negative で明確に REJECT。機序: pace-fit は既存特徴と高冗長（corr 0.61 = `past_nige_rate_self`、-0.55/-0.56 = popularity/odds）。注記: 2024 fold の treatment 学習が外部 SIGTERM ×2 で未完了のため最終 pooled は 2-fold（2023+2025）構成だが、2023 fold は train/test とも候補全 NULL で baseline 同等、判定は informative fold で conclusive。Ban-ei は着手せず（`rs_p_*` の複数年欠損という同一データ blocker が適用され、JRA/NAR と同じ結論に帰着する見込み）。
- **2026-07-02 相関/派生特徴量ハント（career-rate 内レース正規化 + trend 2階微分 + market-form gap）**: 既存カラムに `_rank_in_race`/`_diff_from_race_avg` companion が無かった `career_win_rate`/`career_place_rate`（Candidate A: `career_win_rate_rank_in_race`/`career_place_rate_rank_in_race`/`career_win_rate_diff_from_race_avg`/`career_place_rate_diff_from_race_avg`）、`finish_trend_5` の 2 階微分（Candidate B: `finish_trend_prior5`/`finish_trend_acceleration_5_10`）、JRA 限定の市場×実績 gap（Candidate C: `form_market_edge` = career_win_rate − inverse_odds_implied_prob）を schema 拡張として追加（commit `aa391227`）し、3 カテゴリ全てで 3-fold WF（2023/2024/2025 blind）+ bootstrap LB95（2000 iter、race-level paired）で検証。**全 8 組み合わせ（JRA×3 candidate、NAR×2、Ban-ei×2）で REJECT**。JRA（n=10,365 races）: A top1 −0.068pp[LB95 −0.396]/place2 −0.068[−0.482]/place3 −0.270[−0.685]、B top1 −0.077[−0.473]/place2 +0.010[−0.482]/place3 −0.193[−0.685]、C top1 −0.174[−0.482]/place2 −0.029[−0.425]/place3 −0.010[−0.425]（全 primary で LB95<0）。NAR（n=40,710 races）: A 全 7 指標 negative（top1 −0.059pp[LB95 −0.199] 等）、B は top1 +0.056[LB95 −0.081]/place5 +0.142[LB95 −0.059] と点推定のみ僅かに正だが全 LB95<0。Ban-ei（n=5,232 races）: A top1 +0.038pp[LB95 −0.32]/place2 +0.32[LB95 −0.19]/place3 +0.31[LB95 −0.27]、B 全指標微小、いずれも 0 個の primary metric が gate の +0.08pp かつ LB95>0 を通過（§7.2 の「2/3 positive」未達）。**Why**: 3 特徴とも GBDT が既存カラムから再構成不可能な設計（career_win_rate には元々 field-aggregate 列が存在せず、trend の 2 階微分も元データに無い）だが、3 モデルとも既存特徴量（JRA 263/NAR 192/Ban-ei 130）で実質的に飽和しており増分価値ゼロ。DO-NOT-RETEST（3 candidate 全て、全カテゴリ）。本番モデル無変更（jra-cb-v9-sim-2013 / iter12-nar-xgb-hpo-v8 / banei-cb-v9-sim-2011）。評価 harness: `apps/pc-keiba-viewer/tmp/candidate-eval-{jra,nar,banei}/`（gitignored、pooled_report.json に詳細）。
- **sim 系 per-horse エンティティ率の within-race 相対化 companion（Family A: `sim_jockey_win_rate`/`sim_sire_win_rate` の within-race diff/rank 派生 4 列、Family B: `sim_jockey_place_rate`/`sim_trainer_place_rate` の同 4 列）— JRA/Ban-ei とも全 REJECT（2026-07-02、DO-NOT-RETEST。NAR は sim 系レイヤ自体が REJECT 済み（`project_similar_race_features_reject_2026_06_26`）のため対象外）**: sim 系 Phase-2 の per-horse エンティティ率は行単位の絶対値で、CatBoost は cross-row 集約を木の分岐から構造的に再構成できない——within-race 相対化（同一レース内の diff/rank）のみが GBDT に真に新規という設計根拠で着手。既 REJECT の `career_win_rate` companion（本節上記 Candidate A）は全コンテキスト career 率の相対化だったのに対し、こちらは類似レース pool 由来のコンテキスト特化率の相対化で別系列であること、および本番 254/130 feature list に sim 相対化列が皆無であることを事前確認（restatement ではない）。検証は 3-fold WF blind（2023/2024/2025）、bootstrap LB95（2000 iterations、race-level paired）、verbatim hyperparameters（JRA は iter300/depth8/lr0.05/l2 3.0 YetiRank・学習開始 2013、Ban-ei は学習開始 2016=deployed 相当）、baseline JRA 257 feat vs +4／Ban-ei 134 feat vs +4。JRA（pooled n=10,365）: Family A top1 +0.222pp[LB95 -0.087]/place2 +0.405[+0.019]/place3 +0.048[-0.328]/place5 -0.058（回帰）——place2 のみ LB95>0 だが primary 1/3 で §7.2 gate FAIL、Family B は全 primary LB95<0。Ban-ei（pooled n=5,232）: Family A top1 +0.000pp[-0.401]/place2 -0.210[-0.765]/place3 +0.000[-0.516]、Family B は place3 +0.210 の点推定のみ正だが LB95<0——全 REJECT。cell-level: JRA canonical 6-dim cell は blind 3 年 pooled で最大 152 races（`MIN_RACES=200` 到達 0 件）のため coarse scan（surface/distband/class）で確認——Family A は place2 を intermediate（+0.79pp[LB95 +0.16]）/unknown-class（+0.56[+0.11]）で、top1 を turf_long（+1.27[+0.56]）で改善するが、同一 cell で 2 primary 同時陽性は皆無・place3 は LB95>0 に一度も届かず cell-conditional ADOPT 不成立。Ban-ei は >=200 cell 4 件で pocket 0。**Why**: 派生列の生 corr は本物（place3 標的で r=0.146）だが人気（log odds）統制後の partial ρ が ~0.003-0.015 に崩壊——市場が within-race 相対エンティティ能力を既に価格に織り込み、GBDT は生 sim 率 + odds で残差を吸収済み（career-companion REJECT と同機序）。Ban-ei は単一 venue で pool の context 変動が乏しく信号ほぼ消失。評価 harness: `apps/pc-keiba-viewer/tmp/candidate-sim-derived/`（gitignored、`build_derived_store.py`・`eval_sim_derived` 系スクリプト・`pooled_report.json`・cell 別 report JSON）。本番 4 モデル無変更。副次的観測: 学習 background job が高負荷時に exit 144（signal 16）で断続 kill される現象があり、`--resume-from-checkpoint` の per-fold 再開で全 fold 完走（`feature_count` parity 確認済）。
- **same-day dynamic track bias（当日先行レース由来トラックバイアス、JRA+NAR、residual 3 列 + interaction 2 列）全 REJECT（2026-07-02、DO-NOT-RETEST）** — 当日の先行レース結果からトラックバイアスを推定する intra-day 特徴量群を新規設計し検証、全 arm REJECT。
  - **差別化確認済み**: 既存 `track_bias_inside` / `track_bias_front`（`finish_position_features_duckdb.py:1941-1956`）は prior-day 5 日窓で当日レースを一切含まず、intra-day 特徴は皆無だった（2026-06-20 の draw/speed ablation は歴史的特徴で別物）。
  - **設計**: race t（venue V, day D, bango N）に対し同日 bango<N のレースのみから `sameday_inside_bias` / `sameday_front_bias`（venue baseline との residual 化、leak-free 検証済み）/ `sameday_prior_race_count`。coverage ~91-92%。
  - **JRA**（n=10,365、254-feat baseline 再利用）: residual arm は gate 0/3 + no-reg FAIL（top1 -0.058／place2 +0.222[LB95 -0.164]／fukusho_2p -0.106）、interaction arm も 0/3。per-year top1 符号反転 = noise。**決定的反証: 情報が最も濃い「先行>=4」サブグループでむしろ悪化**（top1 -0.101／place2 -0.043／place3 -0.072）。cell pocket 2 件（extended place3 +2.14[+0.63]／函館 top1 +1.62[+0.23]）は interaction arm で消滅 = 多重比較ノイズ。
  - **NAR**（n=40,710、117 reduced baseline + `iter12-nar-xgb-hpo-v8` verbatim）: residual arm は top1 +0.162[LB95 +0.010] と fukusho_2p +0.101[+0.017] のみ正だが place2/3 fail + place4 -0.145 回帰で gate FAIL、interaction arm は top1 も LB95<0 に沈む。先行>=4 で top1 LB95 -0.056 に失効（JRA と同じ反証）。venue 内で季節により符号反転し、routing 可能な locus なし。
  - **機序**: GBDT は `track_condition_normalized` / `weather_normalized` / `current_baba_condition` + 5 日 `track_bias` で当日の馬場状態を既に（≤11 レースの top-3 標本より低ノイズに）保持している。weather / field-difficulty / draw の各 REJECT と同型。
  - **artifact**: `apps/pc-keiba-viewer/tmp/candidate-sameday-bias/`。本番無変更（`jra-cb-v9-sim-2013` / `iter12-nar-xgb-hpo-v8`）。
- **騎手乗り替わり（jockey-switch）delta（`jockey_switched` / `jockey_quality_delta`＝現騎手 career_win_rate − 前走騎手 career_win_rate / `jockey_recent_quality_delta` / `switched_to_pair_experienced`）— JRA REJECT（WF）+ NAR EARLY-REJECT（probe）（2026-07-02、DO-NOT-RETEST）**。前走騎手は同一馬の直前レースから leak-free 導出。**同日の jockey-track chemistry REJECT（本節上記）との差別化**: chemistry は同一騎手内の subgroup 勝率と career 勝率の文脈差（operand が全て行内に存在）であったのに対し、本件は騎手「間」の交代 delta で「前走騎手」情報は当該行に一切存在しない真の新規列——それでも REJECT だった点が重要（行内非在の直交情報でも market が既に price-in している証左）。**JRA WF**（`tmp/candidate-jockey-switch/result_3fold.json`、control=`jra-cb-v9-sim-2013` 254 feat 再利用、treatment=+4 switch 列、pooled n=10,365 races 2023-2025、CatBoost YetiRank verbatim、bootstrap LB95 2000 iterations）: top1 Δ-0.154pp[LB95 -0.482]／place2 Δ+0.077[-0.367]／place3 Δ+0.289[-0.116]／place4 Δ+0.444[+0.029]／place5 Δ+0.396[-0.039]／place6 Δ+0.309[-0.097]／top3_box Δ+0.232[LB95 0.000]／fukusho_2p Δ+0.135[-0.145]——primary の LB95>0 は 0/3 で §7.2 gate FAIL。switched-races サブグループ（n=9,451、乗り替わり発生レースのみ）でも top1 Δ-0.064[-0.402]／place2 +0.138[-0.317]／place3 +0.296[-0.138] で gate FAIL——仮説が最も効くはずの母集団でも改善しない。entry-level 診断では方向性のみ確認（upgrade entries winner_recall Δ+0.596pp／downgrade Δ-0.444pp）だが race-level では消失。cell pocket（函館02 top1 Δ+1.62[LB95 0.000]／place3 +2.78[+0.463]、distance extended place3 +2.51[+0.879]）は多重比較ノイズで、class B/C/L は逆に回帰。**NAR probe**（`tmp/candidate-jockey-switch/result_probe_nar.json`、`feat-nar-v9-new` store、merged 1,755,514 rows、switch 率 39.87%）: raw の方向別勝率は実在（downgrade 6.76%／no-switch 11.18%／upgrade 9.65%）で raw ρ も本物（`jockey_quality_delta` place3 raw +0.047／`jockey_recent_quality_delta` place3 raw +0.052）だが、人気（odds）統制後の pooled partial ρ が全 4 列 0.007-0.013（max_abs 0.0125）に崩壊し |ρ|<0.02 の probe gate 未達＝WF 省略で EARLY-REJECT。switched-only サブグループ（n=699,999）で `jockey_recent_quality_delta` の partial ρ が 0.0285 と僅かに 0.02 を超えるが、JRA の等価サブグループが full WF で既に top1 回帰 REJECT 済みのため昇格せず。**機序**: sameday-jockey / jockey-track chemistry と同一の market-pricing 壁——市場が乗り替わり（現騎手の質・調子）を既にオッズへ織り込み済みで、明示的な switch delta 列は増分を持たない。artifact: `apps/pc-keiba-viewer/tmp/candidate-jockey-switch/`。本番 `jra-cb-v9-sim-2013` / `iter12-nar-xgb-hpo-v8` 無変更。
- **same-day jockey form（当日騎手成績、`sameday_jockey_ride_count` / `sameday_jockey_win_rate` / `sameday_jockey_top3_rate` / `sameday_jockey_avg_finish_norm`）EARLY-REJECT（JRA、2026-07-02、DO-NOT-RETEST）**。**仮説**: viewer の client-side heuristic（`finish-position-prediction.ts` の `getSameDayJockeyScore`、`sameDayJockeyWeight` JRA=0.03）は当日同 venue の騎手勝利数を既に使うが、本番 GBDT の騎手特徴は全て期間集計（career／recent-90d／season／keibajo／distance）で当日成績は皆無——真の欠落信号のはずだった。**設計**: 同日同 venue で `race_bango<N` の先行騎乗のみから集計（初騎乗は NULL、`pg.jvd_se data_kubun='7'` JRA keibajo 01-10、2013+、n=650,946、nonnull coverage 76.4%）。**probe**（odds 統制後 partial Spearman vs finish_norm、blind 2023-2025）: form 3 列（win_rate 0.0166／top3_rate 0.0176／avg_finish_norm -0.0001、`jockey_recent_win_rate` も併せ統制すると 0.0116／0.0117／0.0060）が全 cut で |ρ|<0.02 floor 未達、しかも**仮説が予測する hot-jockey（先行騎乗>=4）／後半レース（bango>=7）サブグループでむしろ最弱**（hot-jockey で 0.0023-0.0097、late-race で 0.0021-0.0085＝仮説と逆）。raw Spearman は実在（top3_rate -0.119／avg_finish_norm +0.135）。`sameday_jockey_ride_count`（+0.0376）のみ 0.02 を超えるが、form ではなく騎手需要/番組編成 proxy（form 仮説に対し wrong-signed、hot-jockey/late-race サブグループで 0.0106-0.0147 に崩壊）。probe gate（|ρ|<0.02）により WF 省略で EARLY-REJECT。**機序**: 「今日勝っている騎手は人気馬に乗っている」——市場が当日 form を完全にオッズへ織り込み済みで、model は既に odds + `jockey_recent_win_rate` を保持。jockey-track chemistry REJECT・sim-derived REJECT（いずれも 2026-07-02）と同機序。viewer heuristic の当該信号は、model 予測が無い場合の odds 代理として機能していたと解釈できる。artifact: `apps/pc-keiba-viewer/tmp/candidate-sameday-jockey/verdict.json`。本番無変更。
- **micro 仮説 3 候補 batch probe（trainer-switch / rotation-fit / horse-draw-affinity、JRA、probe-first）全 REJECT（2026-07-02、DO-NOT-RETEST）** — 3 個の micro 仮説を probe-first 方式（odds 統制 partial Spearman |ρ|>=0.02 かつ subgroup で強化 かつ 3 年中 2 年以上同符号を満たした候補のみ WF へ昇格）で検証。baseline は `jra-cb-v9-sim-2013`（254/263 feat parity）、blind 2023-2025（n=141,523 entries）。**3 候補中 ADOPT 0**——C1/C3 は probe で EARLY-REJECT、C2 は probe 通過も WF で REJECT。
  - **C1 trainer-switch（転厩 delta = 新厩舎 career/recent 勝率 − 前厩舎勝率、`trainer_quality_delta` / `trainer_recent_quality_delta`）EARLY-REJECT** — 転厩は全騎乗の **1.48%**（blind n=2,092）のみで actionable 母集団が薄い。switched==1 subgroup 内の odds 統制 place3 partial ρ は最大 0.0156（<0.02 floor 未達）、win partial ρ は -0.009 と逆符号。市場が厩舎異動を既にオッズへ織り込み済み。同日の jockey-switch full-WF REJECT（本節上記、より大きな母集団）と同機序で corroborated。
  - **C2 rotation-fit（`rotation_dev` / `rotation_dev_rel` = |days_since_last_race − 当該馬の好走時 interval 中央値|、馬固有の最適ローテからの逸脱）REJECT（probe PASS → WF REJECT、本日 2 件目の probe-通過→WF-fail near-miss）** — probe は通過（odds 統制 place3 partial ρ pooled -0.0224 / subgroup（n_prior_top3>=5、n=30,946）-0.0235、いずれも |ρ|>=0.02 かつ correct sign、3/3 年同符号）。だが 3-fold WF（pooled n=10,365 races、CatBoost YetiRank verbatim、bootstrap LB95 2000 iterations、treatment=+4 rotation 列）で pooled gate 0/3: top1 Δ-0.135pp[LB95 -0.444]／place2 Δ+0.106[-0.299]／place3 Δ+0.048[-0.367]。**決定打: 特徴が active な covered-races subgroup（n=7,000）でむしろ悪化**——top1 Δ-0.329pp[LB95 -0.700]／place2 -0.129／place3 -0.057、entry-level でも winner-recall covered -0.48pp・high-dev(>=median) -0.609pp。cell pocket は 函館02 のみ（下記メタ知見）。**機序**: CatBoost が `days_since_last_race` + odds から有用部分を既抽出済みで、明示的な逸脱列はノイズ注入。「partial ρ は必要だが十分でない」（`project_relationship_perclass_investigation_2026_06_12`）の再々確認。
  - **C3 horse-draw-affinity（`draw_affinity_signal` / `horse_inside_edge` = 馬固有の内枠対外枠 edge、既 REJECT の global draw-bias とは別で per-horse conditional）EARLY-REJECT** — coverage 49.16%（n_prior>=10、n=49,924）。odds 統制 |ρ| <= 0.0073（interacted `draw_affinity_signal` は期待符号 +1 に対し逆符号 -0.003〜-0.004、raw `horse_inside_edge` ~0.007）、subgroup 強化なし・年別符号 mixed で probe floor 未達。per-horse 枠適性は odds 統制後に消失＝市場+GBDT が既捕捉（`project_draw_distspeed_momentum_ablation_2026_06_20` の global draw-bias REJECT と整合）。
  - **メタ知見（重要、cell pocket 評価への一般教訓）**: C2 の唯一の positive cell pocket は 函館（venue 02、n=432）で top1 Δ+2.32pp[LB95 +0.69]／place2 Δ+3.70[+1.39]（3/3 fold 陽性）だが place3 は Δ+1.39[LB95 -0.93] で頑健でない。同一の 函館 pocket は**機構的に無関係な複数の特徴量家族で繰り返し出現している**——同日 REJECT の pace-style-fit（top3_box +1.62[+0.463]）・same-day-bias（top1 +1.62[+0.23]）・jockey-switch（top1 +1.62[LB95 0.000]）に続き rotation-fit で少なくとも 4 例目。**互いに無相関の signal で同一 venue が繰り返し positive pocket を出す＝函館は baseline 不安定性による多重比較ノイズの磁石であり causal signal ではない**。今後の cell pocket 評価では venue 02 単独 pocket を特に懐疑的に扱うこと。JRA venue routing 自体は `project_venue_cell_round2_2026_06_20` で確立済みの DO-NOT-adopt。
  - **総括**: 3 候補全て REJECT、本番 `jra-cb-v9-sim-2013` 無変更。DO-NOT-RETEST（3 候補とも）。artifact: `apps/pc-keiba-viewer/tmp/candidate-batch-probe/`（`probe.py`→`probe_report.json`、`rot_fit.py`＋`rot_aggregate.py`→`rot_result_3fold.json`、`verdicts.json`）。
- **NAR probe batch 3 候補（rotation-fit / same-day jockey form / trainer-switch、probe-first、本節 JRA batch probe（直上の micro 仮説 batch）の NAR 対応版）全 REJECT（2026-07-02、DO-NOT-RETEST——JRA 版と合わせ switch/same-day/rotation 系は両カテゴリでクローズ）** — blind NAR store（`feat-nar-v9-new`、2023-2025、n=412,429 rows）で probe-first（odds 統制 partial Spearman |ρ|>=0.02 かつ subgroup 強化 かつ 3 年中 2 年以上同符号）検証。**恒久的な構造発見 2 点**: (a) **NAR 騎手は 1 日ちょうど 1 venue のみ騎乗**（max_venues=1、2023-2025 の 101,479 jockey-days で multi-venue 0.0%）——このため venue-scoped な `race_bango` 昇順がそのまま完全な leak-free 当日時系列であり、venue 内集計と全 venue 集計は同一（JRA と異なり variant を 1 つに限定できる）。(b) **NAR の転厩率は 4.04%（session 中の測定で 3.4-4.0%）で JRA の 1.48% の約 2.7 倍**——このため trainer-switch は JRA では EARLY-REJECT だったのに対し NAR でのみ probe を通過した。
  - **rotation-fit（`rotation_dev` / `rotation_dev_rel` = |days_since_last_race − 当該馬の好走時 interval 中央値|）EARLY-REJECT** — coverage 66.59%。odds 統制後の |ρ| は全 cut で 0.02 floor 未達（最大は `rotation_dev_rel` の n_prior_top3>=5 subgroup place3 -0.0167、同 pooled place3 -0.0157、`rotation_dev` subgroup place3 -0.0157）。place3 は 3/3 年同符号（負、正しい向き）だが magnitude が 0.02 に一度も届かず、win partial ρ は +0.0116 と逆符号。JRA analog（同 subgroup place3 -0.0235）より弱く、その JRA 版自体が probe 通過後 WF で covered-subgroup top1 -0.329pp で REJECT 済み——NAR は JRA の probe 水準を下回るため EARLY-REJECT は conclusive。CatBoost/XGB + `days_since_last_race` + odds が有用部分を既抽出。
  - **same-day jockey form（`sameday_jockey_ride_count` / `sameday_jockey_win_rate` / `sameday_jockey_top3_rate` / `sameday_jockey_avg_finish_norm`）EARLY-REJECT** — coverage 77.66%、control=log(odds) および log(odds)+`jockey_recent_win_rate`。raw Spearman は実在（win_rate -0.090、avg_finish_norm +0.121）だが odds + `jockey_recent_win_rate`（既に本番特徴）統制後は全 4 列が 0.02 floor 未達。最強の `sameday_jockey_avg_finish_norm` でも hot-jockey（先行騎乗>=4）subgroup で 0.0194 止まり。`sameday_jockey_ride_count` は騎手需要/番組編成 proxy で hot-jockey subgroup で 0.0048 に崩壊。同日の JRA same-day-jockey EARLY-REJECT（本節上記）と同一機構・同一結末（市場が当日 form をオッズへ既に織り込み）。
  - **trainer-switch（`trainer_quality_delta` / `trainer_recent_quality_delta` = 新厩舎 career/recent 勝率 − 前厩舎勝率 + `trainer_switched` / `switched_to_barn_experienced`）REJECT（probe PASS → WF null、本日 3 件目の probe-通過→WF-fail near-miss）** — probe 通過: switched==1 subgroup（n=16,668）で odds 統制 place3 partial ρ +0.044（`trainer_quality_delta`）/ +0.055（`trainer_recent_quality_delta`）、win +0.033 / +0.037、いずれも正しい向きで 3/3 年 >=0.02。だが 3-fold WF（`iter12-nar-xgb-hpo-v8` verbatim: 650 rounds、depth 7、lr 0.0527、`rank:pairwise`、train 2006+、pooled n=40,710 races 2023-2025、117 conservative reduced baseline + 4 switch 列、bootstrap LB95 2000 iterations）で clean null: top1 Δ+0.130pp[LB95 -0.012]／place2 Δ+0.064[-0.133]／place3 Δ+0.007[-0.202]／top3_box Δ+0.125[-0.025]——**7 指標中 LB95>0 は 0 件**で §7.2 accept gate 不成立。fold 非頑健（top1 2/3 年正だが 2024 -0.102pp、place2 2/3、place3 1/3、per-fold LB95 は全て 0 をまたぐ）で cell も隣接 cell で符号反転する多重比較ノイズのみ。有害ではない（worst place4 Δ-0.037[LB95 -0.226] は no-regression floor 内）が銀行できる gain も皆無。**機序**: 117 baseline に既に現調教師の質を表す trainer 系 10 列（career / keibajo / distance / horse-win-rate + style rate）が在るため、switch delta = 新質 − 旧質 は新質が既に特徴である以上ほぼ冗長——「partial ρ は必要だが十分でない」（`project_relationship_perclass_investigation_2026_06_12`）の再確認、および同日 JRA trainer-switch EARLY-REJECT と同結論。
  - **総括**: 3 候補とも本番無変更（`iter12-nar-xgb-hpo-v8`）。これで JRA/NAR 両カテゴリの switch/same-day/rotation 系はすべてクローズ。DO-NOT-RETEST（3 候補とも）。artifact: `apps/pc-keiba-viewer/tmp/candidate-nar-probe-batch/`（`probe_nar.py`→`probe_report.json`、`join_trainer.py`＋`run_arms.sh`＋`eval_trainer.py`→`wf_result.json`、`verdicts.json`）。
- **Ban-ei probe batch 3 候補（rotation-fit / same-day jockey form / trainer-switch、probe-first、本節 JRA batch probe（micro 仮説 batch）・NAR probe batch の Ban-ei 対応版）全 REJECT（2026-07-02、DO-NOT-RETEST——これで rotation-fit / same-day jockey form / trainer-switch の 3 候補は JRA/NAR/Ban-ei の cross-category sweep 完了）** — blind Ban-ei store で probe-first（odds 統制 partial Spearman |ρ|>=0.02 かつ subgroup 強化 かつ 3 年中 2 年以上同符号を満たした候補のみ WF へ昇格）検証、baseline は 130 本番特徴量相当（`banei-cb-v9-sim-2011` verbatim）、3-fold WF blind（train≤2022/2023/2024→blind 2023/2024/2025、bootstrap LB95 2000 iterations）。**恒久的な構造発見 2 点**: (a) **Ban-ei（帯広単一 venue）の転厩率はわずか 0.41%（193/47,451）**で feature 分散がほぼゼロ——単一 venue では厩舎がほぼ不変（JRA 1.48%・NAR 4.04% と対照的で、この構造差が trainer-switch の可否を分ける）。(b) 一方で**当日騎乗構造はリッチ**——82% のエントリが同日に prior >=1 騎乗を持ち平均 2.83 騎乗、rotation-fit 被覆も 83.6% で same-day / rotation 系は評価母数が十分。
  - **same-day jockey form（`sameday_jockey_ride_count` / `sameday_jockey_win_rate` / `sameday_jockey_top3_rate` / `sameday_jockey_avg_finish_norm`）EARLY-REJECT** — **「Ban-ei は市場が薄いのでオッズへの織り込みが弱く、当日 form が真の欠落信号になりうる」という thesis を明示的に検証したが不成立**。raw ρ +0.016 が popularity（人気）統制後に -0.001 へ消滅——薄い市場でも当日 form は既にオッズへ織り込まれており、JRA/NAR の same-day-jockey EARLY-REJECT と完全に同一機構。probe gate（|ρ|<0.02）未達で WF 省略。
  - **trainer-switch（`trainer_quality_delta` / `trainer_recent_quality_delta` = 新厩舎 career/recent 勝率 − 前厩舎勝率）EARLY-REJECT** — 上記の構造発見（転厩率 0.41%）により switched==1 subgroup が実質定数で母集団が構造的に死んでおり、probe 段階で EARLY-REJECT。JRA（1.48% で EARLY-REJECT）と同型だが Ban-ei はさらに希少で、単一 venue の Ban-ei では原理的に評価母数を確保できない lever。
  - **rotation-fit（`rotation_dev` / `rotation_dev_rel` = |days_since_last_race − 当該馬の好走時 interval 中央値|）REJECT（probe PASS → WF REJECT、本 batch 唯一の probe-通過→WF-fail near-miss）** — probe は境界通過（odds 統制後 pooled partial ρ +0.0158、長休養 dsl>=21 subgroup で +0.0573 と強化）。だが 3-fold WF（pooled n=5,232 races、CatBoost YetiRank verbatim、treatment=+rotation 列）で REJECT: top1 Δ-0.096pp[LB95 -0.497]／place2 Δ+0.115[-0.344]／place3 Δ+0.229[-0.287]／top3_box Δ-0.076、primary_positives 0 件、regressors=top1/place4/place5/top3_box。**決定打: probe 最強の長休養 subgroup（dsl>=21、n=2,651）でも全指標 LB95<0、より極端な dsl>=35 では delta が負転**——per-class/subgroup routing も不成立。**機序**: `days_since_last_race` は既に本番特徴量で GBDT が有用部分を既抽出済みのため、明示的な「最適ローテからの逸脱」列はノイズ注入にしかならない（JRA rotation-fit の covered-subgroup 悪化 REJECT と同結論、「partial ρ は必要だが十分でない」の再確認）。
  - **総括**: 3 候補とも本番無変更（`banei-cb-v9-sim-2011` sim デフォルト + `grade_code=='E'` の `banei-cb-v8-window2011-wf-15y` routing）。**これで rotation-fit / same-day jockey form / trainer-switch の 3 候補は JRA/NAR/Ban-ei 全カテゴリでクローズ**し、switch/same-day/rotation 系の cross-category sweep が完了、Ban-ei frontier 再確定。DO-NOT-RETEST（3 候補とも）。artifact: `apps/pc-keiba-viewer/tmp/candidate-banei-probe-batch/`（`eval_report.json` / `cell_report.json` / probe 出力）。
- **overnight mega-probe: 新規 micro 仮説 30 列・13 家族を probe-first で一括検証、全 REJECT（2026-07-02 深夜、DO-NOT-RETEST）** — 「本番 254-feat に無さそうな新規シグナル」を広く洗い出す夜間キャンペーン。13 家族 30 候補列を probe-first（odds 統制 partial Spearman + subgroup 強化 + 多年同符号）で選別し、通過分のみ 3 カテゴリの本番 loss で 3-fold WF（train≤2022/2023/2024→blind 2023/2024/2025、bootstrap LB95 2000 iterations）検証。ADOPT 0。
  - **事前 pre-emption 確認（恒久知見）**: 「未使用に見える」仮説の多くは本番 254 feature list に既在で dead——head-to-head（`h2h_encounter` / `win_count_vs_field`）・`weight_volatility_5`・`past_corner_1_norm_std` / `past_corner_1_norm_iqr_5`・`is_returning_from_layoff`・`horse_baba_win_rate` 等は feature-name 監査で既搭載を確認し着手前に除外した。新規性の事前確認を怠ると既存列の restatement を WF まで回してしまう。
  - **新しい再利用可能な方法論 2 つ**: (a) **variance/surprise channel probe**——`|finish − ninkijun|`（人気からの乖離＝upset の大きさ）を target に odds 統制 partial ρ を測り、mean-only probe が見逃す 2 次モーメント signal を検出。(b) **incremental probe**——control に log(odds) だけでなく既存の mean-finish 特徴群（`career_win_rate` / `recent_finish` / `finish_trend_5` / `last_3_avg_finish_norm` / `avg_finish` / `popularity_score`）を追加し、既搭載 mean 特徴で説明できる分を差し引いた真の増分だけを残す。
  - **本命 finish-consistency family（`fn_std_5` / `fn_std_10` / `fn_cv_5` / `fn_iqr_5`＝過去着順の分散/変動係数/IQR、store に着順分散列はゼロで GBDT は行内から再構成不能）は probe 全統制を強く通過（`fn_std_5` の surprise channel partial ρ +0.058(pooled)/+0.052(subgroup)、incremental probe でも place3 partial ρ が mean 特徴統制後に -0.044 で残存）したが、WF で 3 カテゴリ × 2 loss（JRA/Ban-ei CatBoost YetiRank・NAR XGBoost `rank:pairwise`）すべて REJECT**。JRA（pooled n=10,365）top1 Δ-0.058pp[LB95 -0.386]／place2 Δ-0.097[-0.502]（§7.2 no-reg floor -0.05pp 超過）／place3 Δ+0.222[-0.203]、gate FAIL。NAR（pooled n=40,710）top1 Δ-0.086pp[LB95 -0.236] で per-year 一様負（2023 -0.037／2024 -0.117／2025 -0.104pp）。Ban-ei（pooled n=5,232）place3 Δ-0.229pp[LB95 -0.726]／top3_box Δ-0.172[-0.440]。
  - **機構的発見（重要、新規）**: consist / market-alpha 系の odds と直交な 2 次モーメント signal は **exact top1-3 を改善しないが place4-6 / top3_box を頑健に改善する**——JRA で consist place4 Δ+0.560pp[LB95 +0.125]、market-alpha-expected（`alphaexp` arm）place4 Δ+0.424[+0.010]／place5 Δ+0.463[+0.039]／top3_box Δ+0.309[+0.068]（いずれも LB95>0）。loss 非依存・カテゴリ非依存で再現。解釈: **市場（odds）は着順の平均を織り込むため exact top1-3 は動かせないが、深い着順の並びだけが 2 次モーメントで tighten される**。place4-6 特化 variant が理論上唯一の open direction だが、現行 accept gate（primary=top1/place2/place3）の範囲外のため本セッションでは非採用。
  - **その他 EARLY-REJECT（probe floor |ρ|<0.02 未達、全て odds/既存質列の壁）**: strength-of-schedule（`sos_field_q_5` / `sos_qadj_finish_5`）・market-alpha（`market_alpha_5` / `market_alpha_10`、mean channel の primary は floor 未達）・same-day trainer form（`sd_tr_win_rate` / `sd_tr_top3_rate` / `sd_tr_ride_count`）・trainer hot-streak（`tr_hot30_*`）・owner form（`owner_yr_*`）——いずれも trainer/owner の質列との共線と市場の壁で same-day jockey（本節上記）と同機構。time-consistency soha（`soha_pm_std_5` / `soha_pm_cv_5`＝走破タイム分散）は finish-consistency と冗長で probe が null（ρ≈0.000）。form-streaks（`top3_streak` / `win_streak`）・interval-regularity（`interval_std_5` / `interval_cv_5`）・Nth-off-layoff（`starts_since_layoff`）・racing-load（`races_last_90d` / `races_last_180d`）・experience-rank（`experience_rank_in_race`）も同様に floor 未達。
  - **総括**: 13 家族 30 列すべて REJECT、本番 4 モデル無変更（`jra-cb-v9-sim-2013` / `iter12-nar-xgb-hpo-v8` / `banei-cb-v9-sim-2011` + `grade_code=='E'` の `banei-cb-v8-window2011-wf-15y`）。DO-NOT-RETEST。artifact: `apps/pc-keiba-viewer/tmp/candidate-mega-probe/`（`probe_store_report.json` / `probe_incremental_report.json` / `probe_pg_report.json` / `arm_results.json` / `nar_consist_result.json` / `banei_consist_result.json` / `build_*.py`。訓練 store コピーは disk 回収のため削除済み、`build_*.py` で再生成可）。
- **文献調査由来の新規候補 5 件 全 REJECT（2026-07-02 深夜、文献調査由来、DO-NOT-RETEST）** — `docs/journals` の Journal of Equine Science 論文 119 本 + 「馬の科学」corpus を 5 テーマ（馬齢・成長 / 体重・遺伝 / 血統・心肺 / 走法・故障 / 生理・概日）で並列 survey し、「実データにマップ可能 + 本節と非重複 + leak-free に構築可能」な候補に triage。大半は生理実測値（心拍・乳酸・VO2max・血液マーカー・体格実測）が race DB に存在せず実装不能、または既搭載列 / 既 REJECT 済みシグナルの重複で除外。実装可能な新規候補として Ban-ei 馬体重 within-race relativization（上記 Ban-ei ブロックに別記）＋以下 5 件を本番 base で 3-fold WF 検証し全 REJECT。market-efficiency の壁と place4-6 shuffle の壁を、競走馬科学の知見からも別角度で再確認した。
  - **候補 1 — sire cross-cell specialization gap（種牡馬の turf/dirt × 距離帯ごとの得意差、論文 9_3_89、Grade A）REJECT**: 新規性は厳密確認（既存 store は matched-cell rate のみで cross-cell contrast 列は無い。production 集約を corr 0.99996 で再現した上で 6 contrast 列を pure incremental として追加）。しかし `jra-cb-v9-sim-2013` の 254-feat baseline 上の 3-fold WF pooled（n=10,365）で top1 Δ-0.145pp[LB95 -0.492] / place2 Δ-0.174pp[LB95 -0.608]、全 primary 回帰。probe の specialist pocket（dirt / sprint / extended-distance の place3）も blind で消失。機序 = matched rate + odds が既に吸収し、254-feat GBDT が cross-cell 構造を非線形に捕捉済み（partial ρ は既 REJECT の pedigree signal の ~1/10）。artifact `apps/pc-keiba-viewer/tmp/candidate-sire-spec/`。
  - **候補 2 — 3 世代近交係数 + blood proportion（`inbreeding_coef_3gen`、論文 35_2407、Grade B）EARLY-REJECT**: raw finish ρ +0.026 が odds 統制で +0.003 に崩壊し、年で符号反転（2023 +0.005 / 2024 +0.007 / 2025 -0.003）。3 世代深度では近交係数の分散が小さく（多くが COI=0）、有名近交血統は低オッズで市場が織り込み済み。probe floor 未達で WF まで進めず。
  - **候補 3 — race time-of-day / circadian（NAR、未使用の `hasso_jikoku`、論文 8_3_81、Grade B）EARLY-REJECT**: NAR は実際に ~10h の発走時刻分散（夜間 18-20 時に runner の約 23%）がありデータ軸は実在するが、within-race で変動する `tod_novelty` / `is_first_night_race` は pooled で null（`tod_novelty` rho_odds +0.002、pass_magnitude False）、`race_hour` は**レース内定数**で ranker を並べ替え不能——`H-RS-KEIBAJO-IMPUTE`（NAR venue ブロック）と同一の失敗モード。
  - **候補 4 — within-cohort relative age（若馬限定の同世代内月齢、Grade B）PROBE-PASS → WF REJECT**: batch 唯一の odds 直交 probe signal（`age_in_days_rank` partial ρ -0.041、pass_magnitude True、若馬 subgroup で強化）だが、WF で place4 Δ+0.531pp[LB95 +0.087] を上げる一方 exact place2/place3 を regress（place3 Δ-0.174pp[LB95 -0.589]）。仮説の集中先である **maiden cell（n=908）で place3 Δ-0.441pp** と負——「若馬コホートが月齢差を解禁する」仮説は gate に対して反証。`age_in_days_rank` は同世代フィールド内で変動するので barei（そこでは定数）とは別軸だが、barei-REJECT lineage と同じ「place4-6 shuffle、exact top1-3 は不動」の壁。
  - **候補 5 — transport distance（前走→今走 keibajo 間の great-circle km、Grade B）PROBE-magnitude-only → WF REJECT**: pooled で primary LB95>0 はゼロ（top1 Δ+0.116pp[LB95 -0.183] / place2 Δ+0.058pp[LB95 -0.328] / place3 Δ+0.328pp[LB95 -0.039]）、coverage 制限で place3 Δ+0.507pp[LB95 +0.015] だが 3 primary 中 1 のみ。遠征 venue 間で内部矛盾——函館（venue02）は place2/place3 Δ+2.55/+2.31pp[LB95 +0.232] positive だが同じ北海道の札幌 place3 は Δ-0.60pp——transport 機序でなく small-n noise。artifact `apps/pc-keiba-viewer/tmp/candidate-litB-probe/`。
  - **総括**: 文献調査は「119 論文 → 実装可能候補 6 群 → 全 REJECT」。再利用可能な副産物 = variance/surprise channel probe（overnight mega-probe と共有）＋ 論文 triage の枠組み。本節の各 REJECT が示す「partial ρ は必要だが十分でない」「market が織り込み済み」「place4-6 shuffle で exact top1-3 は不変」の教訓を、競走馬科学の文献由来候補でも裏づけた。本番 3 モデル無変更（`jra-cb-v9-sim-2013` / `iter12-nar-xgb-hpo-v8` / `banei-cb-v9-sim-2011`）。

採否判定は必ず本番 serve system（base + ensemble、正しい特徴量数）を baseline とし、cell 単位で rank 1-6 を評価すること。
