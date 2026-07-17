# JRA 夏開催 (函館/福島/小倉) 本番 serving 監査 — 2026-06-01〜07-12

- **監査日**: 2026-07-17
- **対象**: JRA venue 02 (函館) / 03 (福島) / 10 (小倉)、2026-06-01 以降の全確定レース。venue 01 (札幌) は 2026 年通年で `jvd_ra` に 1 行も存在せず未開幕 — 確認のみで対象から除外。
- **データソース**: local PG mirror (`postgresql://127.0.0.1:15432/horse_racing`, TCP 直結で Apple container runtime インスタンスであることを `pg_postmaster_start_time` / `inet_server_addr`=`192.168.64.2` と `container list` の IP 一致で確認済み。colima 側の同名コンテナ [`docker ps` で確認できる shadow 双子] には接続していない) と Neon primary (`NEON_PRIMARY_URL`, 読み取り専用セッション)。両者は完全に一致 (venue 02/03/10 いずれも行数・日付レンジ一致)。
- **手法**: `jvd_ra` を `trim(shusso_tosu) not in ('','00')` で確定レースのみ抽出 (jvd placeholder 罠ガード)、`race_finish_position_model_predictions` と `(kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)` で突合。ルーティング期待値は `cell_routing.json` + `cell_router.py` の first-match-wins ロジックを Python で再実装 (venue ルール込み) して算出。精度指標は `serve_accuracy_report.py::aggregate_fp_metrics` と同一定義 (top1 / place2 / place3 / fukusho_2p / top3_box)。

## TL;DR

対象 264 レース (函館120 / 福島72 / 小倉72) のうち、**実際に「まともな品質の」着順予測を viewer が表示できたレースは 0 件**だった。内訳は次の 4 種のみ:

1. **154 レース (58.3%)** — `race_finish_position_model_predictions` に行が一度も存在しない (完全な欠落)。
2. **36 レース (13.6%, 06-27 全体)** — WIN5 オーバーレイ行のみが存在し、真の着順予測は 0 件 (別モデル `win5-xgb-v7-lineage-v1-rs-overlay-20260627` が同一テーブルを共有しているだけ)。
3. **72 レース (27.3%, 07-11+07-12 の全レース)** — 正しい `cell_routing.json` ルールに従うモデル版で頭数分すべて埋まっている「完全 coverage」に見えるが、**実測 top1 ≈ 8.3%・市場単勝1番人気の top1 32.4% を大幅に下回り、既知の健全ベースライン (`serve_accuracy_report.py` docstring: FULL 44.71% / DEGRADED 31.78%) にも届かない、ほぼランダムに近い品質**。score の within-race 標準偏差も健全時の 1/11 程度に潰れており、退化した (壊れた/placeholder な) 特徴量からスコアリングされた強いシグネチャを示す。この 72 レースの予測はすべて単一の書き込みイベント (2026-07-12 14:51-14:52 JST、36 レースを 23 秒で処理する明らかな一括スクリプト実行、ドキュメント記載の per-race Cloudflare Container パイプラインの挙動とは一致しない) に由来する。
4. **2 レース (07-07 に単発バックフィル)** — n が小さすぎて品質評価不能。

一方、**唯一「健全」な精度 (top1=42.86%、市場超え) を示したのは 2026-07-11 10:47 JST の Mac ローカルバッチ fallback (21 レース、`docs/finish-position-prediction-system.md` §1.2 に記載の同日中に無効化されたインシデント本体)** だったが、これは cell_routing 非対応の stale image で書かれたため誤ったモデル版 (plain default) の下に存在し、viewer の priority-0 機構は正しいモデル版 (= 上記③の壊れたデータ) を優先するため、**この健全な予測はユーザーには一切見えない**。

加えて、コードレベルで新規の defect を 1 件特定 (`focused-full-completion.ts` の `expectedModelVersion()` が venue==02 ルールを欠落) — 生きた D1 ログで false-positive re-trigger を確認済み。

安全確認できない状態で admin API backfill は実行していない (理由は「対処」節参照)。データ削除・書き込みは一切行っていない。

---

## 1. 対象レース台帳と serving 突合

| date (DOW)       | venue          | races   | 真の coverage | 完全欠落 | WIN5-only masked |
| ---------------- | -------------- | ------- | ------------- | -------- | ---------------- |
| 2026-06-13 (Sat) | 02 函館        | 12      | 0             | 12       | 0                |
| 2026-06-14 (Sun) | 02 函館        | 12      | 1\*           | 11       | 0                |
| 2026-06-20 (Sat) | 02 函館        | 12      | 0             | 12       | 0                |
| 2026-06-21 (Sun) | 02 函館        | 12      | 0             | 12       | 0                |
| 2026-06-27 (Sat) | 02/03/10 (3場) | 36      | 0             | 0        | 36               |
| 2026-06-28 (Sun) | 02/03/10 (3場) | 36      | 0             | 36       | 0                |
| 2026-07-04 (Sat) | 02/03/10 (3場) | 36      | 0             | 36       | 0                |
| 2026-07-05 (Sun) | 02/03/10 (3場) | 36      | 1\*           | 35       | 0                |
| 2026-07-11 (Sat) | 02/03/10 (3場) | 36      | 36†           | 0        | 0                |
| 2026-07-12 (Sun) | 02/03/10 (3場) | 36      | 36†           | 0        | 0                |
| **合計**         |                | **264** | **74**        | **154**  | **36**           |

\* n=1 の単発バックフィル (2026-07-07 生成、対象レースの 3 週間以上後)、predicted_rank=1 の的中はいずれも外れ。品質評価に値しない。
† 「真の coverage」＝期待モデル版で頭数一致。ただし §3 の通りこの 72 レース全ての実測精度はほぼランダム。

**主要な観測**: 06-13, 06-20, 06-21, 06-28, 07-04 の 5 開催日 (156 レース) は完全に無音。これは既知メモリ「FP serving 6週間 blackout (5/25-7/7)」と符合し、対象 3 場について 07-05 まで blackout が実質継続していたことを追加確認した (memory の 7/7 という終了日以降、この 3 場での最初の genuine coverage は 07-11)。06-27 は別モデル (WIN5 overlay) の存在によって欠落が隠蔽される回避パターンだった。

## 2. Defect カタログ

### Defect A (最重要・現在進行形): 「coverage あり」に見える 72 レースの予測品質がほぼランダム

- **件数**: 72 レース (07-11 全 36 + 07-12 全 36)。対象監査期間で唯一「完全 coverage」を達成した日。
- **代表 race_id**: `jra:2026:0712:02:01` (函館 R1)、`jra:2026:0711:03:02` (福島 R2)。
- **実測**:

  | 集団                                                               | n races | top1                              | place2 | place3 | fukusho_2p      | race内 score stddev (平均) |
  | ------------------------------------------------------------------ | ------- | --------------------------------- | ------ | ------ | --------------- | -------------------------- |
  | Cluster B (2026-07-12 05:51-05:52 UTC ＝ 14:51-14:52 JST 一括書込) | 72      | **8.33%**                         | 19.44% | 29.17% | —               | **0.095** (0.05〜0.15)     |
  | Cluster A (2026-07-11 01:47 UTC ＝ 10:47 JST Mac batch fallback)   | 21      | **42.86%**                        | 52.38% | 66.67% | —               | **1.085** (0.72〜1.57)     |
  | 市場 (単勝1番人気、同一 74 レース)                                 | 74      | 32.43%                            | 48.65% | 58.11% | 74.32%          | —                          |
  | `serve_accuracy_report.py` 記載の既知健全 baseline                 | —       | 44.71% (FULL) / 31.78% (DEGRADED) | —      | —      | 74.79% / 57.76% | —                          |

  Cluster B は市場にも劣り、既知の DEGRADED baseline にすら届かない。venue 別でも 02=12.50%, 03=4.17%, 10=8.33% と一貫して低く、特定 venue のみの問題ではない。model_version 別でも champion (`jra-cb-v9-sim-2013-clean`, n=30, top1=3.33%) と routed variant (`jockey-pedigree269`, n=42, top1=9.52%) の両方が同様に悪く、**特定モデルの重みの問題ではなくこのバッチが使った入力側の問題**であることを示す。score の within-race 標準偏差が健全時 (Cluster A: 平均1.085) の約 1/11 (Cluster B: 平均0.095) に潰れているのは、モデルがほぼ差別化できない (退化した/placeholder に近い) 特徴量を渡された典型的なシグネチャ。

- **原因仮説 (確度: 中 — 状況証拠に基づく)**: Cluster B の書込タイムスタンプは 36 レースを 23 秒で処理する厳密に逐次的なパターン (venue → race_bango 順、0.6〜0.7 秒間隔) であり、ドキュメント記載の per-race Cloudflare Container full pipeline (JRA 実測 worst-case 約 27.5 分/レース) とは整合しない。同日 (2026-07-12) の git log は `finish-position-cron` / `finish-position-predict-container` の大規模リファクタ (day-base Phase1 split、`is_final_race` cell-routing dimension 追加、coverage-self-heal cron 配線) が午前中に連続しており、直後 1 時間20分後には `fix(finish-position): populate R2 feat-cache for focused-full requests` (focused-full request が R2 feature cache を構造的に一度も populate していなかった、という別の欠陥の修正) がコミットされている。これらは Cluster B が「本番の自動 per-race パイプラインの成功結果」ではなく、**その日の開発/検証セッション中に手動または簡易スクリプトで実行された一括バックフィル/スモークテストであり、実特徴量ではなく劣化した入力 (欠損/デフォルト値埋め) でスコアリングされた**可能性が高いことを示唆する。ただし、生成スクリプト自体を直接特定する証跡 (git コミットに紐づく実行ログ等) は本監査の read-only 権限では確認できていない。
- **対処状況**: **未対処**。理由は §4「対処」参照。**現在も Neon に live で存在し、viewer の priority-0 機構により正しい (と誤認される) 予測として表示され続けている。**

### Defect B: 唯一の健全な予測 (Cluster A, 21 レース) が viewer から恒久的に不可視

- **件数**: 21 レース (函館12 / 福島4 / 小倉5)。
- **代表 race_id**: `jra:2026:0711:02:01` 〜 `:02:12` (函館全 12 レース)。
- **内容**: 2026-07-11 10:47 JST の Mac ローカルバッチ fallback (`docs/finish-position-prediction-system.md` §1.2 記載の同日中に無効化されたインシデントそのもの) が書いた `model_version='jra-cb-v9-sim-2013-clean'` (plain default) の行。ルーティング未対応の stale image で書かれたため、函館 (venue=02) や 703-joken レースなど本来 `jockey-pedigree269` / `prior-corner274` に routing されるべきレースにも default モデルが書かれている。しかし **この default 行自体の予測品質は健全** (top1=42.86%、市場超え、score 分散も健全)。
- **問題**: viewer の priority-0 機構 (`finish-position-cell-routing.ts`) は「cell-routing 期待モデル版」を最優先で拾う設計 — これは 2026-07-11 の display-priority incident (正しい routing が Mac fallback の一括上書きで見えなくなった事故) の再発防止のために導入されたものだが、皮肉にも今回は **正しい routing 変数の下に Defect A の壊れたデータ (Cluster B) が存在するため、priority-0 がその壊れたデータを最優先表示し、同じレースに存在する健全な default 行 (Cluster A) を完全にシャドーイングする**。函館 12 レース全てで該当。
- **対処状況**: 未対処 (コード変更なし、read-only 監査のため)。

### Defect C: `focused-full-completion.ts` の `expectedModelVersion()` が venue==02 ルールを欠落 — 完了済みレースへの false-positive re-trigger

- **件数 (構造的露出)**: 函館 120 レース中 **62 レース (51.7%)** が対象。703-joken による一致 (50 レース) を除く、venue ルールのみに依存する全レースが該当。今後の函館開催日でも継続して発生する。
- **代表 race_id**: `jra:2026:0712:02:11` (自己修復ログで確認)。
- **コード根拠**: `apps/finish-position-predict-container/src/predict_lib/cell_routing.json` の JRA ルール 3 番目「`venue == "02"` → `jockey_pedigree_703` variant (= `jra-cb-v9-sim-2013-clean-jockey-pedigree269`)」は `cell_router.py::resolve_variant` の first-match-wins で確かに適用される (本番 scoring の正式な経路)。しかし `apps/finish-position-cron/src/focused-full-completion.ts::expectedModelVersion()` (queue の "already complete" 判定と coverage-self-heal cron の両方が使う完了チェック関数) は prior-corner-005 ルールと joken=703 ルールしか実装しておらず、**venue==02 分岐が存在しない**。既存テスト `focused-full-completion.test.ts` も venue ケースを一切カバーしていない。この結果、函館の非703/非prior-corner レースは常に「default モデルが N 行あるか」を確認してしまい、実際には `jockey-pedigree269` が正しく N 行揃っていても complete と判定できない。
- **実データでの確認**: D1 `finish_position_coverage_gap_events` (self-heal cron が 2026-07-12 04:26 JST に配線されて以降、`enqueued`/`escalated` イベントのみを記録する insert-only ログ) を remote 照会したところ、JRA 対象で記録された **6 件全て**が該当。うち 5 件 (函館 R05/R08/R09/R10/R11) は self-heal 発火時刻の **45 分以上前に、頭数一致で `jockey-pedigree269` が既に正しく書き込み済み**だったことを Neon の `prediction_generated_at` と突合して確認した (例: R11 は 14:52:16 JST 生成済み・self-heal 発火は 15:37:49 JST)。つまり全て false-positive による無駄な再トリガーであり、genuine なギャップ検出は 1 件 (小倉 R07、こちらは実際に self-heal 発火時点で未生成だった) のみ。
- **影響**: 完了済みレースへの無駄な再エンキュー (`MAX_SELF_HEAL_ENQUEUES_PER_RACE=2` まで)。各再トリガーは共有の `predict-jra` Container プロセス slot を最大 ~27.5 分占有しうるため、同日同時間帯に走る福島/小倉レースの slot starvation (§5.4 item 13-16 で既知の failure mode) を誘発するリスクがある。データ破損や誤ったモデル版での上書きは確認されなかった (UPSERT は同一正しい値で idempotent に再書込されるのみ)。
- **修理提案 (コード変更は未実施、提案のみ)**: `expectedModelVersion()` に venue=="02" 分岐を追加するか、根本的には `apps/pc-keiba-viewer/src/lib/finish-position-cell-routing.ts` が `cell_routing.json` に対して持つ parity test と同種の仕組みを `focused-full-completion.ts` にも適用し、ルール変更時にこのファイルの追従漏れを機械的に検出できるようにすることを推奨する。

### Defect D (informational, 非バグ): WIN5 オーバーレイが同一テーブルを共有し、素朴な coverage チェックを欺く

- **件数**: 06-27 の 36 レース全て。
- **内容**: `generate-win5-overlay.ts` が `model_version='win5-xgb-v7-lineage-v1-rs-overlay-20260627'` として同じ `race_finish_position_model_predictions` に行を書く。viewer 側 `FINISH_POSITION_LEAK_FREE_MODEL_VERSIONS` allowlist (`FINISH_POSITION_LEAK_FREE_BASE_MODEL_VERSIONS` + `getAllFinishPositionDisplayPriorityModelVersions()`) にはこの model_version が含まれておらず、**viewer が誤って表示することはない**ことをコード読解で確認 (defect ではない)。ただし本監査のような「テーブルに行があるか」だけを見る素朴な coverage チェックは誤検知しうるため、将来の監査/監視のために明記する。

### Defect E: viewer allowlist 網羅性 — 問題なし (確認のみ)

`getAllFinishPositionDisplayPriorityModelVersions()` は `FINISH_POSITION_CELL_ROUTING_CONFIG` の全 variant (`jockey_pedigree_703`, `prior_corner_dirt_smallfield_005`, `sim`) を機械的に収集するため、`jra-cb-v9-sim-2013-clean-jockey-pedigree269` / `jra-cb-v10-prior-corner274-2013` は両方とも allowlist に含まれることをコードで確認した。2026-07-11 incident の修理は健在。

## 3. venue02 routed-arm (`jockey-pedigree269`) の実測効果 — 評価不能

団長の依頼にある「venue02 の routed 269 と 703 route が当たったレースを層別」した効果測定は、**現在の Neon データからは評価不能**と結論する。理由: 本監査期間に存在する `jockey-pedigree269` 行は 100% が Defect A の壊れた Cluster B 由来であり (単発バックフィル2件を除く)、Cluster A (健全) 側には venue02/703-joken レースの `jockey-pedigree269` 行が一切存在しない (default モデルの下にのみ健全な予測が眠っている)。したがって「ルーティング自体が有効か」と「このバッチの入力が壊れていたか」の 2 つの効果が完全に交絡しており、分離できない。**Cluster A の函館 12 レース (top1=41.67%) と福島/小倉の非703レース (同バッチ内、champion default) を比較すれば venue 単体の base rate は見えるが、これは "269 モデルの効果" ではなく "default モデルの函館での base rate" である**ため、この監査ではそれ以上の結論を出さない。健全な (Cluster A 品質の) 269 モデル出力が Neon に載った時点で改めて評価すべき。

## 4. 対処 (欠落レースの後埋めについて)

154 件の完全欠落レースおよび Defect A の壊れた 72 レースについて、**本番 admin API 経由の focused-full 再トリガーは実行しなかった**。理由:

1. 監査対象は全て**過去に終了した確定レース**であり、ユーザーが賭票判断に使う実利は既に消滅している (レース結果は確定済み)。
2. §2 Defect A の通り、直近 (2026-07-12) に同じパイプライン周辺で「per-race full generation を名乗りながら実際にはほぼランダムな出力を返す」事象が発生し、**その根本原因はこの監査 (read-only) の範囲では未特定**。この状態で欠落 154 件を一括で admin API 再トリガーした場合、Defect A と同種の劣化した予測が「正常に coverage された」ように見える形でさらに Neon に書き込まれるリスクがあり、「予測が存在しない」よりも「自信満々に見える誤った予測が存在する」方が下流 (viewer / 将来の学習データ) にとって有害である。
3. team-lead の指示「安全に後埋め可能なら…起動、できない/不確実なら手を出さず報告に列挙」に従い、上記の不確実性を理由に見送った。

**推奨アクション (実装は別 agent/次セッションへ)**: Defect A の根本原因 (退化した入力の発生源) を先に特定・修正し、Cloudflare Container の genuine per-race 出力で健全な score 分散 (stddev ≳ 0.7〜1.5 目安、Cluster A 実測値を参照) が確認できることを 1 レースで smoke test してから、初めて欠落分の backfill を検討すべき。07-18 (土) は次回 JRA 開催日であり、その日の live serving を実際に観測して Cluster A 相当の品質が再現するか確認するのが最も安全な次の一歩である。

データの削除・UPDATE は一切行っていない。全操作は Neon 3種 (`neon_primary`, readonly session)・local PG (readonly session)・D1 (`wrangler d1 execute --remote`, SELECT のみ) の read 系のみ。

## 5. データ鮮度

- local PG mirror: `jvd_ra` / `jvd_se` とも最新レース日 = **2026-07-12** (全 venue 含む)。
- Neon primary: 同じく `jvd_ra` / `jvd_se` 最新レース日 = **2026-07-12**、venue 02/03/10 の行数・日付レンジも local PG mirror と完全一致。
- 07-13〜07-17 (本監査日) の間 JRA venue 02/03/10 に確定レースは存在しない (次回開催は 07-18 土)。したがって「鮮度ギャップ」ではなく、単に開催が無い期間。
- `race_finish_position_model_predictions` の JRA venue 01/02/03/10 における最終書込は 2026-07-12T05:52:32Z (= Defect A の Cluster B) のままで、それ以降 (07-13〜07-17) 一切更新されていない。テーブル全体 (他カテゴリ含む) では 2026-07-15T19:48Z まで活動がある — NAR 等他カテゴリでは pipeline が動き続けている一方、対象 3 場については 07-12 以降まったく触れられていない。
- 参考: 対象4venue のうち **01 (札幌) は 2026 年に `jvd_ra` へ 1 行も存在せず** (`kaisai_nen='2026' and keibajo_code='01'` で 0 件)、未開幕であることを確認した。

## 6. 補足: Defect A/B の技術的検証詳細

- ランダムな join バグではないことのクロスチェック: `predicted_rank` の race 内重複ゼロ・1..N 連番であることを確認。Python 側の集計ロジックとは独立に、model_version を `jockey-pedigree269` に固定した純 SQL のみでの top1 再計算 (n=40) でも 10.00% と、Python 集計 (9.52%, n=42) と整合する結果を得た (バグではなく実測)。
- Cluster A / B の切り分けは `prediction_generated_at` の書込クラスタ (`date_trunc('minute', ...)` で group化) から機械的に検出した。07-11 は 2 つの明確なクラスタ (01:47 UTC 21件、05:51-05:52 UTC 22+14件) に完全分離し、時間的に重複しない。
- MLflow backend (`HORSE_RACING_MLFLOW_BACKEND_URI`) に `finish-position/production-usage` experiment で該当日の "serving as data" ログが 2026-07-12 13:31-14:22 JST (Cluster B の書込から約 7〜8 時間後) に記録されているのを確認したが、これは `cf_serving_recorder.py` による**事後の読み取り専用集計ログ**であり、Cluster B 自体の書き込み元を特定するものではない (`fp_races_live=15, fp_races_backfilled=0` のようなタグが付いているが、この分類ヒューリスティックが「同一暦日に書かれたか」以上の判定をしているかは未検証)。

## 7. 追加検証 (team-lead 指示、独立手法によるクロス確認 — 2026-07-17 追記)

並行 agent (summer-baseline) から「`jockey-pedigree269` の 2026 serve が top1 0/29」という重大疑いが team-lead 経由で共有され、専任診断 agent (serve-defect-269) が根本原因調査を担当することになった。以下は本監査が既に持つデータに対し、team-lead 指定の独立手法 (ASC per-horse dedup、優勝馬 predicted_rank 分布、viewer priority-tier のコード読解) を適用したクロス確認であり、根本原因の特定はスコープ外 (serve-defect-269 の担当) として深追いしない。

### 7.1 per-horse ASC (最古書込優先) dedup による model_version 別精度

race×horse ごとに、その組に存在する全 model_version 行の中から `prediction_generated_at` が最古の 1 行を採用 (同一レースで複数 model_version が競合する場合に「最初に生成された予測」を選ぶ手法。1412 組中 285 組が複数 model_version 競合)。この最古行自身の model_version で group化した。

| model_version (ASC選出)                                           | races | top1      | place2 | place3 |
| ----------------------------------------------------------------- | ----- | --------- | ------ | ------ |
| `jra-cb-v9-sim-2013-clean` (champion)                             | 51    | 19.61%    | 29.41% | 39.22% |
| `jra-cb-v9-sim-2013-clean-jockey-pedigree269`                     | 22    | **0.00%** | 9.09%  | 27.27% |
| `jra-cb-v10-prior-corner274-2013`                                 | 1     | 0.00%     | 0.00%  | 0.00%  |
| `win5-xgb-v7-lineage-v1-rs-overlay-20260627` (参考、対象外タスク) | 36    | 22.22%    | 25.00% | 30.56% |

**269 は独立手法 (ASC dedup, n=22, 対象は venue 02/03/10 の 06-01〜07-12 のみ) でも top1=0.00% と、並行 agent の報告 (0/29、恐らく全 JRA venue・全 2026 スコープ) と方向性が一致し、独立に再現確認できた。** 274 は n=1 のため結論不可 (母数不足、監視継続を推奨するのみ)。

champion (n=51) をさらに書込クラスタで分解すると (§2 Defect A の Cluster A/B 定義を再利用):

| bucket                                      | races | top1                          |
| ------------------------------------------- | ----- | ----------------------------- |
| Cluster A (Mac batch, 2026-07-11 10:47 JST) | 21    | 42.86% (健全)                 |
| それ以外 (実質 2026-07-12 一括書込)         | 30    | 3.33% (Defect A と同一の劣化) |

champion default 自体も「どの書込クラスタか」で精度が全く異なる。ただし **269 は「他クラスタと競合しない単独最古書込」に限定した ASC dedup (n=22) でも 0.00%** であり、champion の劣化 bucket (3.33%) よりさらに悪い — Defect A (2026-07-12 一括書込全体の入力劣化) だけでは 269 の 0% を完全には説明しきれず、269 固有の追加劣化要因がある可能性を否定できない。**この切り分け自体は serve-defect-269 の根本原因分析に委ねる。**

### 7.2 優勝馬の predicted_rank 分布 (一様 = 特徴ズレ、末尾偏り = 順位反転 signature)

model_version の生行全件 (dedup なし) で、実際の優勝馬 (`kakutei_chakujun='01'`) が何位と予測されていたかを集計した。

| model_version         | n races | avg field size | 優勝馬 predicted_rank 平均 | 一様乱択期待値 | 下位半分入り       |
| --------------------- | ------- | -------------- | -------------------------- | -------------- | ------------------ |
| `jockey-pedigree269`  | 42      | 13.8           | 6.36                       | 7.40           | 35.7% (15/42)      |
| `prior-corner274`     | 2       | 8.5            | 3.50                       | 4.75           | 50.0% (1/2、n僅少) |
| champion (混合、参考) | 51      | 12.9           | 4.94                       | 6.93           | 35.3% (18/51)      |

**269 の分布は「一様 (feature drift/無情報化)」でも「末尾偏り (順位反転)」でもない。** 優勝馬の predicted_rank 平均 (6.36) は一様乱択期待値 (7.40) より明確に良く、下位半分入りの割合 (35.7%) も乱択期待の 50% を下回る — 弱いながら方向として正しいシグナルは残っている。にもかかわらず predicted_rank=1 の的中率が (§7.1 の ASC dedup 単独評価で) 0% なのは、§2 Defect A で確認済みの「レース内 score 標準偏差が健全時の 1/11 に潰れている」現象と整合する解釈が成り立つ: 弱いが正しい方向のシグナルは残存するが、上位候補間の score 差が数値的にほぼゼロまで圧縮されているため「誰を1位予測にするか」という最終選択だけが実質ノイズで決まる。**順位反転バグよりも、入力特徴量の減衰/圧縮 (欠損値のデフォルト埋め等) による signal attenuation が疑わしいという解釈を、独立指標 (score 分散 + rank 分布) の双方から補強する。** 断定はしない — あくまで serve-defect-269 への仮説提供。

### 7.3 venue02 routing 生存期間の viewer 実効果 (ユーザー影響の定量化)

`apps/pc-keiba-viewer/src/db/queries.ts::getFinishPositionLambdarankPredictions` (~2919行) の priority CTE を直接読解した。`priority=0` tier (`cellVariantModelVersion` = `resolveFinishPositionDisplayPriorityModelVersion` が返す cell-routing 表示優先候補) は、**その model_version の行が 1 行でも存在すれば無条件で最優先 select される** (他 tier の recency とは無関係、`union all` で priority 整数を付けて後段で `order by priority, recency desc limit 1` する構造をコードで確認)。

venue02 ルール (`cell_routing.json` rule 3) が本番投入された 2026-07-11 以降、本監査で確認できる函館開催日は 07-11・07-12 の 2 日 (計 24 レース) のみだが、**この 24 レース全てで `jockey-pedigree269` 行が存在する (Defect A の一括書込由来)**。priority-0 の無条件優先ロジックにより、**この 24/24 レース (100%) で viewer が実際に表示するのは 269 の壊れた予測であり、07-11 に限り同時に存在する健全な champion default 行 (Cluster A, top1=42.86%) は 1 レースも表示されない**。ユーザー視点では、07-11・07-12 の函館全レースで「自信ありげだが実質ランダムな」予測が表示され続けていたことになる。

### 7.4 2026-07-12 backfill/rescore 発生の記録 (時刻・件数のみ、深追いなし)

既報 (§2 Defect A) の通り、2026-07-12 05:51:45〜05:52:32 UTC (14:51-14:52 JST) の約 47 秒間に、対象 3 場 (函館/福島/小倉) 72 レース分の予測行が生成された。同時間帯に 07-11 分 (22+14=36 行グループ) と 07-12 分 (36 行グループ) の両方が書き込まれている。根本原因・生成元スクリプトの特定は serve-defect-269 の担当範囲のため、本監査ではこれ以上追跡しない。

## 8. 週末監視手順 (serve_health_check.py, 2026-07-17 追記)

本監査 (§1-§7) が手動/ad-hoc SQL で発見した Cluster A/B 劣化パターンを毎開催日で自動検知できる read-only 診断ツール `apps/pc-keiba-viewer/src/scripts/serve_health_check.py` (183 tests、カバレッジ99%) を追加した。以下はその運用手順 (runbook) である。

### 8.1 実行コマンド

`cd apps/pc-keiba-viewer && uv run python src/scripts/serve_health_check.py --date YYYYMMDD --category jra [--json]`。`--date` 省略時は当日 JST (`resolve_date_arg()`)。`--category` 省略時は `jra` — この tool の各種閾値 (quality の stddev<0.3、burst の>10件/分) は JRA incident データで較正されており、他カテゴリ値も実行自体は受け付けるが閾値の転用は未検証。`--json` を付けると人間可読テキストの代わりに JSON を stdout に出力する。次回開催日 (2026-07-18 土、函館/福島/小倉) 以降、毎開催日の監視に本コマンドを使うことを想定する。

### 8.2 5チェック項目の要約

| #   | チェック                         | 判定対象                                                                                                                                                                                                                                                                     | 異常判定基準                                                                                                                                       |
| --- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Coverage                         | `jvd_ra` (placeholder-safe 述語 `trim(shusso_tosu) NOT IN ('', '00')` で確定レースのみ抽出 — 本監査 §「手法」と同一) と `race_finish_position_model_predictions` の突合。発走時刻 (`hasso_jikoku`) が現在時刻を過ぎた確定レースのみを対象とする post-time-passed filter 付き | model_version 問わず予測行が 1 件も存在しないレースがあれば異常                                                                                    |
| 2   | Quality (Cluster-B signature)    | `predicted_score` を (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, model_version) で group化 — race 単独ではない (§2 Defect A/B の通り、同一レースが健全な model_version 行集合と劣化した model_version 行集合を同時に持ちうるため) — した within-group 標準偏差    | stddev < 0.3 (劣化域 0.05〜0.15 と健全域 0.72〜1.57 の中間に双方へ余裕を持たせた閾値) の group を DEGRADED として列挙                              |
| 3   | Routing parity                   | `cell_routing.json` の first-match-wins ルールを Python で再実装し、確定レースごとに期待 model_version 行の有無を確認                                                                                                                                                        | 期待 model_version 行が欠落していれば mismatch。対象カテゴリに cell_routing ルール定義が無い場合 (現状 jra / ban-ei のみ定義、nar は未定義) は N/A |
| 4   | Burst detection                  | `prediction_generated_at` を `date_trunc('minute', ...)` で分単位 group化                                                                                                                                                                                                    | 同一分内の書き込みレース数が 10 件を超える (11 件以上の) bucket を検出 (実測例は §8.4 参照)                                                        |
| 5   | D1 self-heal件数 (informational) | `bunx wrangler d1 execute finish-position-cron-db --remote` で `finish_position_coverage_gap_events` の該当日件数を取得                                                                                                                                                      | 情報提供のみ、exit code に一切影響しない。wrangler 不使用/失敗時は `N/A (wrangler unavailable)`                                                    |

### 8.3 健全/異常の判定基準 (exit code)

- **0**: checks 1-4 が全てクリーン (異常ゼロ)。
- **1**: checks 1-4 のいずれかが異常を検知 (check 5/D1 は informational のため対象外)。
- **2**: ツール自体の失敗 (Neon 接続エラー、`NEON_PRIMARY_URL` 未設定、`cell_routing.json` 読み込み失敗、または `main()` の outer catch-all が捕捉するその他の未処理例外)。exit code 1 は「本物の品質異常を検知した」場合専用に予約されており、コードのバグ等によって exit code 1 が偶発的に出力されることはない設計 — その場合は exit code 2 になる。

### 8.4 受け入れテスト実証結果

本ツールを、本ドキュメント §1-§7 が手動/ad-hoc SQL で解明した 2 つの実インシデント日に対して実行し、既知のパターンを自動検知できることを確認した。

2026-07-12 (jra、§2 Defect A の Cluster B 一律劣化) を対象に `--date 20260712 --category jra` で実行した結果:

| チェック           | 結果                                                                                                       |
| ------------------ | ---------------------------------------------------------------------------------------------------------- |
| [2] Quality        | 36/36 (race, model_version) group が全て DEGRADED、stddev 範囲 0.048〜0.160                                |
| [3] Routing parity | 0 mismatch (ルーティング自体の _存在_ はこの日正常 — 壊れていたのは _品質_ のみ、§2 Defect A の実測と整合) |
| [4] Burst          | 1 分 flagged: 2026-07-12 05:52 UTC (14:52 JST)、36 races                                                   |
| [5] D1 count       | 6                                                                                                          |
| **exit code**      | **1**                                                                                                      |

2026-07-11 (jra、§2 Defect A/B の Cluster A/B 混在 — 本ツールの (race, model_version) grouping 設計が意図通り機能する最重要の検証ケース) を対象に `--date 20260711 --category jra` で実行した結果:

| チェック                      | 結果                                                                                                                                                                                                                                                                                                                                                                               |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [2] Quality (per-race rollup) | `fully_healthy=0 partially_degraded=21 fully_degraded=15`。21 レースは `model_version=jra-cb-v9-sim-2013-clean` の健全な group (stddev 0.694〜1.507、§2 Cluster A 実測 0.72〜1.57 と整合) と、同一レース上の劣化した routed-variant/backfill group を同時に持つ。この 21 という件数は §2 Defect B が独立に報告する Cluster A 対象レース数 (函館12/福島4/小倉5=21) と完全に一致する |
| [4] Burst                     | 3 分 flagged: 01:47 UTC/21 races (Cluster A、健全バッチ)、05:51 UTC/22 races + 05:52 UTC/14 races (Cluster B backfill、22+14=36 は §6 記載の「22+14件」と一致)                                                                                                                                                                                                                     |
| [5] D1 count                  | 0 (self-heal cron は §2 Defect C の通り 2026-07-12 04:26 JST 配線であり、07-11 時点ではまだ存在しない)                                                                                                                                                                                                                                                                             |
| **exit code**                 | **1**                                                                                                                                                                                                                                                                                                                                                                              |

[1] Coverage と (07-11分の) [3] Routing parity の個別出力値はこの受け入れテストの記録には含まれていない。ただし両日とも完全欠落レースが存在しないことは本ドキュメント §1 の台帳 (07-11 / 07-12 とも「真の coverage」36†) で既に確認済みであり、両日の exit code 1 は checks [2] (quality) と [4] (burst) の実測異常のみで十分に説明される。

両日とも、本ドキュメントが独立に (手動/ad-hoc SQL で) 解明した内容を本ツールが正確に再現した。特に 07-11 の MIXED 結果は (race, model_version) 粒度での group化設計が意図通りに機能する証拠として最も重要である — この粒度がなければ、同一レース内の健全な行集合と劣化した行集合の `predicted_score` が単一の stddev 計算に混在し、`fully_healthy=0` という明確なシグナルは失われていたはずである (race 単独 group化との比較実行は本ツールでは行っていない — これは grouping 設計の論理的帰結であり、実測比較ではない)。

### 8.5 異常検知時の対処順

1. **`feature_guard.py` のログ確認**: `apps/finish-position-predict-container/src/predict_lib/feature_guard.py` (commit `57a4cd7f` で deploy 済み、単体 17 tests / カバレッジ100%。同 commit を含む `finish-position-predict-container` package 全体では 1288 tests / cov 99.81%) が該当レースを reject しているか確認する。特徴量欠損/劣化率がレース平均 50% 以上の場合に書き込み自体を拒否する fail-closed 恒久対策であり、正しく機能していれば Cluster B 型の劣化書き込みはそもそも Neon に到達しない。ログ上で reject されているか、あるいはこの guard 自体が呼ばれていない別経路 (§2 Defect A が特定できなかった書込元) なのかを切り分ける。

2. **該当レースの focused-full 再トリガー**: admin API `POST /api/admin/run-focused-full-race` (`Authorization: Bearer $FINISH_POSITION_CRON_TRIGGER_TOKEN`) を使う。**Cloudflare WAF が非ブラウザ User-Agent を error 1010 で 403 拒否するため、curl 実行時はブラウザ相当の User-Agent 明示が必須** (既存メモリ `project_cf_only_serving_2026_07_11` / `reference_realtime_odds_weight_architecture` 参照)。単一 slot lock の設計により、既に処理中/完了済みレースへの再トリガーは安全な no-op になる。

   **再トリガー前に必ず確認すること (既知の罠)**: `focused-full-completion.ts::expectedModelVersion()` が使う「既に complete」判定は、期待 model_version の **行数が揃っているかのみ** を見ており、`predicted_score` の分散などの **値の健全性は一切見ない**。この罠は実際に発生済みであり、並行する専任診断 (`jra-269-serve-defect-2026-07-17.md` §7.1) の本番 admin trigger smoke test で、劣化した 269 行が既に存在する対象レース (2026-07-12 venue02 R01) への再トリガーが行数一致のみを理由に `status: "already-complete"` で実行されずスキップされたことが確認されている。本ドキュメント §8.4 の実証結果自体 (2026-07-12 の 36 レースは行数としては期待頭数分揃っているが quality check では全て DEGRADED) がまさにこの罠の的中条件を示している。したがって再トリガーの前には、対象レースが本当に未生成かを **count だけでなく本ツールの quality check (check 2) でも確認** し、count は揃っているが quality が悪いレースについては、素朴な再トリガーが `already-complete` にブロックされて効果を持たない可能性を踏まえて対処する (§2 Defect C も参照)。

3. **それでも解消しない場合のロールバック**: CF-only serving パイプライン自体を疑う場合は `docs/finish-position-prediction-system.md` §1.2 の Mac batch fallback 緊急ロールバックを検討する — 無効化済みの launchd plist は 1 コマンドで復元できる (`cp ~/Library/LaunchAgents.disabled-20260711/com.kkk4oru.finish-position-predict.plist ~/Library/LaunchAgents/ && launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.kkk4oru.finish-position-predict.plist`、詳細は既存 project memory `project_cf_only_serving_2026_07_11` に記載)。あるいは直近の `finish-position-cron` / `finish-position-predict-container` deploy commit への revert を検討する。いずれもデータ削除・UPDATE を伴わない (pointer/flag の revert のみ)。

### 8.6 既知の限界

- **Routing parity (check 3)** は「期待 model_version の行が存在するか」の二値判定のみを行う。複数の model_version が同一レースに競合して存在する場合に viewer がどちらを表示するか (`finish-position-cell-routing.ts` の priority-0 ロジック、§7.3 参照) までは検証しない。したがって check 3 が OK でも viewer が実際に表示している行が健全とは限らない — 本ドキュメント §2 Defect B (健全な Cluster A 行が priority-0 の壊れた行集合に恒久的にシャドーイングされる事象) は check 3 単独では検出できない。
- **D1 self-heal件数 (check 5)** は informational のみで exit code に一切影響しない。self-heal 発火は genuine な自己修復 (本当に未生成だったレースの検出) と、§2 Defect C の false-positive re-trigger (行数は揃っているが `expectedModelVersion()` のルール欠落により complete と判定できず再エンキューされる) の両方でカウントが増えうるため、単独の pass/fail 指標として扱えない。

---

## 9. Cell×rank1-5 精度標準への準拠 + 06-06/06-07 旧モデル世代データの発見 (2026-07-17 追記)

### 9.1 新基準 (team-lead 経由、USER)

> 評価は常に cell 単位で、着順 1,2,3,4,5 の個別精度で行う。要約した精度で評価しない。

以降、精度に関するあらゆる主張は **cell (本節では venue = `keibajo_code`) 単位**で、**top1 / place2 / place3 / place4 / place5 の 5 個の個別数値**を primary 根拠とする。`fukusho_2p` / `top3_box` のような複数着順を 1 つの hit フラグに畳み込む集約指標 (本ドキュメント §1-§8 が使ってきたもの、および pooled/全体平均の数値全般) は reference のみとし、単独では主張の根拠にしない。本節は今日実施した JRA summer serve-accuracy 再計測タスクをこの基準に合わせて re-work したものであり、あわせて本節作成中に発見した独立のデータ来歴問題 (06-06/06-07 の旧モデル世代混入) を訂正する。

### 9.2 実装: `rank1_5_by_venue.py`

`serve_accuracy_report.py --json` の出力は `top1_pct / place2_pct / place3_pct / fukusho_2p_pct / top3_box_pct` のみで **place4 / place5 を一切計算しない** (実際の JSON 出力を読んで確認済み)。team-lead 指示 (「script が cell 粒度未対応なら、既存出力に venue×rank 分解を追加する後処理でも可 — script 本体の大改修までは不要」) に従い、`serve_accuracy_report.py` 自体は**一切変更せず**、新規スタンドアロンスクリプト `apps/pc-keiba-viewer/tmp/ms-summer-serve/rank1_5_by_venue.py` (gitignored scratch、未 commit) を追加した。

- **再利用 (import、再実装せず)**: `serve_accuracy_report.parse_post_time_jst` / `select_serving_row` / `dedup_prediction_rows_per_horse` / `FpRow` / `query_finish_position_metrics`。生行 fetch の SQL 文自体は `query_finish_position_metrics` から byte-for-byte 複製した (JOIN 構造のみで分岐/順序ロジックを含まないため、dedup ロジックの二重実装リスクとは性質が異なると判断)。
- **place4_pct / place5_pct の定義**: この repo 既存の正式な convention (`apps/mlflow/src/mlflow_tracking/serve_eval.py::_compute_race_hits` / `compute_rank_pct`、2026-07-17 直接読解で確認済み) を踏襲。top1/place2/place3 は predicted 1 位馬の実着順が該当順位以下かを**無条件**判定 (`serve_accuracy_report.py` 自身の top1_hits/place2_hits/place3_hits と同一)。place4/place5 は同じ判定に加え、**その競走の頭数 (`shusso_tosu`) がその順位未満なら判定を None (非該当、ミス扱いにしない) として分母から除外** — 小頭数競走で機械的に「4着以内」を満たしてしまい place4_pct を水増しする不具合を防ぐガード。JRA 実データでは頭数がほぼ常に 5 頭を超えるため、今回の対象母集団では実際に除外が発生したケースは 0 件だった (全 cell で `place4_eligible_races == race_count`)。
- **正しさの検証**: 0711/0712 それぞれについて、本スクリプトの venue 別 top1/place2/place3 を `serve_accuracy_report.query_finish_position_metrics` 自身がテスト済みの `subgroups` (dimension="venue") 出力と突合 — **不一致 0 件**。dedup ロジックの再利用が正しく機能していることを独立に確認した。

### 9.3 現行チャンピオン (0711 + 0712) venue×rank1-5 — PRIMARY

対象: JRA venue 02 (函館) / 03 (福島) / 10 (小倉)、2026-07-11 + 2026-07-12 (36 レース/日 × 2 日、既知の genuine 現行チャンピオン full coverage 日)。`dedup_prediction_rows_per_horse` により horse 単位で 1 行に確定した「実際に serve された予測」に基づく。

| venue                            | race_count |       top1 |     place2 |     place3 |      place4 (elig) |      place5 (elig) |
| -------------------------------- | ---------: | ---------: | ---------: | ---------: | -----------------: | -----------------: |
| 02 函館                          |         24 |     20.83% |     29.17% |     37.50% |     50.00% (24/24) |     54.17% (24/24) |
| 03 福島                          |         24 |      8.33% |     20.83% |     29.17% |     45.83% (24/24) |     50.00% (24/24) |
| 10 小倉                          |         24 |     12.50% |     20.83% |     37.50% |     54.17% (24/24) |     62.50% (24/24) |
| **ALL (pooled, reference-only)** |     **72** | **13.89%** | **23.61%** | **34.72%** | **50.00% (72/72)** | **55.56% (72/72)** |

**この 3 行 (02/03/10) が primary の根拠であり、ALL 行は reference のみ**。全 venue で市場ベースライン (§2 実測 32.43%) および `serve_accuracy_report.py` docstring 記載の健全 baseline (FULL top1=44.71%) を大きく下回っている — これは §2 Defect A (2026-07-12 Cluster B の劣化書込が cell-routing priority-0 により最優先表示される) が cell 単位でも一貫して効いていることを裏付ける。venue 間でも一様ではない: 函館 (top1=20.83%) は §2 Defect B の Cluster A 健全予測 (函館 12 レース全て) が pooled 対象の半分を占めるため相対的に高く、福島 (top1=8.33%) は Cluster A 該当が 4 レースのみで大半が Cluster B 由来のため最も低い — pooled 平均 (13.89%) だけを見ていては venue 間のこの差が完全に隠れる、まさに新基準が防ごうとしている blind spot の実例。

### 9.4 06-06 / 06-07 旧モデル世代データの発見 — 訂正: 「genuine subtotal」への pooling は誤りだった

**本日の先行タスク (Task 40 — 本ドキュメントとは別の predecessor report、本ドキュメントには未反映) は 2026-06-06 を 07-11/07-12 と並ぶ「genuine full coverage 3 日」の 1 つとして扱い、「genuine subtotal (0606+0711+0712)」として pooled した。これは誤りであり、本節で明示的に訂正する。**

`serve_health_check.py --date 20260606/20260607 --category jra` を実行し、独立に Neon への直接 SQL 照会 (`race_finish_position_model_predictions` の raw row count、JOIN を経由しない) で二重確認した結果:

- **2026-06-06**: `race_finish_position_model_predictions` に **332 行** (source='jra')、venue は **05 (東京) / 09 (阪神) の 2 場のみ** — 函館/福島/小倉 (02/03/10) では **0 行**。model_version は下記 5 種全て `iter14`/`iter25`/`iter26` 系統であり、**現行チャンピオン (`jra-cb-v9-sim-2013-clean` およびその routed variants `jockey-pedigree269`/`prior-corner274`) は 1 行も存在しない**:
  - `iter14-jra-cb-pacestyle-course-v8`
  - `iter25-jra-cb-ensemble-010-v8`
  - `iter26-jra-cb-ensemble-005-v8`
  - `iter26-jra-cb-ensemble-016-v8`
  - `iter26-jra-cb-ensemble-703-v8`

  現行 `cell_routing.json` に対する routing parity は 24/24 (全確定レース) mismatch。加えて `serve_health_check.py` の check 2 (quality) で 10 件の (race, model_version) group が stddev 0.096〜0.300 (健全閾値 0.3 未満、§2 Cluster-B シグネチャと同型) を示し、check 4 (burst) で 2026-06-06 05:27 JST に 24 レースの write-burst を検出した。**旧モデル世代であることに加え、その世代自身の出力内でも追加の品質劣化が見られる**。

  venue×rank1-5 (05 東京 / 09 阪神、reference のみ — 現行チャンピオンとは比較不能):

  | venue        | race_count |  top1 | place2 | place3 |  place4 (elig) |  place5 (elig) |
  | ------------ | ---------: | ----: | -----: | -----: | -------------: | -------------: |
  | 05 東京      |         12 | 0.00% | 16.67% | 25.00% | 50.00% (12/12) | 50.00% (12/12) |
  | 09 阪神      |         12 | 0.00% |  0.00% | 16.67% | 16.67% (12/12) | 33.33% (12/12) |
  | ALL (pooled) |         24 | 0.00% |  8.33% | 20.83% | 33.33% (24/24) | 41.67% (24/24) |

- **2026-06-07**: **新たな独立検証 (本節作成中に発見、team-lead の元指示の記述を訂正)**。元指示は「24/24 routing mismatch、quality degradation ゼロ、burst flag ゼロ ⇒ 同じ旧モデル世代からの NORMAL, HEALTHY serving」としていたが、`race_finish_position_model_predictions` を直接 `COUNT(*)` で照会した結果 **0606 とは異なり raw row が source='jra' で 0 件** (model_version 問わず) であることを確認した (venue も 0、model_version も 0)。念のため 2026-06-01〜06-15 の全 `kaisai_tsukihi` を走査したが、0606 (332 行) と既知の 0614 (n=1 backfill、11 行) 以外に該当日近傍で行が分散して存在する形跡もなかった。

  **訂正**: 06-07 の「24/24 mismatch・quality degradation ゼロ・burst flag ゼロ」というシグネチャは、**予測が 1 行も存在しない (旧モデルであれ現行であれ) ことの vacuous な (空集合ゆえの自明な) 現れ**であり、「健全な旧モデル世代 serving」の証拠ではない。予測行が 0 件であれば、quality check (グループ自体が存在しない) と burst check (書込自体が存在しない) は機械的に「異常ゼロ」を返し、routing check は「期待 model_version が一致しない」を全件で返す — これは §9.5 で述べる check 3 の限界とも合わせて、**old_model_era というより既存の "no_data" 系日 (0613/0620/0628、§9.6 参照) と構造的に同一**である。06-07 は本節の old-model-era 比較データセットから実質的に空 (race_count=0) として扱う。

**実務上の帰結**: 「現行チャンピオンの summer 期間 serve accuracy」を主張する際、**06-06 と 06-07 はいずれも "genuine current-system" framing から除外する**。06-06 は現行チャンピオンとは異なる旧モデル世代の出力であり (かつ venue も函館/福島/小倉ではなく東京/阪神)、06-07 は単なるカバレッジ欠落 (予測 0 件) であって旧モデルの健全 serving ですらない。どちらも 07-11/07-12 の "genuine subtotal" に pooling してはならない — 元の Task 40 の framing はこの 2 点いずれについても誤りだった。

### 9.5 `serve_health_check.py` routing-parity check の既知の限界 (§8.6 に追加)

§8.6 は check 3 (routing parity) の「行の有無のみ判定し、複数行競合時の viewer 表示優先度までは見ない」という限界を既に記載しているが、本節の発見によりもう 1 つの限界を追記する:

- **Check 3 はカレントの `cell_routing.json` (commit 済みの現行版) しか知らず、バージョン履歴を持たない**。したがって過去の日付に対する routing "mismatch" は、「その日実際に live だったモデル世代に現行のルーティング規則を遡って適用できない (構造的に適用しようがない)」ことを意味するに過ぎず、**古い日付であればあるほど mismatch は期待通り/無害**であり、それ自体は defect の証拠にならない。06-06/06-07 の 24/24 mismatch はまさにこのケース。
- これは check 2 (quality stddev) / check 4 (burst) とは性質が全く異なる点が重要: **quality/burst の異常検知は「その値自体」が異常性の証拠**になる (06-06 の stddev 0.096〜0.300 や 05:27 JST の burst は、旧モデル世代であることとは独立に、それ自体が品質問題を示す)。一方 **routing mismatch は age (古さ) の副作用として自明に発生しうる non-signal** であり、単独では「何か問題がある」ことを意味しない。この 2 種類のシグナルを混同すると、06-06 (旧モデル + 追加劣化の両方) と 06-07 (単なる欠落) のような性質の異なる 2 日を誤って同一カテゴリに分類してしまう — 実際に元の task brief がこの混同を犯していた (§9.4 訂正参照)。

### 9.6 0613 / 0620 / 0628 "no_data" の再確認 (スポットチェック)

team-lead 指示に基づき、`serve_accuracy_report.py` が "no_data" と判定するこの 3 日について、JOIN を経由しない直接 `COUNT(*)` (`race_finish_position_model_predictions`, source='jra', model_version 問わず) でスポットチェックした。結果: **3 日とも raw row 0 件** (distinct venue = 0, distinct model_version = 0)。06-07 (§9.4) のような「隠れた旧モデル行が JOIN で silently 除外されている」パターンはこの 3 日には**見つからなかった** — `serve_accuracy_report.py` の "no_data" 判定は 3 日とも文字通り正しい (予測テーブルに本当に何も無い)。既知メモリ「FP serving 6週間 blackout (5/25-7/7)」と整合する。

### 9.7 MLflow 記録

`apps/mlflow/src/mlflow_tracking/ingest_eval.ingest_cell_report` (Path B、`experiment=finish-position/wf-eval`、`eval_regime=serve`) で 2 本の run を記録した。**1 本にまとめなかった理由**: `logging_api.select_headline_metrics` は渡された cell 表の全行を `race_count` 加重平均して `overall_*` headline metric を自動計算するが、"era" という概念を持たない。current_champion と old_model_era の disjoint venue 行を同じ表に混在させると、この自動集計が 2 つの異なる (比較不能な) serving 世代を 1 つの `overall_top1_pct` に blend してしまい、本節が是正しようとしている pooling の誤りを MLflow 側で再生産することになる。そのため era ごとに別 run・別 cell 表とした。

| run                                                | run_id                             | tags                                                                                                                                 | 主な metrics                                                                               |
| -------------------------------------------------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `jra-current-champion-rank1-5-by-venue-2026-07-17` | `cd092e57fbd24d3c898646475db42c2d` | `campaign=2026-07-17-cell-rank-eval-standard`, `era=current_champion`, `dates=20260711,20260712`                                     | `overall_top1_pct=13.89` (`pooled_top1_pct` と一致、独立集計クロスチェック) 他 place2-5    |
| `jra-old-model-era-rank1-5-by-venue-2026-07-17`    | `ceb367bcf12e4720b9b5b91a1cd4f5eb` | `campaign=2026-07-17-cell-rank-eval-standard`, `era=old_model_era`, `dates=20260606,20260607`, `coverage_correction=(§9.4 の訂正文)` | `overall_top1_pct=0.00`、`pooled_0607_race_count=0.00` (§9.4 の訂正を metric としても明示) |

いずれも `run_key` によるべき等ログ (再実行しても新規 run を作らず既存を再利用)。cell 表は `cell_metrics.json` / `cell_metrics.parquet` として artifact に添付。両 run とも書込プロセスとは別プロセスの fresh `MlflowClient` (`config.load_dotenv_local()` → `config.load_repo_root_env_fallback()`、scheme=`postgresql` 確認済み、生 URI は一切出力していない) で read-back し、tags/metrics/artifacts が byte-for-byte 一致することを確認した。`overall_place4_pct`/`overall_place5_pct` は `select_headline_metrics` の自動集計 (cell 表由来) と、本節手動計算の `pooled_place4_pct`/`pooled_place5_pct` の両方が独立に一致しており、§9.2 のクロスチェックに続く 2 段目の正しさの根拠になっている。

---

## 10. 記録訂正 + 0606/0614/0621 分類 (2026-07-17 追記)

本節は 2026-07-17 に行った 3 件の追加確認をまとめる: (a) commit `1d7e3215` 自身の検証記述の訂正、(b) `serve_health_check.py` による 2026-06-06 / 06-14 / 06-21 の分類、(c) MLflow timeline ツールの skip-if-present 挙動に関する注意喚起。いずれも独立したクロスチェックを経て確認済み。本追記はドキュメント記述のみを対象とし、コード変更・MLflow への書込は一切行っていない。

### 10.1 commit `1d7e3215` の検証記述の訂正 — 引用レースは genuine/garbage いずれの選択でも miss

commit `1d7e3215` (`fix(pc-keiba-viewer): dedup serve-accuracy predictions by post-time, not latest write`) 自身のコミットメッセージは、修正の検証根拠として次のように主張していた:

> Verified the fix against the concrete incident: jra:2026:0711:02:01 (genuine row 10:47 JST predicted the actual winner correctly, Cluster B garbage row generated the next day predicted 5th) is now a correct top1 hit instead of being silently swapped for the garbage row.

**この主張は直接の再検証では再現しない。** 本日、独立した 2 つの経路 (先行 sub-agent による調査、および `jvd_se` / `race_finish_position_model_predictions` に対する筆者自身の直接 SQL 照会) で確認し、両者は完全に一致した:

| 行                                                                                                       | horse_id     | predicted_rank (該当馬) | rank1 pick   | rank1 pick の実際の着順 |
| -------------------------------------------------------------------------------------------------------- | ------------ | ----------------------- | ------------ | ----------------------- |
| 実際の優勝馬 (`kakutei_chakujun='01'`)                                                                   | `2024101292` | —                       | —            | 1着 (実際)              |
| genuine row (`jra-cb-v9-sim-2013-clean`、2026-07-11 01:47:52 UTC = 10:47:52 JST 生成)                    | `2024101292` | **2**                   | `2024108137` | 2着                     |
| garbage row (`jra-cb-v9-sim-2013-clean-jockey-pedigree269`、2026-07-12 05:51:45 UTC = 14:51:45 JST 生成) | `2024101292` | **3**                   | `2024108137` | 2着                     |

genuine row・garbage row とも優勝馬 (`2024101292`) を rank1 には選んでおらず (それぞれ 2 位・3 位予測)、さらに **両行の rank1 pick は同一馬 (`2024108137`) であり、この馬の実際の着順は 2着** — commit メッセージが主張する「garbage row は5着馬を rank1 予測した」という記述とも食い違う。**このレースは genuine row を採用しても garbage row を採用しても top1 は miss であり、commit が主張する「garbage row にすり替えられていた hit」は存在しない。**

**これは dedup fix 自体の正しさを損なうものではない。** 「検証根拠として挙げた具体例」と「fix が実際に行っている仕事」は別の話であり、明確に区別する必要がある。fix の実際の仕事 (同一馬に対して複数 `model_version` 行が競合するとき、レースの post time より後に書かれた行より、post time 以前に生成された行を優先する) は、この 1 レースの誤った例とは独立に、少なくとも次の 2 つの経路で頑健に検証済みである:

1. **`jra:2026:0711:02:01` に出走した全 10 頭について**、genuine row が garbage row より正しく優先選択されていることを確認した — dedup の選択機構自体は全頭で正しく機能していた。優勝馬に対する genuine row の予測が rank1 でなかったのは、その日のモデル精度についての事実であって、dedup がどちらの行を選んだかとは別の論点である。
2. **day-level の集計効果は大きく、方向も正しい**: 2026-07-11 の pooled top1 は naive dedup (`ORDER BY prediction_generated_at DESC`) の 13.89% から、fixed dedup では 25.00% に改善した。これは重複/garbage 行 285 件 (本ドキュメント §7.1 記載の「1412 組中 285 組が複数 model_version 競合」と同一の競合集合) を正しく除外したことによる、機構的に予期される通りの実改善である。

fix 自体は健全であり、訂正が必要なのは commit メッセージが挙げた「具体例による検証」の記述だけである。時間的制約下でコミットメッセージを書く際の transcription/lookup ミスである可能性が高い。今後この特定レースを「top1 が反転した証拠」として再引用しないよう、ここに記録しておく。

### 10.2 0606/0614/0621 分類 (`serve_health_check.py` 実行結果)

- **2026-06-06 — Cluster B とは別由来の、新規に特定された劣化/異常 serving 日**: 本ドキュメント §9.4 が既に報告した通り、この日の 24 レース全ては旧世代 model_version (`iter14-jra-cb-pacestyle-course-v8` / `iter25-jra-cb-ensemble-010-v8` / `iter26-jra-cb-ensemble-703-v8` / `iter26-jra-cb-ensemble-005-v8` およびその近縁) で serve されており、対象 venue は 05 (東京) / 09 (阪神) のみ、現行チャンピオン系統 (`jra-cb-v9-sim-2013-clean` / `jockey-pedigree269` / `prior-corner274`) は 1 行も含まれない — 現行チャンピオンの serve accuracy とは比較不能である点は §9.4 で確認済み。

  **本追記での新規知見**: `serve_health_check.py` の quality check を本日改めて実行し、該当する (race, model_version) group のうち **10 件が DEGRADED** (`predicted_score` の group内標準偏差 0.096〜0.300、健全閾値 0.3 未満) であることを、具体例つきで確認した — 例: `jra:2026:0606:05:03` (`iter26-jra-cb-ensemble-703-v8`、stddev=0.298)、`jra:2026:0606:09:05` (`iter14-jra-cb-pacestyle-course-v8`、stddev=0.096)。per-race rollup は `fully_healthy=14 partially_degraded=0 fully_degraded=10`。

  burst detection も本日改めて確認した: 24 レース全てが 2026-06-05 20:27 UTC (= 2026-06-06 05:27 JST) の同一 1 分間に書き込まれている。この単一分バッチ書込という形状は Cluster B (2026-07-12 05:51-05:52 UTC = 14:51-14:52 JST) のシグネチャと構造的に類似するが、**モデル世代が異なり (旧世代 vs 現行チャンピオン系統)、時期も約 5 週間離れている — Cluster B と同一のインシデントではない**。本日のツール実行で新たに発見された、Cluster B とは別に provenance を持つ独立した第二の劣化バッチ書込インシデントとして、ここに明示的に記録する (Cluster B の焼き直しとして扱わない)。

- **2026-06-14 / 2026-06-21 — 健全、新規インシデントではない**: 両日とも `serve_health_check.py --date 20260614/20260621 --category jra` を実行し確認した。全国 JRA 36 レース中 **35 レースが予測行ゼロ** (品質劣化ではなく完全な coverage gap — 既知の FP serving blackout window、本ドキュメント §1 の台帳と整合) で、残り **1 レースのみ genuine backfill 予測が存在**し、その 1 レースの quality は両日とも **健全** (`fully_healthy=1 partially_degraded=0 fully_degraded=0`、degraded group は両日ゼロ件)。

  ※ この「36 レース」は全国 JRA 集計であり、本ドキュメント §1 の対象範囲 (venue 02/03/10 限定、この 2 日とも函館のみが開催中) とはスコープが異なる。06-14 の全国 1 genuine レースは §1 の函館分 1\* (12 レース中 1 件) と同一のレースであることを確認した。一方 06-21 は §1 の函館分が 0 件 (12 レース中 0、完全欠落 12) であるため、全国 1 genuine レースは対象 3 場 (函館/福島/小倉) のいずれでもなく、スコープ外の JRA 他 venue のレースである。

  `serve_health_check.py` が報告する 35/36 の "routing parity mismatch" は、本ドキュメント §9.4/§9.5 が 2026-06-07 について既に説明した vacuous artifact と同一である — 予測行がゼロのレースは期待されるどの model_version とも自明に「不一致」になるため、これ自体は何の証拠にもならず、意味を持つのは genuine coverage gap の件数のみである。**この 2 日について、インシデントカタログへの追加は不要** — 既存の blackout 特性づけを裏付けるのみで、新規の発見はない。

### 10.3 MLflow timeline の skip-if-present 挙動 — 事後訂正が自動反映されない

`apps/mlflow/src/mlflow_tracking/timeline.py::upsert_timeline_point` は、timeline point の重複判定を**値ではなく step (対象日) の存在有無のみ**で行う。該当 step に 1 点でも既にログ済みであれば、新しい値がバグ修正後の正しい値であっても**その書込は無条件でスキップされる** (同関数の docstring より: "Each metric key is deduped by STEP, not by value: ... a point at the same step is skipped entirely (even if the value differs -- re-ingesting the same date is assumed to reproduce the same number; this is a presence check, not a reconciliation)")。

この結果、**バグ修正前に記録された stale な point は、修正後に同じ ingestion を再実行するだけでは訂正されない** — stale な値はチャート上に残り続け、対象 run の対象 step を明示的に狙った別手段 (削除/上書き) で個別に対処するまで残存する。本日の JRA summer serve-accuracy 再計測作業 (本ドキュメント §9) の過程で、同関数の docstring 読解と、実際の再 ingestion 前後の値を突き合わせる経験的確認の両方から独立に確認した。

---

**この commit は本ドキュメント (§8・§9 に加えて本 §10 を追加) のみを対象とする。§8 追加時点の commit には `serve_health_check.py`／そのテスト／`pyproject.toml` の coverage 設定も含まれていた (当時の変更内容、本 commit には含まれない)。§9 追加にあたり `apps/pc-keiba-viewer/tmp/ms-summer-serve/rank1_5_by_venue.py` を新規作成したが、`tmp/` 配下は `.gitignore` 対象の scratch であり本 commit には含まれない。§10 の追加はドキュメント記述 (commit `1d7e3215` の検証記述の訂正、0606/0614/0621 の分類、MLflow timeline 挙動の注意喚起) のみであり、コード変更・新規ファイル作成・MLflow への書込は一切行っていない。`serve_accuracy_report.py` / `serve_health_check.py` / `timeline.py` を含む既存スクリプトは本追記に際して一切変更していない。他の untracked/modified ファイルには一切触れていない。**
