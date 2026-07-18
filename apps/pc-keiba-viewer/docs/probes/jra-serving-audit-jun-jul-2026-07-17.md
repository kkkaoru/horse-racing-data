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

### Defect F (2026-07-18 発見・現在進行形): `force=true` は completion gate を 1 段しか bypass せず、`claimFocusedFullRace` の terminal-status gate で永久にブロックされる — Defect A の 72 レース修復が全面停止中

- **発見の経緯**: 07-18 早朝、day-base reuse smoke test (Hakodate R01→R02) の一環として `POST /run`（`category=jra runYmd=20260712 keibajoCode=02 raceBango=01 mode=full skipDedup=true force=true`）を実行したが、6.5 時間後も Neon 行は 2026-07-12T05:52:09Z の元の Cluster B 壊れた行のまま一切変化しなかった。`wrangler tail --format pretty` を張った状態で同一リクエストを `debug=true` 付きで再実行し、原因をログで直接特定した。
- **実測ログ (該当部分そのまま)**:
  ```
  [predict-queue] focused-completion-check bypassed (force) ... keibajo=02 race=01
  [predict-queue] focused-claim ... proceed=false state=success staleAfterMs=900000
  Focused full already in flight ... -- will re-check on redelivery
  ```
- **コード根拠**: `force=true` は `queue-consumer.ts::ackIfFocusedFullAlreadyComplete`（Neon 行数ベースの完了チェック）を正しく bypass する（`4d2f256b` の対象範囲はここまで、動作は正しい）。しかしその直後に呼ばれる `claimFocusedFullOrRetry` → `PredictRunCoordinator.claimFocusedFullRace`（`apps/finish-position-cron/src/predict-run-coordinator.ts:138-158`）には `force` パラメータ自体が一切渡っていない。同関数は対象レースの DO storage キー `focused-full:{runYmd}:{category}:{keibajoCode}:{raceBango}`（`buildFocusedFullRaceKey`、`predict-run-coordinator.ts:67-68` — 日付・カテゴリ・場・レース番号のみで構成、`model_version` も生成時刻も含まない）を読み、既存レコードの `status` が `TERMINAL_STATUSES`（`predict-run-coordinator.ts:70`、内容は `new Set(["success"])` のみ）に含まれる場合は無条件で `{ proceed: false, state: existing.status }` を返す（`predict-run-coordinator.ts:143-146`）。**この分岐に `force` を見る条件は無く、また一度 `status:"success"` に達した DO storage キーをリセットするコードパスはこのファイル内のどこにも存在しない。**
- **`status:"success"` に到達する経路は 2 つ**: (1) 本物の完走時に `completeFocusedFullRace({status:"success"})` が呼ばれる正規の完了、(2) `ackIfFocusedFullAlreadyComplete` 自身が「行数さえ揃っていれば」complete と判定した時にも同じ `completeFocusedFullRace({status:"success"})` を呼ぶ（`queue-consumer.ts:278-286`）— これは Defect A の壊れた行（頭数分の行はあるが score 品質はランダム）でも成立してしまう判定なので、**Defect A の 72 レースは実質 100% がこの DO キーを既に `success` で持っていると推定される**（本監査 §1 の「真の coverage 72 レース」がまさにこの判定基準で「coverage あり」と分類されていたこと自体が状況証拠）。
- **影響範囲**: `mode=full + skipDedup=true`（focused-full）経由の再トリガー全般が対象。`force=true` を付けても一度 `success` に達したレースは二度と再トリガーできない — 72 レース修復タスクは対象 24 レース (函館) に限らずこの 1 点で全面ブロックされている。
- **本番 live serving への影響: 低いと判断**（未着手・裏取り済み）: coordinator の近post rescore 経路 (`isPerRaceRescore` → `processPerRaceRescore`、`queue-consumer.ts:213-218,626`) は `ackIfFocusedFullAlreadyComplete` / `claimFocusedFullOrRetry` のどちらも通らない別コードパスであり、本 defect の影響を受けない。したがって本日 (07-18) 09:25 開催後の `COORDINATOR_ENABLED=1` rescore 発火自体は無関係。
- **対処状況**: **未修正**（コード変更なし）。開催 2 時間前のタイミングでの coverage-protected package への急ぎ修正は USER 判断でリスク超過と判断し見送り。修正案 (要 `predict-run-coordinator.test.ts` でのテスト追加、両方とも要検討):
  1. `claimFocusedFullRace` のシグネチャに `force?: boolean` を足し、`force===true` のときは `TERMINAL_STATUSES` チェックを丸ごとスキップして無条件で新規 claim を発行する。
  2. または `force===true` のときに既存の terminal レコードを明示的に delete/overwrite してから通常の claim ロジックに入る（DO storage の実質的な「reset」）。
     いずれの場合も、(a) 「stale だが terminal ではない」既存の 15分 staleness ガード (`FOCUSED_FULL_IN_FLIGHT_STALE_MS`) の挙動を壊さないこと、(b) force で reset した後の claim が本当に新しいパイプライン起動に繋がることを実際の redelivery サイクルで確認する統合テストを追加すること、の 2 点が受け入れ条件。17:00 JST 以降の落ち着いた窓で ⑬⑭⑮ deploy と合わせて実装・テスト・deploy する第 3 bundle 項目として扱う。72 レース修復の再開はこの fix の deploy 後。

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

## 11. Viewer 表示側 dedup 意味論監査 — 結論: バグなし (2026-07-17 追記)

本節は、team-lead 経由で共有された USER 指示のバグ調査をまとめる: `apps/pc-keiba-viewer/src/db/queries.ts` の `getFinishPositionLambdarankPredictions` (2919行) に、`serve_accuracy_report.py` の analysis 側 dedup が本日の修正 (commit `1d7e3215`) 以前に抱えていた「post-race backfill 行を、より古い genuine な行より優先して選んでしまう」罠と同型のバグが存在するか、という監査依頼である。確認すべき点は3つ: (a) 同一 model_version 内での複数行選択にこの罠が存在するか、(b) 2026-07-11・07-12 の実データで裏付けられるか、(c) tier priority による選択ロジックと allowlist の網羅性。**結論を先に記す — バグではない。** ただし調査の途中で 1 件の誤った仮説を立て、独立した re-verification によってその場で訂正された経緯があり (§11.3)、これも省略・軽視せずそのまま記録する。本節はドキュメント記述のみを対象とし、コード変更・MLflow への書込は一切行っていない。

### 11.1 (a)(b): 同一 model_version 内の複数行選択は構造的に不可能、実データでも裏付け

**(a)**: `race_finish_position_model_predictions` の UPSERT 主キー (`apps/finish-position-predict-container/src/predict_lib/upsert_sql.py:23-31`) は `PRIMARY_KEY_COLUMNS = (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)` である。これは同一 (model_version, 馬, レース) の組に対して行が常に高々 1 行しか存在し得ないことを意味する。同じキーへの後続の書込は `build_upsert_sql` が組み立てる `on conflict (...) do update set ...` により既存行を in-place で上書きする (内容を書き換え、`prediction_generated_at = now()` で鮮度だけ更新する) のみであり、競合する 2 本目の行が新たに生まれることは構造的にあり得ない。したがって、**「同一 model_version 内で複数行のうちどれを選ぶか」という判断自体が viewer の read path のどこにも存在せず、監査すべき対象がそもそも無い** — これは `serve_accuracy_report.py` の元々のバグ (priority-tier 構造を一切持たず、`ORDER BY prediction_generated_at DESC` という生の並び替えだけで、**異なる** model_version の行を横断的に選んでいた) とは構造的に別物である。

**(b)**: 既知のインシデント対象日である 2026-07-11・2026-07-12 について、Neon primary への直接 read-only query で確認した。`race_finish_position_model_predictions` の該当期間 93 件の (date, venue, race, model_version) group 全てにおいて、行数と distinct 馬 (`ketto_toroku_bango`) 数の不一致は **0 件**、`prediction_generated_at` の min/max スプレッドが 1 時間を超える group も **0 件** — このインシデント期間に限って言えば、in-place 上書き以外のパターン (行の競合・重複) が発生した痕跡は一切見当たらなかった。

### 11.2 (c) クエリの tier 構造 (`getFinishPositionLambdarankPredictions` 読解)

(c) を検討するため、`apps/pc-keiba-viewer/src/db/queries.ts::getFinishPositionLambdarankPredictions` (2919-3082行) を直接読解して確認した。このクエリは `selected_model` という CTE で 4 段の priority を `union all` し、`order by priority, recency desc nulls last limit 1` によりレース全体に対して単一の `model_version` を1つだけ選ぶ。外側の `SELECT` はその model_version に属する行を馬の数だけそのまま返すだけなので、§11.1 (a) の通り馬単位の曖昧さはここでも生じない。

| priority | 選択条件                                                                                                                                                                                                                                     | recency の扱い                           |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| 0        | そのレースの cell-routing 期待 variant (`resolveFinishPositionDisplayPriorityModelVersion`, `finish-position-cell-routing.ts:353`) に該当する行があれば無条件選択。null/該当行なしなら次点へフォールスルー                                   | 未使用 (行の有無のみ)                    |
| 1        | このレースの RS-overlay model_version (`active.model_version` に `-rs-overlay-` とレース開催年月日を連結した文字列) に該当する行があれば選択                                                                                                 | 未使用                                   |
| 2        | 現在登録されている "active"/champion model_version に、このレースの行が **1 行でも** あれば選択 — 存在有無のみのチェックで鮮度/品質シグナルは一切見ない                                                                                      | `null::timestamptz` を固定でハードコード |
| 3        | 上記いずれにも該当しない場合、そのレースに行を持つ他の allowed model_version の中から `prediction_generated_at` の最大値が最も新しいものを選ぶ — このクエリが唯一「異なる model_version 間の genuine な "latest write wins" 比較」を行う箇所 | `max(p3.prediction_generated_at)`        |

priority=0/1/2 はいずれも「特定 1 model_version の行があるかどうか」の existence check であり、複数 model_version 間の比較は発生しない。priority=3 のみが `serve_accuracy_report.py` の旧バグと同種の「複数 model_version 間の recency 比較」を行う。

(補足) priority=3 の WHERE 句には `finish_position_active_models.subclass is not null` な「stale」モデルを除外する `not exists` 節が既に存在するが、これは cell-routing variant を対象にした exclusion ではなく無関係な別の防御機構である。§11.3/§11.5 が指摘する「cell-routing variant 用の exclusion が無い」という観察はこれとは別の話であり、混同しないよう明記する。

(参考) 「allowlist の網羅性」というテーマは、本ドキュメント §2 **Defect E** が既に別角度から確認済みである: Defect E は「cell-routing variant の model_version が `FINISH_POSITION_LEAK_FREE_MODEL_VERSIONS` に正しく **含まれている** か (priority=0 が選びたい時に誤って弾かれないか)」を確認し、問題なしと結論している。次節 §11.3 が検証するのはその裏側 — 「cell-routing variant の model_version が priority=3 の候補プールから正しく **除外されている** か」— であり、両者は同じ「allowlist 完全性」という主題の異なる半分にあたる。

### 11.3 調査過程の自己訂正 — grade_code と kyoso_joken_code の取り違え

§11.2 の tier 構造を検討する過程で、当初、次の仮説を立てた: **priority=3 の WHERE 句には cell-routing variant の model_version (`getAllFinishPositionCellRoutingModelVersions()` が返す `jockey-pedigree269` / `prior-corner274` など、本来 priority=0 経由でのみ — そのレースの属性が `cell_routing.json` のルールに一致した場合にのみ — 選ばれるべきもの) を除外する防御的な exclusion が存在しない**、というもの。

この仮説を裏付けるように見える具体例として、2026-07-12 に、champion model_version の行が 0 件でありながら `jockey-pedigree269` の行が存在するレースを 9 件発見した (福島 keibajo_code=03 race_bango 01/02/03/06/07、小倉 keibajo_code=10 race_bango 01/02/03/04)。これらのレースの `jvd_ra.grade_code` を確認したところ全レース空欄であり、「どの routing ルールにも一致していない = priority=0 は本来ミスするはずで、priority=3 がこの off-label な variant を漏らして選んでしまったに違いない」と結論した。

**この結論は誤りであり、独立した先行 sub-agent による careful re-verification によってその場で指摘・訂正された。** JRA の cell-routing ルール1 (`finish-position-cell-routing.ts:123-126`、`conditions: [{ dimension: "kyoso_joken_code", values: ["703"] }], variant: "jockey_pedigree_703"`) が実際に見ているのは `grade_code` ではなく `kyoso_joken_code` である。両者は `jvd_ra` 上の全く別の分類軸であり、`grade_code` は G1/G2/G3/OP/空欄という重賞格付け、`kyoso_joken_code` はレース条件コードで、`"703"` はこのルールが対象とする特定の条件値である。**筆者は誤った列を確認していた。** 該当 9 レースを `kyoso_joken_code` で再照会したところ、**全 9 レースが `kyoso_joken_code='703'` であることを確認した (sub-agent の指摘を受けた後、筆者自身による独立再確認でも同じ結果を得た)**。つまりルール1は正当に一致しており、`cellVariantModelVersion` は 9 レース全てで正しく `jockey-pedigree269` に解決され、priority=0 がそれを直接選択している。これらのレースは priority=3 に一切到達しておらず、意図通り正しくルーティングされたものであって、off-label な fallback 漏れではない。

re-verification はさらに踏み込み、このテーブルの全履歴で書き込まれたことのある (race, cell-routing-variant model_version) の組を**すべて**洗い出した (65 組)。このうち champion model_version の行が 0 件の 44 組 (= priority=3 の候補プール構成が結果に影響し得る唯一の部分集合) それぞれに対し、`cell_routing.json` の実際の first-match-wins ロジック (rule1: `kyoso_joken_code=703` → `jockey_pedigree_703`、rule2: dirt かつ field≤10 かつ `kyoso_joken_code=005` → `prior_corner_dirt_smallfield_005`、rule3: `venue=02` → `jockey_pedigree_703`) を適用し、left join でレースの取りこぼしが無いことも確認した。**結果: 65 組中、正当なルールに一致しなかったものは 0 件** — rule1 経由 30 件、rule3 経由 13 件 (全て venue=02/函館)、rule2 経由 1 件 (この 1 件は選択された model_version が文字通り `jra-cb-v10-prior-corner274-2013` であり、rule2 が予測する variant と完全一致することも独立に確認した)。priority=0 のルーティング解決は、このテーブルの全履歴を見る限り、cell-routing variant の model_version について priority=3 に到達する前に取りこぼしたことが一度もない。

この一連の流れ (誤った仮説の提示 → 独立した sub-agent の re-verification による指摘・訂正) は、検証規律が機能した実例としてそのまま記録する。最初の分析 (`grade_code` を確認して「一致していない」と判定) は、コード上のルール定義を正しく読めば防げたはずの単純な列違いのミスだったが、「確定した bug」として出荷される前に独立した re-verification によって捕捉・訂正された。**「priority=3 が cell-routing variant を漏らして選んでいる」という主張は、bug としては成立しない。**

**このスレッドの結論**: 理論上疑われた priority=3 の leakage は実際には発生していない — インシデント期間だけでなく全履歴を通じて 0 件。priority=3 の SQL が cell-routing variant に対する防御的 exclusion を欠いているという構造的な事実そのものは正確な観察だが、priority=0 が常に先に正しく解決しているため、これまで一度もその欠落が実害として顕在化したことがない。

### 11.4 9 レースの予測が壊れている理由 (既知の別問題、担当スレッドも既に割当済み)

上記 9 レースの予測は実際に品質としてほぼ乱数に近い (`predicted_score` の score 分散が壊滅的に小さい) が、これは本ドキュメント §2 **Defect A** (2026-07-12 の「Cluster B」一括書込インシデントが `predicted_score` の品質を劣化させている問題、within-race score 標準偏差が健全時 ~0.7〜1.5 に対し ~0.095) が、**正しくルーティングされた priority=0 serve の内側に、たまたま乗っている**だけであり、tier 選択自体の欠陥ではない。正しく routing された serve の中身が (Defect A により) データ品質として壊れている、という §2 で既に報告・未対処のまま `serve-defect-269` 調査スレッドに割り当て済みの問題であり (§7 参照)、本節でこれを再オープン・再割当てすることはしない。読者が「本節の tier 選択ロジックはクリーンなのに、なぜこの 9 レースの予測は依然として悪く見えるのか」を理解できるよう、接続関係のみをここに明記する。

### 11.5 (任意・非緊急) 将来的な防御強化案

priority=3 に、cell-routing variant の model_version を除外する防御的 exclusion を追加する余地はある — 既存の `allowed_model_versions` CTE パターンを踏襲し、`getAllFinishPositionCellRoutingModelVersions()` が返す集合を priority=3 の候補プールから除外する形が考えられる。これは **現時点で実在するバグを修正するものではない** (§11.3 の通り実害 0 件)。あくまで、将来 priority=0 のルーティングロジックとテーブルに実際に書き込まれる model_version の集合とが乖離するような未知のシナリオに備えた belt-and-suspenders 的な防御強化案であり、backlog 候補としての記録に留める。緊急性はなく、着手するかどうかは将来のセッションの判断に委ねる。

### 11.6 Defect B との関係 (既存の結論を変更しない)

この display path において唯一 genuine に「誤った予測が表示され得る」既知のメカニズムは、本ドキュメント §2 **Defect B** (priority=0 が一度一致すると無条件に信頼され、priority=2 の existence-only check が本来サーブしていたはずのものと比べた品質/鮮度チェックが一切無い) である。本節の監査はこの既存の結論を変更・再オープンするものではなく、本節が調査した (実在しなかった) priority=3 leak と Defect B とを明確に区別するためにのみ触れる。

**結論: 分析側 (`serve_accuracy_report.py`) が本日修正したものと同型のバグは、viewer 側 (`getFinishPositionLambdarankPredictions`) には存在しない。**

### 11.7 (実装記録) §11.5 の防御強化案を USER 決定⑯として実施 (2026-07-18 追記)

§11.5 が backlog として記録した防御強化案を、USER 決定⑯として本日実施した。実装は `apps/pc-keiba-viewer/src/db/queries.ts::getFinishPositionLambdarankPredictions` の `selected_model` CTE、priority=3 の WHERE 句に `and p3.model_version not in (select model_version from cell_routing_off_label_variant_model_versions)` を追加しただけであり、priority=0/1/2 には一切手を入れていない (priority=0 は cell-routing variant を正当に選ぶ唯一の経路であり、そのため `allowed_prediction_model_versions` は従来どおり cell-routing variant を含んだ全集合を使い続ける)。§11.2 補足が指摘していた stale-subclass 除外の `not exists` 節もそのまま残し、統合はしていない。

**§11.5 が名指しした `getAllFinishPositionCellRoutingModelVersions()` (`finish-position-cell-routing.ts:375`) を、そのままは exclusion に使わなかった点を明記する。** 実装着手前にこの関数の返り値を実測したところ、各カテゴリの `variants` マップには rule 経由でのみ到達する variant だけでなく、そのカテゴリ自身の `default_variant` (= 平場のチャンピオンモデル、現行は `jra-cb-v9-sim-2013-clean` / `banei-cb-v9-sim-2011`) も同じマップ内に列挙されており、関数はそれもまとめて返す。これは viewer 側ミラーだけの現象ではなく、コンテナ側の実 `cell_routing.json` (`apps/finish-position-predict-container/src/predict_lib/cell_routing.json`) を直接確認しても同型である。したがって `getAllFinishPositionCellRoutingModelVersions()` の返り値をそのまま priority=3 の exclusion set に転用すると、`jra-cb-v9-sim-2013-clean` / `banei-cb-v9-sim-2011` という「off-label どころかむしろ最も正当な」平場モデルまで priority=3 の候補プールから恒久的に締め出してしまう。今日時点ではこの 2 モデルがそのまま category の "active" チャンピオンでもあるため priority=2 が常に先に拾い実害は顕在化しないが、`FINISH_POSITION_LEAK_FREE_BASE_MODEL_VERSIONS` にこの 2 モデルが明示的に残されているのは「active でなくなった後も priority=3 経由で拾えるように」という意図であり、将来の champion 交代のタイミングで初めて顕在化しうる潜在的な regression だった。「実害ゼロの防御強化」のはずが、逆に実害ゼロではない新しい bug を仕込みかねなかったという意味で、§11.3 の自己訂正と同じ精神で扱うべき発見として記録する。

このため `finish-position-cell-routing.ts` に `getAllFinishPositionCellRoutingOffLabelVariantModelVersions()` (line 401) を新設した。ロジックは「各カテゴリの `variants` から、その `default_variant` と一致する要素だけを除いたもの」で、現行設定では `["banei-cb-v8-window2011-wf-15y", "jra-cb-v9-sim-2013-clean-jockey-pedigree269", "jra-cb-v10-prior-corner274-2013"]` の 3 件のみを返す (= rule 経由でしか到達しない、真に "off-label" な variant のみ)。priority=3 の新 CTE `cell_routing_off_label_variant_model_versions` はこちらから構築しており、`getAllFinishPositionCellRoutingModelVersions()` (allowlist 用、default variant を含む広い集合) とは明確に使い分けている。設定から直接導出する構造 (ハードコードでない) は維持されているため、「ハードコード禁止、parity test が守る形」という既存方針にはそのまま従っている。両関数の docstring に相互参照とこの非対称性 (allowlist は広いほど安全、exclusion list は正確でなければならない) を追記し、次に同じ関数を別用途へ転用しようとする読者が同じ見落としを繰り返さないようにした。

テストは `queries.test.ts` に (a) priority=3 が off-label variant を除外して何も選ばない回帰確認、(b) `jra-cb-v9-sim-2013-clean` のような平場モデルは引き続き priority=3 経由で選べることを示す non-regression 確認の 2 本を追加し、`finish-position-cell-routing.test.ts` にも新関数が default variant を除いた 3 件のみを固定順序で返すことを確認するテストを追加した。`bun run --filter pc-keiba-viewer tsc` / `lint` / `test:coverage` はいずれも成功し (3929 tests pass)、4 指標とも 95% のしきい値を上回っている (実測: statements 99.36% / branches 97.31% / functions 99.14% / lines 99.39%、`finish-position-cell-routing.ts` は uncovered line 0)。

念のため明記する ── 本節はあくまで §11.5 の backlog を USER 承認のもとで実施した記録であり、§11.3-11.4 が確認した「実害 0 件」という事実、および §11 の「結論: バグなし」を覆すものではない。今回見つかった `getAllFinishPositionCellRoutingModelVersions()` の対象範囲の広さも、priority=3 の exclusion に転用しようとして初めて問題になるものであり、既存の allowlist 用途 (`FINISH_POSITION_LEAK_FREE_MODEL_VERSIONS`) では何の問題も無い。

## 12. バグ調査A: RS shard 欠損の silent 劣化監査 + 22:00 refresher tick 検証 (2026-07-17 追記)

team-lead 指示により、(1) 22:00 JST の `corner-features-refresh` evening cron tick の live 検証を最優先で差し込み、(2) `468a4f0e` (tolerate missing running-style shard) が rs*p*\* NULL 経由で FP serving を silent に劣化させていないかを監査した。両方の結果をここにまとめる。

### 12.1 22:00 JST refresher tick 検証 (優先事項) — 結果: **確定 (cron 未発火)、根拠は 12.1a 追記を参照**

`CORNER_FEATURES_REFRESH_CRON_EVENING = "0 13 * * *"` (UTC) = JST 22:00。デプロイ済み Worker Version `3a75b34f-14c2-426b-b661-bb8f6258d6f2` (2026-07-17T01:23:06Z UTC) がこの tick を含む — `wrangler deployments list` で本検証時点でも同 Version が 100% active であることを再確認済み (ロールバック無し)。

**直接証拠 (2 回測定、うち 1 回目は不正確だったため訂正した)**: Neon に対する無条件クエリ `select now(), max(updated_at) from race_entry_corner_features` を、libpq (psycopg, `NEON_PRIMARY_URL`) と Worker 自身が使うのと同じ HTTP driver (`@neondatabase/serverless` の `neon()`) の両方で計 8 回 (libpq 4 回 + HTTP 4 回) クロスチェックした。1 回目の測定 (`max(updated_at)=2026-07-17 12:00:13.187748+00:00`) はこの 8 回の再測定と一致せず、単発の不整合な読み取りだったと判断し破棄した。8 回全て一致した値を正とする:

```
neon now() (直近):   2026-07-17 14:06:38.515Z  (23:06:38 JST)
max(updated_at):     2026-07-17 00:17:46.067Z  (09:17:46 JST) — libpq 4回 / HTTP driver 4回、全て一致
```

さらにこの `09:17:46 JST` という時刻自体を、本日のデプロイ時刻 `Worker Version 3a75b34f` (`2026-07-17T01:23:06.113Z` = **10:23:06 JST**) と比較すると、**書込みはデプロイの 1 時間 5 分前** であることが分かった。つまりこの書込みは、evening/morning cron が配線された新コードのデプロイ後に自動発火したものでは有り得ず、デプロイ前に (おそらく開発時の real-Neon smoke test として) write 可能な credential で手動実行された結果だと考えられる — 本日の cron 経由の自動書込みの証拠としては使えない。

言い換えると、**今日中に「新しく配線された cron が自動発火して書き込んだ」と言える機会は 22:00 JST の evening tick 一回のみ** (今日の 09:15 JST の morning tick はデプロイ完了 [10:23 JST] より前なので、たとえ発火していたとしても新コードではない)。その唯一の機会である 13:00 UTC (22:00 JST) の tick から、直近の再確認時点 (23:06 JST) で **1 時間 6 分経過しても `updated_at` は動いていない**。

13:00 UTC の tick が発火し何らかの行を UPSERT していれば、`updated_at = now()` は `ON CONFLICT DO UPDATE SET` 節で無条件 (値が変化したかどうかに関わらず) に更新される (`corner-features-refresh.ts:413`)。UPSERT の元となる `raw_rows` SELECT (`buildJraSelectSql`/`buildNarSelectSql`) は `[fromDate, toDate]` 日付レンジのみで絞り込む全件再計算であり、NULL 行だけに絞る WHERE 句は無い。本 tick の window は `[runYmd-7日, runYmd+2日]` = `[07-10, 07-19]` — この 10 日間に JRA・NAR 双方で多数の確定レースが存在することは本ドキュメント既存セクションで確認済みであり、「対象 0 行だったから何も動かなかった」は考えにくい。つまり **tick が正常発火していれば `updated_at` は動いていたはずだが、動いていない**。

なお、この 1 回目の測定値が 8 回の再測定と食い違った事実自体も報告に値する: `NEON_PRIMARY_URL` 経由の読み取りは (libpq・HTTP driver どちらであっても) 常に同一の最新値を返すとは限らない、という不整合を実際に観測した。原因は未特定 (Neon 側のプーラー/キャッシュ層の可能性が高いが未確認)。本監査の他セクションで同クレデンシャルを使った一度限りの読み取り結果についても、複数回のクロスチェック無しでは額面通りに信頼しきれない可能性がある点を留意事項として記録する。

**診断の試み**: デプロイ済みコードと全く同じ `refreshCornerFeatures()` を、evening cron と同一パラメータ (`daysAhead=2, lookbackDays=7, runYmd="20260717"`) で手動呼び出しし、`.env` の `NEON_PRIMARY_URL` を使って本番 Neon に対して 3 回連続実行した (scratchpad 上の bun script、`apps/finish-position-cron` 配下は一切変更なし)。3 回とも同一のエラーで失敗:

```
[corner-features-refresh] create extension vector skipped: NeonDbError: cannot execute CREATE EXTENSION in a read-only transaction
[corner-features-refresh] failed ...: NeonDbError: cannot execute CREATE TABLE in a read-only transaction
```

ただし **この結果は結論を出すには使えない** — `NEON_PRIMARY_URL` は本セッションを通じて読み取り専用検証用に使ってきた credential であり、Worker が実際に使う `env.NEON_DATABASE_URL` (Cloudflare Worker secret、ローカルから値を読む手段が無い) とは別物。今回 3 回とも read-only だった一方、本セッション序盤の NAR 07-13-15 backfill 時は同種の read-only 状態が数秒単位で flicker し再試行で解消した実績がある (`corner-features-settlement-backfill-heal-2026-07-17.md` §5.2 既述) ため、「ロールが恒常的に read-only」とも断定できない。この診断は **本番 credential の write path を検証できなかった** という事実のみが確定結果。

**除外できたもの**: デプロイの取り消し/ロールバック (`wrangler deployments list` で該当 Version が現在も 100% active であることを確認)。`wrangler.jsonc` 側のクロン文字列登録 (`"0 13 * * *"` が現在のファイルに存在、構文有効)。

**確認できなかったもの**: Cloudflare 側の実行ログ・呼出し履歴。`wrangler whoami` を再実行し、依然として `account (read)` のみで `Account Analytics: Read` スコープが無いことを再確認 (GraphQL Analytics API 利用不可)。ローカルに scoped API call 用の wrangler token file も存在しない。D1/KV/R2 いずれにもこの cron 専用の実行マーカーは無い (grep で該当箇所ゼロ)。

**構造的な観測ギャップ (副次的発見)**: `refreshCornerFeatures()` は自身の catch 節でエラーを `console.error` するのみで re-throw しない (`corner-features-refresh.ts:486-490`)。つまり Cloudflare 側が「scheduled ハンドラは正常終了した」と記録していても、内部で Neon 接続/権限エラーが起きて全処理がスキップされていた可能性を外部から区別する手段が (Analytics スコープなしでは) 無い。これは本タスクの rs*p*\* 観測ギャップ (§12.4) と同じ「silent catch で症状が外部に伝播しない」というクラスの問題であり、根はコードのバグではなく **観測可能性の欠如** である。

**当初の結論 (下記 12.1a で更新される前の時点)**: 22:00 JST tick が実際に発火したかどうかを Neon 側の間接証拠だけでは確定できなかった。本日中に新デプロイの cron が自動発火したと言える唯一の機会である 22:00 JST tick から 1 時間超経過しても `updated_at` が全く動いていない事実と、UPSERT が対象 0 行になり得ない window 設計であることから、「発火したが正常に完了した」の可能性は低いと考えるが、「発火しなかった」と「発火したが内部エラーで silent に失敗した」を区別する材料が無い、として一旦未解決のまま報告した。

### 12.1a 追加検証: Cloudflare GraphQL Analytics による確定 (team-lead 指示、2026-07-17 深夜追記)

team-lead から、別スレッド (hpo-catboost、調査 `b6e91420`) の副産物として **`.env` の `CLOUDFLARE_DEBUG_TOKEN` に `Account Analytics: Read` が既に付与済み**であることが判明したと連絡があった (sync-realtime-data worker の 22:00:15 JST バーストで実証済み)。上記時点で「ローカルに scoped API call 用の wrangler token file も存在しない」と記録したのは誤りではないが、`.env` の別トークンで代替できることを見落としていた。この token で Cloudflare GraphQL Analytics API (`POST https://api.cloudflare.com/client/v4/graphql`、`workersInvocationsAdaptive` dataset) を直接叩けることを確認した。

**手法の妥当性確認 (control check)**: 本 worker には 22:00 tick 以前から長期稼働している cron が複数あり (`wrangler.jsonc` の `triggers.crons`)、そのうち `WARM_CRON_PRE_JRA = "25 0 * * *"` (00:25 UTC) と `FEATURE_BUILD_CRON = "30 0 * * *"` (00:30 UTC) を control として選んだ。2026-07-16・2026-07-17 の両日とも、この 2 つの時刻ちょうど (00:25:27-58Z / 00:30:27-58Z) に `status=success, requests=1, subrequests=1` という最小・一貫した signature の invocation が記録されていることを確認した — これにより (a) `workersInvocationsAdaptive` が scheduled (cron) 発火を確かに記録すること、(b) `scriptName: "finish-position-cron"` フィルタが正しいこと、(c) cron tick は `requests=1, subrequests=1` という識別可能な signature を持つこと、の 3 点を実測で担保した (想像ではなく計測、`feedback_always_measure_never_assume` の実践)。

**本題のクエリ結果**: `finish-position-cron` の 2026-07-17 11:30-14:30 UTC の全 invocation (34 件) を取得し時系列で並べたところ、**12:50:32Z の invocation の次は 13:41:56Z まで一件も存在しない** — 13:00:00 UTC ちょうどを挟む **51 分間、完全に空白**だった。`wrangler.jsonc` の `triggers.crons` を全数確認すると、22:00 evening tick (`0 13 * * *`) 以外に 12:xx-13:xx UTC の時間帯に発火すべき cron は実は 1 つも無い (`*/30 1-11 * * *` / `*/10 1-11 * * *` / `7,22,37,52 1-11 * * *` はいずれも hour レンジが `1-11` UTC までで、12 時以降は対象外) — つまりこの時間帯に本来発火すべきだった cron は evening tick ただ 1 つであり、その 1 つが観測されなかった。**結論: 22:00 JST evening tick は発火しなかった (deploy 後、この cron 文字列にとって最初の scheduled 機会だった)。**

これにより当初の 2 択 (「発火しなかった」 vs 「発火したが internal error で silent に失敗した」) の後者は排除された — invocation record 自体が存在しないので、内部で catch されたエラーを云々する以前の話である。原因の確定はできていないが (Cloudflare 側の内部ログへのアクセス権はまだ無い)、**最も筋が良い仮説は cron trigger の新規登録が実際にスケジューラへ反映されるまでの伝播遅延**である — 22:00 tick はこの cron 文字列 (`wrangler.jsonc` へ本日 10:23 JST デプロイで追加) にとって配線後で迎えた最初の scheduled 機会だった。09:15 JST の morning tick (`15 0 * * *`) は今日分がデプロイ前の時刻だったため未検証のまま残っており、これが配線後 23 時間以上を経た初めての機会になる — 発火すれば伝播遅延仮説を支持、発火しなければ別の原因を疑う必要がある、というクリーンな判別テストになる。

副次的に、12:02-12:50 UTC に見えていた「約 10 分間隔」の invocation 群は上記の通り `triggers.crons` のどの cron にも該当せず (全て hour 1-11 UTC 止まり)、queue consumer のメッセージ処理や `sync-realtime-data` からの HTTP webhook 等、cron 以外の起動要因によるものと考えられる (深追いはしていない、本題と無関係)。また 13:41:56Z に `status=scriptThrewException, errors=1` の invocation が 1 件記録されているが、時刻が `15 0 * * *` / `0 13 * * *` いずれの corner-features-refresh cron 分とも一致しないため無関係と判断し、これも深追いしていない。

**結論 (最終)**: 22:00 JST evening tick は発火しなかったことを Cloudflare 側の一次データで確定した。原因は cron 登録の伝播遅延が最有力仮説だが未確証。**フォローアップ**: (a) 翌朝 09:15 JST の morning tick 後、同じ GraphQL 手法で発火有無を確認 (§g にも追記、配線後 23 時間超で初回発火となるためクリーンな判別テスト)、(b) 発火していれば伝播遅延仮説を支持、evening tick も明日以降は正常発火すると予想、(c) 明日 22:00 JST も発火しなければ伝播遅延ではなく別の恒久的な問題を疑い再調査、(d) §12.5 で提案した durable last-run marker はこの種の確認を将来 Neon 側だけで完結できるようにする改善として引き続き有効。

### 12.2 `468a4f0e` の tolerate semantics 確定

`git show 468a4f0e` の diff を読解した。変更の中心は `add-pacestyle-features.py` の `stage_rs_predictions_from_r2()` — R2 の該当日 shard 読み込みを try/except で囲み、`duckdb.IOException` のメッセージが `"No files found that match"` の場合のみ `create_empty_rs_predictions()` (7 列の型付き NULL 一時テーブル) にフォールバックし、それ以外の `IOException` は再 raise する。

後続の `joined` CTE は常に `base b LEFT JOIN rs_preds rs ON rs.race_id = ... AND rs.ketto_toroku_bango = ...` (`add-pacestyle-features.py:477-479`) という構造であるため、shard が無くても **レースそのものが feature build から脱落することは無い** — 対象レースの全馬について rs*p*\* 系列の列だけが NULL 化される「値の欠損」であり、「行の欠損」ではない。つまり team-lead の想定通り「NULL 化」が正しい semantics であり、default 値埋めではない。

### 12.3 rs*p*\* NULL 化の影響列 — 実測 11 列 (7 直接 + 4 派生、全て連動)

`rs_extra` (`add-pacestyle-features.py:436-460`) の SELECT リストを実際に数え上げたところ、最終 feature テーブルに現れる `rs_`-prefix 列は **11 列**:

- 直接ソース (7列、`create_empty_rs_predictions()` の空テーブルもこの 7 列を型付き NULL で用意): `rs_p_nige`, `rs_p_senkou`, `rs_p_sashi`, `rs_p_oikomi`, `rs_predicted_class`, `rs_predicted_corner_front_score`, `rs_predicted_corner_rank`
- 派生 (4列、`CASE WHEN rs.rs_p_nige IS NOT NULL THEN ... END` 等の NULL-safe SQL 式で上記 7 列から計算): `rs_predicted_corner_rank_pct`, `rs_confidence_entropy`, `rs_p_nige_x_field_pace`, `rs_sire_style_match`

派生 4 列は全て `CASE WHEN rs.rs_p_nige IS NOT NULL` (または `rs_predicted_corner_rank IS NOT NULL`) ガード付きのため、7 列がまとめて NULL 化されればこの 4 列も自動的に連動して NULL 化される。逆に言えば `create_empty_rs_predictions()` が 7 列しか持たないことによるスキーマ不整合(列不足エラー)は無い — 確認した限り実装は健全。team-lead 指示文中の「rs 8 列」は恐らく `rs_p_*` 4 列 + `rs_predicted_class/corner_front_score/corner_rank/corner_rank_pct` の主要 8 列を指す概算だが、実測の完全な影響範囲は **11 列**である。

### 12.4 rs*p*\* NULL 率の間接推定 — R2 shard 存在確認による考察

Neon 側に特徴量そのものは保存されないため直接測定はできない。R2 の shard 存在確認 (間接手段) を実施した。

**generation 移行の発見**: `RUNNING_STYLE_CATALOG_GENERATION = "raw-iceberg-v1"` は 2026-07-15 未明に writer/reader 双方に導入されたばかりの新しい世代識別子である:

| 時刻 (JST)       | commit     | 内容                                                                                     |
| ---------------- | ---------- | ---------------------------------------------------------------------------------------- |
| 02:48:50         | `11caa696` | Python reader (`add-pacestyle-features.py`) が `raw-iceberg-v1` パスを読みに行くよう変更 |
| 03:20:40 (+32分) | `0c26aedb` | TS writer (`running-style-parquet-export.ts`) が `raw-iceberg-v1` パスに書くよう変更     |
| 04:22:21 (+62分) | `468a4f0e` | tolerate-missing-shard フォールバックを追加 (本調査の起点)                               |

reader が writer より 32 分早くデプロイされているため、02:48-03:20 JST の間は reader が新パスを、writer が旧パスを見ており、万一この 32 分間に feature build が走っていれば shard-miss になり得た。さらに 03:20-04:22 JST の 62 分間は reader/writer は新パスで一致しているが tolerate フォールバックはまだ無く、この間の shard-miss は (NULL 化ではなく) 未処理例外だったはずである。ただしこの 94 分間は深夜 02:48-04:22 JST であり、JRA は日中開催・NAR も大半が夕方までに終了するため、この window 内に実際の feature build が走った可能性は低いと考えられる (R2 オブジェクトの正確な書込時刻までは今回確認していない — 推測であり確証ではない)。

**shard 存在の実測** (boto3 S3 互換 list、`pc-keiba-features-archive` バケット):

- 旧世代パス (`running-style/predictions/by-day/2026/...`、generation サブディレクトリ無し): 2026-06-07〜07-14 に 50 オブジェクト。JRA shard は本監査の対象レース日 (06-13, 06-14, 06-20, 06-21, 06-27, 06-28, 07-04, 07-05, 07-11, 07-12) **全てに存在**する。07-14 以降は旧パスへの書込が止まっている (07-15 の migration と符合)。
- 新世代パス (`running-style/predictions/by-day/raw-iceberg-v1/...`): 2026-07-15〜07-19 に **NAR shard のみ 5 オブジェクト、JRA shard は 0 件**。

**解釈**: 本監査が対象とした JRA レース日 (§1 の 264 レース、healthy cluster である 07-11/07-12 含む) は全て 07-14 以前であり、該当日の RS shard は (新旧いずれのパスでも該当する世代の) **旧世代パスに存在が確認できている** — つまりこれらの日について「shard 自体が存在しない」ことは NULL 化の原因ではなかった (shard の中身/カバレッジ範囲まで完全とは断定しない — 存在確認のみ)。

**前向きのリスク (最重要の発見)**: 07-15 の generation 移行後、**JRA のレース開催日が一度も無い** (直近 JRA 開催は 07-12、次回は明日 07-18)。NAR は 07-15 以降 daily で新パスへの書込・読み出しが機能していることが shard 存在から確認できるが、これは JRA でも同じパスが機能する保証にはならない — カテゴリ別の分岐やパス構築ロジックに JRA 固有の問題が無いとは限らない。**明日 07-18 (土) が、generation 移行後で初めての JRA 開催日であり、JRA の RS 予測が新しい `raw-iceberg-v1` パスへ正しく書込・読み出しされるかどうかの実質的な初回検証になる。** ここで shard-miss が起きれば、tolerate フォールバックにより §12.3 の 11 列が NULL 化されたまま FP feature build が「成功」として進み、§12.4 の通り feature_guard は検知しない。

### 12.5 `feature_guard` の 50% 閾値との関係 — 実測: 11 列は素通りする

`apps/finish-position-predict-container/src/predict_lib/feature_guard.py` を読解した。`is_degenerate_feature_matrix()` は `race_missing_feature_fraction(entries, feature_names) >= threshold` (`DEFAULT_MISSING_FEATURE_FRACTION_THRESHOLD = 0.5`) で判定し、`race_missing_feature_fraction` は **エントリごとの「全 feature_names 中 None の列の割合」をレース平均**したものである (列別の重み付けは無い、`feature_guard.py:66-82`)。`feature_names` はモデルのブースター自身が記録する全特徴量名 (booster_pool 経由でロード) であり、この監査で確認した範囲では総数は概ね 250 列超。

team-lead の想定通り、rs*p*\* 系列の §12.3 で確定した 11 列 (総数の 5% 未満) が丸ごと NULL 化されても、`race_missing_feature_fraction` は 50% には遠く及ばず **このガードは一切反応しない**。feature_guard は「レース全体がほぼ空のフィーチャ行列でスコアされた」という壊滅的な失敗 (2026-07-12 の 269 インシデント相当) を検知する設計であり、少数列だけが体系的に欠損する本ケースのような部分的な機能劣化は設計上の対象外 — これは feature_guard の欠陥ではなく、そもそも別の失敗モードに対する別のガードが必要であることを意味する。

### 12.6 観測手段の設計提案 (次サイクル候補、1 paragraph)

rs*p*\* 欠損率を将来観測可能にする最も安価な方法は、既存パターンの踏襲で実現できる: `add-pacestyle-features.py` が `stage_rs_predictions_from_r2()` の呼び出し結果 (shard hit/miss を bool で既に判定している) を、feature build 完了時に `[finish-position-features] rs_shard_status category=<JRA|NAR> race_date=<YYYYMMDD> shard_found=<true|false>` 形式の構造化ログとして 1 行出力する (この module 自身が cron 実行の一部として呼ばれる場合は既存の `[corner-features-refresh] ok/failed` と同じ命名規約に揃える)。ログだけでは今回同様 Analytics スコープが無いと見えないため、恒久的な可視化には `serve_health_check.py` (本ドキュメント §8、今回のセッションで新設済み) に 6 番目のチェック項目として「直近 N 日の JRA/NAR 各レース日について R2 の新世代パスに shard オブジェクトが存在するか」を boto3 で走査する処理を追加するのが最も低コストで、既存の週末監視ランブックにそのまま乗る。行レベルの `n_null_rs_columns` を Neon の予測テーブル自体に永続化する案は feature_guard 同様「書き込み側の変更」を伴い影響範囲が大きいため、まずは shard 存在監視 (読み取りのみ、既存 boto3 パターンの再利用) から始めるのが妥当と考える。

---

**この commit は本ドキュメント §12.1a の追加のみを対象とする。診断のため `.env` の `CLOUDFLARE_DEBUG_TOKEN` を使い Cloudflare GraphQL Analytics API (`workersInvocationsAdaptive`) へ読み取り専用クエリを複数回実行した (scratchpad 上の一時 JSON ファイル + curl から実行) 以外、コード・設定ファイルへの変更は一切無い。**`apps/finish-position-cron/wrangler.jsonc` は cron 文字列一覧の確認のため read のみ、`apps/sync-realtime-data` は無関係のため触れていない。MLflow・D1・R2・Neon への書込みは本追記では一切行っていない (Cloudflare Analytics API はアカウントの請求/実行メタデータを返す read-only API であり、対象 worker のコードや設定は変更されない)。

**過去の commit 履歴 (本ドキュメント §12 系列)**: 初稿 (§12 全体) は 1 commit、libpq 単発読み取り 1 回のみに基づく §12.1 の証拠を libpq 4 回 + HTTP driver 4 回のクロスチェックで訂正した commit が 1 つ、そして本 commit (§12.1a、GraphQL Analytics による確定) が続く。

---

## 13. 2026-07-18 朝間チェックポイント実績（07:30、team-lead 指示による開催前監視）

本節は §12.6 が提案していた「serve_health_check.py への第6チェック追加」実装後、**raw-iceberg-v1 世代移行後で初めての JRA 開催日**の実地観察記録である。team-lead 指示（campaign-summary-2026-07-17.md §g）に基づく朝間チェックシーケンスの 07:30 分。read-only 確認のみ、コード・設定変更なし。

### 13.1 D1 JRA レース登録状況

`realtime_race_sources`（`apps/sync-realtime-data` 所有、D1 binding `REALTIME_DB`）を `source='jra' AND kaisai_nen='2026' AND kaisai_tsukihi='0718'` で照会（07:2x JST 実行）: **0 行**。既知の過去実績（discovery は概ね 09:00 JST 前後）と整合し、この時点では異常ではない。opening-day（世代移行後最初の開催日）の前例が無いため、09:00 を過ぎても 0 のままなら初めてその時点で異常と扱う。

（補足: 当初 team-lead 指示文にあった `jra_race_keys` という別テーブル名は誤りと判明——同名の migration ファイル `0008_jra_race_keys.sql` は実際には一回限りの `race_key` プレフィックス修復スクリプトであり、CREATE TABLE は存在しない。JRA 行は `realtime_race_sources` 自体に `source='jra'` として同居しており、日付列は他 source 同様 `kaisai_nen`/`kaisai_tsukihi` 分割。以後この単一テーブルのみを対象にすれば良い。）

### 13.2 `serve_health_check.py --date 20260718 --category jra` full run

Neon 接続に `NEON_PRIMARY_URL` が必要——非対話シェルでは repo-root `.env`（`direnv`/`dotenv` 前提）が自動ロードされないため、`set -a && source .env && set +a` で明示ロードしてから実行（値は一切出力していない）。

```
[1] Coverage: OK (0 gaps)
[2] Quality (Cluster-B signature): OK (0 degraded groups)
[3] Routing parity: OK (0 mismatches)
[4] Burst detection: OK (0 minute-buckets > 10 races)
[5] D1 self-heal activity (informational): 0 event(s)
[6] R2 shard presence (trailing 7d, jra/nar): 9 gap(s) found
      - 20260712/20260713/20260714: jra AND nar both not found
      - 20260715/20260716/20260717: jra not found（nar は found）
      - 20260718 (本日): jra=not found yet, nar=found
Exit code: 1
```

**解釈（exit code 1 は本日時点では genuine incident ではないと判断）**:

- 07-13〜07-17 の jra gap: この5日間 JRA 開催そのものが無かった週日（次回開催が本日07-18である旨、本ドキュメント §5 に既述）——予測が生成されていないので shard が無いのは自明であり、§8.6/§9.5 が check 3（routing parity）について指摘した「no-data 日の vacuous mismatch」と同種の non-signal。
- 07-12 の jra gap: この日自体は genuine な JRA 開催日（§1〜§9 の Cluster A/B インシデント本体）だったが、raw-iceberg-v1 世代パスへの移行は 07-15 のため、07-12 の RS 予測は旧世代パスにのみ存在する（§12.4 で既確認）。新パスに無いのは移行前データとして完全に予期通り。
- **本日 07-18 の `jra=not found yet` のみが今回唯一の生きた観察対象**。ただし §13.1 の通り D1 へのレース登録自体がまだ 0 件（RS予測生成はレース登録より下流の工程）のため、07:30 時点でこれが未発見であること自体は想定内——正式な異常判定は 08:30 判断点（D1 登録が進んだ後も shard が付かない場合）まで持ち越す。

**check 6 の設計上の限界（新規記録、§8.6 に類する追記候補）**: check 6 は「対象日にレースが実在したか」を判定せず、trailing window 内の全 (day, category) の組を機械的に shard 存在チェックする。したがって non-race day や pre-migration date の gap は check 3 と同じ vacuous-artifact 性質を持ち、exit code 1 だけを見て「異常」と早合点しないよう運用上の注意が必要（今回がその実例）。恒久対応は次サイクル候補として記録するに留め、本日の運用では手動解釈で対処する。

### 13.3 含水率/クッション値 当日値取得

`tmp/cushion-moisture-pilot-2026-07-18/fetch_daily.py` を実行（07:2x JST）: 正常終了、accumulator は 58 行（前回 00:2x JST 実行時と同数）。**2026-07-18 分の読み取りは 0 行——本日分はまだ未公表**（公表窓 05:00-07:00/10:00 諸説あり、doc 記載の窓とスクリプト自身の出力メッセージで幅がある）。前セッションが仕込んだ `CronCreate` 自動取得（id `bf67d061`）はセッションスコープのため生存確認できず、本実行が唯一確認できた実行。08:30 チェックポイントで再実行し取得を試みる。

**次アクション**: 08:30 判断点で (a) D1 JRA 登録の再確認、(b) R2 shard 再確認（本日分が着地していなければ `plan-running-style-predictions` inline job 起動を検討）、(c) cushion/moisture 再取得、を実施する。

### 13.4 08:30 判断点実績

3項目とも再確認したが、07:30 時点から変化なし:

1. **D1 JRA 登録**: `realtime_race_sources`（source='jra', 2026/0718）= **依然 0 行**。09:00 discovery 見込みにはまだ届いていないため、この時点では想定内（team-lead 指示通り、09:00 超過をもって初めて異常判定）。
2. **R2 shard (check 6)**: 07:30 と完全に同一の出力（9 gap、本日分 `jra=not found yet`）。**prewarm inline job はあえてトリガしなかった**——理由: 上記 (1) の通り D1 にレースが1件も登録されていないため、`plan-running-style-predictions` が対象とすべきレースそのものが存在しない。この状態でトリガしても空振り（無駄な job 起動）にしかならず、team-lead 指示の「登録待ち = prewarm 不能、09:00 discovery 後に再判断」の判定に該当すると判断し、意図的に見送った。
3. **含水率/クッション値**: `fetch_daily.py` 再実行、accumulator 引き続き 58 行（増分ゼロ）。**本日分は 08:30 時点でもまだ未公表**。doc 記載の公表窓（05:00-07:00）は過ぎているが、スクリプト自身のメッセージが示す通り実際は venue 依存で 10:00 まで幅がありうる。09:20/09:40 チェックポイントで再試行する。

**次アクション**: 09:00 を過ぎた時点で D1 登録が依然 0 なら team-lead へ即エスカレーション（このチェックポイント自体では実施しない、次の自然な確認機会である 09:20 チェックで併せて確認）。09:20 で corner-features 09:15 tick 検証と合わせて D1/shard 再確認、cushion/moisture 再試行を行う。

### 13.5 09:16 JST 開催直前チェック実績

**1. D1 JRA 登録 — discovery 完了確認**: `realtime_race_sources`（source='jra', 2026/0718）を再照会したところ **36 行 (keibajo 02/03/10 各12レース)**。08:30 時点ではまだ 0 行だったため、09:00 discovery が想定通り機能したことを確認 — opening-day 前例が無かった項目だが正常に discovery された。

**2. R2 shard (check 6)**: `serve_health_check.py` 再実行、出力は 08:30 と同一（本日分 `jra=not found yet`）。races が discovery されたのは直近数分のため、RS 予測生成（discovery のさらに下流の工程）がまだ追いついていないだけと解釈——09:25 の第一レースまでに着地するかを引き続き監視する。races 未登録による「生成対象なし」の段階は今回で終わり、以後の not-found は genuine な生成遅延の可能性を帯びる。

**3. 09:15 JST corner-features tick — 発火確認（伝播遅延仮説が支持された）**: 2つの独立手法で確認した。

- **Neon 直接読取**（psycopg、2回連続クロスチェック、両方一致）: `now()=2026-07-18 00:16:59 UTC` に対し `max(updated_at)=2026-07-18 00:16:16 UTC`（= 09:16:16 JST）——ほぼ現在時刻に一致。2回目の読取（`now()=00:17:25 UTC`）でも同一値で安定。
- **Cloudflare GraphQL Analytics**（§12.1a と同一手法、`workersInvocationsAdaptive`、`scriptName=finish-position-cron`、00:10-00:25 UTC 窓）: `datetimeMinute=2026-07-18T00:15:00Z`（= 09:15 JST ちょうど）に `status=success, requests=2, subrequests=50` の invocation を確認。§12.1a の control check が示した「trivial cron は requests=1,subrequests=1」という基準に対し、`subrequests=50` は実質的な DB 処理（複数レース分の upsert）を伴う genuine な実行であることを示す——昨夜 22:00 JST tick の完全な無音（invocation 自体が存在しない）とは明確に異なる。

**結論: 昨夜 22:00 JST tick 未発火の原因として最有力視していた「cron 登録の伝播遅延」仮説が支持された** — 配線後 23 時間超を経た今朝の 09:15 tick は正常発火し、Neon への書込みも実際に発生している。§12.1a のフォローアップ項目 (a)(b) はこれで解消。残る (c)「今夜 22:00 JST も発火しなければ伝播遅延ではなく別の恒久的な問題」は今夜の観察待ち。

**留意点（team-lead 指摘の deploy churn 切り分け）**: `apps/finish-position-cron` の `wrangler deployments list` を確認したところ、直近デプロイが **00:14:45 UTC**（version `36cba906`）と **00:18:00 UTC**（version `12344a51`）の2件——tick 発火時刻 (00:15:00Z) をほぼ挟む形で do-sharding/deploy-verify によるデプロイが進行中だったことを確認した。scheduled cron invocation は deploy イベントとは別記録（`workersInvocationsAdaptive` は実行ログ、deploy は `Source: Upload` の別メタデータ）であるため tick 発火の実在性自体は揺るがないが、**tick がどちらのバージョンで実行されたかは本チェックでは確定できない**（00:14:45 デプロイ直後〜00:18:00 デプロイ直前の狭い窓）。実害の兆候（エラー、silent failure）は見当たらないため、現時点では informational な記録に留める。

**次アクション**: 09:25 開催後、最初の JRA レース群の serving 行が発走前 created・健全 stddev (>0.5) で着地するかを最重要観察点として確認する。

### 13.6 🚨 09:47 JST インシデント — 第一レース発走直前、本日 JRA 予測行ゼロ

第一レース（函館R1, post 09:50 JST）まで残り3分未満の 09:47:55 JST 時点で確認:

- `race_finish_position_model_predictions` を `source='jra' AND kaisai_nen='2026' AND kaisai_tsukihi='0718'` で group化照会 → **row groups: 0**。36レース中1レースも予測行が存在しない（score劣化ではなく完全な不在）。
- `serve_health_check.py --date 20260718 --category jra`: check 6 は依然 `jra=not found yet`（07:30 から一度も変化なし）。
- R2 を直接照会（boto3、region_name="auto"）: `running-style/predictions/by-day/raw-iceberg-v1/2026/07/18/jra/` = **0 objects**。一方 `.../2026/07/18/nar/` には `nar-running-style-lgbm-prod-v3.parquet`（LastModified 2026-07-18 00:47:57 UTC = 09:47:57 JST、確認の**まさにその瞬間**）が存在——**raw-iceberg-v1 の write パイプライン自体は健全（NAR が証明）、JRA カテゴリのみ本日一度も書き込みが無い**。

**切り分け仮説（read-only 調査、未検証）**: `RUNNING_STYLE_INFERENCE_CRON`（10分毎）→ `planRunningStylePredictionsForDate` → `listRunningStyleRacesByDate` は D1/local PG ではなく **R2 Iceberg カタログ**（`pc-keiba-r2-catalog` Worker の `/v1/race-keys`、`jvd_ra`/`nvd_ra` UNION ALL、`apps/sync-realtime-data/src/running-style-race-list.ts:12-18`）からレース一覧を取得している。この R2 Iceberg 側への `jvd_ra`/`jvd_se` 反映は `apps/pc-keiba-r2-catalog/scripts/sync_r2_catalog.py` という別経路のスクリプトで、本監査では**自動 cron/launchd トリガーを発見できなかった**。NAR は毎日開催があるため習慣的に再実行されている可能性が高いが、**本日が raw-iceberg-v1 移行後で最初の JRA 開催日であり、この sync が JRA 分についてまだ再実行されていない**可能性がある——D1/Neon に地上の JRA レースが登録済み（§13.5）であることと、R2 Iceberg カタログに反映済みであることは別の2段構造。

**未検証な理由**: R2 SQL への直接照会に `R2_SQL_TOKEN`/`WRANGLER_R2_SQL_AUTH_TOKEN`/`R2_CATALOG_TOKEN` が必要だが、本チェック実行環境にはこれらが無く直接確認できなかった。

team-lead へ即時報告済み（2件のメッセージ、09:47台）。提案した回復アクション（`sync_r2_catalog.py --date 20260718 --tables jvd_ra,jvd_se` の実行）は権限保持者（deploy-verify 想定）の判断・実行に委ね、本 agent からは実行していない。rollback 候補（`RACE_SHARDED_DO` 削除、`COORDINATOR_ENABLED=0`）の要否も team-lead 判断待ち。

**続報は本節に追記する。**
