# 2026-10-10 勝率ヒートマップ復旧

## 原因と修正

- 当日の本番一覧は59レース。修正前、東京・京都の24レースが `heatmap_execution_blocked`。地方35レースは取得可能（一部の通常GETには一時的な30秒タイムアウトがあり、再取得で成功）。
- `HeatmapExecutionCoordinator.ensure` が終了済みの失敗を3回数えると、期限に関係なく永久停止していた。上流の一時障害や公開前データから回復しても、国内レースは通常GET・定期warmで復帰できなかった。
- ログには10/10向けCatalogの未公開出走データ・SQLタイムアウトが存在する。ただしログはサンプリングされており、24件すべての最初の失敗理由を特定したものではない。
- 終了確認済みの失敗のみ再試行可能に変更。最初の2回は30秒、3回目以降は15分待機し、失敗カウンタを3で飽和させる。成功すると解除する。
- queued/running/不明状態の所有権は解除しない。古いトークンのclaim/settle拒否、同時GETから1件だけ投入する制約、保存後の完了通知は維持。所有権リセット・キャッシュキー変更・ダミー値の補完は行っていない。

## 最初の失敗の追跡

- Workflows履歴をUTC 10/8 00:00〜10/9 15:00でページング取得。912件を列挙、実行owner形式186件の詳細を読み、10/10向け128件を確認した。中央24レースは各3件、計72件で失敗していた（開始は10/9 09:11・09:20・09:40頃JST）。永久停止に至る実行履歴と一致する。
- 当時のViewerログには `Heatmap cache warm failed Partnership catalog request failed: 503` と `Catalog race runners unavailable` がある。Catalogには対象日の統計SQLタイムアウト、`race_runners_read_failed` がある。後者は元の例外をログに保存しないため、失敗理由の細部は復元できない。
- 当時から同じCatalog version `acc9142c`。`warm=1` の組み合わせ統計が503になる分岐は、snapshot未準備か、当該レースの出走データJOINが空である場合。同時刻の地方向け同APIは200であり、中央向け出走データ未準備と整合する。ただし当時の503本文やSQL結果は保存されておらず、24件すべてを同一の直接原因とは断定しない。
- 復旧時の京都2R・4R・9Rの初回再実行は約60秒で503となり、10秒後のWorkflow内再試行は短時間でERRORキャッシュを返していた。最初の障害と、復旧時の上流timeoutは区別する。
- 原本: `workflow-history/target-history.json`、`initial-failure-viewer-logs.json`、`initial-failure-catalog-logs.json`（すべて上記private cache配下）。

## 本番反映

- 旧: `ee830d21-3646-4caf-8157-372fed8c15a9`
- 新: `a5350081-ff0a-4368-a567-7bb0dc687aa4`（2026-10-10 07:10 JST）
- 旧リリースの隔離コピーからバイト一致で旧Workerを再現し、coordinatorと隣接テストだけを変更。既存OpenNext成果物・静的assetsは保持し、無関係な未コミット変更は配信しない。
- 本番Workerは検証済みdry-run成果物とバイト一致。35 bindings、secret名、cron、互換性・実行設定、公開設定を維持。
- 初回deployはWranglerのOpenNext自動委譲がローカルHyperdrive設定不足で配信前に終了。現行版不変を再確認し、既存成果物を直接配信する `--x-autoconfig=false` で成功した。

## テスト

- root TypeScript: 6,329テスト、282ファイル成功。coverage statements 98.82%、branches 96.74%、functions 99.19%、lines 98.95%。tsc・lint成功。
- 隔離candidate: 6,306テスト成功、format・lint・tsc成功、4指標同上。初回は隔離コピーに周辺パッケージのテスト入力がなく失敗し、旧リリースの入力を参照して再検証した。
- Python: 5,315テスト成功、97.43%（既存の警告27件）。Pythonは変更なし。
- 時刻境界、15分待機の永続化、連続失敗時の再制限、旧blocked状態からの復帰、成功時の解除、二重投入・古いtoken拒否を回帰テストに追加。

## 復旧確認（07:30 JST）

- 07:13 JSTは56/59。京都2R・4R・9Rの15分backoff終了後、通常GETで再生成を要求し、各Workflowの保存完了後に再取得した。
- 07:30:14 JSTの監査で **59/59レース、678頭、数値セル11,389個** を確認。各馬20列、馬番・馬名一致、率の範囲・件数との整合性を検証し、配信元Reactコンポーネントのローカル静的描画で全59テーブルと全出走馬行を確認した。
- 京都9Rは最初のWorkflow保存後のGETでもqueuedとなったが、その通常GETからの次のowner Workflowも保存成功し、最終取得で表示データを確認した。キャッシュの読み取り地域差・伝播時間を測定していないため、2回目の生成理由は断定しない。
- 原本: `retry-after-backoff/content-render-audit.json`、各59レースのJSON、`audit-retry-after-backoff-final.log`。`retry-after-backoff/progress.json`は最初のGETラウンドの56/59という途中結果を保持し、後続の3件再取得後の最終判定はcontent-render-auditを参照する。
- Container短縮策の反映後、**07:55:40 JSTにも全59レース・678頭・数値11,389セル・全59テーブルを再確認**し、再取得した当日一覧の全59パスとの一致を検証した。京都10Rの初回GETは30秒timeout（curl exit 28）で、1件だけ再取得して成功。`final/progress.json`は再取得前の58/59を保持し、最終結果は `final/content-render-audit.json`、`audit-final-retry.log`、`final/day.html`。これは恒常的な失敗解除と表示データの復旧確認であり、上流の一時timeoutが皆無になったという証明ではない。

証拠: `.cache/heatmap-recovery-20261010/`。`postdeploy/deployment-audit.json`、`worker.diff`、`candidate-check-2.log`、`root-coverage.log`、`root-python.log`、`verified/progress.json`。

## Container実行経路の確認（07:25 JST）

- 本番 `prediction-readiness 20261010` は59/59 `preWeight.complete=true`。全59件が馬体重未配信であり、発表後予測の未生成はこの時点では欠損ではない。復旧目的の予測Container再生成は不要。
- 本番prediction Worker `3702ed48` の成果物に、day-base永続化確認後・Worker fanout前の `stop-day-base-container`、Workflow `step.sleep`、Worker側market-signal foundation処理が含まれている。focused-fullも永続キャッシュ確認後、Viewer I/Oより先に所有権付きstopする実装。
- 本番設定はday-base分割 `jra,nar,ban-ei`、race-chain fused有効、Worker market-signal foundation有効。JRA Worker rescoreは無効、ばんえいWorker rescoreはshadow。数値一致を確認せず有効化しない。
- Container usageのアプリ・時間別集計だけでは個別ジョブの最短時間や削減率を証明できない。処理中の強制stop、45分のbuild保護上限の一律短縮、未検証モデル移行は実施しない。上記は既存本番経路の確認であり、今回新たな実行時間削減を達成したという意味ではない。
- 原本: `prediction-readiness.json`、`prediction-version.json`、`live-prediction-index.js`、`container-usage.json`。

### 追加短縮策の本番反映（07:52 JST）

`finish-position-cron` を `12b293c3-3d23-40d4-9dd9-7ce374a7b3f2` にWorker-onlyで反映。永続化済みday-baseからのWorker materialization/fanoutより先に、既存の所有権付きContainer停止依頼を送る。対象関数以外の本番バイト、54 bindings・10 cron・5 Queue設定、予測用3アプリのContainer image/versionを維持した。全2447テスト・4指標95%以上合格。両start Queue再開、予測Container inactive、予測59/59を独立確認。削減秒数は未測定。詳細: [`Container runtime`](../../finish-position-cron/docs/container-runtime.md)。

### 継続対応：再計算後の停止依頼も前倒し（10:18 JST）

追加デプロイ許可後、既存の未反映rescore修正のみを `finish-position-cron` **`fb1c9690-3257-48fc-a5db-6af79a37443e`** へ反映した。保存済みスコアのshadow比較・KV/Viewer処理を待たずに停止依頼を送る。早期依頼失敗でも配信を続け、終端で停止依頼を再試行するテストを追加。全2,449テストと4指標95%以上に合格し、配信バイト・54 bindings・10 cron・5 Queue設定・予測用3 Containerアプリのimage/versionを維持、start Queue再開を確認した。

**10:18:10 JSTにも59/59レース・678頭・11,389数値セル・全59テーブルを再検証**した。予測は59/59、馬体重配信済み6件はすべて完了、残り53件は馬体重待ち。証拠: `.cache/container-runtime-20261010-followup/`、`.cache/heatmap-recovery-20261010/rescore-postdeploy/content-render-audit.json`。時間帯・アプリ別のCPU集計と短時間tailでは同一ジョブの保存・物理停止の対応が取れず、実削減秒数は未測定。計測のための予測再投入はしていない。

実ブラウザの直接検証は行わない（Playwright禁止）。APIの内容検査と、配信元Reactコンポーネントを使うローカル静的描画を区別して記録する。netkeibaへの通信、Container起動、モデル再生成、元データ更新は行っていない。
