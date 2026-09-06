# 2026-09-06 NAR Container 強制停止・調査

## 操作

- ユーザー指定: `race-chain-predict-nar-0`, `race-chain-predict-nar-2` の2台。
- 11:42–11:43 JSTに既存 admin stop APIへ `overrideActive: true` で送信。HTTP 202だけで完了とはせず、0が stopped / 2が inactive になったことを確認。
- 調査中の新規開始を抑制するため共有 `finish-position-predict-queue` を一時 pause。完了通知・Container制御Queueは停止していない。
- 正常性確認後、既存 `resume-delivery` で配信再開済み。Containerの手動直接起動・ローカル代替推論なし。

## 正常性の確認

- 停止前のWorkerログで NAR 54-06 の `/predict` と 55-07 の status pollを確認。
- D1 lifecycle の完了記録は11:32から11:42:27まで増加。当日NARの prediction retry errorテーブルは0件。
- Viewer APIで54-06は11/11頭、55-04は12/12頭、55-05は11/11頭。予測値は有限で[0,1]内。
- この観測範囲では、放置稼働・異常ループの証拠なし。ジョブ受理ログだけを成功と扱わない。
- 再開後のログで `race-chain-predict-nar-2` に54-09の通常予測・durable watchが受理され、`/predict` が呼ばれることを確認。D1では46-11が11:53:01.454 JST、54-09が11:53:31.838 JSTに完了。
- `nar-0` と `nar-2` の両方で完了後の warm-reuse grace が記録され、通常の完了・クリーンアップ経路へ戻ったことを確認。全NARレースの処理完了とは別。コード修正は行っていない。

## 起動時間の訂正と調査範囲

CLI `created` は稼働中には deployment作成時刻、非稼働時にはDO assignmentに由来する値が表示される。停止→再起動しても06:37と表示された実例があり、差分「約4時間55分」は連続稼働時間ではない。

現行dash APIの `current_placement.events` は `VMStarted` / `ContainerStarted` を区別して保持する。これを使えば取得済みplacementの起動を確認できるが、停止後に解放された対象2台はDO→deploymentの現在の割当がなくなり、過去placement一覧が必要になる。

過去履歴取得は未達: installed Wrangler が使う `/cloudchamber/deployments/{uuid}/placements` と `/cloudchamber/deployments/v2` は既存認証で401。`/containers/deployments/{uuid}/placements` は400 / No route。現行dashのcurrent placementは読めるが、停止で割当が解放された対象2台の過去期間を復元できない。

従って、停止前の2台の正確な連続稼働時間・累積稼働時間は算出していない。これはサービスの停止状態ではなく、過去イベントの取得手段に関するブロッカー。履歴を閲覧できるCloudflareアクセス、または対象DO IDに対応する開始・停止イベントのエクスポートが必要。イベント両端のない区間を推測で埋めない。Container uptimeとVM lifetime、課金時間を同一視しない。

## 追加のロジック監査と修正（12:03 JST反映）

後続のコード調査で、`/focused-full-status` と `/focused-full-cache` も通常要求と同じ `startAndWaitForPorts` / `containerFetch` を通る問題を確認。過去の稼働が全て異常だったと断定する証拠ではないが、遅延・重複watchで停止済みContainerを再起動し、終了済み処理の監視でidle期限を延長できる再発経路だった。

- 監視・結果取得は既に動くruntimeのTCP portのみを呼ぶ。停止時は status=missing / cache=404 を返し、起動しない。
- statusが対象レースのrunningで、実開始から31分未満、進捗が4分以内、時刻が妥当な場合だけactivity更新。
- 終了済み・欠損・期限切れ・無進捗・不正応答・別レースの監視は更新しない。結果取得も更新しない。
- 観測失敗で別の正常処理をdestroyしない。監視I/Oは5秒で打ち切る。
- race-chainの2分idle fallback、既存watch/完了後の所有権付き停止を維持。正常な連続ジョブ再利用を時間だけで強制停止しない。

検証: 1,920 tests成功、lint/type成功、coverage 97.69 / 95.04 / 98.81 / 98.24%。新規activity判定の境界・不正値と、監視/取得によるcold start防止を回帰テスト化。

安全deployで開始キュー停止・既存処理drainを行い、Worker版 `cb652b3c-0682-4d03-a36c-464a6c4e0504` を反映。予測の強制再計算なし。予測/再採点キューは配信再開済み。ビルド用ローカルColimaも停止済み。

本番確認（task151–152）: nar-2のinactiveを確認し、反映後も55-08が12:04:55.933、54-10が12:05:36.791 JSTに完了。NAR当日retry error記録は0。再採点のHTTP 200と、その後のContainer停止ログも確認。未完了レースが残るため稼働中Containerがあること自体は異常扱いしない。

## 証跡

作業ログ: task137–147。作業JSON: `tmp/container-events-*`, `tmp/container-history-*`。認証トークンはメモリ内のみで利用し、文書やコンソールへ出力していない。
