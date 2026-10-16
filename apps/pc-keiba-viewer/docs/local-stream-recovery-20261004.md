# 2026-10-04 localhost 馬体重ストリーム復旧

対象: `https://localhost/races/2026/10/04/08/01`。

## 原因と修正

`src/lib/production-api-proxy.server.ts` のSSE転送がJSON用の10秒AbortSignalを流用し、ヘッダー取得後も正常なストリームを切断していた。さらに、更新のない接続ではNodeのHTTP/2受信タイムアウト（`UND_ERR_BODY_TIMEOUT`, 300000ms）が発生することを実ログで確認した。

- 接続開始時の10秒期限は維持し、ヘッダー取得後に解除する。
- `horse-weights-stream/route.ts` からブラウザのAbortSignalを渡す。切断時は上流も終了させる。
- 4分でストリームを正常終了し、標準EventSourceの自動再接続に任せる。再接続時には上流が最新の馬体重スナップショットを送る。全本文をバッファせず、バックプレッシャーを維持して転送する。
- 上流本文の予期しないエラーはログに残し、再接続できるよう下流を正常終了する。タイマーと上流のキャンセルもテストする。
- 接続開始時のDOMExceptionは、Next.jsがエラーメッセージを書き換えられるErrorに包む。接続失敗そのものは隠さない。

## 検証と運用

証拠は `.cache/local-stream-fix-20261004/` に保存。

- 修正途中の65秒有界観測でHTTP 200と8頭分の馬体重を確認。
- 再起動後のページは3回連続HTTP 200で全文取得完了（約1.5秒・0.7秒・0.6秒）。
- 最終実URL検証: HTTP 200のストリームが240.581秒で正常終了（curl終了0）。新しい接続でも8頭分の馬体重を受信。後続ページもHTTP 200、1.491秒で全文取得完了。検証ジョブ全体の終了statusは0。`rotated-stream.*` / `reconnected-stream.*` に保存。再接続確認はcurlによる新規接続であり、実ブラウザの自動再接続を操作した検証ではない。
- `rotation-tests.log` / `rotation-coverage.log` / `rotation-tsc.log` / `rotation-lint.log` が最終コードの検証記録。
- TypeScriptは5,869テスト成功。カバレッジ statements 98.80%、branches 96.69%、functions 99.21%、lines 98.93%。対象2ファイルの44テスト、型検査、lintも成功。
- Pythonは5,315テスト成功、カバレッジ97%（警告27件）。閾値・計測対象は変更していない。
- 古い検証用tmuxタスクを終了し、ローカル常駐サーバーを専用tmuxセッション `pc-keiba-viewer-dev-https` に移した。ソケット名は `pi-tmux-7f9977cee7a4dbafd52bc25a4ffbe981`。常駐プロセスを短時間タスクとして扱うoverdue通知の繰り返しを避ける。ログは同ディレクトリの `dev-server.log`。

## 別件のCloudflare開発接続ログ

`internal error; reference = ...` / `Network connection lost` は、ストリームの10秒・5分切断とは区別する。開発サーバー再起動後も一部残る。別プロセスのWranglerプラットフォームプロキシでKVのMISS・実際のSSRキャッシュHIT（17,326文字）・D1の読取を確認した。

同じ3件単位の内部エラーと、長時間のremote bindings接続劣化は上流でも報告されている。ただし、本環境の全ログが同一原因と断定したり、恒久解決済みと扱ったりしない。

- https://github.com/cloudflare/workers-sdk/issues/15351
- https://github.com/cloudflare/workers-sdk/issues/15421 （初回調査時open）

追記: その後、不要なRPC接続に対するPR #15432がWrangler 4.131.0に収録済みと確認し更新した。更新内容・再検証・残る制限は [Wrangler更新記録](./wrangler-upgrade-20261004.md) を参照。

本番デプロイ、コミット、DBデータ更新、netkeiba直接通信は行っていない。ローカルのコード・プロセスのみを変更した。
