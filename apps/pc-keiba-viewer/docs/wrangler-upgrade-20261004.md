# 2026-10-04 Wrangler remote bindings 更新

## 変更

- 共有 `package.json` catalog の Wrangler を `4.100.0` → `4.131.0` に固定更新し、`bun.lock` を再生成。
- [PR #15432](https://github.com/cloudflare/workers-sdk/pull/15432) はfetch-only bindingにも不要なWebSocket RPC接続を生成していた問題を修正する。RPCメソッドの実呼び出しまで接続を遅延する。
- [Wrangler 4.131.0の正式リリース](https://github.com/cloudflare/workers-sdk/releases/tag/wrangler%404.131.0)に修正収録。OpenNextが解決するWranglerも4.131.0であることを確認。
- 同Wranglerの依存はMiniflare `5.20260910.0-alpha` / workerd `1.20260910.1`。インストール済みremote-proxy-clientの遅延生成実装を確認した。Miniflareのalpha表記はWrangler正式版が指定する依存であり、独自にoverrideしたものではない。
- テスト用の直接Miniflare `4.20260611.0` とvitest-pool-workersは維持。後者の内部Wrangler 4.100.0も残るが、OpenNextの開発接続には使われない。
- Workers typesは既存 `4.20260616.1` を維持。新Wranglerのoptional peer要求 `^5.20260910.1` は満たしていない。下記の型検査・実行検証は成功したが、全peer条件が整合した依存ツリーとは扱わない。型定義全体のメジャー更新は今回行っていない。
- 本番binding設定・coverageの閾値/計測対象・アプリケーションコードは今回変更していない。既存の作業中の変更は保持。

## 検証

証跡: `.cache/wrangler-upgrade-20261004/`。

- `install.log` / `frozen-install.log`: 通常installとfrozen lockfileの再現確認に成功。
- `check-status.tsv`: Wranglerを直接利用する13パッケージでtsc / lint / test:coverageの39コマンドすべて終了0。計14,769テスト成功。
- viewer coverage: statements 98.80%、branches 96.69%、functions 99.21%、lines 98.93%。
- `next-build.log`: Next.jsビルド成功。
- `opennext-build-full.log` / `pc-keiba-viewer-bundle.log`: OpenNextのフルビルドとWrangler dry-run成功。
- `bundle-status.tsv`: 他12 WorkersのWrangler dry-runすべて終了0。Containersは `--containers-rollout none` でイメージ構築・更新を対象外にした。全dry-runはアップロードなし。
- `probe-bindings-env.log`: 独立したWranglerプロキシでKV MISS、SSRキャッシュHIT（17,326文字）、D1 `SELECT 1` 成功。コースの診断リクエストは404のため成功扱いにしていない。
- `page-before.*`: 対象ページHTTP 200、3.212秒で全文取得。
- `realtime.*`: 実在するrealtime APIはHTTP 200、1.463秒。
- 8分のSSE観測が終了0で完了。`stream-1.*` はHTTP 200・241.057秒、再接続した `stream-2.*` はHTTP 200・240.621秒で正常終了。2回目も8頭分の馬体重を受信した。curlの新規接続による確認であり、実ブラウザの自動再接続テストではない。
- `page-after.*`: 観測後も対象ページはHTTP 200・2.583秒で全文取得。
- 再起動後から観測完了までの `dev-server.log` に `internal error; reference` / `Network connection lost` は0件。今回の有界観測では対象警告の改善を確認した。

### 検証途中の失敗と再実行

- 初回API確認は存在しない `/horse-weights` にアクセスしたため404。正しい `/realtime?source=jra` に修正して成功。
- 最初のOpenNext `--skipNextBuild` は通常のNextビルドにstandalone出力がなく失敗。OpenNextのフルビルドで成功。アプリの修正による回避はしていない。
- 最初の独立binding診断は環境ファイル未ロードでHyperdrive設定不足。既存 `.env` / `.env.local` を読み込んで再実行した。

## 運用・制限

- localhostは専用tmux `pc-keiba-viewer-dev-https` で維持。ソケットは `pi-tmux-7f9977cee7a4dbafd52bc25a4ffbe981`。今回のログは証跡ディレクトリの `dev-server.log`。
- 再起動直後にはブラウザ拡張の `Failed to fetch` と `ResponseAborted` が記録された。これらは対象のCloudflare `internal error; reference` / `Network connection lost` と区別する。全種類のエラーが解消したとは扱わない。
- 有界観測で再発しなくても、長時間のremote bindings劣化すべてが恒久解決したとは断定しない。上流も [#15421](https://github.com/cloudflare/workers-sdk/issues/15421) の接続劣化と今回のRPC修正を区別している。
- 本番デプロイ・コミット・DBデータ更新・netkeiba直接通信はしていない。
