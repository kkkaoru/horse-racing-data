# ローカルから本番を確認する

入口はリポジトリルートの **`bun run prod`** です。Wrangler と cloudflared の既存認証を利用します。1Password、独自の資格情報ストア、確認専用Worker、新しいAccessポリシーは不要です。

## 初回だけ

1. Bun、Wrangler（リポジトリの依存関係）、`cloudflared` を用意します。macOSでcloudflaredが未導入なら `brew install cloudflared`。
2. ルートの既存 `.env` に **秘密ではない接続先だけ** を追加します。既存ファイル全体を上書きしないでください。

   ```dotenv
   PC_KEIBA_VIEWER_ORIGIN="https://your-viewer.example"
   ```

   `bun run` が `.env` を読み込みます。直接 `bash scripts/prod.sh` を呼ぶ場合は環境変数をexportしてください。ホスト名・トークンはGitへ追加しません。

3. Cloudflare APIの認証を確認します。

   ```sh
   bun run prod status
   # 未認証の場合のみ（既存のログインを再利用）
   bunx wrangler login
   ```

   複数アカウントを使う場合は既存の `.env` の `CLOUDFLARE_ACCOUNT_ID` で対象を指定します。静的APIトークンを新規発行する必要はありません。

4. WebページのAccess認証を行います。

   ```sh
   bun run prod login
   ```

   通常のブラウザーで既存のメール認証等を完了します。JWTはcloudflaredの標準キャッシュで管理し、`--quiet` で端末には出力しません。資格情報を `.env` にコピーする必要はありません。有効期限が切れたら同じコマンドを実行します。

**Wrangler／Cloudflare MCPの管理API認証と、WebサイトのCloudflare Access認証は別です。** APIを読めてもブラウザーのログイン済み状態にはなりません。Accessを無効化したりBypassに変更する構成ではありません。

## 日常の確認

```sh
# 本番HTMLまたはAPIをGET（ログイン済みのcloudflared認証を使用）
bun run prod get /races/2026/09/12/83/01 > /tmp/production-race.html

# 通常のブラウザーで開くURLを表示
bun run prod url /races/2026/09/12/83/01

# 既存viewer設定のDETAIL_SECTION_CACHE_KVを本番から読む
bun run prod kv pred:fp:v1:20260912:83:01

# R2のbucket/keyを指定してローカルへダウンロード
bun run prod r2 your-feature-bucket/path/to/features.parquet /tmp/features.parquet
```

- KVは `apps/pc-keiba-viewer/wrangler.jsonc` の既存bindingを使います。namespace IDの二重管理はしません。
- KV/R2には常に `--remote` を付けます。ローカルエミュレーターを本番と誤認しません。
- R2は既存ファイル・シンボリックリンクを上書きしません。
- `get` は対象originのGETだけです。任意のcurlオプション、別ホストのURL、リダイレクト追従は受け付けません。認証情報をリダイレクト先へ送らないためです。正規URLに301/302で転送される場合は、そのサイト内の正規パスを指定し直してください。
- 書き込み、デプロイ、予測再生成のコマンドは提供しません。
- HTML取得だけではヒートマップの描画確認は完了しません。実画面は通常のログイン済みブラウザーで確認します。別の自動ブラウザーへ認証が自動共有されるわけではありません。

## エージェントからCloudflare MCPを使う場合

設定済みのCloudflare API MCPも引き続き利用できます。別の資格情報管理システムは追加しません。

```sh
executor tools search 'search' --namespace cloudflare-api --limit 3
executor tools describe cloudflare-api.user.default.search
```

実際に登録されている接続パスを確認し、CloudflareスキルとAPIスキーマを読んでから実行してください。管理API操作の認可を、そのまま保護ページの認証や描画確認と扱わないでください。

## 検証

```sh
shellcheck scripts/prod.sh
cd scripts
uv run pytest
uv run ruff check tests/test_prod_script.py
uv run ruff format --check tests/test_prod_script.py
uv run basedpyright
uv run ty check
```

テストは偽のBun/cloudflaredと一時ディレクトリを使います。本番や実際の認証キャッシュにはアクセスしません。
