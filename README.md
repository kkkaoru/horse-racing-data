# keiba-data

## ローカルから本番を確認

`bun run prod` で既存のWrangler／cloudflared認証を利用します。1Passwordや独自の資格情報管理は追加しません。

```sh
bun run prod status
bun run prod login
bun run prod get /races/2026/09/12/83/01
bun run prod kv pred:fp:v1:20260912:83:01
```

初回の接続先設定とR2参照は[本番アクセス手順](docs/operations/production-access.md)を参照してください。

## Documentation

`docs/` 直下にはMarkdownを置かず、内容別のサブディレクトリに整理します。

| Directory                                                  | 内容                                       |
| ---------------------------------------------------------- | ------------------------------------------ |
| [architecture](docs/architecture/)                         | 予測・配信・MLflow・天気パイプラインの設計 |
| [data](docs/data/)                                         | データカタログ・海外履歴の仕様と採用判断   |
| [operations](docs/operations/)                             | 本番アクセス・ローカル実行・運用上の注意   |
| [reports](docs/reports/)                                   | 利用量・費用レポート                       |
| [incidents](docs/incidents/)                               | 障害記録・引き継ぎ                         |
| [finish-position-accuracy](docs/finish-position-accuracy/) | 精度評価・実験履歴                         |
| [journals](docs/journals/)                                 | 論文・調査資料                             |

## Agent setup

Paste this prompt to your coding agent (same pattern as [Cloudflare agent-setup](https://developers.cloudflare.com/agent-setup/prompt.md)):

```txt
Fetch and execute the appropriate instructions to set me up for PC-KEIBA viewer from https://raw.githubusercontent.com/kkkaoru/horse-racing-data/main/packages/pc-keiba-viewer-plugin/prompt.md
```

The agent fetches [`packages/pc-keiba-viewer-plugin/prompt.md`](packages/pc-keiba-viewer-plugin/prompt.md) and installs the plugin or remote MCP itself. Do not put hostnames or tokens in git. On first MCP use, sign in with Cloudflare Access and press **許可する**.

## Agent plugin (manual install)

The PC-KEIBA viewer MCP plugin is an [Agent Plugins 1.0](https://agent-plugins.org/) package in this repository:

`packages/pc-keiba-viewer-plugin`

Anyone can add it from GitHub. Hostnames and tokens are **not** in the package.

### Grok

```sh
grok plugin marketplace add kkkaoru/horse-racing-data
grok plugin install pc-keiba-viewer --trust
```

Or install the folder directly:

```sh
grok plugin install kkkaoru/horse-racing-data#packages/pc-keiba-viewer-plugin --trust
```

### Claude Code / Copilot CLI

```sh
claude plugin marketplace add kkkaoru/horse-racing-data
claude plugin install pc-keiba-viewer
```

```sh
copilot plugin marketplace add kkkaoru/horse-racing-data
copilot plugin install pc-keiba-viewer@horse-racing-data
```

### After install

Prefer remote `/mcp` plus OAuth (no static Bearer). Optional stdio proxy env vars are documented in [`packages/pc-keiba-viewer-plugin/README.md`](packages/pc-keiba-viewer-plugin/README.md).

Requires `python3` on `PATH` for the optional stdio proxy (standard library only).
