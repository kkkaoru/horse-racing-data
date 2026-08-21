# keiba-data

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
