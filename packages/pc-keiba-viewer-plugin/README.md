# pc-keiba-viewer Agent Plugin

## Agent setup

Paste this prompt to your coding agent (same pattern as [Cloudflare agent-setup](https://developers.cloudflare.com/agent-setup/prompt.md)):

```txt
Fetch and execute the appropriate instructions to set me up for PC-KEIBA viewer from https://raw.githubusercontent.com/kkkaoru/horse-racing-data/main/packages/pc-keiba-viewer-plugin/prompt.md
```

The agent must fetch [`prompt.md`](prompt.md) and run the install commands itself.

Portable [Agent Plugins 1.0.0](https://agent-plugins.org/) package for the PC-KEIBA viewer MCP server and skill.

This directory is the plugin root (`plugin.json`, `mcp.json`, `skills/`). Compatible clients discover it as one folder.

Hostnames and credentials are **not** in this package. Agent Plugins 1.0.0 forbids secrets in `mcp.json` headers.

Humans who can sign in to the viewer with Cloudflare Access should add a **remote** MCP URL ending in `/mcp` (no static Authorization header). On first use the agent opens the site consent page; after **許可する**, it stores the OAuth tokens itself.

The stdio proxy below is an optional machine path that still reads env credentials.

## Layout

```text
packages/pc-keiba-viewer-plugin/
├── plugin.json
├── mcp.json
├── prompt.md
├── skills/pc-keiba-viewer-mcp/SKILL.md
└── src/run_mcp_stdio_proxy.py
```

## Human OAuth (recommended)

1. Add the viewer's `/mcp` URL as a remote MCP server. Do not put a static Bearer token in the client config.
2. Open the viewer's **MCP 接続** page while signed in with Cloudflare Access, or wait for the agent to open the consent page on first tool use.
3. Press **許可する**. The agent stores the access and refresh tokens locally.

## Environment (optional stdio proxy)

Set these in the agent client's environment only if you use the plugin's stdio proxy instead of remote OAuth:

| Variable                        | Purpose                                                                           |
| ------------------------------- | --------------------------------------------------------------------------------- |
| `PC_KEIBA_VIEWER_MCP_URL`       | Absolute `https` URL whose path ends with `/mcp`                                  |
| `MCP_AUTH_TOKEN`                | Bearer token the Worker secret `MCP_AUTH_TOKEN` accepts                           |
| `PC_KEIBA_ACCESS_CLIENT_ID`     | Cloudflare Access service token Client ID (optional if `/mcp` is Access-bypassed) |
| `PC_KEIBA_ACCESS_CLIENT_SECRET` | Cloudflare Access service token Client Secret                                     |

## Install from GitHub

`plugin.json` is not at the repository root. Clients must load the subdirectory
`packages/pc-keiba-viewer-plugin`, or add this repo as a Grok marketplace.

### Grok

```sh
grok plugin marketplace add kkkaoru/horse-racing-data
grok plugin install pc-keiba-viewer --trust
```

Direct install of the plugin folder:

```sh
grok plugin install kkkaoru/horse-racing-data#packages/pc-keiba-viewer-plugin --trust
```

A full git URL also works:

```sh
grok plugin install https://github.com/kkkaoru/horse-racing-data.git#packages/pc-keiba-viewer-plugin --trust
```

Then set the environment variables above in the Grok process (shell env, or
headers/env in the client's MCP config). Restart the session or press `r` in
`/plugins`.

### Claude Code / Copilot CLI

The repository root is a marketplace (`.claude-plugin/marketplace.json`):

```sh
claude plugin marketplace add kkkaoru/horse-racing-data
claude plugin install pc-keiba-viewer
```

```sh
copilot plugin marketplace add kkkaoru/horse-racing-data
copilot plugin install pc-keiba-viewer@horse-racing-data
```

### Other Agent Plugins clients

Clone the repo (or sparse-checkout the plugin directory) and point the client
at the plugin root — the folder that contains `plugin.json`:

```sh
git clone https://github.com/kkkaoru/horse-racing-data.git
# plugin root:
#   horse-racing-data/packages/pc-keiba-viewer-plugin
```

VS Code: Command Palette → **Chat: Install Plugin From Source** → paste the
GitHub URL, then if the client asks for a subdirectory, use
`packages/pc-keiba-viewer-plugin`.

Do not install the repository root as the plugin. Root has no `plugin.json`.

Requires `python3` on `PATH` (`mcp.json` `command` is `python3`; the proxy uses the standard library only).

After install, the skill `/pc-keiba-viewer-mcp` and MCP tools such as
`authenticate` and `get_win_rate_heatmap_display` should appear.
