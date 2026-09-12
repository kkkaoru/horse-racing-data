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

## Compact heatmap for MCP clients

Prefer `get_win_rate_heatmap_compact` for analysis. It returns all three rates
as numeric percentages, preserving null (unavailable) separately from zero.
Each row contains only `horseNumber`, `horseName`, and `heatmap`; each
`heatmap[columnKey]` contains `name`, `starts`, `winRate`, `quinellaRate`, and
`showRate`. All column keys from the shared row builder are retained, including
columns that the UI may hide. Precomputed `horseRateStats` are passed through
as on the site. The site's rendering is unchanged. The existing
`get_win_rate_heatmap_display` tool now also defaults to compact, one-horse
pages. Explicit `viewMode` or `showStarts` requests the legacy large display
model; omit both for normal analysis. Horse selection and pagination apply only
to compact output.

Example: retrieve the first horse in 2026-09-12 Hanshin 4R:

```json
{
  "year": "2026",
  "month": "09",
  "day": "12",
  "keibajoCode": "09",
  "raceNumber": "04",
  "source": "jra",
  "offset": 0,
  "limit": 1
}
```

Repeat with the returned `nextOffset` until it is null. `total` counts selected
horses before row pagination. An offset beyond the final horse returns an empty
terminal page. Omit `limit` for one horse per page; explicitly set `limit: 99`
to request all selected horses. `horseNumbers` is an
optional nonempty array such as `["1", "02", "18"]`; omitted means all runners.
Leading zeros and duplicates are normalized. Unknown horse numbers are errors.
Rows remain in horse-number order, with filtering applied before offset/limit
and after the shared statistics are built.

Large responses still use the server's existing JSON-text envelope. If
`encoding` is `json-text`, repeat exactly the same tool and arguments, adding
`responseCursor` equal to `nextResponseCursor`. Concatenate every `dataChunk`
before parsing the JSON; stop when `complete` is true. The cursor counts Unicode
characters, not bytes. `limit: 1` usually avoids this extra step, but clients must
still handle unusually long names. This transport envelope may exceed 5,000
characters because JSON escaping and envelope fields are additional to the
5,000-character data chunk.

These are live reads, not a snapshot token: if the race data changes during
pagination, restart from offset/cursor zero. Clients requiring an immutable
snapshot should collect and store the upstream API payload once.

Deploy the updated Worker before refreshing/reconnecting the client's MCP tool
definitions. Confirm `get_win_rate_heatmap_compact` and its `horseNumbers`,
`offset`, `limit`, and `responseCursor` arguments appear. If a client still shows
the older display-only schema without `responseCursor`, it cannot continue a
chunked response even when the current server implementation supports it.

Validated locally against saved production API data for 2026-09-12 Hanshin 4R
(JRA, venue 09, race 04): 18 horses and 252 cells matched the shared row builder.
The serialized display/all response was 209,784 Unicode characters; compact was
23,631 (88.7% smaller). Single-horse pages were 1,314–1,395 characters, and all
18 pages exactly reconstructed the full compact rows. This verifies saved-data
processing, not deployment or refreshed-client connectivity.
