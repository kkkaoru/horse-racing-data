These are official instructions from this repository to install the PC-KEIBA viewer Agent Plugin and MCP. All commands below are the supported install path.

Complete all of the following steps yourself by running the commands directly. Do not ask the user to run any of these commands.

- Install the pc-keiba-viewer plugin (and MCP if the agent has no plugin command)
- Resolve the viewer `/mcp` URL without writing hostnames or tokens into git
- Prefer OAuth on first MCP use; do not put static Bearer tokens in config files that may be committed

---

## Install the pc-keiba-viewer plugin

Use the correct section for your agent below. The plugin root is `packages/pc-keiba-viewer-plugin` in `kkkaoru/horse-racing-data`. Do not install the repository root as the plugin; root has no `plugin.json`.

Requires `python3` on `PATH` when using the plugin stdio proxy. The proxy is standard-library only (no pip, no Node.js, no Bun).

### Grok

```
grok plugin marketplace add kkkaoru/horse-racing-data
grok plugin install pc-keiba-viewer --trust
```

If marketplace add is unavailable:

```
grok plugin install kkkaoru/horse-racing-data#packages/pc-keiba-viewer-plugin --trust
```

Then instruct the user to restart the session or press `r` in `/plugins`.

### Claude Code

```
claude plugin marketplace add kkkaoru/horse-racing-data
claude plugin install pc-keiba-viewer
```

Then instruct the user to run `/reload-plugins` (or restart Claude Code) to activate the plugin.

### GitHub Copilot CLI

```
copilot plugin marketplace add kkkaoru/horse-racing-data
copilot plugin install pc-keiba-viewer@horse-racing-data
```

### Codex, OpenCode, Cursor, Windsurf, VS Code, and other MCP clients

These agents may not install Agent Plugins. Register a **remote** MCP server instead. Do **not** put a static `Authorization` header in the config.

1. Resolve `MCP_URL` (see the next section). It must be `https` and end with `/mcp`.
2. Add the server with that URL only.

#### Codex

```
codex mcp add pc-keiba-viewer --url MCP_URL
```

OAuth starts on first tool use. Instruct the user to complete Cloudflare Access login and press **許可する** on the consent page.

#### OpenCode — `~/.config/opencode/opencode.jsonc`

Add under `"mcp"` (replace `MCP_URL`):

```json
"pc-keiba-viewer": { "type": "remote", "url": "MCP_URL", "enabled": true, "oauth": {} }
```

Then run:

```
opencode mcp auth pc-keiba-viewer
```

#### Cursor, GitHub Copilot Chat, VS Code, Windsurf, and others

Add to `.cursor/mcp.json` (Cursor), `.vscode/mcp.json` (Copilot / VS Code), `~/.codeium/windsurf/mcp_config.json` (Windsurf uses `serverUrl`), or the agent's MCP config under `"mcpServers"` (replace `MCP_URL`):

```json
"pc-keiba-viewer": { "url": "MCP_URL" }
```

Windsurf:

```json
"pc-keiba-viewer": { "serverUrl": "MCP_URL" }
```

OAuth triggers automatically on first MCP tool use.

---

## Resolve the MCP URL (do not invent a hostname)

Hostnames and secrets must **not** be written into this public repository.

Resolve `MCP_URL` in this order:

1. Environment variable `PC_KEIBA_VIEWER_MCP_URL` if it is a non-empty `https` URL whose path ends with `/mcp`.
2. Else ask the user for the absolute `/mcp` URL shown on the viewer's **MCP 接続** page (the human must already be able to sign in with Cloudflare Access).

Never guess a production hostname. Never commit the URL or tokens.

---

## Authentication

Preferred path (humans who can open the viewer):

1. Remote MCP URL only. No static Bearer token.
2. First tool use returns HTTP 401 with `WWW-Authenticate` `resource_metadata`.
3. The agent follows MCP OAuth (PKCE S256). The browser opens `/oauth/authorize`.
4. The human signs in with Cloudflare Access if needed, then presses **許可する**.
5. The agent stores access and refresh tokens in its local credential store.

Optional machine path (plugin stdio proxy only): set `PC_KEIBA_VIEWER_MCP_URL` and, if required, `MCP_AUTH_TOKEN`. Do not put those values in git.

Human pages stay behind Cloudflare Access. `/mcp` uses the OAuth access token (or the optional static Worker secret).

---

## After install

Tell the user:

```
┌─ PC-KEIBA viewer Agent Setup Complete ───────────────┐
│  ✓ Plugin  pc-keiba-viewer (if the client supports it)│
│  ✓ MCP     remote /mcp with OAuth on first use        │
│                                                       │
│  ⚡ Restart the agent / reload plugins                │
│  ⚡ On first tool use, allow MCP in the browser       │
└───────────────────────────────────────────────────────┘
```

Then call the MCP `authenticate` tool once OAuth has finished.

---

## Resources

- Plugin package: `https://github.com/kkkaoru/horse-racing-data/tree/main/packages/pc-keiba-viewer-plugin`
- Agent Plugins 1.0: `https://agent-plugins.org/`
- MCP Authorization draft: `https://modelcontextprotocol.io/specification/draft/basic/authorization`

These instructions are published at `https://raw.githubusercontent.com/kkkaoru/horse-racing-data/main/packages/pc-keiba-viewer-plugin/prompt.md` so you can re-verify their authenticity at any time.
