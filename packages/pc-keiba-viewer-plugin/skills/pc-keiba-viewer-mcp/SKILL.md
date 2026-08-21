---
name: pc-keiba-viewer-mcp
description: >
  Use the authenticated pc-keiba-viewer MCP server to authenticate to production
  viewer data, fetch race/horse JSON the site displays, and search horses,
  jockeys, owners, and trainers. Use when the user asks about production race
  cards, heatmaps, finish predictions, odds, trends, or to query PC-KEIBA viewer
  data through MCP. Slash command: /pc-keiba-viewer-mcp.
---

# PC-KEIBA viewer MCP

This skill is part of the Agent Plugins 1.0 package in `packages/pc-keiba-viewer-plugin`.

Human Cloudflare Access stays on the viewer pages and on `/oauth/authorize`.
People who can open the site can issue MCP credentials by allowing the agent
on the consent page (**MCP 接続** / `/oauth/authorize`). First launch stores
OAuth tokens in the agent. Discovery uses Protected Resource Metadata and
Authorization Server Metadata (PKCE S256, DCR and CIMD).

Preferred client setup: remote MCP URL ending in `/mcp`, no static
Authorization header. Optional machine path: stdio proxy with
`PC_KEIBA_VIEWER_MCP_URL` and `MCP_AUTH_TOKEN`.

Pages and browser APIs never read the MCP token.

Heatmap display is `buildWinRateHeatmapDisplay` — the same function the table
uses. Defaults match first paint (勝率, レース数 off).

## Tool order

1. `search_tool` query `pc-keiba-viewer`.
2. `authenticate` — MCP bearer succeeded; Worker can read `/api/spec`.
3. `search_entities` — horses, jockeys, owners, trainers.
4. `get_win_rate_heatmap_display` — same display builder as the table.
5. `get_json` for `/api/races/.../realtime?source=jra|nar` (odds) and `/trends`.
6. `get_race_section` / `list_top_races` — same GET handlers the browser uses.

Do not call cache-warm, internal, admin, or POST paddock endpoints.
