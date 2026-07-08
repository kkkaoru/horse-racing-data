# mlflow-ui-proxy

Cloudflare Worker that puts an authenticated reverse proxy in front of a
local MLflow tracking server. MLflow's own server has zero authentication
built in, so this Worker is the only auth gate: every request requires valid
HTTP Basic auth before any proxying happens, with no unauthenticated path
except the 401 challenge response itself. On success, the request (any
method, path, query string, and body) is forwarded unchanged to
`env.MLFLOW_ORIGIN`.

## Required environment

| Name                 | Kind   | Purpose                                                     |
| -------------------- | ------ | ----------------------------------------------------------- |
| `MLFLOW_ORIGIN`      | var    | Base URL of the MLflow origin (Cloudflare Tunnel hostname). |
| `MLFLOW_UI_USERNAME` | secret | Basic-auth username required to reach MLflow.               |
| `MLFLOW_UI_PASSWORD` | secret | Basic-auth password required to reach MLflow.               |

`MLFLOW_ORIGIN` is set in `wrangler.jsonc` under `vars` (currently a
placeholder value — replace it with the real tunnel hostname once the tunnel
exists). The credentials are secrets and must never be committed; set them
with:

```sh
wrangler secret put MLFLOW_UI_USERNAME
wrangler secret put MLFLOW_UI_PASSWORD
```

## Tunnel dependency

`MLFLOW_ORIGIN` must point at a Cloudflare Tunnel hostname that fronts the
Mac's local `mlflow server` process listening on `127.0.0.1:5252`. This
package does not create, configure, or manage that tunnel — it only treats
`MLFLOW_ORIGIN` as an opaque upstream base URL.

### Current deployment (Mac-side tunnel)

- **Tunnel name:** `mlflow-ui` (Cloudflare Tunnel, account
  `78109ec18c7c85b194b19fb32e3bb149`, zone `kkk4oru.com`).
- **Public hostname:** the value currently set in `wrangler.jsonc` under
  `vars.MLFLOW_ORIGIN` (deliberately non-obvious, does not contain
  `mlflow`, chosen with a random hex suffix — do not repeat this string in
  commit messages or issues).
- **Config file:** `~/.cloudflared/config.yml` on the Mac running
  `mlflow server`. Ingress routes the tunnel hostname to
  `http://127.0.0.1:5252`, with `originRequest.httpHostHeader:
127.0.0.1:5252` — this is required because MLflow's server rejects
  requests whose `Host` header doesn't match an allowed value (DNS
  rebinding protection), so cloudflared must present a trusted Host to the
  origin even though the public-facing SNI/Host is the tunnel hostname.
  Credentials file: `~/.cloudflared/<tunnel-uuid>.json`.
- **Process supervision:** a LaunchAgent at
  `~/Library/LaunchAgents/com.horse-racing.cloudflared-mlflow-ui.plist`
  runs `cloudflared tunnel run mlflow-ui` with `KeepAlive`/`RunAtLoad`, so
  it restarts on crash and on login. Logs:
  `~/Library/Logs/cloudflared-mlflow-ui.log`.
  - Restart after a config change: `launchctl kickstart -k
gui/$(id -u)/com.horse-racing.cloudflared-mlflow-ui`
  - Status: `launchctl print gui/$(id -u)/com.horse-racing.cloudflared-mlflow-ui`
  - Stop: `launchctl bootout gui/$(id -u)/com.horse-racing.cloudflared-mlflow-ui`

### Redeploying the Worker after a tunnel/hostname change

1. Update `vars.MLFLOW_ORIGIN` in `wrangler.jsonc` to the new tunnel
   hostname.
2. `bun run --filter mlflow-ui-proxy check` (tsc, lint, format:check,
   coverage — must all stay green, thresholds 95%).
3. `bun run --filter mlflow-ui-proxy deploy` (wraps `wrangler deploy`).

## Local dev / test

```sh
bun run --filter mlflow-ui-proxy check
```

This runs `tsc --noEmit`, `oxlint`, `oxfmt --check`, and `vitest run
--coverage` (thresholds: lines/branches/functions/statements all >= 95%).

## Troubleshooting

- **`GET`/`POST /ajax-api/2.0/mlflow/users/current` returns 404.** This is
  MLflow's basic-auth app endpoint, which only exists when MLflow itself is
  started with `--app-name basic-auth`. This deployment runs a plain MLflow
  server (this Worker is the only auth gate — see above), so the endpoint
  genuinely doesn't exist upstream. The UI console calls it anyway and logs
  the 404; it's expected noise with no functional effect. The proxy forwards
  it faithfully rather than stubbing a response, since it isn't a proxy bug.
- **`403 Cross-origin request blocked` on write requests
  (`traces/search`, `runs/search`, etc.).** MLflow 3.x's server rejects
  mutating requests whose `Origin`/`Referer` header doesn't match its own
  host. This Worker strips both headers before forwarding upstream (see
  `BROWSER_CONTEXT_HEADERS` in `src/proxy.ts`) so MLflow always sees a
  same-origin, non-browser request. If this reappears, check that the
  stripping logic wasn't reverted.

## Residual risk: tunnel hostname has no auth of its own

Cloudflare Tunnel's DNS hostname is not itself authenticated at the
tunnel/DNS layer. Anyone who discovers the tunnel hostname can reach the
local MLflow server directly, bypassing this Worker's Basic-auth gate
entirely, unless a separate control is placed in front of the tunnel
hostname. To mitigate:

- Keep the hostname non-obvious (avoid anything containing `mlflow`).
- If available on the account, additionally gate the tunnel hostname with
  Cloudflare Access / Zero Trust so requests must pass both Access and this
  Worker's Basic auth.
