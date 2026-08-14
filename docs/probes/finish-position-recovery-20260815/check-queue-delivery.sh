#!/usr/bin/env bash
set -euo pipefail

queue_id=${FINISH_POSITION_QUEUE_ID:-3f98959acc68467484117e0f3b8d4a24}
account_id=${CLOUDFLARE_ACCOUNT_ID:-78109ec18c7c85b194b19fb32e3bb149}

# Refresh the local Wrangler OAuth token without printing account details.
bunx wrangler whoami >/dev/null

QUEUE_ID="$queue_id" ACCOUNT_ID="$account_id" bun - <<'BUN'
const configPath = `${process.env.HOME}/.wrangler/config/default.toml`;
const config = Bun.TOML.parse(await Bun.file(configPath).text());
const token = config.oauth_token;
if (typeof token !== "string" || token.length === 0) {
  throw new Error(`Wrangler OAuth token not found in ${configPath}`);
}
const response = await fetch(
  `https://api.cloudflare.com/client/v4/accounts/${process.env.ACCOUNT_ID}/queues/${process.env.QUEUE_ID}`,
  { headers: { Authorization: `Bearer ${token}`, "User-Agent": "horse-racing-queue-readiness/20260815" } },
);
const payload = await response.json();
if (!response.ok || payload.success !== true) {
  console.error(`queue delivery check failed: HTTP ${response.status}`);
  process.exit(3);
}
const paused = payload.result?.settings?.delivery_paused;
if (paused === false) {
  console.log(`ACTIVE queue=${payload.result.queue_name} modified_on=${payload.result.modified_on}`);
  process.exit(0);
}
if (paused === true) {
  console.error(`PAUSED queue=${payload.result.queue_name} modified_on=${payload.result.modified_on}`);
  process.exit(2);
}
console.error("queue delivery check failed: delivery_paused missing from API response");
process.exit(3);
BUN
