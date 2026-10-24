// Run with:
//   CLOUDFLARE_API_TOKEN=... bun run share:race-page share <race page URL> <expiry ISO with offset>
//   CLOUDFLARE_API_TOKEN=... bun run share:race-page revoke <race page URL>
//   CLOUDFLARE_API_TOKEN=... bun run share:race-page prune
// The token needs Access: Apps and Policies Edit and Workers KV Storage Edit.

import { buildSharedPageKey } from "../lib/shared-page-access";
import {
  buildShareAppName,
  buildShareAssetDestinations,
  buildShareDestinations,
  buildShareEntryTtlSeconds,
  parseShareAppName,
  parseSharedPagePath,
  parseShareExpiry,
  SHARE_ASSETS_APP_NAME,
} from "../lib/shared-page-share";

const ACCOUNT_ID = "78109ec18c7c85b194b19fb32e3bb149";
// MCP_OAUTH_KV in wrangler.jsonc; the Worker reads share entries from it.
const SHARE_KV_NAMESPACE_ID = "295a119fc4ab43e0ae861aea77641033";
const ACCESS_APPS_PATH = `/accounts/${ACCOUNT_ID}/access/apps`;

interface AccessApp {
  id: string;
  name: string;
}

interface CloudflareResponseEnvelope {
  errors?: unknown[];
  result: unknown;
  success: boolean;
}

const getApiToken = (): string => {
  const token = process.env.CLOUDFLARE_API_TOKEN?.trim();
  if (!token) throw new Error("CLOUDFLARE_API_TOKEN is required.");
  return token;
};

const cloudflareRequest = async (
  path: string,
  init?: RequestInit,
): Promise<CloudflareResponseEnvelope> => {
  const headers = new Headers(init?.headers);
  headers.set("Authorization", `Bearer ${getApiToken()}`);
  const response = await fetch(`https://api.cloudflare.com/client/v4${path}`, { ...init, headers });
  const payload: CloudflareResponseEnvelope = await response.json();
  if (!payload.success) {
    throw new Error(`Cloudflare API ${path} failed: ${JSON.stringify(payload.errors)}`);
  }
  return payload;
};

const isAccessApp = (value: unknown): value is AccessApp =>
  typeof value === "object" &&
  value !== null &&
  "id" in value &&
  typeof value.id === "string" &&
  "name" in value &&
  typeof value.name === "string";

const listAccessApps = async (): Promise<AccessApp[]> => {
  const { result } = await cloudflareRequest(`${ACCESS_APPS_PATH}?per_page=1000`);
  return Array.isArray(result) ? result.filter(isAccessApp) : [];
};

const createBypassApp = (name: string, destinations: readonly string[]) =>
  cloudflareRequest(ACCESS_APPS_PATH, {
    body: JSON.stringify({
      app_launcher_visible: false,
      destinations: destinations.map((uri) => ({ type: "public", uri })),
      domain: destinations[0],
      name,
      policies: [{ decision: "bypass", include: [{ everyone: {} }], name: "Bypass shared page" }],
      type: "self_hosted",
    }),
    headers: { "content-type": "application/json" },
    method: "POST",
  });

const deleteAccessApp = async (app: AccessApp): Promise<void> => {
  await cloudflareRequest(`${ACCESS_APPS_PATH}/${app.id}`, { method: "DELETE" });
  console.log(`Deleted Access app: ${app.name}`);
};

const kvKeyPath = (key: string): string =>
  `/accounts/${ACCOUNT_ID}/storage/kv/namespaces/${SHARE_KV_NAMESPACE_ID}/values/${encodeURIComponent(key)}`;

// Removes the shared assets app once no share app is left.
const deleteAssetsAppWhenUnused = async (): Promise<void> => {
  const apps = await listAccessApps();
  if (apps.some((app) => parseShareAppName(app.name) !== null)) return;
  await Promise.all(
    apps.filter((app) => app.name === SHARE_ASSETS_APP_NAME).map((app) => deleteAccessApp(app)),
  );
};

const share = async (pageUrl: string, expiry: string): Promise<void> => {
  const nowMs = Date.now();
  const pagePath = parseSharedPagePath(pageUrl);
  const expiresAt = parseShareExpiry(expiry, nowMs);
  // The Worker refuses the page until this entry exists, so write it first.
  await cloudflareRequest(
    `${kvKeyPath(buildSharedPageKey(pagePath))}?expiration_ttl=${buildShareEntryTtlSeconds(expiresAt, nowMs)}`,
    { body: JSON.stringify({ expiresAt }), method: "PUT" },
  );
  const apps = await listAccessApps();
  await Promise.all(
    apps
      .filter((app) => parseShareAppName(app.name)?.pagePath === pagePath)
      .map((app) => deleteAccessApp(app)),
  );
  if (!apps.some((app) => app.name === SHARE_ASSETS_APP_NAME)) {
    await createBypassApp(SHARE_ASSETS_APP_NAME, buildShareAssetDestinations());
  }
  await createBypassApp(
    buildShareAppName({ expiresAt, pagePath }),
    buildShareDestinations(pagePath),
  );
  console.log(`Shared ${pageUrl} until ${expiresAt}`);
};

const revoke = async (pageUrl: string): Promise<void> => {
  const pagePath = parseSharedPagePath(pageUrl);
  const apps = await listAccessApps();
  await Promise.all(
    apps
      .filter((app) => parseShareAppName(app.name)?.pagePath === pagePath)
      .map((app) => deleteAccessApp(app)),
  );
  await cloudflareRequest(kvKeyPath(buildSharedPageKey(pagePath)), { method: "DELETE" });
  await deleteAssetsAppWhenUnused();
  console.log(`Revoked ${pageUrl}`);
};

// Expired shares are already refused by the Worker; this removes their Access
// apps so the clef bets path, which has no Worker gate, closes too.
const prune = async (): Promise<void> => {
  const nowMs = Date.now();
  const apps = await listAccessApps();
  await Promise.all(
    apps
      .filter((app) => {
        const parsed = parseShareAppName(app.name);
        return parsed !== null && Date.parse(parsed.expiresAt) <= nowMs;
      })
      .map((app) => deleteAccessApp(app)),
  );
  await deleteAssetsAppWhenUnused();
};

const main = async (): Promise<void> => {
  const [command, pageUrl, expiry] = process.argv.slice(2);
  if (command === "share" && pageUrl && expiry) return share(pageUrl, expiry);
  if (command === "revoke" && pageUrl) return revoke(pageUrl);
  if (command === "prune") return prune();
  throw new Error("Usage: share <race page URL> <expiry> | revoke <race page URL> | prune");
};

await main();
