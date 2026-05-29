// Stale-while-revalidate cache for the home-page `getTopRaceWindows`
// query. Neon is allowed to auto-suspend (cost optimization), so we
// never schedule warming requests against it. Instead the page always
// renders from cache and the next user transparently drives the wake-up.
//
//   1. Fresh tier (5 min): Cache API per-colo + KV cross-colo. Any
//      request inside the 5 min window hits cache and never touches DB.
//   2. Stale tier (24h, KV only): served immediately when the fresh tier
//      is empty (post-deploy, post-eviction, post-Neon-suspend), with a
//      background refresh queued via `waitUntil`. The user sees a fast
//      response; the cold Neon wake happens off the request path.
//
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import "server-only";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import type { TopRaceSummary } from "./race-types";

export interface TopRaceWindowsPayload {
  finished: TopRaceSummary[];
  upcoming: TopRaceSummary[];
}

const FRESH_CACHE_KEY = "pc-keiba-viewer:top-races:fresh:v1";
const STALE_CACHE_KEY = "pc-keiba-viewer:top-races:stale:v1";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/top-races-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";
const FRESH_TTL_SECONDS = 5 * 60;
const STALE_TTL_SECONDS = 24 * 60 * 60;

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getCacheRequest = (cacheKey: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isTopRaceWindowsPayload = (value: unknown): value is TopRaceWindowsPayload =>
  isRecord(value) && Array.isArray(value.finished) && Array.isArray(value.upcoming);

const parsePayload = (body: string): TopRaceWindowsPayload | null => {
  try {
    const parsed: unknown = JSON.parse(body);
    return isTopRaceWindowsPayload(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

const promoteKvBodyToCacheApi = async (body: string): Promise<void> => {
  const cache = getDefaultCache();
  if (!cache) {
    return;
  }
  await cache.put(
    getCacheRequest(FRESH_CACHE_KEY),
    new Response(body, {
      headers: {
        "Cache-Control": `public, max-age=${FRESH_TTL_SECONDS}`,
        "Content-Type": DEFAULT_CONTENT_TYPE,
      },
    }),
  );
};

export const getCachedTopRaceWindows = async (): Promise<TopRaceWindowsPayload | null> => {
  const cachedResponse = await getDefaultCache()
    ?.match(getCacheRequest(FRESH_CACHE_KEY))
    .catch(() => undefined);
  if (cachedResponse?.ok) {
    return parsePayload(await cachedResponse.text());
  }
  const { env, ctx } = await safeGetCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(FRESH_CACHE_KEY).catch(() => null);
  if (!kvBody) {
    return null;
  }
  ctx?.waitUntil(promoteKvBodyToCacheApi(kvBody).catch(() => undefined));
  return parsePayload(kvBody);
};

export const putTopRaceWindowsCache = async (payload: TopRaceWindowsPayload): Promise<void> => {
  const body = JSON.stringify(payload);
  const { env } = await safeGetCloudflareRuntime();
  await Promise.all([
    getDefaultCache()
      ?.put(
        getCacheRequest(FRESH_CACHE_KEY),
        new Response(body, {
          headers: {
            "Cache-Control": `public, max-age=${FRESH_TTL_SECONDS}`,
            "Content-Type": DEFAULT_CONTENT_TYPE,
          },
        }),
      )
      .catch(() => undefined),
    env?.DETAIL_SECTION_CACHE_KV?.put(FRESH_CACHE_KEY, body, {
      expirationTtl: FRESH_TTL_SECONDS,
    }).catch(() => undefined),
    env?.DETAIL_SECTION_CACHE_KV?.put(STALE_CACHE_KEY, body, {
      expirationTtl: STALE_TTL_SECONDS,
    }).catch(() => undefined),
  ]);
};

export const getStaleTopRaceWindowsSnapshot = async (): Promise<TopRaceWindowsPayload | null> => {
  const { env } = await safeGetCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(STALE_CACHE_KEY).catch(() => null);
  return kvBody ? parsePayload(kvBody) : null;
};

interface SwrResult {
  payload: TopRaceWindowsPayload | null;
  source: "fresh" | "stale" | "miss";
}

// Stale-while-revalidate read. On a stale hit, schedules `refresh` on the
// Cloudflare execution context so the user doesn't wait for the Neon
// wake-up. Returns `source: "miss"` when neither tier has data — callers
// then run `refresh` synchronously.
export const readTopRaceWindowsWithSwr = async (
  refresh: () => Promise<TopRaceWindowsPayload>,
): Promise<SwrResult> => {
  const fresh = await getCachedTopRaceWindows();
  if (fresh) {
    return { payload: fresh, source: "fresh" };
  }
  const stale = await getStaleTopRaceWindowsSnapshot();
  if (!stale) {
    return { payload: null, source: "miss" };
  }
  const { ctx } = await safeGetCloudflareRuntime();
  ctx?.waitUntil(
    refresh().catch((error: unknown) => {
      console.error("background top-race-windows refresh failed", error);
    }),
  );
  return { payload: stale, source: "stale" };
};
