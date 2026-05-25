// Two-tier cache + stale fallback for the home-page `getTopRaceWindows`
// query. The query takes ~5-15s when Neon cold-starts, so we keep:
//
//   1. Fresh tier (60s): Cache API per-colo + KV cross-colo. Both written
//      together so the next request anywhere in the world stays warm.
//   2. Stale tier (24h): KV-only snapshot at a stable key. When Neon is
//      cold and the fresh DB query times out, the page renders this
//      instead of hanging.
//
// A cron (`*/5 * * * *`) writes both tiers every five minutes to keep
// Neon warm and the fresh tier populated.
//
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import type { TopRaceSummary } from "./race-types";

export interface TopRaceWindowsPayload {
  finished: TopRaceSummary[];
  upcoming: TopRaceSummary[];
}

const FRESH_CACHE_KEY = "pc-keiba-viewer:top-races:fresh:v1";
const STALE_CACHE_KEY = "pc-keiba-viewer:top-races:stale:v1";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/top-races-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";
const FRESH_TTL_SECONDS = 60;
const STALE_TTL_SECONDS = 24 * 60 * 60;

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const tryGetCloudflareRuntime = async (): Promise<{
  ctx: PcKeibaExecutionContext | null;
  env: CloudflareEnv | null;
}> => {
  try {
    const context = await getCloudflareContext<Record<string, unknown>, PcKeibaExecutionContext>({
      async: true,
    });
    return { ctx: context.ctx, env: context.env };
  } catch {
    return { ctx: null, env: null };
  }
};

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
  const { env, ctx } = await tryGetCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(FRESH_CACHE_KEY).catch(() => null);
  if (!kvBody) {
    return null;
  }
  ctx?.waitUntil(promoteKvBodyToCacheApi(kvBody).catch(() => undefined));
  return parsePayload(kvBody);
};

export const putTopRaceWindowsCache = async (payload: TopRaceWindowsPayload): Promise<void> => {
  const body = JSON.stringify(payload);
  const { env } = await tryGetCloudflareRuntime();
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
  const { env } = await tryGetCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(STALE_CACHE_KEY).catch(() => null);
  return kvBody ? parsePayload(kvBody) : null;
};
