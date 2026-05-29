// Run with bun. Gate 4: Cache API for /api/features/race-trend endpoint responses.

import type { Env } from "../types";

const EDGE_CACHE_BASE = "https://sync-realtime-data-features.kkk4oru.com";
const DEFAULT_EDGE_CACHE_TTL_SECONDS = 60;
const JSON_CONTENT_TYPE = "application/json";

const resolveEdgeCacheTtl = (env: Env): number => {
  const raw = env.FEATURES_EDGE_CACHE_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_EDGE_CACHE_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_EDGE_CACHE_TTL_SECONDS;
};

const buildEdgeCacheKey = (cacheKey: string): Request =>
  new Request(`${EDGE_CACHE_BASE}/api/features/race-trend?key=${encodeURIComponent(cacheKey)}`, {
    method: "GET",
  });

export const readRaceTrendFromEdgeCache = async (cacheKey: string): Promise<Response | null> => {
  const cached = await caches.default.match(buildEdgeCacheKey(cacheKey));
  return cached ?? null;
};

export const writeRaceTrendToEdgeCache = async (
  cacheKey: string,
  payload: unknown,
  env: Env,
): Promise<void> => {
  const ttl = resolveEdgeCacheTtl(env);
  const response = new Response(JSON.stringify(payload), {
    headers: {
      "Cache-Control": `public, max-age=${ttl}, s-maxage=${ttl}`,
      "Content-Type": JSON_CONTENT_TYPE,
    },
  });
  await caches.default.put(buildEdgeCacheKey(cacheKey), response);
};

export const purgeRaceTrendEdgeCache = async (cacheKey: string): Promise<void> => {
  await caches.default.delete(buildEdgeCacheKey(cacheKey));
};
