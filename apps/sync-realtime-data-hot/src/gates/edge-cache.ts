import type { Env } from "../types";

const EDGE_CACHE_BASE = "https://sync-realtime-data-hot.kkk4oru.com";
const DEFAULT_EDGE_CACHE_TTL_SECONDS = 15;
const JSON_CONTENT_TYPE = "application/json";

const resolveEdgeCacheTtl = (env: Env): number => {
  const raw = env.ODDS_EDGE_CACHE_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_EDGE_CACHE_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_EDGE_CACHE_TTL_SECONDS;
};

const buildEdgeCacheKey = (raceKey: string): Request =>
  new Request(`${EDGE_CACHE_BASE}/api/odds/${encodeURIComponent(raceKey)}`, { method: "GET" });

export const isForceFreshRequest = (request: Request): boolean => {
  if (request.headers.get("X-Odds-Force-Fresh") === "1") {
    return true;
  }
  const url = new URL(request.url);
  return url.searchParams.get("fresh") === "1";
};

export const readFromEdgeCache = async (raceKey: string): Promise<Response | null> => {
  const cached = await caches.default.match(buildEdgeCacheKey(raceKey));
  return cached ?? null;
};

export const writeToEdgeCache = async (
  raceKey: string,
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
  await caches.default.put(buildEdgeCacheKey(raceKey), response);
};

export const purgeEdgeCache = async (raceKey: string): Promise<void> => {
  await caches.default.delete(buildEdgeCacheKey(raceKey));
};
