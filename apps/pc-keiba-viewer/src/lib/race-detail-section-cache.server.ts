import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import {
  DETAIL_SECTION_CACHE_AFTER_START_SECONDS,
  buildDetailSectionCacheKey,
  type DetailSectionCacheWarmMessage,
} from "./race-detail-section-cache";
import type { RaceDetail } from "./race-types";

const CACHE_CONTROL_HEADER = "public, max-age=%d";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/detail-section-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";

// Stale snapshots persist much longer than the fresh tier (which expires
// race start + 6h). Past races are immutable, and for upcoming races we
// only need stale to last long enough that the next visitor can serve it
// while the background refresh runs.
const STALE_CACHE_KEY_PREFIX = "stale";
const STALE_TTL_SECONDS = 30 * 24 * 60 * 60;

type CacheSource = "cache-api" | "kv";

const getCloudflareRuntime = async (): Promise<{
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

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getConfiguredAfterStartSeconds = (env: CloudflareEnv | null): number => {
  const parsed = Number(env?.PC_KEIBA_DETAIL_SECTION_CACHE_AFTER_START_SECONDS);
  return Number.isFinite(parsed) && parsed >= 60
    ? Math.floor(parsed)
    : DETAIL_SECTION_CACHE_AFTER_START_SECONDS;
};

const getRaceStartTimeMs = (race: RaceDetail): number | null => {
  const normalizedTime = race.hassoJikoku?.trim().padStart(4, "0");
  if (!normalizedTime || !/^\d{4}$/u.test(normalizedTime)) {
    return null;
  }
  const startTime = Date.parse(
    `${race.kaisaiNen}-${race.kaisaiTsukihi.slice(0, 2)}-${race.kaisaiTsukihi.slice(
      2,
      4,
    )}T${normalizedTime.slice(0, 2)}:${normalizedTime.slice(2, 4)}:00+09:00`,
  );
  return Number.isFinite(startTime) ? startTime : null;
};

const getRaceDayFallbackBaseTimeMs = (race: RaceDetail): number =>
  Date.parse(
    `${race.kaisaiNen}-${race.kaisaiTsukihi.slice(0, 2)}-${race.kaisaiTsukihi.slice(
      2,
      4,
    )}T23:59:59+09:00`,
  );

export const getDetailSectionCacheTtlSeconds = (
  race: RaceDetail,
  env: CloudflareEnv | null,
  nowMs = Date.now(),
): number => {
  const afterStartSeconds = getConfiguredAfterStartSeconds(env);
  const raceStartTime = getRaceStartTimeMs(race);
  const expiresAt =
    (raceStartTime ?? getRaceDayFallbackBaseTimeMs(race)) + afterStartSeconds * 1000;
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

const buildCachedResponse = (body: string, source: CacheSource): Response =>
  new Response(body, {
    headers: {
      "Cache-Control": "public, max-age=60",
      "Content-Type": DEFAULT_CONTENT_TYPE,
      "X-Detail-Section-Cache": `HIT-${source}`,
    },
  });

export const getCachedDetailSectionResponse = async (
  cacheKey: string,
): Promise<Response | null> => {
  const defaultCache = getDefaultCache();
  const cacheRequest = getCacheRequest(cacheKey);
  const cachedResponse = await defaultCache?.match(cacheRequest);
  if (cachedResponse?.ok) {
    return buildCachedResponse(await cachedResponse.text(), "cache-api");
  }

  const { env, ctx } = await getCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) {
    return null;
  }

  const putCache = async () => {
    await defaultCache?.put(
      cacheRequest,
      new Response(kvBody, {
        headers: {
          "Cache-Control": "public, max-age=60",
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    );
  };
  ctx?.waitUntil(putCache());
  return buildCachedResponse(kvBody, "kv");
};

const getStaleCacheKey = (cacheKey: string): string =>
  `${STALE_CACHE_KEY_PREFIX}:${cacheKey}`;

export const getStaleDetailSectionBody = async (cacheKey: string): Promise<string | null> => {
  const { env } = await getCloudflareRuntime();
  return (
    (await env?.DETAIL_SECTION_CACHE_KV?.get(getStaleCacheKey(cacheKey)).catch(() => null)) ?? null
  );
};

export const buildStaleDetailSectionResponse = (body: string): Response =>
  new Response(body, {
    headers: {
      "Cache-Control": "public, max-age=60",
      "Content-Type": DEFAULT_CONTENT_TYPE,
      "X-Detail-Section-Cache": "STALE-kv",
    },
  });

export const putDetailSectionCache = async ({
  body,
  cacheKey,
  race,
}: {
  body: string;
  cacheKey: string;
  race: RaceDetail;
}): Promise<void> => {
  const { env } = await getCloudflareRuntime();
  const ttlSeconds = getDetailSectionCacheTtlSeconds(race, env);
  const cacheControl = CACHE_CONTROL_HEADER.replace("%d", String(ttlSeconds));
  // The 30-day stale snapshot is written even when fresh TTL is already
  // 0 (the race finished more than 6h ago) so future visits still get an
  // instant render via the SWR path.
  const stalePut = env?.DETAIL_SECTION_CACHE_KV?.put(getStaleCacheKey(cacheKey), body, {
    expirationTtl: STALE_TTL_SECONDS,
  }).catch(() => undefined);
  if (ttlSeconds <= 0) {
    await stalePut;
    return;
  }
  await Promise.all([
    getDefaultCache()
      ?.put(
        getCacheRequest(cacheKey),
        new Response(body, {
          headers: {
            "Cache-Control": cacheControl,
            "Content-Type": DEFAULT_CONTENT_TYPE,
          },
        }),
      )
      .catch(() => undefined),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: ttlSeconds }).catch(
      () => undefined,
    ),
    stalePut,
  ]);
};

export const buildDetailSectionCacheKeyForMessage = (
  message: Omit<DetailSectionCacheWarmMessage, "source">,
): string => buildDetailSectionCacheKey(message);
