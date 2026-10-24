// Cache the heavy SSR fan-out (race + runners + courseInfo + sameVenueRaces)
// in KV so warm cache hits anywhere in the world skip the Hyperdrive round
// trips. Cache API gives a per-colo fast path; KV provides global coverage.
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import "server-only";
import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import { buildRaceDetailSsrCacheKey, type RaceDetailSsrCacheKeyParams } from "./race-cache-bust";
import { resolveRaceCacheKey } from "./race-cache-bust.server";
import {
  DETAIL_SECTION_CACHE_AFTER_START_SECONDS,
  getLegacyCacheKey,
} from "./race-detail-section-cache";
import type { CourseInfo, RaceDetail, RaceListItem, Runner } from "./race-types";

export interface RaceDetailSsrSnapshot {
  courseInfo: CourseInfo | null;
  race: RaceDetail;
  runners: Runner[];
  sameVenueRaces: RaceListItem[];
  // Epoch ms after which a reader refreshes the snapshot in the background; missing means stale.
  refreshAt?: number;
}

interface RefreshRaceDetailSsrSnapshotParams {
  cacheKey: string | null;
  load: () => Promise<RaceDetailSsrSnapshot | null>;
  params: RaceDetailSsrCacheKeyParams;
  snapshot: RaceDetailSsrSnapshot;
}

const CACHE_URL_BASE = "https://pc-keiba-viewer.local/race-detail-ssr-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";
const CACHE_CONTROL_HEADER = "public, max-age=%d";
const MIN_KV_TTL_SECONDS = 60;
const PRE_RACE_MAX_TTL_SECONDS = 60;

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getRaceStartTimeMs = (
  params: RaceDetailSsrCacheKeyParams,
  race: RaceDetail,
): number | null => {
  const normalizedTime = race.hassoJikoku?.trim().padStart(4, "0");
  if (!normalizedTime || !/^\d{4}$/u.test(normalizedTime)) {
    return null;
  }
  const startTime = Date.parse(
    `${params.year}-${params.month}-${params.day}T${normalizedTime.slice(0, 2)}:${normalizedTime.slice(2, 4)}:00+09:00`,
  );
  return Number.isFinite(startTime) ? startTime : null;
};

const getRaceDayFallbackBaseTimeMs = (params: RaceDetailSsrCacheKeyParams): number =>
  Date.parse(`${params.year}-${params.month}-${params.day}T23:59:59+09:00`);

const getConfiguredAfterStartSeconds = (env: CloudflareEnv | null): number => {
  const parsed = Number(env?.PC_KEIBA_DETAIL_SECTION_CACHE_AFTER_START_SECONDS);
  return Number.isFinite(parsed) && parsed >= MIN_KV_TTL_SECONDS
    ? Math.floor(parsed)
    : DETAIL_SECTION_CACHE_AFTER_START_SECONDS;
};

// How long KV keeps the snapshot: until the configured window after the start, so a page view never
// waits on the Hyperdrive fan-out while the warm generation still marks the race as valid.
export const getRaceDetailSsrKvTtlSeconds = (
  params: RaceDetailSsrCacheKeyParams,
  snapshot: Pick<RaceDetailSsrSnapshot, "race">,
  env: CloudflareEnv | null,
  nowMs = Date.now(),
): number => {
  const afterStartSeconds = getConfiguredAfterStartSeconds(env);
  const raceStartTime = getRaceStartTimeMs(params, snapshot.race);
  const expiresAt =
    (raceStartTime ?? getRaceDayFallbackBaseTimeMs(params)) + afterStartSeconds * 1000;
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

// How long the snapshot counts as fresh: 60 seconds before the start so late runner changes show up
// on the next view, and the whole KV window afterwards.
export const getRaceDetailSsrCacheTtlSeconds = (
  params: RaceDetailSsrCacheKeyParams,
  snapshot: Pick<RaceDetailSsrSnapshot, "race">,
  env: CloudflareEnv | null,
  nowMs = Date.now(),
): number => {
  const ttlSeconds = getRaceDetailSsrKvTtlSeconds(params, snapshot, env, nowMs);
  const raceStartTime = getRaceStartTimeMs(params, snapshot.race);
  return raceStartTime !== null && nowMs < raceStartTime
    ? Math.min(ttlSeconds, PRE_RACE_MAX_TTL_SECONDS)
    : ttlSeconds;
};

export const isRaceDetailSsrSnapshotStale = (
  snapshot: Pick<RaceDetailSsrSnapshot, "refreshAt">,
  nowMs = Date.now(),
): boolean => (snapshot.refreshAt ?? 0) <= nowMs;

export { buildRaceDetailSsrCacheKey };
export type { RaceDetailSsrCacheKeyParams };

const getCacheRequest = (cacheKey: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isRaceDetailSsrSnapshot = (value: unknown): value is RaceDetailSsrSnapshot =>
  isRecord(value) &&
  isRecord(value.race) &&
  Array.isArray(value.runners) &&
  Array.isArray(value.sameVenueRaces) &&
  (value.courseInfo === null || isRecord(value.courseInfo));

const parseSnapshot = (body: string): RaceDetailSsrSnapshot | null => {
  try {
    const parsed: unknown = JSON.parse(body);
    return isRaceDetailSsrSnapshot(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

const promoteKvHitToCacheApi = async (body: string, cacheKey: string): Promise<void> => {
  const cache = getDefaultCache();
  if (!cache) {
    return;
  }
  await cache.put(
    getCacheRequest(cacheKey),
    new Response(body, {
      headers: {
        "Cache-Control": "public, max-age=60",
        "Content-Type": DEFAULT_CONTENT_TYPE,
      },
    }),
  );
};

// Null when the race generation cannot be read; callers then skip the cache.
export const resolveRaceDetailSsrCacheKey = (
  params: RaceDetailSsrCacheKeyParams,
): Promise<string | null> =>
  resolveRaceCacheKey(buildRaceDetailSsrCacheKey(params), {
    keibajoCode: params.keibajoCode,
    mmdd: `${params.month}${params.day}`,
    raceBango: params.raceNumber,
    year: params.year,
  });

const readKvSnapshotBody = async (
  kv: PcKeibaKvNamespace,
  cacheKey: string,
): Promise<string | null> => {
  const body = await kv.get(cacheKey).catch(() => null);
  const legacyKey = getLegacyCacheKey(cacheKey);
  return body || legacyKey === null ? body : kv.get(legacyKey).catch(() => null);
};

export const getCachedRaceDetailSsrSnapshot = async (
  cacheKey: string | null,
): Promise<RaceDetailSsrSnapshot | null> => {
  if (cacheKey === null) {
    return null;
  }
  // Defensive try/catch on each tier: a transient Cache API or KV failure
  // must degrade to "cache miss" so the page handler can fall back to the
  // Hyperdrive fan-out instead of throwing all the way to the error
  // boundary (which used to land users on the bare "再読み込み" page).
  const cachedResponse = await getDefaultCache()
    ?.match(getCacheRequest(cacheKey))
    .catch(() => undefined);
  if (cachedResponse?.ok) {
    try {
      return parseSnapshot(await cachedResponse.text());
    } catch {
      return null;
    }
  }
  const { env, ctx } = await safeGetCloudflareRuntime();
  const kv = env?.DETAIL_SECTION_CACHE_KV;
  const kvBody = kv ? await readKvSnapshotBody(kv, cacheKey) : null;
  if (!kvBody) {
    return null;
  }
  ctx?.waitUntil(promoteKvHitToCacheApi(kvBody, cacheKey).catch(() => undefined));
  return parseSnapshot(kvBody);
};

interface PutRaceDetailSsrSnapshotParams {
  cacheKey: string | null;
  params: RaceDetailSsrCacheKeyParams;
  snapshot: RaceDetailSsrSnapshot;
}

export const putRaceDetailSsrSnapshot = async ({
  cacheKey,
  params,
  snapshot,
}: PutRaceDetailSsrSnapshotParams): Promise<void> => {
  if (cacheKey === null) {
    return;
  }
  const { env } = await safeGetCloudflareRuntime();
  const nowMs = Date.now();
  const kvTtlSeconds = getRaceDetailSsrKvTtlSeconds(params, snapshot, env, nowMs);
  if (kvTtlSeconds < MIN_KV_TTL_SECONDS) {
    return;
  }
  const ttlSeconds = getRaceDetailSsrCacheTtlSeconds(params, snapshot, env, nowMs);
  const body = JSON.stringify({ ...snapshot, refreshAt: nowMs + ttlSeconds * 1000 });
  const cacheControl = CACHE_CONTROL_HEADER.replace("%d", String(ttlSeconds));
  await Promise.all([
    getDefaultCache()?.put(
      getCacheRequest(cacheKey),
      new Response(body, {
        headers: {
          "Cache-Control": cacheControl,
          "Content-Type": DEFAULT_CONTENT_TYPE,
        },
      }),
    ),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: kvTtlSeconds }),
  ]);
};

// Serve-stale: the reader already has a snapshot to render, so the fresh fan-out runs after the
// response instead of holding the skeleton.
export const refreshRaceDetailSsrSnapshotIfStale = async ({
  cacheKey,
  load,
  params,
  snapshot,
}: RefreshRaceDetailSsrSnapshotParams): Promise<void> => {
  if (!isRaceDetailSsrSnapshotStale(snapshot)) {
    return;
  }
  const { ctx } = await safeGetCloudflareRuntime();
  ctx?.waitUntil(
    load()
      .then((fresh) =>
        fresh === null
          ? undefined
          : putRaceDetailSsrSnapshot({ cacheKey, params, snapshot: fresh }),
      )
      .catch(() => undefined),
  );
};
