// Cache the heavy SSR fan-out (race + runners + courseInfo + sameVenueRaces)
// in KV so warm cache hits anywhere in the world skip the Hyperdrive round
// trips. Cache API gives a per-colo fast path; KV provides global coverage.
// Execute with bun: opennextjs-cloudflare build && wrangler dev

import "server-only";
import { getCloudflareContext } from "@opennextjs/cloudflare";

import type { RaceSource } from "./codes";
import { DETAIL_SECTION_CACHE_AFTER_START_SECONDS } from "./race-detail-section-cache";
import type { CourseInfo, RaceDetail, RaceListItem, Runner } from "./race-types";

export interface RaceDetailSsrSnapshot {
  courseInfo: CourseInfo | null;
  race: RaceDetail;
  runners: Runner[];
  sameVenueRaces: RaceListItem[];
}

export interface RaceDetailSsrCacheKeyParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

const RACE_DETAIL_SSR_CACHE_VERSION = "v1";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/race-detail-ssr-cache/";
const DEFAULT_CONTENT_TYPE = "application/json; charset=utf-8";
const CACHE_CONTROL_HEADER = "public, max-age=%d";
const MIN_KV_TTL_SECONDS = 60;

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

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const getRaceStartTimeMs = (params: RaceDetailSsrCacheKeyParams, race: RaceDetail): number | null => {
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

export const getRaceDetailSsrCacheTtlSeconds = (
  params: RaceDetailSsrCacheKeyParams,
  snapshot: Pick<RaceDetailSsrSnapshot, "race">,
  env: CloudflareEnv | null,
  nowMs = Date.now(),
): number => {
  const afterStartSeconds = getConfiguredAfterStartSeconds(env);
  const raceStartTime = getRaceStartTimeMs(params, snapshot.race);
  const expiresAt = (raceStartTime ?? getRaceDayFallbackBaseTimeMs(params)) + afterStartSeconds * 1000;
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

export const buildRaceDetailSsrCacheKey = (params: RaceDetailSsrCacheKeyParams): string =>
  [
    "race-detail-ssr",
    RACE_DETAIL_SSR_CACHE_VERSION,
    params.source,
    params.year,
    params.month,
    params.day,
    params.keibajoCode,
    params.raceNumber,
  ].join(":");

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

const promoteKvHitToCacheApi = async (
  body: string,
  cacheKey: string,
): Promise<void> => {
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

export const getCachedRaceDetailSsrSnapshot = async (
  cacheKey: string,
): Promise<RaceDetailSsrSnapshot | null> => {
  const defaultCache = getDefaultCache();
  const cachedResponse = await defaultCache?.match(getCacheRequest(cacheKey));
  if (cachedResponse?.ok) {
    return parseSnapshot(await cachedResponse.text());
  }
  const { env, ctx } = await getCloudflareRuntime();
  const kvBody = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!kvBody) {
    return null;
  }
  ctx?.waitUntil(promoteKvHitToCacheApi(kvBody, cacheKey));
  return parseSnapshot(kvBody);
};

interface PutRaceDetailSsrSnapshotParams {
  cacheKey: string;
  params: RaceDetailSsrCacheKeyParams;
  snapshot: RaceDetailSsrSnapshot;
}

export const putRaceDetailSsrSnapshot = async ({
  cacheKey,
  params,
  snapshot,
}: PutRaceDetailSsrSnapshotParams): Promise<void> => {
  const { env } = await getCloudflareRuntime();
  const ttlSeconds = getRaceDetailSsrCacheTtlSeconds(params, snapshot, env);
  if (ttlSeconds < MIN_KV_TTL_SECONDS) {
    return;
  }
  const body = JSON.stringify(snapshot);
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
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: ttlSeconds }),
  ]);
};
