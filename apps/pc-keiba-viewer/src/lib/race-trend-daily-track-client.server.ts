// Run with bun. Client wrapper for sync-realtime-data's
// `RaceTrendDailyTrackDO` service binding. Returns the per-day, per-track
// starter rows the trend page needs for sibling-race aggregation.
//
// Cache strategy: edge Cache API only with a 10s TTL. KV / memory caches
// are intentionally avoided — the DO is the freshness primary and a longer
// cache would re-introduce the cross-venue staleness this client was built
// to fix. The cache key MUST include keibajoCode (and beforeRaceBango) so
// two different venues on the same day never share a cache entry.
import "server-only";
import type {
  RaceTrendDailyTrackQuery,
  RaceTrendDailyTrackResponse,
  RaceTrendDailyTrackRow,
} from "horse-racing-realtime/race-trend-daily-track-types";

const DO_ENDPOINT = "https://internal/race-trend-daily-track";
const DO_RESPONSE_HEADER = "X-Race-Trend-DO";
const DO_HIT_HEADER_VALUE = "hit";
const CACHE_URL_BASE = "https://pc-keiba-viewer.local/race-trend-do-cache/";
const CACHE_TTL_SECONDS = 10;
const CACHE_KEY_VERSION = "v1";

export type RaceTrendDailyTrackFetchStatus = "hit" | "miss" | "error";

export interface RaceTrendDailyTrackFetchResult {
  rows: RaceTrendDailyTrackRow[];
  status: RaceTrendDailyTrackFetchStatus;
}

const EMPTY_RESULT_MISS: RaceTrendDailyTrackFetchResult = { rows: [], status: "miss" };
const EMPTY_RESULT_ERROR: RaceTrendDailyTrackFetchResult = { rows: [], status: "error" };

interface CachedPayload {
  rows: RaceTrendDailyTrackRow[];
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isRow = (value: unknown): value is RaceTrendDailyTrackRow => {
  if (!isRecord(value)) return false;
  return (
    typeof value.raceBango === "string" &&
    typeof value.raceKey === "string" &&
    typeof value.isComplete === "boolean" &&
    typeof value.fetchedAt === "string" &&
    Array.isArray(value.starterRows) &&
    Array.isArray(value.runningStyles)
  );
};

const parseRowsArray = (value: unknown): RaceTrendDailyTrackRow[] | null => {
  if (!Array.isArray(value)) return null;
  return value.filter(isRow);
};

const parseResponseBody = (value: unknown): RaceTrendDailyTrackRow[] | null => {
  if (!isRecord(value)) return null;
  return parseRowsArray((value as Partial<RaceTrendDailyTrackResponse>).races);
};

const parseCachedBody = (value: unknown): RaceTrendDailyTrackRow[] | null => {
  if (!isRecord(value)) return null;
  return parseRowsArray((value as Partial<CachedPayload>).rows);
};

export const buildRaceTrendDailyTrackCacheKey = (query: RaceTrendDailyTrackQuery): string =>
  `race-trend-do:${CACHE_KEY_VERSION}:${query.source}:${query.targetYmd}:${query.keibajoCode}:${query.beforeRaceBango}`;

const buildCacheRequest = (query: RaceTrendDailyTrackQuery): Request => {
  const cacheKey = buildRaceTrendDailyTrackCacheKey(query);
  return new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);
};

const buildServiceBindingUrl = (query: RaceTrendDailyTrackQuery): string => {
  const params = new URLSearchParams({
    source: query.source,
    ymd: query.targetYmd,
    keibajo: query.keibajoCode,
    beforeRaceBango: query.beforeRaceBango,
  });
  return `${DO_ENDPOINT}?${params.toString()}`;
};

const readCachedResult = async (
  query: RaceTrendDailyTrackQuery,
): Promise<RaceTrendDailyTrackFetchResult | null> => {
  const cache = typeof caches === "undefined" ? null : caches.default;
  if (!cache) return null;
  const cached = await cache.match(buildCacheRequest(query));
  if (!cached?.ok) return null;
  try {
    const parsed: unknown = await cached.json();
    const rows = parseCachedBody(parsed);
    if (rows === null) return null;
    return rows.length > 0 ? { rows, status: "hit" } : EMPTY_RESULT_MISS;
  } catch {
    return null;
  }
};

const writeCachedResult = async (
  query: RaceTrendDailyTrackQuery,
  rows: RaceTrendDailyTrackRow[],
): Promise<void> => {
  const cache = typeof caches === "undefined" ? null : caches.default;
  if (!cache) return;
  const body = JSON.stringify({ rows } satisfies CachedPayload);
  await cache.put(
    buildCacheRequest(query),
    new Response(body, {
      headers: {
        "Cache-Control": `public, max-age=${CACHE_TTL_SECONDS}`,
        "Content-Type": "application/json; charset=utf-8",
      },
    }),
  );
};

const parseServiceBindingResponse = async (
  response: Response,
): Promise<RaceTrendDailyTrackFetchResult> => {
  if (!response.ok) return EMPTY_RESULT_ERROR;
  const headerValue = response.headers.get(DO_RESPONSE_HEADER);
  try {
    const payload: unknown = await response.json();
    const rows = parseResponseBody(payload);
    if (rows === null) return EMPTY_RESULT_ERROR;
    if (headerValue !== DO_HIT_HEADER_VALUE) return EMPTY_RESULT_MISS;
    return rows.length > 0 ? { rows, status: "hit" } : EMPTY_RESULT_MISS;
  } catch {
    return EMPTY_RESULT_ERROR;
  }
};

export const fetchRaceTrendDailyTrack = async (
  env: CloudflareEnv | null,
  query: RaceTrendDailyTrackQuery,
): Promise<RaceTrendDailyTrackFetchResult> => {
  const cached = await readCachedResult(query);
  if (cached !== null) return cached;
  const service = env?.REALTIME_DATA;
  if (!service) return EMPTY_RESULT_ERROR;
  try {
    const response = await service.fetch(buildServiceBindingUrl(query));
    const result = await parseServiceBindingResponse(response);
    if (result.status === "hit") {
      await writeCachedResult(query, result.rows);
    }
    return result;
  } catch (error) {
    console.error("RaceTrendDailyTrackDO service binding fetch failed", error);
    return EMPTY_RESULT_ERROR;
  }
};
