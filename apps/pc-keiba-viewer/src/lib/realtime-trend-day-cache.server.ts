// Run with bun. Per (source × ymd) cache for the realtime starter rows that
// the race-trend route stitches into its merged payload. Sharing the realtime
// fetch across every race on the same day collapses the worst-case cold trend
// request from ~100 sync-realtime-data fetches per trend call down to one
// fetch batch per (source, ymd) per TTL window.
//
// Today's cache uses a short TTL so finishing-race results land within ~1
// minute; older days use a 6h TTL because the row set is stable once D1 daily
// has been backfilled.
import "server-only";
import type { RaceTrendStarterRow } from "horse-racing-realtime/race-trend-daily-track-types";

import { safeGetCloudflareRuntime } from "./cloudflare-context.server";
import type { RaceSource } from "./codes";

const CACHE_URL_BASE = "https://pc-keiba-viewer.local/realtime-trend-day-cache/";
const TODAY_EDGE_TTL_SECONDS = 60;
const TODAY_KV_TTL_SECONDS = 90;
const PAST_EDGE_TTL_SECONDS = 6 * 60 * 60;
const PAST_KV_TTL_SECONDS = 12 * 60 * 60;
const JSON_CONTENT_TYPE = "application/json; charset=utf-8";
const JST_OFFSET_MS = 9 * 60 * 60 * 1000;

interface RealtimeDayCacheTtl {
  edge: number;
  kv: number;
}

export interface GetRealtimeRowsForDayInput {
  fetcher: () => Promise<RaceTrendStarterRow[]>;
  source: RaceSource;
  ymd: string;
}

export const buildRealtimeTrendDayCacheKey = (source: RaceSource, ymd: string): string =>
  `race-trend-realtime-day:v1:${source}:${ymd}`;

export const getJstTodayYmd = (now: Date): string => {
  const jst = new Date(now.getTime() + JST_OFFSET_MS);
  return `${jst.getUTCFullYear()}${String(jst.getUTCMonth() + 1).padStart(2, "0")}${String(
    jst.getUTCDate(),
  ).padStart(2, "0")}`;
};

const resolveTtl = (ymd: string, now: Date): RealtimeDayCacheTtl =>
  ymd === getJstTodayYmd(now)
    ? { edge: TODAY_EDGE_TTL_SECONDS, kv: TODAY_KV_TTL_SECONDS }
    : { edge: PAST_EDGE_TTL_SECONDS, kv: PAST_KV_TTL_SECONDS };

const getDefaultCache = (): Cache | null =>
  typeof caches === "undefined" || !caches.default ? null : caches.default;

const buildCacheRequest = (cacheKey: string): Request =>
  new Request(`${CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);

const buildCachedResponseBody = (body: string, ttlSeconds: number): Response =>
  new Response(body, {
    headers: {
      "Cache-Control": `public, max-age=${ttlSeconds}`,
      "Content-Type": JSON_CONTENT_TYPE,
    },
  });

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isRaceSource = (value: unknown): value is RaceSource => value === "jra" || value === "nar";

const isRaceTrendStarterRow = (value: unknown): value is RaceTrendStarterRow => {
  if (!isRecord(value)) return false;
  return (
    isRaceSource(value.source) &&
    typeof value.kaisaiNen === "string" &&
    typeof value.kaisaiTsukihi === "string" &&
    typeof value.keibajoCode === "string" &&
    typeof value.raceBango === "string" &&
    typeof value.finishPosition === "number"
  );
};

const parseStarterRowArray = (value: unknown): RaceTrendStarterRow[] | null => {
  if (!Array.isArray(value)) return null;
  return value.filter(isRaceTrendStarterRow);
};

const readFromEdge = async (cacheKey: string): Promise<RaceTrendStarterRow[] | null> => {
  const cache = getDefaultCache();
  if (!cache) return null;
  const cached = await cache.match(buildCacheRequest(cacheKey));
  if (!cached?.ok) return null;
  try {
    const parsed: unknown = await cached.json();
    return parseStarterRowArray(parsed);
  } catch {
    return null;
  }
};

interface ReadFromKvInput {
  cacheKey: string;
  ctx: PcKeibaExecutionContext | null;
  env: CloudflareEnv | null;
  ttl: RealtimeDayCacheTtl;
}

const backfillEdgeFromKv = async (
  cacheKey: string,
  body: string,
  edgeTtl: number,
): Promise<void> => {
  const cache = getDefaultCache();
  if (!cache) return;
  await cache.put(buildCacheRequest(cacheKey), buildCachedResponseBody(body, edgeTtl));
};

const safeParseStarterRowsBody = (body: string): RaceTrendStarterRow[] | null => {
  try {
    const parsed: unknown = JSON.parse(body);
    return parseStarterRowArray(parsed);
  } catch {
    return null;
  }
};

const readFromKv = async ({
  cacheKey,
  ctx,
  env,
  ttl,
}: ReadFromKvInput): Promise<RaceTrendStarterRow[] | null> => {
  const body = await env?.DETAIL_SECTION_CACHE_KV?.get(cacheKey);
  if (!body) return null;
  const rows = safeParseStarterRowsBody(body);
  if (rows === null) return null;
  const backfill = backfillEdgeFromKv(cacheKey, body, ttl.edge);
  if (ctx) ctx.waitUntil(backfill);
  else await backfill;
  return rows;
};

interface PutCachesInput {
  body: string;
  cacheKey: string;
  ctx: PcKeibaExecutionContext | null;
  env: CloudflareEnv | null;
  ttl: RealtimeDayCacheTtl;
}

const putCaches = async ({ body, cacheKey, ctx, env, ttl }: PutCachesInput): Promise<void> => {
  const cache = getDefaultCache();
  const writes = Promise.all([
    cache?.put(buildCacheRequest(cacheKey), buildCachedResponseBody(body, ttl.edge)),
    env?.DETAIL_SECTION_CACHE_KV?.put(cacheKey, body, { expirationTtl: ttl.kv }),
  ]);
  if (ctx) ctx.waitUntil(writes.then(() => undefined));
  else await writes;
};

export const getRealtimeRowsForDayWithCache = async ({
  fetcher,
  source,
  ymd,
}: GetRealtimeRowsForDayInput): Promise<RaceTrendStarterRow[]> => {
  const cacheKey = buildRealtimeTrendDayCacheKey(source, ymd);
  const edgeRows = await readFromEdge(cacheKey);
  if (edgeRows !== null) return edgeRows;
  const { ctx, env } = await safeGetCloudflareRuntime();
  const ttl = resolveTtl(ymd, new Date());
  const kvRows = await readFromKv({ cacheKey, ctx, env, ttl });
  if (kvRows !== null) return kvRows;
  const rows = await fetcher();
  await putCaches({ body: JSON.stringify(rows), cacheKey, ctx, env, ttl });
  return rows;
};

export const bustRealtimeRowsForDay = async (params: {
  source: RaceSource;
  ymd: string;
}): Promise<void> => {
  const cacheKey = buildRealtimeTrendDayCacheKey(params.source, params.ymd);
  const cache = getDefaultCache();
  const { env } = await safeGetCloudflareRuntime();
  await Promise.all([
    cache?.delete(buildCacheRequest(cacheKey)),
    env?.DETAIL_SECTION_CACHE_KV?.delete(cacheKey),
  ]);
};
