// Run with bun. Race-trend aggregator: walks N R2 prefixes (one per date in the
// requested range), fetches per-race Parquet bytes (with Gate 6 KV list cache +
// Gate 7 Cache API bytes cache + live R2 fallback), decodes via hyparquet, and
// returns the viewer-ready envelope (starterRows + byJockey / byWaku).

import { aggregateRaceTrendRows, type RaceTrendAggregateResult } from "./aggregate";
import { decodeRaceFeaturesParquet } from "./parquet";
import { readRaceTrendFromEdgeCache, writeRaceTrendToEdgeCache } from "../gates/edge-cache";
import { readParquetBytesFromCache, writeParquetBytesToCache } from "../gates/parquet-bytes-cache";
import { readR2ListFromKv, writeR2ListToKv } from "../gates/r2-list-cache";
import { jsonResponse } from "../http";
import type { DailyRaceEntryRow, Env } from "../types";

const YMD_PATTERN = /^\d{8}$/u;
const KEIBAJO_PAD_WIDTH = 2;
const RACE_BANGO_PAD_WIDTH = 2;
const R2_PREFIX = "features/by-race";
const FEATURES_KV_BIND_NAME = "FEATURES_KV";
const MS_PER_DAY = 24 * 60 * 60 * 1000;

export interface RaceTrendQueryParams {
  source: "jra" | "nar";
  keibajoCode: string;
  raceBango: string;
  from: string;
  to: string;
}

export const buildRaceTrendCacheKey = (params: RaceTrendQueryParams): string =>
  `race-trend:${params.source}:${params.keibajoCode}:${params.raceBango}:${params.from}:${params.to}`;

export const buildRaceTrendPrefix = (input: {
  source: "jra" | "nar";
  ymd: string;
  keibajoCode: string;
}): string =>
  `${R2_PREFIX}/${input.ymd.slice(0, 4)}/${input.ymd.slice(4, 6)}/${input.ymd.slice(6, 8)}/${input.source}/${input.keibajoCode.padStart(KEIBAJO_PAD_WIDTH, "0")}/`;

export const buildRaceParquetKey = (input: {
  source: "jra" | "nar";
  ymd: string;
  keibajoCode: string;
  raceBango: string;
}): string =>
  `${buildRaceTrendPrefix({ source: input.source, ymd: input.ymd, keibajoCode: input.keibajoCode })}${input.raceBango.padStart(RACE_BANGO_PAD_WIDTH, "0")}.parquet`;

const ymdToDate = (ymd: string): Date =>
  new Date(Date.UTC(Number(ymd.slice(0, 4)), Number(ymd.slice(4, 6)) - 1, Number(ymd.slice(6, 8))));

const dateToYmd = (date: Date): string => {
  const year = date.getUTCFullYear().toString().padStart(4, "0");
  const month = (date.getUTCMonth() + 1).toString().padStart(2, "0");
  const day = date.getUTCDate().toString().padStart(2, "0");
  return `${year}${month}${day}`;
};

export const expandDateRange = (from: string, to: string): string[] => {
  const start = ymdToDate(from);
  const end = ymdToDate(to);
  if (start.getTime() > end.getTime()) return [];
  const dates: string[] = [];
  const cursor = start;
  while (cursor.getTime() <= end.getTime()) {
    dates.push(dateToYmd(cursor));
    cursor.setTime(cursor.getTime() + MS_PER_DAY);
  }
  return dates;
};

const isValidSource = (value: string | null): value is "jra" | "nar" =>
  value === "jra" || value === "nar";

const parseQueryParams = (url: URL): RaceTrendQueryParams | null => {
  const source = url.searchParams.get("source");
  const keibajoCode = url.searchParams.get("keibajoCode");
  const raceBango = url.searchParams.get("raceBango");
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to");
  if (!isValidSource(source)) return null;
  if (!keibajoCode || !raceBango) return null;
  if (!from || !YMD_PATTERN.test(from)) return null;
  if (!to || !YMD_PATTERN.test(to)) return null;
  return { from, keibajoCode, raceBango, source, to };
};

const hasFeaturesKv = (env: Env): boolean =>
  Object.hasOwn(env as unknown as Record<string, unknown>, FEATURES_KV_BIND_NAME) &&
  Boolean(env.FEATURES_KV);

interface R2ObjectKey {
  key: string;
}

interface R2ListResultLike {
  objects: ReadonlyArray<R2ObjectKey>;
}

const listR2KeysLive = async (env: Env, prefix: string): Promise<string[]> => {
  const result = (await env.FEATURES_ARCHIVE.list({ prefix })) as R2ListResultLike;
  return result.objects.map((object) => object.key);
};

const readKeysWithGate6 = async (env: Env, prefix: string): Promise<string[]> => {
  if (!hasFeaturesKv(env)) {
    return listR2KeysLive(env, prefix);
  }
  const cached = await readR2ListFromKv(env, prefix);
  if (cached !== null) return cached;
  const keys = await listR2KeysLive(env, prefix);
  await writeR2ListToKv(env, prefix, keys);
  return keys;
};

const fetchR2BytesLive = async (env: Env, key: string): Promise<ArrayBuffer | null> => {
  const object = await env.FEATURES_ARCHIVE.get(key);
  if (!object) return null;
  return await object.arrayBuffer();
};

const readBytesWithGate7 = async (env: Env, key: string): Promise<ArrayBuffer | null> => {
  const cached = await readParquetBytesFromCache(key);
  if (cached !== null) return cached;
  const live = await fetchR2BytesLive(env, key);
  if (live === null) return null;
  await writeParquetBytesToCache(key, live, env);
  return live;
};

const assignRaceDate = (row: DailyRaceEntryRow): DailyRaceEntryRow => ({
  ...row,
  race_date: `${row.kaisai_nen}${row.kaisai_tsukihi}`,
});

const fetchRaceRows = async (env: Env, key: string): Promise<DailyRaceEntryRow[]> => {
  const buffer = await readBytesWithGate7(env, key);
  if (!buffer) return [];
  const rows = await decodeRaceFeaturesParquet(new Uint8Array(buffer));
  return rows.map(assignRaceDate);
};

const findTargetKey = (keys: string[], expected: string): string | null =>
  keys.find((key) => key === expected) ?? null;

const collectRowsForDay = async (
  env: Env,
  params: {
    source: "jra" | "nar";
    ymd: string;
    keibajoCode: string;
    raceBango: string;
  },
): Promise<DailyRaceEntryRow[]> => {
  const prefix = buildRaceTrendPrefix({
    keibajoCode: params.keibajoCode,
    source: params.source,
    ymd: params.ymd,
  });
  const keys = await readKeysWithGate6(env, prefix);
  const expected = buildRaceParquetKey({
    keibajoCode: params.keibajoCode,
    raceBango: params.raceBango,
    source: params.source,
    ymd: params.ymd,
  });
  const targetKey = findTargetKey(keys, expected);
  if (!targetKey) return [];
  return fetchRaceRows(env, targetKey);
};

const collectAllRows = async (
  env: Env,
  params: RaceTrendQueryParams,
): Promise<DailyRaceEntryRow[]> => {
  const dates = expandDateRange(params.from, params.to);
  const perDay = await Promise.all(
    dates.map((ymd) =>
      collectRowsForDay(env, {
        keibajoCode: params.keibajoCode,
        raceBango: params.raceBango,
        source: params.source,
        ymd,
      }),
    ),
  );
  return perDay.flat();
};

export const buildRaceTrendPayload = async (
  env: Env,
  params: RaceTrendQueryParams,
): Promise<RaceTrendAggregateResult> => {
  const rows = await collectAllRows(env, params);
  return aggregateRaceTrendRows(rows);
};

const tryReadEdgeCache = async (cacheKey: string): Promise<Response | null> => {
  if (typeof caches === "undefined") return null;
  return readRaceTrendFromEdgeCache(cacheKey);
};

const buildEmptyPayload = (): RaceTrendAggregateResult => ({
  byJockey: {},
  byWaku: {},
  raceCount: 0,
  starterCount: 0,
  starterRows: [],
});

export const handleRaceTrend = async (env: Env, request: Request): Promise<Response> => {
  const url = new URL(request.url);
  const params = parseQueryParams(url);
  if (!params) return jsonResponse(buildEmptyPayload());
  const cacheKey = buildRaceTrendCacheKey(params);
  const edgeHit = await tryReadEdgeCache(cacheKey);
  if (edgeHit) return edgeHit;
  const payload = await buildRaceTrendPayload(env, params);
  await writeRaceTrendToEdgeCache(cacheKey, payload, env);
  return jsonResponse(payload);
};
