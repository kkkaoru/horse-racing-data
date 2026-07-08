import { extractYyyymmddFromRaceKey } from "./race-key";
import { toHorseTrends, toOddsTrendsByType } from "./storage";
import type { Env, HorseOddsTrend, OddsData, OddsTrend, OddsTrendPoint, OddsType } from "./types";

const R2_LIVE_PREFIX = "odds-live/v1";
const R2_SNAPSHOT_PREFIX = "odds-snapshots/v1";
const R2_CATALOG_STAGING_PREFIX = "odds-catalog-staging/v1";
const R2_POINTER_KV_PREFIX = "odds:r2:payload";
const DEFAULT_R2_POINTER_KV_TTL_SECONDS = 172_800;
const JSON_CONTENT_TYPE = "application/json";
const NDJSON_CONTENT_TYPE = "application/x-ndjson";

const ODDS_HISTORY_POINTS_LIMIT_BY_TYPE = {
  "3renpuku": 120,
  "3rentan": 120,
  fukusho: 640,
  tansho: 1600,
  umaren: 480,
  umatan: 240,
  wakuren: 288,
  wide: 240,
} satisfies Record<OddsType, number>;

export interface R2OddsPayload {
  fetchedAt: string | null;
  history: HorseOddsTrend[];
  historyByType: Partial<Record<OddsType, OddsTrend[]>>;
  latest: Partial<Record<OddsType, OddsData[]>>;
  raceKey: string;
}

interface StoredR2OddsPayload {
  fetchedAt: string | null;
  historyByType: Partial<Record<OddsType, OddsTrendPoint[]>>;
  latest: Partial<Record<OddsType, OddsData[]>>;
  raceKey: string;
  schemaVersion: 1;
}

interface OddsCatalogEvent {
  average_odds: number | null;
  combination: string;
  fetched_at: string;
  kaisai_yyyymmdd: string | null;
  max_odds: number | null;
  min_odds: number | null;
  odds: number | null;
  odds_type: string;
  race_key: string;
  rank: number | null;
  source: string | null;
}

const sanitizePathSegment = (value: string): string => value.replace(/[^A-Za-z0-9_:-]/g, "_");

const getSourceFromRaceKey = (raceKey: string): string | null => {
  const source = raceKey.split(":").at(0);
  return source === "jra" || source === "nar" ? source : null;
};

const getYyyymmddFromRaceKey = (raceKey: string): string | null => {
  const modern = extractYyyymmddFromRaceKey(raceKey);
  if (modern) {
    return modern;
  }
  const parts = raceKey.split(":");
  const legacyDate = parts.at(1);
  return legacyDate && /^\d{8}$/u.test(legacyDate) ? legacyDate : null;
};

const buildRaceStoragePrefix = (raceKey: string): string => {
  const source = getSourceFromRaceKey(raceKey) ?? "unknown";
  const yyyymmdd = getYyyymmddFromRaceKey(raceKey) ?? "unknown-date";
  return `${source}/${yyyymmdd}/${sanitizePathSegment(raceKey)}`;
};

export const buildLiveOddsR2Key = (raceKey: string): string =>
  `${R2_LIVE_PREFIX}/${buildRaceStoragePrefix(raceKey)}/payload.json`;

export const buildSnapshotOddsR2Key = (raceKey: string, fetchedAt: string): string =>
  `${R2_SNAPSHOT_PREFIX}/${buildRaceStoragePrefix(raceKey)}/${sanitizePathSegment(fetchedAt)}.json`;

export const buildCatalogStagingR2Key = (raceKey: string, fetchedAt: string): string => {
  const yyyymmdd = getYyyymmddFromRaceKey(raceKey) ?? fetchedAt.slice(0, 10).replaceAll("-", "");
  return `${R2_CATALOG_STAGING_PREFIX}/kaisai_yyyymmdd=${yyyymmdd}/${sanitizePathSegment(raceKey)}/${sanitizePathSegment(fetchedAt)}.ndjson`;
};

const buildPointerKvKey = (raceKey: string): string => `${R2_POINTER_KV_PREFIX}:${raceKey}`;

const buildSnapshotOddsR2Prefix = (raceKey: string): string =>
  `${R2_SNAPSHOT_PREFIX}/${buildRaceStoragePrefix(raceKey)}/`;

const buildCatalogStagingR2Prefix = (raceKey: string): string | null => {
  const yyyymmdd = getYyyymmddFromRaceKey(raceKey);
  return yyyymmdd
    ? `${R2_CATALOG_STAGING_PREFIX}/kaisai_yyyymmdd=${yyyymmdd}/${sanitizePathSegment(raceKey)}/`
    : null;
};

const resolvePointerKvTtl = (env: Env): number => {
  const raw = env.ODDS_R2_POINTER_KV_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_R2_POINTER_KV_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_R2_POINTER_KV_TTL_SECONDS;
};

const oddsDataToTrendPoint = (fetchedAt: string, row: OddsData): OddsTrendPoint => ({
  combination: row.combination,
  fetchedAt,
  odds: row.odds ?? null,
  rank: row.rank ?? null,
});

const appendPointsForType = (
  existing: OddsTrendPoint[],
  next: OddsTrendPoint[],
  oddsType: OddsType,
): OddsTrendPoint[] => {
  const merged = [...existing, ...next];
  const limit = ODDS_HISTORY_POINTS_LIMIT_BY_TYPE[oddsType];
  return merged.length > limit ? merged.slice(merged.length - limit) : merged;
};

const mergeHistory = (
  current: Partial<Record<OddsType, OddsTrendPoint[]>>,
  fetchedAt: string,
  latest: Partial<Record<OddsType, OddsData[]>>,
): Partial<Record<OddsType, OddsTrendPoint[]>> => {
  const next: Partial<Record<OddsType, OddsTrendPoint[]>> = { ...current };
  const entries = Object.entries(latest) as [OddsType, OddsData[] | undefined][];
  entries.forEach(([oddsType, rows]) => {
    if (!rows || rows.length === 0) {
      return;
    }
    next[oddsType] = appendPointsForType(
      current[oddsType] ?? [],
      rows.map((row) => oddsDataToTrendPoint(fetchedAt, row)),
      oddsType,
    );
  });
  return next;
};

const mergeLatest = (
  current: Partial<Record<OddsType, OddsData[]>>,
  incoming: Partial<Record<OddsType, OddsData[]>>,
): Partial<Record<OddsType, OddsData[]>> => ({
  ...current,
  ...incoming,
});

const tanshoPointsToHorseTrends = (points: OddsTrendPoint[]): HorseOddsTrend[] =>
  toHorseTrends(
    points.map((point) => ({
      fetchedAt: point.fetchedAt,
      horseNumber: point.combination,
      odds: point.odds,
      popularity: point.rank,
    })),
  );

const toPublicPayload = (stored: StoredR2OddsPayload): R2OddsPayload => ({
  fetchedAt: stored.fetchedAt,
  history: tanshoPointsToHorseTrends(stored.historyByType.tansho ?? []),
  historyByType: toOddsTrendsByType(stored.historyByType),
  latest: stored.latest,
  raceKey: stored.raceKey,
});

const parseStoredPayload = async (object: R2ObjectBody): Promise<StoredR2OddsPayload | null> => {
  const parsed = (await object.json()) as Partial<StoredR2OddsPayload>;
  if (parsed.schemaVersion !== 1 || typeof parsed.raceKey !== "string") {
    return null;
  }
  return {
    fetchedAt: parsed.fetchedAt ?? null,
    historyByType: parsed.historyByType ?? {},
    latest: parsed.latest ?? {},
    raceKey: parsed.raceKey,
    schemaVersion: 1,
  };
};

const readPointerKey = async (env: Env, raceKey: string): Promise<string | null> => {
  try {
    return await env.ODDS_HOT_KV.get(buildPointerKvKey(raceKey));
  } catch {
    return null;
  }
};

const writePointerKey = async (env: Env, raceKey: string, key: string): Promise<void> => {
  await env.ODDS_HOT_KV.put(buildPointerKvKey(raceKey), key, {
    expirationTtl: resolvePointerKvTtl(env),
  }).catch(() => undefined);
};

export const readStoredOddsPayloadFromR2 = async (
  env: Env,
  raceKey: string,
): Promise<StoredR2OddsPayload | null> => {
  const pointerKey = await readPointerKey(env, raceKey);
  const candidateKeys = pointerKey
    ? [pointerKey, buildLiveOddsR2Key(raceKey)]
    : [buildLiveOddsR2Key(raceKey)];
  for (const key of candidateKeys) {
    const object = await env.ODDS_ARCHIVE.get(key).catch(() => null);
    if (!object) {
      continue;
    }
    const payload = await parseStoredPayload(object);
    if (payload?.raceKey === raceKey) {
      await writePointerKey(env, raceKey, key);
      return payload;
    }
  }
  return null;
};

export const readOddsPayloadFromR2 = async (
  env: Env,
  raceKey: string,
): Promise<R2OddsPayload | null> => {
  const stored = await readStoredOddsPayloadFromR2(env, raceKey);
  return stored ? toPublicPayload(stored) : null;
};

const buildNextStoredPayload = (
  raceKey: string,
  fetchedAt: string,
  latest: Partial<Record<OddsType, OddsData[]>>,
  existing: StoredR2OddsPayload | null,
): StoredR2OddsPayload => ({
  fetchedAt,
  historyByType: mergeHistory(existing?.historyByType ?? {}, fetchedAt, latest),
  latest: mergeLatest(existing?.latest ?? {}, latest),
  raceKey,
  schemaVersion: 1,
});

const flattenCatalogEvents = (
  raceKey: string,
  fetchedAt: string,
  latest: Partial<Record<OddsType, OddsData[]>>,
): OddsCatalogEvent[] => {
  const source = getSourceFromRaceKey(raceKey);
  const kaisaiYyyymmdd = getYyyymmddFromRaceKey(raceKey);
  return (Object.entries(latest) as [OddsType, OddsData[] | undefined][]).flatMap(
    ([oddsType, rows]) =>
      (rows ?? []).map((row) => ({
        average_odds: row.averageOdds ?? null,
        combination: row.combination,
        fetched_at: fetchedAt,
        kaisai_yyyymmdd: kaisaiYyyymmdd,
        max_odds: row.maxOdds ?? null,
        min_odds: row.minOdds ?? null,
        odds: row.odds ?? null,
        odds_type: oddsType,
        race_key: raceKey,
        rank: row.rank ?? null,
        source,
      })),
  );
};

const putJson = async (env: Env, key: string, value: unknown): Promise<void> => {
  await env.ODDS_ARCHIVE.put(key, JSON.stringify(value), {
    httpMetadata: { contentType: JSON_CONTENT_TYPE },
  });
};

const putCatalogStagingEvents = async (
  env: Env,
  raceKey: string,
  fetchedAt: string,
  latest: Partial<Record<OddsType, OddsData[]>>,
): Promise<void> => {
  const events = flattenCatalogEvents(raceKey, fetchedAt, latest);
  if (events.length === 0) {
    return;
  }
  await env.ODDS_CATALOG_STREAM?.send(events).catch(() => undefined);
  const body = `${events.map((event) => JSON.stringify(event)).join("\n")}\n`;
  await env.ODDS_ARCHIVE.put(buildCatalogStagingR2Key(raceKey, fetchedAt), body, {
    httpMetadata: { contentType: NDJSON_CONTENT_TYPE },
  });
};

export const writeOddsPayloadToR2 = async (
  env: Env,
  raceKey: string,
  fetchedAt: string,
  latest: Partial<Record<OddsType, OddsData[]>>,
): Promise<R2OddsPayload> => {
  const existing = await readStoredOddsPayloadFromR2(env, raceKey);
  const stored = buildNextStoredPayload(raceKey, fetchedAt, latest, existing);
  const liveKey = buildLiveOddsR2Key(raceKey);
  await putJson(env, liveKey, stored);
  await putJson(env, buildSnapshotOddsR2Key(raceKey, fetchedAt), {
    fetchedAt,
    latest,
    raceKey,
    schemaVersion: 1,
  });
  await putCatalogStagingEvents(env, raceKey, fetchedAt, latest);
  await writePointerKey(env, raceKey, liveKey);
  return toPublicPayload(stored);
};

export interface PurgeOddsPayloadFromR2Result {
  deletedKeys: string[];
}

const listR2KeysByPrefix = async (env: Env, prefix: string): Promise<string[]> => {
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const result = await env.ODDS_ARCHIVE.list({ cursor, prefix });
    keys.push(...result.objects.map((object) => object.key));
    cursor = result.truncated ? result.cursor : undefined;
  } while (cursor);
  return keys;
};

export const purgeOddsPayloadFromR2 = async (
  env: Env,
  raceKey: string,
): Promise<PurgeOddsPayloadFromR2Result> => {
  const prefixes = [
    buildSnapshotOddsR2Prefix(raceKey),
    buildCatalogStagingR2Prefix(raceKey),
  ].filter((prefix): prefix is string => prefix !== null);
  const listedKeys = (
    await Promise.all(prefixes.map((prefix) => listR2KeysByPrefix(env, prefix)))
  ).flat();
  const deletedKeys = Array.from(new Set([buildLiveOddsR2Key(raceKey), ...listedKeys]));
  await Promise.all([
    ...deletedKeys.map((key) => env.ODDS_ARCHIVE.delete(key)),
    env.ODDS_HOT_KV.delete(buildPointerKvKey(raceKey)),
  ]);
  return { deletedKeys };
};
