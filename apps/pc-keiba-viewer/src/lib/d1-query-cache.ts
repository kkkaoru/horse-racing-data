export const SHARED_D1_QUERY_CACHE_NAMESPACE = "horse-racing-data:d1-query:v1";
export const SHARED_D1_QUERY_CACHE_URL_BASE = "https://horse-racing-data.local/d1-query-cache/";

export type D1QueryCacheProfile =
  | "horse-running-style-history"
  | "realtime-short"
  | "running-style-race"
  | "running-style-races";

export interface D1QueryCacheRaceDayContext {
  kaisaiNen: string;
  kaisaiTsukihi: string;
}

const PROFILE_DEFAULT_TTL_SECONDS: Record<D1QueryCacheProfile, number> = {
  "horse-running-style-history": 60 * 60,
  "realtime-short": 60,
  "running-style-race": 6 * 60 * 60,
  "running-style-races": 6 * 60 * 60,
};

const isPlainRecord = (value: unknown): value is Record<string, unknown> =>
  Object.prototype.toString.call(value) === "[object Object]";

const stableStringify = (value: unknown): string =>
  JSON.stringify(value, (_key, item: unknown) => {
    if (!isPlainRecord(item)) {
      return item;
    }
    return Object.fromEntries(
      Object.entries(item).toSorted(([left], [right]) => left.localeCompare(right)),
    );
  });

const hashString = (value: string): string => {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
};

const getRaceDayExpiresAtMs = (raceDay: D1QueryCacheRaceDayContext): number =>
  Date.parse(
    `${raceDay.kaisaiNen}-${raceDay.kaisaiTsukihi.slice(0, 2)}-${raceDay.kaisaiTsukihi.slice(
      2,
      4,
    )}T23:59:59+09:00`,
  );

const getRaceDayTtlSeconds = (raceDay: D1QueryCacheRaceDayContext, nowMs = Date.now()): number => {
  const expiresAt = getRaceDayExpiresAtMs(raceDay);
  if (!Number.isFinite(expiresAt)) {
    return 0;
  }
  return Math.max(0, Math.floor((expiresAt - nowMs) / 1000));
};

export const resolveD1QueryCacheTtlSeconds = (
  profile: D1QueryCacheProfile,
  raceDay?: D1QueryCacheRaceDayContext,
  nowMs = Date.now(),
): number => {
  if (
    (profile === "running-style-race" || profile === "running-style-races") &&
    raceDay !== undefined
  ) {
    return getRaceDayTtlSeconds(raceDay, nowMs);
  }
  return PROFILE_DEFAULT_TTL_SECONDS[profile];
};

export const buildD1QueryCacheKey = (
  profile: D1QueryCacheProfile,
  keyParts: readonly unknown[],
): string => hashString(stableStringify([SHARED_D1_QUERY_CACHE_NAMESPACE, profile, keyParts]));

export const createD1QueryCacheRequest = (cacheKey: string): Request =>
  new Request(`${SHARED_D1_QUERY_CACHE_URL_BASE}${encodeURIComponent(cacheKey)}`);
