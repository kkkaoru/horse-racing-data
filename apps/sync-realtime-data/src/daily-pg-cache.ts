// run with: bun
// In-memory per-worker-instance memoization for date-keyed Neon Postgres
// fetches. Neon mirror data is replaced atomically once per day by
// `apps/local-postgresql/scripts/push-neon-sync.ts`, so a given (source,
// targetDate) tuple returns identical rows for the rest of the JST day after
// the push completes. Re-issuing the same query keeps the Neon compute clock
// alive — memoizing in worker memory removes that cost entirely.
//
// Correctness: invalidation is row-count based. The planner already triggers
// re-discovery when D1 vs Neon row counts disagree, so we expose
// `invalidateDailyPgCache(source, targetDate)` to drop a stale entry when
// callers detect a mismatch. Default TTL is 1 hour, well below the daily push
// cadence and short enough that stale data never reaches a full race-day.

const DEFAULT_TTL_MS = 60 * 60 * 1000;

export type DailyPgCacheSource = "jra" | "nar";

interface DailyPgCacheEntry<T> {
  expiresAtMs: number;
  rows: ReadonlyArray<T>;
}

interface DailyPgCacheKey {
  source: DailyPgCacheSource;
  targetDate: string;
}

const store = new Map<string, DailyPgCacheEntry<unknown>>();

const buildKey = (key: DailyPgCacheKey): string => `${key.source}:${key.targetDate}`;

const getTtlMs = (): number => {
  const raw = Number(process.env.SYNC_REALTIME_DAILY_PG_CACHE_TTL_MS);
  return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : DEFAULT_TTL_MS;
};

export const getCachedDailyPgRows = <T>(key: DailyPgCacheKey): ReadonlyArray<T> | null => {
  const entry = store.get(buildKey(key));
  if (!entry) return null;
  if (entry.expiresAtMs <= Date.now()) {
    store.delete(buildKey(key));
    return null;
  }
  return entry.rows as ReadonlyArray<T>;
};

export const setCachedDailyPgRows = <T>(key: DailyPgCacheKey, rows: ReadonlyArray<T>): void => {
  store.set(buildKey(key), {
    expiresAtMs: Date.now() + getTtlMs(),
    rows,
  });
};

export const invalidateDailyPgCache = (key: DailyPgCacheKey): void => {
  store.delete(buildKey(key));
};

export const clearDailyPgCache = (): void => {
  store.clear();
};

export const getDailyPgCacheSize = (): number => store.size;
