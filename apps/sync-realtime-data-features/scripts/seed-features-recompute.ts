// Run with: bun run apps/sync-realtime-data-features/scripts/seed-features-recompute.ts
//
// Phase B-1 seed script.
//
// Re-computes per-race Parquet features for the past N days (default 30) and
// uploads them to R2 via the new sync-realtime-data-features Worker.
//
// CRITICAL: this script does NOT touch the legacy D1 `daily_race_entries`
// table at any layer (Phase 0 plan rule 3). Race-key discovery instead goes
// through the legacy Worker endpoint
// POST /api/internal/list-race-keys-by-date-from-hyperdrive which queries
// `realtime_race_sources` over Hyperdrive (Postgres). The new worker then
// re-builds the features for each race using Hyperdrive directly and writes
// the Parquet bytes to R2.

const DEFAULT_SEED_DAYS = 30;
const DEFAULT_PER_RACE_SLEEP_MS = 500;
const DEFAULT_PER_DAY_SLEEP_MS = 5000;
const DEFAULT_RETRY_LIMIT = 3;
const DEFAULT_RETRY_BACKOFF_MS = 60_000;
const DEFAULT_CIRCUIT_PAUSE_MS = 30 * 60 * 1000;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const SATURATION_STATUS_CODE = 429;
const MIGRATION_PROGRESS_KEY = "b1-last-recomputed-race-key";

export interface RaceKeyParts {
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

export interface ListRaceKeysResponse {
  rows: { race_key: string }[];
}

export interface RecomputeResponse {
  raceKey: string;
  rowCount: number;
  r2Key: string;
  builtAt: string;
}

export interface SeedFeaturesRecomputeConfig {
  oldWorkerUrl: string;
  newFeaturesWorkerUrl: string;
  adminToken: string;
  internalToken: string;
  seedDays: number;
  perRaceSleepMs: number;
  perDaySleepMs: number;
  retryLimit: number;
  retryBackoffMs: number;
  circuitPauseMs: number;
  fetchImpl: typeof fetch;
  sleepImpl: (ms: number) => Promise<void>;
  now: Date;
}

export interface SeedFeaturesRecomputeResult {
  totalRaces: number;
  totalBuilt: number;
  totalFailed: number;
  lastRaceKey: string | null;
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const formatDateYyyymmdd = (date: Date): string => {
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
};

export const buildPastDateList = (now: Date, seedDays: number): string[] => {
  const list: string[] = [];
  for (let offset = 0; offset < seedDays; offset += 1) {
    const d = new Date(now.getTime() - offset * MS_PER_DAY);
    list.push(formatDateYyyymmdd(d));
  }
  return list;
};

const splitYyyymmdd = (yyyymmdd: string): { kaisaiNen: string; kaisaiTsukihi: string } => ({
  kaisaiNen: yyyymmdd.slice(0, 4),
  kaisaiTsukihi: yyyymmdd.slice(4, 8),
});

// race_key format from realtime_race_sources is "{source}:{nen}:{tsukihi}:{keibajoCode}:{raceBango}"
// e.g. "nar:2026:0529:30:08" or "jra:2026:0529:08:01".
export const parseRaceKey = (raceKey: string): RaceKeyParts | null => {
  const parts = raceKey.split(":");
  if (parts.length !== 5) {
    return null;
  }
  const source = parts[0];
  if (source !== "jra" && source !== "nar") {
    return null;
  }
  return {
    kaisaiNen: parts[1]!,
    kaisaiTsukihi: parts[2]!,
    keibajoCode: parts[3]!,
    raceBango: parts[4]!,
    source,
  };
};

const listRaceKeysOnce = async (
  config: SeedFeaturesRecomputeConfig,
  kaisaiNen: string,
  kaisaiTsukihi: string,
): Promise<ListRaceKeysResponse> => {
  const response = await config.fetchImpl(
    `${config.oldWorkerUrl}/api/internal/list-race-keys-by-date-from-hyperdrive`,
    {
      body: JSON.stringify({ kaisaiNen, kaisaiTsukihi }),
      headers: {
        authorization: `Bearer ${config.adminToken}`,
        "content-type": "application/json",
      },
      method: "POST",
    },
  );
  if (response.status === SATURATION_STATUS_CODE) {
    throw new Error(`old worker returned ${SATURATION_STATUS_CODE}`);
  }
  if (!response.ok) {
    throw new Error(`list-race-keys failed: ${response.status}`);
  }
  return (await response.json()) as ListRaceKeysResponse;
};

const listRaceKeysWithBackoff = async (
  config: SeedFeaturesRecomputeConfig,
  kaisaiNen: string,
  kaisaiTsukihi: string,
): Promise<ListRaceKeysResponse> => {
  let lastError: unknown = new Error("retryLimit must be > 0");
  for (let attempt = 0; attempt < config.retryLimit; attempt += 1) {
    try {
      return await listRaceKeysOnce(config, kaisaiNen, kaisaiTsukihi);
    } catch (error) {
      lastError = error;
      const isLastAttempt = attempt === config.retryLimit - 1;
      if (isLastAttempt) {
        throw error;
      }
      await config.sleepImpl(config.retryBackoffMs * (attempt + 1));
    }
  }
  throw lastError;
};

const recomputeRaceOnce = async (
  config: SeedFeaturesRecomputeConfig,
  raceKey: string,
  parts: RaceKeyParts,
): Promise<RecomputeResponse> => {
  const response = await config.fetchImpl(
    `${config.newFeaturesWorkerUrl}/api/internal/recompute-and-build-parquet`,
    {
      body: JSON.stringify({
        kaisaiNen: parts.kaisaiNen,
        kaisaiTsukihi: parts.kaisaiTsukihi,
        keibajoCode: parts.keibajoCode,
        raceBango: parts.raceBango,
        raceKey,
        source: parts.source,
      }),
      headers: {
        "content-type": "application/json",
        "x-pc-keiba-internal-token": config.internalToken,
      },
      method: "POST",
    },
  );
  if (response.status === SATURATION_STATUS_CODE) {
    throw new Error(`features worker returned ${SATURATION_STATUS_CODE}`);
  }
  if (!response.ok) {
    throw new Error(`recompute-and-build-parquet failed: ${response.status}`);
  }
  return (await response.json()) as RecomputeResponse;
};

const recomputeRaceWithBackoff = async (
  config: SeedFeaturesRecomputeConfig,
  raceKey: string,
  parts: RaceKeyParts,
): Promise<RecomputeResponse> => {
  let lastError: unknown = new Error("retryLimit must be > 0");
  for (let attempt = 0; attempt < config.retryLimit; attempt += 1) {
    try {
      return await recomputeRaceOnce(config, raceKey, parts);
    } catch (error) {
      lastError = error;
      const isLastAttempt = attempt === config.retryLimit - 1;
      if (isLastAttempt) {
        throw error;
      }
      await config.sleepImpl(config.retryBackoffMs * (attempt + 1));
    }
  }
  throw lastError;
};

const saveProgress = async (
  config: SeedFeaturesRecomputeConfig,
  raceKey: string,
): Promise<void> => {
  await config.fetchImpl(`${config.newFeaturesWorkerUrl}/api/internal/migration-state`, {
    body: JSON.stringify({ key: MIGRATION_PROGRESS_KEY, value: raceKey }),
    headers: {
      "content-type": "application/json",
      "x-pc-keiba-internal-token": config.internalToken,
    },
    method: "POST",
  });
};

interface DayProcessAccumulator {
  totalRaces: number;
  totalBuilt: number;
  totalFailed: number;
  lastRaceKey: string | null;
  consecutiveFailures: number;
}

const initialAccumulator = (): DayProcessAccumulator => ({
  consecutiveFailures: 0,
  lastRaceKey: null,
  totalBuilt: 0,
  totalFailed: 0,
  totalRaces: 0,
});

const processSingleRace = async (
  config: SeedFeaturesRecomputeConfig,
  raceKey: string,
  acc: DayProcessAccumulator,
): Promise<DayProcessAccumulator> => {
  acc.totalRaces += 1;
  const parts = parseRaceKey(raceKey);
  if (!parts) {
    acc.totalFailed += 1;
    return acc;
  }
  try {
    await recomputeRaceWithBackoff(config, raceKey, parts);
    acc.totalBuilt += 1;
    acc.lastRaceKey = raceKey;
    acc.consecutiveFailures = 0;
    await saveProgress(config, raceKey);
  } catch (error) {
    acc.totalFailed += 1;
    acc.consecutiveFailures += 1;
    if (acc.consecutiveFailures >= config.retryLimit) {
      await config.sleepImpl(config.circuitPauseMs);
      throw error;
    }
  }
  await config.sleepImpl(config.perRaceSleepMs);
  return acc;
};

const processOneDay = async (
  config: SeedFeaturesRecomputeConfig,
  yyyymmdd: string,
  acc: DayProcessAccumulator,
): Promise<DayProcessAccumulator> => {
  const { kaisaiNen, kaisaiTsukihi } = splitYyyymmdd(yyyymmdd);
  const list = await listRaceKeysWithBackoff(config, kaisaiNen, kaisaiTsukihi);
  let next = acc;
  for (const row of list.rows) {
    next = await processSingleRace(config, row.race_key, next);
  }
  return next;
};

export const seedFeaturesRecompute = async (
  config: SeedFeaturesRecomputeConfig,
): Promise<SeedFeaturesRecomputeResult> => {
  const dates = buildPastDateList(config.now, config.seedDays);
  let acc = initialAccumulator();
  for (const yyyymmdd of dates) {
    acc = await processOneDay(config, yyyymmdd, acc);
    await config.sleepImpl(config.perDaySleepMs);
  }
  return {
    lastRaceKey: acc.lastRaceKey,
    totalBuilt: acc.totalBuilt,
    totalFailed: acc.totalFailed,
    totalRaces: acc.totalRaces,
  };
};

const requireEnvVar = (name: string): string => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`missing env var: ${name}`);
  }
  return value;
};

const parseSeedDays = (raw: string | undefined): number => {
  if (!raw) {
    return DEFAULT_SEED_DAYS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_SEED_DAYS;
};

const resolveInternalToken = (): string => {
  const explicit = process.env.FEATURES_INTERNAL_TOKEN;
  if (explicit) {
    return explicit;
  }
  return requireEnvVar("PC_KEIBA_VIEWER_INTERNAL_TOKEN");
};

export const buildDefaultConfig = (now: Date): SeedFeaturesRecomputeConfig => ({
  adminToken: requireEnvVar("REALTIME_ADMIN_TOKEN"),
  circuitPauseMs: DEFAULT_CIRCUIT_PAUSE_MS,
  fetchImpl: globalThis.fetch,
  internalToken: resolveInternalToken(),
  newFeaturesWorkerUrl: requireEnvVar("NEW_FEATURES_WORKER_URL"),
  now,
  oldWorkerUrl: requireEnvVar("OLD_WORKER_URL"),
  perDaySleepMs: DEFAULT_PER_DAY_SLEEP_MS,
  perRaceSleepMs: DEFAULT_PER_RACE_SLEEP_MS,
  retryBackoffMs: DEFAULT_RETRY_BACKOFF_MS,
  retryLimit: DEFAULT_RETRY_LIMIT,
  seedDays: parseSeedDays(process.env.SEED_DAYS),
  sleepImpl: sleep,
});

/* v8 ignore next 5 */
if (import.meta.main) {
  const config = buildDefaultConfig(new Date());
  const result = await seedFeaturesRecompute(config);
  console.log(`seed-features-recompute done: ${JSON.stringify(result)}`);
}
