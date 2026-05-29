// Run with: bun run apps/sync-realtime-data-hot/scripts/migrate-odds-recent.ts
//
// Reads odds_snapshots rows from the legacy sync-realtime-data Worker via
// POST /api/internal/export-odds-chunk (200 rows / 2s sleep) and forwards
// them to the new sync-realtime-data-hot Worker via
// POST /api/internal/import-odds-chunk. Only the most recent N days are
// migrated; the long-tail backfill runs separately in Phase B-3.

const DEFAULT_BATCH_SIZE = 200;
const DEFAULT_SLEEP_MS = 2000;
const DEFAULT_RETRY_LIMIT = 3;
const DEFAULT_RETRY_BACKOFF_MS = 60_000;
const DEFAULT_CIRCUIT_PAUSE_MS = 30 * 60 * 1000;
const DEFAULT_LOOKBACK_HOURS = 24;
const MS_PER_HOUR = 60 * 60 * 1000;
const SATURATION_STATUS_CODE = 429;

export interface ExportedOddsRow {
  id: number;
  race_key: string;
  fetched_at: string;
  odds_type: string;
  combination: string;
  odds: number | null;
  min_odds: number | null;
  max_odds: number | null;
  average_odds: number | null;
  rank: number | null;
}

export interface ExportChunkResponse {
  rows: ExportedOddsRow[];
  next_since_id: number;
  done: boolean;
}

export interface MigrateOddsRecentConfig {
  oldWorkerUrl: string;
  newWorkerUrl: string;
  adminToken: string;
  internalToken: string;
  batchSize: number;
  sleepMs: number;
  retryLimit: number;
  retryBackoffMs: number;
  circuitPauseMs: number;
  afterFetchedAt: string;
  fetchImpl: typeof fetch;
  sleepImpl: (ms: number) => Promise<void>;
}

export interface MigrateOddsRecentResult {
  totalInserted: number;
  maxId: number;
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const computeAfterFetchedAt = (now: Date, lookbackHours: number): string =>
  new Date(now.getTime() - lookbackHours * MS_PER_HOUR).toISOString();

const fetchChunkOnce = async (
  config: MigrateOddsRecentConfig,
  sinceId: number,
): Promise<ExportChunkResponse> => {
  const response = await config.fetchImpl(`${config.oldWorkerUrl}/api/internal/export-odds-chunk`, {
    body: JSON.stringify({
      after_fetched_at: config.afterFetchedAt,
      batch_size: config.batchSize,
      since_id: sinceId,
    }),
    headers: {
      authorization: `Bearer ${config.adminToken}`,
      "content-type": "application/json",
    },
    method: "POST",
  });
  if (response.status === SATURATION_STATUS_CODE) {
    throw new Error(`old worker returned ${SATURATION_STATUS_CODE}`);
  }
  if (!response.ok) {
    throw new Error(`export-odds-chunk failed: ${response.status}`);
  }
  return (await response.json()) as ExportChunkResponse;
};

const fetchChunkWithBackoff = async (
  config: MigrateOddsRecentConfig,
  sinceId: number,
): Promise<ExportChunkResponse> => {
  let lastError: unknown = new Error("retryLimit must be > 0");
  for (let attempt = 0; attempt < config.retryLimit; attempt += 1) {
    try {
      return await fetchChunkOnce(config, sinceId);
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

const importChunk = async (
  config: MigrateOddsRecentConfig,
  rows: ExportedOddsRow[],
): Promise<number> => {
  const response = await config.fetchImpl(`${config.newWorkerUrl}/api/internal/import-odds-chunk`, {
    body: JSON.stringify({ rows }),
    headers: {
      "content-type": "application/json",
      "x-pc-keiba-internal-token": config.internalToken,
    },
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(`import-odds-chunk failed: ${response.status}`);
  }
  const body = (await response.json()) as { inserted: number };
  return body.inserted;
};

const saveMigrationState = async (
  config: MigrateOddsRecentConfig,
  key: string,
  value: string,
): Promise<void> => {
  await config.fetchImpl(`${config.newWorkerUrl}/api/internal/migration-state`, {
    body: JSON.stringify({ key, value }),
    headers: {
      "content-type": "application/json",
      "x-pc-keiba-internal-token": config.internalToken,
    },
    method: "POST",
  });
};

export const migrateOddsRecent = async (
  config: MigrateOddsRecentConfig,
): Promise<MigrateOddsRecentResult> => {
  let sinceId = 0;
  let totalInserted = 0;
  let consecutiveFailures = 0;
  while (true) {
    let chunk: ExportChunkResponse;
    try {
      chunk = await fetchChunkWithBackoff(config, sinceId);
      consecutiveFailures = 0;
    } catch (error) {
      consecutiveFailures += 1;
      if (consecutiveFailures >= config.retryLimit) {
        await config.sleepImpl(config.circuitPauseMs);
        throw error;
      }
      continue;
    }
    if (chunk.rows.length === 0) {
      break;
    }
    const inserted = await importChunk(config, chunk.rows);
    totalInserted += inserted;
    sinceId = chunk.next_since_id;
    if (chunk.done) {
      break;
    }
    await config.sleepImpl(config.sleepMs);
  }
  await saveMigrationState(config, "b1-max-id", String(sinceId));
  return { maxId: sinceId, totalInserted };
};

const requireEnvVar = (name: string): string => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`missing env var: ${name}`);
  }
  return value;
};

const parseLookbackHours = (raw: string | undefined): number => {
  if (!raw) {
    return DEFAULT_LOOKBACK_HOURS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_LOOKBACK_HOURS;
};

export const buildDefaultConfig = (now: Date): MigrateOddsRecentConfig => ({
  adminToken: requireEnvVar("REALTIME_ADMIN_TOKEN"),
  afterFetchedAt: computeAfterFetchedAt(
    now,
    parseLookbackHours(process.env.MIGRATION_LOOKBACK_HOURS),
  ),
  batchSize: DEFAULT_BATCH_SIZE,
  circuitPauseMs: DEFAULT_CIRCUIT_PAUSE_MS,
  fetchImpl: globalThis.fetch,
  internalToken: requireEnvVar("PC_KEIBA_VIEWER_INTERNAL_TOKEN"),
  newWorkerUrl: requireEnvVar("NEW_WORKER_URL"),
  oldWorkerUrl: requireEnvVar("OLD_WORKER_URL"),
  retryBackoffMs: DEFAULT_RETRY_BACKOFF_MS,
  retryLimit: DEFAULT_RETRY_LIMIT,
  sleepImpl: sleep,
  sleepMs: DEFAULT_SLEEP_MS,
});

/* v8 ignore start */
if (import.meta.main) {
  const config = buildDefaultConfig(new Date());
  const result = await migrateOddsRecent(config);
  console.log(`migrate-odds-recent done: ${result.totalInserted} rows, maxId=${result.maxId}`);
}
/* v8 ignore stop */
