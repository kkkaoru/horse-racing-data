// Run with: bun run apps/sync-realtime-data-hot/scripts/seed-odds-fetch-state.ts
//
// PR3 cutover seed. Reads upcoming realtime_race_sources rows from the legacy
// sync-realtime-data Worker via POST /api/internal/export-race-sources-chunk
// (50 rows / 1s sleep) and forwards each one individually to the new
// sync-realtime-data-hot Worker via POST /api/internal/odds-fetch-state so
// races discovered before cutover are still odds-pollable.
// Newly discovered races after cutover are forwarded automatically by the
// old worker's forwardRaceSourceToHot helper.

const DEFAULT_BATCH_SIZE = 50;
const DEFAULT_SLEEP_MS = 1000;
const DEFAULT_RETRY_LIMIT = 3;
const DEFAULT_RETRY_BACKOFF_MS = 60_000;
const DEFAULT_CIRCUIT_PAUSE_MS = 30 * 60 * 1000;
const SATURATION_STATUS_CODE = 429;
const SEED_MAX_ROWID_STATE_KEY = "odds:migration:seed-max-rowid";

export interface ExportedRaceSourceRow {
  race_key: string;
  source: "jra" | "nar";
  race_start_at_jst: string;
  deba_url: string;
  odds_links_json: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  rowid: number;
}

export interface ExportRaceSourcesChunkResponse {
  rows: ExportedRaceSourceRow[];
  next_since_id: number;
  done: boolean;
}

export interface SeedOddsFetchStateConfig {
  oldWorkerUrl: string;
  newWorkerUrl: string;
  adminToken: string;
  internalToken: string;
  batchSize: number;
  sleepMs: number;
  retryLimit: number;
  retryBackoffMs: number;
  circuitPauseMs: number;
  fetchImpl: typeof fetch;
  sleepImpl: (ms: number) => Promise<void>;
}

export interface SeedOddsFetchStateResult {
  totalSeeded: number;
  maxRowid: number;
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const fetchChunkOnce = async (
  config: SeedOddsFetchStateConfig,
  sinceId: number,
): Promise<ExportRaceSourcesChunkResponse> => {
  const response = await config.fetchImpl(
    `${config.oldWorkerUrl}/api/internal/export-race-sources-chunk`,
    {
      body: JSON.stringify({
        batch_size: config.batchSize,
        since_id: sinceId,
      }),
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
    throw new Error(`export-race-sources-chunk failed: ${response.status}`);
  }
  return (await response.json()) as ExportRaceSourcesChunkResponse;
};

const fetchChunkWithBackoff = async (
  config: SeedOddsFetchStateConfig,
  sinceId: number,
): Promise<ExportRaceSourcesChunkResponse> => {
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

const forwardRow = async (
  config: SeedOddsFetchStateConfig,
  row: ExportedRaceSourceRow,
): Promise<void> => {
  const response = await config.fetchImpl(`${config.newWorkerUrl}/api/internal/odds-fetch-state`, {
    body: JSON.stringify({
      debaUrl: row.deba_url,
      kaisaiNen: row.kaisai_nen,
      kaisaiTsukihi: row.kaisai_tsukihi,
      keibajoCode: row.keibajo_code,
      oddsLinksJson: row.odds_links_json,
      raceBango: row.race_bango,
      raceKey: row.race_key,
      raceStartAtJst: row.race_start_at_jst,
      source: row.source,
    }),
    headers: {
      "content-type": "application/json",
      "x-pc-keiba-internal-token": config.internalToken,
    },
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(`odds-fetch-state forward failed: ${response.status}`);
  }
};

const forwardChunk = async (
  config: SeedOddsFetchStateConfig,
  rows: ExportedRaceSourceRow[],
): Promise<number> => {
  for (const row of rows) {
    await forwardRow(config, row);
  }
  return rows.length;
};

const saveMigrationState = async (
  config: SeedOddsFetchStateConfig,
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

export const seedOddsFetchState = async (
  config: SeedOddsFetchStateConfig,
): Promise<SeedOddsFetchStateResult> => {
  let sinceId = 0;
  let totalSeeded = 0;
  let consecutiveFailures = 0;
  while (true) {
    let chunk: ExportRaceSourcesChunkResponse;
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
    const seeded = await forwardChunk(config, chunk.rows);
    totalSeeded += seeded;
    sinceId = chunk.next_since_id;
    if (chunk.done) {
      break;
    }
    await config.sleepImpl(config.sleepMs);
  }
  await saveMigrationState(config, SEED_MAX_ROWID_STATE_KEY, String(sinceId));
  return { maxRowid: sinceId, totalSeeded };
};

const requireEnvVar = (name: string): string => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`missing env var: ${name}`);
  }
  return value;
};

export const buildDefaultConfig = (_now: Date): SeedOddsFetchStateConfig => ({
  adminToken: requireEnvVar("REALTIME_ADMIN_TOKEN"),
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

/* v8 ignore next 5 */
if (import.meta.main) {
  const config = buildDefaultConfig(new Date());
  const result = await seedOddsFetchState(config);
  console.log(
    `seed-odds-fetch-state done: ${result.totalSeeded} rows, maxRowid=${result.maxRowid}`,
  );
}
