// Run with: bun run apps/sync-realtime-data-hot/scripts/export-old-odds-to-r2-final.ts
//
// Phase F final-backup of the migration. Reads every legacy odds_snapshots row
// (id <= odds:migration:b1-max-id) from the old sync-realtime-data Worker via
// POST /api/internal/export-odds-chunk (200 rows / 2s sleep) and forwards them
// to the new sync-realtime-data-hot Worker via POST /api/internal/r2-archive-
// rows. The new Worker groups rows by (race_key, odds_type, YYYY-MM-DD) and
// PUTs them under the R2 prefix `odds-final-backup-old-d1/`. This is the read-
// only safety net before the old D1 rows are actually deleted.

const DEFAULT_BATCH_SIZE = 200;
const DEFAULT_SLEEP_MS = 2000;
const DEFAULT_RETRY_LIMIT = 3;
const DEFAULT_RETRY_BACKOFF_MS = 60_000;
const DEFAULT_CIRCUIT_PAUSE_MS = 30 * 60 * 1000;
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

export interface ExportFinalBackupConfig {
  oldWorkerUrl: string;
  newWorkerUrl: string;
  adminToken: string;
  internalToken: string;
  batchSize: number;
  sleepMs: number;
  retryLimit: number;
  retryBackoffMs: number;
  circuitPauseMs: number;
  upperBoundId: number;
  fetchImpl: typeof fetch;
  sleepImpl: (ms: number) => Promise<void>;
}

export interface ExportFinalBackupResult {
  totalRows: number;
  totalGroups: number;
  finalSinceId: number;
  stoppedReason: "completed" | "upper-bound-reached";
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const fetchChunkOnce = async (
  config: ExportFinalBackupConfig,
  sinceId: number,
): Promise<ExportChunkResponse> => {
  const response = await config.fetchImpl(`${config.oldWorkerUrl}/api/internal/export-odds-chunk`, {
    body: JSON.stringify({ batch_size: config.batchSize, since_id: sinceId }),
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
  config: ExportFinalBackupConfig,
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

interface ArchiveRowsResponseBody {
  groups: number;
  rows: number;
}

const archiveRowsToR2 = async (
  config: ExportFinalBackupConfig,
  rows: ExportedOddsRow[],
): Promise<ArchiveRowsResponseBody> => {
  const response = await config.fetchImpl(`${config.newWorkerUrl}/api/internal/r2-archive-rows`, {
    body: JSON.stringify({ rows }),
    headers: {
      "content-type": "application/json",
      "x-pc-keiba-internal-token": config.internalToken,
    },
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(`r2-archive-rows failed: ${response.status}`);
  }
  return (await response.json()) as ArchiveRowsResponseBody;
};

const saveProgress = async (
  config: ExportFinalBackupConfig,
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

const loadResumeId = async (config: ExportFinalBackupConfig): Promise<number> => {
  const response = await config.fetchImpl(
    `${config.newWorkerUrl}/api/internal/migration-state?key=f-last-archived-id`,
    { headers: { "x-pc-keiba-internal-token": config.internalToken } },
  );
  if (!response.ok) {
    return 0;
  }
  const body = (await response.json()) as { value: string | null };
  if (!body.value) {
    return 0;
  }
  const parsed = Number(body.value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
};

const filterUpToUpperBound = (rows: ExportedOddsRow[], upperBound: number): ExportedOddsRow[] =>
  rows.filter((row) => row.id <= upperBound);

export const exportOldOddsToR2Final = async (
  config: ExportFinalBackupConfig,
): Promise<ExportFinalBackupResult> => {
  let sinceId = await loadResumeId(config);
  let totalRows = 0;
  let totalGroups = 0;
  let consecutiveFailures = 0;
  while (true) {
    if (sinceId >= config.upperBoundId) {
      return {
        finalSinceId: sinceId,
        stoppedReason: "upper-bound-reached",
        totalGroups,
        totalRows,
      };
    }
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
      return { finalSinceId: sinceId, stoppedReason: "completed", totalGroups, totalRows };
    }
    const filtered = filterUpToUpperBound(chunk.rows, config.upperBoundId);
    if (filtered.length === 0) {
      return {
        finalSinceId: sinceId,
        stoppedReason: "upper-bound-reached",
        totalGroups,
        totalRows,
      };
    }
    const archived = await archiveRowsToR2(config, filtered);
    totalRows += archived.rows;
    totalGroups += archived.groups;
    sinceId = filtered.at(-1)!.id;
    await saveProgress(config, "f-last-archived-id", String(sinceId));
    if (chunk.done) {
      return { finalSinceId: sinceId, stoppedReason: "completed", totalGroups, totalRows };
    }
    await config.sleepImpl(config.sleepMs);
  }
};

const requireEnvVar = (name: string): string => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`missing env var: ${name}`);
  }
  return value;
};

const loadUpperBound = async (
  newWorkerUrl: string,
  internalToken: string,
  fetchImpl: typeof fetch,
): Promise<number> => {
  const response = await fetchImpl(`${newWorkerUrl}/api/internal/migration-state?key=b1-max-id`, {
    headers: { "x-pc-keiba-internal-token": internalToken },
  });
  if (!response.ok) {
    throw new Error(`failed to read b1-max-id: ${response.status}`);
  }
  const body = (await response.json()) as { value: string | null };
  if (!body.value) {
    throw new Error("b1-max-id not set; run migrate-odds-recent first");
  }
  const parsed = Number(body.value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`b1-max-id is not a positive integer: ${body.value}`);
  }
  return parsed;
};

export const buildDefaultConfig = async (
  fetchImpl: typeof fetch,
): Promise<ExportFinalBackupConfig> => {
  const internalToken = requireEnvVar("PC_KEIBA_VIEWER_INTERNAL_TOKEN");
  const newWorkerUrl = requireEnvVar("NEW_WORKER_URL");
  const upperBoundId = await loadUpperBound(newWorkerUrl, internalToken, fetchImpl);
  return {
    adminToken: requireEnvVar("REALTIME_ADMIN_TOKEN"),
    batchSize: DEFAULT_BATCH_SIZE,
    circuitPauseMs: DEFAULT_CIRCUIT_PAUSE_MS,
    fetchImpl,
    internalToken,
    newWorkerUrl,
    oldWorkerUrl: requireEnvVar("OLD_WORKER_URL"),
    retryBackoffMs: DEFAULT_RETRY_BACKOFF_MS,
    retryLimit: DEFAULT_RETRY_LIMIT,
    sleepImpl: sleep,
    sleepMs: DEFAULT_SLEEP_MS,
    upperBoundId,
  };
};

/* v8 ignore start */
if (import.meta.main) {
  const config = await buildDefaultConfig(globalThis.fetch);
  const result = await exportOldOddsToR2Final(config);
  console.log(
    `export-old-odds-to-r2-final: stopped=${result.stoppedReason}, rows=${result.totalRows}, groups=${result.totalGroups}, lastId=${result.finalSinceId}`,
  );
}
/* v8 ignore stop */
