// Run with: bun run apps/sync-realtime-data-hot/scripts/backfill-old-odds.ts
//
// Phase B-3 of the migration. Reads the legacy odds_snapshots rows that
// predate the Phase B-1 window (id <= odds:migration:b1-max-id) and forwards
// them to the new D1 in 200-row chunks with 2s sleep between. The script is
// designed to be invoked from a JST 23-04 cron / launchd timer so it does not
// compete with the daytime polling load on the legacy D1.

const DEFAULT_BATCH_SIZE = 200;
const DEFAULT_SLEEP_MS = 2000;
const DEFAULT_RETRY_LIMIT = 3;
const DEFAULT_RETRY_BACKOFF_MS = 60_000;
const DEFAULT_CIRCUIT_PAUSE_MS = 30 * 60 * 1000;
const NIGHT_WINDOW_START_HOUR = 23;
const NIGHT_WINDOW_END_HOUR = 4;
const SATURATION_STATUS_CODE = 429;

export interface BackfillOddsRow {
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

export interface BackfillChunkResponse {
  rows: BackfillOddsRow[];
  next_since_id: number;
  done: boolean;
}

export interface BackfillOldOddsConfig {
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
  nowImpl: () => Date;
}

export interface BackfillOldOddsResult {
  totalInserted: number;
  finalSinceId: number;
  stoppedReason: "completed" | "outside-night-window" | "upper-bound-reached";
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const getJstHour = (date: Date): number => {
  const formatter = new Intl.DateTimeFormat("ja-JP-u-ca-gregory", {
    hour: "2-digit",
    hour12: false,
    timeZone: "Asia/Tokyo",
  });
  const hourPart = formatter.formatToParts(date).find((part) => part.type === "hour");
  return Number(hourPart!.value);
};

export const isWithinNightWindow = (date: Date): boolean => {
  const hour = getJstHour(date);
  return hour >= NIGHT_WINDOW_START_HOUR || hour < NIGHT_WINDOW_END_HOUR;
};

const fetchChunkOnce = async (
  config: BackfillOldOddsConfig,
  sinceId: number,
): Promise<BackfillChunkResponse> => {
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
  return (await response.json()) as BackfillChunkResponse;
};

const fetchChunkWithBackoff = async (
  config: BackfillOldOddsConfig,
  sinceId: number,
): Promise<BackfillChunkResponse> => {
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
  config: BackfillOldOddsConfig,
  rows: BackfillOddsRow[],
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

const saveProgress = async (
  config: BackfillOldOddsConfig,
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

const loadResumeId = async (config: BackfillOldOddsConfig): Promise<number> => {
  const response = await config.fetchImpl(
    `${config.newWorkerUrl}/api/internal/migration-state?key=b3-last-imported-id`,
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

const filterUpToUpperBound = (rows: BackfillOddsRow[], upperBound: number): BackfillOddsRow[] =>
  rows.filter((row) => row.id <= upperBound);

export const backfillOldOdds = async (
  config: BackfillOldOddsConfig,
): Promise<BackfillOldOddsResult> => {
  let sinceId = await loadResumeId(config);
  let totalInserted = 0;
  let consecutiveFailures = 0;
  while (true) {
    if (!isWithinNightWindow(config.nowImpl())) {
      return { finalSinceId: sinceId, stoppedReason: "outside-night-window", totalInserted };
    }
    if (sinceId >= config.upperBoundId) {
      return { finalSinceId: sinceId, stoppedReason: "upper-bound-reached", totalInserted };
    }
    let chunk: BackfillChunkResponse;
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
      return { finalSinceId: sinceId, stoppedReason: "completed", totalInserted };
    }
    const filtered = filterUpToUpperBound(chunk.rows, config.upperBoundId);
    if (filtered.length === 0) {
      return { finalSinceId: sinceId, stoppedReason: "upper-bound-reached", totalInserted };
    }
    const inserted = await importChunk(config, filtered);
    totalInserted += inserted;
    sinceId = filtered.at(-1)!.id;
    await saveProgress(config, "b3-last-imported-id", String(sinceId));
    if (chunk.done) {
      return { finalSinceId: sinceId, stoppedReason: "completed", totalInserted };
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
  now: Date,
  fetchImpl: typeof fetch,
): Promise<BackfillOldOddsConfig> => {
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
    nowImpl: () => now,
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
  const config = await buildDefaultConfig(new Date(), globalThis.fetch);
  const result = await backfillOldOdds(config);
  console.log(
    `backfill-old-odds: stopped=${result.stoppedReason}, inserted=${result.totalInserted}, lastId=${result.finalSinceId}`,
  );
}
/* v8 ignore stop */
