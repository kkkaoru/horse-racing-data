// Run with: CONFIRM_DELETE=1 bun run apps/sync-realtime-data/scripts/run-delete-race-running-styles-chunk.ts
//
// Phase F final step for race_running_styles cleanup. Deletes legacy
// race_running_styles rows (rowid > resume cursor) from the old
// sync-realtime-data D1 via POST /api/internal/delete-race-running-styles-chunk
// in 500 rows / 3s sleep batches, gated to JST 23-04 only so the daytime
// polling load on the legacy D1 is unaffected. CONFIRM_DELETE=1 is required as
// an operational safety belt — the script throws without it. The resume cursor
// is persisted to KV `features:cleanup:race-running-styles-cursor` through the
// new features worker's /api/internal/migration-state endpoint.
//
// Old-worker D1 race_running_styles writes have already been stopped at this
// point (Phase F precondition), so max(rowid) is static and the script can
// scan-and-delete safely.

const DEFAULT_BATCH_SIZE = 500;
const DEFAULT_SLEEP_MS = 3000;
const DEFAULT_RETRY_LIMIT = 3;
const DEFAULT_RETRY_BACKOFF_MS = 60_000;
const DEFAULT_CIRCUIT_PAUSE_MS = 30 * 60 * 1000;
const NIGHT_WINDOW_START_HOUR = 23;
const NIGHT_WINDOW_END_HOUR = 5;
const SATURATION_STATUS_CODE = 429;
const CURSOR_KEY = "features:cleanup:race-running-styles-cursor";

export interface DeleteRaceRunningStylesChunkResponse {
  deletedRowCount: number;
  nextSinceRowid: number;
}

export interface DeleteRaceRunningStylesChunkConfig {
  oldWorkerUrl: string;
  featuresWorkerUrl: string;
  adminToken: string;
  internalToken: string;
  batchSize: number;
  sleepMs: number;
  retryLimit: number;
  retryBackoffMs: number;
  circuitPauseMs: number;
  fetchImpl: typeof fetch;
  sleepImpl: (ms: number) => Promise<void>;
  nowImpl: () => Date;
}

export interface DeleteRaceRunningStylesChunkResult {
  totalDeleted: number;
  finalSinceRowid: number;
  stoppedReason: "completed" | "outside-night-window";
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const getJstHour = (date: Date): number => {
  const formatter = new Intl.DateTimeFormat("ja-JP-u-ca-gregory", {
    hour: "2-digit",
    hour12: false,
    timeZone: "Asia/Tokyo",
  });
  // `hour: "2-digit"` guarantees a hour part — no defensive fallback needed.
  const hourPart = formatter.formatToParts(date).find((part) => part.type === "hour")!;
  return Number(hourPart.value);
};

// Night window: JST 23:00 - 04:59 (inclusive). Re-checked at the start of every
// chunk so a long run rolls off the moment 05:00 hits.
export const isWithinNightWindow = (date: Date): boolean => {
  const hour = getJstHour(date);
  return hour >= NIGHT_WINDOW_START_HOUR || hour < NIGHT_WINDOW_END_HOUR;
};

const deleteChunkOnce = async (
  config: DeleteRaceRunningStylesChunkConfig,
  sinceRowid: number,
): Promise<DeleteRaceRunningStylesChunkResponse> => {
  const response = await config.fetchImpl(
    `${config.oldWorkerUrl}/api/internal/delete-race-running-styles-chunk`,
    {
      body: JSON.stringify({
        chunk_size: config.batchSize,
        since_rowid: sinceRowid,
      }),
      headers: {
        "content-type": "application/json",
        "x-pc-keiba-internal-token": config.internalToken,
      },
      method: "POST",
    },
  );
  if (response.status === SATURATION_STATUS_CODE) {
    throw new Error(`old worker returned ${SATURATION_STATUS_CODE}`);
  }
  if (!response.ok) {
    throw new Error(`delete-race-running-styles-chunk failed: ${response.status}`);
  }
  return (await response.json()) as DeleteRaceRunningStylesChunkResponse;
};

const deleteChunkWithBackoff = async (
  config: DeleteRaceRunningStylesChunkConfig,
  sinceRowid: number,
): Promise<DeleteRaceRunningStylesChunkResponse> => {
  let lastError: unknown = new Error("retryLimit must be > 0");
  for (let attempt = 0; attempt < config.retryLimit; attempt += 1) {
    try {
      return await deleteChunkOnce(config, sinceRowid);
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

const saveCursor = async (
  config: DeleteRaceRunningStylesChunkConfig,
  value: string,
): Promise<void> => {
  await config.fetchImpl(`${config.featuresWorkerUrl}/api/internal/migration-state`, {
    body: JSON.stringify({ key: CURSOR_KEY, value }),
    headers: {
      "content-type": "application/json",
      "x-pc-keiba-internal-token": config.internalToken,
    },
    method: "POST",
  });
};

const loadResumeCursor = async (config: DeleteRaceRunningStylesChunkConfig): Promise<number> => {
  const response = await config.fetchImpl(
    `${config.featuresWorkerUrl}/api/internal/migration-state?key=${CURSOR_KEY}`,
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
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
};

// Main loop. Three-consecutive-fail circuit breaker pauses 30 min before
// rethrowing — the operator can re-run the script after looking at the cause.
export const deleteRaceRunningStylesChunked = async (
  config: DeleteRaceRunningStylesChunkConfig,
): Promise<DeleteRaceRunningStylesChunkResult> => {
  let sinceRowid = await loadResumeCursor(config);
  let totalDeleted = 0;
  let consecutiveFailures = 0;
  while (true) {
    if (!isWithinNightWindow(config.nowImpl())) {
      return { finalSinceRowid: sinceRowid, stoppedReason: "outside-night-window", totalDeleted };
    }
    let chunk: DeleteRaceRunningStylesChunkResponse;
    try {
      chunk = await deleteChunkWithBackoff(config, sinceRowid);
      consecutiveFailures = 0;
    } catch (error) {
      consecutiveFailures += 1;
      if (consecutiveFailures >= config.retryLimit) {
        await config.sleepImpl(config.circuitPauseMs);
        throw error;
      }
      continue;
    }
    totalDeleted += chunk.deletedRowCount;
    sinceRowid = chunk.nextSinceRowid;
    await saveCursor(config, String(sinceRowid));
    if (chunk.deletedRowCount < config.batchSize) {
      return { finalSinceRowid: sinceRowid, stoppedReason: "completed", totalDeleted };
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

export const readPositiveIntEnv = (name: string, fallback: number, minimum: 0 | 1): number => {
  const raw = process.env[name];
  if (raw === undefined || raw === "") {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < minimum) {
    throw new Error(`${name} must be a positive integer (>= ${minimum})`);
  }
  return parsed;
};

export const assertDeleteConfirmed = (): void => {
  if (process.env.CONFIRM_DELETE !== "1") {
    throw new Error("Refusing to delete: set CONFIRM_DELETE=1 to acknowledge irreversibility");
  }
};

export const buildDefaultConfig = (
  now: Date,
  fetchImpl: typeof fetch,
): DeleteRaceRunningStylesChunkConfig => {
  assertDeleteConfirmed();
  return {
    adminToken: requireEnvVar("REALTIME_ADMIN_TOKEN"),
    batchSize: readPositiveIntEnv("CHUNK_SIZE", DEFAULT_BATCH_SIZE, 1),
    circuitPauseMs: readPositiveIntEnv("CIRCUIT_PAUSE_MS", DEFAULT_CIRCUIT_PAUSE_MS, 0),
    featuresWorkerUrl: requireEnvVar("FEATURES_WORKER_URL"),
    fetchImpl,
    internalToken: requireEnvVar("PC_KEIBA_VIEWER_INTERNAL_TOKEN"),
    nowImpl: () => now,
    oldWorkerUrl: requireEnvVar("OLD_WORKER_URL"),
    retryBackoffMs: readPositiveIntEnv("RETRY_BACKOFF_MS", DEFAULT_RETRY_BACKOFF_MS, 0),
    retryLimit: readPositiveIntEnv("RETRY_LIMIT", DEFAULT_RETRY_LIMIT, 1),
    sleepImpl: sleep,
    sleepMs: readPositiveIntEnv("CHUNK_DELAY_MS", DEFAULT_SLEEP_MS, 0),
  };
};
