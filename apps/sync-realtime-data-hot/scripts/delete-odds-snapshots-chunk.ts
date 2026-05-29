// Run with: CONFIRM_DELETE=1 bun run apps/sync-realtime-data-hot/scripts/delete-odds-snapshots-chunk.ts
//
// Phase F final step. Deletes legacy odds_snapshots rows (id <= odds:migration
// :b1-max-id) from the old sync-realtime-data D1 via POST /api/internal/delete-
// odds-chunk in 500 rows / 3s sleep batches, gated to JST 23-04 only so the
// daytime polling load on the legacy D1 is unaffected. CONFIRM_DELETE=1 is
// required as an operational safety belt — the script throws without it.

const DEFAULT_BATCH_SIZE = 500;
const DEFAULT_SLEEP_MS = 3000;
const DEFAULT_RETRY_LIMIT = 3;
const DEFAULT_RETRY_BACKOFF_MS = 60_000;
const DEFAULT_CIRCUIT_PAUSE_MS = 30 * 60 * 1000;
const NIGHT_WINDOW_START_HOUR = 23;
const NIGHT_WINDOW_END_HOUR = 4;
const SATURATION_STATUS_CODE = 429;

export interface DeleteChunkResponse {
  next_since_id: number;
  deleted: number;
  done: boolean;
}

export interface DeleteOddsSnapshotsChunkConfig {
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

export interface DeleteOddsSnapshotsChunkResult {
  totalDeleted: number;
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

const deleteChunkOnce = async (
  config: DeleteOddsSnapshotsChunkConfig,
  sinceId: number,
): Promise<DeleteChunkResponse> => {
  const response = await config.fetchImpl(`${config.oldWorkerUrl}/api/internal/delete-odds-chunk`, {
    body: JSON.stringify({
      batch_size: config.batchSize,
      since_id: sinceId,
      upper_bound_id: config.upperBoundId,
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
    throw new Error(`delete-odds-chunk failed: ${response.status}`);
  }
  return (await response.json()) as DeleteChunkResponse;
};

const deleteChunkWithBackoff = async (
  config: DeleteOddsSnapshotsChunkConfig,
  sinceId: number,
): Promise<DeleteChunkResponse> => {
  let lastError: unknown = new Error("retryLimit must be > 0");
  for (let attempt = 0; attempt < config.retryLimit; attempt += 1) {
    try {
      return await deleteChunkOnce(config, sinceId);
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
  config: DeleteOddsSnapshotsChunkConfig,
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

const loadResumeId = async (config: DeleteOddsSnapshotsChunkConfig): Promise<number> => {
  const response = await config.fetchImpl(
    `${config.newWorkerUrl}/api/internal/migration-state?key=f-last-deleted-id`,
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

export const deleteOddsSnapshotsChunked = async (
  config: DeleteOddsSnapshotsChunkConfig,
): Promise<DeleteOddsSnapshotsChunkResult> => {
  let sinceId = await loadResumeId(config);
  let totalDeleted = 0;
  let consecutiveFailures = 0;
  while (true) {
    if (!isWithinNightWindow(config.nowImpl())) {
      return { finalSinceId: sinceId, stoppedReason: "outside-night-window", totalDeleted };
    }
    if (sinceId >= config.upperBoundId) {
      return { finalSinceId: sinceId, stoppedReason: "upper-bound-reached", totalDeleted };
    }
    let chunk: DeleteChunkResponse;
    try {
      chunk = await deleteChunkWithBackoff(config, sinceId);
      consecutiveFailures = 0;
    } catch (error) {
      consecutiveFailures += 1;
      if (consecutiveFailures >= config.retryLimit) {
        await config.sleepImpl(config.circuitPauseMs);
        throw error;
      }
      continue;
    }
    totalDeleted += chunk.deleted;
    sinceId = chunk.next_since_id;
    await saveProgress(config, "f-last-deleted-id", String(sinceId));
    if (chunk.done) {
      return { finalSinceId: sinceId, stoppedReason: "completed", totalDeleted };
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

export const assertDeleteConfirmed = (): void => {
  if (process.env.CONFIRM_DELETE !== "1") {
    throw new Error("CONFIRM_DELETE=1 is required to run delete-odds-snapshots-chunk");
  }
};

export const buildDefaultConfig = async (
  now: Date,
  fetchImpl: typeof fetch,
): Promise<DeleteOddsSnapshotsChunkConfig> => {
  assertDeleteConfirmed();
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
  const result = await deleteOddsSnapshotsChunked(config);
  console.log(
    `delete-odds-snapshots-chunk: stopped=${result.stoppedReason}, deleted=${result.totalDeleted}, lastId=${result.finalSinceId}`,
  );
}
/* v8 ignore stop */
