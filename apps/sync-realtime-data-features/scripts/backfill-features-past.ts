// Run with: bun run apps/sync-realtime-data-features/scripts/run-backfill-features-past.ts
//
// Phase B-3 nightly backfill script.
//
// Re-computes per-race Parquet features for days OLDER than the Phase B-1
// floor (now - b1FloorDays) and uploads them to R2 via the new
// sync-realtime-data-features Worker. Iterates day-by-day from the resume
// point (KV `features:migration:b3-last-seeded-date`) backwards (newest old
// day -> older). Capped at max-days-per-run (default 7) so one nightly run
// touches a bounded slice.
//
// CRITICAL: this script does NOT touch the legacy D1 `daily_race_entries`
// table (Phase 0 plan rule 3). Race-key discovery goes through the old
// Worker endpoint POST /api/internal/list-race-keys-by-date-from-hyperdrive
// (queries realtime_race_sources over Hyperdrive). The new worker then
// re-builds features for each race and writes Parquet bytes to R2.
//
// Night-window guard: only runs during JST 23, 00, 01, 02, 03, 04. The guard
// is re-checked at the start of every race iteration so a long run that
// crosses 05:00 JST stops cleanly with stoppedReason = "outside-night-window".

const DEFAULT_MAX_DAYS_PER_RUN = 7;
const DEFAULT_B1_FLOOR_DAYS = 30;
const DEFAULT_PER_RACE_SLEEP_MS = 500;
const DEFAULT_PER_DAY_SLEEP_MS = 5000;
const DEFAULT_RETRY_LIMIT = 3;
const DEFAULT_RETRY_BACKOFF_START_MS = 2000;
const DEFAULT_RETRY_BACKOFF_MAX_MS = 60_000;
const DEFAULT_CIRCUIT_PAUSE_MS = 30 * 60 * 1000;
const RETRY_BACKOFF_FACTOR = 2;
const MS_PER_DAY = 24 * 60 * 60 * 1000;
const MS_PER_MINUTE = 60_000;
const JST_OFFSET_MINUTES = 9 * 60;
const SATURATION_STATUS_CODE = 429;
const MIGRATION_PROGRESS_KEY = "b3-last-seeded-date";
const NIGHT_WINDOW_HOURS_JST: number[] = [23, 0, 1, 2, 3, 4];
const NIGHT_WINDOW_HOURS_SET: Set<number> = new Set(NIGHT_WINDOW_HOURS_JST);
const STOPPED_REASON_OUTSIDE_NIGHT_WINDOW = "outside-night-window";
const STOPPED_REASON_MAX_DAYS_REACHED = "max-days-reached";
const STOPPED_REASON_COMPLETED = "completed";

export type BackfillStoppedReason =
  | typeof STOPPED_REASON_OUTSIDE_NIGHT_WINDOW
  | typeof STOPPED_REASON_MAX_DAYS_REACHED
  | typeof STOPPED_REASON_COMPLETED;

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

export interface MigrationStateGetResponse {
  key: string;
  value: string | null;
}

export interface BackfillFeaturesPastConfig {
  oldWorkerUrl: string;
  newFeaturesWorkerUrl: string;
  adminToken: string;
  internalToken: string;
  maxDaysPerRun: number;
  b1FloorDays: number;
  perRaceSleepMs: number;
  perDaySleepMs: number;
  retryLimit: number;
  retryBackoffStartMs: number;
  retryBackoffMaxMs: number;
  circuitPauseMs: number;
  fetchImpl: typeof fetch;
  sleepImpl: (ms: number) => Promise<void>;
  nowProvider: () => Date;
}

export interface BackfillFeaturesPastResult {
  stoppedReason: BackfillStoppedReason;
  totalDays: number;
  totalRaces: number;
  totalRows: number;
  finalDate: string | null;
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const formatDateYyyymmdd = (date: Date): string => {
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
};

const parseYyyymmdd = (yyyymmdd: string): Date => {
  const year = Number(yyyymmdd.slice(0, 4));
  const month = Number(yyyymmdd.slice(4, 6));
  const day = Number(yyyymmdd.slice(6, 8));
  return new Date(Date.UTC(year, month - 1, day));
};

const splitYyyymmdd = (yyyymmdd: string): { kaisaiNen: string; kaisaiTsukihi: string } => ({
  kaisaiNen: yyyymmdd.slice(0, 4),
  kaisaiTsukihi: yyyymmdd.slice(4, 8),
});

export const getJstHour = (now: Date): number => {
  const jst = new Date(now.getTime() + JST_OFFSET_MINUTES * MS_PER_MINUTE);
  return jst.getUTCHours();
};

export const isWithinNightWindow = (now: Date): boolean =>
  NIGHT_WINDOW_HOURS_SET.has(getJstHour(now));

// b1OldestDate = (now - (b1FloorDays - 1) days). The B-3 cold-start point is
// the day BEFORE that, i.e. (now - b1FloorDays days).
export const computeColdStartDate = (now: Date, b1FloorDays: number): string => {
  const coldStart = new Date(now.getTime() - b1FloorDays * MS_PER_DAY);
  return formatDateYyyymmdd(coldStart);
};

export const previousDate = (yyyymmdd: string): string => {
  const current = parseYyyymmdd(yyyymmdd);
  const prev = new Date(current.getTime() - MS_PER_DAY);
  return formatDateYyyymmdd(prev);
};

// race_key format: "{source}:{nen}:{tsukihi}:{keibajoCode}:{raceBango}"
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

const computeBackoffMs = (config: BackfillFeaturesPastConfig, attempt: number): number => {
  const exponent = Math.pow(RETRY_BACKOFF_FACTOR, attempt);
  const candidate = config.retryBackoffStartMs * exponent;
  return Math.min(candidate, config.retryBackoffMaxMs);
};

export const loadResumeDate = async (
  config: BackfillFeaturesPastConfig,
): Promise<string | null> => {
  const response = await config.fetchImpl(
    `${config.newFeaturesWorkerUrl}/api/internal/migration-state?key=${MIGRATION_PROGRESS_KEY}`,
    {
      headers: { "x-pc-keiba-internal-token": config.internalToken },
      method: "GET",
    },
  );
  if (!response.ok) {
    throw new Error(`load-resume-date failed: ${response.status}`);
  }
  const body = (await response.json()) as MigrationStateGetResponse;
  return body.value;
};

export const resolveStartDate = async (config: BackfillFeaturesPastConfig): Promise<string> => {
  const persisted = await loadResumeDate(config);
  if (persisted) {
    return previousDate(persisted);
  }
  return computeColdStartDate(config.nowProvider(), config.b1FloorDays);
};

const listRaceKeysOnce = async (
  config: BackfillFeaturesPastConfig,
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
  config: BackfillFeaturesPastConfig,
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
      await config.sleepImpl(computeBackoffMs(config, attempt));
    }
  }
  throw lastError;
};

const recomputeRaceOnce = async (
  config: BackfillFeaturesPastConfig,
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
  config: BackfillFeaturesPastConfig,
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
      await config.sleepImpl(computeBackoffMs(config, attempt));
    }
  }
  throw lastError;
};

const saveProgress = async (
  config: BackfillFeaturesPastConfig,
  yyyymmdd: string,
): Promise<void> => {
  await config.fetchImpl(`${config.newFeaturesWorkerUrl}/api/internal/migration-state`, {
    body: JSON.stringify({ key: MIGRATION_PROGRESS_KEY, value: yyyymmdd }),
    headers: {
      "content-type": "application/json",
      "x-pc-keiba-internal-token": config.internalToken,
    },
    method: "POST",
  });
};

interface DayAccumulator {
  totalRaces: number;
  totalRows: number;
  consecutiveFailures: number;
  nightWindowExited: boolean;
}

const newDayAccumulator = (): DayAccumulator => ({
  consecutiveFailures: 0,
  nightWindowExited: false,
  totalRaces: 0,
  totalRows: 0,
});

const processSingleRace = async (
  config: BackfillFeaturesPastConfig,
  raceKey: string,
  acc: DayAccumulator,
): Promise<DayAccumulator> => {
  if (!isWithinNightWindow(config.nowProvider())) {
    acc.nightWindowExited = true;
    return acc;
  }
  acc.totalRaces += 1;
  const parts = parseRaceKey(raceKey);
  if (!parts) {
    return acc;
  }
  try {
    const built = await recomputeRaceWithBackoff(config, raceKey, parts);
    acc.totalRows += built.rowCount;
    acc.consecutiveFailures = 0;
  } catch (error) {
    acc.consecutiveFailures += 1;
    if (acc.consecutiveFailures >= config.retryLimit) {
      await config.sleepImpl(config.circuitPauseMs);
      throw error;
    }
  }
  await config.sleepImpl(config.perRaceSleepMs);
  return acc;
};

interface ProcessDayResult {
  acc: DayAccumulator;
  exited: boolean;
}

const processOneDay = async (
  config: BackfillFeaturesPastConfig,
  yyyymmdd: string,
): Promise<ProcessDayResult> => {
  const { kaisaiNen, kaisaiTsukihi } = splitYyyymmdd(yyyymmdd);
  const list = await listRaceKeysWithBackoff(config, kaisaiNen, kaisaiTsukihi);
  let acc = newDayAccumulator();
  for (const row of list.rows) {
    acc = await processSingleRace(config, row.race_key, acc);
    if (acc.nightWindowExited) {
      return { acc, exited: true };
    }
  }
  return { acc, exited: false };
};

interface RunAccumulator {
  totalDays: number;
  totalRaces: number;
  totalRows: number;
  finalDate: string | null;
  currentDate: string;
}

const initialRunAccumulator = (startDate: string): RunAccumulator => ({
  currentDate: startDate,
  finalDate: null,
  totalDays: 0,
  totalRaces: 0,
  totalRows: 0,
});

const advanceRun = (run: RunAccumulator, dayResult: ProcessDayResult): RunAccumulator => ({
  currentDate: previousDate(run.currentDate),
  finalDate: run.currentDate,
  totalDays: run.totalDays + 1,
  totalRaces: run.totalRaces + dayResult.acc.totalRaces,
  totalRows: run.totalRows + dayResult.acc.totalRows,
});

const mergePartialDay = (run: RunAccumulator, dayResult: ProcessDayResult): RunAccumulator => ({
  currentDate: run.currentDate,
  finalDate: run.finalDate,
  totalDays: run.totalDays,
  totalRaces: run.totalRaces + dayResult.acc.totalRaces,
  totalRows: run.totalRows + dayResult.acc.totalRows,
});

const finishWith = (
  run: RunAccumulator,
  stoppedReason: BackfillStoppedReason,
): BackfillFeaturesPastResult => ({
  finalDate: run.finalDate,
  stoppedReason,
  totalDays: run.totalDays,
  totalRaces: run.totalRaces,
  totalRows: run.totalRows,
});

export const backfillFeaturesPast = async (
  config: BackfillFeaturesPastConfig,
): Promise<BackfillFeaturesPastResult> => {
  if (!isWithinNightWindow(config.nowProvider())) {
    return finishWith(initialRunAccumulator(""), STOPPED_REASON_OUTSIDE_NIGHT_WINDOW);
  }
  const startDate = await resolveStartDate(config);
  let run = initialRunAccumulator(startDate);
  while (run.totalDays < config.maxDaysPerRun) {
    if (!isWithinNightWindow(config.nowProvider())) {
      return finishWith(run, STOPPED_REASON_OUTSIDE_NIGHT_WINDOW);
    }
    const dayResult = await processOneDay(config, run.currentDate);
    if (dayResult.exited) {
      return finishWith(mergePartialDay(run, dayResult), STOPPED_REASON_OUTSIDE_NIGHT_WINDOW);
    }
    await saveProgress(config, run.currentDate);
    run = advanceRun(run, dayResult);
    if (run.totalDays >= config.maxDaysPerRun) {
      return finishWith(run, STOPPED_REASON_MAX_DAYS_REACHED);
    }
    await config.sleepImpl(config.perDaySleepMs);
  }
  return finishWith(run, STOPPED_REASON_COMPLETED);
};

const requireEnvVar = (name: string): string => {
  const value = process.env[name];
  if (!value) {
    throw new Error(`missing env var: ${name}`);
  }
  return value;
};

const parsePositiveIntEnv = (raw: string | undefined, fallback: number): number => {
  if (!raw) {
    return fallback;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const resolveInternalToken = (): string => {
  const explicit = process.env.FEATURES_INTERNAL_TOKEN;
  if (explicit) {
    return explicit;
  }
  return requireEnvVar("PC_KEIBA_VIEWER_INTERNAL_TOKEN");
};

export const buildDefaultConfig = (nowProvider: () => Date): BackfillFeaturesPastConfig => ({
  adminToken: requireEnvVar("REALTIME_ADMIN_TOKEN"),
  b1FloorDays: parsePositiveIntEnv(process.env.B1_FLOOR_DAYS, DEFAULT_B1_FLOOR_DAYS),
  circuitPauseMs: DEFAULT_CIRCUIT_PAUSE_MS,
  fetchImpl: globalThis.fetch,
  internalToken: resolveInternalToken(),
  maxDaysPerRun: parsePositiveIntEnv(process.env.BACKFILL_MAX_DAYS, DEFAULT_MAX_DAYS_PER_RUN),
  newFeaturesWorkerUrl: requireEnvVar("NEW_FEATURES_WORKER_URL"),
  nowProvider,
  oldWorkerUrl: requireEnvVar("OLD_WORKER_URL"),
  perDaySleepMs: DEFAULT_PER_DAY_SLEEP_MS,
  perRaceSleepMs: DEFAULT_PER_RACE_SLEEP_MS,
  retryBackoffMaxMs: DEFAULT_RETRY_BACKOFF_MAX_MS,
  retryBackoffStartMs: DEFAULT_RETRY_BACKOFF_START_MS,
  retryLimit: DEFAULT_RETRY_LIMIT,
  sleepImpl: sleep,
});
