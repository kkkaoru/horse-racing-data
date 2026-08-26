// Run with bun. Bounded production attestation for pre-weight prediction and KV readiness.

export interface FinishPositionReadinessOptions {
  baseUrl: string;
  fetcher: typeof fetch;
  log: (message: string) => void;
  nowMilliseconds: () => number;
  pollIntervalMilliseconds: number;
  pollTimeoutMilliseconds: number;
  retryDelay: (milliseconds: number) => Promise<void>;
  runYmd: string;
  token: string;
}

interface ReadinessPreWeight {
  complete: boolean;
  kvComplete: boolean;
}

interface ReadinessRace {
  keibajoCode: string;
  raceBango: string;
  raceKey: string;
  source: string;
  started: boolean;
  preWeight: ReadinessPreWeight;
}

interface ReadinessResponse {
  races: ReadinessRace[];
  runYmd: string;
}

class PermanentReadinessError extends Error {}

const READINESS_PATH = "/api/internal/prediction-readiness";
const RUN_FOCUSED_FULL_RACE_PATH = "/api/admin/run-focused-full-race";
const HTTP_UNAUTHORIZED = 401;
const HTTP_FORBIDDEN = 403;
const MAX_HEALS_PER_POLL = 3;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseRace = (value: unknown): ReadinessRace | null => {
  if (!isRecord(value) || !isRecord(value.preWeight)) return null;
  return typeof value.keibajoCode === "string" &&
    typeof value.raceBango === "string" &&
    typeof value.raceKey === "string" &&
    typeof value.source === "string" &&
    typeof value.started === "boolean" &&
    typeof value.preWeight.complete === "boolean" &&
    typeof value.preWeight.kvComplete === "boolean"
    ? {
        keibajoCode: value.keibajoCode,
        preWeight: {
          complete: value.preWeight.complete,
          kvComplete: value.preWeight.kvComplete,
        },
        raceBango: value.raceBango,
        raceKey: value.raceKey,
        source: value.source,
        started: value.started,
      }
    : null;
};

const predictionCategory = (race: ReadinessRace): "ban-ei" | "jra" | "nar" => {
  if (race.source === "jra") return "jra";
  return race.keibajoCode.padStart(2, "0") === "83" ? "ban-ei" : "nar";
};

const enqueueIncompleteRace = async (
  options: FinishPositionReadinessOptions,
  race: ReadinessRace,
): Promise<void> => {
  const response = await options.fetcher(new URL(RUN_FOCUSED_FULL_RACE_PATH, options.baseUrl), {
    body: JSON.stringify({
      category: predictionCategory(race),
      force: true,
      keibajoCode: race.keibajoCode,
      raceBango: race.raceBango,
      runYmd: options.runYmd,
    }),
    headers: {
      Authorization: `Bearer ${options.token}`,
      "Content-Type": "application/json",
    },
    method: "POST",
  });
  if (response.status === HTTP_UNAUTHORIZED || response.status === HTTP_FORBIDDEN) {
    throw new PermanentReadinessError(
      `Finish-position readiness repair authorization failed with HTTP ${response.status}`,
    );
  }
  if (!response.ok) {
    throw new Error(
      `Finish-position readiness repair failed for ${race.raceKey} with HTTP ${response.status}`,
    );
  }
};

export const parseFinishPositionReadiness = (value: unknown): ReadinessResponse => {
  if (!isRecord(value) || typeof value.runYmd !== "string" || !Array.isArray(value.races)) {
    throw new PermanentReadinessError("Finish-position readiness returned an invalid response");
  }
  const races = value.races.map(parseRace);
  if (races.some((race) => race === null)) {
    throw new PermanentReadinessError("Finish-position readiness returned an invalid race");
  }
  return {
    races: races.filter((race): race is ReadinessRace => race !== null),
    runYmd: value.runYmd,
  };
};

const fetchReadiness = async (
  options: FinishPositionReadinessOptions,
): Promise<ReadinessResponse> => {
  const url = new URL(READINESS_PATH, options.baseUrl);
  url.searchParams.set("runYmd", options.runYmd);
  const response = await options.fetcher(url.toString(), {
    headers: { Authorization: `Bearer ${options.token}` },
  });
  if (response.status === HTTP_UNAUTHORIZED || response.status === HTTP_FORBIDDEN) {
    throw new PermanentReadinessError(
      `Finish-position readiness authorization failed with HTTP ${response.status}`,
    );
  }
  if (!response.ok) {
    throw new Error(`Finish-position readiness failed with HTTP ${response.status}`);
  }
  return parseFinishPositionReadiness(await response.json());
};

export const attestPreWeightPredictionReadiness = async (
  options: FinishPositionReadinessOptions,
): Promise<void> => {
  const deadline = options.nowMilliseconds() + options.pollTimeoutMilliseconds;
  const repairRequestedRaceKeys = new Set<string>();
  const poll = async (attempt: number): Promise<void> => {
    const readiness = await fetchReadiness(options).catch(async (error: unknown) => {
      if (error instanceof PermanentReadinessError) throw error;
      if (options.nowMilliseconds() >= deadline) {
        throw new Error(
          `Finish-position readiness timed out after request failure: ${String(error)}`,
        );
      }
      options.log(`Finish-position readiness attempt ${attempt} was unavailable; retrying.`);
      await options.retryDelay(options.pollIntervalMilliseconds);
      await poll(attempt + 1);
    });
    if (readiness === undefined) return;
    if (readiness.runYmd !== options.runYmd) {
      throw new PermanentReadinessError(
        `Finish-position readiness date mismatch: expected ${options.runYmd}, received ${readiness.runYmd}`,
      );
    }
    const upcoming = readiness.races.filter((race) => !race.started);
    const incomplete = upcoming.filter(
      (race) => !race.preWeight.complete || !race.preWeight.kvComplete,
    );
    options.log(
      `Finish-position readiness attempt ${attempt}: ${upcoming.length - incomplete.length}/${upcoming.length} upcoming races have complete pre-weight predictions and KV.`,
    );
    if (incomplete.length === 0) return;
    if (options.nowMilliseconds() >= deadline) {
      throw new Error(
        `Finish-position readiness timed out with incomplete upcoming races: ${incomplete.map((race) => race.raceKey).join(",")}`,
      );
    }
    const repairCandidates = incomplete
      .filter((race) => !repairRequestedRaceKeys.has(race.raceKey))
      .slice(0, MAX_HEALS_PER_POLL);
    for (const race of repairCandidates) {
      try {
        await enqueueIncompleteRace(options, race);
        repairRequestedRaceKeys.add(race.raceKey);
        options.log(`Finish-position readiness requested repair for ${race.raceKey}.`);
      } catch (error) {
        if (error instanceof PermanentReadinessError) throw error;
        options.log(
          `Finish-position readiness repair for ${race.raceKey} was unavailable; retrying.`,
        );
      }
    }
    await options.retryDelay(options.pollIntervalMilliseconds);
    await poll(attempt + 1);
  };
  await poll(1);
};
