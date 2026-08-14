// Run with bun. Batched end-to-end prediction readiness for the independent monitor.

import { neon } from "@neondatabase/serverless";
import type { Env } from "./types";

const LEAD_WINDOW_MINUTES = 120;
// Keep posted races in the response for the rest of the race day so an open
// incident continues hourly reminders and can observe eventual recovery.
const RECOVERY_WINDOW_MINUTES = 24 * 60;
const MS_PER_MINUTE = 60_000;
const RUN_YMD_YEAR_END = 4;
const RUN_YMD_LENGTH = 8;
const ZERO_ID_PATTERN = /^0+$/u;

interface RaceSourceRow {
  source: string;
  keibajo_code: string;
  race_bango: string;
  race_start_at_jst: string;
}

interface EntryRow {
  source: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
}

interface PredictionRow {
  source: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  generated_at: string;
}

export type ReadinessDeadline = "T-120" | "T-60" | "T-30" | "post";

export interface PredictionReadinessRace {
  raceKey: string;
  source: string;
  keibajoCode: string;
  raceBango: string;
  raceStartAtJst: string;
  minutesToPost: number;
  deadline: ReadinessDeadline;
  expectedCount: number;
  predictionCount: number;
  missingCount: number;
  oldestPredictionAt: string | null;
  newestPredictionAt: string | null;
  complete: boolean;
}

export interface PredictionReadinessResponse {
  checkedAt: string;
  runYmd: string;
  races: PredictionReadinessRace[];
}

interface ReadinessInput {
  env: Env;
  now: Date;
  runYmd: string;
}

const raceKey = (source: string, keibajoCode: string, raceBango: string): string =>
  `${source}:${keibajoCode.padStart(2, "0")}:${raceBango.padStart(2, "0")}`;

const isEligibleHorseId = (value: string): boolean => {
  const trimmed = value.trim();
  return trimmed.length > 0 && !ZERO_ID_PATTERN.test(trimmed);
};

const resolveDeadline = (minutesToPost: number): ReadinessDeadline => {
  if (minutesToPost <= 0) return "post";
  if (minutesToPost <= 30) return "T-30";
  if (minutesToPost <= 60) return "T-60";
  return "T-120";
};

const listRaceSources = async (env: Env, runYmd: string): Promise<RaceSourceRow[]> => {
  const result = await env.REALTIME_DB.prepare(
    `select source, keibajo_code, race_bango, race_start_at_jst
       from realtime_race_sources
      where kaisai_nen = ?1 and kaisai_tsukihi = ?2
      order by race_start_at_jst, keibajo_code, race_bango`,
  )
    .bind(runYmd.slice(0, RUN_YMD_YEAR_END), runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_LENGTH))
    .all<RaceSourceRow>();
  return result.results;
};

const listEntries = async (env: Env, runYmd: string): Promise<EntryRow[]> => {
  const result = await env.REALTIME_DB.prepare(
    `select source, keibajo_code, race_bango, ketto_toroku_bango
       from daily_race_entries
      where kaisai_nen = ?1 and kaisai_tsukihi = ?2`,
  )
    .bind(runYmd.slice(0, RUN_YMD_YEAR_END), runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_LENGTH))
    .all<EntryRow>();
  return result.results.filter((row) => isEligibleHorseId(row.ketto_toroku_bango));
};

const listPredictions = async (env: Env, runYmd: string): Promise<PredictionRow[]> => {
  const sql = neon(env.NEON_DATABASE_URL);
  const rows: unknown = await sql.query(
    `select source, keibajo_code, race_bango, ketto_toroku_bango,
            max(prediction_generated_at)::text as generated_at
       from race_finish_position_model_predictions
      where kaisai_nen = $1 and kaisai_tsukihi = $2
        and predicted_rank is not null
        and prediction_generated_at >= $3::timestamptz
      group by source, keibajo_code, race_bango, ketto_toroku_bango`,
    [
      runYmd.slice(0, RUN_YMD_YEAR_END),
      runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_LENGTH),
      `${runYmd.slice(0, 4)}-${runYmd.slice(4, 6)}-${runYmd.slice(6, 8)}T00:00:00+09:00`,
    ],
  );
  return Array.isArray(rows) ? (rows as PredictionRow[]) : [];
};

const inMonitorWindow = (raceStartAtJst: string, now: Date): boolean => {
  const postMs = Date.parse(raceStartAtJst);
  if (Number.isNaN(postMs)) return false;
  const minutesToPost = (postMs - now.getTime()) / MS_PER_MINUTE;
  return minutesToPost <= LEAD_WINDOW_MINUTES && minutesToPost >= -RECOVERY_WINDOW_MINUTES;
};

export const buildPredictionReadiness = (input: {
  entries: readonly EntryRow[];
  now: Date;
  predictions: readonly PredictionRow[];
  races: readonly RaceSourceRow[];
  runYmd: string;
}): PredictionReadinessResponse => {
  const entriesByRace = new Map<string, Set<string>>();
  for (const entry of input.entries) {
    const key = raceKey(entry.source, entry.keibajo_code, entry.race_bango);
    const ids = entriesByRace.get(key) ?? new Set<string>();
    ids.add(entry.ketto_toroku_bango.trim());
    entriesByRace.set(key, ids);
  }
  const predictionsByRace = new Map<string, Map<string, string>>();
  for (const prediction of input.predictions) {
    const key = raceKey(prediction.source, prediction.keibajo_code, prediction.race_bango);
    const rows = predictionsByRace.get(key) ?? new Map<string, string>();
    rows.set(prediction.ketto_toroku_bango.trim(), prediction.generated_at);
    predictionsByRace.set(key, rows);
  }
  const races = input.races.filter((race) => inMonitorWindow(race.race_start_at_jst, input.now));
  return {
    checkedAt: input.now.toISOString(),
    runYmd: input.runYmd,
    races: races.map((race) => {
      const key = raceKey(race.source, race.keibajo_code, race.race_bango);
      const expected = entriesByRace.get(key) ?? new Set<string>();
      const predicted = predictionsByRace.get(key) ?? new Map<string, string>();
      const matchedTimes = [...expected]
        .map((id) => predicted.get(id))
        .filter((value): value is string => value !== undefined)
        .toSorted();
      const expectedCount = expected.size;
      const predictionCount = matchedTimes.length;
      const minutesToPost = Math.ceil(
        (Date.parse(race.race_start_at_jst) - input.now.getTime()) / MS_PER_MINUTE,
      );
      return {
        complete: expectedCount > 0 && predictionCount === expectedCount,
        deadline: resolveDeadline(minutesToPost),
        expectedCount,
        keibajoCode: race.keibajo_code.padStart(2, "0"),
        minutesToPost,
        missingCount: Math.max(0, expectedCount - predictionCount),
        newestPredictionAt: matchedTimes.at(-1) ?? null,
        oldestPredictionAt: matchedTimes[0] ?? null,
        predictionCount,
        raceBango: race.race_bango.padStart(2, "0"),
        raceKey: key,
        raceStartAtJst: race.race_start_at_jst,
        source: race.source,
      };
    }),
  };
};

export const getPredictionReadiness = async (
  input: ReadinessInput,
): Promise<PredictionReadinessResponse> => {
  const [races, entries, predictions] = await Promise.all([
    listRaceSources(input.env, input.runYmd),
    listEntries(input.env, input.runYmd),
    listPredictions(input.env, input.runYmd),
  ]);
  return buildPredictionReadiness({ ...input, entries, predictions, races });
};
