// Run with bun. Batched end-to-end prediction readiness for the independent monitor.

import { neon } from "@neondatabase/serverless";
import { buildFinishPositionPredictionKvKey } from "./prediction-kv-keys";
import type { Env, PredictCategory } from "./types";

// Keep posted races in the response for the rest of the race day so an open
// incident continues hourly reminders and can observe eventual recovery.
const RECOVERY_WINDOW_MINUTES = 24 * 60;
const MS_PER_MINUTE = 60_000;
const RUN_YMD_YEAR_END = 4;
const RUN_YMD_LENGTH = 8;
const ZERO_ID_PATTERN = /^0+$/u;
const CATALOG_ORIGIN = "https://pc-keiba-r2-catalog.internal";
const BULK_FRESH_RACE_ENTRIES_PATH = "/v1/internal/fresh-race-entries-bulk";
const CATALOG_SOURCES: ReadonlyArray<PredictCategory> = ["jra", "nar", "ban-ei"];
const SCRATCH_STATUSES = new Set(["出場停止", "出走取消", "取消", "競走除外", "除外"]);

interface RaceSourceRow {
  source: string;
  keibajo_code: string;
  race_bango: string;
  race_start_at_jst: string;
  last_weight_fetch_at?: string | null;
  weight_snapshot_at?: string | null;
  weight_snapshot_count?: number | string | null;
}

interface EntryRow {
  source: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango?: string;
  status?: string | null;
  umaban?: number | string | null;
}

interface PredictionRow {
  source: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  umaban?: number | string | null;
  generated_at: string;
  model_version?: string;
}

interface PredictionKvRow {
  horseNumber: string;
  modelVersion: string;
  predictionGeneratedAt: string;
}

interface BulkCatalogEntry {
  source: PredictCategory;
  keibajoCode: string;
  raceBango: string;
  kettoTorokuBango: string;
  umaban: number;
}

export type PredictionExpectedSource = "catalog" | "d1" | "none";

export interface PredictionPhaseReadiness {
  complete: boolean;
  kvComplete: boolean;
  kvGenerationMatchesNeon: boolean;
  kvPredictionCount: number;
  kvSingleGeneration: boolean;
  missingCount: number;
  neonComplete: boolean;
  newestPredictionAt: string | null;
  oldestPredictionAt: string | null;
  predictionCount: number;
  reason: string | null;
}

export interface PostWeightPredictionReadiness extends PredictionPhaseReadiness {
  kvAfterWeight: boolean;
  lastWeightFetchAt: string | null;
  predictionAfterWeightCount: number;
  status: "complete" | "pending" | "waiting-for-weight";
  weightSnapshotAt: string | null;
  weightSnapshotCount: number;
  weightReady: boolean;
}

export interface PredictionReadinessSummary {
  notStartedRaceCount: number;
  postWeightCompleteRaceCount: number;
  postWeightIncompleteBeforePostCount: number;
  preWeightCompleteRaceCount: number;
  preWeightIncompleteBeforePostCount: number;
  raceCount: number;
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
  expectedSource: PredictionExpectedSource;
  predictionCount: number;
  missingCount: number;
  oldestPredictionAt: string | null;
  newestPredictionAt: string | null;
  complete: boolean;
  started: boolean;
  preWeight: PredictionPhaseReadiness;
  postWeight: PostWeightPredictionReadiness;
}

export interface PredictionReadinessResponse {
  checkedAt: string;
  runYmd: string;
  races: PredictionReadinessRace[];
  summary: PredictionReadinessSummary;
}

interface ReadinessInput {
  env: Env;
  now: Date;
  runYmd: string;
}

interface PostWeightReasonInput {
  kvAfterWeight: boolean;
  lastWeightFetchAt: string | null;
  postNeonComplete: boolean;
  weightSnapshotAt: string | null;
  weightSnapshotCount: number;
  expectedCount: number;
}

const raceKey = (source: string, keibajoCode: string, raceBango: string): string =>
  `${source}:${keibajoCode.padStart(2, "0")}:${raceBango.padStart(2, "0")}`;

const isEligibleHorseId = (value: string): boolean => {
  const trimmed = value.trim();
  return trimmed.length > 0 && !ZERO_ID_PATTERN.test(trimmed);
};

const normalizeHorseNumber = (value: number | string | null | undefined): string | null => {
  if (value === null || value === undefined) return null;
  const trimmed = String(value).trim();
  if (trimmed.length === 0 || ZERO_ID_PATTERN.test(trimmed)) return null;
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) && parsed > 0 ? String(parsed) : null;
};

const isNormalizedHorseNumber = (value: string | null): value is string => value !== null;

const isActiveEntry = (entry: EntryRow): boolean =>
  entry.status === undefined || entry.status === null || !SCRATCH_STATUSES.has(entry.status);

const entrantKey = (row: EntryRow | PredictionRow): string | null => {
  const horseNumber = normalizeHorseNumber(row.umaban);
  if (horseNumber !== null) return `umaban:${horseNumber}`;
  const horseId = row.ketto_toroku_bango;
  if (horseId === undefined || !isEligibleHorseId(horseId)) return null;
  return `horse:${horseId.trim()}`;
};

const resolveDeadline = (minutesToPost: number): ReadinessDeadline => {
  if (minutesToPost <= 0) return "post";
  if (minutesToPost <= 30) return "T-30";
  if (minutesToPost <= 60) return "T-60";
  return "T-120";
};

const sameInstant = (left: string, right: string): boolean => {
  const leftMs = Date.parse(left);
  const rightMs = Date.parse(right);
  return Number.isFinite(leftMs) && Number.isFinite(rightMs) ? leftMs === rightMs : left === right;
};

const resolvePostWeightReason = (input: PostWeightReasonInput): string | null => {
  if (input.lastWeightFetchAt === null) return "weight-not-delivered";
  if (input.weightSnapshotAt === null) return "weight-snapshot-missing";
  if (input.weightSnapshotCount < input.expectedCount) return "weight-snapshot-incomplete";
  if (!input.postNeonComplete) return "prediction-before-weight";
  return input.kvAfterWeight ? null : "kv-generation-mismatch";
};

const resolvePostWeightStatus = (
  lastWeightFetchAt: string | null,
  complete: boolean,
): PostWeightPredictionReadiness["status"] => {
  if (lastWeightFetchAt === null) return "waiting-for-weight";
  return complete ? "complete" : "pending";
};

const resolvePreWeightReason = (neonComplete: boolean, kvComplete: boolean): string | null => {
  if (!neonComplete) return "neon-incomplete";
  return kvComplete ? null : "kv-generation-mismatch";
};

const listRaceSources = async (env: Env, runYmd: string): Promise<RaceSourceRow[]> => {
  const result = await env.REALTIME_DB.prepare(
    `with target_races as (
       select race_key, source, keibajo_code, race_bango, race_start_at_jst,
              last_weight_fetch_at
         from realtime_race_sources
        where kaisai_nen = ?1 and kaisai_tsukihi = ?2
     ), latest_weights as (
       select weights.race_key, max(weights.fetched_at) as weight_snapshot_at
         from horse_weight_snapshots weights
         join target_races races on races.race_key = weights.race_key
        group by weights.race_key
     ), weight_counts as (
       select weights.race_key, count(*) as weight_snapshot_count
         from horse_weight_snapshots weights
         join latest_weights latest
           on latest.race_key = weights.race_key
          and latest.weight_snapshot_at = weights.fetched_at
        group by weights.race_key
     )
     select races.source, races.keibajo_code, races.race_bango,
            races.race_start_at_jst, races.last_weight_fetch_at,
            latest.weight_snapshot_at,
            coalesce(counts.weight_snapshot_count, 0) as weight_snapshot_count
       from target_races races
       left join latest_weights latest on latest.race_key = races.race_key
       left join weight_counts counts on counts.race_key = races.race_key
      order by race_start_at_jst, keibajo_code, race_bango`,
  )
    .bind(runYmd.slice(0, RUN_YMD_YEAR_END), runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_LENGTH))
    .all<RaceSourceRow>();
  return result.results;
};

const listEntries = async (env: Env, runYmd: string): Promise<EntryRow[]> => {
  const year = runYmd.slice(0, RUN_YMD_YEAR_END);
  const monthDay = runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_LENGTH);
  const [dailyResult, snapshotResult] = await Promise.all([
    env.REALTIME_DB.prepare(
      `select source, keibajo_code, race_bango, ketto_toroku_bango,
              cast(umaban as text) as umaban
       from daily_race_entries
      where kaisai_nen = ?1 and kaisai_tsukihi = ?2`,
    )
      .bind(year, monthDay)
      .all<EntryRow>(),
    env.REALTIME_DB.prepare(
      `with target_races as (
         select race_key, source, keibajo_code, race_bango
           from realtime_race_sources
          where kaisai_nen = ?1 and kaisai_tsukihi = ?2
       ), latest_entries as (
         select entries.race_key, max(entries.fetched_at) as fetched_at
           from race_entry_snapshots entries
           join target_races races on races.race_key = entries.race_key
          group by entries.race_key
       )
       select races.source, races.keibajo_code, races.race_bango,
              entries.horse_number as umaban, entries.status
         from target_races races
         join latest_entries latest on latest.race_key = races.race_key
         join race_entry_snapshots entries
           on entries.race_key = latest.race_key
          and entries.fetched_at = latest.fetched_at`,
    )
      .bind(year, monthDay)
      .all<EntryRow>(),
  ]);

  const dailyByRace = new Map<string, EntryRow[]>();
  for (const row of dailyResult.results.filter(isActiveEntry)) {
    if (entrantKey(row) === null) continue;
    const key = raceKey(row.source, row.keibajo_code, row.race_bango);
    dailyByRace.set(key, [...(dailyByRace.get(key) ?? []), row]);
  }
  const snapshotsByRace = new Map<string, EntryRow[]>();
  for (const row of snapshotResult.results.filter(isActiveEntry)) {
    if (normalizeHorseNumber(row.umaban) === null) continue;
    const key = raceKey(row.source, row.keibajo_code, row.race_bango);
    snapshotsByRace.set(key, [...(snapshotsByRace.get(key) ?? []), row]);
  }

  const entries: EntryRow[] = [];
  const raceKeys = new Set([...dailyByRace.keys(), ...snapshotsByRace.keys()]);
  for (const key of raceKeys) {
    const daily = dailyByRace.get(key) ?? [];
    const snapshots = snapshotsByRace.get(key) ?? [];
    const dailyNumbers = new Set(
      daily.map((row) => normalizeHorseNumber(row.umaban)).filter(isNormalizedHorseNumber),
    );
    const snapshotNumbers = new Set(
      snapshots.map((row) => normalizeHorseNumber(row.umaban)).filter(isNormalizedHorseNumber),
    );
    const sameNumbers =
      dailyNumbers.size > 0 &&
      dailyNumbers.size === snapshotNumbers.size &&
      [...dailyNumbers].every((number) => snapshotNumbers.has(number));
    entries.push(...(snapshots.length > 0 && !sameNumbers ? snapshots : daily));
  }
  return entries;
};

const listPredictions = async (env: Env, runYmd: string): Promise<PredictionRow[]> => {
  const sql = neon(env.NEON_DATABASE_URL);
  const rows: unknown = await sql.query(
    `select distinct on (
              source, keibajo_code, race_bango, ketto_toroku_bango, umaban
            )
            source, keibajo_code, race_bango, ketto_toroku_bango, umaban,
            model_version, prediction_generated_at::text as generated_at
      from race_finish_position_model_predictions
      where kaisai_nen = $1 and kaisai_tsukihi = $2
        and predicted_rank is not null
      order by source, keibajo_code, race_bango, ketto_toroku_bango, umaban,
               prediction_generated_at desc`,
    [runYmd.slice(0, RUN_YMD_YEAR_END), runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_LENGTH)],
  );
  return Array.isArray(rows) ? (rows as PredictionRow[]) : [];
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const categoryForRace = (race: RaceSourceRow): PredictCategory => {
  if (race.source === "jra") return "jra";
  return race.keibajo_code.padStart(2, "0") === "83" ? "ban-ei" : "nar";
};

const matchesCategoryVenue = (source: PredictCategory, keibajoCode: string): boolean => {
  const venue = keibajoCode.padStart(2, "0");
  if (source === "ban-ei") return venue === "83";
  return source === "nar" ? venue !== "83" : true;
};

const parseBulkCatalogEntry = (
  value: unknown,
  expectedSource: PredictCategory,
): BulkCatalogEntry | null => {
  if (!isRecord(value)) return null;
  if (
    value.source !== expectedSource ||
    typeof value.keibajoCode !== "string" ||
    typeof value.raceBango !== "string" ||
    typeof value.kettoTorokuBango !== "string" ||
    !matchesCategoryVenue(expectedSource, value.keibajoCode) ||
    typeof value.umaban !== "number" ||
    !Number.isInteger(value.umaban) ||
    value.umaban <= 0
  ) {
    return null;
  }
  return {
    keibajoCode: value.keibajoCode,
    kettoTorokuBango: value.kettoTorokuBango,
    raceBango: value.raceBango,
    source: expectedSource,
    umaban: value.umaban,
  };
};

const fetchBulkCatalogEntries = async (
  env: Env,
  runYmd: string,
  source: PredictCategory,
): Promise<EntryRow[]> => {
  const catalog = env.PC_KEIBA_R2_CATALOG;
  const token = env.FINISH_POSITION_ATTESTATION_TOKEN;
  if (catalog === undefined || token === undefined || token.length === 0) {
    throw new Error("Catalog binding and attestation token are required for readiness entries");
  }
  const url = new URL(BULK_FRESH_RACE_ENTRIES_PATH, CATALOG_ORIGIN);
  url.searchParams.set("date", runYmd);
  url.searchParams.set("source", source);
  const response = await catalog.fetch(
    new Request(url, { headers: { Authorization: `Bearer ${token}` } }),
  );
  if (!response.ok) {
    throw new Error(`Catalog bulk readiness entries failed with HTTP ${response.status}`);
  }
  const payload: unknown = await response.json();
  if (
    !isRecord(payload) ||
    payload.date !== runYmd ||
    payload.source !== source ||
    !Array.isArray(payload.entries)
  ) {
    throw new Error("Catalog bulk readiness entries returned an invalid envelope");
  }
  const parsed = payload.entries.map((entry) => parseBulkCatalogEntry(entry, source));
  if (parsed.some((entry) => entry === null)) {
    throw new Error("Catalog bulk readiness entries returned an invalid entry");
  }
  return parsed.flatMap((entry): EntryRow[] =>
    entry === null
      ? []
      : [
          {
            keibajo_code: entry.keibajoCode,
            ketto_toroku_bango: entry.kettoTorokuBango,
            race_bango: entry.raceBango,
            source: entry.source === "jra" ? "jra" : "nar",
            umaban: entry.umaban,
          },
        ],
  );
};

const missingEntryCategories = (
  entries: readonly EntryRow[],
  races: readonly RaceSourceRow[],
): PredictCategory[] => {
  const presentRaceKeys = new Set(
    entries.flatMap((entry): string[] => {
      if (entrantKey(entry) === null) return [];
      return [raceKey(entry.source, entry.keibajo_code, entry.race_bango)];
    }),
  );
  const missing = new Set(
    races.flatMap((race): PredictCategory[] => {
      const key = raceKey(race.source, race.keibajo_code, race.race_bango);
      return presentRaceKeys.has(key) ? [] : [categoryForRace(race)];
    }),
  );
  return CATALOG_SOURCES.filter((source) => missing.has(source));
};

const listCatalogEntries = async (
  env: Env,
  runYmd: string,
  sources: readonly PredictCategory[],
): Promise<EntryRow[]> =>
  (await Promise.all(sources.map((source) => fetchBulkCatalogEntries(env, runYmd, source)))).flat();

const parsePredictionKvRow = (value: unknown): PredictionKvRow | null => {
  if (!isRecord(value)) return null;
  if (
    typeof value.horseNumber !== "string" ||
    typeof value.modelVersion !== "string" ||
    typeof value.predictionGeneratedAt !== "string"
  ) {
    return null;
  }
  return {
    horseNumber: value.horseNumber,
    modelVersion: value.modelVersion,
    predictionGeneratedAt: value.predictionGeneratedAt,
  };
};

const parsePredictionKvPayload = (value: unknown): PredictionKvRow[] =>
  Array.isArray(value)
    ? value.map(parsePredictionKvRow).filter((row): row is PredictionKvRow => row !== null)
    : [];

const listPredictionKvPayloads = async (
  env: Env,
  races: readonly RaceSourceRow[],
  runYmd: string,
): Promise<Map<string, PredictionKvRow[]>> => {
  const kv = env.DETAIL_SECTION_CACHE_KV;
  if (kv === undefined) return new Map<string, PredictionKvRow[]>();
  const rows = await Promise.all(
    races.map(async (race) => {
      const key = raceKey(race.source, race.keibajo_code, race.race_bango);
      const value: unknown = await kv.get(
        buildFinishPositionPredictionKvKey({
          keibajoCode: race.keibajo_code,
          mmdd: runYmd.slice(RUN_YMD_YEAR_END),
          raceBango: race.race_bango,
          year: runYmd.slice(0, RUN_YMD_YEAR_END),
        }),
        "json",
      );
      return [key, parsePredictionKvPayload(value)] satisfies [string, PredictionKvRow[]];
    }),
  );
  return new Map(rows);
};

const inRaceDayRecoveryWindow = (raceStartAtJst: string, now: Date): boolean => {
  const postMs = Date.parse(raceStartAtJst);
  if (Number.isNaN(postMs)) return false;
  const minutesToPost = (postMs - now.getTime()) / MS_PER_MINUTE;
  return minutesToPost >= -RECOVERY_WINDOW_MINUTES;
};

export const buildPredictionReadiness = (input: {
  catalogEntries?: readonly EntryRow[];
  entries: readonly EntryRow[];
  kvPayloads?: ReadonlyMap<string, readonly PredictionKvRow[]>;
  now: Date;
  predictions: readonly PredictionRow[];
  races: readonly RaceSourceRow[];
  runYmd: string;
}): PredictionReadinessResponse => {
  const entriesByRace = new Map<string, Set<string>>();
  for (const entry of input.entries) {
    const entrant = entrantKey(entry);
    if (entrant === null) continue;
    const key = raceKey(entry.source, entry.keibajo_code, entry.race_bango);
    const ids = entriesByRace.get(key) ?? new Set<string>();
    ids.add(entrant);
    entriesByRace.set(key, ids);
  }
  const catalogEntriesByRace = new Map<string, Set<string>>();
  for (const entry of input.catalogEntries ?? []) {
    const entrant = entrantKey(entry);
    if (entrant === null) continue;
    const key = raceKey(entry.source, entry.keibajo_code, entry.race_bango);
    const ids = catalogEntriesByRace.get(key) ?? new Set<string>();
    ids.add(entrant);
    catalogEntriesByRace.set(key, ids);
  }
  const predictionsByRace = new Map<string, Map<string, PredictionRow>>();
  for (const prediction of input.predictions) {
    const entrant = entrantKey(prediction);
    if (entrant === null) continue;
    const key = raceKey(prediction.source, prediction.keibajo_code, prediction.race_bango);
    const rows = predictionsByRace.get(key) ?? new Map<string, PredictionRow>();
    rows.set(entrant, prediction);
    predictionsByRace.set(key, rows);
  }
  const races = input.races.filter((race) =>
    inRaceDayRecoveryWindow(race.race_start_at_jst, input.now),
  );
  const readinessRaces = races.map((race) => {
    const key = raceKey(race.source, race.keibajo_code, race.race_bango);
    const d1Expected = entriesByRace.get(key) ?? new Set<string>();
    const catalogExpected = catalogEntriesByRace.get(key) ?? new Set<string>();
    const expected = d1Expected.size > 0 ? d1Expected : catalogExpected;
    const expectedSource: PredictionExpectedSource =
      d1Expected.size > 0 ? "d1" : catalogExpected.size > 0 ? "catalog" : "none";
    const predicted = predictionsByRace.get(key) ?? new Map<string, PredictionRow>();
    const matchedTimes = [...expected]
      .map((id) => predicted.get(id)?.generated_at)
      .filter((value): value is string => value !== undefined)
      .toSorted();
    const matchedPredictions = [...expected]
      .map((id) => predicted.get(id))
      .filter((prediction): prediction is PredictionRow => prediction !== undefined);
    const expectedCount = expected.size;
    const predictionCount = matchedTimes.length;
    const minutesToPost = Math.ceil(
      (Date.parse(race.race_start_at_jst) - input.now.getTime()) / MS_PER_MINUTE,
    );
    const catalogNeonSetExact = expectedSource !== "catalog" || predicted.size === expectedCount;
    const neonComplete =
      expectedCount > 0 && predictionCount === expectedCount && catalogNeonSetExact;
    const kvRows = input.kvPayloads?.get(key) ?? [];
    const kvByHorseNumber = new Map(
      kvRows
        .map((row) => [normalizeHorseNumber(row.horseNumber), row])
        .filter((entry): entry is [string, PredictionKvRow] => entry[0] !== null),
    );
    const matchedKvRows = [...expected]
      .map((id) => predicted.get(id))
      .map((prediction) =>
        prediction === undefined
          ? undefined
          : kvByHorseNumber.get(normalizeHorseNumber(prediction.umaban) ?? ""),
      )
      .filter((row): row is PredictionKvRow => row !== undefined);
    const kvGenerationMatchesNeon =
      neonComplete &&
      matchedPredictions.every((prediction) => {
        const kvRow = kvByHorseNumber.get(normalizeHorseNumber(prediction.umaban) ?? "");
        if (kvRow === undefined) return false;
        const modelMatches =
          prediction.model_version === undefined || prediction.model_version === kvRow.modelVersion;
        return modelMatches && sameInstant(prediction.generated_at, kvRow.predictionGeneratedAt);
      });
    const kvGenerationTimes = matchedKvRows.map((row) => Date.parse(row.predictionGeneratedAt));
    const kvSingleGeneration =
      matchedKvRows.length === expectedCount &&
      kvGenerationTimes.every(Number.isFinite) &&
      new Set(kvGenerationTimes).size === 1;
    const catalogKvSetExact =
      expectedSource !== "catalog" ||
      (kvRows.length === expectedCount && kvByHorseNumber.size === expectedCount);
    const kvComplete =
      expectedCount > 0 &&
      matchedKvRows.length === expectedCount &&
      kvGenerationMatchesNeon &&
      kvSingleGeneration &&
      catalogKvSetExact;
    const phaseBase = {
      kvComplete,
      kvGenerationMatchesNeon,
      kvPredictionCount: matchedKvRows.length,
      kvSingleGeneration,
      missingCount: Math.max(0, expectedCount - predictionCount),
      neonComplete,
      newestPredictionAt: matchedTimes.at(-1) ?? null,
      oldestPredictionAt: matchedTimes[0] ?? null,
      predictionCount,
    };
    const preWeight: PredictionPhaseReadiness = {
      ...phaseBase,
      complete: neonComplete && kvComplete,
      reason: resolvePreWeightReason(neonComplete, kvComplete),
    };
    const lastWeightFetchAt = race.last_weight_fetch_at ?? null;
    const weightSnapshotAt = race.weight_snapshot_at ?? null;
    const parsedWeightCount = Number(race.weight_snapshot_count ?? 0);
    const weightSnapshotCount = Number.isFinite(parsedWeightCount) ? parsedWeightCount : 0;
    const snapshotMs = weightSnapshotAt === null ? Number.NaN : Date.parse(weightSnapshotAt);
    const fetchMs = lastWeightFetchAt === null ? Number.NaN : Date.parse(lastWeightFetchAt);
    const weightGenerationMs = Math.max(snapshotMs, fetchMs);
    const predictionsAfterWeight = [...expected]
      .map((id) => predicted.get(id)?.generated_at)
      .filter(
        (generatedAt): generatedAt is string =>
          generatedAt !== undefined && Date.parse(generatedAt) >= weightGenerationMs,
      );
    const predictionAfterWeightCount = predictionsAfterWeight.length;
    const weightReady =
      expectedCount > 0 &&
      weightSnapshotCount >= expectedCount &&
      Number.isFinite(snapshotMs) &&
      Number.isFinite(fetchMs);
    const postNeonComplete =
      weightReady && neonComplete && predictionAfterWeightCount === expectedCount;
    const kvAfterWeight =
      kvComplete &&
      matchedKvRows.every((row) => Date.parse(row.predictionGeneratedAt) >= weightGenerationMs);
    const postComplete = postNeonComplete && kvAfterWeight;
    const postReason = resolvePostWeightReason({
      expectedCount,
      kvAfterWeight,
      lastWeightFetchAt,
      postNeonComplete,
      weightSnapshotAt,
      weightSnapshotCount,
    });
    const sortedPredictionsAfterWeight = predictionsAfterWeight.toSorted();
    const postWeight: PostWeightPredictionReadiness = {
      ...phaseBase,
      complete: postComplete,
      kvAfterWeight,
      lastWeightFetchAt,
      missingCount: Math.max(0, expectedCount - predictionAfterWeightCount),
      neonComplete: postNeonComplete,
      newestPredictionAt: sortedPredictionsAfterWeight.at(-1) ?? null,
      oldestPredictionAt: sortedPredictionsAfterWeight[0] ?? null,
      predictionAfterWeightCount,
      predictionCount: predictionAfterWeightCount,
      reason: postReason,
      status: resolvePostWeightStatus(lastWeightFetchAt, postComplete),
      weightSnapshotAt,
      weightSnapshotCount,
      weightReady,
    };
    return {
      complete: neonComplete,
      deadline: resolveDeadline(minutesToPost),
      expectedCount,
      expectedSource,
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
      started: minutesToPost <= 0,
      preWeight,
      postWeight,
    };
  });
  const notStarted = readinessRaces.filter((race) => !race.started);
  return {
    checkedAt: input.now.toISOString(),
    races: readinessRaces,
    runYmd: input.runYmd,
    summary: {
      notStartedRaceCount: notStarted.length,
      postWeightCompleteRaceCount: readinessRaces.filter((race) => race.postWeight.complete).length,
      postWeightIncompleteBeforePostCount: notStarted.filter((race) => !race.postWeight.complete)
        .length,
      preWeightCompleteRaceCount: readinessRaces.filter((race) => race.preWeight.complete).length,
      preWeightIncompleteBeforePostCount: notStarted.filter((race) => !race.preWeight.complete)
        .length,
      raceCount: readinessRaces.length,
    },
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
  const catalogSources = missingEntryCategories(entries, races);
  const [catalogEntries, kvPayloads] = await Promise.all([
    listCatalogEntries(input.env, input.runYmd, catalogSources),
    listPredictionKvPayloads(input.env, races, input.runYmd),
  ]);
  const unresolvedSources = missingEntryCategories([...entries, ...catalogEntries], races);
  if (unresolvedSources.length > 0) {
    throw new Error(
      `Catalog bulk readiness entries did not cover race categories: ${unresolvedSources.join(",")}`,
    );
  }
  return buildPredictionReadiness({
    ...input,
    catalogEntries,
    entries,
    kvPayloads,
    predictions,
    races,
  });
};
