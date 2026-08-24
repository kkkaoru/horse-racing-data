// Run with bun. Worker-native JRA per-race rescore. It refreshes only the live
// odds/weight columns in the final per-race R2 cache, uses the same current
// champion routing and CatBoost models as the Container, then performs the
// prediction-table UPSERT. Callers retain a fail-closed Container fallback.

import { neon } from "@neondatabase/serverless";

import {
  buildFeatCacheKey,
  buildPerRaceFeatCacheKey,
  decodeCacheParquet,
  groupRowsByRace,
  refreshLateBindingColumns,
} from "./feature-cache";
import {
  fetchOddsForRace,
  fetchWeightForRace,
  sourceForCategory,
  type WeightSnapshotGeneration,
} from "./rescore-realtime";
import {
  JRA_SHADOW_MODEL_SPECS,
  loadSelectedJraShadowModel,
  scoreJraRaceShadow,
  selectJraShadowModel,
  type JraShadowPrediction,
  type JraShadowScoreResult,
} from "./jra-shadow-scorer";
import { createRescoreAttestation, type RescoreAttestation } from "../rescore-attestation";
import { assertBeforeRaceStartDeadline } from "../race-deadline";
import type { Env, PredictQueueMessage } from "../types";
import type { FeatureEntry } from "./feature-projection";

const JRA_CATEGORY = "jra";
const RACE_ID_NEN_END = 4;
// popularity_score needs runner_count > 1; a 0- or 1-entry odds map cannot give
// a valid denominator, so it degrades to the category median (null runnerCount).
const ODDS_MAP_RUNNER_FLOOR = 1;
const SCRATCH_STATUSES: ReadonlySet<string> = new Set([
  "出場停止",
  "出走取消",
  "取消",
  "競走除外",
  "除外",
]);

const PREDICTIONS_TABLE = "race_finish_position_model_predictions";
const PRIMARY_KEY_COLUMNS = [
  "model_version",
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
];
const UPDATABLE_COLUMNS = [
  "umaban",
  "predicted_score",
  "predicted_rank",
  "predicted_top1_prob",
  "predicted_top3_prob",
  "predicted_finish_position",
  "odds_score",
  "tansho_odds",
  "futan_juryo",
  "weight_diff_from_avg",
  "distance_band",
  "field_size_band",
  "season_band",
  "class_code",
  "surface",
];
const INSERT_COLUMNS = [...PRIMARY_KEY_COLUMNS, ...UPDATABLE_COLUMNS];
const TURF_TRACK_CODE_MIN = 10;
const TURF_TRACK_CODE_MAX = 22;
const DIRT_TRACK_CODE_MIN = 23;
const DIRT_TRACK_CODE_MAX = 29;
const OBSTACLE_TRACK_CODE_MIN = 51;
const OBSTACLE_TRACK_CODE_MAX = 59;
const SPRINT_DISTANCE_MAX = 1400;
const MILE_DISTANCE_MAX = 1800;
const INTERMEDIATE_DISTANCE_MAX = 2200;
const LONG_DISTANCE_MAX = 2800;
const SMALL_FIELD_MAX = 8;
const MEDIUM_FIELD_MAX = 14;

export type RescoreStatus = "ok" | "cache_miss" | "race_not_found";

export interface RescoreJraRaceInput {
  env: Env;
  message: PredictQueueMessage;
  // Injectable realtime fetch so the consumer can be tested without network I/O.
  fetchImpl: typeof fetch;
}

export interface RescoreJraRaceResult {
  status: RescoreStatus;
  racesPredicted: number;
  predictionCount: number;
  modelVersion: string | null;
}

interface RaceIdParts {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

interface PredictionRowContext {
  entries: ReadonlyArray<FeatureEntry>;
  modelVersion: string;
  parts: RaceIdParts;
}

const requiredWeightGeneration = (message: PredictQueueMessage): WeightSnapshotGeneration => {
  const count = message.weightSnapshotCount;
  const fetchedAt = message.weightSnapshotFetchedAt;
  const hash = message.weightSnapshotHash;
  if (count === undefined || fetchedAt === undefined || hash === undefined) {
    throw new Error("Horse weight snapshot generation is missing");
  }
  return {
    weightSnapshotCount: count,
    weightSnapshotFetchedAt: fetchedAt,
    weightSnapshotHash: hash,
  };
};

interface EntrySnapshotRow {
  horse_number: string | number | null;
  status: string | null;
}

interface ExpectedRunnerSnapshot {
  active: ReadonlySet<number>;
  scratched: ReadonlySet<number>;
}

interface WeightSetValidationInput extends ExpectedRunnerSnapshot {
  raceId: string;
  weights: ReadonlyMap<number, number>;
}

const CACHE_MISS_RESULT: RescoreJraRaceResult = {
  modelVersion: null,
  predictionCount: 0,
  racesPredicted: 0,
  status: "cache_miss",
};
const RACE_NOT_FOUND_RESULT: RescoreJraRaceResult = {
  modelVersion: null,
  predictionCount: 0,
  racesPredicted: 0,
  status: "race_not_found",
};

// Build the target race_id the cache rows carry:
// jra:{nen}:{tsukihi}:{keibajoCode}:{raceBango}. runYmd -> nen[0:4] / tsukihi[4:8].
const buildTargetRaceId = (message: PredictQueueMessage): string => {
  const nen = message.runYmd.slice(0, RACE_ID_NEN_END);
  const tsukihi = message.runYmd.slice(RACE_ID_NEN_END);
  return `${JRA_CATEGORY}:${nen}:${tsukihi}:${message.keibajoCode}:${message.raceBango}`;
};

const splitRaceId = (raceId: string): RaceIdParts => {
  const [source, kaisaiNen, kaisaiTsukihi, keibajoCode, raceBango] = raceId.split(":");
  return {
    kaisaiNen: kaisaiNen ?? "",
    kaisaiTsukihi: kaisaiTsukihi ?? "",
    keibajoCode: keibajoCode ?? "",
    raceBango: raceBango ?? "",
    source: source ?? "",
  };
};

// Coerce an arbitrary parquet cell to a scalar string, or null for missing /
// non-scalar cells. Avoids Object's "[object Object]" stringification while
// keeping the column-name -> unknown cache cell shape.
const cellToString = (value: unknown): string | null => {
  if (typeof value === "string") return value;
  if (typeof value === "number") return String(value);
  if (typeof value === "bigint") return String(value);
  return null;
};

const cellToNumber = (value: unknown): number | null => {
  const text = cellToString(value);
  if (text === null) return null;
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
};

const classifyDistanceBand = (value: unknown): string | null => {
  const distance = cellToNumber(value);
  if (distance === null) return null;
  if (distance <= SPRINT_DISTANCE_MAX) return "sprint";
  if (distance <= MILE_DISTANCE_MAX) return "mile";
  if (distance <= INTERMEDIATE_DISTANCE_MAX) return "intermediate";
  if (distance <= LONG_DISTANCE_MAX) return "long";
  return "extended";
};

const classifyFieldSizeBand = (value: unknown): string | null => {
  const fieldSize = cellToNumber(value);
  if (fieldSize === null) return null;
  if (fieldSize <= SMALL_FIELD_MAX) return "small";
  if (fieldSize <= MEDIUM_FIELD_MAX) return "medium";
  return "large";
};

const classifySeasonBand = (kaisaiTsukihi: string): string | null => {
  const month = Number(kaisaiTsukihi.slice(0, 2));
  if (!Number.isInteger(month) || month < 1 || month > 12) return null;
  if (month >= 3 && month <= 5) return "spring";
  if (month >= 6 && month <= 8) return "summer";
  if (month >= 9 && month <= 11) return "autumn";
  return "winter";
};

const classifySurface = (value: unknown): string | null => {
  const trackCode = cellToNumber(value);
  if (trackCode === null) return null;
  if (trackCode >= TURF_TRACK_CODE_MIN && trackCode <= TURF_TRACK_CODE_MAX) return "turf";
  if (trackCode >= DIRT_TRACK_CODE_MIN && trackCode <= DIRT_TRACK_CODE_MAX) return "dirt";
  if (trackCode >= OBSTACLE_TRACK_CODE_MIN && trackCode <= OBSTACLE_TRACK_CODE_MAX)
    return "obstacle";
  return null;
};

interface RefreshRowsInput {
  rows: ReadonlyArray<FeatureEntry>;
  oddsMap: Map<number, { tanshoOdds: number; tanshoNinkijun: number }>;
  weightMap: Map<number, number>;
}

// The popularity_score denominator is the per-race field size. The cache's
// shusso_tosu column is NULL at rescore time, so it is sourced from the count of
// horses with valid live tansho odds (oddsMap.size). An empty / single-horse
// odds map (e.g. the odds fetch failed) yields null so computePopularityScore
// falls back to the category median, matching the graceful-degradation contract.
const runnerCountFromOdds = (
  oddsMap: Map<number, { tanshoOdds: number; tanshoNinkijun: number }>,
): number | null => (oddsMap.size > ODDS_MAP_RUNNER_FLOOR ? oddsMap.size : null);

interface RefreshRowInput {
  row: FeatureEntry;
  rowsInput: RefreshRowsInput;
  runnerCount: number | null;
}

const refreshRow = (input: RefreshRowInput): FeatureEntry => {
  const umaban = Number(cellToString(input.row.umaban));
  const odds = input.rowsInput.oddsMap.get(umaban);
  return refreshLateBindingColumns({
    category: JRA_CATEGORY,
    currentBataiju: input.rowsInput.weightMap.get(umaban) ?? null,
    row: input.row,
    runnerCount: input.runnerCount,
    tanshoNinkijun: odds?.tanshoNinkijun ?? null,
    tanshoOdds: odds?.tanshoOdds ?? null,
  });
};

const buildEntries = (input: RefreshRowsInput): FeatureEntry[] => {
  const runnerCount = runnerCountFromOdds(input.oddsMap);
  return input.rows.map((row) => refreshRow({ row, rowsInput: input, runnerCount }));
};

// Build a $n-placeholder VALUES tuple for one row (INSERT_COLUMNS wide). The
// columnCount * rowIndex offset gives each row its own consecutive parameters.
const placeholderRow = (rowIndex: number, columnCount: number): string => {
  const offset = rowIndex * columnCount;
  const placeholders = Array.from(
    { length: columnCount },
    (_value, column) => `$${offset + column + 1}`,
  );
  return `(${placeholders.join(", ")})`;
};

// Parameterised multi-row UPSERT, mirroring upsert_sql.build_upsert_sql but with
// libpq-native $n placeholders (neon serverless binds positionally).
export const buildUpsertSql = (rowCount: number): string => {
  const valuesClause = Array.from({ length: rowCount }, (_value, rowIndex) =>
    placeholderRow(rowIndex, INSERT_COLUMNS.length),
  ).join(",\n      ");
  const updateAssignments = UPDATABLE_COLUMNS.map(
    (column) => `${column} = excluded.${column}`,
  ).join(",\n      ");
  return (
    `insert into ${PREDICTIONS_TABLE} (${INSERT_COLUMNS.join(", ")})\n` +
    `    values\n      ${valuesClause}\n` +
    `    on conflict (${PRIMARY_KEY_COLUMNS.join(", ")})\n` +
    `    do update set\n      ${updateAssignments},\n` +
    "      prediction_generated_at = now()"
  );
};

const buildRowParams = (
  prediction: JraShadowPrediction,
  context: PredictionRowContext,
): (string | number | null)[] => {
  const entry = context.entries.find(
    (candidate) => cellToString(candidate.ketto_toroku_bango) === prediction.kettoTorokuBango,
  );
  const raceEntry = context.entries[0];
  return [
    context.modelVersion,
    context.parts.source,
    context.parts.kaisaiNen,
    context.parts.kaisaiTsukihi,
    context.parts.keibajoCode,
    context.parts.raceBango,
    prediction.kettoTorokuBango,
    prediction.umaban,
    prediction.predictedScore,
    prediction.predictedRank,
    null,
    null,
    null,
    cellToNumber(entry?.odds_score),
    cellToNumber(entry?.tansho_odds),
    cellToNumber(entry?.futan_juryo),
    cellToNumber(entry?.weight_diff_from_avg),
    classifyDistanceBand(raceEntry?.kyori),
    classifyFieldSizeBand(raceEntry?.shusso_tosu),
    classifySeasonBand(context.parts.kaisaiTsukihi),
    cellToString(raceEntry?.kyoso_joken_code),
    classifySurface(raceEntry?.track_code),
  ];
};

export const buildUpsertParams = (
  predictions: ReadonlyArray<JraShadowPrediction>,
  context: PredictionRowContext,
): (string | number | null)[] =>
  predictions.flatMap((prediction) => buildRowParams(prediction, context));

interface UpsertInput {
  env: Env;
  entries: ReadonlyArray<FeatureEntry>;
  scored: JraShadowScoreResult;
  parts: RaceIdParts;
}

const upsertPredictions = async (input: UpsertInput): Promise<void> => {
  const sql = neon(input.env.NEON_DATABASE_URL);
  const statement = buildUpsertSql(input.scored.predictions.length);
  const params = buildUpsertParams(input.scored.predictions, {
    entries: input.entries,
    modelVersion: input.scored.modelVersion,
    parts: input.parts,
  });
  await sql.query(statement, params);
};

interface TargetRaceRows {
  cacheEtag: string;
  cacheVersion: string;
  isPerRace: boolean;
  status: RescoreStatus;
  rows: FeatureEntry[];
}

interface CacheRowsValidationInput {
  attestation: RescoreAttestation;
  cacheEtag: string;
  cacheVersion: string;
  isPerRace: boolean;
  rows: ReadonlyArray<FeatureEntry>;
  targetRaceId: string;
}

const decodeR2Object = async (object: R2ObjectBody): Promise<FeatureEntry[]> =>
  decodeCacheParquet(new Uint8Array(await object.arrayBuffer()));

// The per-race cache parquet already contains exactly one race's rows, so it is
// returned directly (no groupRowsByRace filtering) — empty means the race row set
// was not materialised, mirroring the whole-day race_not_found contract.
const loadPerRaceRows = async (object: R2ObjectBody): Promise<TargetRaceRows> => {
  const rows = await decodeR2Object(object);
  return rows.length > 0
    ? {
        cacheEtag: object.etag,
        cacheVersion: object.version,
        isPerRace: true,
        rows,
        status: "ok",
      }
    : {
        cacheEtag: object.etag,
        cacheVersion: object.version,
        isPerRace: true,
        rows: [],
        status: "race_not_found",
      };
};

const loadWholeDayRows = async (
  env: Env,
  message: PredictQueueMessage,
): Promise<TargetRaceRows> => {
  const object = await env.FEATURES_CACHE.get(buildFeatCacheKey(JRA_CATEGORY, message.runYmd));
  if (object === null) {
    return { cacheEtag: "", cacheVersion: "", isPerRace: false, rows: [], status: "cache_miss" };
  }
  const rows = await decodeR2Object(object);
  const targetRaceId = buildTargetRaceId(message);
  const group = groupRowsByRace(rows).find((race) => race.raceId === targetRaceId);
  if (group === undefined) {
    return {
      cacheEtag: object.etag,
      cacheVersion: object.version,
      isPerRace: false,
      rows: [],
      status: "race_not_found",
    };
  }
  return {
    cacheEtag: object.etag,
    cacheVersion: object.version,
    isPerRace: false,
    rows: group.rows,
    status: "ok",
  };
};

// Read the smaller per-race cache key first. The legacy whole-day lookup is
// retained only to preserve cache_miss/race_not_found diagnostics; attestation
// validation prevents a whole-day object from reaching scoring or publication.
const loadTargetRaceRows = async (
  env: Env,
  message: PredictQueueMessage,
): Promise<TargetRaceRows> => {
  const perRaceKey = buildPerRaceFeatCacheKey(
    JRA_CATEGORY,
    message.runYmd,
    message.keibajoCode ?? "",
    message.raceBango ?? "",
  );
  const perRaceObject = await env.FEATURES_CACHE.get(perRaceKey);
  if (perRaceObject !== null) return loadPerRaceRows(perRaceObject);
  return loadWholeDayRows(env, message);
};

const normalizePositiveHorseNumber = (value: unknown): number | null => {
  const parsed = Number(cellToString(value));
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
};

const hex = (bytes: ArrayBuffer): string =>
  [...new Uint8Array(bytes)].map((value) => value.toString(16).padStart(2, "0")).join("");

const hashCacheEntries = async (rows: ReadonlyArray<FeatureEntry>): Promise<string> => {
  const tokens = rows.map((row) => {
    const ketto = cellToString(row.ketto_toroku_bango)?.trim();
    const umaban = normalizePositiveHorseNumber(row.umaban);
    if (ketto === undefined || ketto === "" || umaban === null) {
      throw new Error("JRA final cache contains an invalid entry identity");
    }
    return `${ketto}:${String(umaban)}`;
  });
  if (new Set(tokens).size !== tokens.length) {
    throw new Error("JRA final cache contains duplicate entry identities");
  }
  const contract = [...tokens].sort().join("\n");
  return hex(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(contract)));
};

const assertAttestedTargetCacheRows = async (input: CacheRowsValidationInput): Promise<void> => {
  if (!input.isPerRace) throw new Error("JRA final cache is not race-scoped");
  if (
    input.cacheEtag !== input.attestation.featureCacheEtag ||
    input.cacheVersion !== input.attestation.featureCacheVersion
  ) {
    throw new Error("JRA final cache object does not match its attestation");
  }
  if (input.rows.some((row) => cellToString(row.race_id) !== input.targetRaceId)) {
    throw new Error(`JRA final cache race scope mismatch: ${input.targetRaceId}`);
  }
  if (input.rows.length !== input.attestation.entryCount) {
    throw new Error(`JRA final cache entry count mismatch: ${input.targetRaceId}`);
  }
  if ((await hashCacheEntries(input.rows)) !== input.attestation.entrySetHash) {
    throw new Error(`JRA final cache entry set mismatch: ${input.targetRaceId}`);
  }
};

const loadExpectedRunnerNumbers = async (
  env: Env,
  message: PredictQueueMessage,
): Promise<ExpectedRunnerSnapshot> => {
  const result = await env.REALTIME_DB.prepare(
    `with latest as (
       select max(fetched_at) as fetched_at
         from race_entry_snapshots
        where race_key = ?1
     )
     select entries.horse_number, entries.status
       from race_entry_snapshots entries
       join latest on latest.fetched_at = entries.fetched_at
      where entries.race_key = ?1
      order by cast(entries.horse_number as integer)`,
  )
    .bind(buildTargetRaceId(message))
    .all<EntrySnapshotRow>();
  const active = new Set<number>();
  const scratched = new Set<number>();
  result.results.forEach((row) => {
    const horseNumber = normalizePositiveHorseNumber(row.horse_number);
    if (horseNumber === null) return;
    if (row.status !== null && SCRATCH_STATUSES.has(row.status)) {
      scratched.add(horseNumber);
      return;
    }
    active.add(horseNumber);
  });
  if (active.size === 0) {
    throw new Error(`Active JRA runner snapshot is empty: ${buildTargetRaceId(message)}`);
  }
  return { active, scratched };
};

const assertCompleteWeightSet = (input: WeightSetValidationInput): void => {
  const missing = [...input.active].filter((horseNumber) => !input.weights.has(horseNumber));
  if (missing.length > 0) {
    throw new Error(
      `JRA horse weight rows are incomplete: ${input.raceId} missing=${missing.join(",")}`,
    );
  }
  const unexpected = [...input.weights.keys()].filter(
    (horseNumber) => !input.active.has(horseNumber) && !input.scratched.has(horseNumber),
  );
  if (unexpected.length > 0) {
    throw new Error(
      `JRA horse weight rows do not match entries: ${input.raceId} unexpected=${unexpected.join(",")}`,
    );
  }
};

interface ScoreAndWriteInput {
  env: Env;
  message: PredictQueueMessage;
  rows: FeatureEntry[];
  oddsMap: Map<number, { tanshoOdds: number; tanshoNinkijun: number }>;
  weightMap: Map<number, number>;
}

const scoreAndWrite = async (input: ScoreAndWriteInput): Promise<RescoreJraRaceResult> => {
  const entries = buildEntries({
    oddsMap: input.oddsMap,
    rows: input.rows,
    weightMap: input.weightMap,
  });
  const selected = selectJraShadowModel(entries, { preservedOddsGateEnabled: true });
  const loaded = await loadSelectedJraShadowModel(input.env.FEATURES_CACHE, selected);
  const initialScore = scoreJraRaceShadow(entries, loaded);
  const scored = initialScore.stage1RescoreRequired
    ? scoreJraRaceShadow(
        entries,
        await loadSelectedJraShadowModel(
          input.env.FEATURES_CACHE,
          JRA_SHADOW_MODEL_SPECS.stage1_marketfree,
        ),
      )
    : initialScore;
  const parts = splitRaceId(cellToString(input.rows[0]?.race_id) ?? "");
  assertBeforeRaceStartDeadline({
    nowMs: Date.now(),
    raceStartAtJst: input.message.raceStartAtJst,
  });
  await upsertPredictions({ entries, env: input.env, parts, scored });
  return {
    modelVersion: scored.modelVersion,
    predictionCount: scored.predictions.length,
    racesPredicted: 1,
    status: "ok",
  };
};

const MISS_RESULT_BY_STATUS: Record<"cache_miss" | "race_not_found", RescoreJraRaceResult> = {
  cache_miss: CACHE_MISS_RESULT,
  race_not_found: RACE_NOT_FOUND_RESULT,
};

export const rescoreJraRace = async (input: RescoreJraRaceInput): Promise<RescoreJraRaceResult> => {
  const target = await loadTargetRaceRows(input.env, input.message);
  if (target.status !== "ok") {
    const targetRaceId = buildTargetRaceId(input.message);
    console.warn(`rescore ${target.status} race_id=${targetRaceId} runYmd=${input.message.runYmd}`);
    return MISS_RESULT_BY_STATUS[target.status];
  }
  const targetRaceId = buildTargetRaceId(input.message);
  const attestation = await createRescoreAttestation({
    category: JRA_CATEGORY,
    env: input.env,
    keibajoCode: input.message.keibajoCode ?? "",
    raceBango: input.message.raceBango ?? "",
    runYmd: input.message.runYmd,
  });
  await assertAttestedTargetCacheRows({
    attestation,
    cacheEtag: target.cacheEtag,
    cacheVersion: target.cacheVersion,
    isPerRace: target.isPerRace,
    rows: target.rows,
    targetRaceId,
  });
  const fetchInput = {
    fetchImpl: input.fetchImpl,
    keibajoCode: input.message.keibajoCode ?? "",
    raceBango: input.message.raceBango ?? "",
    runYmd: input.message.runYmd,
    source: sourceForCategory(JRA_CATEGORY),
    weightGeneration: requiredWeightGeneration(input.message),
  };
  const [oddsMap, weightMap, expectedRunners] = await Promise.all([
    fetchOddsForRace(fetchInput),
    fetchWeightForRace(fetchInput),
    loadExpectedRunnerNumbers(input.env, input.message),
  ]);
  assertCompleteWeightSet({
    ...expectedRunners,
    raceId: targetRaceId,
    weights: weightMap,
  });
  return scoreAndWrite({
    env: input.env,
    message: input.message,
    oddsMap,
    rows: target.rows,
    weightMap,
  });
};

export {
  buildTargetRaceId,
  classifyDistanceBand,
  classifyFieldSizeBand,
  classifySeasonBand,
  classifySurface,
  splitRaceId,
  assertCompleteWeightSet,
  assertAttestedTargetCacheRows,
};
