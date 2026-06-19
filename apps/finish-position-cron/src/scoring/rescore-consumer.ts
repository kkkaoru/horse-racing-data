// Run with bun. Worker-native JRA per-race rescore consumer body. Given a
// per-race rescore message (mode="rescore" with keibajoCode + raceBango), it:
//   (a) reads the Stage-1 R2 feature-cache parquet for the run date,
//   (b) isolates the target race's rows by race_id,
//   (c) fetches the freshest tansho odds + bataiju from the realtime workers,
//   (d) recomputes the 5 late-binding feature columns per horse,
//   (e) loads CB iter20 + XGB E-top2 + feature_names from R2,
//   (f) scores the race with the E-top2 override (scoreJraRace), and
//   (g) UPSERTs the ranked predictions into Neon.
//
// This replaces the container round-trip for JRA per-race rescores: no 21y Neon
// scan, no DuckDB feature build — just cache get + realtime fetch + score + write.
// Mirrors the Python predict_upcoming._score_one_race_etop2 + upsert_sql path.
//
// The Neon connection string is never logged (matching neon-warm.ts).

import { neon } from "@neondatabase/serverless";

import {
  buildFeatCacheKey,
  decodeCacheParquet,
  groupRowsByRace,
  refreshLateBindingColumns,
  toJraRaceEntry,
} from "./feature-cache";
import { JRA_ETOP2_MODEL_VERSION, loadJraModels } from "./model-loader";
import { fetchOddsForRace, fetchWeightForRace, sourceForCategory } from "./rescore-realtime";
import { scoreJraRace, type JraRaceEntry, type JraScoredPrediction } from "./jra-scorer";
import type { Env, PredictQueueMessage } from "../types";
import type { FeatureEntry } from "./feature-projection";

const JRA_CATEGORY = "jra";
const RACE_ID_NEN_END = 4;
const RACE_CLASS_FIELD = "kyoso_joken_code";

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
];
const INSERT_COLUMNS = [...PRIMARY_KEY_COLUMNS, ...UPDATABLE_COLUMNS];

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
  etop2Fired: boolean;
}

interface RaceIdParts {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

const CACHE_MISS_RESULT: RescoreJraRaceResult = {
  etop2Fired: false,
  predictionCount: 0,
  racesPredicted: 0,
  status: "cache_miss",
};
const RACE_NOT_FOUND_RESULT: RescoreJraRaceResult = {
  etop2Fired: false,
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

const raceClassFrom = (rows: ReadonlyArray<FeatureEntry>): string | null => {
  const first = rows[0];
  if (first === undefined) return null;
  return cellToString(first[RACE_CLASS_FIELD]);
};

interface RefreshRowsInput {
  rows: ReadonlyArray<FeatureEntry>;
  oddsMap: Map<number, { tanshoOdds: number; tanshoNinkijun: number }>;
  weightMap: Map<number, number>;
}

const refreshRow = (row: FeatureEntry, input: RefreshRowsInput): JraRaceEntry => {
  const entry = toJraRaceEntry(row);
  const odds = input.oddsMap.get(entry.umaban);
  const refreshed = refreshLateBindingColumns({
    category: JRA_CATEGORY,
    currentBataiju: input.weightMap.get(entry.umaban) ?? null,
    row,
    tanshoNinkijun: odds?.tanshoNinkijun ?? null,
    tanshoOdds: odds?.tanshoOdds ?? null,
  });
  return toJraRaceEntry(refreshed);
};

const buildEntries = (input: RefreshRowsInput): JraRaceEntry[] =>
  input.rows.map((row) => refreshRow(row, input));

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
  prediction: JraScoredPrediction,
  parts: RaceIdParts,
): (string | number | null)[] => [
  JRA_ETOP2_MODEL_VERSION,
  parts.source,
  parts.kaisaiNen,
  parts.kaisaiTsukihi,
  parts.keibajoCode,
  parts.raceBango,
  prediction.kettoTorokuBango,
  prediction.umaban,
  prediction.predictedScore,
  prediction.predictedRank,
  prediction.predictedTop1Prob,
  prediction.predictedTop3Prob,
  prediction.predictedFinishPosition,
];

export const buildUpsertParams = (
  predictions: ReadonlyArray<JraScoredPrediction>,
  parts: RaceIdParts,
): (string | number | null)[] =>
  predictions.flatMap((prediction) => buildRowParams(prediction, parts));

interface UpsertInput {
  env: Env;
  predictions: ReadonlyArray<JraScoredPrediction>;
  parts: RaceIdParts;
}

const upsertPredictions = async (input: UpsertInput): Promise<void> => {
  const sql = neon(input.env.NEON_DATABASE_URL);
  const statement = buildUpsertSql(input.predictions.length);
  const params = buildUpsertParams(input.predictions, input.parts);
  await sql.query(statement, params);
};

interface TargetRaceRows {
  status: RescoreStatus;
  rows: FeatureEntry[];
}

const loadTargetRaceRows = async (
  env: Env,
  message: PredictQueueMessage,
): Promise<TargetRaceRows> => {
  const cacheKey = buildFeatCacheKey(JRA_CATEGORY, message.runYmd);
  const object = await env.FEATURES_CACHE.get(cacheKey);
  if (object === null) return { rows: [], status: "cache_miss" };
  const rows = await decodeCacheParquet(new Uint8Array(await object.arrayBuffer()));
  const targetRaceId = buildTargetRaceId(message);
  const group = groupRowsByRace(rows).find((race) => race.raceId === targetRaceId);
  if (group === undefined) return { rows: [], status: "race_not_found" };
  return { rows: group.rows, status: "ok" };
};

interface ScoreAndWriteInput {
  env: Env;
  rows: FeatureEntry[];
  oddsMap: Map<number, { tanshoOdds: number; tanshoNinkijun: number }>;
  weightMap: Map<number, number>;
}

const scoreAndWrite = async (input: ScoreAndWriteInput): Promise<RescoreJraRaceResult> => {
  const models = await loadJraModels(input.env.FEATURES_CACHE);
  const entries = buildEntries({
    oddsMap: input.oddsMap,
    rows: input.rows,
    weightMap: input.weightMap,
  });
  const scored = scoreJraRace({
    catboostModel: models.catboostModel,
    entries,
    featureNames: models.featureNames,
    raceClass: raceClassFrom(input.rows),
    xgboostModel: models.xgboostModel,
  });
  const parts = splitRaceId(cellToString(input.rows[0]?.race_id) ?? "");
  await upsertPredictions({ env: input.env, parts, predictions: scored.predictions });
  return {
    etop2Fired: scored.etop2Fired,
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
  const fetchInput = {
    fetchImpl: input.fetchImpl,
    keibajoCode: input.message.keibajoCode ?? "",
    raceBango: input.message.raceBango ?? "",
    runYmd: input.message.runYmd,
    source: sourceForCategory(JRA_CATEGORY),
  };
  const [oddsMap, weightMap] = await Promise.all([
    fetchOddsForRace(fetchInput),
    fetchWeightForRace(fetchInput),
  ]);
  return scoreAndWrite({ env: input.env, oddsMap, rows: target.rows, weightMap });
};

export { buildTargetRaceId, raceClassFrom, splitRaceId };
