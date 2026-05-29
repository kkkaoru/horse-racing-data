// Run with: imported by Agent A's evaluate-bucket-21y.ts (bun runtime).
// Reads local Hive-partitioned parquet from Agent G Phase B' output via DuckDB and exposes
// a race-key + ketto_toroku_bango -> FinishPositionLocalPrediction Map.
// Implements FinishPositionLocalLoaderInterface.

import type {
  FinishPositionLocalLoaderInterface,
  FinishPositionLocalPrediction,
  RunningStyleLocalPredictionRaceKey,
} from "../../lib/finish-prediction-dimensions";

interface FinishPositionLocalLoaderOptions {
  parquetGlob: string;
  duckdbModule: DuckDBModuleLike;
}

interface DuckDBConnectionLike {
  query(sql: string): { toArray(): readonly Record<string, unknown>[] };
  close(): void;
}

interface DuckDBModuleLike {
  Database: new (path: string) => DuckDBDatabaseLike;
}

interface DuckDBDatabaseLike {
  connect(): DuckDBConnectionLike;
  close(): void;
}

interface RawFinishPositionRow {
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  predicted_score: number;
  predicted_rank: number;
  predicted_top1_prob: number;
  predicted_top3_prob: number;
  predicted_finish_position: number;
  model_version: string;
  running_style_feature_version: string;
  finish_position_version: string;
}

const RACE_KEY_DELIMITER = "|";
const SELECT_COLUMNS = [
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
  "predicted_score",
  "predicted_rank",
  "predicted_top1_prob",
  "predicted_top3_prob",
  "predicted_finish_position",
  "model_version",
  "running_style_feature_version",
  "finish_position_version",
];

export const buildRaceKey = (raceKey: RunningStyleLocalPredictionRaceKey): string =>
  [
    raceKey.source,
    raceKey.kaisaiNen,
    raceKey.kaisaiTsukihi,
    raceKey.keibajoCode,
    raceKey.raceBango,
    raceKey.kettoTorokuBango,
  ].join(RACE_KEY_DELIMITER);

export const buildSelectSql = (parquetGlob: string): string =>
  `SELECT ${SELECT_COLUMNS.join(", ")} FROM read_parquet('${parquetGlob}', hive_partitioning = true)`;

const requireString = (value: unknown, name: string): string => {
  if (typeof value !== "string") throw new Error(`Column ${name} is not a string.`);
  return value;
};

const coerceNumber = (value: unknown, name: string): number => {
  if (value === null || value === undefined) {
    return 0;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`Column ${name} is not numeric.`);
  return parsed;
};

const coerceRow = (row: Record<string, unknown>): RawFinishPositionRow => ({
  source: requireString(row.source, "source"),
  kaisai_nen: requireString(row.kaisai_nen, "kaisai_nen"),
  kaisai_tsukihi: requireString(row.kaisai_tsukihi, "kaisai_tsukihi"),
  keibajo_code: requireString(row.keibajo_code, "keibajo_code"),
  race_bango: requireString(row.race_bango, "race_bango"),
  ketto_toroku_bango: requireString(row.ketto_toroku_bango, "ketto_toroku_bango"),
  predicted_score: coerceNumber(row.predicted_score, "predicted_score"),
  predicted_rank: coerceNumber(row.predicted_rank, "predicted_rank"),
  predicted_top1_prob: coerceNumber(row.predicted_top1_prob, "predicted_top1_prob"),
  predicted_top3_prob: coerceNumber(row.predicted_top3_prob, "predicted_top3_prob"),
  predicted_finish_position: coerceNumber(
    row.predicted_finish_position,
    "predicted_finish_position",
  ),
  model_version: requireString(row.model_version, "model_version"),
  running_style_feature_version: requireString(
    row.running_style_feature_version,
    "running_style_feature_version",
  ),
  finish_position_version: requireString(row.finish_position_version, "finish_position_version"),
});

const rowToRaceKey = (row: RawFinishPositionRow): RunningStyleLocalPredictionRaceKey => ({
  source: row.source,
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: row.keibajo_code,
  raceBango: row.race_bango,
  kettoTorokuBango: row.ketto_toroku_bango,
});

const rowToPrediction = (row: RawFinishPositionRow): FinishPositionLocalPrediction => ({
  predictedScore: row.predicted_score,
  predictedRank: row.predicted_rank,
  predictedTop1Prob: row.predicted_top1_prob,
  predictedTop3Prob: row.predicted_top3_prob,
  predictedFinishPosition: row.predicted_finish_position,
  modelVersion: row.model_version,
  runningStyleFeatureVersion: row.running_style_feature_version,
  finishPositionVersion: row.finish_position_version,
});

export const buildPredictionMapFromRows = (
  rows: readonly Record<string, unknown>[],
): Map<string, FinishPositionLocalPrediction> => {
  const entries = rows.map((raw) => {
    const row = coerceRow(raw);
    const key = buildRaceKey(rowToRaceKey(row));
    return [key, rowToPrediction(row)] satisfies [string, FinishPositionLocalPrediction];
  });
  return new Map(entries);
};

export class FinishPositionLocalLoader implements FinishPositionLocalLoaderInterface {
  private readonly map: ReadonlyMap<string, FinishPositionLocalPrediction>;

  constructor(map: ReadonlyMap<string, FinishPositionLocalPrediction>) {
    this.map = map;
  }

  get(raceKey: RunningStyleLocalPredictionRaceKey): FinishPositionLocalPrediction | null {
    return this.map.get(buildRaceKey(raceKey)) ?? null;
  }
}

export const loadFinishPositionLocalLoader = (
  options: FinishPositionLocalLoaderOptions,
): FinishPositionLocalLoader => {
  const database = new options.duckdbModule.Database(":memory:");
  const connection = database.connect();
  const rows = connection.query(buildSelectSql(options.parquetGlob)).toArray();
  connection.close();
  database.close();
  return new FinishPositionLocalLoader(buildPredictionMapFromRows(rows));
};
