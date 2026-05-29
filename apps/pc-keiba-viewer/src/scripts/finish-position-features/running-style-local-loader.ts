// Run with: imported by Agent A's evaluate-bucket-21y.ts (bun runtime).
// Reads local Hive-partitioned parquet from Agent F Phase B output via DuckDB and exposes
// a race-key -> RunningStyleLocalPrediction Map. Implements RunningStyleLocalLoaderInterface.

import type {
  RunningStyleLocalLoaderInterface,
  RunningStyleLocalPrediction,
  RunningStyleLocalPredictionRaceKey,
} from "../../lib/finish-prediction-dimensions";

interface RunningStyleLocalLoaderOptions {
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

interface RawPredictionRow {
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  predicted_label: string;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
  running_style_feature_version: string;
}

const RACE_KEY_DELIMITER = "|";
const SELECT_COLUMNS = [
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
  "predicted_label",
  "p_nige",
  "p_senkou",
  "p_sashi",
  "p_oikomi",
  "running_style_feature_version",
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

const requireNumber = (value: unknown, name: string): number => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`Column ${name} is not numeric.`);
  return parsed;
};

const coerceRow = (row: Record<string, unknown>): RawPredictionRow => ({
  source: requireString(row.source, "source"),
  kaisai_nen: requireString(row.kaisai_nen, "kaisai_nen"),
  kaisai_tsukihi: requireString(row.kaisai_tsukihi, "kaisai_tsukihi"),
  keibajo_code: requireString(row.keibajo_code, "keibajo_code"),
  race_bango: requireString(row.race_bango, "race_bango"),
  ketto_toroku_bango: requireString(row.ketto_toroku_bango, "ketto_toroku_bango"),
  predicted_label: requireString(row.predicted_label, "predicted_label"),
  p_nige: requireNumber(row.p_nige, "p_nige"),
  p_senkou: requireNumber(row.p_senkou, "p_senkou"),
  p_sashi: requireNumber(row.p_sashi, "p_sashi"),
  p_oikomi: requireNumber(row.p_oikomi, "p_oikomi"),
  running_style_feature_version: requireString(
    row.running_style_feature_version,
    "running_style_feature_version",
  ),
});

const rowToRaceKey = (row: RawPredictionRow): RunningStyleLocalPredictionRaceKey => ({
  source: row.source,
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: row.keibajo_code,
  raceBango: row.race_bango,
  kettoTorokuBango: row.ketto_toroku_bango,
});

const rowToPrediction = (row: RawPredictionRow): RunningStyleLocalPrediction => ({
  predictedLabel: row.predicted_label,
  pNige: row.p_nige,
  pSenkou: row.p_senkou,
  pSashi: row.p_sashi,
  pOikomi: row.p_oikomi,
  featureVersion: row.running_style_feature_version,
});

export const buildPredictionMapFromRows = (
  rows: readonly Record<string, unknown>[],
): Map<string, RunningStyleLocalPrediction> => {
  const entries = rows.map((raw) => {
    const row = coerceRow(raw);
    const key = buildRaceKey(rowToRaceKey(row));
    return [key, rowToPrediction(row)] satisfies [string, RunningStyleLocalPrediction];
  });
  return new Map(entries);
};

export class RunningStyleLocalLoader implements RunningStyleLocalLoaderInterface {
  private readonly map: ReadonlyMap<string, RunningStyleLocalPrediction>;

  constructor(map: ReadonlyMap<string, RunningStyleLocalPrediction>) {
    this.map = map;
  }

  get(raceKey: RunningStyleLocalPredictionRaceKey): RunningStyleLocalPrediction | null {
    return this.map.get(buildRaceKey(raceKey)) ?? null;
  }
}

export const loadRunningStyleLocalLoader = (
  options: RunningStyleLocalLoaderOptions,
): RunningStyleLocalLoader => {
  const database = new options.duckdbModule.Database(":memory:");
  const connection = database.connect();
  const rows = connection.query(buildSelectSql(options.parquetGlob)).toArray();
  connection.close();
  database.close();
  return new RunningStyleLocalLoader(buildPredictionMapFromRows(rows));
};
