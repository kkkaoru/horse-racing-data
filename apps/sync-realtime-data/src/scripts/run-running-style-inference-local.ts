// Run with bun: `bun run src/scripts/run-running-style-inference-local.ts \
//   --model-flatbin <path/to/model.flatbin> \
//   --features-parquet <path/to/features.parquet> \
//   --output-parquet <path/to/output.parquet> \
//   --category jra \
//   --predicted-at 2026-05-31T00:00:00Z \
//   --model-version jra-running-style-lgbm-prod-v2 \
//   --feature-version v1 \
//   --rs-p-from-flatbin <path/to/v1.5/model.flatbin>`
//
// Bun-side mirror of the Cloudflare Worker running-style inference pipeline.
// Loads the flat-binary LightGBM model and per-horse parquet features locally
// (no D1, R2, or Worker binding), recomputes the race-internal field features,
// and writes per-horse softmax probabilities back to parquet via DuckDB.
// Because it imports the same production helpers (`decodeFlatLightGBMModel`,
// `predictFlatRunningStyle`, `computeFieldFeaturesPerHorse`) the output is
// bit-exact with the Worker prediction path.
//
// Supports both single-stage (117-feature v1.5 model) and chained-predict
// (138-feature v2 model) inference. A v2 model that lists any of the
// `rs_p_*` columns in its header requires `--rs-p-from-flatbin` pointing at
// the v1.5 model; v1.5 logits are softmaxed into 4 `rs_p_*` columns that
// augment the v2 input vector. The 117 vs 138 distinction is detected from
// `model.header.feature_names`; no command-line mode switch is needed.
//
// Uses the @duckdb/node-api neo async API: `DuckDBInstance.create`,
// `connection.runAndReadAll`, and `connection.run` all return Promises.

import {
  computeFieldFeaturesPerHorse,
  type HorseFieldRow,
  type HorsePeerInputs,
} from "../running-style-field-features";
import {
  decodeFlatLightGBMModel,
  predictFlatRunningStyle,
  type FlatLightGBMModel,
} from "../running-style-model-binary";

interface CliOptions {
  modelFlatbin: string;
  rsPFromFlatbin: string;
  featuresParquet: string;
  outputParquet: string;
  category: "jra" | "nar";
  predictedAt: string;
  modelVersion: string;
  featureVersion: string;
}

interface ApplyArgResult {
  advanceBy: number;
}

interface RaceKey {
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
}

interface RawFeatureRow extends RaceKey {
  ketto_toroku_bango: string;
  perHorseFeatures: Record<string, number | null>;
  peerInputs: HorsePeerInputs;
  targetRunningStyleClass: number | null;
}

interface PredictionRow extends RaceKey {
  ketto_toroku_bango: string;
  p_nige: number;
  p_senkou: number;
  p_sashi: number;
  p_oikomi: number;
  model_version: string;
  running_style_feature_version: string;
  target_running_style_class: number | null;
}

interface DuckDBResultReaderLike {
  getRowObjects(): readonly Record<string, unknown>[];
}

interface DuckDBConnectionLike {
  runAndReadAll(sql: string): Promise<DuckDBResultReaderLike>;
  run(sql: string): Promise<unknown>;
  disconnectSync(): void;
}

interface DuckDBInstanceLike {
  connect(): Promise<DuckDBConnectionLike>;
  closeSync(): void;
}

interface DuckDBInstanceFactory {
  create(path: string): Promise<DuckDBInstanceLike>;
}

interface DuckDBModuleLike {
  DuckDBInstance: DuckDBInstanceFactory;
}

interface ModelFileReader {
  (path: string): Promise<ArrayBuffer>;
}

interface CliLogger {
  info: (message: string) => void;
}

interface RunInferenceLocalParams {
  options: CliOptions;
  duckdbModule: DuckDBModuleLike;
  readModelFile: ModelFileReader;
  logger: CliLogger;
}

interface RaceGroupHorse {
  raw: RawFeatureRow;
  fieldRow: HorseFieldRow;
}

interface ReadFeaturesParams {
  duckdbModule: DuckDBModuleLike;
  featuresParquet: string;
}

interface WriteOutputParams {
  duckdbModule: DuckDBModuleLike;
  outputParquet: string;
  rows: readonly PredictionRow[];
  includeTargetClass: boolean;
}

const PEER_INPUT_COLUMN_MAP: Readonly<Record<keyof HorsePeerInputs, string>> = {
  careerWinRate: "career_win_rate",
  kohan3fAvg5: "kohan3f_avg_5",
  pastCorner1NormAvg5: "past_corner_1_norm_avg_5",
  pastFirst3fAvg5: "past_first_3f_avg_5",
  pastNigeRate: "past_nige_rate_self",
  pastOikomiRate: "past_oikomi_rate_self",
  pastSashiRate: "past_sashi_rate_self",
  pastSenkouRate: "past_senkou_rate_self",
  speedIndexAvg5: "speed_index_avg_5",
  speedIndexBest5: "speed_index_best_5",
};

const FIELD_FEATURE_COLUMN_MAP: Readonly<Record<keyof HorseFieldRow, string>> = {
  field_avg_career_win_rate: "field_avg_career_win_rate",
  field_avg_past_first_3f: "field_avg_past_first_3f",
  field_avg_past_kohan_3f: "field_avg_past_kohan_3f",
  field_avg_speed_index: "field_avg_speed_index",
  field_has_pure_nige_horse: "field_has_pure_nige_horse",
  field_max_past_corner_1_norm: "field_max_past_corner_1_norm",
  field_min_past_corner_1_norm: "field_min_past_corner_1_norm",
  field_nige_candidate_count: "field_nige_candidate_count",
  field_nige_pressure: "field_nige_pressure",
  field_oikomi_pressure: "field_oikomi_pressure",
  field_pace_index: "field_pace_index",
  field_sashi_pressure: "field_sashi_pressure",
  field_senkou_pressure: "field_senkou_pressure",
  field_spread_past_corner_1_norm: "field_spread_past_corner_1_norm",
  field_top_speed_index: "field_top_speed_index",
  self_nige_rate_minus_field_avg: "self_nige_rate_minus_field_avg",
  self_speed_index_vs_field_top: "self_speed_index_vs_field_top",
};

const RACE_KEY_COLUMNS = [
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
] satisfies ReadonlyArray<keyof RaceKey>;

const OUTPUT_COLUMN_NAMES = [
  "source",
  "kaisai_nen",
  "kaisai_tsukihi",
  "keibajo_code",
  "race_bango",
  "ketto_toroku_bango",
  "p_nige",
  "p_senkou",
  "p_sashi",
  "p_oikomi",
  "model_version",
  "running_style_feature_version",
] satisfies readonly string[];

const TARGET_CLASS_COLUMN_NAME = "target_running_style_class";

const RS_P_COLUMN_NAMES = [
  "rs_p_nige",
  "rs_p_senkou",
  "rs_p_sashi",
  "rs_p_oikomi",
] satisfies readonly string[];

const SUPPORTED_CATEGORIES = ["jra", "nar"] satisfies ReadonlyArray<CliOptions["category"]>;
const DUCKDB_MODULE_SPECIFIER = "@duckdb/node-api";
const DUCKDB_IN_MEMORY_PATH = ":memory:";

export const initialOptions = (): CliOptions => ({
  modelFlatbin: "",
  rsPFromFlatbin: "",
  featuresParquet: "",
  outputParquet: "",
  category: "jra",
  predictedAt: "",
  modelVersion: "",
  featureVersion: "",
});

export const buildUsageText = (): string =>
  [
    "Usage:",
    "  bun run src/scripts/run-running-style-inference-local.ts \\",
    "    --model-flatbin <path/to/model.flatbin> \\",
    "    --features-parquet <path/to/features.parquet> \\",
    "    --output-parquet <path/to/output.parquet> \\",
    "    --category <jra|nar> \\",
    "    --predicted-at <ISO 8601> \\",
    "    --model-version <model_version> \\",
    "    --feature-version <feature_version> \\",
    "    [--rs-p-from-flatbin <path/to/v1.5/model.flatbin>]",
  ].join("\n");

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

const requireCategory = (name: string, value: string | undefined): "jra" | "nar" => {
  const raw = requireValue(name, value);
  if (raw === "jra" || raw === "nar") return raw;
  throw new Error(`${name} must be one of: ${SUPPORTED_CATEGORIES.join(", ")}.`);
};

export const applyArg = (
  options: CliOptions,
  name: string,
  value: string | undefined,
): ApplyArgResult => {
  if (name === "--model-flatbin") {
    options.modelFlatbin = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--rs-p-from-flatbin") {
    options.rsPFromFlatbin = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--features-parquet") {
    options.featuresParquet = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--output-parquet") {
    options.outputParquet = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--category") {
    options.category = requireCategory(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--predicted-at") {
    options.predictedAt = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version") {
    options.modelVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--feature-version") {
    options.featureVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--help" || name === "-h") {
    console.log(buildUsageText());
    process.exit(0);
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgs = (options: CliOptions, argv: readonly string[], cursor: number): CliOptions => {
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgs(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): CliOptions => {
  const options = consumeArgs(initialOptions(), argv, 0);
  if (options.modelFlatbin === "") throw new Error("--model-flatbin is required.");
  if (options.featuresParquet === "") throw new Error("--features-parquet is required.");
  if (options.outputParquet === "") throw new Error("--output-parquet is required.");
  if (options.predictedAt === "") throw new Error("--predicted-at is required.");
  if (options.modelVersion === "") throw new Error("--model-version is required.");
  if (options.featureVersion === "") throw new Error("--feature-version is required.");
  return options;
};

const requireString = (value: unknown, name: string): string => {
  if (typeof value !== "string") throw new Error(`Column ${name} is not a string.`);
  return value;
};

const toNumberOrNull = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "string") {
    if (value.length === 0) return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const toIntegerOrNull = (value: unknown): number | null => {
  const parsed = toNumberOrNull(value);
  if (parsed === null) return null;
  return Math.trunc(parsed);
};

export const buildRaceKeyString = (key: RaceKey): string =>
  `${key.source}:${key.kaisai_nen}:${key.kaisai_tsukihi}:${key.keibajo_code}:${key.race_bango}`;

const extractRaceKey = (raw: Record<string, unknown>): RaceKey => ({
  source: requireString(raw["source"], "source"),
  kaisai_nen: requireString(raw["kaisai_nen"], "kaisai_nen"),
  kaisai_tsukihi: requireString(raw["kaisai_tsukihi"], "kaisai_tsukihi"),
  keibajo_code: requireString(raw["keibajo_code"], "keibajo_code"),
  race_bango: requireString(raw["race_bango"], "race_bango"),
});

const extractPeerInputs = (raw: Record<string, unknown>): HorsePeerInputs => {
  const peerInputs: HorsePeerInputs = {
    careerWinRate: null,
    kohan3fAvg5: null,
    pastCorner1NormAvg5: null,
    pastFirst3fAvg5: null,
    pastNigeRate: null,
    pastOikomiRate: null,
    pastSashiRate: null,
    pastSenkouRate: null,
    speedIndexAvg5: null,
    speedIndexBest5: null,
  };
  (Object.keys(PEER_INPUT_COLUMN_MAP) as ReadonlyArray<keyof HorsePeerInputs>).forEach(
    (peerKey) => {
      peerInputs[peerKey] = toNumberOrNull(raw[PEER_INPUT_COLUMN_MAP[peerKey]]);
    },
  );
  return peerInputs;
};

const extractPerHorseFeatures = (
  raw: Record<string, unknown>,
  featureNames: ReadonlyArray<string>,
): Record<string, number | null> => {
  const features: Record<string, number | null> = {};
  featureNames.forEach((name) => {
    features[name] = toNumberOrNull(raw[name]);
  });
  return features;
};

export const hasTargetClassColumn = (rows: ReadonlyArray<Record<string, unknown>>): boolean =>
  rows.some((row) => TARGET_CLASS_COLUMN_NAME in row);

export const buildRawFeatureRow = (
  raw: Record<string, unknown>,
  featureNames: ReadonlyArray<string>,
): RawFeatureRow => {
  const raceKey = extractRaceKey(raw);
  return {
    ...raceKey,
    ketto_toroku_bango: requireString(raw["ketto_toroku_bango"], "ketto_toroku_bango"),
    perHorseFeatures: extractPerHorseFeatures(raw, featureNames),
    peerInputs: extractPeerInputs(raw),
    targetRunningStyleClass: toIntegerOrNull(raw[TARGET_CLASS_COLUMN_NAME]),
  };
};

export const groupRowsByRace = (
  rows: ReadonlyArray<RawFeatureRow>,
): ReadonlyArray<ReadonlyArray<RawFeatureRow>> => {
  const groups = new Map<string, RawFeatureRow[]>();
  rows.forEach((row) => {
    const key = buildRaceKeyString(row);
    const list = groups.get(key);
    if (list === undefined) {
      groups.set(key, [row]);
      return;
    }
    list.push(row);
  });
  return Array.from(groups.values());
};

export const mergeFeatureMap = (
  perHorse: Record<string, number | null>,
  fieldRow: HorseFieldRow,
): Record<string, number | null> => {
  const numericField: Record<string, number | null> = {};
  (Object.keys(FIELD_FEATURE_COLUMN_MAP) as ReadonlyArray<keyof HorseFieldRow>).forEach((key) => {
    const value = fieldRow[key];
    const column = FIELD_FEATURE_COLUMN_MAP[key];
    numericField[column] = typeof value === "boolean" ? Number(value) : value;
  });
  return { ...perHorse, ...numericField };
};

export const modelRequiresChainedPredict = (model: FlatLightGBMModel): boolean =>
  model.header.feature_names.some((name) => name.startsWith("rs_p_"));

interface RsPProbabilityVector {
  rs_p_nige: number;
  rs_p_senkou: number;
  rs_p_sashi: number;
  rs_p_oikomi: number;
}

const predictRsPForHorse = (
  raw: RawFeatureRow,
  fieldRow: HorseFieldRow,
  v1Model: FlatLightGBMModel,
): RsPProbabilityVector => {
  const features = mergeFeatureMap(raw.perHorseFeatures, fieldRow);
  const prediction = predictFlatRunningStyle(v1Model, features);
  return {
    rs_p_nige: prediction.probabilities.nige,
    rs_p_senkou: prediction.probabilities.senkou,
    rs_p_sashi: prediction.probabilities.sashi,
    rs_p_oikomi: prediction.probabilities.oikomi,
  };
};

const augmentPerHorseWithRsP = (
  perHorse: Record<string, number | null>,
  rsP: RsPProbabilityVector,
): Record<string, number | null> => ({
  ...perHorse,
  rs_p_nige: rsP.rs_p_nige,
  rs_p_senkou: rsP.rs_p_senkou,
  rs_p_sashi: rsP.rs_p_sashi,
  rs_p_oikomi: rsP.rs_p_oikomi,
});

interface PredictHorseParams {
  raw: RawFeatureRow;
  fieldRow: HorseFieldRow;
  model: FlatLightGBMModel;
  v1Model: FlatLightGBMModel | null;
  modelVersion: string;
  featureVersion: string;
}

const predictHorse = (params: PredictHorseParams): PredictionRow => {
  const rsPAugment =
    params.v1Model === null
      ? params.raw.perHorseFeatures
      : augmentPerHorseWithRsP(
          params.raw.perHorseFeatures,
          predictRsPForHorse(params.raw, params.fieldRow, params.v1Model),
        );
  const features = mergeFeatureMap(rsPAugment, params.fieldRow);
  const prediction = predictFlatRunningStyle(params.model, features);
  return {
    source: params.raw.source,
    kaisai_nen: params.raw.kaisai_nen,
    kaisai_tsukihi: params.raw.kaisai_tsukihi,
    keibajo_code: params.raw.keibajo_code,
    race_bango: params.raw.race_bango,
    ketto_toroku_bango: params.raw.ketto_toroku_bango,
    p_nige: prediction.probabilities.nige,
    p_senkou: prediction.probabilities.senkou,
    p_sashi: prediction.probabilities.sashi,
    p_oikomi: prediction.probabilities.oikomi,
    model_version: params.modelVersion,
    running_style_feature_version: params.featureVersion,
    target_running_style_class: params.raw.targetRunningStyleClass,
  };
};

interface PredictRaceParams {
  group: ReadonlyArray<RawFeatureRow>;
  model: FlatLightGBMModel;
  v1Model: FlatLightGBMModel | null;
  modelVersion: string;
  featureVersion: string;
}

export const predictRace = (params: PredictRaceParams): ReadonlyArray<PredictionRow> => {
  const fieldRows = computeFieldFeaturesPerHorse(params.group.map((row) => row.peerInputs));
  const horses: RaceGroupHorse[] = params.group.map((raw, index) => ({
    raw,
    fieldRow: fieldRows[index]!,
  }));
  return horses.map((horse) =>
    predictHorse({
      raw: horse.raw,
      fieldRow: horse.fieldRow,
      model: params.model,
      v1Model: params.v1Model,
      modelVersion: params.modelVersion,
      featureVersion: params.featureVersion,
    }),
  );
};

interface PredictAllParams {
  rows: ReadonlyArray<RawFeatureRow>;
  model: FlatLightGBMModel;
  v1Model: FlatLightGBMModel | null;
  modelVersion: string;
  featureVersion: string;
}

export const predictAll = (params: PredictAllParams): ReadonlyArray<PredictionRow> => {
  const grouped = groupRowsByRace(params.rows);
  return grouped.flatMap((group) =>
    predictRace({
      group,
      model: params.model,
      v1Model: params.v1Model,
      modelVersion: params.modelVersion,
      featureVersion: params.featureVersion,
    }),
  );
};

export const buildReadFeaturesSql = (featuresParquet: string): string =>
  `SELECT * FROM read_parquet('${featuresParquet}')`;

const openConnection = async (
  duckdbModule: DuckDBModuleLike,
): Promise<{ instance: DuckDBInstanceLike; connection: DuckDBConnectionLike }> => {
  const instance = await duckdbModule.DuckDBInstance.create(DUCKDB_IN_MEMORY_PATH);
  const connection = await instance.connect();
  return { instance, connection };
};

const closeConnection = (instance: DuckDBInstanceLike, connection: DuckDBConnectionLike): void => {
  connection.disconnectSync();
  instance.closeSync();
};

export const readFeatures = async (
  params: ReadFeaturesParams,
): Promise<readonly Record<string, unknown>[]> => {
  const { instance, connection } = await openConnection(params.duckdbModule);
  try {
    const reader = await connection.runAndReadAll(buildReadFeaturesSql(params.featuresParquet));
    return reader.getRowObjects();
  } finally {
    closeConnection(instance, connection);
  }
};

const escapeStringLiteral = (value: string): string => value.replaceAll("'", "''");

const formatValueForSql = (value: string | number | null): string => {
  if (value === null) return "NULL";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "NULL";
  return `'${escapeStringLiteral(value)}'`;
};

const baseTupleValues = (row: PredictionRow): string[] => [
  formatValueForSql(row.source),
  formatValueForSql(row.kaisai_nen),
  formatValueForSql(row.kaisai_tsukihi),
  formatValueForSql(row.keibajo_code),
  formatValueForSql(row.race_bango),
  formatValueForSql(row.ketto_toroku_bango),
  formatValueForSql(row.p_nige),
  formatValueForSql(row.p_senkou),
  formatValueForSql(row.p_sashi),
  formatValueForSql(row.p_oikomi),
  formatValueForSql(row.model_version),
  formatValueForSql(row.running_style_feature_version),
];

const formatRowAsValuesTuple = (row: PredictionRow, includeTargetClass: boolean): string => {
  const base = baseTupleValues(row);
  if (!includeTargetClass) return base.join(", ");
  const targetValue = formatValueForSql(row.target_running_style_class);
  return [...base, targetValue].join(", ");
};

const baseEmptyColumnDefinitions = (): readonly string[] => [
  "CAST(NULL AS VARCHAR) AS source",
  "CAST(NULL AS VARCHAR) AS kaisai_nen",
  "CAST(NULL AS VARCHAR) AS kaisai_tsukihi",
  "CAST(NULL AS VARCHAR) AS keibajo_code",
  "CAST(NULL AS VARCHAR) AS race_bango",
  "CAST(NULL AS VARCHAR) AS ketto_toroku_bango",
  "CAST(NULL AS DOUBLE) AS p_nige",
  "CAST(NULL AS DOUBLE) AS p_senkou",
  "CAST(NULL AS DOUBLE) AS p_sashi",
  "CAST(NULL AS DOUBLE) AS p_oikomi",
  "CAST(NULL AS VARCHAR) AS model_version",
  "CAST(NULL AS VARCHAR) AS running_style_feature_version",
];

export const buildEmptyOutputCopySql = (
  outputParquet: string,
  includeTargetClass: boolean,
): string => {
  const base = baseEmptyColumnDefinitions();
  const columns = (
    includeTargetClass ? [...base, "CAST(NULL AS INTEGER) AS target_running_style_class"] : base
  ).join(", ");
  return `COPY (SELECT ${columns} WHERE 1 = 0) TO '${outputParquet}' (FORMAT PARQUET, COMPRESSION ZSTD)`;
};

export const buildWriteOutputCopySql = (
  outputParquet: string,
  rows: readonly PredictionRow[],
  includeTargetClass: boolean,
): string => {
  const tuples = rows
    .map((row) => `(${formatRowAsValuesTuple(row, includeTargetClass)})`)
    .join(", ");
  const columns = (
    includeTargetClass ? [...OUTPUT_COLUMN_NAMES, TARGET_CLASS_COLUMN_NAME] : OUTPUT_COLUMN_NAMES
  ).join(", ");
  return `COPY (SELECT * FROM (VALUES ${tuples}) AS t(${columns})) TO '${outputParquet}' (FORMAT PARQUET, COMPRESSION ZSTD)`;
};

export const writeOutput = async (params: WriteOutputParams): Promise<void> => {
  const { instance, connection } = await openConnection(params.duckdbModule);
  try {
    if (params.rows.length === 0) {
      await connection.run(
        buildEmptyOutputCopySql(params.outputParquet, params.includeTargetClass),
      );
      return;
    }
    await connection.run(
      buildWriteOutputCopySql(params.outputParquet, params.rows, params.includeTargetClass),
    );
  } finally {
    closeConnection(instance, connection);
  }
};

export const decodeModelFromBuffer = (buffer: ArrayBuffer): FlatLightGBMModel =>
  decodeFlatLightGBMModel(buffer);

interface LoadV1ModelParams {
  options: CliOptions;
  v2Model: FlatLightGBMModel;
  readModelFile: ModelFileReader;
}

const loadV1ModelIfRequired = async (
  params: LoadV1ModelParams,
): Promise<FlatLightGBMModel | null> => {
  const requiresChained = modelRequiresChainedPredict(params.v2Model);
  if (!requiresChained) return null;
  if (params.options.rsPFromFlatbin === "") {
    throw new Error(
      "--rs-p-from-flatbin is required when the primary model includes rs_p_* features.",
    );
  }
  const buffer = await params.readModelFile(params.options.rsPFromFlatbin);
  return decodeModelFromBuffer(buffer);
};

export const runInferenceLocal = async (
  params: RunInferenceLocalParams,
): Promise<{ rowCount: number; raceCount: number }> => {
  const buffer = await params.readModelFile(params.options.modelFlatbin);
  const model = decodeModelFromBuffer(buffer);
  const v1Model = await loadV1ModelIfRequired({
    options: params.options,
    v2Model: model,
    readModelFile: params.readModelFile,
  });
  const featureNames = model.header.feature_names;
  const rawRows = await readFeatures({
    duckdbModule: params.duckdbModule,
    featuresParquet: params.options.featuresParquet,
  });
  const includeTargetClass = hasTargetClassColumn(rawRows);
  const rows = rawRows.map((raw) => buildRawFeatureRow(raw, featureNames));
  const predictions = predictAll({
    rows,
    model,
    v1Model,
    modelVersion: params.options.modelVersion,
    featureVersion: params.options.featureVersion,
  });
  await writeOutput({
    duckdbModule: params.duckdbModule,
    outputParquet: params.options.outputParquet,
    rows: predictions,
    includeTargetClass,
  });
  const raceCount = groupRowsByRace(rows).length;
  params.logger.info(
    `[run-running-style-inference-local] category=${params.options.category} predicted_at=${params.options.predictedAt} model_version=${params.options.modelVersion} feature_version=${params.options.featureVersion} input=${params.options.featuresParquet} output=${params.options.outputParquet} races=${raceCount} rows=${predictions.length} chained_predict=${v1Model === null ? "no" : "yes"} include_target_class=${includeTargetClass ? "yes" : "no"}`,
  );
  return { rowCount: predictions.length, raceCount };
};

interface RunCliParams {
  argv: readonly string[];
  duckdbModule: DuckDBModuleLike;
  readModelFile: ModelFileReader;
  logger: CliLogger;
}

export const runCli = async (
  params: RunCliParams,
): Promise<{ rowCount: number; raceCount: number }> => {
  const options = parseArgs(params.argv);
  return runInferenceLocal({
    options,
    duckdbModule: params.duckdbModule,
    readModelFile: params.readModelFile,
    logger: params.logger,
  });
};

const isObjectRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isDuckdbInstanceFactory = (value: unknown): value is DuckDBInstanceFactory => {
  if (!isObjectRecord(value) && typeof value !== "function") return false;
  const candidate = value as { create?: unknown };
  return typeof candidate.create === "function";
};

const isDuckdbModuleLike = (value: unknown): value is DuckDBModuleLike => {
  if (!isObjectRecord(value)) return false;
  return isDuckdbInstanceFactory(value["DuckDBInstance"]);
};

const extractNestedDefault = (value: Record<string, unknown>): unknown => value["default"];

export const resolveDuckdbModule = (moduleNamespace: unknown): DuckDBModuleLike => {
  if (isDuckdbModuleLike(moduleNamespace)) return moduleNamespace;
  if (isObjectRecord(moduleNamespace)) {
    const nested = extractNestedDefault(moduleNamespace);
    if (isDuckdbModuleLike(nested)) return nested;
  }
  throw new Error("@duckdb/node-api does not export DuckDBInstance.");
};

export const loadDuckdbModule = async (): Promise<DuckDBModuleLike> => {
  const specifier: string = DUCKDB_MODULE_SPECIFIER;
  const moduleNamespace: unknown = await import(specifier);
  return resolveDuckdbModule(moduleNamespace);
};

export const defaultLogger = (): CliLogger => ({
  info: (message) => {
    console.log(message);
  },
});

interface BunFileLike {
  arrayBuffer(): Promise<ArrayBuffer>;
}

interface BunRuntime {
  file(path: string): BunFileLike;
}

export const getBunRuntime = (): BunRuntime => {
  const candidate = (globalThis as { Bun?: BunRuntime }).Bun;
  if (candidate === undefined) throw new Error("Bun runtime not available.");
  return candidate;
};

export const defaultReadModelFile: ModelFileReader = (path) =>
  getBunRuntime().file(path).arrayBuffer();

/* v8 ignore next 13 */
const main = async (): Promise<void> => {
  const duckdbModule = await loadDuckdbModule();
  await runCli({
    argv: process.argv.slice(2),
    duckdbModule,
    readModelFile: defaultReadModelFile,
    logger: defaultLogger(),
  });
};
if (import.meta.main) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}

export {
  RACE_KEY_COLUMNS,
  OUTPUT_COLUMN_NAMES,
  PEER_INPUT_COLUMN_MAP,
  FIELD_FEATURE_COLUMN_MAP,
  SUPPORTED_CATEGORIES,
  TARGET_CLASS_COLUMN_NAME,
  RS_P_COLUMN_NAMES,
};

export type {
  CliOptions,
  RawFeatureRow,
  PredictionRow,
  RaceKey,
  DuckDBModuleLike,
  DuckDBInstanceLike,
  DuckDBInstanceFactory,
  DuckDBConnectionLike,
  DuckDBResultReaderLike,
  ModelFileReader,
  CliLogger,
};
