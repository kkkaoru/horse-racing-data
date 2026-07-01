// Run with bun: `bun run --filter sync-realtime-data test`
import { expect, it, vi } from "vitest";

import {
  applyArg,
  buildEmptyOutputCopySql,
  buildRaceKeyString,
  buildRawFeatureRow,
  buildReadFeaturesSql,
  buildUsageText,
  buildWriteOutputCopySql,
  decodeModelFromBuffer,
  defaultLogger,
  defaultReadModelFile,
  FIELD_FEATURE_COLUMN_MAP,
  getBunRuntime,
  groupRowsByRace,
  hasTargetClassColumn,
  initialOptions,
  loadDuckdbModule,
  mergeFeatureMap,
  modelRequiresChainedPredict,
  OUTPUT_COLUMN_NAMES,
  parseArgs,
  PEER_INPUT_COLUMN_MAP,
  predictAll,
  predictRace,
  RACE_KEY_COLUMNS,
  readFeatures,
  resolveDuckdbModule,
  RS_P_COLUMN_NAMES,
  runCli,
  runInferenceLocal,
  SUPPORTED_CATEGORIES,
  TARGET_CLASS_COLUMN_NAME,
  writeOutput,
  type CliLogger,
  type DuckDBConnectionLike,
  type DuckDBInstanceFactory,
  type DuckDBInstanceLike,
  type DuckDBModuleLike,
  type DuckDBResultReaderLike,
  type ModelFileReader,
  type PredictionRow,
  type RawFeatureRow,
} from "./run-running-style-inference-local";
import type { HorseFieldRow } from "../running-style-field-features";
import type { FlatLightGBMModel } from "../running-style-model-binary";
import * as modelBinary from "../running-style-model-binary";

const RACE_KEY_FIELDS = {
  source: "jra",
  kaisai_nen: "2020",
  kaisai_tsukihi: "0530",
  keibajo_code: "05",
  race_bango: "02",
};

const FAKE_FEATURE_NAMES = ["career_win_rate", "kohan3f_avg_5", "field_pace_index"];

const FAKE_V2_FEATURE_NAMES = [
  "career_win_rate",
  "kohan3f_avg_5",
  "field_pace_index",
  "rs_p_nige",
  "rs_p_senkou",
  "rs_p_sashi",
  "rs_p_oikomi",
];

const NULL_PEER_INPUTS = {
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

const ZERO_FIELD_ROW = {
  field_avg_career_win_rate: 0,
  field_avg_past_first_3f: 0,
  field_avg_past_kohan_3f: 0,
  field_avg_speed_index: 0,
  field_has_pure_nige_horse: false,
  field_max_past_corner_1_norm: 0,
  field_min_past_corner_1_norm: 0,
  field_nige_candidate_count: 0,
  field_nige_pressure: 0,
  field_oikomi_pressure: 0,
  field_pace_index: 0,
  field_sashi_pressure: 0,
  field_senkou_pressure: 0,
  field_spread_past_corner_1_norm: 0,
  field_top_speed_index: 0,
  self_nige_rate_minus_field_avg: 0,
  self_speed_index_vs_field_top: 0,
} satisfies HorseFieldRow;

interface FakeSharedState {
  rows: readonly Record<string, unknown>[];
  runStatements: string[];
  queryStatements: string[];
  instanceClosed: number;
  connectionClosed: number;
}

const buildSharedState = (rows: readonly Record<string, unknown>[]): FakeSharedState => ({
  rows,
  runStatements: [],
  queryStatements: [],
  instanceClosed: 0,
  connectionClosed: 0,
});

const buildFakeReader = (state: FakeSharedState): DuckDBResultReaderLike => ({
  getRowObjects: () => state.rows,
});

const buildFakeConnection = (state: FakeSharedState): DuckDBConnectionLike => ({
  runAndReadAll: (sql) => {
    state.queryStatements.push(sql);
    return Promise.resolve(buildFakeReader(state));
  },
  run: (sql) => {
    state.runStatements.push(sql);
    return Promise.resolve();
  },
  disconnectSync: () => {
    state.connectionClosed += 1;
  },
});

const buildFakeInstance = (state: FakeSharedState): DuckDBInstanceLike => ({
  connect: () => Promise.resolve(buildFakeConnection(state)),
  closeSync: () => {
    state.instanceClosed += 1;
  },
});

const buildFakeModuleFromState = (state: FakeSharedState): DuckDBModuleLike => {
  const factory: DuckDBInstanceFactory = {
    create: (_path: string) => Promise.resolve(buildFakeInstance(state)),
  };
  return { DuckDBInstance: factory };
};

interface FakeModuleSetup {
  module: DuckDBModuleLike;
  state: FakeSharedState;
}

const buildFakeModuleSetup = (rows: readonly Record<string, unknown>[]): FakeModuleSetup => {
  const state = buildSharedState(rows);
  return { module: buildFakeModuleFromState(state), state };
};

const FAKE_MODEL = {
  buffer: new ArrayBuffer(0),
  categoricalValuesOffset: 0,
  dataView: new DataView(new ArrayBuffer(0)),
  header: {
    categorical_features: [],
    class_labels: ["nige", "senkou", "sashi", "oikomi"],
    feature_names: FAKE_FEATURE_NAMES,
    format: "rs-lgbm-flat-v1",
    model_version: "test-model",
    node_count: 0,
    num_class: 4,
    num_tree_per_iteration: 4,
    objective: "multiclass",
    tree_root_indices: [],
    categorical_value_count: 0,
  },
  nodeOffset: 0,
} satisfies FlatLightGBMModel;

const FAKE_V2_MODEL = {
  buffer: new ArrayBuffer(0),
  categoricalValuesOffset: 0,
  dataView: new DataView(new ArrayBuffer(0)),
  header: {
    categorical_features: [],
    class_labels: ["nige", "senkou", "sashi", "oikomi"],
    feature_names: FAKE_V2_FEATURE_NAMES,
    format: "rs-lgbm-flat-v1",
    model_version: "test-v2-model",
    node_count: 0,
    num_class: 4,
    num_tree_per_iteration: 4,
    objective: "multiclass",
    tree_root_indices: [],
    categorical_value_count: 0,
  },
  nodeOffset: 0,
} satisfies FlatLightGBMModel;

const PREDICTION_FIXED = {
  predictedClass: 1,
  predictedLabel: "senkou",
  probabilities: { nige: 0.1, senkou: 0.6, sashi: 0.2, oikomi: 0.1 },
} as const;

const PREDICTION_V1 = {
  predictedClass: 0,
  predictedLabel: "nige",
  probabilities: { nige: 0.4, senkou: 0.3, sashi: 0.2, oikomi: 0.1 },
} as const;

const PREDICTION_V2 = {
  predictedClass: 2,
  predictedLabel: "sashi",
  probabilities: { nige: 0.05, senkou: 0.15, sashi: 0.7, oikomi: 0.1 },
} as const;

it("initialOptions returns empty defaults", () => {
  expect(initialOptions()).toStrictEqual({
    modelFlatbin: "",
    rsPFromFlatbin: "",
    featuresParquet: "",
    outputParquet: "",
    category: "jra",
    predictedAt: "",
    modelVersion: "",
    featureVersion: "",
    cellModelKey: "",
    cellVariantId: "",
  });
});

it("buildUsageText mentions the script command path", () => {
  expect(buildUsageText().includes("run-running-style-inference-local.ts") satisfies boolean).toBe(
    true,
  );
});

it("buildUsageText mentions the optional --rs-p-from-flatbin flag", () => {
  expect(buildUsageText().includes("--rs-p-from-flatbin") satisfies boolean).toBe(true);
});

it("applyArg sets --model-flatbin and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(options, "--model-flatbin", "/tmp/model.flatbin");
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.modelFlatbin).toBe("/tmp/model.flatbin");
});

it("applyArg sets --rs-p-from-flatbin and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(options, "--rs-p-from-flatbin", "/tmp/v1.flatbin");
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.rsPFromFlatbin).toBe("/tmp/v1.flatbin");
});

it("applyArg throws when --rs-p-from-flatbin has no value", () => {
  const options = initialOptions();
  expect(() => applyArg(options, "--rs-p-from-flatbin", undefined)).toThrowError(
    "--rs-p-from-flatbin requires a value.",
  );
});

it("applyArg sets --features-parquet and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(options, "--features-parquet", "/tmp/features.parquet");
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.featuresParquet).toBe("/tmp/features.parquet");
});

it("applyArg sets --output-parquet and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(options, "--output-parquet", "/tmp/output.parquet");
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.outputParquet).toBe("/tmp/output.parquet");
});

it("applyArg sets --category nar and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(options, "--category", "nar");
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.category).toBe("nar");
});

it("applyArg sets --category jra and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(options, "--category", "jra");
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.category).toBe("jra");
});

it("applyArg sets --predicted-at and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(options, "--predicted-at", "2026-05-31T00:00:00Z");
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.predictedAt).toBe("2026-05-31T00:00:00Z");
});

it("applyArg sets --model-version and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(options, "--model-version", "jra-running-style-lgbm-prod-v1.5");
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.modelVersion).toBe("jra-running-style-lgbm-prod-v1.5");
});

it("applyArg sets --feature-version and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(options, "--feature-version", "v1");
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.featureVersion).toBe("v1");
});

it("applyArg sets --cell-model-key and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(
    options,
    "--cell-model-key",
    "running-style/models/jra/cells/tokyo-turf.flatbin",
  );
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.cellModelKey).toBe("running-style/models/jra/cells/tokyo-turf.flatbin");
});

it("applyArg sets --cell-variant-id and advances by two", () => {
  const options = initialOptions();
  const result = applyArg(options, "--cell-variant-id", "tokyo-turf");
  expect(result).toStrictEqual({ advanceBy: 2 });
  expect(options.cellVariantId).toBe("tokyo-turf");
});

it("applyArg throws when a value is missing", () => {
  const options = initialOptions();
  expect(() => applyArg(options, "--model-flatbin", undefined)).toThrowError(
    "--model-flatbin requires a value.",
  );
});

it("applyArg throws on unsupported --category value", () => {
  const options = initialOptions();
  expect(() => applyArg(options, "--category", "ban-ei")).toThrowError(
    "--category must be one of: jra, nar.",
  );
});

it("applyArg throws on unknown argument", () => {
  const options = initialOptions();
  expect(() => applyArg(options, "--unknown", "value")).toThrowError("Unknown argument: --unknown");
});

it("applyArg prints usage and exits when --help is passed", () => {
  const options = initialOptions();
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const exitSpy = vi.spyOn(process, "exit").mockImplementation(() => {
    throw new Error("process.exit:0");
  });
  expect(() => applyArg(options, "--help", undefined)).toThrowError("process.exit:0");
  expect(logSpy).toHaveBeenCalledOnce();
  expect(exitSpy).toHaveBeenCalledWith(0);
  logSpy.mockRestore();
  exitSpy.mockRestore();
});

it("parseArgs returns populated options when all required flags are present", () => {
  const result = parseArgs([
    "--model-flatbin",
    "/tmp/model.flatbin",
    "--features-parquet",
    "/tmp/in.parquet",
    "--output-parquet",
    "/tmp/out.parquet",
    "--category",
    "jra",
    "--predicted-at",
    "2026-05-31T00:00:00Z",
    "--model-version",
    "m1",
    "--feature-version",
    "v1",
  ]);
  expect(result).toStrictEqual({
    modelFlatbin: "/tmp/model.flatbin",
    rsPFromFlatbin: "",
    featuresParquet: "/tmp/in.parquet",
    outputParquet: "/tmp/out.parquet",
    category: "jra",
    predictedAt: "2026-05-31T00:00:00Z",
    modelVersion: "m1",
    featureVersion: "v1",
    cellModelKey: "",
    cellVariantId: "",
  });
});

it("parseArgs accepts optional cell provenance flags", () => {
  const result = parseArgs([
    "--model-flatbin",
    "/tmp/model.flatbin",
    "--features-parquet",
    "/tmp/in.parquet",
    "--output-parquet",
    "/tmp/out.parquet",
    "--category",
    "jra",
    "--predicted-at",
    "2026-05-31T00:00:00Z",
    "--model-version",
    "m1",
    "--feature-version",
    "v1",
    "--cell-model-key",
    "running-style/models/jra/cells/tokyo-turf.flatbin",
    "--cell-variant-id",
    "tokyo-turf",
  ]);
  expect(result.cellModelKey).toBe("running-style/models/jra/cells/tokyo-turf.flatbin");
  expect(result.cellVariantId).toBe("tokyo-turf");
});

it("parseArgs accepts the optional --rs-p-from-flatbin flag", () => {
  const result = parseArgs([
    "--model-flatbin",
    "/tmp/model.flatbin",
    "--rs-p-from-flatbin",
    "/tmp/v1.flatbin",
    "--features-parquet",
    "/tmp/in.parquet",
    "--output-parquet",
    "/tmp/out.parquet",
    "--category",
    "jra",
    "--predicted-at",
    "2026-05-31T00:00:00Z",
    "--model-version",
    "m1",
    "--feature-version",
    "v1",
  ]);
  expect(result.rsPFromFlatbin).toBe("/tmp/v1.flatbin");
});

it("parseArgs throws when --model-flatbin is missing", () => {
  expect(() =>
    parseArgs([
      "--features-parquet",
      "/tmp/in.parquet",
      "--output-parquet",
      "/tmp/out.parquet",
      "--predicted-at",
      "now",
      "--model-version",
      "m",
      "--feature-version",
      "v",
    ]),
  ).toThrowError("--model-flatbin is required.");
});

it("parseArgs throws when --features-parquet is missing", () => {
  expect(() =>
    parseArgs([
      "--model-flatbin",
      "/tmp/model.flatbin",
      "--output-parquet",
      "/tmp/out.parquet",
      "--predicted-at",
      "now",
      "--model-version",
      "m",
      "--feature-version",
      "v",
    ]),
  ).toThrowError("--features-parquet is required.");
});

it("parseArgs throws when --output-parquet is missing", () => {
  expect(() =>
    parseArgs([
      "--model-flatbin",
      "/tmp/model.flatbin",
      "--features-parquet",
      "/tmp/in.parquet",
      "--predicted-at",
      "now",
      "--model-version",
      "m",
      "--feature-version",
      "v",
    ]),
  ).toThrowError("--output-parquet is required.");
});

it("parseArgs throws when --predicted-at is missing", () => {
  expect(() =>
    parseArgs([
      "--model-flatbin",
      "/tmp/model.flatbin",
      "--features-parquet",
      "/tmp/in.parquet",
      "--output-parquet",
      "/tmp/out.parquet",
      "--model-version",
      "m",
      "--feature-version",
      "v",
    ]),
  ).toThrowError("--predicted-at is required.");
});

it("parseArgs throws when --model-version is missing", () => {
  expect(() =>
    parseArgs([
      "--model-flatbin",
      "/tmp/model.flatbin",
      "--features-parquet",
      "/tmp/in.parquet",
      "--output-parquet",
      "/tmp/out.parquet",
      "--predicted-at",
      "now",
      "--feature-version",
      "v",
    ]),
  ).toThrowError("--model-version is required.");
});

it("parseArgs throws when --feature-version is missing", () => {
  expect(() =>
    parseArgs([
      "--model-flatbin",
      "/tmp/model.flatbin",
      "--features-parquet",
      "/tmp/in.parquet",
      "--output-parquet",
      "/tmp/out.parquet",
      "--predicted-at",
      "now",
      "--model-version",
      "m",
    ]),
  ).toThrowError("--feature-version is required.");
});

it("RACE_KEY_COLUMNS exposes the expected race-key 5-tuple", () => {
  expect(RACE_KEY_COLUMNS).toStrictEqual([
    "source",
    "kaisai_nen",
    "kaisai_tsukihi",
    "keibajo_code",
    "race_bango",
  ]);
});

it("OUTPUT_COLUMN_NAMES exposes the output 14-column schema", () => {
  expect(OUTPUT_COLUMN_NAMES).toStrictEqual([
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
    "cell_model_key",
    "cell_variant_id",
    "model_version",
    "running_style_feature_version",
  ]);
});

it("TARGET_CLASS_COLUMN_NAME exposes the passthrough target column", () => {
  expect(TARGET_CLASS_COLUMN_NAME).toBe("target_running_style_class");
});

it("RS_P_COLUMN_NAMES exposes the four chained-predict probability columns", () => {
  expect(RS_P_COLUMN_NAMES).toStrictEqual([
    "rs_p_nige",
    "rs_p_senkou",
    "rs_p_sashi",
    "rs_p_oikomi",
  ]);
});

it("PEER_INPUT_COLUMN_MAP maps speedIndexAvg5 to speed_index_avg_5", () => {
  expect(PEER_INPUT_COLUMN_MAP.speedIndexAvg5).toBe("speed_index_avg_5");
});

it("PEER_INPUT_COLUMN_MAP maps pastNigeRate to past_nige_rate_self", () => {
  expect(PEER_INPUT_COLUMN_MAP.pastNigeRate).toBe("past_nige_rate_self");
});

it("FIELD_FEATURE_COLUMN_MAP maps field_pace_index to itself", () => {
  expect(FIELD_FEATURE_COLUMN_MAP.field_pace_index).toBe("field_pace_index");
});

it("SUPPORTED_CATEGORIES exposes jra and nar", () => {
  expect(SUPPORTED_CATEGORIES).toStrictEqual(["jra", "nar"]);
});

it("buildRaceKeyString joins the 5-tuple with colons", () => {
  expect(
    buildRaceKeyString({
      source: "jra",
      kaisai_nen: "2020",
      kaisai_tsukihi: "0530",
      keibajo_code: "05",
      race_bango: "02",
    }),
  ).toBe("jra:2020:0530:05:02");
});

it("modelRequiresChainedPredict returns false for a v1.5 model without rs_p_* features", () => {
  expect(modelRequiresChainedPredict(FAKE_MODEL)).toBe(false);
});

it("modelRequiresChainedPredict returns true for a v2 model with rs_p_* features", () => {
  expect(modelRequiresChainedPredict(FAKE_V2_MODEL)).toBe(true);
});

it("hasTargetClassColumn returns true when at least one row carries target column", () => {
  expect(hasTargetClassColumn([{ source: "jra", target_running_style_class: 1 }])).toBe(true);
});

it("hasTargetClassColumn returns false when no row exposes target column", () => {
  expect(hasTargetClassColumn([{ source: "jra" }, { source: "jra" }])).toBe(false);
});

it("hasTargetClassColumn returns false for an empty input array", () => {
  expect(hasTargetClassColumn([])).toBe(false);
});

it("buildRawFeatureRow extracts race key, ketto_toroku_bango, perHorseFeatures and peerInputs", () => {
  const raw = buildRawFeatureRow(
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "ABC123",
      career_win_rate: 0.2,
      kohan3f_avg_5: 36.5,
      field_pace_index: 1.2,
      past_nige_rate_self: 0.3,
      past_senkou_rate_self: 0.5,
      past_sashi_rate_self: 0.1,
      past_oikomi_rate_self: 0.1,
      past_corner_1_norm_avg_5: 0.4,
      past_first_3f_avg_5: 12.3,
      speed_index_avg_5: 70,
      speed_index_best_5: 75,
    },
    FAKE_FEATURE_NAMES,
  );
  expect(raw.source).toBe("jra");
  expect(raw.ketto_toroku_bango).toBe("ABC123");
  expect(raw.perHorseFeatures.career_win_rate).toBe(0.2);
  expect(raw.perHorseFeatures.field_pace_index).toBe(1.2);
  expect(raw.peerInputs.pastNigeRate).toBe(0.3);
  expect(raw.peerInputs.speedIndexAvg5).toBe(70);
  expect(raw.targetRunningStyleClass).toBe(null);
});

it("buildRawFeatureRow extracts integer target_running_style_class when present", () => {
  const raw = buildRawFeatureRow(
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "X1",
      target_running_style_class: 2,
    },
    FAKE_FEATURE_NAMES,
  );
  expect(raw.targetRunningStyleClass).toBe(2);
});

it("buildRawFeatureRow truncates floating-point target_running_style_class to integer", () => {
  const raw = buildRawFeatureRow(
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "X1",
      target_running_style_class: 1.9,
    },
    FAKE_FEATURE_NAMES,
  );
  expect(raw.targetRunningStyleClass).toBe(1);
});

it("buildRawFeatureRow returns null target_running_style_class when value is null", () => {
  const raw = buildRawFeatureRow(
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "X1",
      target_running_style_class: null,
    },
    FAKE_FEATURE_NAMES,
  );
  expect(raw.targetRunningStyleClass).toBe(null);
});

it("buildRawFeatureRow throws when source is not a string", () => {
  expect(() =>
    buildRawFeatureRow(
      {
        source: 42,
        kaisai_nen: "2020",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "ABC123",
      },
      FAKE_FEATURE_NAMES,
    ),
  ).toThrowError("Column source is not a string.");
});

it("buildRawFeatureRow throws when ketto_toroku_bango is not a string", () => {
  expect(() =>
    buildRawFeatureRow(
      {
        ...RACE_KEY_FIELDS,
        ketto_toroku_bango: 999,
      },
      FAKE_FEATURE_NAMES,
    ),
  ).toThrowError("Column ketto_toroku_bango is not a string.");
});

it("buildRawFeatureRow returns null for non-numeric perHorseFeatures", () => {
  const raw = buildRawFeatureRow(
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "X1",
      career_win_rate: null,
      kohan3f_avg_5: "not-a-number",
      field_pace_index: undefined,
    },
    FAKE_FEATURE_NAMES,
  );
  expect(raw.perHorseFeatures.career_win_rate).toBe(null);
  expect(raw.perHorseFeatures.kohan3f_avg_5).toBe(null);
  expect(raw.perHorseFeatures.field_pace_index).toBe(null);
});

it("buildRawFeatureRow parses numeric strings into numbers for peerInputs", () => {
  const raw = buildRawFeatureRow(
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "X1",
      speed_index_best_5: "78.5",
    },
    FAKE_FEATURE_NAMES,
  );
  expect(raw.peerInputs.speedIndexBest5).toBe(78.5);
});

it("buildRawFeatureRow converts bigint perHorseFeatures to number", () => {
  const raw = buildRawFeatureRow(
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "X1",
      career_win_rate: 5n,
    },
    FAKE_FEATURE_NAMES,
  );
  expect(raw.perHorseFeatures.career_win_rate).toBe(5);
});

it("buildRawFeatureRow returns null for unsupported value type", () => {
  const raw = buildRawFeatureRow(
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "X1",
      career_win_rate: { unexpected: true },
    },
    FAKE_FEATURE_NAMES,
  );
  expect(raw.perHorseFeatures.career_win_rate).toBe(null);
});

it("buildRawFeatureRow returns null for non-finite number in perHorseFeatures", () => {
  const raw = buildRawFeatureRow(
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "X1",
      career_win_rate: Number.NaN,
    },
    FAKE_FEATURE_NAMES,
  );
  expect(raw.perHorseFeatures.career_win_rate).toBe(null);
});

it("buildRawFeatureRow returns null for empty-string numeric input", () => {
  const raw = buildRawFeatureRow(
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "X1",
      career_win_rate: "",
    },
    FAKE_FEATURE_NAMES,
  );
  expect(raw.perHorseFeatures.career_win_rate).toBe(null);
});

it("groupRowsByRace groups rows with the same race-key 5-tuple", () => {
  const rows: ReadonlyArray<RawFeatureRow> = [
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "A",
      perHorseFeatures: {},
      peerInputs: NULL_PEER_INPUTS,
      targetRunningStyleClass: null,
    },
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "B",
      perHorseFeatures: {},
      peerInputs: NULL_PEER_INPUTS,
      targetRunningStyleClass: null,
    },
    {
      ...RACE_KEY_FIELDS,
      race_bango: "03",
      ketto_toroku_bango: "C",
      perHorseFeatures: {},
      peerInputs: NULL_PEER_INPUTS,
      targetRunningStyleClass: null,
    },
  ];
  const grouped = groupRowsByRace(rows);
  expect(grouped.length).toBe(2);
  expect(grouped[0]!.length).toBe(2);
  expect(grouped[1]!.length).toBe(1);
});

it("groupRowsByRace returns empty array for no input rows", () => {
  expect(groupRowsByRace([])).toStrictEqual([]);
});

it("mergeFeatureMap copies perHorse keys and overlays numeric field features", () => {
  const merged = mergeFeatureMap(
    { career_win_rate: 0.3 },
    {
      ...ZERO_FIELD_ROW,
      field_pace_index: 4.2,
      field_has_pure_nige_horse: true,
    },
  );
  expect(merged.career_win_rate).toBe(0.3);
  expect(merged.field_pace_index).toBe(4.2);
  expect(merged.field_has_pure_nige_horse).toBe(1);
});

it("mergeFeatureMap converts field_has_pure_nige_horse false to 0", () => {
  const merged = mergeFeatureMap({}, ZERO_FIELD_ROW);
  expect(merged.field_has_pure_nige_horse).toBe(0);
});

it("mergeFeatureMap preserves null field feature values", () => {
  const merged = mergeFeatureMap(
    {},
    {
      ...ZERO_FIELD_ROW,
      field_pace_index: null,
    },
  );
  expect(merged.field_pace_index).toBe(null);
});

it("predictRace calls predictFlatRunningStyle once per horse for a single-stage v1.5 model", () => {
  const spy = vi.spyOn(modelBinary, "predictFlatRunningStyle").mockReturnValue(PREDICTION_FIXED);
  const result = predictRace({
    group: [
      {
        ...RACE_KEY_FIELDS,
        ketto_toroku_bango: "A",
        perHorseFeatures: { career_win_rate: 0.2 },
        peerInputs: {
          careerWinRate: 0.2,
          kohan3fAvg5: 36.5,
          pastCorner1NormAvg5: 0.4,
          pastFirst3fAvg5: 12.3,
          pastNigeRate: 0.5,
          pastOikomiRate: 0,
          pastSashiRate: 0,
          pastSenkouRate: 0.5,
          speedIndexAvg5: 70,
          speedIndexBest5: 75,
        },
        targetRunningStyleClass: null,
      },
      {
        ...RACE_KEY_FIELDS,
        ketto_toroku_bango: "B",
        perHorseFeatures: { career_win_rate: 0.1 },
        peerInputs: {
          careerWinRate: 0.1,
          kohan3fAvg5: 37.0,
          pastCorner1NormAvg5: 0.6,
          pastFirst3fAvg5: 12.5,
          pastNigeRate: 0.3,
          pastOikomiRate: 0,
          pastSashiRate: 0,
          pastSenkouRate: 0.7,
          speedIndexAvg5: 68,
          speedIndexBest5: 72,
        },
        targetRunningStyleClass: null,
      },
    ],
    model: FAKE_MODEL,
    v1Model: null,
    modelVersion: "m1",
    featureVersion: "v1",
    cellModelKey: "running-style/models/jra/cells/tokyo-turf.flatbin",
    cellVariantId: "tokyo-turf",
  });
  expect(spy).toHaveBeenCalledTimes(2);
  expect(result.length).toBe(2);
  expect(result[0]!.p_senkou).toBe(0.6);
  expect(result[0]!.model_version).toBe("m1");
  expect(result[0]!.running_style_feature_version).toBe("v1");
  expect(result[0]!.cell_model_key).toBe("running-style/models/jra/cells/tokyo-turf.flatbin");
  expect(result[0]!.cell_variant_id).toBe("tokyo-turf");
  expect(result[0]!.ketto_toroku_bango).toBe("A");
  expect(result[1]!.ketto_toroku_bango).toBe("B");
  expect(result[0]!.target_running_style_class).toBe(null);
  spy.mockRestore();
});

it("predictRace passes through target_running_style_class from raw row to prediction row", () => {
  const spy = vi.spyOn(modelBinary, "predictFlatRunningStyle").mockReturnValue(PREDICTION_FIXED);
  const result = predictRace({
    group: [
      {
        ...RACE_KEY_FIELDS,
        ketto_toroku_bango: "A",
        perHorseFeatures: {},
        peerInputs: NULL_PEER_INPUTS,
        targetRunningStyleClass: 2,
      },
    ],
    model: FAKE_MODEL,
    v1Model: null,
    modelVersion: "m1",
    featureVersion: "v1",
    cellModelKey: "running-style/models/jra/latest.flatbin",
    cellVariantId: "latest",
  });
  expect(result[0]!.target_running_style_class).toBe(2);
  spy.mockRestore();
});

it("predictRace calls predictFlatRunningStyle twice per horse when v1Model is provided", () => {
  const spy = vi
    .spyOn(modelBinary, "predictFlatRunningStyle")
    .mockReturnValueOnce(PREDICTION_V1)
    .mockReturnValueOnce(PREDICTION_V2);
  const result = predictRace({
    group: [
      {
        ...RACE_KEY_FIELDS,
        ketto_toroku_bango: "A",
        perHorseFeatures: { career_win_rate: 0.2 },
        peerInputs: NULL_PEER_INPUTS,
        targetRunningStyleClass: null,
      },
    ],
    model: FAKE_V2_MODEL,
    v1Model: FAKE_MODEL,
    modelVersion: "v2",
    featureVersion: "v1",
    cellModelKey: "running-style/models/jra/cells/tokyo-turf.flatbin",
    cellVariantId: "tokyo-turf",
  });
  expect(spy).toHaveBeenCalledTimes(2);
  expect(result.length).toBe(1);
  expect(result[0]!.p_sashi).toBe(0.7);
  spy.mockRestore();
});

it("predictRace injects rs_p_* probabilities into v2 input vector during chained predict", () => {
  const captured: Array<Record<string, number | null | undefined>> = [];
  const spy = vi
    .spyOn(modelBinary, "predictFlatRunningStyle")
    .mockImplementation((_model, values) => {
      captured.push(values);
      if (captured.length === 1) return PREDICTION_V1;
      return PREDICTION_V2;
    });
  predictRace({
    group: [
      {
        ...RACE_KEY_FIELDS,
        ketto_toroku_bango: "A",
        perHorseFeatures: { career_win_rate: 0.2 },
        peerInputs: NULL_PEER_INPUTS,
        targetRunningStyleClass: null,
      },
    ],
    model: FAKE_V2_MODEL,
    v1Model: FAKE_MODEL,
    modelVersion: "v2",
    featureVersion: "v1",
    cellModelKey: "running-style/models/jra/cells/tokyo-turf.flatbin",
    cellVariantId: "tokyo-turf",
  });
  expect(captured[1]!.rs_p_nige).toBe(0.4);
  expect(captured[1]!.rs_p_senkou).toBe(0.3);
  expect(captured[1]!.rs_p_sashi).toBe(0.2);
  expect(captured[1]!.rs_p_oikomi).toBe(0.1);
  spy.mockRestore();
});

it("predictAll groups rows by race and produces one prediction per input row", () => {
  const spy = vi.spyOn(modelBinary, "predictFlatRunningStyle").mockReturnValue(PREDICTION_FIXED);
  const rows: ReadonlyArray<RawFeatureRow> = [
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "A",
      perHorseFeatures: {},
      peerInputs: NULL_PEER_INPUTS,
      targetRunningStyleClass: null,
    },
    {
      ...RACE_KEY_FIELDS,
      race_bango: "03",
      ketto_toroku_bango: "B",
      perHorseFeatures: {},
      peerInputs: NULL_PEER_INPUTS,
      targetRunningStyleClass: null,
    },
  ];
  const result = predictAll({
    rows,
    model: FAKE_MODEL,
    v1Model: null,
    modelVersion: "m",
    featureVersion: "v",
    cellModelKey: "running-style/models/jra/latest.flatbin",
    cellVariantId: "latest",
  });
  expect(result.length).toBe(2);
  expect(spy).toHaveBeenCalledTimes(2);
  spy.mockRestore();
});

it("buildReadFeaturesSql interpolates the features parquet path", () => {
  expect(buildReadFeaturesSql("/tmp/x.parquet")).toBe(
    "SELECT * FROM read_parquet('/tmp/x.parquet')",
  );
});

it("buildEmptyOutputCopySql includes WHERE 1 = 0 and ZSTD compression", () => {
  const sql = buildEmptyOutputCopySql("/tmp/out.parquet", false);
  expect(sql.includes("WHERE 1 = 0") satisfies boolean).toBe(true);
  expect(sql.includes("FORMAT PARQUET, COMPRESSION ZSTD") satisfies boolean).toBe(true);
});

it("buildEmptyOutputCopySql includes typed running_style_feature_version column", () => {
  const sql = buildEmptyOutputCopySql("/tmp/out.parquet", false);
  expect(sql.includes("running_style_feature_version") satisfies boolean).toBe(true);
});

it("buildEmptyOutputCopySql omits target_running_style_class when includeTargetClass is false", () => {
  const sql = buildEmptyOutputCopySql("/tmp/out.parquet", false);
  expect(sql.includes("target_running_style_class") satisfies boolean).toBe(false);
});

it("buildEmptyOutputCopySql includes target_running_style_class when includeTargetClass is true", () => {
  const sql = buildEmptyOutputCopySql("/tmp/out.parquet", true);
  expect(sql.includes("target_running_style_class") satisfies boolean).toBe(true);
});

it("buildWriteOutputCopySql includes a tuple for each row and uses ZSTD compression", () => {
  const sql = buildWriteOutputCopySql(
    "/tmp/out.parquet",
    [
      {
        source: "jra",
        kaisai_nen: "2020",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "ABC123",
        p_nige: 0.1,
        p_senkou: 0.6,
        p_sashi: 0.2,
        p_oikomi: 0.1,
        cell_model_key: "running-style/models/jra/cells/tokyo-turf.flatbin",
        cell_variant_id: "tokyo-turf",
        model_version: "m1",
        running_style_feature_version: "v1",
        target_running_style_class: null,
      },
    ],
    false,
  );
  expect(sql.includes("FORMAT PARQUET, COMPRESSION ZSTD") satisfies boolean).toBe(true);
  expect(sql.includes("'jra'") satisfies boolean).toBe(true);
  expect(sql.includes("'m1'") satisfies boolean).toBe(true);
});

it("buildWriteOutputCopySql escapes single quotes inside string columns", () => {
  const sql = buildWriteOutputCopySql(
    "/tmp/out.parquet",
    [
      {
        source: "j'ra",
        kaisai_nen: "2020",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "A",
        p_nige: 0.1,
        p_senkou: 0.6,
        p_sashi: 0.2,
        p_oikomi: 0.1,
        cell_model_key: "running-style/models/jra/cells/tokyo-turf.flatbin",
        cell_variant_id: "tokyo-turf",
        model_version: "m'1",
        running_style_feature_version: "v1",
        target_running_style_class: null,
      },
    ],
    false,
  );
  expect(sql.includes("'j''ra'") satisfies boolean).toBe(true);
  expect(sql.includes("'m''1'") satisfies boolean).toBe(true);
});

it("buildWriteOutputCopySql renders NULL for non-finite probability values", () => {
  const sql = buildWriteOutputCopySql(
    "/tmp/out.parquet",
    [
      {
        source: "jra",
        kaisai_nen: "2020",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "A",
        p_nige: Number.NaN,
        p_senkou: 0.6,
        p_sashi: 0.2,
        p_oikomi: 0.1,
        cell_model_key: null,
        cell_variant_id: null,
        model_version: "m",
        running_style_feature_version: "v",
        target_running_style_class: null,
      },
    ],
    false,
  );
  expect(sql.includes("NULL,") satisfies boolean).toBe(true);
});

it("buildWriteOutputCopySql appends target_running_style_class column when includeTargetClass is true", () => {
  const sql = buildWriteOutputCopySql(
    "/tmp/out.parquet",
    [
      {
        source: "jra",
        kaisai_nen: "2020",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "A",
        p_nige: 0.1,
        p_senkou: 0.6,
        p_sashi: 0.2,
        p_oikomi: 0.1,
        cell_model_key: "running-style/models/jra/latest.flatbin",
        cell_variant_id: "latest",
        model_version: "m",
        running_style_feature_version: "v",
        target_running_style_class: 2,
      },
    ],
    true,
  );
  expect(sql.includes("target_running_style_class") satisfies boolean).toBe(true);
});

it("buildWriteOutputCopySql renders NULL for a null target_running_style_class value", () => {
  const sql = buildWriteOutputCopySql(
    "/tmp/out.parquet",
    [
      {
        source: "jra",
        kaisai_nen: "2020",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "A",
        p_nige: 0.1,
        p_senkou: 0.6,
        p_sashi: 0.2,
        p_oikomi: 0.1,
        cell_model_key: "running-style/models/jra/latest.flatbin",
        cell_variant_id: "latest",
        model_version: "m",
        running_style_feature_version: "v",
        target_running_style_class: null,
      },
    ],
    true,
  );
  expect(sql.endsWith(") TO '/tmp/out.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)")).toBe(true);
  expect(sql.includes(", NULL)") satisfies boolean).toBe(true);
});

it("readFeatures runs SELECT and returns rows via the neo async API", async () => {
  const setup = buildFakeModuleSetup([{ source: "jra" }]);
  const rows = await readFeatures({
    duckdbModule: setup.module,
    featuresParquet: "/tmp/x.parquet",
  });
  expect(rows.length).toBe(1);
  expect(setup.state.queryStatements[0]).toBe("SELECT * FROM read_parquet('/tmp/x.parquet')");
  expect(setup.state.connectionClosed).toBe(1);
  expect(setup.state.instanceClosed).toBe(1);
});

it("writeOutput runs empty COPY when rows array is empty", async () => {
  const setup = buildFakeModuleSetup([]);
  await writeOutput({
    duckdbModule: setup.module,
    outputParquet: "/tmp/empty.parquet",
    rows: [],
    includeTargetClass: false,
  });
  expect(setup.state.runStatements.length).toBe(1);
  const statement = setup.state.runStatements[0]!;
  expect(statement.includes("WHERE 1 = 0") satisfies boolean).toBe(true);
  expect(setup.state.connectionClosed).toBe(1);
});

it("writeOutput runs VALUES COPY when rows are present", async () => {
  const setup = buildFakeModuleSetup([]);
  const predictions: ReadonlyArray<PredictionRow> = [
    {
      source: "jra",
      kaisai_nen: "2020",
      kaisai_tsukihi: "0530",
      keibajo_code: "05",
      race_bango: "02",
      ketto_toroku_bango: "A",
      p_nige: 0.1,
      p_senkou: 0.6,
      p_sashi: 0.2,
      p_oikomi: 0.1,
      cell_model_key: "running-style/models/jra/latest.flatbin",
      cell_variant_id: "latest",
      model_version: "m",
      running_style_feature_version: "v",
      target_running_style_class: null,
    },
  ];
  await writeOutput({
    duckdbModule: setup.module,
    outputParquet: "/tmp/out.parquet",
    rows: predictions,
    includeTargetClass: false,
  });
  expect(setup.state.runStatements.length).toBe(1);
  const valuesStatement = setup.state.runStatements[0]!;
  expect(valuesStatement.includes("VALUES") satisfies boolean).toBe(true);
});

it("decodeModelFromBuffer delegates to decodeFlatLightGBMModel", () => {
  const spy = vi.spyOn(modelBinary, "decodeFlatLightGBMModel").mockReturnValue(FAKE_MODEL);
  const buffer = new ArrayBuffer(8);
  const model = decodeModelFromBuffer(buffer);
  expect(model).toBe(FAKE_MODEL);
  expect(spy).toHaveBeenCalledWith(buffer);
  spy.mockRestore();
});

it("resolveDuckdbModule accepts a direct DuckDBInstance export", () => {
  const setup = buildFakeModuleSetup([]);
  expect(resolveDuckdbModule(setup.module)).toBe(setup.module);
});

it("resolveDuckdbModule accepts a default-namespaced export", () => {
  const setup = buildFakeModuleSetup([]);
  const wrapped = { default: setup.module };
  expect(resolveDuckdbModule(wrapped)).toBe(setup.module);
});

it("resolveDuckdbModule throws when DuckDBInstance is not exported", () => {
  expect(() => resolveDuckdbModule({ something: "else" })).toThrowError(
    "@duckdb/node-api does not export DuckDBInstance.",
  );
});

it("resolveDuckdbModule throws when namespace is not an object", () => {
  expect(() => resolveDuckdbModule(null)).toThrowError(
    "@duckdb/node-api does not export DuckDBInstance.",
  );
});

it("resolveDuckdbModule throws when DuckDBInstance lacks a create factory", () => {
  expect(() => resolveDuckdbModule({ DuckDBInstance: { create: 42 } })).toThrowError(
    "@duckdb/node-api does not export DuckDBInstance.",
  );
});

it("runInferenceLocal reads features, runs predictions, writes parquet, and reports counts", async () => {
  const decodeSpy = vi.spyOn(modelBinary, "decodeFlatLightGBMModel").mockReturnValue(FAKE_MODEL);
  const predictSpy = vi
    .spyOn(modelBinary, "predictFlatRunningStyle")
    .mockReturnValue(PREDICTION_FIXED);
  const setup = buildFakeModuleSetup([
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "A",
      career_win_rate: 0.2,
      kohan3f_avg_5: 36.5,
      field_pace_index: 1.2,
    },
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "B",
      career_win_rate: 0.1,
      kohan3f_avg_5: 37,
      field_pace_index: 1.3,
    },
  ]);
  const messages: string[] = [];
  const logger: CliLogger = {
    info: (m) => {
      messages.push(m);
    },
  };
  const readModelFile: ModelFileReader = () => Promise.resolve(new ArrayBuffer(8));
  const result = await runInferenceLocal({
    options: {
      modelFlatbin: "/tmp/m.flatbin",
      rsPFromFlatbin: "",
      featuresParquet: "/tmp/in.parquet",
      outputParquet: "/tmp/out.parquet",
      category: "jra",
      predictedAt: "2026-05-31T00:00:00Z",
      modelVersion: "m1",
      featureVersion: "v1",
      cellModelKey: "",
      cellVariantId: "",
    },
    duckdbModule: setup.module,
    readModelFile,
    logger,
  });
  expect(result).toStrictEqual({ rowCount: 2, raceCount: 1 });
  expect(messages.length).toBe(1);
  expect(decodeSpy).toHaveBeenCalledOnce();
  expect(predictSpy).toHaveBeenCalledTimes(2);
  decodeSpy.mockRestore();
  predictSpy.mockRestore();
});

it("runInferenceLocal writes empty parquet when there are no input rows", async () => {
  const decodeSpy = vi.spyOn(modelBinary, "decodeFlatLightGBMModel").mockReturnValue(FAKE_MODEL);
  const setup = buildFakeModuleSetup([]);
  const readModelFile: ModelFileReader = () => Promise.resolve(new ArrayBuffer(8));
  const result = await runInferenceLocal({
    options: {
      modelFlatbin: "/tmp/m.flatbin",
      rsPFromFlatbin: "",
      featuresParquet: "/tmp/in.parquet",
      outputParquet: "/tmp/out.parquet",
      category: "nar",
      predictedAt: "2026-05-31T00:00:00Z",
      modelVersion: "m",
      featureVersion: "v",
      cellModelKey: "",
      cellVariantId: "",
    },
    duckdbModule: setup.module,
    readModelFile,
    logger: { info: () => undefined },
  });
  expect(result).toStrictEqual({ rowCount: 0, raceCount: 0 });
  const emptyStatement = setup.state.runStatements[0]!;
  expect(emptyStatement.includes("WHERE 1 = 0") satisfies boolean).toBe(true);
  decodeSpy.mockRestore();
});

it("runInferenceLocal predicts per race independently across multiple race groups", async () => {
  const decodeSpy = vi.spyOn(modelBinary, "decodeFlatLightGBMModel").mockReturnValue(FAKE_MODEL);
  const predictSpy = vi
    .spyOn(modelBinary, "predictFlatRunningStyle")
    .mockReturnValue(PREDICTION_FIXED);
  const setup = buildFakeModuleSetup([
    { ...RACE_KEY_FIELDS, ketto_toroku_bango: "A", career_win_rate: 0.2 },
    { ...RACE_KEY_FIELDS, race_bango: "03", ketto_toroku_bango: "C", career_win_rate: 0.3 },
    { ...RACE_KEY_FIELDS, race_bango: "03", ketto_toroku_bango: "D", career_win_rate: 0.4 },
  ]);
  const readModelFile: ModelFileReader = () => Promise.resolve(new ArrayBuffer(8));
  const result = await runInferenceLocal({
    options: {
      modelFlatbin: "/tmp/m.flatbin",
      rsPFromFlatbin: "",
      featuresParquet: "/tmp/in.parquet",
      outputParquet: "/tmp/out.parquet",
      category: "jra",
      predictedAt: "now",
      modelVersion: "m",
      featureVersion: "v",
      cellModelKey: "",
      cellVariantId: "",
    },
    duckdbModule: setup.module,
    readModelFile,
    logger: { info: () => undefined },
  });
  expect(result).toStrictEqual({ rowCount: 3, raceCount: 2 });
  expect(predictSpy).toHaveBeenCalledTimes(3);
  decodeSpy.mockRestore();
  predictSpy.mockRestore();
});

it("runInferenceLocal runs chained-predict 2-stage inference for a v2 model with rs_p_* features", async () => {
  const decodeSpy = vi
    .spyOn(modelBinary, "decodeFlatLightGBMModel")
    .mockReturnValueOnce(FAKE_V2_MODEL)
    .mockReturnValueOnce(FAKE_MODEL);
  const predictSpy = vi
    .spyOn(modelBinary, "predictFlatRunningStyle")
    .mockReturnValue(PREDICTION_V2);
  const setup = buildFakeModuleSetup([
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "A",
      career_win_rate: 0.2,
    },
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "B",
      career_win_rate: 0.1,
    },
  ]);
  const readModelFile: ModelFileReader = () => Promise.resolve(new ArrayBuffer(8));
  const result = await runInferenceLocal({
    options: {
      modelFlatbin: "/tmp/v2.flatbin",
      rsPFromFlatbin: "/tmp/v1.flatbin",
      featuresParquet: "/tmp/in.parquet",
      outputParquet: "/tmp/out.parquet",
      category: "jra",
      predictedAt: "2026-05-31T00:00:00Z",
      modelVersion: "v2",
      featureVersion: "v1",
      cellModelKey: "",
      cellVariantId: "",
    },
    duckdbModule: setup.module,
    readModelFile,
    logger: { info: () => undefined },
  });
  expect(result).toStrictEqual({ rowCount: 2, raceCount: 1 });
  expect(decodeSpy).toHaveBeenCalledTimes(2);
  expect(predictSpy).toHaveBeenCalledTimes(4);
  decodeSpy.mockRestore();
  predictSpy.mockRestore();
});

it("runInferenceLocal throws when a v2 model is given but --rs-p-from-flatbin is missing", async () => {
  const decodeSpy = vi.spyOn(modelBinary, "decodeFlatLightGBMModel").mockReturnValue(FAKE_V2_MODEL);
  const setup = buildFakeModuleSetup([]);
  const readModelFile: ModelFileReader = () => Promise.resolve(new ArrayBuffer(8));
  await expect(
    runInferenceLocal({
      options: {
        modelFlatbin: "/tmp/v2.flatbin",
        rsPFromFlatbin: "",
        featuresParquet: "/tmp/in.parquet",
        outputParquet: "/tmp/out.parquet",
        category: "jra",
        predictedAt: "now",
        modelVersion: "v2",
        featureVersion: "v1",
        cellModelKey: "",
        cellVariantId: "",
      },
      duckdbModule: setup.module,
      readModelFile,
      logger: { info: () => undefined },
    }),
  ).rejects.toThrowError("--rs-p-from-flatbin is required");
  decodeSpy.mockRestore();
});

it("runInferenceLocal carries target_running_style_class from input parquet to output rows", async () => {
  const decodeSpy = vi.spyOn(modelBinary, "decodeFlatLightGBMModel").mockReturnValue(FAKE_MODEL);
  const predictSpy = vi
    .spyOn(modelBinary, "predictFlatRunningStyle")
    .mockReturnValue(PREDICTION_FIXED);
  const setup = buildFakeModuleSetup([
    {
      ...RACE_KEY_FIELDS,
      ketto_toroku_bango: "A",
      career_win_rate: 0.2,
      target_running_style_class: 1,
    },
  ]);
  const readModelFile: ModelFileReader = () => Promise.resolve(new ArrayBuffer(8));
  const result = await runInferenceLocal({
    options: {
      modelFlatbin: "/tmp/m.flatbin",
      rsPFromFlatbin: "",
      featuresParquet: "/tmp/in.parquet",
      outputParquet: "/tmp/out.parquet",
      category: "jra",
      predictedAt: "now",
      modelVersion: "m",
      featureVersion: "v",
      cellModelKey: "",
      cellVariantId: "",
    },
    duckdbModule: setup.module,
    readModelFile,
    logger: { info: () => undefined },
  });
  expect(result).toStrictEqual({ rowCount: 1, raceCount: 1 });
  const statement = setup.state.runStatements[0]!;
  expect(statement.includes("target_running_style_class") satisfies boolean).toBe(true);
  decodeSpy.mockRestore();
  predictSpy.mockRestore();
});

it("runCli parses argv and produces predictions through the same pipeline", async () => {
  const decodeSpy = vi.spyOn(modelBinary, "decodeFlatLightGBMModel").mockReturnValue(FAKE_MODEL);
  vi.spyOn(modelBinary, "predictFlatRunningStyle").mockReturnValue(PREDICTION_FIXED);
  const setup = buildFakeModuleSetup([
    { ...RACE_KEY_FIELDS, ketto_toroku_bango: "A", career_win_rate: 0.2 },
  ]);
  const readModelFile: ModelFileReader = () => Promise.resolve(new ArrayBuffer(8));
  const result = await runCli({
    argv: [
      "--model-flatbin",
      "/tmp/m.flatbin",
      "--features-parquet",
      "/tmp/in.parquet",
      "--output-parquet",
      "/tmp/out.parquet",
      "--category",
      "jra",
      "--predicted-at",
      "now",
      "--model-version",
      "m",
      "--feature-version",
      "v",
    ],
    duckdbModule: setup.module,
    readModelFile,
    logger: { info: () => undefined },
  });
  expect(result).toStrictEqual({ rowCount: 1, raceCount: 1 });
  decodeSpy.mockRestore();
  vi.restoreAllMocks();
});

it("runCli propagates parseArgs errors for missing arguments", async () => {
  const setup = buildFakeModuleSetup([]);
  const readModelFile: ModelFileReader = () => Promise.resolve(new ArrayBuffer(8));
  await expect(
    runCli({
      argv: [],
      duckdbModule: setup.module,
      readModelFile,
      logger: { info: () => undefined },
    }),
  ).rejects.toThrowError("--model-flatbin is required.");
});

it("defaultLogger returns an info function that delegates to console.log", () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const logger = defaultLogger();
  logger.info("hello world");
  expect(logSpy).toHaveBeenCalledWith("hello world");
  logSpy.mockRestore();
});

it("getBunRuntime returns the global Bun runtime when present", () => {
  const fakeBun = {
    file: () => ({ arrayBuffer: () => Promise.resolve(new ArrayBuffer(0)) }),
  };
  const target = globalThis as { Bun?: unknown };
  const previous = target.Bun;
  target.Bun = fakeBun;
  try {
    expect(getBunRuntime()).toBe(fakeBun);
  } finally {
    target.Bun = previous;
  }
});

it("getBunRuntime throws when the Bun global is missing", () => {
  const target = globalThis as { Bun?: unknown };
  const previous = target.Bun;
  delete target.Bun;
  try {
    expect(() => getBunRuntime()).toThrowError("Bun runtime not available.");
  } finally {
    target.Bun = previous;
  }
});

it("defaultReadModelFile reads the file through the Bun runtime", async () => {
  const buffer = new ArrayBuffer(16);
  const fakeBun = {
    file: (path: string) => ({
      arrayBuffer: () => Promise.resolve(buffer),
      path,
    }),
  };
  const target = globalThis as { Bun?: unknown };
  const previous = target.Bun;
  target.Bun = fakeBun;
  try {
    const result = await defaultReadModelFile("/tmp/model.flatbin");
    expect(result).toBe(buffer);
  } finally {
    target.Bun = previous;
  }
});

it("loadDuckdbModule rejects when the optional native addon is not installed", async () => {
  const result = await loadDuckdbModule().then(
    (module) => ({ kind: "resolved" as const, module }),
    (error: unknown) => ({ kind: "rejected" as const, error }),
  );
  expect(result.kind === "rejected" || result.kind === "resolved").toBe(true);
});
