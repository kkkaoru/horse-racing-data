// Run with: bun run --filter pc-keiba-viewer test
import { describe, expect, test, vi } from "vitest";

import {
  applyArg,
  applyPostprocToRow,
  applyPostprocToRows,
  buildEmptyOutputCopySql,
  buildLabelFromClass,
  buildReadInputSql,
  buildUsageText,
  buildWriteOutputCopySql,
  detectProbabilityResolver,
  initialOptions,
  parseArgs,
  pickArgmax,
  pickSecondArgmax,
  readInputRows,
  resolveDuckdbModule,
  runCli,
  runPostproc,
  softmaxNormalize,
  writeOutputRows,
} from "./apply-running-style-postproc";

const RACE_KEY_FIELDS = {
  source: "jra",
  kaisai_nen: "2026",
  kaisai_tsukihi: "0530",
  keibajo_code: "05",
  race_bango: "02",
  ketto_toroku_bango: "ABC123",
};

const PASSTHROUGH_FIELDS = {
  model_version: "jra-running-style-lgbm-prod-v1.5",
  running_style_feature_version: "v1",
  target_running_style_class: 0,
};

const PLAIN_FUNCTION = () => undefined;

interface FakeConnectionState {
  rows: readonly Record<string, unknown>[];
  runStatements: string[];
  queryStatements: string[];
}

interface FakeDuckdbModule {
  DuckDBInstance: { create: (path: string) => Promise<FakeInstance> };
}

class FakeReader {
  constructor(private readonly rows: readonly Record<string, unknown>[]) {}
  getRowObjectsJson() {
    return this.rows;
  }
}

class FakeConnection {
  constructor(private readonly state: FakeConnectionState) {}
  async runAndReadAll(sql: string) {
    this.state.queryStatements.push(sql);
    return new FakeReader(this.state.rows);
  }
  async run(sql: string) {
    this.state.runStatements.push(sql);
    return undefined;
  }
  disconnectSync() {
    return undefined;
  }
}

class FakeInstance {
  async connect(): Promise<FakeConnection> {
    return new FakeConnection(FakeInstance.sharedState);
  }
  closeSync() {
    return undefined;
  }
  static sharedState: FakeConnectionState = {
    rows: [],
    runStatements: [],
    queryStatements: [],
  };
}

const buildFakeModule = (initialRows: readonly Record<string, unknown>[]): FakeDuckdbModule => {
  FakeInstance.sharedState = {
    rows: initialRows,
    runStatements: [],
    queryStatements: [],
  };
  return {
    DuckDBInstance: {
      create: async (_path: string) => new FakeInstance(),
    },
  };
};

describe("apply-running-style-postproc", () => {
  test("initialOptions returns empty strings for all required options", () => {
    expect(initialOptions()).toStrictEqual({
      logitsParquet: "",
      outputParquet: "",
      runningStyleFeatureVersion: "",
    });
  });

  test("buildUsageText mentions the script command path", () => {
    expect(buildUsageText().includes("apply-running-style-postproc.ts") satisfies boolean).toBe(
      true,
    );
  });

  test("buildUsageText mentions the running style feature version flag", () => {
    expect(buildUsageText().includes("--running-style-feature-version") satisfies boolean).toBe(
      true,
    );
  });

  test("applyArg sets --logits-parquet and advances by two", () => {
    const options = initialOptions();
    const result = applyArg(options, "--logits-parquet", "input.parquet");
    expect(result).toStrictEqual({ advanceBy: 2 });
    expect(options.logitsParquet).toBe("input.parquet");
  });

  test("applyArg sets --output-parquet and advances by two", () => {
    const options = initialOptions();
    const result = applyArg(options, "--output-parquet", "output.parquet");
    expect(result).toStrictEqual({ advanceBy: 2 });
    expect(options.outputParquet).toBe("output.parquet");
  });

  test("applyArg sets --running-style-feature-version and advances by two", () => {
    const options = initialOptions();
    const result = applyArg(options, "--running-style-feature-version", "v1");
    expect(result).toStrictEqual({ advanceBy: 2 });
    expect(options.runningStyleFeatureVersion).toBe("v1");
  });

  test("applyArg throws when a value is missing", () => {
    const options = initialOptions();
    expect(() => applyArg(options, "--logits-parquet", undefined)).toThrowError(
      "--logits-parquet requires a value.",
    );
  });

  test("applyArg throws on unknown argument", () => {
    const options = initialOptions();
    expect(() => applyArg(options, "--unknown", "value")).toThrowError(
      "Unknown argument: --unknown",
    );
  });

  test("applyArg prints usage and exits when --help is passed", () => {
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

  test("parseArgs returns populated options when all required flags are present", () => {
    const result = parseArgs([
      "--logits-parquet",
      "input.parquet",
      "--output-parquet",
      "output.parquet",
      "--running-style-feature-version",
      "v1",
    ]);
    expect(result).toStrictEqual({
      logitsParquet: "input.parquet",
      outputParquet: "output.parquet",
      runningStyleFeatureVersion: "v1",
    });
  });

  test("parseArgs throws when --logits-parquet is missing", () => {
    expect(() =>
      parseArgs(["--output-parquet", "out.parquet", "--running-style-feature-version", "v1"]),
    ).toThrowError("--logits-parquet is required.");
  });

  test("parseArgs throws when --output-parquet is missing", () => {
    expect(() =>
      parseArgs(["--logits-parquet", "in.parquet", "--running-style-feature-version", "v1"]),
    ).toThrowError("--output-parquet is required.");
  });

  test("parseArgs throws when --running-style-feature-version is missing", () => {
    expect(() =>
      parseArgs(["--logits-parquet", "in.parquet", "--output-parquet", "out.parquet"]),
    ).toThrowError("--running-style-feature-version is required.");
  });

  test("softmaxNormalize returns equal probabilities for equal logits", () => {
    expect(softmaxNormalize([1, 1, 1, 1])).toStrictEqual([0.25, 0.25, 0.25, 0.25]);
  });

  test("softmaxNormalize returns empty array for empty input", () => {
    expect(softmaxNormalize([])).toStrictEqual([]);
  });

  test("softmaxNormalize assigns largest probability to largest logit", () => {
    const probabilities = softmaxNormalize([0, 0, 5, 0]);
    expect(probabilities[2] !== undefined && probabilities[2] > 0.95).toBe(true);
  });

  test("softmaxNormalize probabilities sum to one", () => {
    const probabilities = softmaxNormalize([0.5, 1.2, -0.3, 2.1]);
    const total = probabilities.reduce((acc, value) => acc + value, 0);
    expect(Math.abs(total - 1) < 1e-9).toBe(true);
  });

  test("pickArgmax returns the index of the maximum probability", () => {
    expect(pickArgmax([0.1, 0.2, 0.6, 0.1])).toBe(2);
  });

  test("pickArgmax returns lowest index when ties occur", () => {
    expect(pickArgmax([0.5, 0.5, 0, 0])).toBe(0);
  });

  test("pickSecondArgmax returns the second highest probability index", () => {
    expect(pickSecondArgmax([0.1, 0.2, 0.6, 0.1])).toBe(1);
  });

  test("pickSecondArgmax returns next index when ties occur", () => {
    expect(pickSecondArgmax([0.5, 0.5, 0, 0])).toBe(1);
  });

  test("buildLabelFromClass returns nige for class 0", () => {
    expect(buildLabelFromClass(0)).toBe("nige");
  });

  test("buildLabelFromClass returns senkou for class 1", () => {
    expect(buildLabelFromClass(1)).toBe("senkou");
  });

  test("buildLabelFromClass returns sashi for class 2", () => {
    expect(buildLabelFromClass(2)).toBe("sashi");
  });

  test("buildLabelFromClass returns oikomi for class 3", () => {
    expect(buildLabelFromClass(3)).toBe("oikomi");
  });

  test("buildLabelFromClass returns empty string for out-of-range class", () => {
    expect(buildLabelFromClass(99)).toBe("");
  });

  test("detectProbabilityResolver prefers logit columns when both present", () => {
    const row = {
      logit_nige: 0,
      logit_senkou: 1,
      logit_sashi: 0,
      logit_oikomi: 0,
      p_nige: 0,
      p_senkou: 0,
      p_sashi: 0,
      p_oikomi: 0,
    };
    const probabilities = detectProbabilityResolver(row).resolve(row);
    expect(probabilities[1] !== undefined && probabilities[1] > 0.4).toBe(true);
  });

  test("detectProbabilityResolver uses probability columns when only those exist", () => {
    const row = { p_nige: 0.4, p_senkou: 0.4, p_sashi: 0.1, p_oikomi: 0.1 };
    const probabilities = detectProbabilityResolver(row).resolve(row);
    expect(probabilities).toStrictEqual([0.4, 0.4, 0.1, 0.1]);
  });

  test("detectProbabilityResolver throws when neither schema is present", () => {
    expect(() => detectProbabilityResolver({ source: "jra" })).toThrowError(
      "Input parquet must provide either logit_nige/logit_senkou/logit_sashi/logit_oikomi or p_nige/p_senkou/p_sashi/p_oikomi columns.",
    );
  });

  test("applyPostprocToRow handles row with logit columns and assigns argmax class", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        ...PASSTHROUGH_FIELDS,
        logit_nige: -5,
        logit_senkou: -5,
        logit_sashi: 5,
        logit_oikomi: -5,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.predicted_class).toBe(2);
    expect(row.predicted_label).toBe("sashi");
  });

  test("applyPostprocToRow assigns second_predicted_class from second highest logit", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        ...PASSTHROUGH_FIELDS,
        logit_nige: 0,
        logit_senkou: 5,
        logit_sashi: 2,
        logit_oikomi: 0,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.second_predicted_class).toBe(2);
  });

  test("applyPostprocToRow passes through running_style_feature_version onto each row", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        model_version: "jra-rs-prod-v2",
        running_style_feature_version: "v2",
        target_running_style_class: 1,
        logit_nige: 1,
        logit_senkou: 0,
        logit_sashi: 0,
        logit_oikomi: 0,
      },
      runningStyleFeatureVersion: "v2",
    });
    expect(row.running_style_feature_version).toBe("v2");
  });

  test("applyPostprocToRow passes through model_version from input onto each row", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        model_version: "nar-rs-prod-v3",
        running_style_feature_version: "v1",
        target_running_style_class: 2,
        logit_nige: 0,
        logit_senkou: 0,
        logit_sashi: 1,
        logit_oikomi: 0,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.model_version).toBe("nar-rs-prod-v3");
  });

  test("applyPostprocToRow passes through target_running_style_class from input", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        target_running_style_class: 3,
        logit_nige: 0,
        logit_senkou: 0,
        logit_sashi: 0,
        logit_oikomi: 1,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.target_running_style_class).toBe(3);
  });

  test("applyPostprocToRow truncates float target_running_style_class to integer", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        target_running_style_class: 2.7,
        p_nige: 0.1,
        p_senkou: 0.1,
        p_sashi: 0.7,
        p_oikomi: 0.1,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.target_running_style_class).toBe(2);
  });

  test("applyPostprocToRow accepts numeric-string target_running_style_class", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        target_running_style_class: "1",
        p_nige: 0.1,
        p_senkou: 0.7,
        p_sashi: 0.1,
        p_oikomi: 0.1,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.target_running_style_class).toBe(1);
  });

  test("applyPostprocToRow preserves NULL target_running_style_class as null", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        target_running_style_class: null,
        p_nige: 0.7,
        p_senkou: 0.1,
        p_sashi: 0.1,
        p_oikomi: 0.1,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.target_running_style_class).toBe(null);
  });

  test("applyPostprocToRow preserves undefined target_running_style_class as null", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        p_nige: 0.7,
        p_senkou: 0.1,
        p_sashi: 0.1,
        p_oikomi: 0.1,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.target_running_style_class).toBe(null);
  });

  test("applyPostprocToRow still derives non-null predicted_class when target is null", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        target_running_style_class: null,
        p_nige: 0.1,
        p_senkou: 0.1,
        p_sashi: 0.7,
        p_oikomi: 0.1,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.predicted_class).toBe(2);
    expect(row.predicted_label).toBe("sashi");
  });

  test("applyPostprocToRow throws when running_style_feature_version mismatches the CLI flag", () => {
    expect(() =>
      applyPostprocToRow({
        raw: {
          ...RACE_KEY_FIELDS,
          model_version: "jra-rs-prod",
          running_style_feature_version: "v1",
          target_running_style_class: 0,
          p_nige: 0.7,
          p_senkou: 0.1,
          p_sashi: 0.1,
          p_oikomi: 0.1,
        },
        runningStyleFeatureVersion: "v2",
      }),
    ).toThrowError(
      "Input row running_style_feature_version (v1) does not match --running-style-feature-version (v2).",
    );
  });

  test("applyPostprocToRow throws when model_version is missing", () => {
    expect(() =>
      applyPostprocToRow({
        raw: {
          ...RACE_KEY_FIELDS,
          running_style_feature_version: "v1",
          target_running_style_class: 0,
          p_nige: 0.7,
          p_senkou: 0.1,
          p_sashi: 0.1,
          p_oikomi: 0.1,
        },
        runningStyleFeatureVersion: "v1",
      }),
    ).toThrowError("Column model_version is not a string.");
  });

  test("applyPostprocToRow throws when target_running_style_class is not numeric", () => {
    expect(() =>
      applyPostprocToRow({
        raw: {
          ...RACE_KEY_FIELDS,
          model_version: "jra-rs-prod",
          running_style_feature_version: "v1",
          target_running_style_class: "not-a-number",
          p_nige: 0.7,
          p_senkou: 0.1,
          p_sashi: 0.1,
          p_oikomi: 0.1,
        },
        runningStyleFeatureVersion: "v1",
      }),
    ).toThrowError("Column target_running_style_class is not numeric.");
  });

  test("applyPostprocToRow with probability columns sum-normalizes the input", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        ...PASSTHROUGH_FIELDS,
        p_nige: 0.25,
        p_senkou: 0.25,
        p_sashi: 0.25,
        p_oikomi: 0.25,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.p_nige).toBe(0.25);
    expect(row.p_senkou).toBe(0.25);
    expect(row.p_sashi).toBe(0.25);
    expect(row.p_oikomi).toBe(0.25);
  });

  test("applyPostprocToRow preserves the race key 6-tuple", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        ...PASSTHROUGH_FIELDS,
        p_nige: 1,
        p_senkou: 0,
        p_sashi: 0,
        p_oikomi: 0,
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.source).toBe("jra");
    expect(row.kaisai_nen).toBe("2026");
    expect(row.kaisai_tsukihi).toBe("0530");
    expect(row.keibajo_code).toBe("05");
    expect(row.race_bango).toBe("02");
    expect(row.ketto_toroku_bango).toBe("ABC123");
  });

  test("applyPostprocToRow throws when source column is not a string", () => {
    expect(() =>
      applyPostprocToRow({
        raw: {
          source: 42,
          kaisai_nen: "2026",
          kaisai_tsukihi: "0530",
          keibajo_code: "05",
          race_bango: "02",
          ketto_toroku_bango: "ABC123",
          ...PASSTHROUGH_FIELDS,
          p_nige: 0.4,
          p_senkou: 0.3,
          p_sashi: 0.2,
          p_oikomi: 0.1,
        },
        runningStyleFeatureVersion: "v1",
      }),
    ).toThrowError("Column source is not a string.");
  });

  test("applyPostprocToRow accepts numeric strings from DuckDB", () => {
    const row = applyPostprocToRow({
      raw: {
        ...RACE_KEY_FIELDS,
        ...PASSTHROUGH_FIELDS,
        p_nige: "0.4",
        p_senkou: "0.3",
        p_sashi: "0.2",
        p_oikomi: "0.1",
      },
      runningStyleFeatureVersion: "v1",
    });
    expect(row.predicted_class).toBe(0);
  });

  test("applyPostprocToRow throws when probability column is not numeric", () => {
    expect(() =>
      applyPostprocToRow({
        raw: {
          ...RACE_KEY_FIELDS,
          ...PASSTHROUGH_FIELDS,
          p_nige: "not-a-number",
          p_senkou: 0.3,
          p_sashi: 0.2,
          p_oikomi: 0.1,
        },
        runningStyleFeatureVersion: "v1",
      }),
    ).toThrowError("Column p_nige is not numeric.");
  });

  test("applyPostprocToRow throws when a probability column is null instead of silently coercing to 0", () => {
    expect(() =>
      applyPostprocToRow({
        raw: {
          ...RACE_KEY_FIELDS,
          ...PASSTHROUGH_FIELDS,
          p_nige: null,
          p_senkou: 0.3,
          p_sashi: 0.2,
          p_oikomi: 0.1,
        },
        runningStyleFeatureVersion: "v1",
      }),
    ).toThrowError("Column p_nige is not numeric.");
  });

  test("applyPostprocToRow throws when a logit column is null instead of silently coercing to 0", () => {
    expect(() =>
      applyPostprocToRow({
        raw: {
          ...RACE_KEY_FIELDS,
          ...PASSTHROUGH_FIELDS,
          logit_nige: null,
          logit_senkou: 0,
          logit_sashi: 5,
          logit_oikomi: 0,
        },
        runningStyleFeatureVersion: "v1",
      }),
    ).toThrowError("Column logit_nige is not numeric.");
  });

  test("applyPostprocToRows returns same number of output rows as input rows", () => {
    const rows = applyPostprocToRows({
      rows: [
        {
          ...RACE_KEY_FIELDS,
          ...PASSTHROUGH_FIELDS,
          p_nige: 1,
          p_senkou: 0,
          p_sashi: 0,
          p_oikomi: 0,
        },
        {
          ...RACE_KEY_FIELDS,
          ...PASSTHROUGH_FIELDS,
          ketto_toroku_bango: "ZZZ999",
          p_nige: 0,
          p_senkou: 0,
          p_sashi: 0,
          p_oikomi: 1,
        },
      ],
      runningStyleFeatureVersion: "v1",
    });
    expect(rows.length).toBe(2);
  });

  test("applyPostprocToRows returns empty array for zero input rows", () => {
    expect(applyPostprocToRows({ rows: [], runningStyleFeatureVersion: "v1" })).toStrictEqual([]);
  });

  test("applyPostprocToRows allows multiple nige predictions in same race (no nige cap)", () => {
    const rows = applyPostprocToRows({
      rows: [
        {
          ...RACE_KEY_FIELDS,
          ...PASSTHROUGH_FIELDS,
          p_nige: 0.6,
          p_senkou: 0.2,
          p_sashi: 0.1,
          p_oikomi: 0.1,
        },
        {
          ...RACE_KEY_FIELDS,
          ...PASSTHROUGH_FIELDS,
          ketto_toroku_bango: "HORSE2",
          p_nige: 0.55,
          p_senkou: 0.25,
          p_sashi: 0.1,
          p_oikomi: 0.1,
        },
        {
          ...RACE_KEY_FIELDS,
          ...PASSTHROUGH_FIELDS,
          ketto_toroku_bango: "HORSE3",
          p_nige: 0.7,
          p_senkou: 0.1,
          p_sashi: 0.1,
          p_oikomi: 0.1,
        },
      ],
      runningStyleFeatureVersion: "v1",
    });
    expect(rows[0]?.predicted_class).toBe(0);
    expect(rows[1]?.predicted_class).toBe(0);
    expect(rows[2]?.predicted_class).toBe(0);
  });

  test("applyPostprocToRows respects nige=senkou tie by preferring lowest class index", () => {
    const rows = applyPostprocToRows({
      rows: [
        {
          ...RACE_KEY_FIELDS,
          ...PASSTHROUGH_FIELDS,
          p_nige: 0.5,
          p_senkou: 0.5,
          p_sashi: 0,
          p_oikomi: 0,
        },
      ],
      runningStyleFeatureVersion: "v1",
    });
    expect(rows[0]?.predicted_class).toBe(0);
    expect(rows[0]?.second_predicted_class).toBe(1);
  });

  test("buildReadInputSql interpolates the logits parquet path", () => {
    expect(buildReadInputSql("/tmp/x.parquet")).toBe(
      "SELECT * FROM read_parquet('/tmp/x.parquet')",
    );
  });

  test("buildEmptyOutputCopySql wraps a typed null select for empty parquet output", () => {
    const sql = buildEmptyOutputCopySql("/tmp/out.parquet");
    expect(sql.includes("'/tmp/out.parquet'") satisfies boolean).toBe(true);
    expect(sql.includes("WHERE 1 = 0") satisfies boolean).toBe(true);
  });

  test("buildEmptyOutputCopySql includes FORMAT PARQUET and COMPRESSION ZSTD", () => {
    const sql = buildEmptyOutputCopySql("/tmp/out.parquet");
    expect(sql.includes("FORMAT PARQUET, COMPRESSION ZSTD") satisfies boolean).toBe(true);
  });

  test("buildEmptyOutputCopySql declares the running_style_feature_version column", () => {
    const sql = buildEmptyOutputCopySql("/tmp/out.parquet");
    expect(sql.includes("AS running_style_feature_version") satisfies boolean).toBe(true);
  });

  test("buildEmptyOutputCopySql declares the target_running_style_class column", () => {
    const sql = buildEmptyOutputCopySql("/tmp/out.parquet");
    expect(sql.includes("AS target_running_style_class") satisfies boolean).toBe(true);
  });

  test("buildEmptyOutputCopySql declares the model_version column", () => {
    const sql = buildEmptyOutputCopySql("/tmp/out.parquet");
    expect(sql.includes("AS model_version") satisfies boolean).toBe(true);
  });

  test("buildWriteOutputCopySql includes a tuple for each row", () => {
    const sql = buildWriteOutputCopySql("/tmp/out.parquet", [
      {
        source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "ABC123",
        p_nige: 0.4,
        p_senkou: 0.3,
        p_sashi: 0.2,
        p_oikomi: 0.1,
        predicted_class: 0,
        second_predicted_class: 1,
        predicted_label: "nige",
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        target_running_style_class: 0,
      },
    ]);
    expect(sql.includes("'jra'") satisfies boolean).toBe(true);
    expect(sql.includes("FORMAT PARQUET, COMPRESSION ZSTD") satisfies boolean).toBe(true);
  });

  test("buildWriteOutputCopySql escapes single quotes within string columns", () => {
    const sql = buildWriteOutputCopySql("/tmp/out.parquet", [
      {
        source: "j'ra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "ABC123",
        p_nige: 0.4,
        p_senkou: 0.3,
        p_sashi: 0.2,
        p_oikomi: 0.1,
        predicted_class: 0,
        second_predicted_class: 1,
        predicted_label: "nige",
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        target_running_style_class: 0,
      },
    ]);
    expect(sql.includes("'j''ra'") satisfies boolean).toBe(true);
  });

  test("buildWriteOutputCopySql lists model_version, running_style_feature_version, and target_running_style_class in column header", () => {
    const sql = buildWriteOutputCopySql("/tmp/out.parquet", [
      {
        source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "ABC123",
        p_nige: 0.4,
        p_senkou: 0.3,
        p_sashi: 0.2,
        p_oikomi: 0.1,
        predicted_class: 0,
        second_predicted_class: 1,
        predicted_label: "nige",
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        target_running_style_class: 2,
      },
    ]);
    expect(
      sql.includes(
        "model_version, running_style_feature_version, target_running_style_class",
      ) satisfies boolean,
    ).toBe(true);
  });

  test("buildWriteOutputCopySql emits CAST(NULL AS INTEGER) for null target_running_style_class", () => {
    const sql = buildWriteOutputCopySql("/tmp/out.parquet", [
      {
        source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "ABC123",
        p_nige: 0.4,
        p_senkou: 0.3,
        p_sashi: 0.2,
        p_oikomi: 0.1,
        predicted_class: 0,
        second_predicted_class: 1,
        predicted_label: "nige",
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        target_running_style_class: null,
      },
    ]);
    expect(sql.includes("CAST(NULL AS INTEGER)") satisfies boolean).toBe(true);
  });

  test("buildWriteOutputCopySql keeps numeric literal for non-null target_running_style_class", () => {
    const sql = buildWriteOutputCopySql("/tmp/out.parquet", [
      {
        source: "jra",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0530",
        keibajo_code: "05",
        race_bango: "02",
        ketto_toroku_bango: "ABC123",
        p_nige: 0.4,
        p_senkou: 0.3,
        p_sashi: 0.2,
        p_oikomi: 0.1,
        predicted_class: 0,
        second_predicted_class: 1,
        predicted_label: "nige",
        model_version: "jra-rs-prod",
        running_style_feature_version: "v1",
        target_running_style_class: 3,
      },
    ]);
    expect(sql.includes("CAST(NULL AS INTEGER)") satisfies boolean).toBe(false);
  });

  test("readInputRows runs SELECT query and returns plain objects", async () => {
    const fakeModule = buildFakeModule([
      {
        ...RACE_KEY_FIELDS,
        ...PASSTHROUGH_FIELDS,
        p_nige: 0.4,
        p_senkou: 0.3,
        p_sashi: 0.2,
        p_oikomi: 0.1,
      },
    ]);
    const rows = await readInputRows({
      duckdbModule: fakeModule,
      logitsParquet: "/tmp/x.parquet",
    });
    expect(rows.length).toBe(1);
    expect(rows[0]?.source).toBe("jra");
  });

  test("readInputRows records the SELECT statement on the connection", async () => {
    const fakeModule = buildFakeModule([
      {
        ...RACE_KEY_FIELDS,
        ...PASSTHROUGH_FIELDS,
        p_nige: 0.4,
        p_senkou: 0.3,
        p_sashi: 0.2,
        p_oikomi: 0.1,
      },
    ]);
    await readInputRows({
      duckdbModule: fakeModule,
      logitsParquet: "/tmp/x.parquet",
    });
    expect(FakeInstance.sharedState.queryStatements.length).toBe(1);
    expect(
      FakeInstance.sharedState.queryStatements[0]?.includes("/tmp/x.parquet") satisfies boolean,
    ).toBe(true);
  });

  test("writeOutputRows uses empty parquet COPY when rows array is empty", async () => {
    const fakeModule = buildFakeModule([]);
    await writeOutputRows({
      duckdbModule: fakeModule,
      outputParquet: "/tmp/empty.parquet",
      rows: [],
    });
    expect(FakeInstance.sharedState.runStatements.length).toBe(1);
    expect(
      FakeInstance.sharedState.runStatements[0]?.includes("WHERE 1 = 0") satisfies boolean,
    ).toBe(true);
  });

  test("writeOutputRows uses VALUES COPY when rows are present", async () => {
    const fakeModule = buildFakeModule([]);
    await writeOutputRows({
      duckdbModule: fakeModule,
      outputParquet: "/tmp/nonempty.parquet",
      rows: [
        {
          source: "jra",
          kaisai_nen: "2026",
          kaisai_tsukihi: "0530",
          keibajo_code: "05",
          race_bango: "02",
          ketto_toroku_bango: "ABC123",
          p_nige: 0.4,
          p_senkou: 0.3,
          p_sashi: 0.2,
          p_oikomi: 0.1,
          predicted_class: 0,
          second_predicted_class: 1,
          predicted_label: "nige",
          model_version: "jra-rs-prod",
          running_style_feature_version: "v1",
          target_running_style_class: 0,
        },
      ],
    });
    expect(FakeInstance.sharedState.runStatements.length).toBe(1);
    expect(FakeInstance.sharedState.runStatements[0]?.includes("VALUES") satisfies boolean).toBe(
      true,
    );
  });

  test("runPostproc reads rows, post-processes them, and writes to output parquet", async () => {
    const fakeModule = buildFakeModule([
      {
        ...RACE_KEY_FIELDS,
        ...PASSTHROUGH_FIELDS,
        logit_nige: 5,
        logit_senkou: 0,
        logit_sashi: 0,
        logit_oikomi: 0,
      },
    ]);
    const messages: string[] = [];
    const result = await runPostproc({
      duckdbModule: fakeModule,
      logger: {
        info: (m) => {
          messages.push(m);
        },
      },
      options: {
        logitsParquet: "/tmp/in.parquet",
        outputParquet: "/tmp/out.parquet",
        runningStyleFeatureVersion: "v1",
      },
    });
    expect(result).toStrictEqual({ rowCount: 1 });
    expect(messages.length).toBe(1);
  });

  test("runPostproc with empty input writes an empty parquet and reports zero rows", async () => {
    const fakeModule = buildFakeModule([]);
    const messages: string[] = [];
    const result = await runPostproc({
      duckdbModule: fakeModule,
      logger: {
        info: (m) => {
          messages.push(m);
        },
      },
      options: {
        logitsParquet: "/tmp/empty-in.parquet",
        outputParquet: "/tmp/empty-out.parquet",
        runningStyleFeatureVersion: "v1",
      },
    });
    expect(result).toStrictEqual({ rowCount: 0 });
    expect(
      FakeInstance.sharedState.runStatements[0]?.includes("WHERE 1 = 0") satisfies boolean,
    ).toBe(true);
  });

  test("runCli parses argv and produces predictions through the same pipeline", async () => {
    const fakeModule = buildFakeModule([
      {
        ...RACE_KEY_FIELDS,
        ...PASSTHROUGH_FIELDS,
        p_nige: 0,
        p_senkou: 0,
        p_sashi: 1,
        p_oikomi: 0,
      },
    ]);
    const result = await runCli({
      argv: [
        "--logits-parquet",
        "/tmp/in.parquet",
        "--output-parquet",
        "/tmp/out.parquet",
        "--running-style-feature-version",
        "v1",
      ],
      duckdbModule: fakeModule,
      logger: { info: () => undefined },
    });
    expect(result).toStrictEqual({ rowCount: 1 });
  });

  test("runCli propagates parseArgs errors for missing arguments", async () => {
    const fakeModule = buildFakeModule([]);
    await expect(
      runCli({
        argv: [],
        duckdbModule: fakeModule,
        logger: { info: () => undefined },
      }),
    ).rejects.toThrowError("--logits-parquet is required.");
  });

  test("resolveDuckdbModule accepts an object DuckDBInstance export", () => {
    const factory = { create: async (_path: string) => new FakeInstance() };
    const moduleNamespace = { DuckDBInstance: factory };
    expect(resolveDuckdbModule(moduleNamespace)).toBe(moduleNamespace);
  });

  test("resolveDuckdbModule accepts a function (class-like) DuckDBInstance export", () => {
    const fakeDuckDBInstanceFn = Object.assign(
      function fakeDuckDBInstanceFn() {
        return undefined;
      },
      { create: async (_path: string) => new FakeInstance() },
    );
    const moduleNamespace = { DuckDBInstance: fakeDuckDBInstanceFn };
    expect(resolveDuckdbModule(moduleNamespace)).toBe(moduleNamespace);
  });

  test("resolveDuckdbModule unwraps a default-namespaced export", () => {
    const factory = { create: async (_path: string) => new FakeInstance() };
    const inner = { DuckDBInstance: factory };
    const moduleNamespace = { default: inner };
    expect(resolveDuckdbModule(moduleNamespace)).toBe(inner);
  });

  test("resolveDuckdbModule unwraps a default-namespaced function (class-like) export", () => {
    const fakeDuckDBInstanceFn = Object.assign(
      function fakeDuckDBInstanceFn() {
        return undefined;
      },
      { create: async (_path: string) => new FakeInstance() },
    );
    const inner = { DuckDBInstance: fakeDuckDBInstanceFn };
    const moduleNamespace = { default: inner };
    expect(resolveDuckdbModule(moduleNamespace)).toBe(inner);
  });

  test("resolveDuckdbModule throws when DuckDBInstance is missing", () => {
    expect(() => resolveDuckdbModule({ something: "else" })).toThrowError(
      "@duckdb/node-api does not export DuckDBInstance.",
    );
  });

  test("resolveDuckdbModule throws when DuckDBInstance lacks a create factory", () => {
    expect(() => resolveDuckdbModule({ DuckDBInstance: { create: 42 } })).toThrowError(
      "@duckdb/node-api does not export DuckDBInstance.",
    );
  });

  test("resolveDuckdbModule throws when DuckDBInstance is a function without create method", () => {
    expect(() => resolveDuckdbModule({ DuckDBInstance: PLAIN_FUNCTION })).toThrowError(
      "@duckdb/node-api does not export DuckDBInstance.",
    );
  });

  test("resolveDuckdbModule throws when DuckDBInstance is null", () => {
    expect(() => resolveDuckdbModule({ DuckDBInstance: null })).toThrowError(
      "@duckdb/node-api does not export DuckDBInstance.",
    );
  });

  test("resolveDuckdbModule throws when the namespace is null", () => {
    expect(() => resolveDuckdbModule(null)).toThrowError(
      "@duckdb/node-api does not export DuckDBInstance.",
    );
  });

  test("resolveDuckdbModule throws when default-namespaced inner lacks DuckDBInstance", () => {
    expect(() => resolveDuckdbModule({ default: { something: "else" } })).toThrowError(
      "@duckdb/node-api does not export DuckDBInstance.",
    );
  });
});
