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

interface FakeConnectionState {
  rows: readonly Record<string, unknown>[];
  runStatements: string[];
  queryStatements: string[];
}

interface FakeDuckdbModule {
  Database: new (path: string) => FakeDatabase;
}

class FakeConnection {
  constructor(private readonly state: FakeConnectionState) {}
  query(sql: string) {
    this.state.queryStatements.push(sql);
    return { toArray: () => this.state.rows };
  }
  run(sql: string) {
    this.state.runStatements.push(sql);
  }
  close() {
    return undefined;
  }
}

class FakeDatabase {
  constructor(public readonly path: string) {}
  connect(): FakeConnection {
    return new FakeConnection(FakeDatabase.sharedState);
  }
  close() {
    return undefined;
  }
  static sharedState: FakeConnectionState = {
    rows: [],
    runStatements: [],
    queryStatements: [],
  };
}

const buildFakeModule = (initialRows: readonly Record<string, unknown>[]): FakeDuckdbModule => {
  FakeDatabase.sharedState = {
    rows: initialRows,
    runStatements: [],
    queryStatements: [],
  };
  return { Database: FakeDatabase };
};

describe("apply-running-style-postproc", () => {
  test("initialOptions returns empty strings for all required options", () => {
    expect(initialOptions()).toStrictEqual({
      logitsParquet: "",
      outputParquet: "",
      featureVersion: "",
    });
  });

  test("buildUsageText mentions the script command path", () => {
    expect(buildUsageText().includes("apply-running-style-postproc.ts") satisfies boolean).toBe(
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

  test("applyArg sets --feature-version and advances by two", () => {
    const options = initialOptions();
    const result = applyArg(options, "--feature-version", "v1");
    expect(result).toStrictEqual({ advanceBy: 2 });
    expect(options.featureVersion).toBe("v1");
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
      "--feature-version",
      "v1",
    ]);
    expect(result).toStrictEqual({
      logitsParquet: "input.parquet",
      outputParquet: "output.parquet",
      featureVersion: "v1",
    });
  });

  test("parseArgs throws when --logits-parquet is missing", () => {
    expect(() =>
      parseArgs(["--output-parquet", "out.parquet", "--feature-version", "v1"]),
    ).toThrowError("--logits-parquet is required.");
  });

  test("parseArgs throws when --output-parquet is missing", () => {
    expect(() =>
      parseArgs(["--logits-parquet", "in.parquet", "--feature-version", "v1"]),
    ).toThrowError("--output-parquet is required.");
  });

  test("parseArgs throws when --feature-version is missing", () => {
    expect(() =>
      parseArgs(["--logits-parquet", "in.parquet", "--output-parquet", "out.parquet"]),
    ).toThrowError("--feature-version is required.");
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
    const row = applyPostprocToRow(
      {
        ...RACE_KEY_FIELDS,
        logit_nige: -5,
        logit_senkou: -5,
        logit_sashi: 5,
        logit_oikomi: -5,
      },
      "v1",
    );
    expect(row.predicted_class).toBe(2);
    expect(row.predicted_label).toBe("sashi");
  });

  test("applyPostprocToRow assigns second_predicted_class from second highest logit", () => {
    const row = applyPostprocToRow(
      {
        ...RACE_KEY_FIELDS,
        logit_nige: 0,
        logit_senkou: 5,
        logit_sashi: 2,
        logit_oikomi: 0,
      },
      "v1",
    );
    expect(row.second_predicted_class).toBe(2);
  });

  test("applyPostprocToRow propagates feature_version onto each row", () => {
    const row = applyPostprocToRow(
      {
        ...RACE_KEY_FIELDS,
        logit_nige: 1,
        logit_senkou: 0,
        logit_sashi: 0,
        logit_oikomi: 0,
      },
      "v2",
    );
    expect(row.feature_version).toBe("v2");
  });

  test("applyPostprocToRow with probability columns sum-normalizes the input", () => {
    const row = applyPostprocToRow(
      {
        ...RACE_KEY_FIELDS,
        p_nige: 0.25,
        p_senkou: 0.25,
        p_sashi: 0.25,
        p_oikomi: 0.25,
      },
      "v1",
    );
    expect(row.p_nige).toBe(0.25);
    expect(row.p_senkou).toBe(0.25);
    expect(row.p_sashi).toBe(0.25);
    expect(row.p_oikomi).toBe(0.25);
  });

  test("applyPostprocToRow preserves the race key 6-tuple", () => {
    const row = applyPostprocToRow(
      {
        ...RACE_KEY_FIELDS,
        p_nige: 1,
        p_senkou: 0,
        p_sashi: 0,
        p_oikomi: 0,
      },
      "v1",
    );
    expect(row.source).toBe("jra");
    expect(row.kaisai_nen).toBe("2026");
    expect(row.kaisai_tsukihi).toBe("0530");
    expect(row.keibajo_code).toBe("05");
    expect(row.race_bango).toBe("02");
    expect(row.ketto_toroku_bango).toBe("ABC123");
  });

  test("applyPostprocToRow throws when source column is not a string", () => {
    expect(() =>
      applyPostprocToRow(
        {
          source: 42,
          kaisai_nen: "2026",
          kaisai_tsukihi: "0530",
          keibajo_code: "05",
          race_bango: "02",
          ketto_toroku_bango: "ABC123",
          p_nige: 0.4,
          p_senkou: 0.3,
          p_sashi: 0.2,
          p_oikomi: 0.1,
        },
        "v1",
      ),
    ).toThrowError("Column source is not a string.");
  });

  test("applyPostprocToRow accepts numeric strings from DuckDB", () => {
    const row = applyPostprocToRow(
      {
        ...RACE_KEY_FIELDS,
        p_nige: "0.4",
        p_senkou: "0.3",
        p_sashi: "0.2",
        p_oikomi: "0.1",
      },
      "v1",
    );
    expect(row.predicted_class).toBe(0);
  });

  test("applyPostprocToRow throws when probability column is not numeric", () => {
    expect(() =>
      applyPostprocToRow(
        {
          ...RACE_KEY_FIELDS,
          p_nige: "not-a-number",
          p_senkou: 0.3,
          p_sashi: 0.2,
          p_oikomi: 0.1,
        },
        "v1",
      ),
    ).toThrowError("Column p_nige is not numeric.");
  });

  test("applyPostprocToRows returns same number of output rows as input rows", () => {
    const rows = applyPostprocToRows(
      [
        { ...RACE_KEY_FIELDS, p_nige: 1, p_senkou: 0, p_sashi: 0, p_oikomi: 0 },
        {
          ...RACE_KEY_FIELDS,
          ketto_toroku_bango: "ZZZ999",
          p_nige: 0,
          p_senkou: 0,
          p_sashi: 0,
          p_oikomi: 1,
        },
      ],
      "v1",
    );
    expect(rows.length).toBe(2);
  });

  test("applyPostprocToRows returns empty array for zero input rows", () => {
    expect(applyPostprocToRows([], "v1")).toStrictEqual([]);
  });

  test("applyPostprocToRows allows multiple nige predictions in same race (no nige cap)", () => {
    const rows = applyPostprocToRows(
      [
        { ...RACE_KEY_FIELDS, p_nige: 0.6, p_senkou: 0.2, p_sashi: 0.1, p_oikomi: 0.1 },
        {
          ...RACE_KEY_FIELDS,
          ketto_toroku_bango: "HORSE2",
          p_nige: 0.55,
          p_senkou: 0.25,
          p_sashi: 0.1,
          p_oikomi: 0.1,
        },
        {
          ...RACE_KEY_FIELDS,
          ketto_toroku_bango: "HORSE3",
          p_nige: 0.7,
          p_senkou: 0.1,
          p_sashi: 0.1,
          p_oikomi: 0.1,
        },
      ],
      "v1",
    );
    expect(rows[0]?.predicted_class).toBe(0);
    expect(rows[1]?.predicted_class).toBe(0);
    expect(rows[2]?.predicted_class).toBe(0);
  });

  test("applyPostprocToRows respects nige=senkou tie by preferring lowest class index", () => {
    const rows = applyPostprocToRows(
      [{ ...RACE_KEY_FIELDS, p_nige: 0.5, p_senkou: 0.5, p_sashi: 0, p_oikomi: 0 }],
      "v1",
    );
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
        feature_version: "v1",
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
        feature_version: "v1",
      },
    ]);
    expect(sql.includes("'j''ra'") satisfies boolean).toBe(true);
  });

  test("readInputRows runs SELECT query and returns plain objects", () => {
    const fakeModule = buildFakeModule([
      { ...RACE_KEY_FIELDS, p_nige: 0.4, p_senkou: 0.3, p_sashi: 0.2, p_oikomi: 0.1 },
    ]);
    const rows = readInputRows({
      duckdbModule: fakeModule,
      logitsParquet: "/tmp/x.parquet",
    });
    expect(rows.length).toBe(1);
    expect(rows[0]?.source).toBe("jra");
  });

  test("writeOutputRows uses empty parquet COPY when rows array is empty", () => {
    const fakeModule = buildFakeModule([]);
    writeOutputRows({
      duckdbModule: fakeModule,
      outputParquet: "/tmp/empty.parquet",
      rows: [],
    });
    expect(FakeDatabase.sharedState.runStatements.length).toBe(1);
    expect(
      FakeDatabase.sharedState.runStatements[0]?.includes("WHERE 1 = 0") satisfies boolean,
    ).toBe(true);
  });

  test("writeOutputRows uses VALUES COPY when rows are present", () => {
    const fakeModule = buildFakeModule([]);
    writeOutputRows({
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
          feature_version: "v1",
        },
      ],
    });
    expect(FakeDatabase.sharedState.runStatements.length).toBe(1);
    expect(FakeDatabase.sharedState.runStatements[0]?.includes("VALUES") satisfies boolean).toBe(
      true,
    );
  });

  test("runPostproc reads rows, post-processes them, and writes to output parquet", () => {
    const fakeModule = buildFakeModule([
      {
        ...RACE_KEY_FIELDS,
        logit_nige: 5,
        logit_senkou: 0,
        logit_sashi: 0,
        logit_oikomi: 0,
      },
    ]);
    const messages: string[] = [];
    const result = runPostproc({
      duckdbModule: fakeModule,
      logger: {
        info: (m) => {
          messages.push(m);
        },
      },
      options: {
        logitsParquet: "/tmp/in.parquet",
        outputParquet: "/tmp/out.parquet",
        featureVersion: "v1",
      },
    });
    expect(result).toStrictEqual({ rowCount: 1 });
    expect(messages.length).toBe(1);
  });

  test("runPostproc with empty input writes an empty parquet and reports zero rows", () => {
    const fakeModule = buildFakeModule([]);
    const messages: string[] = [];
    const result = runPostproc({
      duckdbModule: fakeModule,
      logger: {
        info: (m) => {
          messages.push(m);
        },
      },
      options: {
        logitsParquet: "/tmp/empty-in.parquet",
        outputParquet: "/tmp/empty-out.parquet",
        featureVersion: "v1",
      },
    });
    expect(result).toStrictEqual({ rowCount: 0 });
    expect(
      FakeDatabase.sharedState.runStatements[0]?.includes("WHERE 1 = 0") satisfies boolean,
    ).toBe(true);
  });

  test("runCli parses argv and produces predictions through the same pipeline", () => {
    const fakeModule = buildFakeModule([
      {
        ...RACE_KEY_FIELDS,
        p_nige: 0,
        p_senkou: 0,
        p_sashi: 1,
        p_oikomi: 0,
      },
    ]);
    const result = runCli({
      argv: [
        "--logits-parquet",
        "/tmp/in.parquet",
        "--output-parquet",
        "/tmp/out.parquet",
        "--feature-version",
        "v1",
      ],
      duckdbModule: fakeModule,
      logger: { info: () => undefined },
    });
    expect(result).toStrictEqual({ rowCount: 1 });
  });

  test("runCli propagates parseArgs errors for missing arguments", () => {
    const fakeModule = buildFakeModule([]);
    expect(() =>
      runCli({
        argv: [],
        duckdbModule: fakeModule,
        logger: { info: () => undefined },
      }),
    ).toThrowError("--logits-parquet is required.");
  });
});
