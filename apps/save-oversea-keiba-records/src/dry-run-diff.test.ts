// This test runs with Bun and Vitest.
import { expect, test } from "vitest";
import {
  buildCurrentStateSelect,
  classifyColumnChange,
  compareRows,
  evaluateDryRunDiff,
  formatDryRunDiffReport,
  isPlaceholderLike,
  JV_PADDING_CHARS,
  JV_PLACEHOLDER_WIDTHS,
  runDryRunDiffGate,
  type DiffableRow,
  type DryRunDiffResult,
  type DryRunQueryOutcome,
  type DryRunRaceKey,
  type DryRunSqlExecutor,
} from "./dry-run-diff";
import type { SqlStatement } from "./types";

const RACE_KEY: DryRunRaceKey = {
  kaisai_nen: "2026",
  kaisai_tsukihi: "0725",
  keibajo_code: "A6",
  race_bango: "05",
};

const row = (overrides: {
  umaban: string;
  ketto_toroku_bango?: string;
  banushi_code?: string;
  kishu_code?: string;
  chokyoshi_code?: string;
  bamei?: string;
  tansho_odds?: string;
  tansho_ninkijun?: string;
  kakutei_chakujun?: string;
  soha_time?: string;
}): DiffableRow => ({
  umaban: overrides.umaban,
  ketto_toroku_bango:
    overrides.ketto_toroku_bango === undefined ? "0000000000" : overrides.ketto_toroku_bango,
  banushi_code: overrides.banushi_code === undefined ? "000000" : overrides.banushi_code,
  kishu_code: overrides.kishu_code === undefined ? "00000" : overrides.kishu_code,
  chokyoshi_code: overrides.chokyoshi_code === undefined ? "00000" : overrides.chokyoshi_code,
  bamei: overrides.bamei === undefined ? "HORSE" : overrides.bamei,
  tansho_odds: overrides.tansho_odds === undefined ? "0000" : overrides.tansho_odds,
  tansho_ninkijun: overrides.tansho_ninkijun === undefined ? "00" : overrides.tansho_ninkijun,
  kakutei_chakujun: overrides.kakutei_chakujun === undefined ? "00" : overrides.kakutei_chakujun,
  soha_time: overrides.soha_time === undefined ? "0000" : overrides.soha_time,
});

test("isPlaceholderLike treats empty string as placeholder", () => {
  expect(isPlaceholderLike("")).toBe(true);
});

test("isPlaceholderLike treats ASCII-space-only as placeholder", () => {
  expect(isPlaceholderLike("   ")).toBe(true);
});

test("isPlaceholderLike treats ideographic-space-only as placeholder", () => {
  expect(isPlaceholderLike("　　")).toBe(true);
});

test("isPlaceholderLike treats mixed ASCII and ideographic padding as placeholder", () => {
  expect(isPlaceholderLike(` ${JV_PADDING_CHARS.ideographicSpace} `)).toBe(true);
});

test("isPlaceholderLike treats single zero as placeholder", () => {
  expect(isPlaceholderLike("0")).toBe(true);
});

test("isPlaceholderLike treats ketto placeholder width of zeros as placeholder", () => {
  expect(isPlaceholderLike("0000000000")).toBe(true);
  expect(JV_PLACEHOLDER_WIDTHS.ketto_toroku_bango).toBe(10);
});

test("isPlaceholderLike treats kishu and chokyoshi five-zero codes as placeholder", () => {
  expect(isPlaceholderLike("00000")).toBe(true);
  expect(JV_PLACEHOLDER_WIDTHS.kishu_code).toBe(5);
  expect(JV_PLACEHOLDER_WIDTHS.chokyoshi_code).toBe(5);
});

test("isPlaceholderLike treats banushi six-zero code as placeholder", () => {
  expect(isPlaceholderLike("000000")).toBe(true);
  expect(JV_PLACEHOLDER_WIDTHS.banushi_code).toBe(6);
});

test("isPlaceholderLike treats short JV zero codes as placeholder", () => {
  expect(isPlaceholderLike("0000")).toBe(true);
  expect(isPlaceholderLike("00")).toBe(true);
  expect(JV_PLACEHOLDER_WIDTHS.tansho_odds).toBe(4);
  expect(JV_PLACEHOLDER_WIDTHS.tansho_ninkijun).toBe(2);
  expect(JV_PLACEHOLDER_WIDTHS.hasso_jikoku).toBe(4);
  expect(JV_PLACEHOLDER_WIDTHS.kakutei_chakujun).toBe(2);
  expect(JV_PLACEHOLDER_WIDTHS.soha_time).toBe(4);
});

test("isPlaceholderLike treats full-width-padded zero code as placeholder", () => {
  const padded: string = `${JV_PADDING_CHARS.ideographicSpace}${JV_PADDING_CHARS.ideographicSpace}000000${JV_PADDING_CHARS.ideographicSpace}`;
  expect(isPlaceholderLike(padded)).toBe(true);
});

test("isPlaceholderLike treats ASCII-space-padded zero code as placeholder", () => {
  expect(isPlaceholderLike("  0000  ")).toBe(true);
});

test("isPlaceholderLike rejects a resolved banushi code", () => {
  expect(isPlaceholderLike("147803")).toBe(false);
});

test("isPlaceholderLike rejects a resolved ketto registration number", () => {
  expect(isPlaceholderLike("2020190005")).toBe(false);
});

test("isPlaceholderLike rejects a value that mixes zeros with a non-zero digit", () => {
  expect(isPlaceholderLike("0001")).toBe(false);
});

test("isPlaceholderLike rejects a non-digit real name", () => {
  expect(isPlaceholderLike("ゴリアット")).toBe(false);
});

test("classifyColumnChange returns unchanged when both values are identical", () => {
  expect(
    classifyColumnChange({
      dbValue: "2020190005",
      incomingValue: "2020190005",
    }),
  ).toBe("unchanged");
});

test("classifyColumnChange returns enriched when DB placeholder becomes a real value", () => {
  expect(
    classifyColumnChange({
      dbValue: "000000",
      incomingValue: "147803",
    }),
  ).toBe("enriched");
});

test("classifyColumnChange returns REGRESSION when DB real value becomes a placeholder", () => {
  expect(
    classifyColumnChange({
      dbValue: "147803",
      incomingValue: "000000",
    }),
  ).toBe("REGRESSION");
});

test("classifyColumnChange returns changed when both sides are different real values", () => {
  expect(
    classifyColumnChange({
      dbValue: "17",
      incomingValue: "11",
    }),
  ).toBe("changed");
});

test("classifyColumnChange returns unchanged when both sides are placeholder-like but not identical", () => {
  expect(
    classifyColumnChange({
      dbValue: "00",
      incomingValue: "0000",
    }),
  ).toBe("unchanged");
});

test("classifyColumnChange treats empty DB cell upgraded to real as enriched", () => {
  expect(
    classifyColumnChange({
      dbValue: "",
      incomingValue: "2020190005",
    }),
  ).toBe("enriched");
});

test("classifyColumnChange treats full-width-padded DB placeholder upgraded to real as enriched", () => {
  const paddedPlaceholder: string = `${JV_PADDING_CHARS.ideographicSpace}000000${JV_PADDING_CHARS.ideographicSpace}`;
  expect(
    classifyColumnChange({
      dbValue: paddedPlaceholder,
      incomingValue: "147803",
    }),
  ).toBe("enriched");
});

test("compareRows marks banushi regression and ketto unchanged on the same row", () => {
  const dbRow: DiffableRow = row({
    umaban: "02",
    ketto_toroku_bango: "2020190005",
    banushi_code: "147803",
    bamei: "ゴリアット",
  });
  const incomingRow: DiffableRow = row({
    umaban: "02",
    ketto_toroku_bango: "2020190005",
    banushi_code: "000000",
    bamei: "ゴリアット",
  });

  const diff = compareRows({ dbRow, incomingRow });

  expect(diff.umaban).toBe("02");
  expect(diff.hasRegression).toBe(true);
  expect(
    diff.columns.find(
      (column) => column.column === "banushi_code" && column.classification === "REGRESSION",
    ),
  ).toStrictEqual({
    umaban: "02",
    column: "banushi_code",
    dbValue: "147803",
    incomingValue: "000000",
    classification: "REGRESSION",
  });
  expect(
    diff.columns.find(
      (column) => column.column === "ketto_toroku_bango" && column.classification === "unchanged",
    ),
  ).toStrictEqual({
    umaban: "02",
    column: "ketto_toroku_bango",
    dbValue: "2020190005",
    incomingValue: "2020190005",
    classification: "unchanged",
  });
});

test("compareRows marks enrichment when placeholder becomes real", () => {
  const dbRow: DiffableRow = row({
    umaban: "03",
    banushi_code: "000000",
  });
  const incomingRow: DiffableRow = row({
    umaban: "03",
    banushi_code: "111111",
  });

  const diff = compareRows({ dbRow, incomingRow });

  expect(diff.hasRegression).toBe(false);
  expect(
    diff.columns.find(
      (column) => column.column === "banushi_code" && column.classification === "enriched",
    ),
  ).toStrictEqual({
    umaban: "03",
    column: "banushi_code",
    dbValue: "000000",
    incomingValue: "111111",
    classification: "enriched",
  });
});

test("compareRows marks real-to-real flip as changed", () => {
  const dbRow: DiffableRow = row({
    umaban: "01",
    kishu_code: "12345",
  });
  const incomingRow: DiffableRow = row({
    umaban: "01",
    kishu_code: "54321",
  });

  const diff = compareRows({ dbRow, incomingRow });

  expect(diff.hasRegression).toBe(false);
  expect(
    diff.columns.find(
      (column) => column.column === "kishu_code" && column.classification === "changed",
    ),
  ).toStrictEqual({
    umaban: "01",
    column: "kishu_code",
    dbValue: "12345",
    incomingValue: "54321",
    classification: "changed",
  });
});

test("compareRows treats missing DB column as empty placeholder when incoming has a real value", () => {
  const dbRow: DiffableRow = {
    umaban: "04",
  };
  const incomingRow: DiffableRow = {
    umaban: "04",
    banushi_code: "147803",
  };

  const diff = compareRows({ dbRow, incomingRow });

  expect(diff.columns.find((column) => column.column === "banushi_code")).toStrictEqual({
    umaban: "04",
    column: "banushi_code",
    dbValue: "",
    incomingValue: "147803",
    classification: "enriched",
  });
});

test("evaluateDryRunDiff blocks when any regression exists and surfaces the tuple", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [
      row({
        umaban: "02",
        ketto_toroku_bango: "2020190005",
        banushi_code: "147803",
      }),
    ],
    incomingRows: [
      row({
        umaban: "02",
        ketto_toroku_bango: "2020190005",
        banushi_code: "000000",
      }),
    ],
  });

  expect(result.verdict).toBe("blocked");
  expect(result.hasRegression).toBe(true);
  expect(result.regressions).toStrictEqual([
    {
      umaban: "02",
      column: "banushi_code",
      dbValue: "147803",
      incomingValue: "000000",
      classification: "REGRESSION",
    },
  ]);
});

test("evaluateDryRunDiff is safe when only enrichments and unchanged columns exist", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [
      row({
        umaban: "01",
        banushi_code: "000000",
        ketto_toroku_bango: "0000000000",
      }),
    ],
    incomingRows: [
      row({
        umaban: "01",
        banushi_code: "147803",
        ketto_toroku_bango: "2020190005",
      }),
    ],
  });

  expect(result.verdict).toBe("safe");
  expect(result.hasRegression).toBe(false);
  expect(result.changes).toStrictEqual([]);
  expect(result.regressions).toStrictEqual([]);
  expect(result.enrichments).toStrictEqual([
    {
      umaban: "01",
      column: "banushi_code",
      dbValue: "000000",
      incomingValue: "147803",
      classification: "enriched",
    },
    {
      umaban: "01",
      column: "ketto_toroku_bango",
      dbValue: "0000000000",
      incomingValue: "2020190005",
      classification: "enriched",
    },
  ]);
});

test("evaluateDryRunDiff surfaces real-to-real changes without blocking", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [
      row({
        umaban: "05",
        kishu_code: "11111",
      }),
    ],
    incomingRows: [
      row({
        umaban: "05",
        kishu_code: "22222",
      }),
    ],
  });

  expect(result.verdict).toBe("safe");
  expect(result.hasRegression).toBe(false);
  expect(result.changes).toStrictEqual([
    {
      umaban: "05",
      column: "kishu_code",
      dbValue: "11111",
      incomingValue: "22222",
      classification: "changed",
    },
  ]);
});

test("evaluateDryRunDiff treats incoming-only rows as inserts and not regressions", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [],
    incomingRows: [
      row({
        umaban: "09",
        banushi_code: "000000",
      }),
    ],
  });

  expect(result.verdict).toBe("safe");
  expect(result.hasRegression).toBe(false);
  expect(result.inserts).toStrictEqual(["09"]);
  expect(result.missingFromIncoming).toStrictEqual([]);
  expect(result.rowDiffs).toStrictEqual([]);
  expect(result.regressions).toStrictEqual([]);
});

test("evaluateDryRunDiff records DB-only rows as missingFromIncoming without blocking", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [
      row({
        umaban: "08",
        banushi_code: "147803",
      }),
    ],
    incomingRows: [],
  });

  expect(result.verdict).toBe("safe");
  expect(result.hasRegression).toBe(false);
  expect(result.missingFromIncoming).toStrictEqual(["08"]);
  expect(result.inserts).toStrictEqual([]);
  expect(result.rowDiffs).toStrictEqual([]);
});

test("evaluateDryRunDiff handles an empty DB result set with empty incoming as safe", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [],
    incomingRows: [],
  });

  expect(result.verdict).toBe("safe");
  expect(result.hasRegression).toBe(false);
  expect(result.regressions).toStrictEqual([]);
  expect(result.changes).toStrictEqual([]);
  expect(result.enrichments).toStrictEqual([]);
  expect(result.unchanged).toStrictEqual([]);
  expect(result.inserts).toStrictEqual([]);
  expect(result.missingFromIncoming).toStrictEqual([]);
  expect(result.rowDiffs).toStrictEqual([]);
});

test("evaluateDryRunDiff sorts multi-umaban regressions and changes by umaban then column", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [
      row({
        umaban: "02",
        banushi_code: "147803",
        kishu_code: "11111",
      }),
      row({
        umaban: "01",
        banushi_code: "999999",
        kishu_code: "22222",
      }),
    ],
    incomingRows: [
      row({
        umaban: "02",
        banushi_code: "000000",
        kishu_code: "33333",
      }),
      row({
        umaban: "01",
        banushi_code: "000000",
        kishu_code: "44444",
      }),
    ],
  });

  expect(result.verdict).toBe("blocked");
  expect(result.regressions).toStrictEqual([
    {
      umaban: "01",
      column: "banushi_code",
      dbValue: "999999",
      incomingValue: "000000",
      classification: "REGRESSION",
    },
    {
      umaban: "02",
      column: "banushi_code",
      dbValue: "147803",
      incomingValue: "000000",
      classification: "REGRESSION",
    },
  ]);
  expect(result.changes).toStrictEqual([
    {
      umaban: "01",
      column: "kishu_code",
      dbValue: "22222",
      incomingValue: "44444",
      classification: "changed",
    },
    {
      umaban: "02",
      column: "kishu_code",
      dbValue: "11111",
      incomingValue: "33333",
      classification: "changed",
    },
  ]);
});

test("buildCurrentStateSelect emits parameterized SELECT only", () => {
  const statement: SqlStatement = buildCurrentStateSelect(RACE_KEY);

  expect(statement.text).toBe(
    "SELECT * FROM jvd_se WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 ORDER BY umaban",
  );
  expect(statement.values).toStrictEqual(["2026", "0725", "A6", "05"]);
  expect(statement.text.startsWith("SELECT ")).toBe(true);
  expect(statement.text.startsWith("INSERT")).toBe(false);
  expect(statement.text.startsWith("UPDATE")).toBe(false);
});

test("formatDryRunDiffReport is deterministic and ordered for operator display", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [
      row({
        umaban: "02",
        banushi_code: "147803",
        kishu_code: "11111",
      }),
      row({
        umaban: "08",
        banushi_code: "1",
      }),
    ],
    incomingRows: [
      row({
        umaban: "02",
        banushi_code: "000000",
        kishu_code: "22222",
        ketto_toroku_bango: "2020190005",
      }),
      row({
        umaban: "09",
        banushi_code: "000000",
      }),
    ],
  });

  const report: readonly string[] = formatDryRunDiffReport(result);

  expect(report).toStrictEqual([
    "VERDICT blocked",
    "REGRESSION umaban=02 column=banushi_code db=147803 incoming=000000",
    "CHANGED umaban=02 column=kishu_code db=11111 incoming=22222",
    "ENRICHED umaban=02 column=ketto_toroku_bango db=0000000000 incoming=2020190005",
    "INSERT umaban=09",
    "DB_ONLY umaban=08",
  ]);
});

test("formatDryRunDiffReport for a fully safe empty compare prints only the verdict", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [],
    incomingRows: [],
  });

  expect(formatDryRunDiffReport(result)).toStrictEqual(["VERDICT safe"]);
});

test("runDryRunDiffGate fetches current state through the injected executor and blocks regressions", async () => {
  const statements: SqlStatement[] = [];
  const executor: DryRunSqlExecutor = {
    execute: (statement: SqlStatement): Promise<DryRunQueryOutcome> => {
      statements.push(statement);
      return Promise.resolve({
        rows: [
          {
            umaban: "02",
            ketto_toroku_bango: "2020190005",
            banushi_code: "147803",
            kishu_code: "00000",
            chokyoshi_code: "00000",
            bamei: "ゴリアット",
            tansho_odds: "0000",
            tansho_ninkijun: "00",
            kakutei_chakujun: "00",
            soha_time: "0000",
          },
        ],
      });
    },
  };

  const result: DryRunDiffResult = await runDryRunDiffGate({
    raceKey: RACE_KEY,
    incomingRows: [
      row({
        umaban: "02",
        ketto_toroku_bango: "2020190005",
        banushi_code: "000000",
        bamei: "ゴリアット",
      }),
    ],
    executor,
  });

  expect(statements).toStrictEqual([
    {
      text: "SELECT * FROM jvd_se WHERE kaisai_nen = $1 AND kaisai_tsukihi = $2 AND keibajo_code = $3 AND race_bango = $4 ORDER BY umaban",
      values: ["2026", "0725", "A6", "05"],
    },
  ]);
  expect(result.verdict).toBe("blocked");
  expect(result.hasRegression).toBe(true);
  expect(result.regressions).toStrictEqual([
    {
      umaban: "02",
      column: "banushi_code",
      dbValue: "147803",
      incomingValue: "000000",
      classification: "REGRESSION",
    },
  ]);
});

test("runDryRunDiffGate treats an empty DB response as all-inserts and safe", async () => {
  const executor: DryRunSqlExecutor = {
    execute: (): Promise<DryRunQueryOutcome> => Promise.resolve({ rows: [] }),
  };

  const result: DryRunDiffResult = await runDryRunDiffGate({
    raceKey: RACE_KEY,
    incomingRows: [
      row({
        umaban: "01",
        banushi_code: "000000",
      }),
    ],
    executor,
  });

  expect(result.verdict).toBe("safe");
  expect(result.hasRegression).toBe(false);
  expect(result.inserts).toStrictEqual(["01"]);
  expect(result.rowDiffs).toStrictEqual([]);
});

test("runDryRunDiffGate ignores DB rows that lack umaban rather than throwing", async () => {
  const rowWithoutUmaban: Readonly<Record<string, string>> = {
    ketto_toroku_bango: "2020190005",
  };
  const rowWithUmaban: Readonly<Record<string, string>> = {
    umaban: "01",
    banushi_code: "000000",
    ketto_toroku_bango: "0000000000",
    kishu_code: "00000",
    chokyoshi_code: "00000",
    bamei: "A",
    tansho_odds: "0000",
    tansho_ninkijun: "00",
    kakutei_chakujun: "00",
    soha_time: "0000",
  };
  const executor: DryRunSqlExecutor = {
    execute: (): Promise<DryRunQueryOutcome> =>
      Promise.resolve({
        rows: [rowWithoutUmaban, rowWithUmaban],
      }),
  };

  const result: DryRunDiffResult = await runDryRunDiffGate({
    raceKey: RACE_KEY,
    incomingRows: [
      row({
        umaban: "01",
        banushi_code: "147803",
      }),
    ],
    executor,
  });

  expect(result.verdict).toBe("safe");
  expect(result.enrichments).toStrictEqual([
    {
      umaban: "01",
      column: "banushi_code",
      dbValue: "000000",
      incomingValue: "147803",
      classification: "enriched",
    },
  ]);
});

test("padding character constants expose the exact space glyphs used by detection", () => {
  expect(JV_PADDING_CHARS.asciiSpace).toBe(" ");
  expect(JV_PADDING_CHARS.ideographicSpace).toBe("　");
  expect(JV_PADDING_CHARS.zeroDigit).toBe("0");
});

test("evaluateDryRunDiff sorts same-umaban multi-column regressions by column name", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [
      row({
        umaban: "02",
        banushi_code: "147803",
        kishu_code: "12345",
        chokyoshi_code: "67890",
      }),
    ],
    incomingRows: [
      row({
        umaban: "02",
        banushi_code: "000000",
        kishu_code: "00000",
        chokyoshi_code: "00000",
      }),
    ],
  });

  expect(result.verdict).toBe("blocked");
  expect(result.regressions).toStrictEqual([
    {
      umaban: "02",
      column: "banushi_code",
      dbValue: "147803",
      incomingValue: "000000",
      classification: "REGRESSION",
    },
    {
      umaban: "02",
      column: "chokyoshi_code",
      dbValue: "67890",
      incomingValue: "00000",
      classification: "REGRESSION",
    },
    {
      umaban: "02",
      column: "kishu_code",
      dbValue: "12345",
      incomingValue: "00000",
      classification: "REGRESSION",
    },
  ]);
});

test("evaluateDryRunDiff sorts inserts and DB-only umabans ascending", () => {
  const result: DryRunDiffResult = evaluateDryRunDiff({
    dbRows: [row({ umaban: "08" }), row({ umaban: "03" })],
    incomingRows: [row({ umaban: "09" }), row({ umaban: "01" })],
  });

  expect(result.verdict).toBe("safe");
  expect(result.inserts).toStrictEqual(["01", "09"]);
  expect(result.missingFromIncoming).toStrictEqual(["03", "08"]);
});
