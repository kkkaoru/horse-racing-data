// Run with: bun run test scripts/backfill-running-style-d1.test.ts

import { describe, expect, test } from "vitest";

import {
  BATCH_SIZE,
  buildFetchSql,
  buildInsertSqlForRow,
  buildRaceKey,
  DEFAULT_FROM_YEAR,
  DEFAULT_TO_YEAR,
  escapeSqlString,
  formatStringValue,
  parseArgs,
} from "./backfill-running-style-d1";

describe("buildRaceKey", () => {
  test("formats source:YYYYMMDD:keibajo:race_bango", () => {
    expect(buildRaceKey("jra", "2025", "0517", "05", "11")).toBe("jra:20250517:05:11");
  });
});

describe("escapeSqlString", () => {
  test("doubles single quotes for SQL safety", () => {
    expect(escapeSqlString("o'brien")).toBe("o''brien");
  });

  test("leaves plain strings untouched", () => {
    expect(escapeSqlString("hello")).toBe("hello");
  });
});

describe("formatStringValue", () => {
  test("wraps string in single quotes", () => {
    expect(formatStringValue("nige")).toBe("'nige'");
  });

  test("returns NULL token for null", () => {
    expect(formatStringValue(null)).toBe("NULL");
  });
});

describe("buildInsertSqlForRow", () => {
  test("emits INSERT OR REPLACE with all 13 columns", () => {
    const sql = buildInsertSqlForRow({
      source: "jra",
      kaisai_nen: "2025",
      kaisai_tsukihi: "0517",
      keibajo_code: "05",
      race_bango: "11",
      ketto_toroku_bango: "2020100001",
      umaban: 3,
      bamei: "ロードカナロア",
      category: "jra",
      model_version: "jra-rs-v1.0",
      p_nige: "0.05",
      p_senkou: "0.62",
      p_sashi: "0.25",
      p_oikomi: "0.08",
      predicted_label: "senkou",
      predicted_at: "2025-05-17 01:00:00+00",
    });
    expect(sql).toContain("insert or replace into race_running_styles");
    expect(sql).toContain("'jra:20250517:05:11'");
    expect(sql).toContain("3, ");
    expect(sql).toContain("'2020100001'");
    expect(sql).toContain("'ロードカナロア'");
    expect(sql).toContain("0.05");
    expect(sql).toContain("'senkou'");
  });

  test("encodes NULL bamei correctly", () => {
    const sql = buildInsertSqlForRow({
      source: "nar",
      kaisai_nen: "2025",
      kaisai_tsukihi: "0228",
      keibajo_code: "42",
      race_bango: "07",
      ketto_toroku_bango: "h1",
      umaban: 1,
      bamei: null,
      category: "nar",
      model_version: "nar-rs-v1.0",
      p_nige: "0.10",
      p_senkou: "0.20",
      p_sashi: "0.30",
      p_oikomi: "0.40",
      predicted_label: "oikomi",
      predicted_at: "2025-02-28 07:00:00+00",
    });
    expect(sql).toContain(", NULL,");
  });

  test("escapes single quotes within string fields", () => {
    const sql = buildInsertSqlForRow({
      source: "jra",
      kaisai_nen: "2025",
      kaisai_tsukihi: "0517",
      keibajo_code: "05",
      race_bango: "11",
      ketto_toroku_bango: "h1",
      umaban: 1,
      bamei: "O'Brien",
      category: "jra",
      model_version: "jra-rs-v1.0",
      p_nige: "0.25",
      p_senkou: "0.25",
      p_sashi: "0.25",
      p_oikomi: "0.25",
      predicted_label: "sashi",
      predicted_at: "2025-05-17 01:00:00+00",
    });
    expect(sql).toContain("'O''Brien'");
  });
});

describe("parseArgs", () => {
  test("requires --pg-url and --output", () => {
    expect(() => parseArgs([])).toThrowError(/pg-url/);
    expect(() => parseArgs(["--pg-url", "postgres://x"])).toThrowError(/output/);
  });

  test("applies defaults for --from-year and --to-year", () => {
    const args = parseArgs([
      "--pg-url",
      "postgres://x",
      "--output",
      "tmp/out.sql",
    ]);
    expect(args.fromYear).toBe(DEFAULT_FROM_YEAR);
    expect(args.toYear).toBe(DEFAULT_TO_YEAR);
  });

  test("parses --from-year and --to-year as numbers", () => {
    const args = parseArgs([
      "--pg-url",
      "postgres://x",
      "--output",
      "tmp/out.sql",
      "--from-year",
      "2023",
      "--to-year",
      "2026",
    ]);
    expect(args.fromYear).toBe(2023);
    expect(args.toYear).toBe(2026);
  });

  test("rejects unknown arguments", () => {
    expect(() =>
      parseArgs(["--pg-url", "postgres://x", "--output", "tmp/o.sql", "--unknown", "v"]),
    ).toThrowError(/Unknown argument/);
  });
});

describe("BATCH_SIZE constant", () => {
  test("is documented at the module level", () => {
    expect(BATCH_SIZE).toBe(500);
  });
});

describe("buildFetchSql", () => {
  test("includes the active_categories CTE and joins to nvd_se", () => {
    const sql = buildFetchSql();
    expect(sql).toContain("with active_categories as");
    expect(sql).toContain("running_style_active_models");
    expect(sql).toContain("nvd_se");
    expect(sql).toContain("p.kaisai_nen between $1 and $2");
  });
});

describe("parseArgs error branches", () => {
  test("throws when --pg-url has no value", () => {
    expect(() => parseArgs(["--pg-url"])).toThrowError(/--pg-url requires a value/);
  });

  test("throws when --output has no value", () => {
    expect(() => parseArgs(["--pg-url", "postgres://x", "--output"])).toThrowError(
      /--output requires a value/,
    );
  });

  test("throws when --from-year has no value", () => {
    expect(() =>
      parseArgs(["--pg-url", "postgres://x", "--output", "out.sql", "--from-year"]),
    ).toThrowError(/--from-year requires a value/);
  });
});
