// Run with: bun run test scripts/backfill-running-style-d1.test.ts

import { describe, expect, test, vi } from "vitest";

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
  writeSqlOutput,
} from "./backfill-running-style-d1";

vi.mock("node:fs/promises", () => ({
  mkdir: vi.fn(async () => undefined),
  writeFile: vi.fn(async () => undefined),
}));

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
  test("emits INSERT OR REPLACE with corner columns", () => {
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
      predicted_corner_front_score: "1.36",
      predicted_corner_rank: "4",
      predicted_label: "senkou",
      predicted_at: "2025-05-17 01:00:00+00",
    });
    expect(sql).toBe(
      "insert or replace into race_running_styles (race_key, horse_number, ketto_toroku_bango, bamei, category, kaisai_nen, model_version, p_nige, p_senkou, p_sashi, p_oikomi, predicted_corner_front_score, predicted_corner_rank, predicted_label, predicted_at) values ('jra:20250517:05:11', 3, '2020100001', 'ロードカナロア', 'jra', '2025', 'jra-rs-v1.0', 0.05, 0.62, 0.25, 0.08, 1.36, 4, 'senkou', '2025-05-17 01:00:00+00');",
    );
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
      predicted_corner_front_score: "2",
      predicted_corner_rank: "7",
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
      predicted_corner_front_score: "1.5",
      predicted_corner_rank: "2",
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
    const args = parseArgs(["--pg-url", "postgres://x", "--output", "tmp/out.sql"]);
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
    expect(sql).toMatch(/coalesce\(\s+p\.predicted_corner_front_score/);
    expect(sql).toMatch(/row_number\(\) over/);
    expect(sql).toMatch(/as predicted_corner_rank/);
  });
});

describe("writeSqlOutput", () => {
  test("creates the directory and writes the joined statements with a trailing newline", async () => {
    const fs = await import("node:fs/promises");
    await writeSqlOutput("/tmp/dir/file.sql", ["a;", "b;"]);
    expect(fs.mkdir).toHaveBeenCalledWith("/tmp/dir", { recursive: true });
    expect(fs.writeFile).toHaveBeenCalledWith("/tmp/dir/file.sql", "a;\nb;\n", "utf8");
  });

  test("handles empty statements array", async () => {
    const fs = await import("node:fs/promises");
    vi.mocked(fs.mkdir).mockClear();
    vi.mocked(fs.writeFile).mockClear();
    await writeSqlOutput("/tmp/empty.sql", []);
    expect(fs.writeFile).toHaveBeenCalledWith("/tmp/empty.sql", "\n", "utf8");
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
