// Runs with bun through Vitest.
import { DuckDBInstance } from "@duckdb/node-api";
import { expect, it } from "vitest";
import {
  digitsOnlyNumericSql,
  hexTripletSql,
  positiveDecimalSql,
  weightNumericSql,
  marginNumericSql,
  monthWindowConditionSql,
  monthWindowMonths,
  raceTimeTenthsSql,
} from "./time-score-sql";

it("builds the tenths expression without regexp_replace", () => {
  const sql: string = raceTimeTenthsSql("se.soha_time");
  expect(sql).toMatch("btrim(coalesce(se.soha_time, ''))");
  expect(sql).toMatch("length(btrim(coalesce(se.soha_time, ''))) BETWEEN 1 AND 4");
  expect(sql).toMatch("replace(btrim(coalesce(se.soha_time, '')), '0', '') <> ''");
  expect(sql).toMatch(
    "try_cast(substring(lpad(btrim(coalesce(se.soha_time, '')), 4, '0') FROM 2 FOR 2) AS INT) < 60",
  );
  expect(sql).toMatch("* 600");
  expect(sql).toMatch("ELSE NULL");
  expect(sql).not.toMatch("regexp_replace");
  expect(sql).not.toMatch("~");
});

it("builds the digits-only numeric expression", () => {
  expect(digitsOnlyNumericSql("se.kohan_3f")).toMatch(
    "try_cast(nullif(btrim(coalesce(se.kohan_3f, '')), '') AS DOUBLE)",
  );
});

it("decodes hex triplets digit by digit instead of casting a 0x literal", () => {
  const sql: string = hexTripletSql("raw");
  expect(sql).toMatch("length(raw) = 3");
  expect(sql).toMatch("(strpos('0123456789ABCDEF', substr(raw, 1, 1)) - 1) * 256");
  expect(sql).toMatch("(strpos('0123456789ABCDEF', substr(raw, 2, 1)) - 1) * 16");
  expect(sql).toMatch("(strpos('0123456789ABCDEF', substr(raw, 3, 1)) - 1) * 1");
  expect(sql).not.toMatch("0x");
});

it("computes the three-month window in TypeScript", () => {
  expect(monthWindowMonths("20260920")).toStrictEqual(["08", "09", "10"]);
  expect(monthWindowMonths("20260115")).toStrictEqual(["12", "01", "02"]);
  expect(monthWindowMonths("20261231")).toStrictEqual(["11", "12", "01"]);
  expect(() => monthWindowMonths("2026-09-20")).toThrow("Invalid race history date");
  expect(() => monthWindowMonths("2026092")).toThrow("Invalid race history date");
});

it("emits the month condition only when enabled", () => {
  expect(monthWindowConditionSql("ra.kaisai_tsukihi", "20260920", false)).toBeNull();
  expect(monthWindowConditionSql("ra.kaisai_tsukihi", "20260920", true)).toBe(
    "substring(ra.kaisai_tsukihi FROM 1 FOR 2) IN ('08', '09', '10')",
  );
});

it("executes strict field decoders against raw fixture values without provider I/O", async () => {
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  try {
    // DuckDB spells the provider SQL btrim function trim.
    await connection.run("CREATE MACRO btrim(value) AS trim(value)");
    const result = await connection.runAndReadAll(`SELECT
      ${weightNumericSql({ column: "raw", venueColumn: "venue", carried: false })} AS body,
      ${weightNumericSql({ column: "raw", venueColumn: "venue", carried: true })} AS carried,
      ${positiveDecimalSql("raw")} AS last3f,
      ${marginNumericSql("raw")} AS margin,
      ${raceTimeTenthsSql("raw")} AS race_time
      FROM (VALUES
        ('83', '3E8'), ('83', '258'), ('83', '24E'), ('83', '26C'),
        ('05', '600'), ('05', '+006'), ('05', '-014'), ('83', '000'),
        ('83', ''), ('83', 'FFF'), ('83', '3E8oops'), ('05', '1e2'),
        ('05', '+ 006'), ('83', '0x258'), ('05', '1 23'), ('05', NULL),
        ('83', ' 3e8 '), ('05', '1234'), ('05', '--014'), ('05', '0.6'),
        ('05', 'FFF'), ('83', '0'), ('83', 'fff'), ('05', ' ')
      ) AS fixture(venue, raw)`);
    expect(result.getRows()).toStrictEqual([
      [1000, 10000, null, null, null],
      [600, 6000, 258, 258, 258],
      [590, 5900, null, null, null],
      [620, 6200, null, null, null],
      [600, 600, 600, 600, null],
      [null, null, null, 6, null],
      [null, null, null, 14, null],
      [null, null, null, 0, null],
      [null, null, null, null, null],
      [null, null, null, null, null],
      [null, null, null, null, null],
      [null, null, null, null, null],
      [null, null, null, null, null],
      [null, null, null, null, null],
      [null, null, null, null, null],
      [null, null, null, null, null],
      [1000, 10000, null, null, null],
      [1234, 1234, 1234, 1234, 834],
      [null, null, null, null, null],
      [null, null, null, null, null],
      [null, null, null, null, null],
      [null, null, null, 0, null],
      [null, null, null, null, null],
      [null, null, null, null, null],
    ]);
  } finally {
    connection.closeSync();
    instance.closeSync();
  }
});
