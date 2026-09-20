// Runs with bun through Vitest.
import { expect, it } from "vitest";
import {
  digitsOnlyNumericSql,
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
  expect(digitsOnlyNumericSql("se.kohan_3f")).toBe(
    "try_cast(nullif(btrim(coalesce(se.kohan_3f, '')), '') AS DOUBLE)",
  );
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
