// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import { buildDailyFeatureSelectSql } from "./build-sql";

it("includes JRA and NAR selects for all scope", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260529" });
  expect(sql.includes("from jvd_se se")).toBe(true);
  expect(sql.includes("from nvd_se se")).toBe(true);
});

it("includes only JRA select for jra scope", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260529", sourceScope: "jra" });
  expect(sql.includes("from jvd_se se")).toBe(true);
  expect(sql.includes("from nvd_se se")).toBe(false);
});

it("includes only NAR select for nar scope and filters keibajo_code", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260529", sourceScope: "nar" });
  expect(sql.includes("from jvd_se se")).toBe(false);
  expect(sql.includes("ra.keibajo_code <> '83'")).toBe(true);
});

it("includes only NAR select for ban-ei scope and filters keibajo_code", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260529", sourceScope: "ban-ei" });
  expect(sql.includes("ra.keibajo_code = '83'")).toBe(true);
});

it("throws when fromDate is invalid", () => {
  expect(() => buildDailyFeatureSelectSql({ fromDate: "2026-05-29" })).toThrowError(
    "fromDate must match YYYYMMDD: 2026-05-29",
  );
});

it("throws when toDate is invalid", () => {
  expect(() => buildDailyFeatureSelectSql({ fromDate: "20260529", toDate: "bad" })).toThrowError(
    "toDate must match YYYYMMDD: bad",
  );
});

it("uses fromDate as toDate default", () => {
  const sql = buildDailyFeatureSelectSql({ fromDate: "20260529" });
  expect(sql.includes("20260529")).toBe(true);
});
