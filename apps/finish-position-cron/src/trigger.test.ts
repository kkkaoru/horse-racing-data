// Run with bun. Tests for the on-demand trigger helpers.

import { expect, test } from "vitest";
import { isAuthorized, isTriggerRequest, parseRunDates } from "./trigger";

test("isTriggerRequest matches POST /run", () => {
  expect(isTriggerRequest("POST", "/run")).toBe(true);
});

test("isTriggerRequest rejects GET /run", () => {
  expect(isTriggerRequest("GET", "/run")).toBe(false);
});

test("isTriggerRequest rejects POST to another path", () => {
  expect(isTriggerRequest("POST", "/")).toBe(false);
});

test("isAuthorized accepts a matching bearer token", () => {
  expect(isAuthorized("Bearer abc123", "abc123")).toBe(true);
});

test("isAuthorized rejects a mismatched token", () => {
  expect(isAuthorized("Bearer wrong", "abc123")).toBe(false);
});

test("isAuthorized rejects a missing header", () => {
  expect(isAuthorized(null, "abc123")).toBe(false);
});

test("isAuthorized rejects when the configured token is empty", () => {
  expect(isAuthorized("Bearer abc123", "")).toBe(false);
});

test("parseRunDates converts a valid YYYYMMDD", () => {
  expect(parseRunDates("20260603")).toStrictEqual({ runDate: "2026-06-03", runYmd: "20260603" });
});

test("parseRunDates handles a year boundary date", () => {
  expect(parseRunDates("20270101")).toStrictEqual({ runDate: "2027-01-01", runYmd: "20270101" });
});

test("parseRunDates throws for a dashed date", () => {
  expect(() => parseRunDates("2026-06-03")).toThrow("RUN_DATE must be 8 digits (YYYYMMDD)");
});

test("parseRunDates throws for a non-numeric date", () => {
  expect(() => parseRunDates("2026june")).toThrow("RUN_DATE must be 8 digits (YYYYMMDD)");
});

test("parseRunDates throws for a short date", () => {
  expect(() => parseRunDates("202606")).toThrow("RUN_DATE must be 8 digits (YYYYMMDD)");
});
