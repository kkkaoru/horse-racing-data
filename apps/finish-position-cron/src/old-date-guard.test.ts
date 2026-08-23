// Run with bun. Tests for the old-date predict-message guard.

import { expect, test } from "vitest";
import { isOldDateRunYmd, OLD_DATE_THRESHOLD_DAYS, parseRunYmdToUtcDate } from "./old-date-guard";

test("OLD_DATE_THRESHOLD_DAYS is 0", () => {
  expect(OLD_DATE_THRESHOLD_DAYS).toBe(0);
});

test("parseRunYmdToUtcDate parses a valid 8-digit runYmd to UTC midnight of that day", () => {
  expect(parseRunYmdToUtcDate("20260712")).toStrictEqual(new Date(Date.UTC(2026, 6, 12)));
});

test("parseRunYmdToUtcDate returns null for a 7-digit runYmd", () => {
  expect(parseRunYmdToUtcDate("2026071")).toBe(null);
});

test("parseRunYmdToUtcDate returns null for a 9-digit runYmd", () => {
  expect(parseRunYmdToUtcDate("202607120")).toBe(null);
});

test("parseRunYmdToUtcDate returns null for non-numeric characters", () => {
  expect(parseRunYmdToUtcDate("2026071x")).toBe(null);
});

test("parseRunYmdToUtcDate returns null for an empty string", () => {
  expect(parseRunYmdToUtcDate("")).toBe(null);
});

test("parseRunYmdToUtcDate returns null for a calendar date that does not exist (Feb 30)", () => {
  expect(parseRunYmdToUtcDate("20260230")).toBe(null);
});

test("parseRunYmdToUtcDate returns null for a month value out of range (month 13)", () => {
  expect(parseRunYmdToUtcDate("20261301")).toBe(null);
});

test("parseRunYmdToUtcDate accepts a leap-day date in a leap year", () => {
  expect(parseRunYmdToUtcDate("20240229")).toStrictEqual(new Date(Date.UTC(2024, 1, 29)));
});

test("isOldDateRunYmd skips yesterday so stale retries cannot starve today's card", () => {
  const now = new Date(Date.UTC(2026, 6, 12));
  expect(isOldDateRunYmd("20260711", now)).toBe(true);
});

test("isOldDateRunYmd skips older historical dates", () => {
  const now = new Date(Date.UTC(2026, 6, 12));
  expect(isOldDateRunYmd("20260709", now)).toBe(true);
});

test("isOldDateRunYmd allows a runYmd matching today", () => {
  const now = new Date(Date.UTC(2026, 6, 12));
  expect(isOldDateRunYmd("20260712", now)).toBe(false);
});

test("isOldDateRunYmd never skips a future-dated runYmd", () => {
  const now = new Date(Date.UTC(2026, 6, 12));
  expect(isOldDateRunYmd("20260715", now)).toBe(false);
});

test("isOldDateRunYmd treats a 7-digit runYmd as not old", () => {
  const now = new Date(Date.UTC(2026, 6, 12));
  expect(isOldDateRunYmd("2026071", now)).toBe(false);
});

test("isOldDateRunYmd treats a non-numeric runYmd as not old", () => {
  const now = new Date(Date.UTC(2026, 6, 12));
  expect(isOldDateRunYmd("2026071x", now)).toBe(false);
});

test("isOldDateRunYmd treats an empty runYmd as not old", () => {
  const now = new Date(Date.UTC(2026, 6, 12));
  expect(isOldDateRunYmd("", now)).toBe(false);
});

test("isOldDateRunYmd applies the JST offset instead of naive UTC calendar math", () => {
  // 2026-07-11T16:30:00Z is JST 2026-07-12 01:30 -- the JST calendar day
  // (07-12) differs from the UTC calendar day (07-11). A runYmd of
  // 20260711 is yesterday relative to the JST day (07-12), so it must be
  // skipped -- but under naive (non-JST) UTC date math it matches the UTC
  // day (07-11) and would be allowed. This proves
  // the JST offset is actually applied, not naive UTC date math.
  const now = new Date("2026-07-11T16:30:00.000Z");
  expect(isOldDateRunYmd("20260711", now)).toBe(true);
});
