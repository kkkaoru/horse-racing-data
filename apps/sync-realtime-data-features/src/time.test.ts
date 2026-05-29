// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import { getTodayJst, isJstPollingWindow, toJstIsoString } from "./time";

it("converts UTC instant to JST yyyymmdd", () => {
  expect(getTodayJst(new Date("2026-05-29T00:00:00Z"))).toBe("20260529");
});

it("handles JST midnight wrap correctly", () => {
  expect(getTodayJst(new Date("2026-05-28T15:30:00Z"))).toBe("20260529");
});

it("returns true within polling window (JST 12:00)", () => {
  expect(isJstPollingWindow(new Date("2026-05-29T03:00:00Z"))).toBe(true);
});

it("returns false before polling window start (JST 05:00)", () => {
  expect(isJstPollingWindow(new Date("2026-05-28T20:00:00Z"))).toBe(false);
});

it("returns false after polling window end (JST 21:00)", () => {
  expect(isJstPollingWindow(new Date("2026-05-29T12:00:00Z"))).toBe(false);
});

it("returns ISO string for fixed Date input", () => {
  expect(toJstIsoString(new Date("2026-05-29T00:00:00Z"))).toBe("2026-05-29T00:00:00.000Z");
});

it("returns ISO string when called with default arg", () => {
  const value = toJstIsoString();
  expect(value.endsWith("Z")).toBe(true);
});
