// Run with bun (vitest).
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { formatJstDate, formatTodayJstDate, formatTomorrowJstDate } from "./jst-date";

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

it("format-today-returns-jst-ymd-when-utc-is-15-00", () => {
  vi.setSystemTime(new Date("2026-05-31T15:00:00Z"));
  expect(formatTodayJstDate(new Date())).toBe("2026-06-01");
});

it("format-today-returns-jst-ymd-when-utc-is-noon", () => {
  vi.setSystemTime(new Date("2026-05-31T12:00:00Z"));
  expect(formatTodayJstDate(new Date())).toBe("2026-05-31");
});

it("format-tomorrow-still-adds-one-jst-day", () => {
  vi.setSystemTime(new Date("2026-05-31T12:00:00Z"));
  expect(formatTomorrowJstDate(new Date())).toBe("2026-06-01");
});

it("format-tomorrow-rolls-over-month-boundary", () => {
  vi.setSystemTime(new Date("2026-05-31T15:00:00Z"));
  expect(formatTomorrowJstDate(new Date())).toBe("2026-06-02");
});

it("format-jst-date-handles-jst-midnight", () => {
  expect(formatJstDate(new Date("2026-05-23T15:00:00Z"))).toBe("2026-05-24");
});

it("format-jst-date-handles-noon-utc", () => {
  expect(formatJstDate(new Date("2026-05-23T03:00:00Z"))).toBe("2026-05-23");
});
