import { describe, expect, it } from "vitest";

import {
  formatRaceStartJst,
  getJstDateParts,
  getOddsFetchIntervalMinutes,
  getTodayJst,
  isJstPollingWindow,
  parseRaceStartJst,
  toJstIsoString,
} from "../src/time";

describe("odds fetch schedule", () => {
  it.each([
    [120, 60],
    [60, 60],
    [59.9, 10],
    [10, 10],
    [9.9, 1],
    [1, 1],
    [0.9, null],
    [0, null],
    [-1, null],
  ])("returns %s-minute pre-race interval", (minutesUntilRace, expected) => {
    expect(getOddsFetchIntervalMinutes(minutesUntilRace)).toBe(expected);
  });
});

describe("JST time helpers", () => {
  it("formats date parts and yyyymmdd in JST", () => {
    expect(getJstDateParts(new Date("2026-05-11T15:05:00.000Z"))).toEqual({
      day: "12",
      hour: "00",
      minute: "05",
      month: "05",
      year: "2026",
      yyyymmdd: "20260512",
    });
    expect(getTodayJst(new Date("2026-05-11T15:05:00.000Z"))).toBe("20260512");
  });

  it("formats Date values as JST ISO strings", () => {
    expect(toJstIsoString(new Date("2026-05-12T03:04:05.000Z"))).toBe("2026-05-12T12:04:05+09:00");
  });

  it("parses and formats race start times", () => {
    expect(parseRaceStartJst("2026", "0512", "1305")?.toISOString()).toBe(
      "2026-05-12T04:05:00.000Z",
    );
    expect(parseRaceStartJst("2026", "0512", null)).toBeNull();
    expect(parseRaceStartJst("2026", "0512", "bad")).toBeNull();
    expect(parseRaceStartJst("2026", "9999", "1305")).toBeNull();
    expect(formatRaceStartJst("2026", "0512", "1305")).toBe("2026-05-12T13:05:00+09:00");
  });

  it("detects JST polling windows", () => {
    expect(isJstPollingWindow(new Date("2026-05-12T00:59:00.000Z"))).toBe(false);
    expect(isJstPollingWindow(new Date("2026-05-12T01:00:00.000Z"))).toBe(true);
    expect(isJstPollingWindow(new Date("2026-05-12T12:59:00.000Z"))).toBe(true);
    expect(isJstPollingWindow(new Date("2026-05-12T13:00:00.000Z"))).toBe(false);
  });
});
