import { describe, expect, it } from "vitest";

import {
  addDaysToYyyymmdd,
  formatRaceStartJst,
  getJraAdvanceOddsFetchSlotAt,
  getJstDateParts,
  getNarOddsFetchSlotAt,
  getNarOddsSaleStartAt,
  getNextOddsFetchSlotAt,
  getOddsFetchIntervalMinutes,
  getOddsFetchSlotAt,
  getTodayJst,
  parseRaceStartJst,
  toJstIsoString,
} from "./time";

describe("odds fetch schedule", () => {
  it.each([
    [120, 60],
    [60.1, 60],
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

  it("aligns odds fetch slots to race start instead of previous fetch time", () => {
    const raceStart = new Date("2026-05-12T11:40:00+09:00");
    expect(getOddsFetchSlotAt(raceStart, new Date("2026-05-12T10:49:39+09:00"))).toBe(
      "2026-05-12T10:40:00+09:00",
    );
    expect(getOddsFetchSlotAt(raceStart, new Date("2026-05-12T10:50:39+09:00"))).toBe(
      "2026-05-12T10:50:00+09:00",
    );
    expect(getOddsFetchSlotAt(raceStart, new Date("2026-05-12T11:39:30+09:00"))).toBeNull();
    expect(getOddsFetchSlotAt(raceStart, new Date("2026-05-12T11:42:00+09:00"))).toBe(
      "2026-05-12T11:42:00+09:00",
    );
  });

  it("aligns JRA advance odds slots hourly from previous-day 19:00 until one hour before post time", () => {
    const raceStart = new Date("2026-05-16T09:45:00+09:00");
    expect(
      getJraAdvanceOddsFetchSlotAt(raceStart, new Date("2026-05-15T18:59:59+09:00")),
    ).toBeNull();
    expect(getJraAdvanceOddsFetchSlotAt(raceStart, new Date("2026-05-15T19:00:00+09:00"))).toBe(
      "2026-05-15T19:00:00+09:00",
    );
    expect(getJraAdvanceOddsFetchSlotAt(raceStart, new Date("2026-05-15T19:59:59+09:00"))).toBe(
      "2026-05-15T19:00:00+09:00",
    );
    expect(getJraAdvanceOddsFetchSlotAt(raceStart, new Date("2026-05-16T08:44:59+09:00"))).toBe(
      "2026-05-16T08:00:00+09:00",
    );
    expect(
      getJraAdvanceOddsFetchSlotAt(raceStart, new Date("2026-05-16T08:45:00+09:00")),
    ).toBeNull();
  });

  it("returns the next JRA odds slot after the current fetch", () => {
    const raceStart = new Date("2026-05-17T13:05:00+09:00");
    expect(getNextOddsFetchSlotAt(raceStart, new Date("2026-05-17T12:18:09+09:00"), "jra")).toBe(
      "2026-05-17T12:25:00+09:00",
    );
    expect(getNextOddsFetchSlotAt(raceStart, new Date("2026-05-17T12:56:30+09:00"), "jra")).toBe(
      "2026-05-17T12:57:00+09:00",
    );
  });

  it("returns the next JRA advance odds slot before the final hour", () => {
    const raceStart = new Date("2026-05-17T16:30:00+09:00");
    expect(getNextOddsFetchSlotAt(raceStart, new Date("2026-05-17T12:19:00+09:00"), "jra")).toBe(
      "2026-05-17T13:00:00+09:00",
    );
    expect(getNextOddsFetchSlotAt(raceStart, new Date("2026-05-16T18:00:00+09:00"), "jra")).toBe(
      "2026-05-16T19:00:00+09:00",
    );
    expect(getNextOddsFetchSlotAt(raceStart, new Date("2026-05-17T15:30:00+09:00"), "jra")).toBe(
      "2026-05-17T15:40:00+09:00",
    );
  });

  it("returns the next regular odds slot for non-JRA races", () => {
    const raceStart = new Date("2026-05-17T13:05:00+09:00");
    expect(getNextOddsFetchSlotAt(raceStart, new Date("2026-05-17T12:18:09+09:00"), "nar")).toBe(
      "2026-05-17T12:25:00+09:00",
    );
    expect(getNextOddsFetchSlotAt(raceStart, new Date("2026-05-17T13:04:30+09:00"), "nar")).toBe(
      "2026-05-17T13:07:00+09:00",
    );
    expect(
      getNextOddsFetchSlotAt(raceStart, new Date("2026-05-17T13:07:00+09:00"), "nar"),
    ).toBeNull();
  });

  it("returns null when raceStartAtJst lacks a yyyy-mm-dd prefix", () => {
    expect(
      getNarOddsSaleStartAt({
        keibajoCode: "44",
        raceStartAtJst: "invalid",
        venueLastRaceStartAtJst: "2026-05-22T20:50:00+09:00",
      }),
    ).toBeNull();
  });

  it("clamps JRA next advance slot to one hour before race when next hourly slot would overshoot", () => {
    const raceStart = new Date("2026-05-17T09:45:00+09:00");
    expect(getNextOddsFetchSlotAt(raceStart, new Date("2026-05-17T08:30:00+09:00"), "jra")).toBe(
      "2026-05-17T08:45:00+09:00",
    );
  });

  it("uses same-day venue sale start for NAR odds", () => {
    const raceStartAtJst = "2026-05-22T14:30:00+09:00";
    expect(
      getNarOddsSaleStartAt({
        keibajoCode: "44",
        raceStartAtJst,
        venueLastRaceStartAtJst: "2026-05-22T20:50:00+09:00",
      })?.toISOString(),
    ).toBe("2026-05-22T03:00:00.000Z");
    expect(
      getNarOddsSaleStartAt({
        keibajoCode: "48",
        raceStartAtJst,
        venueLastRaceStartAtJst: "2026-05-22T20:50:00+09:00",
      })?.toISOString(),
    ).toBe("2026-05-22T01:00:00.000Z");
  });

  it("does not open NAR odds slots before the venue sale start", () => {
    const raceStart = new Date("2026-05-22T14:30:00+09:00");
    const saleStart = new Date("2026-05-22T12:00:00+09:00");
    expect(
      getNarOddsFetchSlotAt(raceStart, new Date("2026-05-22T11:59:59+09:00"), saleStart),
    ).toBeNull();
    expect(getNarOddsFetchSlotAt(raceStart, new Date("2026-05-22T12:05:00+09:00"), saleStart)).toBe(
      "2026-05-22T12:00:00+09:00",
    );
    expect(getNarOddsFetchSlotAt(raceStart, new Date("2026-05-22T13:29:59+09:00"), saleStart)).toBe(
      "2026-05-22T13:00:00+09:00",
    );
    expect(getNarOddsFetchSlotAt(raceStart, new Date("2026-05-22T13:30:00+09:00"), saleStart)).toBe(
      "2026-05-22T13:30:00+09:00",
    );
  });

  it("returns the next NAR slot from the venue sale start", () => {
    const raceStart = new Date("2026-05-22T14:30:00+09:00");
    const saleStart = new Date("2026-05-22T12:00:00+09:00");
    expect(
      getNextOddsFetchSlotAt(raceStart, new Date("2026-05-22T11:30:00+09:00"), "nar", {
        narSaleStartAt: saleStart,
      }),
    ).toBe("2026-05-22T12:00:00+09:00");
    expect(
      getNextOddsFetchSlotAt(raceStart, new Date("2026-05-22T12:00:00+09:00"), "nar", {
        narSaleStartAt: saleStart,
      }),
    ).toBe("2026-05-22T13:00:00+09:00");
    expect(
      getNextOddsFetchSlotAt(raceStart, new Date("2026-05-22T13:00:00+09:00"), "nar", {
        narSaleStartAt: saleStart,
      }),
    ).toBe("2026-05-22T13:30:00+09:00");
  });

  it("falls through to regular offsets when NAR now is within one hour of race start", () => {
    const raceStart = new Date("2026-05-22T14:30:00+09:00");
    const saleStart = new Date("2026-05-22T12:00:00+09:00");
    expect(
      getNextOddsFetchSlotAt(raceStart, new Date("2026-05-22T13:30:00+09:00"), "nar", {
        narSaleStartAt: saleStart,
      }),
    ).toBe("2026-05-22T13:40:00+09:00");
  });

  it("returns null when raceStartAtJst date prefix yields an invalid Date", () => {
    expect(
      getNarOddsSaleStartAt({
        keibajoCode: "44",
        raceStartAtJst: "9999-99-99T14:30:00+09:00",
        venueLastRaceStartAtJst: null,
      }),
    ).toBeNull();
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

  it("adds zero days and returns the same yyyymmdd", () => {
    expect(addDaysToYyyymmdd("20260528", 0)).toBe("20260528");
  });

  it("adds one day across a regular boundary", () => {
    expect(addDaysToYyyymmdd("20260528", 1)).toBe("20260529");
  });

  it("adds two days for the multi-day planner window", () => {
    expect(addDaysToYyyymmdd("20260528", 2)).toBe("20260530");
  });

  it("crosses month boundaries when adding days", () => {
    expect(addDaysToYyyymmdd("20260530", 5)).toBe("20260604");
  });

  it("crosses year boundaries when adding days", () => {
    expect(addDaysToYyyymmdd("20261231", 1)).toBe("20270101");
  });

  it("throws for malformed yyyymmdd input", () => {
    expect(() => addDaysToYyyymmdd("2026-05-28", 1)).toThrowError("invalid yyyymmdd: 2026-05-28");
  });
});
