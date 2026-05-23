import { describe, expect, it } from "vitest";

import { getNextOddsFetchAt, getOddsFetchIntervalMinutes } from "./odds-schedule";

describe("odds schedule", () => {
  it("uses hourly slots until one hour before the race", () => {
    expect(getOddsFetchIntervalMinutes(120)).toBe(60);
    expect(
      getNextOddsFetchAt(
        "2026-05-14T16:00:00+09:00",
        new Date("2026-05-14T13:40:00+09:00").getTime(),
      ),
    ).toBe(new Date("2026-05-14T14:00:00+09:00").toISOString());
  });

  it("schedules JRA odds from the previous day at 19:00", () => {
    expect(
      getNextOddsFetchAt(
        "2026-05-16T09:45:00+09:00",
        new Date("2026-05-15T18:30:00+09:00").getTime(),
        "jra",
      ),
    ).toBe(new Date("2026-05-15T19:00:00+09:00").toISOString());
  });

  it("uses hourly JRA advance odds slots until one hour before the race", () => {
    expect(
      getNextOddsFetchAt(
        "2026-05-16T09:45:00+09:00",
        new Date("2026-05-16T01:21:00+09:00").getTime(),
        "jra",
      ),
    ).toBe(new Date("2026-05-16T02:00:00+09:00").toISOString());
  });

  it("transitions JRA advance odds to the one-hour-before race schedule", () => {
    expect(
      getNextOddsFetchAt(
        "2026-05-16T09:45:00+09:00",
        new Date("2026-05-16T08:40:00+09:00").getTime(),
        "jra",
      ),
    ).toBe(new Date("2026-05-16T08:45:00+09:00").toISOString());
  });

  it("uses ten minute slots from ten minutes to less than one hour before the race", () => {
    expect(getOddsFetchIntervalMinutes(50)).toBe(10);
    expect(
      getNextOddsFetchAt(
        "2026-05-14T16:25:00+09:00",
        new Date("2026-05-14T15:37:00+09:00").getTime(),
      ),
    ).toBe(new Date("2026-05-14T15:45:00+09:00").toISOString());
  });

  it("uses one minute slots from one minute to less than ten minutes before the race", () => {
    expect(getOddsFetchIntervalMinutes(5)).toBe(1);
    expect(
      getNextOddsFetchAt(
        "2026-05-14T16:00:00+09:00",
        new Date("2026-05-14T15:55:30+09:00").getTime(),
      ),
    ).toBe(new Date("2026-05-14T15:56:00+09:00").toISOString());
  });

  it("does not schedule odds after the final minute window has passed", () => {
    expect(getOddsFetchIntervalMinutes(0.5)).toBeNull();
    expect(
      getNextOddsFetchAt(
        "2026-05-14T16:00:00+09:00",
        new Date("2026-05-14T15:59:30+09:00").getTime(),
      ),
    ).toBeNull();
  });

  it("rejects invalid race start timestamps", () => {
    expect(getNextOddsFetchAt("invalid")).toBeNull();
  });

  it("falls back to general slot when JRA race is within one hour", () => {
    expect(
      getNextOddsFetchAt(
        "2026-05-16T09:45:00+09:00",
        new Date("2026-05-16T09:10:00+09:00").getTime(),
        "jra",
      ),
    ).toBe(new Date("2026-05-16T09:15:00+09:00").toISOString());
  });

  it("schedules NAR odds from the same-day venue sale start", () => {
    expect(
      getNextOddsFetchAt(
        "2026-05-22T14:30:00+09:00",
        new Date("2026-05-22T07:30:00+09:00").getTime(),
        "nar",
        {
          keibajoCode: "44",
          venueLastRaceStartAt: "2026-05-22T20:50:00+09:00",
        },
      ),
    ).toBe(new Date("2026-05-22T12:00:00+09:00").toISOString());
  });

  it("keeps Nagoya and Kochi NAR night meetings at the default sale start", () => {
    expect(
      getNextOddsFetchAt(
        "2026-05-22T14:30:00+09:00",
        new Date("2026-05-22T07:30:00+09:00").getTime(),
        "nar",
        {
          keibajoCode: "48",
          venueLastRaceStartAt: "2026-05-22T20:50:00+09:00",
        },
      ),
    ).toBe(new Date("2026-05-22T10:00:00+09:00").toISOString());
  });

  it("advances NAR hourly slots after the sale start and before one hour remains", () => {
    expect(
      getNextOddsFetchAt(
        "2026-05-22T14:30:00+09:00",
        new Date("2026-05-22T12:00:00+09:00").getTime(),
        "nar",
        {
          keibajoCode: "44",
          venueLastRaceStartAt: "2026-05-22T20:50:00+09:00",
        },
      ),
    ).toBe(new Date("2026-05-22T13:00:00+09:00").toISOString());
  });
});
