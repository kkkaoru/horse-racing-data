import { describe, expect, it } from "vitest";

import {
  clearRaceTrendTargetQueryParams,
  DEFAULT_RACE_TREND_TARGETS,
  getRaceTrendTargetQueryValue,
  getRaceTrendTargetsFromSearchParams,
  isDefaultRaceTrendTargets,
  isSameRaceTrendTargets,
  parseRaceTrendTargets,
  serializeRaceTrendTargets,
} from "./race-trend-query";

describe("race trend query helpers", () => {
  it("uses default targets when the query string does not specify trend targets", () => {
    expect(getRaceTrendTargetsFromSearchParams(new URLSearchParams())).toEqual(
      DEFAULT_RACE_TREND_TARGETS,
    );
    expect(isDefaultRaceTrendTargets(DEFAULT_RACE_TREND_TARGETS)).toBe(true);
  });

  it("reads primary and alias query param names", () => {
    expect(getRaceTrendTargetQueryValue(new URLSearchParams("raceTrendTargets=frame"))).toBe(
      "frame",
    );
    expect(getRaceTrendTargetQueryValue(new URLSearchParams("trendTargets=jockey"))).toBe("jockey");
    expect(getRaceTrendTargetQueryValue(new URLSearchParams("trend=race"))).toBe("race");
    expect(getRaceTrendTargetQueryValue({ trend: ["style", "ignored"] })).toBe("style");
    expect(getRaceTrendTargetQueryValue({ raceTrendTargets: undefined })).toBeNull();
  });

  it("parses single target trend shortcuts", () => {
    expect(
      getRaceTrendTargetsFromSearchParams(new URLSearchParams("raceTrendTargets=style")),
    ).toEqual({
      frame: false,
      jockey: false,
      raceNumber: false,
      runningStyle: true,
    });
    expect(
      getRaceTrendTargetsFromSearchParams(new URLSearchParams("raceTrendTargets=frame")),
    ).toEqual({
      frame: true,
      jockey: false,
      raceNumber: false,
      runningStyle: false,
    });
    expect(
      getRaceTrendTargetsFromSearchParams(new URLSearchParams("raceTrendTargets=jockey")),
    ).toEqual({
      frame: false,
      jockey: true,
      raceNumber: false,
      runningStyle: false,
    });
    expect(
      getRaceTrendTargetsFromSearchParams(new URLSearchParams("raceTrendTargets=raceNumber")),
    ).toEqual({
      frame: false,
      jockey: false,
      raceNumber: true,
      runningStyle: false,
    });
  });

  it("parses none and invalid tokens", () => {
    expect(parseRaceTrendTargets(null)).toBeNull();
    expect(parseRaceTrendTargets("none")).toEqual({
      frame: false,
      jockey: false,
      raceNumber: false,
      runningStyle: false,
    });
    expect(parseRaceTrendTargets("unknown")).toBeNull();
    expect(parseRaceTrendTargets("frame,style")).toEqual({
      frame: true,
      jockey: false,
      raceNumber: false,
      runningStyle: true,
    });
  });

  it("serializes and compares targets", () => {
    expect(
      serializeRaceTrendTargets({
        frame: false,
        jockey: false,
        raceNumber: false,
        runningStyle: false,
      }),
    ).toBe("none");
    expect(
      serializeRaceTrendTargets({
        frame: true,
        jockey: true,
        raceNumber: false,
        runningStyle: false,
      }),
    ).toBe("frame,jockey");
    expect(
      isSameRaceTrendTargets(DEFAULT_RACE_TREND_TARGETS, { ...DEFAULT_RACE_TREND_TARGETS }),
    ).toBe(true);
    expect(
      isSameRaceTrendTargets(DEFAULT_RACE_TREND_TARGETS, {
        ...DEFAULT_RACE_TREND_TARGETS,
        frame: false,
      }),
    ).toBe(false);
  });

  it("clears trend target query params", () => {
    const params = new URLSearchParams("raceTrendTargets=frame&trend=jockey&trendTargets=race");
    clearRaceTrendTargetQueryParams(params);
    expect(params.toString()).toBe("");
  });
});
