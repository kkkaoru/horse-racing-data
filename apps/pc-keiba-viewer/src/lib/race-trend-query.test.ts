import { describe, expect, it } from "vitest";

import {
  DEFAULT_RACE_TREND_TARGETS,
  getRaceTrendTargetsFromSearchParams,
  serializeRaceTrendTargets,
} from "./race-trend-query";

describe("race trend query helpers", () => {
  it("uses default targets when the query string does not specify trend targets", () => {
    expect(getRaceTrendTargetsFromSearchParams(new URLSearchParams())).toEqual(
      DEFAULT_RACE_TREND_TARGETS,
    );
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
  });

  it("serializes empty targets as none", () => {
    expect(
      serializeRaceTrendTargets({
        frame: false,
        jockey: false,
        raceNumber: false,
        runningStyle: false,
      }),
    ).toBe("none");
  });
});
