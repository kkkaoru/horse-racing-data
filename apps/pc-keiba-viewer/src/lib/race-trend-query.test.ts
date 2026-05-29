import { describe, expect, it } from "vitest";

import {
  clearRaceTrendScoreConditionsQueryParam,
  clearRaceTrendScoreLinkQuery,
  clearRaceTrendSortKeyQueryParam,
  clearRaceTrendTargetQueryParams,
  DEFAULT_RACE_TREND_SCORE_CONDITIONS_QUERY,
  DEFAULT_RACE_TREND_SCORE_LINK_TO_WIN_RATE,
  DEFAULT_RACE_TREND_SORT_KEY,
  DEFAULT_RACE_TREND_TARGETS,
  getRaceTrendScoreConditionsFromSearchParams,
  getRaceTrendScoreConditionsQueryValue,
  getRaceTrendScoreLinkFromSearchParams,
  getRaceTrendScoreLinkQueryValue,
  getRaceTrendSortKeyFromSearchParams,
  getRaceTrendSortKeyQueryValue,
  getRaceTrendTargetQueryValue,
  getRaceTrendTargetsFromSearchParams,
  isDefaultRaceTrendScoreConditionsQuery,
  isDefaultRaceTrendScoreLinkToWinRate,
  isDefaultRaceTrendSortKey,
  isDefaultRaceTrendTargets,
  isSameRaceTrendScoreConditionsQuery,
  isSameRaceTrendTargets,
  parseRaceTrendScoreConditionsQuery,
  parseRaceTrendScoreLinkQuery,
  parseRaceTrendSortKeyQuery,
  parseRaceTrendTargets,
  serializeRaceTrendScoreConditionsQuery,
  serializeRaceTrendScoreLinkQuery,
  serializeRaceTrendSortKeyQuery,
  serializeRaceTrendTargets,
} from "./race-trend-query";

describe("race trend query helpers", () => {
  it("uses default targets (jockey only) when the query string does not specify trend targets", () => {
    expect(getRaceTrendTargetsFromSearchParams(new URLSearchParams())).toStrictEqual({
      runningStyle: false,
      frame: false,
      jockey: true,
      raceNumber: false,
    });
    expect(DEFAULT_RACE_TREND_TARGETS).toStrictEqual({
      runningStyle: false,
      frame: false,
      jockey: true,
      raceNumber: false,
    });
    expect(
      isDefaultRaceTrendTargets({
        runningStyle: false,
        frame: false,
        jockey: true,
        raceNumber: false,
      }),
    ).toBe(true);
  });

  it("reads primary and alias query param names", () => {
    expect(getRaceTrendTargetQueryValue(new URLSearchParams("raceTrendTargets=frame"))).toBe(
      "frame",
    );
    expect(getRaceTrendTargetQueryValue(new URLSearchParams("trendTargets=jockey"))).toBe("jockey");
    expect(getRaceTrendTargetQueryValue(new URLSearchParams("trend=race"))).toBe("race");
    expect(getRaceTrendTargetQueryValue({ trend: ["style", "ignored"] })).toBe("style");
    expect(getRaceTrendTargetQueryValue({ raceTrendTargets: undefined })).toBeNull();
    expect(getRaceTrendTargetQueryValue({ trend: [] })).toBeNull();
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
      isSameRaceTrendTargets(
        { runningStyle: false, frame: false, jockey: true, raceNumber: false },
        { runningStyle: false, frame: false, jockey: true, raceNumber: false },
      ),
    ).toBe(true);
    expect(
      isSameRaceTrendTargets(
        { runningStyle: false, frame: false, jockey: true, raceNumber: false },
        { runningStyle: false, frame: false, jockey: false, raceNumber: false },
      ),
    ).toBe(false);
  });

  it("clears trend target query params", () => {
    const params = new URLSearchParams("raceTrendTargets=frame&trend=jockey&trendTargets=race");
    clearRaceTrendTargetQueryParams(params);
    expect(params.toString()).toBe("");
  });
});

describe("race trend score condition query helpers", () => {
  it("uses default score conditions when the query string omits the param", () => {
    expect(getRaceTrendScoreConditionsFromSearchParams(new URLSearchParams())).toStrictEqual({
      frame: true,
      jockey: true,
      frameRunningStyle: false,
    });
    expect(
      isDefaultRaceTrendScoreConditionsQuery({
        frame: true,
        jockey: true,
        frameRunningStyle: false,
      }),
    ).toBe(true);
  });

  it("reads the score conditions query param from URLSearchParams", () => {
    expect(
      getRaceTrendScoreConditionsQueryValue(
        new URLSearchParams("raceTrendScoreConditions=frame,jockey"),
      ),
    ).toBe("frame,jockey");
  });

  it("reads the score conditions query param from a search param record", () => {
    expect(
      getRaceTrendScoreConditionsQueryValue({ raceTrendScoreConditions: "frameRunningStyle" }),
    ).toBe("frameRunningStyle");
    expect(
      getRaceTrendScoreConditionsQueryValue({ raceTrendScoreConditions: undefined }),
    ).toBeNull();
    expect(
      getRaceTrendScoreConditionsQueryValue({ raceTrendScoreConditions: ["frame", "ignored"] }),
    ).toBe("frame");
    expect(getRaceTrendScoreConditionsQueryValue({ raceTrendScoreConditions: [] })).toBeNull();
  });

  it("parses none as all-false conditions", () => {
    expect(parseRaceTrendScoreConditionsQuery("none")).toEqual({
      frame: false,
      jockey: false,
      frameRunningStyle: false,
    });
  });

  it("parses an empty string as all-false conditions", () => {
    expect(parseRaceTrendScoreConditionsQuery("")).toEqual({
      frame: false,
      jockey: false,
      frameRunningStyle: false,
    });
  });

  it("parses single token shortcuts", () => {
    expect(parseRaceTrendScoreConditionsQuery("frame")).toEqual({
      frame: true,
      jockey: false,
      frameRunningStyle: false,
    });
    expect(parseRaceTrendScoreConditionsQuery("jockey")).toEqual({
      frame: false,
      jockey: true,
      frameRunningStyle: false,
    });
    expect(parseRaceTrendScoreConditionsQuery("frameRunningStyle")).toEqual({
      frame: false,
      jockey: false,
      frameRunningStyle: true,
    });
  });

  it("parses comma separated tokens and ignores unknown tokens around known ones", () => {
    expect(parseRaceTrendScoreConditionsQuery("frame,unknown,jockey")).toEqual({
      frame: true,
      jockey: true,
      frameRunningStyle: false,
    });
  });

  it("returns null for null input and all-unknown tokens", () => {
    expect(parseRaceTrendScoreConditionsQuery(null)).toBeNull();
    expect(parseRaceTrendScoreConditionsQuery("unknown")).toBeNull();
  });

  it("serializes all-false conditions to none", () => {
    expect(
      serializeRaceTrendScoreConditionsQuery({
        frame: false,
        jockey: false,
        frameRunningStyle: false,
      }),
    ).toBe("none");
  });

  it("serializes selected conditions in canonical order", () => {
    expect(
      serializeRaceTrendScoreConditionsQuery({
        frame: false,
        jockey: true,
        frameRunningStyle: true,
      }),
    ).toBe("jockey,frameRunningStyle");
  });

  it("round-trips parse and serialize for the default conditions", () => {
    expect(
      serializeRaceTrendScoreConditionsQuery(
        parseRaceTrendScoreConditionsQuery("jockey") ?? {
          frame: false,
          jockey: false,
          frameRunningStyle: false,
        },
      ),
    ).toBe("jockey");
  });

  it("default has frame and jockey", () => {
    expect(DEFAULT_RACE_TREND_SCORE_CONDITIONS_QUERY).toStrictEqual({
      frame: true,
      jockey: true,
      frameRunningStyle: false,
    });
  });

  it("compares score conditions by exact equality (same)", () => {
    expect(
      isSameRaceTrendScoreConditionsQuery(
        { frame: false, jockey: true, frameRunningStyle: false },
        { frame: false, jockey: true, frameRunningStyle: false },
      ),
    ).toBe(true);
  });

  it("compares score conditions by exact equality (different)", () => {
    expect(
      isSameRaceTrendScoreConditionsQuery(
        { frame: false, jockey: true, frameRunningStyle: false },
        { frame: true, jockey: true, frameRunningStyle: false },
      ),
    ).toBe(false);
  });

  it("reports non-default conditions as non-default", () => {
    expect(
      isDefaultRaceTrendScoreConditionsQuery({
        frame: true,
        jockey: false,
        frameRunningStyle: false,
      }),
    ).toBe(false);
  });

  it("clears the score conditions query param", () => {
    const params = new URLSearchParams("raceTrendScoreConditions=frame,jockey&other=keep");
    clearRaceTrendScoreConditionsQueryParam(params);
    expect(params.toString()).toBe("other=keep");
  });
});

describe("race trend sort key query helpers", () => {
  it("uses the default sort key when the query string omits the param", () => {
    expect(getRaceTrendSortKeyFromSearchParams(new URLSearchParams())).toBe("showRate");
    expect(DEFAULT_RACE_TREND_SORT_KEY).toBe("showRate");
    expect(isDefaultRaceTrendSortKey("showRate")).toBe(true);
  });

  it("reads the sort key query param from URLSearchParams", () => {
    expect(getRaceTrendSortKeyQueryValue(new URLSearchParams("raceTrendSortBy=score"))).toBe(
      "score",
    );
  });

  it("reads the sort key query param from a search param record", () => {
    expect(getRaceTrendSortKeyQueryValue({ raceTrendSortBy: "winRate" })).toBe("winRate");
    expect(getRaceTrendSortKeyQueryValue({ raceTrendSortBy: undefined })).toBeNull();
    expect(getRaceTrendSortKeyQueryValue({ raceTrendSortBy: ["quinellaRate", "ignored"] })).toBe(
      "quinellaRate",
    );
    expect(getRaceTrendSortKeyQueryValue({ raceTrendSortBy: [] })).toBeNull();
  });

  it("parses each valid sort key", () => {
    expect(parseRaceTrendSortKeyQuery("score")).toBe("score");
    expect(parseRaceTrendSortKeyQuery("showRate")).toBe("showRate");
    expect(parseRaceTrendSortKeyQuery("quinellaRate")).toBe("quinellaRate");
    expect(parseRaceTrendSortKeyQuery("winRate")).toBe("winRate");
  });

  it("trims whitespace around the sort key value", () => {
    expect(parseRaceTrendSortKeyQuery("  winRate  ")).toBe("winRate");
  });

  it("falls back to the default sort key for null or invalid input", () => {
    expect(parseRaceTrendSortKeyQuery(null)).toBe("showRate");
    expect(parseRaceTrendSortKeyQuery("")).toBe("showRate");
    expect(parseRaceTrendSortKeyQuery("unknown")).toBe("showRate");
  });

  it("serializes the sort key as its own string", () => {
    expect(serializeRaceTrendSortKeyQuery("score")).toBe("score");
    expect(serializeRaceTrendSortKeyQuery("showRate")).toBe("showRate");
    expect(serializeRaceTrendSortKeyQuery("quinellaRate")).toBe("quinellaRate");
    expect(serializeRaceTrendSortKeyQuery("winRate")).toBe("winRate");
  });

  it("round-trips parse and serialize for a non-default sort key", () => {
    expect(serializeRaceTrendSortKeyQuery(parseRaceTrendSortKeyQuery("score"))).toBe("score");
  });

  it("reports non-default sort keys as non-default", () => {
    expect(isDefaultRaceTrendSortKey("score")).toBe(false);
    expect(isDefaultRaceTrendSortKey("winRate")).toBe(false);
    expect(isDefaultRaceTrendSortKey("quinellaRate")).toBe(false);
  });

  it("clears the sort key query param", () => {
    const params = new URLSearchParams("raceTrendSortBy=score&other=keep");
    clearRaceTrendSortKeyQueryParam(params);
    expect(params.toString()).toBe("other=keep");
  });
});

it("score-link default is false", () => {
  expect(getRaceTrendScoreLinkFromSearchParams(new URLSearchParams())).toBe(false);
});

it("score-link exposes DEFAULT_RACE_TREND_SCORE_LINK_TO_WIN_RATE as false", () => {
  expect(DEFAULT_RACE_TREND_SCORE_LINK_TO_WIN_RATE).toBe(false);
});

it("score-link parses '1' as true", () => {
  expect(parseRaceTrendScoreLinkQuery("1")).toBe(true);
});

it("score-link parses 'true' as true", () => {
  expect(parseRaceTrendScoreLinkQuery("true")).toBe(true);
});

it("score-link parses 'TRUE' (case insensitive) as true", () => {
  expect(parseRaceTrendScoreLinkQuery("TRUE")).toBe(true);
});

it("score-link parses '0' as false", () => {
  expect(parseRaceTrendScoreLinkQuery("0")).toBe(false);
});

it("score-link parses 'false' as false", () => {
  expect(parseRaceTrendScoreLinkQuery("false")).toBe(false);
});

it("score-link parses null as false (default)", () => {
  expect(parseRaceTrendScoreLinkQuery(null)).toBe(false);
});

it("score-link parses invalid as default (false)", () => {
  expect(parseRaceTrendScoreLinkQuery("garbage")).toBe(false);
});

it("score-link parses empty string as default (false)", () => {
  expect(parseRaceTrendScoreLinkQuery("")).toBe(false);
});

it("score-link trims whitespace around the value", () => {
  expect(parseRaceTrendScoreLinkQuery("  1  ")).toBe(true);
});

it("score-link serialize true is '1'", () => {
  expect(serializeRaceTrendScoreLinkQuery(true)).toBe("1");
});

it("score-link serialize false is '0'", () => {
  expect(serializeRaceTrendScoreLinkQuery(false)).toBe("0");
});

it("score-link round-trip true", () => {
  expect(parseRaceTrendScoreLinkQuery(serializeRaceTrendScoreLinkQuery(true))).toBe(true);
});

it("score-link round-trip false", () => {
  expect(parseRaceTrendScoreLinkQuery(serializeRaceTrendScoreLinkQuery(false))).toBe(false);
});

it("score-link reads the query param from URLSearchParams", () => {
  expect(
    getRaceTrendScoreLinkQueryValue(new URLSearchParams("raceTrendScoreLinkToWinRate=1")),
  ).toBe("1");
});

it("score-link reads the query param from a search param record (string)", () => {
  expect(getRaceTrendScoreLinkQueryValue({ raceTrendScoreLinkToWinRate: "true" })).toBe("true");
});

it("score-link reads the query param from a search param record (array picks first)", () => {
  expect(getRaceTrendScoreLinkQueryValue({ raceTrendScoreLinkToWinRate: ["1", "ignored"] })).toBe(
    "1",
  );
});

it("score-link query param record returns null for undefined", () => {
  expect(getRaceTrendScoreLinkQueryValue({ raceTrendScoreLinkToWinRate: undefined })).toBeNull();
});

it("score-link query param record returns null for empty array", () => {
  expect(getRaceTrendScoreLinkQueryValue({ raceTrendScoreLinkToWinRate: [] })).toBeNull();
});

it("score-link reads true from search params via getRaceTrendScoreLinkFromSearchParams", () => {
  expect(
    getRaceTrendScoreLinkFromSearchParams(new URLSearchParams("raceTrendScoreLinkToWinRate=1")),
  ).toBe(true);
});

it("score-link reads false from search params via getRaceTrendScoreLinkFromSearchParams", () => {
  expect(
    getRaceTrendScoreLinkFromSearchParams(new URLSearchParams("raceTrendScoreLinkToWinRate=0")),
  ).toBe(false);
});

it("score-link reports default value (false) as default", () => {
  expect(isDefaultRaceTrendScoreLinkToWinRate(false)).toBe(true);
});

it("score-link reports non-default value (true) as non-default", () => {
  expect(isDefaultRaceTrendScoreLinkToWinRate(true)).toBe(false);
});

it("score-link clears the query param", () => {
  const params = new URLSearchParams("raceTrendScoreLinkToWinRate=1&other=keep");
  clearRaceTrendScoreLinkQuery(params);
  expect(params.toString()).toBe("other=keep");
});
