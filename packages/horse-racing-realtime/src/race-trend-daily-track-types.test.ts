// Run with bun.
// Structural smoke tests for the daily-track DO shared types.

import { expect, it } from "vitest";

import type {
  RaceTrendDailyTrackQuery,
  RaceTrendDailyTrackResponse,
  RaceTrendDailyTrackRow,
  RaceTrendDailyTrackSource,
  RaceTrendDailyTrackState,
  RaceTrendRunningStyleCache,
  RaceTrendStarterRow,
} from "./race-trend-daily-track-types";

it("constructs a RaceTrendDailyTrackState with a single completed race", () => {
  const starterRow: RaceTrendStarterRow = {
    source: "jra",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0531",
    keibajoCode: "05",
    raceBango: "1",
    raceName: "サンプル",
    hassoJikoku: "1005",
    runnerCount: "16",
    wakuban: "1",
    umaban: "1",
    bamei: "テストウマ",
    jockeyName: "テスト騎手",
    tanshoOdds: "0123",
    tanshoPopularity: "4",
    finishPosition: 1,
    sohaTime: "1234",
    corner1: "1",
    corner2: "1",
    corner3: "1",
    corner4: "1",
    bataiju: "480",
    zogenFugo: "+",
    zogenSa: "2",
  };
  const runningStyle: RaceTrendRunningStyleCache = {
    raceKey: "jra:2026:0531:05:01",
    horseNumber: "1",
    predictedLabel: "senkou",
  };
  const row: RaceTrendDailyTrackRow = {
    raceBango: "1",
    raceKey: "jra:2026:0531:05:01",
    isComplete: true,
    finishedAt: "2026-05-31T01:05:00Z",
    fetchedAt: "2026-05-31T01:05:30Z",
    starterRows: [starterRow],
    runningStyles: [runningStyle],
  };
  const source: RaceTrendDailyTrackSource = "jra";
  const state: RaceTrendDailyTrackState = {
    source,
    targetYmd: "20260531",
    keibajoCode: "05",
    races: { "1": row },
    updatedAt: "2026-05-31T01:05:30Z",
  };
  expect(state.source).toBe("jra");
  expect(state.targetYmd).toBe("20260531");
  expect(state.keibajoCode).toBe("05");
  expect(state.races["1"]?.isComplete).toBe(true);
  expect(state.races["1"]?.starterRows[0]?.finishPosition).toBe(1);
  expect(state.races["1"]?.runningStyles[0]?.predictedLabel).toBe("senkou");
});

it("constructs a RaceTrendDailyTrackQuery with NAR source", () => {
  const query: RaceTrendDailyTrackQuery = {
    source: "nar",
    targetYmd: "20260531",
    keibajoCode: "46",
    beforeRaceBango: "5",
  };
  expect(query.source).toBe("nar");
  expect(query.beforeRaceBango).toBe("5");
});

it("constructs a RaceTrendDailyTrackResponse with an empty races array", () => {
  const response: RaceTrendDailyTrackResponse = { races: [] };
  expect(response.races).toStrictEqual([]);
});
