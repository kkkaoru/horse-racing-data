// Run with bun: bunx vitest run src/lib/race-trend-payload.server.test.ts
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  buildPast14WindowForTargetMock: vi.fn<(...args: never[]) => unknown>(),
  fetchRaceTrendDailyTrackMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceDetailMock: vi.fn<(...args: never[]) => unknown>(),
  getLatestTanshoOddsFromHotD1Mock: vi.fn<(...args: never[]) => unknown>(),
  getRaceRunnersMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceRunningStylesWithCacheMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceTrendPast14StarterRowsMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceTrendRunningStylesFromD1Mock: vi.fn<(...args: never[]) => unknown>(),
  getRaceTrendTodayRunningStylesFromD1Mock: vi.fn<(...args: never[]) => unknown>(),
  getRaceTrendTodaySiblingRunnerDataMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceTrendTodayStarterRowsMock: vi.fn<(...args: never[]) => unknown>(),
  safeGetCloudflareEnvMock: vi.fn<() => Promise<unknown>>(),
}));

vi.mock("../db/d1-trend-queries.server", () => ({
  buildPast14WindowForTarget: mocks.buildPast14WindowForTargetMock,
  getLatestTanshoOddsFromHotD1: mocks.getLatestTanshoOddsFromHotD1Mock,
  getRaceTrendPast14StarterRows: mocks.getRaceTrendPast14StarterRowsMock,
  getRaceTrendRunningStylesFromD1: mocks.getRaceTrendRunningStylesFromD1Mock,
  getRaceTrendTodayRunningStylesFromD1: mocks.getRaceTrendTodayRunningStylesFromD1Mock,
  getRaceTrendTodayStarterRows: mocks.getRaceTrendTodayStarterRowsMock,
}));

vi.mock("../db/queries", () => ({
  getRaceDetail: mocks.getRaceDetailMock,
  getRaceRunners: mocks.getRaceRunnersMock,
}));

vi.mock("../db/today-sibling-runner-data.server", () => ({
  getRaceTrendTodaySiblingRunnerData: mocks.getRaceTrendTodaySiblingRunnerDataMock,
}));

vi.mock("./cloudflare-context.server", () => ({
  safeGetCloudflareEnv: mocks.safeGetCloudflareEnvMock,
}));

vi.mock("./race-trend-daily-track-client.server", () => ({
  fetchRaceTrendDailyTrack: mocks.fetchRaceTrendDailyTrackMock,
}));

vi.mock("./running-style-cache.server", () => ({
  getRaceRunningStylesWithCache: mocks.getRaceRunningStylesWithCacheMock,
}));

const {
  buildPast14WindowForTargetMock,
  fetchRaceTrendDailyTrackMock,
  getLatestTanshoOddsFromHotD1Mock,
  getRaceDetailMock,
  getRaceRunnersMock,
  getRaceRunningStylesWithCacheMock,
  getRaceTrendPast14StarterRowsMock,
  getRaceTrendRunningStylesFromD1Mock,
  getRaceTrendTodayRunningStylesFromD1Mock,
  getRaceTrendTodaySiblingRunnerDataMock,
  getRaceTrendTodayStarterRowsMock,
  safeGetCloudflareEnvMock,
} = mocks;

import type {
  RaceTrendDailyTrackRow,
  RaceTrendStarterRow,
} from "horse-racing-realtime/race-trend-daily-track-types";

import type { RaceTrendDailyTrackFetchResult } from "./race-trend-daily-track-client.server";
import {
  buildRaceTrendRawPayloadForRace,
  getRaceTrendPayloadForRace,
  isCacheableTrendPayload,
  pickTodaySiblingRowsAndSource,
} from "./race-trend-payload.server";
import type { RaceDetail, Runner } from "./race-types";

const buildStarterRow = (overrides: Partial<RaceTrendStarterRow> = {}): RaceTrendStarterRow => ({
  bamei: "テスト",
  bataiju: null,
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  finishPosition: 1,
  hassoJikoku: null,
  jockeyName: "騎手",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0529",
  keibajoCode: "05",
  raceBango: "01",
  raceName: null,
  runnerCount: null,
  sohaTime: null,
  source: "jra",
  tanshoOdds: null,
  tanshoPopularity: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

const buildDailyTrackRow = (
  raceBango: string,
  starterRows: RaceTrendStarterRow[],
): RaceTrendDailyTrackRow => ({
  fetchedAt: "2026-05-29T07:30:00.000Z",
  finishedAt: "2026-05-29T07:20:00.000Z",
  isComplete: true,
  raceBango,
  raceKey: `jra:2026:0529:05:${raceBango}`,
  runningStyles: [],
  starterRows,
});

const buildRaceDetail = (overrides: Partial<RaceDetail> = {}): RaceDetail => ({
  babajotaiCodeDirt: null,
  babajotaiCodeShiba: null,
  gradeCode: null,
  hassoJikoku: "1500",
  juryoShubetsuCode: null,
  kaisaiKai: null,
  kaisaiNen: "2026",
  kaisaiNichime: null,
  kaisaiTsukihi: "0529",
  keibajoCode: "05",
  kyori: null,
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosoShubetsuCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  kyosomeiKakkonai: null,
  raceBango: "07",
  shussoTosu: null,
  source: "jra",
  tenkoCode: null,
  torokuTosu: null,
  trackCode: null,
  ...overrides,
});

const buildRunner = (overrides: Partial<Runner> = {}): Runner => ({
  banushimei: null,
  barei: null,
  bataiju: null,
  bamei: null,
  chokyoshimeiRyakusho: null,
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: null,
  kakuteiChakujun: null,
  kettoTorokuBango: null,
  kishumeiRyakusho: null,
  kohan3f: null,
  seibetsuCode: null,
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: null,
  tanshoOdds: null,
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

const buildOptions = () => ({
  source: "jra" as const,
  jockeyStartYmd: "20260515",
  jockeyEndYmd: "20260529",
  frameStartYmd: "20260515",
  frameEndYmd: "20260529",
  includeRealtimeResults: true,
});

beforeEach(() => {
  buildPast14WindowForTargetMock.mockReset();
  fetchRaceTrendDailyTrackMock.mockReset();
  getLatestTanshoOddsFromHotD1Mock.mockReset();
  getRaceDetailMock.mockReset();
  getRaceRunnersMock.mockReset();
  getRaceRunningStylesWithCacheMock.mockReset();
  getRaceTrendPast14StarterRowsMock.mockReset();
  getRaceTrendRunningStylesFromD1Mock.mockReset();
  getRaceTrendTodayRunningStylesFromD1Mock.mockReset();
  getRaceTrendTodaySiblingRunnerDataMock.mockReset();
  getRaceTrendTodayStarterRowsMock.mockReset();
  safeGetCloudflareEnvMock.mockReset();
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  buildPast14WindowForTargetMock.mockReturnValue({ endYmd: "20260528", startYmd: "20260515" });
  getLatestTanshoOddsFromHotD1Mock.mockResolvedValue(new Map());
  getRaceRunningStylesWithCacheMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([]);
  getRaceTrendTodayRunningStylesFromD1Mock.mockResolvedValue([]);
  getRaceTrendTodaySiblingRunnerDataMock.mockResolvedValue([]);
});

it("isCacheableTrendPayload accepts a payload with both starter rows and running-style history", () => {
  expect(
    isCacheableTrendPayload({
      currentRunningStyles: [],
      historicalRunningStyles: [
        { horseNumber: "1", predictedLabel: "nige", raceKey: "nar:2026:0524:47:01" },
      ],
      raceContext: { keibajoCode: "05", raceBango: "01", source: "jra" },
      runners: [],
      starterRows: [buildStarterRow()],
    }),
  ).toBe(true);
});

it("isCacheableTrendPayload rejects an empty payload", () => {
  expect(
    isCacheableTrendPayload({
      currentRunningStyles: [],
      historicalRunningStyles: [],
      raceContext: { keibajoCode: "05", raceBango: "01", source: "jra" },
      runners: [],
      starterRows: [],
    }),
  ).toBe(false);
});

it("pickTodaySiblingRowsAndSource returns DO rows and do-hit header when DO result status is hit", () => {
  const doRow = buildStarterRow({ raceBango: "01", umaban: "03" });
  const fallbackRow = buildStarterRow({ raceBango: "02", umaban: "07" });
  const result: RaceTrendDailyTrackFetchResult = {
    rows: [buildDailyTrackRow("01", [doRow])],
    status: "hit",
  };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [fallbackRow], result })).toStrictEqual({
    rows: [doRow],
    sourceHeader: "do-hit",
  });
});

it("pickTodaySiblingRowsAndSource returns fallback rows with do-miss-fallback header on DO miss", () => {
  const fallbackRow = buildStarterRow({ raceBango: "04", umaban: "05" });
  const result: RaceTrendDailyTrackFetchResult = { rows: [], status: "miss" };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [fallbackRow], result })).toStrictEqual({
    rows: [fallbackRow],
    sourceHeader: "do-miss-fallback",
  });
});

it("pickTodaySiblingRowsAndSource returns fallback rows with do-error-fallback header on DO error", () => {
  const fallbackRow = buildStarterRow({ raceBango: "06", umaban: "08" });
  const result: RaceTrendDailyTrackFetchResult = { rows: [], status: "error" };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [fallbackRow], result })).toStrictEqual({
    rows: [fallbackRow],
    sourceHeader: "do-error-fallback",
  });
});

it("buildRaceTrendRawPayloadForRace returns starter rows from the past14 window when DO is miss", async () => {
  const past14Row = buildStarterRow({ raceBango: "01" });
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([past14Row]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const result = await buildRaceTrendRawPayloadForRace({
    options: buildOptions(),
    race: buildRaceDetail(),
    runners: [buildRunner()],
  });
  expect(result.sourceHeader).toBe("do-miss-fallback");
  expect(result.payload.starterRows.length).toBe(1);
});

it("buildRaceTrendRawPayloadForRace flags do-hit when DO returns rows", async () => {
  const doRow = buildStarterRow({ raceBango: "03", umaban: "07" });
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({
    rows: [buildDailyTrackRow("03", [doRow])],
    status: "hit",
  });
  const result = await buildRaceTrendRawPayloadForRace({
    options: buildOptions(),
    race: buildRaceDetail(),
    runners: [],
  });
  expect(result.sourceHeader).toBe("do-hit");
  expect(getRaceTrendTodayStarterRowsMock).not.toHaveBeenCalled();
});

it("buildRaceTrendRawPayloadForRace degrades gracefully when past14 promise rejects", async () => {
  getRaceTrendPast14StarterRowsMock.mockRejectedValue(new Error("past14 boom"));
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const result = await buildRaceTrendRawPayloadForRace({
    options: buildOptions(),
    race: buildRaceDetail(),
    runners: [],
  });
  expect(result.payload.starterRows).toStrictEqual([]);
});

it("buildRaceTrendRawPayloadForRace degrades gracefully when DO promise rejects", async () => {
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockRejectedValue(new Error("do boom"));
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const result = await buildRaceTrendRawPayloadForRace({
    options: buildOptions(),
    race: buildRaceDetail(),
    runners: [],
  });
  expect(result.sourceHeader).toBe("do-error-fallback");
});

it("buildRaceTrendRawPayloadForRace maps runner wakuban / umaban / jockey into the payload runners array", async () => {
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const result = await buildRaceTrendRawPayloadForRace({
    options: buildOptions(),
    race: buildRaceDetail(),
    runners: [
      buildRunner({ kishumeiRyakusho: "山田太郎", umaban: "01", wakuban: "1" }),
      buildRunner({ kishumeiRyakusho: "鈴木花子", umaban: "08", wakuban: "5" }),
    ],
  });
  expect(result.payload.runners).toStrictEqual([
    { frameNumber: "1", horseNumber: "01", jockeyName: "山田太郎", trainerName: null },
    { frameNumber: "5", horseNumber: "08", jockeyName: "鈴木花子", trainerName: null },
  ]);
});

it("buildRaceTrendRawPayloadForRace dedupes historical running styles across past14 and today fetches", async () => {
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([buildStarterRow({ raceBango: "01" })]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0529:05:01" },
  ]);
  getRaceTrendTodayRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "1", predictedLabel: "oikomi", raceKey: "jra:2026:0529:05:01" },
  ]);
  const result = await buildRaceTrendRawPayloadForRace({
    options: buildOptions(),
    race: buildRaceDetail(),
    runners: [],
  });
  expect(result.payload.historicalRunningStyles).toStrictEqual([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0529:05:01" },
  ]);
});

it("buildRaceTrendRawPayloadForRace surfaces current running styles fetched from the cache helper", async () => {
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceRunningStylesWithCacheMock.mockResolvedValue([
    { horseNumber: 3, predictedLabel: "sashi" },
  ]);
  const result = await buildRaceTrendRawPayloadForRace({
    options: buildOptions(),
    race: buildRaceDetail(),
    runners: [],
  });
  expect(result.payload.currentRunningStyles).toStrictEqual([
    { horseNumber: "3", predictedLabel: "sashi" },
  ]);
});

it("getRaceTrendPayloadForRace returns an empty payload when the race detail is missing", async () => {
  getRaceDetailMock.mockResolvedValue(null);
  const result = await getRaceTrendPayloadForRace({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "07",
    source: "jra",
    year: "2026",
  });
  expect(result).toStrictEqual({ raceCount: 0, runningStyleRows: [] });
});

it("getRaceTrendPayloadForRace skips the runners fetch when the race detail is missing", async () => {
  getRaceDetailMock.mockResolvedValue(null);
  await getRaceTrendPayloadForRace({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "07",
    source: "jra",
    year: "2026",
  });
  expect(getRaceRunnersMock).not.toHaveBeenCalled();
});

it("getRaceTrendPayloadForRace aggregates a payload with running style rows when race detail is present", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail({ raceBango: "07" }));
  getRaceRunnersMock.mockResolvedValue([
    buildRunner({ kishumeiRyakusho: "山田太郎", umaban: "01", wakuban: "1" }),
  ]);
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([
    buildStarterRow({
      finishPosition: 2,
      jockeyName: "山田太郎",
      kaisaiTsukihi: "0520",
      raceBango: "03",
      umaban: "05",
    }),
  ]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "5", predictedLabel: "sashi", raceKey: "jra:2026:0520:05:03" },
  ]);
  const result = await getRaceTrendPayloadForRace({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "07",
    source: "jra",
    year: "2026",
  });
  expect(result.runningStyleRows.length).toBeGreaterThan(0);
});
