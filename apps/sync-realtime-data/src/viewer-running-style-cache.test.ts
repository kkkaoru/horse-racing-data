// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { RaceRunningStyleRow, RunningStyleInferenceRace } from "./running-style-d1";
import type { Env } from "./types";

vi.mock("./d1-query-cache", () => ({
  putD1QueryCache: vi.fn(async () => {}),
}));
vi.mock("./running-style-entry-coverage", () => ({
  evaluateRunningStyleCacheCoverage: vi.fn(),
}));
vi.mock("./running-style-cache", () => ({
  putRunningStyleCache: vi.fn(),
}));
vi.mock("./running-style-features", () => ({
  buildRealtimeRaceKeyFromRunningStyle: vi.fn(() => "jra:2026:0512:08:01"),
}));
vi.mock("./storage", () => ({
  getLatestRaceEntries: vi.fn(),
}));

const RACE: RunningStyleInferenceRace = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0512",
  keibajoCode: "08",
  raceBango: "01",
  raceKey: "jra:2026:0512:08:01",
  source: "jra",
};

const ROW: RaceRunningStyleRow = {
  bamei: "サンプル",
  category: "jra",
  horseNumber: 1,
  kaisaiNen: "2026",
  kettoTorokuBango: "2024100001",
  modelVersion: "v7-lineage",
  pNige: 0.1,
  pOikomi: 0.2,
  pSashi: 0.3,
  pSenkou: 0.4,
  predictedAt: "2026-05-12T11:30:00+09:00",
  predictedLabel: "senkou",
  raceKey: "jra:2026:0512:08:01",
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("putViewerRunningStyleRaceCache returns false when rows is empty", async () => {
  const { putViewerRunningStyleRaceCache } = await import("./viewer-running-style-cache");
  const env = { REALTIME_DB: {} } as unknown as Env;
  const result = await putViewerRunningStyleRaceCache({
    env,
    race: RACE,
    rows: [],
  });
  expect(result).toBe(false);
});

it("putViewerRunningStyleRaceCache returns false when coverage rejects caching", async () => {
  const { putViewerRunningStyleRaceCache } = await import("./viewer-running-style-cache");
  const { evaluateRunningStyleCacheCoverage } = await import("./running-style-entry-coverage");
  const { getLatestRaceEntries } = await import("./storage");
  vi.mocked(getLatestRaceEntries).mockResolvedValue(null);
  vi.mocked(evaluateRunningStyleCacheCoverage).mockReturnValue({
    cacheable: false,
    cacheableRows: [],
  } as never);
  const env = { REALTIME_DB: {} } as unknown as Env;
  const result = await putViewerRunningStyleRaceCache({
    env,
    race: RACE,
    rows: [ROW],
  });
  expect(result).toBe(false);
});

it("putViewerRunningStyleRaceCache writes cache when coverage is acceptable", async () => {
  const { putViewerRunningStyleRaceCache } = await import("./viewer-running-style-cache");
  const { evaluateRunningStyleCacheCoverage } = await import("./running-style-entry-coverage");
  const { getLatestRaceEntries } = await import("./storage");
  const { putRunningStyleCache } = await import("./running-style-cache");
  const { putD1QueryCache } = await import("./d1-query-cache");
  vi.mocked(getLatestRaceEntries).mockResolvedValue({ fetchedAt: "x", horses: [] });
  vi.mocked(evaluateRunningStyleCacheCoverage).mockReturnValue({
    cacheable: true,
    cacheableRows: [ROW],
  } as never);
  vi.mocked(putRunningStyleCache).mockResolvedValue(true);

  const env = {
    DETAIL_SECTION_CACHE_KV: {},
    REALTIME_DB: {},
  } as unknown as Env;
  const result = await putViewerRunningStyleRaceCache({
    env,
    race: RACE,
    rows: [ROW],
  });
  expect(result).toBe(true);
  expect(putD1QueryCache).toHaveBeenCalledTimes(1);
});

it("putViewerRunningStyleBatchCache returns early when raceKeys empty", async () => {
  const { putViewerRunningStyleBatchCache } = await import("./viewer-running-style-cache");
  const { putD1QueryCache } = await import("./d1-query-cache");
  await putViewerRunningStyleBatchCache({
    raceKeys: ["", ""],
    rows: [ROW],
  });
  expect(putD1QueryCache).not.toHaveBeenCalled();
});

it("putViewerRunningStyleBatchCache returns early when rows empty", async () => {
  const { putViewerRunningStyleBatchCache } = await import("./viewer-running-style-cache");
  const { putD1QueryCache } = await import("./d1-query-cache");
  await putViewerRunningStyleBatchCache({
    raceKeys: ["k1"],
    rows: [],
  });
  expect(putD1QueryCache).not.toHaveBeenCalled();
});

it("putViewerRunningStyleBatchCache writes deduplicated raceKeys to D1QueryCache", async () => {
  const { putViewerRunningStyleBatchCache } = await import("./viewer-running-style-cache");
  const { putD1QueryCache } = await import("./d1-query-cache");
  await putViewerRunningStyleBatchCache({
    raceKeys: ["k1", "k2", "k1"],
    rows: [ROW],
  });
  expect(putD1QueryCache).toHaveBeenCalledTimes(1);
  expect(vi.mocked(putD1QueryCache).mock.calls[0]![1]).toStrictEqual([
    "getRaceRunningStylesByRaceKeysFromD1",
    ["k1", "k2"],
  ]);
});
