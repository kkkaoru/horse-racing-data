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
vi.mock("./running-style-features", async () => {
  const actual = await vi.importActual<typeof import("./running-style-features")>(
    "./running-style-features",
  );
  return {
    buildRealtimeRaceKeyFromRunningStyle: vi.fn(() => "jra:2026:0512:08:01"),
    buildViewerRunningStyleRaceKey: actual.buildViewerRunningStyleRaceKey,
  };
});
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
  expect(vi.mocked(putD1QueryCache).mock.calls[0]![0]).toBe("running-style-race");
  expect(vi.mocked(putD1QueryCache).mock.calls[0]![1]).toStrictEqual([
    "getRaceRunningStylesFromD1",
    "jra:2026:0512:08:01",
  ]);
});

it("putViewerRunningStyleRaceCache writes nar race under the 4-colon viewer raceKey", async () => {
  const { putViewerRunningStyleRaceCache } = await import("./viewer-running-style-cache");
  const { evaluateRunningStyleCacheCoverage } = await import("./running-style-entry-coverage");
  const { getLatestRaceEntries } = await import("./storage");
  const { putRunningStyleCache } = await import("./running-style-cache");
  const { putD1QueryCache } = await import("./d1-query-cache");
  const narRace: RunningStyleInferenceRace = {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0602",
    keibajoCode: "43",
    raceBango: "07",
    raceKey: "nar:20260602:43:07",
    source: "nar",
  };
  const narRow: RaceRunningStyleRow = {
    bamei: "テスト",
    category: "nar",
    horseNumber: 7,
    kaisaiNen: "2026",
    kettoTorokuBango: "2024100007",
    modelVersion: "v7-lineage",
    pNige: 0.4,
    pOikomi: 0.1,
    pSashi: 0.2,
    pSenkou: 0.3,
    predictedAt: "2026-06-02T11:30:00+09:00",
    predictedLabel: "nige",
    raceKey: "nar:20260602:43:07",
  };
  vi.mocked(getLatestRaceEntries).mockResolvedValue({ fetchedAt: "x", horses: [] });
  vi.mocked(evaluateRunningStyleCacheCoverage).mockReturnValue({
    cacheable: true,
    cacheableRows: [narRow],
  } as never);
  vi.mocked(putRunningStyleCache).mockResolvedValue(true);
  const env = {
    DETAIL_SECTION_CACHE_KV: {},
    REALTIME_DB: {},
  } as unknown as Env;

  const result = await putViewerRunningStyleRaceCache({
    env,
    race: narRace,
    rows: [narRow],
  });

  expect(result).toBe(true);
  expect(vi.mocked(putD1QueryCache).mock.calls[0]![1]).toStrictEqual([
    "getRaceRunningStylesFromD1",
    "nar:2026:0602:43:07",
  ]);
});

it("putViewerRunningStyleRaceCache 4-colon key for nar/2026/0602/43/07 hashes to the viewer key", async () => {
  const { buildD1QueryCacheKey } =
    await vi.importActual<typeof import("./d1-query-cache")>("./d1-query-cache");
  expect(
    buildD1QueryCacheKey("running-style-race", [
      "getRaceRunningStylesFromD1",
      "nar:2026:0602:43:07",
    ]),
  ).toBe("400fb61a");
});

it("putViewerRunningStyleRaceCache zero-pads keibajo and race bango into the viewer raceKey", async () => {
  const { putViewerRunningStyleRaceCache } = await import("./viewer-running-style-cache");
  const { evaluateRunningStyleCacheCoverage } = await import("./running-style-entry-coverage");
  const { getLatestRaceEntries } = await import("./storage");
  const { putRunningStyleCache } = await import("./running-style-cache");
  const { putD1QueryCache } = await import("./d1-query-cache");
  const unpaddedRace: RunningStyleInferenceRace = {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "8",
    raceBango: "1",
    raceKey: "jra:20260512:8:1",
    source: "jra",
  };
  const unpaddedRow: RaceRunningStyleRow = {
    bamei: "テスト",
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
    raceKey: "jra:20260512:8:1",
  };
  vi.mocked(getLatestRaceEntries).mockResolvedValue({ fetchedAt: "x", horses: [] });
  vi.mocked(evaluateRunningStyleCacheCoverage).mockReturnValue({
    cacheable: true,
    cacheableRows: [unpaddedRow],
  } as never);
  vi.mocked(putRunningStyleCache).mockResolvedValue(true);
  const env = {
    DETAIL_SECTION_CACHE_KV: {},
    REALTIME_DB: {},
  } as unknown as Env;

  const result = await putViewerRunningStyleRaceCache({
    env,
    race: unpaddedRace,
    rows: [unpaddedRow],
  });

  expect(result).toBe(true);
  expect(vi.mocked(putD1QueryCache).mock.calls[0]![1]).toStrictEqual([
    "getRaceRunningStylesFromD1",
    "jra:2026:0512:08:01",
  ]);
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
