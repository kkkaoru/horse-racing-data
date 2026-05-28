// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { RunningStyleDateProgressRow } from "./running-style-date-progress";
import type { Env } from "./types";

vi.mock("./finish-position-lite-pool", () => ({
  getFinishPositionPool: vi.fn(),
}));
vi.mock("./running-style-d1", () => ({
  getRunningStyleInferenceState: vi.fn(),
  listRaceRunningStyleCounts: vi.fn(),
}));
vi.mock("./running-style-entry-coverage", () => ({
  evaluateRunningStyleCacheCoverage: vi.fn(),
}));
vi.mock("./viewer-running-style-cache-probe", () => ({
  isViewerRunningStyleRaceCacheReady: vi.fn(),
}));
vi.mock("./running-style-race-list", () => ({
  listRunningStyleRacesByDate: vi.fn(),
}));
vi.mock("./storage", () => ({
  getLatestRaceEntries: vi.fn(),
}));

const COMPLETE_ROW: RunningStyleDateProgressRow = {
  cacheReady: true,
  d1Count: 5,
  displayReady: true,
  expectedHorses: 5,
  featuresReady: true,
  inferenceStatus: "completed",
  parquetReady: true,
  raceKey: "jra:20260512:08:01",
  source: "jra",
};

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("isRunningStyleDateProgressRowComplete returns true for a fully-ready row", async () => {
  const { isRunningStyleDateProgressRowComplete } = await import("./running-style-date-progress");
  expect(isRunningStyleDateProgressRowComplete(COMPLETE_ROW)).toBe(true);
});

it("isRunningStyleDateProgressRowComplete returns false when cache is not ready", async () => {
  const { isRunningStyleDateProgressRowComplete } = await import("./running-style-date-progress");
  expect(isRunningStyleDateProgressRowComplete({ ...COMPLETE_ROW, cacheReady: false })).toBe(false);
});

it("isRunningStyleDateProgressRowComplete returns false when d1Count is below expected", async () => {
  const { isRunningStyleDateProgressRowComplete } = await import("./running-style-date-progress");
  expect(isRunningStyleDateProgressRowComplete({ ...COMPLETE_ROW, d1Count: 1 })).toBe(false);
});

it("isRunningStyleDateProgressRowDisplayReady mirrors cacheReady", async () => {
  const { isRunningStyleDateProgressRowDisplayReady } =
    await import("./running-style-date-progress");
  expect(isRunningStyleDateProgressRowDisplayReady(COMPLETE_ROW)).toBe(true);
  expect(isRunningStyleDateProgressRowDisplayReady({ ...COMPLETE_ROW, cacheReady: false })).toBe(
    false,
  );
});

it("summarizeRunningStyleDateProgress counts a fully-ready single row", async () => {
  const { summarizeRunningStyleDateProgress } = await import("./running-style-date-progress");
  expect(summarizeRunningStyleDateProgress([COMPLETE_ROW])).toStrictEqual({
    cacheReady: 1,
    d1Ready: 1,
    displayReady: 1,
    expectedHorses: 5,
    featureReady: 1,
    incomplete: 0,
    parquetReady: 1,
    scanned: 1,
  });
});

it("summarizeRunningStyleDateProgress counts incomplete rows correctly", async () => {
  const { summarizeRunningStyleDateProgress } = await import("./running-style-date-progress");
  expect(
    summarizeRunningStyleDateProgress([
      {
        ...COMPLETE_ROW,
        cacheReady: false,
        d1Count: 0,
        displayReady: false,
        parquetReady: false,
      },
    ]),
  ).toStrictEqual({
    cacheReady: 0,
    d1Ready: 0,
    displayReady: 0,
    expectedHorses: 5,
    featureReady: 1,
    incomplete: 1,
    parquetReady: 0,
    scanned: 1,
  });
});

it("summarizeRunningStyleDateProgress treats expectedHorses=0 as not feature-ready", async () => {
  const { summarizeRunningStyleDateProgress } = await import("./running-style-date-progress");
  expect(
    summarizeRunningStyleDateProgress([{ ...COMPLETE_ROW, expectedHorses: 0, d1Count: 0 }]),
  ).toStrictEqual({
    cacheReady: 1,
    d1Ready: 0,
    displayReady: 1,
    expectedHorses: 0,
    featureReady: 0,
    incomplete: 0,
    parquetReady: 1,
    scanned: 1,
  });
});

it("resolveRunningStyleDateYmd returns YYYYMMDD untouched", async () => {
  const { resolveRunningStyleDateYmd } = await import("./running-style-date-progress");
  expect(resolveRunningStyleDateYmd("20260512")).toBe("20260512");
});

it("resolveRunningStyleDateYmd combines MM-DD with explicit year", async () => {
  const { resolveRunningStyleDateYmd } = await import("./running-style-date-progress");
  expect(resolveRunningStyleDateYmd("5-12", 2026)).toBe("20260512");
});

it("resolveRunningStyleDateYmd derives year from now when not provided", async () => {
  const { resolveRunningStyleDateYmd } = await import("./running-style-date-progress");
  expect(resolveRunningStyleDateYmd("5-12", undefined, new Date("2026-05-12T00:00:00+09:00"))).toBe(
    "20260512",
  );
});

it("resolveRunningStyleDateYmd throws for invalid formats", async () => {
  const { resolveRunningStyleDateYmd } = await import("./running-style-date-progress");
  expect(() => resolveRunningStyleDateYmd("2026/05/12")).toThrow("Invalid --date value");
});

it("collectRunningStyleDateProgress returns empty array when no registered races", async () => {
  const { collectRunningStyleDateProgress } = await import("./running-style-date-progress");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({ races: [], source: "d1" });
  const env = { REALTIME_DB: {} } as unknown as Env;
  expect(await collectRunningStyleDateProgress(env, "20260512")).toStrictEqual([]);
});

it("collectRunningStyleDateProgress falls back to featureCounts when latestEntries is null", async () => {
  const { collectRunningStyleDateProgress } = await import("./running-style-date-progress");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const { listRaceRunningStyleCounts, getRunningStyleInferenceState } =
    await import("./running-style-d1");
  const { isViewerRunningStyleRaceCacheReady } = await import("./viewer-running-style-cache-probe");
  const { getLatestRaceEntries } = await import("./storage");

  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "02",
        source: "jra",
      },
    ],
    source: "d1",
  });
  const query = vi.fn(async () => ({
    rows: [{ count: "7", race_key: "jra:20260512:08:02" }],
  }));
  vi.mocked(getFinishPositionPool).mockReturnValue({ query } as never);
  vi.mocked(listRaceRunningStyleCounts).mockResolvedValue(new Map());
  vi.mocked(getLatestRaceEntries).mockResolvedValue(null);
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue(null);
  vi.mocked(isViewerRunningStyleRaceCacheReady).mockResolvedValue(false);

  const env = {
    REALTIME_DB: {},
    RUNNING_STYLE_MODELS: { head: vi.fn(async () => null) },
  } as unknown as Env;

  const rows = await collectRunningStyleDateProgress(env, "20260512");
  expect(rows).toHaveLength(1);
  expect(rows[0]?.expectedHorses).toBe(7);
  expect(rows[0]?.d1Count).toBe(0);
  expect(rows[0]?.parquetReady).toBe(false);
  expect(rows[0]?.inferenceStatus).toBe("missing");
});

it("collectRunningStyleDateProgress defaults expectedHorses to 0 when no feature count and no entries", async () => {
  const { collectRunningStyleDateProgress } = await import("./running-style-date-progress");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const { listRaceRunningStyleCounts, getRunningStyleInferenceState } =
    await import("./running-style-d1");
  const { isViewerRunningStyleRaceCacheReady } = await import("./viewer-running-style-cache-probe");
  const { getLatestRaceEntries } = await import("./storage");

  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "03",
        source: "jra",
      },
    ],
    source: "d1",
  });
  const query = vi.fn(async () => ({ rows: [] }));
  vi.mocked(getFinishPositionPool).mockReturnValue({ query } as never);
  vi.mocked(listRaceRunningStyleCounts).mockResolvedValue(new Map());
  vi.mocked(getLatestRaceEntries).mockResolvedValue(null);
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 0,
    featuresR2Key: null,
    modelVersion: null,
    status: "queued",
    writtenHorseCount: 0,
  } as never);
  vi.mocked(isViewerRunningStyleRaceCacheReady).mockResolvedValue(false);

  const env = {
    REALTIME_DB: {},
    RUNNING_STYLE_MODELS: { head: vi.fn(async () => null) },
  } as unknown as Env;

  const rows = await collectRunningStyleDateProgress(env, "20260512");
  expect(rows).toHaveLength(1);
  expect(rows[0]?.expectedHorses).toBe(0);
  expect(rows[0]?.featuresReady).toBe(false);
  expect(rows[0]?.parquetReady).toBe(false);
  expect(rows[0]?.inferenceStatus).toBe("queued");
});

it("collectRunningStyleDateProgress builds one row per registered race", async () => {
  const { collectRunningStyleDateProgress } = await import("./running-style-date-progress");
  const { listRunningStyleRacesByDate } = await import("./running-style-race-list");
  const { getFinishPositionPool } = await import("./finish-position-lite-pool");
  const { listRaceRunningStyleCounts, getRunningStyleInferenceState } =
    await import("./running-style-d1");
  const { evaluateRunningStyleCacheCoverage } = await import("./running-style-entry-coverage");
  const { isViewerRunningStyleRaceCacheReady } = await import("./viewer-running-style-cache-probe");
  const { getLatestRaceEntries } = await import("./storage");

  vi.mocked(listRunningStyleRacesByDate).mockResolvedValue({
    races: [
      {
        kaisai_nen: "2026",
        kaisai_tsukihi: "0512",
        keibajo_code: "08",
        race_bango: "01",
        source: "jra",
      },
    ],
    source: "d1",
  });
  const query = vi.fn(async () => ({ rows: [{ count: "5", race_key: "jra:20260512:08:01" }] }));
  vi.mocked(getFinishPositionPool).mockReturnValue({ query } as never);
  vi.mocked(listRaceRunningStyleCounts).mockResolvedValue(new Map([["jra:20260512:08:01", 5]]));
  vi.mocked(getLatestRaceEntries).mockResolvedValue({
    fetchedAt: "x",
    horses: [{ horseNumber: "1" }],
  } as never);
  vi.mocked(evaluateRunningStyleCacheCoverage).mockReturnValue({
    activeHorseCount: 5,
    cacheable: false,
    cacheableRows: [],
  });
  vi.mocked(getRunningStyleInferenceState).mockResolvedValue({
    expectedHorseCount: 5,
    featuresR2Key: "key1",
    modelVersion: "v1",
    status: "completed",
    writtenHorseCount: 5,
  } as never);
  const env = {
    REALTIME_DB: {},
    RUNNING_STYLE_MODELS: { head: vi.fn(async () => ({ etag: "e1" })) },
  } as unknown as Env;
  vi.mocked(isViewerRunningStyleRaceCacheReady).mockResolvedValue(true);

  const rows = await collectRunningStyleDateProgress(env, "20260512");
  expect(rows.length).toBe(1);
  expect(rows[0]!.raceKey).toBe("jra:20260512:08:01");
  expect(rows[0]!.parquetReady).toBe(true);
  expect(rows[0]!.cacheReady).toBe(true);
});
