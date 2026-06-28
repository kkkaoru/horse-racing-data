// run with: bun run test
// Per-race retry coverage for upsertDiscoveredUrls. The discover-urls handler
// historically aborted on the first D1_ERROR in the per-race upsert loop; this
// test file pins down the per-race try/catch + bounded exponential backoff so
// one transient D1 failure leaves the rest of the date's races still ingested.
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { Env } from "./types";

vi.mock("./storage", () => ({
  logFetch: vi.fn(async () => {}),
  upsertNarRaceSource: vi.fn(async () => {}),
  upsertJraRaceSource: vi.fn(async () => {}),
  listRaceSourceKeibajoCodesByDate: vi.fn(async () => []),
  getRaceSource: vi.fn(async () => null),
  listSchedulableRaceSourcesByDate: vi.fn(async () => []),
  getVenueLastRaceStartAtJst: vi.fn(async () => null),
  countRaceSourcesByDate: vi.fn(async () => 0),
  countJraRaceSourcesMissingRaceDateFieldsByDate: vi.fn(async () => 0),
  listJraVenueTrackConditionSchedulesByDate: vi.fn(async () => []),
  markTrackConditionQueued: vi.fn(async () => {}),
  claimTrackConditionFetch: vi.fn(async () => false),
  failTrackConditionFetch: vi.fn(async () => {}),
  completeTrackConditionFetch: vi.fn(async () => {}),
  updateOddsLinks: vi.fn(async () => {}),
  updateLastFetch: vi.fn(async () => {}),
  markResultFetchQueued: vi.fn(async () => {}),
  markOddsFetchQueued: vi.fn(async () => {}),
  claimOddsFetch: vi.fn(async () => false),
  claimResultFetch: vi.fn(async () => false),
  completeOddsFetch: vi.fn(async () => {}),
  failOddsFetch: vi.fn(async () => {}),
  completeResultFetch: vi.fn(async () => {}),
  recordPartialResultFetch: vi.fn(async () => {}),
  failResultFetch: vi.fn(async () => {}),
  incrementEmptyResultAttempts: vi.fn(async () => 0),
  markEmptyResultGiveUp: vi.fn(async () => {}),
  resetEmptyResultAttempts: vi.fn(async () => {}),
  insertOddsSnapshot: vi.fn(async () => 0),
  insertHorseWeightSnapshot: vi.fn(async () => {}),
  insertRaceEntrySnapshot: vi.fn(async () => 0),
  insertRaceResultSnapshot: vi.fn(async () => 0),
  runD1Retention: vi.fn(async () => ({ fetchLogsDeleted: 0, oddsSnapshotsDeleted: 0 })),
  upsertPremiumRaceLink: vi.fn(async () => {}),
  getPremiumRaceLink: vi.fn(async () => null),
  replacePremiumRaceData: vi.fn(async () => {}),
  getPremiumRacePayload: vi.fn(async () => null),
  listPremiumRaceDataFetchCandidatesByDate: vi.fn(async () => []),
  markPremiumRaceDataQueued: vi.fn(async () => {}),
  getPremiumRaceDataFetchState: vi.fn(async () => null),
  updatePremiumRaceDataFetchState: vi.fn(async () => {}),
  markPremiumPaddockQueued: vi.fn(async () => {}),
  getPremiumPaddockFetchState: vi.fn(async () => null),
  updatePremiumPaddockFetchState: vi.fn(async () => {}),
  getPremiumPaddockNotificationState: vi.fn(async () => null),
  updatePremiumPaddockNotificationState: vi.fn(async () => {}),
  claimPremiumPaddockNotificationSend: vi.fn(async () => true),
  recordPremiumPaddockNotificationEvent: vi.fn(async () => {}),
  listTanshoHistory: vi.fn(async () => []),
  listOddsHistoryByType: vi.fn(async () => ({})),
  getLatestOddsFromD1: vi.fn(async () => null),
  toHorseTrends: vi.fn(() => []),
  toOddsTrendsByType: vi.fn(() => ({})),
  getLatestHorseWeights: vi.fn(async () => null),
  getLatestRaceEntries: vi.fn(async () => null),
  getLatestRaceResults: vi.fn(async () => null),
  getLatestTrackConditionForRace: vi.fn(async () => null),
  insertJraTrackConditionSnapshot: vi.fn(async () => []),
  getSameDayVenueJockeyWins: vi.fn(async () => []),
  buildRealtimePayload: vi.fn(async () => ({}) as never),
}));
vi.mock("./postgres", () => ({
  fetchJraRacesByDate: vi.fn(async () => []),
  fetchNarRacesByDate: vi.fn(async () => []),
}));
vi.mock("./keiba-go", async () => {
  const actual = await vi.importActual<typeof import("./keiba-go")>("./keiba-go");
  return {
    ...actual,
    fetchTodayRaceListUrls: vi.fn(async () => []),
    fetchRacePage: vi.fn(async () => "<html></html>"),
    fetchRaceLinksFromRaceList: vi.fn(async () => []),
  };
});
vi.mock("./jra", async () => {
  const actual = await vi.importActual<typeof import("./jra")>("./jra");
  return {
    ...actual,
  };
});

const buildEnv = (overrides?: Partial<Env>): Env =>
  ({
    PREMIUM_RACE_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    REALTIME_DB: {},
    REALTIME_JOBS: { send: vi.fn(async () => {}), sendBatch: vi.fn(async () => {}) },
    ...overrides,
  }) as unknown as Env;

const noopSleep = vi.fn(async () => {});

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.restoreAllMocks();
});

it("upsertDiscoveredUrls returns inserted=1 retried=0 failed=0 on a clean JRA race upsert", async () => {
  const { upsertDiscoveredUrls } = await import("./worker");
  const { fetchJraRacesByDate } = await import("./postgres");
  vi.mocked(fetchJraRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1500",
      kaisai_kai: "02",
      kaisai_nen: "2026",
      kaisai_nichime: "06",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "JRA",
      race_bango: "1",
    },
  ] as never);
  const result = await upsertDiscoveredUrls(buildEnv(), "20260512", { sleep: noopSleep });
  expect(result.inserted).toBe(1);
  expect(result.retried).toBe(0);
  expect(result.failed).toBe(0);
  expect(result.upserted).toBe(1);
  expect(result.failedRaceKeys).toStrictEqual([]);
});

it("upsertDiscoveredUrls retries a JRA race once and reports retried=1 on transient D1 failure", async () => {
  const { upsertDiscoveredUrls } = await import("./worker");
  const { fetchJraRacesByDate } = await import("./postgres");
  const { upsertJraRaceSource } = await import("./storage");
  vi.mocked(fetchJraRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1500",
      kaisai_kai: "02",
      kaisai_nen: "2026",
      kaisai_nichime: "06",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "JRA",
      race_bango: "1",
    },
  ] as never);
  vi.mocked(upsertJraRaceSource)
    .mockRejectedValueOnce(
      new Error("D1_ERROR: Internal error in D1 DB storage caused object to be reset"),
    )
    .mockResolvedValueOnce(undefined);
  const result = await upsertDiscoveredUrls(buildEnv(), "20260512", { sleep: noopSleep });
  expect(result.inserted).toBe(0);
  expect(result.retried).toBe(1);
  expect(result.failed).toBe(0);
  expect(result.upserted).toBe(1);
  expect(noopSleep).toHaveBeenCalledTimes(1);
});

it("upsertDiscoveredUrls marks a JRA race failed=1 and continues after 3 transient throws", async () => {
  const { upsertDiscoveredUrls } = await import("./worker");
  const { fetchJraRacesByDate } = await import("./postgres");
  const { upsertJraRaceSource } = await import("./storage");
  vi.mocked(fetchJraRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1500",
      kaisai_kai: "02",
      kaisai_nen: "2026",
      kaisai_nichime: "06",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "JRA Failing",
      race_bango: "1",
    },
    {
      hasso_jikoku: "1530",
      kaisai_kai: "02",
      kaisai_nen: "2026",
      kaisai_nichime: "06",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "JRA Healthy",
      race_bango: "2",
    },
  ] as never);
  vi.mocked(upsertJraRaceSource)
    .mockRejectedValueOnce(new Error("Idle connection closed"))
    .mockRejectedValueOnce(new Error("Idle connection closed"))
    .mockRejectedValueOnce(new Error("Idle connection closed"))
    .mockResolvedValueOnce(undefined);
  const result = await upsertDiscoveredUrls(buildEnv(), "20260512", { sleep: noopSleep });
  expect(result.inserted).toBe(1);
  expect(result.retried).toBe(0);
  expect(result.failed).toBe(1);
  expect(result.upserted).toBe(1);
  expect(result.failedRaceKeys).toStrictEqual(["jra:2026:0512:08:01"]);
});

it("upsertDiscoveredUrls reports inserted=1 for a NAR race-list link with healthy upsert", async () => {
  const { upsertDiscoveredUrls } = await import("./worker");
  const { fetchNarRacesByDate } = await import("./postgres");
  const { fetchRacePage, fetchRaceLinksFromRaceList, fetchTodayRaceListUrls } =
    await import("./keiba-go");
  vi.mocked(fetchNarRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1300",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "30",
      kyosomei_hondai: "NAR Local",
      race_bango: "1",
    },
  ] as never);
  vi.mocked(fetchTodayRaceListUrls).mockResolvedValueOnce([
    { babaCode: "36", url: "https://nankan.example/race-list" },
  ] as never);
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValueOnce([
    {
      babaCode: "36",
      raceNumber: "1",
      url: "https://nankan.example/race?race_id=1",
    },
  ] as never);
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  const result = await upsertDiscoveredUrls(buildEnv(), "20260512", { sleep: noopSleep });
  expect(result.inserted).toBe(1);
  expect(result.failed).toBe(0);
  expect(result.failedRaceKeys).toStrictEqual([]);
});

it("upsertDiscoveredUrls reports failed NAR race key after 3 throws while preserving other counters", async () => {
  const { upsertDiscoveredUrls } = await import("./worker");
  const { fetchNarRacesByDate } = await import("./postgres");
  const { fetchRacePage, fetchRaceLinksFromRaceList, fetchTodayRaceListUrls } =
    await import("./keiba-go");
  const { upsertNarRaceSource } = await import("./storage");
  vi.mocked(fetchNarRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1300",
      kaisai_nen: "2026",
      kaisai_tsukihi: "0512",
      keibajo_code: "30",
      kyosomei_hondai: "NAR Local",
      race_bango: "1",
    },
  ] as never);
  vi.mocked(fetchTodayRaceListUrls).mockResolvedValueOnce([
    { babaCode: "36", url: "https://nankan.example/race-list" },
  ] as never);
  vi.mocked(fetchRaceLinksFromRaceList).mockResolvedValueOnce([
    {
      babaCode: "36",
      raceNumber: "1",
      url: "https://nankan.example/race?race_id=1",
    },
  ] as never);
  vi.mocked(fetchRacePage).mockResolvedValue("<html></html>");
  vi.mocked(upsertNarRaceSource)
    .mockRejectedValueOnce(new Error("D1_ERROR"))
    .mockRejectedValueOnce(new Error("D1_ERROR"))
    .mockRejectedValueOnce(new Error("D1_ERROR"));
  const result = await upsertDiscoveredUrls(buildEnv(), "20260512", { sleep: noopSleep });
  expect(result.failed).toBe(1);
  expect(result.failedRaceKeys).toStrictEqual(["nar:2026:0512:30:01"]);
  expect(result.upserted).toBe(0);
});

it("upsertDiscoveredUrls skips a JRA race that buildJraEntryUrlFromRace cannot resolve", async () => {
  const { upsertDiscoveredUrls } = await import("./worker");
  const { fetchJraRacesByDate } = await import("./postgres");
  vi.mocked(fetchJraRacesByDate).mockResolvedValueOnce([
    {
      hasso_jikoku: "1500",
      kaisai_nen: "2026",
      kaisai_nichime: "06",
      kaisai_tsukihi: "0512",
      keibajo_code: "08",
      kyosomei_hondai: "JRA No Kai",
      race_bango: "1",
    },
  ] as never);
  const result = await upsertDiscoveredUrls(buildEnv(), "20260512", { sleep: noopSleep });
  expect(result.inserted).toBe(0);
  expect(result.failed).toBe(0);
});
