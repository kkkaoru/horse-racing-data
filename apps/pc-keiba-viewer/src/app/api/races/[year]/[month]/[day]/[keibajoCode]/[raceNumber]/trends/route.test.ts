// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  buildPast14WindowForTargetMock: vi.fn<(...args: never[]) => unknown>(),
  fetchProductionApiMock: vi.fn<(...args: never[]) => unknown>(),
  fetchRaceTrendDailyTrackMock: vi.fn<(...args: never[]) => unknown>(),
  getCachedRaceTrendResponseMock: vi.fn<(...args: never[]) => unknown>(),
  getCloudflareContextMock: vi.fn<() => Promise<unknown>>(),
  getRaceDetailMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceRunnersMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceRunningStylesWithCacheMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceSourceByRouteMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceTrendPast14StarterRowsMock: vi.fn<(...args: never[]) => unknown>(),
  getRaceTrendRunningStylesFromD1Mock: vi.fn<(...args: never[]) => unknown>(),
  getRaceTrendTodayRunningStylesFromD1Mock: vi.fn<(...args: never[]) => unknown>(),
  getRaceTrendTodayStarterRowsMock: vi.fn<(...args: never[]) => unknown>(),
  notifyRaceTrendRoomMock: vi.fn<(...args: never[]) => unknown>(),
  putRaceTrendCacheMock: vi.fn<(...args: never[]) => unknown>(),
  useProductionApiProxyMock: vi.fn<() => boolean>(),
}));

vi.mock("@opennextjs/cloudflare", () => ({
  getCloudflareContext: mocks.getCloudflareContextMock,
}));

vi.mock("../../../../../../../../../db/d1-trend-queries.server", () => ({
  buildPast14WindowForTarget: mocks.buildPast14WindowForTargetMock,
  getRaceTrendPast14StarterRows: mocks.getRaceTrendPast14StarterRowsMock,
  getRaceTrendRunningStylesFromD1: mocks.getRaceTrendRunningStylesFromD1Mock,
  getRaceTrendTodayRunningStylesFromD1: mocks.getRaceTrendTodayRunningStylesFromD1Mock,
  getRaceTrendTodayStarterRows: mocks.getRaceTrendTodayStarterRowsMock,
}));

vi.mock("../../../../../../../../../db/queries", () => ({
  getRaceDetail: mocks.getRaceDetailMock,
  getRaceRunners: mocks.getRaceRunnersMock,
  getRaceSourceByRoute: mocks.getRaceSourceByRouteMock,
}));

vi.mock("../../../../../../../../../lib/production-api-proxy.server", () => ({
  fetchProductionApi: mocks.fetchProductionApiMock,
  useProductionApiProxy: mocks.useProductionApiProxyMock,
}));

vi.mock("../../../../../../../../../lib/race-trend-cache.server", () => ({
  buildRaceTrendCacheKeyForRequest: vi.fn<() => string>(() => "test-cache-key"),
  getCachedRaceTrendResponse: mocks.getCachedRaceTrendResponseMock,
  putRaceTrendCache: mocks.putRaceTrendCacheMock,
}));

vi.mock("../../../../../../../../../lib/race-trend-daily-track-client.server", () => ({
  fetchRaceTrendDailyTrack: mocks.fetchRaceTrendDailyTrackMock,
}));

vi.mock("../../../../../../../../../lib/race-trend-room.server", () => ({
  notifyRaceTrendRoom: mocks.notifyRaceTrendRoomMock,
}));

vi.mock("../../../../../../../../../lib/running-style-cache.server", () => ({
  getRaceRunningStylesWithCache: mocks.getRaceRunningStylesWithCacheMock,
}));

const {
  buildPast14WindowForTargetMock,
  fetchProductionApiMock,
  fetchRaceTrendDailyTrackMock,
  getCachedRaceTrendResponseMock,
  getCloudflareContextMock,
  getRaceDetailMock,
  getRaceRunnersMock,
  getRaceRunningStylesWithCacheMock,
  getRaceSourceByRouteMock,
  getRaceTrendPast14StarterRowsMock,
  getRaceTrendRunningStylesFromD1Mock,
  getRaceTrendTodayRunningStylesFromD1Mock,
  getRaceTrendTodayStarterRowsMock,
  notifyRaceTrendRoomMock,
  putRaceTrendCacheMock,
  useProductionApiProxyMock,
} = mocks;

import type {
  RaceTrendDailyTrackRow,
  RaceTrendStarterRow,
} from "horse-racing-realtime/race-trend-daily-track-types";

import type { RaceTrendDailyTrackFetchResult } from "../../../../../../../../../lib/race-trend-daily-track-client.server";
import type {
  RaceDetail,
  RaceTrendRawPayload,
  Runner,
} from "../../../../../../../../../lib/race-types";
import { GET, isCacheableTrendPayload, pickTodaySiblingRowsAndSource } from "./route";

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

const buildPayload = (overrides: Partial<RaceTrendRawPayload> = {}): RaceTrendRawPayload => ({
  currentRunningStyles: [],
  historicalRunningStyles: [],
  raceContext: { keibajoCode: "42", raceBango: "01", source: "nar" },
  runners: [],
  starterRows: [],
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

const buildTrendRequest = (): Request =>
  new Request("https://example.com/api/races/2026/05/29/05/07/trends?source=jra");

const buildTrendContext = () => ({
  params: Promise.resolve({
    day: "29",
    keibajoCode: "05",
    month: "05",
    raceNumber: "07",
    year: "2026",
  }),
});

// Narrow a JSON response body into a RaceTrendRawPayload using a runtime
// property check so the test reader code stays type-safe without `as`.
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isRaceTrendRawPayload = (value: unknown): value is RaceTrendRawPayload => {
  if (!isRecord(value)) return false;
  return (
    Array.isArray(value.starterRows) &&
    Array.isArray(value.runners) &&
    Array.isArray(value.currentRunningStyles) &&
    Array.isArray(value.historicalRunningStyles) &&
    isRecord(value.raceContext)
  );
};

const readJsonAsPayload = async (response: Response): Promise<RaceTrendRawPayload> => {
  const body: unknown = await response.json();
  if (!isRaceTrendRawPayload(body)) {
    throw new Error("response body is not a RaceTrendRawPayload");
  }
  return body;
};

beforeEach(() => {
  buildPast14WindowForTargetMock.mockReset();
  fetchProductionApiMock.mockReset();
  fetchRaceTrendDailyTrackMock.mockReset();
  getCachedRaceTrendResponseMock.mockReset();
  getCloudflareContextMock.mockReset();
  getRaceDetailMock.mockReset();
  getRaceRunnersMock.mockReset();
  getRaceRunningStylesWithCacheMock.mockReset();
  getRaceSourceByRouteMock.mockReset();
  getRaceTrendPast14StarterRowsMock.mockReset();
  getRaceTrendRunningStylesFromD1Mock.mockReset();
  getRaceTrendTodayRunningStylesFromD1Mock.mockReset();
  getRaceTrendTodayStarterRowsMock.mockReset();
  notifyRaceTrendRoomMock.mockReset();
  putRaceTrendCacheMock.mockReset();
  useProductionApiProxyMock.mockReset();
  useProductionApiProxyMock.mockReturnValue(false);
  buildPast14WindowForTargetMock.mockReturnValue({ endYmd: "20260528", startYmd: "20260515" });
  getRaceRunningStylesWithCacheMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([]);
  getRaceTrendTodayRunningStylesFromD1Mock.mockResolvedValue([]);
  getCachedRaceTrendResponseMock.mockResolvedValue(null);
  putRaceTrendCacheMock.mockResolvedValue(undefined);
  notifyRaceTrendRoomMock.mockResolvedValue(true);
  getRaceRunnersMock.mockResolvedValue([buildRunner()]);
});

it("isCacheableTrendPayload rejects a payload with neither starter rows nor running-style history", () => {
  expect(isCacheableTrendPayload(buildPayload())).toBe(false);
});

it("isCacheableTrendPayload rejects a payload with starter rows but empty running-style history", () => {
  expect(
    isCacheableTrendPayload(
      buildPayload({
        starterRows: [buildStarterRow()],
        historicalRunningStyles: [],
      }),
    ),
  ).toBe(false);
});

it("isCacheableTrendPayload rejects a payload with running-style history but no starter rows", () => {
  expect(
    isCacheableTrendPayload(
      buildPayload({
        starterRows: [],
        historicalRunningStyles: [
          { horseNumber: "1", predictedLabel: "nige", raceKey: "nar:2026:0524:47:01" },
        ],
      }),
    ),
  ).toBe(false);
});

it("isCacheableTrendPayload accepts a payload with both starter rows and running-style history", () => {
  expect(
    isCacheableTrendPayload(
      buildPayload({
        starterRows: [buildStarterRow()],
        historicalRunningStyles: [
          { horseNumber: "1", predictedLabel: "nige", raceKey: "nar:2026:0524:47:01" },
        ],
      }),
    ),
  ).toBe(true);
});

it("pickTodaySiblingRowsAndSource returns DO rows and do-hit header when DO result status is hit", () => {
  const doRow = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    umaban: "03",
  });
  const fallbackRow = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "05",
    raceBango: "02",
    source: "jra",
    umaban: "07",
  });
  const result: RaceTrendDailyTrackFetchResult = {
    rows: [buildDailyTrackRow("01", [doRow])],
    status: "hit",
  };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [fallbackRow], result })).toStrictEqual({
    rows: [doRow],
    sourceHeader: "do-hit",
  });
});

it("pickTodaySiblingRowsAndSource flattens starterRows across multiple DO race rows when status is hit", () => {
  const doRowA = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "05",
    raceBango: "01",
    source: "jra",
    umaban: "01",
  });
  const doRowB = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "05",
    raceBango: "02",
    source: "jra",
    umaban: "02",
  });
  const result: RaceTrendDailyTrackFetchResult = {
    rows: [buildDailyTrackRow("01", [doRowA]), buildDailyTrackRow("02", [doRowB])],
    status: "hit",
  };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [], result })).toStrictEqual({
    rows: [doRowA, doRowB],
    sourceHeader: "do-hit",
  });
});

it("pickTodaySiblingRowsAndSource falls back to legacy rows with do-miss-fallback header when DO result status is miss", () => {
  const fallbackRow = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "47",
    raceBango: "04",
    source: "nar",
    umaban: "05",
  });
  const result: RaceTrendDailyTrackFetchResult = { rows: [], status: "miss" };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [fallbackRow], result })).toStrictEqual({
    rows: [fallbackRow],
    sourceHeader: "do-miss-fallback",
  });
});

it("pickTodaySiblingRowsAndSource falls back to legacy rows with do-error-fallback header when DO result status is error", () => {
  const fallbackRow = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    keibajoCode: "47",
    raceBango: "06",
    source: "nar",
    umaban: "08",
  });
  const result: RaceTrendDailyTrackFetchResult = { rows: [], status: "error" };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [fallbackRow], result })).toStrictEqual({
    rows: [fallbackRow],
    sourceHeader: "do-error-fallback",
  });
});

it("pickTodaySiblingRowsAndSource returns an empty rows array when both DO is miss and fallback is empty", () => {
  const result: RaceTrendDailyTrackFetchResult = { rows: [], status: "miss" };
  expect(pickTodaySiblingRowsAndSource({ fallbackRows: [], result })).toStrictEqual({
    rows: [],
    sourceHeader: "do-miss-fallback",
  });
});

it("GET proxies to production when useProductionApiProxy returns true", async () => {
  useProductionApiProxyMock.mockReturnValue(true);
  fetchProductionApiMock.mockResolvedValue(
    new Response("upstream-body", {
      headers: {
        "Cache-Control": "public, max-age=120",
        "Content-Type": "text/plain",
        "X-Race-Trend-Cache": "PROXIED",
      },
      status: 200,
    }),
  );
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(await response.text()).toBe("upstream-body");
  expect(response.headers.get("Cache-Control")).toBe("public, max-age=120");
  expect(response.headers.get("X-Race-Trend-Cache")).toBe("PROXIED");
});

it("GET proxies to production with default Cache-Control when upstream lacks it", async () => {
  useProductionApiProxyMock.mockReturnValue(true);
  fetchProductionApiMock.mockResolvedValue(
    new Response("upstream-body", {
      headers: { "Content-Type": "application/json" },
      status: 200,
    }),
  );
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.headers.get("Cache-Control")).toBe("public, max-age=60");
  expect(response.headers.get("X-Race-Trend-Cache")).toBe("PROXIED-PRODUCTION");
});

it("GET proxies to production with default Content-Type when upstream lacks it", async () => {
  useProductionApiProxyMock.mockReturnValue(true);
  // Build an upstream response with an empty body + explicitly removed
  // Content-Type so the route falls through to its default value.
  const upstream = new Response("upstream-body", { status: 200 });
  upstream.headers.delete("Content-Type");
  fetchProductionApiMock.mockResolvedValue(upstream);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.headers.get("Content-Type")).toBe("application/json; charset=utf-8");
});

it("GET returns 404 when getRaceSourceByRoute resolves to null and no source param", async () => {
  const request = new Request("https://example.com/api/races/2026/05/29/05/07/trends");
  getRaceSourceByRouteMock.mockResolvedValue(null);
  const response = await GET(request, buildTrendContext());
  expect(response.status).toBe(404);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "race source not found" });
});

it("GET resolves source via getRaceSourceByRoute when source param is missing", async () => {
  const request = new Request("https://example.com/api/races/2026/05/29/05/07/trends");
  getRaceSourceByRouteMock.mockResolvedValue("jra");
  getRaceDetailMock.mockResolvedValue(null);
  const response = await GET(request, buildTrendContext());
  expect(response.status).toBe(404);
  expect(getRaceSourceByRouteMock).toHaveBeenCalledTimes(1);
});

it("GET returns 404 when getRaceDetail returns null", async () => {
  getRaceDetailMock.mockResolvedValue(null);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(404);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ error: "race not found" });
});

it("GET returns the cached response when getCachedRaceTrendResponse returns a hit", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  const cached = new Response("cached-body", {
    headers: { "X-Race-Trend-Cache": "HIT-memory" },
    status: 200,
  });
  getCachedRaceTrendResponseMock.mockResolvedValue(cached);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(await response.text()).toBe("cached-body");
});

it("GET skips cache lookup when __trendCacheWarm=1", async () => {
  const request = new Request(
    "https://example.com/api/races/2026/05/29/05/07/trends?source=jra&__trendCacheWarm=1",
  );
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(request, buildTrendContext());
  expect(getCachedRaceTrendResponseMock).not.toHaveBeenCalled();
  expect(response.headers.get("X-Race-Trend-Cache")).toBe("MISS-STORED-WARM");
});

it("GET skips cache lookup when __trendCacheRefresh=1", async () => {
  const request = new Request(
    "https://example.com/api/races/2026/05/29/05/07/trends?source=jra&__trendCacheRefresh=1",
  );
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(request, buildTrendContext());
  expect(getCachedRaceTrendResponseMock).not.toHaveBeenCalled();
  expect(response.headers.get("X-Race-Trend-Cache")).toBe("MISS-STORED-REFRESH");
});

it("GET merges today rows over past14 with newer-wins so today fields beat past14 stale fields", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  const past14Row = buildStarterRow({
    bamei: "OldName",
    finishPosition: 0,
    jockeyName: "OldJockey",
    raceBango: "03",
    sohaTime: "1234",
    umaban: "05",
  });
  const todayRow = buildStarterRow({
    bamei: "NewName",
    finishPosition: 2,
    jockeyName: "NewJockey",
    raceBango: "03",
    sohaTime: "1100",
    umaban: "05",
  });
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([past14Row]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({
    rows: [buildDailyTrackRow("03", [todayRow])],
    status: "hit",
  });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "5", predictedLabel: "sashi", raceKey: "jra:2026:0529:05:03" },
  ]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  const body = await readJsonAsPayload(response);
  expect(body.starterRows.length).toBe(1);
  const mergedRow = body.starterRows[0];
  expect(mergedRow?.bamei).toBe("NewName");
  expect(mergedRow?.jockeyName).toBe("NewJockey");
  expect(mergedRow?.sohaTime).toBe("1100");
  expect(mergedRow?.finishPosition).toBe(2);
});

it("GET drops DO sibling rows that fail defense-in-depth filter when raceBango is empty", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  const okSibling = buildStarterRow({
    raceBango: "03",
    source: "jra",
    umaban: "05",
  });
  const emptyRaceBangoRow = buildStarterRow({
    raceBango: "",
    source: "jra",
    umaban: "06",
  });
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({
    rows: [buildDailyTrackRow("03", [okSibling, emptyRaceBangoRow])],
    status: "hit",
  });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([]);
  getRaceTrendTodayRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "5", predictedLabel: "sashi", raceKey: "jra:2026:0529:05:03" },
  ]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  const body = await readJsonAsPayload(response);
  expect(body.starterRows.length).toBe(1);
  expect(body.starterRows[0]?.raceBango).toBe("03");
});

it("GET drops DO rows from a stale day via the defense-in-depth filter", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  const staleDayRow = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0528",
    raceBango: "03",
    source: "jra",
    umaban: "05",
  });
  const todayRow = buildStarterRow({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0529",
    raceBango: "03",
    source: "jra",
    umaban: "06",
  });
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({
    rows: [buildDailyTrackRow("03", [staleDayRow, todayRow])],
    status: "hit",
  });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([]);
  getRaceTrendTodayRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "6", predictedLabel: "sashi", raceKey: "jra:2026:0529:05:03" },
  ]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  const body = await readJsonAsPayload(response);
  expect(body.starterRows.length).toBe(1);
  expect(body.starterRows[0]?.umaban).toBe("06");
});

it("GET drops DO rows from a different venue via the defense-in-depth filter", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  const otherVenueRow = buildStarterRow({
    keibajoCode: "06",
    raceBango: "03",
    source: "jra",
    umaban: "05",
  });
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({
    rows: [buildDailyTrackRow("03", [otherVenueRow])],
    status: "hit",
  });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  const body = await readJsonAsPayload(response);
  expect(body.starterRows).toStrictEqual([]);
});

it("GET payload still returns when past14 promise rejects", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockRejectedValue(new Error("past14 boom"));
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  const body = await readJsonAsPayload(response);
  expect(body.starterRows).toStrictEqual([]);
});

it("GET payload still returns when DO promise rejects", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([buildStarterRow({ raceBango: "01" })]);
  fetchRaceTrendDailyTrackMock.mockRejectedValue(new Error("do boom"));
  const todayRow = buildStarterRow({ raceBango: "02", umaban: "08" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([todayRow]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  const body = await readJsonAsPayload(response);
  expect(response.headers.get("X-Race-Trend-Source")).toBe("do-error-fallback");
  expect(body.starterRows.length).toBe(2);
});

it("GET payload populates currentRunningStyles from getRaceRunningStylesWithCache rows", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceRunningStylesWithCacheMock.mockResolvedValue([
    { horseNumber: 3, predictedLabel: "sashi" },
    { horseNumber: 7, predictedLabel: "nige" },
  ]);
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  const body = await readJsonAsPayload(response);
  expect(body.currentRunningStyles).toStrictEqual([
    { horseNumber: "3", predictedLabel: "sashi" },
    { horseNumber: "7", predictedLabel: "nige" },
  ]);
});

it("GET dedupes historicalRunningStyles when past14 and today return overlapping race/horse keys", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([buildStarterRow({ raceBango: "01" })]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0529:05:01" },
    { horseNumber: "2", predictedLabel: "sashi", raceKey: "jra:2026:0529:05:01" },
  ]);
  getRaceTrendTodayRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "1", predictedLabel: "oikomi", raceKey: "jra:2026:0529:05:01" },
  ]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  const body = await readJsonAsPayload(response);
  expect(body.historicalRunningStyles).toStrictEqual([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0529:05:01" },
    { horseNumber: "2", predictedLabel: "sashi", raceKey: "jra:2026:0529:05:01" },
  ]);
});

it("GET runners array maps wakuban/umaban/kishumeiRyakusho into the payload", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceRunnersMock.mockResolvedValue([
    buildRunner({ kishumeiRyakusho: "山田太郎", umaban: "01", wakuban: "1" }),
    buildRunner({ kishumeiRyakusho: "鈴木花子", umaban: "08", wakuban: "5" }),
  ]);
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  const body = await readJsonAsPayload(response);
  expect(body.runners).toStrictEqual([
    { frameNumber: "1", horseNumber: "01", jockeyName: "山田太郎", trainerName: null },
    { frameNumber: "5", horseNumber: "08", jockeyName: "鈴木花子", trainerName: null },
  ]);
});

it("GET payload still returns when currentRunningStyles promise rejects", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceRunningStylesWithCacheMock.mockRejectedValue(new Error("running-style boom"));
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  const body = await readJsonAsPayload(response);
  expect(body.currentRunningStyles).toStrictEqual([]);
});

it("GET payload still returns when legacyToday promise rejects", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([buildStarterRow({ raceBango: "01" })]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockRejectedValue(new Error("today boom"));
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Race-Trend-Source")).toBe("do-miss-fallback");
});

it("GET writes cache when payload has both starterRows and historicalRunningStyles", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([buildStarterRow({ raceBango: "01" })]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0529:05:01" },
  ]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(putRaceTrendCacheMock).toHaveBeenCalledTimes(1);
  expect(notifyRaceTrendRoomMock).toHaveBeenCalledTimes(1);
  expect(response.headers.get("X-Race-Trend-Cache")).toBe("MISS-STORED");
});

it("GET skips cache write when notifyRaceTrendRoom rejects but still returns 200", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([buildStarterRow({ raceBango: "01" })]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0529:05:01" },
  ]);
  notifyRaceTrendRoomMock.mockRejectedValue(new Error("notify boom"));
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
});

it("GET uses MISS-STORED-WARM when warm flag is set and payload is cacheable", async () => {
  const request = new Request(
    "https://example.com/api/races/2026/05/29/05/07/trends?source=jra&__trendCacheWarm=1",
  );
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([buildStarterRow({ raceBango: "01" })]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0529:05:01" },
  ]);
  const response = await GET(request, buildTrendContext());
  expect(response.headers.get("X-Race-Trend-Cache")).toBe("MISS-STORED-WARM");
});

it("GET applies non-default date / frame / jockey query params", async () => {
  const request = new Request(
    "https://example.com/api/races/2026/05/29/05/07/trends?source=jra&jockeyStart=20260101&jockeyEnd=20260201&frameStart=20260102&frameEnd=20260202&includeRealtimeResults=false",
  );
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(request, buildTrendContext());
  expect(response.status).toBe(200);
  expect(response.headers.get("X-Race-Trend-Cache")).toBe("MISS-EMPTY-SKIPPED");
});

it("GET parses dashed jockeyStart query param via parseDateInput", async () => {
  const request = new Request(
    "https://example.com/api/races/2026/05/29/05/07/trends?source=jra&jockeyStart=2026-01-01",
  );
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(request, buildTrendContext());
  expect(response.status).toBe(200);
});

it("GET sets X-Race-Trend-Source header to do-hit when DO returns rows", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  const todayRow = buildStarterRow({ raceBango: "03", umaban: "07" });
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({
    rows: [buildDailyTrackRow("03", [todayRow])],
    status: "hit",
  });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.headers.get("X-Race-Trend-Source")).toBe("do-hit");
});

it("GET surfaces source query param unchanged when valid", async () => {
  const request = new Request("https://example.com/api/races/2026/05/29/05/07/trends?source=nar");
  getRaceDetailMock.mockResolvedValue(buildRaceDetail({ source: "nar" }));
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(request, buildTrendContext());
  expect(response.status).toBe(200);
  expect(getRaceSourceByRouteMock).not.toHaveBeenCalled();
});

it("GET skips getRaceTrendTodayStarterRows when DO result status is hit", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  const doRow = buildStarterRow({ raceBango: "03", umaban: "07" });
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({
    rows: [buildDailyTrackRow("03", [doRow])],
    status: "hit",
  });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(getRaceTrendTodayStarterRowsMock).not.toHaveBeenCalled();
});

it("GET calls getRaceTrendTodayStarterRows when DO result status is miss", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(getRaceTrendTodayStarterRowsMock).toHaveBeenCalledTimes(1);
});

it("GET calls getRaceTrendTodayStarterRows when DO result status is error", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "error" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(getRaceTrendTodayStarterRowsMock).toHaveBeenCalledTimes(1);
});

it("GET calls getRaceTrendTodayStarterRows when DO promise rejects (do-error-fallback)", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockRejectedValue(new Error("do boom"));
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(getRaceTrendTodayStarterRowsMock).toHaveBeenCalledTimes(1);
});

it("GET defers KV cache write through ctx.waitUntil when Cloudflare ctx is available", async () => {
  const waitUntilMock = vi.fn<(promise: Promise<unknown>) => void>();
  getCloudflareContextMock.mockResolvedValue({ ctx: { waitUntil: waitUntilMock }, env: null });
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([buildStarterRow({ raceBango: "01" })]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0529:05:01" },
  ]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(putRaceTrendCacheMock).toHaveBeenCalledTimes(1);
  expect(waitUntilMock).toHaveBeenCalledTimes(1);
  const deferredPromise = waitUntilMock.mock.calls[0]?.[0];
  expect(deferredPromise).toBeInstanceOf(Promise);
});

it("GET still returns 200 when putRaceTrendCache rejects with no Cloudflare ctx", async () => {
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([buildStarterRow({ raceBango: "01" })]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0529:05:01" },
  ]);
  putRaceTrendCacheMock.mockRejectedValue(new Error("kv 429"));
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(putRaceTrendCacheMock).toHaveBeenCalledTimes(1);
  expect(response.headers.get("X-Race-Trend-Cache")).toBe("MISS-STORED");
});

it("GET still returns 200 when putRaceTrendCache rejects via ctx.waitUntil deferral", async () => {
  const waitUntilMock = vi.fn<(promise: Promise<unknown>) => void>();
  getCloudflareContextMock.mockResolvedValue({ ctx: { waitUntil: waitUntilMock }, env: null });
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([buildStarterRow({ raceBango: "01" })]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  getRaceTrendRunningStylesFromD1Mock.mockResolvedValue([
    { horseNumber: "1", predictedLabel: "nige", raceKey: "jra:2026:0529:05:01" },
  ]);
  putRaceTrendCacheMock.mockRejectedValue(new Error("kv 429"));
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(waitUntilMock).toHaveBeenCalledTimes(1);
});

it("GET does not invoke ctx.waitUntil when payload is not cacheable", async () => {
  const waitUntilMock = vi.fn<(promise: Promise<unknown>) => void>();
  getCloudflareContextMock.mockResolvedValue({ ctx: { waitUntil: waitUntilMock }, env: null });
  getRaceDetailMock.mockResolvedValue(buildRaceDetail());
  getRaceTrendPast14StarterRowsMock.mockResolvedValue([]);
  fetchRaceTrendDailyTrackMock.mockResolvedValue({ rows: [], status: "miss" });
  getRaceTrendTodayStarterRowsMock.mockResolvedValue([]);
  const response = await GET(buildTrendRequest(), buildTrendContext());
  expect(response.status).toBe(200);
  expect(putRaceTrendCacheMock).not.toHaveBeenCalled();
  expect(waitUntilMock).not.toHaveBeenCalled();
  expect(response.headers.get("X-Race-Trend-Cache")).toBe("MISS-EMPTY-SKIPPED");
});
