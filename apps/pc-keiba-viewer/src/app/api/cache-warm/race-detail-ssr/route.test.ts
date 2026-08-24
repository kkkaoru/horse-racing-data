// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

import type { RaceListItem } from "../../../../lib/race-types";

vi.mock("server-only", () => ({}));

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const mocks = vi.hoisted(() => ({
  getHorseRaceResultsMock: vi.fn<(...args: never[]) => Promise<unknown[]>>(),
  getRaceCourseInfoMock: vi.fn<(...args: never[]) => Promise<unknown>>(),
  getRaceDetailMock: vi.fn<(...args: never[]) => Promise<unknown>>(),
  getRaceRunnersMock: vi.fn<(...args: never[]) => Promise<unknown[]>>(),
  getRaceSourceByRouteMock:
    vi.fn<
      (
        year: string,
        month: string,
        day: string,
        keibajoCode: string,
        raceNumber: string,
      ) => Promise<string | null>
    >(),
  getRacesByDateMock: vi.fn<(year: string, month: string, day: string) => Promise<unknown[]>>(),
  getSameVenueRacesByDateMock: vi.fn<(...args: never[]) => Promise<unknown[]>>(),
  putRaceDetailSsrSnapshotMock: vi.fn<(input: unknown) => Promise<void>>(),
  putRecentResultsCacheMock: vi.fn<(key: string, value: string) => Promise<void>>(),
  safeGetCloudflareEnvMock: vi.fn<() => Promise<unknown>>(),
}));

vi.mock("../../../../db/queries", () => ({
  getHorseRaceResults: mocks.getHorseRaceResultsMock,
  getRaceCourseInfo: mocks.getRaceCourseInfoMock,
  getRaceDetail: mocks.getRaceDetailMock,
  getRaceRunners: mocks.getRaceRunnersMock,
  getRaceSourceByRoute: mocks.getRaceSourceByRouteMock,
  getRacesByDate: mocks.getRacesByDateMock,
  getSameVenueRacesByDate: mocks.getSameVenueRacesByDateMock,
}));

vi.mock("../../../../lib/race-detail-ssr-cache.server", () => ({
  buildRaceDetailSsrCacheKey: (input: { keibajoCode: string; raceNumber: string }): string =>
    `ssr-${input.keibajoCode}-${input.raceNumber}`,
  putRaceDetailSsrSnapshot: mocks.putRaceDetailSsrSnapshotMock,
}));

vi.mock("../../../../lib/recent-results-cache.server", () => ({
  buildRecentResultsCacheKey: (input: { keibajoCode: string; raceNumber: string }): string =>
    `recent-${input.keibajoCode}-${input.raceNumber}`,
  putRecentResultsCache: mocks.putRecentResultsCacheMock,
}));

vi.mock("../../../../lib/cloudflare-context.server", () => ({
  safeGetCloudflareEnv: mocks.safeGetCloudflareEnvMock,
}));

const {
  getHorseRaceResultsMock,
  getRaceCourseInfoMock,
  getRaceDetailMock,
  getRaceRunnersMock,
  getRaceSourceByRouteMock,
  getRacesByDateMock,
  getSameVenueRacesByDateMock,
  putRaceDetailSsrSnapshotMock,
  putRecentResultsCacheMock,
  safeGetCloudflareEnvMock,
} = mocks;

import { POST } from "./route";

const ENDPOINT_URL = "https://example.com/api/cache-warm/race-detail-ssr";

const buildAuthedRequest = (search: string): Request =>
  new Request(`${ENDPOINT_URL}${search}`, {
    headers: { "X-PC-Keiba-Cache-Warm": "scheduled" },
    method: "POST",
  });

const buildUnauthedRequest = (): Request => new Request(ENDPOINT_URL, { method: "POST" });

const buildJraRow = (overrides: Partial<RaceListItem> = {}): RaceListItem => ({
  gradeCode: null,
  hassoJikoku: null,
  jockeyNames: ["Yamada"],
  juryoShubetsuCode: null,
  kaisaiNen: "2026",
  kaisaiTsukihi: "0529",
  keibajoCode: "05",
  kyori: "1200",
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosoShubetsuCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  raceBango: "01",
  shussoTosu: null,
  source: "jra",
  trackCode: null,
  ...overrides,
});

const readJsonRecord = async (response: Response): Promise<Record<string, unknown>> => {
  const body: unknown = await response.json();
  if (!isRecord(body)) {
    throw new Error("response body is not an object");
  }
  return body;
};

beforeEach(() => {
  getHorseRaceResultsMock.mockReset();
  getRaceCourseInfoMock.mockReset();
  getRaceDetailMock.mockReset();
  getRaceRunnersMock.mockReset();
  getRaceSourceByRouteMock.mockReset();
  getRacesByDateMock.mockReset();
  getSameVenueRacesByDateMock.mockReset();
  putRaceDetailSsrSnapshotMock.mockReset();
  putRecentResultsCacheMock.mockReset();
  safeGetCloudflareEnvMock.mockReset();
  safeGetCloudflareEnvMock.mockResolvedValue(null);
});

it("POST returns 404 when neither the cache-warm header nor debug query is present", async () => {
  const response = await POST(buildUnauthedRequest());
  expect(response.status).toBe(404);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ error: "not_found" });
  expect(getRacesByDateMock).not.toHaveBeenCalled();
});

it("POST queries getRacesByDate with the parsed date parts from the date query", async () => {
  getRacesByDateMock.mockResolvedValue([]);
  const response = await POST(buildAuthedRequest("?date=2026-05-29"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-05-29", raceCount: 0, warmed: 0 });
  expect(getRacesByDateMock).toHaveBeenCalledWith("2026", "05", "29");
});

it("POST warms only the matching venue+race when keibajo and race are set", async () => {
  getRaceSourceByRouteMock.mockResolvedValue("jra");
  getRaceDetailMock.mockResolvedValue({ kyori: "1200", trackCode: "10" });
  getRaceCourseInfoMock.mockResolvedValue({ courseKaishuNengappi: "20200101", courseSetsumei: "" });
  getRaceRunnersMock.mockResolvedValue([]);
  getSameVenueRacesByDateMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  putRaceDetailSsrSnapshotMock.mockResolvedValue(undefined);
  const response = await POST(buildAuthedRequest("?date=2026-05-29&keibajo=05&race=02"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-05-29", raceCount: 1, warmed: 1 });
  expect(getRacesByDateMock).not.toHaveBeenCalled();
  expect(getRaceSourceByRouteMock).toHaveBeenCalledTimes(1);
  expect(getRaceSourceByRouteMock).toHaveBeenCalledWith("2026", "05", "29", "05", "02");
  expect(getRaceDetailMock).toHaveBeenCalledTimes(1);
  expect(getRaceDetailMock).toHaveBeenCalledWith("jra", "2026", "05", "29", "05", "02");
});

it("POST warms every race at the venue when only keibajo is set", async () => {
  getRacesByDateMock.mockResolvedValue([
    buildJraRow({ keibajoCode: "05", raceBango: "01" }),
    buildJraRow({ keibajoCode: "05", raceBango: "02" }),
    buildJraRow({ keibajoCode: "06", raceBango: "01" }),
  ]);
  getRaceDetailMock.mockResolvedValue({ kyori: "1200", trackCode: "10" });
  getRaceCourseInfoMock.mockResolvedValue({ courseKaishuNengappi: "20200101", courseSetsumei: "" });
  getRaceRunnersMock.mockResolvedValue([]);
  getSameVenueRacesByDateMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  putRaceDetailSsrSnapshotMock.mockResolvedValue(undefined);
  const response = await POST(buildAuthedRequest("?date=2026-05-29&keibajo=05"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-05-29", raceCount: 2, warmed: 2 });
  expect(getRaceDetailMock).toHaveBeenCalledTimes(2);
  expect(getRaceDetailMock).toHaveBeenNthCalledWith(1, "jra", "2026", "05", "29", "05", "01");
  expect(getRaceDetailMock).toHaveBeenNthCalledWith(2, "jra", "2026", "05", "29", "05", "02");
});

it("POST warms matching race numbers across venues when only race is set", async () => {
  getRacesByDateMock.mockResolvedValue([
    buildJraRow({ keibajoCode: "05", raceBango: "01" }),
    buildJraRow({ keibajoCode: "05", raceBango: "02" }),
    buildJraRow({ keibajoCode: "06", raceBango: "01" }),
  ]);
  getRaceDetailMock.mockResolvedValue({ kyori: "1200", trackCode: "10" });
  getRaceCourseInfoMock.mockResolvedValue({ courseKaishuNengappi: "20200101", courseSetsumei: "" });
  getRaceRunnersMock.mockResolvedValue([]);
  getSameVenueRacesByDateMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  putRaceDetailSsrSnapshotMock.mockResolvedValue(undefined);
  const response = await POST(buildAuthedRequest("?date=2026-05-29&race=01"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-05-29", raceCount: 2, warmed: 2 });
  expect(getRaceDetailMock).toHaveBeenCalledTimes(2);
  expect(getRaceDetailMock).toHaveBeenNthCalledWith(1, "jra", "2026", "05", "29", "05", "01");
  expect(getRaceDetailMock).toHaveBeenNthCalledWith(2, "jra", "2026", "05", "29", "06", "01");
});

it("POST reports zero races when keibajo and race match nothing", async () => {
  getRaceSourceByRouteMock.mockResolvedValue(null);
  const response = await POST(buildAuthedRequest("?date=2026-05-29&keibajo=05&race=12"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-05-29", raceCount: 0, warmed: 0 });
  expect(getRacesByDateMock).not.toHaveBeenCalled();
  expect(getRaceSourceByRouteMock).toHaveBeenCalledWith("2026", "05", "29", "05", "12");
  expect(getRaceDetailMock).not.toHaveBeenCalled();
});

it("POST warms Ban-ei 83/01 via getRaceSourceByRoute even when getRacesByDate omits it", async () => {
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  getRaceDetailMock.mockResolvedValue({ kyori: "0200", trackCode: "23" });
  getRaceCourseInfoMock.mockResolvedValue({ courseKaishuNengappi: "20200101", courseSetsumei: "" });
  getRaceRunnersMock.mockResolvedValue([]);
  getSameVenueRacesByDateMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  putRaceDetailSsrSnapshotMock.mockResolvedValue(undefined);
  const response = await POST(buildAuthedRequest("?date=2026-08-17&keibajo=83&race=01"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-08-17", raceCount: 1, warmed: 1 });
  expect(getRacesByDateMock).not.toHaveBeenCalled();
  expect(getRaceSourceByRouteMock).toHaveBeenCalledTimes(1);
  expect(getRaceSourceByRouteMock).toHaveBeenCalledWith("2026", "08", "17", "83", "01");
  expect(getRaceDetailMock).toHaveBeenCalledTimes(1);
  expect(getRaceDetailMock).toHaveBeenCalledWith("nar", "2026", "08", "17", "83", "01");
});

it("POST pads unpadded Ban-ei race=1 to 01 before resolving the race", async () => {
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  getRaceDetailMock.mockResolvedValue({ kyori: "0200", trackCode: "23" });
  getRaceCourseInfoMock.mockResolvedValue({ courseKaishuNengappi: "20200101", courseSetsumei: "" });
  getRaceRunnersMock.mockResolvedValue([]);
  getSameVenueRacesByDateMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  putRaceDetailSsrSnapshotMock.mockResolvedValue(undefined);
  const response = await POST(buildAuthedRequest("?date=2026-08-17&keibajo=83&race=1"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-08-17", raceCount: 1, warmed: 1 });
  expect(getRaceSourceByRouteMock).toHaveBeenCalledWith("2026", "08", "17", "83", "01");
  expect(getRaceDetailMock).toHaveBeenCalledWith("nar", "2026", "08", "17", "83", "01");
});

it("POST warms NAR 35/02 via getRaceSourceByRoute without listing the date", async () => {
  getRaceSourceByRouteMock.mockResolvedValue("nar");
  getRaceDetailMock.mockResolvedValue({ kyori: "1400", trackCode: "24" });
  getRaceCourseInfoMock.mockResolvedValue({ courseKaishuNengappi: "20200101", courseSetsumei: "" });
  getRaceRunnersMock.mockResolvedValue([]);
  getSameVenueRacesByDateMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  putRaceDetailSsrSnapshotMock.mockResolvedValue(undefined);
  const response = await POST(buildAuthedRequest("?date=2026-08-17&keibajo=35&race=02"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-08-17", raceCount: 1, warmed: 1 });
  expect(getRacesByDateMock).not.toHaveBeenCalled();
  expect(getRaceSourceByRouteMock).toHaveBeenCalledWith("2026", "08", "17", "35", "02");
  expect(getRaceDetailMock).toHaveBeenCalledWith("nar", "2026", "08", "17", "35", "02");
});

it("POST pad-matches race=01 when listing returns Ban-ei raceBango 1", async () => {
  getRacesByDateMock.mockResolvedValue([
    buildJraRow({ keibajoCode: "83", raceBango: "1", source: "nar" }),
    buildJraRow({ keibajoCode: "35", raceBango: "02", source: "nar" }),
  ]);
  getRaceDetailMock.mockResolvedValue({ kyori: "0200", trackCode: "23" });
  getRaceCourseInfoMock.mockResolvedValue({ courseKaishuNengappi: "20200101", courseSetsumei: "" });
  getRaceRunnersMock.mockResolvedValue([]);
  getSameVenueRacesByDateMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  putRaceDetailSsrSnapshotMock.mockResolvedValue(undefined);
  const response = await POST(buildAuthedRequest("?date=2026-08-17&race=01"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-08-17", raceCount: 1, warmed: 1 });
  expect(getRaceSourceByRouteMock).not.toHaveBeenCalled();
  expect(getRaceDetailMock).toHaveBeenCalledTimes(1);
  expect(getRaceDetailMock).toHaveBeenCalledWith("nar", "2026", "08", "17", "83", "01");
});

it("POST pad-matches Ban-ei 83 when listing returns unpadded raceBango 1", async () => {
  getRacesByDateMock.mockResolvedValue([
    buildJraRow({ keibajoCode: "83", raceBango: "1", source: "nar" }),
    buildJraRow({ keibajoCode: "35", raceBango: "02", source: "nar" }),
  ]);
  getRaceDetailMock.mockResolvedValue({ kyori: "0200", trackCode: "23" });
  getRaceCourseInfoMock.mockResolvedValue({ courseKaishuNengappi: "20200101", courseSetsumei: "" });
  getRaceRunnersMock.mockResolvedValue([]);
  getSameVenueRacesByDateMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  putRaceDetailSsrSnapshotMock.mockResolvedValue(undefined);
  const response = await POST(buildAuthedRequest("?date=2026-08-17&keibajo=83"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-08-17", raceCount: 1, warmed: 1 });
  expect(getRaceSourceByRouteMock).not.toHaveBeenCalled();
  expect(getRaceDetailMock).toHaveBeenCalledTimes(1);
  expect(getRaceDetailMock).toHaveBeenCalledWith("nar", "2026", "08", "17", "83", "01");
});

it("POST counts a warmed race when getRaceDetail resolves and the fan-out succeeds", async () => {
  getRacesByDateMock.mockResolvedValue([buildJraRow({ keibajoCode: "05", raceBango: "01" })]);
  getRaceDetailMock.mockResolvedValue({ kyori: "1200", trackCode: "10" });
  getRaceCourseInfoMock.mockResolvedValue({ courseKaishuNengappi: "20200101", courseSetsumei: "" });
  getRaceRunnersMock.mockResolvedValue([]);
  getSameVenueRacesByDateMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([{ rank: "1" }]);
  putRaceDetailSsrSnapshotMock.mockResolvedValue(undefined);
  putRecentResultsCacheMock.mockResolvedValue(undefined);
  const response = await POST(buildAuthedRequest("?date=2026-05-29"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-05-29", raceCount: 1, warmed: 1 });
  expect(putRaceDetailSsrSnapshotMock).toHaveBeenCalledTimes(1);
});

it("POST counts a missing race when getRaceDetail resolves to null", async () => {
  getRacesByDateMock.mockResolvedValue([buildJraRow({ keibajoCode: "05", raceBango: "01" })]);
  getRaceDetailMock.mockResolvedValue(null);
  const response = await POST(buildAuthedRequest("?date=2026-05-29"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-05-29", raceCount: 1, warmed: 0 });
  expect(putRaceDetailSsrSnapshotMock).not.toHaveBeenCalled();
});

it("POST skips SSR fan-out when the current race generation is already warm", async () => {
  getRacesByDateMock.mockResolvedValue([buildJraRow({ keibajoCode: "05", raceBango: "01" })]);
  const get = vi.fn<(key: string) => Promise<string | null>>();
  get.mockResolvedValueOnce("3").mockResolvedValueOnce("3");
  safeGetCloudflareEnvMock.mockResolvedValue({
    DETAIL_SECTION_CACHE_KV: {
      get,
      put: vi.fn<(key: string, value: string) => Promise<void>>(),
    },
  });
  const response = await POST(buildAuthedRequest("?date=2026-05-29"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-05-29", raceCount: 1, warmed: 0 });
  expect(getRaceDetailMock).not.toHaveBeenCalled();
});

it("POST refreshes SSR and marks an invalidated race generation", async () => {
  getRacesByDateMock.mockResolvedValue([buildJraRow({ keibajoCode: "05", raceBango: "01" })]);
  getRaceDetailMock.mockResolvedValue({ kyori: "1200", trackCode: "10" });
  getRaceCourseInfoMock.mockResolvedValue(null);
  getRaceRunnersMock.mockResolvedValue([]);
  getSameVenueRacesByDateMock.mockResolvedValue([]);
  getHorseRaceResultsMock.mockResolvedValue([]);
  putRaceDetailSsrSnapshotMock.mockResolvedValue(undefined);
  const get = vi.fn<(key: string) => Promise<string | null>>();
  get.mockResolvedValueOnce("4").mockResolvedValueOnce("3").mockResolvedValueOnce("4");
  const put = vi
    .fn<(key: string, value: string, options?: { expirationTtl?: number }) => Promise<void>>()
    .mockResolvedValue(undefined);
  safeGetCloudflareEnvMock.mockResolvedValue({ DETAIL_SECTION_CACHE_KV: { get, put } });
  const response = await POST(buildAuthedRequest("?date=2026-05-29"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({ date: "2026-05-29", raceCount: 1, warmed: 1 });
  expect(put).toHaveBeenCalledTimes(1);
});
