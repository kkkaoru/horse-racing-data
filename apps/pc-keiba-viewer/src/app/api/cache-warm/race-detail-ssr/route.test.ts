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
  getRacesByDateMock: vi.fn<(year: string, month: string, day: string) => Promise<unknown[]>>(),
  getSameVenueRacesByDateMock: vi.fn<(...args: never[]) => Promise<unknown[]>>(),
  putRaceDetailSsrSnapshotMock: vi.fn<(input: unknown) => Promise<void>>(),
  putRecentResultsCacheMock: vi.fn<(key: string, value: string) => Promise<void>>(),
}));

vi.mock("../../../../db/queries", () => ({
  getHorseRaceResults: mocks.getHorseRaceResultsMock,
  getRaceCourseInfo: mocks.getRaceCourseInfoMock,
  getRaceDetail: mocks.getRaceDetailMock,
  getRaceRunners: mocks.getRaceRunnersMock,
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

const {
  getHorseRaceResultsMock,
  getRaceCourseInfoMock,
  getRaceDetailMock,
  getRaceRunnersMock,
  getRacesByDateMock,
  getSameVenueRacesByDateMock,
  putRaceDetailSsrSnapshotMock,
  putRecentResultsCacheMock,
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
  getRacesByDateMock.mockReset();
  getSameVenueRacesByDateMock.mockReset();
  putRaceDetailSsrSnapshotMock.mockReset();
  putRecentResultsCacheMock.mockReset();
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
