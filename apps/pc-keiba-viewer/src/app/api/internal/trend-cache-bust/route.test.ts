// Run with bun. `bun run --filter pc-keiba-viewer test`
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  bustRaceTrendCachesForDayMock: vi.fn<(...args: never[]) => unknown>(),
  getRacesByDateWithoutJockeyNamesMock: vi.fn<(...args: never[]) => unknown>(),
  notifyRaceTrendRoomMock: vi.fn<(...args: never[]) => unknown>(),
}));

vi.mock("../../../../db/queries", () => ({
  getRacesByDateWithoutJockeyNames: mocks.getRacesByDateWithoutJockeyNamesMock,
}));

vi.mock("../../../../lib/race-trend-cache.server", () => ({
  bustRaceTrendCachesForDay: mocks.bustRaceTrendCachesForDayMock,
}));

vi.mock("../../../../lib/race-trend-room.server", () => ({
  notifyRaceTrendRoom: mocks.notifyRaceTrendRoomMock,
}));

const {
  bustRaceTrendCachesForDayMock,
  getRacesByDateWithoutJockeyNamesMock,
  notifyRaceTrendRoomMock,
} = mocks;

import { POST } from "./route";

const INTERNAL_TOKEN = "test-internal-token";
const AUTH_HEADER = "x-pc-keiba-internal-token";
const ENDPOINT_URL = "https://example.com/api/internal/trend-cache-bust";

interface BustResponseBody {
  keys: string[];
  notified: number;
  ok: boolean;
}

interface ErrorResponseBody {
  error: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isBustResponseBody = (value: unknown): value is BustResponseBody => {
  if (!isRecord(value)) return false;
  return Array.isArray(value.keys) && typeof value.notified === "number" && value.ok === true;
};

const isErrorResponseBody = (value: unknown): value is ErrorResponseBody => {
  if (!isRecord(value)) return false;
  return typeof value.error === "string";
};

const readJsonAsBustResponse = async (response: Response): Promise<BustResponseBody> => {
  const body: unknown = await response.json();
  if (!isBustResponseBody(body)) {
    throw new Error("response body is not a BustResponseBody");
  }
  return body;
};

const readJsonAsErrorResponse = async (response: Response): Promise<ErrorResponseBody> => {
  const body: unknown = await response.json();
  if (!isErrorResponseBody(body)) {
    throw new Error("response body is not an ErrorResponseBody");
  }
  return body;
};

const buildAuthedRequest = (body: unknown): Request =>
  new Request(ENDPOINT_URL, {
    body: JSON.stringify(body),
    headers: {
      "content-type": "application/json",
      [AUTH_HEADER]: INTERNAL_TOKEN,
    },
    method: "POST",
  });

const buildUnauthedRequest = (body: unknown): Request =>
  new Request(ENDPOINT_URL, {
    body: JSON.stringify(body),
    headers: { "content-type": "application/json" },
    method: "POST",
  });

const buildRequestWithRawBody = (rawBody: string): Request =>
  new Request(ENDPOINT_URL, {
    body: rawBody,
    headers: {
      "content-type": "application/json",
      [AUTH_HEADER]: INTERNAL_TOKEN,
    },
    method: "POST",
  });

interface DayRaceRow {
  source: "jra" | "nar";
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  kyosomeiHondai: string | null;
  kyosomeiFukudai: string | null;
  gradeCode: string | null;
  kyosoShubetsuCode: string | null;
  kyosoKigoCode: string | null;
  juryoShubetsuCode: string | null;
  kyosoJokenCode: string | null;
  kyosoJokenMeisho: string | null;
  kyori: string | null;
  trackCode: string | null;
  hassoJikoku: string | null;
  shussoTosu: string | null;
}

const buildJraDayRow = (overrides: Partial<DayRaceRow> = {}): DayRaceRow => ({
  gradeCode: null,
  hassoJikoku: null,
  juryoShubetsuCode: null,
  kaisaiNen: "2026",
  kaisaiTsukihi: "0529",
  keibajoCode: "05",
  kyori: null,
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

const buildNarDayRow = (overrides: Partial<DayRaceRow> = {}): DayRaceRow => ({
  gradeCode: null,
  hassoJikoku: null,
  juryoShubetsuCode: null,
  kaisaiNen: "2026",
  kaisaiTsukihi: "0529",
  keibajoCode: "42",
  kyori: null,
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosoShubetsuCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: null,
  raceBango: "01",
  shussoTosu: null,
  source: "nar",
  trackCode: null,
  ...overrides,
});

beforeEach(() => {
  bustRaceTrendCachesForDayMock.mockReset();
  getRacesByDateWithoutJockeyNamesMock.mockReset();
  notifyRaceTrendRoomMock.mockReset();
  vi.stubEnv("PC_KEIBA_INTERNAL_TOKEN", INTERNAL_TOKEN);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: [] });
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([]);
  notifyRaceTrendRoomMock.mockResolvedValue(true);
});

afterEach(() => {
  vi.unstubAllEnvs();
});

it("POST returns 403 when auth header is missing", async () => {
  const response = await POST(buildUnauthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(response.status).toBe(403);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "forbidden" });
});

it("POST returns 403 when auth header value does not match expected token", async () => {
  const request = new Request(ENDPOINT_URL, {
    body: JSON.stringify({ source: "jra", targetYmd: "20260529" }),
    headers: {
      "content-type": "application/json",
      [AUTH_HEADER]: "wrong-token",
    },
    method: "POST",
  });
  const response = await POST(request);
  expect(response.status).toBe(403);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "forbidden" });
});

it("POST returns 403 when PC_KEIBA_INTERNAL_TOKEN env is unset even with header present", async () => {
  vi.unstubAllEnvs();
  delete process.env.PC_KEIBA_INTERNAL_TOKEN;
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(response.status).toBe(403);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "forbidden" });
});

it("POST returns 400 when body is not valid JSON", async () => {
  const response = await POST(buildRequestWithRawBody("{not json"));
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when body is JSON null", async () => {
  const response = await POST(buildAuthedRequest(null));
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when body is a JSON array", async () => {
  const response = await POST(buildAuthedRequest([]));
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when body source is an unknown string", async () => {
  const response = await POST(buildAuthedRequest({ source: "world", targetYmd: "20260529" }));
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when body source is missing", async () => {
  const response = await POST(buildAuthedRequest({ targetYmd: "20260529" }));
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when body targetYmd is missing", async () => {
  const response = await POST(buildAuthedRequest({ source: "jra" }));
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when targetYmd is too short", async () => {
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: "2026" }));
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when targetYmd contains non-digit characters", async () => {
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: "2026-05-29" }));
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 400 when targetYmd is a number, not a string", async () => {
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: 20260529 }));
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST returns 200 with keys and notified count for valid JRA body", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "01" }),
    buildJraDayRow({ keibajoCode: "05", raceBango: "02" }),
  ]);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: ["k-1", "k-2"] });
  notifyRaceTrendRoomMock.mockResolvedValue(true);
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(response.status).toBe(200);
  const body = await readJsonAsBustResponse(response);
  expect(body).toStrictEqual({ keys: ["k-1", "k-2"], notified: 2, ok: true });
  expect(getRacesByDateWithoutJockeyNamesMock).toHaveBeenCalledWith("2026", "05", "29");
  expect(bustRaceTrendCachesForDayMock).toHaveBeenCalledTimes(1);
  expect(notifyRaceTrendRoomMock).toHaveBeenCalledTimes(2);
});

it("POST passes source jra and target races to bustRaceTrendCachesForDay", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "07" }),
  ]);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: ["only-key"] });
  await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(bustRaceTrendCachesForDayMock).toHaveBeenCalledWith({
    races: [{ keibajoCode: "05", raceBango: "07" }],
    source: "jra",
    targetYmd: "20260529",
  });
});

it("POST returns 200 for valid NAR body with NAR-only rows", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildNarDayRow({ keibajoCode: "42", raceBango: "03" }),
  ]);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: ["nar-k"] });
  notifyRaceTrendRoomMock.mockResolvedValue(true);
  const response = await POST(buildAuthedRequest({ source: "nar", targetYmd: "20260529" }));
  expect(response.status).toBe(200);
  const body = await readJsonAsBustResponse(response);
  expect(body).toStrictEqual({ keys: ["nar-k"], notified: 1, ok: true });
  expect(bustRaceTrendCachesForDayMock).toHaveBeenCalledWith({
    races: [{ keibajoCode: "42", raceBango: "03" }],
    source: "nar",
    targetYmd: "20260529",
  });
});

it("POST filters out races whose source does not match the requested source", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "01" }),
    buildNarDayRow({ keibajoCode: "42", raceBango: "11" }),
  ]);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: ["jra-only"] });
  await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(bustRaceTrendCachesForDayMock).toHaveBeenCalledWith({
    races: [{ keibajoCode: "05", raceBango: "01" }],
    source: "jra",
    targetYmd: "20260529",
  });
});

it("POST recovers with empty races when getRacesByDateWithoutJockeyNames rejects", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockRejectedValue(new Error("db boom"));
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: [] });
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(response.status).toBe(200);
  const body = await readJsonAsBustResponse(response);
  expect(body).toStrictEqual({ keys: [], notified: 0, ok: true });
  expect(bustRaceTrendCachesForDayMock).toHaveBeenCalledWith({
    races: [],
    source: "jra",
    targetYmd: "20260529",
  });
});

it("POST propagates the rejection when bustRaceTrendCachesForDay rejects", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "01" }),
  ]);
  bustRaceTrendCachesForDayMock.mockRejectedValue(new Error("bust boom"));
  await expect(POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }))).rejects.toThrow(
    "bust boom",
  );
});

it("POST returns 200 with notified=0 when notifyRaceTrendRoom rejects for every race", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "01" }),
    buildJraDayRow({ keibajoCode: "05", raceBango: "02" }),
  ]);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: ["k-1", "k-2"] });
  notifyRaceTrendRoomMock.mockRejectedValue(new Error("notify boom"));
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(response.status).toBe(200);
  const body = await readJsonAsBustResponse(response);
  expect(body).toStrictEqual({ keys: ["k-1", "k-2"], notified: 0, ok: true });
});

it("POST counts only races where notifyRaceTrendRoom resolved true", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "01" }),
    buildJraDayRow({ keibajoCode: "05", raceBango: "02" }),
    buildJraDayRow({ keibajoCode: "05", raceBango: "03" }),
  ]);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: [] });
  notifyRaceTrendRoomMock
    .mockResolvedValueOnce(true)
    .mockResolvedValueOnce(false)
    .mockResolvedValueOnce(true);
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(response.status).toBe(200);
  const body = await readJsonAsBustResponse(response);
  expect(body).toStrictEqual({ keys: [], notified: 2, ok: true });
});

it("POST calls notifyRaceTrendRoom with split year/month/day plus source and cacheKey", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "07" }),
  ]);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: [] });
  await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(notifyRaceTrendRoomMock).toHaveBeenCalledWith(
    {
      day: "29",
      keibajoCode: "05",
      month: "05",
      raceNumber: "07",
      source: "jra",
      year: "2026",
    },
    { cacheKey: "race-trend-day:jra:20260529" },
  );
});
