// Run with bun. `bun run --filter pc-keiba-viewer test`
import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("server-only", () => ({}));

const mocks = vi.hoisted(() => ({
  bustRaceCachesForRaceMock: vi.fn<(...args: never[]) => unknown>(),
  bustRaceTrendCachesForDayMock: vi.fn<(...args: never[]) => unknown>(),
  getRacesByDateWithoutJockeyNamesMock: vi.fn<(...args: never[]) => unknown>(),
  safeGetCloudflareEnvMock: vi.fn<(...args: never[]) => unknown>(),
  safeGetCloudflareExecutionContextMock: vi.fn<(...args: never[]) => unknown>(),
}));

vi.mock("../../../../db/queries", () => ({
  getRacesByDateWithoutJockeyNames: mocks.getRacesByDateWithoutJockeyNamesMock,
}));

vi.mock("../../../../lib/cloudflare-context.server", () => ({
  safeGetCloudflareEnv: mocks.safeGetCloudflareEnvMock,
  safeGetCloudflareExecutionContext: mocks.safeGetCloudflareExecutionContextMock,
}));

vi.mock("../../../../lib/race-trend-cache.server", () => ({
  bustRaceTrendCachesForDay: mocks.bustRaceTrendCachesForDayMock,
}));

vi.mock("../../../../lib/race-cache-bust.server", () => ({
  bustRaceCachesForRace: mocks.bustRaceCachesForRaceMock,
}));

const {
  bustRaceCachesForRaceMock,
  bustRaceTrendCachesForDayMock,
  getRacesByDateWithoutJockeyNamesMock,
  safeGetCloudflareEnvMock,
  safeGetCloudflareExecutionContextMock,
} = mocks;

import { POST } from "./route";

const INTERNAL_TOKEN = "test-internal-token";
const AUTH_HEADER = "x-pc-keiba-internal-token";
const ENDPOINT_URL = "https://example.com/api/internal/trend-cache-bust";

interface BustResponseBody {
  keys: string[];
  notified: number;
  ok: boolean;
  sectionBusted: number;
}

interface ErrorResponseBody {
  error: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isBustResponseBody = (value: unknown): value is BustResponseBody => {
  if (!isRecord(value)) return false;
  return (
    Array.isArray(value.keys) &&
    typeof value.notified === "number" &&
    value.ok === true &&
    typeof value.sectionBusted === "number"
  );
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
  bustRaceCachesForRaceMock.mockReset();
  bustRaceTrendCachesForDayMock.mockReset();
  getRacesByDateWithoutJockeyNamesMock.mockReset();
  safeGetCloudflareEnvMock.mockReset();
  safeGetCloudflareExecutionContextMock.mockReset();
  vi.stubEnv("PC_KEIBA_INTERNAL_TOKEN", INTERNAL_TOKEN);
  bustRaceCachesForRaceMock.mockResolvedValue({ busted: 0, generation: 1 });
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: [] });
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([]);
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  safeGetCloudflareExecutionContextMock.mockResolvedValue(null);
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
  bustRaceCachesForRaceMock.mockResolvedValue({ busted: 10, generation: 2 });
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(response.status).toBe(200);
  const body = await readJsonAsBustResponse(response);
  expect(body).toStrictEqual({
    keys: ["k-1", "k-2"],
    notified: 0,
    ok: true,
    sectionBusted: 20,
  });
  expect(getRacesByDateWithoutJockeyNamesMock).toHaveBeenCalledWith("2026", "05", "29");
  expect(bustRaceTrendCachesForDayMock).toHaveBeenCalledTimes(1);
  expect(bustRaceCachesForRaceMock).toHaveBeenCalledTimes(2);
});

it("POST accepts a production cache bust before the day-wide work completes", async () => {
  const races = Promise.withResolvers<DayRaceRow[]>();
  const waitUntil = vi.fn<(promise: Promise<unknown>) => void>();
  safeGetCloudflareExecutionContextMock.mockResolvedValue({ waitUntil });
  getRacesByDateWithoutJockeyNamesMock.mockReturnValue(races.promise);

  const response = await POST(buildAuthedRequest({ source: "nar", targetYmd: "20260529" }));

  expect(response.status).toBe(202);
  const body: unknown = await response.json();
  expect(body).toStrictEqual({ accepted: true, ok: true });
  expect(waitUntil).toHaveBeenCalledTimes(1);
  expect(bustRaceTrendCachesForDayMock).not.toHaveBeenCalled();

  races.resolve([buildNarDayRow({ keibajoCode: "42", raceBango: "03" })]);
  await waitUntil.mock.calls[0]![0];
  expect(bustRaceTrendCachesForDayMock).toHaveBeenCalledTimes(1);
  expect(bustRaceCachesForRaceMock).toHaveBeenCalledTimes(1);
});

it("POST contains and logs a rejected production background cache bust", async () => {
  const waitUntil = vi.fn<(promise: Promise<unknown>) => void>();
  const error = new Error("bust unavailable");
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  safeGetCloudflareExecutionContextMock.mockResolvedValue({ waitUntil });
  bustRaceTrendCachesForDayMock.mockRejectedValue(error);

  const response = await POST(buildAuthedRequest({ source: "nar", targetYmd: "20260529" }));

  expect(response.status).toBe(202);
  await waitUntil.mock.calls[0]![0];
  expect(consoleError).toHaveBeenCalledWith("Trend cache bust background task failed", error);
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
  bustRaceCachesForRaceMock.mockResolvedValue({ busted: 5, generation: 3 });
  const response = await POST(buildAuthedRequest({ source: "nar", targetYmd: "20260529" }));
  expect(response.status).toBe(200);
  const body = await readJsonAsBustResponse(response);
  expect(body).toStrictEqual({
    keys: ["nar-k"],
    notified: 0,
    ok: true,
    sectionBusted: 5,
  });
  expect(bustRaceTrendCachesForDayMock).toHaveBeenCalledWith({
    races: [{ keibajoCode: "42", raceBango: "03" }],
    source: "nar",
    targetYmd: "20260529",
  });
  expect(bustRaceCachesForRaceMock).toHaveBeenCalledWith({
    keibajoCode: "42",
    mmdd: "0529",
    raceBango: "03",
    source: "nar",
    year: "2026",
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
  expect(body).toStrictEqual({ keys: [], notified: 0, ok: true, sectionBusted: 0 });
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

it("POST leaves room notification to the changed-hash warm rebuild", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "01" }),
    buildJraDayRow({ keibajoCode: "05", raceBango: "02" }),
  ]);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: ["k-1", "k-2"] });
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(response.status).toBe(200);
  const body = await readJsonAsBustResponse(response);
  expect(body).toStrictEqual({
    keys: ["k-1", "k-2"],
    notified: 0,
    ok: true,
    sectionBusted: 0,
  });
  expect(body).toStrictEqual({ keys: ["k-1", "k-2"], notified: 0, ok: true, sectionBusted: 0 });
});

it("POST swallows bustRaceCachesForRace rejection and returns sectionBusted=0", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "01" }),
  ]);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: [] });
  bustRaceCachesForRaceMock.mockRejectedValue(new Error("section bust boom"));
  const response = await POST(buildAuthedRequest({ source: "jra", targetYmd: "20260529" }));
  expect(response.status).toBe(200);
  const body = await readJsonAsBustResponse(response);
  expect(body).toStrictEqual({
    keys: [],
    notified: 0,
    ok: true,
    sectionBusted: 0,
  });
});

it("POST scopes a result bust to the same venue at and after the changed race", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "06" }),
    buildJraDayRow({ keibajoCode: "05", raceBango: "07" }),
    buildJraDayRow({ keibajoCode: "05", raceBango: "08" }),
    buildJraDayRow({ keibajoCode: "06", raceBango: "08" }),
  ]);
  bustRaceTrendCachesForDayMock.mockResolvedValue({ keys: [] });
  await POST(
    buildAuthedRequest({
      keibajoCode: "05",
      raceBango: "07",
      source: "jra",
      targetYmd: "20260529",
    }),
  );
  expect(bustRaceTrendCachesForDayMock).toHaveBeenCalledWith({
    races: [
      { keibajoCode: "05", raceBango: "07" },
      { keibajoCode: "05", raceBango: "08" },
    ],
    source: "jra",
    targetYmd: "20260529",
  });
  expect(bustRaceCachesForRaceMock).toHaveBeenCalledTimes(2);
});

it("POST enqueues immediate changed-hash rebuilds only for scoped affected races", async () => {
  getRacesByDateWithoutJockeyNamesMock.mockResolvedValue([
    buildJraDayRow({ keibajoCode: "05", raceBango: "06" }),
    buildJraDayRow({ keibajoCode: "05", raceBango: "07" }),
    buildJraDayRow({ keibajoCode: "05", raceBango: "08" }),
    buildJraDayRow({ keibajoCode: "06", raceBango: "08" }),
  ]);
  const get = vi.fn<(key: string) => Promise<string | null>>();
  get.mockResolvedValueOnce("4").mockResolvedValueOnce(null);
  get.mockResolvedValueOnce("5").mockResolvedValueOnce(null);
  const send = vi.fn<(body: unknown) => Promise<void>>().mockResolvedValue(undefined);
  const sendBatch = vi
    .fn<(messages: Array<{ body: unknown }>) => Promise<void>>()
    .mockResolvedValue(undefined);
  safeGetCloudflareEnvMock.mockResolvedValue({
    DETAIL_SECTION_CACHE_KV: {
      get,
      put: vi.fn<(key: string, value: string) => Promise<void>>(),
    },
    DETAIL_SECTION_CACHE_QUEUE: { send, sendBatch },
  });
  await POST(
    buildAuthedRequest({
      keibajoCode: "05",
      raceBango: "07",
      source: "jra",
      targetYmd: "20260529",
    }),
  );
  expect(send).toHaveBeenCalledTimes(2);
  expect(send).toHaveBeenNthCalledWith(
    1,
    expect.objectContaining({ cacheGeneration: "4", keibajoCode: "05", raceNumber: "07" }),
  );
  expect(send).toHaveBeenNthCalledWith(
    2,
    expect.objectContaining({ cacheGeneration: "5", keibajoCode: "05", raceNumber: "08" }),
  );
  expect(sendBatch).toHaveBeenCalledOnce();
  const queuedBodies = sendBatch.mock.calls[0]?.[0].map((message) => message.body) ?? [];
  expect(queuedBodies).toHaveLength(18);
  expect(queuedBodies).toContainEqual(
    expect.objectContaining({
      keibajoCode: "05",
      raceNumber: "07",
      section: "time-score",
    }),
  );
  expect(queuedBodies).toContainEqual(
    expect.objectContaining({
      keibajoCode: "05",
      kind: "race-detail-ssr",
      raceNumber: "08",
    }),
  );
});

it("POST rejects a scoped body when only keibajoCode is present", async () => {
  const response = await POST(
    buildAuthedRequest({ keibajoCode: "05", source: "jra", targetYmd: "20260529" }),
  );
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});

it("POST rejects invalid scoped race identifiers", async () => {
  const response = await POST(
    buildAuthedRequest({
      keibajoCode: "tokyo",
      raceBango: "7",
      source: "jra",
      targetYmd: "20260529",
    }),
  );
  expect(response.status).toBe(400);
  const body = await readJsonAsErrorResponse(response);
  expect(body).toStrictEqual({ error: "invalid body" });
});
