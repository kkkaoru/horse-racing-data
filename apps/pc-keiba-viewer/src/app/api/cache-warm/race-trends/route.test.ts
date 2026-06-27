// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

import type { RaceListItem } from "../../../../lib/race-types";

vi.mock("server-only", () => ({}));

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const mocks = vi.hoisted(() => ({
  getRacesByDateMock: vi.fn<(year: string, month: string, day: string) => Promise<unknown[]>>(),
  safeGetCloudflareEnvMock: vi.fn<() => Promise<unknown>>(),
  sendBatchMock: vi.fn<(messages: unknown) => Promise<void>>(),
  sendMock: vi.fn<(body: unknown, options?: { delaySeconds?: number }) => Promise<void>>(),
}));

vi.mock("../../../../db/queries", () => ({
  getRacesByDate: mocks.getRacesByDateMock,
}));

vi.mock("../../../../lib/cloudflare-context.server", () => ({
  safeGetCloudflareEnv: mocks.safeGetCloudflareEnvMock,
}));

const { getRacesByDateMock, safeGetCloudflareEnvMock, sendBatchMock, sendMock } = mocks;

import { POST } from "./route";

const ENDPOINT_URL = "https://example.com/api/cache-warm/race-trends";

const buildAuthedRequest = (search: string): Request =>
  new Request(`${ENDPOINT_URL}${search}`, {
    headers: { "X-PC-Keiba-Cache-Warm": "scheduled" },
    method: "POST",
  });

const buildUnauthedRequest = (): Request => new Request(ENDPOINT_URL, { method: "POST" });

const buildQueueEnv = () => ({
  DETAIL_SECTION_CACHE_QUEUE: { send: sendMock, sendBatch: sendBatchMock },
});

const buildJraRow = (overrides: Partial<RaceListItem> = {}): RaceListItem => ({
  gradeCode: null,
  hassoJikoku: "1100",
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

const expectRecord = (value: unknown): Record<string, unknown> => {
  if (!isRecord(value)) {
    throw new Error("send call body was not an object");
  }
  return value;
};

beforeEach(() => {
  getRacesByDateMock.mockReset();
  safeGetCloudflareEnvMock.mockReset();
  sendBatchMock.mockReset();
  sendMock.mockReset();
  sendBatchMock.mockResolvedValue(undefined);
  sendMock.mockResolvedValue(undefined);
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
  safeGetCloudflareEnvMock.mockResolvedValue(buildQueueEnv());
  const response = await POST(buildAuthedRequest("?date=2026-05-29&now=2026-05-29T02:00:00Z"));
  expect(response.status).toBe(200);
  expect(getRacesByDateMock).toHaveBeenCalledWith("2026", "05", "29");
});

it("POST returns 503 with raceCount when the cache queue binding is unavailable", async () => {
  getRacesByDateMock.mockResolvedValue([buildJraRow()]);
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  const response = await POST(buildAuthedRequest("?date=2026-05-29&now=2026-05-29T02:00:00Z"));
  expect(response.status).toBe(503);
  const body = await readJsonRecord(response);
  expect(body.raceCount).toStrictEqual(1);
  expect(body.error).toStrictEqual("DETAIL_SECTION_CACHE_QUEUE binding is unavailable");
});

it("POST enqueues trend warm messages for due races when the queue binding exists", async () => {
  getRacesByDateMock.mockResolvedValue([buildJraRow({ keibajoCode: "05", raceBango: "01" })]);
  safeGetCloudflareEnvMock.mockResolvedValue(buildQueueEnv());
  const response = await POST(buildAuthedRequest("?date=2026-05-29&now=2026-05-29T02:05:00Z"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body.raceCount).toStrictEqual(1);
  expect(sendMock).toHaveBeenCalled();
  const firstCall = sendMock.mock.calls[0];
  if (!firstCall) {
    throw new Error("send was not called");
  }
  const messageBody = expectRecord(firstCall[0]);
  expect(messageBody.year).toStrictEqual("2026");
  expect(messageBody.month).toStrictEqual("05");
  expect(messageBody.day).toStrictEqual("29");
  expect(messageBody.keibajoCode).toStrictEqual("05");
  expect(messageBody.raceNumber).toStrictEqual("01");
  expect(messageBody.source).toStrictEqual("jra");
});
