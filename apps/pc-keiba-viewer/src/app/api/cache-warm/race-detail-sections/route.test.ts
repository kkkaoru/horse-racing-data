// Run with bun. `bun run --filter pc-keiba-viewer test`
import { beforeEach, expect, it, vi } from "vitest";

import type { RaceListItem } from "../../../../lib/race-types";

vi.mock("server-only", () => ({}));

interface DetailSectionMessage {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  section: string;
  source: "jra" | "nar";
  year: string;
}

interface SendBatchPayload {
  body: DetailSectionMessage;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isStringSource = (value: unknown): value is "jra" | "nar" =>
  value === "jra" || value === "nar";

const isDetailSectionMessage = (value: unknown): value is DetailSectionMessage => {
  if (!isRecord(value)) return false;
  return (
    typeof value.day === "string" &&
    typeof value.keibajoCode === "string" &&
    typeof value.month === "string" &&
    typeof value.raceNumber === "string" &&
    typeof value.section === "string" &&
    typeof value.year === "string" &&
    isStringSource(value.source)
  );
};

const isSendBatchPayloadArray = (value: unknown): value is SendBatchPayload[] => {
  if (!Array.isArray(value)) return false;
  return value.every((entry) => isRecord(entry) && isDetailSectionMessage(entry.body));
};

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

const ENDPOINT_URL = "https://example.com/api/cache-warm/race-detail-sections";

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

const expectSendBatchPayload = (value: unknown): SendBatchPayload[] => {
  if (!isSendBatchPayloadArray(value)) {
    throw new Error("send batch payload was not the expected shape");
  }
  return value;
};

const expectFirstMessage = (payload: SendBatchPayload[]): DetailSectionMessage => {
  const head = payload[0];
  if (!head) {
    throw new Error("send batch payload was empty");
  }
  return head.body;
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
  getRacesByDateMock.mockResolvedValue([buildJraRow()]);
  safeGetCloudflareEnvMock.mockResolvedValue(buildQueueEnv());
  const response = await POST(buildAuthedRequest("?date=2026-05-29"));
  expect(response.status).toBe(200);
  expect(getRacesByDateMock).toHaveBeenCalledWith("2026", "05", "29");
});

it("POST returns 503 when the cache queue binding is unavailable", async () => {
  getRacesByDateMock.mockResolvedValue([buildJraRow()]);
  safeGetCloudflareEnvMock.mockResolvedValue(null);
  const response = await POST(buildAuthedRequest("?date=2026-05-29"));
  expect(response.status).toBe(503);
  const body = await readJsonRecord(response);
  expect(body).toStrictEqual({
    date: "2026-05-29",
    enqueued: 0,
    error: "DETAIL_SECTION_CACHE_QUEUE binding is unavailable",
    raceCount: 1,
  });
});

it("POST enqueues default sections for a JRA race using the queue's sendBatch", async () => {
  getRacesByDateMock.mockResolvedValue([buildJraRow({ keibajoCode: "05", raceBango: "01" })]);
  safeGetCloudflareEnvMock.mockResolvedValue(buildQueueEnv());
  const response = await POST(buildAuthedRequest("?date=2026-05-29"));
  expect(response.status).toBe(200);
  const body = await readJsonRecord(response);
  expect(body.date).toStrictEqual("2026-05-29");
  expect(body.raceCount).toStrictEqual(1);
  expect(typeof body.enqueued).toBe("number");
  expect(sendBatchMock).toHaveBeenCalledTimes(1);
  const firstCall = sendBatchMock.mock.calls[0];
  if (!firstCall) {
    throw new Error("sendBatch was not called");
  }
  const payload = expectSendBatchPayload(firstCall[0]);
  const message = expectFirstMessage(payload);
  expect(message.year).toStrictEqual("2026");
  expect(message.month).toStrictEqual("05");
  expect(message.day).toStrictEqual("29");
  expect(message.keibajoCode).toStrictEqual("05");
  expect(message.raceNumber).toStrictEqual("01");
  expect(message.source).toStrictEqual("jra");
});
