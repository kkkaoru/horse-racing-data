// Run with bun. Tests for the queue consumer.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env, PredictQueueMessage } from "./types";

const { isAlreadyRunningMock, writeRunStateMock, parseNdjsonStreamMock } = vi.hoisted(() => {
  const isAlreadyRunning = vi.fn(async () => false);
  const writeRunState = vi.fn(async () => undefined);
  const parseNdjsonStream = vi.fn(async () => ({
    type: "result" as const,
    racesPredicted: 5,
    category: "jra",
  }));
  return {
    isAlreadyRunningMock: isAlreadyRunning,
    writeRunStateMock: writeRunState,
    parseNdjsonStreamMock: parseNdjsonStream,
  };
});

vi.mock("./kv-state", () => ({
  isAlreadyRunning: isAlreadyRunningMock,
  writeRunState: writeRunStateMock,
}));

vi.mock("./ndjson-stream", () => ({
  parseNdjsonStream: parseNdjsonStreamMock,
}));

import { handleQueue } from "./queue-consumer";

const ackMock = vi.fn();
const retryMock = vi.fn();
const idFromNameMock = vi.fn(() => ({ name: "test-id" }));
const stubFetchMock = vi.fn(
  async () =>
    new Response(JSON.stringify({ type: "result", racesPredicted: 5, category: "jra" }), {
      status: 200,
    }),
);
const getMock = vi.fn(() => ({ fetch: stubFetchMock }));

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {
    get: getMock,
    idFromName: idFromNameMock,
  } as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: {} as unknown as Env["PREDICT_QUEUE"],
  PREDICT_STATE: {} as unknown as KVNamespace,
  TRIGGER_TOKEN: "secret-token",
});

const makeMessage = (overrides: Partial<PredictQueueMessage> = {}): Message<PredictQueueMessage> =>
  ({
    ack: ackMock,
    body: {
      category: "jra",
      daysAhead: 2,
      mode: "full",
      runDate: "2026-06-03",
      runDateIso: "2026-06-03",
      runYmd: "20260603",
      ...overrides,
    } satisfies PredictQueueMessage,
    retry: retryMock,
  }) as unknown as Message<PredictQueueMessage>;

const makeBatch = (messages: Message<PredictQueueMessage>[]): MessageBatch<PredictQueueMessage> =>
  ({ messages }) as unknown as MessageBatch<PredictQueueMessage>;

beforeEach(() => {
  ackMock.mockClear();
  retryMock.mockClear();
  idFromNameMock.mockClear();
  getMock.mockClear();
  stubFetchMock.mockClear();
  isAlreadyRunningMock.mockClear();
  writeRunStateMock.mockClear();
  parseNdjsonStreamMock.mockClear();
  isAlreadyRunningMock.mockResolvedValue(false);
  parseNdjsonStreamMock.mockResolvedValue({ type: "result", racesPredicted: 5, category: "jra" });
  stubFetchMock.mockResolvedValue(
    new Response(JSON.stringify({ type: "result", racesPredicted: 5, category: "jra" }), {
      status: 200,
    }),
  );
});

test("skips and acks when isAlreadyRunning is true", async () => {
  isAlreadyRunningMock.mockResolvedValue(true);
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(stubFetchMock).not.toHaveBeenCalled();
});

test("writes started state when not already running", async () => {
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(writeRunStateMock).toHaveBeenCalledWith(
    expect.objectContaining({ state: expect.objectContaining({ status: "started" }) }),
  );
});

test("calls stub.fetch with correct URL including mode=full", async () => {
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=2&mode=full&runDate=2026-06-03",
  );
});

test("calls stub.fetch with mode=rescore when message has mode rescore", async () => {
  await handleQueue(
    makeBatch([makeMessage({ daysAhead: 0, mode: "rescore", runDate: "2026-06-19" })]),
    makeEnv(),
  );
  expect(stubFetchMock).toHaveBeenCalledTimes(1);
  const fetchRequest = (stubFetchMock.mock.calls[0] as unknown as [Request])[0];
  expect(fetchRequest.url).toBe(
    "http://do/predict?category=jra&daysAhead=0&mode=rescore&runDate=2026-06-19",
  );
});

test("writes success state and acks on success", async () => {
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(writeRunStateMock).toHaveBeenLastCalledWith(
    expect.objectContaining({
      state: expect.objectContaining({ status: "success", racesPredicted: 5 }),
    }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("writes error state and calls message.retry on failure", async () => {
  stubFetchMock.mockRejectedValue(new Error("network timeout"));
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(writeRunStateMock).toHaveBeenLastCalledWith(
    expect.objectContaining({ state: expect.objectContaining({ status: "error" }) }),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
});

test("processes multiple messages in batch", async () => {
  const msg1 = makeMessage({ category: "jra" });
  const msg2 = makeMessage({ category: "nar" });
  const msg3 = makeMessage({ category: "ban-ei" });
  await handleQueue(makeBatch([msg1, msg2, msg3]), makeEnv());
  expect(stubFetchMock).toHaveBeenCalledTimes(3);
  expect(ackMock).toHaveBeenCalledTimes(3);
});

test("writes error state and retries when response.body is null", async () => {
  stubFetchMock.mockResolvedValue(new Response(null, { status: 200 }));
  await handleQueue(makeBatch([makeMessage()]), makeEnv());
  expect(writeRunStateMock).toHaveBeenLastCalledWith(
    expect.objectContaining({ state: expect.objectContaining({ status: "error" }) }),
  );
  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
});
