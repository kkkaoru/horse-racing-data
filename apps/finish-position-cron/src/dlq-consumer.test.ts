// Run with bun. Tests for the dead-letter queue consumer.

import { beforeEach, expect, test, vi } from "vitest";
import type { RetryErrorLookupRow } from "./retry-errors";
import type { Env, PredictQueueMessage } from "./types";

const { completeFocusedFullRaceMock } = vi.hoisted(() => {
  const completeFocusedFullRace = vi.fn(async () => undefined);
  return { completeFocusedFullRaceMock: completeFocusedFullRace };
});

vi.mock("./do-state", () => ({
  completeFocusedFullRace: completeFocusedFullRaceMock,
}));

import { DLQ_QUEUE_NAME, handleDlqQueue } from "./dlq-consumer";

const ackMock = vi.fn();
const retryMock = vi.fn();
const sendMock = vi.fn();
const runMock = vi.fn(async () => ({ success: true }));
const firstMock = vi.fn(async (): Promise<RetryErrorLookupRow | null> => null);
const bindMock = vi.fn(() => ({ first: firstMock, run: runMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));

const makeEnv = (): Env =>
  ({
    FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
    PREDICT_QUEUE: { send: sendMock } as unknown as Env["PREDICT_QUEUE"],
  }) as unknown as Env;

const makeMessage = (overrides: Partial<PredictQueueMessage> = {}): Message<PredictQueueMessage> =>
  ({
    ack: ackMock,
    attempts: 16,
    body: {
      category: "jra",
      daysAhead: 0,
      mode: "full",
      runDate: "2026-07-12",
      runDateIso: "2026-07-12",
      runYmd: "20260712",
      ...overrides,
    } satisfies PredictQueueMessage,
    id: "dlq-msg-1",
    retry: retryMock,
  }) as unknown as Message<PredictQueueMessage>;

const makeBatch = (messages: Message<PredictQueueMessage>[]): MessageBatch<PredictQueueMessage> =>
  ({ messages, queue: DLQ_QUEUE_NAME }) as unknown as MessageBatch<PredictQueueMessage>;

const nullFailureBindTail = [null, null, null, null, null, 16] as const;

beforeEach(() => {
  ackMock.mockClear();
  retryMock.mockClear();
  sendMock.mockClear();
  sendMock.mockResolvedValue(undefined);
  runMock.mockClear();
  runMock.mockResolvedValue({ success: true });
  firstMock.mockClear();
  firstMock.mockResolvedValue(null);
  bindMock.mockClear();
  prepareMock.mockClear();
  completeFocusedFullRaceMock.mockClear();
  completeFocusedFullRaceMock.mockResolvedValue(undefined);
});

test("DLQ_QUEUE_NAME matches the dead-letter queue name in wrangler.jsonc", () => {
  expect(DLQ_QUEUE_NAME).toBe("finish-position-predict-dlq");
});

test("records a durable event row for a focused-full message", async () => {
  await handleDlqQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(prepareMock).toHaveBeenCalled();
  expect(bindMock).toHaveBeenCalledWith(
    "20260712",
    "jra",
    "full",
    "02",
    "01",
    0,
    1,
    ...nullFailureBindTail,
  );
  expect(runMock).toHaveBeenCalledTimes(1);
});

test("unsticks the focused-full DO claim by forcing status=error", async () => {
  await handleDlqQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );
  expect(completeFocusedFullRaceMock).toHaveBeenCalledTimes(1);
  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith(
    expect.objectContaining({
      category: "jra",
      keibajoCode: "02",
      raceBango: "01",
      runYmd: "20260712",
      status: "error",
    }),
  );
});

test("does not unstick a claim for a non-focused-full message", async () => {
  await handleDlqQueue(
    makeBatch([makeMessage({ keibajoCode: "50", mode: "rescore", raceBango: "12" })]),
    makeEnv(),
  );
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
});

test("redrives a message seen for the first time with dlqRedriveCount=1", async () => {
  await handleDlqQueue(
    makeBatch([makeMessage({ keibajoCode: "02", mode: "full", raceBango: "01", skipDedup: true })]),
    makeEnv(),
  );
  expect(sendMock).toHaveBeenCalledTimes(1);
  const sentBody = sendMock.mock.calls[0]?.[0] as PredictQueueMessage;
  expect(sentBody.dlqRedriveCount).toBe(1);
  expect(sentBody.keibajoCode).toBe("02");
  expect(sentBody.raceBango).toBe("01");
});

test("acks the message after a successful redrive", async () => {
  await handleDlqQueue(makeBatch([makeMessage()]), makeEnv());
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
});

test("does not redrive a message that already exhausted the redrive budget", async () => {
  await handleDlqQueue(makeBatch([makeMessage({ dlqRedriveCount: 1 })]), makeEnv());
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("still records the event row with redriven=false when the budget is exhausted", async () => {
  await handleDlqQueue(
    makeBatch([makeMessage({ dlqRedriveCount: 1, keibajoCode: "02", raceBango: "01" })]),
    makeEnv(),
  );
  expect(bindMock).toHaveBeenCalledWith(
    "20260712",
    "jra",
    "full",
    "02",
    "01",
    1,
    0,
    ...nullFailureBindTail,
  );
});

test("copies lastFailure from the message body onto the dlq event row", async () => {
  await handleDlqQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "83",
        lastFailure: {
          errorMessage: "Container DO returned 503: no instance",
          errorName: "Error",
          errorStack: "Error: Container DO returned 503: no instance",
          httpBodyExcerpt: "no instance",
          httpStatus: 503,
        },
        raceBango: "06",
      }),
    ]),
    makeEnv(),
  );
  expect(bindMock).toHaveBeenCalledWith(
    "20260712",
    "jra",
    "full",
    "83",
    "06",
    0,
    1,
    "Error",
    "Container DO returned 503: no instance",
    "Error: Container DO returned 503: no instance",
    503,
    "no instance",
    16,
  );
  expect(firstMock).not.toHaveBeenCalled();
});

test("looks up the latest retry error by message id when the body has no lastFailure", async () => {
  firstMock.mockResolvedValueOnce({
    errorMessage: "network timeout",
    errorName: "Error",
    errorStack: "Error: network timeout",
    httpBodyExcerpt: null,
    httpStatus: null,
    queueAttempts: 12,
  });
  await handleDlqQueue(makeBatch([makeMessage({ keibajoCode: "05", raceBango: "11" })]), makeEnv());
  expect(bindMock).toHaveBeenCalledWith("dlq-msg-1");
  expect(bindMock).toHaveBeenCalledWith(
    "20260712",
    "jra",
    "full",
    "05",
    "11",
    0,
    1,
    "Error",
    "network timeout",
    "Error: network timeout",
    null,
    null,
    16,
  );
});

test("treats an empty lastFailure object as absent and falls back to D1 lookup", async () => {
  firstMock.mockResolvedValueOnce({
    errorMessage: "from d1",
    errorName: "Error",
    errorStack: null,
    httpBodyExcerpt: null,
    httpStatus: 502,
    queueAttempts: 9,
  });
  await handleDlqQueue(
    makeBatch([
      makeMessage({
        keibajoCode: "83",
        lastFailure: {},
        raceBango: "11",
      }),
    ]),
    makeEnv(),
  );
  expect(firstMock).toHaveBeenCalled();
  expect(bindMock).toHaveBeenCalledWith(
    "20260712",
    "jra",
    "full",
    "83",
    "11",
    0,
    1,
    "Error",
    "from d1",
    null,
    502,
    null,
    16,
  );
});

test("uses retry-error queueAttempts when the DLQ message has no attempts", async () => {
  firstMock.mockResolvedValueOnce({
    errorMessage: "Empty response from predict DO",
    errorName: "Error",
    errorStack: null,
    httpBodyExcerpt: null,
    httpStatus: null,
    queueAttempts: 8,
  });
  const message = {
    ...makeMessage({ keibajoCode: "44", raceBango: "08" }),
    attempts: undefined,
  } as unknown as Message<PredictQueueMessage>;
  await handleDlqQueue(makeBatch([message]), makeEnv());
  expect(bindMock).toHaveBeenCalledWith(
    "20260712",
    "jra",
    "full",
    "44",
    "08",
    0,
    1,
    "Error",
    "Empty response from predict DO",
    null,
    null,
    null,
    8,
  );
});

test("falls back to race-key retry-error lookup when message id is absent", async () => {
  firstMock.mockResolvedValueOnce({
    errorMessage: "Empty response from predict DO",
    errorName: "Error",
    errorStack: null,
    httpBodyExcerpt: null,
    httpStatus: null,
    queueAttempts: 8,
  });
  const message = {
    ack: ackMock,
    attempts: 16,
    body: {
      category: "jra",
      daysAhead: 0,
      keibajoCode: "44",
      mode: "full",
      raceBango: "07",
      runDate: "2026-07-12",
      runDateIso: "2026-07-12",
      runYmd: "20260712",
    },
    retry: retryMock,
  } as unknown as Message<PredictQueueMessage>;
  await handleDlqQueue(makeBatch([message]), makeEnv());
  expect(bindMock).toHaveBeenCalledWith("20260712", "jra", "full", "44", "07");
  expect(bindMock).toHaveBeenCalledWith(
    "20260712",
    "jra",
    "full",
    "44",
    "07",
    0,
    1,
    "Error",
    "Empty response from predict DO",
    null,
    null,
    null,
    16,
  );
});

test("retries (does not ack) when the D1 insert throws", async () => {
  runMock.mockRejectedValue(new Error("d1 unavailable"));
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  try {
    await handleDlqQueue(makeBatch([makeMessage()]), makeEnv());
    expect(retryMock).toHaveBeenCalledTimes(1);
    expect(ackMock).not.toHaveBeenCalled();
    expect(sendMock).not.toHaveBeenCalled();
  } finally {
    consoleError.mockRestore();
  }
});

test("retries when unsticking the focused-full claim throws", async () => {
  completeFocusedFullRaceMock.mockRejectedValue(new Error("do unavailable"));
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  try {
    await handleDlqQueue(
      makeBatch([
        makeMessage({ keibajoCode: "02", mode: "full", raceBango: "01", skipDedup: true }),
      ]),
      makeEnv(),
    );
    expect(retryMock).toHaveBeenCalledTimes(1);
    expect(ackMock).not.toHaveBeenCalled();
  } finally {
    consoleError.mockRestore();
  }
});

test("processes every message in the batch independently", async () => {
  await handleDlqQueue(
    makeBatch([
      makeMessage({ category: "jra", runYmd: "20260712" }),
      makeMessage({ category: "nar", runYmd: "20260712" }),
    ]),
    makeEnv(),
  );
  expect(runMock).toHaveBeenCalledTimes(2);
  expect(ackMock).toHaveBeenCalledTimes(2);
});
