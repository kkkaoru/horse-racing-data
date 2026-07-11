// Run with bun. Tests for the dead-letter queue consumer.

import { beforeEach, expect, test, vi } from "vitest";
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
const bindMock = vi.fn(() => ({ run: runMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));

const makeEnv = (): Env =>
  ({
    FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
    PREDICT_QUEUE: { send: sendMock } as unknown as Env["PREDICT_QUEUE"],
  }) as unknown as Env;

const makeMessage = (overrides: Partial<PredictQueueMessage> = {}): Message<PredictQueueMessage> =>
  ({
    ack: ackMock,
    body: {
      category: "jra",
      daysAhead: 0,
      mode: "full",
      runDate: "2026-07-12",
      runDateIso: "2026-07-12",
      runYmd: "20260712",
      ...overrides,
    } satisfies PredictQueueMessage,
    retry: retryMock,
  }) as unknown as Message<PredictQueueMessage>;

const makeBatch = (messages: Message<PredictQueueMessage>[]): MessageBatch<PredictQueueMessage> =>
  ({ messages, queue: DLQ_QUEUE_NAME }) as unknown as MessageBatch<PredictQueueMessage>;

beforeEach(() => {
  ackMock.mockClear();
  retryMock.mockClear();
  sendMock.mockClear();
  sendMock.mockResolvedValue(undefined);
  runMock.mockClear();
  runMock.mockResolvedValue({ success: true });
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
  expect(prepareMock).toHaveBeenCalledTimes(1);
  expect(bindMock).toHaveBeenCalledWith("20260712", "jra", "full", "02", "01", 0, 1);
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
  expect(bindMock).toHaveBeenCalledWith("20260712", "jra", "full", "02", "01", 1, 0);
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
  expect(prepareMock).toHaveBeenCalledTimes(2);
  expect(ackMock).toHaveBeenCalledTimes(2);
});
