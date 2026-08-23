// Run with bun. Tests for the dead-letter queue consumer.

import { beforeEach, expect, test, vi } from "vitest";
import type { RetryErrorLookupRow } from "./retry-errors";
import type { Env, PredictQueueMessage } from "./types";

const { checkContainerSlotStopMock, clearContainerSlotMock, completeFocusedFullRaceMock } =
  vi.hoisted(() => {
    const checkContainerSlotStop = vi.fn(async () => true);
    const clearContainerSlot = vi.fn(async () => undefined);
    const completeFocusedFullRace = vi.fn(async () => undefined);
    return {
      checkContainerSlotStopMock: checkContainerSlotStop,
      clearContainerSlotMock: clearContainerSlot,
      completeFocusedFullRaceMock: completeFocusedFullRace,
    };
  });

const { isOldDateRunYmdMock } = vi.hoisted(() => ({
  isOldDateRunYmdMock: vi.fn(() => false),
}));

vi.mock("./do-state", () => ({
  checkContainerSlotStop: checkContainerSlotStopMock,
  clearContainerSlot: clearContainerSlotMock,
  completeFocusedFullRace: completeFocusedFullRaceMock,
}));

vi.mock("./old-date-guard", () => ({
  isOldDateRunYmd: isOldDateRunYmdMock,
}));

import { DLQ_QUEUE_NAME, handleDlqQueue } from "./dlq-consumer";

const ackMock = vi.fn();
const retryMock = vi.fn();
const sendMock = vi.fn();
const controlSendMock = vi.fn();
const runMock = vi.fn(async () => ({ success: true }));
const firstMock = vi.fn(async (): Promise<RetryErrorLookupRow | null> => null);
const bindMock = vi.fn(() => ({ first: firstMock, run: runMock }));
const prepareMock = vi.fn(() => ({ bind: bindMock }));
const containerFetchMock = vi.fn(async () => new Response(null, { status: 204 }));

const makeEnv = (): Env =>
  ({
    FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
    FINISH_POSITION_PREDICT_CONTAINER: {
      get: vi.fn(() => ({ fetch: containerFetchMock })),
      idFromName: vi.fn((name: string) => ({ name })),
    } as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
    CONTAINER_CONTROL_QUEUE: {
      send: controlSendMock,
    } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
    PREDICT_QUEUE: { send: sendMock } as unknown as Env["PREDICT_QUEUE"],
    PREDICT_RUN_COORDINATOR: {} as Env["PREDICT_RUN_COORDINATOR"],
    TRIGGER_TOKEN: "secret-token",
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
  controlSendMock.mockClear();
  controlSendMock.mockResolvedValue(undefined);
  containerFetchMock.mockClear();
  containerFetchMock.mockResolvedValue(new Response(null, { status: 204 }));
  checkContainerSlotStopMock.mockClear();
  checkContainerSlotStopMock.mockResolvedValue(true);
  runMock.mockClear();
  runMock.mockResolvedValue({ success: true });
  firstMock.mockClear();
  firstMock.mockResolvedValue(null);
  bindMock.mockClear();
  prepareMock.mockClear();
  completeFocusedFullRaceMock.mockClear();
  completeFocusedFullRaceMock.mockResolvedValue(undefined);
  clearContainerSlotMock.mockClear();
  clearContainerSlotMock.mockResolvedValue(undefined);
  isOldDateRunYmdMock.mockReset();
  isOldDateRunYmdMock.mockReturnValue(false);
});

test("DLQ_QUEUE_NAME matches the dead-letter queue name in wrangler.jsonc", () => {
  expect(DLQ_QUEUE_NAME).toBe("finish-position-predict-dlq");
});

test("acks a delivery canary in the DLQ without recording primary consumption", async () => {
  const ack = vi.fn();
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await handleDlqQueue(
    {
      messages: [
        {
          ack,
          body: {
            enqueuedAt: "2026-08-15T00:00:00Z",
            id: "canary-id",
            type: "delivery-canary",
          },
          retry: vi.fn(),
        },
      ],
    } as never,
    makeEnv(),
  );
  expect(ack).toHaveBeenCalledTimes(1);
  expect(prepareMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith("[predict-dlq] delivery canary reached DLQ id=canary-id");
});

test("acks a day-base pickup in the DLQ without redriving it as a predict", async () => {
  const ack = vi.fn();
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await handleDlqQueue(
    {
      messages: [
        {
          ack,
          body: {
            attempt: 3,
            category: "ban-ei",
            runYmd: "20260817",
            type: "day-base-pickup",
          },
          retry: vi.fn(),
        },
      ],
    } as never,
    makeEnv(),
  );
  expect(ack).toHaveBeenCalledTimes(1);
  expect(sendMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "[predict-dlq] day-base pickup reached DLQ category=ban-ei runYmd=20260817 attempt=3",
  );
  errorSpy.mockRestore();
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
    "dlq-msg-1",
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

test("queues terminal container cleanup for an exhausted focused-full message", async () => {
  await handleDlqQueue(
    makeBatch([
      makeMessage({
        dlqRedriveCount: 1,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        skipDedup: true,
      }),
    ]),
    makeEnv(),
  );

  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra",
      type: "container-stop",
      workKey: "focused-full:20260712:jra:02:01",
    }),
  );
  expect(clearContainerSlotMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("queues terminal shard cleanup for an exhausted rescore message", async () => {
  await handleDlqQueue(
    makeBatch([
      makeMessage({
        category: "nar",
        dlqRedriveCount: 1,
        keibajoCode: "35",
        mode: "rescore",
        raceBango: "08",
      }),
    ]),
    { ...makeEnv(), RACE_SHARDED_DO: "1" },
  );

  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-nar-0",
      type: "container-stop",
      workKey: "rescore:20260712:nar:35:08",
    }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
});

test("retries exhausted focused-full cleanup when the control queue is unavailable", async () => {
  const env = { ...makeEnv(), CONTAINER_CONTROL_QUEUE: undefined };
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await handleDlqQueue(
    makeBatch([
      makeMessage({
        dlqRedriveCount: 1,
        keibajoCode: "02",
        mode: "full",
        raceBango: "01",
        skipDedup: true,
      }),
    ]),
    env,
  );

  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  consoleError.mockRestore();
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
    "dlq-msg-1",
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
    "dlq-msg-1",
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
    "dlq-msg-1",
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
    "dlq-msg-1",
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
    "dlq-msg-1",
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
    null,
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

test("does not redrive an old non-force prediction from the DLQ", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleDlqQueue(
    makeBatch([makeMessage({ keibajoCode: "02", mode: "full", raceBango: "01", skipDedup: true })]),
    makeEnv(),
  );
  expect(sendMock).not.toHaveBeenCalled();
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      type: "container-stop",
      workKey: "focused-full:20260712:jra:02:01",
    }),
  );
  expect(bindMock).toHaveBeenCalledWith(
    "20260712",
    "jra",
    "full",
    "02",
    "01",
    "dlq-msg-1",
    0,
    0,
    ...nullFailureBindTail,
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("keeps the explicit historical force path redrivable from the DLQ", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await handleDlqQueue(makeBatch([makeMessage({ force: true })]), makeEnv());
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, dlqRedriveCount: 1 }),
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  warnSpy.mockRestore();
});

test("cleans repair and legacy container ownership for old non-force day-base DLQ work", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await handleDlqQueue(
    {
      messages: [
        {
          ack: ackMock,
          body: {
            attempt: 4,
            category: "nar",
            runYmd: "20260823",
            type: "day-base-pickup",
          },
          retry: retryMock,
        },
      ],
    } as never,
    makeEnv(),
  );
  expect(runMock).toHaveBeenCalledTimes(1);
  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-nar",
      role: "legacy",
      type: "container-stop",
      workKey: "day-base:20260823:nar",
    }),
  );
  expect(sendMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
});

test("acks expired day-base DLQ work after logging best-effort cleanup failures", async () => {
  isOldDateRunYmdMock.mockReturnValue(true);
  runMock.mockRejectedValueOnce(new Error("D1 unavailable"));
  controlSendMock.mockRejectedValueOnce(new Error("control queue unavailable"));
  sendMock.mockRejectedValueOnce(new Error("cleanup queue unavailable"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await handleDlqQueue(
    {
      messages: [
        {
          ack: ackMock,
          body: {
            category: "jra",
            daysAhead: 0,
            requestedAt: "2026-08-23T00:00:00.000Z",
            runYmd: "20260823",
            type: "day-base-prewarm",
          },
          retry: retryMock,
        },
      ],
    } as never,
    makeEnv(),
  );
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringContaining("old day-base repair cleanup failed"),
    "Error: D1 unavailable",
  );
  expect(errorSpy).toHaveBeenCalledWith(
    expect.stringContaining("old day-base container cleanup failed"),
    "Error: cleanup queue unavailable",
  );
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  errorSpy.mockRestore();
});

test("acks exhausted container control and day-base prewarm messages with an error log", async () => {
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const messages = [
    {
      ack: ackMock,
      body: {
        force: true,
        name: "predict-jra-0",
        requestedAt: "2026-08-22T00:00:00.000Z",
        type: "container-stop",
      },
      retry: retryMock,
    },
    {
      ack: ackMock,
      body: {
        category: "jra",
        daysAhead: 0,
        requestedAt: "2026-08-22T00:00:00.000Z",
        runYmd: "20260822",
        type: "day-base-prewarm",
      },
      retry: retryMock,
    },
  ];
  await handleDlqQueue(
    { messages, queue: DLQ_QUEUE_NAME } as unknown as MessageBatch<
      import("./types").PredictQueueBody | import("./types").ContainerControlMessage
    >,
    makeEnv(),
  );
  expect(ackMock).toHaveBeenCalledTimes(2);
  expect(runMock).not.toHaveBeenCalled();
  expect(consoleError).toHaveBeenCalledTimes(2);
  consoleError.mockRestore();
});

test("retries an exhausted container stop when the direct DLQ attempt fails", async () => {
  containerFetchMock.mockRejectedValueOnce(new Error("container unavailable"));
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const message = {
    ack: ackMock,
    body: {
      force: true,
      name: "predict-jra-0",
      requestedAt: "2026-08-22T00:00:00.000Z",
      type: "container-stop",
    },
    retry: retryMock,
  };

  await handleDlqQueue(
    { messages: [message], queue: DLQ_QUEUE_NAME } as unknown as MessageBatch<
      import("./types").ContainerControlMessage
    >,
    makeEnv(),
  );

  expect(retryMock).toHaveBeenCalledTimes(1);
  expect(ackMock).not.toHaveBeenCalled();
  consoleError.mockRestore();
});

test("acks cleanup-only DLQ work after handing off the exact stop", async () => {
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const consoleLog = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const message = {
    ack: ackMock,
    body: {
      attempt: 5,
      name: "predict-jra-2",
      role: "legacy",
      type: "container-cleanup",
      workKey: "rescore:20260823:jra:04:08",
    },
    retry: retryMock,
  };

  await handleDlqQueue(
    { messages: [message], queue: DLQ_QUEUE_NAME } as unknown as MessageBatch<
      import("./types").ContainerCleanupMessage
    >,
    makeEnv(),
  );

  expect(controlSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-2",
      role: "legacy",
      type: "container-stop",
      workKey: "rescore:20260823:jra:04:08",
    }),
  );
  expect(sendMock).not.toHaveBeenCalled();
  expect(runMock).not.toHaveBeenCalled();
  expect(ackMock).toHaveBeenCalledTimes(1);
  expect(retryMock).not.toHaveBeenCalled();
  consoleError.mockRestore();
  consoleLog.mockRestore();
});

test("bounded-retries cleanup-only DLQ work without prediction redrive", async () => {
  controlSendMock.mockRejectedValue(new Error("control queue unavailable"));
  const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await handleDlqQueue(
    {
      messages: [
        {
          ack: ackMock,
          body: {
            attempt: 5,
            name: "predict-jra-2",
            role: "legacy",
            type: "container-cleanup",
            workKey: "rescore:20260823:jra:04:08",
          },
          retry: retryMock,
        },
      ],
      queue: DLQ_QUEUE_NAME,
    } as unknown as MessageBatch<import("./types").ContainerCleanupMessage>,
    makeEnv(),
  );

  expect(sendMock).not.toHaveBeenCalled();
  expect(runMock).not.toHaveBeenCalled();
  expect(ackMock).not.toHaveBeenCalled();
  expect(retryMock).toHaveBeenCalledTimes(1);
  consoleError.mockRestore();
});
