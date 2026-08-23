// Run with bun. Tests for durable cleanup-only Container stop retries.

import { beforeEach, expect, test, vi } from "vitest";
import {
  CONTAINER_CLEANUP_DELAY_SECONDS,
  CONTAINER_CLEANUP_FIRST_ATTEMPT,
  CONTAINER_CLEANUP_MAX_ATTEMPT,
  consumeContainerCleanup,
  handOffContainerStopOrCleanup,
  isContainerCleanupMessage,
  isContainerCleanupQueueMessage,
} from "./container-cleanup";
import type { ContainerCleanupMessage, Env } from "./types";

const predictSendMock = vi.fn(async () => undefined);

const cleanupMessage: ContainerCleanupMessage = {
  attempt: 1,
  name: "predict-jra-2",
  role: "legacy",
  type: "container-cleanup",
  workKey: "rescore:20260823:jra:04:08",
};

const makeEnv = (): Env =>
  ({
    PREDICT_QUEUE: { send: predictSendMock },
  }) as unknown as Env;

beforeEach(() => {
  predictSendMock.mockClear();
});

test("isContainerCleanupMessage accepts an exact cleanup body", () => {
  expect(isContainerCleanupMessage(cleanupMessage)).toBe(true);
  expect(isContainerCleanupQueueMessage({ body: cleanupMessage } as never)).toBe(true);
});

test("isContainerCleanupMessage rejects malformed cleanup bodies", () => {
  expect(isContainerCleanupMessage({ ...cleanupMessage, attempt: 0 })).toBe(false);
  expect(isContainerCleanupMessage({ ...cleanupMessage, name: "" })).toBe(false);
  expect(isContainerCleanupMessage({ ...cleanupMessage, role: "unknown" })).toBe(false);
  expect(isContainerCleanupMessage({ ...cleanupMessage, workKey: "" })).toBe(false);
  expect(isContainerCleanupMessage({ ...cleanupMessage, type: "predict" })).toBe(false);
  expect(isContainerCleanupMessage(null)).toBe(false);
});

test("handOffContainerStopOrCleanup sends the exact terminal stop", async () => {
  const controlSend = vi.fn(async () => undefined);
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: { send: controlSend } as unknown as NonNullable<
      Env["CONTAINER_CONTROL_QUEUE"]
    >,
  };

  await handOffContainerStopOrCleanup({
    env,
    name: "predict-jra-race-1",
    role: "race-chain",
    workKey: "focused-full:20260823:jra:01:03",
  });

  expect(controlSend).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-race-1",
      role: "race-chain",
      type: "container-stop",
      workKey: "focused-full:20260823:jra:01:03",
    }),
  );
  expect(predictSendMock).not.toHaveBeenCalled();
});

test("handOffContainerStopOrCleanup durably schedules cleanup when stop queue is absent", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await handOffContainerStopOrCleanup({
    env: makeEnv(),
    name: "predict-jra-2",
    role: "legacy",
    workKey: "rescore:20260823:jra:04:08",
  });

  expect(predictSendMock).toHaveBeenCalledWith(cleanupMessage, { delaySeconds: 30 });
  expect(logSpy).toHaveBeenCalledWith(
    "[container-cleanup] scheduled name=predict-jra-2 role=legacy workKey=rescore:20260823:jra:04:08 attempt=1 delaySeconds=30",
  );
  logSpy.mockRestore();
});

test("handOffContainerStopOrCleanup schedules cleanup when stop queue throws", async () => {
  const controlSend = vi.fn(async (): Promise<void> => {
    throw new Error("control overloaded");
  });
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: { send: controlSend } as unknown as NonNullable<
      Env["CONTAINER_CONTROL_QUEUE"]
    >,
  };
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await handOffContainerStopOrCleanup({
    env,
    name: "predict-jra-2",
    role: "legacy",
    workKey: "rescore:20260823:jra:04:08",
  });

  expect(predictSendMock).toHaveBeenCalledWith(cleanupMessage, {
    delaySeconds: CONTAINER_CLEANUP_DELAY_SECONDS,
  });
  expect(errorSpy).toHaveBeenCalledWith(
    "[container-cleanup] stop enqueue failed name=predict-jra-2 role=legacy workKey=rescore:20260823:jra:04:08:",
    "Error: control overloaded",
  );
  errorSpy.mockRestore();
  logSpy.mockRestore();
});

test("consumeContainerCleanup hands off without scheduling another cleanup", async () => {
  const controlSend = vi.fn(async () => undefined);
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: { send: controlSend } as unknown as NonNullable<
      Env["CONTAINER_CONTROL_QUEUE"]
    >,
  };
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await consumeContainerCleanup({ env, message: { ...cleanupMessage, role: "race-chain" } });

  expect(controlSend).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-jra-2",
      role: "race-chain",
      workKey: "rescore:20260823:jra:04:08",
    }),
  );
  expect(predictSendMock).not.toHaveBeenCalled();
  expect(logSpy).toHaveBeenCalledWith(
    "[container-cleanup] handed off name=predict-jra-2 role=race-chain workKey=rescore:20260823:jra:04:08 attempt=1",
  );
  logSpy.mockRestore();
});

test("consumeContainerCleanup retries only cleanup with bounded backoff", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await consumeContainerCleanup({ env: makeEnv(), message: cleanupMessage });

  expect(predictSendMock).toHaveBeenCalledWith(
    { ...cleanupMessage, attempt: CONTAINER_CLEANUP_FIRST_ATTEMPT + 1 },
    { delaySeconds: 30 },
  );
  logSpy.mockRestore();
});

test("consumeContainerCleanup fails safe at the bounded retry limit", async () => {
  await expect(
    consumeContainerCleanup({
      env: makeEnv(),
      message: { ...cleanupMessage, attempt: CONTAINER_CLEANUP_MAX_ATTEMPT },
    }),
  ).rejects.toThrow(
    "Container cleanup exhausted name=predict-jra-2 role=legacy workKey=rescore:20260823:jra:04:08 attempt=5",
  );
  expect(predictSendMock).not.toHaveBeenCalled();
});
