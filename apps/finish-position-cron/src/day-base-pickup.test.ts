// Run with bun. Tests for delayed day-base FEATURES_CACHE pickup.

import { beforeEach, expect, test, vi } from "vitest";
import type { DayBasePickupMessage, Env } from "./types";

const {
  fanOutPredictionsAfterDayBaseHitMock,
  getFocusedFullDayBaseReadinessMock,
  headDayBaseObjectMock,
  pickUpPrewarmDayBaseMock,
} = vi.hoisted(() => ({
  fanOutPredictionsAfterDayBaseHitMock: vi.fn(async (): Promise<number> => 1),
  getFocusedFullDayBaseReadinessMock: vi.fn(async () => ({ ready: true, reason: "ready" })),
  headDayBaseObjectMock: vi.fn(async (): Promise<{ size: number } | null> => null),
  pickUpPrewarmDayBaseMock: vi.fn(async (): Promise<boolean> => false),
}));

vi.mock("./day-base-prewarm-pickup", () => ({
  headDayBaseObject: headDayBaseObjectMock,
  pickUpPrewarmDayBase: pickUpPrewarmDayBaseMock,
}));
vi.mock("./feature-hit-prediction", () => ({
  fanOutPredictionsAfterDayBaseHit: fanOutPredictionsAfterDayBaseHitMock,
}));
vi.mock("./focused-full-day-base-readiness", () => ({
  getFocusedFullDayBaseReadiness: getFocusedFullDayBaseReadinessMock,
}));

import {
  consumeDayBasePickup,
  DAY_BASE_PICKUP_DELAY_SECONDS,
  DAY_BASE_PICKUP_FIRST_ATTEMPT,
  DAY_BASE_PICKUP_MAX_ATTEMPTS,
  DAY_BASE_PICKUP_TYPE,
  enqueueDayBasePickup,
  isDayBasePickupMessage,
  isDayBasePickupQueueMessage,
} from "./day-base-pickup";

const queueSendMock = vi.fn(async () => undefined);

const makeEnv = (): Env =>
  ({
    PREDICT_QUEUE: { send: queueSendMock },
  }) as unknown as Env;

const pickupBody: DayBasePickupMessage = {
  attempt: 1,
  category: "ban-ei",
  runYmd: "20260817",
  type: "day-base-pickup",
};

beforeEach(() => {
  queueSendMock.mockClear();
  headDayBaseObjectMock.mockReset();
  pickUpPrewarmDayBaseMock.mockReset();
  fanOutPredictionsAfterDayBaseHitMock.mockClear();
  getFocusedFullDayBaseReadinessMock.mockReset();
  getFocusedFullDayBaseReadinessMock.mockResolvedValue({
    ready: false,
    reason: "day-base-missing-or-invalid",
  });
  headDayBaseObjectMock.mockResolvedValue(null);
  pickUpPrewarmDayBaseMock.mockResolvedValue(false);
});

test("isDayBasePickupMessage accepts a valid delayed pickup body", () => {
  expect(isDayBasePickupMessage(pickupBody)).toBe(true);
});

test("isDayBasePickupMessage rejects a predict body", () => {
  expect(
    isDayBasePickupMessage({
      category: "ban-ei",
      daysAhead: 2,
      mode: "full",
      runDate: "2026-08-17",
      runDateIso: "2026-08-17",
      runYmd: "20260817",
    }),
  ).toBe(false);
});

test("isDayBasePickupMessage rejects a zero attempt", () => {
  expect(isDayBasePickupMessage({ ...pickupBody, attempt: 0 })).toBe(false);
});

test("isDayBasePickupMessage rejects an empty category", () => {
  expect(
    isDayBasePickupMessage({
      attempt: 1,
      category: "",
      runYmd: "20260817",
      type: "day-base-pickup",
    }),
  ).toBe(false);
});

test("isDayBasePickupMessage rejects an empty runYmd", () => {
  expect(
    isDayBasePickupMessage({
      attempt: 1,
      category: "ban-ei",
      runYmd: "",
      type: "day-base-pickup",
    }),
  ).toBe(false);
});

test("isDayBasePickupMessage validates the optional feature-hit generation flag", () => {
  expect(isDayBasePickupMessage({ ...pickupBody, generatePredictionsAfterHit: true })).toBe(true);
  expect(isDayBasePickupMessage({ ...pickupBody, generatePredictionsAfterHit: "yes" })).toBe(false);
});

test("isDayBasePickupMessage validates the optional historical force flag", () => {
  expect(isDayBasePickupMessage({ ...pickupBody, force: true })).toBe(true);
  expect(isDayBasePickupMessage({ ...pickupBody, force: "yes" })).toBe(false);
});

test("isDayBasePickupQueueMessage reads the message body", () => {
  expect(isDayBasePickupQueueMessage({ body: pickupBody } as never)).toBe(true);
  expect(
    isDayBasePickupQueueMessage({
      body: { type: "delivery-canary", id: "x", enqueuedAt: "now" },
    } as never),
  ).toBe(false);
});

test("enqueueDayBasePickup sends a delayed queue message", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await enqueueDayBasePickup({
    attempt: DAY_BASE_PICKUP_FIRST_ATTEMPT,
    category: "ban-ei",
    env: makeEnv(),
    runYmd: "20260817",
  });
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      category: "ban-ei",
      runYmd: "20260817",
      type: DAY_BASE_PICKUP_TYPE,
    },
    { delaySeconds: DAY_BASE_PICKUP_DELAY_SECONDS },
  );
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-pickup] scheduled category=ban-ei runYmd=20260817 attempt=1 delaySeconds=180",
  );
  logSpy.mockRestore();
});

test("enqueueDayBasePickup preserves an explicit historical force flag", async () => {
  await enqueueDayBasePickup({
    attempt: DAY_BASE_PICKUP_FIRST_ATTEMPT,
    category: "jra",
    env: makeEnv(),
    force: true,
    runYmd: "20260817",
  });
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ force: true, type: DAY_BASE_PICKUP_TYPE }),
    { delaySeconds: DAY_BASE_PICKUP_DELAY_SECONDS },
  );
});

test("consumeDayBasePickup does not trust an old object without a fresh pickup", async () => {
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 80 });
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });
  expect(pickUpPrewarmDayBaseMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledTimes(1);
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-pickup] scheduled category=ban-ei runYmd=20260817 attempt=2 delaySeconds=180",
  );
  logSpy.mockRestore();
});

test("consumeDayBasePickup logs landed after a successful pickup", async () => {
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 80 });
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  getFocusedFullDayBaseReadinessMock
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "ready" });
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      name: "predict-ban-ei",
      role: "legacy",
      type: "container-cleanup",
      workKey: "day-base:20260817:ban-ei",
    },
    { delaySeconds: 30 },
  );
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-pickup] landed category=ban-ei runYmd=20260817 attempt=1",
  );
  logSpy.mockRestore();
});

test("consumeDayBasePickup queues a work-owned stop when the control queue is bound", async () => {
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 80 });
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  getFocusedFullDayBaseReadinessMock
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "ready" });
  const send = vi.fn(async () => undefined);
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: { send } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  };

  await consumeDayBasePickup({ env, message: pickupBody });

  expect(send).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-ban-ei",
      type: "container-stop",
      workKey: "day-base:20260817:ban-ei",
    }),
  );
});

test("consumeDayBasePickup fans out only after a fresh pickup lands", async () => {
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 80 });
  getFocusedFullDayBaseReadinessMock
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "ready" });
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await consumeDayBasePickup({
    env: makeEnv(),
    message: { ...pickupBody, generatePredictionsAfterHit: true },
  });
  expect(fanOutPredictionsAfterDayBaseHitMock).toHaveBeenCalledWith({
    category: "ban-ei",
    env: expect.any(Object),
    runYmd: "20260817",
  });
  logSpy.mockRestore();
});

test("consumeDayBasePickup rejects a stale partial pickup and preserves fanout intent", async () => {
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  getFocusedFullDayBaseReadinessMock
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({
      ready: false,
      reason: "source-row-count-26-of-392",
    });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: { ...pickupBody, generatePredictionsAfterHit: true },
  });

  expect(fanOutPredictionsAfterDayBaseHitMock).not.toHaveBeenCalled();
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({ attempt: 2, generatePredictionsAfterHit: true }),
    { delaySeconds: 180 },
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-pickup] rejected stale pickup category=ban-ei runYmd=20260817 attempt=1 reason=source-row-count-26-of-392",
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup does not wake the Container after the live generation already landed", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({ ready: true, reason: "ready" });
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: { ...pickupBody, generatePredictionsAfterHit: true },
  });

  expect(pickUpPrewarmDayBaseMock).not.toHaveBeenCalled();
  expect(fanOutPredictionsAfterDayBaseHitMock).toHaveBeenCalledTimes(1);
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-pickup] already-landed category=ban-ei runYmd=20260817 attempt=1",
  );
  logSpy.mockRestore();
});

test("consumeDayBasePickup re-enqueues when pickup still misses", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 2,
      category: "ban-ei",
      runYmd: "20260817",
      type: DAY_BASE_PICKUP_TYPE,
    },
    { delaySeconds: 180 },
  );
  logSpy.mockRestore();
});

test("consumeDayBasePickup preserves the feature-hit generation intent while retrying", async () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await consumeDayBasePickup({
    env: makeEnv(),
    message: { ...pickupBody, generatePredictionsAfterHit: true },
  });
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 2,
      category: "ban-ei",
      generatePredictionsAfterHit: true,
      runYmd: "20260817",
      type: DAY_BASE_PICKUP_TYPE,
    },
    { delaySeconds: 180 },
  );
  expect(fanOutPredictionsAfterDayBaseHitMock).not.toHaveBeenCalled();
  logSpy.mockRestore();
});

test("consumeDayBasePickup stops after the last attempt", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await consumeDayBasePickup({
    env: makeEnv(),
    message: { ...pickupBody, attempt: DAY_BASE_PICKUP_MAX_ATTEMPTS },
  });
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      name: "predict-ban-ei",
      role: "legacy",
      type: "container-cleanup",
      workKey: "day-base:20260817:ban-ei",
    },
    { delaySeconds: 30 },
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-pickup] exhausted category=ban-ei runYmd=20260817 attempt=11",
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup schedules cleanup when control queue send throws", async () => {
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  getFocusedFullDayBaseReadinessMock
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "ready" });
  const send = vi.fn(async (): Promise<void> => {
    throw new Error("control queue overloaded");
  });
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: { send } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  };
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await consumeDayBasePickup({ env, message: pickupBody });

  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      name: "predict-ban-ei",
      role: "legacy",
      type: "container-cleanup",
      workKey: "day-base:20260817:ban-ei",
    },
    { delaySeconds: 30 },
  );
  expect(errorSpy).toHaveBeenCalledWith(
    "[container-cleanup] stop enqueue failed name=predict-ban-ei role=legacy workKey=day-base:20260817:ban-ei:",
    "Error: control queue overloaded",
  );
  errorSpy.mockRestore();
});
