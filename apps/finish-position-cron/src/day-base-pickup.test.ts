// Run with bun. Tests for delayed day-base FEATURES_CACHE pickup.

import { beforeEach, expect, test, vi } from "vitest";
import type { DayBasePickupMessage, Env } from "./types";

const {
  fanOutPredictionsAfterDayBaseHitMock,
  headDayBaseObjectMock,
  pickUpPrewarmDayBaseMock,
  releaseContainerSlotMock,
} = vi.hoisted(() => ({
  fanOutPredictionsAfterDayBaseHitMock: vi.fn(async (): Promise<number> => 1),
  headDayBaseObjectMock: vi.fn(async (): Promise<{ size: number } | null> => null),
  pickUpPrewarmDayBaseMock: vi.fn(async (): Promise<boolean> => false),
  releaseContainerSlotMock: vi.fn(async () => undefined),
}));

vi.mock("./day-base-prewarm-pickup", () => ({
  headDayBaseObject: headDayBaseObjectMock,
  pickUpPrewarmDayBase: pickUpPrewarmDayBaseMock,
}));
vi.mock("./do-state", () => ({ releaseContainerSlot: releaseContainerSlotMock }));
vi.mock("./feature-hit-prediction", () => ({
  fanOutPredictionsAfterDayBaseHit: fanOutPredictionsAfterDayBaseHitMock,
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
  releaseContainerSlotMock.mockClear();
  fanOutPredictionsAfterDayBaseHitMock.mockClear();
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
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });
  expect(queueSendMock).not.toHaveBeenCalled();
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-pickup] landed category=ban-ei runYmd=20260817 attempt=1",
  );
  expect(releaseContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-ban-ei",
    env: expect.any(Object),
    kind: "day-base",
  });
  logSpy.mockRestore();
});

test("consumeDayBasePickup fans out only after a fresh pickup lands", async () => {
  pickUpPrewarmDayBaseMock.mockResolvedValueOnce(true);
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 80 });
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
  expect(queueSendMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-pickup] exhausted category=ban-ei runYmd=20260817 attempt=6",
  );
  warnSpy.mockRestore();
});
