// Run with bun. Tests for delayed day-base FEATURES_CACHE pickup.

import { afterEach, beforeEach, expect, test, vi } from "vitest";
import type { DayBasePickupMessage, Env } from "./types";

interface SlotClaimResult {
  proceed: boolean;
  state?: string;
}

const {
  claimDayBaseGenerationMock,
  claimContainerSlotMock,
  fanOutPredictionsAfterDayBaseHitMock,
  getFocusedFullDayBaseReadinessMock,
  headDayBaseObjectMock,
  materializeDayBasePerRaceCacheMock,
  pickUpPrewarmDayBaseWithOutcomeMock,
  releaseContainerSlotMock,
} = vi.hoisted(() => ({
  claimDayBaseGenerationMock: vi.fn(
    async (): Promise<{
      preemptedWorkKey?: string;
      proceed: boolean;
      state: "active" | "busy" | "preempting" | "superseded";
    }> => ({ proceed: true, state: "active" }),
  ),
  claimContainerSlotMock: vi.fn(async (): Promise<SlotClaimResult> => ({ proceed: true })),
  fanOutPredictionsAfterDayBaseHitMock: vi.fn(async (): Promise<number> => 1),
  getFocusedFullDayBaseReadinessMock: vi.fn(async () => ({ ready: true, reason: "ready" })),
  headDayBaseObjectMock: vi.fn(async (): Promise<{ size: number } | null> => null),
  materializeDayBasePerRaceCacheMock: vi.fn(
    async (): Promise<{ status: "materialized" } | { reason: string; status: "fallback" }> => ({
      status: "materialized",
    }),
  ),
  pickUpPrewarmDayBaseWithOutcomeMock: vi.fn(
    async (): Promise<
      "foundation-landed" | "landed" | "missing" | "rejected" | "stale" | "transient-error"
    > => "missing",
  ),
  releaseContainerSlotMock: vi.fn(async () => undefined),
}));

vi.mock("./day-base-prewarm-pickup", () => ({
  headDayBaseObject: headDayBaseObjectMock,
  pickUpPrewarmDayBaseWithOutcome: pickUpPrewarmDayBaseWithOutcomeMock,
}));
vi.mock("./day-base-race-materializer", () => ({
  materializeDayBasePerRaceCache: materializeDayBasePerRaceCacheMock,
}));
vi.mock("./feature-hit-prediction", () => ({
  fanOutPredictionsAfterDayBaseHit: fanOutPredictionsAfterDayBaseHitMock,
}));
vi.mock("./focused-full-day-base-readiness", () => ({
  getFocusedFullDayBaseReadiness: getFocusedFullDayBaseReadinessMock,
}));
vi.mock("./do-state", () => ({
  claimContainerSlot: claimContainerSlotMock,
  releaseContainerSlot: releaseContainerSlotMock,
}));
vi.mock("./predict-run-coordinator", () => ({
  claimDayBaseGeneration: claimDayBaseGenerationMock,
}));

import {
  buildDayBaseWorkKey,
  completeLandedDayBase,
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
const containerFetchMock = vi.fn(
  async () => new Response('{"type":"result","status":"accepted"}\n', { status: 200 }),
);
const containerGetMock = vi.fn(() => ({ fetch: containerFetchMock }));
const containerIdFromNameMock = vi.fn(() => ({ name: "predict-day-base" }));

const makeEnv = (): Env =>
  ({
    FINISH_POSITION_PREDICT_CONTAINER: {
      get: containerGetMock,
      idFromName: containerIdFromNameMock,
    },
    PREDICT_QUEUE: { send: queueSendMock },
  }) as unknown as Env;

const pickupBody: DayBasePickupMessage = {
  attempt: 1,
  category: "ban-ei",
  runYmd: "20260817",
  type: "day-base-pickup",
};

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-17T03:00:00.000Z"));
  queueSendMock.mockReset();
  queueSendMock.mockResolvedValue(undefined);
  claimDayBaseGenerationMock.mockReset();
  claimDayBaseGenerationMock.mockResolvedValue({ proceed: true, state: "active" });
  claimContainerSlotMock.mockReset();
  claimContainerSlotMock.mockResolvedValue({ proceed: true });
  releaseContainerSlotMock.mockReset();
  releaseContainerSlotMock.mockResolvedValue(undefined);
  containerFetchMock.mockReset();
  containerFetchMock.mockResolvedValue(
    new Response('{"type":"result","status":"accepted"}\n', { status: 200 }),
  );
  containerGetMock.mockClear();
  containerIdFromNameMock.mockClear();
  headDayBaseObjectMock.mockReset();
  materializeDayBasePerRaceCacheMock.mockReset();
  materializeDayBasePerRaceCacheMock.mockResolvedValue({ status: "materialized" });
  pickUpPrewarmDayBaseWithOutcomeMock.mockReset();
  fanOutPredictionsAfterDayBaseHitMock.mockClear();
  getFocusedFullDayBaseReadinessMock.mockReset();
  getFocusedFullDayBaseReadinessMock.mockResolvedValue({
    ready: false,
    reason: "day-base-missing-or-invalid",
  });
  headDayBaseObjectMock.mockResolvedValue(null);
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValue("missing");
});

afterEach(() => {
  vi.useRealTimers();
});

test("isDayBasePickupMessage accepts a valid delayed pickup body", () => {
  expect(isDayBasePickupMessage(pickupBody)).toBe(true);
});

test("completeLandedDayBase does not fan out when the per-race Worker cache is missing", async () => {
  materializeDayBasePerRaceCacheMock.mockResolvedValueOnce({
    reason: "manifest-missing",
    status: "fallback",
  });
  await expect(
    completeLandedDayBase({
      category: "nar",
      env: {} as Env,
      generatePredictionsAfterHit: true,
      runYmd: "20260826",
    }),
  ).rejects.toThrow(
    "per-race foundation warm failed category=nar runYmd=20260826 reason=manifest-missing",
  );
  expect(fanOutPredictionsAfterDayBaseHitMock).not.toHaveBeenCalled();
});

test("buildDayBaseWorkKey scopes new owners while retaining the legacy key", () => {
  expect(buildDayBaseWorkKey({ category: "jra", runYmd: "20260825" })).toBe(
    "day-base:20260825:jra",
  );
  expect(
    buildDayBaseWorkKey({
      category: "jra",
      generationId: "generation-a",
      runYmd: "20260825",
    }),
  ).toBe("day-base:20260825:jra:generation-a");
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

test("isDayBasePickupMessage validates the optional generation token", () => {
  expect(isDayBasePickupMessage({ ...pickupBody, generationId: "generation_1-token" })).toBe(true);
  expect(isDayBasePickupMessage({ ...pickupBody, generationId: "" })).toBe(false);
  expect(isDayBasePickupMessage({ ...pickupBody, generationId: "generation:1" })).toBe(false);
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
    generationId: "generation-test",
    runYmd: "20260817",
  });
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      force: true,
      generationId: "generation-test",
      type: DAY_BASE_PICKUP_TYPE,
    }),
    { delaySeconds: DAY_BASE_PICKUP_DELAY_SECONDS },
  );
});

test("consumeDayBasePickup does not trust an old object without a fresh pickup", async () => {
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 80 });
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });
  expect(pickUpPrewarmDayBaseWithOutcomeMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledTimes(1);
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-pickup] scheduled category=ban-ei runYmd=20260817 attempt=2 delaySeconds=180",
  );
  logSpy.mockRestore();
});

test("consumeDayBasePickup drops a past automatic pickup before readiness or Container access", async () => {
  vi.setSystemTime(new Date("2026-08-18T03:00:00.000Z"));
  const controlSend = vi.fn(async () => undefined);
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: {
      send: controlSend,
    } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  };
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({ env, message: pickupBody });

  expect(getFocusedFullDayBaseReadinessMock).not.toHaveBeenCalled();
  expect(pickUpPrewarmDayBaseWithOutcomeMock).not.toHaveBeenCalled();
  expect(containerFetchMock).not.toHaveBeenCalled();
  expect(queueSendMock).not.toHaveBeenCalled();
  expect(controlSend).toHaveBeenCalledTimes(1);
  expect(controlSend).toHaveBeenCalledWith(
    expect.objectContaining({
      acceptableWorkKeys: ["day-base:20260817:ban-ei", "day-base-stale:20260817:ban-ei"],
      name: "predict-ban-ei",
      type: "container-stop",
      workKey: "day-base:20260817:ban-ei",
    }),
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-pickup] dropping past automatic pickup category=ban-ei runYmd=20260817 attempt=1",
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup rejects a past delivery when owner-safe cleanup handoff fails", async () => {
  vi.setSystemTime(new Date("2026-08-18T03:00:00.000Z"));
  const controlSend = vi.fn(async (): Promise<void> => {
    throw new Error("control queue unavailable");
  });
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: {
      send: controlSend,
    } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  };
  queueSendMock.mockRejectedValue(new Error("cleanup queue unavailable"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await expect(consumeDayBasePickup({ env, message: pickupBody })).rejects.toThrow(
    "cleanup queue unavailable",
  );

  expect(getFocusedFullDayBaseReadinessMock).not.toHaveBeenCalled();
  expect(pickUpPrewarmDayBaseWithOutcomeMock).not.toHaveBeenCalled();
  expect(containerFetchMock).not.toHaveBeenCalled();
  expect(controlSend).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
  warnSpy.mockRestore();
});

test("consumeDayBasePickup fences an explicitly forced historical pickup", async () => {
  vi.setSystemTime(new Date("2026-08-18T03:00:00.000Z"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({ env: makeEnv(), message: { ...pickupBody, force: true } });

  expect(getFocusedFullDayBaseReadinessMock).not.toHaveBeenCalled();
  expect(pickUpPrewarmDayBaseWithOutcomeMock).not.toHaveBeenCalled();
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-ban-ei",
      role: "legacy",
      type: "container-cleanup",
      workKey: "day-base:20260817:ban-ei",
    }),
    { delaySeconds: 30 },
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup drops a superseded future generation without Container access", async () => {
  claimDayBaseGenerationMock.mockResolvedValueOnce({ proceed: false, state: "superseded" });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });

  expect(getFocusedFullDayBaseReadinessMock).not.toHaveBeenCalled();
  expect(pickUpPrewarmDayBaseWithOutcomeMock).not.toHaveBeenCalled();
  expect(containerFetchMock).not.toHaveBeenCalled();
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      name: "predict-ban-ei",
      role: "legacy",
      type: "container-cleanup",
      workKey: "day-base:20260817:ban-ei",
      acceptableWorkKeys: ["day-base:20260817:ban-ei", "day-base-stale:20260817:ban-ei"],
    },
    { delaySeconds: 30 },
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-pickup] dropping superseded generation category=ban-ei runYmd=20260817 attempt=1",
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup stops a later owner before continuing an earlier pickup", async () => {
  claimDayBaseGenerationMock.mockResolvedValueOnce({
    preemptedWorkKey: "day-base:20260819:ban-ei",
    proceed: false,
    state: "preempting",
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: { ...pickupBody, force: true, generatePredictionsAfterHit: true },
  });

  expect(getFocusedFullDayBaseReadinessMock).not.toHaveBeenCalled();
  expect(pickUpPrewarmDayBaseWithOutcomeMock).not.toHaveBeenCalled();
  expect(containerFetchMock).not.toHaveBeenCalled();
  expect(queueSendMock).toHaveBeenNthCalledWith(
    1,
    {
      attempt: 1,
      name: "predict-ban-ei",
      role: "legacy",
      type: "container-cleanup",
      workKey: "day-base:20260819:ban-ei",
    },
    { delaySeconds: 30 },
  );
  expect(queueSendMock).toHaveBeenNthCalledWith(
    2,
    {
      attempt: 2,
      category: "ban-ei",
      force: true,
      generatePredictionsAfterHit: true,
      runYmd: "20260817",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-pickup] preempting later generation category=ban-ei runYmd=20260817 attempt=1 preemptedWorkKey=day-base:20260819:ban-ei",
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup exhausts an earlier pickup after preempting its later owner", async () => {
  claimDayBaseGenerationMock.mockResolvedValueOnce({
    preemptedWorkKey: "day-base:20260819:ban-ei",
    proceed: false,
    state: "preempting",
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: { ...pickupBody, attempt: DAY_BASE_PICKUP_MAX_ATTEMPTS },
  });

  expect(queueSendMock).toHaveBeenCalledTimes(2);
  expect(queueSendMock).toHaveBeenNthCalledWith(
    2,
    {
      attempt: 1,
      name: "predict-ban-ei",
      role: "legacy",
      type: "container-cleanup",
      workKey: "day-base:20260817:ban-ei",
      acceptableWorkKeys: ["day-base:20260817:ban-ei", "day-base-stale:20260817:ban-ei"],
    },
    { delaySeconds: 30 },
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-pickup] exhausted category=ban-ei runYmd=20260817 attempt=12",
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup logs landed after a successful pickup", async () => {
  headDayBaseObjectMock.mockResolvedValueOnce({ size: 80 });
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("landed");
  getFocusedFullDayBaseReadinessMock
    .mockResolvedValueOnce({ ready: false, reason: "day-base-missing-or-invalid" })
    .mockResolvedValueOnce({ ready: true, reason: "ready" });
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      acceptableWorkKeys: ["day-base:20260817:ban-ei", "day-base-stale:20260817:ban-ei"],
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
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("landed");
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
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("landed");
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

test("consumeDayBasePickup still hands off the exact Container stop when fanout fails", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({ ready: true, reason: "ready" });
  fanOutPredictionsAfterDayBaseHitMock.mockRejectedValueOnce(new Error("fanout unavailable"));
  const send = vi.fn(async () => undefined);
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: { send } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  };

  await expect(
    consumeDayBasePickup({
      env,
      message: { ...pickupBody, generatePredictionsAfterHit: true },
    }),
  ).rejects.toThrow("fanout unavailable");

  expect(send).toHaveBeenCalledTimes(1);
  expect(send).toHaveBeenCalledWith(
    expect.objectContaining({
      acceptableWorkKeys: ["day-base:20260817:ban-ei", "day-base-stale:20260817:ban-ei"],
      name: "predict-ban-ei",
      type: "container-stop",
      workKey: "day-base:20260817:ban-ei",
    }),
  );
});

test("consumeDayBasePickup hands off one stop accepting canonical or stale owner", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({ ready: true, reason: "ready" });
  const send = vi.fn(async () => undefined);
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: { send } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  };

  await expect(consumeDayBasePickup({ env, message: pickupBody })).resolves.toBeUndefined();

  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(send).toHaveBeenCalledTimes(1);
  expect(send).toHaveBeenCalledWith(
    expect.objectContaining({
      acceptableWorkKeys: ["day-base:20260817:ban-ei", "day-base-stale:20260817:ban-ei"],
      workKey: "day-base:20260817:ban-ei",
    }),
  );
});

test("consumeDayBasePickup preserves completion and cleanup failures for Queue retry", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({ ready: true, reason: "ready" });
  fanOutPredictionsAfterDayBaseHitMock.mockRejectedValueOnce(new Error("fanout unavailable"));
  const send = vi.fn(async (): Promise<void> => {
    throw new Error("control queue unavailable");
  });
  const env = {
    ...makeEnv(),
    CONTAINER_CONTROL_QUEUE: { send } as unknown as NonNullable<Env["CONTAINER_CONTROL_QUEUE"]>,
  };
  queueSendMock.mockRejectedValue(new Error("cleanup queue unavailable"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await expect(
    consumeDayBasePickup({
      env,
      message: { ...pickupBody, generatePredictionsAfterHit: true },
    }),
  ).rejects.toThrow(
    "Day-base completion and Container cleanup failed category=ban-ei runYmd=20260817",
  );

  expect(send).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledTimes(1);
  errorSpy.mockRestore();
});

test("consumeDayBasePickup rejects a stale partial pickup and preserves fanout intent", async () => {
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("landed");
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
    expect.objectContaining({ attempt: 1, generatePredictionsAfterHit: true }),
    { delaySeconds: 180 },
  );
  expect(containerFetchMock).toHaveBeenCalledTimes(1);
  expect(claimContainerSlotMock).toHaveBeenCalledWith({
    category: "ban-ei",
    doName: "predict-ban-ei",
    env: expect.any(Object),
    kind: "day-base",
    replaceWorkKey: "day-base:20260817:ban-ei",
    staleAfterMs: 3_600_000,
    workKey: "day-base-stale:20260817:ban-ei",
  });
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
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

  expect(pickUpPrewarmDayBaseWithOutcomeMock).not.toHaveBeenCalled();
  expect(fanOutPredictionsAfterDayBaseHitMock).toHaveBeenCalledTimes(1);
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-pickup] already-landed category=ban-ei runYmd=20260817 attempt=1",
  );
  logSpy.mockRestore();
});

test("consumeDayBasePickup force waits for its Container payload instead of an old canonical", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({ ready: true, reason: "ready" });

  await consumeDayBasePickup({
    env: makeEnv(),
    message: {
      ...pickupBody,
      force: true,
      generationId: "generation-force",
    },
  });

  expect(getFocusedFullDayBaseReadinessMock).not.toHaveBeenCalled();
  expect(pickUpPrewarmDayBaseWithOutcomeMock).toHaveBeenCalledTimes(1);
  expect(materializeDayBasePerRaceCacheMock).not.toHaveBeenCalled();
  expect(queueSendMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 2,
      category: "ban-ei",
      force: true,
      generationId: "generation-force",
      runYmd: "20260817",
      type: DAY_BASE_PICKUP_TYPE,
    },
    { delaySeconds: 180 },
  );
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

test("consumeDayBasePickup keeps polling after the running-style foundation lands", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "rs-row-count-368-of-479",
  });
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("foundation-landed");
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: {
      attempt: 7,
      category: "nar",
      force: true,
      generatePredictionsAfterHit: true,
      runYmd: "20260824",
      type: "day-base-pickup",
    },
  });

  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 8,
      category: "nar",
      force: true,
      generatePredictionsAfterHit: true,
      runYmd: "20260824",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  expect(containerFetchMock).not.toHaveBeenCalled();
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(fanOutPredictionsAfterDayBaseHitMock).not.toHaveBeenCalled();
  expect(logSpy).toHaveBeenCalledWith(
    "[day-base-pickup] foundation-landed category=nar runYmd=20260824 attempt=7",
  );
  logSpy.mockRestore();
});

test("kicks running-style planning when the first future-day foundation lands", async () => {
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("foundation-landed");
  const runningStyleSendMock = vi.fn(async () => ({
    metadata: { metrics: { backlogBytes: 0, backlogCount: 0 } },
  }));
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: {
      ...makeEnv(),
      RUNNING_STYLE_PLAN_JOBS: {
        metrics: async () => ({ backlogBytes: 0, backlogCount: 0 }),
        send: runningStyleSendMock,
        sendBatch: async () => ({ metadata: { metrics: { backlogBytes: 0, backlogCount: 0 } } }),
      },
    },
    message: {
      attempt: 1,
      category: "nar",
      runYmd: "20260828",
      type: "day-base-pickup",
    },
  });

  expect(runningStyleSendMock).toHaveBeenCalledWith({
    date: "20260828",
    type: "plan-running-style-predictions",
  });
  logSpy.mockRestore();
});

test("consumeDayBasePickup stops the Container when foundation pickup is exhausted", async () => {
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("foundation-landed");
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: {
      attempt: DAY_BASE_PICKUP_MAX_ATTEMPTS,
      category: "nar",
      generatePredictionsAfterHit: true,
      runYmd: "20260824",
      type: "day-base-pickup",
    },
  });

  expect(queueSendMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      acceptableWorkKeys: ["day-base:20260824:nar", "day-base-stale:20260824:nar"],
      attempt: 1,
      name: "predict-nar",
      role: "legacy",
      type: "container-cleanup",
      workKey: "day-base:20260824:nar",
    },
    { delaySeconds: 30 },
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  expect(fanOutPredictionsAfterDayBaseHitMock).not.toHaveBeenCalled();
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-pickup] exhausted category=nar runYmd=20260824 attempt=12",
  );
  warnSpy.mockRestore();
  logSpy.mockRestore();
});

test("consumeDayBasePickup exhaustion keeps the stale lease for the stop consumer", async () => {
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("foundation-landed");
  releaseContainerSlotMock.mockRejectedValueOnce(new Error("coordinator unavailable"));
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: {
      attempt: DAY_BASE_PICKUP_MAX_ATTEMPTS,
      category: "nar",
      runYmd: "20260824",
      type: "day-base-pickup",
    },
  });

  expect(queueSendMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledWith(
    expect.objectContaining({
      name: "predict-nar",
      type: "container-cleanup",
      workKey: "day-base:20260824:nar",
    }),
    { delaySeconds: 30 },
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
  logSpy.mockRestore();
});

test("consumeDayBasePickup rebuilds when canonical R2 is stale and the Container candidate is missing", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "rs-row-count-368-of-479",
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: {
      attempt: 10,
      category: "nar",
      generatePredictionsAfterHit: true,
      runYmd: "20260824",
      type: "day-base-pickup",
    },
  });

  expect(containerFetchMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      category: "nar",
      generatePredictionsAfterHit: true,
      runYmd: "20260824",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup rebuilds for a deterministic source watermark mismatch", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "source-watermark-mismatch",
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });

  expect(containerFetchMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      category: "ban-ei",
      runYmd: "20260817",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup rebuilds for a deterministic source row-count mismatch", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "source-row-count-455-of-479",
  });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });

  expect(containerFetchMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      category: "ban-ei",
      runYmd: "20260817",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup rebuilds for a deterministic RS timestamp mismatch", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "rs-predicted-at-max-mismatch",
  });
  containerFetchMock.mockResolvedValueOnce(
    new Response('{"type":"result","status":"success"}\n', { status: 200 }),
  );
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });

  expect(containerFetchMock).toHaveBeenCalledTimes(1);
  expect(releaseContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-ban-ei",
    env: expect.any(Object),
    kind: "day-base",
    workKey: "day-base-stale:20260817:ban-ei",
  });
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      category: "ban-ei",
      runYmd: "20260817",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup does not rebuild for a transient readiness probe error", async () => {
  getFocusedFullDayBaseReadinessMock.mockRejectedValueOnce(new Error("Catalog unavailable"));
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

  await consumeDayBasePickup({ env: makeEnv(), message: pickupBody });

  expect(containerFetchMock).not.toHaveBeenCalled();
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 2,
      category: "ban-ei",
      runYmd: "20260817",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  logSpy.mockRestore();
});

test("consumeDayBasePickup rebuilds a stale candidate and resets the pickup window", async () => {
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("stale");
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: {
      attempt: 10,
      category: "nar",
      force: true,
      generatePredictionsAfterHit: true,
      runYmd: "20260824",
      type: "day-base-pickup",
    },
  });

  expect(containerIdFromNameMock).toHaveBeenCalledWith("predict-nar");
  expect(containerFetchMock).toHaveBeenCalledWith(
    new Request("http://do/prewarm-day-base?category=nar&daysAhead=0&runDate=20260824"),
  );
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      category: "nar",
      force: true,
      generatePredictionsAfterHit: true,
      runYmd: "20260824",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-pickup] stale candidate rebuild started category=nar runYmd=20260824",
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("concurrent stale pickups allow only one Container restart", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValue({
    ready: false,
    reason: "rs-row-count-368-of-479",
  });
  claimContainerSlotMock
    .mockResolvedValueOnce({ proceed: true })
    .mockResolvedValueOnce({ proceed: false, state: "busy" });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await Promise.all([
    consumeDayBasePickup({
      env: makeEnv(),
      message: { attempt: 7, category: "nar", runYmd: "20260824", type: "day-base-pickup" },
    }),
    consumeDayBasePickup({
      env: makeEnv(),
      message: { attempt: 7, category: "nar", runYmd: "20260824", type: "day-base-pickup" },
    }),
  ]);

  expect(claimContainerSlotMock).toHaveBeenCalledTimes(2);
  expect(containerFetchMock).toHaveBeenCalledTimes(1);
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 1,
      category: "nar",
      runYmd: "20260824",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 8,
      category: "nar",
      runYmd: "20260824",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("a busy stale-restart slot does not fetch or reset the pickup attempt", async () => {
  getFocusedFullDayBaseReadinessMock.mockResolvedValueOnce({
    ready: false,
    reason: "rs-row-count-368-of-479",
  });
  claimContainerSlotMock.mockResolvedValueOnce({ proceed: false, state: "busy" });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: { attempt: 7, category: "nar", runYmd: "20260824", type: "day-base-pickup" },
  });

  expect(containerFetchMock).not.toHaveBeenCalled();
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 8,
      category: "nar",
      runYmd: "20260824",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  expect(releaseContainerSlotMock).not.toHaveBeenCalled();
  warnSpy.mockRestore();
});

test("consumeDayBasePickup keeps the existing retry window when stale rebuild fails", async () => {
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("stale");
  containerFetchMock.mockResolvedValueOnce(new Response("unavailable", { status: 503 }));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: { attempt: 3, category: "nar", runYmd: "20260824", type: "day-base-pickup" },
  });

  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 4,
      category: "nar",
      runYmd: "20260824",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-pickup] stale rebuild failed category=nar runYmd=20260824 status=503",
  );
  expect(releaseContainerSlotMock).toHaveBeenCalledWith({
    doName: "predict-nar",
    env: expect.any(Object),
    kind: "day-base",
    workKey: "day-base-stale:20260824:nar",
  });
  errorSpy.mockRestore();
});

test("consumeDayBasePickup does not reset the pickup window for a 200 error result", async () => {
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("stale");
  containerFetchMock.mockResolvedValueOnce(
    new Response('{"type":"result","status":"error"}\n', { status: 200 }),
  );
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: { attempt: 3, category: "nar", runYmd: "20260824", type: "day-base-pickup" },
  });

  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 4,
      category: "nar",
      runYmd: "20260824",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-pickup] stale rebuild rejected category=nar runYmd=20260824",
  );
  errorSpy.mockRestore();
});

test("consumeDayBasePickup retries when the stale rebuild request rejects", async () => {
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("stale");
  containerFetchMock.mockImplementationOnce(async (): Promise<Response> => {
    throw new Error("container unavailable");
  });
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

  await consumeDayBasePickup({
    env: makeEnv(),
    message: { attempt: 3, category: "nar", runYmd: "20260824", type: "day-base-pickup" },
  });

  expect(queueSendMock).toHaveBeenCalledWith(
    {
      attempt: 4,
      category: "nar",
      runYmd: "20260824",
      type: "day-base-pickup",
    },
    { delaySeconds: 180 },
  );
  expect(errorSpy).toHaveBeenCalledWith(
    "[day-base-pickup] stale rebuild request failed category=nar runYmd=20260824: Error: container unavailable",
  );
  errorSpy.mockRestore();
});

test("consumeDayBasePickup stops after the last attempt", async () => {
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);
  await consumeDayBasePickup({
    env: makeEnv(),
    message: { ...pickupBody, attempt: DAY_BASE_PICKUP_MAX_ATTEMPTS },
  });
  expect(queueSendMock).toHaveBeenCalledWith(
    {
      acceptableWorkKeys: ["day-base:20260817:ban-ei", "day-base-stale:20260817:ban-ei"],
      attempt: 1,
      name: "predict-ban-ei",
      role: "legacy",
      type: "container-cleanup",
      workKey: "day-base:20260817:ban-ei",
    },
    { delaySeconds: 30 },
  );
  expect(warnSpy).toHaveBeenCalledWith(
    "[day-base-pickup] exhausted category=ban-ei runYmd=20260817 attempt=12",
  );
  warnSpy.mockRestore();
});

test("consumeDayBasePickup preserves the Container after a transient pickup failure", async () => {
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("transient-error");

  await expect(
    consumeDayBasePickup({
      env: makeEnv(),
      message: { ...pickupBody, attempt: DAY_BASE_PICKUP_MAX_ATTEMPTS },
    }),
  ).rejects.toThrow("Transient day-base pickup failure category=ban-ei runYmd=20260817 attempt=12");
  expect(queueSendMock).not.toHaveBeenCalled();
});

test("consumeDayBasePickup preserves the Container after canonical readiness fails", async () => {
  getFocusedFullDayBaseReadinessMock.mockRejectedValueOnce(new Error("D1 unavailable"));

  await expect(
    consumeDayBasePickup({
      env: makeEnv(),
      message: { ...pickupBody, attempt: DAY_BASE_PICKUP_MAX_ATTEMPTS },
    }),
  ).rejects.toThrow(
    "Transient day-base readiness failure category=ban-ei runYmd=20260817 attempt=12",
  );
  expect(queueSendMock).not.toHaveBeenCalled();
});

test("consumeDayBasePickup schedules cleanup when control queue send throws", async () => {
  pickUpPrewarmDayBaseWithOutcomeMock.mockResolvedValueOnce("landed");
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
      acceptableWorkKeys: ["day-base:20260817:ban-ei", "day-base-stale:20260817:ban-ei"],
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
