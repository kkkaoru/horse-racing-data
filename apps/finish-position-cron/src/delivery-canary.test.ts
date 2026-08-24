import { expect, it, vi } from "vitest";

import {
  consumeDeliveryCanary,
  enqueueDeliveryCanary,
  isDeliveryCanaryMessage,
  isDeliveryCanaryQueueMessage,
  isPredictQueueMessage,
  listDeliveryCanaries,
  shouldRunDeliveryCanaryCron,
} from "./delivery-canary";
import type { DeliveryCanaryMessage, Env, PredictQueueMessage } from "./types";

const makeEnv = (results: unknown[] = []) => {
  const runMock = vi.fn(async () => undefined);
  const allMock = vi.fn(async () => ({ results }));
  const bindMock = vi.fn((..._values: unknown[]) => ({ all: allMock, run: runMock }));
  const prepareMock = vi.fn((_sql: string) => ({ bind: bindMock }));
  const sendMock = vi.fn(async () => undefined);
  return {
    env: {
      FINISH_POSITION_CRON_DB: { prepare: prepareMock },
      PREDICT_QUEUE: { send: sendMock },
    } as unknown as Env,
    bindMock,
    prepareMock,
    runMock,
    sendMock,
  };
};

it("matches only the dedicated five-minute canary cron", () => {
  expect(shouldRunDeliveryCanaryCron("*/5 0-13 * * *")).toBe(true);
  expect(shouldRunDeliveryCanaryCron("*/10 1-11 * * *")).toBe(false);
});

it("persists a canary before enqueueing it", async () => {
  const { env, prepareMock, sendMock } = makeEnv();
  const result = await enqueueDeliveryCanary(env, new Date("2026-08-15T00:00:00Z"));
  expect(result.type).toBe("delivery-canary");
  expect(prepareMock.mock.invocationCallOrder[0]).toBeLessThan(
    sendMock.mock.invocationCallOrder[0] ?? 0,
  );
  expect(sendMock).toHaveBeenCalledWith(result);
});

it("records consumption and non-negative delivery lag", async () => {
  const { bindMock, env } = makeEnv();
  await consumeDeliveryCanary(
    env,
    { enqueuedAt: "2026-08-15T00:00:00Z", id: "id", type: "delivery-canary" },
    new Date("2026-08-15T00:01:00Z"),
  );
  expect(bindMock).toHaveBeenCalledWith(
    "id",
    "2026-08-15T00:00:00Z",
    "2026-08-15T00:01:00.000Z",
    60_000,
  );
});

it("stores null lag for an invalid enqueue timestamp and maps listed rows", async () => {
  const first = makeEnv();
  await consumeDeliveryCanary(
    first.env,
    { enqueuedAt: "invalid", id: "id", type: "delivery-canary" },
    new Date("2026-08-15T00:01:00Z"),
  );
  expect(first.bindMock.mock.calls[0]?.[3]).toBeNull();
  const listed = makeEnv([
    {
      consumed_at: "2026-08-15T00:01:00Z",
      delivery_lag_ms: 60_000,
      enqueued_at: "2026-08-15T00:00:00Z",
      id: "id",
    },
  ]);
  await expect(listDeliveryCanaries(listed.env)).resolves.toEqual([
    {
      consumedAt: "2026-08-15T00:01:00Z",
      deliveryLagMs: 60_000,
      enqueuedAt: "2026-08-15T00:00:00Z",
      id: "id",
    },
  ]);
});

it("narrows canary and prediction queue messages", () => {
  const canary: DeliveryCanaryMessage = {
    enqueuedAt: "now",
    id: "id",
    type: "delivery-canary",
  };
  const prediction: PredictQueueMessage = {
    category: "jra",
    daysAhead: 0,
    mode: "full",
    runDate: "2026-08-15",
    runDateIso: "2026-08-15",
    runYmd: "20260815",
  };
  expect(isDeliveryCanaryMessage(canary)).toBe(true);
  expect(isDeliveryCanaryMessage(prediction)).toBe(false);
  expect(isDeliveryCanaryQueueMessage({ body: canary } as never)).toBe(true);
  expect(isPredictQueueMessage({ body: prediction } as never)).toBe(true);
  expect(
    isPredictQueueMessage({
      body: {
        attempt: 1,
        category: "ban-ei",
        runYmd: "20260817",
        type: "day-base-pickup",
      },
    } as never),
  ).toBe(false);
  expect(
    isPredictQueueMessage({
      body: {
        category: "nar",
        keibajoCode: "55",
        raceBango: "10",
        runYmd: "20260823",
        type: "prediction-cache-repair",
      },
    } as never),
  ).toBe(false);
});
