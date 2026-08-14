import { expect, it, vi } from "vitest";

import {
  recordDeliveryConsumed,
  recordDeliveryDetected,
  recordDeliveryEnqueued,
  recordPredictionCompleted,
} from "./delivery-lifecycle";
import type { Env, PredictQueueMessage } from "./types";

const message = (overrides: Partial<PredictQueueMessage> = {}): PredictQueueMessage => ({
  category: "jra",
  daysAhead: 0,
  deliveryTrackingId: "tracking-id",
  keibajoCode: "05",
  mode: "full",
  raceBango: "01",
  runDate: "2026-08-15",
  runDateIso: "2026-08-15",
  runYmd: "20260815",
  ...overrides,
});

const makeEnv = () => {
  const runMock = vi.fn(async () => undefined);
  const bindMock = vi.fn((..._values: unknown[]) => ({ run: runMock }));
  const prepareMock = vi.fn((_sql: string) => ({ bind: bindMock }));
  return {
    bindMock,
    env: { FINISH_POSITION_CRON_DB: { prepare: prepareMock } } as unknown as Env,
    prepareMock,
  };
};

it("records detected identity and all three lifecycle timestamps", async () => {
  const { bindMock, env, prepareMock } = makeEnv();
  const now = new Date("2026-08-15T00:00:00Z");
  await recordDeliveryDetected(env, message(), now);
  await recordDeliveryEnqueued(env, message(), now);
  await recordDeliveryConsumed(env, message(), now);
  await recordPredictionCompleted(env, message(), now);
  expect(bindMock).toHaveBeenNthCalledWith(
    1,
    "tracking-id",
    "20260815",
    "jra",
    "05",
    "01",
    now.toISOString(),
  );
  expect(prepareMock).toHaveBeenCalledTimes(4);
  expect(prepareMock.mock.calls[1]?.[0]).toContain("enqueued_at");
  expect(prepareMock.mock.calls[2]?.[0]).toContain("consumed_at");
  expect(prepareMock.mock.calls[3]?.[0]).toContain("prediction_completed_at");
});

it("does nothing for untracked or unscoped messages", async () => {
  const { env, prepareMock } = makeEnv();
  const now = new Date();
  await recordDeliveryDetected(env, message({ deliveryTrackingId: undefined }), now);
  await recordDeliveryEnqueued(env, message({ keibajoCode: undefined }), now);
  await recordDeliveryConsumed(env, message({ raceBango: undefined }), now);
  await recordPredictionCompleted(env, message({ deliveryTrackingId: undefined }), now);
  expect(prepareMock).not.toHaveBeenCalled();
});
