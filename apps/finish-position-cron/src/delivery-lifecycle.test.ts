import { expect, it, vi } from "vitest";

import {
  recordDeliveryConsumed,
  recordDeliveryDetected,
  recordDeliveryEnqueued,
  recordPredictionCompleted,
  recordPreweightDisplayCompleted,
  recordPreweightGenerationStarted,
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
    "full",
    now.toISOString(),
  );
  expect(prepareMock).toHaveBeenCalledTimes(4);
  expect(prepareMock.mock.calls[1]?.[0]).toContain("enqueued_at");
  expect(prepareMock.mock.calls[2]?.[0]).toContain("consumed_at");
  expect(prepareMock.mock.calls[3]?.[0]).toContain("prediction_completed_at");
});

it("derives a stable timing identity for ordinary full builds", async () => {
  const { bindMock, env, prepareMock } = makeEnv();
  const now = new Date("2026-08-15T00:00:00Z");
  await recordDeliveryDetected(env, message({ deliveryTrackingId: undefined }), now);
  expect(bindMock).toHaveBeenCalledWith(
    "preweight:20260815:jra:05:01",
    "20260815",
    "jra",
    "05",
    "01",
    "full",
    now.toISOString(),
  );
  expect(prepareMock).toHaveBeenCalledTimes(1);
});

it("does nothing for post-weight or unscoped messages", async () => {
  const { env, prepareMock } = makeEnv();
  const now = new Date();
  await recordDeliveryDetected(
    env,
    message({ deliveryTrackingId: undefined, mode: "rescore" }),
    now,
  );
  await recordDeliveryEnqueued(env, message({ keibajoCode: undefined }), now);
  await recordDeliveryConsumed(env, message({ raceBango: undefined }), now);
  await recordPredictionCompleted(
    env,
    message({ deliveryTrackingId: undefined, mode: "rescore" }),
    now,
  );
  expect(prepareMock).not.toHaveBeenCalled();
});

it("records generation and display timestamps only for pre-weight builds", async () => {
  const { env, prepareMock } = makeEnv();
  const startedAt = new Date("2026-08-15T00:00:01.000Z");
  const displayedAt = new Date("2026-08-15T00:00:03.000Z");
  await recordPreweightGenerationStarted(env, message(), startedAt);
  await recordPreweightDisplayCompleted(env, message(), displayedAt);
  await recordPreweightGenerationStarted(env, message({ mode: "rescore" }), startedAt);
  await recordPreweightDisplayCompleted(env, message({ mode: "rescore" }), displayedAt);
  expect(prepareMock).toHaveBeenCalledTimes(2);
  expect(prepareMock.mock.calls[0]?.[0]).toContain("generation_started_at");
  expect(prepareMock.mock.calls[1]?.[0]).toContain("kv_display_completed_at");
});
