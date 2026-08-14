// Run with bun. Durable self-heal message lifecycle accounting.

import type { Env, PredictQueueMessage } from "./types";

interface LifecycleIdentity {
  deliveryTrackingId: string;
  runYmd: string;
  category: string;
  keibajoCode: string;
  raceBango: string;
}

const lifecycleIdentity = (message: PredictQueueMessage): LifecycleIdentity | null => {
  if (
    message.deliveryTrackingId === undefined ||
    message.keibajoCode === undefined ||
    message.raceBango === undefined
  ) {
    return null;
  }
  return {
    category: message.category,
    deliveryTrackingId: message.deliveryTrackingId,
    keibajoCode: message.keibajoCode,
    raceBango: message.raceBango,
    runYmd: message.runYmd,
  };
};

export const recordDeliveryDetected = async (
  env: Env,
  message: PredictQueueMessage,
  now: Date,
): Promise<void> => {
  const identity = lifecycleIdentity(message);
  if (identity === null) return;
  await env.FINISH_POSITION_CRON_DB.prepare(
    `insert into finish_position_delivery_lifecycle
       (tracking_id, run_ymd, category, keibajo_code, race_bango, detected_at)
     values (?1, ?2, ?3, ?4, ?5, ?6)
     on conflict(tracking_id) do nothing`,
  )
    .bind(
      identity.deliveryTrackingId,
      identity.runYmd,
      identity.category,
      identity.keibajoCode,
      identity.raceBango,
      now.toISOString(),
    )
    .run();
};

const recordTimestamp = async (
  env: Env,
  message: PredictQueueMessage,
  column: "consumed_at" | "enqueued_at" | "prediction_completed_at",
  now: Date,
): Promise<void> => {
  const identity = lifecycleIdentity(message);
  if (identity === null) return;
  await env.FINISH_POSITION_CRON_DB.prepare(
    `update finish_position_delivery_lifecycle
        set ${column} = coalesce(${column}, ?2)
      where tracking_id = ?1`,
  )
    .bind(identity.deliveryTrackingId, now.toISOString())
    .run();
};

export const recordDeliveryEnqueued = (
  env: Env,
  message: PredictQueueMessage,
  now: Date,
): Promise<void> => recordTimestamp(env, message, "enqueued_at", now);

export const recordDeliveryConsumed = (
  env: Env,
  message: PredictQueueMessage,
  now: Date,
): Promise<void> => recordTimestamp(env, message, "consumed_at", now);

export const recordPredictionCompleted = (
  env: Env,
  message: PredictQueueMessage,
  now: Date,
): Promise<void> => recordTimestamp(env, message, "prediction_completed_at", now);
