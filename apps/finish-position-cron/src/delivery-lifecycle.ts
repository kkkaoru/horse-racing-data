// Run with bun. Durable self-heal message lifecycle accounting.

import type { Env, PredictQueueMessage } from "./types";

interface LifecycleIdentity {
  deliveryTrackingId: string;
  runYmd: string;
  category: string;
  keibajoCode: string;
  raceBango: string;
}

const isPreweightFullMessage = (message: PredictQueueMessage): boolean => message.mode === "full";

const lifecycleIdentity = (message: PredictQueueMessage): LifecycleIdentity | null => {
  if (message.keibajoCode === undefined || message.raceBango === undefined) {
    return null;
  }
  // Every per-race full build is timed automatically.  Older callers may not
  // provide an explicit self-heal tracking id, so derive a stable identity
  // from the immutable race scope.  Rescore messages are intentionally not
  // included: their timing belongs to the separate post-weight generation.
  const deliveryTrackingId =
    message.deliveryTrackingId ??
    (message.mode === "full"
      ? `preweight:${message.runYmd}:${message.category}:${message.keibajoCode}:${message.raceBango}`
      : undefined);
  if (deliveryTrackingId === undefined) return null;
  return {
    category: message.category,
    deliveryTrackingId,
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
       (tracking_id, run_ymd, category, keibajo_code, race_bango, mode, detected_at)
     values (?1, ?2, ?3, ?4, ?5, ?6, ?7)
     on conflict(tracking_id) do nothing`,
  )
    .bind(
      identity.deliveryTrackingId,
      identity.runYmd,
      identity.category,
      identity.keibajoCode,
      identity.raceBango,
      message.mode,
      now.toISOString(),
    )
    .run();
};

const recordTimestamp = async (
  env: Env,
  message: PredictQueueMessage,
  column:
    | "consumed_at"
    | "enqueued_at"
    | "prediction_completed_at"
    | "generation_started_at"
    | "kv_display_completed_at",
  now: Date,
): Promise<void> => {
  const identity = lifecycleIdentity(message);
  if (identity === null) return;
  const durationSql =
    column === "generation_started_at"
      ? `,
       queue_to_generation_start_ms = case
         when generation_started_at is null and enqueued_at is not null then
           cast(max(0, (julianday(?2) - julianday(enqueued_at)) * 86400000) as integer)
         else queue_to_generation_start_ms
       end`
      : column === "prediction_completed_at"
        ? `,
       generation_duration_ms = case
         when prediction_completed_at is null and generation_started_at is not null then
           cast(max(0, (julianday(?2) - julianday(generation_started_at)) * 86400000) as integer)
         else generation_duration_ms
       end`
        : column === "kv_display_completed_at"
          ? `,
       generation_to_display_ms = case
         when kv_display_completed_at is null and prediction_completed_at is not null then
           cast(max(0, (julianday(?2) - julianday(prediction_completed_at)) * 86400000) as integer)
         else generation_to_display_ms
       end,
       enqueue_to_display_ms = case
         when kv_display_completed_at is null and enqueued_at is not null then
           cast(max(0, (julianday(?2) - julianday(enqueued_at)) * 86400000) as integer)
         else enqueue_to_display_ms
       end`
          : "";
  await env.FINISH_POSITION_CRON_DB.prepare(
    `update finish_position_delivery_lifecycle
        set ${column} = coalesce(${column}, ?2)${durationSql}
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

/** Records the instant the pre-weight full build is handed to the Container. */
export const recordPreweightGenerationStarted = (
  env: Env,
  message: PredictQueueMessage,
  now: Date,
): Promise<void> =>
  isPreweightFullMessage(message)
    ? recordTimestamp(env, message, "generation_started_at", now)
    : Promise.resolve();

/** Records the instant the pre-weight prediction is visible through the viewer warm path. */
export const recordPreweightDisplayCompleted = (
  env: Env,
  message: PredictQueueMessage,
  now: Date,
): Promise<void> =>
  isPreweightFullMessage(message)
    ? recordTimestamp(env, message, "kv_display_completed_at", now)
    : Promise.resolve();
