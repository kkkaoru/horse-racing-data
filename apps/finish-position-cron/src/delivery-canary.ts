// Run with bun. Primary predict-queue delivery canary and durable D1 heartbeat.

import type { Message } from "@cloudflare/workers-types";
import { isDayBasePickupMessage } from "./day-base-pickup";
import { isDayBasePrewarmMessage } from "./day-base-prewarm";
import type { DeliveryCanaryMessage, Env, PredictQueueBody, PredictQueueMessage } from "./types";

const CANARY_TYPE = "delivery-canary";
const LATEST_CANARY_LIMIT = 12;
export const DELIVERY_CANARY_CRON = "*/5 0-13 * * *";

export const shouldRunDeliveryCanaryCron = (cron: string): boolean => cron === DELIVERY_CANARY_CRON;

export interface DeliveryCanaryRecord {
  id: string;
  enqueuedAt: string;
  consumedAt: string | null;
  deliveryLagMs: number | null;
}

interface CanaryRow {
  id: string;
  enqueued_at: string;
  consumed_at: string | null;
  delivery_lag_ms: number | null;
}

export const isDeliveryCanaryMessage = (value: PredictQueueBody): value is DeliveryCanaryMessage =>
  "type" in value && value.type === CANARY_TYPE;

export const isDeliveryCanaryQueueMessage = (
  message: Message<PredictQueueBody>,
): message is Message<DeliveryCanaryMessage> => isDeliveryCanaryMessage(message.body);

export const isPredictQueueMessage = (
  message: Message<PredictQueueBody>,
): message is Message<PredictQueueMessage> =>
  !isDeliveryCanaryMessage(message.body) &&
  !isDayBasePickupMessage(message.body) &&
  !isDayBasePrewarmMessage(message.body) &&
  !("type" in message.body && message.body.type === "prediction-cache-repair");

export const enqueueDeliveryCanary = async (
  env: Env,
  now: Date,
): Promise<DeliveryCanaryMessage> => {
  const message: DeliveryCanaryMessage = {
    enqueuedAt: now.toISOString(),
    id: crypto.randomUUID(),
    type: CANARY_TYPE,
  };
  await env.FINISH_POSITION_CRON_DB.prepare(
    `insert into finish_position_delivery_canaries (id, enqueued_at)
     values (?1, ?2)`,
  )
    .bind(message.id, message.enqueuedAt)
    .run();
  await env.PREDICT_QUEUE.send(message);
  return message;
};

export const consumeDeliveryCanary = async (
  env: Env,
  message: DeliveryCanaryMessage,
  now: Date,
): Promise<void> => {
  const consumedAt = now.toISOString();
  const enqueuedMs = Date.parse(message.enqueuedAt);
  const deliveryLagMs = Number.isNaN(enqueuedMs) ? null : Math.max(0, now.getTime() - enqueuedMs);
  await env.FINISH_POSITION_CRON_DB.prepare(
    `insert into finish_position_delivery_canaries
       (id, enqueued_at, consumed_at, delivery_lag_ms)
     values (?1, ?2, ?3, ?4)
     on conflict(id) do update set
       consumed_at = excluded.consumed_at,
       delivery_lag_ms = excluded.delivery_lag_ms`,
  )
    .bind(message.id, message.enqueuedAt, consumedAt, deliveryLagMs)
    .run();
};

export const listDeliveryCanaries = async (env: Env): Promise<DeliveryCanaryRecord[]> => {
  const result = await env.FINISH_POSITION_CRON_DB.prepare(
    `select id, enqueued_at, consumed_at, delivery_lag_ms
       from finish_position_delivery_canaries
      order by enqueued_at desc
      limit ?1`,
  )
    .bind(LATEST_CANARY_LIMIT)
    .all<CanaryRow>();
  return result.results.map((row) => ({
    consumedAt: row.consumed_at,
    deliveryLagMs: row.delivery_lag_ms,
    enqueuedAt: row.enqueued_at,
    id: row.id,
  }));
};
