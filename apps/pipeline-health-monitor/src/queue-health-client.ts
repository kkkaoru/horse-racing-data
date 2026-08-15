// Run with bun.
import type { Env, QueueHealthMetrics } from "./types";

const QUEUE_HEALTH_URL = "https://sync-realtime-data.kkk4oru.com/api/internal/queue-health";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isNullableString = (value: unknown): value is string | null =>
  value === null || typeof value === "string";

const isQueueHealthMetrics = (value: unknown): value is QueueHealthMetrics =>
  isRecord(value) &&
  isNullableString(value.lastSuccessfulFetchResultsAt) &&
  isNullableString(value.lastSuccessfulFetchWeightsAt) &&
  typeof value.racesQueuedNotFetchedToday === "number" &&
  typeof value.racesStuckOverThirtyMin === "number";

export const fetchQueueHealth = async (env: Env): Promise<QueueHealthMetrics> => {
  const response = await env.REALTIME.fetch(
    new Request(QUEUE_HEALTH_URL, {
      headers: {
        Authorization: `Bearer ${env.REALTIME_ADMIN_TOKEN}`,
      },
    }),
  );
  if (!response.ok) {
    throw new Error(`queue-health request failed with status ${response.status}`);
  }
  const body: unknown = await response.json();
  if (!isQueueHealthMetrics(body)) {
    throw new Error(
      "queue-health endpoint returned an unexpected response shape; queue-health may not be deployed",
    );
  }
  return body;
};
