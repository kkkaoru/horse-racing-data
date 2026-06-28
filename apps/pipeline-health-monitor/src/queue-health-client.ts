// Run with bun.
import type { Env, QueueHealthMetrics } from "./types";

const QUEUE_HEALTH_URL = "https://sync-realtime-data.kkk4oru.com/api/internal/queue-health";

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
  return (await response.json()) as QueueHealthMetrics;
};
