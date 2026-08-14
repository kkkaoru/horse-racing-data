// Run with bun. Authenticated service-binding client for finish-position health signals.

import type { DeliveryCanaryResponse, Env, PredictionReadinessResponse } from "./types";

const FINISH_POSITION_ORIGIN = "https://finish-position-cron.internal";

const fetchJson = async <T>(env: Env, path: string): Promise<T> => {
  const response = await env.FINISH_POSITION_CRON.fetch(
    new Request(new URL(path, FINISH_POSITION_ORIGIN), {
      headers: { Authorization: `Bearer ${env.FINISH_POSITION_CRON_TOKEN}` },
    }),
  );
  if (!response.ok) {
    throw new Error(`finish-position health request failed path=${path} status=${response.status}`);
  }
  return response.json<T>();
};

export const fetchPredictionReadiness = (env: Env): Promise<PredictionReadinessResponse> =>
  fetchJson(env, "/api/internal/prediction-readiness");

export const fetchDeliveryCanaries = (env: Env): Promise<DeliveryCanaryResponse> =>
  fetchJson(env, "/api/internal/delivery-canaries");
