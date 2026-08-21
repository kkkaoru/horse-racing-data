// Run with bun. Authenticated service-binding client for finish-position health signals.

import type { DeliveryCanaryResponse, Env, PredictionReadinessResponse } from "./types";

const FINISH_POSITION_ORIGIN = "https://finish-position-cron.internal";
const READINESS_PATH = "/api/internal/prediction-readiness";
const CANARIES_PATH = "/api/internal/delivery-canaries";
const READINESS_STRING_FIELDS = [
  "raceKey",
  "source",
  "keibajoCode",
  "raceBango",
  "raceStartAtJst",
] satisfies string[];
const READINESS_NUMBER_FIELDS = [
  "minutesToPost",
  "expectedCount",
  "predictionCount",
  "missingCount",
] satisfies string[];
const CANARY_STRING_FIELDS = ["id", "enqueuedAt"] satisfies string[];

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isNullableString = (value: unknown): value is string | null =>
  value === null || typeof value === "string";

const isReadinessDeadline = (value: unknown): value is "T-120" | "T-60" | "T-30" | "post" =>
  value === "T-120" || value === "T-60" || value === "T-30" || value === "post";

const fieldsHaveType = (
  value: Record<string, unknown>,
  fields: readonly string[],
  expectedType: "number" | "string",
): boolean => fields.every((field) => typeof value[field] === expectedType);

const isPredictionReadinessRace = (value: unknown): boolean => {
  if (!isRecord(value)) return false;
  return (
    fieldsHaveType(value, READINESS_STRING_FIELDS, "string") &&
    fieldsHaveType(value, READINESS_NUMBER_FIELDS, "number") &&
    isReadinessDeadline(value.deadline) &&
    isNullableString(value.oldestPredictionAt) &&
    isNullableString(value.newestPredictionAt) &&
    typeof value.complete === "boolean"
  );
};

const isDeliveryCanaryRecord = (value: unknown): boolean => {
  if (!isRecord(value)) return false;
  return (
    fieldsHaveType(value, CANARY_STRING_FIELDS, "string") &&
    isNullableString(value.consumedAt) &&
    (value.deliveryLagMs === null || typeof value.deliveryLagMs === "number")
  );
};

const isPredictionReadinessResponse = (value: unknown): value is PredictionReadinessResponse =>
  isRecord(value) &&
  typeof value.checkedAt === "string" &&
  typeof value.runYmd === "string" &&
  Array.isArray(value.races) &&
  value.races.every(isPredictionReadinessRace);

const isDeliveryCanaryResponse = (value: unknown): value is DeliveryCanaryResponse =>
  isRecord(value) &&
  typeof value.checkedAt === "string" &&
  Array.isArray(value.canaries) &&
  value.canaries.every(isDeliveryCanaryRecord);

const fetchJson = async (env: Env, path: string): Promise<unknown> => {
  const response = await env.FINISH_POSITION_CRON.fetch(
    new Request(new URL(path, FINISH_POSITION_ORIGIN), {
      headers: { Authorization: `Bearer ${env.FINISH_POSITION_CRON_TOKEN}` },
    }),
  );
  if (!response.ok) {
    throw new Error(`finish-position health request failed path=${path} status=${response.status}`);
  }
  return response.json<unknown>();
};

const unexpectedResponseError = (signal: string): Error =>
  new Error(
    `finish-position ${signal} endpoint returned an unexpected response shape; ${signal} may not be deployed`,
  );

export const fetchPredictionReadiness = async (env: Env): Promise<PredictionReadinessResponse> => {
  const response = await fetchJson(env, READINESS_PATH);
  if (!isPredictionReadinessResponse(response)) {
    throw unexpectedResponseError("prediction-readiness");
  }
  return response;
};

export const fetchDeliveryCanaries = async (env: Env): Promise<DeliveryCanaryResponse> => {
  const response = await fetchJson(env, CANARIES_PATH);
  if (!isDeliveryCanaryResponse(response)) {
    throw unexpectedResponseError("delivery-canaries");
  }
  return response;
};
