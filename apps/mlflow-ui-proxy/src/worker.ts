// Run with bun.
import { isAuthorized, unauthorizedResponse } from "./auth";
import { proxyRequest } from "./proxy";
import type { Env, MlflowSyncResult } from "./types";

const INTERNAL_PATH_PREFIX = "/__internal/";
const MLFLOW_CONTAINER_NAME = "primary";
const JST_OFFSET_MS = 9 * 60 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;
const SYNC_HORIZON_DAYS = 2;
const NOT_FOUND_STATUS = 404;

export interface SyncWindow {
  dateFrom: string;
  dateTo: string;
}

export interface ScheduledEventLike {
  scheduledTime: number;
}

const formatJstYmd = (timestampMs: number): string => {
  const shifted = new Date(timestampMs + JST_OFFSET_MS);
  const year = shifted.getUTCFullYear().toString().padStart(4, "0");
  const month = (shifted.getUTCMonth() + 1).toString().padStart(2, "0");
  const day = shifted.getUTCDate().toString().padStart(2, "0");
  return `${year}${month}${day}`;
};

export const buildSyncWindow = (timestampMs: number): SyncWindow => ({
  dateFrom: formatJstYmd(timestampMs),
  dateTo: formatJstYmd(timestampMs + SYNC_HORIZON_DAYS * DAY_MS),
});

export const handleFetch = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);
  if (url.pathname.startsWith(INTERNAL_PATH_PREFIX)) {
    return new Response("Not Found", { status: NOT_FOUND_STATUS });
  }
  const authorized = await isAuthorized(request, env);
  return authorized ? proxyRequest(request, env) : unauthorizedResponse();
};

export const handleScheduled = async (
  event: ScheduledEventLike,
  env: Env,
): Promise<MlflowSyncResult> => {
  const window = buildSyncWindow(event.scheduledTime);
  const container = env.MLFLOW_CONTAINER.getByName(MLFLOW_CONTAINER_NAME);
  const result = await container.syncProductionPreview(window.dateFrom, window.dateTo);
  if (result.exitCode !== 0) {
    throw new Error(`MLflow production preview sync failed: ${result.stderr}`);
  }
  console.log(`[mlflow-sync] range=${window.dateFrom}..${window.dateTo} ${result.stdout.trim()}`);
  return result;
};

export default {
  fetch: handleFetch,
  scheduled: async (event: ScheduledEvent, env: Env): Promise<void> => {
    await handleScheduled(event, env);
  },
};
