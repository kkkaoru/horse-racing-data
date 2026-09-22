// Run with bun. Workflow owns day-card orchestration. The container only builds.

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";
import { fanOutPredictionsAfterDayBaseHit } from "./feature-hit-prediction";
import { getFocusedFullDayBaseReadiness } from "./focused-full-day-base-readiness";
import { PREDICT_DO_NAME_PREFIX } from "./predict-do-shard";
import { isAuthorized } from "./trigger";
import type { Env, PredictCategory } from "./types";

const PREWARM_HOST = "http://predict-container-do";
const VIEWER_ORIGIN = "https://pc-keiba-viewer.local";
const HEATMAP_WARM_PATH = "/api/cache-warm/race-detail-sections";
const ADMIN_RACE_DAY_PATH = "/api/admin/race-day";
const CATEGORIES: readonly PredictCategory[] = ["jra", "nar", "ban-ei"];

export interface RaceDayParams {
  category: PredictCategory;
  runYmd: string;
  warmHeatmap?: boolean;
}

interface RaceDayStep {
  do<T>(name: string, callback: () => Promise<T>): Promise<T>;
  do<T>(name: string, config: unknown, callback: () => Promise<T>): Promise<T>;
}

const isCategory = (value: unknown): value is PredictCategory =>
  value === "jra" || value === "nar" || value === "ban-ei";

const isoDate = (runYmd: string): string =>
  `${runYmd.slice(0, 4)}-${runYmd.slice(4, 6)}-${runYmd.slice(6, 8)}`;

export const syncDayBase = async (
  env: Env,
  category: PredictCategory,
  runYmd: string,
): Promise<void> => {
  const url = new URL("/prewarm-day-base", PREWARM_HOST);
  url.searchParams.set("category", category);
  url.searchParams.set("daysAhead", "0");
  url.searchParams.set("rebuild", "1");
  url.searchParams.set("runDate", runYmd);
  url.searchParams.set("sync", "1");
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(
    env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(`${PREDICT_DO_NAME_PREFIX}${category}`),
  );
  const response = await stub.fetch(new Request(url));
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`sync day-base failed category=${category} status=${response.status}`);
  }
  const lastLine = body.trim().split("\n").at(-1) ?? "";
  const parsed: unknown = lastLine === "" ? null : JSON.parse(lastLine);
  const status =
    typeof parsed === "object" && parsed !== null && "status" in parsed ? parsed.status : "-";
  const error =
    typeof parsed === "object" && parsed !== null && "error" in parsed ? parsed.error : "-";
  const committed =
    typeof parsed === "object" &&
    parsed !== null &&
    "parquetBase64" in parsed &&
    typeof parsed.parquetBase64 === "string" &&
    parsed.parquetBase64.length > 0;
  if (status !== "success" || !committed) {
    throw new Error(
      `sync day-base not committed category=${category} status=${String(status)} error=${String(error)}`,
    );
  }
};

const warmHeatmaps = async (env: Env, runYmd: string): Promise<void> => {
  if (env.PC_KEIBA_VIEWER === undefined) return;
  const url = new URL(HEATMAP_WARM_PATH, VIEWER_ORIGIN);
  url.searchParams.set("date", isoDate(runYmd));
  const response = await env.PC_KEIBA_VIEWER.fetch(
    new Request(url, { headers: { "X-PC-Keiba-Cache-Warm": "scheduled" }, method: "POST" }),
  );
  if (!response.ok) {
    throw new Error(`heatmap warm failed status=${response.status}`);
  }
  await response.arrayBuffer();
};

export const runRaceDay = async (
  env: Env,
  step: RaceDayStep,
  params: RaceDayParams,
): Promise<{ heatmap: boolean; racesEnqueued: number }> => {
  const ready = await step.do("day-base-readiness", () =>
    getFocusedFullDayBaseReadiness({ category: params.category, env, runYmd: params.runYmd }),
  );
  if (!ready.ready) {
    await step.do(
      "sync-day-base",
      { retries: { limit: 1, delay: "30 seconds" }, timeout: "26 minutes" },
      () => syncDayBase(env, params.category, params.runYmd),
    );
    const confirmed = await step.do("confirm-day-base", () =>
      getFocusedFullDayBaseReadiness({ category: params.category, env, runYmd: params.runYmd }),
    );
    if (!confirmed.ready) {
      throw new Error(`day-base not ready category=${params.category} reason=${confirmed.reason}`);
    }
  }
  const racesEnqueued = await step.do("fanout-predictions", () =>
    fanOutPredictionsAfterDayBaseHit({
      category: params.category,
      env,
      runYmd: params.runYmd,
    }),
  );
  if (params.warmHeatmap === true) {
    await step.do("warm-heatmaps", () => warmHeatmaps(env, params.runYmd));
  }
  return { heatmap: params.warmHeatmap === true, racesEnqueued };
};

export class RaceDayWorkflow extends WorkflowEntrypoint<Env, RaceDayParams> {
  override async run(event: WorkflowEvent<RaceDayParams>, step: WorkflowStep) {
    return runRaceDay(this.env, step as unknown as RaceDayStep, event.payload);
  }
}

export const startRaceDay = async (
  env: Env,
  params: RaceDayParams,
): Promise<{ id: string; ok: true }> => {
  if (env.RACE_DAY_WORKFLOW === undefined) {
    throw new Error("RACE_DAY_WORKFLOW binding missing");
  }
  const id = `race-day-${params.category}-${params.runYmd}-${Date.now().toString(36)}`;
  try {
    const instance = await env.RACE_DAY_WORKFLOW.create({ id, params });
    return { id: instance.id, ok: true };
  } catch (error) {
    const message = String(error);
    if (!message.includes("already exists")) throw error;
    return { id, ok: true };
  }
};

export const handleAdminRaceDay = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    return Response.json({ error: "unauthorized", ok: false }, { status: 401 });
  }
  const body: unknown = await request.json().catch(() => null);
  if (typeof body !== "object" || body === null || !("runYmd" in body)) {
    return Response.json({ error: "invalid request", ok: false }, { status: 400 });
  }
  const runYmd = body.runYmd;
  if (typeof runYmd !== "string" || !/^\d{8}$/u.test(runYmd)) {
    return Response.json({ error: "invalid request", ok: false }, { status: 400 });
  }
  const requested = "category" in body ? body.category : undefined;
  const categories =
    requested === undefined ? CATEGORIES : isCategory(requested) ? [requested] : [];
  if (categories.length === 0) {
    return Response.json({ error: "invalid request", ok: false }, { status: 400 });
  }
  const started = await Promise.all(
    categories.map((category, index) =>
      startRaceDay(env, {
        category,
        runYmd,
        ...(index === 0 ? { warmHeatmap: true } : {}),
      }),
    ),
  );
  return Response.json({ ok: true, started });
};

export const isAdminRaceDayRequest = (method: string, pathname: string): boolean =>
  method === "POST" && pathname === ADMIN_RACE_DAY_PATH;
