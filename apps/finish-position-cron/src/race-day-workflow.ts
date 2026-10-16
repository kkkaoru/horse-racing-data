// Run with bun. Workflow owns day-card orchestration. The container only builds.

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";
import { fanOutPredictionsAfterDayBaseHit } from "./feature-hit-prediction";
import { getFocusedFullDayBaseReadiness } from "./focused-full-day-base-readiness";
import { isOldDateRunYmd } from "./old-date-guard";
import { enumerateTodaysRaces } from "./cron-decision";
import { prewarmCategoryWithOutcome } from "./day-base-prewarm";
import { buildDayBaseWorkKey, buildStaleDayBaseWorkKey } from "./day-base-pickup";
import { consumeContainerStop } from "./container-control";
import { PREDICT_DO_NAME_PREFIX } from "./predict-do-shard";
import {
  acquireDayBaseRepair,
  clearDayBaseRepairReservation,
  heartbeatDayBaseRepair,
} from "./day-base-repair";
import { isAuthorized } from "./trigger";
import type { Env, PredictCategory, RaceDayWorkflowPayload } from "./types";

const VIEWER_ORIGIN = "https://pc-keiba-viewer.local";
// Eight waits alone add 40m; any shared 45m D1 repair lease needs heartbeat because
// prewarm/readiness step retries can extend total Workflow time well beyond it.
const DAY_BASE_READINESS_ATTEMPTS = 8;
const DAY_BASE_READINESS_DELAY = "5 minutes";
const HEATMAP_WARM_PATH = "/api/cache-warm/race-detail-sections";
const ADMIN_RACE_DAY_PATH = "/api/admin/race-day";
const CATEGORIES: readonly PredictCategory[] = ["jra", "nar", "ban-ei"];
const WORKFLOW_GENERATION_ID_PATTERN: RegExp = /^[A-Za-z0-9_-]{1,36}$/u;

export interface RaceDayParams {
  category: PredictCategory;
  daysAhead?: number;
  runYmd: string;
  generationId?: string;
  mode?: "fanout" | "prewarm-only";
  repairOwner?: boolean;
  warmHeatmap?: boolean;
}

interface RaceDayStep {
  do<T>(name: string, callback: () => Promise<T>): Promise<T>;
  do<T>(name: string, config: unknown, callback: () => Promise<T>): Promise<T>;
  sleep(name: string, duration: string): Promise<void>;
}

interface DayBaseWaitState {
  attempt: number;
  pendingPickup: boolean;
  readiness: Awaited<ReturnType<typeof getFocusedFullDayBaseReadiness>>;
}

const isCategory = (value: unknown): value is PredictCategory =>
  value === "jra" || value === "nar" || value === "ban-ei";

const isoDate = (runYmd: string): string =>
  `${runYmd.slice(0, 4)}-${runYmd.slice(4, 6)}-${runYmd.slice(6, 8)}`;

const requireRepairOwnerGenerationId = (params: RaceDayParams): string => {
  if (params.generationId === undefined) {
    throw new Error("day-base repair owner generation missing");
  }
  return params.generationId;
};

const heartbeatRepairOwner = async (
  env: Env,
  step: RaceDayStep,
  params: RaceDayParams,
  stepName: string,
): Promise<void> => {
  if (params.repairOwner !== true) return;
  const generationId = requireRepairOwnerGenerationId(params);
  const alive = await step.do(stepName, () =>
    heartbeatDayBaseRepair({
      category: params.category,
      env,
      generationId,
      runYmd: params.runYmd,
    }),
  );
  if (!alive) throw new Error("day-base repair owner lost");
};

const resolveDayBaseReadiness = async (
  env: Env,
  step: RaceDayStep,
  params: RaceDayParams,
  state: DayBaseWaitState,
): Promise<Awaited<ReturnType<typeof getFocusedFullDayBaseReadiness>>> => {
  const { attempt, pendingPickup, readiness } = state;
  if (readiness.ready) return readiness;
  // The artifact may have landed during the final sleep; never return its stale input.
  if (attempt >= DAY_BASE_READINESS_ATTEMPTS)
    return step.do("confirm-day-base-final", () =>
      getFocusedFullDayBaseReadiness({ category: params.category, env, runYmd: params.runYmd }),
    );
  if (params.repairOwner === true)
    await heartbeatRepairOwner(env, step, params, `heartbeat-day-base-${attempt}`);
  // Once the Queue owns an accepted build, the Workflow only observes readiness.
  // Retrying the start step on every wake can re-enter the Container after a
  // lease takeover; the Queue pickup is already responsible for recovery.
  const outcome =
    params.repairOwner === true && !pendingPickup
      ? await step.do(
          `prewarm-day-base-${attempt}`,
          { retries: { limit: 2, delay: "30 seconds" }, timeout: "5 minutes" },
          () =>
            prewarmCategoryWithOutcome({
              category: params.category,
              daysAhead: params.daysAhead ?? 0,
              env,
              generationId: params.generationId ?? `race-day-${params.category}-${params.runYmd}`,
              runYmd: params.runYmd,
            }),
        )
      : null;
  if (outcome === "failed") {
    await step.sleep(`wait-day-base-${attempt}`, DAY_BASE_READINESS_DELAY);
    return resolveDayBaseReadiness(env, step, params, {
      attempt: attempt + 1,
      pendingPickup: false,
      readiness: { ready: false, reason: "day-base-prewarm-failed" },
    });
  }
  const pickupScheduled = pendingPickup || outcome === "pickup-scheduled" || outcome === "owned";
  const confirmed = await step.do(`confirm-day-base-${attempt}`, () =>
    getFocusedFullDayBaseReadiness({ category: params.category, env, runYmd: params.runYmd }),
  );
  if (confirmed.ready) return confirmed;
  await step.sleep(`wait-day-base-${attempt}`, DAY_BASE_READINESS_DELAY);
  return resolveDayBaseReadiness(env, step, params, {
    attempt: attempt + 1,
    pendingPickup: pickupScheduled,
    readiness: confirmed,
  });
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
  await heartbeatRepairOwner(env, step, params, "heartbeat-day-base-start");
  const ready = await step.do("day-base-readiness", () =>
    getFocusedFullDayBaseReadiness({ category: params.category, env, runYmd: params.runYmd }),
  );
  const confirmed = await resolveDayBaseReadiness(env, step, params, {
    attempt: 0,
    pendingPickup: false,
    readiness: ready,
  });
  if (!confirmed.ready) {
    throw new Error(`day-base not ready category=${params.category} reason=${confirmed.reason}`);
  }
  if (params.repairOwner === true) {
    await heartbeatRepairOwner(env, step, params, "heartbeat-day-base-release");
    const generationId = requireRepairOwnerGenerationId(params);
    // R2 readiness proves the build is durable. Stop before Worker-only fanout,
    // without waiting for prediction queue delivery or extending Container idle time.
    // The existing atomic owner fence protects a newer generation on step retries.
    await step.do("stop-day-base-container", () =>
      consumeContainerStop(env, {
        acceptableWorkKeys: [buildDayBaseWorkKey(params), buildStaleDayBaseWorkKey(params)],
        name: `${PREDICT_DO_NAME_PREFIX}${params.category}`,
        requestedAt: new Date().toISOString(),
        role: "legacy",
        type: "container-stop",
        workKey: buildDayBaseWorkKey(params),
      }),
    );
    const released = await step.do("release-day-base-owner", () =>
      clearDayBaseRepairReservation({
        category: params.category,
        env,
        generationId,
        runYmd: params.runYmd,
      }),
    );
    if (!released) throw new Error("day-base repair owner lost");
  }
  if (params.mode === "prewarm-only") return { heatmap: false, racesEnqueued: 0 };
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

export class RaceDayWorkflow extends WorkflowEntrypoint<Env, RaceDayWorkflowPayload> {
  override async run(event: WorkflowEvent<RaceDayWorkflowPayload>, step: WorkflowStep) {
    return runRaceDay(this.env, step as unknown as RaceDayStep, event.payload);
  }
}

const canAttachToExistingWorkflow = async (
  workflow: NonNullable<Env["RACE_DAY_WORKFLOW"]>,
  id: string,
): Promise<boolean> => {
  try {
    const { status } = await (await workflow.get(id)).status();
    return (
      status === "queued" ||
      status === "running" ||
      status === "paused" ||
      status === "waiting" ||
      status === "waitingForPause" ||
      status === "complete"
    );
  } catch {
    return false;
  }
};

export const startRaceDay = async (
  env: Env,
  params: RaceDayParams,
): Promise<{ id: string; ok: true }> => {
  if (env.RACE_DAY_WORKFLOW === undefined) {
    throw new Error("RACE_DAY_WORKFLOW binding missing");
  }
  const proposedGenerationId = params.generationId ?? crypto.randomUUID();
  if (!WORKFLOW_GENERATION_ID_PATTERN.test(proposedGenerationId)) {
    throw new Error("invalid day-base repair generation id");
  }
  // Reserving a fresh generation makes a previously published manifest
  // pending. Verify the existing artifact first so a second scheduled fanout
  // does not rebuild the same full-day features just to read them again.
  const readiness = await getFocusedFullDayBaseReadiness({
    category: params.category,
    env,
    runYmd: params.runYmd,
  });
  if (readiness?.ready === true && isOldDateRunYmd(params.runYmd, new Date()))
    throw new Error("cannot start race-day workflow for old date");
  const owner =
    readiness?.ready === true
      ? { generationId: proposedGenerationId, status: "ready" as const }
      : await acquireDayBaseRepair({
          category: params.category,
          env,
          generationId: proposedGenerationId,
          runYmd: params.runYmd,
        });
  if (owner.status === "skipped-old-date")
    throw new Error("cannot start race-day workflow for old date");
  if (owner.generationId === null) throw new Error("day-base repair owner unavailable");
  const mode = params.mode ?? "fanout";
  const id = `day-${params.category}-${params.runYmd}-${mode}-${owner.generationId}`;
  const workflowParams: RaceDayWorkflowPayload = {
    category: params.category,
    daysAhead: params.daysAhead ?? 0,
    generationId: owner.generationId,
    mode,
    repairOwner: owner.status === "acquired",
    runYmd: params.runYmd,
    ...(params.warmHeatmap === true ? { warmHeatmap: true } : {}),
  };
  try {
    const instance = await env.RACE_DAY_WORKFLOW.create({ id, params: workflowParams });
    return { id: instance.id, ok: true };
  } catch (error) {
    if (await canAttachToExistingWorkflow(env.RACE_DAY_WORKFLOW, id)) return { id, ok: true };
    throw error;
  }
};

export const runDayBasePrewarmWorkflows = async (params: {
  daysAhead: number;
  env: Env;
  runYmd: string;
}): Promise<boolean> => {
  const { daysAhead, env, runYmd } = params;
  console.log(`[day-base-prewarm-workflow] start runYmd=${runYmd}`);
  const races = await enumerateTodaysRaces(env.REALTIME_DB, runYmd).catch((error: unknown) => {
    console.error(
      `[day-base-prewarm-workflow] enumerate failed runYmd=${runYmd}: ${String(error)}`,
    );
    return null;
  });
  if (races === null) return false;
  const categories = [...new Set(races.map((race) => race.category))];
  if (categories.length === 0) {
    console.log(`[day-base-prewarm-workflow] no races scheduled runYmd=${runYmd}`);
    return false;
  }
  const results = await Promise.allSettled(
    categories.map((category) =>
      startRaceDay(env, { category, daysAhead, mode: "prewarm-only", runYmd }),
    ),
  );
  results.forEach((result, index) => {
    if (result.status === "rejected") {
      console.error(
        `[day-base-prewarm-workflow] start failed category=${categories[index] ?? "-"} runYmd=${runYmd}: ${String(result.reason)}`,
      );
    }
  });
  return results.every((result) => result.status === "fulfilled");
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
