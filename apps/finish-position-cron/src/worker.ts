// Run with bun. Fetch (health + on-demand trigger) + scheduled (cron -> container)
// handlers.

import { getContainer } from "@cloudflare/containers";
import { buildAuditBindParams, buildAuditInsertSql, buildAuditRecord } from "./audit";
import { FinishPositionPredictContainer } from "./container-class";
import { PREDICT_CRON, shouldRunPredictCron, shouldRunWarmCron } from "./cron-decision";
import { buildPredictStartOptions } from "./dispatch";
import { warmNeon } from "./neon-warm";
import { getRunDateJst, getRunYmdJst } from "./time";
import { isAuthorized, isTriggerRequest, parseRunDates } from "./trigger";
import type { CronAuditRecord, Env, RunDates } from "./types";

const CONTAINER_INSTANCE_NAME = "daily-finish-position-predict";
const ZERO_RACES = 0;
const RUN_DATE_FIELD = "runDate";
const HTTP_UNAUTHORIZED = 401;
const HTTP_BAD_REQUEST = 400;

export { FinishPositionPredictContainer };

const healthResponse = (): Response =>
  Response.json({ cron: PREDICT_CRON, name: "finish-position-cron", ok: true });

// Persist one audit row. Excluded from coverage with worker.ts (D1 binding),
// but the record + SQL it uses are built by tested helpers.
const recordAudit = async (env: Env, record: CronAuditRecord): Promise<void> => {
  await env.FINISH_POSITION_CRON_DB.prepare(buildAuditInsertSql())
    .bind(...buildAuditBindParams(record))
    .run();
};

// Start the predictor container as a batch job for the given run dates. Uses
// start() (not fetch) because the container exposes no port — it runs the
// entrypoint to completion. start() resolves once the container has launched;
// the audit row here marks the dispatch, and the container writes its own
// detailed audit row on completion (see predict_lib/audit.py).
const runPrediction = async (env: Env, dates: RunDates): Promise<void> => {
  const startedAt = Date.now();
  const container = getContainer(env.FINISH_POSITION_PREDICT_CONTAINER, CONTAINER_INSTANCE_NAME);
  await container.start(
    buildPredictStartOptions({ env, runDate: dates.runDate, runYmd: dates.runYmd }),
  );
  await recordAudit(
    env,
    buildAuditRecord({
      durationMs: Date.now() - startedAt,
      error: null,
      racesPredicted: ZERO_RACES,
      runDate: dates.runDate,
      status: "started",
    }),
  );
};

// Resolve the run date for an on-demand trigger: an explicit "YYYYMMDD" in the
// JSON body, or today's JST date when the body omits it.
const resolveTriggerDates = (body: Record<string, unknown>): RunDates => {
  const requested = body[RUN_DATE_FIELD];
  if (typeof requested === "string") {
    return parseRunDates(requested);
  }
  const now = new Date();
  return { runDate: getRunDateJst(now), runYmd: getRunYmdJst(now) };
};

const parseBody = async (request: Request): Promise<Record<string, unknown>> => {
  const text = await request.text();
  if (!text) {
    return {};
  }
  return JSON.parse(text) as Record<string, unknown>;
};

// Authenticated POST /run: start a prediction for an explicit or today's date.
const handleTrigger = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const dates = resolveTriggerDates(await parseBody(request));
  await runPrediction(env, dates);
  return Response.json({ ok: true, runDate: dates.runDate });
};

const guardedTrigger = async (request: Request, env: Env): Promise<Response> => {
  try {
    return await handleTrigger(request, env);
  } catch (error) {
    return Response.json({ error: String(error), ok: false }, { status: HTTP_BAD_REQUEST });
  }
};

export const handleFetch = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);
  if (isTriggerRequest(request.method, url.pathname)) {
    return guardedTrigger(request, env);
  }
  return healthResponse();
};

export const handleScheduled = async (event: ScheduledEvent, env: Env): Promise<void> => {
  if (shouldRunWarmCron(event.cron)) {
    await warmNeon(env.NEON_DATABASE_URL);
    return;
  }
  if (!shouldRunPredictCron(event.cron)) {
    return;
  }
  const scheduledAt = new Date(event.scheduledTime);
  await runPrediction(env, {
    runDate: getRunDateJst(scheduledAt),
    runYmd: getRunYmdJst(scheduledAt),
  });
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    return handleFetch(request, env);
  },
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    await handleScheduled(event, env);
  },
};
