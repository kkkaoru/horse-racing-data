// Run with bun. Fetch (health + on-demand trigger) + scheduled (cron -> container) + queue handlers.

import { getContainer } from "@cloudflare/containers";
import { buildAuditBindParams, buildAuditInsertSql, buildAuditRecord } from "./audit";
import { FinishPositionPredictContainer } from "./container-class";
import {
  PREDICT_CRON,
  shouldRunPredictCron,
  shouldRunRescoreCron,
  shouldRunWarmCron,
} from "./cron-decision";
import { buildPredictStartOptions } from "./dispatch";
import { warmNeon } from "./neon-warm";
import { handleQueue } from "./queue-consumer";
import { enqueuePredict } from "./queue-producer";
import { getRunDateJst, getRunYmdJst } from "./time";
import { isAuthorized, isTriggerRequest, parseRunDates } from "./trigger";
import type {
  CronAuditRecord,
  Env,
  PredictCategory,
  PredictMode,
  PredictQueueMessage,
  RunDates,
} from "./types";

const CONTAINER_INSTANCE_NAME = "daily-finish-position-predict";
const ZERO_RACES = 0;
const RUN_DATE_FIELD = "runDate";
const MODE_FIELD = "mode";
const CATEGORY_FIELD = "category";
const DEFAULT_MODE: PredictMode = "full";
const VALID_MODES: ReadonlySet<string> = new Set(["full", "rescore"]);
const VALID_CATEGORIES: ReadonlySet<string> = new Set(["jra", "nar", "ban-ei"]);
const RESCORE_DAYS_AHEAD = 0;
const HTTP_UNAUTHORIZED = 401;
const HTTP_BAD_REQUEST = 400;
const HTTP_ACCEPTED = 202;

export { FinishPositionPredictContainer };

const healthResponse = (): Response =>
  Response.json({ cron: PREDICT_CRON, name: "finish-position-cron", ok: true });

const recordAudit = async (env: Env, record: CronAuditRecord): Promise<void> => {
  await env.FINISH_POSITION_CRON_DB.prepare(buildAuditInsertSql())
    .bind(...buildAuditBindParams(record))
    .run();
};

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

const resolveMode = (body: Record<string, unknown>): PredictMode => {
  const requested = body[MODE_FIELD];
  return typeof requested === "string" && VALID_MODES.has(requested)
    ? (requested as PredictMode)
    : DEFAULT_MODE;
};

const resolveCategory = (body: Record<string, unknown>): PredictCategory | undefined => {
  const requested = body[CATEGORY_FIELD];
  return typeof requested === "string" && VALID_CATEGORIES.has(requested)
    ? (requested as PredictCategory)
    : undefined;
};

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

const handleTrigger = async (request: Request, env: Env): Promise<Response> => {
  if (!isAuthorized(request.headers.get("authorization"), env.TRIGGER_TOKEN)) {
    return Response.json({ error: "unauthorized", ok: false }, { status: HTTP_UNAUTHORIZED });
  }
  const body = await parseBody(request);
  const dates = resolveTriggerDates(body);
  const mode = resolveMode(body);
  const queued = await enqueuePredict({
    category: resolveCategory(body),
    daysAhead: Number(env.PREDICT_DAYS_AHEAD),
    env,
    mode,
    runDate: dates.runDate,
    runYmd: dates.runYmd,
  });
  return Response.json({ ok: true, queued, runDate: dates.runDate }, { status: HTTP_ACCEPTED });
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
  if (shouldRunRescoreCron(event.cron)) {
    // Enqueue rescore messages for all categories (race-hours freshness).
    // daysAhead=0: only today's races need re-scoring.
    const scheduledAt = new Date(event.scheduledTime);
    await enqueuePredict({
      daysAhead: RESCORE_DAYS_AHEAD,
      env,
      mode: "rescore",
      runDate: getRunDateJst(scheduledAt),
      runYmd: getRunYmdJst(scheduledAt),
    });
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
  async queue(batch: MessageBatch<PredictQueueMessage>, env: Env): Promise<void> {
    await handleQueue(batch, env);
  },
};
