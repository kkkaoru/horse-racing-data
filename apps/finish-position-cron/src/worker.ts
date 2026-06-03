// Run with bun. Fetch (health) + scheduled (cron -> container) handlers.

import { getContainer } from "@cloudflare/containers";
import { buildAuditBindParams, buildAuditInsertSql, buildAuditRecord } from "./audit";
import { FinishPositionPredictContainer } from "./container-class";
import { PREDICT_CRON, shouldRunPredictCron } from "./cron-decision";
import { buildPredictStartOptions } from "./dispatch";
import { getRunDateJst, getRunYmdJst } from "./time";
import type { CronAuditRecord, Env } from "./types";

const CONTAINER_INSTANCE_NAME = "daily-finish-position-predict";
const ZERO_RACES = 0;

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

// Start the predictor container as a batch job for the given run date. Uses
// start() (not fetch) because the container exposes no port — it runs the
// entrypoint to completion. start() resolves once the container has launched;
// the audit row here marks the dispatch, and the container writes its own
// detailed audit row on completion (see predict_lib/audit.py).
const dispatchPrediction = async (env: Env, scheduledAt: Date): Promise<void> => {
  const startedAt = Date.now();
  const runDate = getRunDateJst(scheduledAt);
  const runYmd = getRunYmdJst(scheduledAt);
  const container = getContainer(env.FINISH_POSITION_PREDICT_CONTAINER, CONTAINER_INSTANCE_NAME);
  await container.start(buildPredictStartOptions({ env, runDate, runYmd }));
  await recordAudit(
    env,
    buildAuditRecord({
      durationMs: Date.now() - startedAt,
      error: null,
      racesPredicted: ZERO_RACES,
      runDate,
      status: "started",
    }),
  );
};

export const handleScheduled = async (event: ScheduledEvent, env: Env): Promise<void> => {
  if (!shouldRunPredictCron(event.cron)) {
    return;
  }
  await dispatchPrediction(env, new Date(event.scheduledTime));
};

export default {
  fetch(): Response {
    return healthResponse();
  },
  async scheduled(event: ScheduledEvent, env: Env): Promise<void> {
    await handleScheduled(event, env);
  },
};
