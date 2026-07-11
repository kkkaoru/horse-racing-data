// Run with bun. Dead-letter queue consumer for finish-position-predict-queue.
// Messages land here only after exhausting max_retries on the primary queue
// (see wrangler.jsonc). Historically this queue had zero consumer, so a
// genuinely dead message -- most often a focused-full per-race pipeline
// whose detached container thread died silently, leaving no error and no
// prediction rows -- left no durable trace and was never retried again: a
// black hole with the same shape as the finish-position serving blackout.
// This consumer logs a durable ERROR row for every dead-lettered message and
// re-enqueues it once, bounded by dlqRedriveCount on the message body so a
// poison-pill message cannot bounce between the two queues forever.

import { buildDlqEventBindParams, buildDlqEventInsertSql, buildDlqEventRecord } from "./dlq-events";
import { completeFocusedFullRace } from "./do-state";
import { isFocusedSkipDedupMessage } from "./queue-consumer";
import type { Env, PredictQueueMessage } from "./types";

export const DLQ_QUEUE_NAME = "finish-position-predict-dlq";
const MAX_DLQ_REDRIVES = 1;
const DLQ_REDRIVE_COUNT_ZERO = 0;
const DLQ_REDRIVE_COUNT_INCREMENT = 1;

const describeDlqMessage = (body: PredictQueueMessage): string =>
  `category=${body.category} runYmd=${body.runYmd} mode=${body.mode} keibajo=${
    body.keibajoCode ?? "-"
  } race=${body.raceBango ?? "-"}`;

const recordDlqEvent = async (
  env: Env,
  body: PredictQueueMessage,
  redriveCount: number,
  redriven: boolean,
): Promise<void> => {
  const record = buildDlqEventRecord({
    category: body.category,
    keibajoCode: body.keibajoCode,
    mode: body.mode,
    raceBango: body.raceBango,
    redriveCount,
    redriven,
    runYmd: body.runYmd,
  });
  await env.FINISH_POSITION_CRON_DB.prepare(buildDlqEventInsertSql())
    .bind(...buildDlqEventBindParams(record))
    .run();
};

// Focused-full per-race messages hold a DO-backed in-flight claim
// (claimFocusedFullRace, predict-run-coordinator.ts) that only clears via
// completeFocusedFullRace or the heartbeat going stale (see
// FOCUSED_FULL_IN_FLIGHT_STALE_MS, queue-consumer.ts). A message reaching
// this consumer has, by definition, stopped being redelivered, so no further
// heartbeat refresh is coming and the claim would eventually go stale on its
// own -- but forcing it to "error" here is strictly faster and more certain
// than waiting: it makes the race immediately reclaimable the moment the
// redrive below lands, instead of depending on staleness timing. Other
// message shapes (legacy per-category full, per-race/per-category rescore)
// do not hold a claim that can get stuck this way: claimRun has no staleness
// gate at all (any non-"success" status is always reclaimable), and rescore
// messages carry no completion-claim, only a same-day claimRescoreRace dedup
// that a redrive does not need to touch.
const unstickFocusedFullClaim = async (env: Env, body: PredictQueueMessage): Promise<void> => {
  if (!isFocusedSkipDedupMessage(body)) return;
  await completeFocusedFullRace({
    category: body.category,
    env,
    keibajoCode: body.keibajoCode,
    raceBango: body.raceBango,
    runYmd: body.runYmd,
    status: "error",
  });
};

const redriveMessage = async (env: Env, body: PredictQueueMessage): Promise<void> => {
  const redriveCount = body.dlqRedriveCount ?? DLQ_REDRIVE_COUNT_ZERO;
  await env.PREDICT_QUEUE.send({
    ...body,
    dlqRedriveCount: redriveCount + DLQ_REDRIVE_COUNT_INCREMENT,
  });
};

const processDlqMessage = async (
  message: Message<PredictQueueMessage>,
  env: Env,
): Promise<void> => {
  const body = message.body;
  const redriveCount = body.dlqRedriveCount ?? DLQ_REDRIVE_COUNT_ZERO;
  const shouldRedrive = redriveCount < MAX_DLQ_REDRIVES;
  try {
    await recordDlqEvent(env, body, redriveCount, shouldRedrive);
    await unstickFocusedFullClaim(env, body);
    if (shouldRedrive) {
      await redriveMessage(env, body);
      console.warn(
        `[predict-dlq] redriven ${describeDlqMessage(body)} redriveCount=${redriveCount}`,
      );
    } else {
      console.error(
        `[predict-dlq] redrive budget exhausted ${describeDlqMessage(body)} redriveCount=${redriveCount}`,
      );
    }
    message.ack();
  } catch (err) {
    console.error(
      `[predict-dlq] failed to process dead-lettered message ${describeDlqMessage(body)}:`,
      String(err),
    );
    message.retry();
  }
};

export const handleDlqQueue = async (
  batch: MessageBatch<PredictQueueMessage>,
  env: Env,
): Promise<void> => {
  for (const message of batch.messages) {
    await processDlqMessage(message, env);
  }
};
