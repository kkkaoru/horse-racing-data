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

import { isDayBasePickupQueueMessage } from "./day-base-pickup";
import { isDeliveryCanaryQueueMessage, isPredictQueueMessage } from "./delivery-canary";
import {
  buildDlqEventBindParams,
  buildDlqEventInsertSql,
  buildDlqEventRecord,
  emptyPredictFailure,
} from "./dlq-events";
import { completeFocusedFullRace } from "./do-state";
import type { PredictFailureSnapshot } from "./predict-failure";
import { isFocusedSkipDedupMessage } from "./queue-consumer";
import {
  buildRetryErrorLookupByMessageIdSql,
  buildRetryErrorLookupByRaceSql,
  retryErrorLookupRowToSnapshot,
  type RetryErrorLookupRow,
} from "./retry-errors";
import type {
  DayBasePickupMessage,
  DeliveryCanaryMessage,
  Env,
  PredictQueueMessage,
} from "./types";

export const DLQ_QUEUE_NAME = "finish-position-predict-dlq";
const MAX_DLQ_REDRIVES = 1;
const DLQ_REDRIVE_COUNT_ZERO = 0;
const DLQ_REDRIVE_COUNT_INCREMENT = 1;

const describeDlqMessage = (body: PredictQueueMessage): string =>
  `category=${body.category} runYmd=${body.runYmd} mode=${body.mode} keibajo=${
    body.keibajoCode ?? "-"
  } race=${body.raceBango ?? "-"}`;

const optionalQueueAttempts = (message: Message<PredictQueueMessage>): number | null =>
  typeof message.attempts === "number" ? message.attempts : null;

const optionalQueueMessageId = (message: Message<PredictQueueMessage>): string | null =>
  typeof message.id === "string" && message.id.length > 0 ? message.id : null;

const snapshotFromMessageBody = (body: PredictQueueMessage): PredictFailureSnapshot | null => {
  const lastFailure = body.lastFailure;
  if (lastFailure === undefined) return null;
  if (
    lastFailure.errorName == null &&
    lastFailure.errorMessage == null &&
    lastFailure.errorStack == null &&
    lastFailure.httpStatus == null &&
    lastFailure.httpBodyExcerpt == null
  ) {
    return null;
  }
  return {
    errorMessage: lastFailure.errorMessage ?? null,
    errorName: lastFailure.errorName ?? null,
    errorStack: lastFailure.errorStack ?? null,
    httpBodyExcerpt: lastFailure.httpBodyExcerpt ?? null,
    httpStatus: lastFailure.httpStatus ?? null,
  };
};

const lookupRetryErrorByMessageId = async (
  env: Env,
  messageId: string,
): Promise<RetryErrorLookupRow | null> =>
  env.FINISH_POSITION_CRON_DB.prepare(buildRetryErrorLookupByMessageIdSql())
    .bind(messageId)
    .first<RetryErrorLookupRow>();

const lookupRetryErrorByRace = async (
  env: Env,
  body: PredictQueueMessage,
): Promise<RetryErrorLookupRow | null> =>
  env.FINISH_POSITION_CRON_DB.prepare(buildRetryErrorLookupByRaceSql())
    .bind(body.runYmd, body.category, body.mode, body.keibajoCode ?? null, body.raceBango ?? null)
    .first<RetryErrorLookupRow>();

const resolveDlqFailure = async (
  env: Env,
  message: Message<PredictQueueMessage>,
): Promise<{ failure: PredictFailureSnapshot; queueAttempts: number | null }> => {
  const queueAttempts = optionalQueueAttempts(message);
  const fromBody = snapshotFromMessageBody(message.body);
  if (fromBody !== null) {
    return { failure: fromBody, queueAttempts };
  }
  const messageId = optionalQueueMessageId(message);
  const byId = messageId === null ? null : await lookupRetryErrorByMessageId(env, messageId);
  const row = byId ?? (await lookupRetryErrorByRace(env, message.body));
  if (row === null) {
    return { failure: emptyPredictFailure(), queueAttempts };
  }
  return {
    failure: retryErrorLookupRowToSnapshot(row),
    queueAttempts: queueAttempts ?? row.queueAttempts,
  };
};

const recordDlqEvent = async (
  env: Env,
  message: Message<PredictQueueMessage>,
  redriveCount: number,
  redriven: boolean,
): Promise<void> => {
  const { failure, queueAttempts } = await resolveDlqFailure(env, message);
  const body = message.body;
  const record = buildDlqEventRecord({
    category: body.category,
    errorMessage: failure.errorMessage,
    errorName: failure.errorName,
    errorStack: failure.errorStack,
    httpBodyExcerpt: failure.httpBodyExcerpt,
    httpStatus: failure.httpStatus,
    keibajoCode: body.keibajoCode,
    mode: body.mode,
    queueAttempts,
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
    await recordDlqEvent(env, message, redriveCount, shouldRedrive);
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
  batch: MessageBatch<PredictQueueMessage | DeliveryCanaryMessage | DayBasePickupMessage>,
  env: Env,
): Promise<void> => {
  for (const message of batch.messages) {
    if (isDeliveryCanaryQueueMessage(message)) {
      console.error(`[predict-dlq] delivery canary reached DLQ id=${message.body.id}`);
      message.ack();
    } else if (isDayBasePickupQueueMessage(message)) {
      console.error(
        `[predict-dlq] day-base pickup reached DLQ category=${message.body.category} runYmd=${message.body.runYmd} attempt=${message.body.attempt}`,
      );
      message.ack();
    } else if (isPredictQueueMessage(message)) {
      await processDlqMessage(message, env);
    }
  }
};
