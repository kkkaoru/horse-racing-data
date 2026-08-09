// Run with bun. Builder + lookup SQL for finish_position_predict_retry_errors.
// Insert-only breadcrumb written by the primary predict-queue consumer before
// message.retry(), so the DLQ consumer can copy the last failure onto
// finish_position_predict_dlq_events. Cloudflare Queues cannot mutate a
// retried message body, so this out-of-band table is the only durable path.

import type { PredictFailureSnapshot } from "./predict-failure";
import type { PredictCategory, PredictMode } from "./types";

const RETRY_ERRORS_TABLE = "finish_position_predict_retry_errors";

export interface RetryErrorRecord extends PredictFailureSnapshot {
  queueMessageId: string | null;
  runYmd: string;
  category: PredictCategory;
  mode: PredictMode;
  keibajoCode: string | null;
  raceBango: string | null;
  queueAttempts: number | null;
}

interface BuildRetryErrorRecordInput extends PredictFailureSnapshot {
  queueMessageId?: string;
  runYmd: string;
  category: PredictCategory;
  mode: PredictMode;
  keibajoCode?: string;
  raceBango?: string;
  queueAttempts?: number;
}

export const buildRetryErrorRecord = (input: BuildRetryErrorRecordInput): RetryErrorRecord => ({
  category: input.category,
  errorMessage: input.errorMessage,
  errorName: input.errorName,
  errorStack: input.errorStack,
  httpBodyExcerpt: input.httpBodyExcerpt,
  httpStatus: input.httpStatus,
  keibajoCode: input.keibajoCode ?? null,
  mode: input.mode,
  queueAttempts: input.queueAttempts ?? null,
  queueMessageId: input.queueMessageId ?? null,
  raceBango: input.raceBango ?? null,
  runYmd: input.runYmd,
});

export const buildRetryErrorInsertSql = (): string =>
  `insert into ${RETRY_ERRORS_TABLE} (queue_message_id, run_ymd, category, mode, keibajo_code, race_bango, error_name, error_message, error_stack, http_status, http_body_excerpt, queue_attempts)
     values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)`;

export const buildRetryErrorBindParams = (
  record: RetryErrorRecord,
): [
  string | null,
  string,
  PredictCategory,
  PredictMode,
  string | null,
  string | null,
  string | null,
  string | null,
  string | null,
  number | null,
  string | null,
  number | null,
] => [
  record.queueMessageId,
  record.runYmd,
  record.category,
  record.mode,
  record.keibajoCode,
  record.raceBango,
  record.errorName,
  record.errorMessage,
  record.errorStack,
  record.httpStatus,
  record.httpBodyExcerpt,
  record.queueAttempts,
];

export const buildRetryErrorLookupByMessageIdSql = (): string =>
  `select error_name as errorName, error_message as errorMessage, error_stack as errorStack, http_status as httpStatus, http_body_excerpt as httpBodyExcerpt, queue_attempts as queueAttempts
     from ${RETRY_ERRORS_TABLE}
    where queue_message_id = ?1
    order by id desc
    limit 1`;

export const buildRetryErrorLookupByRaceSql = (): string =>
  `select error_name as errorName, error_message as errorMessage, error_stack as errorStack, http_status as httpStatus, http_body_excerpt as httpBodyExcerpt, queue_attempts as queueAttempts
     from ${RETRY_ERRORS_TABLE}
    where run_ymd = ?1 and category = ?2 and mode = ?3
      and ifnull(keibajo_code, '') = ifnull(?4, '')
      and ifnull(race_bango, '') = ifnull(?5, '')
    order by id desc
    limit 1`;

export interface RetryErrorLookupRow {
  errorName: string | null;
  errorMessage: string | null;
  errorStack: string | null;
  httpStatus: number | null;
  httpBodyExcerpt: string | null;
  queueAttempts: number | null;
}

export const retryErrorLookupRowToSnapshot = (
  row: RetryErrorLookupRow,
): PredictFailureSnapshot => ({
  errorMessage: row.errorMessage,
  errorName: row.errorName,
  errorStack: row.errorStack,
  httpBodyExcerpt: row.httpBodyExcerpt,
  httpStatus: row.httpStatus,
});
