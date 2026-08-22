// Run with bun. Builder for the finish_position_predict_dlq_events audit row.

import type { PredictFailureSnapshot } from "./predict-failure";
import type { PredictCategory, PredictMode } from "./types";

const DLQ_EVENTS_TABLE = "finish_position_predict_dlq_events";

export interface DlqEventRecord extends PredictFailureSnapshot {
  runYmd: string;
  category: PredictCategory;
  mode: PredictMode;
  keibajoCode: string | null;
  raceBango: string | null;
  queueMessageId: string | null;
  redriveCount: number;
  redriven: boolean;
  queueAttempts: number | null;
}

interface BuildDlqEventRecordInput {
  runYmd: string;
  category: PredictCategory;
  mode: PredictMode;
  keibajoCode?: string;
  raceBango?: string;
  queueMessageId?: string | null;
  redriveCount: number;
  redriven: boolean;
  queueAttempts?: number | null;
  errorName?: string | null;
  errorMessage?: string | null;
  errorStack?: string | null;
  httpStatus?: number | null;
  httpBodyExcerpt?: string | null;
}

const EMPTY_FAILURE: PredictFailureSnapshot = {
  errorMessage: null,
  errorName: null,
  errorStack: null,
  httpBodyExcerpt: null,
  httpStatus: null,
};

// Construct the dead-letter event record, guarding against a negative
// redriveCount so a bad call site never persists nonsensical metrics.
// Insert-only -- there is no delete / retention on this table
// (feedback_no_data_delete).
export const buildDlqEventRecord = (input: BuildDlqEventRecordInput): DlqEventRecord => {
  if (input.redriveCount < 0) {
    throw new Error("redriveCount must be non-negative");
  }
  return {
    category: input.category,
    errorMessage: input.errorMessage ?? null,
    errorName: input.errorName ?? null,
    errorStack: input.errorStack ?? null,
    httpBodyExcerpt: input.httpBodyExcerpt ?? null,
    httpStatus: input.httpStatus ?? null,
    keibajoCode: input.keibajoCode ?? null,
    mode: input.mode,
    queueAttempts: input.queueAttempts ?? null,
    queueMessageId: input.queueMessageId ?? null,
    raceBango: input.raceBango ?? null,
    redriveCount: input.redriveCount,
    redriven: input.redriven,
    runYmd: input.runYmd,
  };
};

export const emptyPredictFailure = (): PredictFailureSnapshot => ({ ...EMPTY_FAILURE });

// Parameterised single-row INSERT bound through D1 prepare().bind(...).
export const buildDlqEventInsertSql = (): string =>
  `insert into ${DLQ_EVENTS_TABLE} (run_ymd, category, mode, keibajo_code, race_bango, queue_message_id, redrive_count, redriven, error_name, error_message, error_stack, http_status, http_body_excerpt, queue_attempts)
     values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
     on conflict do nothing`;

// Positional bind parameters in the same order as the INSERT placeholders.
// D1 does not accept JS booleans as bind params, so redriven is passed as 0/1.
export const buildDlqEventBindParams = (
  record: DlqEventRecord,
): [
  string,
  PredictCategory,
  PredictMode,
  string | null,
  string | null,
  string | null,
  number,
  number,
  string | null,
  string | null,
  string | null,
  number | null,
  string | null,
  number | null,
] => [
  record.runYmd,
  record.category,
  record.mode,
  record.keibajoCode,
  record.raceBango,
  record.queueMessageId,
  record.redriveCount,
  record.redriven ? 1 : 0,
  record.errorName,
  record.errorMessage,
  record.errorStack,
  record.httpStatus,
  record.httpBodyExcerpt,
  record.queueAttempts,
];
