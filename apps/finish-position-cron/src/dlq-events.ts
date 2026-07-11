// Run with bun. Builder for the finish_position_predict_dlq_events audit row.

import type { PredictCategory, PredictMode } from "./types";

const DLQ_EVENTS_TABLE = "finish_position_predict_dlq_events";

export interface DlqEventRecord {
  runYmd: string;
  category: PredictCategory;
  mode: PredictMode;
  keibajoCode: string | null;
  raceBango: string | null;
  redriveCount: number;
  redriven: boolean;
}

interface BuildDlqEventRecordInput {
  runYmd: string;
  category: PredictCategory;
  mode: PredictMode;
  keibajoCode?: string;
  raceBango?: string;
  redriveCount: number;
  redriven: boolean;
}

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
    keibajoCode: input.keibajoCode ?? null,
    mode: input.mode,
    raceBango: input.raceBango ?? null,
    redriveCount: input.redriveCount,
    redriven: input.redriven,
    runYmd: input.runYmd,
  };
};

// Parameterised single-row INSERT bound through D1 prepare().bind(...).
export const buildDlqEventInsertSql = (): string =>
  `insert into ${DLQ_EVENTS_TABLE} (run_ymd, category, mode, keibajo_code, race_bango, redrive_count, redriven)
     values (?1, ?2, ?3, ?4, ?5, ?6, ?7)`;

// Positional bind parameters in the same order as the INSERT placeholders.
// D1 does not accept JS booleans as bind params, so redriven is passed as 0/1.
export const buildDlqEventBindParams = (
  record: DlqEventRecord,
): [string, PredictCategory, PredictMode, string | null, string | null, number, number] => [
  record.runYmd,
  record.category,
  record.mode,
  record.keibajoCode,
  record.raceBango,
  record.redriveCount,
  record.redriven ? 1 : 0,
];
