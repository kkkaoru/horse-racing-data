// Run with bun. Builder for the finish_position_old_date_skip_events audit row.

import type { PredictCategory, PredictMode } from "./types";

const OLD_DATE_SKIP_EVENTS_TABLE = "finish_position_old_date_skip_events";

export interface OldDateSkipEventRecord {
  runYmd: string;
  category: PredictCategory;
  mode: PredictMode;
  keibajoCode: string | null;
  raceBango: string | null;
  thresholdDays: number;
}

interface BuildOldDateSkipEventRecordInput {
  runYmd: string;
  category: PredictCategory;
  mode: PredictMode;
  keibajoCode?: string;
  raceBango?: string;
  thresholdDays: number;
}

// Construct the old-date-skip audit row, guarding against a negative
// thresholdDays so a bad call site never persists a nonsensical guard
// window. Insert-only -- there is no delete / retention on this table
// (feedback_no_data_delete).
export const buildOldDateSkipEventRecord = (
  input: BuildOldDateSkipEventRecordInput,
): OldDateSkipEventRecord => {
  if (input.thresholdDays < 0) {
    throw new Error("thresholdDays must be non-negative");
  }
  return {
    category: input.category,
    keibajoCode: input.keibajoCode ?? null,
    mode: input.mode,
    raceBango: input.raceBango ?? null,
    runYmd: input.runYmd,
    thresholdDays: input.thresholdDays,
  };
};

// Parameterised single-row INSERT bound through D1 prepare().bind(...).
export const buildOldDateSkipEventInsertSql = (): string =>
  `insert into ${OLD_DATE_SKIP_EVENTS_TABLE} (run_ymd, category, mode, keibajo_code, race_bango, threshold_days)
     values (?1, ?2, ?3, ?4, ?5, ?6)`;

// Positional bind parameters in the same order as the INSERT placeholders.
export const buildOldDateSkipEventBindParams = (
  record: OldDateSkipEventRecord,
): [string, PredictCategory, PredictMode, string | null, string | null, number] => [
  record.runYmd,
  record.category,
  record.mode,
  record.keibajoCode,
  record.raceBango,
  record.thresholdDays,
];
