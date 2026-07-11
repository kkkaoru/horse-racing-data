// Run with bun. Builder for the finish_position_coverage_gap_events audit row.

import type { PredictCategory } from "./types";

const COVERAGE_GAP_EVENTS_TABLE = "finish_position_coverage_gap_events";

export interface CoverageGapEventRecord {
  runYmd: string;
  category: PredictCategory;
  keibajoCode: string;
  raceBango: string;
  raceStartAtJst: string;
  priorEnqueueCount: number;
  enqueued: boolean;
  escalated: boolean;
}

// Validates the coverage-gap-event audit row, guarding against a negative
// priorEnqueueCount so a bad call site never persists a nonsensical
// escalation count. Insert-only -- there is no delete / retention on this
// table (feedback_no_data_delete).
export const buildCoverageGapEventRecord = (
  input: CoverageGapEventRecord,
): CoverageGapEventRecord => {
  if (input.priorEnqueueCount < 0) {
    throw new Error("priorEnqueueCount must be non-negative");
  }
  return input;
};

// Parameterised single-row INSERT bound through D1 prepare().bind(...).
export const buildCoverageGapEventInsertSql = (): string =>
  `insert into ${COVERAGE_GAP_EVENTS_TABLE} (run_ymd, category, keibajo_code, race_bango, race_start_at_jst, prior_enqueue_count, enqueued, escalated)
     values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)`;

// Positional bind parameters in the same order as the INSERT placeholders.
// D1 does not accept JS booleans as bind params, so enqueued/escalated are
// passed as 0/1.
export const buildCoverageGapEventBindParams = (
  record: CoverageGapEventRecord,
): [string, PredictCategory, string, string, string, number, number, number] => [
  record.runYmd,
  record.category,
  record.keibajoCode,
  record.raceBango,
  record.raceStartAtJst,
  record.priorEnqueueCount,
  record.enqueued ? 1 : 0,
  record.escalated ? 1 : 0,
];
