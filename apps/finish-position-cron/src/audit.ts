// Run with bun. Builder for the finish_position_cron_executions audit row.

import type { CronAuditRecord, CronAuditStatus } from "./types";

const AUDIT_TABLE = "finish_position_cron_executions";

interface BuildAuditRecordInput {
  runDate: string;
  status: CronAuditStatus;
  racesPredicted: number;
  durationMs: number;
  error: string | null;
}

// Construct the audit record, guarding against negative counters so a bad call
// site never persists nonsensical metrics. Insert-only — there is no delete /
// retention on this table (feedback_no_data_delete).
export const buildAuditRecord = (input: BuildAuditRecordInput): CronAuditRecord => {
  if (input.racesPredicted < 0) {
    throw new Error("racesPredicted must be non-negative");
  }
  if (input.durationMs < 0) {
    throw new Error("durationMs must be non-negative");
  }
  return {
    durationMs: input.durationMs,
    error: input.error,
    racesPredicted: input.racesPredicted,
    runDate: input.runDate,
    status: input.status,
  };
};

// Parameterised single-row INSERT bound through D1 prepare().bind(...).
export const buildAuditInsertSql = (): string =>
  `insert into ${AUDIT_TABLE} (run_date, status, races_predicted, duration_ms, error)
     values (?1, ?2, ?3, ?4, ?5)`;

// Positional bind parameters in the same order as the INSERT placeholders.
export const buildAuditBindParams = (
  record: CronAuditRecord,
): [string, CronAuditStatus, number, number, string | null] => [
  record.runDate,
  record.status,
  record.racesPredicted,
  record.durationMs,
  record.error,
];
