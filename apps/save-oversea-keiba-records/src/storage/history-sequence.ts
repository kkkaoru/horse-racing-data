// This file runs with Bun.
// Repair imported identity counters by consuming values, never by setting them
// backwards. Concurrent allocations can only create harmless additional gaps.
import type { SqlStatement } from "../types";

export interface HistorySequenceInput {
  readonly table: string;
  readonly lastValue: string | null;
  readonly maximumId: string;
  readonly incrementBy: string;
  readonly maximumAdvance: number;
}

const HISTORY_TABLES: ReadonlySet<string> = new Set([
  "oversea_horse_race_history",
  "oversea_person_race_history",
]);
const UNSIGNED_INTEGER: RegExp = /^\d+$/u;

export const buildHistorySequenceAdvance = (input: HistorySequenceInput): SqlStatement | null => {
  if (
    !HISTORY_TABLES.has(input.table) ||
    input.incrementBy !== "1" ||
    !UNSIGNED_INTEGER.test(input.maximumId) ||
    !Number.isSafeInteger(input.maximumAdvance) ||
    input.maximumAdvance < 0
  ) {
    throw new Error("History sequence metadata or advancement budget is invalid.");
  }
  if (input.maximumId === "0") return null;
  if (input.lastValue === null || !UNSIGNED_INTEGER.test(input.lastValue)) {
    throw new Error("History sequence position is unavailable; automatic repair is unsafe.");
  }
  const count: bigint = BigInt(input.maximumId) - BigInt(input.lastValue);
  if (count <= 0n) return null;
  if (count > BigInt(input.maximumAdvance)) {
    throw new Error("History sequence repair exceeds the explicitly allowed advancement budget.");
  }
  return {
    text: `select count(*)::integer as advanced, max(value)::text as last_value
from (
  select nextval(pg_get_serial_sequence($1, 'history_id')) as value
  from generate_series(1, $2::integer)
) allocated`,
    values: [input.table, count.toString()],
  };
};
