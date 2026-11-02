// This file runs with Bun. Lists the stored history races of one overseas meeting's runners that
// still have no race time (local PostgreSQL, read-only), as input for racing-post-times-*.
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { localHistoryEnvironment } from "./history-local-mirror";
import { createTargetPool, type TargetPool } from "./storage/history-connection";

export interface MissingRaceTimesInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
}
export interface MissingRaceTimesReport {
  readonly status: "complete";
  readonly operation: "listed";
  readonly horses: number;
  readonly missing: number;
}

const KEY_PATTERN: RegExp = /^\d{4}$/u;
const VENUE_PATTERN: RegExp = /^[0-9A-Z]{2}$/u;
const PRIVATE_FILE_MODE: number = 0o600;
const SELECT_SQL: string = `SELECT h.source_horse_id, h.source_race_id, h.race_date::text AS d, h.venue,
  h.race_name, h.finish_position, h.finish_position_text, h.distance_metres, h.surface
FROM oversea_horse_race_history h
WHERE h.source = 'netkeiba' AND h.race_time_text IS NULL AND h.source_race_id IS NOT NULL
  AND h.source_horse_id IN (
    SELECT o.source_horse_id FROM oversea_runner_source_id o
    WHERE o.source = 'netkeiba' AND o.kaisai_nen = $1 AND o.kaisai_tsukihi = $2 AND o.keibajo_code = $3)
ORDER BY h.race_date, h.source_horse_id`;
const USAGE: string = "Use missing-race-times YYYY MMDD VENUE OUTPUT_JSON.";

export const runMissingRaceTimes = async ({
  argv,
  env,
}: MissingRaceTimesInput): Promise<MissingRaceTimesReport> => {
  const [, year, monthDay, venue, output] = argv;
  assert(
    argv.length === 5 &&
      KEY_PATTERN.test(String(year)) &&
      KEY_PATTERN.test(String(monthDay)) &&
      VENUE_PATTERN.test(String(venue)) &&
      output !== undefined,
    USAGE,
  );
  const pool: TargetPool = createTargetPool(localHistoryEnvironment(env), "local");
  try {
    const result = await pool.pool.query(SELECT_SQL, [
      String(year),
      String(monthDay),
      String(venue),
    ]);
    await writeFile(output, JSON.stringify(result.rows), { mode: PRIVATE_FILE_MODE });
    return {
      status: "complete",
      operation: "listed",
      horses: new Set(result.rows.map((row) => String(row.source_horse_id))).size,
      missing: result.rows.length,
    };
  } finally {
    await pool.pool.end();
  }
};
