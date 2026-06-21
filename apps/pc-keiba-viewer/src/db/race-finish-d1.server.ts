// Run with bun. Server-only helper that reads the target race's 着順 from the
// sync-realtime-data REALTIME_DB (`race_result_snapshots`) when the PostgreSQL
// mirror has not imported it yet. The runners table renders this as a fallback
// between the live realtime payload and the PG `kakuteiChakujun` value.
//
// `realtime_race_sources` stores `kaisai_tsukihi` as MMDD, `keibajo_code` as a
// 2-digit code, and `race_bango` zero-padded to 2 digits (see
// sync-realtime-data/src/storage.ts upsertNarRaceSource / upsertJraRaceSource).
// The WHERE binds must match those stored formats exactly, so the caller's
// month / day / raceNumber are zero-padded here before binding.

import "server-only";
import { safeGetCloudflareEnv } from "../lib/cloudflare-context.server";
import type { RaceSource } from "../lib/codes";

interface RawRaceFinishD1Row {
  horseNumber: string;
  finishPosition: string;
}

export interface RaceFinishPositionEntry {
  finishPosition: string;
  horseNumber: string;
}

export interface GetRaceFinishPositionsFromD1Params {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

const KAISAI_TSUKIHI_PART_WIDTH = 2;
const RACE_BANGO_WIDTH = 2;

// Latest finish per horse for the ONE target race. The correlated subquery
// keeps only the newest snapshot per (race_key, horse_number), matching the
// `latest_result` CTE pattern in d1-trend-queries.server.ts.
const SELECT_RACE_FINISH_SQL = `
  with latest_result as (
    select r1.race_key, r1.horse_number, r1.finish_position
    from race_result_snapshots r1
    where r1.fetched_at = (
      select max(r2.fetched_at) from race_result_snapshots r2
      where r2.race_key = r1.race_key and r2.horse_number = r1.horse_number
    )
  )
  select lr.horse_number as horseNumber, lr.finish_position as finishPosition
  from latest_result lr
  join realtime_race_sources s on s.race_key = lr.race_key
  where s.source = ?
    and s.kaisai_nen = ?
    and s.kaisai_tsukihi = ?
    and s.keibajo_code = ?
    and s.race_bango = ?
`;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isRawRaceFinishD1Row = (value: unknown): value is RawRaceFinishD1Row => {
  if (!isRecord(value)) return false;
  return typeof value.horseNumber === "string" && typeof value.finishPosition === "string";
};

const toEntry = (row: RawRaceFinishD1Row): RaceFinishPositionEntry => ({
  finishPosition: row.finishPosition,
  horseNumber: row.horseNumber,
});

export const getRaceFinishPositionsFromD1 = async (
  params: GetRaceFinishPositionsFromD1Params,
): Promise<RaceFinishPositionEntry[]> => {
  const env = await safeGetCloudflareEnv();
  const db = env?.REALTIME_DB;
  if (!db) return [];
  const kaisaiTsukihi = `${params.month.padStart(KAISAI_TSUKIHI_PART_WIDTH, "0")}${params.day.padStart(
    KAISAI_TSUKIHI_PART_WIDTH,
    "0",
  )}`;
  const raceBango = params.raceNumber.padStart(RACE_BANGO_WIDTH, "0");
  try {
    const result = await db
      .prepare(SELECT_RACE_FINISH_SQL)
      .bind(params.source, params.year, kaisaiTsukihi, params.keibajoCode, raceBango)
      .all<RawRaceFinishD1Row>();
    return result.results.filter(isRawRaceFinishD1Row).map(toEntry);
  } catch (error) {
    console.error("D1 race finish position query failed", error);
    return [];
  }
};
