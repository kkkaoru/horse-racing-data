// Run with bun. Fetches per-runner metadata (wakuban + trainer name) for
// every sibling race on a given (source, year, monthDay, keibajoCode) in a
// single jvd_se / nvd_se round trip. Backs `mergeTodaySiblingRunnerData` in
// the trend route so today-sibling RaceTrendStarterRow records carry the two
// fields that race_entry_snapshots / race_result_snapshots can never
// populate. Lives behind the viewer-side Hyperdrive (R1) pool so the realtime
// (R2) pool stays free of the extra read pressure that motivated the earlier
// trainer-fetch revert in sync-realtime-data.
import "server-only";
import { sql } from "drizzle-orm";

import type { RaceSource } from "../lib/codes";
import type { TodaySiblingRunnerEntry } from "../lib/today-sibling-runner-merge";
import { getDb } from "./client";
import { jvdSe, nvdSe } from "./schema";

export interface TodaySiblingRunnerDataParams {
  source: RaceSource;
  year: string;
  monthDay: string;
  keibajoCode: string;
  beforeRaceBango: string;
}

interface RawRunnerRow {
  raceBango: string;
  umaban: string | null;
  wakuban: string | null;
  chokyoshimeiRyakusho: string | null;
}

const normalizeText = (value: string | null): string | null => {
  if (value === null) return null;
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
};

const toEntry = (row: RawRunnerRow): TodaySiblingRunnerEntry | null => {
  if (row.umaban === null) return null;
  const umaban = row.umaban.trim();
  if (umaban === "") return null;
  return {
    raceBango: row.raceBango,
    umaban,
    wakuban: normalizeText(row.wakuban),
    chokyoshiName: normalizeText(row.chokyoshimeiRyakusho),
  };
};

// `beforeRaceBango` mirrors the trend route's sibling filter: we only need
// rows for races whose raceBango is strictly less than the viewed race's,
// which keeps the result set small (1-11 races x ~16 runners) even on a
// full meeting day.
export const getRaceTrendTodaySiblingRunnerData = async (
  params: TodaySiblingRunnerDataParams,
): Promise<TodaySiblingRunnerEntry[]> => {
  const table = params.source === "jra" ? jvdSe : nvdSe;
  const result = await getDb().execute<RawRunnerRow & Record<string, unknown>>(sql`
    select
      se.race_bango as "raceBango",
      se.umaban as "umaban",
      se.wakuban as "wakuban",
      se.chokyoshimei_ryakusho as "chokyoshimeiRyakusho"
    from ${table} se
    where
      se.kaisai_nen = ${params.year}
      and se.kaisai_tsukihi = ${params.monthDay}
      and se.keibajo_code = ${params.keibajoCode}
      and se.race_bango < ${params.beforeRaceBango}
  `);
  return result.rows
    .map(toEntry)
    .filter((entry): entry is TodaySiblingRunnerEntry => entry !== null);
};
