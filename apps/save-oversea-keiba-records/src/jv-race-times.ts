// This file runs with Bun. Race times and closing 3F of overseas history rows from the official JV
// runner rows of JRA-sold overseas races (local PostgreSQL), filled only where empty on the target.
import assert from "node:assert/strict";
import { localHistoryEnvironment } from "./history-local-mirror";
import { applyRaceTimeRows, type RaceTimeRow, type RaceTimeStorePlan } from "./race-time-store";
import { createTargetPool, type TargetPool } from "./storage/history-connection";

export interface JvRaceTimeRecord {
  readonly source_horse_id: string;
  readonly source_race_id: string;
  readonly soha_time: string;
  readonly kohan_3f: string;
}
export interface JvRaceTimesInput {
  readonly argv: readonly string[];
  readonly env: Readonly<Record<string, string | undefined>>;
}
export interface JvRaceTimesReport {
  readonly status: "complete";
  readonly operation: "planned" | "applied";
  readonly target: "local" | "production";
  readonly officialRows: number;
  readonly filled: number;
  readonly conflicts: readonly string[];
}

const SOURCE: string = "jra-van";
const JV_TIME_PATTERN: RegExp = /^(\d)(\d{2})(\d)$/u;
const JV_THREE_PATTERN: RegExp = /^(\d{2})(\d)$/u;
const SECONDS_PER_MINUTE: number = 60;
const TIME_DECIMALS: number = 2;
const TENTHS: number = 10;
// netkeiba ids of overseas runners come from oversea_runner_source_id; JRA-registered horses share ids.
const SELECT_SQL: string = `
with se as (
  select se.soha_time, se.kohan_3f, se.kaisai_nen, se.kaisai_tsukihi,
    coalesce(o.source_horse_id, se.ketto_toroku_bango) as horse_id
  from jvd_se se
  left join oversea_runner_source_id o on o.source = 'netkeiba' and o.kaisai_nen = se.kaisai_nen
    and o.kaisai_tsukihi = se.kaisai_tsukihi and o.keibajo_code = se.keibajo_code
    and o.race_bango = se.race_bango and o.umaban = se.umaban
  where se.keibajo_code !~ '^[0-9]'
)
select h.source_horse_id, h.source_race_id, se.soha_time, se.kohan_3f
from oversea_horse_race_history h
join se on se.horse_id = h.source_horse_id
  and se.kaisai_nen || se.kaisai_tsukihi = to_char(h.race_date, 'YYYYMMDD')
where h.source = 'netkeiba' and h.source_race_id is not null and h.venue !~ '^[0-9].*[0-9]$'`;
const USAGE: string =
  "Use jv-race-times-plan TARGET or jv-race-times-apply TARGET --confirm-write; TARGET is local or production.";

// JV soha_time "1475" is 1:47.5; "0000" or blank is unavailable.
export const jvRaceTimeSeconds = (value: string): number | null => {
  const match: RegExpExecArray | null = JV_TIME_PATTERN.exec(value.trim());
  const seconds: number =
    match === null
      ? 0
      : Number(match[1]) * SECONDS_PER_MINUTE + Number(match[2]) + Number(match[3]) / TENTHS;
  return seconds > 0 ? seconds : null;
};

export const jvClosingThree = (value: string): string | null => {
  const match: RegExpExecArray | null = JV_THREE_PATTERN.exec(value.trim());
  return match === null || Number(match[0]) === 0 ? null : `${match[1]}.${match[2]}`;
};

const timeText = (seconds: number): string =>
  `${String(Math.floor(seconds / SECONDS_PER_MINUTE))}:${(seconds % SECONDS_PER_MINUTE).toFixed(1).padStart(4, "0")}`;

export const jvRaceTimeRow = (record: JvRaceTimeRecord): RaceTimeRow | null => {
  const seconds: number | null = jvRaceTimeSeconds(record.soha_time);
  const closing: string | null = jvClosingThree(record.kohan_3f);
  if (seconds === null && closing === null) {
    return null;
  }
  return {
    sourceHorseId: record.source_horse_id,
    sourceRaceId: record.source_race_id,
    raceTimeText: seconds === null ? null : timeText(seconds),
    raceTimeSeconds: seconds === null ? null : seconds.toFixed(TIME_DECIMALS),
    lastThreeFurlongsSeconds: closing,
    raceTimeSource: SOURCE,
    raceTimeKind: "measured",
  };
};

const officialRows = async (env: JvRaceTimesInput["env"]): Promise<RaceTimeRow[]> => {
  const pool: TargetPool = createTargetPool(localHistoryEnvironment(env), "local");
  try {
    const result = await pool.pool.query(SELECT_SQL, []);
    return result.rows.flatMap((row) => {
      const parsed: RaceTimeRow | null = jvRaceTimeRow({
        source_horse_id: String(row.source_horse_id),
        source_race_id: String(row.source_race_id),
        soha_time: String(row.soha_time ?? ""),
        kohan_3f: String(row.kohan_3f ?? ""),
      });
      return parsed === null ? [] : [parsed];
    });
  } finally {
    await pool.pool.end();
  }
};

export const runJvRaceTimes = async ({
  argv,
  env,
}: JvRaceTimesInput): Promise<JvRaceTimesReport> => {
  const [action, target, confirmation]: readonly (string | undefined)[] = argv;
  const apply: boolean =
    action === "jv-race-times-apply" && argv.length === 3 && confirmation === "--confirm-write";
  assert(
    (target === "local" || target === "production") &&
      ((action === "jv-race-times-plan" && argv.length === 2) || apply),
    USAGE,
  );
  const rows: RaceTimeRow[] = await officialRows(env);
  const plan: RaceTimeStorePlan = await applyRaceTimeRows({ env, target, rows, apply });
  return {
    status: "complete",
    operation: apply ? "applied" : "planned",
    target,
    officialRows: rows.length,
    filled: plan.fills.length,
    conflicts: plan.conflicts,
  };
};
