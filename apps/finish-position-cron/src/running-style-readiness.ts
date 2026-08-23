// Run with bun. Fail-closed per-race running-style readiness for the first
// finish-position prediction fanout after a day-base HIT.

import type { RaceEntry } from "./cron-decision";
import type { PredictCategory } from "./types";

interface RunningStyleReadinessParams {
  category: PredictCategory;
  db: D1Database;
  races: readonly RaceEntry[];
  runYmd: string;
}

interface RunningStyleReadinessTarget {
  race: RaceEntry;
  realtimeKey: string;
  runningStyleKey: string;
}

interface RunningStyleReadinessRow {
  entrant_count: number | null;
  expected_horse_count: number | null;
  features_r2_key: string | null;
  prediction_count: number | null;
  running_key: string;
  status: string | null;
  written_horse_count: number | null;
}

export interface RunningStyleRaceReadiness {
  race: RaceEntry;
  reason: string | null;
}

const RUN_YMD_YEAR_END: number = 4;
const RUN_YMD_LENGTH: number = 8;
const COMPLETED_STATUS: string = "completed";
const RUNNING_STYLE_JRA_SOURCE: string = "jra";
const RUNNING_STYLE_NAR_SOURCE: string = "nar";
const TARGET_PLACEHOLDER: string = "(?, ?)";
const SCRATCHED_STATUSES_SQL: string = "'出場停止', '出走取消', '取消', '競走除外', '除外'";

const resolveRunningStyleSource = (category: PredictCategory): string =>
  category === "jra" ? RUNNING_STYLE_JRA_SOURCE : RUNNING_STYLE_NAR_SOURCE;

const buildTarget = (
  category: PredictCategory,
  runYmd: string,
  race: RaceEntry,
): RunningStyleReadinessTarget => {
  const source = resolveRunningStyleSource(category);
  const year = runYmd.slice(0, RUN_YMD_YEAR_END);
  const monthDay = runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_LENGTH);
  return {
    race,
    realtimeKey: `${source}:${year}:${monthDay}:${race.keibajoCode}:${race.raceBango}`,
    runningStyleKey: `${source}:${runYmd}:${race.keibajoCode}:${race.raceBango}`,
  };
};

const readinessReason = (row: RunningStyleReadinessRow | undefined): string | null => {
  if (row === undefined) return "state-missing";
  const mirroredEntrantCount = Number(row.entrant_count ?? 0);
  const expectedCount = Number(row.expected_horse_count ?? 0);
  // Running-style inference reads its rows from the R2 Catalog. The realtime
  // snapshot/daily-entry mirrors can legitimately still be empty when that
  // Catalog-backed inference has already completed. In that case the
  // inference state's expected count is the authoritative entrant count used
  // for feature filtering and completion; requiring a second mirror made all
  // ready races fail closed as entrants-missing.
  const entrantCount = mirroredEntrantCount > 0 ? mirroredEntrantCount : expectedCount;
  const writtenCount = Number(row.written_horse_count ?? 0);
  const predictionCount = Number(row.prediction_count ?? 0);
  if (entrantCount <= 0) return "entrants-missing";
  if (row.status !== COMPLETED_STATUS) return `status-${row.status ?? "missing"}`;
  if (row.features_r2_key === null || row.features_r2_key.length === 0)
    return "feature-artifact-missing";
  if (expectedCount < entrantCount)
    return `feature-count-${String(expectedCount)}-of-${String(entrantCount)}`;
  if (writtenCount < entrantCount)
    return `written-count-${String(writtenCount)}-of-${String(entrantCount)}`;
  return predictionCount < entrantCount
    ? `prediction-count-${String(predictionCount)}-of-${String(entrantCount)}`
    : null;
};

const buildReadinessSql = (targetCount: number): string => {
  const placeholders = Array.from({ length: targetCount }, () => TARGET_PLACEHOLDER).join(", ");
  return `with target(running_key, realtime_key) as (values ${placeholders}),
    latest_entries as (
      select entries.race_key, max(entries.fetched_at) as fetched_at
        from race_entry_snapshots entries
        join target on target.realtime_key = entries.race_key
       group by entries.race_key
    ),
    snapshot_entries as (
      select target.running_key,
             cast(entries.horse_number as integer) as horse_number
        from latest_entries latest
        join target on target.realtime_key = latest.race_key
        join race_entry_snapshots entries
          on entries.race_key = latest.race_key
         and entries.fetched_at = latest.fetched_at
       where cast(entries.horse_number as integer) > 0
         and (entries.status is null or entries.status not in (${SCRATCHED_STATUSES_SQL}))
    ),
    active_entries as (
      select running_key, horse_number from snapshot_entries
      union all
      select target.running_key, daily.umaban as horse_number
        from target
        join daily_race_entries daily on daily.race_key = target.running_key
       where daily.umaban > 0
         and not exists (
           select 1 from snapshot_entries snapshot
            where snapshot.running_key = target.running_key
         )
    )
    select target.running_key,
           state.status,
           state.features_r2_key,
           state.expected_horse_count,
           state.written_horse_count,
           count(distinct active.horse_number) as entrant_count,
           count(distinct styles.horse_number) as prediction_count
      from target
      left join running_style_inference_state state
        on state.race_key = target.running_key
      left join active_entries active
        on active.running_key = target.running_key
      left join race_running_styles styles
        on styles.race_key = target.running_key
       and (
         active.horse_number is null
         or styles.horse_number = active.horse_number
       )
     group by target.running_key, state.status, state.features_r2_key,
              state.expected_horse_count, state.written_horse_count`;
};

export const getRunningStyleRaceReadiness = async (
  params: RunningStyleReadinessParams,
): Promise<readonly RunningStyleRaceReadiness[]> => {
  if (params.races.length === 0) return [];
  const targets = params.races.map((race) => buildTarget(params.category, params.runYmd, race));
  const result = await params.db
    .prepare(buildReadinessSql(targets.length))
    .bind(...targets.flatMap((target) => [target.runningStyleKey, target.realtimeKey]))
    .all<RunningStyleReadinessRow>();
  const rowsByKey = new Map(result.results.map((row) => [row.running_key, row]));
  return targets.map((target) => ({
    race: target.race,
    reason: readinessReason(rowsByKey.get(target.runningStyleKey)),
  }));
};
