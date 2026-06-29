// Run with bun. Gate that decides whether a scheduled event should run the
// finish-position prediction container or the Neon pre-wake warm-up.
//
// NOTE: The legacy monolithic predict cron is intentionally absent from
// wrangler.jsonc because start()-style container batches can be idle-reaped
// before the DuckDB feature build completes. Production full runs are
// Cloudflare-owned per-race: sync-realtime-data calls POST /run after
// running-style completes, and this Worker enqueues container work.

import type { PredictCategory } from "./types";

// A row of realtime_race_sources (sync-realtime-data D1) projected to the
// columns the race enumeration helper needs: the underlying source (jra/nar)
// plus the keibajo / race identifiers.
interface RaceSourceRow {
  source: string;
  keibajo_code: string;
  race_bango: string;
}

// One race the feature-build cron should fan a per-race full build out to.
// keibajoCode / raceBango are zero-padded to width 2 (matching the per-race
// coordinator + the container race_id scope).
export interface RaceEntry {
  category: PredictCategory;
  keibajoCode: string;
  raceBango: string;
}

const ENUMERATE_RACES_SQL =
  "SELECT DISTINCT source, keibajo_code, race_bango FROM realtime_race_sources WHERE kaisai_nen = ? AND kaisai_tsukihi = ? ORDER BY source, keibajo_code, race_bango";
const RUN_YMD_YEAR_START = 0;
const RUN_YMD_YEAR_END = 4;
const RUN_YMD_LENGTH = 8;
const JRA_SOURCE = "jra";
const BAN_EI_KEIBAJO_CODE = "83";
const JRA_CATEGORY: PredictCategory = "jra";
const NAR_CATEGORY: PredictCategory = "nar";
const BAN_EI_CATEGORY: PredictCategory = "ban-ei";
const KEIBAJO_PAD_WIDTH = 2;
const RACE_BANGO_PAD_WIDTH = 2;
const PAD_CHAR = "0";

// The historical monolithic schedule preserved as the canonical "predict cron"
// name. "0 18 * * *" is 18:00 UTC == JST 03:00. Re-enabling it in wrangler.jsonc
// is NOT the production path; use the Cloudflare per-race POST /run queue path.
export const PREDICT_CRON = "0 18 * * *";

// Warm cron: 17:55 UTC == JST 02:55 (5 min before NAR/ban-ei 03:00 prediction)
export const WARM_CRON_PRE_NAR = "55 17 * * *";

// Warm cron: 00:25 UTC == JST 09:25 (5 min before JRA 09:30 prediction)
export const WARM_CRON_PRE_JRA = "25 0 * * *";

// Warm cron: every 30 min during race hours (01:00-11:59 UTC == JST 10:00-20:59)
export const WARM_CRON_RACE_HOURS = "*/30 1-11 * * *";

// Rescore cron: every 20 min during race hours (01:00-11:59 UTC == JST 10:00-20:59)
// NOTE: This cron is NOT active in wrangler.jsonc triggers yet — enabled after pilot phase.
export const RESCORE_CRON_RACE_HOURS = "*/20 1-11 * * *";

// Per-race coordinator cron: every 10 min during race hours
// (01:00-11:59 UTC == JST 10:00-20:59). Finer-grained than RESCORE so each race
// can be enqueued close to its post time T-X window. Shadow-safe: it only
// enqueues per-race rescore messages; it does not start the container or touch
// the predict / warm crons. Mirrors the running-style "*/10" coordinator.
export const COORDINATOR_CRON_RACE_HOURS = "*/10 1-11 * * *";

// Feature-build cron: 00:30 UTC == JST 09:30. The worker now treats this as an
// observable no-op; production full per-race runs are triggered by
// sync-realtime-data after running-style completion via POST /run.
export const FEATURE_BUILD_CRON = "30 0 * * *";

const WARM_CRONS: ReadonlySet<string> = new Set([
  WARM_CRON_PRE_NAR,
  WARM_CRON_PRE_JRA,
  WARM_CRON_RACE_HOURS,
]);

const RESCORE_CRONS: ReadonlySet<string> = new Set([RESCORE_CRON_RACE_HOURS]);

const COORDINATOR_CRONS: ReadonlySet<string> = new Set([COORDINATOR_CRON_RACE_HOURS]);

const FEATURE_BUILD_CRONS: ReadonlySet<string> = new Set([FEATURE_BUILD_CRON]);

// Only the configured cron triggers a prediction run. Any other cron string
// (or no cron at all, which is the deployed state) is ignored.
export const shouldRunPredictCron = (cron: string): boolean => cron === PREDICT_CRON;

// Returns true when the cron string matches one of the Neon pre-wake schedules.
export const shouldRunWarmCron = (cron: string): boolean => WARM_CRONS.has(cron);

// Returns true when the cron string matches one of the rescore (race-hours freshness) schedules.
export const shouldRunRescoreCron = (cron: string): boolean => RESCORE_CRONS.has(cron);

// Returns true when the cron string matches the per-race coordinator schedule.
export const shouldRunCoordinatorCron = (cron: string): boolean => COORDINATOR_CRONS.has(cron);

// Returns true when the cron string matches the feature-build (Container full pipeline) schedule.
export const shouldRunFeatureBuildCron = (cron: string): boolean => FEATURE_BUILD_CRONS.has(cron);

const pad = (value: string, width: number): string => value.padStart(width, PAD_CHAR);

// jra source -> jra; otherwise keibajo 83 (帯広) is ban-ei and every other
// nar-source keibajo is plain nar. Mirrors how the predict pipeline categorises
// realtime_race_sources rows.
const resolveRaceCategory = (source: string, keibajoCode: string): PredictCategory => {
  if (source === JRA_SOURCE) return JRA_CATEGORY;
  if (keibajoCode === BAN_EI_KEIBAJO_CODE) return BAN_EI_CATEGORY;
  return NAR_CATEGORY;
};

const toRaceEntry = (row: RaceSourceRow): RaceEntry => {
  const keibajoCode = pad(row.keibajo_code, KEIBAJO_PAD_WIDTH);
  return {
    category: resolveRaceCategory(row.source, keibajoCode),
    keibajoCode,
    raceBango: pad(row.race_bango, RACE_BANGO_PAD_WIDTH),
  };
};

// Enumerate today's races from the sync-realtime-data D1 (realtime_race_sources)
// so the feature-build cron can fan out one per-race full build per race instead
// of one 21y full-batch scan. runYmd is the JST "YYYYMMDD" calendar date;
// kaisai_nen is the 4-digit year and kaisai_tsukihi the "MMDD" tail.
export const enumerateTodaysRaces = async (
  db: D1Database,
  runYmd: string,
): Promise<readonly RaceEntry[]> => {
  const year = runYmd.slice(RUN_YMD_YEAR_START, RUN_YMD_YEAR_END);
  const monthDay = runYmd.slice(RUN_YMD_YEAR_END, RUN_YMD_LENGTH);
  const result = await db.prepare(ENUMERATE_RACES_SQL).bind(year, monthDay).all<RaceSourceRow>();
  return result.results.map(toRaceEntry);
};
