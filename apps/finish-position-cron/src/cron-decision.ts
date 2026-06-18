// Run with bun. Gate that decides whether a scheduled event should run the
// finish-position prediction container or the Neon pre-wake warm-up.
//
// NOTE: Cloudflare Cron Triggers are intentionally disabled in wrangler.jsonc
// because Cloudflare Containers reaps batch instances at ~90-110s regardless
// of sleepAfter and the DuckDB feature build needs ~10+ min. Mac launchd
// drives the daily run instead (scripts/launchd/). The gate below remains so
// that if anyone ever re-enables a cron and uses a different schedule by
// mistake, scheduled() stays a no-op until PREDICT_CRON itself is changed.

// The historical schedule preserved as the canonical "predict cron" name.
// "0 18 * * *" is 18:00 UTC == JST 03:00. Re-enabling it in wrangler.jsonc is
// NOT recommended (Container reap window); use launchd.
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

// Per-race coordinator cron: every 5 min during race hours
// (01:00-11:59 UTC == JST 10:00-20:59). Finer-grained than RESCORE so each race
// can be enqueued close to its post time T-X window. Shadow-safe: it only
// enqueues per-race rescore messages; it does not start the container or touch
// the predict / warm crons. Mirrors the running-style "*/10" coordinator.
export const COORDINATOR_CRON_RACE_HOURS = "*/5 1-11 * * *";

const WARM_CRONS: ReadonlySet<string> = new Set([
  WARM_CRON_PRE_NAR,
  WARM_CRON_PRE_JRA,
  WARM_CRON_RACE_HOURS,
]);

const RESCORE_CRONS: ReadonlySet<string> = new Set([RESCORE_CRON_RACE_HOURS]);

const COORDINATOR_CRONS: ReadonlySet<string> = new Set([COORDINATOR_CRON_RACE_HOURS]);

// Only the configured cron triggers a prediction run. Any other cron string
// (or no cron at all, which is the deployed state) is ignored.
export const shouldRunPredictCron = (cron: string): boolean => cron === PREDICT_CRON;

// Returns true when the cron string matches one of the Neon pre-wake schedules.
export const shouldRunWarmCron = (cron: string): boolean => WARM_CRONS.has(cron);

// Returns true when the cron string matches one of the rescore (race-hours freshness) schedules.
export const shouldRunRescoreCron = (cron: string): boolean => RESCORE_CRONS.has(cron);

// Returns true when the cron string matches the per-race coordinator schedule.
export const shouldRunCoordinatorCron = (cron: string): boolean => COORDINATOR_CRONS.has(cron);
