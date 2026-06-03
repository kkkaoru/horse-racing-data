// Run with bun. Gate that decides whether a scheduled event should run the
// finish-position prediction container.
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

// Only the configured cron triggers a prediction run. Any other cron string
// (or no cron at all, which is the deployed state) is ignored.
export const shouldRunPredictCron = (cron: string): boolean => cron === PREDICT_CRON;
