// Run with bun. Gate that decides whether a scheduled event should run the
// finish-position prediction container.

// The single configured cron for daily upcoming-race prediction. "0 18 * * *"
// is 18:00 UTC == JST 03:00, after the day's race cards are published and
// before morning viewing.
export const PREDICT_CRON = "0 18 * * *";

// Only the configured cron triggers a prediction run. Any other cron string
// (e.g. a future schedule added to the same Worker) is ignored, so adding more
// triggers never accidentally double-fires the container.
export const shouldRunPredictCron = (cron: string): boolean => cron === PREDICT_CRON;
