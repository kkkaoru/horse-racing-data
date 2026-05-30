// Run with: bun run --filter pc-keiba-viewer test src/lib/race-trend-live-backoff.test.ts
// Exponential backoff helpers for the race-trend live WebSocket reconnect.
// Pure functions so they are independently unit-testable and stay inside
// `vitest.config.ts` coverage.include.

const BACKOFF_BASE_MS = 1000;
const BACKOFF_CAP_MS = 30_000;
const BACKOFF_EXPONENT = 2;

export const RACE_TREND_LIVE_BACKOFF_BASE_MS = BACKOFF_BASE_MS;
export const RACE_TREND_LIVE_BACKOFF_CAP_MS = BACKOFF_CAP_MS;

// Returns the next reconnect delay in ms for a 0-indexed `attempt`. Caller
// should clamp the attempt count to a sane bound to avoid Math.pow overflow on
// degenerate cases. NaN and negative attempts collapse to the base delay;
// positive Infinity collapses to the cap.
export const computeRaceTrendLiveBackoffMs = (attempt: number): number => {
  if (Number.isNaN(attempt) || attempt <= 0) {
    return BACKOFF_BASE_MS;
  }
  if (attempt === Number.POSITIVE_INFINITY) {
    return BACKOFF_CAP_MS;
  }
  const exponential = BACKOFF_BASE_MS * BACKOFF_EXPONENT ** Math.floor(attempt);
  return exponential > BACKOFF_CAP_MS ? BACKOFF_CAP_MS : exponential;
};
