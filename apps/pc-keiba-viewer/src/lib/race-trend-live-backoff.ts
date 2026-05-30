// Run with: bun run --filter pc-keiba-viewer test src/lib/race-trend-live-backoff.test.ts
// Exponential backoff helpers for the race-trend live WebSocket reconnect.
// Pure functions so they are independently unit-testable and stay inside
// `vitest.config.ts` coverage.include.

const BACKOFF_BASE_MS = 1000;
const BACKOFF_CAP_MS = 30_000;
const BACKOFF_EXPONENT = 2;
// Circuit breaker: once the consecutive-failure count crosses this threshold
// the section stops scheduling further reconnects and falls back to the 60s
// polling loop. This defends against a permanently unreachable WS URL (e.g.
// a localhost relay env leak in production, a hard DNS failure, or an L4
// firewall drop) silently keeping the browser busy with reconnect timers
// for the entire session. The attempt counter resets when the page becomes
// visible again or the user clicks the manual retry button, so the section
// can recover automatically once the underlying issue clears.
const MAX_RECONNECT_ATTEMPTS = 5;

export const RACE_TREND_LIVE_BACKOFF_BASE_MS = BACKOFF_BASE_MS;
export const RACE_TREND_LIVE_BACKOFF_CAP_MS = BACKOFF_CAP_MS;
export const RACE_TREND_LIVE_MAX_RECONNECT_ATTEMPTS = MAX_RECONNECT_ATTEMPTS;

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

// Returns true when the consecutive-failure count has reached the circuit
// breaker threshold and further reconnects should be skipped. NaN, negative,
// and fractional inputs are treated as "below the limit" so the caller always
// gets at least one reconnect attempt before tripping the breaker.
export const isRaceTrendLiveReconnectExhausted = (attempt: number): boolean => {
  if (!Number.isFinite(attempt)) {
    return attempt === Number.POSITIVE_INFINITY;
  }
  return Math.floor(attempt) >= MAX_RECONNECT_ATTEMPTS;
};
