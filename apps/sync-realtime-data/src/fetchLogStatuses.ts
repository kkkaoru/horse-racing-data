// run with: bun run test
// Fixed constant module for fetch_logs statuses. Centralising these keeps the
// status strings consistent between the producers (worker queue handlers) and
// the consumers (the /api/internal/queue-health endpoint + ops dashboards).
// Adding a new skip status here is the only place worker code should mint one
// — ad-hoc string literals were the silent-return source that made the
// 2026-06-28 JRA result-fetch outage invisible for 7 hours.

// Skip statuses use a `skip:` prefix so a single SQL `like 'skip:%'` query
// surfaces every non-error early-exit path. The value itself describes the
// specific reason so an operator can immediately tell why the queue handler
// returned without doing work.
export interface SkipStatuses {
  readonly authRequired: "skip:auth-required";
  readonly claimFailed: "skip:claim-failed";
  readonly configMissing: "skip:config-missing";
  readonly giveUp: "skip:give-up";
  readonly lockHeld: "skip:lock-held";
  readonly notFinished: "skip:not-finished";
  readonly paddockUrlMissing: "skip:paddock-url-missing";
  readonly raceNotFound: "skip:race-not-found";
  readonly weightsSparse: "skip:weights-sparse";
}

export const SKIP_STATUS: SkipStatuses = {
  authRequired: "skip:auth-required",
  claimFailed: "skip:claim-failed",
  configMissing: "skip:config-missing",
  giveUp: "skip:give-up",
  lockHeld: "skip:lock-held",
  notFinished: "skip:not-finished",
  paddockUrlMissing: "skip:paddock-url-missing",
  raceNotFound: "skip:race-not-found",
  weightsSparse: "skip:weights-sparse",
};

// Single summary row emitted by every plan-result-fetches tick so a missing
// row (or a long run of `enqueued=0` rows when D1 says there are eligible
// races) is visible without diffing fetch_logs against realtime_race_sources.
export const PLAN_RESULT_FETCHES_SUMMARY_STATUS = "plan-result-fetches-summary";

// Status prefix the queue-health endpoint uses to find the most recent
// successful fetch-results / fetch-weights observation. Producers must keep
// the existing `ok` status string for those job types — this constant only
// defines the *expected* status filter for the read side.
export interface FetchLogSuccessStatuses {
  readonly fetchResultsJobType: "fetch-results";
  readonly fetchWeightsJobType: "fetch-weights";
  readonly okStatus: "ok";
}

export const FETCH_LOG_SUCCESS: FetchLogSuccessStatuses = {
  fetchResultsJobType: "fetch-results",
  fetchWeightsJobType: "fetch-weights",
  okStatus: "ok",
};
