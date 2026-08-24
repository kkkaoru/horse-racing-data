// run with: bun run test
import { expect, it } from "vitest";
import {
  FETCH_LOG_SUCCESS,
  PLAN_RESULT_FETCHES_SUMMARY_STATUS,
  SKIP_STATUS,
} from "./fetchLogStatuses";

it("fetch log status constants expose every silent-return and pending status", () => {
  expect(SKIP_STATUS).toStrictEqual({
    authRequired: "skip:auth-required",
    awaitingPublish: "skip:awaiting-publish",
    claimFailed: "skip:claim-failed",
    configMissing: "skip:config-missing",
    giveUp: "skip:give-up",
    lockHeld: "skip:lock-held",
    notFinished: "skip:not-finished",
    paddockUrlMissing: "skip:paddock-url-missing",
    raceNotFound: "skip:race-not-found",
    rescoreDisabled: "skip:rescore-disabled",
    rescoreNotClaimed: "skip:not-claimed",
    weightsAlreadyStored: "skip:weights-already-stored",
    weightsPending: "pending:weights-unavailable",
    weightsIncomplete: "pending:weights-incomplete",
  });
});

it("SKIP_STATUS.weightsAlreadyStored is the literal used for the fetch-weights already-captured short-circuit", () => {
  expect(SKIP_STATUS.weightsAlreadyStored).toBe("skip:weights-already-stored");
});

it("SKIP_STATUS.rescoreDisabled is the literal used when finish-position-cron reports rescoreEnabled false", () => {
  expect(SKIP_STATUS.rescoreDisabled).toBe("skip:rescore-disabled");
});

it("SKIP_STATUS.rescoreNotClaimed is the literal used on a rescore claim collision", () => {
  expect(SKIP_STATUS.rescoreNotClaimed).toBe("skip:not-claimed");
});

it("SKIP_STATUS.awaitingPublish is the literal used for NAR result publish-window logs", () => {
  expect(SKIP_STATUS.awaitingPublish).toBe("skip:awaiting-publish");
});

it("PLAN_RESULT_FETCHES_SUMMARY_STATUS uses the plan-result-fetches-summary literal", () => {
  expect(PLAN_RESULT_FETCHES_SUMMARY_STATUS).toBe("plan-result-fetches-summary");
});

it("FETCH_LOG_SUCCESS exposes the fetch-results / fetch-weights / ok literals", () => {
  expect(FETCH_LOG_SUCCESS).toStrictEqual({
    fetchResultsJobType: "fetch-results",
    fetchWeightsJobType: "fetch-weights",
    okStatus: "ok",
  });
});
