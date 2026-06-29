// run with: bun run test
import { expect, it } from "vitest";
import {
  FETCH_LOG_SUCCESS,
  PLAN_RESULT_FETCHES_SUMMARY_STATUS,
  SKIP_STATUS,
} from "./fetchLogStatuses";

it("SKIP_STATUS exposes every silent-return status with the skip: prefix", () => {
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
    weightsSparse: "skip:weights-sparse",
  });
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
