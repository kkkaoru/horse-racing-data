// Run with bun. Tests for the cron gate.

import { expect, test } from "vitest";
import {
  COORDINATOR_CRON_RACE_HOURS,
  FEATURE_BUILD_CRON,
  PREDICT_CRON,
  RESCORE_CRON_RACE_HOURS,
  WARM_CRON_PRE_JRA,
  WARM_CRON_PRE_NAR,
  WARM_CRON_RACE_HOURS,
  shouldRunCoordinatorCron,
  shouldRunFeatureBuildCron,
  shouldRunPredictCron,
  shouldRunRescoreCron,
  shouldRunWarmCron,
} from "./cron-decision";

test("PREDICT_CRON is the JST 03:00 schedule", () => {
  expect(PREDICT_CRON).toBe("0 18 * * *");
});

test("shouldRunPredictCron matches the configured cron", () => {
  expect(shouldRunPredictCron("0 18 * * *")).toBe(true);
});

test("shouldRunPredictCron rejects a different cron", () => {
  expect(shouldRunPredictCron("*/10 * * * *")).toBe(false);
});

test("shouldRunPredictCron rejects an empty string", () => {
  expect(shouldRunPredictCron("")).toBe(false);
});

test("shouldRunPredictCron rejects when the wrangler cron array is empty (deployed state)", () => {
  // Cloudflare will not fire scheduled() when crons is empty, but defend against
  // an accidental empty event.cron arriving anyway.
  expect(shouldRunPredictCron("")).toBe(false);
});

test("WARM_CRON_PRE_NAR is the JST 02:55 schedule", () => {
  expect(WARM_CRON_PRE_NAR).toBe("55 17 * * *");
});

test("WARM_CRON_PRE_JRA is the JST 09:25 schedule", () => {
  expect(WARM_CRON_PRE_JRA).toBe("25 0 * * *");
});

test("WARM_CRON_RACE_HOURS is the every-30-min race-hours schedule", () => {
  expect(WARM_CRON_RACE_HOURS).toBe("*/30 1-11 * * *");
});

test("shouldRunWarmCron matches the pre-NAR warm cron", () => {
  expect(shouldRunWarmCron("55 17 * * *")).toBe(true);
});

test("shouldRunWarmCron matches the pre-JRA warm cron", () => {
  expect(shouldRunWarmCron("25 0 * * *")).toBe(true);
});

test("shouldRunWarmCron matches the race-hours warm cron", () => {
  expect(shouldRunWarmCron("*/30 1-11 * * *")).toBe(true);
});

test("shouldRunWarmCron rejects the predict cron", () => {
  expect(shouldRunWarmCron("0 18 * * *")).toBe(false);
});

test("shouldRunWarmCron rejects an empty string", () => {
  expect(shouldRunWarmCron("")).toBe(false);
});

test("shouldRunWarmCron rejects an unrelated cron", () => {
  expect(shouldRunWarmCron("*/10 * * * *")).toBe(false);
});

test("RESCORE_CRON_RACE_HOURS is the every-20-min race-hours schedule", () => {
  expect(RESCORE_CRON_RACE_HOURS).toBe("*/20 1-11 * * *");
});

test("shouldRunRescoreCron matches the rescore race-hours cron", () => {
  expect(shouldRunRescoreCron("*/20 1-11 * * *")).toBe(true);
});

test("shouldRunRescoreCron rejects the warm race-hours cron", () => {
  expect(shouldRunRescoreCron("*/30 1-11 * * *")).toBe(false);
});

test("shouldRunRescoreCron rejects the predict cron", () => {
  expect(shouldRunRescoreCron("0 18 * * *")).toBe(false);
});

test("shouldRunRescoreCron rejects an empty string", () => {
  expect(shouldRunRescoreCron("")).toBe(false);
});

test("shouldRunRescoreCron rejects an unrelated cron", () => {
  expect(shouldRunRescoreCron("*/10 * * * *")).toBe(false);
});

test("COORDINATOR_CRON_RACE_HOURS is the every-5-min race-hours schedule", () => {
  expect(COORDINATOR_CRON_RACE_HOURS).toBe("*/5 1-11 * * *");
});

test("shouldRunCoordinatorCron matches the coordinator race-hours cron", () => {
  expect(shouldRunCoordinatorCron("*/5 1-11 * * *")).toBe(true);
});

test("shouldRunCoordinatorCron rejects the rescore cron", () => {
  expect(shouldRunCoordinatorCron("*/20 1-11 * * *")).toBe(false);
});

test("shouldRunCoordinatorCron rejects the warm race-hours cron", () => {
  expect(shouldRunCoordinatorCron("*/30 1-11 * * *")).toBe(false);
});

test("shouldRunCoordinatorCron rejects the predict cron", () => {
  expect(shouldRunCoordinatorCron("0 18 * * *")).toBe(false);
});

test("shouldRunCoordinatorCron rejects an empty string", () => {
  expect(shouldRunCoordinatorCron("")).toBe(false);
});

test("shouldRunWarmCron rejects the coordinator cron", () => {
  expect(shouldRunWarmCron("*/5 1-11 * * *")).toBe(false);
});

test("FEATURE_BUILD_CRON is the JST 09:30 schedule", () => {
  expect(FEATURE_BUILD_CRON).toBe("30 0 * * *");
});

test("shouldRunFeatureBuildCron matches the feature-build cron", () => {
  expect(shouldRunFeatureBuildCron("30 0 * * *")).toBe(true);
});

test("shouldRunFeatureBuildCron rejects the pre-JRA warm cron", () => {
  expect(shouldRunFeatureBuildCron("25 0 * * *")).toBe(false);
});

test("shouldRunFeatureBuildCron rejects the coordinator cron", () => {
  expect(shouldRunFeatureBuildCron("*/5 1-11 * * *")).toBe(false);
});

test("shouldRunFeatureBuildCron rejects the predict cron", () => {
  expect(shouldRunFeatureBuildCron("0 18 * * *")).toBe(false);
});

test("shouldRunFeatureBuildCron rejects an empty string", () => {
  expect(shouldRunFeatureBuildCron("")).toBe(false);
});

test("shouldRunWarmCron rejects the feature-build cron", () => {
  expect(shouldRunWarmCron("30 0 * * *")).toBe(false);
});

test("shouldRunCoordinatorCron rejects the feature-build cron", () => {
  expect(shouldRunCoordinatorCron("30 0 * * *")).toBe(false);
});

test("shouldRunRescoreCron rejects the feature-build cron", () => {
  expect(shouldRunRescoreCron("30 0 * * *")).toBe(false);
});

test("shouldRunPredictCron rejects the feature-build cron", () => {
  expect(shouldRunPredictCron("30 0 * * *")).toBe(false);
});
