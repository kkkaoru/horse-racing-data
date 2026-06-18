// Run with bun. Tests for the cron gate.

import { expect, test } from "vitest";
import {
  PREDICT_CRON,
  WARM_CRON_PRE_JRA,
  WARM_CRON_PRE_NAR,
  WARM_CRON_RACE_HOURS,
  shouldRunPredictCron,
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
