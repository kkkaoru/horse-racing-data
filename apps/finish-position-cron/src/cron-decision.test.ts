// Run with bun. Tests for the cron gate.

import { expect, test } from "vitest";
import { PREDICT_CRON, shouldRunPredictCron } from "./cron-decision";

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
  // wrangler.jsonc currently sets triggers.crons = []. Cloudflare will not fire
  // scheduled() at all in that state, but defend against an accidental empty
  // event.cron arriving anyway.
  expect(shouldRunPredictCron("")).toBe(false);
});
