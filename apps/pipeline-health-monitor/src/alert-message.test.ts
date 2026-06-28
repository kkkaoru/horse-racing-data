// Run with: bun run --filter pipeline-health-monitor test
import { expect, it } from "vitest";

import { buildAlertMessage } from "./alert-message";

// 2026-06-28 15:30:00 JST = 2026-06-28T06:30:00Z. Fixed timestamp keeps
// formatJstIso output literal-asserts deterministic.
const FIXED_NOW = new Date("2026-06-28T06:30:00Z");

it("buildAlertMessage produces a warning-tagged title and field set", () => {
  const message = buildAlertMessage({
    check: {
      name: "fetch-results-staleness",
      ok: false,
      value: 45,
      threshold: 30,
      message: "exceeded freshness threshold",
    },
    severity: "warning",
    failureCount: 2,
    nowJst: FIXED_NOW,
  });
  expect(message).toStrictEqual({
    checkName: "fetch-results-staleness",
    severity: "warning",
    title: "[WARNING] fetch-results-staleness",
    description: "exceeded freshness threshold (value=45, threshold=30)",
    fields: [
      { name: "Check", value: "fetch-results-staleness" },
      { name: "Value", value: "45" },
      { name: "Threshold", value: "30" },
      { name: "Failure Count", value: "2" },
    ],
    timestampJst: "2026-06-28T15:30:00+09:00",
  });
});

it("buildAlertMessage produces a critical-tagged title", () => {
  const message = buildAlertMessage({
    check: {
      name: "races-queued-not-fetched-today",
      ok: false,
      value: 12,
      threshold: 10,
      message: "exceeded counter threshold",
    },
    severity: "critical",
    failureCount: 3,
    nowJst: FIXED_NOW,
  });
  expect(message).toStrictEqual({
    checkName: "races-queued-not-fetched-today",
    severity: "critical",
    title: "[CRITICAL] races-queued-not-fetched-today",
    description: "exceeded counter threshold (value=12, threshold=10)",
    fields: [
      { name: "Check", value: "races-queued-not-fetched-today" },
      { name: "Value", value: "12" },
      { name: "Threshold", value: "10" },
      { name: "Failure Count", value: "3" },
    ],
    timestampJst: "2026-06-28T15:30:00+09:00",
  });
});

it("buildAlertMessage produces a recovery-tagged title", () => {
  const message = buildAlertMessage({
    check: {
      name: "fetch-weights-staleness",
      ok: true,
      value: 5,
      threshold: 30,
      message: "within freshness threshold",
    },
    severity: "recovery",
    failureCount: 4,
    nowJst: FIXED_NOW,
  });
  expect(message).toStrictEqual({
    checkName: "fetch-weights-staleness",
    severity: "recovery",
    title: "[RECOVERY] fetch-weights-staleness",
    description: "within freshness threshold (value=5, threshold=30)",
    fields: [
      { name: "Check", value: "fetch-weights-staleness" },
      { name: "Value", value: "5" },
      { name: "Threshold", value: "30" },
      { name: "Failure Count", value: "4" },
    ],
    timestampJst: "2026-06-28T15:30:00+09:00",
  });
});
