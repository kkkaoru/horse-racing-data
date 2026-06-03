// Run with bun. Tests for the audit-record builder.

import { expect, test } from "vitest";
import { buildAuditBindParams, buildAuditInsertSql, buildAuditRecord } from "./audit";

test("buildAuditRecord returns a normalised started record", () => {
  const record = buildAuditRecord({
    durationMs: 1200,
    error: null,
    racesPredicted: 0,
    runDate: "2026-06-03",
    status: "started",
  });
  expect(record).toStrictEqual({
    durationMs: 1200,
    error: null,
    racesPredicted: 0,
    runDate: "2026-06-03",
    status: "started",
  });
});

test("buildAuditRecord keeps the error message on failure", () => {
  const record = buildAuditRecord({
    durationMs: 50,
    error: "container failed to start",
    racesPredicted: 0,
    runDate: "2026-06-03",
    status: "error",
  });
  expect(record.error).toBe("container failed to start");
});

test("buildAuditRecord rejects negative racesPredicted", () => {
  expect(() =>
    buildAuditRecord({
      durationMs: 10,
      error: null,
      racesPredicted: -1,
      runDate: "2026-06-03",
      status: "success",
    }),
  ).toThrow("racesPredicted must be non-negative");
});

test("buildAuditRecord rejects negative durationMs", () => {
  expect(() =>
    buildAuditRecord({
      durationMs: -5,
      error: null,
      racesPredicted: 1,
      runDate: "2026-06-03",
      status: "success",
    }),
  ).toThrow("durationMs must be non-negative");
});

test("buildAuditInsertSql targets the executions table", () => {
  expect(buildAuditInsertSql()).toBe(
    `insert into finish_position_cron_executions (run_date, status, races_predicted, duration_ms, error)
     values (?1, ?2, ?3, ?4, ?5)`,
  );
});

test("buildAuditBindParams orders params to match the placeholders", () => {
  const record = buildAuditRecord({
    durationMs: 90000,
    error: null,
    racesPredicted: 412,
    runDate: "2026-06-03",
    status: "success",
  });
  expect(buildAuditBindParams(record)).toStrictEqual(["2026-06-03", "success", 412, 90000, null]);
});
