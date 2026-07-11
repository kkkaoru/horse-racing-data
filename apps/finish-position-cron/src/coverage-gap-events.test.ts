// Run with bun. Tests for the coverage-gap event record builder.

import { expect, test } from "vitest";
import {
  buildCoverageGapEventBindParams,
  buildCoverageGapEventInsertSql,
  buildCoverageGapEventRecord,
} from "./coverage-gap-events";

test("buildCoverageGapEventRecord returns the input record unchanged for a valid enqueue event", () => {
  const record = buildCoverageGapEventRecord({
    category: "jra",
    enqueued: true,
    escalated: false,
    keibajoCode: "05",
    priorEnqueueCount: 0,
    raceBango: "11",
    raceStartAtJst: "2026-07-12T14:00:00+09:00",
    runYmd: "20260712",
  });
  expect(record).toStrictEqual({
    category: "jra",
    enqueued: true,
    escalated: false,
    keibajoCode: "05",
    priorEnqueueCount: 0,
    raceBango: "11",
    raceStartAtJst: "2026-07-12T14:00:00+09:00",
    runYmd: "20260712",
  });
});

test("buildCoverageGapEventRecord returns the input record unchanged for an escalation event", () => {
  const record = buildCoverageGapEventRecord({
    category: "nar",
    enqueued: false,
    escalated: true,
    keibajoCode: "44",
    priorEnqueueCount: 2,
    raceBango: "07",
    raceStartAtJst: "2026-07-12T15:30:00+09:00",
    runYmd: "20260712",
  });
  expect(record).toStrictEqual({
    category: "nar",
    enqueued: false,
    escalated: true,
    keibajoCode: "44",
    priorEnqueueCount: 2,
    raceBango: "07",
    raceStartAtJst: "2026-07-12T15:30:00+09:00",
    runYmd: "20260712",
  });
});

test("buildCoverageGapEventRecord rejects negative priorEnqueueCount", () => {
  expect(() =>
    buildCoverageGapEventRecord({
      category: "ban-ei",
      enqueued: true,
      escalated: false,
      keibajoCode: "83",
      priorEnqueueCount: -1,
      raceBango: "03",
      raceStartAtJst: "2026-07-12T16:00:00+09:00",
      runYmd: "20260712",
    }),
  ).toThrow("priorEnqueueCount must be non-negative");
});

test("buildCoverageGapEventInsertSql targets the coverage gap events table", () => {
  expect(buildCoverageGapEventInsertSql()).toBe(
    `insert into finish_position_coverage_gap_events (run_ymd, category, keibajo_code, race_bango, race_start_at_jst, prior_enqueue_count, enqueued, escalated)
     values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)`,
  );
});

test("buildCoverageGapEventBindParams orders params to match the placeholders and encodes booleans as 1", () => {
  const record = buildCoverageGapEventRecord({
    category: "ban-ei",
    enqueued: true,
    escalated: true,
    keibajoCode: "83",
    priorEnqueueCount: 2,
    raceBango: "03",
    raceStartAtJst: "2026-07-12T16:00:00+09:00",
    runYmd: "20260712",
  });
  expect(buildCoverageGapEventBindParams(record)).toStrictEqual([
    "20260712",
    "ban-ei",
    "83",
    "03",
    "2026-07-12T16:00:00+09:00",
    2,
    1,
    1,
  ]);
});

test("buildCoverageGapEventBindParams encodes false booleans as 0", () => {
  const record = buildCoverageGapEventRecord({
    category: "jra",
    enqueued: false,
    escalated: false,
    keibajoCode: "05",
    priorEnqueueCount: 0,
    raceBango: "11",
    raceStartAtJst: "2026-07-12T14:00:00+09:00",
    runYmd: "20260712",
  });
  expect(buildCoverageGapEventBindParams(record)).toStrictEqual([
    "20260712",
    "jra",
    "05",
    "11",
    "2026-07-12T14:00:00+09:00",
    0,
    0,
    0,
  ]);
});
