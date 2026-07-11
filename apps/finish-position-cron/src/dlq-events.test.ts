// Run with bun. Tests for the dead-letter event record builder.

import { expect, test } from "vitest";
import { buildDlqEventBindParams, buildDlqEventInsertSql, buildDlqEventRecord } from "./dlq-events";

test("buildDlqEventRecord returns a normalised per-race record", () => {
  const record = buildDlqEventRecord({
    category: "jra",
    keibajoCode: "02",
    mode: "full",
    raceBango: "01",
    redriveCount: 0,
    redriven: true,
    runYmd: "20260712",
  });
  expect(record).toStrictEqual({
    category: "jra",
    keibajoCode: "02",
    mode: "full",
    raceBango: "01",
    redriveCount: 0,
    redriven: true,
    runYmd: "20260712",
  });
});

test("buildDlqEventRecord defaults absent keibajoCode/raceBango to null", () => {
  const record = buildDlqEventRecord({
    category: "nar",
    mode: "rescore",
    redriveCount: 1,
    redriven: false,
    runYmd: "20260712",
  });
  expect(record.keibajoCode).toBe(null);
  expect(record.raceBango).toBe(null);
});

test("buildDlqEventRecord rejects negative redriveCount", () => {
  expect(() =>
    buildDlqEventRecord({
      category: "jra",
      mode: "full",
      redriveCount: -1,
      redriven: false,
      runYmd: "20260712",
    }),
  ).toThrow("redriveCount must be non-negative");
});

test("buildDlqEventInsertSql targets the dlq events table", () => {
  expect(buildDlqEventInsertSql()).toBe(
    `insert into finish_position_predict_dlq_events (run_ymd, category, mode, keibajo_code, race_bango, redrive_count, redriven)
     values (?1, ?2, ?3, ?4, ?5, ?6, ?7)`,
  );
});

test("buildDlqEventBindParams orders params to match the placeholders and encodes redriven as 1", () => {
  const record = buildDlqEventRecord({
    category: "ban-ei",
    keibajoCode: "83",
    mode: "full",
    raceBango: "03",
    redriveCount: 0,
    redriven: true,
    runYmd: "20260712",
  });
  expect(buildDlqEventBindParams(record)).toStrictEqual([
    "20260712",
    "ban-ei",
    "full",
    "83",
    "03",
    0,
    1,
  ]);
});

test("buildDlqEventBindParams encodes redriven false as 0", () => {
  const record = buildDlqEventRecord({
    category: "nar",
    mode: "rescore",
    redriveCount: 1,
    redriven: false,
    runYmd: "20260712",
  });
  expect(buildDlqEventBindParams(record)).toStrictEqual([
    "20260712",
    "nar",
    "rescore",
    null,
    null,
    1,
    0,
  ]);
});
