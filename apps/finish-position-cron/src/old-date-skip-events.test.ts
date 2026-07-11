// Run with bun. Tests for the old-date-skip event record builder.

import { expect, test } from "vitest";
import {
  buildOldDateSkipEventBindParams,
  buildOldDateSkipEventInsertSql,
  buildOldDateSkipEventRecord,
} from "./old-date-skip-events";

test("buildOldDateSkipEventRecord returns a normalised per-race record", () => {
  const record = buildOldDateSkipEventRecord({
    category: "jra",
    keibajoCode: "02",
    mode: "full",
    raceBango: "01",
    runYmd: "20260603",
    thresholdDays: 2,
  });
  expect(record).toStrictEqual({
    category: "jra",
    keibajoCode: "02",
    mode: "full",
    raceBango: "01",
    runYmd: "20260603",
    thresholdDays: 2,
  });
});

test("buildOldDateSkipEventRecord defaults absent keibajoCode/raceBango to null", () => {
  const record = buildOldDateSkipEventRecord({
    category: "nar",
    mode: "rescore",
    runYmd: "20260603",
    thresholdDays: 2,
  });
  expect(record.keibajoCode).toBe(null);
  expect(record.raceBango).toBe(null);
});

test("buildOldDateSkipEventRecord rejects negative thresholdDays", () => {
  expect(() =>
    buildOldDateSkipEventRecord({
      category: "jra",
      mode: "full",
      runYmd: "20260603",
      thresholdDays: -1,
    }),
  ).toThrow("thresholdDays must be non-negative");
});

test("buildOldDateSkipEventInsertSql targets the old date skip events table", () => {
  expect(buildOldDateSkipEventInsertSql()).toBe(
    `insert into finish_position_old_date_skip_events (run_ymd, category, mode, keibajo_code, race_bango, threshold_days)
     values (?1, ?2, ?3, ?4, ?5, ?6)`,
  );
});

test("buildOldDateSkipEventBindParams orders params to match the placeholders", () => {
  const record = buildOldDateSkipEventRecord({
    category: "ban-ei",
    keibajoCode: "83",
    mode: "full",
    raceBango: "03",
    runYmd: "20260603",
    thresholdDays: 2,
  });
  expect(buildOldDateSkipEventBindParams(record)).toStrictEqual([
    "20260603",
    "ban-ei",
    "full",
    "83",
    "03",
    2,
  ]);
});

test("buildOldDateSkipEventBindParams encodes absent keibajoCode/raceBango as null", () => {
  const record = buildOldDateSkipEventRecord({
    category: "nar",
    mode: "rescore",
    runYmd: "20260603",
    thresholdDays: 2,
  });
  expect(buildOldDateSkipEventBindParams(record)).toStrictEqual([
    "20260603",
    "nar",
    "rescore",
    null,
    null,
    2,
  ]);
});
