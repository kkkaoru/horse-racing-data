// Run with bun. Tests for the dead-letter event record builder.

import { expect, test } from "vitest";
import {
  buildDlqEventBindParams,
  buildDlqEventInsertSql,
  buildDlqEventRecord,
  emptyPredictFailure,
} from "./dlq-events";

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
    errorMessage: null,
    errorName: null,
    errorStack: null,
    httpBodyExcerpt: null,
    httpStatus: null,
    keibajoCode: "02",
    mode: "full",
    queueAttempts: null,
    queueMessageId: null,
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

test("buildDlqEventRecord keeps failure snapshot and queueAttempts when provided", () => {
  const record = buildDlqEventRecord({
    category: "ban-ei",
    errorMessage: "Container DO returned 503: no instance",
    errorName: "Error",
    errorStack: "Error: Container DO returned 503: no instance",
    httpBodyExcerpt: "no instance",
    httpStatus: 503,
    keibajoCode: "83",
    mode: "full",
    queueAttempts: 16,
    raceBango: "06",
    redriveCount: 0,
    redriven: true,
    runYmd: "20260809",
  });
  expect(record.errorName).toBe("Error");
  expect(record.errorMessage).toBe("Container DO returned 503: no instance");
  expect(record.httpStatus).toBe(503);
  expect(record.httpBodyExcerpt).toBe("no instance");
  expect(record.queueAttempts).toBe(16);
});

test("buildDlqEventInsertSql inserts message identity idempotently", () => {
  expect(buildDlqEventInsertSql()).toBe(
    `insert into finish_position_predict_dlq_events (run_ymd, category, mode, keibajo_code, race_bango, queue_message_id, redrive_count, redriven, error_name, error_message, error_stack, http_status, http_body_excerpt, queue_attempts)
     values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
     on conflict do nothing`,
  );
});

test("buildDlqEventBindParams orders params to match the placeholders and encodes redriven as 1", () => {
  const record = buildDlqEventRecord({
    category: "ban-ei",
    errorMessage: "busy budget exhausted",
    errorName: "Error",
    errorStack: null,
    httpBodyExcerpt: null,
    httpStatus: null,
    keibajoCode: "83",
    mode: "full",
    queueAttempts: 16,
    queueMessageId: "dlq-msg-1",
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
    "dlq-msg-1",
    0,
    1,
    "Error",
    "busy budget exhausted",
    null,
    null,
    null,
    16,
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
    null,
    1,
    0,
    null,
    null,
    null,
    null,
    null,
    null,
  ]);
});

test("emptyPredictFailure returns all-null snapshot fields", () => {
  expect(emptyPredictFailure()).toStrictEqual({
    errorMessage: null,
    errorName: null,
    errorStack: null,
    httpBodyExcerpt: null,
    httpStatus: null,
  });
});
