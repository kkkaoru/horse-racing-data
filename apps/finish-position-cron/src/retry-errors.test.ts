// Run with bun. Tests for retry-error D1 row builders and lookup SQL.

import { expect, test } from "vitest";
import {
  buildRetryErrorBindParams,
  buildRetryErrorInsertSql,
  buildRetryErrorLookupByMessageIdSql,
  buildRetryErrorLookupByRaceSql,
  buildRetryErrorRecord,
  retryErrorLookupRowToSnapshot,
} from "./retry-errors";

test("buildRetryErrorRecord copies failure fields and defaults optional ids to null", () => {
  expect(
    buildRetryErrorRecord({
      category: "ban-ei",
      errorMessage: "Container DO returned 503: no instance",
      errorName: "Error",
      errorStack: "Error: Container DO returned 503: no instance",
      httpBodyExcerpt: "no instance",
      httpStatus: 503,
      mode: "full",
      runYmd: "20260809",
    }),
  ).toStrictEqual({
    category: "ban-ei",
    errorMessage: "Container DO returned 503: no instance",
    errorName: "Error",
    errorStack: "Error: Container DO returned 503: no instance",
    httpBodyExcerpt: "no instance",
    httpStatus: 503,
    keibajoCode: null,
    mode: "full",
    queueAttempts: null,
    queueMessageId: null,
    raceBango: null,
    runYmd: "20260809",
  });
});

test("buildRetryErrorRecord keeps optional race scope and queue metadata when provided", () => {
  const record = buildRetryErrorRecord({
    category: "nar",
    errorMessage: "boom",
    errorName: "TypeError",
    errorStack: null,
    httpBodyExcerpt: null,
    httpStatus: null,
    keibajoCode: "44",
    mode: "rescore",
    queueAttempts: 4,
    queueMessageId: "msg-1",
    raceBango: "06",
    runYmd: "20260809",
  });
  expect(record.keibajoCode).toBe("44");
  expect(record.raceBango).toBe("06");
  expect(record.queueAttempts).toBe(4);
  expect(record.queueMessageId).toBe("msg-1");
});

test("buildRetryErrorInsertSql targets the retry errors table", () => {
  expect(buildRetryErrorInsertSql()).toBe(
    `insert into finish_position_predict_retry_errors (queue_message_id, run_ymd, category, mode, keibajo_code, race_bango, error_name, error_message, error_stack, http_status, http_body_excerpt, queue_attempts)
     values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)`,
  );
});

test("buildRetryErrorBindParams orders params to match the placeholders", () => {
  expect(
    buildRetryErrorBindParams(
      buildRetryErrorRecord({
        category: "jra",
        errorMessage: "network timeout",
        errorName: "Error",
        errorStack: "Error: network timeout",
        httpBodyExcerpt: null,
        httpStatus: null,
        keibajoCode: "05",
        mode: "full",
        queueAttempts: 2,
        queueMessageId: "msg-2",
        raceBango: "11",
        runYmd: "20260809",
      }),
    ),
  ).toStrictEqual([
    "msg-2",
    "20260809",
    "jra",
    "full",
    "05",
    "11",
    "Error",
    "network timeout",
    "Error: network timeout",
    null,
    null,
    2,
  ]);
});

test("buildRetryErrorLookupByMessageIdSql filters on queue_message_id newest first", () => {
  expect(buildRetryErrorLookupByMessageIdSql()).toContain("where queue_message_id = ?1");
  expect(buildRetryErrorLookupByMessageIdSql()).toContain("order by id desc");
  expect(buildRetryErrorLookupByMessageIdSql()).toContain("limit 1");
});

test("buildRetryErrorLookupByRaceSql matches nullable keibajo/race via ifnull", () => {
  expect(buildRetryErrorLookupByRaceSql()).toContain("ifnull(keibajo_code, '') = ifnull(?4, '')");
  expect(buildRetryErrorLookupByRaceSql()).toContain("ifnull(race_bango, '') = ifnull(?5, '')");
});

test("retryErrorLookupRowToSnapshot drops queueAttempts", () => {
  expect(
    retryErrorLookupRowToSnapshot({
      errorMessage: "boom",
      errorName: "Error",
      errorStack: null,
      httpBodyExcerpt: null,
      httpStatus: 502,
      queueAttempts: 16,
    }),
  ).toStrictEqual({
    errorMessage: "boom",
    errorName: "Error",
    errorStack: null,
    httpBodyExcerpt: null,
    httpStatus: 502,
  });
});
