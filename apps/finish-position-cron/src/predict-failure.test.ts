// Run with bun. Tests for predict-queue failure snapshot normalisation.

import { expect, test } from "vitest";
import {
  PREDICT_FAILURE_BODY_EXCERPT_MAX_CHARS,
  PREDICT_FAILURE_MESSAGE_MAX_CHARS,
  PREDICT_FAILURE_STACK_MAX_CHARS,
  parsePredictFailure,
  truncateFailureText,
} from "./predict-failure";

test("truncateFailureText returns the original string when it fits", () => {
  expect(truncateFailureText("abc", 3)).toBe("abc");
  expect(truncateFailureText("ab", 3)).toBe("ab");
});

test("truncateFailureText slices to maxChars when longer", () => {
  expect(truncateFailureText("abcd", 3)).toBe("abc");
});

test("parsePredictFailure extracts Error name, message, and stack", () => {
  const err = new TypeError("boom");
  const snapshot = parsePredictFailure(err);
  expect(snapshot.errorName).toBe("TypeError");
  expect(snapshot.errorMessage).toBe("boom");
  expect(snapshot.errorStack).toContain("TypeError: boom");
  expect(snapshot.httpStatus).toBe(null);
  expect(snapshot.httpBodyExcerpt).toBe(null);
});

test("parsePredictFailure extracts HTTP status and body excerpt from Container DO errors", () => {
  const err = new Error(
    'Container DO returned 502: {"error":"Container start failed","detail":"timeout"}',
  );
  expect(parsePredictFailure(err)).toStrictEqual({
    errorMessage:
      'Container DO returned 502: {"error":"Container start failed","detail":"timeout"}',
    errorName: "Error",
    errorStack: err.stack ?? null,
    httpBodyExcerpt: '{"error":"Container start failed","detail":"timeout"}',
    httpStatus: 502,
  });
});

test("parsePredictFailure truncates a very long Container DO body excerpt", () => {
  const body = "x".repeat(PREDICT_FAILURE_BODY_EXCERPT_MAX_CHARS + 50);
  const snapshot = parsePredictFailure(new Error(`Container DO returned 503: ${body}`));
  expect(snapshot.httpStatus).toBe(503);
  expect(snapshot.httpBodyExcerpt).toBe("x".repeat(PREDICT_FAILURE_BODY_EXCERPT_MAX_CHARS));
});

test("parsePredictFailure truncates a very long Error message and stack", () => {
  const longMessage = "m".repeat(PREDICT_FAILURE_MESSAGE_MAX_CHARS + 10);
  const err = new Error(longMessage);
  err.stack = "s".repeat(PREDICT_FAILURE_STACK_MAX_CHARS + 10);
  const snapshot = parsePredictFailure(err);
  expect(snapshot.errorMessage).toBe("m".repeat(PREDICT_FAILURE_MESSAGE_MAX_CHARS));
  expect(snapshot.errorStack).toBe("s".repeat(PREDICT_FAILURE_STACK_MAX_CHARS));
});

test("parsePredictFailure treats a non-Error throw as a string message", () => {
  expect(parsePredictFailure("plain failure")).toStrictEqual({
    errorMessage: "plain failure",
    errorName: null,
    errorStack: null,
    httpBodyExcerpt: null,
    httpStatus: null,
  });
});

test("parsePredictFailure treats an Error without stack as stack=null", () => {
  const err = new Error("no stack");
  err.stack = undefined;
  expect(parsePredictFailure(err).errorStack).toBe(null);
});
