// Run with bun.
import { expect, it } from "vitest";

import { errorLogFields, formatError, formatErrorLogLine } from "./format-error";

it("returns the message of an Error instance", () => {
  expect(formatError(new Error("boom"))).toBe("boom");
});

it("stringifies non-Error values via String()", () => {
  expect(formatError("raw")).toBe("raw");
  expect(formatError(42)).toBe("42");
  expect(formatError(null)).toBe("null");
});

it("errorLogFields keeps Error name message and stack", () => {
  const error = new Error("boom");
  expect(errorLogFields(error)).toStrictEqual({
    message: "boom",
    name: "Error",
    stack: error.stack,
  });
});

it("errorLogFields uses an empty stack when Error.stack is missing", () => {
  const error = Object.assign(Object.create(Error.prototype), {
    message: "boom",
    name: "Error",
  }) as Error;
  expect(errorLogFields(error)).toStrictEqual({
    message: "boom",
    name: "Error",
    stack: "",
  });
});

it("errorLogFields stringifies non-Error values", () => {
  expect(errorLogFields("raw")).toStrictEqual({
    message: "raw",
    name: "unknown",
    stack: "",
  });
  expect(errorLogFields(42)).toStrictEqual({
    message: "42",
    name: "unknown",
    stack: "",
  });
  expect(errorLogFields(null)).toStrictEqual({
    message: "null",
    name: "unknown",
    stack: "",
  });
});

it("formatErrorLogLine keeps prefix extra fields and Error details in one string", () => {
  const error = new Error("boom");
  expect(
    formatErrorLogLine("Queue job failed", { type: "generate-running-style-predictions" }, error),
  ).toBe(
    `Queue job failed type=generate-running-style-predictions name=Error message=boom stack=${error.stack}`,
  );
});

it("formatErrorLogLine stringifies non-Error values", () => {
  expect(
    formatErrorLogLine("Running-style cache/sync failed", { raceKey: "nar:20260810:42:03" }, null),
  ).toBe(
    "Running-style cache/sync failed raceKey=nar:20260810:42:03 name=unknown message=null stack=",
  );
});
