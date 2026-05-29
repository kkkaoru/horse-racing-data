// Run with: bun run --filter sync-realtime-data-features test
import { expect, it } from "vitest";

import { formatError } from "./format-error";

it("returns stack for Error with stack", () => {
  const error = new Error("boom");
  expect(formatError(error)).toBe(error.stack);
});

it("returns message when stack is missing", () => {
  const error = new Error("no stack");
  Object.defineProperty(error, "stack", { value: undefined });
  expect(formatError(error)).toBe("no stack");
});

it("returns string verbatim", () => {
  expect(formatError("oops")).toBe("oops");
});

it("returns JSON for plain object", () => {
  expect(formatError({ code: 1 })).toBe('{"code":1}');
});
