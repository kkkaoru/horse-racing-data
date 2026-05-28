// Run with bun.
import { expect, it } from "vitest";

import { formatError } from "./format-error";

it("returns the message from an Error instance", () => {
  expect(formatError(new Error("oops"))).toBe("oops");
});

it("stringifies non-Error values", () => {
  expect(formatError({ code: 1 })).toBe("[object Object]");
});

it("stringifies string values", () => {
  expect(formatError("plain")).toBe("plain");
});
