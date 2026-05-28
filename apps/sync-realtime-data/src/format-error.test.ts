// Run with bun.
import { expect, it } from "vitest";

import { formatError } from "./format-error";

it("returns the message of an Error instance", () => {
  expect(formatError(new Error("boom"))).toBe("boom");
});

it("stringifies non-Error values via String()", () => {
  expect(formatError("raw")).toBe("raw");
  expect(formatError(42)).toBe("42");
  expect(formatError(null)).toBe("null");
});
