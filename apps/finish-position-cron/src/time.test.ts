// Run with bun. Tests for JST run-date helpers.

import { expect, test } from "vitest";
import { getRunDateJst, getRunYmdJst } from "./time";

test("getRunDateJst returns the JST calendar day for an 18:00 UTC trigger", () => {
  expect(getRunDateJst(new Date("2026-06-02T18:00:00.000Z"))).toBe("2026-06-03");
});

test("getRunDateJst stays on the same JST day for a midday UTC instant", () => {
  expect(getRunDateJst(new Date("2026-06-03T01:00:00.000Z"))).toBe("2026-06-03");
});

test("getRunYmdJst returns the 8-digit form", () => {
  expect(getRunYmdJst(new Date("2026-06-02T18:00:00.000Z"))).toBe("20260603");
});

test("getRunYmdJst handles year boundary", () => {
  expect(getRunYmdJst(new Date("2026-12-31T18:00:00.000Z"))).toBe("20270101");
});
