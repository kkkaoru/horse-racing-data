// Run with bun test apps/pc-keiba-viewer/src/lib/db-query-cacheability.test.ts
import { expect, test } from "vitest";

import { isEmptyQueryResult } from "./db-query-cacheability";

test("isEmptyQueryResult treats null as empty", () => {
  expect(isEmptyQueryResult(null)).toBe(true);
});

test("isEmptyQueryResult treats undefined as empty", () => {
  expect(isEmptyQueryResult(undefined)).toBe(true);
});

test("isEmptyQueryResult treats an empty array as empty", () => {
  expect(isEmptyQueryResult([])).toBe(true);
});

test("isEmptyQueryResult treats a non-empty array as cacheable", () => {
  expect(isEmptyQueryResult([1])).toBe(false);
});

test("isEmptyQueryResult treats a plain object as cacheable", () => {
  expect(isEmptyQueryResult({ a: 1 })).toBe(false);
});

test("isEmptyQueryResult treats the number zero as cacheable", () => {
  expect(isEmptyQueryResult(0)).toBe(false);
});
