// Run with: bun run --filter finish-position-cron test
import { expect, test } from "vitest";
import {
  assertFeatureCount,
  coerceFeature,
  projectCatBoostCells,
  projectFeatureRow,
} from "./feature-projection";

test("coerceFeature maps null to 0", () => {
  expect(coerceFeature(null)).toBe(0);
});

test("coerceFeature maps undefined to 0", () => {
  expect(coerceFeature(undefined)).toBe(0);
});

test("coerceFeature maps empty string to 0", () => {
  expect(coerceFeature("")).toBe(0);
});

test("coerceFeature maps true to 1 and false to 0", () => {
  expect(coerceFeature(true)).toBe(1);
  expect(coerceFeature(false)).toBe(0);
});

test("coerceFeature passes through a number unchanged", () => {
  expect(coerceFeature(3.14)).toBe(3.14);
});

test("coerceFeature parses a numeric string after trimming", () => {
  expect(coerceFeature(" 42.5 ")).toBe(42.5);
});

test("coerceFeature converts a bigint cell from a decoded int64 column", () => {
  expect(coerceFeature(16n)).toBe(16);
});

test("coerceFeature maps a non-string non-primitive cell to 0", () => {
  expect(coerceFeature({ unexpected: true })).toBe(0);
});

test("projectFeatureRow projects entry onto feature order with float64 for catboost", () => {
  const row = projectFeatureRow(
    { a: 1, b: null, c: "2.5" },
    ["a", "b", "c", "missing"],
    "catboost",
  );
  expect(row).toStrictEqual([1, 0, 2.5, 0]);
});

test("projectFeatureRow quantises to float32 for xgboost", () => {
  const row = projectFeatureRow({ a: 0.1 }, ["a"], "xgboost");
  expect(row).toStrictEqual([new Float32Array([0.1])[0]]);
});

test("projectCatBoostCells builds non-missing cells with coerced values", () => {
  const cells = projectCatBoostCells({ a: 1, b: null }, ["a", "b"]);
  expect(cells).toStrictEqual([
    { isMissing: false, value: 1 },
    { isMissing: false, value: 0 },
  ]);
});

test("assertFeatureCount accepts a matching count", () => {
  expect(() => assertFeatureCount(["a", "b"], 2)).not.toThrow();
});

test("assertFeatureCount throws on a width mismatch", () => {
  expect(() => assertFeatureCount(["a", "b"], 3)).toThrow("expected 3 features, metadata has 2");
});
