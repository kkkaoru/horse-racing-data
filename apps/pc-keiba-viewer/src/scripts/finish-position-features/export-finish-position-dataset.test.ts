import { expect, test } from "vitest";

import {
  applyArg,
  buildUsageText,
  csvEscape,
  initialOptions,
  isCategory,
  isTarget,
  parseArgs,
  resolveOutputPath,
} from "./export-finish-position-dataset";

test("initialOptions defaults to a JRA / local / 2016-2025 window", () => {
  expect(initialOptions()).toStrictEqual({
    category: "jra",
    featureSchemaVersion: "v1",
    fromDate: "20160101",
    output: "",
    target: "local",
    toDate: "20251231",
  });
});

test("isCategory recognises known categories", () => {
  expect(isCategory("jra")).toBe(true);
  expect(isCategory("nar")).toBe(true);
  expect(isCategory("ban-ei")).toBe(true);
  expect(isCategory("all")).toBe(true);
});

test("isCategory rejects unknowns", () => {
  expect(isCategory("xx")).toBe(false);
});

test("isTarget recognises local and neon", () => {
  expect(isTarget("local")).toBe(true);
  expect(isTarget("neon")).toBe(true);
});

test("isTarget rejects unknowns", () => {
  expect(isTarget("staging")).toBe(false);
});

test("buildUsageText documents the CLI", () => {
  expect(buildUsageText()).toContain("--output");
  expect(buildUsageText()).toContain("--feature-schema-version");
});

test("applyArg sets target", () => {
  const opts = initialOptions();
  applyArg(opts, "--target", "neon");
  expect(opts.target).toBe("neon");
});

test("applyArg rejects invalid target", () => {
  expect(() => applyArg(initialOptions(), "--target", "x")).toThrow(
    "--target must be local or neon.",
  );
});

test("applyArg strips hyphens from --from-date", () => {
  const opts = initialOptions();
  applyArg(opts, "--from-date", "2016-01-01");
  expect(opts.fromDate).toBe("20160101");
});

test("applyArg sets --output", () => {
  const opts = initialOptions();
  applyArg(opts, "--output", "tmp/dataset.csv");
  expect(opts.output).toBe("tmp/dataset.csv");
});

test("applyArg rejects unknown flag", () => {
  expect(() => applyArg(initialOptions(), "--bogus", "x")).toThrow("Unknown argument: --bogus");
});

test("parseArgs requires --output", () => {
  expect(() => parseArgs([])).toThrow("--output is required.");
});

test("parseArgs accepts a full flag list", () => {
  const parsed = parseArgs([
    "--target",
    "neon",
    "--category",
    "ban-ei",
    "--from-date",
    "20200101",
    "--to-date",
    "20251231",
    "--feature-schema-version",
    "v2",
    "--output",
    "tmp/out.csv",
  ]);
  expect(parsed).toStrictEqual({
    category: "ban-ei",
    featureSchemaVersion: "v2",
    fromDate: "20200101",
    output: "tmp/out.csv",
    target: "neon",
    toDate: "20251231",
  });
});

test("csvEscape emits empty string for null", () => {
  expect(csvEscape(null)).toBe("");
});

test("csvEscape emits empty string for undefined", () => {
  expect(csvEscape(undefined)).toBe("");
});

test("csvEscape emits empty string for NaN", () => {
  expect(csvEscape(Number.NaN)).toBe("");
});

test("csvEscape emits empty string for Infinity", () => {
  expect(csvEscape(Number.POSITIVE_INFINITY)).toBe("");
});

test("csvEscape stringifies finite numbers", () => {
  expect(csvEscape(42)).toBe("42");
  expect(csvEscape(-3.14)).toBe("-3.14");
});

test("csvEscape stringifies bigint", () => {
  expect(csvEscape(123n)).toBe("123");
});

test("csvEscape boolean is 0/1", () => {
  expect(csvEscape(true)).toBe("1");
  expect(csvEscape(false)).toBe("0");
});

test("csvEscape leaves plain strings untouched", () => {
  expect(csvEscape("simple")).toBe("simple");
});

test("csvEscape quotes strings containing commas", () => {
  expect(csvEscape("a,b")).toBe('"a,b"');
});

test("csvEscape quotes strings containing double quotes", () => {
  expect(csvEscape('he said "hi"')).toBe('"he said ""hi"""');
});

test("csvEscape quotes strings containing newlines", () => {
  expect(csvEscape("line1\nline2")).toBe('"line1\nline2"');
});

test("resolveOutputPath leaves absolute paths alone", () => {
  expect(resolveOutputPath("/var/tmp/x.csv")).toBe("/var/tmp/x.csv");
});

test("resolveOutputPath resolves relative paths against cwd", () => {
  const resolved = resolveOutputPath("tmp/x.csv");
  expect(resolved.endsWith("tmp/x.csv")).toBe(true);
  expect(resolved.startsWith("/")).toBe(true);
});
