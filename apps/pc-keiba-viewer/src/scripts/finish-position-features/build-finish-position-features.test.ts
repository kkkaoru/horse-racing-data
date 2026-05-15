import { expect, test } from "vitest";

import {
  applyArg,
  buildUsageText,
  initialOptions,
  isCategory,
  isTarget,
  parseArgs,
} from "./build-finish-position-features";
import type { BuildOptions } from "./build-finish-position-features-types";

test("initialOptions returns sensible defaults", () => {
  expect(initialOptions()).toStrictEqual({
    category: "jra",
    dryRun: false,
    featureSchemaVersion: "v1",
    fromDate: "20160101",
    target: "local",
    toDate: "20261231",
  });
});

test("isCategory accepts known categories", () => {
  expect(isCategory("jra")).toBe(true);
  expect(isCategory("nar")).toBe(true);
  expect(isCategory("ban-ei")).toBe(true);
  expect(isCategory("all")).toBe(true);
});

test("isCategory rejects unknown categories", () => {
  expect(isCategory("xx")).toBe(false);
});

test("isTarget accepts local and neon", () => {
  expect(isTarget("local")).toBe(true);
  expect(isTarget("neon")).toBe(true);
});

test("isTarget rejects unknown values", () => {
  expect(isTarget("staging")).toBe(false);
});

test("buildUsageText documents the CLI", () => {
  expect(buildUsageText()).toContain("--feature-schema-version");
});

test("applyArg sets target", () => {
  const opts = initialOptions();
  expect(applyArg(opts, "--target", "neon").advanceBy).toBe(2);
  expect(opts.target).toBe("neon");
});

test("applyArg rejects invalid target", () => {
  const opts = initialOptions();
  expect(() => applyArg(opts, "--target", "bogus")).toThrow("--target must be local or neon.");
});

test("applyArg sets category", () => {
  const opts = initialOptions();
  applyArg(opts, "--category", "nar");
  expect(opts.category).toBe("nar");
});

test("applyArg rejects invalid category", () => {
  const opts = initialOptions();
  expect(() => applyArg(opts, "--category", "xxx")).toThrow(
    "--category must be all, jra, nar, or ban-ei.",
  );
});

test("applyArg strips hyphens from --from-date", () => {
  const opts = initialOptions();
  applyArg(opts, "--from-date", "2016-01-01");
  expect(opts.fromDate).toBe("20160101");
});

test("applyArg strips hyphens from --to-date", () => {
  const opts = initialOptions();
  applyArg(opts, "--to-date", "2026-12-31");
  expect(opts.toDate).toBe("20261231");
});

test("applyArg sets feature-schema-version", () => {
  const opts = initialOptions();
  applyArg(opts, "--feature-schema-version", "v2-beta");
  expect(opts.featureSchemaVersion).toBe("v2-beta");
});

test("applyArg --dry-run advances by 1", () => {
  const opts = initialOptions();
  expect(applyArg(opts, "--dry-run", undefined).advanceBy).toBe(1);
  expect(opts.dryRun).toBe(true);
});

test("applyArg rejects unknown flag", () => {
  const opts = initialOptions();
  expect(() => applyArg(opts, "--bogus", "x")).toThrow("Unknown argument: --bogus");
});

test("applyArg throws when required value missing", () => {
  const opts = initialOptions();
  expect(() => applyArg(opts, "--target", undefined)).toThrow("--target requires a value.");
});

test("parseArgs accepts a full flag list", () => {
  const parsed: BuildOptions = parseArgs([
    "--target",
    "neon",
    "--category",
    "ban-ei",
    "--from-date",
    "20180101",
    "--to-date",
    "20251231",
    "--feature-schema-version",
    "v2",
    "--dry-run",
  ]);
  expect(parsed).toStrictEqual({
    category: "ban-ei",
    dryRun: true,
    featureSchemaVersion: "v2",
    fromDate: "20180101",
    target: "neon",
    toDate: "20251231",
  });
});

test("parseArgs returns defaults when no flags given", () => {
  expect(parseArgs([])).toStrictEqual(initialOptions());
});
