// Run with: bunx vitest run src/scripts/finish-position-features/print-running-style-feature-sql.test.ts
import { afterEach, expect, test, vi } from "vitest";

import * as featureSqlModule from "../../../../sync-realtime-data/src/running-style-feature-sql";
import { parseArgs, printRunningStyleFeatureSql } from "./print-running-style-feature-sql";

const STUB_SQL = "select 1 as feature_sql_stub";

afterEach(() => {
  vi.restoreAllMocks();
});

test("parseArgs returns the canonical options object when every flag is provided", () => {
  expect(
    parseArgs([
      "--source",
      "jra",
      "--from-date",
      "20060101",
      "--to-date",
      "20061231",
      "--feature-version",
      "v1",
    ]),
  ).toStrictEqual({
    featureVersion: "v1",
    fromDate: "20060101",
    source: "jra",
    toDate: "20061231",
  });
});

test("parseArgs accepts source=nar", () => {
  expect(
    parseArgs([
      "--source",
      "nar",
      "--from-date",
      "20210101",
      "--to-date",
      "20210601",
      "--feature-version",
      "v1",
    ]),
  ).toStrictEqual({
    featureVersion: "v1",
    fromDate: "20210101",
    source: "nar",
    toDate: "20210601",
  });
});

test("parseArgs throws when --source is missing", () => {
  expect(() =>
    parseArgs(["--from-date", "20060101", "--to-date", "20061231", "--feature-version", "v1"]),
  ).toThrowError("--source is required");
});

test("parseArgs throws when --from-date is missing", () => {
  expect(() =>
    parseArgs(["--source", "jra", "--to-date", "20061231", "--feature-version", "v1"]),
  ).toThrowError("--from-date is required");
});

test("parseArgs throws when --to-date is missing", () => {
  expect(() =>
    parseArgs(["--source", "jra", "--from-date", "20060101", "--feature-version", "v1"]),
  ).toThrowError("--to-date is required");
});

test("parseArgs throws when --feature-version is missing", () => {
  expect(() =>
    parseArgs(["--source", "jra", "--from-date", "20060101", "--to-date", "20061231"]),
  ).toThrowError("--feature-version is required");
});

test("parseArgs throws when --source is invalid", () => {
  expect(() =>
    parseArgs([
      "--source",
      "banei",
      "--from-date",
      "20060101",
      "--to-date",
      "20061231",
      "--feature-version",
      "v1",
    ]),
  ).toThrowError("--source must be one of jra | nar; got banei");
});

test("parseArgs throws when --from-date is not 8 digits", () => {
  expect(() =>
    parseArgs([
      "--source",
      "jra",
      "--from-date",
      "2006-01-01",
      "--to-date",
      "20061231",
      "--feature-version",
      "v1",
    ]),
  ).toThrowError("--from-date must be 8 digits (YYYYMMDD); got 2006-01-01");
});

test("parseArgs throws when --to-date is not 8 digits", () => {
  expect(() =>
    parseArgs([
      "--source",
      "jra",
      "--from-date",
      "20060101",
      "--to-date",
      "2006",
      "--feature-version",
      "v1",
    ]),
  ).toThrowError("--to-date must be 8 digits (YYYYMMDD); got 2006");
});

test("parseArgs throws on unknown argument", () => {
  expect(() =>
    parseArgs([
      "--source",
      "jra",
      "--from-date",
      "20060101",
      "--to-date",
      "20061231",
      "--feature-version",
      "v1",
      "--bogus",
      "x",
    ]),
  ).toThrowError("Unknown argument: --bogus");
});

test("parseArgs throws when a flag value is missing", () => {
  expect(() => parseArgs(["--source"])).toThrowError("--source requires a value");
});

test("printRunningStyleFeatureSql writes the production SQL to the supplied writer", () => {
  const writes: string[] = [];
  const buildSpy = vi
    .spyOn(featureSqlModule, "buildRunningStyleBatchFeatureSql")
    .mockReturnValue(STUB_SQL);
  printRunningStyleFeatureSql({
    deps: {
      write: (chunk) => {
        writes.push(chunk);
      },
    },
    options: {
      featureVersion: "v1",
      fromDate: "20060101",
      source: "jra",
      toDate: "20061231",
    },
  });
  expect(writes).toStrictEqual(["select 1 as feature_sql_stub"]);
  expect(buildSpy).toHaveBeenCalledTimes(1);
  expect(buildSpy).toHaveBeenCalledWith({
    featureSchemaVersion: "v1",
    fromDate: "20060101",
    source: "jra",
    toDate: "20061231",
  });
});

test("printRunningStyleFeatureSql forwards source=nar and the provided date window", () => {
  const writes: string[] = [];
  const buildSpy = vi
    .spyOn(featureSqlModule, "buildRunningStyleBatchFeatureSql")
    .mockReturnValue(STUB_SQL);
  printRunningStyleFeatureSql({
    deps: {
      write: (chunk) => {
        writes.push(chunk);
      },
    },
    options: {
      featureVersion: "v1",
      fromDate: "20210101",
      source: "nar",
      toDate: "20210601",
    },
  });
  expect(writes).toStrictEqual(["select 1 as feature_sql_stub"]);
  expect(buildSpy).toHaveBeenCalledTimes(1);
  expect(buildSpy).toHaveBeenCalledWith({
    featureSchemaVersion: "v1",
    fromDate: "20210101",
    source: "nar",
    toDate: "20210601",
  });
});
