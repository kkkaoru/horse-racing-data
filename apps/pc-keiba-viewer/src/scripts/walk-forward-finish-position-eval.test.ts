import { describe, expect, test } from "vitest";

import {
  buildAggregate,
  buildAggregateForKey,
  buildCompareArgs,
  buildReport,
  buildUsageText,
  computeStats,
  extractJson,
  formatRange,
  isCategory,
  isTarget,
  parseArgs,
  parseYearList,
  round2,
  runFolds,
  toFoldResult,
  validateCompareFinishJson,
} from "./walk-forward-finish-position-eval";
import type {
  CliArgs,
  CompareFinishJson,
  FoldResult,
} from "./walk-forward-finish-position-eval-types";

const baselineCompareJson = (): CompareFinishJson => ({
  fromDate: "20200101",
  pairScore: 70.96,
  place1Accuracy: 36.12,
  place2Accuracy: 19.33,
  place3Accuracy: 14.81,
  raceCount: 5808,
  toDate: "20201231",
  top1Accuracy: 36.12,
  top3BoxAccuracy: 11.07,
  top3ExactOrderAccuracy: 2.93,
  top3PlaceRelation: 55.27,
  top3WinnerCapture: 69.31,
  top5WinnerCapture: 84.91,
});

const baselineCliArgs = (): CliArgs => ({
  category: "jra",
  concurrency: 6,
  holdoutYears: [2022, 2023],
  outputPath: null,
  target: "local",
  tuningConfigPath: "config.json",
});

test("isCategory accepts known categories", () => {
  expect(isCategory("jra")).toBe(true);
  expect(isCategory("nar")).toBe(true);
  expect(isCategory("ban-ei")).toBe(true);
  expect(isCategory("all")).toBe(true);
});

test("isCategory rejects unknown category", () => {
  expect(isCategory("unknown")).toBe(false);
});

test("isTarget accepts local and neon", () => {
  expect(isTarget("local")).toBe(true);
  expect(isTarget("neon")).toBe(true);
});

test("isTarget rejects unknown target", () => {
  expect(isTarget("staging")).toBe(false);
});

test("parseYearList parses comma-separated years", () => {
  expect(parseYearList("2020,2021,2022")).toStrictEqual([2020, 2021, 2022]);
});

test("parseYearList sorts and deduplicates", () => {
  expect(parseYearList("2023,2021,2023,2020")).toStrictEqual([2020, 2021, 2023]);
});

test("parseYearList skips empty tokens", () => {
  expect(parseYearList(" 2020 , , 2021 ,")).toStrictEqual([2020, 2021]);
});

test("parseYearList throws on non-numeric token", () => {
  expect(() => parseYearList("2020,bad,2022")).toThrow("Invalid year");
});

test("parseYearList throws on out-of-range year", () => {
  expect(() => parseYearList("1800")).toThrow("Invalid year");
});

test("parseArgs requires --tuning-config", () => {
  expect(() => parseArgs(["--holdout-years", "2020"])).toThrow("--tuning-config is required.");
});

test("parseArgs requires --holdout-years", () => {
  expect(() => parseArgs(["--tuning-config", "config.json"])).toThrow(
    "--holdout-years must list at least one year.",
  );
});

test("parseArgs returns parsed CLI arguments", () => {
  const args = parseArgs([
    "--tuning-config",
    "config.json",
    "--category",
    "nar",
    "--target",
    "neon",
    "--holdout-years",
    "2022,2023",
    "--concurrency",
    "4",
    "--output",
    "report.json",
  ]);
  expect(args).toStrictEqual({
    category: "nar",
    concurrency: 4,
    holdoutYears: [2022, 2023],
    outputPath: "report.json",
    target: "neon",
    tuningConfigPath: "config.json",
  });
});

test("parseArgs rejects unknown argument", () => {
  expect(() =>
    parseArgs(["--tuning-config", "config.json", "--holdout-years", "2020", "--bogus", "x"]),
  ).toThrow("Unknown argument: --bogus");
});

test("parseArgs rejects invalid category", () => {
  expect(() =>
    parseArgs(["--tuning-config", "c.json", "--holdout-years", "2020", "--category", "xx"]),
  ).toThrow("--category must be all, jra, nar, or ban-ei.");
});

test("parseArgs rejects invalid target", () => {
  expect(() =>
    parseArgs(["--tuning-config", "c.json", "--holdout-years", "2020", "--target", "xx"]),
  ).toThrow("--target must be local or neon.");
});

test("formatRange yields full-year window", () => {
  expect(formatRange(2024)).toStrictEqual({ fromDate: "20240101", toDate: "20241231" });
});

test("buildCompareArgs builds bun spawn arguments", () => {
  expect(buildCompareArgs(baselineCliArgs(), 2024)).toStrictEqual([
    "run",
    "src/scripts/compare-finish-position-predictions.ts",
    "--target",
    "local",
    "--category",
    "jra",
    "--from-date",
    "20240101",
    "--to-date",
    "20241231",
    "--concurrency",
    "6",
    "--tuning-config",
    "config.json",
  ]);
});

test("buildUsageText documents the CLI", () => {
  expect(buildUsageText()).toContain("--tuning-config");
});

test("validateCompareFinishJson returns the typed payload", () => {
  expect(validateCompareFinishJson(baselineCompareJson())).toStrictEqual(baselineCompareJson());
});

test("validateCompareFinishJson throws when not an object", () => {
  expect(() => validateCompareFinishJson(null)).toThrow("compare-finish output is not an object.");
});

test("validateCompareFinishJson throws when missing number field", () => {
  const payload = { ...baselineCompareJson(), pairScore: "70" };
  expect(() => validateCompareFinishJson(payload)).toThrow(
    "compare-finish output missing number field",
  );
});

test("validateCompareFinishJson throws when missing string field", () => {
  const payload = { ...baselineCompareJson(), fromDate: 20200101 };
  expect(() => validateCompareFinishJson(payload)).toThrow(
    "compare-finish output missing string field",
  );
});

test("extractJson parses surrounding noise", () => {
  const stdout = `garbage prefix\n${JSON.stringify(baselineCompareJson())}\ntrailing noise`;
  expect(extractJson(stdout)).toStrictEqual(baselineCompareJson());
});

test("extractJson throws when no JSON object found", () => {
  expect(() => extractJson("no braces here")).toThrow("compare-finish output is not JSON");
});

test("round2 rounds to two decimals", () => {
  expect(round2(70.965)).toBe(70.97);
  expect(round2(0)).toBe(0);
});

test("computeStats yields zeroed stats for empty input", () => {
  expect(computeStats([])).toStrictEqual({ count: 0, max: 0, mean: 0, min: 0, stdev: 0 });
});

test("computeStats yields single-value stats with zero stdev", () => {
  expect(computeStats([10])).toStrictEqual({ count: 1, max: 10, mean: 10, min: 10, stdev: 0 });
});

test("computeStats yields aggregated stats for multiple values", () => {
  expect(computeStats([1, 2, 3, 4, 5])).toStrictEqual({
    count: 5,
    max: 5,
    mean: 3,
    min: 1,
    stdev: 1.58,
  });
});

test("toFoldResult attaches the holdout year", () => {
  expect(toFoldResult(2024, baselineCompareJson())).toStrictEqual({
    ...baselineCompareJson(),
    year: 2024,
  });
});

const fold1: FoldResult = { ...baselineCompareJson(), year: 2022 };
const fold2: FoldResult = {
  ...baselineCompareJson(),
  pairScore: 68.5,
  place1Accuracy: 34.0,
  top3ExactOrderAccuracy: 2.5,
  year: 2023,
};

test("buildAggregateForKey computes stats for a specific metric", () => {
  expect(buildAggregateForKey("pairScore", [fold1, fold2])).toStrictEqual({
    count: 2,
    max: 70.96,
    mean: 69.73,
    min: 68.5,
    stdev: 1.74,
  });
});

test("buildAggregate covers every metric", () => {
  const aggregate = buildAggregate([fold1, fold2]);
  expect(aggregate.pairScore.count).toBe(2);
  expect(aggregate.place1Accuracy.count).toBe(2);
  expect(aggregate.top3ExactOrderAccuracy.count).toBe(2);
  expect(aggregate.raceCount.count).toBe(2);
});

test("runFolds invokes runner sequentially per holdout year", async () => {
  const observed: number[] = [];
  const runner = async (year: number): Promise<string> => {
    observed.push(year);
    const payload = JSON.stringify({
      ...baselineCompareJson(),
      fromDate: `${year}0101`,
      toDate: `${year}1231`,
    });
    return payload;
  };
  const args: CliArgs = { ...baselineCliArgs(), holdoutYears: [2022, 2023, 2024] };
  const folds = await runFolds(args, runner);
  expect(observed).toStrictEqual([2022, 2023, 2024]);
  expect(folds.map((fold) => fold.year)).toStrictEqual([2022, 2023, 2024]);
});

const throwingRunner = async (): Promise<string> => {
  throw new Error("boom");
};

test("runFolds propagates runner errors", async () => {
  await expect(runFolds(baselineCliArgs(), throwingRunner)).rejects.toThrow("boom");
});

test("buildReport assembles a complete payload", () => {
  const report = buildReport(baselineCliArgs(), [fold1, fold2]);
  expect(report.category).toBe("jra");
  expect(report.target).toBe("local");
  expect(report.tuningConfigPath).toBe("config.json");
  expect(report.folds).toStrictEqual([fold1, fold2]);
  expect(report.aggregate.pairScore.count).toBe(2);
  expect(typeof report.generatedAt).toBe("string");
});

describe("toBe assertions on aggregate values", () => {
  test("mean across two folds is rounded", () => {
    expect(buildAggregateForKey("top3ExactOrderAccuracy", [fold1, fold2]).mean).toBe(2.72);
  });
});
