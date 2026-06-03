// Run with: bunx vitest run src/scripts/finish-position-features/verify-finish-position-v7lineage-coverage.test.ts
import { expect, test, vi } from "vitest";

import type {
  ActiveModelRow,
  ActualYearRaceCountRow,
  BucketYearRaceCountRow,
  CategoryExpectation,
  CliDeps,
  CronExecutionRow,
  FoldCoverageRow,
  GlobalEvalRow,
  QueryResultLike,
  QueryRunner,
  Top1PlausibilityRow,
  VerifyDeps,
  VerifyReport,
} from "./verify-finish-position-v7lineage-coverage";
import {
  assessFoldCoverage,
  assessGlobalEvalPresence,
  assessRaceCountCrosscheck,
  assessTop1Plausibility,
  buildActiveModelsSql,
  buildActualRaceCountSql,
  buildBucketYearRaceCountSql,
  buildCronLastRunSql,
  buildCronTableExistsSql,
  buildFoldCoverageSql,
  buildGlobalEvalSql,
  buildReport,
  buildTop1PlausibilitySql,
  buildUsageText,
  collectIssues,
  CATEGORY_EXPECTATIONS,
  defaultLog,
  defaultWriteOut,
  deriveOverall,
  emitReport,
  escapeSqlLiteral,
  exitCodeFor,
  initialOptions,
  parseArgs,
  runCli,
  runVerification,
  summarizeActiveModels,
  summarizeCron,
} from "./verify-finish-position-v7lineage-coverage";

const hoisted = vi.hoisted(() => ({
  writeFileMock: vi.fn<typeof import("node:fs/promises").writeFile>(),
}));

vi.mock("node:fs/promises", () => ({
  default: { writeFile: hoisted.writeFileMock },
  writeFile: hoisted.writeFileMock,
}));

const jraExpectation: CategoryExpectation = {
  category: "jra",
  source: "jra",
  storedSource: "jra",
  modelVersion: "jra-cb-v7-lineage-wf-21y",
  expectedYears: 20,
  top1Low: 0.45,
  top1High: 0.55,
};

const naroExpectation: CategoryExpectation = {
  category: "nar",
  source: "nar",
  storedSource: "nar",
  modelVersion: "nar-xgb-v7-lineage-wf-21y",
  expectedYears: 20,
  top1Low: 0.5,
  top1High: 0.6,
};

const baneiExpectation: CategoryExpectation = {
  category: "banei",
  source: "ban-ei",
  storedSource: "nar",
  modelVersion: "banei-cb-v7-lineage-wf-21y",
  expectedYears: 19,
  top1Low: 0.3,
  top1High: 0.4,
};

test("escapeSqlLiteral doubles single quotes", () => {
  expect(escapeSqlLiteral("ab'cd")).toBe("ab''cd");
});

test("CATEGORY_EXPECTATIONS lists jra nar banei with their WF model versions", () => {
  expect(CATEGORY_EXPECTATIONS).toStrictEqual([
    {
      category: "jra",
      source: "jra",
      storedSource: "jra",
      modelVersion: "jra-cb-v7-lineage-wf-21y",
      expectedYears: 20,
      top1Low: 0.45,
      top1High: 0.55,
    },
    {
      category: "nar",
      source: "nar",
      storedSource: "nar",
      modelVersion: "nar-xgb-v7-lineage-wf-21y",
      expectedYears: 20,
      top1Low: 0.5,
      top1High: 0.6,
    },
    {
      category: "banei",
      source: "ban-ei",
      storedSource: "nar",
      modelVersion: "banei-cb-v7-lineage-wf-21y",
      expectedYears: 19,
      top1Low: 0.3,
      top1High: 0.4,
    },
  ]);
});

test("buildFoldCoverageSql groups by model_version and source for the given version", () => {
  expect(buildFoldCoverageSql("jra-cb-v7-lineage-wf-21y")).toBe(`
    select
      model_version,
      source,
      count(distinct evaluation_window_from) as years,
      sum(race_count) as race_count_sum
    from model_prediction_bucket_evaluations
    where model_version = 'jra-cb-v7-lineage-wf-21y'
    group by model_version, source
  `);
});

test("buildTop1PlausibilitySql sums top1_hit_sum and race_count", () => {
  expect(buildTop1PlausibilitySql("nar-xgb-v7-lineage-wf-21y")).toBe(`
    select
      model_version,
      sum(top1_hit_sum) as top1_hit_sum,
      sum(race_count) as race_count
    from model_prediction_bucket_evaluations
    where model_version = 'nar-xgb-v7-lineage-wf-21y'
    group by model_version
  `);
});

test("buildBucketYearRaceCountSql buckets race_count by source and year prefix", () => {
  expect(buildBucketYearRaceCountSql("banei-cb-v7-lineage-wf-21y")).toBe(`
    select
      source,
      left(evaluation_window_from, 4) as year,
      sum(race_count) as bucket_race_count
    from model_prediction_bucket_evaluations
    where model_version = 'banei-cb-v7-lineage-wf-21y'
    group by source, left(evaluation_window_from, 4)
  `);
});

test("buildActualRaceCountSql for jra counts distinct races in jvd_ra", () => {
  expect(buildActualRaceCountSql("jra")).toBe(`
    select
      kaisai_nen as year,
      count(distinct (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)) as actual_race_count
    from jvd_ra
    group by kaisai_nen
  `);
});

test("buildActualRaceCountSql for nar filters out the ban-ei keibajo in nvd_ra", () => {
  expect(buildActualRaceCountSql("nar")).toBe(`
    select
      kaisai_nen as year,
      count(distinct (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)) as actual_race_count
    from nvd_ra
    where keibajo_code <> '83'
    group by kaisai_nen
  `);
});

test("buildActualRaceCountSql for ban-ei keeps only keibajo 83 in nvd_ra", () => {
  expect(buildActualRaceCountSql("ban-ei")).toBe(`
    select
      kaisai_nen as year,
      count(distinct (kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango)) as actual_race_count
    from nvd_ra
    where keibajo_code = '83'
    group by kaisai_nen
  `);
});

test("buildGlobalEvalSql selects WF 21y rollup rows by suffix", () => {
  expect(buildGlobalEvalSql()).toBe(`
    select model_version, top1_accuracy
    from model_prediction_evaluations
    where model_version like '%-v7-lineage-wf-21y'
  `);
});

test("buildActiveModelsSql selects category and model_version", () => {
  expect(buildActiveModelsSql()).toBe(
    "select category, model_version from finish_position_active_models",
  );
});

test("buildCronTableExistsSql probes the cron table via to_regclass", () => {
  expect(buildCronTableExistsSql()).toBe(
    "select to_regclass('public.finish_position_cron_executions') is not null as exists",
  );
});

test("buildCronLastRunSql selects max run_date and status", () => {
  expect(buildCronLastRunSql()).toBe(
    "select max(run_date) as max_run_date, max(status) as status from finish_position_cron_executions",
  );
});

test("assessFoldCoverage marks ok when observed years equal expected", () => {
  const rows: FoldCoverageRow[] = [
    { model_version: "jra-cb-v7-lineage-wf-21y", source: "jra", years: 20, race_count_sum: 500000 },
  ];
  expect(assessFoldCoverage(jraExpectation, rows)).toStrictEqual({
    category: "jra",
    source: "jra",
    storedSource: "jra",
    modelVersion: "jra-cb-v7-lineage-wf-21y",
    expectedYears: 20,
    observedYears: 20,
    raceCountSum: 500000,
    ok: true,
  });
});

test("assessFoldCoverage matches ban-ei stored rows by storedSource nar", () => {
  const rows: FoldCoverageRow[] = [
    {
      model_version: "banei-cb-v7-lineage-wf-21y",
      source: "nar",
      years: 19,
      race_count_sum: 31771,
    },
  ];
  expect(assessFoldCoverage(baneiExpectation, rows)).toStrictEqual({
    category: "banei",
    source: "ban-ei",
    storedSource: "nar",
    modelVersion: "banei-cb-v7-lineage-wf-21y",
    expectedYears: 19,
    observedYears: 19,
    raceCountSum: 31771,
    ok: true,
  });
});

test("assessFoldCoverage parses string years and race_count_sum from numeric columns", () => {
  const rows: FoldCoverageRow[] = [
    {
      model_version: "nar-xgb-v7-lineage-wf-21y",
      source: "nar",
      years: "20",
      race_count_sum: "812345",
    },
  ];
  expect(assessTop1Plausibility(naroExpectation, []).withinBand).toBe(false);
  expect(assessFoldCoverage(naroExpectation, rows)).toStrictEqual({
    category: "nar",
    source: "nar",
    storedSource: "nar",
    modelVersion: "nar-xgb-v7-lineage-wf-21y",
    expectedYears: 20,
    observedYears: 20,
    raceCountSum: 812345,
    ok: true,
  });
});

test("assessFoldCoverage flags missing coverage when stored source row is absent", () => {
  const rows: FoldCoverageRow[] = [
    { model_version: "banei-cb-v7-lineage-wf-21y", source: "jra", years: 19, race_count_sum: 1 },
  ];
  expect(assessFoldCoverage(baneiExpectation, rows)).toStrictEqual({
    category: "banei",
    source: "ban-ei",
    storedSource: "nar",
    modelVersion: "banei-cb-v7-lineage-wf-21y",
    expectedYears: 19,
    observedYears: 0,
    raceCountSum: 0,
    ok: false,
  });
});

test("assessFoldCoverage flags partial coverage when observed years are short", () => {
  const rows: FoldCoverageRow[] = [
    { model_version: "jra-cb-v7-lineage-wf-21y", source: "jra", years: 18, race_count_sum: 400000 },
  ];
  expect(assessFoldCoverage(jraExpectation, rows)).toStrictEqual({
    category: "jra",
    source: "jra",
    storedSource: "jra",
    modelVersion: "jra-cb-v7-lineage-wf-21y",
    expectedYears: 20,
    observedYears: 18,
    raceCountSum: 400000,
    ok: false,
  });
});

test("assessFoldCoverage treats a non-numeric race_count_sum as zero", () => {
  const rows: FoldCoverageRow[] = [
    {
      model_version: "jra-cb-v7-lineage-wf-21y",
      source: "jra",
      years: 20,
      race_count_sum: "not-a-number",
    },
  ];
  expect(assessFoldCoverage(jraExpectation, rows)).toStrictEqual({
    category: "jra",
    source: "jra",
    storedSource: "jra",
    modelVersion: "jra-cb-v7-lineage-wf-21y",
    expectedYears: 20,
    observedYears: 20,
    raceCountSum: 0,
    ok: true,
  });
});

test("assessFoldCoverage treats a null race_count_sum as zero", () => {
  const rows: FoldCoverageRow[] = [
    {
      model_version: "jra-cb-v7-lineage-wf-21y",
      source: "jra",
      years: 20,
      race_count_sum: null,
    },
  ];
  expect(assessFoldCoverage(jraExpectation, rows)).toStrictEqual({
    category: "jra",
    source: "jra",
    storedSource: "jra",
    modelVersion: "jra-cb-v7-lineage-wf-21y",
    expectedYears: 20,
    observedYears: 20,
    raceCountSum: 0,
    ok: true,
  });
});

test("assessTop1Plausibility computes the per-race ratio inside the JRA band", () => {
  const rows: Top1PlausibilityRow[] = [
    { model_version: "jra-cb-v7-lineage-wf-21y", top1_hit_sum: 50, race_count: 100 },
  ];
  expect(assessTop1Plausibility(jraExpectation, rows)).toStrictEqual({
    category: "jra",
    modelVersion: "jra-cb-v7-lineage-wf-21y",
    top1Rate: 0.5,
    low: 0.45,
    high: 0.55,
    withinBand: true,
  });
});

test("assessTop1Plausibility divides ban-ei hits by race_count not prediction_count", () => {
  const rows: Top1PlausibilityRow[] = [
    { model_version: "banei-cb-v7-lineage-wf-21y", top1_hit_sum: "10690", race_count: "31771" },
  ];
  expect(assessTop1Plausibility(baneiExpectation, rows)).toStrictEqual({
    category: "banei",
    modelVersion: "banei-cb-v7-lineage-wf-21y",
    top1Rate: 10690 / 31771,
    low: 0.3,
    high: 0.4,
    withinBand: true,
  });
});

test("assessTop1Plausibility flags a ratio below the band", () => {
  const rows: Top1PlausibilityRow[] = [
    { model_version: "nar-xgb-v7-lineage-wf-21y", top1_hit_sum: "10", race_count: "100" },
  ];
  expect(assessTop1Plausibility(naroExpectation, rows)).toStrictEqual({
    category: "nar",
    modelVersion: "nar-xgb-v7-lineage-wf-21y",
    top1Rate: 0.1,
    low: 0.5,
    high: 0.6,
    withinBand: false,
  });
});

test("assessTop1Plausibility flags a ratio above the band", () => {
  const rows: Top1PlausibilityRow[] = [
    { model_version: "banei-cb-v7-lineage-wf-21y", top1_hit_sum: 90, race_count: 100 },
  ];
  expect(assessTop1Plausibility(baneiExpectation, rows)).toStrictEqual({
    category: "banei",
    modelVersion: "banei-cb-v7-lineage-wf-21y",
    top1Rate: 0.9,
    low: 0.3,
    high: 0.4,
    withinBand: false,
  });
});

test("assessTop1Plausibility returns null rate when race_count is zero", () => {
  const rows: Top1PlausibilityRow[] = [
    { model_version: "jra-cb-v7-lineage-wf-21y", top1_hit_sum: 0, race_count: 0 },
  ];
  expect(assessTop1Plausibility(jraExpectation, rows)).toStrictEqual({
    category: "jra",
    modelVersion: "jra-cb-v7-lineage-wf-21y",
    top1Rate: null,
    low: 0.45,
    high: 0.55,
    withinBand: false,
  });
});

test("assessRaceCountCrosscheck passes when each year is within tolerance", () => {
  const bucketRows: BucketYearRaceCountRow[] = [
    { source: "jra", year: "2024", bucket_race_count: 3000 },
    { source: "jra", year: "2025", bucket_race_count: "3010" },
  ];
  const actualRows: ActualYearRaceCountRow[] = [
    { year: "2024", actual_race_count: 3000 },
    { year: "2025", actual_race_count: "3015" },
  ];
  expect(
    assessRaceCountCrosscheck({ source: "jra", storedSource: "jra", bucketRows, actualRows }),
  ).toStrictEqual({
    source: "jra",
    entries: [
      {
        source: "jra",
        year: "2024",
        bucketRaceCount: 3000,
        actualRaceCount: 3000,
        relativeDelta: 0,
        withinTolerance: true,
      },
      {
        source: "jra",
        year: "2025",
        bucketRaceCount: 3010,
        actualRaceCount: 3015,
        relativeDelta: 5 / 3015,
        withinTolerance: true,
      },
    ],
    worstRelativeDelta: 5 / 3015,
    allWithinTolerance: true,
  });
});

test("assessRaceCountCrosscheck flags a year outside tolerance and ignores other stored sources", () => {
  const bucketRows: BucketYearRaceCountRow[] = [
    { source: "nar", year: "2024", bucket_race_count: 2000 },
    { source: "jra", year: "2024", bucket_race_count: 999 },
  ];
  const actualRows: ActualYearRaceCountRow[] = [{ year: "2024", actual_race_count: 1000 }];
  expect(
    assessRaceCountCrosscheck({ source: "nar", storedSource: "nar", bucketRows, actualRows }),
  ).toStrictEqual({
    source: "nar",
    entries: [
      {
        source: "nar",
        year: "2024",
        bucketRaceCount: 2000,
        actualRaceCount: 1000,
        relativeDelta: 1,
        withinTolerance: false,
      },
    ],
    worstRelativeDelta: 1,
    allWithinTolerance: false,
  });
});

test("assessRaceCountCrosscheck filters ban-ei stored nar rows against ban-ei actuals", () => {
  const bucketRows: BucketYearRaceCountRow[] = [
    { source: "nar", year: "2024", bucket_race_count: 1700 },
    { source: "nar", year: "2025", bucket_race_count: 1700 },
  ];
  const actualRows: ActualYearRaceCountRow[] = [
    { year: "2024", actual_race_count: 1700 },
    { year: "2025", actual_race_count: 1700 },
  ];
  expect(
    assessRaceCountCrosscheck({ source: "ban-ei", storedSource: "nar", bucketRows, actualRows }),
  ).toStrictEqual({
    source: "ban-ei",
    entries: [
      {
        source: "ban-ei",
        year: "2024",
        bucketRaceCount: 1700,
        actualRaceCount: 1700,
        relativeDelta: 0,
        withinTolerance: true,
      },
      {
        source: "ban-ei",
        year: "2025",
        bucketRaceCount: 1700,
        actualRaceCount: 1700,
        relativeDelta: 0,
        withinTolerance: true,
      },
    ],
    worstRelativeDelta: 0,
    allWithinTolerance: true,
  });
});

test("assessRaceCountCrosscheck keeps the larger delta when a later year deviates less", () => {
  const bucketRows: BucketYearRaceCountRow[] = [
    { source: "jra", year: "2023", bucket_race_count: 1300 },
    { source: "jra", year: "2024", bucket_race_count: 1010 },
  ];
  const actualRows: ActualYearRaceCountRow[] = [
    { year: "2023", actual_race_count: 1000 },
    { year: "2024", actual_race_count: 1000 },
  ];
  expect(
    assessRaceCountCrosscheck({ source: "jra", storedSource: "jra", bucketRows, actualRows }),
  ).toStrictEqual({
    source: "jra",
    entries: [
      {
        source: "jra",
        year: "2023",
        bucketRaceCount: 1300,
        actualRaceCount: 1000,
        relativeDelta: 0.3,
        withinTolerance: false,
      },
      {
        source: "jra",
        year: "2024",
        bucketRaceCount: 1010,
        actualRaceCount: 1000,
        relativeDelta: 0.01,
        withinTolerance: true,
      },
    ],
    worstRelativeDelta: 0.3,
    allWithinTolerance: false,
  });
});

test("assessRaceCountCrosscheck records null delta when actuals are missing", () => {
  const bucketRows: BucketYearRaceCountRow[] = [
    { source: "nar", year: "2024", bucket_race_count: 500 },
  ];
  const actualRows: ActualYearRaceCountRow[] = [];
  expect(
    assessRaceCountCrosscheck({ source: "ban-ei", storedSource: "nar", bucketRows, actualRows }),
  ).toStrictEqual({
    source: "ban-ei",
    entries: [
      {
        source: "ban-ei",
        year: "2024",
        bucketRaceCount: 500,
        actualRaceCount: 0,
        relativeDelta: null,
        withinTolerance: false,
      },
    ],
    worstRelativeDelta: null,
    allWithinTolerance: false,
  });
});

test("assessRaceCountCrosscheck on empty bucket rows is vacuously within tolerance", () => {
  const bucketRows: BucketYearRaceCountRow[] = [];
  const actualRows: ActualYearRaceCountRow[] = [{ year: "2024", actual_race_count: 1000 }];
  expect(
    assessRaceCountCrosscheck({ source: "jra", storedSource: "jra", bucketRows, actualRows }),
  ).toStrictEqual({
    source: "jra",
    entries: [],
    worstRelativeDelta: null,
    allWithinTolerance: true,
  });
});

test("assessGlobalEvalPresence is true when exactly three rows exist", () => {
  const rows: GlobalEvalRow[] = [
    { model_version: "jra-cb-v7-lineage-wf-21y", top1_accuracy: 0.5 },
    { model_version: "nar-xgb-v7-lineage-wf-21y", top1_accuracy: 0.55 },
    { model_version: "banei-cb-v7-lineage-wf-21y", top1_accuracy: 0.35 },
  ];
  expect(assessGlobalEvalPresence(rows)).toBe(true);
});

test("assessGlobalEvalPresence is false when a category is missing", () => {
  const rows: GlobalEvalRow[] = [
    { model_version: "jra-cb-v7-lineage-wf-21y", top1_accuracy: 0.5 },
    { model_version: "nar-xgb-v7-lineage-wf-21y", top1_accuracy: 0.55 },
  ];
  expect(assessGlobalEvalPresence(rows)).toBe(false);
});

test("summarizeActiveModels reports all three flipped to WF 21y", () => {
  const rows: ActiveModelRow[] = [
    { category: "jra", model_version: "jra-cb-v7-lineage-wf-21y" },
    { category: "nar", model_version: "nar-xgb-v7-lineage-wf-21y" },
    { category: "banei", model_version: "banei-cb-v7-lineage-wf-21y" },
  ];
  expect(summarizeActiveModels(rows)).toStrictEqual({
    byCategory: {
      jra: "jra-cb-v7-lineage-wf-21y",
      nar: "nar-xgb-v7-lineage-wf-21y",
      banei: "banei-cb-v7-lineage-wf-21y",
    },
    flippedToWf21y: { jra: true, nar: true, banei: true },
    allFlipped: true,
  });
});

test("summarizeActiveModels reports not-yet-flipped when versions are legacy or missing", () => {
  const rows: ActiveModelRow[] = [
    { category: "jra", model_version: "jra-cb-v7-lineage" },
    { category: "nar", model_version: "nar-xgb-v7-lineage-wf-21y" },
  ];
  expect(summarizeActiveModels(rows)).toStrictEqual({
    byCategory: {
      jra: "jra-cb-v7-lineage",
      nar: "nar-xgb-v7-lineage-wf-21y",
      banei: null,
    },
    flippedToWf21y: { jra: false, nar: true, banei: false },
    allFlipped: false,
  });
});

test("summarizeCron reports the last run when the table exists", () => {
  const rows: CronExecutionRow[] = [{ max_run_date: "2026-06-02", status: "success" }];
  expect(summarizeCron(true, rows)).toStrictEqual({
    tableExists: true,
    lastRunDate: "2026-06-02",
    status: "success",
  });
});

test("summarizeCron reports nulls when the table is missing", () => {
  expect(summarizeCron(false, [])).toStrictEqual({
    tableExists: false,
    lastRunDate: null,
    status: null,
  });
});

test("summarizeCron reports nulls when an existing table has no rows", () => {
  expect(summarizeCron(true, [])).toStrictEqual({
    tableExists: true,
    lastRunDate: null,
    status: null,
  });
});

test("collectIssues returns empty when everything passes", () => {
  expect(
    collectIssues({
      coverage: [
        {
          category: "jra",
          source: "jra",
          storedSource: "jra",
          modelVersion: "jra-cb-v7-lineage-wf-21y",
          expectedYears: 20,
          observedYears: 20,
          raceCountSum: 1,
          ok: true,
        },
      ],
      top1: [
        {
          category: "jra",
          modelVersion: "jra-cb-v7-lineage-wf-21y",
          top1Rate: 0.5,
          low: 0.45,
          high: 0.55,
          withinBand: true,
        },
      ],
      crosschecks: [
        { source: "jra", entries: [], worstRelativeDelta: null, allWithinTolerance: true },
      ],
      globalEvalPresent: true,
    }),
  ).toStrictEqual([]);
});

test("collectIssues reports a FAIL line for short fold coverage", () => {
  expect(
    collectIssues({
      coverage: [
        {
          category: "nar",
          source: "nar",
          storedSource: "nar",
          modelVersion: "nar-xgb-v7-lineage-wf-21y",
          expectedYears: 20,
          observedYears: 18,
          raceCountSum: 1,
          ok: false,
        },
      ],
      top1: [],
      crosschecks: [],
      globalEvalPresent: true,
    }),
  ).toStrictEqual(["FAIL fold-coverage nar: expected 20 year-windows, observed 18"]);
});

test("collectIssues reports a FAIL line when the global rollup is absent", () => {
  expect(
    collectIssues({
      coverage: [],
      top1: [],
      crosschecks: [],
      globalEvalPresent: false,
    }),
  ).toStrictEqual(["FAIL global-eval: expected 3 rows in model_prediction_evaluations"]);
});

test("collectIssues reports a WARN line for a top1 ratio outside the band", () => {
  expect(
    collectIssues({
      coverage: [],
      top1: [
        {
          category: "banei",
          modelVersion: "banei-cb-v7-lineage-wf-21y",
          top1Rate: 0.1,
          low: 0.3,
          high: 0.4,
          withinBand: false,
        },
      ],
      crosschecks: [],
      globalEvalPresent: true,
    }),
  ).toStrictEqual(["WARN top1-plausibility banei: 10.00% outside [30.00%, 40.00%]"]);
});

test("collectIssues reports a WARN line for a top1 ratio that is null", () => {
  expect(
    collectIssues({
      coverage: [],
      top1: [
        {
          category: "jra",
          modelVersion: "jra-cb-v7-lineage-wf-21y",
          top1Rate: null,
          low: 0.45,
          high: 0.55,
          withinBand: false,
        },
      ],
      crosschecks: [],
      globalEvalPresent: true,
    }),
  ).toStrictEqual(["WARN top1-plausibility jra: n/a outside [45.00%, 55.00%]"]);
});

test("collectIssues reports a WARN line for a race-count crosscheck deviation", () => {
  expect(
    collectIssues({
      coverage: [],
      top1: [],
      crosschecks: [
        {
          source: "jra",
          entries: [
            {
              source: "jra",
              year: "2024",
              bucketRaceCount: 2000,
              actualRaceCount: 1000,
              relativeDelta: 1,
              withinTolerance: false,
            },
          ],
          worstRelativeDelta: 1,
          allWithinTolerance: false,
        },
      ],
      globalEvalPresent: true,
    }),
  ).toStrictEqual(["WARN race-count-crosscheck jra: worst relative delta 100.00% exceeds 2.00%"]);
});

test("deriveOverall returns fail when any FAIL issue is present", () => {
  expect(deriveOverall(["WARN x", "FAIL y"])).toBe("fail");
});

test("deriveOverall returns warn when only WARN issues are present", () => {
  expect(deriveOverall(["WARN x"])).toBe("warn");
});

test("deriveOverall returns pass when there are no issues", () => {
  expect(deriveOverall([])).toBe("pass");
});

test("buildReport composes a passing report with no issues", () => {
  const report = buildReport({
    coverage: [
      {
        category: "jra",
        source: "jra",
        storedSource: "jra",
        modelVersion: "jra-cb-v7-lineage-wf-21y",
        expectedYears: 20,
        observedYears: 20,
        raceCountSum: 100,
        ok: true,
      },
    ],
    top1: [
      {
        category: "jra",
        modelVersion: "jra-cb-v7-lineage-wf-21y",
        top1Rate: 0.5,
        low: 0.45,
        high: 0.55,
        withinBand: true,
      },
    ],
    crosschecks: [
      { source: "jra", entries: [], worstRelativeDelta: null, allWithinTolerance: true },
    ],
    globalEvalPresent: true,
    activeModels: {
      byCategory: { jra: "jra-cb-v7-lineage-wf-21y", nar: null, banei: null },
      flippedToWf21y: { jra: true, nar: false, banei: false },
      allFlipped: false,
    },
    cron: { tableExists: false, lastRunDate: null, status: null },
  });
  expect(report.overall).toBe("pass");
  expect(report.issues).toStrictEqual([]);
});

test("buildReport composes a failing report when fold coverage is short", () => {
  const report = buildReport({
    coverage: [
      {
        category: "nar",
        source: "nar",
        storedSource: "nar",
        modelVersion: "nar-xgb-v7-lineage-wf-21y",
        expectedYears: 20,
        observedYears: 5,
        raceCountSum: 10,
        ok: false,
      },
    ],
    top1: [],
    crosschecks: [],
    globalEvalPresent: true,
    activeModels: {
      byCategory: { jra: null, nar: null, banei: null },
      flippedToWf21y: { jra: false, nar: false, banei: false },
      allFlipped: false,
    },
    cron: { tableExists: false, lastRunDate: null, status: null },
  });
  expect(report.overall).toBe("fail");
  expect(report.global_eval_present).toBe(true);
});

test("buildUsageText documents the pg-url and out flags", () => {
  expect(buildUsageText()).toBe(`Usage:
  bun run src/scripts/finish-position-features/verify-finish-position-v7lineage-coverage.ts \\
    [--pg-url <connection-string>] [--out tmp/stage-8-verify-<ts>.json]`);
});

test("initialOptions defaults out to null and uses DATABASE_URL_LOCAL when set", () => {
  vi.stubEnv("DATABASE_URL_LOCAL", "postgresql://user:pass@host/db");
  expect(initialOptions()).toStrictEqual({ pgUrl: "postgresql://user:pass@host/db", out: null });
  vi.unstubAllEnvs();
});

test("initialOptions falls back to the local default when DATABASE_URL_LOCAL is unset", () => {
  vi.stubEnv("DATABASE_URL_LOCAL", undefined);
  expect(initialOptions()).toStrictEqual({
    pgUrl: "postgresql://horse_racing:horse_racing@127.0.0.1:5432/horse_racing",
    out: null,
  });
  vi.unstubAllEnvs();
});

test("parseArgs reads pg-url and out flags", () => {
  expect(parseArgs(["--pg-url", "postgresql://x", "--out", "tmp/report.json"])).toStrictEqual({
    pgUrl: "postgresql://x",
    out: "tmp/report.json",
  });
});

test("parseArgs throws when pg-url has no value", () => {
  expect(() => parseArgs(["--pg-url"])).toThrowError("--pg-url requires a value.");
});

test("parseArgs throws on an unknown argument", () => {
  expect(() => parseArgs(["--nope"])).toThrowError("Unknown argument: --nope");
});

test("exitCodeFor maps fail to 1 and pass to 0 and warn to 0", () => {
  expect(exitCodeFor("fail")).toBe(1);
  expect(exitCodeFor("pass")).toBe(0);
  expect(exitCodeFor("warn")).toBe(0);
});

const respond = <Row>(rows: Row[]): Promise<QueryResultLike<Row>> => Promise.resolve({ rows });

const buildScriptedRunner = (): QueryRunner => {
  const query = vi.fn<QueryRunner["query"]>((sql: string) => {
    if (sql.includes("count(distinct evaluation_window_from)")) {
      if (sql.includes("jra-cb-v7-lineage-wf-21y")) {
        return respond([
          {
            model_version: "jra-cb-v7-lineage-wf-21y",
            source: "jra",
            years: 20,
            race_count_sum: 100,
          },
        ]);
      }
      if (sql.includes("nar-xgb-v7-lineage-wf-21y")) {
        return respond([
          {
            model_version: "nar-xgb-v7-lineage-wf-21y",
            source: "nar",
            years: 20,
            race_count_sum: 100,
          },
        ]);
      }
      return respond([
        {
          model_version: "banei-cb-v7-lineage-wf-21y",
          source: "nar",
          years: 19,
          race_count_sum: 100,
        },
      ]);
    }
    if (sql.includes("sum(top1_hit_sum) as top1_hit_sum")) {
      if (sql.includes("jra-cb-v7-lineage-wf-21y")) {
        return respond([
          { model_version: "jra-cb-v7-lineage-wf-21y", top1_hit_sum: 50, race_count: 100 },
        ]);
      }
      if (sql.includes("nar-xgb-v7-lineage-wf-21y")) {
        return respond([
          { model_version: "nar-xgb-v7-lineage-wf-21y", top1_hit_sum: 55, race_count: 100 },
        ]);
      }
      return respond([
        { model_version: "banei-cb-v7-lineage-wf-21y", top1_hit_sum: 35, race_count: 100 },
      ]);
    }
    if (sql.includes("as bucket_race_count")) {
      return respond([{ source: "jra", year: "2024", bucket_race_count: 100 }]);
    }
    if (sql.includes("as actual_race_count")) {
      return respond([{ year: "2024", actual_race_count: 100 }]);
    }
    if (sql.includes("from model_prediction_evaluations")) {
      return respond([
        { model_version: "jra-cb-v7-lineage-wf-21y", top1_accuracy: 0.5 },
        { model_version: "nar-xgb-v7-lineage-wf-21y", top1_accuracy: 0.55 },
        { model_version: "banei-cb-v7-lineage-wf-21y", top1_accuracy: 0.35 },
      ]);
    }
    if (sql.includes("from finish_position_active_models")) {
      return respond([
        { category: "jra", model_version: "jra-cb-v7-lineage-wf-21y" },
        { category: "nar", model_version: "nar-xgb-v7-lineage-wf-21y" },
        { category: "banei", model_version: "banei-cb-v7-lineage-wf-21y" },
      ]);
    }
    if (sql.includes("is not null as exists")) {
      return respond([{ exists: true }]);
    }
    if (sql.includes("max(run_date)")) {
      return respond([{ max_run_date: "2026-06-02", status: "success" }]);
    }
    return respond([]);
  });
  return { query };
};

test("runVerification produces a passing report against a fully populated mock DB", async () => {
  const writeOut = vi.fn<VerifyDeps["writeOut"]>().mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();
  const deps: VerifyDeps = { runner: buildScriptedRunner(), log, writeOut };
  const report = await runVerification(deps);
  expect(report.overall).toBe("pass");
  expect(report.global_eval_present).toBe(true);
});

test("runVerification reports flipped active models and the last cron run", async () => {
  const writeOut = vi.fn<VerifyDeps["writeOut"]>().mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();
  const deps: VerifyDeps = { runner: buildScriptedRunner(), log, writeOut };
  const report = await runVerification(deps);
  expect(report.active_models.allFlipped).toBe(true);
  expect(report.cron_last_run).toStrictEqual({
    tableExists: true,
    lastRunDate: "2026-06-02",
    status: "success",
  });
});

test("runVerification reports a missing cron table when to_regclass is null", async () => {
  const query = vi.fn<QueryRunner["query"]>((sql: string) => {
    if (sql.includes("count(distinct evaluation_window_from)")) {
      return respond([{ model_version: "x", source: "none", years: 0, race_count_sum: 0 }]);
    }
    if (sql.includes("from model_prediction_evaluations")) {
      return respond([
        { model_version: "jra-cb-v7-lineage-wf-21y", top1_accuracy: 0.5 },
        { model_version: "nar-xgb-v7-lineage-wf-21y", top1_accuracy: 0.55 },
        { model_version: "banei-cb-v7-lineage-wf-21y", top1_accuracy: 0.35 },
      ]);
    }
    if (sql.includes("is not null as exists")) {
      return respond([{ exists: false }]);
    }
    return respond([]);
  });
  const deps: VerifyDeps = {
    runner: { query },
    log: vi.fn<(message: string) => void>(),
    writeOut: vi.fn<VerifyDeps["writeOut"]>().mockResolvedValue(undefined),
  };
  const report = await runVerification(deps);
  expect(report.cron_last_run).toStrictEqual({
    tableExists: false,
    lastRunDate: null,
    status: null,
  });
  expect(report.overall).toBe("fail");
});

const passReport: VerifyReport = {
  coverage: [],
  top1_plausibility: [],
  race_count_crosscheck: [],
  global_eval_present: true,
  active_models: {
    byCategory: { jra: null, nar: null, banei: null },
    flippedToWf21y: { jra: false, nar: false, banei: false },
    allFlipped: false,
  },
  cron_last_run: { tableExists: false, lastRunDate: null, status: null },
  overall: "pass",
  issues: [],
};

test("emitReport logs the JSON body and does not write a file when out is null", async () => {
  const writeOut = vi.fn<VerifyDeps["writeOut"]>().mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();
  const deps: VerifyDeps = { runner: { query: vi.fn<QueryRunner["query"]>() }, log, writeOut };
  await emitReport(deps, { pgUrl: "postgresql://x", out: null }, passReport);
  expect(log).toHaveBeenCalledWith(JSON.stringify(passReport, null, 2));
  expect(writeOut).toHaveBeenCalledTimes(0);
});

test("emitReport writes the JSON body when out is provided", async () => {
  const writeOut = vi.fn<VerifyDeps["writeOut"]>().mockResolvedValue(undefined);
  const log = vi.fn<(message: string) => void>();
  const deps: VerifyDeps = { runner: { query: vi.fn<QueryRunner["query"]>() }, log, writeOut };
  await emitReport(deps, { pgUrl: "postgresql://x", out: "tmp/out.json" }, passReport);
  expect(writeOut).toHaveBeenCalledWith("tmp/out.json", JSON.stringify(passReport, null, 2));
  expect(log).toHaveBeenCalledTimes(1);
});

test("defaultLog forwards the message to console.log", () => {
  const spy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  defaultLog("hello report");
  expect(spy).toHaveBeenCalledWith("hello report");
  spy.mockRestore();
});

test("defaultWriteOut appends a trailing newline and writes utf-8", async () => {
  hoisted.writeFileMock.mockClear();
  hoisted.writeFileMock.mockResolvedValue(undefined);
  await defaultWriteOut("tmp/x.json", "{}");
  expect(hoisted.writeFileMock).toHaveBeenCalledWith("tmp/x.json", "{}\n", "utf-8");
});

test("runCli opens a pool runs verification ends the pool and returns the exit code", async () => {
  const endMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const runner = buildScriptedRunner();
  const pool = { query: runner.query, end: endMock };
  const openPool = vi.fn<CliDeps["openPool"]>().mockReturnValue(pool);
  const log = vi.fn<(message: string) => void>();
  const writeOut = vi.fn<CliDeps["writeOut"]>().mockResolvedValue(undefined);
  const cliDeps: CliDeps = { openPool, log, writeOut };
  const code = await runCli(cliDeps, { pgUrl: "postgresql://x", out: null });
  expect(code).toBe(0);
  expect(openPool).toHaveBeenCalledWith("postgresql://x");
  expect(endMock).toHaveBeenCalledTimes(1);
});

test("runCli ends the pool and rethrows when verification rejects", async () => {
  const endMock = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const query = vi.fn<QueryRunner["query"]>().mockRejectedValue(new Error("db down"));
  const pool = { query, end: endMock };
  const openPool = vi.fn<CliDeps["openPool"]>().mockReturnValue(pool);
  const cliDeps: CliDeps = {
    openPool,
    log: vi.fn<(message: string) => void>(),
    writeOut: vi.fn<CliDeps["writeOut"]>().mockResolvedValue(undefined),
  };
  await expect(runCli(cliDeps, { pgUrl: "postgresql://x", out: null })).rejects.toThrowError(
    "db down",
  );
  expect(endMock).toHaveBeenCalledTimes(1);
});

test("parseArgs prints usage and exits when help is requested", () => {
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  const exitSpy = vi.spyOn(process, "exit").mockImplementation((() => {
    throw new Error("exit-0");
  }) as never as typeof process.exit);
  expect(() => parseArgs(["--help"])).toThrowError("exit-0");
  expect(exitSpy).toHaveBeenCalledWith(0);
  expect(logSpy).toHaveBeenCalledTimes(1);
  logSpy.mockRestore();
  exitSpy.mockRestore();
});
