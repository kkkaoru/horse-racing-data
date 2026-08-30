// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  ANALYSIS_CELL_PARAM_NAMES,
  buildCellMatchingStatsSettings,
  toConditionAnalysisFallbackCell,
  withDisabledCellDimensions,
} from "./past-race-cell-matching";

it("maps analysis cell URL param names", () => {
  expect(ANALYSIS_CELL_PARAM_NAMES).toStrictEqual({
    keibajo: "analysisCellKeibajo",
    distance: "analysisCellDistance",
    kyosoShubetsu: "analysisCellShubetsu",
    kyosoJoken: "analysisCellJoken",
    condition: "analysisCellCondition",
    track: "analysisCellTrack",
    grade: "analysisCellGrade",
    raceName: "analysisCellRaceName",
  });
});

it("builds cell-matching stats settings from NAR flags", () => {
  expect(
    buildCellMatchingStatsSettings({
      classConditionName: "A1",
      flags: {
        condition: true,
        distance: true,
        grade: false,
        keibajo: true,
        kyosoJoken: false,
        kyosoShubetsu: true,
        raceName: false,
        track: true,
      },
      sourceScope: "nar",
      years: 10,
    }),
  ).toStrictEqual({
    cellMatching: true,
    classConditionName: "A1",
    includeAge: true,
    includeBloodlineAncestors: true,
    includeClass: false,
    includeConditionKey: true,
    includeDistance: true,
    includeFrame: false,
    includeGrade: false,
    includeMonthWindow: false,
    includeNarOnly: true,
    includeRaceNumber: false,
    includeRaceSubtitle: false,
    includeRaceTitle: false,
    includeRunnerCount: false,
    includeSex: false,
    includeSurface: false,
    includeTrackCode: true,
    includeTurn: false,
    includeVenue: true,
    includeWeight: false,
    runnerCount: null,
    sourceScope: "nar",
    years: 10,
  });
});

it("builds cell-matching stats settings from JRA grade and race-name flags", () => {
  expect(
    buildCellMatchingStatsSettings({
      classConditionName: "G1",
      flags: {
        condition: false,
        distance: true,
        grade: true,
        keibajo: true,
        kyosoJoken: true,
        kyosoShubetsu: true,
        raceName: true,
        track: true,
      },
      sourceScope: "jra",
      years: 5,
    }),
  ).toStrictEqual({
    cellMatching: true,
    classConditionName: "G1",
    includeAge: true,
    includeBloodlineAncestors: true,
    includeClass: true,
    includeConditionKey: false,
    includeDistance: true,
    includeFrame: false,
    includeGrade: true,
    includeMonthWindow: false,
    includeNarOnly: false,
    includeRaceNumber: false,
    includeRaceSubtitle: false,
    includeRaceTitle: true,
    includeRunnerCount: false,
    includeSex: false,
    includeSurface: false,
    includeTrackCode: true,
    includeTurn: false,
    includeVenue: true,
    includeWeight: false,
    runnerCount: null,
    sourceScope: "jra",
    years: 5,
  });
});

it("keeps venue when disabling other cell dimensions", () => {
  const settings = buildCellMatchingStatsSettings({
    classConditionName: null,
    flags: {
      condition: true,
      distance: true,
      grade: true,
      keibajo: true,
      kyosoJoken: true,
      kyosoShubetsu: true,
      raceName: true,
      track: true,
    },
    sourceScope: "all",
    years: null,
  });
  expect(withDisabledCellDimensions(settings, true)).toStrictEqual({
    ...settings,
    includeAge: false,
    includeClass: false,
    includeConditionKey: false,
    includeDistance: false,
    includeGrade: false,
    includeRaceTitle: false,
    includeTrackCode: false,
    includeVenue: true,
  });
});

it("keeps distance on condition-analysis fallback while other cell dimensions drop", () => {
  const settings = buildCellMatchingStatsSettings({
    classConditionName: null,
    flags: {
      condition: true,
      distance: true,
      grade: true,
      keibajo: true,
      kyosoJoken: true,
      kyosoShubetsu: true,
      raceName: true,
      track: true,
    },
    sourceScope: "all",
    years: null,
  });
  expect(toConditionAnalysisFallbackCell(settings, true)).toStrictEqual({
    ...settings,
    includeAge: false,
    includeClass: false,
    includeConditionKey: false,
    includeDistance: true,
    includeGrade: false,
    includeRaceTitle: false,
    includeTrackCode: false,
    includeVenue: true,
  });
});

it("keeps an explicit off distance flag on condition-analysis fallback", () => {
  const settings = buildCellMatchingStatsSettings({
    classConditionName: null,
    flags: {
      condition: false,
      distance: false,
      grade: false,
      keibajo: true,
      kyosoJoken: false,
      kyosoShubetsu: false,
      raceName: false,
      track: false,
    },
    sourceScope: "nar",
    years: 10,
  });
  expect(toConditionAnalysisFallbackCell(settings, false)).toStrictEqual({
    ...settings,
    includeAge: false,
    includeClass: false,
    includeConditionKey: false,
    includeDistance: false,
    includeGrade: false,
    includeRaceTitle: false,
    includeTrackCode: false,
    includeVenue: false,
  });
});

it("drops venue when disabling every cell dimension", () => {
  const settings = buildCellMatchingStatsSettings({
    classConditionName: null,
    flags: {
      condition: false,
      distance: true,
      grade: false,
      keibajo: true,
      kyosoJoken: false,
      kyosoShubetsu: false,
      raceName: false,
      track: false,
    },
    sourceScope: "all",
    years: 10,
  });
  expect(withDisabledCellDimensions(settings, false).includeVenue).toBe(false);
});
