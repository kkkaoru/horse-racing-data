// bun で実行する (bunx vitest)
import { expect, it } from "vitest";

import {
  buildBucketFilter,
  FINISH_PREDICTION_PARAM_NAMES,
  getFinishPredictionDimensionFlags,
} from "./finish-prediction-dimensions";

it("returns all flags ON by default for NAR non-banei without grade", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags).toStrictEqual({
    keibajo: true,
    distance: true,
    kyosoShubetsu: true,
    kyosoJoken: false,
    condition: true,
    track: true,
    grade: false,
    raceName: false,
  });
});

it("turns the keibajo flag OFF when the query param equals 0", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: { finishPredictionKeibajo: "0" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.keibajo).toBe(false);
});

it("turns the track flag OFF when the query param equals 0", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: { finishPredictionTrack: "0" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.track).toBe(false);
});

it("forces kyosoJoken OFF when source is NAR even if user enabled it", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: { finishPredictionJoken: "1" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.kyosoJoken).toBe(false);
});

it("forces condition and grade OFF when source is JRA", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: {},
    source: "jra",
    gradeCode: "A",
    isBanEi: false,
  });
  expect(flags.condition).toBe(false);
  expect(flags.grade).toBe(false);
});

it("forces track OFF when the race is ban-ei", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: null,
    isBanEi: true,
  });
  expect(flags.track).toBe(false);
});

it("forces grade OFF when gradeCode is null for NAR", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.grade).toBe(false);
});

it("forces grade OFF when gradeCode is empty for NAR", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: "",
    isBanEi: false,
  });
  expect(flags.grade).toBe(false);
});

it("keeps raceName ON when gradeCode is A", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: "A",
    isBanEi: false,
  });
  expect(flags.raceName).toBe(true);
});

it("forces raceName OFF when gradeCode is C", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: "C",
    isBanEi: false,
  });
  expect(flags.raceName).toBe(false);
});

it("keeps raceName ON when gradeCode is F", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: {},
    source: "nar",
    gradeCode: "F",
    isBanEi: false,
  });
  expect(flags.raceName).toBe(true);
});

it("allows raceName flag to be disabled via query param when gradeCode is A", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: { finishPredictionRaceName: "0" },
    source: "nar",
    gradeCode: "A",
    isBanEi: false,
  });
  expect(flags.raceName).toBe(false);
});

it("reads array query value first element for the keibajo flag", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: { finishPredictionKeibajo: ["0", "1"] },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.keibajo).toBe(false);
});

it("turns the distance flag OFF when the query param equals 0", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: { finishPredictionDistance: "0" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.distance).toBe(false);
});

it("turns the kyosoShubetsu flag OFF when the query param equals 0", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: { finishPredictionShubetsu: "0" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.kyosoShubetsu).toBe(false);
});

it("turns the condition flag OFF for NAR when the query param equals 0", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: { finishPredictionCondition: "0" },
    source: "nar",
    gradeCode: null,
    isBanEi: false,
  });
  expect(flags.condition).toBe(false);
});

it("turns the grade flag OFF for NAR when the query param equals 0", () => {
  const flags = getFinishPredictionDimensionFlags({
    query: { finishPredictionGrade: "0" },
    source: "nar",
    gradeCode: "A",
    isBanEi: false,
  });
  expect(flags.grade).toBe(false);
});

it("builds a bucket filter for a JRA race with all dims ON", () => {
  const filter = buildBucketFilter(
    {
      source: "jra",
      keibajoCode: "05",
      kyori: "2400",
      kyosoShubetsuCode: "11",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3歳未勝利",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
      conditionKey: null,
      raceName: null,
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: true,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  );
  expect(filter).toStrictEqual({
    category: "jra",
    source: "jra",
    keibajoCode: "05",
    kyori: 2400,
    kyosoShubetsuCode: "11",
    kyosoJokenCode: "005",
    conditionKey: null,
    trackCode: "10",
    gradeCode: null,
    raceName: null,
    enabled: {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: true,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  });
});

it("sets kyosoJokenCode to null when the kyosoJoken flag is OFF", () => {
  const filter = buildBucketFilter(
    {
      source: "jra",
      keibajoCode: "05",
      kyori: "2400",
      kyosoShubetsuCode: "11",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3歳未勝利",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
      conditionKey: null,
      raceName: null,
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  );
  expect(filter.kyosoJokenCode).toBe(null);
});

it("sets conditionKey to null when the condition flag is OFF for a NAR race", () => {
  const filter = buildBucketFilter(
    {
      source: "nar",
      keibajoCode: "30",
      kyori: "1600",
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "B3",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
      conditionKey: "B3",
      raceName: null,
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  );
  expect(filter.conditionKey).toBe(null);
});

it("keeps conditionKey when the condition flag is ON for a NAR race", () => {
  const filter = buildBucketFilter(
    {
      source: "nar",
      keibajoCode: "30",
      kyori: "1600",
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "B3",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
      conditionKey: "B3",
      raceName: null,
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: true,
      track: true,
      grade: false,
      raceName: false,
    },
  );
  expect(filter.conditionKey).toBe("B3");
});

it("sets trackCode to null when the track flag is OFF", () => {
  const filter = buildBucketFilter(
    {
      source: "jra",
      keibajoCode: "05",
      kyori: "2400",
      kyosoShubetsuCode: "11",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3歳未勝利",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
      conditionKey: null,
      raceName: null,
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: true,
      condition: false,
      track: false,
      grade: false,
      raceName: false,
    },
  );
  expect(filter.trackCode).toBe(null);
});

it("sets gradeCode and raceName to null when respective flags are OFF", () => {
  const filter = buildBucketFilter(
    {
      source: "nar",
      keibajoCode: "30",
      kyori: "1800",
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "G1",
      trackCode: null,
      gradeCode: "A",
      kyosomeiHondai: "東京大賞典",
      conditionKey: null,
      raceName: "東京大賞典",
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: false,
      grade: false,
      raceName: false,
    },
  );
  expect(filter.gradeCode).toBe(null);
  expect(filter.raceName).toBe(null);
});

it("keeps gradeCode and raceName when both flags are ON for a NAR graded race", () => {
  const filter = buildBucketFilter(
    {
      source: "nar",
      keibajoCode: "30",
      kyori: "1800",
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "G1",
      trackCode: null,
      gradeCode: "A",
      kyosomeiHondai: "東京大賞典",
      conditionKey: null,
      raceName: "東京大賞典",
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: false,
      track: false,
      grade: true,
      raceName: true,
    },
  );
  expect(filter.gradeCode).toBe("A");
  expect(filter.raceName).toBe("東京大賞典");
});

it("derives category as ban-ei for keibajoCode 83", () => {
  const filter = buildBucketFilter(
    {
      source: "nar",
      keibajoCode: "83",
      kyori: "200",
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "B3",
      trackCode: null,
      gradeCode: null,
      kyosomeiHondai: null,
      conditionKey: "B3",
      raceName: null,
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: true,
      track: false,
      grade: false,
      raceName: false,
    },
  );
  expect(filter.category).toBe("ban-ei");
});

it("derives category as nar for non-banei NAR keibajoCode", () => {
  const filter = buildBucketFilter(
    {
      source: "nar",
      keibajoCode: "30",
      kyori: "1600",
      kyosoShubetsuCode: "11",
      kyosoJokenCode: null,
      kyosoJokenMeisho: "B3",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
      conditionKey: "B3",
      raceName: null,
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: false,
      condition: true,
      track: true,
      grade: false,
      raceName: false,
    },
  );
  expect(filter.category).toBe("nar");
});

it("parses null kyori as 0 via Number coercion", () => {
  const filter = buildBucketFilter(
    {
      source: "jra",
      keibajoCode: "05",
      kyori: null,
      kyosoShubetsuCode: "11",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3歳未勝利",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
      conditionKey: null,
      raceName: null,
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: true,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  );
  expect(filter.kyori).toBe(0);
});

it("falls back to 0 when kyori is a non-numeric string", () => {
  const filter = buildBucketFilter(
    {
      source: "jra",
      keibajoCode: "05",
      kyori: "abc",
      kyosoShubetsuCode: "11",
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3歳未勝利",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
      conditionKey: null,
      raceName: null,
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: true,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  );
  expect(filter.kyori).toBe(0);
});

it("substitutes empty string when kyosoShubetsuCode is null", () => {
  const filter = buildBucketFilter(
    {
      source: "jra",
      keibajoCode: "05",
      kyori: "2400",
      kyosoShubetsuCode: null,
      kyosoJokenCode: "005",
      kyosoJokenMeisho: "3歳未勝利",
      trackCode: "10",
      gradeCode: null,
      kyosomeiHondai: null,
      conditionKey: null,
      raceName: null,
    },
    {
      keibajo: true,
      distance: true,
      kyosoShubetsu: true,
      kyosoJoken: true,
      condition: false,
      track: true,
      grade: false,
      raceName: false,
    },
  );
  expect(filter.kyosoShubetsuCode).toBe("");
});

it("exports the expected URL param names mapping", () => {
  expect(FINISH_PREDICTION_PARAM_NAMES).toStrictEqual({
    keibajo: "finishPredictionKeibajo",
    distance: "finishPredictionDistance",
    kyosoShubetsu: "finishPredictionShubetsu",
    kyosoJoken: "finishPredictionJoken",
    condition: "finishPredictionCondition",
    track: "finishPredictionTrack",
    grade: "finishPredictionGrade",
    raceName: "finishPredictionRaceName",
  });
});
