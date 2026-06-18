// Run with: bun run --filter finish-position-cron test
//
// Bit-exact parity test for the Worker-native JRA scoring core. The fixtures
// (cb-small.json / xgb-small.json / jra-parity-golden.json) are produced by
// tmp/gen_parity_golden.py with the SAME catboost (1.2.10) + xgboost (3.2.0)
// runtimes the production container uses. The golden carries the exact Python
// RawFormulaVal (CatBoost) + rank:ndcg (XGBoost) raw scores for a synthetic race
// projected through scorer._coerce (null/""->0.0) — the identical path
// predict_upcoming._score_one_race_etop2 takes. We re-score with the TS scorer
// and assert raw scores match to ~1e-6, then assert the full E-top2 ranking
// (umaban + ketto + rank + injected override score) matches the Python golden.
import { expect, test } from "vitest";
import { parseCatBoostJsonModel, scoreCatBoostModel } from "catboost-json-tree";
import { parseXgboostJsonModel, scoreXgboostModel } from "xgboost-json-tree";

import cbSmall from "./__fixtures__/cb-small.json";
import golden from "./__fixtures__/jra-parity-golden.json";
import xgbSmall from "./__fixtures__/xgb-small.json";
import { projectCatBoostCells, projectFeatureRow } from "./feature-projection";
import { scoreJraRace, type JraRaceEntry } from "./jra-scorer";

const RAW_SCORE_TOLERANCE = 1e-6;
const FIRST = 0;

const cbModel = parseCatBoostJsonModel(cbSmall);
const xgbModel = parseXgboostJsonModel(xgbSmall);
const featureNames = golden.featureNames;

const goldenEntries: JraRaceEntry[] = golden.entries.map((entry) => ({
  features: entry,
  kettoTorokuBango: String(entry.ketto_toroku_bango),
  umaban: Number(entry.umaban),
}));

test("TS CatBoost raw scores match Python RawFormulaVal to 1e-6 for every horse", () => {
  const tsScores = golden.entries.map((entry) =>
    scoreCatBoostModel({ features: projectCatBoostCells(entry, featureNames), model: cbModel }),
  );
  const maxDiff = Math.max(
    ...tsScores.map((score, index) => Math.abs(score - golden.cbScores[index]!)),
  );
  expect(maxDiff).toBeLessThan(RAW_SCORE_TOLERANCE);
});

test("TS XGBoost raw scores match Python rank:ndcg to 1e-6 for every horse", () => {
  const tsScores = golden.entries.map((entry) =>
    scoreXgboostModel({
      features: projectFeatureRow(entry, featureNames, "xgboost"),
      model: xgbModel,
    }),
  );
  const maxDiff = Math.max(
    ...tsScores.map((score, index) => Math.abs(score - golden.xgbScores[index]!)),
  );
  expect(maxDiff).toBeLessThan(RAW_SCORE_TOLERANCE);
});

test("scoreJraRace reproduces the Python end-to-end E-top2 ranking for an eligible class", () => {
  const result = scoreJraRace({
    catboostModel: cbModel,
    entries: goldenEntries,
    featureNames,
    raceClass: "703",
    xgboostModel: xgbModel,
  });
  expect(result.etop2Fired).toBe(true);
  const tsRanking = result.predictions.map((row) => ({
    ketto: row.kettoTorokuBango,
    predictedRank: row.predictedRank,
    umaban: row.umaban,
  }));
  const goldenRanking = golden.endToEnd.eligible.map((row) => ({
    ketto: row.ketto,
    predictedRank: row.predictedRank,
    umaban: row.umaban,
  }));
  expect(tsRanking).toStrictEqual(goldenRanking);
});

test("scoreJraRace injects the override scores matching the Python golden scores", () => {
  const result = scoreJraRace({
    catboostModel: cbModel,
    entries: goldenEntries,
    featureNames,
    raceClass: "703",
    xgboostModel: xgbModel,
  });
  const topScore = result.predictions[FIRST]?.predictedScore ?? 0;
  const goldenTopScore = golden.endToEnd.eligible[FIRST]?.predictedScore ?? 0;
  expect(Math.abs(topScore - goldenTopScore)).toBeLessThan(RAW_SCORE_TOLERANCE);
});

test("scoreJraRace reproduces the pure CatBoost ranking for the excluded class 701", () => {
  const result = scoreJraRace({
    catboostModel: cbModel,
    entries: goldenEntries,
    featureNames,
    raceClass: "701",
    xgboostModel: xgbModel,
  });
  expect(result.etop2Fired).toBe(false);
  const tsRanking = result.predictions.map((row) => ({
    predictedRank: row.predictedRank,
    umaban: row.umaban,
  }));
  const goldenRanking = golden.endToEnd.excluded.map((row) => ({
    predictedRank: row.predictedRank,
    umaban: row.umaban,
  }));
  expect(tsRanking).toStrictEqual(goldenRanking);
});

test("scoreJraRace emits null probability + finish-position fields", () => {
  const result = scoreJraRace({
    catboostModel: cbModel,
    entries: goldenEntries,
    featureNames,
    raceClass: "703",
    xgboostModel: xgbModel,
  });
  expect(result.predictions[FIRST]?.predictedTop1Prob).toBe(null);
  expect(result.predictions[FIRST]?.predictedTop3Prob).toBe(null);
  expect(result.predictions[FIRST]?.predictedFinishPosition).toBe(null);
});

test("scoreJraRace breaks score ties on ascending ketto with deterministic ranks", () => {
  const result = scoreJraRace({
    catboostModel: parseCatBoostJsonModel({
      features_info: { float_features: [] },
      oblivious_trees: [],
      scale_and_bias: [1, [0]],
    }),
    entries: [
      { features: {}, kettoTorokuBango: "ccc", umaban: 3 },
      { features: {}, kettoTorokuBango: "aaa", umaban: 1 },
      { features: {}, kettoTorokuBango: "bbb", umaban: 2 },
    ],
    featureNames: [],
    raceClass: "703",
    xgboostModel: parseXgboostJsonModel({
      learner: {
        gradient_booster: { model: { trees: [] } },
        learner_model_param: { base_score: "[0]", num_feature: "0" },
      },
    }),
  });
  expect(result.predictions.map((row) => row.kettoTorokuBango)).toStrictEqual([
    "aaa",
    "bbb",
    "ccc",
  ]);
});

test("scoreJraRace keeps a deterministic order for identical score + ketto entries", () => {
  const result = scoreJraRace({
    catboostModel: parseCatBoostJsonModel({
      features_info: { float_features: [] },
      oblivious_trees: [],
      scale_and_bias: [1, [0]],
    }),
    entries: [
      { features: {}, kettoTorokuBango: "same", umaban: 5 },
      { features: {}, kettoTorokuBango: "same", umaban: 6 },
    ],
    featureNames: [],
    raceClass: "703",
    xgboostModel: parseXgboostJsonModel({
      learner: {
        gradient_booster: { model: { trees: [] } },
        learner_model_param: { base_score: "[0]", num_feature: "0" },
      },
    }),
  });
  expect(result.predictions.map((row) => row.umaban)).toStrictEqual([5, 6]);
});
