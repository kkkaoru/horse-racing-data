// Run with: bun run --filter xgboost-json-tree test
import { expect, it } from "vitest";
import {
  parseXgboostJsonModel,
  scoreXgboostModel,
  scoreXgboostRanking,
  type XgboostModel,
} from "./index.ts";

it("parses a save_model JSON with bracketed string base_score and string num_feature", () => {
  const parsed = parseXgboostJsonModel({
    learner: {
      feature_names: ["odds", "weight"],
      learner_model_param: { base_score: "[-3.0519525E-9]", num_feature: "175" },
      gradient_booster: {
        model: {
          trees: [
            {
              base_weights: [0, 1.5, -2],
              default_left: [1, 0, 0],
              left_children: [1, -1, -1],
              right_children: [2, -1, -1],
              split_conditions: [0.5, 1.5, -2],
              split_indices: [0, 0, 0],
            },
          ],
        },
      },
    },
    version: [3, 2, 0],
  });
  expect(parsed.base_score).toBe(-3.0519525e-9);
  expect(parsed.num_feature).toBe(175);
  expect(parsed.feature_names).toStrictEqual(["odds", "weight"]);
  expect(parsed.trees.length).toBe(1);
});

it("parses numeric base_score and numeric num_feature without string coercion", () => {
  const parsed = parseXgboostJsonModel({
    learner: {
      learner_model_param: { base_score: 0.25, num_feature: 8 },
      gradient_booster: {
        model: {
          trees: [
            {
              base_weights: [0, 0.7, -0.7],
              default_left: [0, 0, 0],
              left_children: [1, -1, -1],
              right_children: [2, -1, -1],
              split_conditions: [0.5, 0.7, -0.7],
              split_indices: [0, 0, 0],
            },
          ],
        },
      },
    },
  });
  expect(parsed.base_score).toBe(0.25);
  expect(parsed.num_feature).toBe(8);
  expect(parsed.feature_names).toStrictEqual([]);
});

it("defaults feature_names to an empty array when absent", () => {
  const parsed = parseXgboostJsonModel({
    learner: {
      learner_model_param: { base_score: "[0.0]", num_feature: "3" },
      gradient_booster: {
        model: {
          trees: [
            {
              base_weights: [0, 1, -1],
              default_left: [0, 0, 0],
              left_children: [1, -1, -1],
              right_children: [2, -1, -1],
              split_conditions: [0.5, 1, -1],
              split_indices: [0, 0, 0],
            },
          ],
        },
      },
    },
  });
  expect(parsed.feature_names).toStrictEqual([]);
});

it("throws when the top-level JSON is not an object", () => {
  expect(() => parseXgboostJsonModel(42)).toThrowError("XGBoost model JSON must be an object");
});

it("throws when learner is missing", () => {
  expect(() => parseXgboostJsonModel({ version: [3, 2, 0] })).toThrowError(
    "XGBoost model JSON is missing learner",
  );
});

it("throws when learner_model_param is missing", () => {
  expect(() => parseXgboostJsonModel({ learner: { gradient_booster: {} } })).toThrowError(
    "XGBoost model JSON is missing learner_model_param",
  );
});

it("throws when gradient_booster is missing", () => {
  expect(() =>
    parseXgboostJsonModel({
      learner: { learner_model_param: { base_score: "[0.0]", num_feature: "1" } },
    }),
  ).toThrowError("XGBoost model JSON is missing gradient_booster");
});

it("throws when gradient_booster.model is missing", () => {
  expect(() =>
    parseXgboostJsonModel({
      learner: {
        learner_model_param: { base_score: "[0.0]", num_feature: "1" },
        gradient_booster: { name: "gbtree" },
      },
    }),
  ).toThrowError("XGBoost model JSON is missing gradient_booster.model");
});

it("throws when the trees array is missing", () => {
  expect(() =>
    parseXgboostJsonModel({
      learner: {
        learner_model_param: { base_score: "[0.0]", num_feature: "1" },
        gradient_booster: { model: { tree_info: [0] } },
      },
    }),
  ).toThrowError("XGBoost model JSON is missing trees array");
});

it("throws when a tree entry is not an object", () => {
  expect(() =>
    parseXgboostJsonModel({
      learner: {
        learner_model_param: { base_score: "[0.0]", num_feature: "1" },
        gradient_booster: { model: { trees: [7] } },
      },
    }),
  ).toThrowError("XGBoost tree entry must be an object");
});

it("throws when a tree field is not a number array", () => {
  expect(() =>
    parseXgboostJsonModel({
      learner: {
        learner_model_param: { base_score: "[0.0]", num_feature: "1" },
        gradient_booster: {
          model: {
            trees: [
              {
                base_weights: ["x"],
                default_left: [0],
                left_children: [-1],
                right_children: [-1],
                split_conditions: [0],
                split_indices: [0],
              },
            ],
          },
        },
      },
    }),
  ).toThrowError("XGBoost tree field base_weights must be a number array");
});

it("throws when base_score is neither number nor string", () => {
  expect(() =>
    parseXgboostJsonModel({
      learner: {
        learner_model_param: { base_score: true, num_feature: "1" },
        gradient_booster: { model: { trees: [] } },
      },
    }),
  ).toThrowError("XGBoost base_score must be a number or string");
});

it("throws when base_score string is not a finite number", () => {
  expect(() =>
    parseXgboostJsonModel({
      learner: {
        learner_model_param: { base_score: "[oops]", num_feature: "1" },
        gradient_booster: { model: { trees: [] } },
      },
    }),
  ).toThrowError("XGBoost base_score is not a finite number");
});

it("throws when num_feature is neither number nor string", () => {
  expect(() =>
    parseXgboostJsonModel({
      learner: {
        learner_model_param: { base_score: "[0.0]", num_feature: null },
        gradient_booster: { model: { trees: [] } },
      },
    }),
  ).toThrowError("XGBoost num_feature must be a number or string");
});

it("throws when num_feature string is not a finite number", () => {
  expect(() =>
    parseXgboostJsonModel({
      learner: {
        learner_model_param: { base_score: "[0.0]", num_feature: "abc" },
        gradient_booster: { model: { trees: [] } },
      },
    }),
  ).toThrowError("XGBoost num_feature is not a finite number");
});

it("throws when feature_names is present but not a string array", () => {
  expect(() =>
    parseXgboostJsonModel({
      learner: {
        feature_names: [1, 2, 3],
        learner_model_param: { base_score: "[0.0]", num_feature: "1" },
        gradient_booster: { model: { trees: [] } },
      },
    }),
  ).toThrowError("XGBoost feature_names must be a string array");
});

it("scores a single tree taking the left leaf when the feature is below the split", () => {
  const model: XgboostModel = {
    base_score: 0,
    feature_names: ["f0"],
    num_feature: 1,
    trees: [
      {
        base_weights: [0, 1.5, -2],
        default_left: [1, 0, 0],
        left_children: [1, -1, -1],
        right_children: [2, -1, -1],
        split_conditions: [0.5, 1.5, -2],
        split_indices: [0, 0, 0],
      },
    ],
  };
  expect(scoreXgboostModel({ features: [0.2], model })).toBe(1.5);
});

it("scores a single tree taking the right leaf when the feature is at or above the split", () => {
  const model: XgboostModel = {
    base_score: 0,
    feature_names: ["f0"],
    num_feature: 1,
    trees: [
      {
        base_weights: [0, 1.5, -2],
        default_left: [1, 0, 0],
        left_children: [1, -1, -1],
        right_children: [2, -1, -1],
        split_conditions: [0.5, 1.5, -2],
        split_indices: [0, 0, 0],
      },
    ],
  };
  expect(scoreXgboostModel({ features: [0.9], model })).toBe(-2);
});

it("follows default_left when the feature is NaN", () => {
  const model: XgboostModel = {
    base_score: 0,
    feature_names: ["f0"],
    num_feature: 1,
    trees: [
      {
        base_weights: [0, 1.5, -2],
        default_left: [1, 0, 0],
        left_children: [1, -1, -1],
        right_children: [2, -1, -1],
        split_conditions: [0.5, 1.5, -2],
        split_indices: [0, 0, 0],
      },
    ],
  };
  expect(scoreXgboostModel({ features: [Number.NaN], model })).toBe(1.5);
});

it("follows the right child by default when the feature is missing and default_left is 0", () => {
  const model: XgboostModel = {
    base_score: 0,
    feature_names: ["f0", "f1"],
    num_feature: 2,
    trees: [
      {
        base_weights: [0, 3, 0.25],
        default_left: [0, 0, 0],
        left_children: [1, -1, -1],
        right_children: [2, -1, -1],
        split_conditions: [10, 3, 0.25],
        split_indices: [1, 0, 0],
      },
    ],
  };
  expect(scoreXgboostModel({ features: [0.9], model })).toBe(0.25);
});

it("sums leaf outputs across multiple trees and adds base_score", () => {
  const model: XgboostModel = {
    base_score: 0.5,
    feature_names: ["f0", "f1"],
    num_feature: 2,
    trees: [
      {
        base_weights: [0, 1.5, -2],
        default_left: [1, 0, 0],
        left_children: [1, -1, -1],
        right_children: [2, -1, -1],
        split_conditions: [0.5, 1.5, -2],
        split_indices: [0, 0, 0],
      },
      {
        base_weights: [0, 3, 0.25],
        default_left: [0, 0, 0],
        left_children: [1, -1, -1],
        right_children: [2, -1, -1],
        split_conditions: [10, 3, 0.25],
        split_indices: [1, 0, 0],
      },
    ],
  };
  expect(scoreXgboostModel({ features: [0.2, 5], model })).toBe(5);
});

it("sums the low branch of both trees plus base_score for a high-feature horse", () => {
  const model: XgboostModel = {
    base_score: 0.5,
    feature_names: ["f0", "f1"],
    num_feature: 2,
    trees: [
      {
        base_weights: [0, 1.5, -2],
        default_left: [1, 0, 0],
        left_children: [1, -1, -1],
        right_children: [2, -1, -1],
        split_conditions: [0.5, 1.5, -2],
        split_indices: [0, 0, 0],
      },
      {
        base_weights: [0, 3, 0.25],
        default_left: [0, 0, 0],
        left_children: [1, -1, -1],
        right_children: [2, -1, -1],
        split_conditions: [10, 3, 0.25],
        split_indices: [1, 0, 0],
      },
    ],
  };
  expect(scoreXgboostModel({ features: [0.9, 20], model })).toBe(-1.25);
});

it("walks a depth-2 tree down the left-then-right path", () => {
  const model: XgboostModel = {
    base_score: 0,
    feature_names: ["f0", "f1"],
    num_feature: 2,
    trees: [
      {
        base_weights: [0, 0, 0, 4, -4, 9, -9],
        default_left: [0, 0, 0, 0, 0, 0, 0],
        left_children: [1, 3, 5, -1, -1, -1, -1],
        right_children: [2, 4, 6, -1, -1, -1, -1],
        split_conditions: [0.5, 0.25, 0.75, 4, -4, 9, -9],
        split_indices: [0, 1, 1, 0, 0, 0, 0],
      },
    ],
  };
  expect(scoreXgboostModel({ features: [0.1, 0.9], model })).toBe(-4);
});

it("ranks horses descending by raw score with a lower-umaban tiebreak", () => {
  const model: XgboostModel = {
    base_score: 0.5,
    feature_names: ["f0", "f1"],
    num_feature: 2,
    trees: [
      {
        base_weights: [0, 1.5, -2],
        default_left: [1, 0, 0],
        left_children: [1, -1, -1],
        right_children: [2, -1, -1],
        split_conditions: [0.5, 1.5, -2],
        split_indices: [0, 0, 0],
      },
      {
        base_weights: [0, 3, 0.25],
        default_left: [0, 0, 0],
        left_children: [1, -1, -1],
        right_children: [2, -1, -1],
        split_conditions: [10, 3, 0.25],
        split_indices: [1, 0, 0],
      },
    ],
  };
  const ranked = scoreXgboostRanking({
    model,
    rows: [
      { features: [0.2, 5], umaban: 3 },
      { features: [0.9, 20], umaban: 1 },
      { features: [0.2, 5], umaban: 5 },
    ],
  });
  expect(ranked).toStrictEqual([
    { predicted_rank: 1, raw_score: 5, umaban: 3 },
    { predicted_rank: 2, raw_score: 5, umaban: 5 },
    { predicted_rank: 3, raw_score: -1.25, umaban: 1 },
  ]);
});
