// Run with bun test apps/sync-realtime-data/src/running-style-lightgbm-tree.test.ts
import { expect, test } from "vitest";

import {
  buildFeatureVector,
  predictRunningStyle,
  walkTree,
  type CompactLightGBMModel,
  type LightGBMTreeNode,
} from "./running-style-lightgbm-tree";

const SIMPLE_NUMERIC_TREE: LightGBMTreeNode = {
  decision_type: "<=",
  default_left: true,
  left_child: { leaf_value: -1.5 },
  right_child: { leaf_value: 2.5 },
  split_feature: 0,
  threshold: 0.5,
};

test("walkTree returns left leaf when value satisfies <= split", () => {
  const result = walkTree(SIMPLE_NUMERIC_TREE, {
    missing: Uint8Array.from([0]),
    values: Float64Array.from([0.3]),
  });
  expect(result).toBe(-1.5);
});

test("walkTree returns right leaf when value exceeds <= split", () => {
  const result = walkTree(SIMPLE_NUMERIC_TREE, {
    missing: Uint8Array.from([0]),
    values: Float64Array.from([0.7]),
  });
  expect(result).toBe(2.5);
});

test("walkTree follows default_left=true when value is missing", () => {
  const result = walkTree(SIMPLE_NUMERIC_TREE, {
    missing: Uint8Array.from([1]),
    values: Float64Array.from([0]),
  });
  expect(result).toBe(-1.5);
});

test("walkTree honors default_left=false when value is missing", () => {
  const tree: LightGBMTreeNode = { ...SIMPLE_NUMERIC_TREE, default_left: false };
  const result = walkTree(tree, {
    missing: Uint8Array.from([1]),
    values: Float64Array.from([0]),
  });
  expect(result).toBe(2.5);
});

test("walkTree handles categorical == split with matching token", () => {
  const tree: LightGBMTreeNode = {
    decision_type: "==",
    default_left: false,
    left_child: { leaf_value: 5.0 },
    right_child: { leaf_value: -5.0 },
    split_feature: 0,
    threshold: "1||3||5",
  };
  const result = walkTree(tree, {
    missing: Uint8Array.from([0]),
    values: Float64Array.from([3]),
  });
  expect(result).toBe(5.0);
});

test("walkTree handles categorical == split with non-matching token", () => {
  const tree: LightGBMTreeNode = {
    decision_type: "==",
    default_left: false,
    left_child: { leaf_value: 5.0 },
    right_child: { leaf_value: -5.0 },
    split_feature: 0,
    threshold: "1||3||5",
  };
  const result = walkTree(tree, {
    missing: Uint8Array.from([0]),
    values: Float64Array.from([2]),
  });
  expect(result).toBe(-5.0);
});

const buildFourClassModel = (): CompactLightGBMModel => ({
  categorical_features: [],
  class_labels: ["nige", "senkou", "sashi", "oikomi"],
  feature_names: ["feature_0"],
  model_version: "test-v0",
  num_class: 4,
  num_tree_per_iteration: 4,
  objective: "multiclass num_class:4",
  trees: [
    { tree_structure: { leaf_value: 10.0 } },
    { tree_structure: { leaf_value: 0.0 } },
    { tree_structure: { leaf_value: 0.0 } },
    { tree_structure: { leaf_value: 0.0 } },
  ],
});

test("predictRunningStyle picks nige when class 0 logit dominates", () => {
  const result = predictRunningStyle(buildFourClassModel(), {
    missing: Uint8Array.from([0]),
    values: Float64Array.from([0]),
  });
  expect(result.predictedLabel).toBe("nige");
});

test("predictRunningStyle returns predictedClass 0 for class 0 dominance", () => {
  const result = predictRunningStyle(buildFourClassModel(), {
    missing: Uint8Array.from([0]),
    values: Float64Array.from([0]),
  });
  expect(result.predictedClass).toBe(0);
});

test("predictRunningStyle probabilities sum to 1.0 (within tolerance)", () => {
  const result = predictRunningStyle(buildFourClassModel(), {
    missing: Uint8Array.from([0]),
    values: Float64Array.from([0]),
  });
  const sum = result.probabilities.nige + result.probabilities.senkou + result.probabilities.sashi + result.probabilities.oikomi;
  expect(Math.abs(sum - 1.0) < 1e-6).toBe(true);
});

test("buildFeatureVector marks missing fields when value is null", () => {
  const vector = buildFeatureVector({
    featureNames: ["a", "b"],
    values: { a: 1.5, b: null },
  });
  expect(vector.missing[1]).toBe(1);
});

test("buildFeatureVector populates value for present fields", () => {
  const vector = buildFeatureVector({
    featureNames: ["a", "b"],
    values: { a: 1.5, b: null },
  });
  expect(vector.values[0]).toBe(1.5);
});

test("buildFeatureVector treats undefined the same as missing", () => {
  const vector = buildFeatureVector({
    featureNames: ["a"],
    values: {},
  });
  expect(vector.missing[0]).toBe(1);
});

test("buildFeatureVector treats NaN as missing", () => {
  const vector = buildFeatureVector({
    featureNames: ["a"],
    values: { a: Number.NaN },
  });
  expect(vector.missing[0]).toBe(1);
});

test("predictRunningStyle picks senkou when class 1 logit dominates", () => {
  const model: CompactLightGBMModel = {
    ...buildFourClassModel(),
    trees: [
      { tree_structure: { leaf_value: 0.0 } },
      { tree_structure: { leaf_value: 10.0 } },
      { tree_structure: { leaf_value: 0.0 } },
      { tree_structure: { leaf_value: 0.0 } },
    ],
  };
  const result = predictRunningStyle(model, {
    missing: Uint8Array.from([0]),
    values: Float64Array.from([0]),
  });
  expect(result.predictedLabel).toBe("senkou");
});

test("predictRunningStyle picks oikomi when class 3 logit dominates", () => {
  const model: CompactLightGBMModel = {
    ...buildFourClassModel(),
    trees: [
      { tree_structure: { leaf_value: 0.0 } },
      { tree_structure: { leaf_value: 0.0 } },
      { tree_structure: { leaf_value: 0.0 } },
      { tree_structure: { leaf_value: 8.0 } },
    ],
  };
  const result = predictRunningStyle(model, {
    missing: Uint8Array.from([0]),
    values: Float64Array.from([0]),
  });
  expect(result.predictedLabel).toBe("oikomi");
});

test("predictRunningStyle accumulates logits across multiple iterations", () => {
  const model: CompactLightGBMModel = {
    ...buildFourClassModel(),
    trees: [
      { tree_structure: { leaf_value: 5.0 } },
      { tree_structure: { leaf_value: 0.0 } },
      { tree_structure: { leaf_value: 0.0 } },
      { tree_structure: { leaf_value: 0.0 } },
      { tree_structure: { leaf_value: 0.0 } },
      { tree_structure: { leaf_value: 10.0 } },
      { tree_structure: { leaf_value: 0.0 } },
      { tree_structure: { leaf_value: 0.0 } },
    ],
  };
  const result = predictRunningStyle(model, {
    missing: Uint8Array.from([0]),
    values: Float64Array.from([0]),
  });
  expect(result.predictedLabel).toBe("senkou");
});
