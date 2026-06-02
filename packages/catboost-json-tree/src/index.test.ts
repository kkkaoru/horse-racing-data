// Run with: bun run --filter catboost-json-tree test
import { expect, test } from "vitest";
import {
  parseCatBoostJsonModel,
  scoreCatBoostModel,
  scoreCatBoostRanking,
  type CatBoostModel,
} from "./index";

// Hand-crafted two-tree float model. Leaf index = (split0 bit) | (split1 bit << 1).
// Tree A: split0 float_feature_index=0 border=0.5, split1 float_feature_index=1 border=1.5.
//   leaf_values [1, 2, 3, 4] => idx0=1, idx1=2, idx2=3, idx3=4.
// Tree B: split0 float_feature_index=0 border=2.5, leaf_values [0.5, -0.5].
const FLOAT_MODEL_JSON = {
  features_info: {
    float_features: [
      {
        borders: [0.5, 2.5],
        feature_id: "",
        feature_index: 0,
        flat_feature_index: 0,
        has_nans: false,
        nan_value_treatment: "AsIs",
      },
      {
        borders: [1.5],
        feature_id: "",
        feature_index: 1,
        flat_feature_index: 1,
        has_nans: true,
        nan_value_treatment: "AsFalse",
      },
    ],
  },
  oblivious_trees: [
    {
      leaf_values: [1, 2, 3, 4],
      leaf_weights: [10, 10, 10, 10],
      splits: [
        { border: 0.5, float_feature_index: 0, split_index: 1, split_type: "FloatFeature" },
        { border: 1.5, float_feature_index: 1, split_index: 2, split_type: "FloatFeature" },
      ],
    },
    {
      leaf_values: [0.5, -0.5],
      leaf_weights: [20, 20],
      splits: [{ border: 2.5, float_feature_index: 0, split_index: 3, split_type: "FloatFeature" }],
    },
  ],
  scale_and_bias: [1, [0]],
};

// Single-tree float model with AsTrue missing treatment on feature 0.
const AS_TRUE_MODEL_JSON = {
  features_info: {
    float_features: [
      {
        borders: [0.5],
        feature_id: "",
        feature_index: 0,
        flat_feature_index: 0,
        has_nans: true,
        nan_value_treatment: "AsTrue",
      },
      {
        borders: [1.5],
        feature_id: "",
        feature_index: 1,
        flat_feature_index: 1,
        has_nans: false,
        nan_value_treatment: "AsIs",
      },
    ],
  },
  oblivious_trees: [
    {
      leaf_values: [1, 2, 3, 4],
      leaf_weights: [5, 5, 5, 5],
      splits: [
        { border: 0.5, float_feature_index: 0, split_index: 1, split_type: "FloatFeature" },
        { border: 1.5, float_feature_index: 1, split_index: 2, split_type: "FloatFeature" },
      ],
    },
  ],
  scale_and_bias: [1, [0]],
};

// One-hot categorical model. cat_feature_index=0, matches values {7, 9}.
const CATEGORICAL_MODEL_JSON = {
  features_info: {
    categorical_features: [{ feature_id: "", feature_index: 0, flat_feature_index: 0 }],
    float_features: [],
  },
  oblivious_trees: [
    {
      leaf_values: [10, 20],
      leaf_weights: [3, 3],
      splits: [
        { cat_feature_index: 0, split_index: 5, split_type: "OneHotFeature", values: [7, 9] },
      ],
    },
  ],
  scale_and_bias: [1, [0]],
};

// Scale/bias model reusing the two-tree float layout but scale=2, bias=10.
const SCALED_MODEL_JSON = {
  features_info: {
    float_features: [
      {
        borders: [0.5, 2.5],
        feature_index: 0,
        flat_feature_index: 0,
        has_nans: false,
        nan_value_treatment: "AsIs",
      },
      {
        borders: [1.5],
        feature_index: 1,
        flat_feature_index: 1,
        has_nans: false,
        nan_value_treatment: "AsIs",
      },
    ],
  },
  oblivious_trees: [
    {
      leaf_values: [1, 2, 3, 4],
      leaf_weights: [10, 10, 10, 10],
      splits: [
        { border: 0.5, float_feature_index: 0, split_index: 1, split_type: "FloatFeature" },
        { border: 1.5, float_feature_index: 1, split_index: 2, split_type: "FloatFeature" },
      ],
    },
    {
      leaf_values: [0.5, -0.5],
      leaf_weights: [20, 20],
      splits: [{ border: 2.5, float_feature_index: 0, split_index: 3, split_type: "FloatFeature" }],
    },
  ],
  scale_and_bias: [2, [10]],
};

test("parseCatBoostJsonModel extracts trees, float features, scale and bias", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  expect(model.trees.length).toBe(2);
  expect(model.trees[0]?.leafValues).toStrictEqual([1, 2, 3, 4]);
  expect(model.trees[0]?.splits[0]?.featureIndex).toBe(0);
  expect(model.trees[0]?.splits[0]?.border).toBe(0.5);
  expect(model.trees[0]?.splits[1]?.border).toBe(1.5);
  expect(model.trees[1]?.leafValues).toStrictEqual([0.5, -0.5]);
  expect(model.floatFeatures.length).toBe(2);
  expect(model.floatFeatures[1]?.hasNans).toBe(true);
  expect(model.floatFeatures[1]?.nanTreatment).toBe("AsFalse");
  expect(model.categoricalFeatures).toStrictEqual([]);
  expect(model.scale).toBe(1);
  expect(model.bias).toBe(0);
});

test("parseCatBoostJsonModel reads one-hot categorical features and splits", () => {
  const model = parseCatBoostJsonModel(CATEGORICAL_MODEL_JSON);
  expect(model.floatFeatures).toStrictEqual([]);
  expect(model.categoricalFeatures.length).toBe(1);
  expect(model.categoricalFeatures[0]?.featureIndex).toBe(0);
  expect(model.trees[0]?.splits[0]?.splitType).toBe("OneHotFeature");
  expect(model.trees[0]?.splits[0]?.featureIndex).toBe(0);
  expect(model.trees[0]?.splits[0]?.oneHotValues).toStrictEqual([7, 9]);
  expect(model.trees[0]?.splits[0]?.border).toBe(0);
});

test("parseCatBoostJsonModel defaults missing scale_and_bias to scale 1 bias 0", () => {
  const model = parseCatBoostJsonModel({
    features_info: { float_features: [] },
    oblivious_trees: [],
  });
  expect(model.scale).toBe(1);
  expect(model.bias).toBe(0);
  expect(model.trees).toStrictEqual([]);
});

test("parseCatBoostJsonModel rejects non-object input", () => {
  expect(() => parseCatBoostJsonModel(42)).toThrow("CatBoost JSON model: expected an object");
});

test("parseCatBoostJsonModel rejects array input", () => {
  expect(() => parseCatBoostJsonModel([1, 2, 3])).toThrow(
    "CatBoost JSON model: expected an object",
  );
});

test("parseCatBoostJsonModel rejects non-array oblivious_trees", () => {
  expect(() =>
    parseCatBoostJsonModel({ features_info: { float_features: [] }, oblivious_trees: 7 }),
  ).toThrow("CatBoost JSON model: expected array for oblivious_trees");
});

test("parseCatBoostJsonModel rejects unsupported split type", () => {
  expect(() =>
    parseCatBoostJsonModel({
      features_info: { float_features: [] },
      oblivious_trees: [
        { leaf_values: [1, 2], splits: [{ split_index: 1, split_type: "OnlineCtr" }] },
      ],
    }),
  ).toThrow("CatBoost JSON model: unsupported split_type OnlineCtr");
});

test("parseCatBoostJsonModel rejects non-string split_type", () => {
  expect(() =>
    parseCatBoostJsonModel({
      features_info: { float_features: [] },
      oblivious_trees: [{ leaf_values: [1, 2], splits: [{ split_index: 1, split_type: 9 }] }],
    }),
  ).toThrow("CatBoost JSON model: expected string for splits.split_type");
});

test("parseCatBoostJsonModel rejects non-number leaf value", () => {
  expect(() =>
    parseCatBoostJsonModel({
      features_info: { float_features: [] },
      oblivious_trees: [
        {
          leaf_values: [1, "x"],
          splits: [{ border: 0.5, float_feature_index: 0, split_type: "FloatFeature" }],
        },
      ],
    }),
  ).toThrow("CatBoost JSON model: expected number for oblivious_trees.leaf_values[1]");
});

test("parseCatBoostJsonModel rejects scale_and_bias of wrong length", () => {
  expect(() =>
    parseCatBoostJsonModel({
      features_info: { float_features: [] },
      oblivious_trees: [],
      scale_and_bias: [1],
    }),
  ).toThrow("CatBoost JSON model: scale_and_bias must have 2 elements");
});

test("scoreCatBoostModel scores a single tree at leaf index 3", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  const singleTree: CatBoostModel = {
    bias: model.bias,
    categoricalFeatures: model.categoricalFeatures,
    floatFeatures: model.floatFeatures,
    scale: model.scale,
    trees: [model.trees[0]!],
  };
  const score = scoreCatBoostModel({
    features: [
      { isMissing: false, value: 1 },
      { isMissing: false, value: 2 },
    ],
    model: singleTree,
  });
  expect(score).toBe(4);
});

test("scoreCatBoostModel scores a single tree at leaf index 0", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  const singleTree: CatBoostModel = {
    bias: model.bias,
    categoricalFeatures: model.categoricalFeatures,
    floatFeatures: model.floatFeatures,
    scale: model.scale,
    trees: [model.trees[0]!],
  };
  const score = scoreCatBoostModel({
    features: [
      { isMissing: false, value: 0 },
      { isMissing: false, value: 0 },
    ],
    model: singleTree,
  });
  expect(score).toBe(1);
});

test("scoreCatBoostModel scores a single tree at leaf index 1 and 2", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  const singleTree: CatBoostModel = {
    bias: model.bias,
    categoricalFeatures: model.categoricalFeatures,
    floatFeatures: model.floatFeatures,
    scale: model.scale,
    trees: [model.trees[0]!],
  };
  const scoreIdx1 = scoreCatBoostModel({
    features: [
      { isMissing: false, value: 1 },
      { isMissing: false, value: 0 },
    ],
    model: singleTree,
  });
  const scoreIdx2 = scoreCatBoostModel({
    features: [
      { isMissing: false, value: 0 },
      { isMissing: false, value: 2 },
    ],
    model: singleTree,
  });
  expect(scoreIdx1).toBe(2);
  expect(scoreIdx2).toBe(3);
});

test("scoreCatBoostModel sums contributions across multiple trees", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  const score = scoreCatBoostModel({
    features: [
      { isMissing: false, value: 1 },
      { isMissing: false, value: 2 },
    ],
    model,
  });
  expect(score).toBe(4.5);
});

test("scoreCatBoostModel sums multiple trees when first feature crosses second border", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  const score = scoreCatBoostModel({
    features: [
      { isMissing: false, value: 3 },
      { isMissing: false, value: 2 },
    ],
    model,
  });
  expect(score).toBe(3.5);
});

test("scoreCatBoostModel applies scale and bias", () => {
  const model = parseCatBoostJsonModel(SCALED_MODEL_JSON);
  const score = scoreCatBoostModel({
    features: [
      { isMissing: false, value: 1 },
      { isMissing: false, value: 2 },
    ],
    model,
  });
  expect(score).toBe(19);
});

test("scoreCatBoostModel treats missing AsFalse feature as the low branch", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  const singleTree: CatBoostModel = {
    bias: model.bias,
    categoricalFeatures: model.categoricalFeatures,
    floatFeatures: model.floatFeatures,
    scale: model.scale,
    trees: [model.trees[0]!],
  };
  const score = scoreCatBoostModel({
    features: [
      { isMissing: false, value: 0 },
      { isMissing: true, value: 0 },
    ],
    model: singleTree,
  });
  expect(score).toBe(1);
});

test("scoreCatBoostModel treats NaN value as missing on the low branch", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  const singleTree: CatBoostModel = {
    bias: model.bias,
    categoricalFeatures: model.categoricalFeatures,
    floatFeatures: model.floatFeatures,
    scale: model.scale,
    trees: [model.trees[0]!],
  };
  const score = scoreCatBoostModel({
    features: [
      { isMissing: false, value: Number.NaN },
      { isMissing: false, value: 2 },
    ],
    model: singleTree,
  });
  expect(score).toBe(3);
});

test("scoreCatBoostModel routes AsTrue missing feature to the high branch", () => {
  const model = parseCatBoostJsonModel(AS_TRUE_MODEL_JSON);
  const scoreHigh = scoreCatBoostModel({
    features: [
      { isMissing: true, value: 0 },
      { isMissing: false, value: 2 },
    ],
    model,
  });
  const scoreLow = scoreCatBoostModel({
    features: [
      { isMissing: true, value: 0 },
      { isMissing: false, value: 0 },
    ],
    model,
  });
  expect(scoreHigh).toBe(4);
  expect(scoreLow).toBe(2);
});

test("scoreCatBoostModel matches a one-hot categorical split", () => {
  const model = parseCatBoostJsonModel(CATEGORICAL_MODEL_JSON);
  const matchSeven = scoreCatBoostModel({ features: [{ isMissing: false, value: 7 }], model });
  const matchNine = scoreCatBoostModel({ features: [{ isMissing: false, value: 9 }], model });
  const noMatch = scoreCatBoostModel({ features: [{ isMissing: false, value: 3 }], model });
  expect(matchSeven).toBe(20);
  expect(matchNine).toBe(20);
  expect(noMatch).toBe(10);
});

test("scoreCatBoostModel treats a missing categorical value as no match", () => {
  const model = parseCatBoostJsonModel(CATEGORICAL_MODEL_JSON);
  const score = scoreCatBoostModel({ features: [{ isMissing: true, value: 7 }], model });
  expect(score).toBe(10);
});

test("scoreCatBoostModel throws when a feature cell is missing from the row", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  expect(() => scoreCatBoostModel({ features: [{ isMissing: false, value: 1 }], model })).toThrow(
    "CatBoost scoring: missing feature cell at index 1",
  );
});

test("scoreCatBoostRanking sorts horses by descending score with predicted ranks", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  const ranked = scoreCatBoostRanking({
    model,
    rows: [
      {
        features: [
          { isMissing: false, value: 0 },
          { isMissing: false, value: 0 },
        ],
        id: "low",
      },
      {
        features: [
          { isMissing: false, value: 1 },
          { isMissing: false, value: 2 },
        ],
        id: "high",
      },
      {
        features: [
          { isMissing: false, value: 3 },
          { isMissing: false, value: 2 },
        ],
        id: "mid",
      },
    ],
  });
  expect(ranked).toStrictEqual([
    { id: "high", predictedRank: 1, score: 4.5 },
    { id: "mid", predictedRank: 2, score: 3.5 },
    { id: "low", predictedRank: 3, score: 1.5 },
  ]);
});

test("parseCatBoostJsonModel defaults nan_value_treatment to AsIs when absent", () => {
  const model = parseCatBoostJsonModel({
    features_info: {
      float_features: [{ feature_index: 0, flat_feature_index: 0, has_nans: false }],
    },
    oblivious_trees: [],
  });
  expect(model.floatFeatures[0]?.nanTreatment).toBe("AsIs");
  expect(model.floatFeatures[0]?.hasNans).toBe(false);
});

test("parseCatBoostJsonModel defaults feature lists to empty when features_info omits them", () => {
  const model = parseCatBoostJsonModel({ features_info: {}, oblivious_trees: [] });
  expect(model.floatFeatures).toStrictEqual([]);
  expect(model.categoricalFeatures).toStrictEqual([]);
});

test("parseCatBoostJsonModel reads a one-hot split that omits the values key", () => {
  const model = parseCatBoostJsonModel({
    features_info: {
      categorical_features: [{ feature_index: 0, flat_feature_index: 0 }],
      float_features: [],
    },
    oblivious_trees: [
      {
        leaf_values: [1, 2],
        splits: [{ cat_feature_index: 0, split_index: 5, split_type: "OneHotFeature" }],
      },
    ],
  });
  expect(model.trees[0]?.splits[0]?.oneHotValues).toStrictEqual([]);
});

test("scoreCatBoostModel throws when a tree references an out-of-range leaf index", () => {
  const model = parseCatBoostJsonModel({
    features_info: {
      float_features: [{ feature_index: 0, flat_feature_index: 0, nan_value_treatment: "AsIs" }],
    },
    oblivious_trees: [
      {
        leaf_values: [1, 2],
        splits: [
          { border: 0.5, float_feature_index: 0, split_index: 1, split_type: "FloatFeature" },
          { border: 0.5, float_feature_index: 0, split_index: 2, split_type: "FloatFeature" },
        ],
      },
    ],
  });
  expect(() => scoreCatBoostModel({ features: [{ isMissing: false, value: 1 }], model })).toThrow(
    "CatBoost scoring: leaf index 3 out of range",
  );
});

test("scoreCatBoostRanking keeps input order for tied scores", () => {
  const model = parseCatBoostJsonModel(FLOAT_MODEL_JSON);
  const ranked = scoreCatBoostRanking({
    model,
    rows: [
      {
        features: [
          { isMissing: false, value: 1 },
          { isMissing: false, value: 2 },
        ],
        id: "first",
      },
      {
        features: [
          { isMissing: false, value: 1 },
          { isMissing: false, value: 2 },
        ],
        id: "second",
      },
    ],
  });
  expect(ranked).toStrictEqual([
    { id: "first", predictedRank: 1, score: 4.5 },
    { id: "second", predictedRank: 2, score: 4.5 },
  ]);
});
