// Run with bun. Tests Ban-ei production-route shadow scoring and fail-closed artifacts.

import { parseCatBoostJsonModel } from "catboost-json-tree";
import { expect, test, vi } from "vitest";

import {
  BANEI_SHADOW_MODEL_SPECS,
  loadSelectedBaneiShadowModel,
  scoreBaneiRaceShadow,
  selectBaneiShadowModel,
  type LoadedBaneiShadowModel,
} from "./banei-shadow-scorer";
import type { FeatureEntry } from "./feature-projection";

const entry = (overrides: FeatureEntry = {}): FeatureEntry => ({
  grade_code: "A",
  ketto_toroku_bango: "HORSE-1",
  race_id: "nar:2026:0907:83:01",
  umaban: 1,
  x: 0,
  ...overrides,
});

const loadedModel = (): LoadedBaneiShadowModel => ({
  featureNames: ["x"],
  model: parseCatBoostJsonModel({
    features_info: {
      float_features: [
        {
          feature_index: 0,
          flat_feature_index: 0,
          has_nans: false,
          nan_value_treatment: "AsFalse",
        },
      ],
    },
    oblivious_trees: [
      {
        leaf_values: [-1, 2],
        splits: [{ border: 0.5, float_feature_index: 0, split_type: "FloatFeature" }],
      },
    ],
    scale_and_bias: [1, [0]],
  }),
  spec: BANEI_SHADOW_MODEL_SPECS.sim,
});

const jsonObject = (value: unknown): R2ObjectBody =>
  ({ json: vi.fn(async () => value) }) as unknown as R2ObjectBody;

const metadata = (overrides: Record<string, unknown> = {}): Record<string, unknown> => {
  const names = Array.from(
    { length: BANEI_SHADOW_MODEL_SPECS.sim.featureCount },
    (_value, index) => `f${index}`,
  );
  return {
    architecture: "catboost-yetirank",
    feature_count: names.length,
    feature_names: names,
    model_version: BANEI_SHADOW_MODEL_SPECS.sim.modelVersion,
    ...overrides,
  };
};

const emptyModelJson = {
  features_info: { float_features: [] },
  oblivious_trees: [],
  scale_and_bias: [1, [0]],
};

test("selects the base model only for grade E and otherwise the sim model", () => {
  expect(selectBaneiShadowModel([entry({ grade_code: "E" })])).toBe(BANEI_SHADOW_MODEL_SPECS.base);
  expect(selectBaneiShadowModel([entry({ grade_code: null })])).toBe(BANEI_SHADOW_MODEL_SPECS.sim);
  expect(selectBaneiShadowModel([entry({ grade_code: 1 })])).toBe(BANEI_SHADOW_MODEL_SPECS.sim);
});

test("scores and deterministically ranks a Ban-ei race", () => {
  const result = scoreBaneiRaceShadow(
    [
      entry({ ketto_toroku_bango: "HORSE-2", umaban: 2, x: 1 }),
      entry(),
      entry({ ketto_toroku_bango: "HORSE-3", umaban: 3, x: 1 }),
    ],
    loadedModel(),
  );

  expect(result).toMatchObject({
    gradeCode: "A",
    modelVersion: "banei-cb-v9-sim-2011",
    raceId: "nar:2026:0907:83:01",
    shadowOnly: true,
    variant: "sim",
  });
  expect(result.predictions).toStrictEqual([
    { kettoTorokuBango: "HORSE-2", predictedRank: 1, predictedScore: 2, umaban: 2 },
    { kettoTorokuBango: "HORSE-3", predictedRank: 2, predictedScore: 2, umaban: 3 },
    { kettoTorokuBango: "HORSE-1", predictedRank: 3, predictedScore: -1, umaban: 1 },
  ]);
});

test("breaks identical score and horse-id ties by ascending horse number", () => {
  const result = scoreBaneiRaceShadow(
    [
      entry({ ketto_toroku_bango: "SAME", umaban: 3, x: 1 }),
      entry({ ketto_toroku_bango: "SAME", umaban: 2, x: 1 }),
    ],
    loadedModel(),
  );
  expect(result.predictions.map((prediction) => prediction.umaban)).toStrictEqual([2, 3]);
});

test.each([
  [[], "race has no entries"],
  [[entry(), entry({ race_id: "nar:2026:0907:83:02" })], "race_id must be constant"],
  [[entry(), entry({ grade_code: "E" })], "grade_code must be constant"],
  [[entry({ ketto_toroku_bango: null })], "missing horse identity"],
  [[entry({ umaban: {} })], "missing horse identity"],
])("rejects invalid race rows %#", (entries, reason) => {
  expect(() => scoreBaneiRaceShadow(entries as FeatureEntry[], loadedModel())).toThrow(reason);
});

test("rejects missing and non-numeric model feature cells", () => {
  const loaded = loadedModel();
  expect(() => scoreBaneiRaceShadow([entry({ x: undefined })], loaded)).not.toThrow();
  expect(() => scoreBaneiRaceShadow([entry({ x: null })], loaded)).not.toThrow();
  expect(() => scoreBaneiRaceShadow([entry({ x: "NaN" })], loaded)).toThrow(
    "non-numeric model features: x",
  );
  const missing = entry();
  delete missing.x;
  expect(() => scoreBaneiRaceShadow([missing], loaded)).toThrow("missing model features: x");
});

test("loads exactly the selected model and attested metadata from R2", async () => {
  const get = vi.fn(async (key: string) =>
    jsonObject(key.endsWith("metadata.json") ? metadata() : emptyModelJson),
  );

  const bucket = { get } as unknown as R2Bucket;
  const result = await loadSelectedBaneiShadowModel(bucket, BANEI_SHADOW_MODEL_SPECS.sim);
  const cached = await loadSelectedBaneiShadowModel(bucket, BANEI_SHADOW_MODEL_SPECS.sim);

  expect(result.featureNames).toHaveLength(130);
  expect(cached).toBe(result);
  expect(result.model.trees).toStrictEqual([]);
  expect(get.mock.calls.map(([key]) => key)).toStrictEqual([
    "finish-position/ban-ei/banei-cb-v9-sim-2011/model.json",
    "finish-position/ban-ei/banei-cb-v9-sim-2011/metadata.json",
  ]);
});

test("fails closed when a selected R2 artifact is absent", async () => {
  const get = vi.fn(async () => null);
  await expect(
    loadSelectedBaneiShadowModel({ get } as unknown as R2Bucket, BANEI_SHADOW_MODEL_SPECS.sim),
  ).rejects.toThrow("R2 object not found");
});

test.each([
  [null, "must be an object"],
  [{}, "feature_names must be an array"],
  [metadata({ feature_names: ["x", ""] }), "must contain non-empty strings"],
  [metadata({ feature_names: Array(130).fill("x") }), "must be unique"],
  [metadata({ feature_count: 129 }), "feature count mismatch"],
  [metadata({ model_version: "wrong" }), "version mismatch"],
  [metadata({ architecture: "xgboost" }), "architecture mismatch"],
])("rejects invalid selected model metadata %#", async (invalidMetadata, reason) => {
  const get = vi.fn(async (key: string) =>
    jsonObject(key.endsWith("metadata.json") ? invalidMetadata : emptyModelJson),
  );
  await expect(
    loadSelectedBaneiShadowModel({ get } as unknown as R2Bucket, BANEI_SHADOW_MODEL_SPECS.sim),
  ).rejects.toThrow(reason);
});
