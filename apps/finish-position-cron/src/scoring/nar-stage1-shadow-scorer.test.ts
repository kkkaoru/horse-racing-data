// Run with bun run test. No network, storage or prediction writes.
import { expect, test } from "vitest";
import type { XgboostModel } from "xgboost-json-tree";

import {
  scoreNarStage1Shadow,
  scoreNarStage1ProphetShadow,
  type NarStage1ShadowInput,
} from "./nar-stage1-shadow-scorer";
import type { FeatureEntry } from "./feature-projection";

const FEATURE_NAMES: string[] = Array.from({ length: 184 }, (_, index) => `f${index}`);
const MODEL: XgboostModel = {
  base_score: 0.25,
  feature_names: [],
  num_feature: 184,
  trees: [
    {
      base_weights: [0, 1.5, -2],
      default_left: [1, 0, 0],
      left_children: [1, -1, -1],
      right_children: [2, -1, -1],
      split_conditions: [Math.fround(0.6666667), 1.5, -2],
      split_indices: [0, 0, 0],
    },
  ],
};
const ENTRY: FeatureEntry = {
  ...Object.fromEntries(FEATURE_NAMES.map((name) => [name, null])),
  race_id: "nar:2026:0930:48:01",
  ketto_toroku_bango: "2023100624",
  umaban: "04",
};
const INPUT: NarStage1ShadowInput = {
  entries: [ENTRY],
  featureNames: FEATURE_NAMES,
  model: MODEL,
  modelVersion: "iter12-nar-xgb-hpo-v8-stage1-marketfree-184",
};

test("returns raw scores only, float32 projected in metadata order", () => {
  expect(
    scoreNarStage1Shadow({ ...INPUT, entries: [{ ...ENTRY, f0: 0.6666666865348816 }] }),
  ).toStrictEqual({
    modelVersion: "iter12-nar-xgb-hpo-v8-stage1-marketfree-184",
    predictions: [{ kettoTorokuBango: "2023100624", rawScore: -1.75, umaban: 4 }],
    raceId: "nar:2026:0930:48:01",
    shadowOnly: true,
  });
});
test("accepts null features as missing without claiming rank or final scores", () => {
  expect(scoreNarStage1Shadow(INPUT).predictions).toStrictEqual([
    { kettoTorokuBango: "2023100624", rawScore: 1.75, umaban: 4 },
  ]);
});
test("routes a null feature down the tree's default direction, not as zero", () => {
  const rightByDefault: XgboostModel = {
    ...MODEL,
    trees: [{ ...MODEL.trees[0]!, default_left: [0, 0, 0] }],
  };
  expect(scoreNarStage1Shadow({ ...INPUT, model: rightByDefault }).predictions).toStrictEqual([
    { kettoTorokuBango: "2023100624", rawScore: -1.75, umaban: 4 },
  ]);
});
test("validates the booster trained feature order when present", () => {
  expect(
    scoreNarStage1Shadow({ ...INPUT, model: { ...MODEL, feature_names: [...FEATURE_NAMES] } })
      .predictions,
  ).toStrictEqual([{ kettoTorokuBango: "2023100624", rawScore: 1.75, umaban: 4 }]);
});

test("rejects a different model version", () => {
  expect(() => scoreNarStage1Shadow({ ...INPUT, modelVersion: "other" })).toThrow(
    "unexpected model version",
  );
});
test.each([
  { featureNames: ["f0"] },
  { model: { ...MODEL, num_feature: 183 } },
  { model: { ...MODEL, feature_names: ["wrong"] } },
  {
    model: {
      ...MODEL,
      feature_names: FEATURE_NAMES.map((name, index) => (index === 0 ? "wrong" : name)),
    },
  },
  { featureNames: FEATURE_NAMES.map((name, index) => (index === 0 ? " " : name)) },
  { featureNames: FEATURE_NAMES.map(() => "f0") },
])("rejects invalid model metadata %j", (override) => {
  expect(() => scoreNarStage1Shadow({ ...INPUT, ...override })).toThrow(
    "invalid ordered feature metadata",
  );
});
test("rejects empty input", () => {
  expect(() => scoreNarStage1Shadow({ ...INPUT, entries: [] })).toThrow("empty race");
});
test.each([undefined, {}, "  "])("rejects missing horse identity %j", (value) => {
  expect(() =>
    scoreNarStage1Shadow({ ...INPUT, entries: [{ ...ENTRY, ketto_toroku_bango: value }] }),
  ).toThrow("missing ketto_toroku_bango");
});
test("accepts numeric identity", () => {
  expect(
    scoreNarStage1Shadow({ ...INPUT, entries: [{ ...ENTRY, ketto_toroku_bango: 123 }] })
      .predictions,
  ).toStrictEqual([{ kettoTorokuBango: "123", rawScore: 1.75, umaban: 4 }]);
});
test.each(["0", "-1", "1.5", "NaN"])("rejects invalid horse number %s", (value) => {
  expect(() => scoreNarStage1Shadow({ ...INPUT, entries: [{ ...ENTRY, umaban: value }] })).toThrow(
    "invalid horse number",
  );
});
test("rejects mixed races", () => {
  expect(() =>
    scoreNarStage1Shadow({
      ...INPUT,
      entries: [ENTRY, { ...ENTRY, race_id: "nar:2026:0930:48:02" }],
    }),
  ).toThrow("mixed or invalid race identity");
});
test("rejects non NAR races", () => {
  expect(() =>
    scoreNarStage1Shadow({ ...INPUT, entries: [{ ...ENTRY, race_id: "jra:2026:0930:06:01" }] }),
  ).toThrow("mixed or invalid race identity");
});
test("rejects missing model columns rather than silently median filling", () => {
  const entry = { ...ENTRY };
  delete entry.f0;
  expect(() => scoreNarStage1Shadow({ ...INPUT, entries: [entry] })).toThrow(
    "missing model feature f0",
  );
});
test("rejects non-finite features", () => {
  expect(() => scoreNarStage1Shadow({ ...INPUT, entries: [{ ...ENTRY, f0: "bad" }] })).toThrow(
    "non-finite model feature f0",
  );
});
test("rejects finite double values that overflow float32", () => {
  expect(() => scoreNarStage1Shadow({ ...INPUT, entries: [{ ...ENTRY, f0: 1e100 }] })).toThrow(
    "float32 feature overflow",
  );
});
test("rejects non-finite score", () => {
  expect(() =>
    scoreNarStage1Shadow({ ...INPUT, model: { ...MODEL, base_score: Infinity } }),
  ).toThrow("non-finite score");
});
test("rejects duplicate horse IDs", () => {
  expect(() =>
    scoreNarStage1Shadow({ ...INPUT, entries: [ENTRY, { ...ENTRY, umaban: 5 }] }),
  ).toThrow("duplicate horse identity");
});
test("scopes Prophet output to the explicitly observed Stage1 signature", () => {
  const result = scoreNarStage1ProphetShadow({
    ...INPUT,
    servedSignature:
      "v1;mode=nar-transformer-blend;stage2=transformer-fused;stage2-model=iter40-nar-settransformer-blend-v1-nowin1;stage1=odds-missing:direct-stage1;final=iter12-nar-xgb-hpo-v8-stage1-marketfree-184",
    entries: [
      { ...ENTRY, ketto_toroku_bango: "b", umaban: 2 },
      { ...ENTRY, ketto_toroku_bango: "a", umaban: 1 },
      { ...ENTRY, ketto_toroku_bango: "c", umaban: 3, f0: 1 },
    ],
  });
  expect(result.scope).toBe("stage1-plus-prophet-only");
  expect(result.shadowOnly).toBe(true);
  expect(result.predictions).toStrictEqual([
    { kettoTorokuBango: "a", umaban: 1, predictedRank: 1, predictedScore: 1.75 },
    { kettoTorokuBango: "b", umaban: 2, predictedRank: 2, predictedScore: 1.75 },
    { kettoTorokuBango: "c", umaban: 3, predictedRank: 3, predictedScore: -1.75 },
  ]);
});
test("applies the shared Prophet adjustment to NAR raw scores with explicit config", () => {
  const result = scoreNarStage1ProphetShadow({
    ...INPUT,
    servedSignature:
      "v1;mode=nar-transformer-blend;stage2=transformer-fused;stage2-model=iter40-nar-settransformer-blend-v1-nowin1;stage1=odds-missing:direct-stage1;final=iter12-nar-xgb-hpo-v8-stage1-marketfree-184",
    prophetConfig: { weight: "0.5" },
    entries: [
      {
        ...ENTRY,
        ketto_toroku_bango: "a",
        umaban: 1,
        prophet_entity_coverage: 1,
        prophet_entity_performance_mean: 0,
      },
      {
        ...ENTRY,
        ketto_toroku_bango: "b",
        umaban: 2,
        f0: 1,
        prophet_entity_coverage: 1,
        prophet_entity_performance_mean: 2,
      },
    ],
  });
  expect(result.predictions).toStrictEqual([
    { kettoTorokuBango: "a", umaban: 1, predictedRank: 1, predictedScore: 0.875 },
    { kettoTorokuBango: "b", umaban: 2, predictedRank: 2, predictedScore: -0.875 },
  ]);
});

test("does not silently infer a served signature from raw scores", () => {
  expect(() => scoreNarStage1ProphetShadow({ ...INPUT, servedSignature: "other" })).toThrow(
    "unsupported served signature",
  );
});

test("rejects duplicate horse numbers", () => {
  expect(() =>
    scoreNarStage1Shadow({ ...INPUT, entries: [ENTRY, { ...ENTRY, ketto_toroku_bango: "123" }] }),
  ).toThrow("duplicate horse identity");
});
