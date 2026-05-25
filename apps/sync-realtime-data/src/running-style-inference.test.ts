// Run with bun test apps/sync-realtime-data/src/running-style-inference.test.ts
import { expect, test, vi } from "vitest";

import {
  runRunningStyleInference,
  runRunningStyleInferenceForRows,
  runRunningStyleInferenceForRowsWithFlatModel,
  runRunningStyleInferenceRowsWithFlatModel,
} from "./running-style-inference";
import type { CompactLightGBMModel } from "./running-style-lightgbm-tree";
import type { RaceHorseFeatureRow } from "./running-style-r2";

const TEST_MODEL: CompactLightGBMModel = {
  categorical_features: [],
  class_labels: ["nige", "senkou", "sashi", "oikomi"],
  feature_names: ["past_nige_rate_self", "field_nige_pressure"],
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
};

const HORSE_ROW_1: RaceHorseFeatureRow = {
  bamei: "ホースエース",
  category: "nar",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0518",
  keibajoCode: "44",
  kettoTorokuBango: "2023101309",
  perHorseFeatures: { past_nige_rate_self: 0.5 },
  peerInputs: {
    careerWinRate: 0.1,
    kohan3fAvg5: 36.0,
    pastCorner1NormAvg5: 0.3,
    pastFirst3fAvg5: 35.5,
    pastNigeRate: 0.5,
    pastOikomiRate: 0.0,
    pastSashiRate: 0.0,
    pastSenkouRate: 0.5,
    speedIndexAvg5: 60,
    speedIndexBest5: 70,
  },
  raceBango: "01",
  raceKey: "nar:20260518:44:01",
  source: "nar",
  umaban: 1,
};

const HORSE_ROW_2: RaceHorseFeatureRow = {
  ...HORSE_ROW_1,
  bamei: "ホースビー",
  kettoTorokuBango: "2023100508",
  perHorseFeatures: { past_nige_rate_self: 0.1 },
  peerInputs: { ...HORSE_ROW_1.peerInputs, pastNigeRate: 0.1, speedIndexBest5: 65 },
  umaban: 2,
};

const makeJsonlBody = (rows: ReadonlyArray<RaceHorseFeatureRow>): ReadableStream<Uint8Array> => {
  const text = rows.map((row) => JSON.stringify(row)).join("\n");
  return new Response(text).body as ReadableStream<Uint8Array>;
};

const makeJsonBody = (payload: unknown): ReadableStream<Uint8Array> =>
  new Response(JSON.stringify(payload)).body as ReadableStream<Uint8Array>;

const buildMockBucket = (
  modelKey: string,
  featuresKey: string,
  rows: ReadonlyArray<RaceHorseFeatureRow>,
): R2Bucket => {
  const head = vi.fn(async (key: string) => ({ etag: `etag-${key}-${Math.random()}` }));
  const get = vi.fn(async (key: string) => {
    if (key === modelKey) return { body: makeJsonBody(TEST_MODEL) };
    if (key === featuresKey) return { body: makeJsonlBody(rows) };
    return null;
  });
  return { get, head } as unknown as R2Bucket;
};

const buildMockD1 = (): { db: D1Database; calls: unknown[][] } => {
  const calls: unknown[][] = [];
  const prepare = vi.fn(() => ({
    bind: (...args: unknown[]) => {
      calls.push(args);
      return { args } as unknown;
    },
  }));
  const batch = vi.fn(async () => []);
  const db = { batch, prepare } as unknown as D1Database;
  return { calls, db };
};

test("runRunningStyleInference reports horseCount equal to feature row count", async () => {
  const bucket = buildMockBucket("model/key", "features/key", [HORSE_ROW_1, HORSE_ROW_2]);
  const { db } = buildMockD1();
  const summary = await runRunningStyleInference(bucket, db, {
    featuresKey: "features/key",
    modelKey: "model/key",
    predictedAt: "2026-05-18T10:00:00Z",
  });
  expect(summary.horseCount).toBe(2);
});

test("runRunningStyleInference groups rows into one race", async () => {
  const bucket = buildMockBucket("model/key", "features/key", [HORSE_ROW_1, HORSE_ROW_2]);
  const { db } = buildMockD1();
  const summary = await runRunningStyleInference(bucket, db, {
    featuresKey: "features/key",
    modelKey: "model/key",
    predictedAt: "2026-05-18T10:00:00Z",
  });
  expect(summary.raceCount).toBe(1);
});

test("runRunningStyleInference writes one row per horse", async () => {
  const bucket = buildMockBucket("model/key", "features/key", [HORSE_ROW_1, HORSE_ROW_2]);
  const { db } = buildMockD1();
  const summary = await runRunningStyleInference(bucket, db, {
    featuresKey: "features/key",
    modelKey: "model/key",
    predictedAt: "2026-05-18T10:00:00Z",
  });
  expect(summary.writtenCount).toBe(2);
});

test("runRunningStyleInference reports model_version from loaded model", async () => {
  const bucket = buildMockBucket("model/key", "features/key", [HORSE_ROW_1]);
  const { db } = buildMockD1();
  const summary = await runRunningStyleInference(bucket, db, {
    featuresKey: "features/key",
    modelKey: "model/key",
    predictedAt: "2026-05-18T10:00:00Z",
  });
  expect(summary.modelVersion).toBe("test-v0");
});

test("runRunningStyleInference binds race_key as first arg in D1 insert", async () => {
  const bucket = buildMockBucket("model/key", "features/key", [HORSE_ROW_1]);
  const { calls, db } = buildMockD1();
  await runRunningStyleInference(bucket, db, {
    featuresKey: "features/key",
    modelKey: "model/key",
    predictedAt: "2026-05-18T10:00:00Z",
  });
  expect(calls[0]?.[0]).toBe("nar:20260518:44:01");
});

test("runRunningStyleInference predicts nige for class-0 dominated leaves", async () => {
  const bucket = buildMockBucket("model/key", "features/key", [HORSE_ROW_1]);
  const { calls, db } = buildMockD1();
  await runRunningStyleInference(bucket, db, {
    featuresKey: "features/key",
    modelKey: "model/key",
    predictedAt: "2026-05-18T10:00:00Z",
  });
  expect(calls[0]?.[11]).toBe("nige");
});

test("runRunningStyleInferenceForRows skips the features fetch and consumes provided rows", async () => {
  const bucket = buildMockBucket("model/key", "features/key", []);
  const { calls, db } = buildMockD1();
  const summary = await runRunningStyleInferenceForRows(bucket, db, {
    modelKey: "model/key",
    predictedAt: "2026-05-18T10:00:00Z",
    rows: [HORSE_ROW_1, HORSE_ROW_2],
  });
  expect(summary.horseCount).toBe(2);
  expect(summary.raceCount).toBe(1);
  expect(calls.length).toBe(2);
});

const FLAT_MAGIC = "RSLGBM1\0";
const FLAT_NODE_BYTES = 40;

interface FlatNodeSpec {
  kind: 0;
  leafValue: number;
}

const buildFlatBuffer = (leafValues: FlatNodeSpec[]): ArrayBuffer => {
  const header = {
    categorical_features: [],
    categorical_value_count: 0,
    class_labels: ["nige", "senkou", "sashi", "oikomi"],
    feature_names: ["past_nige_rate_self"],
    format: "rs-lgbm-flat-v1",
    model_version: "flat-v1",
    node_count: leafValues.length,
    num_class: 4,
    num_tree_per_iteration: 4,
    objective: "multiclass",
    tree_root_indices: leafValues.map((_, index) => index),
  };
  const headerBytes = new TextEncoder().encode(JSON.stringify(header));
  const magicBytes = new TextEncoder().encode(FLAT_MAGIC);
  const totalLength =
    magicBytes.byteLength + 4 + headerBytes.byteLength + leafValues.length * FLAT_NODE_BYTES;
  const buffer = new ArrayBuffer(totalLength);
  const view = new DataView(buffer);
  new Uint8Array(buffer).set(magicBytes, 0);
  view.setUint32(magicBytes.byteLength, headerBytes.byteLength, true);
  new Uint8Array(buffer).set(headerBytes, magicBytes.byteLength + 4);
  let cursor = magicBytes.byteLength + 4 + headerBytes.byteLength;
  leafValues.forEach((spec) => {
    view.setUint8(cursor, spec.kind);
    view.setFloat64(cursor + 32, spec.leafValue, true);
    cursor += FLAT_NODE_BYTES;
  });
  return buffer;
};

const buildMockFlatBucket = (key: string): R2Bucket => {
  const buffer = buildFlatBuffer([
    { kind: 0, leafValue: 1 },
    { kind: 0, leafValue: 0 },
    { kind: 0, leafValue: 0 },
    { kind: 0, leafValue: 0 },
  ]);
  const head = vi.fn(async () => ({ etag: "etag-flat" }));
  const get = vi.fn(async (requestedKey: string) =>
    requestedKey === key ? { arrayBuffer: async (): Promise<ArrayBuffer> => buffer } : null,
  );
  return { get, head } as unknown as R2Bucket;
};

test("runRunningStyleInferenceForRowsWithFlatModel loads model from R2 and runs inference", async () => {
  const bucket = buildMockFlatBucket("flat/key");
  const { db } = buildMockD1();
  const summary = await runRunningStyleInferenceForRowsWithFlatModel(bucket, db, {
    modelKey: "flat/key",
    predictedAt: "2026-05-18T10:00:00Z",
    rows: [HORSE_ROW_1, HORSE_ROW_2],
  });
  expect(summary.modelVersion).toBe("flat-v1");
  expect(summary.raceCount).toBe(1);
  expect(summary.horseCount).toBe(2);
});

test("runRunningStyleInferenceRowsWithFlatModel writes predictions when given a loaded flat model", async () => {
  const bucket = buildMockFlatBucket("flat/key");
  const { calls, db } = buildMockD1();
  const summary = await runRunningStyleInferenceForRowsWithFlatModel(bucket, db, {
    modelKey: "flat/key",
    predictedAt: "2026-05-18T10:00:00Z",
    rows: [HORSE_ROW_1],
  });
  expect(summary.writtenCount).toBe(1);
  expect(calls[0]?.[11]).toBe("nige");
});

test("runRunningStyleInferenceRowsWithFlatModel is also callable directly with a loaded model", async () => {
  const bucket = buildMockFlatBucket("flat/key");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const model = await loadFlatLightGBMModelFromR2(bucket, "flat/key");
  const { db } = buildMockD1();
  const summary = await runRunningStyleInferenceRowsWithFlatModel(db, {
    model,
    predictedAt: "2026-05-18T10:00:00Z",
    rows: [HORSE_ROW_1, HORSE_ROW_2],
  });
  expect(summary.raceCount).toBe(1);
});
