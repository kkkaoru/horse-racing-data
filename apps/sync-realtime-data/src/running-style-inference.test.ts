// Run with bun test apps/sync-realtime-data/src/running-style-inference.test.ts
import { expect, test, vi } from "vitest";

import { runRunningStyleInference } from "./running-style-inference";
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
