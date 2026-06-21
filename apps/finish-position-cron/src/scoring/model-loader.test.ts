// Run with: bun run --filter finish-position-cron test
import { expect, test, vi } from "vitest";
import cbSmall from "./__fixtures__/cb-small.json";
import xgbSmall from "./__fixtures__/xgb-small.json";
import {
  buildModelKey,
  JRA_CB_MODEL_VERSION,
  JRA_ETOP2_MODEL_VERSION,
  JRA_XGB_MODEL_VERSION,
  loadJraModels,
} from "./model-loader";

const FEATURE_NAMES = ["umaban", "kyori", "weight_avg_5"];

interface JsonObject {
  json: () => Promise<unknown>;
}

const jsonObject = (body: unknown): JsonObject => ({ json: async () => body });

const makeBucket = (getImpl: (key: string) => Promise<unknown>): R2Bucket =>
  ({ get: vi.fn(getImpl) }) as unknown as R2Bucket;

test("buildModelKey mirrors the container R2 object-key layout", () => {
  expect(buildModelKey("jra", "iter20-jra-cb-2013-v8", "model.json")).toBe(
    "finish-position/jra/iter20-jra-cb-2013-v8/model.json",
  );
});

test("exposes the JRA CB / XGB / E-top2 model-version constants", () => {
  expect(JRA_CB_MODEL_VERSION).toBe("iter20-jra-cb-2013-v8");
  expect(JRA_XGB_MODEL_VERSION).toBe("xgb-jra-2013-v8");
  expect(JRA_ETOP2_MODEL_VERSION).toBe("iter22-jra-etop2");
});

test("loadJraModels reads CB, XGB and feature_names from the three R2 keys", async () => {
  const responses = new Map<string, unknown>([
    ["finish-position/jra/iter20-jra-cb-2013-v8/model.json", cbSmall],
    ["finish-position/jra/xgb-jra-2013-v8/model.json", xgbSmall],
    ["finish-position/jra/iter20-jra-cb-2013-v8/metadata.json", { feature_names: FEATURE_NAMES }],
  ]);
  const bucket = makeBucket(async (key) => {
    const body = responses.get(key);
    return body === undefined ? null : jsonObject(body);
  });
  const models = await loadJraModels(bucket);
  expect(models.featureNames).toStrictEqual(["umaban", "kyori", "weight_avg_5"]);
  expect(models.catboostModel.trees.length).toBeGreaterThan(0);
  expect(models.xgboostModel.trees.length).toBeGreaterThan(0);
});

test("loadJraModels throws when the CatBoost object is absent", async () => {
  const bucket = makeBucket(async (key) =>
    key === "finish-position/jra/iter20-jra-cb-2013-v8/model.json"
      ? null
      : jsonObject({ feature_names: FEATURE_NAMES }),
  );
  await expect(loadJraModels(bucket)).rejects.toThrow(
    "R2 object not found: finish-position/jra/iter20-jra-cb-2013-v8/model.json",
  );
});

test("loadJraModels throws when metadata feature_names is not a string array", async () => {
  const responses = new Map<string, unknown>([
    ["finish-position/jra/iter20-jra-cb-2013-v8/model.json", cbSmall],
    ["finish-position/jra/xgb-jra-2013-v8/model.json", xgbSmall],
    ["finish-position/jra/iter20-jra-cb-2013-v8/metadata.json", { feature_names: [1, 2, 3] }],
  ]);
  const bucket = makeBucket(async (key) => {
    const body = responses.get(key);
    return body === undefined ? null : jsonObject(body);
  });
  await expect(loadJraModels(bucket)).rejects.toThrow(
    "metadata.json feature_names must be a string array",
  );
});

test("loadJraModels throws when metadata is not an object", async () => {
  const responses = new Map<string, unknown>([
    ["finish-position/jra/iter20-jra-cb-2013-v8/model.json", cbSmall],
    ["finish-position/jra/xgb-jra-2013-v8/model.json", xgbSmall],
    ["finish-position/jra/iter20-jra-cb-2013-v8/metadata.json", "not-an-object"],
  ]);
  const bucket = makeBucket(async (key) => {
    const body = responses.get(key);
    return body === undefined ? null : jsonObject(body);
  });
  await expect(loadJraModels(bucket)).rejects.toThrow("metadata.json must be an object");
});
