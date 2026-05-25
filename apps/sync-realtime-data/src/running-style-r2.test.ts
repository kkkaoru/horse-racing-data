// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  clearRunningStyleCaches,
  loadFeaturesFromR2,
  loadLightGBMModelFromR2,
} from "./running-style-r2";

const MODEL_BODY = JSON.stringify({
  categorical_features: [],
  class_labels: ["nige", "senkou", "sashi", "oikomi"],
  feature_names: ["x"],
  model_version: "test",
  num_class: 4,
  num_tree_per_iteration: 4,
  objective: "multiclass",
  trees: [],
});
const FEATURE_ROWS_TEXT = '{"raceKey":"a"}\n{"raceKey":"b"}\n';

afterEach(() => {
  vi.restoreAllMocks();
  clearRunningStyleCaches();
});

it("loadLightGBMModelFromR2 throws when head returns null", async () => {
  const head = vi.fn(async () => null);
  const get = vi.fn();
  const bucket = { get, head } as unknown as R2Bucket;
  await expect(loadLightGBMModelFromR2(bucket, "missing")).rejects.toThrow(
    "R2 object not found: missing",
  );
});

it("loadLightGBMModelFromR2 throws when get returns null after head ok", async () => {
  const head = vi.fn(async () => ({ etag: "etag1" }));
  const get = vi.fn(async () => null);
  const bucket = { get, head } as unknown as R2Bucket;
  await expect(loadLightGBMModelFromR2(bucket, "key")).rejects.toThrow("R2 object not found: key");
});

it("loadLightGBMModelFromR2 fetches and parses body on first call", async () => {
  const head = vi.fn(async () => ({ etag: "etag1" }));
  const get = vi.fn(async () => ({ body: new Response(MODEL_BODY).body }));
  const bucket = { get, head } as unknown as R2Bucket;
  const model = await loadLightGBMModelFromR2(bucket, "key1");
  expect(model.class_labels).toStrictEqual(["nige", "senkou", "sashi", "oikomi"]);
  expect(get).toHaveBeenCalledTimes(1);
});

it("loadLightGBMModelFromR2 returns cached payload when etag matches", async () => {
  const head = vi.fn(async () => ({ etag: "etag-cached" }));
  const get = vi.fn(async () => ({ body: new Response(MODEL_BODY).body }));
  const bucket = { get, head } as unknown as R2Bucket;
  await loadLightGBMModelFromR2(bucket, "key-cache");
  await loadLightGBMModelFromR2(bucket, "key-cache");
  expect(get).toHaveBeenCalledTimes(1);
});

it("loadFeaturesFromR2 throws when head returns null", async () => {
  const head = vi.fn(async () => null);
  const get = vi.fn();
  const bucket = { get, head } as unknown as R2Bucket;
  await expect(loadFeaturesFromR2(bucket, "missing")).rejects.toThrow(
    "R2 object not found: missing",
  );
});

it("loadFeaturesFromR2 throws when get returns null", async () => {
  const head = vi.fn(async () => ({ etag: "etag1" }));
  const get = vi.fn(async () => null);
  const bucket = { get, head } as unknown as R2Bucket;
  await expect(loadFeaturesFromR2(bucket, "key")).rejects.toThrow("R2 object not found: key");
});

it("loadFeaturesFromR2 parses JSONL body and caches by etag", async () => {
  const head = vi.fn(async () => ({ etag: "etag-features" }));
  const get = vi.fn(async () => ({ body: new Response(FEATURE_ROWS_TEXT).body }));
  const bucket = { get, head } as unknown as R2Bucket;
  const rows = await loadFeaturesFromR2(bucket, "feat1");
  expect(rows.length).toBe(2);
  expect(rows[0]!.raceKey).toBe("a");
  await loadFeaturesFromR2(bucket, "feat1");
  expect(get).toHaveBeenCalledTimes(1);
});

it("loadLightGBMModelFromR2 throws when body is null after a valid head", async () => {
  const head = vi.fn(async () => ({ etag: "etag1" }));
  const get = vi.fn(async () => ({ body: null }));
  const bucket = { get, head } as unknown as R2Bucket;
  await expect(loadLightGBMModelFromR2(bucket, "no-body")).rejects.toThrow(
    "R2 object body missing",
  );
});

it("clearRunningStyleCaches forces a fresh fetch on the next call", async () => {
  const head = vi.fn(async () => ({ etag: "etag1" }));
  const get = vi.fn(async () => ({ body: new Response(MODEL_BODY).body }));
  const bucket = { get, head } as unknown as R2Bucket;
  await loadLightGBMModelFromR2(bucket, "key");
  clearRunningStyleCaches();
  await loadLightGBMModelFromR2(bucket, "key");
  expect(get).toHaveBeenCalledTimes(2);
});
