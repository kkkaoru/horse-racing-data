// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  buildRunningStyleFeatureParquetKey,
  deserializeRunningStyleFeatureParquet,
  loadRunningStyleFeatureParquet,
  metadataColumns,
  putRunningStyleFeatureParquet,
  runningStyleParquetVerificationKey,
  serializeRunningStyleFeatureParquet,
  validateFeatureCoverage,
} from "./running-style-feature-parquet";
import type { RaceHorseFeatureRow } from "./running-style-r2";

const FEATURE_NAMES = ["career_win_rate", "kohan3f_avg_5"];

const ROW: RaceHorseFeatureRow = {
  bamei: "サンプル",
  category: "jra",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0512",
  keibajoCode: "08",
  kettoTorokuBango: "2024100001",
  peerInputs: {
    careerWinRate: 0.2,
    kohan3fAvg5: 36.5,
    pastCorner1NormAvg5: 0.5,
    pastFirst3fAvg5: 12.5,
    pastNigeRate: 0,
    pastOikomiRate: 0,
    pastSashiRate: 0,
    pastSenkouRate: 1,
    speedIndexAvg5: 70,
    speedIndexBest5: 75,
  },
  perHorseFeatures: { career_win_rate: 0.2, kohan3f_avg_5: 36.5 },
  raceBango: "01",
  raceKey: "jra:20260512:08:01",
  source: "jra",
  umaban: 1,
};

afterEach(() => {
  vi.restoreAllMocks();
});

it("buildRunningStyleFeatureParquetKey builds the per-race parquet key", () => {
  expect(
    buildRunningStyleFeatureParquetKey({
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    }),
  ).toBe("running-style/features-parquet/jra/20260512/jra:20260512:08:01.parquet");
});

it("runningStyleParquetVerificationKey builds the verification key", () => {
  expect(runningStyleParquetVerificationKey("jra", "20260512", "key1")).toBe(
    "running-style/verification/features-parquet/jra/20260512/key1.parquet",
  );
});

it("metadataColumns exposes the expected fixed column list", () => {
  expect(metadataColumns).toStrictEqual([
    "raceKey",
    "source",
    "kaisaiNen",
    "kaisaiTsukihi",
    "keibajoCode",
    "raceBango",
    "category",
    "kettoTorokuBango",
    "bamei",
  ]);
});

it("validateFeatureCoverage reports missingFeatureNames when every row lacks the feature", () => {
  const result = validateFeatureCoverage([ROW], ["career_win_rate", "absent_feature"]);
  expect(result.missingFeatureNames).toStrictEqual(["absent_feature"]);
  expect(result.missingCells).toBe(1);
});

it("validateFeatureCoverage returns zero counts when all features present", () => {
  const result = validateFeatureCoverage([ROW], FEATURE_NAMES);
  expect(result).toStrictEqual({ missingCells: 0, missingFeatureNames: [] });
});

it("serializeRunningStyleFeatureParquet + deserializeRunningStyleFeatureParquet roundtrip", async () => {
  const bytes = await serializeRunningStyleFeatureParquet([ROW], FEATURE_NAMES);
  const buffer = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(buffer).set(bytes);
  const rows = await deserializeRunningStyleFeatureParquet(buffer, FEATURE_NAMES);
  expect(rows.length).toBe(1);
  expect(rows[0]!.raceKey).toBe("jra:20260512:08:01");
  expect(rows[0]!.umaban).toBe(1);
  expect(rows[0]!.perHorseFeatures.career_win_rate).toBe(0.2);
});

it("putRunningStyleFeatureParquet writes Parquet bytes to R2 with the right contentType", async () => {
  const put = vi.fn(async (_key: string, _body: Uint8Array, _options?: unknown) => {});
  const bucket = { put } as unknown as R2Bucket;
  const bytesWritten = await putRunningStyleFeatureParquet(bucket, "key1", [ROW], FEATURE_NAMES);
  expect(bytesWritten).toBeGreaterThan(0);
  expect(put).toHaveBeenCalledTimes(1);
  expect(put.mock.calls[0]![0]).toBe("key1");
  const options = put.mock.calls[0]![2] as { httpMetadata: { contentType: string } };
  expect(options.httpMetadata.contentType).toBe("application/vnd.apache.parquet");
});

it("loadRunningStyleFeatureParquet throws when R2 object is missing", async () => {
  const get = vi.fn(async () => null);
  const bucket = { get } as unknown as R2Bucket;
  await expect(loadRunningStyleFeatureParquet(bucket, "missing", FEATURE_NAMES)).rejects.toThrow(
    "R2 object not found: missing",
  );
});

it("deserializeRunningStyleFeatureParquet treats null/empty bamei as null and rounds invalid umaban to 0", async () => {
  const bytes = await serializeRunningStyleFeatureParquet(
    [{ ...ROW, bamei: null, perHorseFeatures: {} }],
    FEATURE_NAMES,
  );
  const buffer = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(buffer).set(bytes);
  const rows = await deserializeRunningStyleFeatureParquet(buffer, FEATURE_NAMES);
  expect(rows[0]?.bamei).toBeNull();
  expect(rows[0]?.perHorseFeatures.career_win_rate).toBeNull();
});

it("loadRunningStyleFeatureParquet deserializes the R2 buffer", async () => {
  const bytes = await serializeRunningStyleFeatureParquet([ROW], FEATURE_NAMES);
  const buffer = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(buffer).set(bytes);
  const get = vi.fn(async () => ({
    arrayBuffer: async (): Promise<ArrayBuffer> => buffer,
  }));
  const bucket = { get } as unknown as R2Bucket;
  const rows = await loadRunningStyleFeatureParquet(bucket, "key1", FEATURE_NAMES);
  expect(rows.length).toBe(1);
  expect(rows[0]!.raceKey).toBe("jra:20260512:08:01");
});
