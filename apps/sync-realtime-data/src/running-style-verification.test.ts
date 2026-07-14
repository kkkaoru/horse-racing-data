// run with: bun run test
import { beforeEach, expect, it, vi } from "vitest";
import type { RaceHorseFeatureRow } from "./running-style-r2";
import type { Env } from "./types";

vi.mock("./running-style-catalog-client", () => ({
  fetchRunningStyleFeaturesFromCatalog: vi.fn(),
  RUNNING_STYLE_CATALOG_GENERATION: "raw-iceberg-v1",
}));
vi.mock("./running-style-feature-parquet", () => ({
  loadRunningStyleFeatureParquet: vi.fn(),
  putRunningStyleFeatureParquet: vi.fn(),
  runningStyleParquetVerificationKey: vi.fn(() => "verification/key"),
  validateFeatureCoverage: vi.fn(),
}));
vi.mock("./running-style-inference", () => ({
  runRunningStyleInferenceRowsWithFlatModel: vi.fn(),
}));
vi.mock("./running-style-model-binary", () => ({
  buildRunningStyleFlatModelKey: vi.fn(() => "models/v7-lineage.bin"),
  loadFlatLightGBMModelFromR2: vi.fn(),
}));
vi.mock("./running-style-calibration", () => ({
  buildCalibrationR2Key: vi.fn(() => "running-style/models/jra/calibrators.json"),
  loadCalibratorsFromR2: vi.fn(),
}));

const makeEnv = (): Env =>
  Object.assign(JSON.parse("{}"), {
    PC_KEIBA_R2_CATALOG: { fetch: vi.fn() },
    REALTIME_DB: {},
    RUNNING_STYLE_MODELS: {},
  });

const featureRows = (): RaceHorseFeatureRow[] => JSON.parse('[{"raceKey":"jra:20260512:08:01"}]');

beforeEach(() => {
  vi.clearAllMocks();
});

it("parseRunningStylePostgresVerificationParams rejects unrelated paths", async () => {
  const { parseRunningStylePostgresVerificationParams } =
    await import("./running-style-verification");
  expect(parseRunningStylePostgresVerificationParams(new URL("https://x.test/other"))).toBeNull();
});

it("parseRunningStylePostgresVerificationParams parses both source contracts", async () => {
  const { parseRunningStylePostgresVerificationParams } =
    await import("./running-style-verification");
  expect(
    parseRunningStylePostgresVerificationParams(
      new URL("https://x.test/admin/running-style/verify-postgres/jra/2026/05/12/08/01"),
    ),
  ).toStrictEqual({
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    raceBango: "01",
    source: "jra",
  });
  expect(
    parseRunningStylePostgresVerificationParams(
      new URL("https://x.test/admin/running-style/verify-postgres/nar/2026/05/12/B0/12"),
    )?.source,
  ).toBe("nar");
});

const prepareSuccessfulVerification = async (): Promise<void> => {
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { loadRunningStyleFeatureParquet, putRunningStyleFeatureParquet, validateFeatureCoverage } =
    await import("./running-style-feature-parquet");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue(
    JSON.parse('{"header":{"feature_names":["a","b"]}}'),
  );
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockResolvedValue(featureRows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({ missingCells: 0, missingFeatureNames: [] });
  vi.mocked(putRunningStyleFeatureParquet).mockResolvedValue(4096);
  vi.mocked(loadRunningStyleFeatureParquet).mockResolvedValue(featureRows());
  vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mockResolvedValue(
    JSON.parse('{"modelVersion":"v7-lineage","writtenCount":1}'),
  );
};

it("verification obtains heavy features only from Catalog", async () => {
  const { runRunningStyleWorkerPostgresVerification } =
    await import("./running-style-verification");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  await prepareSuccessfulVerification();
  const env = makeEnv();
  const summary = await runRunningStyleWorkerPostgresVerification(
    env,
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
    "2026-05-12T11:30:00.000Z",
  );
  expect(fetchRunningStyleFeaturesFromCatalog).toHaveBeenCalledWith(
    env.PC_KEIBA_R2_CATALOG,
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
    ["a", "b"],
  );
  expect(summary.inputFeaturesKey).toBe("raw-iceberg-v1");
  expect(summary.featureCount).toBe(2);
  expect(summary.parquetBytes).toBe(4096);
  expect(summary.readBackRows).toBe(1);
  expect(summary.writtenCount).toBe(1);
});

it("verification fails closed when Catalog returns no rows", async () => {
  const { runRunningStyleWorkerPostgresVerification } =
    await import("./running-style-verification");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue(
    JSON.parse('{"header":{"feature_names":["a"]}}'),
  );
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockResolvedValue([]);
  await expect(
    runRunningStyleWorkerPostgresVerification(makeEnv(), {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    }),
  ).rejects.toThrow("no running-style feature rows found");
});

it("verification rejects Catalog rows with model coverage gaps", async () => {
  const { runRunningStyleWorkerPostgresVerification } =
    await import("./running-style-verification");
  const { fetchRunningStyleFeaturesFromCatalog } = await import("./running-style-catalog-client");
  const { loadFlatLightGBMModelFromR2 } = await import("./running-style-model-binary");
  const { validateFeatureCoverage } = await import("./running-style-feature-parquet");
  vi.mocked(loadFlatLightGBMModelFromR2).mockResolvedValue(
    JSON.parse('{"header":{"feature_names":["a"]}}'),
  );
  vi.mocked(fetchRunningStyleFeaturesFromCatalog).mockResolvedValue(featureRows());
  vi.mocked(validateFeatureCoverage).mockReturnValue({
    missingCells: 1,
    missingFeatureNames: ["a"],
  });
  await expect(
    runRunningStyleWorkerPostgresVerification(makeEnv(), {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    }),
  ).rejects.toThrow("catalog feature build missing model features: a");
});

it("verification passes loaded calibrators to inference", async () => {
  const { runRunningStyleWorkerPostgresVerification } =
    await import("./running-style-verification");
  const { loadCalibratorsFromR2 } = await import("./running-style-calibration");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  await prepareSuccessfulVerification();
  const calibrators = JSON.parse('{"category":"jra","calibrators":{}}');
  vi.mocked(loadCalibratorsFromR2).mockResolvedValue(calibrators);
  await runRunningStyleWorkerPostgresVerification(
    makeEnv(),
    {
      kaisaiNen: "2026",
      kaisaiTsukihi: "0512",
      keibajoCode: "08",
      raceBango: "01",
      source: "jra",
    },
    "2026-05-12T11:30:00.000Z",
  );
  expect(
    vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mock.calls[0]?.[1].calibrators,
  ).toStrictEqual(calibrators);
});

it("verification remains available without optional calibrators", async () => {
  const { runRunningStyleWorkerPostgresVerification } =
    await import("./running-style-verification");
  const { loadCalibratorsFromR2 } = await import("./running-style-calibration");
  const { runRunningStyleInferenceRowsWithFlatModel } = await import("./running-style-inference");
  await prepareSuccessfulVerification();
  vi.mocked(loadCalibratorsFromR2).mockRejectedValue(new Error("not found"));
  await runRunningStyleWorkerPostgresVerification(makeEnv(), {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0512",
    keibajoCode: "08",
    raceBango: "01",
    source: "jra",
  });
  expect(
    vi.mocked(runRunningStyleInferenceRowsWithFlatModel).mock.calls[0]?.[1].calibrators,
  ).toBeUndefined();
});
