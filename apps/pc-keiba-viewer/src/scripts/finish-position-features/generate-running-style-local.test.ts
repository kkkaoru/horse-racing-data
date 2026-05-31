// Run with: bun run --filter pc-keiba-viewer test
import { describe, expect, test, vi } from "vitest";

import {
  assertColimaCapacity,
  buildCategoryFeaturesDir,
  buildCategoryLogitsDir,
  buildCategoryPredictionsDir,
  buildDefaultOptions,
  buildFeaturesRoot,
  buildLogitsRoot,
  buildManifest,
  buildPhaseACommand,
  buildPhaseBCommand,
  buildPhaseCCommand,
  buildPredictionsRoot,
  chunkYears,
  isInsideNightWindow,
  isJraV2ModelVersion,
  parseArgs,
  resolveMemoryLimitPerChunk,
  runGenerateRunningStyleLocal,
  runVoidTasksWithConcurrencyLimit,
  COLIMA_MIN_CPU,
  DEFAULT_MAX_YEARS_PER_RUN,
  DEFAULT_MEMORY_LIMIT_PER_CHUNK,
  DEFAULT_PHASE_A_CONCURRENCY,
  JRA_V2_MODEL_TAG,
  PER_CATEGORY_SLEEP_MS,
  PER_YEAR_SLEEP_MS,
  PHASE_A_SCRIPT,
  PHASE_A_UV_PROJECT,
  PHASE_B_SCRIPT,
  PHASE_C_SCRIPT,
  RUNNING_STYLE_CATEGORIES,
} from "./generate-running-style-local";

const FIXED_NIGHT_DATE = new Date("2026-05-30T16:00:00Z"); // 01:00 JST -> inside window
const FIXED_DAY_DATE = new Date("2026-05-30T05:00:00Z"); // 14:00 JST -> outside window

describe("generate-running-style-local", () => {
  test("buildDefaultOptions returns initial empty strings and 8 threads", () => {
    const options = buildDefaultOptions();
    expect(options.threads).toBe(8);
    expect(options.memoryLimit).toBe("16GB");
    expect(options.maxYearsPerRun).toBe(1);
  });

  test("buildDefaultOptions returns phaseAConcurrency of 4", () => {
    const options = buildDefaultOptions();
    expect(options.phaseAConcurrency).toBe(4);
  });

  test("buildDefaultOptions returns empty memoryLimitPerChunk so it falls back to memoryLimit", () => {
    const options = buildDefaultOptions();
    expect(options.memoryLimitPerChunk).toBe("");
  });

  test("buildDefaultOptions returns empty rsPFromFlatbinJra", () => {
    const options = buildDefaultOptions();
    expect(options.rsPFromFlatbinJra).toBe("");
  });

  test("parseArgs throws when pg-url missing", () => {
    expect(() => parseArgs([])).toThrowError("--pg-url is required.");
  });

  test("parseArgs throws when model-version-jra missing", () => {
    expect(() => parseArgs(["--pg-url", "u"])).toThrowError("--model-version-jra is required.");
  });

  test("parseArgs throws when model-version-nar missing", () => {
    expect(() => parseArgs(["--pg-url", "u", "--model-version-jra", "mj"])).toThrowError(
      "--model-version-nar is required.",
    );
  });

  test("parseArgs throws when model-flatbin-jra missing", () => {
    expect(() =>
      parseArgs(["--pg-url", "u", "--model-version-jra", "mj", "--model-version-nar", "mn"]),
    ).toThrowError("--model-flatbin-jra is required.");
  });

  test("parseArgs throws when model-flatbin-nar missing", () => {
    expect(() =>
      parseArgs([
        "--pg-url",
        "u",
        "--model-version-jra",
        "mj",
        "--model-version-nar",
        "mn",
        "--model-flatbin-jra",
        "/p/jra.flatbin",
      ]),
    ).toThrowError("--model-flatbin-nar is required.");
  });

  test("parseArgs throws when jra v2 model lacks --rs-p-from-flatbin-jra", () => {
    expect(() =>
      parseArgs([
        "--pg-url",
        "u",
        "--model-version-jra",
        "jra-running-style-lgbm-prod-v2",
        "--model-version-nar",
        "nar-running-style-lgbm-prod-v1.5",
        "--model-flatbin-jra",
        "/p/jra.flatbin",
        "--model-flatbin-nar",
        "/p/nar.flatbin",
      ]),
    ).toThrowError(
      "--rs-p-from-flatbin-jra is required when --model-version-jra targets prod-v2 (chained predict from v1.5).",
    );
  });

  test("parseArgs succeeds when jra v2 model has --rs-p-from-flatbin-jra", () => {
    const options = parseArgs([
      "--pg-url",
      "u",
      "--model-version-jra",
      "jra-running-style-lgbm-prod-v2",
      "--model-version-nar",
      "nar-running-style-lgbm-prod-v1.5",
      "--model-flatbin-jra",
      "/p/jra-v2.flatbin",
      "--model-flatbin-nar",
      "/p/nar.flatbin",
      "--rs-p-from-flatbin-jra",
      "/p/jra-v1.5.flatbin",
    ]);
    expect(options.rsPFromFlatbinJra).toBe("/p/jra-v1.5.flatbin");
  });

  test("parseArgs succeeds when jra v1.5 model omits --rs-p-from-flatbin-jra", () => {
    const options = parseArgs([
      "--pg-url",
      "u",
      "--model-version-jra",
      "jra-running-style-lgbm-prod-v1.5",
      "--model-version-nar",
      "nar-running-style-lgbm-prod-v1.5",
      "--model-flatbin-jra",
      "/p/jra-v1.5.flatbin",
      "--model-flatbin-nar",
      "/p/nar.flatbin",
    ]);
    expect(options.rsPFromFlatbinJra).toBe("");
  });

  test("parseArgs throws on unknown argument", () => {
    expect(() => parseArgs(["--unknown", "x"])).toThrowError("Unknown argument: --unknown");
  });

  test("parseArgs returns all required fields when supplied", () => {
    const options = parseArgs([
      "--pg-url",
      "postgres://u",
      "--running-style-feature-version",
      "v1",
      "--threads",
      "4",
      "--memory-limit",
      "8GB",
      "--max-years-per-run",
      "3",
      "--ignore-night-window",
      "1",
      "--output-root",
      "/tmp/out",
      "--model-version-jra",
      "mj",
      "--model-version-nar",
      "mn",
      "--model-flatbin-jra",
      "/p/jra.flatbin",
      "--model-flatbin-nar",
      "/p/nar.flatbin",
    ]);
    expect(options.threads).toBe(4);
    expect(options.ignoreNightWindow).toBe(true);
    expect(options.modelVersionJra).toBe("mj");
    expect(options.modelFlatbinJra).toBe("/p/jra.flatbin");
    expect(options.modelFlatbinNar).toBe("/p/nar.flatbin");
  });

  test("parseArgs ignore-night-window false when value is 0", () => {
    const options = parseArgs([
      "--pg-url",
      "u",
      "--model-version-jra",
      "mj",
      "--model-version-nar",
      "mn",
      "--model-flatbin-jra",
      "/p/jra.flatbin",
      "--model-flatbin-nar",
      "/p/nar.flatbin",
      "--ignore-night-window",
      "0",
    ]);
    expect(options.ignoreNightWindow).toBe(false);
  });

  test("parseArgs succeeds with jra and nar model versions plus flatbin paths", () => {
    const options = parseArgs([
      "--pg-url",
      "postgres://u",
      "--model-version-jra",
      "X",
      "--model-version-nar",
      "Y",
      "--model-flatbin-jra",
      "/p/jra.flatbin",
      "--model-flatbin-nar",
      "/p/nar.flatbin",
    ]);
    expect(options.modelVersionJra).toBe("X");
    expect(options.modelVersionNar).toBe("Y");
  });

  test("parseArgs throws Unknown argument when --model-version-ban-ei is supplied", () => {
    expect(() =>
      parseArgs([
        "--pg-url",
        "postgres://u",
        "--model-version-jra",
        "X",
        "--model-version-nar",
        "Y",
        "--model-flatbin-jra",
        "/p/jra.flatbin",
        "--model-flatbin-nar",
        "/p/nar.flatbin",
        "--model-version-ban-ei",
        "Z",
      ]),
    ).toThrowError("Unknown argument: --model-version-ban-ei");
  });

  test("parseArgs throws when --pg-url is supplied without a value", () => {
    expect(() => parseArgs(["--pg-url"])).toThrowError("--pg-url requires a value.");
  });

  test("parseArgs throws when --rs-p-from-flatbin-jra is supplied without a value", () => {
    expect(() => parseArgs(["--rs-p-from-flatbin-jra"])).toThrowError(
      "--rs-p-from-flatbin-jra requires a value.",
    );
  });

  test("RUNNING_STYLE_CATEGORIES equals jra and nar only", () => {
    expect(RUNNING_STYLE_CATEGORIES).toStrictEqual(["jra", "nar"]);
  });

  test("JRA_V2_MODEL_TAG equals -v2", () => {
    expect(JRA_V2_MODEL_TAG).toBe("-v2");
  });

  test("isJraV2ModelVersion returns true for prod-v2 suffix", () => {
    expect(isJraV2ModelVersion("jra-running-style-lgbm-prod-v2")).toBe(true);
  });

  test("isJraV2ModelVersion returns false for prod-v1.5 suffix", () => {
    expect(isJraV2ModelVersion("jra-running-style-lgbm-prod-v1.5")).toBe(false);
  });

  test("isInsideNightWindow returns true for 01:00 JST", () => {
    expect(isInsideNightWindow(FIXED_NIGHT_DATE)).toBe(true);
  });

  test("isInsideNightWindow returns false for 14:00 JST", () => {
    expect(isInsideNightWindow(FIXED_DAY_DATE)).toBe(false);
  });

  test("assertColimaCapacity throws when CPU below minimum", () => {
    expect(() => assertColimaCapacity({ cpu: 2, memoryGiB: 24, diskGiB: 100 })).toThrowError(
      `Colima CPU 2 below minimum ${COLIMA_MIN_CPU}.`,
    );
  });

  test("assertColimaCapacity throws when memory below minimum", () => {
    expect(() => assertColimaCapacity({ cpu: 8, memoryGiB: 4, diskGiB: 100 })).toThrowError(
      "Colima memory 4 GiB below minimum 24 GiB.",
    );
  });

  test("assertColimaCapacity throws when disk below minimum", () => {
    expect(() => assertColimaCapacity({ cpu: 8, memoryGiB: 24, diskGiB: 10 })).toThrowError(
      "Colima disk 10 GiB below minimum 100 GiB.",
    );
  });

  test("assertColimaCapacity passes when all minimums met", () => {
    expect(() => assertColimaCapacity({ cpu: 8, memoryGiB: 24, diskGiB: 100 })).not.toThrowError();
  });

  test("chunkYears splits 5-year range into 2 chunks with size 3", () => {
    expect(chunkYears([2020, 2021, 2022, 2023, 2024], 3)).toStrictEqual([
      { yearFrom: 2020, yearTo: 2022 },
      { yearFrom: 2023, yearTo: 2024 },
    ]);
  });

  test("chunkYears throws on zero chunkSize", () => {
    expect(() => chunkYears([2020], 0)).toThrowError("chunkSize must be positive.");
  });

  test("chunkYears empty input returns empty array", () => {
    expect(chunkYears([], 5)).toStrictEqual([]);
  });

  test("buildPhaseACommand emits PHASE_A_SCRIPT path after uv project flag and python interpreter", () => {
    const cmd = buildPhaseACommand({
      pgUrl: "u",
      outputDir: "/d",
      featureVersion: "v1",
      yearFrom: 2020,
      yearTo: 2024,
      category: "jra",
      threads: 8,
      memoryLimit: "16GB",
    });
    expect(cmd[5]).toBe(PHASE_A_SCRIPT);
  });

  test("buildPhaseACommand pins uv at pc-keiba-viewer venv via --project flag", () => {
    const cmd = buildPhaseACommand({
      pgUrl: "u",
      outputDir: "/d",
      featureVersion: "v1",
      yearFrom: 2020,
      yearTo: 2024,
      category: "jra",
      threads: 8,
      memoryLimit: "16GB",
    });
    expect(cmd[0]).toBe("uv");
    expect(cmd[1]).toBe("--project");
    expect(cmd[2]).toBe("apps/pc-keiba-viewer");
    expect(cmd[3]).toBe("run");
    expect(cmd[4]).toBe("python");
  });

  test("PHASE_A_UV_PROJECT equals apps/pc-keiba-viewer so duckdb venv resolves from repo root", () => {
    expect(PHASE_A_UV_PROJECT).toBe("apps/pc-keiba-viewer");
  });

  test("buildPhaseACommand encodes year range as strings", () => {
    const cmd = buildPhaseACommand({
      pgUrl: "u",
      outputDir: "/d",
      featureVersion: "v1",
      yearFrom: 2020,
      yearTo: 2024,
      category: "jra",
      threads: 8,
      memoryLimit: "16GB",
    });
    expect(cmd[13]).toBe("2020");
    expect(cmd[15]).toBe("2024");
  });

  test("buildPhaseBCommand uses bun run with PHASE_B_SCRIPT path", () => {
    const cmd = buildPhaseBCommand({
      modelFlatbin: "/m/jra.flatbin",
      featuresParquet: "/f",
      outputParquet: "/o",
      category: "nar",
      predictedAt: "2026-05-31T01:00:00.000Z",
      modelVersion: "m1",
      featureVersion: "v1",
      rsPFromFlatbin: null,
    });
    expect(cmd[0]).toBe("bun");
    expect(cmd[1]).toBe("run");
    expect(cmd[2]).toBe(PHASE_B_SCRIPT);
  });

  test("buildPhaseBCommand encodes model version after --model-version", () => {
    const cmd = buildPhaseBCommand({
      modelFlatbin: "/m/jra.flatbin",
      featuresParquet: "/f",
      outputParquet: "/o",
      category: "nar",
      predictedAt: "2026-05-31T01:00:00.000Z",
      modelVersion: "m1",
      featureVersion: "v1",
      rsPFromFlatbin: null,
    });
    const modelVersionIndex = cmd.indexOf("--model-version");
    expect(cmd[modelVersionIndex + 1]).toBe("m1");
  });

  test("buildPhaseBCommand encodes --model-flatbin flag and value", () => {
    const cmd = buildPhaseBCommand({
      modelFlatbin: "/m/jra.flatbin",
      featuresParquet: "/f",
      outputParquet: "/o",
      category: "jra",
      predictedAt: "2026-05-31T01:00:00.000Z",
      modelVersion: "m1",
      featureVersion: "v1",
      rsPFromFlatbin: null,
    });
    const flatbinIndex = cmd.indexOf("--model-flatbin");
    expect(cmd[flatbinIndex + 1]).toBe("/m/jra.flatbin");
  });

  test("buildPhaseBCommand encodes --predicted-at flag and value", () => {
    const cmd = buildPhaseBCommand({
      modelFlatbin: "/m/jra.flatbin",
      featuresParquet: "/f",
      outputParquet: "/o",
      category: "jra",
      predictedAt: "2026-05-31T01:00:00.000Z",
      modelVersion: "m1",
      featureVersion: "v1",
      rsPFromFlatbin: null,
    });
    const predictedAtIndex = cmd.indexOf("--predicted-at");
    expect(cmd[predictedAtIndex + 1]).toBe("2026-05-31T01:00:00.000Z");
  });

  test("buildPhaseBCommand encodes --feature-version flag and value", () => {
    const cmd = buildPhaseBCommand({
      modelFlatbin: "/m/jra.flatbin",
      featuresParquet: "/f",
      outputParquet: "/o",
      category: "jra",
      predictedAt: "2026-05-31T01:00:00.000Z",
      modelVersion: "m1",
      featureVersion: "v1",
      rsPFromFlatbin: null,
    });
    const featureVersionIndex = cmd.indexOf("--feature-version");
    expect(cmd[featureVersionIndex + 1]).toBe("v1");
  });

  test("buildPhaseBCommand encodes --category flag and value", () => {
    const cmd = buildPhaseBCommand({
      modelFlatbin: "/m/jra.flatbin",
      featuresParquet: "/f",
      outputParquet: "/o",
      category: "jra",
      predictedAt: "2026-05-31T01:00:00.000Z",
      modelVersion: "m1",
      featureVersion: "v1",
      rsPFromFlatbin: null,
    });
    const categoryIndex = cmd.indexOf("--category");
    expect(cmd[categoryIndex + 1]).toBe("jra");
  });

  test("buildPhaseBCommand does NOT contain --pg-url flag", () => {
    const cmd = buildPhaseBCommand({
      modelFlatbin: "/m/jra.flatbin",
      featuresParquet: "/f",
      outputParquet: "/o",
      category: "jra",
      predictedAt: "2026-05-31T01:00:00.000Z",
      modelVersion: "m1",
      featureVersion: "v1",
      rsPFromFlatbin: null,
    });
    expect(cmd.includes("--pg-url")).toBe(false);
  });

  test("buildPhaseBCommand does NOT spawn python (no uv / python tokens)", () => {
    const cmd = buildPhaseBCommand({
      modelFlatbin: "/m/jra.flatbin",
      featuresParquet: "/f",
      outputParquet: "/o",
      category: "jra",
      predictedAt: "2026-05-31T01:00:00.000Z",
      modelVersion: "m1",
      featureVersion: "v1",
      rsPFromFlatbin: null,
    });
    expect(cmd.includes("uv")).toBe(false);
    expect(cmd.includes("python")).toBe(false);
  });

  test("buildPhaseBCommand encodes --rs-p-from-flatbin when chained path is provided", () => {
    const cmd = buildPhaseBCommand({
      modelFlatbin: "/m/jra-v2.flatbin",
      featuresParquet: "/f",
      outputParquet: "/o",
      category: "jra",
      predictedAt: "2026-05-31T01:00:00.000Z",
      modelVersion: "jra-running-style-lgbm-prod-v2",
      featureVersion: "v1",
      rsPFromFlatbin: "/m/jra-v1.5.flatbin",
    });
    const chainIndex = cmd.indexOf("--rs-p-from-flatbin");
    expect(cmd[chainIndex + 1]).toBe("/m/jra-v1.5.flatbin");
  });

  test("buildPhaseBCommand omits --rs-p-from-flatbin when chained path is null", () => {
    const cmd = buildPhaseBCommand({
      modelFlatbin: "/m/nar.flatbin",
      featuresParquet: "/f",
      outputParquet: "/o",
      category: "nar",
      predictedAt: "2026-05-31T01:00:00.000Z",
      modelVersion: "nar-running-style-lgbm-prod-v1.5",
      featureVersion: "v1",
      rsPFromFlatbin: null,
    });
    expect(cmd.includes("--rs-p-from-flatbin")).toBe(false);
  });

  test("buildPhaseCCommand emits PHASE_C_SCRIPT after bun run", () => {
    const cmd = buildPhaseCCommand({
      logitsParquet: "/l",
      outputParquet: "/p",
      featureVersion: "v1",
    });
    expect(cmd[0]).toBe("bun");
    expect(cmd[1]).toBe("run");
    expect(cmd[2]).toBe(PHASE_C_SCRIPT);
  });

  test("buildPhaseCCommand encodes feature-version flag", () => {
    const cmd = buildPhaseCCommand({
      logitsParquet: "/l",
      outputParquet: "/p",
      featureVersion: "v9",
    });
    const flagIndex = cmd.indexOf("--feature-version");
    expect(cmd[flagIndex + 1]).toBe("v9");
  });

  test("buildPhaseCCommand encodes logits and output flags", () => {
    const cmd = buildPhaseCCommand({
      logitsParquet: "/l",
      outputParquet: "/p",
      featureVersion: "v1",
    });
    const logitsIndex = cmd.indexOf("--logits-parquet");
    const outputIndex = cmd.indexOf("--output-parquet");
    expect(cmd[logitsIndex + 1]).toBe("/l");
    expect(cmd[outputIndex + 1]).toBe("/p");
  });

  test("buildFeaturesRoot composes outputRoot, version, and features segment", () => {
    expect(
      buildFeaturesRoot({
        ...buildDefaultOptions(),
        outputRoot: "/tmp/r",
        runningStyleFeatureVersion: "v1",
      }),
    ).toBe("/tmp/r/v1/features");
  });

  test("buildLogitsRoot composes outputRoot, version, and logits segment", () => {
    expect(
      buildLogitsRoot({
        ...buildDefaultOptions(),
        outputRoot: "/tmp/r",
        runningStyleFeatureVersion: "v1",
      }),
    ).toBe("/tmp/r/v1/logits");
  });

  test("buildPredictionsRoot composes outputRoot, version, and predictions segment", () => {
    expect(
      buildPredictionsRoot({
        ...buildDefaultOptions(),
        outputRoot: "/tmp/r",
        runningStyleFeatureVersion: "v1",
      }),
    ).toBe("/tmp/r/v1/predictions");
  });

  test("buildCategoryFeaturesDir composes Hive-style category subpath for jra", () => {
    expect(
      buildCategoryFeaturesDir(
        { ...buildDefaultOptions(), outputRoot: "/tmp/r", runningStyleFeatureVersion: "v1" },
        "jra",
      ),
    ).toBe("/tmp/r/v1/features/category=jra");
  });

  test("buildCategoryLogitsDir composes Hive-style category subpath for nar", () => {
    expect(
      buildCategoryLogitsDir(
        { ...buildDefaultOptions(), outputRoot: "/tmp/r", runningStyleFeatureVersion: "v1" },
        "nar",
      ),
    ).toBe("/tmp/r/v1/logits/category=nar");
  });

  test("buildCategoryPredictionsDir composes Hive-style category subpath for jra", () => {
    expect(
      buildCategoryPredictionsDir(
        { ...buildDefaultOptions(), outputRoot: "/tmp/r", runningStyleFeatureVersion: "v1" },
        "jra",
      ),
    ).toBe("/tmp/r/v1/predictions/category=jra");
  });

  test("buildManifest summarizes jra and nar categories with year ranges", () => {
    const options = buildDefaultOptions();
    const manifest = buildManifest(
      {
        ...options,
        runningStyleFeatureVersion: "v1",
        outputRoot: "/tmp",
        modelVersionJra: "mj",
        modelVersionNar: "mn",
      },
      new Date("2026-05-30T15:00:00Z"),
    );
    expect(manifest.featureVersion).toBe("v1");
    expect(manifest.categories.jra?.modelVersion).toBe("mj");
    expect(manifest.categories.nar?.modelVersion).toBe("mn");
  });

  test("buildManifest does not include ban-ei category", () => {
    const options = buildDefaultOptions();
    const manifest = buildManifest(
      {
        ...options,
        runningStyleFeatureVersion: "v1",
        outputRoot: "/tmp",
        modelVersionJra: "mj",
        modelVersionNar: "mn",
      },
      new Date("2026-05-30T15:00:00Z"),
    );
    expect(manifest.categories["ban-ei"]).toBe(undefined);
  });

  test("buildManifest exposes featuresPath, logitsPath, and predictionsPath", () => {
    const manifest = buildManifest(
      {
        ...buildDefaultOptions(),
        runningStyleFeatureVersion: "v1",
        outputRoot: "/tmp",
        modelVersionJra: "mj",
        modelVersionNar: "mn",
      },
      new Date("2026-05-30T15:00:00Z"),
    );
    expect(manifest.featuresPath).toBe("/tmp/v1/features");
    expect(manifest.logitsPath).toBe("/tmp/v1/logits");
    expect(manifest.predictionsPath).toBe("/tmp/v1/predictions");
  });

  test("buildManifest carries model versions in dedicated modelVersions field", () => {
    const manifest = buildManifest(
      {
        ...buildDefaultOptions(),
        runningStyleFeatureVersion: "v1",
        outputRoot: "/tmp",
        modelVersionJra: "mj",
        modelVersionNar: "mn",
      },
      new Date("2026-05-30T15:00:00Z"),
    );
    expect(manifest.modelVersions).toStrictEqual({ jra: "mj", nar: "mn" });
  });

  test("buildManifest exposes rsPFromFlatbin with jra path and null nar when configured", () => {
    const manifest = buildManifest(
      {
        ...buildDefaultOptions(),
        runningStyleFeatureVersion: "v1",
        outputRoot: "/tmp",
        modelVersionJra: "jra-running-style-lgbm-prod-v2",
        modelVersionNar: "nar-running-style-lgbm-prod-v1.5",
        modelFlatbinJra: "/p/jra-v2.flatbin",
        modelFlatbinNar: "/p/nar.flatbin",
        rsPFromFlatbinJra: "/p/jra-v1.5.flatbin",
      },
      new Date("2026-05-30T15:00:00Z"),
    );
    expect(manifest.rsPFromFlatbin).toStrictEqual({ jra: "/p/jra-v1.5.flatbin", nar: null });
  });

  test("buildManifest exposes rsPFromFlatbin null for both when omitted", () => {
    const manifest = buildManifest(
      {
        ...buildDefaultOptions(),
        runningStyleFeatureVersion: "v1",
        outputRoot: "/tmp",
        modelVersionJra: "jra-running-style-lgbm-prod-v1.5",
        modelVersionNar: "nar-running-style-lgbm-prod-v1.5",
        modelFlatbinJra: "/p/jra-v1.5.flatbin",
        modelFlatbinNar: "/p/nar.flatbin",
      },
      new Date("2026-05-30T15:00:00Z"),
    );
    expect(manifest.rsPFromFlatbin).toStrictEqual({ jra: null, nar: null });
  });

  test("PHASE_A_SCRIPT points at the Agent X3 feature-builder script (repo-root relative)", () => {
    expect(PHASE_A_SCRIPT).toBe(
      "apps/pc-keiba-viewer/src/scripts/generate_running_style_features_local.py",
    );
  });

  test("PHASE_B_SCRIPT points at the Agent W3 precision-0 LightGBM Bun TS inference script", () => {
    expect(PHASE_B_SCRIPT).toBe(
      "apps/sync-realtime-data/src/scripts/run-running-style-inference-local.ts",
    );
  });

  test("PHASE_C_SCRIPT points at the Agent X2 post-processing TS script (repo-root relative)", () => {
    expect(PHASE_C_SCRIPT).toBe(
      "apps/pc-keiba-viewer/src/scripts/finish-position-features/apply-running-style-postproc.ts",
    );
  });

  test("PHASE_A_SCRIPT, PHASE_B_SCRIPT, and PHASE_C_SCRIPT are all repo-root relative (start with apps/)", () => {
    expect(PHASE_A_SCRIPT.startsWith("apps/")).toBe(true);
    expect(PHASE_B_SCRIPT.startsWith("apps/")).toBe(true);
    expect(PHASE_C_SCRIPT.startsWith("apps/")).toBe(true);
  });

  test("runGenerateRunningStyleLocal aborts outside night window when guard active", async () => {
    const spawn = vi.fn<() => Promise<{ exitCode: number }>>();
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>();
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: false,
    };
    await expect(
      runGenerateRunningStyleLocal(options, {
        spawn,
        sleep,
        probeColima,
        now: () => FIXED_DAY_DATE,
      }),
    ).rejects.toThrowError(
      "Outside JST night window 23-04. Pass --ignore-night-window 1 to bypass.",
    );
    expect(spawn).not.toHaveBeenCalled();
  });

  test("runGenerateRunningStyleLocal aborts when Colima resources insufficient", async () => {
    const spawn = vi.fn<() => Promise<{ exitCode: number }>>();
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 2, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
    };
    await expect(
      runGenerateRunningStyleLocal(options, {
        spawn,
        sleep,
        probeColima,
        now: () => FIXED_DAY_DATE,
      }),
    ).rejects.toThrowError("Colima CPU 2 below minimum 8.");
    expect(spawn).not.toHaveBeenCalled();
  });

  test("runGenerateRunningStyleLocal runs Phase A then B then C in order per category", async () => {
    const spawnCalls: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        const phaseAHit = command[5] === PHASE_A_SCRIPT ? PHASE_A_SCRIPT : "";
        const phaseBHit = command[2] === PHASE_B_SCRIPT ? PHASE_B_SCRIPT : "";
        const phaseCHit = command[2] === PHASE_C_SCRIPT ? PHASE_C_SCRIPT : "";
        spawnCalls.push(phaseBHit || phaseCHit || phaseAHit);
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/test-out",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    const firstA = spawnCalls.indexOf(PHASE_A_SCRIPT);
    const firstB = spawnCalls.indexOf(PHASE_B_SCRIPT);
    const firstC = spawnCalls.indexOf(PHASE_C_SCRIPT);
    expect(firstA).toBe(0);
    expect(firstB > firstA).toBe(true);
    expect(firstC > firstB).toBe(true);
  });

  test("runGenerateRunningStyleLocal Phase B and C only spawned for jra and nar (no ban-ei)", async () => {
    const phaseBCategories: string[] = [];
    const phaseCInvocations: number[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[2] === PHASE_B_SCRIPT) {
          const categoryIndex = command.indexOf("--category");
          phaseBCategories.push(command[categoryIndex + 1] ?? "");
        }
        if (command[2] === PHASE_C_SCRIPT) {
          phaseCInvocations.push(phaseCInvocations.length);
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/test-out-no-banei",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    expect(phaseBCategories.toSorted()).toStrictEqual(["jra", "nar"]);
    expect(phaseCInvocations.length).toBe(2);
  });

  test("runGenerateRunningStyleLocal Phase C input directory chains from Phase B logits dir", async () => {
    const phaseCInputs: string[] = [];
    const phaseBOutputs: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[2] === PHASE_B_SCRIPT) {
          const outputIndex = command.indexOf("--output-parquet");
          phaseBOutputs.push(command[outputIndex + 1] ?? "");
        }
        if (command[2] === PHASE_C_SCRIPT) {
          const logitsIndex = command.indexOf("--logits-parquet");
          phaseCInputs.push(command[logitsIndex + 1] ?? "");
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/chain-out",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    expect(phaseCInputs).toStrictEqual(phaseBOutputs);
  });

  test("runGenerateRunningStyleLocal Phase A output directory chains into Phase B features dir", async () => {
    const phaseAOutputs: string[] = [];
    const phaseBInputs: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[5] === PHASE_A_SCRIPT) {
          const outputIndex = command.indexOf("--output-dir");
          phaseAOutputs.push(command[outputIndex + 1] ?? "");
        }
        if (command[2] === PHASE_B_SCRIPT) {
          const featuresIndex = command.indexOf("--features-parquet");
          phaseBInputs.push(command[featuresIndex + 1] ?? "");
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/featchain",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    const uniquePhaseAOutputs = [...new Set(phaseAOutputs)].toSorted();
    const uniquePhaseBInputs = [...new Set(phaseBInputs)].toSorted();
    expect(uniquePhaseAOutputs).toStrictEqual(uniquePhaseBInputs);
  });

  test("runGenerateRunningStyleLocal Phase B receives --model-flatbin per category", async () => {
    const phaseBFlatbins: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[2] === PHASE_B_SCRIPT) {
          const flatbinIndex = command.indexOf("--model-flatbin");
          phaseBFlatbins.push(command[flatbinIndex + 1] ?? "");
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/flatbin-chain",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    expect(phaseBFlatbins.toSorted()).toStrictEqual(["/p/jra.flatbin", "/p/nar.flatbin"]);
  });

  test("runGenerateRunningStyleLocal Phase B for jra includes --rs-p-from-flatbin when configured", async () => {
    const jraCommands: ReadonlyArray<string>[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[2] === PHASE_B_SCRIPT) {
          const categoryIndex = command.indexOf("--category");
          if (command[categoryIndex + 1] === "jra") {
            jraCommands.push(command);
          }
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "jra-running-style-lgbm-prod-v2",
      modelVersionNar: "nar-running-style-lgbm-prod-v1.5",
      modelFlatbinJra: "/p/jra-v2.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      rsPFromFlatbinJra: "/p/jra-v1.5.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/jra-chain",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    const jraCmd = jraCommands[0] ?? [];
    const chainIndex = jraCmd.indexOf("--rs-p-from-flatbin");
    expect(jraCmd[chainIndex + 1]).toBe("/p/jra-v1.5.flatbin");
  });

  test("runGenerateRunningStyleLocal Phase B for nar omits --rs-p-from-flatbin", async () => {
    const narCommands: ReadonlyArray<string>[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[2] === PHASE_B_SCRIPT) {
          const categoryIndex = command.indexOf("--category");
          if (command[categoryIndex + 1] === "nar") {
            narCommands.push(command);
          }
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "jra-running-style-lgbm-prod-v2",
      modelVersionNar: "nar-running-style-lgbm-prod-v1.5",
      modelFlatbinJra: "/p/jra-v2.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      rsPFromFlatbinJra: "/p/jra-v1.5.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/nar-no-chain",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    const narCmd = narCommands[0] ?? [];
    expect(narCmd.includes("--rs-p-from-flatbin")).toBe(false);
  });

  test("runGenerateRunningStyleLocal Phase B for jra omits --rs-p-from-flatbin when not configured", async () => {
    const jraCommands: ReadonlyArray<string>[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[2] === PHASE_B_SCRIPT) {
          const categoryIndex = command.indexOf("--category");
          if (command[categoryIndex + 1] === "jra") {
            jraCommands.push(command);
          }
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "jra-running-style-lgbm-prod-v1.5",
      modelVersionNar: "nar-running-style-lgbm-prod-v1.5",
      modelFlatbinJra: "/p/jra-v1.5.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/jra-no-chain",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    const jraCmd = jraCommands[0] ?? [];
    expect(jraCmd.includes("--rs-p-from-flatbin")).toBe(false);
  });

  test("runGenerateRunningStyleLocal Phase B receives ISO timestamp from deps.now via --predicted-at", async () => {
    const phaseBPredictedAts: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[2] === PHASE_B_SCRIPT) {
          const predictedAtIndex = command.indexOf("--predicted-at");
          phaseBPredictedAts.push(command[predictedAtIndex + 1] ?? "");
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/predicted-at-chain",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    expect(phaseBPredictedAts).toStrictEqual([
      "2026-05-30T16:00:00.000Z",
      "2026-05-30T16:00:00.000Z",
    ]);
  });

  test("runGenerateRunningStyleLocal throws when Phase A spawn returns non-zero exit", async () => {
    const spawn = vi.fn<() => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 1 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
    };
    await expect(
      runGenerateRunningStyleLocal(options, {
        spawn,
        sleep,
        probeColima,
        now: () => FIXED_NIGHT_DATE,
      }),
    ).rejects.toThrowError("Phase A failed");
  });

  test("runGenerateRunningStyleLocal throws when Phase B spawn returns non-zero exit", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        const failed = command[2] === PHASE_B_SCRIPT ? 1 : 0;
        return Promise.resolve({ exitCode: failed });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
    };
    await expect(
      runGenerateRunningStyleLocal(options, {
        spawn,
        sleep,
        probeColima,
        now: () => FIXED_NIGHT_DATE,
      }),
    ).rejects.toThrowError(/Phase B failed for category=(jra|nar)\.$/);
  });

  test("runGenerateRunningStyleLocal throws when Phase C spawn returns non-zero exit", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        const failed = command[2] === PHASE_C_SCRIPT ? 1 : 0;
        return Promise.resolve({ exitCode: failed });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
    };
    await expect(
      runGenerateRunningStyleLocal(options, {
        spawn,
        sleep,
        probeColima,
        now: () => FIXED_NIGHT_DATE,
      }),
    ).rejects.toThrowError(/Phase C failed for category=(jra|nar)\.$/);
  });

  test("per-year sleep constant equals 2000", () => {
    expect(PER_YEAR_SLEEP_MS).toBe(2000);
  });

  test("per-category sleep constant equals 5000", () => {
    expect(PER_CATEGORY_SLEEP_MS).toBe(5000);
  });

  test("DEFAULT_MAX_YEARS_PER_RUN equals 1 so each chunk covers a single year", () => {
    expect(DEFAULT_MAX_YEARS_PER_RUN).toBe(1);
  });

  test("DEFAULT_PHASE_A_CONCURRENCY equals 4 so 4 chunks may run in parallel", () => {
    expect(DEFAULT_PHASE_A_CONCURRENCY).toBe(4);
  });

  test("DEFAULT_MEMORY_LIMIT_PER_CHUNK equals empty so it falls back to --memory-limit", () => {
    expect(DEFAULT_MEMORY_LIMIT_PER_CHUNK).toBe("");
  });

  test("parseArgs accepts --phase-a-concurrency and sets numeric value", () => {
    const options = parseArgs([
      "--pg-url",
      "u",
      "--model-version-jra",
      "mj",
      "--model-version-nar",
      "mn",
      "--model-flatbin-jra",
      "/p/jra.flatbin",
      "--model-flatbin-nar",
      "/p/nar.flatbin",
      "--phase-a-concurrency",
      "6",
    ]);
    expect(options.phaseAConcurrency).toBe(6);
  });

  test("parseArgs accepts --memory-limit-per-chunk and stores raw string", () => {
    const options = parseArgs([
      "--pg-url",
      "u",
      "--model-version-jra",
      "mj",
      "--model-version-nar",
      "mn",
      "--model-flatbin-jra",
      "/p/jra.flatbin",
      "--model-flatbin-nar",
      "/p/nar.flatbin",
      "--memory-limit-per-chunk",
      "4GB",
    ]);
    expect(options.memoryLimitPerChunk).toBe("4GB");
  });

  test("parseArgs throws when --phase-a-concurrency is supplied without a value", () => {
    expect(() => parseArgs(["--phase-a-concurrency"])).toThrowError(
      "--phase-a-concurrency requires a value.",
    );
  });

  test("parseArgs throws when --memory-limit-per-chunk is supplied without a value", () => {
    expect(() => parseArgs(["--memory-limit-per-chunk"])).toThrowError(
      "--memory-limit-per-chunk requires a value.",
    );
  });

  test("resolveMemoryLimitPerChunk returns memoryLimit when per-chunk is empty", () => {
    const options = {
      ...buildDefaultOptions(),
      memoryLimit: "16GB",
      memoryLimitPerChunk: "",
    };
    expect(resolveMemoryLimitPerChunk(options)).toBe("16GB");
  });

  test("resolveMemoryLimitPerChunk returns per-chunk override when configured", () => {
    const options = {
      ...buildDefaultOptions(),
      memoryLimit: "16GB",
      memoryLimitPerChunk: "4GB",
    };
    expect(resolveMemoryLimitPerChunk(options)).toBe("4GB");
  });

  test("runVoidTasksWithConcurrencyLimit throws when concurrency is zero", async () => {
    await expect(runVoidTasksWithConcurrencyLimit([], 0)).rejects.toThrowError(
      "concurrency must be positive.",
    );
  });

  test("runVoidTasksWithConcurrencyLimit returns immediately for empty task list", async () => {
    const result = await runVoidTasksWithConcurrencyLimit([], 4);
    expect(result).toBe(undefined);
  });

  test("runVoidTasksWithConcurrencyLimit executes every task exactly once", async () => {
    const completed: string[] = [];
    const tasks: ReadonlyArray<() => Promise<void>> = [
      () => Promise.resolve().then(() => void completed.push("a")),
      () => Promise.resolve().then(() => void completed.push("b")),
      () => Promise.resolve().then(() => void completed.push("c")),
      () => Promise.resolve().then(() => void completed.push("d")),
      () => Promise.resolve().then(() => void completed.push("e")),
    ];
    await runVoidTasksWithConcurrencyLimit(tasks, 2);
    expect(completed.toSorted()).toStrictEqual(["a", "b", "c", "d", "e"]);
  });

  test("runVoidTasksWithConcurrencyLimit caps active task count to concurrency", async () => {
    const observed: number[] = [];
    const state = { active: 0 };
    const decrementActive = (): undefined => {
      state.active -= 1;
      return undefined;
    };
    const probeTask = (): Promise<void> => {
      state.active += 1;
      observed.push(state.active);
      return Promise.resolve().then(decrementActive);
    };
    const tasks: ReadonlyArray<() => Promise<void>> = [
      probeTask,
      probeTask,
      probeTask,
      probeTask,
      probeTask,
      probeTask,
      probeTask,
      probeTask,
    ];
    await runVoidTasksWithConcurrencyLimit(tasks, 3);
    expect(Math.max(...observed)).toBe(3);
  });

  test("runGenerateRunningStyleLocal Phase A spawns chunkSize=1 commands by default (1y chunks)", async () => {
    const phaseAYearPairs: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[5] === PHASE_A_SCRIPT) {
          const yearFromIndex = command.indexOf("--year-from");
          const yearToIndex = command.indexOf("--year-to");
          phaseAYearPairs.push(`${command[yearFromIndex + 1]}-${command[yearToIndex + 1]}`);
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/chunksize-default",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    expect(phaseAYearPairs.every((pair) => pair.split("-")[0] === pair.split("-")[1])).toBe(true);
  });

  test("runGenerateRunningStyleLocal Phase A spawn memory limit uses memoryLimitPerChunk override when set", async () => {
    const phaseAMemoryLimits: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[5] === PHASE_A_SCRIPT) {
          const memIndex = command.indexOf("--memory-limit");
          phaseAMemoryLimits.push(command[memIndex + 1] ?? "");
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      memoryLimit: "16GB",
      memoryLimitPerChunk: "4GB",
      ignoreNightWindow: true,
      outputRoot: "/tmp/memchunk-override",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    expect(phaseAMemoryLimits.every((limit) => limit === "4GB")).toBe(true);
  });

  test("runGenerateRunningStyleLocal Phase A spawn memory limit falls back to memoryLimit when per-chunk empty", async () => {
    const phaseAMemoryLimits: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[5] === PHASE_A_SCRIPT) {
          const memIndex = command.indexOf("--memory-limit");
          phaseAMemoryLimits.push(command[memIndex + 1] ?? "");
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      memoryLimit: "16GB",
      ignoreNightWindow: true,
      outputRoot: "/tmp/memchunk-fallback",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    expect(phaseAMemoryLimits.every((limit) => limit === "16GB")).toBe(true);
  });

  test("runGenerateRunningStyleLocal Phase A spawns jra and nar chunks in interleaved order (categories run in parallel)", async () => {
    const phaseACategories: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[5] === PHASE_A_SCRIPT) {
          const categoryIndex = command.indexOf("--category");
          phaseACategories.push(command[categoryIndex + 1] ?? "");
        }
        return Promise.resolve({ exitCode: 0 });
      },
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/parallel-cats",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    const firstNarIndex = phaseACategories.indexOf("nar");
    const lastJraIndex = phaseACategories.lastIndexOf("jra");
    expect(firstNarIndex < lastJraIndex).toBe(true);
  });
});
