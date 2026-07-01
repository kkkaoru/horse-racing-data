// Run with: bun run --filter pc-keiba-viewer test
import { describe, expect, test, vi } from "vitest";

import {
  assertColimaCapacity,
  buildCategoryFeaturesDir,
  buildCategoryLogitsDir,
  buildCategoryPredictionsDir,
  buildChunkYearDir,
  buildChunkYearRange,
  buildDefaultOptions,
  buildFeaturesRoot,
  buildLogitsRoot,
  buildManifest,
  buildPhaseACommand,
  buildPhaseBCommand,
  buildPhaseCCommand,
  buildPredictionsRoot,
  collectMacVmStatResourceSnapshot,
  chunkHasExistingParquet,
  chunkYears,
  chunkYearsByMonth,
  expandYearsToMonths,
  isInsideNightWindow,
  isJraV2ModelVersion,
  logitsFileHasContent,
  parseVmStatPages,
  parseArgs,
  resolveMemoryLimitPerChunk,
  runGenerateRunningStyleLocal,
  runVoidTasksWithConcurrencyLimit,
  yearHasFullMonthParquet,
  ALL_MONTHS,
  AUTO_RESOURCE_VALUE,
  CHUNK_GRANULARITIES,
  COLIMA_MIN_CPU,
  DEFAULT_CATEGORY_CONCURRENCY,
  DEFAULT_CHUNK_GRANULARITY,
  DEFAULT_FORCE,
  DEFAULT_MAX_YEARS_PER_RUN,
  DEFAULT_MEMORY_LIMIT_PER_CHUNK,
  DEFAULT_PHASE_A_CONCURRENCY,
  JRA_V2_MODEL_TAG,
  MONTHS_PER_YEAR,
  PER_CATEGORY_SLEEP_MS,
  PER_YEAR_SLEEP_MS,
  PHASE_A_SCRIPT,
  PHASE_A_UV_PROJECT,
  PHASE_B_SCRIPT,
  PHASE_C_SCRIPT,
  RUNNING_STYLE_CATEGORIES,
  resolveAutoCategoryConcurrency,
  resolveAutoMemoryLimit,
  resolveAutoPhaseAConcurrency,
  resolveAutoThreads,
  resolveRuntimeResourceOptions,
  type ChunkGranularity,
  type LocalResourceSnapshot,
} from "./generate-running-style-local";

const FIXED_NIGHT_DATE = new Date("2026-05-30T16:00:00Z"); // 01:00 JST -> inside window
const FIXED_DAY_DATE = new Date("2026-05-30T05:00:00Z"); // 14:00 JST -> outside window

describe("generate-running-style-local", () => {
  test("buildDefaultOptions returns initial empty strings and auto resources", () => {
    const options = buildDefaultOptions();
    expect(options.threads).toBe(AUTO_RESOURCE_VALUE);
    expect(options.memoryLimit).toBe("");
    expect(options.maxYearsPerRun).toBe(1);
  });

  test("buildDefaultOptions returns auto phaseAConcurrency", () => {
    const options = buildDefaultOptions();
    expect(options.phaseAConcurrency).toBe(AUTO_RESOURCE_VALUE);
  });

  test("buildDefaultOptions returns auto categoryConcurrency", () => {
    const options = buildDefaultOptions();
    expect(options.categoryConcurrency).toBe(DEFAULT_CATEGORY_CONCURRENCY);
  });

  test("buildDefaultOptions returns empty memoryLimitPerChunk so it falls back to memoryLimit", () => {
    const options = buildDefaultOptions();
    expect(options.memoryLimitPerChunk).toBe("");
  });

  test("buildDefaultOptions returns empty rsPFromFlatbinJra", () => {
    const options = buildDefaultOptions();
    expect(options.rsPFromFlatbinJra).toBe("");
  });

  test("resolveAutoMemoryLimit uses half of Colima memory with a 6GB floor", () => {
    expect(resolveAutoMemoryLimit({ cpu: 12, memoryGiB: 24, diskGiB: 100 })).toBe("12GB");
    expect(resolveAutoMemoryLimit({ cpu: 4, memoryGiB: 8, diskGiB: 100 })).toBe("6GB");
  });

  test("resolveAutoThreads shrinks under high load or low free memory", () => {
    const calm: LocalResourceSnapshot = {
      cpuCount: 15,
      load1m: 3.1,
      totalMemoryBytes: 48 * 1024 ** 3,
      freeMemoryBytes: 26 * 1024 ** 3,
    };
    const pressured: LocalResourceSnapshot = {
      ...calm,
      load1m: 13,
      freeMemoryBytes: 5 * 1024 ** 3,
    };
    expect(resolveAutoThreads({ cpu: 12, memoryGiB: 24, diskGiB: 100 }, calm)).toBeGreaterThan(1);
    expect(resolveAutoThreads({ cpu: 12, memoryGiB: 24, diskGiB: 100 }, pressured)).toBe(1);
  });

  test("resolveAutoThreads shrinks when macOS compressor is high", () => {
    const snapshot: LocalResourceSnapshot = {
      cpuCount: 15,
      load1m: 2,
      totalMemoryBytes: 48 * 1024 ** 3,
      freeMemoryBytes: 30 * 1024 ** 3,
      compressorBytes: 5 * 1024 ** 3,
    };
    expect(resolveAutoThreads({ cpu: 12, memoryGiB: 24, diskGiB: 100 }, snapshot)).toBe(1);
  });

  test("parseVmStatPages parses macOS page counters", () => {
    const pages = parseVmStatPages(
      "Pages free: 1,024.\nPages inactive: 2.\nPages occupied by compressor: 3.",
    );
    expect(pages.get("Pages free")).toBe(1024);
    expect(pages.get("Pages inactive")).toBe(2);
    expect(pages.get("Pages occupied by compressor")).toBe(3);
  });

  test("collectMacVmStatResourceSnapshot returns available and compressor bytes", () => {
    const execFile = vi.fn<(file: string, args: readonly string[]) => string>((file) => {
      if (file === "sysctl") return "4096\n";
      return [
        "Pages free: 10.",
        "Pages inactive: 20.",
        "Pages speculative: 30.",
        "Pages purgeable: 40.",
        "Pages occupied by compressor: 5.",
      ].join("\n");
    });
    expect(collectMacVmStatResourceSnapshot(execFile)).toStrictEqual({
      freeMemoryBytes: 100 * 4096,
      compressorBytes: 5 * 4096,
    });
  });

  test("resolveAutoPhaseAConcurrency leaves headroom for both categories", () => {
    const snapshot: LocalResourceSnapshot = {
      cpuCount: 15,
      load1m: 2,
      totalMemoryBytes: 48 * 1024 ** 3,
      freeMemoryBytes: 30 * 1024 ** 3,
    };
    expect(
      resolveAutoPhaseAConcurrency({ cpu: 12, memoryGiB: 24, diskGiB: 100 }, snapshot, 2),
    ).toBe(2);
  });

  test("resolveAutoCategoryConcurrency shrinks category-level parallelism under pressure", () => {
    const calm: LocalResourceSnapshot = {
      cpuCount: 15,
      load1m: 2,
      totalMemoryBytes: 48 * 1024 ** 3,
      freeMemoryBytes: 30 * 1024 ** 3,
    };
    const pressured: LocalResourceSnapshot = {
      ...calm,
      load1m: 8,
      freeMemoryBytes: 10 * 1024 ** 3,
    };
    expect(resolveAutoCategoryConcurrency({ cpu: 12, memoryGiB: 24, diskGiB: 100 }, calm)).toBe(
      RUNNING_STYLE_CATEGORIES.length,
    );
    expect(
      resolveAutoCategoryConcurrency({ cpu: 12, memoryGiB: 24, diskGiB: 100 }, pressured),
    ).toBe(1);
  });

  test("resolveRuntimeResourceOptions keeps explicit resource overrides", () => {
    const snapshot: LocalResourceSnapshot = {
      cpuCount: 15,
      load1m: 3,
      totalMemoryBytes: 48 * 1024 ** 3,
      freeMemoryBytes: 24 * 1024 ** 3,
    };
    const result = resolveRuntimeResourceOptions(
      {
        ...buildDefaultOptions(),
        threads: 3,
        memoryLimit: "9GB",
        phaseAConcurrency: 2,
        categoryConcurrency: 1,
      },
      { cpu: 12, memoryGiB: 24, diskGiB: 100 },
      snapshot,
    );
    expect(result.threads).toBe(3);
    expect(result.memoryLimit).toBe("9GB");
    expect(result.phaseAConcurrency).toBe(2);
    expect(result.categoryConcurrency).toBe(1);
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

  test("buildPhaseCCommand encodes running-style-feature-version flag", () => {
    const cmd = buildPhaseCCommand({
      logitsParquet: "/l",
      outputParquet: "/p",
      featureVersion: "v9",
    });
    const flagIndex = cmd.indexOf("--running-style-feature-version");
    expect(cmd[flagIndex + 1]).toBe("v9");
  });

  test("buildPhaseCCommand does NOT emit legacy --feature-version flag (Phase C accepts only --running-style-feature-version)", () => {
    const cmd = buildPhaseCCommand({
      logitsParquet: "/l",
      outputParquet: "/p",
      featureVersion: "v9",
    });
    expect(cmd.includes("--feature-version")).toBe(false);
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
        log: () => undefined,
        listDirectoryEntries: () => Promise.resolve([]),
        statFile: () => Promise.reject(new Error("ENOENT")),
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
        log: () => undefined,
        listDirectoryEntries: () => Promise.resolve([]),
        statFile: () => Promise.reject(new Error("ENOENT")),
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
      probeLocalResources: () =>
        Promise.resolve({
          cpuCount: 15,
          load1m: 1,
          totalMemoryBytes: 48 * 1024 ** 3,
          freeMemoryBytes: 36 * 1024 ** 3,
        }),
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
        log: () => undefined,
        listDirectoryEntries: () => Promise.resolve([]),
        statFile: () => Promise.reject(new Error("ENOENT")),
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
        log: () => undefined,
        listDirectoryEntries: () => Promise.resolve([]),
        statFile: () => Promise.reject(new Error("ENOENT")),
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
        log: () => undefined,
        listDirectoryEntries: () => Promise.resolve([]),
        statFile: () => Promise.reject(new Error("ENOENT")),
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

  test("DEFAULT_PHASE_A_CONCURRENCY is auto so runtime resources choose the cap", () => {
    expect(DEFAULT_PHASE_A_CONCURRENCY).toBe(AUTO_RESOURCE_VALUE);
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
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
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    expect(phaseAMemoryLimits.every((limit) => limit === "16GB")).toBe(true);
  });

  test("runGenerateRunningStyleLocal can start jra and nar concurrently when categoryConcurrency is 2", async () => {
    const phaseACategories: string[] = [];
    const gateHolder: { jra: () => void; nar: () => void } = {
      jra: () => undefined,
      nar: () => undefined,
    };
    const jraGate = new Promise<void>((resolve) => {
      gateHolder.jra = resolve;
    });
    const narGate = new Promise<void>((resolve) => {
      gateHolder.nar = resolve;
    });
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      async (command) => {
        if (command[5] === PHASE_A_SCRIPT) {
          const categoryIndex = command.indexOf("--category");
          const category = command[categoryIndex + 1] ?? "";
          phaseACategories.push(category);
          await (category === "jra" ? jraGate : narGate);
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
      phaseAConcurrency: 1,
      categoryConcurrency: 2,
      chunkGranularity: "year" as ChunkGranularity,
    };
    const runPromise = runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(phaseACategories.slice(0, 2).toSorted()).toStrictEqual(["jra", "nar"]);
    gateHolder.jra();
    gateHolder.nar();
    await runPromise;
  });

  test("DEFAULT_FORCE equals false so existing parquet is skipped by default", () => {
    expect(DEFAULT_FORCE).toBe(false);
  });

  test("buildDefaultOptions returns force=false", () => {
    expect(buildDefaultOptions().force).toBe(false);
  });

  test("parseArgs accepts --force 1 and sets force to true", () => {
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
      "--force",
      "1",
    ]);
    expect(options.force).toBe(true);
  });

  test("parseArgs accepts --force 0 and keeps force false", () => {
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
      "--force",
      "0",
    ]);
    expect(options.force).toBe(false);
  });

  test("buildChunkYearRange expands single-year chunk to one year", () => {
    expect(buildChunkYearRange({ yearFrom: 2024, yearTo: 2024 })).toStrictEqual([2024]);
  });

  test("buildChunkYearRange expands multi-year chunk into ascending sequence", () => {
    expect(buildChunkYearRange({ yearFrom: 2022, yearTo: 2024 })).toStrictEqual([2022, 2023, 2024]);
  });

  test("buildChunkYearDir composes race_year=YEAR subpath under features dir", () => {
    expect(buildChunkYearDir("/tmp/v1/features/category=jra", 2024)).toBe(
      "/tmp/v1/features/category=jra/race_year=2024",
    );
  });

  test("chunkHasExistingParquet returns true when every year directory has a parquet file", async () => {
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(["data_0.parquet"]),
    );
    const result = await chunkHasExistingParquet(listDirectoryEntries, {
      featuresDir: "/tmp/v1/features/category=jra",
      yearFrom: 2023,
      yearTo: 2024,
    });
    expect(result).toBe(true);
  });

  test("chunkHasExistingParquet returns false when at least one year directory is missing parquet", async () => {
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>((path) =>
      Promise.resolve(path.endsWith("race_year=2024") ? [] : ["data_0.parquet"]),
    );
    const result = await chunkHasExistingParquet(listDirectoryEntries, {
      featuresDir: "/tmp/v1/features/category=jra",
      yearFrom: 2023,
      yearTo: 2024,
    });
    expect(result).toBe(false);
  });

  test("chunkHasExistingParquet returns false when listing rejects (directory missing)", async () => {
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.reject(new Error("ENOENT")),
    );
    const result = await chunkHasExistingParquet(listDirectoryEntries, {
      featuresDir: "/tmp/v1/features/category=jra",
      yearFrom: 2023,
      yearTo: 2023,
    });
    expect(result).toBe(false);
  });

  test("chunkHasExistingParquet returns false when directory contains only non-parquet files", async () => {
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(["foo.txt", "bar.csv"]),
    );
    const result = await chunkHasExistingParquet(listDirectoryEntries, {
      featuresDir: "/tmp/v1/features/category=jra",
      yearFrom: 2024,
      yearTo: 2024,
    });
    expect(result).toBe(false);
  });

  test("runGenerateRunningStyleLocal Phase A skips chunks under year-granularity when listDirectoryEntries reports existing parquet", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const log = vi.fn<(message: string) => void>(() => undefined);
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(["data_0.parquet"]),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/resume-skip",
      chunkGranularity: "year" satisfies ChunkGranularity,
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log,
      listDirectoryEntries,
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    const phaseASpawnCount = spawn.mock.calls.filter(
      (call) => call[0][5] === PHASE_A_SCRIPT,
    ).length;
    expect(phaseASpawnCount).toBe(0);
  });

  test("runGenerateRunningStyleLocal Phase A logs a skip message per chunk when parquet exists under year-granularity", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const log = vi.fn<(message: string) => void>(() => undefined);
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(["data_0.parquet"]),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/resume-log",
      chunkGranularity: "year" satisfies ChunkGranularity,
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log,
      listDirectoryEntries,
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    const hadSkipLog = log.mock.calls.some(
      (call) => typeof call[0] === "string" && call[0].startsWith("[Phase A] skip "),
    );
    expect(hadSkipLog).toBe(true);
  });

  test("runGenerateRunningStyleLocal Phase A still spawns when force=true even if parquet exists", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const log = vi.fn<(message: string) => void>(() => undefined);
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(["data_0.parquet"]),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/resume-force",
      force: true,
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log,
      listDirectoryEntries,
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    const phaseASpawnCount = spawn.mock.calls.filter(
      (call) => call[0][5] === PHASE_A_SCRIPT,
    ).length;
    expect(phaseASpawnCount > 0).toBe(true);
  });

  test("runGenerateRunningStyleLocal Phase A spawns when parquet listing is empty (no existing data)", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const log = vi.fn<(message: string) => void>(() => undefined);
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve([]),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/resume-empty",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log,
      listDirectoryEntries,
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    const phaseASpawnCount = spawn.mock.calls.filter(
      (call) => call[0][5] === PHASE_A_SCRIPT,
    ).length;
    expect(phaseASpawnCount > 0).toBe(true);
  });

  test("CHUNK_GRANULARITIES lists year and month in that order", () => {
    expect(CHUNK_GRANULARITIES).toStrictEqual(["year", "month"]);
  });

  test("DEFAULT_CHUNK_GRANULARITY equals month so per-month spawn becomes the default", () => {
    expect(DEFAULT_CHUNK_GRANULARITY).toBe("month");
  });

  test("buildDefaultOptions returns chunkGranularity month by default", () => {
    expect(buildDefaultOptions().chunkGranularity).toBe("month");
  });

  test("MONTHS_PER_YEAR equals 12 so year-level resume waits for all 12 month parquet files", () => {
    expect(MONTHS_PER_YEAR).toBe(12);
  });

  test("ALL_MONTHS lists 1 through 12 in ascending order", () => {
    expect(ALL_MONTHS).toStrictEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
  });

  test("expandYearsToMonths expands one year into 12 entries", () => {
    expect(expandYearsToMonths([2024])).toStrictEqual([
      { year: 2024, month: 1 },
      { year: 2024, month: 2 },
      { year: 2024, month: 3 },
      { year: 2024, month: 4 },
      { year: 2024, month: 5 },
      { year: 2024, month: 6 },
      { year: 2024, month: 7 },
      { year: 2024, month: 8 },
      { year: 2024, month: 9 },
      { year: 2024, month: 10 },
      { year: 2024, month: 11 },
      { year: 2024, month: 12 },
    ]);
  });

  test("expandYearsToMonths concatenates months from multiple years preserving year order", () => {
    const result = expandYearsToMonths([2023, 2024]);
    expect(result.length).toBe(24);
    expect(result[0]).toStrictEqual({ year: 2023, month: 1 });
    expect(result[11]).toStrictEqual({ year: 2023, month: 12 });
    expect(result[12]).toStrictEqual({ year: 2024, month: 1 });
    expect(result[23]).toStrictEqual({ year: 2024, month: 12 });
  });

  test("expandYearsToMonths returns empty array for empty years input", () => {
    expect(expandYearsToMonths([])).toStrictEqual([]);
  });

  test("chunkYearsByMonth emits 12 chunks per year with year + month set", () => {
    const result = chunkYearsByMonth([2024]);
    expect(result.length).toBe(12);
    expect(result[0]).toStrictEqual({ year: 2024, month: 1 });
    expect(result[11]).toStrictEqual({ year: 2024, month: 12 });
  });

  test("chunkYearsByMonth returns empty when years is empty", () => {
    expect(chunkYearsByMonth([])).toStrictEqual([]);
  });

  test("buildPhaseACommand omits month tokens when monthFrom is null", () => {
    const cmd = buildPhaseACommand({
      pgUrl: "u",
      outputDir: "/d",
      featureVersion: "v1",
      yearFrom: 2024,
      yearTo: 2024,
      monthFrom: null,
      monthTo: null,
      category: "jra",
      threads: 8,
      memoryLimit: "16GB",
    });
    expect(cmd.includes("--month-from")).toBe(false);
    expect(cmd.includes("--month-to")).toBe(false);
  });

  test("buildPhaseACommand appends --month-from and --month-to when month chunk is requested", () => {
    const cmd = buildPhaseACommand({
      pgUrl: "u",
      outputDir: "/d",
      featureVersion: "v1",
      yearFrom: 2024,
      yearTo: 2024,
      monthFrom: 3,
      monthTo: 3,
      category: "jra",
      threads: 8,
      memoryLimit: "16GB",
    });
    const monthFromIndex = cmd.indexOf("--month-from");
    const monthToIndex = cmd.indexOf("--month-to");
    expect(cmd[monthFromIndex + 1]).toBe("3");
    expect(cmd[monthToIndex + 1]).toBe("3");
  });

  test("buildPhaseACommand month tokens appear after the year tokens", () => {
    const cmd = buildPhaseACommand({
      pgUrl: "u",
      outputDir: "/d",
      featureVersion: "v1",
      yearFrom: 2024,
      yearTo: 2024,
      monthFrom: 5,
      monthTo: 5,
      category: "nar",
      threads: 8,
      memoryLimit: "16GB",
    });
    const yearToIndex = cmd.indexOf("--year-to");
    const monthFromIndex = cmd.indexOf("--month-from");
    expect(monthFromIndex > yearToIndex).toBe(true);
  });

  test("parseArgs accepts --chunk-granularity month and stores it", () => {
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
      "--chunk-granularity",
      "month",
    ]);
    expect(options.chunkGranularity).toBe("month");
  });

  test("parseArgs accepts --chunk-granularity year and stores it", () => {
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
      "--chunk-granularity",
      "year",
    ]);
    expect(options.chunkGranularity).toBe("year");
  });

  test("parseArgs throws when --chunk-granularity is supplied without a value", () => {
    expect(() => parseArgs(["--chunk-granularity"])).toThrowError(
      "--chunk-granularity requires a value.",
    );
  });

  test("parseArgs throws when --chunk-granularity receives an unsupported value", () => {
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
        "--model-flatbin-nar",
        "/p/nar.flatbin",
        "--chunk-granularity",
        "week",
      ]),
    ).toThrowError("--chunk-granularity must be one of year, month.");
  });

  test("yearHasFullMonthParquet returns true when 12 parquet files exist in race_year dir", async () => {
    const twelveParquet: readonly string[] = [
      "data_01.parquet",
      "data_02.parquet",
      "data_03.parquet",
      "data_04.parquet",
      "data_05.parquet",
      "data_06.parquet",
      "data_07.parquet",
      "data_08.parquet",
      "data_09.parquet",
      "data_10.parquet",
      "data_11.parquet",
      "data_12.parquet",
    ];
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(twelveParquet),
    );
    const result = await yearHasFullMonthParquet(listDirectoryEntries, {
      featuresDir: "/tmp/v1/features/category=jra",
      year: 2024,
    });
    expect(result).toBe(true);
  });

  test("yearHasFullMonthParquet returns false when only 11 parquet files exist (incomplete year)", async () => {
    const elevenParquet: readonly string[] = [
      "data_01.parquet",
      "data_02.parquet",
      "data_03.parquet",
      "data_04.parquet",
      "data_05.parquet",
      "data_06.parquet",
      "data_07.parquet",
      "data_08.parquet",
      "data_09.parquet",
      "data_10.parquet",
      "data_11.parquet",
    ];
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(elevenParquet),
    );
    const result = await yearHasFullMonthParquet(listDirectoryEntries, {
      featuresDir: "/tmp/v1/features/category=jra",
      year: 2024,
    });
    expect(result).toBe(false);
  });

  test("yearHasFullMonthParquet ignores non-parquet entries when counting completion", async () => {
    const mixedEntries: readonly string[] = [
      "data_01.parquet",
      "data_02.parquet",
      "data_03.parquet",
      "data_04.parquet",
      "data_05.parquet",
      "data_06.parquet",
      "data_07.parquet",
      "data_08.parquet",
      "data_09.parquet",
      "data_10.parquet",
      "data_11.parquet",
      "data_12.parquet",
      "notes.txt",
      "summary.csv",
    ];
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(mixedEntries),
    );
    const result = await yearHasFullMonthParquet(listDirectoryEntries, {
      featuresDir: "/tmp/v1/features/category=jra",
      year: 2024,
    });
    expect(result).toBe(true);
  });

  test("yearHasFullMonthParquet returns false when listing rejects (directory missing)", async () => {
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.reject(new Error("ENOENT")),
    );
    const result = await yearHasFullMonthParquet(listDirectoryEntries, {
      featuresDir: "/tmp/v1/features/category=jra",
      year: 2024,
    });
    expect(result).toBe(false);
  });

  test("runGenerateRunningStyleLocal Phase A spawns month chunks by default with single-month --month-from/--month-to pairs", async () => {
    const monthPairs: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[5] === PHASE_A_SCRIPT) {
          const monthFromIndex = command.indexOf("--month-from");
          const monthToIndex = command.indexOf("--month-to");
          monthPairs.push(`${command[monthFromIndex + 1]}-${command[monthToIndex + 1]}`);
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
      outputRoot: "/tmp/month-default-chunk",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    expect(monthPairs.every((pair) => pair.split("-")[0] === pair.split("-")[1])).toBe(true);
  });

  test("runGenerateRunningStyleLocal Phase A under month granularity emits 12 spawns per year per category", async () => {
    const phaseASpawnCount = { count: 0 };
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[5] === PHASE_A_SCRIPT) {
          phaseASpawnCount.count += 1;
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
      outputRoot: "/tmp/month-count",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    expect(phaseASpawnCount.count).toBe(420);
  });

  test("runGenerateRunningStyleLocal Phase A under year granularity emits one spawn per year per category", async () => {
    const phaseASpawnCount = { count: 0 };
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[5] === PHASE_A_SCRIPT) {
          phaseASpawnCount.count += 1;
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
      outputRoot: "/tmp/year-count",
      chunkGranularity: "year" satisfies ChunkGranularity,
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    expect(phaseASpawnCount.count).toBe(35);
  });

  test("runGenerateRunningStyleLocal Phase A under month granularity emits --month-from/--month-to ranging from 1 to 12 per year", async () => {
    const phaseAMonthValues: number[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[5] === PHASE_A_SCRIPT) {
          const monthFromIndex = command.indexOf("--month-from");
          phaseAMonthValues.push(Number(command[monthFromIndex + 1] ?? "0"));
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
      outputRoot: "/tmp/month-range",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    const uniqueMonths = [...new Set(phaseAMonthValues)].toSorted((a, b) => a - b);
    expect(uniqueMonths).toStrictEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
  });

  test("runGenerateRunningStyleLocal Phase A under year granularity omits --month-from in spawned commands", async () => {
    const sawMonthFlag = { value: false };
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        if (command[5] === PHASE_A_SCRIPT && command.includes("--month-from")) {
          sawMonthFlag.value = true;
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
      outputRoot: "/tmp/year-no-month-flag",
      chunkGranularity: "year" satisfies ChunkGranularity,
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    expect(sawMonthFlag.value).toBe(false);
  });

  test("runGenerateRunningStyleLocal Phase A under month granularity skips a whole year when 12 parquet exist (year-level resume)", async () => {
    const twelveParquet: readonly string[] = [
      "data_01.parquet",
      "data_02.parquet",
      "data_03.parquet",
      "data_04.parquet",
      "data_05.parquet",
      "data_06.parquet",
      "data_07.parquet",
      "data_08.parquet",
      "data_09.parquet",
      "data_10.parquet",
      "data_11.parquet",
      "data_12.parquet",
    ];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const log = vi.fn<(message: string) => void>(() => undefined);
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(twelveParquet),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/month-skip",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log,
      listDirectoryEntries,
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    const phaseASpawnCount = spawn.mock.calls.filter(
      (call) => call[0][5] === PHASE_A_SCRIPT,
    ).length;
    expect(phaseASpawnCount).toBe(0);
  });

  test("runGenerateRunningStyleLocal Phase A under month granularity emits a year-level skip log message", async () => {
    const twelveParquet: readonly string[] = [
      "data_01.parquet",
      "data_02.parquet",
      "data_03.parquet",
      "data_04.parquet",
      "data_05.parquet",
      "data_06.parquet",
      "data_07.parquet",
      "data_08.parquet",
      "data_09.parquet",
      "data_10.parquet",
      "data_11.parquet",
      "data_12.parquet",
    ];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const log = vi.fn<(message: string) => void>(() => undefined);
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(twelveParquet),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/month-skip-log",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log,
      listDirectoryEntries,
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    const hadYearLevelSkipLog = log.mock.calls.some(
      (call) => typeof call[0] === "string" && call[0].includes("year-level resume"),
    );
    expect(hadYearLevelSkipLog).toBe(true);
  });

  test("runGenerateRunningStyleLocal Phase A under month granularity still spawns when fewer than 12 parquet found (incomplete year)", async () => {
    const onlyOneParquet: readonly string[] = ["data_01.parquet"];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const log = vi.fn<(message: string) => void>(() => undefined);
    const listDirectoryEntries = vi.fn<(path: string) => Promise<readonly string[]>>(() =>
      Promise.resolve(onlyOneParquet),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/month-skip-partial",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log,
      listDirectoryEntries,
      statFile: () => Promise.reject(new Error("ENOENT")),
    });
    const phaseASpawnCount = spawn.mock.calls.filter(
      (call) => call[0][5] === PHASE_A_SCRIPT,
    ).length;
    expect(phaseASpawnCount > 0).toBe(true);
  });

  test("runGenerateRunningStyleLocal Phase A under month granularity throws with year+month in error when spawn fails", async () => {
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
        log: () => undefined,
        listDirectoryEntries: () => Promise.resolve([]),
        statFile: () => Promise.reject(new Error("ENOENT")),
      }),
    ).rejects.toThrowError(/Phase A failed for category=(jra|nar) year=\d{4} month=\d+\./);
  });

  test("logitsFileHasContent returns true when statFile resolves with positive size", async () => {
    const statFile = vi.fn<(path: string) => Promise<{ size: number }>>(() =>
      Promise.resolve({ size: 56_623_104 }),
    );
    const result = await logitsFileHasContent(statFile, "/tmp/v1/logits/category=jra");
    expect(result).toBe(true);
  });

  test("logitsFileHasContent returns false when statFile resolves with zero size", async () => {
    const statFile = vi.fn<(path: string) => Promise<{ size: number }>>(() =>
      Promise.resolve({ size: 0 }),
    );
    const result = await logitsFileHasContent(statFile, "/tmp/v1/logits/category=nar");
    expect(result).toBe(false);
  });

  test("logitsFileHasContent returns false when statFile rejects (file missing)", async () => {
    const statFile = vi.fn<(path: string) => Promise<{ size: number }>>(() =>
      Promise.reject(new Error("ENOENT")),
    );
    const result = await logitsFileHasContent(statFile, "/tmp/v1/logits/category=jra");
    expect(result).toBe(false);
  });

  test("runGenerateRunningStyleLocal Phase B skips spawn when existing logits parquet has positive size", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const statFile = vi.fn<(path: string) => Promise<{ size: number }>>(() =>
      Promise.resolve({ size: 56_623_104 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/phase-b-resume-skip",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve(["data_0.parquet"]),
      statFile,
    });
    const phaseBSpawnCount = spawn.mock.calls.filter(
      (call) => call[0][2] === PHASE_B_SCRIPT,
    ).length;
    expect(phaseBSpawnCount).toBe(0);
  });

  test("runGenerateRunningStyleLocal Phase B logs a skip message when existing logits parquet has positive size", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const log = vi.fn<(message: string) => void>(() => undefined);
    const statFile = vi.fn<(path: string) => Promise<{ size: number }>>(() =>
      Promise.resolve({ size: 56_623_104 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/phase-b-resume-log",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log,
      listDirectoryEntries: () => Promise.resolve(["data_0.parquet"]),
      statFile,
    });
    const hadSkipLog = log.mock.calls.some(
      (call) => typeof call[0] === "string" && call[0].startsWith("[Phase B] skip "),
    );
    expect(hadSkipLog).toBe(true);
  });

  test("runGenerateRunningStyleLocal Phase B still spawns when statFile rejects (logits file absent)", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const statFile = vi.fn<(path: string) => Promise<{ size: number }>>(() =>
      Promise.reject(new Error("ENOENT")),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/phase-b-resume-absent",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile,
    });
    const phaseBSpawnCount = spawn.mock.calls.filter(
      (call) => call[0][2] === PHASE_B_SCRIPT,
    ).length;
    expect(phaseBSpawnCount).toBe(2);
  });

  test("runGenerateRunningStyleLocal Phase B still spawns when statFile resolves with zero size (empty file)", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const statFile = vi.fn<(path: string) => Promise<{ size: number }>>(() =>
      Promise.resolve({ size: 0 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/phase-b-resume-empty",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve([]),
      statFile,
    });
    const phaseBSpawnCount = spawn.mock.calls.filter(
      (call) => call[0][2] === PHASE_B_SCRIPT,
    ).length;
    expect(phaseBSpawnCount).toBe(2);
  });

  test("runGenerateRunningStyleLocal Phase B still spawns when force=true even if existing logits parquet has content", async () => {
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(() =>
      Promise.resolve({ exitCode: 0 }),
    );
    const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
    const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
      () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
    );
    const statFile = vi.fn<(path: string) => Promise<{ size: number }>>(() =>
      Promise.resolve({ size: 56_623_104 }),
    );
    const options = {
      ...buildDefaultOptions(),
      pgUrl: "u",
      modelVersionJra: "mj",
      modelVersionNar: "mn",
      modelFlatbinJra: "/p/jra.flatbin",
      modelFlatbinNar: "/p/nar.flatbin",
      ignoreNightWindow: true,
      outputRoot: "/tmp/phase-b-resume-force",
      force: true,
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
      log: () => undefined,
      listDirectoryEntries: () => Promise.resolve(["data_0.parquet"]),
      statFile,
    });
    const phaseBSpawnCount = spawn.mock.calls.filter(
      (call) => call[0][2] === PHASE_B_SCRIPT,
    ).length;
    expect(phaseBSpawnCount).toBe(2);
  });
});
