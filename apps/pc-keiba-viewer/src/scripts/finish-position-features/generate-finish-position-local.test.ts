// Run with: bun run --filter pc-keiba-viewer test
import { describe, expect, test, vi } from "vitest";

import {
  assertColimaCapacity,
  assertRunningStyleManifestMatches,
  AUTO_RESOURCE_VALUE,
  buildDefaultOptions,
  buildFeaturesDir,
  buildManifest,
  buildManifestPath,
  buildPhaseACommand,
  buildPhaseBCommand,
  buildPredictionsDir,
  buildRunningStyleManifestPath,
  buildRunningStylePredictionsDir,
  chunkYears,
  COLIMA_MIN_CPU,
  isInsideNightWindow,
  parseArgs,
  PER_CATEGORY_SLEEP_MS,
  PER_YEAR_SLEEP_MS,
  PHASE_A_SCRIPT,
  PHASE_B_SCRIPT,
  resolveRuntimeResourceOptions,
  runGenerateFinishPositionLocal,
} from "./generate-finish-position-local";

const FIXED_NIGHT_DATE = new Date("2026-05-30T16:00:00Z"); // 01:00 JST -> inside window
const FIXED_DAY_DATE = new Date("2026-05-30T05:00:00Z"); // 14:00 JST -> outside window
const CALM_LOCAL_SNAPSHOT = {
  cpuCount: 8,
  load1m: 1,
  totalMemoryBytes: 64 * 1024 ** 3,
  freeMemoryBytes: 40 * 1024 ** 3,
  compressorBytes: 0,
};

const buildFakeFs = (
  manifestJson: string,
  predictionsExists: boolean,
  manifestExists: boolean,
) => ({
  mkdir: vi.fn<(path: string) => Promise<void>>(() => Promise.resolve()),
  writeFile: vi.fn<(path: string, contents: string) => Promise<void>>(() => Promise.resolve()),
  readFile: vi.fn<(path: string) => Promise<string>>(() => Promise.resolve(manifestJson)),
  pathExists: vi.fn<(path: string) => Promise<boolean>>((path) =>
    Promise.resolve(path.endsWith("manifest.json") ? manifestExists : predictionsExists),
  ),
});

const buildRunOptions = () => ({
  ...buildDefaultOptions(),
  pgUrl: "u",
  modelVersions: { jra: "mj", nar: "mn", banEi: "mb" },
  ignoreNightWindow: true,
});

test("buildDefaultOptions returns auto resources / max 5 years per run", () => {
  const options = buildDefaultOptions();
  expect(options.threads).toBe(AUTO_RESOURCE_VALUE);
  expect(options.memoryLimit).toBe("");
  expect(options.maxYearsPerRun).toBe(5);
});

test("buildDefaultOptions reads finishPositionVersion v1 from JSON SSoT", () => {
  expect(buildDefaultOptions().finishPositionVersion).toBe("v1");
});

test("buildDefaultOptions reads runningStyleFeatureVersion v1 from JSON SSoT", () => {
  expect(buildDefaultOptions().runningStyleFeatureVersion).toBe("v1");
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

test("parseArgs throws when model-version-ban-ei missing", () => {
  expect(() =>
    parseArgs(["--pg-url", "u", "--model-version-jra", "mj", "--model-version-nar", "mn"]),
  ).toThrowError("--model-version-ban-ei is required.");
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
    "--finish-position-version",
    "v1",
    "--threads",
    "4",
    "--memory-limit",
    "8GB",
    "--max-years-per-run",
    "3",
    "--ignore-night-window",
    "1",
    "--running-style-root",
    "/tmp/rs",
    "--output-root",
    "/tmp/fp",
    "--model-version-jra",
    "mj",
    "--model-version-nar",
    "mn",
    "--model-version-ban-ei",
    "mb",
  ]);
  expect(options.threads).toBe(4);
  expect(options.ignoreNightWindow).toBe(true);
  expect(options.modelVersions.jra).toBe("mj");
  expect(options.runningStyleRoot).toBe("/tmp/rs");
  expect(options.outputRoot).toBe("/tmp/fp");
});

test("parseArgs accepts --threads auto", () => {
  const options = parseArgs([
    "--pg-url",
    "postgres://u",
    "--threads",
    "auto",
    "--model-version-jra",
    "mj",
    "--model-version-nar",
    "mn",
    "--model-version-ban-ei",
    "mb",
  ]);
  expect(options.threads).toBe(AUTO_RESOURCE_VALUE);
});

test("resolveRuntimeResourceOptions uses current local resource snapshot for auto values", () => {
  const options = resolveRuntimeResourceOptions(
    buildRunOptions(),
    { cpu: 8, memoryGiB: 24, diskGiB: 100 },
    CALM_LOCAL_SNAPSHOT,
  );
  expect(options.memoryLimit).toBe("12GB");
  expect(options.threads).toBe(7);
});

test("resolveRuntimeResourceOptions keeps explicit resource overrides", () => {
  const options = resolveRuntimeResourceOptions(
    { ...buildRunOptions(), threads: 3, memoryLimit: "7GB" },
    { cpu: 8, memoryGiB: 24, diskGiB: 100 },
    CALM_LOCAL_SNAPSHOT,
  );
  expect(options.memoryLimit).toBe("7GB");
  expect(options.threads).toBe(3);
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

test("buildPhaseACommand puts PHASE_A_SCRIPT at index 3", () => {
  const cmd = buildPhaseACommand({
    pgUrl: "u",
    runningStyleParquet: "/p",
    outputDir: "/d",
    finishPositionVersion: "v1",
    runningStyleFeatureVersion: "v1",
    yearFrom: 2020,
    yearTo: 2024,
    category: "jra",
    threads: 8,
    memoryLimit: "16GB",
  });
  expect(cmd[3]).toBe(PHASE_A_SCRIPT);
});

test("buildPhaseACommand encodes running-style parquet immediately after --running-style-parquet", () => {
  const cmd = buildPhaseACommand({
    pgUrl: "u",
    runningStyleParquet: "/p",
    outputDir: "/d",
    finishPositionVersion: "v1",
    runningStyleFeatureVersion: "v1",
    yearFrom: 2020,
    yearTo: 2024,
    category: "jra",
    threads: 8,
    memoryLimit: "16GB",
  });
  expect(cmd).toStrictEqual([
    "uv",
    "run",
    "python",
    "src/scripts/generate_finish_position_features_local.py",
    "--pg-url",
    "u",
    "--running-style-parquet",
    "/p",
    "--output-dir",
    "/d",
    "--finish-position-version",
    "v1",
    "--running-style-feature-version",
    "v1",
    "--year-from",
    "2020",
    "--year-to",
    "2024",
    "--category",
    "jra",
    "--threads",
    "8",
    "--memory-limit",
    "16GB",
  ]);
});

test("buildPhaseBCommand puts PHASE_B_SCRIPT at index 3", () => {
  const cmd = buildPhaseBCommand({
    featuresParquet: "/f",
    outputParquet: "/o",
    finishPositionVersion: "v1",
    runningStyleFeatureVersion: "v1",
    modelVersion: "m1",
    pgUrl: "u",
    category: "nar",
  });
  expect(cmd[3]).toBe(PHASE_B_SCRIPT);
});

test("buildPhaseBCommand encodes the full positional command for nar", () => {
  const cmd = buildPhaseBCommand({
    featuresParquet: "/f",
    outputParquet: "/o",
    finishPositionVersion: "v1",
    runningStyleFeatureVersion: "v1",
    modelVersion: "m1",
    pgUrl: "u",
    category: "nar",
  });
  expect(cmd).toStrictEqual([
    "uv",
    "run",
    "python",
    "src/scripts/score_finish_position_local.py",
    "--features-parquet",
    "/f",
    "--output-parquet",
    "/o",
    "--finish-position-version",
    "v1",
    "--running-style-feature-version",
    "v1",
    "--model-version",
    "m1",
    "--pg-url",
    "u",
    "--category",
    "nar",
  ]);
});

test("buildRunningStylePredictionsDir uses runningStyleRoot and version", () => {
  const options = {
    ...buildDefaultOptions(),
    runningStyleRoot: "/tmp/rs",
    runningStyleFeatureVersion: "v1",
  };
  expect(buildRunningStylePredictionsDir(options)).toBe("/tmp/rs/v1/predictions");
});

test("buildRunningStyleManifestPath uses runningStyleRoot and version", () => {
  const options = {
    ...buildDefaultOptions(),
    runningStyleRoot: "/tmp/rs",
    runningStyleFeatureVersion: "v1",
  };
  expect(buildRunningStyleManifestPath(options)).toBe("/tmp/rs/v1/manifest.json");
});

test("buildFeaturesDir uses outputRoot and finishPositionVersion", () => {
  const options = {
    ...buildDefaultOptions(),
    outputRoot: "/tmp/fp",
    finishPositionVersion: "v1",
  };
  expect(buildFeaturesDir(options)).toBe("/tmp/fp/v1/features");
});

test("buildPredictionsDir uses outputRoot and finishPositionVersion", () => {
  const options = {
    ...buildDefaultOptions(),
    outputRoot: "/tmp/fp",
    finishPositionVersion: "v1",
  };
  expect(buildPredictionsDir(options)).toBe("/tmp/fp/v1/predictions");
});

test("buildManifestPath joins outputRoot, version and manifest.json", () => {
  const options = {
    ...buildDefaultOptions(),
    outputRoot: "/tmp/fp",
    finishPositionVersion: "v1",
  };
  expect(buildManifestPath(options)).toBe("/tmp/fp/v1/manifest.json");
});

test("buildManifest carries finishPositionVersion and runningStyleFeatureVersion", () => {
  const options = {
    ...buildDefaultOptions(),
    runningStyleFeatureVersion: "v1",
    finishPositionVersion: "v1",
    outputRoot: "/tmp/fp",
    modelVersions: { jra: "mj", nar: "mn", banEi: "mb" },
  };
  const manifest = buildManifest(options, new Date("2026-05-30T15:00:00Z"));
  expect(manifest.finishPositionVersion).toBe("v1");
  expect(manifest.runningStyleFeatureVersion).toBe("v1");
  expect(manifest.featuresDir).toBe("/tmp/fp/v1/features");
  expect(manifest.predictionsDir).toBe("/tmp/fp/v1/predictions");
  expect(manifest.categories.jra?.modelVersion).toBe("mj");
  expect(manifest.categories.nar?.modelVersion).toBe("mn");
  expect(manifest.categories["ban-ei"]?.modelVersion).toBe("mb");
});

test("assertRunningStyleManifestMatches accepts featureVersion field name", () => {
  expect(() =>
    assertRunningStyleManifestMatches('{"featureVersion":"v1"}', "v1"),
  ).not.toThrowError();
});

test("assertRunningStyleManifestMatches accepts runningStyleFeatureVersion field name", () => {
  expect(() =>
    assertRunningStyleManifestMatches('{"runningStyleFeatureVersion":"v1"}', "v1"),
  ).not.toThrowError();
});

test("assertRunningStyleManifestMatches throws when neither field present", () => {
  expect(() => assertRunningStyleManifestMatches("{}", "v1")).toThrowError(
    "Agent F manifest.json missing runningStyleFeatureVersion / featureVersion.",
  );
});

test("assertRunningStyleManifestMatches throws on version mismatch", () => {
  expect(() => assertRunningStyleManifestMatches('{"featureVersion":"v2"}', "v1")).toThrowError(
    "Agent F manifest runningStyleFeatureVersion v2 does not match expected v1.",
  );
});

test("runGenerateFinishPositionLocal aborts outside night window when guard active", async () => {
  const spawn = vi.fn<() => Promise<{ exitCode: number }>>();
  const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
  const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>();
  const options = { ...buildRunOptions(), ignoreNightWindow: false };
  await expect(
    runGenerateFinishPositionLocal(options, {
      spawn,
      sleep,
      probeColima,
      probeLocalResources: () => Promise.resolve(CALM_LOCAL_SNAPSHOT),
      now: () => FIXED_DAY_DATE,
      fs: buildFakeFs('{"featureVersion":"v1"}', true, true),
    }),
  ).rejects.toThrowError("Outside JST night window 23-04. Pass --ignore-night-window 1 to bypass.");
  expect(spawn).not.toHaveBeenCalled();
});

test("runGenerateFinishPositionLocal aborts when Colima resources insufficient", async () => {
  const spawn = vi.fn<() => Promise<{ exitCode: number }>>();
  const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
  const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
    () => Promise.resolve({ cpu: 2, memoryGiB: 24, diskGiB: 100 }),
  );
  await expect(
    runGenerateFinishPositionLocal(buildRunOptions(), {
      spawn,
      sleep,
      probeColima,
      probeLocalResources: () => Promise.resolve(CALM_LOCAL_SNAPSHOT),
      now: () => FIXED_DAY_DATE,
      fs: buildFakeFs('{"featureVersion":"v1"}', true, true),
    }),
  ).rejects.toThrowError("Colima CPU 2 below minimum 8.");
  expect(spawn).not.toHaveBeenCalled();
});

test("runGenerateFinishPositionLocal aborts when Agent F predictions dir missing", async () => {
  const spawn = vi.fn<() => Promise<{ exitCode: number }>>();
  const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
  const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
    () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
  );
  await expect(
    runGenerateFinishPositionLocal(buildRunOptions(), {
      spawn,
      sleep,
      probeColima,
      probeLocalResources: () => Promise.resolve(CALM_LOCAL_SNAPSHOT),
      now: () => FIXED_NIGHT_DATE,
      fs: buildFakeFs('{"featureVersion":"v1"}', false, true),
    }),
  ).rejects.toThrowError("Agent F predictions directory not found");
  expect(spawn).not.toHaveBeenCalled();
});

test("runGenerateFinishPositionLocal aborts when Agent F manifest missing", async () => {
  const spawn = vi.fn<() => Promise<{ exitCode: number }>>();
  const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
  const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
    () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
  );
  await expect(
    runGenerateFinishPositionLocal(buildRunOptions(), {
      spawn,
      sleep,
      probeColima,
      probeLocalResources: () => Promise.resolve(CALM_LOCAL_SNAPSHOT),
      now: () => FIXED_NIGHT_DATE,
      fs: buildFakeFs('{"featureVersion":"v1"}', true, false),
    }),
  ).rejects.toThrowError("Agent F manifest.json not found");
  expect(spawn).not.toHaveBeenCalled();
});

test("runGenerateFinishPositionLocal aborts on Agent F manifest version mismatch", async () => {
  const spawn = vi.fn<() => Promise<{ exitCode: number }>>();
  const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
  const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
    () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
  );
  await expect(
    runGenerateFinishPositionLocal(buildRunOptions(), {
      spawn,
      sleep,
      probeColima,
      probeLocalResources: () => Promise.resolve(CALM_LOCAL_SNAPSHOT),
      now: () => FIXED_NIGHT_DATE,
      fs: buildFakeFs('{"featureVersion":"v2"}', true, true),
    }),
  ).rejects.toThrowError(
    "Agent F manifest runningStyleFeatureVersion v2 does not match expected v1.",
  );
  expect(spawn).not.toHaveBeenCalled();
});

test("runGenerateFinishPositionLocal spawns Phase A before Phase B", async () => {
  const spawnCalls: string[] = [];
  const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>((command) => {
    spawnCalls.push(command[3] ?? "");
    return Promise.resolve({ exitCode: 0 });
  });
  const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
  const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
    () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
  );
  await runGenerateFinishPositionLocal(buildRunOptions(), {
    spawn,
    sleep,
    probeColima,
    probeLocalResources: () => Promise.resolve(CALM_LOCAL_SNAPSHOT),
    now: () => FIXED_NIGHT_DATE,
    fs: buildFakeFs('{"featureVersion":"v1"}', true, true),
  });
  expect(spawnCalls[0]).toBe(PHASE_A_SCRIPT);
  // First Phase B' invocation (JRA) must come AFTER all JRA Phase A' chunks.
  const firstPhaseBIndex = spawnCalls.indexOf(PHASE_B_SCRIPT);
  const phaseACountBeforeFirstB = spawnCalls
    .slice(0, firstPhaseBIndex)
    .filter((script) => script === PHASE_A_SCRIPT).length;
  expect(phaseACountBeforeFirstB > 0).toBe(true);
  expect(firstPhaseBIndex > 0).toBe(true);
});

test("runGenerateFinishPositionLocal throws when Phase A spawn returns non-zero", async () => {
  const spawn = vi.fn<() => Promise<{ exitCode: number }>>(() => Promise.resolve({ exitCode: 1 }));
  const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
  const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
    () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
  );
  await expect(
    runGenerateFinishPositionLocal(buildRunOptions(), {
      spawn,
      sleep,
      probeColima,
      probeLocalResources: () => Promise.resolve(CALM_LOCAL_SNAPSHOT),
      now: () => FIXED_NIGHT_DATE,
      fs: buildFakeFs('{"featureVersion":"v1"}', true, true),
    }),
  ).rejects.toThrowError("Phase A' failed");
});

test("runGenerateFinishPositionLocal throws when Phase B spawn returns non-zero", async () => {
  let invocation = 0;
  const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>((command) => {
    invocation += 1;
    const exitCode = command[3] === PHASE_B_SCRIPT ? 1 : 0;
    return Promise.resolve({ exitCode });
  });
  const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
  const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
    () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
  );
  await expect(
    runGenerateFinishPositionLocal(buildRunOptions(), {
      spawn,
      sleep,
      probeColima,
      probeLocalResources: () => Promise.resolve(CALM_LOCAL_SNAPSHOT),
      now: () => FIXED_NIGHT_DATE,
      fs: buildFakeFs('{"featureVersion":"v1"}', true, true),
    }),
  ).rejects.toThrowError("Phase B' failed");
  expect(invocation > 0).toBe(true);
});

test("runGenerateFinishPositionLocal writes manifest.json with both versions after both phases", async () => {
  const spawn = vi.fn<() => Promise<{ exitCode: number }>>(() => Promise.resolve({ exitCode: 0 }));
  const sleep = vi.fn<() => Promise<void>>(() => Promise.resolve());
  const probeColima = vi.fn<() => Promise<{ cpu: number; memoryGiB: number; diskGiB: number }>>(
    () => Promise.resolve({ cpu: 8, memoryGiB: 24, diskGiB: 100 }),
  );
  const fs = buildFakeFs('{"featureVersion":"v1"}', true, true);
  await runGenerateFinishPositionLocal(buildRunOptions(), {
    spawn,
    sleep,
    probeColima,
    probeLocalResources: () => Promise.resolve(CALM_LOCAL_SNAPSHOT),
    now: () => FIXED_NIGHT_DATE,
    fs,
  });
  expect(fs.writeFile).toHaveBeenCalledTimes(1);
  const manifestContents = fs.writeFile.mock.calls[0]?.[1] ?? "";
  const parsed: {
    finishPositionVersion: string;
    runningStyleFeatureVersion: string;
  } = JSON.parse(manifestContents);
  expect(parsed.finishPositionVersion).toBe("v1");
  expect(parsed.runningStyleFeatureVersion).toBe("v1");
});

test("per-year sleep constant equals 2000", () => {
  expect(PER_YEAR_SLEEP_MS).toBe(2000);
});

test("per-category sleep constant equals 5000", () => {
  expect(PER_CATEGORY_SLEEP_MS).toBe(5000);
});

describe("constants", () => {
  test("PHASE_A_SCRIPT points at the Agent G feature-builder script", () => {
    expect(PHASE_A_SCRIPT).toBe("src/scripts/generate_finish_position_features_local.py");
  });

  test("PHASE_B_SCRIPT points at the Agent G scoring script", () => {
    expect(PHASE_B_SCRIPT).toBe("src/scripts/score_finish_position_local.py");
  });
});
