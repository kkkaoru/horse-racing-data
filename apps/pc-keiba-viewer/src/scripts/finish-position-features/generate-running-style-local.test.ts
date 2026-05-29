// Run with: bun run --filter pc-keiba-viewer test
import { describe, expect, test, vi } from "vitest";

import {
  assertColimaCapacity,
  buildDefaultOptions,
  buildManifest,
  buildPhaseACommand,
  buildPhaseBCommand,
  chunkYears,
  isInsideNightWindow,
  parseArgs,
  runGenerateRunningStyleLocal,
  COLIMA_MIN_CPU,
  PER_CATEGORY_SLEEP_MS,
  PER_YEAR_SLEEP_MS,
  PHASE_A_SCRIPT,
  PHASE_B_SCRIPT,
} from "./generate-running-style-local";

const FIXED_NIGHT_DATE = new Date("2026-05-30T16:00:00Z"); // 01:00 JST -> inside window
const FIXED_DAY_DATE = new Date("2026-05-30T05:00:00Z"); // 14:00 JST -> outside window

describe("generate-running-style-local", () => {
  test("buildDefaultOptions returns initial empty strings and 8 threads", () => {
    const options = buildDefaultOptions();
    expect(options.threads).toBe(8);
    expect(options.memoryLimit).toBe("16GB");
    expect(options.maxYearsPerRun).toBe(5);
  });

  test("parseArgs throws when pg-url missing", () => {
    expect(() => parseArgs([])).toThrowError("--pg-url is required.");
  });

  test("parseArgs throws when model-version-jra missing", () => {
    expect(() => parseArgs(["--pg-url", "u"])).toThrowError("--model-version-jra is required.");
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
      "--model-version-ban-ei",
      "mb",
    ]);
    expect(options.threads).toBe(4);
    expect(options.ignoreNightWindow).toBe(true);
    expect(options.modelVersionJra).toBe("mj");
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

  test("buildPhaseACommand emits PHASE_A_SCRIPT path", () => {
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
    expect(cmd[3]).toBe(PHASE_A_SCRIPT);
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
    expect(cmd[11]).toBe("2020");
    expect(cmd[13]).toBe("2024");
  });

  test("buildPhaseBCommand uses PHASE_B_SCRIPT path", () => {
    const cmd = buildPhaseBCommand({
      featuresParquet: "/f",
      outputParquet: "/o",
      featureVersion: "v1",
      modelVersion: "m1",
      pgUrl: "u",
      category: "nar",
    });
    expect(cmd[3]).toBe(PHASE_B_SCRIPT);
  });

  test("buildPhaseBCommand encodes model version after --model-version", () => {
    const cmd = buildPhaseBCommand({
      featuresParquet: "/f",
      outputParquet: "/o",
      featureVersion: "v1",
      modelVersion: "m1",
      pgUrl: "u",
      category: "nar",
    });
    expect(cmd[11]).toBe("m1");
  });

  test("buildManifest summarizes three categories with year ranges", () => {
    const options = buildDefaultOptions();
    const manifest = buildManifest(
      {
        ...options,
        runningStyleFeatureVersion: "v1",
        outputRoot: "/tmp",
        modelVersionJra: "mj",
        modelVersionNar: "mn",
        modelVersionBanEi: "mb",
      },
      new Date("2026-05-30T15:00:00Z"),
    );
    expect(manifest.featureVersion).toBe("v1");
    expect(manifest.categories.jra?.modelVersion).toBe("mj");
    expect(manifest.categories.nar?.modelVersion).toBe("mn");
    expect(manifest.categories["ban-ei"]?.modelVersion).toBe("mb");
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
      modelVersionBanEi: "mb",
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
      modelVersionBanEi: "mb",
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

  test("runGenerateRunningStyleLocal spawns Phase A before Phase B", async () => {
    const spawnCalls: string[] = [];
    const spawn = vi.fn<(command: readonly string[]) => Promise<{ exitCode: number }>>(
      (command) => {
        spawnCalls.push(command[3] ?? "");
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
      modelVersionBanEi: "mb",
      ignoreNightWindow: true,
      outputRoot: "/tmp/test-out",
    };
    await runGenerateRunningStyleLocal(options, {
      spawn,
      sleep,
      probeColima,
      now: () => FIXED_NIGHT_DATE,
    });
    expect(spawnCalls[0]).toBe(PHASE_A_SCRIPT);
    const firstPhaseBIndex = spawnCalls.indexOf(PHASE_B_SCRIPT);
    const firstPhaseAIndex = spawnCalls.indexOf(PHASE_A_SCRIPT);
    expect(firstPhaseBIndex > firstPhaseAIndex).toBe(true);
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
      modelVersionBanEi: "mb",
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

  test("per-year sleep constant equals 2000", () => {
    expect(PER_YEAR_SLEEP_MS).toBe(2000);
  });

  test("per-category sleep constant equals 5000", () => {
    expect(PER_CATEGORY_SLEEP_MS).toBe(5000);
  });
});
