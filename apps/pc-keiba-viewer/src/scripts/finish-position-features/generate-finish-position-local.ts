// Run with: bun run src/scripts/finish-position-features/generate-finish-position-local.ts \
//   --pg-url $DATABASE_URL_LOCAL --running-style-feature-version v1 --finish-position-version v1
// Sequential 2-phase orchestrator (Agent G):
//   Phase A': spawn `uv run python src/scripts/generate_finish_position_features_local.py`
//   Phase B': spawn `uv run python src/scripts/score_finish_position_local.py`
// Writes manifest.json after both phases complete. No PG/R2/D1/KV writeback.

import { mkdir, readFile, stat, writeFile } from "node:fs/promises";

import { FINISH_POSITION_VERSION } from "./finish-position-version";
import {
  collectLocalResourceSnapshot,
  resolveAutoMemoryLimit,
  resolveAutoThreads,
  type LocalResourceSnapshot,
} from "./generate-running-style-local";
import { RUNNING_STYLE_FEATURE_VERSION } from "./running-style-feature-version";

interface CategoryModelVersions {
  jra: string;
  nar: string;
  banEi: string;
}

interface GenerateFinishPositionLocalOptions {
  pgUrl: string;
  runningStyleFeatureVersion: string;
  finishPositionVersion: string;
  runningStyleRoot: string;
  outputRoot: string;
  threads: number;
  memoryLimit: string;
  maxYearsPerRun: number;
  ignoreNightWindow: boolean;
  modelVersions: CategoryModelVersions;
}

interface SpawnResult {
  exitCode: number;
}

interface SpawnRunner {
  (command: readonly string[]): Promise<SpawnResult>;
}

interface SleepRunner {
  (durationMs: number): Promise<void>;
}

interface ColimaResource {
  cpu: number;
  memoryGiB: number;
  diskGiB: number;
}

interface ColimaProbe {
  (): Promise<ColimaResource>;
}

interface LocalResourceProbe {
  (): Promise<LocalResourceSnapshot>;
}

interface NowProvider {
  (): Date;
}

interface FsLike {
  mkdir(path: string): Promise<void>;
  writeFile(path: string, contents: string): Promise<void>;
  readFile(path: string): Promise<string>;
  pathExists(path: string): Promise<boolean>;
}

interface CategoryWindow {
  category: "jra" | "nar" | "ban-ei";
  years: readonly number[];
  modelVersion: string;
}

interface PhaseAArgs {
  pgUrl: string;
  runningStyleParquet: string;
  outputDir: string;
  finishPositionVersion: string;
  runningStyleFeatureVersion: string;
  yearFrom: number;
  yearTo: number;
  category: "jra" | "nar" | "ban-ei";
  threads: number;
  memoryLimit: string;
}

interface PhaseBArgs {
  featuresParquet: string;
  outputParquet: string;
  finishPositionVersion: string;
  runningStyleFeatureVersion: string;
  modelVersion: string;
  pgUrl: string;
  category: "jra" | "nar" | "ban-ei";
}

interface ManifestCategorySummary {
  yearFrom: number;
  yearTo: number;
  years: readonly number[];
  modelVersion: string;
}

interface FinishPositionManifest {
  finishPositionVersion: string;
  runningStyleFeatureVersion: string;
  generatedAt: string;
  outputRoot: string;
  featuresDir: string;
  predictionsDir: string;
  categories: Record<string, ManifestCategorySummary>;
}

interface RunningStyleManifestProbe {
  featureVersion?: string;
  runningStyleFeatureVersion?: string;
}

interface RunDeps {
  spawn: SpawnRunner;
  sleep: SleepRunner;
  probeColima: ColimaProbe;
  probeLocalResources?: LocalResourceProbe;
  now: NowProvider;
  fs: FsLike;
}

interface YearChunk {
  yearFrom: number;
  yearTo: number;
}

export const COLIMA_MIN_CPU = 8;
export const COLIMA_MIN_MEMORY_GIB = 24;
export const COLIMA_MIN_DISK_GIB = 100;
export const NIGHT_WINDOW_HOURS_JST: readonly number[] = [23, 0, 1, 2, 3, 4];
export const PER_YEAR_SLEEP_MS = 2000;
export const PER_CATEGORY_SLEEP_MS = 5000;
export const DEFAULT_RUNNING_STYLE_ROOT = "apps/pc-keiba-viewer/tmp/bucket-eval/running-style";
export const DEFAULT_OUTPUT_ROOT = "apps/pc-keiba-viewer/tmp/bucket-eval/finish-position";
export const AUTO_RESOURCE_VALUE = 0;
export const DEFAULT_THREADS = AUTO_RESOURCE_VALUE;
export const DEFAULT_MEMORY_LIMIT = "";
export const DEFAULT_MAX_YEARS_PER_RUN = 5;
export const PHASE_A_SCRIPT = "src/scripts/generate_finish_position_features_local.py";
export const PHASE_B_SCRIPT = "src/scripts/score_finish_position_local.py";
export const NIGHT_WINDOW_SET: ReadonlySet<number> = new Set(NIGHT_WINDOW_HOURS_JST);

const JRA_YEARS: readonly number[] = [
  2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021,
  2022, 2023, 2024, 2025, 2026,
];
const NAR_YEARS: readonly number[] = [
  2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2026,
];
const BAN_EI_YEARS: readonly number[] = [
  2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026,
];

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value.`);
  return value;
};

const parseBooleanFlag = (raw: string | undefined): boolean => raw === "1" || raw === "true";
const parseAutoInteger = (raw: string): number => {
  const normalized = raw.trim().toLowerCase();
  if (normalized === "auto") return AUTO_RESOURCE_VALUE;
  return Number(normalized);
};

export const buildDefaultOptions = (): GenerateFinishPositionLocalOptions => ({
  pgUrl: "",
  runningStyleFeatureVersion: RUNNING_STYLE_FEATURE_VERSION,
  finishPositionVersion: FINISH_POSITION_VERSION,
  runningStyleRoot: DEFAULT_RUNNING_STYLE_ROOT,
  outputRoot: DEFAULT_OUTPUT_ROOT,
  threads: DEFAULT_THREADS,
  memoryLimit: DEFAULT_MEMORY_LIMIT,
  maxYearsPerRun: DEFAULT_MAX_YEARS_PER_RUN,
  ignoreNightWindow: false,
  modelVersions: { jra: "", nar: "", banEi: "" },
});

const applyArg = (
  options: GenerateFinishPositionLocalOptions,
  name: string,
  value: string | undefined,
): { advanceBy: number } => {
  if (name === "--pg-url") {
    options.pgUrl = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--running-style-feature-version") {
    options.runningStyleFeatureVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--finish-position-version") {
    options.finishPositionVersion = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--running-style-root") {
    options.runningStyleRoot = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--output-root") {
    options.outputRoot = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--threads") {
    options.threads = parseAutoInteger(requireValue(name, value));
    return { advanceBy: 2 };
  }
  if (name === "--memory-limit") {
    options.memoryLimit = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--max-years-per-run") {
    options.maxYearsPerRun = Number(requireValue(name, value));
    return { advanceBy: 2 };
  }
  if (name === "--ignore-night-window") {
    options.ignoreNightWindow = parseBooleanFlag(value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version-jra") {
    options.modelVersions.jra = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version-nar") {
    options.modelVersions.nar = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version-ban-ei") {
    options.modelVersions.banEi = requireValue(name, value);
    return { advanceBy: 2 };
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgsRecursive = (
  options: GenerateFinishPositionLocalOptions,
  argv: readonly string[],
  cursor: number,
): GenerateFinishPositionLocalOptions => {
  if (cursor >= argv.length) return options;
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgsRecursive(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): GenerateFinishPositionLocalOptions => {
  const options = consumeArgsRecursive(buildDefaultOptions(), argv, 0);
  if (options.pgUrl === "") throw new Error("--pg-url is required.");
  if (options.modelVersions.jra === "") throw new Error("--model-version-jra is required.");
  if (options.modelVersions.nar === "") throw new Error("--model-version-nar is required.");
  if (options.modelVersions.banEi === "") throw new Error("--model-version-ban-ei is required.");
  return options;
};

export const isInsideNightWindow = (now: Date): boolean => {
  const jstHour = (now.getUTCHours() + 9) % 24;
  return NIGHT_WINDOW_SET.has(jstHour);
};

export const assertColimaCapacity = (resource: ColimaResource): void => {
  if (resource.cpu < COLIMA_MIN_CPU) {
    throw new Error(`Colima CPU ${resource.cpu} below minimum ${COLIMA_MIN_CPU}.`);
  }
  if (resource.memoryGiB < COLIMA_MIN_MEMORY_GIB) {
    throw new Error(
      `Colima memory ${resource.memoryGiB} GiB below minimum ${COLIMA_MIN_MEMORY_GIB} GiB.`,
    );
  }
  if (resource.diskGiB < COLIMA_MIN_DISK_GIB) {
    throw new Error(
      `Colima disk ${resource.diskGiB} GiB below minimum ${COLIMA_MIN_DISK_GIB} GiB.`,
    );
  }
};

export const resolveRuntimeResourceOptions = (
  options: GenerateFinishPositionLocalOptions,
  resource: ColimaResource,
  snapshot: LocalResourceSnapshot,
): GenerateFinishPositionLocalOptions => ({
  ...options,
  memoryLimit:
    options.memoryLimit === "" ? resolveAutoMemoryLimit(resource, snapshot) : options.memoryLimit,
  threads: options.threads <= 0 ? resolveAutoThreads(resource, snapshot) : options.threads,
});

export const buildCategoryWindows = (
  options: GenerateFinishPositionLocalOptions,
): readonly CategoryWindow[] => [
  { category: "jra", years: JRA_YEARS, modelVersion: options.modelVersions.jra },
  { category: "nar", years: NAR_YEARS, modelVersion: options.modelVersions.nar },
  { category: "ban-ei", years: BAN_EI_YEARS, modelVersion: options.modelVersions.banEi },
];

export const buildRunningStylePredictionsDir = (
  options: GenerateFinishPositionLocalOptions,
): string => `${options.runningStyleRoot}/${options.runningStyleFeatureVersion}/predictions`;

export const buildRunningStyleManifestPath = (
  options: GenerateFinishPositionLocalOptions,
): string => `${options.runningStyleRoot}/${options.runningStyleFeatureVersion}/manifest.json`;

export const buildFeaturesDir = (options: GenerateFinishPositionLocalOptions): string =>
  `${options.outputRoot}/${options.finishPositionVersion}/features`;

export const buildPredictionsDir = (options: GenerateFinishPositionLocalOptions): string =>
  `${options.outputRoot}/${options.finishPositionVersion}/predictions`;

export const buildManifestPath = (options: GenerateFinishPositionLocalOptions): string =>
  `${options.outputRoot}/${options.finishPositionVersion}/manifest.json`;

export const buildPhaseACommand = (args: PhaseAArgs): readonly string[] => [
  "uv",
  "run",
  "python",
  PHASE_A_SCRIPT,
  "--pg-url",
  args.pgUrl,
  "--running-style-parquet",
  args.runningStyleParquet,
  "--output-dir",
  args.outputDir,
  "--finish-position-version",
  args.finishPositionVersion,
  "--running-style-feature-version",
  args.runningStyleFeatureVersion,
  "--year-from",
  String(args.yearFrom),
  "--year-to",
  String(args.yearTo),
  "--category",
  args.category,
  "--threads",
  String(args.threads),
  "--memory-limit",
  args.memoryLimit,
];

export const buildPhaseBCommand = (args: PhaseBArgs): readonly string[] => [
  "uv",
  "run",
  "python",
  PHASE_B_SCRIPT,
  "--features-parquet",
  args.featuresParquet,
  "--output-parquet",
  args.outputParquet,
  "--finish-position-version",
  args.finishPositionVersion,
  "--running-style-feature-version",
  args.runningStyleFeatureVersion,
  "--model-version",
  args.modelVersion,
  "--pg-url",
  args.pgUrl,
  "--category",
  args.category,
];

const buildYearChunkAt = (
  years: readonly number[],
  offset: number,
  size: number,
): YearChunk | null => {
  const slice = years.slice(offset, offset + size);
  const head = slice[0];
  const tail = slice[slice.length - 1];
  if (head === undefined || tail === undefined) return null;
  return { yearFrom: head, yearTo: tail };
};

const buildChunkOffsets = (totalLength: number, chunkSize: number): number[] =>
  Array.from({ length: Math.ceil(totalLength / chunkSize) }, (_unused, index) => index * chunkSize);

export const chunkYears = (years: readonly number[], chunkSize: number): readonly YearChunk[] => {
  if (chunkSize <= 0) throw new Error("chunkSize must be positive.");
  const offsets = buildChunkOffsets(years.length, chunkSize);
  return offsets
    .map((offset) => buildYearChunkAt(years, offset, chunkSize))
    .filter((entry): entry is YearChunk => entry !== null);
};

const summarizeCategory = (window: CategoryWindow): ManifestCategorySummary => {
  const head = window.years[0];
  const tail = window.years[window.years.length - 1];
  if (head === undefined || tail === undefined) {
    throw new Error(`Empty year window for category ${window.category}.`);
  }
  return {
    yearFrom: head,
    yearTo: tail,
    years: window.years,
    modelVersion: window.modelVersion,
  };
};

const summariesByCategory = (
  windows: readonly CategoryWindow[],
): Record<string, ManifestCategorySummary> =>
  Object.fromEntries(windows.map((window) => [window.category, summarizeCategory(window)]));

export const buildManifest = (
  options: GenerateFinishPositionLocalOptions,
  generatedAt: Date,
): FinishPositionManifest => {
  const windows = buildCategoryWindows(options);
  return {
    finishPositionVersion: options.finishPositionVersion,
    runningStyleFeatureVersion: options.runningStyleFeatureVersion,
    generatedAt: generatedAt.toISOString(),
    outputRoot: options.outputRoot,
    featuresDir: buildFeaturesDir(options),
    predictionsDir: buildPredictionsDir(options),
    categories: summariesByCategory(windows),
  };
};

const extractRunningStyleFeatureVersion = (parsed: RunningStyleManifestProbe): string | undefined =>
  parsed.runningStyleFeatureVersion ?? parsed.featureVersion;

export const assertRunningStyleManifestMatches = (
  manifestJson: string,
  expectedVersion: string,
): void => {
  const parsed: RunningStyleManifestProbe = JSON.parse(manifestJson);
  const actual = extractRunningStyleFeatureVersion(parsed);
  if (actual === undefined) {
    throw new Error("Agent F manifest.json missing runningStyleFeatureVersion / featureVersion.");
  }
  if (actual !== expectedVersion) {
    throw new Error(
      `Agent F manifest runningStyleFeatureVersion ${actual} does not match expected ${expectedVersion}.`,
    );
  }
};

const assertRunningStyleInputsPresent = async (
  options: GenerateFinishPositionLocalOptions,
  fs: FsLike,
): Promise<void> => {
  const predictionsDir = buildRunningStylePredictionsDir(options);
  const manifestPath = buildRunningStyleManifestPath(options);
  const predictionsExists = await fs.pathExists(predictionsDir);
  if (!predictionsExists) {
    throw new Error(`Agent F predictions directory not found: ${predictionsDir}`);
  }
  const manifestExists = await fs.pathExists(manifestPath);
  if (!manifestExists) {
    throw new Error(`Agent F manifest.json not found: ${manifestPath}`);
  }
  const manifestJson = await fs.readFile(manifestPath);
  assertRunningStyleManifestMatches(manifestJson, options.runningStyleFeatureVersion);
};

const runPhaseAForChunk = async (
  deps: RunDeps,
  options: GenerateFinishPositionLocalOptions,
  window: CategoryWindow,
  chunk: YearChunk,
): Promise<void> => {
  const command = buildPhaseACommand({
    pgUrl: options.pgUrl,
    runningStyleParquet: buildRunningStylePredictionsDir(options),
    outputDir: buildFeaturesDir(options),
    finishPositionVersion: options.finishPositionVersion,
    runningStyleFeatureVersion: options.runningStyleFeatureVersion,
    yearFrom: chunk.yearFrom,
    yearTo: chunk.yearTo,
    category: window.category,
    threads: options.threads,
    memoryLimit: options.memoryLimit,
  });
  const result = await deps.spawn(command);
  if (result.exitCode !== 0) {
    throw new Error(
      `Phase A' failed for category=${window.category} years=${chunk.yearFrom}-${chunk.yearTo}.`,
    );
  }
  await deps.sleep(PER_YEAR_SLEEP_MS);
};

const runPhaseBForCategory = async (
  deps: RunDeps,
  options: GenerateFinishPositionLocalOptions,
  window: CategoryWindow,
): Promise<void> => {
  const command = buildPhaseBCommand({
    featuresParquet: buildFeaturesDir(options),
    outputParquet: buildPredictionsDir(options),
    finishPositionVersion: options.finishPositionVersion,
    runningStyleFeatureVersion: options.runningStyleFeatureVersion,
    modelVersion: window.modelVersion,
    pgUrl: options.pgUrl,
    category: window.category,
  });
  const result = await deps.spawn(command);
  if (result.exitCode !== 0) {
    throw new Error(`Phase B' failed for category=${window.category}.`);
  }
};

const runPhaseAForCategory = (
  deps: RunDeps,
  options: GenerateFinishPositionLocalOptions,
  window: CategoryWindow,
): Promise<void> => {
  const chunks = chunkYears(window.years, options.maxYearsPerRun);
  return chunks.reduce<Promise<void>>(
    (prev, chunk) => prev.then(() => runPhaseAForChunk(deps, options, window, chunk)),
    Promise.resolve(),
  );
};

const runCategory = async (
  deps: RunDeps,
  options: GenerateFinishPositionLocalOptions,
  window: CategoryWindow,
): Promise<void> => {
  await runPhaseAForCategory(deps, options, window);
  await runPhaseBForCategory(deps, options, window);
  await deps.sleep(PER_CATEGORY_SLEEP_MS);
};

const runAllCategoriesSequentially = (
  deps: RunDeps,
  options: GenerateFinishPositionLocalOptions,
  windows: readonly CategoryWindow[],
): Promise<void> =>
  windows.reduce<Promise<void>>(
    (prev, window) => prev.then(() => runCategory(deps, options, window)),
    Promise.resolve(),
  );

const writeManifest = async (
  options: GenerateFinishPositionLocalOptions,
  fs: FsLike,
  generatedAt: Date,
): Promise<void> => {
  const manifest = buildManifest(options, generatedAt);
  await fs.mkdir(`${options.outputRoot}/${options.finishPositionVersion}`);
  await fs.writeFile(buildManifestPath(options), JSON.stringify(manifest, null, 2));
};

export const runGenerateFinishPositionLocal = async (
  options: GenerateFinishPositionLocalOptions,
  deps: RunDeps,
): Promise<void> => {
  if (!options.ignoreNightWindow && !isInsideNightWindow(deps.now())) {
    throw new Error("Outside JST night window 23-04. Pass --ignore-night-window 1 to bypass.");
  }
  const resource = await deps.probeColima();
  assertColimaCapacity(resource);
  const snapshot =
    deps.probeLocalResources === undefined
      ? collectLocalResourceSnapshot()
      : await deps.probeLocalResources();
  const runtimeOptions = resolveRuntimeResourceOptions(options, resource, snapshot);
  await assertRunningStyleInputsPresent(options, deps.fs);
  const windows = buildCategoryWindows(runtimeOptions);
  await runAllCategoriesSequentially(deps, runtimeOptions, windows);
  await writeManifest(runtimeOptions, deps.fs, deps.now());
};

const buildBunSpawnRunner = (): SpawnRunner => async (command) => {
  const proc = Bun.spawn({ cmd: [...command], stdout: "inherit", stderr: "inherit" });
  const exitCode = await proc.exited;
  return { exitCode };
};

const buildBunSleepRunner = (): SleepRunner => (durationMs) =>
  new Promise((resolve) => setTimeout(resolve, durationMs));

const buildColimaProbe = (): ColimaProbe => async () => {
  const proc = Bun.spawn({ cmd: ["colima", "status", "--json"], stdout: "pipe", stderr: "pipe" });
  const stdout = await new Response(proc.stdout).text();
  const exitCode = await proc.exited;
  if (exitCode !== 0) throw new Error("colima status command failed.");
  const parsed: { cpu?: number; memory?: number; disk?: number } = JSON.parse(stdout);
  return {
    cpu: parsed.cpu ?? 0,
    memoryGiB: parsed.memory ?? 0,
    diskGiB: parsed.disk ?? 0,
  };
};

const buildBunNowProvider = (): NowProvider => () => new Date();

const buildBunFs = (): FsLike => ({
  mkdir: async (path) => {
    await mkdir(path, { recursive: true });
  },
  writeFile: async (path, contents) => {
    await writeFile(path, contents, "utf8");
  },
  readFile: async (path) => readFile(path, "utf8"),
  pathExists: async (path) => {
    try {
      await stat(path);
      return true;
    } catch {
      return false;
    }
  },
});

/* v8 ignore start */
if (import.meta.main) {
  const options = parseArgs(process.argv.slice(2));
  const deps: RunDeps = {
    spawn: buildBunSpawnRunner(),
    sleep: buildBunSleepRunner(),
    probeColima: buildColimaProbe(),
    now: buildBunNowProvider(),
    fs: buildBunFs(),
  };
  runGenerateFinishPositionLocal(options, deps).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
/* v8 ignore stop */
