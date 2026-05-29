// Run with: bun run src/scripts/finish-position-features/generate-running-style-local.ts \
//   --pg-url $DATABASE_URL_LOCAL --running-style-feature-version v1
// Sequential 2-phase orchestrator:
//   Phase A: spawn `uv run python src/scripts/generate_running_style_features_local.py`
//   Phase B: spawn `uv run python src/scripts/score_running_style_local.py`
// Writes manifest.json after both phases complete. No PG/R2/D1/KV writeback.

import { mkdir, writeFile } from "node:fs/promises";

import { RUNNING_STYLE_FEATURE_VERSION } from "./running-style-feature-version";

interface GenerateRunningStyleLocalOptions {
  pgUrl: string;
  runningStyleFeatureVersion: string;
  threads: number;
  memoryLimit: string;
  maxYearsPerRun: number;
  ignoreNightWindow: boolean;
  outputRoot: string;
  modelVersionJra: string;
  modelVersionNar: string;
  modelVersionBanEi: string;
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

interface NowProvider {
  (): Date;
}

interface CategoryWindow {
  category: "jra" | "nar" | "ban-ei";
  years: readonly number[];
  modelVersion: string;
}

interface PhaseAArgs {
  pgUrl: string;
  outputDir: string;
  featureVersion: string;
  yearFrom: number;
  yearTo: number;
  category: "jra" | "nar" | "ban-ei";
  threads: number;
  memoryLimit: string;
}

interface PhaseBArgs {
  featuresParquet: string;
  outputParquet: string;
  featureVersion: string;
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

interface Manifest {
  featureVersion: string;
  generatedAt: string;
  outputRoot: string;
  categories: Record<string, ManifestCategorySummary>;
}

interface RunDeps {
  spawn: SpawnRunner;
  sleep: SleepRunner;
  probeColima: ColimaProbe;
  now: NowProvider;
}

export const COLIMA_MIN_CPU = 8;
export const COLIMA_MIN_MEMORY_GIB = 24;
export const COLIMA_MIN_DISK_GIB = 100;
export const NIGHT_WINDOW_HOURS_JST: readonly number[] = [23, 0, 1, 2, 3, 4];
export const PER_YEAR_SLEEP_MS = 2000;
export const PER_CATEGORY_SLEEP_MS = 5000;
export const DEFAULT_OUTPUT_ROOT = "apps/pc-keiba-viewer/tmp/bucket-eval/running-style";
export const DEFAULT_THREADS = 8;
export const DEFAULT_MEMORY_LIMIT = "16GB";
export const DEFAULT_MAX_YEARS_PER_RUN = 5;
export const PHASE_A_SCRIPT = "src/scripts/generate_running_style_features_local.py";
export const PHASE_B_SCRIPT = "src/scripts/score_running_style_local.py";
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

export const buildDefaultOptions = (): GenerateRunningStyleLocalOptions => ({
  pgUrl: "",
  runningStyleFeatureVersion: RUNNING_STYLE_FEATURE_VERSION,
  threads: DEFAULT_THREADS,
  memoryLimit: DEFAULT_MEMORY_LIMIT,
  maxYearsPerRun: DEFAULT_MAX_YEARS_PER_RUN,
  ignoreNightWindow: false,
  outputRoot: DEFAULT_OUTPUT_ROOT,
  modelVersionJra: "",
  modelVersionNar: "",
  modelVersionBanEi: "",
});

const applyArg = (
  options: GenerateRunningStyleLocalOptions,
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
  if (name === "--threads") {
    options.threads = Number(requireValue(name, value));
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
  if (name === "--output-root") {
    options.outputRoot = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version-jra") {
    options.modelVersionJra = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version-nar") {
    options.modelVersionNar = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-version-ban-ei") {
    options.modelVersionBanEi = requireValue(name, value);
    return { advanceBy: 2 };
  }
  throw new Error(`Unknown argument: ${name}`);
};

const consumeArgsRecursive = (
  options: GenerateRunningStyleLocalOptions,
  argv: readonly string[],
  cursor: number,
): GenerateRunningStyleLocalOptions => {
  if (cursor >= argv.length) return options;
  const name = argv[cursor];
  if (name === undefined) return options;
  const { advanceBy } = applyArg(options, name, argv[cursor + 1]);
  return consumeArgsRecursive(options, argv, cursor + advanceBy);
};

export const parseArgs = (argv: readonly string[]): GenerateRunningStyleLocalOptions => {
  const options = consumeArgsRecursive(buildDefaultOptions(), argv, 0);
  if (options.pgUrl === "") throw new Error("--pg-url is required.");
  if (options.modelVersionJra === "") throw new Error("--model-version-jra is required.");
  if (options.modelVersionNar === "") throw new Error("--model-version-nar is required.");
  if (options.modelVersionBanEi === "") throw new Error("--model-version-ban-ei is required.");
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

export const buildCategoryWindows = (
  options: GenerateRunningStyleLocalOptions,
): readonly CategoryWindow[] => [
  { category: "jra", years: JRA_YEARS, modelVersion: options.modelVersionJra },
  { category: "nar", years: NAR_YEARS, modelVersion: options.modelVersionNar },
  { category: "ban-ei", years: BAN_EI_YEARS, modelVersion: options.modelVersionBanEi },
];

export const buildPhaseACommand = (args: PhaseAArgs): readonly string[] => [
  "uv",
  "run",
  "python",
  PHASE_A_SCRIPT,
  "--pg-url",
  args.pgUrl,
  "--output-dir",
  args.outputDir,
  "--running-style-feature-version",
  args.featureVersion,
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
  "--running-style-feature-version",
  args.featureVersion,
  "--model-version",
  args.modelVersion,
  "--pg-url",
  args.pgUrl,
  "--category",
  args.category,
];

interface YearChunk {
  yearFrom: number;
  yearTo: number;
}

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

const buildFeaturesDir = (options: GenerateRunningStyleLocalOptions): string =>
  `${options.outputRoot}/${options.runningStyleFeatureVersion}/features`;

const buildPredictionsDir = (options: GenerateRunningStyleLocalOptions): string =>
  `${options.outputRoot}/${options.runningStyleFeatureVersion}/predictions`;

const buildManifestPath = (options: GenerateRunningStyleLocalOptions): string =>
  `${options.outputRoot}/${options.runningStyleFeatureVersion}/manifest.json`;

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

export const buildManifest = (
  options: GenerateRunningStyleLocalOptions,
  generatedAt: Date,
): Manifest => {
  const windows = buildCategoryWindows(options);
  const categories: Record<string, ManifestCategorySummary> = {};
  for (const window of windows) {
    categories[window.category] = summarizeCategory(window);
  }
  return {
    featureVersion: options.runningStyleFeatureVersion,
    generatedAt: generatedAt.toISOString(),
    outputRoot: options.outputRoot,
    categories,
  };
};

const runPhaseAForChunk = async (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
  chunk: { yearFrom: number; yearTo: number },
): Promise<void> => {
  const command = buildPhaseACommand({
    pgUrl: options.pgUrl,
    outputDir: buildFeaturesDir(options),
    featureVersion: options.runningStyleFeatureVersion,
    yearFrom: chunk.yearFrom,
    yearTo: chunk.yearTo,
    category: window.category,
    threads: options.threads,
    memoryLimit: options.memoryLimit,
  });
  const result = await deps.spawn(command);
  if (result.exitCode !== 0) {
    throw new Error(
      `Phase A failed for category=${window.category} years=${chunk.yearFrom}-${chunk.yearTo}.`,
    );
  }
  await deps.sleep(PER_YEAR_SLEEP_MS);
};

const runPhaseBForCategory = async (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
): Promise<void> => {
  const command = buildPhaseBCommand({
    featuresParquet: buildFeaturesDir(options),
    outputParquet: buildPredictionsDir(options),
    featureVersion: options.runningStyleFeatureVersion,
    modelVersion: window.modelVersion,
    pgUrl: options.pgUrl,
    category: window.category,
  });
  const result = await deps.spawn(command);
  if (result.exitCode !== 0) {
    throw new Error(`Phase B failed for category=${window.category}.`);
  }
};

const runPhaseAForCategory = (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
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
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
): Promise<void> => {
  await runPhaseAForCategory(deps, options, window);
  await runPhaseBForCategory(deps, options, window);
  await deps.sleep(PER_CATEGORY_SLEEP_MS);
};

const writeManifest = async (
  options: GenerateRunningStyleLocalOptions,
  generatedAt: Date,
): Promise<void> => {
  const manifest = buildManifest(options, generatedAt);
  const path = buildManifestPath(options);
  await mkdir(`${options.outputRoot}/${options.runningStyleFeatureVersion}`, { recursive: true });
  await writeFile(path, JSON.stringify(manifest, null, 2), "utf8");
};

const runAllCategoriesSequentially = (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  windows: readonly CategoryWindow[],
): Promise<void> =>
  windows.reduce<Promise<void>>(
    (prev, window) => prev.then(() => runCategory(deps, options, window)),
    Promise.resolve(),
  );

export const runGenerateRunningStyleLocal = async (
  options: GenerateRunningStyleLocalOptions,
  deps: RunDeps,
): Promise<void> => {
  if (!options.ignoreNightWindow && !isInsideNightWindow(deps.now())) {
    throw new Error("Outside JST night window 23-04. Pass --ignore-night-window 1 to bypass.");
  }
  const resource = await deps.probeColima();
  assertColimaCapacity(resource);
  const windows = buildCategoryWindows(options);
  await runAllCategoriesSequentially(deps, options, windows);
  await writeManifest(options, deps.now());
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

/* v8 ignore start */
if (import.meta.main) {
  const options = parseArgs(process.argv.slice(2));
  const deps: RunDeps = {
    spawn: buildBunSpawnRunner(),
    sleep: buildBunSleepRunner(),
    probeColima: buildColimaProbe(),
    now: buildBunNowProvider(),
  };
  runGenerateRunningStyleLocal(options, deps).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
/* v8 ignore stop */
