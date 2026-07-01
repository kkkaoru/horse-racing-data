// Run with (cwd MUST be repo root):
//   bun run apps/pc-keiba-viewer/src/scripts/finish-position-features/generate-running-style-local.ts \
//   --pg-url $DATABASE_URL_LOCAL --running-style-feature-version v1 \
//   --model-version-jra <m> --model-version-nar <m> \
//   --model-flatbin-jra <path> --model-flatbin-nar <path> \
//   [--rs-p-from-flatbin-jra <path>] \
//   [--max-years-per-run 1] [--phase-a-concurrency auto] [--memory-limit-per-chunk 4GB] \
//   [--chunk-granularity month]
// Chunk granularity (Agent D1):
//   "month" (default): each Phase A spawn covers a single year-month slice
//     so the SQL pgsql_tmp spill stays roughly an order of magnitude below
//     the year-chunk baseline (~6 GB / chunk vs ~75 GB). chunk count grows
//     ~12x, mitigated by resource-aware Phase A concurrency and a year-level resume
//     check (a race_year dir is treated as complete once 12 parquet exist).
//   "year": legacy behaviour kept for one-shot reruns / debugging. spawns
//     a single Phase A command per year window.
// Parallel 3-phase orchestrator (Agent Y4 + W4/W6 switch, bucket-eval X5 design + B1 chunk-parallel).
// All spawned subprocess paths are repo-root relative so the driver works only
// when launched with cwd=repo root. The Python Phase A script in turn spawns a
// TS print-sql subprocess that also assumes cwd=repo root, so the convention is
// uniform: every phase resolves paths against repo root.
//   Phase A: spawn `uv --project apps/pc-keiba-viewer run python apps/pc-keiba-viewer/src/scripts/generate_running_style_features_local.py`
//           (--project pins uv at the pc-keiba-viewer venv so duckdb/psycopg/lightgbm are visible
//            even when the driver runs from cwd=repo root)
//   Phase B: spawn `bun run apps/sync-realtime-data/src/scripts/run-running-style-inference-local.ts`
//           (precision-0 LightGBM v1.5/v2 via W3 deliverable, bit-exact with production)
//   Phase C: spawn `bun run apps/pc-keiba-viewer/src/scripts/finish-position-features/apply-running-style-postproc.ts`
// Writes manifest.json after all phases complete. No PG/R2/D1/KV writeback.
// JRA: prod-v2 (138 feature) chained from v1.5 (117 feature) via --rs-p-from-flatbin.
// NAR: prod-v1.5 (117 feature) single-stage (no chained predict).
// Race-level nige cap / RaceLevelNigeConstraint intentionally NOT applied;
// memory rule `feedback_no_race_level_nige_constraint.md` forbids it.
// Legacy Python score_running_style_local.py is preserved for backward compat;
// it is simply no longer referenced from the orchestrator.

import { execFileSync } from "node:child_process";
import { mkdir, readdir, stat, writeFile } from "node:fs/promises";
import { cpus, freemem, loadavg, totalmem } from "node:os";

import { RUNNING_STYLE_FEATURE_VERSION } from "./running-style-feature-version";

const PARQUET_EXTENSION = ".parquet";

export type ChunkGranularity = "year" | "month";
export const CHUNK_GRANULARITIES: readonly ChunkGranularity[] = ["year", "month"];
const CHUNK_GRANULARITY_SET: ReadonlySet<string> = new Set<string>(CHUNK_GRANULARITIES);

export const DEFAULT_CHUNK_GRANULARITY: ChunkGranularity = "month";
export const MONTHS_PER_YEAR = 12;
export const ALL_MONTHS: readonly number[] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

interface GenerateRunningStyleLocalOptions {
  pgUrl: string;
  runningStyleFeatureVersion: string;
  threads: number;
  memoryLimit: string;
  memoryLimitPerChunk: string;
  phaseAConcurrency: number;
  categoryConcurrency: number;
  maxYearsPerRun: number;
  chunkGranularity: ChunkGranularity;
  ignoreNightWindow: boolean;
  outputRoot: string;
  modelVersionJra: string;
  modelVersionNar: string;
  modelFlatbinJra: string;
  modelFlatbinNar: string;
  rsPFromFlatbinJra: string;
  force: boolean;
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

export interface LocalResourceSnapshot {
  cpuCount: number;
  load1m: number;
  totalMemoryBytes: number;
  freeMemoryBytes: number;
  compressorBytes?: number;
}

interface LocalResourceProbe {
  (): Promise<LocalResourceSnapshot>;
}

type ExecFileSyncText = (file: string, args: readonly string[]) => string;

interface NowProvider {
  (): Date;
}

interface Logger {
  (message: string): void;
}

interface ListDirectoryEntriesFn {
  (path: string): Promise<readonly string[]>;
}

interface FileStat {
  size: number;
}

interface StatFileFn {
  (path: string): Promise<FileStat>;
}

interface ChunkParquetCheckArgs {
  featuresDir: string;
  yearFrom: number;
  yearTo: number;
}

interface MonthChunkParquetCheckArgs {
  featuresDir: string;
  year: number;
}

interface CategoryWindow {
  category: "jra" | "nar";
  years: readonly number[];
  modelVersion: string;
  modelFlatbin: string;
  rsPFromFlatbin: string | null;
}

interface PhaseAArgs {
  pgUrl: string;
  outputDir: string;
  featureVersion: string;
  yearFrom: number;
  yearTo: number;
  monthFrom: number | null;
  monthTo: number | null;
  category: "jra" | "nar";
  threads: number;
  memoryLimit: string;
}

interface PhaseBArgs {
  modelFlatbin: string;
  featuresParquet: string;
  outputParquet: string;
  category: "jra" | "nar";
  predictedAt: string;
  modelVersion: string;
  featureVersion: string;
  rsPFromFlatbin: string | null;
}

interface PhaseCArgs {
  logitsParquet: string;
  outputParquet: string;
  featureVersion: string;
}

interface ManifestCategorySummary {
  yearFrom: number;
  yearTo: number;
  years: readonly number[];
  modelVersion: string;
}

interface ManifestRsPFromFlatbin {
  jra: string | null;
  nar: string | null;
}

interface Manifest {
  featureVersion: string;
  generatedAt: string;
  outputRoot: string;
  featuresPath: string;
  logitsPath: string;
  predictionsPath: string;
  modelVersions: { jra: string; nar: string };
  rsPFromFlatbin: ManifestRsPFromFlatbin;
  categories: Record<string, ManifestCategorySummary>;
}

interface RunDeps {
  spawn: SpawnRunner;
  sleep: SleepRunner;
  probeColima: ColimaProbe;
  probeLocalResources?: LocalResourceProbe;
  now: NowProvider;
  log: Logger;
  listDirectoryEntries: ListDirectoryEntriesFn;
  statFile: StatFileFn;
}

interface YearChunk {
  yearFrom: number;
  yearTo: number;
}

interface MonthChunk {
  year: number;
  month: number;
}

interface YearMonth {
  year: number;
  month: number;
}

export const COLIMA_MIN_CPU = 8;
export const COLIMA_MIN_MEMORY_GIB = 24;
export const COLIMA_MIN_DISK_GIB = 100;
export const NIGHT_WINDOW_HOURS_JST: readonly number[] = [23, 0, 1, 2, 3, 4];
export const PER_YEAR_SLEEP_MS = 2000;
export const PER_CATEGORY_SLEEP_MS = 5000;
export const DEFAULT_OUTPUT_ROOT = "apps/pc-keiba-viewer/tmp/bucket-eval/running-style";
export const AUTO_RESOURCE_VALUE = 0;
export const DEFAULT_THREADS = AUTO_RESOURCE_VALUE;
export const DEFAULT_MEMORY_LIMIT = "";
export const DEFAULT_MAX_YEARS_PER_RUN = 1;
export const DEFAULT_PHASE_A_CONCURRENCY = AUTO_RESOURCE_VALUE;
export const DEFAULT_CATEGORY_CONCURRENCY = AUTO_RESOURCE_VALUE;
export const DEFAULT_MEMORY_LIMIT_PER_CHUNK = "";
export const DEFAULT_FORCE = false;
export const PHASE_A_SCRIPT =
  "apps/pc-keiba-viewer/src/scripts/generate_running_style_features_local.py";
export const PHASE_B_SCRIPT =
  "apps/sync-realtime-data/src/scripts/run-running-style-inference-local.ts";
export const PHASE_C_SCRIPT =
  "apps/pc-keiba-viewer/src/scripts/finish-position-features/apply-running-style-postproc.ts";
export const NIGHT_WINDOW_SET: ReadonlySet<number> = new Set(NIGHT_WINDOW_HOURS_JST);
export const JRA_V2_MODEL_TAG = "-v2";
export const PHASE_A_UV_PROJECT = "apps/pc-keiba-viewer";

export const RUNNING_STYLE_CATEGORIES = ["jra", "nar"] satisfies readonly string[];

const JRA_YEARS: readonly number[] = [
  2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021,
  2022, 2023, 2024, 2025, 2026,
];
const NAR_YEARS: readonly number[] = [
  2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2026,
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

const isChunkGranularity = (raw: string): raw is ChunkGranularity => CHUNK_GRANULARITY_SET.has(raw);

const parseChunkGranularity = (raw: string): ChunkGranularity => {
  if (!isChunkGranularity(raw)) {
    throw new Error(`--chunk-granularity must be one of ${CHUNK_GRANULARITIES.join(", ")}.`);
  }
  return raw;
};

export const isJraV2ModelVersion = (modelVersion: string): boolean =>
  modelVersion.includes(JRA_V2_MODEL_TAG);

export const buildDefaultOptions = (): GenerateRunningStyleLocalOptions => ({
  pgUrl: "",
  runningStyleFeatureVersion: RUNNING_STYLE_FEATURE_VERSION,
  threads: DEFAULT_THREADS,
  memoryLimit: DEFAULT_MEMORY_LIMIT,
  memoryLimitPerChunk: DEFAULT_MEMORY_LIMIT_PER_CHUNK,
  phaseAConcurrency: DEFAULT_PHASE_A_CONCURRENCY,
  categoryConcurrency: DEFAULT_CATEGORY_CONCURRENCY,
  maxYearsPerRun: DEFAULT_MAX_YEARS_PER_RUN,
  chunkGranularity: DEFAULT_CHUNK_GRANULARITY,
  ignoreNightWindow: false,
  outputRoot: DEFAULT_OUTPUT_ROOT,
  modelVersionJra: "",
  modelVersionNar: "",
  modelFlatbinJra: "",
  modelFlatbinNar: "",
  rsPFromFlatbinJra: "",
  force: DEFAULT_FORCE,
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
    options.threads = parseAutoInteger(requireValue(name, value));
    return { advanceBy: 2 };
  }
  if (name === "--memory-limit") {
    options.memoryLimit = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--memory-limit-per-chunk") {
    options.memoryLimitPerChunk = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--phase-a-concurrency") {
    options.phaseAConcurrency = parseAutoInteger(requireValue(name, value));
    return { advanceBy: 2 };
  }
  if (name === "--max-years-per-run") {
    options.maxYearsPerRun = Number(requireValue(name, value));
    return { advanceBy: 2 };
  }
  if (name === "--chunk-granularity") {
    options.chunkGranularity = parseChunkGranularity(requireValue(name, value));
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
  if (name === "--model-flatbin-jra") {
    options.modelFlatbinJra = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--model-flatbin-nar") {
    options.modelFlatbinNar = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--rs-p-from-flatbin-jra") {
    options.rsPFromFlatbinJra = requireValue(name, value);
    return { advanceBy: 2 };
  }
  if (name === "--force") {
    options.force = parseBooleanFlag(value);
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

const assertJraV2HasChainPath = (options: GenerateRunningStyleLocalOptions): void => {
  if (!isJraV2ModelVersion(options.modelVersionJra)) return;
  if (options.rsPFromFlatbinJra !== "") return;
  throw new Error(
    "--rs-p-from-flatbin-jra is required when --model-version-jra targets prod-v2 (chained predict from v1.5).",
  );
};

export const parseArgs = (argv: readonly string[]): GenerateRunningStyleLocalOptions => {
  const options = consumeArgsRecursive(buildDefaultOptions(), argv, 0);
  if (options.pgUrl === "") throw new Error("--pg-url is required.");
  if (options.modelVersionJra === "") throw new Error("--model-version-jra is required.");
  if (options.modelVersionNar === "") throw new Error("--model-version-nar is required.");
  if (options.modelFlatbinJra === "") throw new Error("--model-flatbin-jra is required.");
  if (options.modelFlatbinNar === "") throw new Error("--model-flatbin-nar is required.");
  assertJraV2HasChainPath(options);
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

export const collectLocalResourceSnapshot = (): LocalResourceSnapshot => ({
  ...collectLocalResourceSnapshotFallback(),
  ...collectMacVmStatResourceSnapshot(),
});

const collectLocalResourceSnapshotFallback = (): LocalResourceSnapshot => ({
  cpuCount: cpus().length || 1,
  load1m: loadavg()[0] ?? 0,
  totalMemoryBytes: totalmem(),
  freeMemoryBytes: freemem(),
});

export const parseVmStatPages = (output: string): Map<string, number> => {
  const pages = new Map<string, number>();
  for (const line of output.split("\n")) {
    const [rawKey, rawValue] = line.split(":");
    if (rawKey === undefined || rawValue === undefined) continue;
    const normalized = rawValue.trim().replace(/\.$/, "").replaceAll(".", "").replaceAll(",", "");
    const parsed = Number.parseInt(normalized, 10);
    if (Number.isFinite(parsed)) pages.set(rawKey.trim(), parsed);
  }
  return pages;
};

const execFileSyncText: ExecFileSyncText = (file, args) =>
  execFileSync(file, args, { encoding: "utf8" });

export const collectMacVmStatResourceSnapshot = (
  execFile: ExecFileSyncText = execFileSyncText,
): Partial<LocalResourceSnapshot> => {
  try {
    const pageSize = Number.parseInt(execFile("sysctl", ["-n", "hw.pagesize"]), 10);
    if (!Number.isFinite(pageSize) || pageSize <= 0) return {};
    const output = execFile("vm_stat", []);
    const pages = parseVmStatPages(output);
    const availablePages =
      (pages.get("Pages free") ?? 0) +
      (pages.get("Pages inactive") ?? 0) +
      (pages.get("Pages speculative") ?? 0) +
      (pages.get("Pages purgeable") ?? 0);
    return {
      freeMemoryBytes: availablePages * pageSize,
      compressorBytes: (pages.get("Pages occupied by compressor") ?? 0) * pageSize,
    };
  } catch {
    return {};
  }
};

const bytesToGiB = (bytes: number): number => bytes / 1024 ** 3;

export const resolveAutoMemoryLimit = (
  resource: ColimaResource,
  snapshot?: LocalResourceSnapshot,
): string => {
  const halfColimaMemory = Math.floor(resource.memoryGiB * 0.5);
  const capacityLimit = Math.max(1, halfColimaMemory);
  if (snapshot === undefined || snapshot.totalMemoryBytes <= 0) {
    return `${Math.max(6, capacityLimit)}GB`;
  }
  const freeGiB = Math.max(1, Math.floor(bytesToGiB(snapshot.freeMemoryBytes)));
  const compressorRatio = (snapshot.compressorBytes ?? 0) / snapshot.totalMemoryBytes;
  if (compressorRatio > 0.08) {
    return `${Math.max(1, Math.min(capacityLimit, Math.floor(freeGiB / 3)))}GB`;
  }
  if (compressorRatio > 0.05) {
    return `${Math.max(2, Math.min(capacityLimit, Math.floor(freeGiB / 2)))}GB`;
  }
  return `${Math.max(1, Math.min(Math.max(6, capacityLimit), Math.max(1, freeGiB - 2)))}GB`;
};

export const resolveAutoThreads = (
  resource: ColimaResource,
  snapshot: LocalResourceSnapshot,
): number => {
  const cpuCount = Math.max(1, Math.min(resource.cpu, snapshot.cpuCount));
  const load1m = Math.max(0, snapshot.load1m);
  const freeRatio =
    snapshot.totalMemoryBytes > 0 ? snapshot.freeMemoryBytes / snapshot.totalMemoryBytes : 1;
  const compressorRatio =
    snapshot.totalMemoryBytes > 0 ? (snapshot.compressorBytes ?? 0) / snapshot.totalMemoryBytes : 0;
  const cpuHeadroom = Math.max(1, cpuCount - Math.ceil(load1m));
  const memoryLimitGiB = Math.max(6, Math.floor(resource.memoryGiB * 0.5));
  const memoryCap = Math.max(1, Math.floor(memoryLimitGiB / 1.5));
  if (freeRatio < 0.15 || compressorRatio > 0.08 || load1m >= cpuCount * 0.85) return 1;
  if (freeRatio < 0.25 || compressorRatio > 0.05 || load1m >= cpuCount * 0.65) {
    return Math.max(1, Math.min(2, cpuHeadroom, memoryCap));
  }
  return Math.max(1, Math.min(cpuCount, cpuHeadroom, memoryCap));
};

export const resolveAutoPhaseAConcurrency = (
  resource: ColimaResource,
  snapshot: LocalResourceSnapshot,
  resolvedThreads: number,
): number => {
  const cpuCount = Math.max(1, Math.min(resource.cpu, snapshot.cpuCount));
  const load1m = Math.max(0, snapshot.load1m);
  const freeGiB = Math.max(0, bytesToGiB(snapshot.freeMemoryBytes));
  const freeRatio =
    snapshot.totalMemoryBytes > 0 ? snapshot.freeMemoryBytes / snapshot.totalMemoryBytes : 1;
  const compressorRatio =
    snapshot.totalMemoryBytes > 0 ? (snapshot.compressorBytes ?? 0) / snapshot.totalMemoryBytes : 0;
  if (freeRatio < 0.18 || compressorRatio > 0.05 || load1m >= cpuCount * 0.85) return 1;
  const cpuSlots = Math.max(
    1,
    Math.floor(Math.max(1, cpuCount - Math.ceil(load1m)) / resolvedThreads),
  );
  const memorySlots = Math.max(1, Math.floor(freeGiB / 6));
  const globalSlots = Math.max(1, Math.min(cpuSlots, memorySlots));
  return Math.max(1, Math.floor(globalSlots / RUNNING_STYLE_CATEGORIES.length));
};

export const resolveAutoCategoryConcurrency = (
  resource: ColimaResource,
  snapshot: LocalResourceSnapshot,
): number => {
  const cpuCount = Math.max(1, Math.min(resource.cpu, snapshot.cpuCount));
  const load1m = Math.max(0, snapshot.load1m);
  const freeRatio =
    snapshot.totalMemoryBytes > 0 ? snapshot.freeMemoryBytes / snapshot.totalMemoryBytes : 1;
  const compressorRatio =
    snapshot.totalMemoryBytes > 0 ? (snapshot.compressorBytes ?? 0) / snapshot.totalMemoryBytes : 0;
  if (freeRatio < 0.28 || compressorRatio > 0.03 || load1m >= cpuCount * 0.5) return 1;
  return RUNNING_STYLE_CATEGORIES.length;
};

export const resolveRuntimeResourceOptions = (
  options: GenerateRunningStyleLocalOptions,
  resource: ColimaResource,
  snapshot: LocalResourceSnapshot,
): GenerateRunningStyleLocalOptions => {
  const memoryLimit =
    options.memoryLimit === "" ? resolveAutoMemoryLimit(resource, snapshot) : options.memoryLimit;
  const threads = options.threads <= 0 ? resolveAutoThreads(resource, snapshot) : options.threads;
  const phaseAConcurrency =
    options.phaseAConcurrency <= 0
      ? resolveAutoPhaseAConcurrency(resource, snapshot, threads)
      : options.phaseAConcurrency;
  const categoryConcurrency =
    options.categoryConcurrency <= 0
      ? resolveAutoCategoryConcurrency(resource, snapshot)
      : options.categoryConcurrency;
  return { ...options, memoryLimit, threads, phaseAConcurrency, categoryConcurrency };
};

const resolveRsPFromFlatbinForJra = (options: GenerateRunningStyleLocalOptions): string | null =>
  options.rsPFromFlatbinJra === "" ? null : options.rsPFromFlatbinJra;

export const buildCategoryWindows = (
  options: GenerateRunningStyleLocalOptions,
): readonly CategoryWindow[] => [
  {
    category: "jra",
    years: JRA_YEARS,
    modelVersion: options.modelVersionJra,
    modelFlatbin: options.modelFlatbinJra,
    rsPFromFlatbin: resolveRsPFromFlatbinForJra(options),
  },
  {
    category: "nar",
    years: NAR_YEARS,
    modelVersion: options.modelVersionNar,
    modelFlatbin: options.modelFlatbinNar,
    rsPFromFlatbin: null,
  },
];

export const buildFeaturesRoot = (options: GenerateRunningStyleLocalOptions): string =>
  `${options.outputRoot}/${options.runningStyleFeatureVersion}/features`;

export const buildLogitsRoot = (options: GenerateRunningStyleLocalOptions): string =>
  `${options.outputRoot}/${options.runningStyleFeatureVersion}/logits`;

export const buildPredictionsRoot = (options: GenerateRunningStyleLocalOptions): string =>
  `${options.outputRoot}/${options.runningStyleFeatureVersion}/predictions`;

export const buildManifestPath = (options: GenerateRunningStyleLocalOptions): string =>
  `${options.outputRoot}/${options.runningStyleFeatureVersion}/manifest.json`;

export const buildCategoryFeaturesDir = (
  options: GenerateRunningStyleLocalOptions,
  category: "jra" | "nar",
): string => `${buildFeaturesRoot(options)}/category=${category}`;

export const buildCategoryLogitsDir = (
  options: GenerateRunningStyleLocalOptions,
  category: "jra" | "nar",
): string => `${buildLogitsRoot(options)}/category=${category}`;

export const buildCategoryPredictionsDir = (
  options: GenerateRunningStyleLocalOptions,
  category: "jra" | "nar",
): string => `${buildPredictionsRoot(options)}/category=${category}`;

const buildPhaseAMonthTokens = (
  monthFrom: number | null,
  monthTo: number | null,
): readonly string[] => {
  if (monthFrom === null || monthTo === null) return [];
  return ["--month-from", String(monthFrom), "--month-to", String(monthTo)];
};

const buildPhaseABaseTokens = (args: PhaseAArgs): readonly string[] => [
  "uv",
  "--project",
  PHASE_A_UV_PROJECT,
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

export const buildPhaseACommand = (args: PhaseAArgs): readonly string[] => [
  ...buildPhaseABaseTokens(args),
  ...buildPhaseAMonthTokens(args.monthFrom, args.monthTo),
];

const buildPhaseBBaseTokens = (args: PhaseBArgs): readonly string[] => [
  "bun",
  "run",
  PHASE_B_SCRIPT,
  "--model-flatbin",
  args.modelFlatbin,
  "--features-parquet",
  args.featuresParquet,
  "--output-parquet",
  args.outputParquet,
  "--category",
  args.category,
  "--predicted-at",
  args.predictedAt,
  "--model-version",
  args.modelVersion,
  "--feature-version",
  args.featureVersion,
];

const buildPhaseBChainTokens = (rsPFromFlatbin: string | null): readonly string[] =>
  rsPFromFlatbin === null ? [] : ["--rs-p-from-flatbin", rsPFromFlatbin];

export const buildPhaseBCommand = (args: PhaseBArgs): readonly string[] => [
  ...buildPhaseBBaseTokens(args),
  ...buildPhaseBChainTokens(args.rsPFromFlatbin),
];

export const buildPhaseCCommand = (args: PhaseCArgs): readonly string[] => [
  "bun",
  "run",
  PHASE_C_SCRIPT,
  "--logits-parquet",
  args.logitsParquet,
  "--output-parquet",
  args.outputParquet,
  "--running-style-feature-version",
  args.featureVersion,
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

const expandYearToMonths = (year: number): readonly YearMonth[] =>
  ALL_MONTHS.map((month) => ({ year, month }));

export const expandYearsToMonths = (years: readonly number[]): readonly YearMonth[] =>
  years.flatMap(expandYearToMonths);

export const chunkYearsByMonth = (years: readonly number[]): readonly MonthChunk[] =>
  expandYearsToMonths(years).map(({ year, month }) => ({ year, month }));

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

const findCategoryWindow = (
  windows: readonly CategoryWindow[],
  category: CategoryWindow["category"],
): CategoryWindow | undefined => windows.find((window) => window.category === category);

const buildManifestRsPFromFlatbin = (
  windows: readonly CategoryWindow[],
): ManifestRsPFromFlatbin => ({
  jra: findCategoryWindow(windows, "jra")?.rsPFromFlatbin ?? null,
  nar: findCategoryWindow(windows, "nar")?.rsPFromFlatbin ?? null,
});

export const buildManifest = (
  options: GenerateRunningStyleLocalOptions,
  generatedAt: Date,
): Manifest => {
  const windows = buildCategoryWindows(options);
  return {
    featureVersion: options.runningStyleFeatureVersion,
    generatedAt: generatedAt.toISOString(),
    outputRoot: options.outputRoot,
    featuresPath: buildFeaturesRoot(options),
    logitsPath: buildLogitsRoot(options),
    predictionsPath: buildPredictionsRoot(options),
    modelVersions: { jra: options.modelVersionJra, nar: options.modelVersionNar },
    rsPFromFlatbin: buildManifestRsPFromFlatbin(windows),
    categories: summariesByCategory(windows),
  };
};

export const resolveMemoryLimitPerChunk = (options: GenerateRunningStyleLocalOptions): string =>
  options.memoryLimitPerChunk === "" ? options.memoryLimit : options.memoryLimitPerChunk;

export const buildChunkYearRange = (chunk: YearChunk): readonly number[] =>
  Array.from(
    { length: chunk.yearTo - chunk.yearFrom + 1 },
    (_unused, index) => chunk.yearFrom + index,
  );

export const buildChunkYearDir = (featuresDir: string, year: number): string =>
  `${featuresDir}/race_year=${year}`;

const isParquetEntry = (entry: string): boolean => entry.endsWith(PARQUET_EXTENSION);

const yearHasParquet = async (
  listDirectoryEntries: ListDirectoryEntriesFn,
  featuresDir: string,
  year: number,
): Promise<boolean> => {
  const dir = buildChunkYearDir(featuresDir, year);
  const entries = await listDirectoryEntries(dir).catch(() => [] satisfies readonly string[]);
  return entries.some(isParquetEntry);
};

export const chunkHasExistingParquet = async (
  listDirectoryEntries: ListDirectoryEntriesFn,
  args: ChunkParquetCheckArgs,
): Promise<boolean> => {
  const years = buildChunkYearRange({ yearFrom: args.yearFrom, yearTo: args.yearTo });
  const results = await Promise.all(
    years.map((year) => yearHasParquet(listDirectoryEntries, args.featuresDir, year)),
  );
  return results.every((hit) => hit);
};

export const yearHasFullMonthParquet = async (
  listDirectoryEntries: ListDirectoryEntriesFn,
  args: MonthChunkParquetCheckArgs,
): Promise<boolean> => {
  const dir = buildChunkYearDir(args.featuresDir, args.year);
  const entries = await listDirectoryEntries(dir).catch(() => [] satisfies readonly string[]);
  const parquetCount = entries.filter(isParquetEntry).length;
  return parquetCount >= MONTHS_PER_YEAR;
};

const runPhaseAForYearChunk = async (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
  chunk: YearChunk,
): Promise<void> => {
  const featuresDir = buildCategoryFeaturesDir(options, window.category);
  if (!options.force) {
    const skip = await chunkHasExistingParquet(deps.listDirectoryEntries, {
      featuresDir,
      yearFrom: chunk.yearFrom,
      yearTo: chunk.yearTo,
    });
    if (skip) {
      deps.log(
        `[Phase A] skip ${window.category} ${chunk.yearFrom}-${chunk.yearTo} (existing parquet found)`,
      );
      return;
    }
  }
  const command = buildPhaseACommand({
    pgUrl: options.pgUrl,
    outputDir: featuresDir,
    featureVersion: options.runningStyleFeatureVersion,
    yearFrom: chunk.yearFrom,
    yearTo: chunk.yearTo,
    monthFrom: null,
    monthTo: null,
    category: window.category,
    threads: options.threads,
    memoryLimit: resolveMemoryLimitPerChunk(options),
  });
  const result = await deps.spawn(command);
  if (result.exitCode !== 0) {
    throw new Error(
      `Phase A failed for category=${window.category} years=${chunk.yearFrom}-${chunk.yearTo}.`,
    );
  }
  await deps.sleep(PER_YEAR_SLEEP_MS);
};

const runPhaseAForMonthChunk = async (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
  chunk: MonthChunk,
): Promise<void> => {
  const featuresDir = buildCategoryFeaturesDir(options, window.category);
  if (!options.force) {
    const skip = await yearHasFullMonthParquet(deps.listDirectoryEntries, {
      featuresDir,
      year: chunk.year,
    });
    if (skip) {
      deps.log(
        `[Phase A] skip ${window.category} ${chunk.year}-${chunk.month} (year-level resume: 12 parquet found)`,
      );
      return;
    }
  }
  const command = buildPhaseACommand({
    pgUrl: options.pgUrl,
    outputDir: featuresDir,
    featureVersion: options.runningStyleFeatureVersion,
    yearFrom: chunk.year,
    yearTo: chunk.year,
    monthFrom: chunk.month,
    monthTo: chunk.month,
    category: window.category,
    threads: options.threads,
    memoryLimit: resolveMemoryLimitPerChunk(options),
  });
  const result = await deps.spawn(command);
  if (result.exitCode !== 0) {
    throw new Error(
      `Phase A failed for category=${window.category} year=${chunk.year} month=${chunk.month}.`,
    );
  }
  await deps.sleep(PER_YEAR_SLEEP_MS);
};

interface ConcurrencyCursor {
  next: number;
}

const runVoidTaskWorker = async (
  tasks: ReadonlyArray<() => Promise<void>>,
  cursor: ConcurrencyCursor,
): Promise<void> => {
  const idx = cursor.next;
  cursor.next = idx + 1;
  const task = tasks[idx];
  if (task === undefined) return;
  await task();
  await runVoidTaskWorker(tasks, cursor);
};

export const runVoidTasksWithConcurrencyLimit = async (
  tasks: ReadonlyArray<() => Promise<void>>,
  concurrency: number,
): Promise<void> => {
  if (concurrency <= 0) throw new Error("concurrency must be positive.");
  if (tasks.length === 0) return;
  const cursor: ConcurrencyCursor = { next: 0 };
  const workerCount = Math.min(concurrency, tasks.length);
  const workers = Array.from({ length: workerCount }, () => runVoidTaskWorker(tasks, cursor));
  await Promise.all(workers);
};

export const logitsFileHasContent = async (
  statFile: StatFileFn,
  path: string,
): Promise<boolean> => {
  const result = await statFile(path).catch(() => null);
  if (result === null) return false;
  return result.size > 0;
};

const runPhaseBForCategory = async (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
): Promise<void> => {
  const outputPath = buildCategoryLogitsDir(options, window.category);
  if (!options.force) {
    const skip = await logitsFileHasContent(deps.statFile, outputPath);
    if (skip) {
      deps.log(`[Phase B] skip ${window.category} (existing logits parquet at ${outputPath})`);
      return;
    }
  }
  const command = buildPhaseBCommand({
    modelFlatbin: window.modelFlatbin,
    featuresParquet: buildCategoryFeaturesDir(options, window.category),
    outputParquet: outputPath,
    category: window.category,
    predictedAt: deps.now().toISOString(),
    modelVersion: window.modelVersion,
    featureVersion: options.runningStyleFeatureVersion,
    rsPFromFlatbin: window.rsPFromFlatbin,
  });
  const result = await deps.spawn(command);
  if (result.exitCode !== 0) {
    throw new Error(`Phase B failed for category=${window.category}.`);
  }
};

const runPhaseCForCategory = async (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
): Promise<void> => {
  const command = buildPhaseCCommand({
    logitsParquet: buildCategoryLogitsDir(options, window.category),
    outputParquet: buildCategoryPredictionsDir(options, window.category),
    featureVersion: options.runningStyleFeatureVersion,
  });
  const result = await deps.spawn(command);
  if (result.exitCode !== 0) {
    throw new Error(`Phase C failed for category=${window.category}.`);
  }
};

const buildPhaseAYearTasks = (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
): ReadonlyArray<() => Promise<void>> => {
  const chunks = chunkYears(window.years, options.maxYearsPerRun);
  return chunks.map((chunk) => () => runPhaseAForYearChunk(deps, options, window, chunk));
};

const buildPhaseAMonthTasks = (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
): ReadonlyArray<() => Promise<void>> => {
  const chunks = chunkYearsByMonth(window.years);
  return chunks.map((chunk) => () => runPhaseAForMonthChunk(deps, options, window, chunk));
};

const buildPhaseATasks = (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
): ReadonlyArray<() => Promise<void>> => {
  if (options.chunkGranularity === "month") return buildPhaseAMonthTasks(deps, options, window);
  return buildPhaseAYearTasks(deps, options, window);
};

const runPhaseAForCategory = (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
): Promise<void> => {
  const tasks = buildPhaseATasks(deps, options, window);
  return runVoidTasksWithConcurrencyLimit(tasks, options.phaseAConcurrency);
};

const runCategory = async (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  window: CategoryWindow,
): Promise<void> => {
  await runPhaseAForCategory(deps, options, window);
  await runPhaseBForCategory(deps, options, window);
  await runPhaseCForCategory(deps, options, window);
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

const runAllCategoriesInParallel = async (
  deps: RunDeps,
  options: GenerateRunningStyleLocalOptions,
  windows: readonly CategoryWindow[],
): Promise<void> => {
  const tasks = windows.map((window) => () => runCategory(deps, options, window));
  await runVoidTasksWithConcurrencyLimit(tasks, options.categoryConcurrency);
};

export const runGenerateRunningStyleLocal = async (
  options: GenerateRunningStyleLocalOptions,
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
  deps.log(
    `[resource] threads=${runtimeOptions.threads} phaseAConcurrency=${runtimeOptions.phaseAConcurrency} categoryConcurrency=${runtimeOptions.categoryConcurrency} memoryLimit=${runtimeOptions.memoryLimit} load1m=${snapshot.load1m.toFixed(2)} freeMemoryGiB=${bytesToGiB(snapshot.freeMemoryBytes).toFixed(1)}`,
  );
  const windows = buildCategoryWindows(runtimeOptions);
  await runAllCategoriesInParallel(deps, runtimeOptions, windows);
  await writeManifest(runtimeOptions, deps.now());
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

const buildConsoleLogger = (): Logger => (message) => console.log(message);

const buildFsListDirectoryEntries = (): ListDirectoryEntriesFn => async (path) => readdir(path);

const buildFsStatFile = (): StatFileFn => async (path) => {
  const result = await stat(path);
  return { size: result.size };
};

/* v8 ignore start */
if (import.meta.main) {
  const options = parseArgs(process.argv.slice(2));
  const deps: RunDeps = {
    spawn: buildBunSpawnRunner(),
    sleep: buildBunSleepRunner(),
    probeColima: buildColimaProbe(),
    now: buildBunNowProvider(),
    log: buildConsoleLogger(),
    listDirectoryEntries: buildFsListDirectoryEntries(),
    statFile: buildFsStatFile(),
  };
  runGenerateRunningStyleLocal(options, deps).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
/* v8 ignore stop */
