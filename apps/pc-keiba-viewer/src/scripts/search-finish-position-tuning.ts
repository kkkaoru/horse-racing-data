/**
 * Finish-position tuning search runner.
 *
 * Usage:
 *   cd apps/pc-keiba-viewer
 *   bun run dev:search-finish-tuning -- \
 *     --search-config src/scripts/finish-position-tuning/jra-search.example.json
 *   bun run dev:search-finish-tuning -- \
 *     --search-config src/scripts/finish-position-tuning/nar-search.example.json
 *   bun run dev:search-finish-tuning -- \
 *     --search-config src/scripts/finish-position-tuning/ban-ei-search.example.json
 *   bun run dev:search-finish-tuning -- \
 *     --search-config src/scripts/finish-position-tuning/jra-search.example.json \
 *     --run-until 23:30 --random-category jra
 *
 * Search config shape:
 *   - baseArgs: common compare-finish arguments, such as category/fromYear/toYear.
 *   - trials[].args: partial compare-finish argument overrides per trial.
 *   - baseTuningConfig: large common tuning JSON shared by every trial.
 *   - baseTuningConfigPath: JSON file merged before baseTuningConfig.
 *   - trials[].tuningConfigOverrides: partial override merged onto baseTuningConfig.
 *   - trials[].tuningConfig: backward-compatible full/partial tuning config, also merged onto baseTuningConfig.
 *   - changedRaceLimit: max changed-race samples per trial in the log. Default is 30.
 *   - output: optional explicit result file. A timestamped log is always written to logs/.
 *
 * Time-boxed random mode:
 *   - --run-until accepts HH:mm or an ISO date/time. New validations stop starting at that time.
 *   - --random-category accepts all, jra, nar, or ban-ei.
 *   - --random-min-changes and --random-max-changes control how many parameter paths are changed.
 *   - --random-parameter-kinds limits randomly changed parameter groups.
 *   - Tried parameter combinations are persisted in PostgreSQL with a parameter schema version.
 *
 * Merge behavior:
 *   - Objects are deep-merged.
 *   - Arrays and primitive values replace the base value.
 */
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join } from "node:path";

import { Pool } from "pg";

import { getConnectionString, loadEnv } from "./compare-corner-predictions";

type CompareArgs = {
  category?: "all" | "ban-ei" | "jra" | "nar";
  concurrency?: number;
  fromDate?: string;
  fromYear?: string;
  target?: "local" | "neon";
  toDate?: string;
  toYear?: string;
};

type RandomParameterKind =
  | "component-models"
  | "ensemble-mode"
  | "ensemble-weights"
  | "mixed-weighted-share"
  | "score-base"
  | "score-rules";

type CliOptions = {
  parameterSchemaVersion: string;
  randomCategory?: CompareArgs["category"];
  randomMaxChanges: number;
  randomMinChanges: number;
  randomParameterKinds?: RandomParameterKind[];
  runUntil?: Date;
  searchConfigPath: string;
};

type SearchConfig = {
  baseArgs?: CompareArgs;
  baseTuningConfig?: unknown;
  baseTuningConfigPath?: string;
  changedRaceLimit?: number;
  output?: string;
  trials: Array<{
    args?: CompareArgs & {
      ensembleMode?: "auto" | "mixed" | "off" | "vote" | "weighted";
    };
    name: string;
    tuningConfig?: unknown;
    tuningConfigOverrides?: unknown;
  }>;
};

type RandomTrialRecord = {
  id: number;
  parameterHash: string;
};

type TrialResult = {
  changedRaces?: unknown;
  name: string;
  pairScore: number;
  place1Accuracy: number;
  place2Accuracy: number;
  place3Accuracy: number;
  raceCount: number;
  top3BoxAccuracy: number;
  top3ExactOrderAccuracy: number;
  top3PlaceRelation: number;
  top3WinnerCapture: number;
  top5WinnerCapture: number;
};

const randomParameterKinds: RandomParameterKind[] = [
  "score-base",
  "score-rules",
  "component-models",
  "ensemble-mode",
  "ensemble-weights",
  "mixed-weighted-share",
];

const isRandomParameterKind = (value: string): value is RandomParameterKind =>
  randomParameterKinds.includes(value as RandomParameterKind);

const parseRunUntil = (value: string): Date => {
  if (/^\d{1,2}:\d{2}$/u.test(value)) {
    const [hourText, minuteText] = value.split(":");
    const date = new Date();
    date.setHours(Number(hourText), Number(minuteText), 0, 0);
    if (date.getTime() <= Date.now()) {
      date.setDate(date.getDate() + 1);
    }
    return date;
  }
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) {
    throw new Error("--run-until must be HH:mm or an ISO date/time.");
  }
  return parsed;
};

const parseArgs = (args: string[]): CliOptions => {
  const options: CliOptions = {
    parameterSchemaVersion: "finish-position-tuning-v1",
    randomMaxChanges: 4,
    randomMinChanges: 1,
    searchConfigPath: "",
  };
  for (let index = 0; index < args.length; index += 1) {
    if (args[index] === "--search-config") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error("--search-config requires a value.");
      }
      options.searchConfigPath = value;
      index += 1;
    } else if (args[index] === "--run-until") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error("--run-until requires a value.");
      }
      options.runUntil = parseRunUntil(value);
      index += 1;
    } else if (args[index] === "--random-category") {
      const value = args[index + 1];
      if (value !== "all" && value !== "jra" && value !== "nar" && value !== "ban-ei") {
        throw new Error("--random-category must be all, jra, nar, or ban-ei.");
      }
      options.randomCategory = value;
      index += 1;
    } else if (args[index] === "--random-min-changes") {
      options.randomMinChanges = Math.max(1, Number(args[index + 1]));
      index += 1;
    } else if (args[index] === "--random-max-changes") {
      options.randomMaxChanges = Math.max(1, Number(args[index + 1]));
      index += 1;
    } else if (args[index] === "--random-parameter-kinds") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error("--random-parameter-kinds requires comma-separated values.");
      }
      const kinds = value.split(",").map((item) => item.trim());
      const unknown = kinds.find((kind) => !isRandomParameterKind(kind));
      if (unknown !== undefined) {
        throw new Error(
          `Unknown random parameter kind: ${unknown}. Allowed: ${randomParameterKinds.join(",")}`,
        );
      }
      options.randomParameterKinds = kinds.filter(isRandomParameterKind);
      index += 1;
    } else if (args[index] === "--parameter-schema-version") {
      const value = args[index + 1];
      if (value === undefined) {
        throw new Error("--parameter-schema-version requires a value.");
      }
      options.parameterSchemaVersion = value;
      index += 1;
    } else {
      throw new Error(`Unknown argument: ${args[index]}`);
    }
  }
  if (!options.searchConfigPath) {
    throw new Error(
      "Usage: bun run src/scripts/search-finish-position-tuning.ts --search-config path/to/search.json",
    );
  }
  if (options.randomMaxChanges < options.randomMinChanges) {
    throw new Error("--random-max-changes must be greater than or equal to --random-min-changes.");
  }
  return options;
};

const loadJson = async (path: string): Promise<unknown> => JSON.parse(await readFile(path, "utf8"));

const resolveConfigPath = (basePath: string, targetPath: string): string =>
  isAbsolute(targetPath) ? targetPath : join(dirname(basePath), targetPath);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const getString = (record: Record<string, unknown>, key: string): string | undefined =>
  typeof record[key] === "string" ? record[key] : undefined;

const getNumber = (record: Record<string, unknown>, key: string): number | undefined =>
  typeof record[key] === "number" ? record[key] : undefined;

const getCategory = (record: Record<string, unknown>): CompareArgs["category"] => {
  const value = getString(record, "category");
  return value === "all" || value === "ban-ei" || value === "jra" || value === "nar"
    ? value
    : undefined;
};

const getTarget = (record: Record<string, unknown>): CompareArgs["target"] => {
  const value = getString(record, "target");
  return value === "local" || value === "neon" ? value : undefined;
};

const compactCompareArgs = (args: SearchConfig["trials"][number]["args"]) => {
  const compacted: SearchConfig["trials"][number]["args"] = {};
  if (args?.category !== undefined) {
    compacted.category = args.category;
  }
  if (args?.concurrency !== undefined) {
    compacted.concurrency = args.concurrency;
  }
  if (args?.ensembleMode !== undefined) {
    compacted.ensembleMode = args.ensembleMode;
  }
  if (args?.fromDate !== undefined) {
    compacted.fromDate = args.fromDate;
  }
  if (args?.fromYear !== undefined) {
    compacted.fromYear = args.fromYear;
  }
  if (args?.target !== undefined) {
    compacted.target = args.target;
  }
  if (args?.toDate !== undefined) {
    compacted.toDate = args.toDate;
  }
  if (args?.toYear !== undefined) {
    compacted.toYear = args.toYear;
  }
  return compacted;
};

const toSearchConfig = (value: unknown): SearchConfig => {
  if (!isRecord(value) || !Array.isArray(value.trials)) {
    throw new Error("search config requires trials array.");
  }
  const baseArgsRecord = isRecord(value.baseArgs) ? value.baseArgs : {};
  const baseArgs = compactCompareArgs({
    category: getCategory(baseArgsRecord),
    concurrency: getNumber(baseArgsRecord, "concurrency"),
    fromDate: getString(baseArgsRecord, "fromDate"),
    fromYear: getString(baseArgsRecord, "fromYear"),
    target: getTarget(baseArgsRecord),
    toDate: getString(baseArgsRecord, "toDate"),
    toYear: getString(baseArgsRecord, "toYear"),
  });
  return {
    baseArgs,
    baseTuningConfig: value.baseTuningConfig ?? {},
    baseTuningConfigPath: getString(value, "baseTuningConfigPath"),
    changedRaceLimit: getNumber(value, "changedRaceLimit"),
    output: getString(value, "output"),
    trials: value.trials.map((trial, index) => {
      if (!isRecord(trial)) {
        throw new Error(`trial at index ${index} must be an object.`);
      }
      const args = isRecord(trial.args) ? trial.args : {};
      const ensembleMode = getString(args, "ensembleMode");
      return {
        args: compactCompareArgs({
          category: getCategory(args),
          concurrency: getNumber(args, "concurrency"),
          ensembleMode:
            ensembleMode === "auto" ||
            ensembleMode === "mixed" ||
            ensembleMode === "off" ||
            ensembleMode === "vote" ||
            ensembleMode === "weighted"
              ? ensembleMode
              : undefined,
          fromDate: getString(args, "fromDate"),
          fromYear: getString(args, "fromYear"),
          target: getTarget(args),
          toDate: getString(args, "toDate"),
          toYear: getString(args, "toYear"),
        }),
        name: getString(trial, "name") ?? `trial-${index + 1}`,
        tuningConfig: trial.tuningConfig,
        tuningConfigOverrides: trial.tuningConfigOverrides,
      };
    }),
  };
};

const deepMerge = (base: unknown, override: unknown): unknown => {
  if (!isRecord(base) || !isRecord(override)) {
    return override ?? base;
  }
  const merged: Record<string, unknown> = { ...base };
  for (const [key, value] of Object.entries(override)) {
    merged[key] = deepMerge(merged[key], value);
  }
  return merged;
};

const stableStringify = (value: unknown): string => {
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .toSorted()
      .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
};

const parameterHash = (version: string, category: string, value: unknown): string =>
  createHash("sha256").update(`${version}:${category}:${stableStringify(value)}`).digest("hex");

const randomFloat = (min: number, max: number): number =>
  Math.round((min + Math.random() * (max - min)) * 1000) / 1000;

const randomInt = (min: number, max: number): number =>
  min + Math.floor(Math.random() * (max - min + 1));

const pickRandom = <T>(values: T[]): T => {
  const value = values[Math.floor(Math.random() * values.length)];
  if (value === undefined) {
    throw new Error("cannot pick from an empty array.");
  }
  return value;
};

const pickRandomMany = <T>(values: T[], count: number): T[] => {
  const remaining = [...values];
  const picked: T[] = [];
  while (picked.length < count && remaining.length > 0) {
    const index = Math.floor(Math.random() * remaining.length);
    const value = remaining.splice(index, 1)[0];
    if (value !== undefined) {
      picked.push(value);
    }
  }
  return picked;
};

const setPath = (base: unknown, path: string[], value: unknown): unknown => {
  if (path.length === 0) {
    return value;
  }
  const [key, ...rest] = path;
  if (key === undefined) {
    return value;
  }
  const record = isRecord(base) ? { ...base } : {};
  record[key] = setPath(record[key], rest, value);
  return record;
};

const randomSourceMatcher = (category: CompareArgs["category"]): string[] => {
  if (category === "all") {
    return [pickRandom(["jra", "nar", "ban-ei"])];
  }
  return [category ?? "jra"];
};

const buildRandomOverrideForKind = (
  kind: RandomParameterKind,
  category: CompareArgs["category"],
): unknown => {
  if (kind === "score-base") {
    return setPath(
      {},
      ["scoreWeights", "base", pickRandom(["avgFinish", "recentFinish", "popularity", "odds"])],
      randomFloat(0.04, 0.75),
    );
  }
  if (kind === "score-rules") {
    return {
      scoreWeights: {
        rules: [
          {
            when: {
              distanceBands: [pickRandom(["sprint", "middle", "long"])],
              sources: randomSourceMatcher(category),
            },
            multiply: {
              [pickRandom(["avgFinish", "recentFinish", "popularity", "odds"])]: randomFloat(
                0.75,
                1.3,
              ),
            },
          },
        ],
      },
    };
  }
  if (kind === "component-models") {
    return setPath(
      {},
      [
        "componentModels",
        pickRandom(["lstmLike", "transformerLike"]),
        "weights",
        pickRandom(["avgFinish", "recentFinish", "popularity", "odds", "sameDayJockey"]),
      ],
      randomFloat(0, 0.65),
    );
  }
  if (kind === "ensemble-mode") {
    return {
      ensemble: {
        defaultMode: pickRandom(["mixed", "off", "vote", "weighted"]),
      },
    };
  }
  if (kind === "ensemble-weights") {
    return {
      ensemble: {
        weights: {
          lightgbm: randomFloat(0.25, 0.7),
          lstm: randomFloat(0.05, 0.4),
          transformer: randomFloat(0.1, 0.55),
        },
      },
    };
  }
  return {
    ensemble: {
      mixedWeightedShare: randomFloat(0.35, 0.9),
    },
  };
};

const buildRandomOverride = (
  options: CliOptions,
  category: CompareArgs["category"],
  index: number,
): unknown => {
  const availableKinds = options.randomParameterKinds ?? randomParameterKinds;
  const count = randomInt(
    options.randomMinChanges,
    Math.min(options.randomMaxChanges, availableKinds.length),
  );
  const selectedKinds = pickRandomMany(availableKinds, count);
  const override = selectedKinds.reduce<unknown>(
    (current, kind) => deepMerge(current, buildRandomOverrideForKind(kind, category)),
    {},
  );
  return deepMerge(override, {
    version: `${options.parameterSchemaVersion}-${category}-random-${String(index).padStart(6, "0")}`,
  });
};

const ensureRandomTrialTable = async (pool: Pool) => {
  await pool.query(`
    create table if not exists finish_position_tuning_random_trials (
      id bigserial primary key,
      parameter_schema_version text not null,
      category text not null,
      parameter_hash text not null,
      tuning_config jsonb not null,
      result jsonb,
      status text not null default 'running',
      error_message text,
      started_at timestamptz not null default now(),
      completed_at timestamptz,
      unique (parameter_schema_version, category, parameter_hash)
    )
  `);
};

const reserveRandomTrial = async (
  pool: Pool,
  options: CliOptions,
  category: CompareArgs["category"],
  tuningConfig: unknown,
): Promise<RandomTrialRecord | null> => {
  const hash = parameterHash(options.parameterSchemaVersion, category ?? "jra", tuningConfig);
  const result = await pool.query<{ id: string; parameter_hash: string }>(
    `
      insert into finish_position_tuning_random_trials (
        parameter_schema_version,
        category,
        parameter_hash,
        tuning_config
      )
      values ($1, $2, $3, $4::jsonb)
      on conflict (parameter_schema_version, category, parameter_hash) do nothing
      returning id, parameter_hash
    `,
    [options.parameterSchemaVersion, category ?? "jra", hash, JSON.stringify(tuningConfig)],
  );
  const row = result.rows[0];
  return row === undefined ? null : { id: Number(row.id), parameterHash: row.parameter_hash };
};

const completeRandomTrial = async (pool: Pool, id: number, result: TrialResult) => {
  await pool.query(
    `
      update finish_position_tuning_random_trials
      set status = 'completed',
          result = $2::jsonb,
          completed_at = now()
      where id = $1
    `,
    [id, JSON.stringify(result)],
  );
};

const failRandomTrial = async (pool: Pool, id: number, error: unknown) => {
  await pool.query(
    `
      update finish_position_tuning_random_trials
      set status = 'failed',
          error_message = $2,
          completed_at = now()
      where id = $1
    `,
    [id, error instanceof Error ? error.message : String(error)],
  );
};

const loadSearchConfig = async (path: string): Promise<SearchConfig> => {
  const searchConfig = toSearchConfig(await loadJson(path));
  if (searchConfig.baseTuningConfigPath === undefined) {
    return searchConfig;
  }
  const externalBase = await loadJson(resolveConfigPath(path, searchConfig.baseTuningConfigPath));
  return {
    ...searchConfig,
    baseTuningConfig: deepMerge(externalBase, searchConfig.baseTuningConfig ?? {}),
  };
};

const getTrialTuningConfig = (
  searchConfig: SearchConfig,
  trial: SearchConfig["trials"][number],
): unknown =>
  deepMerge(
    deepMerge(searchConfig.baseTuningConfig ?? {}, trial.tuningConfig ?? {}),
    trial.tuningConfigOverrides ?? {},
  );

const getLogCategoryLabel = (searchConfig: SearchConfig): string => {
  const categories = new Set<string>();
  if (searchConfig.baseArgs?.category !== undefined) {
    categories.add(searchConfig.baseArgs.category);
  }
  for (const trial of searchConfig.trials) {
    if (trial.args?.category !== undefined) {
      categories.add(trial.args.category);
    }
  }
  return [...categories].toSorted().join("-") || "jra";
};

const buildCompareArgs = (
  config: SearchConfig,
  trial: SearchConfig["trials"][number],
  tuningConfigPath: string,
  ensembleMode: string,
) => {
  const base = {
    ...config.baseArgs,
    ...trial.args,
  };
  const args = ["run", "src/scripts/compare-finish-position-predictions.ts"];
  args.push("--category", base.category ?? "jra");
  args.push("--target", base.target ?? "local");
  args.push("--concurrency", String(base.concurrency ?? 8));
  args.push("--ensemble-mode", ensembleMode);
  args.push("--tuning-config", tuningConfigPath);
  args.push("--changed-races");
  args.push("--changed-race-limit", String(config.changedRaceLimit ?? 30));
  if (base.fromDate !== undefined) {
    args.push("--from-date", base.fromDate);
  }
  if (base.toDate !== undefined) {
    args.push("--to-date", base.toDate);
  }
  if (base.fromYear !== undefined) {
    args.push("--from-year", base.fromYear);
  }
  if (base.toYear !== undefined) {
    args.push("--to-year", base.toYear);
  }
  return args;
};

const runTrial = async (
  searchConfig: SearchConfig,
  trial: SearchConfig["trials"][number],
  index: number,
  tempDir: string,
): Promise<TrialResult> => {
  const tuningConfigPath = join(tempDir, `${String(index).padStart(4, "0")}-${trial.name}.json`);
  await writeFile(
    tuningConfigPath,
    JSON.stringify(getTrialTuningConfig(searchConfig, trial), null, 2),
  );
  const output = await new Promise<{ exitCode: number | null; stderr: string; stdout: string }>(
    (resolve, reject) => {
      const proc = spawn(
        "bun",
        buildCompareArgs(searchConfig, trial, tuningConfigPath, trial.args?.ensembleMode ?? "off"),
        {
          cwd: process.cwd(),
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      let stdout = "";
      let stderr = "";
      proc.stdout.setEncoding("utf8");
      proc.stderr.setEncoding("utf8");
      proc.stdout.on("data", (chunk: string) => {
        stdout += chunk;
      });
      proc.stderr.on("data", (chunk: string) => {
        stderr += chunk;
      });
      proc.on("error", reject);
      proc.on("close", (exitCode) => {
        resolve({ exitCode, stderr, stdout });
      });
    },
  );
  if (output.exitCode !== 0) {
    throw new Error(`trial failed: ${trial.name}\n${output.stderr}\n${output.stdout}`);
  }
  const jsonStart = output.stdout.indexOf("{");
  if (jsonStart < 0) {
    throw new Error(`trial did not return JSON: ${trial.name}\n${output.stdout}`);
  }
  return toTrialResult(trial.name, JSON.parse(output.stdout.slice(jsonStart)));
};

const sortResults = (results: TrialResult[]): TrialResult[] =>
  results.toSorted(
    (left, right) =>
      right.top3ExactOrderAccuracy - left.top3ExactOrderAccuracy ||
      right.top3BoxAccuracy - left.top3BoxAccuracy ||
      right.top3PlaceRelation - left.top3PlaceRelation ||
      right.place1Accuracy - left.place1Accuracy,
  );

const toConsoleResult = (result: TrialResult) => ({
  name: result.name,
  pairScore: result.pairScore,
  place1Accuracy: result.place1Accuracy,
  place2Accuracy: result.place2Accuracy,
  place3Accuracy: result.place3Accuracy,
  raceCount: result.raceCount,
  top3BoxAccuracy: result.top3BoxAccuracy,
  top3ExactOrderAccuracy: result.top3ExactOrderAccuracy,
  top3PlaceRelation: result.top3PlaceRelation,
  top3WinnerCapture: result.top3WinnerCapture,
  top5WinnerCapture: result.top5WinnerCapture,
});

const toTrialResult = (name: string, value: unknown): TrialResult => {
  if (!isRecord(value)) {
    throw new Error(`trial did not return an object: ${name}`);
  }
  return {
    changedRaces: value.changedRaces,
    name,
    pairScore: getNumber(value, "pairScore") ?? 0,
    place1Accuracy: getNumber(value, "place1Accuracy") ?? 0,
    place2Accuracy: getNumber(value, "place2Accuracy") ?? 0,
    place3Accuracy: getNumber(value, "place3Accuracy") ?? 0,
    raceCount: getNumber(value, "raceCount") ?? 0,
    top3BoxAccuracy: getNumber(value, "top3BoxAccuracy") ?? 0,
    top3ExactOrderAccuracy: getNumber(value, "top3ExactOrderAccuracy") ?? 0,
    top3PlaceRelation: getNumber(value, "top3PlaceRelation") ?? 0,
    top3WinnerCapture: getNumber(value, "top3WinnerCapture") ?? 0,
    top5WinnerCapture: getNumber(value, "top5WinnerCapture") ?? 0,
  };
};

const main = async () => {
  const { searchConfigPath } = parseArgs(process.argv.slice(2));
  const searchConfig = await loadSearchConfig(searchConfigPath);
  const startedAt = new Date();
  const category = getLogCategoryLabel(searchConfig);
  const timestamp = startedAt
    .toISOString()
    .replaceAll(":", "")
    .replaceAll("-", "")
    .replace(".", "-");
  const tempDir = join(process.cwd(), "tmp", "finish-position-tuning-search", timestamp);
  await rm(tempDir, { force: true, recursive: true });
  await mkdir(tempDir, { recursive: true });
  const results = await Promise.all(
    searchConfig.trials.map((trial, index) => runTrial(searchConfig, trial, index, tempDir)),
  );
  for (const result of results) {
    console.log(
      `${result.name}: exact=${result.top3ExactOrderAccuracy} box=${result.top3BoxAccuracy} place1=${result.place1Accuracy}`,
    );
  }
  const sorted = sortResults(results);
  const output = {
    baseArgs: searchConfig.baseArgs ?? {},
    best: sorted[0] ?? null,
    finishedAt: new Date().toISOString(),
    resultCount: sorted.length,
    results: sorted,
    searchConfigPath,
    startedAt: startedAt.toISOString(),
  };
  const logPath = join(
    process.cwd(),
    "logs",
    `${timestamp}-${category}-finish-position-tuning-search.json`,
  );
  await mkdir(dirname(logPath), { recursive: true });
  await writeFile(logPath, JSON.stringify(output, null, 2));
  if (searchConfig.output !== undefined) {
    await mkdir(dirname(searchConfig.output), { recursive: true });
    await writeFile(searchConfig.output, JSON.stringify(output, null, 2));
  }
  console.log(`log=${logPath}`);
  console.log(
    JSON.stringify(
      {
        ...output,
        best: sorted[0] === undefined ? null : toConsoleResult(sorted[0]),
        results: sorted.map(toConsoleResult),
      },
      null,
      2,
    ),
  );
};

await main();
