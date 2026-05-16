// Run with: bun run src/scripts/finish-position-features/search-ensemble-weights.ts \
//   --lambdarank-dir <dir> --top1-dir <dir> --top3-dir <dir> \
//   --actuals-csv <path> --validation-years 2024,2025 \
//   --output <weights.json>

import { createReadStream, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";

import {
  blendNormalizedScores,
  normalizeWithinRace,
  parsePredictionLine,
  rerankWithinRace,
  validateWeights,
  type EnsembleWeights,
  type PredictionRow,
} from "./ensemble-predictions";

const NDCG_RELEVANCE_TIERS: Readonly<Record<number, number>> = {
  1: 3,
  2: 2,
  3: 1,
};
const NDCG_K = 3;
const IDEAL_DCG = 3 / Math.log(3) + 2 / Math.log(4) + 1 / Math.log(5);
const LAMBDARANK_GRID: readonly number[] = [0.4, 0.5, 0.6, 0.7, 0.8];
const TOP1_GRID: readonly number[] = [0.05, 0.15, 0.25, 0.35];
const MIN_TOP3_WEIGHT = 0.01;
const STABILITY_TOLERANCE_DEFAULT = 0.005;

interface ActualRow {
  raceId: string;
  kettoTorokuBango: string;
  finishPosition: number;
}

export interface YearMetric {
  ndcg_at_3: number;
  race_count: number;
  year: number;
}

export interface WeightTrialResult {
  ndcg_at_3_max_year_gap: number;
  ndcg_at_3_mean: number;
  per_year: YearMetric[];
  weights: EnsembleWeights;
}

export interface SearchOutput {
  best: WeightTrialResult;
  stability_tolerance: number;
  top_candidates: WeightTrialResult[];
  validation_years: number[];
}

export interface SearchOptions {
  actualsCsv: string;
  lambdarankDir: string;
  output: string;
  stabilityTolerance: number;
  top1Dir: string;
  top3Dir: string;
  validationYears: number[];
}

const relevance = (finishPosition: number): number => NDCG_RELEVANCE_TIERS[finishPosition] ?? 0;

export const computeRaceNdcgAt3 = (
  predictionRows: PredictionRow[],
  actuals: Map<string, number>,
): number => {
  let dcg = 0;
  for (const row of predictionRows) {
    if (row.predicted_rank > NDCG_K) continue;
    const finishPosition = actuals.get(row.ketto_toroku_bango);
    if (finishPosition === undefined) continue;
    const gain = relevance(finishPosition);
    dcg += gain / Math.log(2 + row.predicted_rank);
  }
  return dcg / IDEAL_DCG;
};

export const generateWeightGrid = (): EnsembleWeights[] => {
  const grid: EnsembleWeights[] = [];
  for (const lambdarank of LAMBDARANK_GRID) {
    for (const top1 of TOP1_GRID) {
      const top3 = round4(1 - lambdarank - top1);
      if (top3 < MIN_TOP3_WEIGHT) continue;
      grid.push({ lambdarank, top1, top3 });
    }
  }
  return grid;
};

const round4 = (value: number): number => Math.round(value * 10000) / 10000;

const groupRowsByRace = (rows: PredictionRow[]): Map<string, PredictionRow[]> => {
  const groups = new Map<string, PredictionRow[]>();
  for (const row of rows) {
    const bucket = groups.get(row.race_id) ?? [];
    bucket.push(row);
    groups.set(row.race_id, bucket);
  }
  return groups;
};

const readJsonlFile = (path: string): PredictionRow[] => {
  const text = readFileSync(path, "utf-8");
  return text
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map(parsePredictionLine);
};

const buildActualsMapByRace = (rows: ActualRow[]): Map<string, Map<string, number>> => {
  const byRace = new Map<string, Map<string, number>>();
  for (const row of rows) {
    const racePositions = byRace.get(row.raceId) ?? new Map<string, number>();
    racePositions.set(row.kettoTorokuBango, row.finishPosition);
    byRace.set(row.raceId, racePositions);
  }
  return byRace;
};

const parseCsvHeader = (line: string): string[] => line.split(",").map((cell) => cell.trim());

const findColumnIndex = (header: string[], name: string): number => {
  const index = header.indexOf(name);
  if (index < 0) throw new Error(`actuals csv missing column ${name}`);
  return index;
};

const yearOfRaceId = (raceId: string): number => {
  const parts = raceId.split(":");
  const year = Number(parts[1]);
  return Number.isFinite(year) ? year : -1;
};

export const readActualsCsv = async (
  path: string,
  validationYears: ReadonlySet<number>,
): Promise<ActualRow[]> => {
  const stream = createReadStream(path, { encoding: "utf-8" });
  const reader = createInterface({ crlfDelay: Infinity, input: stream });
  const collected: ActualRow[] = [];
  let header: string[] | null = null;
  let raceIdIdx = -1;
  let horseIdIdx = -1;
  let finishIdx = -1;
  for await (const line of reader) {
    if (header === null) {
      header = parseCsvHeader(line);
      raceIdIdx = findColumnIndex(header, "race_id");
      horseIdIdx = findColumnIndex(header, "ketto_toroku_bango");
      finishIdx = findColumnIndex(header, "finish_position");
      continue;
    }
    const cells = line.split(",");
    const raceId = cells[raceIdIdx] ?? "";
    if (!validationYears.has(yearOfRaceId(raceId))) continue;
    const finishRaw = cells[finishIdx] ?? "";
    if (finishRaw.length === 0) continue;
    const finishPosition = Number(finishRaw);
    if (!Number.isFinite(finishPosition)) continue;
    collected.push({
      finishPosition,
      kettoTorokuBango: cells[horseIdIdx] ?? "",
      raceId,
    });
  }
  return collected;
};

const evaluateWeightOnYear = (
  lambdarankRows: PredictionRow[],
  top1Rows: PredictionRow[],
  top3Rows: PredictionRow[],
  weights: EnsembleWeights,
  actualsByRace: Map<string, Map<string, number>>,
): YearMetric => {
  const lambdarankNorm = normalizeWithinRace(lambdarankRows);
  const top1Norm = normalizeWithinRace(top1Rows);
  const top3Norm = normalizeWithinRace(top3Rows);
  const blended = blendNormalizedScores(lambdarankNorm, top1Norm, top3Norm, weights);
  const reranked = rerankWithinRace(blended);
  const grouped = groupRowsByRace(reranked);
  let totalNdcg = 0;
  let raceCount = 0;
  for (const [raceId, rows] of grouped) {
    const actuals = actualsByRace.get(raceId);
    if (actuals === undefined) continue;
    totalNdcg += computeRaceNdcgAt3(rows, actuals);
    raceCount += 1;
  }
  const safe = Math.max(raceCount, 1);
  return { ndcg_at_3: totalNdcg / safe, race_count: raceCount, year: -1 };
};

interface PerYearJsonl {
  lambdarank: PredictionRow[];
  top1: PredictionRow[];
  top3: PredictionRow[];
  year: number;
}

const collectJsonlByYear = (
  lambdarankDir: string,
  top1Dir: string,
  top3Dir: string,
  validationYears: readonly number[],
): PerYearJsonl[] =>
  validationYears.map((year) => ({
    lambdarank: readJsonlFile(join(lambdarankDir, `${year}.jsonl`)),
    top1: readJsonlFile(join(top1Dir, `${year}.jsonl`)),
    top3: readJsonlFile(join(top3Dir, `${year}.jsonl`)),
    year,
  }));

const evaluateOneTrial = (
  weights: EnsembleWeights,
  perYear: PerYearJsonl[],
  actualsByRace: Map<string, Map<string, number>>,
): WeightTrialResult => {
  const yearResults = perYear.map((entry) => {
    const metric = evaluateWeightOnYear(
      entry.lambdarank,
      entry.top1,
      entry.top3,
      weights,
      actualsByRace,
    );
    return { ...metric, year: entry.year };
  });
  const ndcgValues = yearResults.map((m) => m.ndcg_at_3);
  const mean = ndcgValues.reduce((acc, v) => acc + v, 0) / Math.max(ndcgValues.length, 1);
  const max = ndcgValues.reduce((acc, v) => (v > acc ? v : acc), -Infinity);
  const min = ndcgValues.reduce((acc, v) => (v < acc ? v : acc), Infinity);
  return {
    ndcg_at_3_max_year_gap: max - min,
    ndcg_at_3_mean: mean,
    per_year: yearResults,
    weights,
  };
};

const compareTrialsByMeanDesc = (a: WeightTrialResult, b: WeightTrialResult): number =>
  b.ndcg_at_3_mean - a.ndcg_at_3_mean;

const compareTrialsByStability = (a: WeightTrialResult, b: WeightTrialResult): number =>
  a.ndcg_at_3_max_year_gap - b.ndcg_at_3_max_year_gap;

export const pickBestStableTrial = (
  trials: WeightTrialResult[],
  stabilityTolerance: number,
): WeightTrialResult => {
  if (trials.length === 0) throw new Error("no trials evaluated");
  const sortedByMean = trials.toSorted(compareTrialsByMeanDesc);
  const topMean = sortedByMean[0]?.ndcg_at_3_mean ?? 0;
  const acceptable = sortedByMean.filter(
    (trial) => topMean - trial.ndcg_at_3_mean <= stabilityTolerance,
  );
  const stableSorted = acceptable.toSorted(compareTrialsByStability);
  const best = stableSorted[0];
  if (best === undefined) throw new Error("no acceptable trials");
  return best;
};

export const searchEnsembleWeights = async (options: SearchOptions): Promise<SearchOutput> => {
  const validationSet = new Set(options.validationYears);
  const perYear = collectJsonlByYear(
    options.lambdarankDir,
    options.top1Dir,
    options.top3Dir,
    options.validationYears,
  );
  const actuals = await readActualsCsv(options.actualsCsv, validationSet);
  const actualsByRace = buildActualsMapByRace(actuals);
  const grid = generateWeightGrid();
  const trials = grid.map((weights) => evaluateOneTrial(weights, perYear, actualsByRace));
  const sorted = trials.toSorted(compareTrialsByMeanDesc);
  const best = pickBestStableTrial(trials, options.stabilityTolerance);
  return {
    best,
    stability_tolerance: options.stabilityTolerance,
    top_candidates: sorted.slice(0, 10),
    validation_years: options.validationYears,
  };
};

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value`);
  return value;
};

const parseYearListArg = (raw: string): number[] => {
  const pieces = raw
    .split(",")
    .map((piece) => piece.trim())
    .filter((piece) => piece.length > 0);
  if (pieces.length === 0) throw new Error("--validation-years cannot be empty");
  return pieces.map((piece) => {
    const year = Number(piece);
    if (!Number.isInteger(year)) throw new Error(`invalid year: ${piece}`);
    return year;
  });
};

const applyArg = (
  options: Partial<SearchOptions>,
  name: string,
  value: string | undefined,
): { advance: number } => {
  if (name === "--lambdarank-dir") {
    options.lambdarankDir = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--top1-dir") {
    options.top1Dir = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--top3-dir") {
    options.top3Dir = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--actuals-csv") {
    options.actualsCsv = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--validation-years") {
    options.validationYears = parseYearListArg(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--stability-tolerance") {
    options.stabilityTolerance = Number(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--output") {
    options.output = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  throw new Error(`Unknown argument: ${name}`);
};

export const parseSearchArgs = (argv: readonly string[]): SearchOptions => {
  const collected: Partial<SearchOptions> = { stabilityTolerance: STABILITY_TOLERANCE_DEFAULT };
  let cursor = 0;
  while (cursor < argv.length) {
    const name = argv[cursor];
    if (name === undefined) break;
    const { advance } = applyArg(collected, name, argv[cursor + 1]);
    cursor += advance;
  }
  if (
    collected.lambdarankDir === undefined ||
    collected.top1Dir === undefined ||
    collected.top3Dir === undefined ||
    collected.actualsCsv === undefined ||
    collected.validationYears === undefined ||
    collected.output === undefined
  ) {
    throw new Error(
      "--lambdarank-dir, --top1-dir, --top3-dir, --actuals-csv, --validation-years, --output are all required",
    );
  }
  return {
    actualsCsv: collected.actualsCsv,
    lambdarankDir: collected.lambdarankDir,
    output: collected.output,
    stabilityTolerance: collected.stabilityTolerance ?? STABILITY_TOLERANCE_DEFAULT,
    top1Dir: collected.top1Dir,
    top3Dir: collected.top3Dir,
    validationYears: collected.validationYears,
  };
};

const writeSearchResult = (path: string, result: SearchOutput): void => {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(result, null, 2)}\n`, "utf-8");
};

const main = async (): Promise<void> => {
  const options = parseSearchArgs(process.argv.slice(2));
  validateWeights({ lambdarank: 0.6, top1: 0.2, top3: 0.2 });
  const result = await searchEnsembleWeights(options);
  writeSearchResult(options.output, result);
  process.stdout.write(
    `${JSON.stringify({
      best: result.best,
      output: options.output,
      validation_years: options.validationYears,
    })}\n`,
  );
};

if (import.meta.main) {
  await main();
}

export { round4, IDEAL_DCG };
