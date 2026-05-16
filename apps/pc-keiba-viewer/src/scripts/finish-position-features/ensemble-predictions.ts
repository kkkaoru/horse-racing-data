// Run with: bun run src/scripts/finish-position-features/ensemble-predictions.ts \
//   --lambdarank-jsonl <path> --top1-jsonl <path> --top3-jsonl <path> \
//   --weights "0.6,0.2,0.2" --output <ensemble.jsonl>

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

const SINGLE_RUNNER_DENOMINATOR_FLOOR = 1;
const WEIGHT_TOLERANCE = 1e-6;
const EXPECTED_WEIGHT_COUNT = 3;

export interface PredictionRow {
  ketto_toroku_bango: string;
  predicted_rank: number;
  predicted_score: number;
  race_id: string;
  umaban: number;
}

export interface EnsembleWeights {
  lambdarank: number;
  top1: number;
  top3: number;
}

export interface EnsembleOptions {
  lambdarankJsonl: string;
  top1Jsonl: string;
  top3Jsonl: string;
  output: string;
  weights: EnsembleWeights;
}

interface NormalizedScore {
  raceId: string;
  kettoTorokuBango: string;
  umaban: number;
  predictedRank: number;
  normalizedScore: number;
}

interface CombinedRow {
  raceId: string;
  kettoTorokuBango: string;
  umaban: number;
  blendedScore: number;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const parsePredictionLine = (line: string): PredictionRow => {
  const raw: unknown = JSON.parse(line);
  if (!isRecord(raw)) throw new Error(`Prediction line is not an object: ${line}`);
  if (typeof raw.race_id !== "string") throw new Error("predicted row missing race_id");
  if (typeof raw.ketto_toroku_bango !== "string") {
    throw new Error("predicted row missing ketto_toroku_bango");
  }
  if (typeof raw.umaban !== "number") throw new Error("predicted row missing umaban");
  if (typeof raw.predicted_score !== "number") {
    throw new Error("predicted row missing predicted_score");
  }
  if (typeof raw.predicted_rank !== "number") {
    throw new Error("predicted row missing predicted_rank");
  }
  return {
    ketto_toroku_bango: raw.ketto_toroku_bango,
    predicted_rank: raw.predicted_rank,
    predicted_score: raw.predicted_score,
    race_id: raw.race_id,
    umaban: raw.umaban,
  };
};

const parseJsonlFile = (path: string): PredictionRow[] => {
  const text = readFileSync(path, "utf-8");
  return text
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map(parsePredictionLine);
};

const rankNormalize = (rank: number, runnerCount: number): number => {
  const denom = Math.max(runnerCount - 1, SINGLE_RUNNER_DENOMINATOR_FLOOR);
  return 1 - (rank - 1) / denom;
};

export const normalizeWithinRace = (rows: PredictionRow[]): NormalizedScore[] => {
  const groupedByRace = new Map<string, PredictionRow[]>();
  for (const row of rows) {
    const bucket = groupedByRace.get(row.race_id) ?? [];
    bucket.push(row);
    groupedByRace.set(row.race_id, bucket);
  }
  const normalized: NormalizedScore[] = [];
  for (const [, raceRows] of groupedByRace) {
    const runnerCount = raceRows.length;
    for (const row of raceRows) {
      normalized.push({
        kettoTorokuBango: row.ketto_toroku_bango,
        normalizedScore: rankNormalize(row.predicted_rank, runnerCount),
        predictedRank: row.predicted_rank,
        raceId: row.race_id,
        umaban: row.umaban,
      });
    }
  }
  return normalized;
};

const keyFor = (raceId: string, kettoTorokuBango: string): string =>
  `${raceId}|${kettoTorokuBango}`;

const indexByKey = (entries: NormalizedScore[]): Map<string, NormalizedScore> => {
  const lookup = new Map<string, NormalizedScore>();
  for (const entry of entries) {
    lookup.set(keyFor(entry.raceId, entry.kettoTorokuBango), entry);
  }
  return lookup;
};

const blendOneRow = (
  base: NormalizedScore,
  top1Score: number,
  top3Score: number,
  weights: EnsembleWeights,
): CombinedRow => ({
  blendedScore:
    base.normalizedScore * weights.lambdarank + top1Score * weights.top1 + top3Score * weights.top3,
  kettoTorokuBango: base.kettoTorokuBango,
  raceId: base.raceId,
  umaban: base.umaban,
});

export const blendNormalizedScores = (
  lambdarank: NormalizedScore[],
  top1: NormalizedScore[],
  top3: NormalizedScore[],
  weights: EnsembleWeights,
): CombinedRow[] => {
  const top1Index = indexByKey(top1);
  const top3Index = indexByKey(top3);
  return lambdarank.map((base) => {
    const top1Entry = top1Index.get(keyFor(base.raceId, base.kettoTorokuBango));
    const top3Entry = top3Index.get(keyFor(base.raceId, base.kettoTorokuBango));
    const top1Score = top1Entry !== undefined ? top1Entry.normalizedScore : base.normalizedScore;
    const top3Score = top3Entry !== undefined ? top3Entry.normalizedScore : base.normalizedScore;
    return blendOneRow(base, top1Score, top3Score, weights);
  });
};

const rankByBlendedScoreDesc = (a: CombinedRow, b: CombinedRow): number =>
  b.blendedScore - a.blendedScore;

export const rerankWithinRace = (rows: CombinedRow[]): PredictionRow[] => {
  const groupedByRace = new Map<string, CombinedRow[]>();
  for (const row of rows) {
    const bucket = groupedByRace.get(row.raceId) ?? [];
    bucket.push(row);
    groupedByRace.set(row.raceId, bucket);
  }
  const output: PredictionRow[] = [];
  for (const [, raceRows] of groupedByRace) {
    const sorted = raceRows.toSorted(rankByBlendedScoreDesc);
    sorted.forEach((row, index) => {
      output.push({
        ketto_toroku_bango: row.kettoTorokuBango,
        predicted_rank: index + 1,
        predicted_score: row.blendedScore,
        race_id: row.raceId,
        umaban: row.umaban,
      });
    });
  }
  return output;
};

const writeJsonl = (path: string, rows: PredictionRow[]): void => {
  mkdirSync(dirname(path), { recursive: true });
  const body = rows.map((row) => JSON.stringify(row)).join("\n");
  writeFileSync(path, `${body}\n`, "utf-8");
};

export const ensemblePredictions = (options: EnsembleOptions): PredictionRow[] => {
  validateWeights(options.weights);
  const lambdarank = normalizeWithinRace(parseJsonlFile(options.lambdarankJsonl));
  const top1 = normalizeWithinRace(parseJsonlFile(options.top1Jsonl));
  const top3 = normalizeWithinRace(parseJsonlFile(options.top3Jsonl));
  const blended = blendNormalizedScores(lambdarank, top1, top3, options.weights);
  const reranked = rerankWithinRace(blended).toSorted(comparePredictionRows);
  writeJsonl(options.output, reranked);
  return reranked;
};

const comparePredictionRows = (a: PredictionRow, b: PredictionRow): number => {
  if (a.race_id !== b.race_id) return a.race_id < b.race_id ? -1 : 1;
  return a.predicted_rank - b.predicted_rank;
};

export const validateWeights = (weights: EnsembleWeights): void => {
  const sum = weights.lambdarank + weights.top1 + weights.top3;
  if (Math.abs(sum - 1) > WEIGHT_TOLERANCE) {
    throw new Error(`weights must sum to 1.0; got ${sum}`);
  }
  if (weights.lambdarank < 0 || weights.top1 < 0 || weights.top3 < 0) {
    throw new Error("each weight must be >= 0");
  }
};

export const parseWeights = (raw: string): EnsembleWeights => {
  const pieces = raw.split(",").map((piece) => piece.trim());
  if (pieces.length !== EXPECTED_WEIGHT_COUNT) {
    throw new Error(
      `--weights expects three comma-separated numbers (lambdarank,top1,top3); got ${pieces.length}`,
    );
  }
  const [lambdarankRaw, top1Raw, top3Raw] = pieces;
  const lambdarank = Number(lambdarankRaw);
  const top1 = Number(top1Raw);
  const top3 = Number(top3Raw);
  if (!Number.isFinite(lambdarank) || !Number.isFinite(top1) || !Number.isFinite(top3)) {
    throw new Error(`--weights values must be finite numbers; got "${raw}"`);
  }
  const weights = { lambdarank, top1, top3 };
  validateWeights(weights);
  return weights;
};

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value`);
  return value;
};

const applyArg = (
  options: Partial<EnsembleOptions>,
  name: string,
  value: string | undefined,
): { advance: number } => {
  if (name === "--lambdarank-jsonl") {
    options.lambdarankJsonl = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--top1-jsonl") {
    options.top1Jsonl = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--top3-jsonl") {
    options.top3Jsonl = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--weights") {
    options.weights = parseWeights(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--output") {
    options.output = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  throw new Error(`Unknown argument: ${name}`);
};

export const parseEnsembleArgs = (argv: readonly string[]): EnsembleOptions => {
  const collected: Partial<EnsembleOptions> = {};
  let cursor = 0;
  while (cursor < argv.length) {
    const name = argv[cursor];
    if (name === undefined) break;
    const { advance } = applyArg(collected, name, argv[cursor + 1]);
    cursor += advance;
  }
  if (
    collected.lambdarankJsonl === undefined ||
    collected.top1Jsonl === undefined ||
    collected.top3Jsonl === undefined ||
    collected.output === undefined ||
    collected.weights === undefined
  ) {
    throw new Error(
      "--lambdarank-jsonl, --top1-jsonl, --top3-jsonl, --weights, --output are all required",
    );
  }
  return {
    lambdarankJsonl: collected.lambdarankJsonl,
    output: collected.output,
    top1Jsonl: collected.top1Jsonl,
    top3Jsonl: collected.top3Jsonl,
    weights: collected.weights,
  };
};

const main = (): void => {
  const options = parseEnsembleArgs(process.argv.slice(2));
  const rows = ensemblePredictions(options);
  process.stdout.write(
    `${JSON.stringify({
      input: {
        lambdarank: options.lambdarankJsonl,
        top1: options.top1Jsonl,
        top3: options.top3Jsonl,
      },
      output: options.output,
      rows_written: rows.length,
      weights: options.weights,
    })}\n`,
  );
};

if (import.meta.main) {
  main();
}
