// Run with: bun run src/scripts/finish-position-features/ensemble-with-transformer.ts \
//   --lgbm-ensemble-jsonl <path> --transformer-jsonl <path> \
//   --transformer-weight 0.20 --output <path>

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

import {
  normalizeWithinRace,
  parsePredictionLine,
  rerankWithinRace,
  type PredictionRow,
} from "./ensemble-predictions";

const DEFAULT_TRANSFORMER_WEIGHT = 0.2;
const MIN_TRANSFORMER_WEIGHT = 0;
const MAX_TRANSFORMER_WEIGHT = 1;

export interface BlendWithTransformerOptions {
  lgbmEnsembleJsonl: string;
  transformerJsonl: string;
  transformerWeight: number;
  output: string;
}

interface IndexedScore {
  raceId: string;
  kettoTorokuBango: string;
  umaban: number;
  blendedScore: number;
}

const readJsonlFile = (path: string): PredictionRow[] => {
  const text = readFileSync(path, "utf-8");
  return text
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map(parsePredictionLine);
};

export const validateTransformerWeight = (weight: number): void => {
  if (!Number.isFinite(weight)) {
    throw new Error(`--transformer-weight must be a finite number; got ${weight}`);
  }
  if (weight < MIN_TRANSFORMER_WEIGHT || weight > MAX_TRANSFORMER_WEIGHT) {
    throw new Error(
      `--transformer-weight must be in [${MIN_TRANSFORMER_WEIGHT}, ${MAX_TRANSFORMER_WEIGHT}]; got ${weight}`,
    );
  }
};

const keyFor = (raceId: string, kettoTorokuBango: string): string =>
  `${raceId}|${kettoTorokuBango}`;

export const blendTwoSources = (
  lgbmRows: PredictionRow[],
  transformerRows: PredictionRow[],
  transformerWeight: number,
): IndexedScore[] => {
  const lgbmNorm = normalizeWithinRace(lgbmRows);
  const transformerNorm = normalizeWithinRace(transformerRows);
  const transformerIndex = new Map<string, number>();
  for (const entry of transformerNorm) {
    transformerIndex.set(keyFor(entry.raceId, entry.kettoTorokuBango), entry.normalizedScore);
  }
  const lgbmWeight = 1 - transformerWeight;
  return lgbmNorm.map((base) => {
    const transformerScore = transformerIndex.get(keyFor(base.raceId, base.kettoTorokuBango));
    const effectiveTransformer =
      transformerScore !== undefined ? transformerScore : base.normalizedScore;
    return {
      blendedScore: base.normalizedScore * lgbmWeight + effectiveTransformer * transformerWeight,
      kettoTorokuBango: base.kettoTorokuBango,
      raceId: base.raceId,
      umaban: base.umaban,
    };
  });
};

const comparePredictionRows = (a: PredictionRow, b: PredictionRow): number => {
  if (a.race_id !== b.race_id) return a.race_id < b.race_id ? -1 : 1;
  return a.predicted_rank - b.predicted_rank;
};

const writeJsonl = (path: string, rows: PredictionRow[]): void => {
  mkdirSync(dirname(path), { recursive: true });
  const body = rows.map((row) => JSON.stringify(row)).join("\n");
  writeFileSync(path, `${body}\n`, "utf-8");
};

export const blendWithTransformer = (options: BlendWithTransformerOptions): PredictionRow[] => {
  validateTransformerWeight(options.transformerWeight);
  const lgbmRows = readJsonlFile(options.lgbmEnsembleJsonl);
  const transformerRows = readJsonlFile(options.transformerJsonl);
  const blended = blendTwoSources(lgbmRows, transformerRows, options.transformerWeight);
  const reranked = rerankWithinRace(blended).toSorted(comparePredictionRows);
  writeJsonl(options.output, reranked);
  return reranked;
};

const requireValue = (name: string, value: string | undefined): string => {
  if (value === undefined) throw new Error(`${name} requires a value`);
  return value;
};

const applyArg = (
  options: Partial<BlendWithTransformerOptions>,
  name: string,
  value: string | undefined,
): { advance: number } => {
  if (name === "--lgbm-ensemble-jsonl") {
    options.lgbmEnsembleJsonl = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--transformer-jsonl") {
    options.transformerJsonl = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--transformer-weight") {
    options.transformerWeight = Number(requireValue(name, value));
    return { advance: 2 };
  }
  if (name === "--output") {
    options.output = resolve(requireValue(name, value));
    return { advance: 2 };
  }
  throw new Error(`Unknown argument: ${name}`);
};

export const parseBlendArgs = (argv: readonly string[]): BlendWithTransformerOptions => {
  const collected: Partial<BlendWithTransformerOptions> = {
    transformerWeight: DEFAULT_TRANSFORMER_WEIGHT,
  };
  let cursor = 0;
  while (cursor < argv.length) {
    const name = argv[cursor];
    if (name === undefined) break;
    const { advance } = applyArg(collected, name, argv[cursor + 1]);
    cursor += advance;
  }
  if (
    collected.lgbmEnsembleJsonl === undefined ||
    collected.transformerJsonl === undefined ||
    collected.output === undefined
  ) {
    throw new Error("--lgbm-ensemble-jsonl, --transformer-jsonl, --output are all required");
  }
  return {
    lgbmEnsembleJsonl: collected.lgbmEnsembleJsonl,
    output: collected.output,
    transformerJsonl: collected.transformerJsonl,
    transformerWeight: collected.transformerWeight ?? DEFAULT_TRANSFORMER_WEIGHT,
  };
};

const main = (): void => {
  const options = parseBlendArgs(process.argv.slice(2));
  const rows = blendWithTransformer(options);
  process.stdout.write(
    `${JSON.stringify({
      input: {
        lgbm: options.lgbmEnsembleJsonl,
        transformer: options.transformerJsonl,
      },
      output: options.output,
      rows_written: rows.length,
      transformer_weight: options.transformerWeight,
    })}\n`,
  );
};

if (import.meta.main) {
  main();
}
