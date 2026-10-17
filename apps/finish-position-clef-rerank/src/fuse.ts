// Run with bun (bunx vitest). Pure clef-flash rerank helpers shared by the Worker.

export interface FuseWeights {
  base: number;
  feature: number;
  clefTop3: number;
  clefWin: number;
}

export interface BundleRunner {
  horseNumber: number;
  featureScore: number;
}

export interface ClefStateRunner {
  no: number;
  br: number | null;
  [key: string]: unknown;
}

export interface ClefState {
  runners: ClefStateRunner[];
  [key: string]: unknown;
}

export interface ClefQuestion {
  type: string;
  instructions: string;
  criteria?: Record<string, string>;
}

export interface ClefRequest {
  state: ClefState;
  questions: Record<string, ClefQuestion>;
}

export interface RerankBundle {
  modelVersion: string;
  raceKey: string;
  label: string;
  weights: FuseWeights;
  runners: BundleRunner[];
  request: ClefRequest;
}

export interface BasePrediction {
  horseNumber: string;
  modelVersion: string;
  predictedFinishNorm: number;
  predictionGeneratedAt: string;
}

export interface BaseRank {
  horseNumber: number;
  rank: number;
}

export interface ClefAnswer {
  horseNumber: number;
  win: number;
  top3: number;
}

export interface RerankRow {
  horseNumber: number;
  rank: number;
  baseRank: number;
  score: number;
  clefWin: number;
  clefTop3: number;
}

export interface RerankResult {
  modelVersion: string;
  label: string;
  raceKey: string;
  baseModelVersion: string;
  baseGeneratedAt: string;
  generatedAt: string;
  predictions: RerankRow[];
}

interface FuseInput {
  bundle: RerankBundle;
  baseRanks: BaseRank[];
  answers: ClefAnswer[];
}

interface JoinedRunner {
  horseNumber: number;
  baseRank: number;
  feature: number;
  top3: number;
  win: number;
  clefWin: number;
  clefTop3: number;
}

interface ScoredRunner {
  horseNumber: number;
  baseRank: number;
  score: number;
  clefWin: number;
  clefTop3: number;
}

const PROBABILITY_FLOOR: number = 1e-4;
const MODEL_SELECTOR: string = "clef-flash";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === "number" && Number.isFinite(value);

const isBasePrediction = (value: unknown): value is BasePrediction =>
  isRecord(value) &&
  typeof value.horseNumber === "string" &&
  typeof value.modelVersion === "string" &&
  isFiniteNumber(value.predictedFinishNorm) &&
  typeof value.predictionGeneratedAt === "string";

const isBundleRunner = (value: unknown): value is BundleRunner =>
  isRecord(value) && isFiniteNumber(value.horseNumber) && isFiniteNumber(value.featureScore);

const isWeights = (value: unknown): value is FuseWeights =>
  isRecord(value) &&
  isFiniteNumber(value.base) &&
  isFiniteNumber(value.feature) &&
  isFiniteNumber(value.clefTop3) &&
  isFiniteNumber(value.clefWin);

const isStateRunner = (value: unknown): value is ClefStateRunner =>
  isRecord(value) && isFiniteNumber(value.no);

const isRequest = (value: unknown): value is ClefRequest =>
  isRecord(value) &&
  isRecord(value.state) &&
  Array.isArray(value.state.runners) &&
  value.state.runners.every(isStateRunner) &&
  isRecord(value.questions);

export const parseBundle = (value: unknown): RerankBundle | null =>
  isRecord(value) &&
  typeof value.modelVersion === "string" &&
  typeof value.raceKey === "string" &&
  typeof value.label === "string" &&
  isWeights(value.weights) &&
  Array.isArray(value.runners) &&
  value.runners.every(isBundleRunner) &&
  isRequest(value.request)
    ? {
        modelVersion: value.modelVersion,
        raceKey: value.raceKey,
        label: value.label,
        weights: value.weights,
        runners: value.runners,
        request: value.request,
      }
    : null;

export const parseBasePredictions = (value: unknown): BasePrediction[] | null =>
  Array.isArray(value) && value.length > 1 && value.every(isBasePrediction) ? value : null;

const compareBaseNorm = (a: BasePrediction, b: BasePrediction): number =>
  a.predictedFinishNorm - b.predictedFinishNorm || Number(a.horseNumber) - Number(b.horseNumber);

// predictedFinishNorm is (rank - 1) / (field - 1); sorting recovers the served rank.
export const toBaseRanks = (rows: readonly BasePrediction[]): BaseRank[] =>
  rows
    .toSorted(compareBaseNorm)
    .map((row, index) => ({ horseNumber: Number(row.horseNumber), rank: index + 1 }));

const laterBase = (latest: BasePrediction, row: BasePrediction): BasePrediction =>
  row.predictionGeneratedAt > latest.predictionGeneratedAt ? row : latest;

// Parsed base predictions always have at least two rows, so reduce has a seed.
export const latestBase = (rows: readonly BasePrediction[]): BasePrediction =>
  rows.reduce(laterBase);

export const buildClefInput = (
  bundle: RerankBundle,
  baseRanks: readonly BaseRank[],
): Record<string, unknown> => {
  const ranks = new Map(baseRanks.map((row) => [row.horseNumber, row.rank]));
  const runners = bundle.request.state.runners.flatMap((runner) => {
    const rank = ranks.get(runner.no);
    return rank === undefined ? [] : [{ ...runner, br: rank }];
  });
  const kept = new Set(runners.map((runner) => String(runner.no)));
  const winner = bundle.request.questions.winner;
  const questions = Object.fromEntries(
    Object.entries(bundle.request.questions)
      .filter(([id]) => id === "winner" || kept.has(id.replace("top3_", "")))
      .map(([id, question]) =>
        id === "winner" && winner?.criteria !== undefined
          ? [
              id,
              {
                ...question,
                criteria: Object.fromEntries(
                  Object.entries(winner.criteria).filter(([no]) => kept.has(no)),
                ),
              },
            ]
          : [id, question],
      ),
  );
  return {
    model: MODEL_SELECTOR,
    state: { ...bundle.request.state, runners },
    questions,
  };
};

const answersOf = (value: unknown): Record<string, unknown> | null =>
  isRecord(value) && isRecord(value.answers) ? value.answers : null;

// The binding returns the System One body; the REST API wraps it in `result`.
const readAnswers = (value: unknown): Record<string, unknown> | null =>
  answersOf(value) ?? (isRecord(value) ? answersOf(value.result) : null);

export const parseClefAnswers = (
  value: unknown,
  horseNumbers: readonly number[],
): ClefAnswer[] | null => {
  const answers = readAnswers(value);
  const winner = answers?.winner;
  const probabilities = isRecord(winner) ? winner.probabilities : null;
  if (answers === null || !isRecord(probabilities)) return null;
  const rows = horseNumbers.map((horseNumber) => {
    const top3 = answers[`top3_${String(horseNumber)}`];
    const win = probabilities[String(horseNumber)];
    return isRecord(top3) && isFiniteNumber(top3.noul) && isFiniteNumber(win)
      ? { horseNumber, top3: top3.noul, win }
      : null;
  });
  return rows.every((row) => row !== null) ? rows : null;
};

// Returns a z-score function over the given values (0 when they do not vary).
const zScorer = (values: readonly number[]): ((value: number) => number) => {
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const std = Math.sqrt(
    values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length,
  );
  return (value: number): number => (std > 0 ? (value - mean) / std : 0);
};

const clampProbability = (value: number): number =>
  Math.min(1 - PROBABILITY_FLOOR, Math.max(PROBABILITY_FLOOR, value));

const logit = (value: number): number => Math.log(value / (1 - value));

const compareScored = (a: ScoredRunner, b: ScoredRunner): number =>
  b.score - a.score || a.baseRank - b.baseRank || a.horseNumber - b.horseNumber;

const joinEvidence = ({ bundle, baseRanks, answers }: FuseInput): JoinedRunner[] => {
  const features = new Map(bundle.runners.map((row) => [row.horseNumber, row.featureScore]));
  const clef = new Map(answers.map((row) => [row.horseNumber, row]));
  return baseRanks.flatMap((row) => {
    const feature = features.get(row.horseNumber);
    const answer = clef.get(row.horseNumber);
    return feature === undefined || answer === undefined
      ? []
      : [
          {
            horseNumber: row.horseNumber,
            baseRank: row.rank,
            feature,
            top3: logit(clampProbability(answer.top3)),
            win: Math.log(clampProbability(answer.win)),
            clefWin: answer.win,
            clefTop3: answer.top3,
          },
        ];
  });
};

export const fuseRanks = (input: FuseInput): RerankRow[] => {
  const rows = joinEvidence(input);
  const field = Math.max(rows.length - 1, 1);
  const baseValue = (row: JoinedRunner): number => -(row.baseRank - 1) / field;
  const base = zScorer(rows.map(baseValue));
  const feature = zScorer(rows.map((row) => row.feature));
  const top3 = zScorer(rows.map((row) => row.top3));
  const win = zScorer(rows.map((row) => row.win));
  const { weights } = input.bundle;
  return rows
    .map((row) => ({
      horseNumber: row.horseNumber,
      baseRank: row.baseRank,
      score:
        weights.base * base(baseValue(row)) +
        weights.feature * feature(row.feature) +
        weights.clefTop3 * top3(row.top3) +
        weights.clefWin * win(row.win),
      clefWin: row.clefWin,
      clefTop3: row.clefTop3,
    }))
    .toSorted(compareScored)
    .map((row, index) => ({ ...row, rank: index + 1 }));
};
