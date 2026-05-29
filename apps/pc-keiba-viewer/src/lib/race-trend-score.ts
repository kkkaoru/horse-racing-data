// Run with bun.

// Public condition union.
export type RaceTrendScoreCondition = "frame" | "jockey" | "frameRunningStyle";

// Score detail aggregated from a past starter row.
export interface ScoreDetailInput {
  popularity: number | null;
  finishPosition: number;
  winOdds: number | null;
  frameNumber: string | null;
  jockeyKey: string | null;
  runningStyle: string | null;
}

// Per-umaban context for the current race row.
export interface UmabanContext {
  umaban: string;
  frameNumber: string | null;
  jockeyKey: string | null;
  runningStyle: string | null;
}

// Score condition flags selected by the user.
export interface RaceTrendScoreConditions {
  frame: boolean;
  jockey: boolean;
  frameRunningStyle: boolean;
}

// Arguments for rawScoreForUmabanCondition.
export interface RawScoreParams {
  context: UmabanContext;
  details: ScoreDetailInput[];
  condition: RaceTrendScoreCondition;
}

// Arguments for computeRawUmabanScores.
export interface ComputeRawScoresParams {
  contexts: UmabanContext[];
  details: ScoreDetailInput[];
  conditions: RaceTrendScoreConditions;
}

// Internal helper params: keeps argument count <= 1 object.
interface TierBonusParams {
  finishPosition: number;
  popularity: number | null;
  oddsWeight: number;
}

type DetailPredicate = (detail: ScoreDetailInput, context: UmabanContext) => boolean;

// Score formula constants.
const ODDS_WEIGHT_FLOOR = 1.1;
const ODDS_WEIGHT_BIAS = 1;
const FINISH_TOP_TIER = 3;
const FINISH_BOARD_TIER = 5;
const BOARD_TIER_SCALE = 0.5;
const NEUTRAL_ODDS_WEIGHT = 1;
const TIE_SCORE = 0.5;

// Ordered, runtime-iterable list of condition keys (no enum, no `as const`).
export const RACE_TREND_SCORE_CONDITION_KEYS = [
  "frame",
  "jockey",
  "frameRunningStyle",
] satisfies readonly RaceTrendScoreCondition[];

// Default conditions: enable all signals.
export const DEFAULT_RACE_TREND_SCORE_CONDITIONS: RaceTrendScoreConditions = {
  frame: true,
  jockey: true,
  frameRunningStyle: true,
};

// Odds weight: log10(max(winOdds, floor)) + 1, neutral 1 when null.
const calculateOddsWeight = (winOdds: number | null): number =>
  winOdds === null
    ? NEUTRAL_ODDS_WEIGHT
    : Math.log10(Math.max(winOdds, ODDS_WEIGHT_FLOOR)) + ODDS_WEIGHT_BIAS;

const calculateTopTierBonus = (params: TierBonusParams): number =>
  (FINISH_TOP_TIER + 1 - params.finishPosition) * params.oddsWeight;

const calculateBoardTierBonus = (params: TierBonusParams): number => {
  const { finishPosition, popularity, oddsWeight } = params;
  const spread = popularity === null ? 0 : Math.max(0, popularity - finishPosition);
  return spread * oddsWeight * BOARD_TIER_SCALE;
};

const calculateTierBonus = (params: TierBonusParams): number => {
  if (params.finishPosition <= FINISH_TOP_TIER) return calculateTopTierBonus(params);
  if (params.finishPosition <= FINISH_BOARD_TIER) return calculateBoardTierBonus(params);
  return 0;
};

const calculateBaseSpread = (popularity: number | null, finishPosition: number): number =>
  popularity === null ? 0 : popularity - finishPosition;

const clampSpreadOutsideBoard = (baseSpread: number, finishPosition: number): number =>
  finishPosition <= FINISH_BOARD_TIER ? baseSpread : Math.min(baseSpread, 0);

// Score for a single past race.
export const scoreSinglePastRace = (detail: ScoreDetailInput): number => {
  if (detail.finishPosition <= 0) return 0;
  const { popularity, finishPosition, winOdds } = detail;
  const oddsWeight = calculateOddsWeight(winOdds);
  const tierBonus = calculateTierBonus({ finishPosition, popularity, oddsWeight });
  const baseSpread = calculateBaseSpread(popularity, finishPosition);
  return clampSpreadOutsideBoard(baseSpread, finishPosition) + tierBonus;
};

const matchByFrame: DetailPredicate = (detail, context) =>
  detail.frameNumber !== null && detail.frameNumber === context.frameNumber;

const matchByJockey: DetailPredicate = (detail, context) =>
  detail.jockeyKey !== null && detail.jockeyKey === context.jockeyKey;

const matchByFrameRunningStyle: DetailPredicate = (detail, context) =>
  matchByFrame(detail, context) &&
  detail.runningStyle !== null &&
  detail.runningStyle === context.runningStyle;

// Map-based dispatch to avoid if/switch chains.
const CONDITION_PREDICATES = new Map<RaceTrendScoreCondition, DetailPredicate>([
  ["frame", matchByFrame],
  ["jockey", matchByJockey],
  ["frameRunningStyle", matchByFrameRunningStyle],
]);

const averageScore = (details: ScoreDetailInput[]): number =>
  details.reduce((acc, detail) => acc + scoreSinglePastRace(detail), 0) / details.length;

// Raw score for one umaban under one condition (null when unknown condition or no matching details).
export const rawScoreForUmabanCondition = (params: RawScoreParams): number | null => {
  const predicate = CONDITION_PREDICATES.get(params.condition);
  if (predicate === undefined) return null;
  const filtered = params.details.filter((detail) => predicate(detail, params.context));
  if (filtered.length === 0) return null;
  return averageScore(filtered);
};

const selectedConditions = (conditions: RaceTrendScoreConditions): RaceTrendScoreCondition[] =>
  RACE_TREND_SCORE_CONDITION_KEYS.filter((key) => conditions[key]);

const isNumericValue = (value: number | null): value is number => value !== null;

const averageNullable = (values: (number | null)[]): number | null => {
  const valid = values.filter(isNumericValue);
  if (valid.length === 0) return null;
  return valid.reduce((acc, value) => acc + value, 0) / valid.length;
};

const allNullMap = (contexts: UmabanContext[]): Map<string, number | null> =>
  new Map(contexts.map((context) => [context.umaban, null]));

interface ScoreContextAcrossParams {
  context: UmabanContext;
  details: ScoreDetailInput[];
  conditions: RaceTrendScoreCondition[];
}

const scoreContextAcross = (params: ScoreContextAcrossParams): number | null =>
  averageNullable(
    params.conditions.map((condition) =>
      rawScoreForUmabanCondition({
        context: params.context,
        details: params.details,
        condition,
      }),
    ),
  );

// Compute raw (un-normalized) per-umaban score across selected conditions.
export const computeRawUmabanScores = (
  params: ComputeRawScoresParams,
): Map<string, number | null> => {
  const conditions = selectedConditions(params.conditions);
  if (conditions.length === 0) return allNullMap(params.contexts);
  return new Map(
    params.contexts.map((context) => [
      context.umaban,
      scoreContextAcross({ context, details: params.details, conditions }),
    ]),
  );
};

const extractRawValues = (raw: Map<string, number | null>): number[] =>
  [...raw.values()].filter(isNumericValue);

interface ScaledEntryParams {
  entry: [string, number | null];
  min: number;
  scale: number;
}

const buildScaledEntry = (params: ScaledEntryParams): [string, number | null] => {
  const [key, value] = params.entry;
  return [key, value === null ? null : (value - params.min) / params.scale];
};

const buildAllNullEntry = (entry: [string, number | null]): [string, number | null] => [
  entry[0],
  null,
];

const buildTieEntry = (entry: [string, number | null]): [string, number | null] => [
  entry[0],
  entry[1] === null ? null : TIE_SCORE,
];

// Min-max normalize per-umaban raw score map to [0, 1].
export const normalizeUmabanScores = (
  raw: Map<string, number | null>,
): Map<string, number | null> => {
  const values = extractRawValues(raw);
  if (values.length === 0) return new Map([...raw].map(buildAllNullEntry));
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min === max) return new Map([...raw].map(buildTieEntry));
  const scale = max - min;
  return new Map([...raw].map((entry) => buildScaledEntry({ entry, min, scale })));
};
