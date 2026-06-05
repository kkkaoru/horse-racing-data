// Run with bun.

// Public condition union.
export type RaceTrendScoreCondition = "frame" | "jockey" | "trainer" | "frameRunningStyle";

// Score detail aggregated from a past starter row.
export interface ScoreDetailInput {
  popularity: number | null;
  finishPosition: number;
  winOdds: number | null;
  frameNumber: string | null;
  jockeyKey: string | null;
  trainerKey: string | null;
  runningStyle: string | null;
}

// Per-umaban context for the current race row.
export interface UmabanContext {
  umaban: string;
  frameNumber: string | null;
  jockeyKey: string | null;
  trainerKey: string | null;
  runningStyle: string | null;
}

// Score condition flags selected by the user.
export interface RaceTrendScoreConditions {
  frame: boolean;
  jockey: boolean;
  trainer: boolean;
  frameRunningStyle: boolean;
}

// Record-filter callback params (passed to user-supplied predicate).
export interface RecordFilterParams {
  context: UmabanContext;
  detail: ScoreDetailInput;
}

// Optional per-record predicate type.
export type RecordFilter = (params: RecordFilterParams) => boolean;

// Arguments for computeRawUmabanScores.
export interface ComputeRawScoresParams {
  contexts: UmabanContext[];
  details: ScoreDetailInput[];
  conditions: RaceTrendScoreConditions;
  recordFilter?: RecordFilter;
}

// Internal helper params: keeps argument count <= 1 object.
interface TierBonusParams {
  finishPosition: number;
  popularity: number | null;
  oddsWeight: number;
}

type DetailPredicate = (detail: ScoreDetailInput, context: UmabanContext) => boolean;

// Per-condition aggregate output: value carries the log-scaled contribution,
// matched carries the eligible record count so the caller can detect "no signal".
interface ConditionContributionResult {
  value: number;
  matched: number;
}

interface ConditionAggregateInput {
  context: UmabanContext;
  details: ScoreDetailInput[];
  condition: RaceTrendScoreCondition;
  recordFilter: RecordFilter | undefined;
}

interface UmabanRawScoreParams {
  context: UmabanContext;
  details: ScoreDetailInput[];
  selectedConditions: RaceTrendScoreCondition[];
  recordFilter: RecordFilter | undefined;
}

interface EligibilityParams {
  context: UmabanContext;
  detail: ScoreDetailInput;
  recordFilter: RecordFilter | undefined;
}

// Score formula constants.
const ODDS_WEIGHT_FLOOR = 1.1;
const ODDS_WEIGHT_BIAS = 1;
const FINISH_TOP_TIER = 3;
const FINISH_BOARD_TIER = 5;
const BOARD_TIER_SCALE = 0.5;
const NEUTRAL_ODDS_WEIGHT = 1;
const TIE_SCORE = 0.5;
const EMPTY_SELECTED_CONDITION_COUNT = 0;
const NO_MATCH_COUNT = 0;
// Offset inside log(1 + n) so 1 start still produces a positive factor (log(2) > 0).
const STARTS_FACTOR_OFFSET = 1;

// Ordered, runtime-iterable list of condition keys (no enum, no `as const`).
export const RACE_TREND_SCORE_CONDITION_KEYS = [
  "frame",
  "jockey",
  "trainer",
  "frameRunningStyle",
] satisfies readonly RaceTrendScoreCondition[];

// Default conditions: frame + jockey + trainer signal.
export const DEFAULT_RACE_TREND_SCORE_CONDITIONS: RaceTrendScoreConditions = {
  frame: true,
  jockey: true,
  trainer: true,
  frameRunningStyle: false,
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

const matchByTrainer: DetailPredicate = (detail, context) =>
  detail.trainerKey !== null && detail.trainerKey === context.trainerKey;

const matchByFrameRunningStyle: DetailPredicate = (detail, context) =>
  matchByFrame(detail, context) &&
  detail.runningStyle !== null &&
  detail.runningStyle === context.runningStyle;

// Map-based dispatch to avoid if/switch chains.
const CONDITION_PREDICATES = new Map<RaceTrendScoreCondition, DetailPredicate>([
  ["frame", matchByFrame],
  ["jockey", matchByJockey],
  ["trainer", matchByTrainer],
  ["frameRunningStyle", matchByFrameRunningStyle],
]);

const selectedConditionKeys = (conditions: RaceTrendScoreConditions): RaceTrendScoreCondition[] =>
  RACE_TREND_SCORE_CONDITION_KEYS.filter((key) => conditions[key]);

const isEligibleDetail = (params: EligibilityParams): boolean => {
  const { recordFilter, context, detail } = params;
  if (recordFilter === undefined) return true;
  return recordFilter({ context, detail });
};

const sumDetailScores = (details: ScoreDetailInput[]): number =>
  details.reduce((acc, detail) => acc + scoreSinglePastRace(detail), 0);

// Aggregate one condition: filter eligible records, sum their raw scores,
// then scale by log(1 + matched) to reward higher start counts with diminishing returns.
const computeConditionContribution = (
  params: ConditionAggregateInput,
): ConditionContributionResult => {
  const predicate = CONDITION_PREDICATES.get(params.condition);
  if (predicate === undefined) return { value: 0, matched: NO_MATCH_COUNT };
  const eligible = params.details.filter(
    (detail) =>
      isEligibleDetail({
        context: params.context,
        detail,
        recordFilter: params.recordFilter,
      }) && predicate(detail, params.context),
  );
  if (eligible.length === NO_MATCH_COUNT) return { value: 0, matched: NO_MATCH_COUNT };
  const sumOfScores = sumDetailScores(eligible);
  const startsFactor = Math.log(STARTS_FACTOR_OFFSET + eligible.length);
  return { value: sumOfScores * startsFactor, matched: eligible.length };
};

const hasMatchedRecord = (result: ConditionContributionResult): boolean =>
  result.matched > NO_MATCH_COUNT;

const sumContributionValues = (results: ConditionContributionResult[]): number =>
  results.reduce((acc, result) => acc + result.value, 0);

// Aggregate all selected conditions for one umaban. Returns null when no
// condition produced a matched record (so the umaban carries no signal).
const computeUmabanRawScore = (params: UmabanRawScoreParams): number | null => {
  const perCondition = params.selectedConditions.map((condition) =>
    computeConditionContribution({
      context: params.context,
      details: params.details,
      condition,
      recordFilter: params.recordFilter,
    }),
  );
  if (!perCondition.some(hasMatchedRecord)) return null;
  return sumContributionValues(perCondition);
};

const isNumericValue = (value: number | null): value is number => value !== null;

const allNullMap = (contexts: UmabanContext[]): Map<string, number | null> =>
  new Map(contexts.map((context) => [context.umaban, null]));

// Compute raw (un-normalized) per-umaban score. For each selected condition,
// matching records are summed and then scaled by log(1 + matchedCount) so that
// umaban with consistent multi-start histories outrank rare single-spike umaban.
export const computeRawUmabanScores = (
  params: ComputeRawScoresParams,
): Map<string, number | null> => {
  const conditions = selectedConditionKeys(params.conditions);
  if (conditions.length === EMPTY_SELECTED_CONDITION_COUNT) return allNullMap(params.contexts);
  return new Map(
    params.contexts.map((context) => [
      context.umaban,
      computeUmabanRawScore({
        context,
        details: params.details,
        selectedConditions: conditions,
        recordFilter: params.recordFilter,
      }),
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
