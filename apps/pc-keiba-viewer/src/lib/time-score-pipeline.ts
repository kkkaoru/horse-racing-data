// Run with bun. Pure scoring pipeline for the Catalog time-score reader;
// reproduces the FILTER-aware aggregates, sub-scores and JSON assembly of the
// incumbent SQL in `getTimeScoreRows`.
import { ageBandDays, dayGap, distanceScore, recencyWeight } from "./time-score-scoring";

export interface TimeScoreHistoryRow {
  horseNumber: string;
  raceDate: string;
  keibajoCode: string | null;
  distance: number | null;
  raceTime: number | null;
  last3f: number | null;
  bodyWeight: number | null;
  carriedWeight: number | null;
  margin: number | null;
}

export interface TimeScoreCurrentHorse {
  horseNumber: string;
  horseNumberSort: number | null;
  horseName: string;
  currentAge: number | null;
}

export interface TimeScoreTargetProfile {
  targetRaceTime: number | null;
  targetLast3f: number | null;
  targetBodyWeight: number | null;
  targetCarriedWeight: number | null;
  targetMargin: number | null;
}

// Declared as type aliases (not interfaces) so they satisfy the app's
// `Record<string, unknown>`-extending row types via TypeScript's implicit
// index signature, avoiding a cast at the db boundary.
export type TimeScoreDetail = {
  label: string;
  value: number | null;
  target: number | null;
  score: number;
  weight: number;
  reason: string;
};

export type TimeScoreRow = {
  horseNumber: string;
  horseName: string;
  score: number;
  details: TimeScoreDetail[];
};

export interface WeightedProfile {
  weightedRaceTime: number | null;
  weightedLast3f: number | null;
  weightedBodyWeight: number | null;
  weightedCarriedWeight: number | null;
  weightedMargin: number | null;
  venueScore: number;
  distanceScore: number;
}

const VENUE_FALLBACK: number = 0.5;
const DISTANCE_FALLBACK: number = 0.5;
const SCORE_FALLBACK: number = 0.5;
const DISTANCE_MULTIPLIER_BASE: number = 0.5;
const RACE_TIME_RATIO: number = 0.08;
const RACE_TIME_MINIMUM_DENOMINATOR: number = 80;
const LAST3F_DENOMINATOR: number = 30;
const BODY_WEIGHT_DENOMINATOR: number = 80;
const CARRIED_WEIGHT_DENOMINATOR: number = 30;
const MARGIN_DENOMINATOR: number = 50;
const PERCENT_SCALE: number = 100;
const ROUNDING_EPSILON: number = 1e-9;

const WEIGHTS: readonly number[] = [0.3, 0.2, 0.15, 0.15, 0.1, 0.05, 0.05];

// PostgreSQL rounds `numeric` half away from zero; JavaScript's Math.round is
// half up, so the sign is applied around the absolute value. The epsilon
// absorbs binary float representation error (e.g. 1.005).
export const postgresRound = (value: number, digits: number): number => {
  if (!Number.isFinite(value)) throw new Error("Invalid rounding input");
  const factor: number = 10 ** digits;
  const scaled: number = Math.abs(value) * factor;
  return (Math.sign(value) * Math.round(scaled + ROUNDING_EPSILON)) / factor;
};

const rowWeight = (
  row: TimeScoreHistoryRow,
  raceDate: string,
  age: number | null,
  targetDistance: number | null,
): number => {
  const gap: number | null = dayGap(raceDate, row.raceDate);
  const band: number = ageBandDays(age);
  return (
    recencyWeight(gap, band) *
    (DISTANCE_MULTIPLIER_BASE +
      distanceScore(row.distance, targetDistance) * DISTANCE_MULTIPLIER_BASE)
  );
};

const weightedMean = (
  rows: readonly TimeScoreHistoryRow[],
  value: (row: TimeScoreHistoryRow) => number | null,
  weight: (row: TimeScoreHistoryRow) => number,
): number | null => {
  let numerator = 0;
  let denominator = 0;
  for (const row of rows) {
    const item: number | null = value(row);
    if (item === null) continue;
    const factor: number = weight(row);
    numerator += item * factor;
    denominator += factor;
  }
  return denominator === 0 ? null : numerator / denominator;
};

export const buildWeightedProfile = (
  rows: readonly TimeScoreHistoryRow[],
  horse: TimeScoreCurrentHorse,
  raceDate: string,
  targetDistance: number | null,
  keibajoCode: string,
): WeightedProfile => {
  const weight = (row: TimeScoreHistoryRow): number =>
    rowWeight(row, raceDate, horse.currentAge, targetDistance);
  let venueWeighted = 0;
  let venueDenominator = 0;
  let distanceNumerator = 0;
  let distanceDenominator = 0;
  for (const row of rows) {
    const factor: number = weight(row);
    if (row.keibajoCode !== null) {
      venueDenominator += factor;
      if (row.keibajoCode === keibajoCode) venueWeighted += factor;
    }
    const band: number = ageBandDays(horse.currentAge);
    const factor2: number = recencyWeight(dayGap(raceDate, row.raceDate), band);
    distanceNumerator += distanceScore(row.distance, targetDistance) * factor2;
    distanceDenominator += factor2;
  }
  return {
    weightedRaceTime: weightedMean(rows, (row) => row.raceTime, weight),
    weightedLast3f: weightedMean(rows, (row) => row.last3f, weight),
    weightedBodyWeight: weightedMean(rows, (row) => row.bodyWeight, weight),
    weightedCarriedWeight: weightedMean(rows, (row) => row.carriedWeight, weight),
    weightedMargin: weightedMean(rows, (row) => row.margin, weight),
    venueScore: venueDenominator === 0 ? VENUE_FALLBACK : venueWeighted / venueDenominator,
    distanceScore:
      distanceDenominator === 0 ? DISTANCE_FALLBACK : distanceNumerator / distanceDenominator,
  };
};

const relativeScore = (
  value: number | null,
  target: number | null,
  denominator: (target: number) => number,
): number => {
  if (value === null || target === null) return SCORE_FALLBACK;
  return Math.max(0, 1 - Math.abs(value - target) / denominator(target));
};

export const buildTimeScoreRow = (
  horse: TimeScoreCurrentHorse,
  profile: WeightedProfile,
  target: TimeScoreTargetProfile,
): TimeScoreRow => {
  const raceTimeScore: number = relativeScore(
    profile.weightedRaceTime,
    target.targetRaceTime,
    (item) => Math.max(item * RACE_TIME_RATIO, RACE_TIME_MINIMUM_DENOMINATOR),
  );
  const last3fScore: number = relativeScore(
    profile.weightedLast3f,
    target.targetLast3f,
    () => LAST3F_DENOMINATOR,
  );
  const bodyWeightScore: number = relativeScore(
    profile.weightedBodyWeight,
    target.targetBodyWeight,
    () => BODY_WEIGHT_DENOMINATOR,
  );
  const carriedWeightScore: number = relativeScore(
    profile.weightedCarriedWeight,
    target.targetCarriedWeight,
    () => CARRIED_WEIGHT_DENOMINATOR,
  );
  const marginScore: number = relativeScore(
    profile.weightedMargin,
    target.targetMargin,
    () => MARGIN_DENOMINATOR,
  );
  const scores: readonly number[] = [
    raceTimeScore,
    last3fScore,
    profile.distanceScore,
    profile.venueScore,
    bodyWeightScore,
    carriedWeightScore,
    marginScore,
  ];
  const total: number = scores.reduce((sum, item, index) => sum + item * (WEIGHTS[index] ?? 0), 0);
  const weighted = (value: number | null): number | null =>
    value === null ? null : postgresRound(value, 1);
  return {
    horseNumber: horse.horseNumber,
    horseName: horse.horseName,
    score: postgresRound(total, 2),
    details: [
      {
        label: "レースタイム",
        value: weighted(profile.weightedRaceTime),
        target: weighted(target.targetRaceTime),
        score: postgresRound(raceTimeScore, 2),
        weight: WEIGHTS[0] ?? 0,
        reason:
          "全ての過去成績を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均レースタイムに近いほど高評価",
      },
      {
        label: "上がり3F",
        value: weighted(profile.weightedLast3f),
        target: weighted(target.targetLast3f),
        score: postgresRound(last3fScore, 2),
        weight: WEIGHTS[1] ?? 0,
        reason:
          "全ての過去成績を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均上がり3Fに近いほど高評価",
      },
      {
        label: "距離適性",
        value: postgresRound(profile.distanceScore * PERCENT_SCALE, 1),
        target: PERCENT_SCALE,
        score: postgresRound(profile.distanceScore, 2),
        weight: WEIGHTS[2] ?? 0,
        reason: "全ての過去成績について、今回レース距離に近い成績ほど高く評価",
      },
      {
        label: "競馬場",
        value: postgresRound(profile.venueScore * PERCENT_SCALE, 1),
        target: PERCENT_SCALE,
        score: postgresRound(profile.venueScore, 2),
        weight: WEIGHTS[3] ?? 0,
        reason: "過去成績のうち今回と同じ競馬場の比率を日付の新しさで重み付け",
      },
      {
        label: "馬体重",
        value: weighted(profile.weightedBodyWeight),
        target: weighted(target.targetBodyWeight),
        score: postgresRound(bodyWeightScore, 2),
        weight: WEIGHTS[4] ?? 0,
        reason: "過去成績の馬体重を日付が新しいほど重く見て、同条件1〜3着馬の平均に近いほど高評価",
      },
      {
        label: "負担重量",
        value: weighted(profile.weightedCarriedWeight),
        target: weighted(target.targetCarriedWeight),
        score: postgresRound(carriedWeightScore, 2),
        weight: WEIGHTS[5] ?? 0,
        reason:
          "全ての過去成績の負担重量を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均に近いほど高評価",
      },
      {
        label: "着差",
        value: weighted(profile.weightedMargin),
        target: weighted(target.targetMargin),
        score: postgresRound(marginScore, 2),
        weight: WEIGHTS[6] ?? 0,
        reason:
          "全ての過去成績の着差を日付と今回距離への近さで重み付けし、同条件1〜3着馬の平均に近いほど高評価",
      },
    ],
  };
};

// The SQL orders by the total score descending, then by the horse-number sort
// ascending.
export const orderTimeScoreRows = (
  rows: readonly TimeScoreRow[],
  sorts: ReadonlyMap<string, number | null>,
): TimeScoreRow[] =>
  [...rows].toSorted(
    (left, right) =>
      right.score - left.score ||
      (sorts.get(left.horseNumber) ?? 0) - (sorts.get(right.horseNumber) ?? 0),
  );
