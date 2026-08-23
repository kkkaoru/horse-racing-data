// This file runs with bun.

import type { FinishPositionStatsRow, PayoutStatsDetail, PayoutStatsRow } from "./race-types";

export interface PayoutBoxPlot {
  betType: string;
  count: number;
  median: number;
  q1: number;
  q3: number;
  samples: number[];
  whiskerHigh: number;
  whiskerLow: number;
}

export interface PayoutSummaryRange {
  average: number | null;
  betType: string;
  count: number;
  max: number;
  median: number;
  min: number;
}

export interface PayoutBeePoint {
  betType: string;
  date: string;
  index: number;
  isOutlier: boolean;
  raceName: string;
  yen: number;
}

export interface PayoutDistributionView {
  bees: PayoutBeePoint[];
  boxes: PayoutBoxPlot[];
  ranges: PayoutSummaryRange[];
}

export type FinishGroupLabel = "1着" | "2着" | "3着" | "着外";

export interface FinishHorsePoint {
  date: string;
  finishGroup: FinishGroupLabel;
  finishPosition: number;
  horseName: string;
  odds: number;
  popularity: number;
  raceName: string;
}

export interface FinishBeePoint extends FinishHorsePoint {
  index: number;
  isOutlier: boolean;
}

export interface FinishOddsDistributionView {
  bees: FinishBeePoint[];
  boxes: PayoutBoxPlot[];
}

export interface FavoriteConditionRates {
  quinellaRate: number;
  showRate: number;
  starts: number;
  winRate: number;
}

export interface ChartTooltipContent {
  lines: string[];
  meta: string | null;
  title: string;
}

export interface PayoutTooltipInput {
  bee: PayoutBeePoint;
  box: PayoutBoxPlot | null;
}

export interface FinishOddsTooltipInput {
  bee: FinishBeePoint;
  box: PayoutBoxPlot | null;
}

interface AppendFinishOddsGroupInput {
  label: FinishGroupLabel;
  points: FinishHorsePoint[];
  view: FinishOddsDistributionView;
}

export interface ChartRaceMetaInput {
  date: string;
  raceName: string;
}

const MAX_RATE: number = 100;
const RATE_DECIMAL_FACTOR: number = 10;
const FAVORITE_POPULARITY: number = 1;
const QUINELLA_RANK_LIMIT: number = 2;
const SHOW_RANK_LIMIT: number = 3;
const YEN_MAN_THRESHOLD: number = 10000;
const YEN_MAN_LARGE_THRESHOLD: number = 100000;
const UNKNOWN_PAYOUT_BET_TYPE_RANK: number = 100;
const TUKEY_FENCE: number = 1.5;
const BOX_SAMPLE_MIN: number = 4;
const BEE_JITTER_WIDTH: number = 22;
const BEE_JITTER_MOD: number = 1000;
const BEE_JITTER_STEP: number = 37;
const BEE_JITTER_SEED: number = 13;
const LOG_DOMAIN_PAD: number = 1.2;
const ODDS_SCALE: number = 10;
const WIN_FINISH: number = 1;
const PLACE_FINISH: number = 2;
const SHOW_FINISH: number = 3;
const PAYOUT_LOG_TICKS: readonly number[] = [
  100, 300, 1000, 3000, 10000, 30000, 100000, 300000, 1000000, 3000000,
];
const ODDS_LOG_TICKS: readonly number[] = [
  1, 1.5, 2, 3, 5, 10, 20, 30, 50, 100, 200, 300, 500, 1000,
];
export const FINISH_GROUP_LABELS: readonly FinishGroupLabel[] = ["1着", "2着", "3着", "着外"];
const PAYOUT_BET_TYPE_RANK = new Map<string, number>([
  ["単勝", 0],
  ["複勝", 1],
  ["枠連", 2],
  ["馬連", 3],
  ["ワイド", 4],
  ["馬単", 5],
  ["三連複", 6],
  ["3連複", 6],
  ["三連単", 7],
  ["3連単", 7],
]);

export const DEFAULT_CONDITION_PAYOUT_CHART: boolean = true;
export const DEFAULT_CONDITION_FINISH_CHART: boolean = true;
export const CONDITION_PAYOUT_CHART_NOTE: string =
  "箱は四分位、ひげは外れ値を除いた範囲、点は各レースの払戻です。縦軸は対数なので、単勝から三連単まで分布の形と大穴を同じ図で比較できます。";
export const CONDITION_FINISH_CHART_NOTE: string =
  "箱は着順グループごとの単勝オッズの四分位、ひげは外れ値を除いた範囲、点は各馬です。縦軸は対数なので本命と大穴の分布を同じ図で比べられます。";

const toRate = (count: number, starts: number): number =>
  Math.round((count * MAX_RATE * RATE_DECIMAL_FACTOR) / starts) / RATE_DECIMAL_FACTOR;

const parsePositiveNumber = (value: string): number | null => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

const payoutBetTypeOrder = (betType: string): number => {
  const rank = PAYOUT_BET_TYPE_RANK.get(betType);
  return rank === undefined ? UNKNOWN_PAYOUT_BET_TYPE_RANK : rank;
};

const compareBetType = (left: string, right: string): number => {
  const orderCompared = payoutBetTypeOrder(left) - payoutBetTypeOrder(right);
  if (orderCompared !== 0) {
    return orderCompared;
  }
  return left.localeCompare(right, "ja");
};

const comparePayoutBoxes = (left: PayoutBoxPlot, right: PayoutBoxPlot): number =>
  compareBetType(left.betType, right.betType);

const comparePayoutRanges = (left: PayoutSummaryRange, right: PayoutSummaryRange): number =>
  compareBetType(left.betType, right.betType);

const formatChartDate = (date: string): string | null => {
  if (date.length === 0) {
    return null;
  }
  if (date.length !== 8) {
    return date;
  }
  return `${date.slice(0, 4)}/${date.slice(4, 6)}/${date.slice(6, 8)}`;
};

export const quantileSorted = (sorted: number[], p: number): number | null => {
  if (sorted.length === 0) {
    return null;
  }
  const index = (sorted.length - 1) * p;
  const lo = Math.floor(index);
  const hi = Math.ceil(index);
  const left = sorted[lo];
  const right = sorted[hi];
  if (left === undefined || right === undefined) {
    return null;
  }
  if (lo === hi) {
    return left;
  }
  return left + (right - left) * (index - lo);
};

export const buildTukeyBox = (samples: number[], betType: string): PayoutBoxPlot | null => {
  const sorted = samples.filter((yen) => yen > 0).toSorted((left, right) => left - right);
  if (sorted.length < BOX_SAMPLE_MIN) {
    return null;
  }
  const q1 = quantileSorted(sorted, 0.25);
  const median = quantileSorted(sorted, 0.5);
  const q3 = quantileSorted(sorted, 0.75);
  if (q1 === null || median === null || q3 === null) {
    return null;
  }
  const fenceSpan = (q3 - q1) * TUKEY_FENCE;
  const inFence = sorted.filter((yen) => yen >= q1 - fenceSpan && yen <= q3 + fenceSpan);
  const whiskerLow = inFence[0];
  const whiskerHigh = inFence[inFence.length - 1];
  if (whiskerLow === undefined || whiskerHigh === undefined) {
    return null;
  }
  return {
    betType,
    count: sorted.length,
    median,
    q1,
    q3,
    samples: sorted,
    whiskerHigh,
    whiskerLow,
  };
};

const beesFromDetails = (details: PayoutStatsDetail[], box: PayoutBoxPlot): PayoutBeePoint[] =>
  details.map((detail, index) => ({
    betType: box.betType,
    date: detail.date,
    index,
    isOutlier: detail.payout < box.whiskerLow || detail.payout > box.whiskerHigh,
    raceName: detail.raceName,
    yen: detail.payout,
  }));

const appendPayoutRow = (
  view: PayoutDistributionView,
  row: PayoutStatsRow,
): PayoutDistributionView => {
  const positiveDetails = row.details
    .filter((detail) => detail.payout > 0)
    .toSorted((left, right) => left.payout - right.payout);
  const box = buildTukeyBox(
    positiveDetails.map((detail) => detail.payout),
    row.betType,
  );
  if (box !== null) {
    return {
      bees: [...view.bees, ...beesFromDetails(positiveDetails, box)],
      boxes: [...view.boxes, box],
      ranges: view.ranges,
    };
  }
  if (
    row.minPayout !== null &&
    row.maxPayout !== null &&
    row.medianPayout !== null &&
    row.minPayout > 0 &&
    row.maxPayout > 0
  ) {
    return {
      bees: view.bees,
      boxes: view.boxes,
      ranges: [
        ...view.ranges,
        {
          average: row.averagePayout,
          betType: row.betType,
          count: row.count,
          max: row.maxPayout,
          median: row.medianPayout,
          min: row.minPayout,
        },
      ],
    };
  }
  return view;
};

export const buildPayoutDistributionView = (rows: PayoutStatsRow[]): PayoutDistributionView => {
  const view = rows.reduce<PayoutDistributionView>(
    (current, row) => appendPayoutRow(current, row),
    { bees: [], boxes: [], ranges: [] },
  );
  return {
    bees: view.bees,
    boxes: view.boxes.toSorted(comparePayoutBoxes),
    ranges: view.ranges.toSorted(comparePayoutRanges),
  };
};

export const payoutCategoryLabels = (view: PayoutDistributionView): string[] =>
  [...view.boxes.map((box) => box.betType), ...view.ranges.map((range) => range.betType)].toSorted(
    compareBetType,
  );

export const payoutYenValues = (view: PayoutDistributionView): number[] => [
  ...view.boxes.flatMap((box) => box.samples),
  ...view.ranges.flatMap((range) =>
    range.average === null
      ? [range.min, range.median, range.max]
      : [range.min, range.median, range.max, range.average],
  ),
];

export const paddedLogDomain = (minValue: number, maxValue: number): readonly [number, number] => {
  if (minValue === maxValue) {
    return [minValue / LOG_DOMAIN_PAD, minValue * LOG_DOMAIN_PAD];
  }
  return [minValue, maxValue];
};

export const payoutLogTicks = (domain: readonly [number, number]): number[] => {
  const listed = PAYOUT_LOG_TICKS.filter((tick) => tick >= domain[0] && tick <= domain[1]);
  return listed.length === 0 ? [domain[0], domain[1]] : [...listed];
};

export const hasPayoutChartData = (view: PayoutDistributionView): boolean => {
  const values = payoutYenValues(view).filter((yen) => yen > 0);
  return values.length > 0 && payoutCategoryLabels(view).length > 0;
};

export const payoutBeeJitter = (index: number): number =>
  (((index * BEE_JITTER_STEP + BEE_JITTER_SEED) % BEE_JITTER_MOD) / BEE_JITTER_MOD) *
    BEE_JITTER_WIDTH -
  BEE_JITTER_WIDTH / 2;

export const payoutBoxForBetType = (
  boxes: PayoutBoxPlot[],
  betType: string,
): PayoutBoxPlot | null => {
  const found = boxes.find((box) => box.betType === betType);
  return found === undefined ? null : found;
};

export const finishGroupLabel = (finishPosition: number): FinishGroupLabel => {
  if (finishPosition === WIN_FINISH) {
    return "1着";
  }
  if (finishPosition === PLACE_FINISH) {
    return "2着";
  }
  if (finishPosition === SHOW_FINISH) {
    return "3着";
  }
  return "着外";
};

const firstPositiveNumber = (values: readonly (number | null)[]): number | null => {
  const found = values.find((value): value is number => value !== null && value > 0);
  return found === undefined ? null : found;
};

const parseWinOdds = (value: string): number | null => {
  const parsed = parsePositiveNumber(value);
  if (parsed === null) {
    return null;
  }
  return value.includes(".") ? parsed : parsed / ODDS_SCALE;
};

const popularityFromDetail = (value: string): number => {
  const parsed = parsePositiveNumber(value);
  return parsed === null ? 0 : parsed;
};

const pointsFromFinishDetails = (row: FinishPositionStatsRow): FinishHorsePoint[] =>
  row.details.flatMap((detail) => {
    const finishPosition = parsePositiveNumber(detail.rank);
    const odds = parseWinOdds(detail.winOdds);
    if (finishPosition === null || odds === null) {
      return [];
    }
    return [
      {
        date: detail.date,
        finishGroup: finishGroupLabel(finishPosition),
        finishPosition,
        horseName: detail.horseName,
        odds,
        popularity: popularityFromDetail(detail.popularity),
        raceName: detail.raceName,
      },
    ];
  });

const pointsFromFinishAggregates = (row: FinishPositionStatsRow): FinishHorsePoint[] => {
  if (row.finishPosition <= 0) {
    return [];
  }
  const odds = firstPositiveNumber([row.medianOdds, row.averageOdds]);
  if (odds === null) {
    return [];
  }
  const popularity = firstPositiveNumber([row.medianPopularity, row.averagePopularity]);
  const finishGroup = finishGroupLabel(row.finishPosition);
  return [
    {
      date: "",
      finishGroup,
      finishPosition: row.finishPosition,
      horseName: finishGroup,
      odds,
      popularity: popularity === null ? 0 : popularity,
      raceName: "",
    },
  ];
};

export const buildFinishHorsePoints = (rows: FinishPositionStatsRow[]): FinishHorsePoint[] => {
  const fromDetails = rows.flatMap(pointsFromFinishDetails);
  return fromDetails.length > 0 ? fromDetails : rows.flatMap(pointsFromFinishAggregates);
};

const finishBeesForGroup = (
  points: FinishHorsePoint[],
  box: PayoutBoxPlot | null,
): FinishBeePoint[] =>
  points.map((point, index) => ({
    date: point.date,
    finishGroup: point.finishGroup,
    finishPosition: point.finishPosition,
    horseName: point.horseName,
    index,
    isOutlier: box !== null && (point.odds < box.whiskerLow || point.odds > box.whiskerHigh),
    odds: point.odds,
    popularity: point.popularity,
    raceName: point.raceName,
  }));

const appendFinishOddsGroup = ({
  label,
  points,
  view,
}: AppendFinishOddsGroupInput): FinishOddsDistributionView => {
  const groupPoints = points.filter((point) => point.finishGroup === label);
  if (groupPoints.length === 0) {
    return view;
  }
  const box = buildTukeyBox(
    groupPoints.map((point) => point.odds),
    label,
  );
  return {
    bees: [...view.bees, ...finishBeesForGroup(groupPoints, box)],
    boxes: box === null ? view.boxes : [...view.boxes, box],
  };
};

export const buildFinishOddsDistributionView = (
  points: FinishHorsePoint[],
): FinishOddsDistributionView =>
  FINISH_GROUP_LABELS.reduce<FinishOddsDistributionView>(
    (view, label) => appendFinishOddsGroup({ label, points, view }),
    { bees: [], boxes: [] },
  );

export const finishGroupLabels = (view: FinishOddsDistributionView): string[] =>
  FINISH_GROUP_LABELS.filter(
    (label) =>
      view.bees.some((bee) => bee.finishGroup === label) ||
      view.boxes.some((box) => box.betType === label),
  );

export const finishOddsValues = (view: FinishOddsDistributionView): number[] => [
  ...view.boxes.flatMap((box) => box.samples),
  ...view.bees.map((bee) => bee.odds),
];

export const finishOddsLogTicks = (domain: readonly [number, number]): number[] => {
  const listed = ODDS_LOG_TICKS.filter((tick) => tick >= domain[0] && tick <= domain[1]);
  return listed.length === 0 ? [domain[0], domain[1]] : [...listed];
};

export const hasFinishChartData = (points: FinishHorsePoint[]): boolean => points.length > 0;

export const hasFinishOddsChartData = (view: FinishOddsDistributionView): boolean =>
  finishOddsValues(view).some((odds) => odds > 0);

export const finishBoxForGroup = (
  boxes: PayoutBoxPlot[],
  finishGroup: FinishGroupLabel,
): PayoutBoxPlot | null => payoutBoxForBetType(boxes, finishGroup);

export const formatAnalysisOdds = (value: number): string =>
  Number.isInteger(value) ? `${value}` : value.toFixed(1);

export const buildFavoriteConditionRates = (
  rows: FinishPositionStatsRow[],
): FavoriteConditionRates | null => {
  const favoriteDetails = rows
    .flatMap((row) => row.details)
    .filter((detail) => {
      const popularity = parsePositiveNumber(detail.popularity);
      const rank = parsePositiveNumber(detail.rank);
      return popularity === FAVORITE_POPULARITY && rank !== null;
    });
  if (favoriteDetails.length === 0) {
    return null;
  }
  const winCount = favoriteDetails.filter(
    (detail) => parsePositiveNumber(detail.rank) === 1,
  ).length;
  const quinellaCount = favoriteDetails.filter((detail) => {
    const rank = parsePositiveNumber(detail.rank);
    return rank !== null && rank <= QUINELLA_RANK_LIMIT;
  }).length;
  const showCount = favoriteDetails.filter((detail) => {
    const rank = parsePositiveNumber(detail.rank);
    return rank !== null && rank <= SHOW_RANK_LIMIT;
  }).length;
  return {
    quinellaRate: toRate(quinellaCount, favoriteDetails.length),
    showRate: toRate(showCount, favoriteDetails.length),
    starts: favoriteDetails.length,
    winRate: toRate(winCount, favoriteDetails.length),
  };
};

export const formatAnalysisYen = (value: number): string => {
  if (value >= YEN_MAN_LARGE_THRESHOLD) {
    return `${Math.round(value / YEN_MAN_THRESHOLD).toLocaleString("ja-JP")}万`;
  }
  if (value >= YEN_MAN_THRESHOLD) {
    return `${(Math.round((value / YEN_MAN_THRESHOLD) * RATE_DECIMAL_FACTOR) / RATE_DECIMAL_FACTOR).toFixed(1)}万`;
  }
  return `${Math.round(value).toLocaleString("ja-JP")}円`;
};

export const formatAnalysisRate = (value: number): string => `${value.toFixed(1)}%`;

export const formatFavoriteConditionRates = (rates: FavoriteConditionRates): string =>
  `この条件の1番人気は勝率${formatAnalysisRate(rates.winRate)} / 連対率${formatAnalysisRate(rates.quinellaRate)} / 複勝率${formatAnalysisRate(rates.showRate)}（${rates.starts.toLocaleString("ja-JP")}頭）`;

export const formatChartRaceMeta = (input: ChartRaceMetaInput): string | null => {
  const dateText = formatChartDate(input.date);
  if (input.raceName.length === 0) {
    return dateText;
  }
  if (dateText === null) {
    return input.raceName;
  }
  return `${input.raceName} / ${dateText}`;
};

export const payoutTooltipContent = (input: PayoutTooltipInput): ChartTooltipContent => {
  const quartileLine =
    input.box === null
      ? null
      : `Q1 ${formatAnalysisYen(input.box.q1)} / 中央値 ${formatAnalysisYen(input.box.median)} / Q3 ${formatAnalysisYen(input.box.q3)}`;
  const yenLine = formatAnalysisYen(input.bee.yen);
  const fenceLine = input.bee.isOutlier ? "外れ値" : "箱ひげ内";
  const lines = quartileLine === null ? [yenLine, fenceLine] : [yenLine, fenceLine, quartileLine];
  return {
    lines,
    meta: formatChartRaceMeta({ date: input.bee.date, raceName: input.bee.raceName }),
    title: input.bee.betType,
  };
};

export const finishTooltipContent = (input: FinishOddsTooltipInput): ChartTooltipContent => {
  const quartileLine =
    input.box === null
      ? null
      : `Q1 ${formatAnalysisOdds(input.box.q1)} / 中央値 ${formatAnalysisOdds(input.box.median)} / Q3 ${formatAnalysisOdds(input.box.q3)}`;
  const fenceLine = input.bee.isOutlier ? "外れ値" : "箱ひげ内";
  const lines =
    quartileLine === null
      ? [
          `${input.bee.finishPosition}着`,
          `人気 ${input.bee.popularity}`,
          `オッズ ${formatAnalysisOdds(input.bee.odds)}`,
          fenceLine,
        ]
      : [
          `${input.bee.finishPosition}着`,
          `人気 ${input.bee.popularity}`,
          `オッズ ${formatAnalysisOdds(input.bee.odds)}`,
          fenceLine,
          quartileLine,
        ];
  return {
    lines,
    meta: formatChartRaceMeta({ date: input.bee.date, raceName: input.bee.raceName }),
    title: input.bee.horseName,
  };
};
