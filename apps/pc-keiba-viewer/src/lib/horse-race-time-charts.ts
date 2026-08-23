// This file runs with bun.

import { scaleLinear } from "d3";

import { cleanText, formatDate, formatKeibajo } from "./format";
import type { HorseRaceResult, RaceTimeStats } from "./race-types";
import { formatRunnerNumber, isBanEiKeibajoCode } from "./runner-format";

export interface RaceTimeChartPoint {
  carriedWeightKg: number | null;
  dateLabel: string;
  distanceDeltaMeters: number | null;
  distanceLabel: string;
  distanceWeight: number;
  finishRank: number | null;
  horseName: string;
  id: string;
  isLatest: boolean;
  keibajoLabel: string;
  kohan3fTenths: number | null;
  pastJockeyLabel: string;
  radius: number | null;
  scaledSohaTimeTenths: number;
  scheduledJockeyLabel: string;
  sortKey: string;
  sohaTimeTenths: number;
  stroke: string;
  umaban: string;
  x: number;
  y: number;
}

export interface RaceTimeChartAxisTick {
  label: string;
  x: number;
  y: number;
}

export interface RaceTimeReferenceLine {
  kind: RaceTimeReferenceKind;
  label: string;
  orientation: "horizontal" | "vertical";
  stroke: string;
  strokeDasharray: string;
  x1: number;
  x2: number;
  y1: number;
  y2: number;
}

export interface DrawnRaceTimeChart {
  height: number;
  horseLinks: RaceTimeHorseLink[];
  plotBottom: number;
  plotLeft: number;
  plotRight: number;
  plotTop: number;
  points: RaceTimeChartPoint[];
  references: RaceTimeReferenceLine[];
  width: number;
  xAxisTitle: string;
  xTicks: RaceTimeChartAxisTick[];
  yAxisTitle: string;
  yTicks: RaceTimeChartAxisTick[];
}

export interface RaceTimeHorseLink {
  path: string;
  umaban: string;
}

export type RaceTimeReferenceKind =
  | "averageKohan3f"
  | "averageRaceTime"
  | "fastestKohan3f"
  | "fastestRaceTime"
  | "medianKohan3f"
  | "medianRaceTime";

interface RaceTimeChartDomain {
  maxValue: number;
  minValue: number;
}

interface RaceTimeChartLayout {
  invertX: boolean;
  xScale: (tenths: number) => number;
  yScale: (tenths: number) => number;
}

interface BuildLayoutParams {
  invertX: boolean;
  plotBottom: number;
  plotTop: number;
  xDomain: RaceTimeChartDomain;
  yDomain: RaceTimeChartDomain;
}

interface BanEiAbilityRow {
  carriedWeightKg: number;
  finishRank: number;
  result: HorseRaceResult;
  scaledSohaTimeTenths: number;
  sohaTimeTenths: number;
}

interface CollectBanEiAbilityRowsParams {
  currentDistance: number | null;
  results: HorseRaceResult[];
}

interface ToBanEiAbilityPointParams {
  currentDistance: number | null;
  isLatest: boolean;
  layout: RaceTimeChartLayout;
  row: BanEiAbilityRow;
  xValue: number;
}

interface ToReferenceLineParams {
  layout: RaceTimeChartLayout;
  plotBottom: number;
  plotTop: number;
  spec: RaceTimeReferenceSpec;
}

interface BuildDrawnRaceTimeChartParams {
  currentDistance: string | null | undefined;
  keibajoCode?: string | null;
  results: HorseRaceResult[];
  stats: RaceTimeStats | null;
}

interface RaceTimeChartRow {
  kohan3fTenths: number | null;
  result: HorseRaceResult;
  scaledSohaTimeTenths: number;
  sohaTimeTenths: number;
  xValue: number;
}

interface CollectChartRowsParams {
  currentDistance: number | null;
  results: HorseRaceResult[];
}

interface RaceTimeReferenceSpec {
  kind: RaceTimeReferenceKind;
  label: string;
  orientation: "horizontal" | "vertical";
  stroke: string;
  strokeDasharray: string;
  value: number | null;
}

interface ToChartPointParams {
  currentDistance: number | null;
  layout: RaceTimeChartLayout;
  row: RaceTimeChartRow;
}

export const DEFAULT_SHOW_RESULTS_CHART: boolean = true;

export const RACE_TIME_CHART_NOTE: string =
  "各点は出走予定馬の過去レースです。レースタイムは今走の距離に比例換算しています。今走と同じ距離ほど点は濃く、距離が離れるほど薄くします。上ほど換算タイムが速く、右ほど上がり3Fが速い。点の色は着順、数字は馬番。最速・平均・中央値の線は目安です。";

export const RACE_TIME_CHART_BAN_EI_NOTE: string =
  "ばんえいには上がり3Fがありません。1つの図で斤量・換算タイム・着順を見ます。上ほど速く、右ほど斤量が重い。点の色と大きさは着順、数字は馬番。同じ馬の複数レースは薄い線でつなぎます。";

export const RACE_TIME_CHART_EMPTY: string = "レースタイムと上がり3Fが揃った競走成績がありません。";

export const RACE_TIME_CHART_BAN_EI_EMPTY: string =
  "レースタイムと着順と斤量が揃った競走成績がありません。";

export const RACE_TIME_CHART_VIEW_WIDTH: number = 720;
export const RACE_TIME_CHART_VIEW_HEIGHT: number = 400;
export const RACE_TIME_CHART_PLOT_LEFT: number = 78;
export const RACE_TIME_CHART_PLOT_RIGHT: number = 704;
export const RACE_TIME_CHART_PLOT_TOP: number = 20;
export const RACE_TIME_CHART_PLOT_BOTTOM: number = 332;
export const RACE_TIME_CHART_TOOLTIP_OFFSET: number = 12;
export const RACE_TIME_CHART_X_AXIS_TITLE: string = "上がり3F（右が速い）";
export const RACE_TIME_CHART_BAN_EI_X_AXIS_TITLE: string = "着順（右が上位）";
export const RACE_TIME_CHART_BAN_EI_WEIGHT_X_AXIS_TITLE: string = "斤量（右が重い）";
export const RACE_TIME_CHART_Y_AXIS_TITLE: string = "換算レースタイム（今走距離、上が速い）";
export const BAN_EI_ABILITY_HORSE_LINK_STROKE: string = "#d5ddd8";
export const BAN_EI_FINISH_FIRST_RADIUS: number = 7;
export const BAN_EI_FINISH_SECOND_RADIUS: number = 5.8;
export const BAN_EI_FINISH_THIRD_RADIUS: number = 4.8;
export const BAN_EI_FINISH_PLACE_RADIUS: number = 3.8;
export const BAN_EI_FINISH_OTHER_RADIUS: number = 2.6;
export const RACE_TIME_REFERENCE_STROKE_OPACITY: number = 0.1;
export const RACE_TIME_REFERENCE_STROKE_WIDTH: number = 0.45;
export const RACE_TIME_DISTANCE_FADE_METERS: number = 400;
export const RACE_TIME_MIN_DISTANCE_WEIGHT: number = 0.18;

const PLOT_LEFT: number = RACE_TIME_CHART_PLOT_LEFT;
const PLOT_RIGHT: number = RACE_TIME_CHART_PLOT_RIGHT;
const PLOT_TOP: number = RACE_TIME_CHART_PLOT_TOP;
const PLOT_BOTTOM: number = RACE_TIME_CHART_PLOT_BOTTOM;
const RATIO_PAD_ABS: number = 8;
const RATIO_PAD_RATIO: number = 0.08;
const TICK_FRACTIONS: number[] = [0, 0.25, 0.5, 0.75, 1];
const FINISH_FIRST_STROKE: string = "#b45309";
const FINISH_SECOND_STROKE: string = "#64748b";
const FINISH_THIRD_STROKE: string = "#c2410c";
const FINISH_PLACE_STROKE: string = "#355f9f";
const FINISH_OTHER_STROKE: string = "#94a3b8";
const FASTEST_STROKE: string = "#be123c";
const AVERAGE_STROKE: string = "#166534";
const MEDIAN_STROKE: string = "#4338ca";
const RACE_TIME_DASH: string = "6 4";
const KOHAN_DASH: string = "2 3";
const HEX_RADIX: number = 16;
const BAN_EI_WEIGHT_SENTINEL: string = "FFF";

const parseClockNumber = (value: string | null | undefined): number | null => {
  const cleaned = cleanText(value, "");
  if (cleaned.length === 0 || /^0+$/.test(cleaned)) {
    return null;
  }
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

export const parseSohaTimeTenths = (
  value: string | null | undefined,
  decodeBanEi: boolean,
): number | null => {
  const raw = parseClockNumber(value);
  if (raw === null) {
    return null;
  }
  if (!decodeBanEi) {
    return raw;
  }
  const padded = cleanText(value, "").padStart(4, "0");
  const minutes = Number(padded.slice(0, -3));
  const seconds = Number(padded.slice(-3, -1));
  const tenths = Number(padded.slice(-1));
  return minutes * 600 + seconds * 10 + tenths;
};

export const parseKohan3fTenths = (value: string | null | undefined): number | null =>
  parseClockNumber(value);

export const parseRaceDistanceMeters = (value: string | null | undefined): number | null => {
  const parsed = Number(cleanText(value, ""));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

export const scaleSohaTimeToDistance = (
  sohaTimeTenths: number,
  fromDistance: number,
  toDistance: number,
): number => sohaTimeTenths * (toDistance / fromDistance);

export const raceTimeDistanceWeight = (
  resultDistance: number | null,
  currentDistance: number | null,
): number => {
  if (currentDistance === null || resultDistance === null) {
    return 1;
  }
  const faded = 1 - Math.abs(resultDistance - currentDistance) / RACE_TIME_DISTANCE_FADE_METERS;
  return faded < RACE_TIME_MIN_DISTANCE_WEIGHT ? RACE_TIME_MIN_DISTANCE_WEIGHT : faded;
};

export const parseRaceFinishRank = (value: string | null | undefined): number | null => {
  const parsed = Number(cleanText(value, ""));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

export const formatRaceTimeTenthsLabel = (tenths: number): string => {
  const rounded = Math.round(tenths);
  const minutes = Math.floor(rounded / 600);
  const seconds = Math.floor((rounded % 600) / 10);
  const remainder = rounded % 10;
  if (minutes > 0) {
    return `${minutes}:${String(seconds).padStart(2, "0")}.${remainder}`;
  }
  return `${seconds}.${remainder}`;
};

export const formatKohan3fTenthsLabel = (tenths: number): string => (tenths / 10).toFixed(1);

export const formatFinishRankAxisLabel = (rank: number): string => `${Math.round(rank)}着`;

export const formatCarriedWeightKgLabel = (kg: number): string => `${Math.round(kg)}kg`;

export const banEiFinishMarkRadius = (rank: number | null): number => {
  if (rank === 1) {
    return BAN_EI_FINISH_FIRST_RADIUS;
  }
  if (rank === 2) {
    return BAN_EI_FINISH_SECOND_RADIUS;
  }
  if (rank === 3) {
    return BAN_EI_FINISH_THIRD_RADIUS;
  }
  if (rank !== null && rank <= 5) {
    return BAN_EI_FINISH_PLACE_RADIUS;
  }
  return BAN_EI_FINISH_OTHER_RADIUS;
};

export const parseBanEiCarriedWeightKg = (value: string | null | undefined): number | null => {
  const cleaned = cleanText(value, "");
  if (
    cleaned.length === 0 ||
    /^0+$/.test(cleaned) ||
    cleaned.toUpperCase() === BAN_EI_WEIGHT_SENTINEL
  ) {
    return null;
  }
  const parsed = Number.parseInt(cleaned, HEX_RADIX);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
};

export const raceTimeChartNote = (isBanEi: boolean): string => {
  if (isBanEi) {
    return RACE_TIME_CHART_BAN_EI_NOTE;
  }
  return RACE_TIME_CHART_NOTE;
};

export const raceTimeChartEmptyMessage = (isBanEi: boolean): string => {
  if (isBanEi) {
    return RACE_TIME_CHART_BAN_EI_EMPTY;
  }
  return RACE_TIME_CHART_EMPTY;
};

export const raceTimeFinishStroke = (rank: number | null): string => {
  if (rank === 1) {
    return FINISH_FIRST_STROKE;
  }
  if (rank === 2) {
    return FINISH_SECOND_STROKE;
  }
  if (rank === 3) {
    return FINISH_THIRD_STROKE;
  }
  if (rank !== null && rank <= 5) {
    return FINISH_PLACE_STROKE;
  }
  return FINISH_OTHER_STROKE;
};

const paddedDomain = (domain: RaceTimeChartDomain): RaceTimeChartDomain => {
  const span = domain.maxValue - domain.minValue;
  const pad = span === 0 ? RATIO_PAD_ABS : span * RATIO_PAD_RATIO;
  return {
    maxValue: domain.maxValue + pad,
    minValue: domain.minValue - pad,
  };
};

const collectDomain = (values: number[]): RaceTimeChartDomain =>
  paddedDomain({
    maxValue: Math.max(...values),
    minValue: Math.min(...values),
  });

const buildLayout = ({
  invertX,
  plotBottom,
  plotTop,
  xDomain,
  yDomain,
}: BuildLayoutParams): RaceTimeChartLayout => {
  const xDomainEnds = invertX
    ? [xDomain.maxValue, xDomain.minValue]
    : [xDomain.minValue, xDomain.maxValue];
  const xScale = scaleLinear().domain(xDomainEnds).range([PLOT_LEFT, PLOT_RIGHT]);
  const yScale = scaleLinear()
    .domain([yDomain.maxValue, yDomain.minValue])
    .range([plotBottom, plotTop]);
  return {
    invertX,
    xScale: (tenths) => xScale(tenths),
    yScale: (tenths) => yScale(tenths),
  };
};

const tickValue = (domain: RaceTimeChartDomain, fraction: number, invert: boolean): number => {
  if (invert) {
    return domain.maxValue + (domain.minValue - domain.maxValue) * fraction;
  }
  return domain.minValue + (domain.maxValue - domain.minValue) * fraction;
};

const compareRaceTimeChartPoints = (
  left: RaceTimeChartPoint,
  right: RaceTimeChartPoint,
): number => {
  if (left.umaban !== right.umaban) {
    return left.umaban.localeCompare(right.umaban, "ja");
  }
  return left.id.localeCompare(right.id, "ja");
};

const toChartPoint = ({ currentDistance, layout, row }: ToChartPointParams): RaceTimeChartPoint => {
  const finishRank = parseRaceFinishRank(row.result.kakuteiChakujun);
  const umaban = formatRunnerNumber(row.result.currentUmaban);
  const distanceMeters = parseRaceDistanceMeters(row.result.kyori);
  const distanceDeltaMeters =
    currentDistance === null || distanceMeters === null ? null : distanceMeters - currentDistance;
  return {
    carriedWeightKg: null,
    dateLabel:
      row.result.kaisaiNen.length >= 4 && row.result.kaisaiTsukihi.length >= 4
        ? formatDate(row.result.kaisaiNen, row.result.kaisaiTsukihi)
        : "-",
    distanceDeltaMeters,
    distanceLabel: distanceMeters === null ? "-" : `${distanceMeters}m`,
    distanceWeight: raceTimeDistanceWeight(distanceMeters, currentDistance),
    finishRank,
    horseName: cleanText(row.result.bamei),
    id: `${umaban}-${row.result.kaisaiNen}${row.result.kaisaiTsukihi}-${row.result.keibajoCode}-${row.result.raceBango}`,
    isLatest: true,
    keibajoLabel: formatKeibajo(row.result.keibajoCode),
    kohan3fTenths: row.kohan3fTenths,
    pastJockeyLabel: cleanText(row.result.kishumeiRyakusho),
    radius: null,
    scaledSohaTimeTenths: row.scaledSohaTimeTenths,
    scheduledJockeyLabel: cleanText(row.result.currentJockey),
    sortKey: `${row.result.kaisaiNen}${row.result.kaisaiTsukihi}${row.result.keibajoCode}${row.result.raceBango}`,
    sohaTimeTenths: row.sohaTimeTenths,
    stroke: raceTimeFinishStroke(finishRank),
    umaban,
    x: layout.xScale(row.xValue),
    y: layout.yScale(row.scaledSohaTimeTenths),
  };
};

const collectChartRows = ({
  currentDistance,
  results,
}: CollectChartRowsParams): RaceTimeChartRow[] =>
  results.flatMap((result) => {
    const sohaTimeTenths = parseSohaTimeTenths(
      result.sohaTime,
      isBanEiKeibajoCode(result.keibajoCode),
    );
    const kohan3fTenths = parseKohan3fTenths(result.kohan3f);
    if (sohaTimeTenths === null || kohan3fTenths === null) {
      return [];
    }
    const resultDistance = parseRaceDistanceMeters(result.kyori);
    if (currentDistance !== null && resultDistance === null) {
      return [];
    }
    const scaledSohaTimeTenths =
      currentDistance === null || resultDistance === null
        ? sohaTimeTenths
        : scaleSohaTimeToDistance(sohaTimeTenths, resultDistance, currentDistance);
    return [{ kohan3fTenths, result, scaledSohaTimeTenths, sohaTimeTenths, xValue: kohan3fTenths }];
  });

const kohanReferenceSpecs = (stats: RaceTimeStats): RaceTimeReferenceSpec[] => [
  {
    kind: "fastestKohan3f",
    label: "最速上がり3F",
    orientation: "vertical",
    stroke: FASTEST_STROKE,
    strokeDasharray: KOHAN_DASH,
    value: stats.fastestKohan3f,
  },
  {
    kind: "averageKohan3f",
    label: "平均上がり3F",
    orientation: "vertical",
    stroke: AVERAGE_STROKE,
    strokeDasharray: KOHAN_DASH,
    value: stats.averageKohan3f,
  },
  {
    kind: "medianKohan3f",
    label: "中央値上がり3F",
    orientation: "vertical",
    stroke: MEDIAN_STROKE,
    strokeDasharray: KOHAN_DASH,
    value: stats.medianKohan3f,
  },
];

const referenceSpecs = (
  stats: RaceTimeStats | null,
  includeKohan: boolean,
): RaceTimeReferenceSpec[] => {
  if (stats === null) {
    return [];
  }
  const raceTimeSpecs: RaceTimeReferenceSpec[] = [
    {
      kind: "fastestRaceTime",
      label: "最速レースタイム",
      orientation: "horizontal",
      stroke: FASTEST_STROKE,
      strokeDasharray: RACE_TIME_DASH,
      value: stats.fastestRaceTime,
    },
    {
      kind: "averageRaceTime",
      label: "平均レースタイム",
      orientation: "horizontal",
      stroke: AVERAGE_STROKE,
      strokeDasharray: RACE_TIME_DASH,
      value: stats.averageRaceTime,
    },
    {
      kind: "medianRaceTime",
      label: "中央値レースタイム",
      orientation: "horizontal",
      stroke: MEDIAN_STROKE,
      strokeDasharray: RACE_TIME_DASH,
      value: stats.medianRaceTime,
    },
  ];
  if (!includeKohan) {
    return raceTimeSpecs;
  }
  return [...raceTimeSpecs, ...kohanReferenceSpecs(stats)];
};

const toReferenceLine = ({
  layout,
  plotBottom,
  plotTop,
  spec,
}: ToReferenceLineParams): RaceTimeReferenceLine | null => {
  if (spec.value === null) {
    return null;
  }
  if (spec.orientation === "horizontal") {
    const y = layout.yScale(spec.value);
    return {
      kind: spec.kind,
      label: spec.label,
      orientation: spec.orientation,
      stroke: spec.stroke,
      strokeDasharray: spec.strokeDasharray,
      x1: PLOT_LEFT,
      x2: PLOT_RIGHT,
      y1: y,
      y2: y,
    };
  }
  const x = layout.xScale(spec.value);
  return {
    kind: spec.kind,
    label: spec.label,
    orientation: spec.orientation,
    stroke: spec.stroke,
    strokeDasharray: spec.strokeDasharray,
    x1: x,
    x2: x,
    y1: plotTop,
    y2: plotBottom,
  };
};

const formatSignedDistanceDelta = (deltaMeters: number): string =>
  deltaMeters > 0 ? `+${deltaMeters}` : String(deltaMeters);

export const formatRaceTimeChartTooltip = (point: RaceTimeChartPoint): string[] => {
  const scaledLine =
    point.scaledSohaTimeTenths === point.sohaTimeTenths
      ? []
      : [`換算 ${formatRaceTimeTenthsLabel(point.scaledSohaTimeTenths)}`];
  const distanceLine =
    point.distanceDeltaMeters === null || point.distanceDeltaMeters === 0
      ? [`距離 ${point.distanceLabel}`]
      : [
          `距離 ${point.distanceLabel}`,
          `距離差 ${formatSignedDistanceDelta(point.distanceDeltaMeters)}m`,
        ];
  return [
    `${point.umaban} ${point.horseName}`,
    point.dateLabel,
    point.keibajoLabel,
    `過去騎手 ${point.pastJockeyLabel}`,
    `予定騎手 ${point.scheduledJockeyLabel}`,
    ...distanceLine,
    `着順 ${point.finishRank === null ? "-" : String(point.finishRank)}`,
    ...(point.carriedWeightKg === null
      ? []
      : [`斤量 ${formatCarriedWeightKgLabel(point.carriedWeightKg)}`]),
    `レースタイム ${formatRaceTimeTenthsLabel(point.sohaTimeTenths)}`,
    ...scaledLine,
    ...(point.kohan3fTenths === null
      ? []
      : [`上がり3F ${formatKohan3fTenthsLabel(point.kohan3fTenths)}`]),
  ];
};

export const buildDrawnRaceTimeChart = ({
  currentDistance,
  results,
  stats,
}: BuildDrawnRaceTimeChartParams): DrawnRaceTimeChart | null => {
  const specs = referenceSpecs(stats, true);
  const currentDistanceMeters = parseRaceDistanceMeters(currentDistance);
  const rows = collectChartRows({
    currentDistance: currentDistanceMeters,
    results,
  });
  if (rows.length === 0) {
    return null;
  }
  const xValues = [
    ...rows.map((row) => row.xValue),
    ...specs.flatMap((spec) =>
      spec.orientation === "vertical" && spec.value !== null ? [spec.value] : [],
    ),
  ];
  const yValues = [
    ...rows.map((row) => row.scaledSohaTimeTenths),
    ...specs.flatMap((spec) =>
      spec.orientation === "horizontal" && spec.value !== null ? [spec.value] : [],
    ),
  ];
  const xDomain = collectDomain(xValues);
  const yDomain = collectDomain(yValues);
  const layout = buildLayout({
    invertX: true,
    plotBottom: PLOT_BOTTOM,
    plotTop: PLOT_TOP,
    xDomain,
    yDomain,
  });
  const points = rows
    .map((row) => toChartPoint({ currentDistance: currentDistanceMeters, layout, row }))
    .toSorted(compareRaceTimeChartPoints);
  return {
    height: RACE_TIME_CHART_VIEW_HEIGHT,
    horseLinks: [],
    plotBottom: PLOT_BOTTOM,
    plotLeft: PLOT_LEFT,
    plotRight: PLOT_RIGHT,
    plotTop: PLOT_TOP,
    points,
    references: specs.flatMap((spec) => {
      const line = toReferenceLine({
        layout,
        plotBottom: PLOT_BOTTOM,
        plotTop: PLOT_TOP,
        spec,
      });
      return line === null ? [] : [line];
    }),
    width: RACE_TIME_CHART_VIEW_WIDTH,
    xAxisTitle: RACE_TIME_CHART_X_AXIS_TITLE,
    xTicks: TICK_FRACTIONS.map((fraction) => {
      const value = tickValue(xDomain, fraction, true);
      return {
        label: formatKohan3fTenthsLabel(value),
        x: layout.xScale(value),
        y: PLOT_BOTTOM,
      };
    }),
    yAxisTitle: RACE_TIME_CHART_Y_AXIS_TITLE,
    yTicks: TICK_FRACTIONS.map((fraction) => {
      const value = tickValue(yDomain, fraction, true);
      return {
        label: formatRaceTimeTenthsLabel(value),
        x: PLOT_LEFT,
        y: layout.yScale(value),
      };
    }),
  };
};

const compareBanEiLinkPoints = (left: RaceTimeChartPoint, right: RaceTimeChartPoint): number => {
  if (left.sortKey !== right.sortKey) {
    return left.sortKey.localeCompare(right.sortKey, "ja");
  }
  return left.id.localeCompare(right.id, "ja");
};

const banEiAbilityRowSortKey = (row: BanEiAbilityRow): string =>
  `${row.result.kaisaiNen}${row.result.kaisaiTsukihi}${row.result.keibajoCode}${row.result.raceBango}`;

const collectBanEiAbilityRows = ({
  currentDistance,
  results,
}: CollectBanEiAbilityRowsParams): BanEiAbilityRow[] =>
  results.flatMap((result) => {
    const sohaTimeTenths = parseSohaTimeTenths(
      result.sohaTime,
      isBanEiKeibajoCode(result.keibajoCode),
    );
    const finishRank = parseRaceFinishRank(result.kakuteiChakujun);
    const carriedWeightKg = parseBanEiCarriedWeightKg(result.futanJuryo);
    if (sohaTimeTenths === null || finishRank === null || carriedWeightKg === null) {
      return [];
    }
    const resultDistance = parseRaceDistanceMeters(result.kyori);
    if (currentDistance !== null && resultDistance === null) {
      return [];
    }
    const scaledSohaTimeTenths =
      currentDistance === null || resultDistance === null
        ? sohaTimeTenths
        : scaleSohaTimeToDistance(sohaTimeTenths, resultDistance, currentDistance);
    return [{ carriedWeightKg, finishRank, result, scaledSohaTimeTenths, sohaTimeTenths }];
  });

const latestSortKeyByUmaban = (rows: BanEiAbilityRow[]): Map<string, string> =>
  rows.reduce((index, row) => {
    const umaban = formatRunnerNumber(row.result.currentUmaban);
    const key = banEiAbilityRowSortKey(row);
    const current = index.get(umaban);
    if (current !== undefined && current >= key) {
      return index;
    }
    return new Map(index).set(umaban, key);
  }, new Map<string, string>());

const toBanEiAbilityPoint = ({
  currentDistance,
  isLatest,
  layout,
  row,
  xValue,
}: ToBanEiAbilityPointParams): RaceTimeChartPoint => {
  const umaban = formatRunnerNumber(row.result.currentUmaban);
  const distanceMeters = parseRaceDistanceMeters(row.result.kyori);
  const distanceDeltaMeters =
    currentDistance === null || distanceMeters === null ? null : distanceMeters - currentDistance;
  return {
    carriedWeightKg: row.carriedWeightKg,
    dateLabel:
      row.result.kaisaiNen.length >= 4 && row.result.kaisaiTsukihi.length >= 4
        ? formatDate(row.result.kaisaiNen, row.result.kaisaiTsukihi)
        : "-",
    distanceDeltaMeters,
    distanceLabel: distanceMeters === null ? "-" : `${distanceMeters}m`,
    distanceWeight: raceTimeDistanceWeight(distanceMeters, currentDistance),
    finishRank: row.finishRank,
    horseName: cleanText(row.result.bamei),
    id: `${umaban}-${row.result.kaisaiNen}${row.result.kaisaiTsukihi}-${row.result.keibajoCode}-${row.result.raceBango}`,
    isLatest,
    keibajoLabel: formatKeibajo(row.result.keibajoCode),
    kohan3fTenths: null,
    pastJockeyLabel: cleanText(row.result.kishumeiRyakusho),
    radius: banEiFinishMarkRadius(row.finishRank),
    scaledSohaTimeTenths: row.scaledSohaTimeTenths,
    scheduledJockeyLabel: cleanText(row.result.currentJockey),
    sortKey: banEiAbilityRowSortKey(row),
    sohaTimeTenths: row.sohaTimeTenths,
    stroke: raceTimeFinishStroke(row.finishRank),
    umaban,
    x: layout.xScale(xValue),
    y: layout.yScale(row.scaledSohaTimeTenths),
  };
};

const buildBanEiHorseLinks = (points: RaceTimeChartPoint[]): RaceTimeHorseLink[] => {
  const grouped = points.reduce((index, point) => {
    const current = index.get(point.umaban) ?? [];
    return new Map(index).set(point.umaban, [...current, point]);
  }, new Map<string, RaceTimeChartPoint[]>());
  return Array.from(grouped.entries()).flatMap(([umaban, horsePoints]) => {
    const ordered = horsePoints.toSorted(compareBanEiLinkPoints);
    const first = ordered[0];
    if (first === undefined || ordered.length < 2) {
      return [];
    }
    const path = ordered
      .slice(1)
      .reduce((current, point) => `${current} L ${point.x} ${point.y}`, `M ${first.x} ${first.y}`);
    return [{ path, umaban }];
  });
};

export const buildDrawnBanEiAbilityChart = ({
  currentDistance,
  results,
  stats,
}: BuildDrawnRaceTimeChartParams): DrawnRaceTimeChart | null => {
  const currentDistanceMeters = parseRaceDistanceMeters(currentDistance);
  const rows = collectBanEiAbilityRows({
    currentDistance: currentDistanceMeters,
    results,
  });
  if (rows.length === 0) {
    return null;
  }
  const specs = referenceSpecs(stats, false);
  const yValues = [
    ...rows.map((row) => row.scaledSohaTimeTenths),
    ...specs.flatMap((spec) =>
      spec.orientation === "horizontal" && spec.value !== null ? [spec.value] : [],
    ),
  ];
  const xDomain = collectDomain(rows.map((row) => row.carriedWeightKg));
  const yDomain = collectDomain(yValues);
  const latestKeys = latestSortKeyByUmaban(rows);
  const layout = buildLayout({
    invertX: false,
    plotBottom: PLOT_BOTTOM,
    plotTop: PLOT_TOP,
    xDomain,
    yDomain,
  });
  const points = rows
    .map((row) =>
      toBanEiAbilityPoint({
        currentDistance: currentDistanceMeters,
        isLatest:
          latestKeys.get(formatRunnerNumber(row.result.currentUmaban)) ===
          banEiAbilityRowSortKey(row),
        layout,
        row,
        xValue: row.carriedWeightKg,
      }),
    )
    .toSorted(compareRaceTimeChartPoints);
  return {
    height: RACE_TIME_CHART_VIEW_HEIGHT,
    horseLinks: buildBanEiHorseLinks(points),
    plotBottom: PLOT_BOTTOM,
    plotLeft: PLOT_LEFT,
    plotRight: PLOT_RIGHT,
    plotTop: PLOT_TOP,
    points,
    references: specs.flatMap((spec) => {
      const line = toReferenceLine({
        layout,
        plotBottom: PLOT_BOTTOM,
        plotTop: PLOT_TOP,
        spec,
      });
      return line === null ? [] : [line];
    }),
    width: RACE_TIME_CHART_VIEW_WIDTH,
    xAxisTitle: RACE_TIME_CHART_BAN_EI_WEIGHT_X_AXIS_TITLE,
    xTicks: TICK_FRACTIONS.map((fraction) => {
      const value = tickValue(xDomain, fraction, false);
      return {
        label: formatCarriedWeightKgLabel(value),
        x: layout.xScale(value),
        y: PLOT_BOTTOM,
      };
    }),
    yAxisTitle: RACE_TIME_CHART_Y_AXIS_TITLE,
    yTicks: TICK_FRACTIONS.map((fraction) => {
      const value = tickValue(yDomain, fraction, true);
      return {
        label: formatRaceTimeTenthsLabel(value),
        x: PLOT_LEFT,
        y: layout.yScale(value),
      };
    }),
  };
};
