// This file runs with bun.

import { scaleLinear } from "d3";

import { cleanText, formatDate } from "./format";
import type { Training } from "./race-types";
import { formatRunnerNumber } from "./runner-format";
import { formatTracen, formatTrainingTime, formatWoodCourse } from "./training-format";

export type TrainingFurlongKey =
  | "lapTime1f"
  | "timeGokei2f"
  | "timeGokei3f"
  | "timeGokei4f"
  | "timeGokei5f"
  | "timeGokei6f";

export type TrainingEvenFurlongs = 3 | 4;
export type TrainingSustainFurlongs = 2 | 3 | 4;

export interface TrainingFurlongColumn {
  key: TrainingFurlongKey;
  label: string;
}

export interface TrainingScatterRow {
  courseFacet: string;
  courseSummary: string;
  dateLabel: string;
  evaluationGrade: string;
  evaluationText: string;
  evenPace1FSeconds: number;
  evenPaceFurlongs: TrainingEvenFurlongs;
  fiveFSeconds: number | null;
  fourFSeconds: number | null;
  horseName: string;
  id: string;
  kireruRatio: number;
  oneFSeconds: number;
  recencyKey: string;
  sixFSeconds: number | null;
  split21: number | null;
  split32: number | null;
  stroke: string;
  sustainFurlongs: TrainingSustainFurlongs;
  sustainRatio: number;
  threeFSeconds: number | null;
  twoFSeconds: number | null;
  umaban: string;
}

export interface TrainingScatterPoint extends TrainingScatterRow {
  isLatest: boolean;
  x: number;
  y: number;
}

export interface TrainingHorseSeries {
  path: string;
  umaban: string;
}

export interface TrainingTrendPoint extends TrainingScatterRow {
  isLatest: boolean;
  x: number;
  y: number;
}

export interface TrainingTrendLane {
  evenY: number;
  horseName: string;
  labelY: number;
  path: string;
  plotBottom: number;
  plotTop: number;
  points: TrainingTrendPoint[];
  umaban: string;
}

export interface DrawnTrainingTrendChart {
  height: number;
  lanes: TrainingTrendLane[];
  plotLeft: number;
  plotRight: number;
  width: number;
  xAxisTitle: string;
  xTicks: DrawnTrainingAxisTick[];
  yAxisTitle: string;
}

export interface DrawnTrainingAxisTick {
  label: string;
  x: number;
  y: number;
}

export interface DrawnTrainingChart {
  evenLabel: string;
  evenLabelX: number;
  evenLabelY: number;
  evenX: number;
  evenY: number;
  height: number;
  plotBottom: number;
  plotLeft: number;
  plotRight: number;
  plotTop: number;
  points: TrainingScatterPoint[];
  series: TrainingHorseSeries[];
  width: number;
  xAxisTitle: string;
  xTicks: DrawnTrainingAxisTick[];
  yAxisTitle: string;
  yTicks: DrawnTrainingAxisTick[];
}

export interface TrainingChartTooltipPosition {
  x: number;
  y: number;
}

export interface TrainingChartFrameOrigin {
  left: number;
  top: number;
}

interface TrainingChartTooltipPositionParams {
  clientX: number;
  clientY: number;
  frameLeft: number;
  frameTop: number;
}

interface TrainingChartFrameBox {
  getBoundingClientRect: () => TrainingChartFrameOrigin;
}

interface TrainingRatioDomain {
  maxRatio: number;
  minRatio: number;
}

interface TrainingScatterLayout {
  xScale: (ratio: number) => number;
  yScale: (ratio: number) => number;
}

interface TrainingEvenPace {
  furlongs: TrainingEvenFurlongs;
  seconds: number;
}

interface TrainingSustainPace {
  furlongs: TrainingSustainFurlongs;
  seconds: number;
}

interface ToChartPointParams {
  isLatest: boolean;
  layout: TrainingScatterLayout;
  row: TrainingScatterRow;
}

interface TrainingPathPoint {
  id: string;
  recencyKey: string;
  x: number;
  y: number;
}

interface BuildDrawnTrainingChartParams {
  trainings: Training[];
}

interface TrainingParsedTimes {
  fiveFSeconds: number | null;
  fourFSeconds: number | null;
  oneFSeconds: number | null;
  sixFSeconds: number | null;
  threeFSeconds: number | null;
  twoFSeconds: number | null;
}

export const DEFAULT_SHOW_TRAINING_CHART: boolean = true;
export const DEFAULT_SHOW_ALL_TRAINING_WORKOUTS: boolean = false;

export const TRAINING_CHART_NOTE: string =
  "点線の十字は均等ペースです。上ほど最終1Fが自分の均等より速く、右ほど最終2Fが自分の均等より速い。2Fがない調教は3Fで見ます。コースが違う時計は直接比べません。点の色は調教記号、数字は馬番。コースはホバーで確認。";

export const TRAINING_TREND_CHART_NOTE: string =
  "馬ごとの行は最終1Fの時系列です。右が新しい調教。各行の上へ向かうほど、その調教の均等ペースより速い。線は位置、丸の色は調教記号。ホバーで時計と最終2F。";

export const TRAINING_CHART_VIEW_WIDTH: number = 720;
export const TRAINING_CHART_VIEW_HEIGHT: number = 380;
export const TRAINING_CHART_PLOT_LEFT: number = 72;
export const TRAINING_CHART_PLOT_RIGHT: number = 704;
export const TRAINING_CHART_PLOT_TOP: number = 20;
export const TRAINING_CHART_PLOT_BOTTOM: number = 332;
export const TRAINING_CHART_TOOLTIP_OFFSET: number = 12;
export const TRAINING_CHART_X_AXIS_TITLE: string = "最終2F（右が均等より速い）";
export const TRAINING_CHART_Y_AXIS_TITLE: string = "最終1F（上が均等より速い）";
export const TRAINING_CHART_EVEN_PACE_LABEL: string = "均等ペース";
export const TRAINING_CHART_EVEN_PACE_RATIO: number = 1;
export const TRAINING_CHART_EVEN_LABEL_OFFSET_X: number = 8;
export const TRAINING_CHART_EVEN_LABEL_OFFSET_Y: number = 8;
export const TRAINING_TREND_X_AXIS_TITLE: string = "調教日（右が新しい）";
export const TRAINING_TREND_Y_AXIS_TITLE: string = "最終1F（各行の上が速い）";
export const TRAINING_TREND_LINE_STROKE: string = "#d5ddd8";
export const TRAINING_TREND_ROW_HEIGHT: number = 34;
export const TRAINING_TREND_ROW_PAD: number = 5;
export const TRAINING_TREND_TOP: number = 18;
export const TRAINING_TREND_AXIS_HEIGHT: number = 28;
export const TRAINING_TREND_LABEL_X: number = 18;
export const TRAINING_TREND_PLOT_LEFT: number = 40;
export const TRAINING_TREND_PLOT_RIGHT: number = 704;
export const TRAINING_TREND_DAY_MS: number = 86_400_000;

export const TRAINING_FURLONG_COLUMNS: TrainingFurlongColumn[] = [
  { key: "timeGokei6f", label: "6F" },
  { key: "timeGokei5f", label: "5F" },
  { key: "timeGokei4f", label: "4F" },
  { key: "timeGokei3f", label: "3F" },
  { key: "timeGokei2f", label: "2F" },
  { key: "lapTime1f", label: "1F" },
];

const PLOT_LEFT: number = TRAINING_CHART_PLOT_LEFT;
const PLOT_RIGHT: number = TRAINING_CHART_PLOT_RIGHT;
const PLOT_TOP: number = TRAINING_CHART_PLOT_TOP;
const PLOT_BOTTOM: number = TRAINING_CHART_PLOT_BOTTOM;
const RATIO_PAD_ABS: number = 0.05;
const RATIO_PAD_RATIO: number = 0.08;
const RATIO_TICK_FRACTIONS: number[] = [0, 0.25, 0.5, 0.75, 1];
const GRADE_STROKE_FALLBACK: string = "#64748b";
const UNKNOWN_COURSE_FACET: string = "コース不明";
const GRADE_STROKE = new Map<string, string>([
  ["◎", "#b45309"],
  ["SS", "#b45309"],
  ["S", "#b45309"],
  ["1", "#b45309"],
  ["○", "#166534"],
  ["◯", "#166534"],
  ["A", "#166534"],
  ["2", "#166534"],
  ["▲", "#c2410c"],
  ["B", "#c2410c"],
  ["3", "#c2410c"],
  ["△", "#355f9f"],
  ["C", "#355f9f"],
  ["4", "#355f9f"],
]);
export const parseTrainingSeconds = (value: string | null | undefined): number | null => {
  const formatted = formatTrainingTime(value);
  if (formatted === "-") {
    return null;
  }
  return Number(formatted);
};

export const formatTrainingPlaceSummary = (training: Training): string => {
  const values = [
    formatTracen(training.tracenKubun),
    cleanText(training.trainingType, "-"),
    formatWoodCourse(training.course, training.babamawari),
  ].filter((value) => value !== "" && value !== "-");
  return values.length > 0 ? values.join(" / ") : "-";
};

export const trainingGradeStroke = (grade: string): string => {
  const key = grade.trim().toUpperCase();
  if (key.length === 0) {
    return GRADE_STROKE_FALLBACK;
  }
  return GRADE_STROKE.get(key) ?? GRADE_STROKE_FALLBACK;
};

export const deriveTrainingSplitSeconds = (
  longerSeconds: number | null,
  shorterSeconds: number | null,
): number | null => {
  if (longerSeconds === null || shorterSeconds === null) {
    return null;
  }
  const split = longerSeconds - shorterSeconds;
  if (split <= 0) {
    return null;
  }
  return split;
};

export const trainingChartTooltipPosition = (
  params: TrainingChartTooltipPositionParams,
): TrainingChartTooltipPosition => ({
  x: params.clientX - params.frameLeft + TRAINING_CHART_TOOLTIP_OFFSET,
  y: params.clientY - params.frameTop + TRAINING_CHART_TOOLTIP_OFFSET,
});

export const trainingChartFrameOrigin = (
  frame: TrainingChartFrameBox | null,
): TrainingChartFrameOrigin => {
  if (frame === null) {
    return { left: 0, top: 0 };
  }
  return frame.getBoundingClientRect();
};

const formatTrainingDateLabel = (training: Training): string => {
  if (training.chokyoNengappi.length < 8) {
    return "-";
  }
  return formatDate(training.chokyoNengappi.slice(0, 4), training.chokyoNengappi.slice(4));
};

const trainingSeriesId = (training: Training): string =>
  `${cleanText(training.umaban, "")}-${training.trainingType}-${training.chokyoNengappi}-${training.chokyoJikoku}-${training.premiumWorkoutIndex === undefined ? "jra" : String(training.premiumWorkoutIndex)}`;

const trainingCourseFacet = (training: Training): string => {
  const typeLabel = cleanText(training.trainingType, "");
  if (typeLabel.length === 0 || typeLabel === "-") {
    return UNKNOWN_COURSE_FACET;
  }
  return typeLabel;
};

const parseTrainingTimes = (training: Training): TrainingParsedTimes => ({
  fiveFSeconds: parseTrainingSeconds(training.timeGokei5f),
  fourFSeconds: parseTrainingSeconds(training.timeGokei4f),
  oneFSeconds: parseTrainingSeconds(training.lapTime1f),
  sixFSeconds: parseTrainingSeconds(training.timeGokei6f),
  threeFSeconds: parseTrainingSeconds(training.timeGokei3f),
  twoFSeconds: parseTrainingSeconds(training.timeGokei2f),
});

const resolveEvenPace = (times: TrainingParsedTimes): TrainingEvenPace | null => {
  if (times.fourFSeconds !== null) {
    return { furlongs: 4, seconds: times.fourFSeconds };
  }
  if (times.threeFSeconds !== null) {
    return { furlongs: 3, seconds: times.threeFSeconds };
  }
  return null;
};

const resolveSustainPace = (
  times: TrainingParsedTimes,
  even: TrainingEvenPace,
): TrainingSustainPace => {
  if (times.twoFSeconds !== null) {
    return { furlongs: 2, seconds: times.twoFSeconds };
  }
  if (times.threeFSeconds !== null) {
    return { furlongs: 3, seconds: times.threeFSeconds };
  }
  return { furlongs: even.furlongs, seconds: even.seconds };
};

const formatChartSeconds = (value: number | null): string =>
  value === null ? "-" : value.toFixed(1);

const formatChartRatio = (value: number): string => value.toFixed(2);

export const formatTrainingPaceVsEven = (ratio: number): string => {
  if (ratio < TRAINING_CHART_EVEN_PACE_RATIO) {
    return "均等より速い";
  }
  if (ratio > TRAINING_CHART_EVEN_PACE_RATIO) {
    return "均等より遅い";
  }
  return "均等";
};

const paddedRatioDomain = (domain: TrainingRatioDomain): TrainingRatioDomain => {
  const span = domain.maxRatio - domain.minRatio;
  const pad = span === 0 ? RATIO_PAD_ABS : span * RATIO_PAD_RATIO;
  return {
    maxRatio: domain.maxRatio + pad,
    minRatio: domain.minRatio - pad,
  };
};

const collectRatioDomain = (values: number[]): TrainingRatioDomain => {
  const withEven = [TRAINING_CHART_EVEN_PACE_RATIO, ...values];
  return paddedRatioDomain({
    maxRatio: Math.max(...withEven),
    minRatio: Math.min(...withEven),
  });
};

const buildLayout = (
  xDomain: TrainingRatioDomain,
  yDomain: TrainingRatioDomain,
): TrainingScatterLayout => {
  const xScale = scaleLinear()
    .domain([xDomain.maxRatio, xDomain.minRatio])
    .range([PLOT_LEFT, PLOT_RIGHT]);
  const yScale = scaleLinear()
    .domain([yDomain.maxRatio, yDomain.minRatio])
    .range([PLOT_BOTTOM, PLOT_TOP]);
  return {
    xScale: (ratio) => xScale(ratio),
    yScale: (ratio) => yScale(ratio),
  };
};

const tickValue = (domain: TrainingRatioDomain, fraction: number): number =>
  domain.maxRatio + (domain.minRatio - domain.maxRatio) * fraction;

const buildXTicks = (
  domain: TrainingRatioDomain,
  layout: TrainingScatterLayout,
): DrawnTrainingAxisTick[] =>
  RATIO_TICK_FRACTIONS.map((fraction) => {
    const value = tickValue(domain, fraction);
    return {
      label: formatChartRatio(value),
      x: layout.xScale(value),
      y: PLOT_BOTTOM,
    };
  });

const buildYTicks = (
  domain: TrainingRatioDomain,
  layout: TrainingScatterLayout,
): DrawnTrainingAxisTick[] =>
  RATIO_TICK_FRACTIONS.map((fraction) => {
    const value = tickValue(domain, fraction);
    return {
      label: formatChartRatio(value),
      x: PLOT_LEFT,
      y: layout.yScale(value),
    };
  });

const compareTrainingScatterRows = (
  left: TrainingScatterRow,
  right: TrainingScatterRow,
): number => {
  if (left.umaban !== right.umaban) {
    return left.umaban.localeCompare(right.umaban, "ja");
  }
  if (left.recencyKey !== right.recencyKey) {
    return left.recencyKey < right.recencyKey ? -1 : 1;
  }
  return left.id.localeCompare(right.id, "ja");
};

const compareTrainingPathPoints = (left: TrainingPathPoint, right: TrainingPathPoint): number => {
  if (left.recencyKey !== right.recencyKey) {
    return left.recencyKey < right.recencyKey ? -1 : 1;
  }
  return left.id.localeCompare(right.id, "ja");
};

const toScatterRow = (training: Training): TrainingScatterRow | null => {
  const times = parseTrainingTimes(training);
  if (times.oneFSeconds === null) {
    return null;
  }
  const even = resolveEvenPace(times);
  if (even === null) {
    return null;
  }
  const evenPace1FSeconds = even.seconds / even.furlongs;
  const sustain = resolveSustainPace(times, even);
  const evaluationGrade = cleanText(training.premiumEvaluationGrade, "");
  return {
    courseFacet: trainingCourseFacet(training),
    courseSummary: formatTrainingPlaceSummary(training),
    dateLabel: formatTrainingDateLabel(training),
    evaluationGrade: evaluationGrade.length > 0 ? evaluationGrade : "-",
    evaluationText: cleanText(training.premiumEvaluationText, "-"),
    evenPace1FSeconds,
    evenPaceFurlongs: even.furlongs,
    fiveFSeconds: times.fiveFSeconds,
    fourFSeconds: times.fourFSeconds,
    horseName: cleanText(training.bamei),
    id: trainingSeriesId(training),
    kireruRatio: times.oneFSeconds / evenPace1FSeconds,
    oneFSeconds: times.oneFSeconds,
    recencyKey: `${training.chokyoNengappi}${training.chokyoJikoku}`,
    sixFSeconds: times.sixFSeconds,
    split21: deriveTrainingSplitSeconds(times.twoFSeconds, times.oneFSeconds),
    split32: deriveTrainingSplitSeconds(times.threeFSeconds, times.twoFSeconds),
    stroke: trainingGradeStroke(evaluationGrade),
    sustainFurlongs: sustain.furlongs,
    sustainRatio: sustain.seconds / (sustain.furlongs * evenPace1FSeconds),
    threeFSeconds: times.threeFSeconds,
    twoFSeconds: times.twoFSeconds,
    umaban: formatRunnerNumber(training.umaban),
  };
};

export const collectTrainingScatterRows = (trainings: Training[]): TrainingScatterRow[] =>
  [
    ...trainings
      .flatMap((training) => {
        const row = toScatterRow(training);
        return row === null ? [] : [row];
      })
      .reduce((unique, row) => {
        if (unique.has(row.id)) {
          return unique;
        }
        return unique.set(row.id, row);
      }, new Map<string, TrainingScatterRow>())
      .values(),
  ].toSorted(compareTrainingScatterRows);

const keepLatestTrainingRows = (rows: TrainingScatterRow[]): TrainingScatterRow[] =>
  [
    ...rows
      .reduce((latest, row) => {
        const current = latest.get(row.umaban);
        if (current === undefined) {
          return latest.set(row.umaban, row);
        }
        return compareTrainingScatterRows(current, row) < 0 ? latest.set(row.umaban, row) : latest;
      }, new Map<string, TrainingScatterRow>())
      .values(),
  ].toSorted(compareTrainingScatterRows);

const toChartPoint = ({ isLatest, layout, row }: ToChartPointParams): TrainingScatterPoint => ({
  ...row,
  isLatest,
  x: layout.xScale(row.sustainRatio),
  y: layout.yScale(row.kireruRatio),
});

const latestRecencyByHorse = (rows: readonly TrainingScatterRow[]): Map<string, string> =>
  rows.reduce((latest, row) => {
    const current = latest.get(row.umaban);
    if (current === undefined || row.recencyKey > current) {
      return latest.set(row.umaban, row.recencyKey);
    }
    return latest;
  }, new Map<string, string>());

const seriesPath = (points: readonly TrainingPathPoint[]): string => {
  const sorted = points.toSorted(compareTrainingPathPoints);
  const first = sorted[0];
  if (first === undefined) {
    return "";
  }
  return sorted
    .slice(1)
    .reduce((path, point) => `${path} L ${point.x} ${point.y}`, `M ${first.x} ${first.y}`);
};

const trainingRecencyMs = (key: string): number | null => {
  if (key.length < 8) {
    return null;
  }
  const year = Number(key.slice(0, 4));
  const month = Number(key.slice(4, 6));
  const day = Number(key.slice(6, 8));
  const hour = key.length >= 10 ? Number(key.slice(8, 10)) : 0;
  const minute = key.length >= 12 ? Number(key.slice(10, 12)) : 0;
  if (
    !Number.isFinite(year) ||
    !Number.isFinite(month) ||
    !Number.isFinite(day) ||
    !Number.isFinite(hour) ||
    !Number.isFinite(minute)
  ) {
    return null;
  }
  return Date.UTC(year, month - 1, day, hour, minute);
};

const trainingRecencyMsOrZero = (key: string): number => {
  const ms = trainingRecencyMs(key);
  return ms === null ? 0 : ms;
};

const formatTrendDateTick = (ms: number): string => {
  const date = new Date(ms);
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return formatDate(String(date.getUTCFullYear()), `${month}${day}`);
};

const collectHorseRows = (rows: TrainingScatterRow[]): TrainingScatterRow[][] =>
  [
    ...rows
      .reduce((byHorse, row) => {
        const existing = byHorse.get(row.umaban);
        if (existing === undefined) {
          return byHorse.set(row.umaban, [row]);
        }
        return byHorse.set(row.umaban, [...existing, row]);
      }, new Map<string, TrainingScatterRow[]>())
      .values(),
  ].map((horseRows) => horseRows.toSorted(compareTrainingScatterRows));

export const formatTrainingChartTooltip = (point: TrainingScatterRow): string[] => {
  const splitLines = [
    point.split32 === null ? [] : [`3-2 ${point.split32.toFixed(1)}`],
    point.split21 === null ? [] : [`2-1 ${point.split21.toFixed(1)}`],
  ].flat();
  return [
    `${point.umaban} ${point.horseName}`,
    point.dateLabel,
    point.courseSummary,
    `評価 ${point.evaluationGrade} ${point.evaluationText}`,
    `6F ${formatChartSeconds(point.sixFSeconds)}`,
    `5F ${formatChartSeconds(point.fiveFSeconds)}`,
    `4F ${formatChartSeconds(point.fourFSeconds)}`,
    `3F ${formatChartSeconds(point.threeFSeconds)}`,
    `2F ${formatChartSeconds(point.twoFSeconds)}`,
    `1F ${formatChartSeconds(point.oneFSeconds)}`,
    ...splitLines,
    `最終1F ${formatChartRatio(point.kireruRatio)}（${formatTrainingPaceVsEven(point.kireruRatio)}）`,
    `最終${point.sustainFurlongs}F ${formatChartRatio(point.sustainRatio)}（${formatTrainingPaceVsEven(point.sustainRatio)}）`,
  ];
};

export const buildDrawnTrainingChart = ({
  trainings,
}: BuildDrawnTrainingChartParams): DrawnTrainingChart | null => {
  const rows = keepLatestTrainingRows(collectTrainingScatterRows(trainings));
  if (rows.length === 0) {
    return null;
  }
  const xDomain = collectRatioDomain(rows.map((row) => row.sustainRatio));
  const yDomain = collectRatioDomain(rows.map((row) => row.kireruRatio));
  const layout = buildLayout(xDomain, yDomain);
  const latestByHorse = latestRecencyByHorse(rows);
  const points = rows.map((row) =>
    toChartPoint({
      isLatest: latestByHorse.get(row.umaban) === row.recencyKey,
      layout,
      row,
    }),
  );
  const evenX = layout.xScale(TRAINING_CHART_EVEN_PACE_RATIO);
  const evenY = layout.yScale(TRAINING_CHART_EVEN_PACE_RATIO);
  return {
    evenLabel: TRAINING_CHART_EVEN_PACE_LABEL,
    evenLabelX: evenX + TRAINING_CHART_EVEN_LABEL_OFFSET_X,
    evenLabelY: evenY - TRAINING_CHART_EVEN_LABEL_OFFSET_Y,
    evenX,
    evenY,
    height: TRAINING_CHART_VIEW_HEIGHT,
    plotBottom: PLOT_BOTTOM,
    plotLeft: PLOT_LEFT,
    plotRight: PLOT_RIGHT,
    plotTop: PLOT_TOP,
    points,
    series: [],
    width: TRAINING_CHART_VIEW_WIDTH,
    xAxisTitle: TRAINING_CHART_X_AXIS_TITLE,
    xTicks: buildXTicks(xDomain, layout),
    yAxisTitle: TRAINING_CHART_Y_AXIS_TITLE,
    yTicks: buildYTicks(yDomain, layout),
  };
};

export const buildDrawnTrainingTrendChart = (
  trainings: Training[],
): DrawnTrainingTrendChart | null => {
  const rows = collectTrainingScatterRows(trainings);
  if (rows.length === 0) {
    return null;
  }
  const horseRows = collectHorseRows(rows);
  const yDomain = collectRatioDomain(rows.map((row) => row.kireruRatio));
  const times = rows.map((row) => trainingRecencyMsOrZero(row.recencyKey));
  const minMs = Math.min(...times);
  const maxMs = Math.max(...times);
  const span = maxMs - minMs;
  const pad = span === 0 ? TRAINING_TREND_DAY_MS : span * RATIO_PAD_RATIO;
  const xMin = minMs - pad;
  const xMax = maxMs + pad;
  const xScale = scaleLinear()
    .domain([xMin, xMax])
    .range([TRAINING_TREND_PLOT_LEFT, TRAINING_TREND_PLOT_RIGHT]);
  const height =
    TRAINING_TREND_TOP + horseRows.length * TRAINING_TREND_ROW_HEIGHT + TRAINING_TREND_AXIS_HEIGHT;
  const latestByHorse = latestRecencyByHorse(rows);
  const lanes = horseRows.flatMap((laneRows, index) => {
    const first = laneRows[0];
    if (first === undefined) {
      return [];
    }
    const plotTop = TRAINING_TREND_TOP + index * TRAINING_TREND_ROW_HEIGHT + TRAINING_TREND_ROW_PAD;
    const plotBottom =
      TRAINING_TREND_TOP + (index + 1) * TRAINING_TREND_ROW_HEIGHT - TRAINING_TREND_ROW_PAD;
    const yScale = scaleLinear()
      .domain([yDomain.maxRatio, yDomain.minRatio])
      .range([plotBottom, plotTop]);
    const points = laneRows.map((row) => ({
      ...row,
      isLatest: latestByHorse.get(row.umaban) === row.recencyKey,
      x: xScale(trainingRecencyMsOrZero(row.recencyKey)),
      y: yScale(row.kireruRatio),
    }));
    return [
      {
        evenY: yScale(TRAINING_CHART_EVEN_PACE_RATIO),
        horseName: first.horseName,
        labelY: (plotTop + plotBottom) / 2,
        path: points.length < 2 ? "" : seriesPath(points),
        plotBottom,
        plotTop,
        points,
        umaban: first.umaban,
      },
    ];
  });
  return {
    height,
    lanes,
    plotLeft: TRAINING_TREND_PLOT_LEFT,
    plotRight: TRAINING_TREND_PLOT_RIGHT,
    width: TRAINING_CHART_VIEW_WIDTH,
    xAxisTitle: TRAINING_TREND_X_AXIS_TITLE,
    xTicks: RATIO_TICK_FRACTIONS.map((fraction) => {
      const ms = xMin + (xMax - xMin) * fraction;
      return {
        label: formatTrendDateTick(ms),
        x: xScale(ms),
        y: height - 8,
      };
    }),
    yAxisTitle: TRAINING_TREND_Y_AXIS_TITLE,
  };
};
