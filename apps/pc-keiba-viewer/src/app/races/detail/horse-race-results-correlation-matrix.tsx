"use client";

// Run with bun (rendered by the race-detail 競走成績グラフ section).
// Race-aligned correlation matrix: one chronological column per race, the four
// metrics vertically aligned so one vertical scan reads a race's full context.
import type { CSSProperties } from "react";

import type { HorseRaceCorrelationRow } from "../../../lib/horse-race-results-chart-data";

interface HorseRaceResultsCorrelationMatrixProps {
  rows: HorseRaceCorrelationRow[];
}

interface RankBucketAppearance {
  backgroundColor: string;
  color: string;
}

interface RankBucket extends RankBucketAppearance {
  maxRank: number;
}

interface WeightSparklinePoint {
  cx: number;
  cy: number;
}

interface WeightSparkline {
  points: WeightSparklinePoint[];
  segments: string[];
}

interface WeightSparklineInput {
  columnWidth: number;
  height: number;
  rows: HorseRaceCorrelationRow[];
}

interface WeightRunAccumulator {
  current: WeightSparklinePoint[];
  runs: WeightSparklinePoint[][];
}

type DeltaBarDirection = "up" | "down";

interface DeltaBarStyle {
  backgroundColor: string;
  direction: DeltaBarDirection;
  heightPercent: number;
}

interface DeltaBarCellProps {
  delta: number | null;
  maxAbsDelta: number;
}

const RANK_BADGE_TEXT_LIGHT = "#ffffff";
const RANK_BADGE_TEXT_DARK = "#1f2937";
// Shared bucket scale for 着順 and 人気: matching colors in one column mean the
// horse ran to market expectation; a mismatch flags an over/under-performance.
const RANK_BUCKETS: RankBucket[] = [
  { backgroundColor: "#d4a017", color: RANK_BADGE_TEXT_LIGHT, maxRank: 1 },
  { backgroundColor: "#8e9aa5", color: RANK_BADGE_TEXT_LIGHT, maxRank: 2 },
  { backgroundColor: "#a9712d", color: RANK_BADGE_TEXT_LIGHT, maxRank: 3 },
  { backgroundColor: "#5b8db8", color: RANK_BADGE_TEXT_LIGHT, maxRank: 5 },
  { backgroundColor: "#8aa0ae", color: RANK_BADGE_TEXT_LIGHT, maxRank: 9 },
];
const OVERFLOW_RANK_BUCKET: RankBucketAppearance = {
  backgroundColor: "#c9d2d8",
  color: RANK_BADGE_TEXT_DARK,
};
const NULL_RANK_BUCKET: RankBucketAppearance = {
  backgroundColor: "#e5e7eb",
  color: RANK_BADGE_TEXT_DARK,
};
const SPARKLINE_STROKE = "#0f766e";
const SPARKLINE_STROKE_WIDTH = 1.5;
const SPARKLINE_POINT_RADIUS = 2.5;
const SPARKLINE_VERTICAL_PADDING = 6;
const SPARKLINE_COLUMN_WIDTH = 44;
const SPARKLINE_HEIGHT = 48;
const DELTA_POSITIVE_COLOR = "#ea580c";
const DELTA_NEGATIVE_COLOR = "#2563eb";
const DELTA_ZERO_COLOR = "#9ca3af";
const ZERO_DELTA_STUB_HEIGHT_PERCENT = 8;
const FULL_PERCENT = 100;
const HALF_DIVISOR = 2;
// 56px fits the widest 13px "YY/MM/DD" date header, so adjacent column labels
// never overlap when every column is at its minimum width (matrix scrolls).
const MATRIX_COLUMN_MIN_WIDTH_PX = 56;
const MATRIX_DATE_YEAR_START = 2;
const MATRIX_DATE_MONTH_START = 4;
const MATRIX_DATE_DAY_START = 6;
const MATRIX_DATE_END = 8;
const EMPTY_VALUE_LABEL = "-";
const MATRIX_FULL_WIDTH_CELL_STYLE: CSSProperties = { gridColumn: "2 / -1" };

const findRankBucketAppearance = (rank: number | null): RankBucketAppearance => {
  if (rank === null) {
    return NULL_RANK_BUCKET;
  }
  return RANK_BUCKETS.find((bucket) => rank <= bucket.maxRank) ?? OVERFLOW_RANK_BUCKET;
};

export const getRankBucketColor = (rank: number | null): string =>
  findRankBucketAppearance(rank).backgroundColor;

export const buildWeightSparkline = ({
  columnWidth,
  height,
  rows,
}: WeightSparklineInput): WeightSparkline => {
  const weights = rows.flatMap((row) => (row.weight === null ? [] : [row.weight]));
  if (weights.length === 0) {
    return { points: [], segments: [] };
  }
  const minWeight = Math.min(...weights);
  const maxWeight = Math.max(...weights);
  const usableHeight = height - SPARKLINE_VERTICAL_PADDING * HALF_DIVISOR;
  const toCy = (weight: number): number =>
    maxWeight === minWeight
      ? height / HALF_DIVISOR
      : SPARKLINE_VERTICAL_PADDING +
        ((maxWeight - weight) / (maxWeight - minWeight)) * usableHeight;
  const toPoint = (weight: number, index: number): WeightSparklinePoint => ({
    cx: columnWidth * index + columnWidth / HALF_DIVISOR,
    cy: toCy(weight),
  });
  const accumulated = rows.reduce<WeightRunAccumulator>(
    (accumulator, row, index) => {
      if (row.weight === null) {
        return accumulator.current.length === 0
          ? accumulator
          : { current: [], runs: [...accumulator.runs, accumulator.current] };
      }
      return {
        current: [...accumulator.current, toPoint(row.weight, index)],
        runs: accumulator.runs,
      };
    },
    { current: [], runs: [] },
  );
  const runs =
    accumulated.current.length === 0
      ? accumulated.runs
      : [...accumulated.runs, accumulated.current];
  return {
    points: runs.flat(),
    segments: runs.map((run) => run.map((point) => `${point.cx},${point.cy}`).join(" ")),
  };
};

export const getDeltaBarStyle = (
  delta: number | null,
  maxAbsDelta: number,
): DeltaBarStyle | null => {
  if (delta === null) {
    return null;
  }
  if (delta === 0) {
    return {
      backgroundColor: DELTA_ZERO_COLOR,
      direction: "up",
      heightPercent: ZERO_DELTA_STUB_HEIGHT_PERCENT,
    };
  }
  const absDelta = Math.abs(delta);
  const heightPercent = Math.round((absDelta / Math.max(maxAbsDelta, absDelta)) * FULL_PERCENT);
  return delta > 0
    ? { backgroundColor: DELTA_POSITIVE_COLOR, direction: "up", heightPercent }
    : { backgroundColor: DELTA_NEGATIVE_COLOR, direction: "down", heightPercent };
};

export const formatDeltaLabel = (delta: number | null): string => {
  if (delta === null) {
    return EMPTY_VALUE_LABEL;
  }
  if (delta === 0) {
    return "±0";
  }
  return delta > 0 ? `+${delta}` : String(delta);
};

const getRankBadgeStyle = (rank: number | null): CSSProperties => {
  const appearance = findRankBucketAppearance(rank);
  return { backgroundColor: appearance.backgroundColor, color: appearance.color };
};

const formatMatrixNumber = (value: number | null): string =>
  value === null ? EMPTY_VALUE_LABEL : String(value);

const formatMatrixDate = (raceDate: string): string =>
  `${raceDate.slice(MATRIX_DATE_YEAR_START, MATRIX_DATE_MONTH_START)}/${raceDate.slice(
    MATRIX_DATE_MONTH_START,
    MATRIX_DATE_DAY_START,
  )}/${raceDate.slice(MATRIX_DATE_DAY_START, MATRIX_DATE_END)}`;

const getMaxAbsDelta = (rows: HorseRaceCorrelationRow[]): number =>
  rows.reduce(
    (max, row) => (row.weightDelta === null ? max : Math.max(max, Math.abs(row.weightDelta))),
    0,
  );

const buildMatrixGridStyle = (columnCount: number): CSSProperties => ({
  gridTemplateColumns: `max-content repeat(${columnCount}, minmax(${MATRIX_COLUMN_MIN_WIDTH_PX}px, 1fr))`,
});

const DeltaBarCell = ({ delta, maxAbsDelta }: DeltaBarCellProps) => {
  const barStyle = getDeltaBarStyle(delta, maxAbsDelta);
  return (
    <span className="race-results-correlation-delta-cell">
      <span className="race-results-correlation-delta-plot">
        <span className="race-results-correlation-delta-half upper">
          {barStyle !== null && barStyle.direction === "up" && (
            <span
              className="race-results-correlation-delta-bar"
              style={{
                backgroundColor: barStyle.backgroundColor,
                height: `${barStyle.heightPercent}%`,
              }}
            />
          )}
        </span>
        <span className="race-results-correlation-delta-half lower">
          {barStyle !== null && barStyle.direction === "down" && (
            <span
              className="race-results-correlation-delta-bar"
              style={{
                backgroundColor: barStyle.backgroundColor,
                height: `${barStyle.heightPercent}%`,
              }}
            />
          )}
        </span>
      </span>
      <span className="race-results-correlation-delta-label">{formatDeltaLabel(delta)}</span>
    </span>
  );
};

export const HorseRaceResultsCorrelationMatrix = ({
  rows,
}: HorseRaceResultsCorrelationMatrixProps) => {
  if (rows.length === 0) {
    return <p className="empty-state">選択した馬の表示できるデータがありません</p>;
  }
  const sparkline = buildWeightSparkline({
    columnWidth: SPARKLINE_COLUMN_WIDTH,
    height: SPARKLINE_HEIGHT,
    rows,
  });
  const maxAbsDelta = getMaxAbsDelta(rows);
  return (
    <div className="race-results-correlation-matrix">
      <div
        className="race-results-correlation-matrix-grid"
        style={buildMatrixGridStyle(rows.length)}
      >
        <span className="race-results-correlation-row-label">日付</span>
        {rows.map((row) => (
          <span
            className="race-results-correlation-cell race-results-correlation-date"
            key={`date-${row.raceDate}`}
          >
            {formatMatrixDate(row.raceDate)}
          </span>
        ))}
        <span className="race-results-correlation-row-label">着順</span>
        {rows.map((row) => (
          <span className="race-results-correlation-cell" key={`finish-${row.raceDate}`}>
            <span
              className="race-results-correlation-rank-badge"
              style={getRankBadgeStyle(row.finish)}
            >
              {formatMatrixNumber(row.finish)}
            </span>
          </span>
        ))}
        <span className="race-results-correlation-row-label">人気</span>
        {rows.map((row) => (
          <span className="race-results-correlation-cell" key={`popularity-${row.raceDate}`}>
            <span
              className="race-results-correlation-rank-badge"
              style={getRankBadgeStyle(row.popularity)}
            >
              {formatMatrixNumber(row.popularity)}
            </span>
          </span>
        ))}
        <span className="race-results-correlation-row-label">馬体重</span>
        <span
          className="race-results-correlation-sparkline-cell"
          style={MATRIX_FULL_WIDTH_CELL_STYLE}
        >
          <svg
            className="race-results-correlation-sparkline"
            height={SPARKLINE_HEIGHT}
            preserveAspectRatio="none"
            role="img"
            viewBox={`0 0 ${rows.length * SPARKLINE_COLUMN_WIDTH} ${SPARKLINE_HEIGHT}`}
            width="100%"
          >
            <title>馬体重の推移</title>
            {sparkline.segments.map((segment) => (
              <polyline
                fill="none"
                key={segment}
                points={segment}
                stroke={SPARKLINE_STROKE}
                strokeWidth={SPARKLINE_STROKE_WIDTH}
              />
            ))}
            {sparkline.points.map((point) => (
              <circle
                cx={point.cx}
                cy={point.cy}
                fill={SPARKLINE_STROKE}
                key={`${point.cx}-${point.cy}`}
                r={SPARKLINE_POINT_RADIUS}
              />
            ))}
          </svg>
        </span>
        <span aria-hidden="true" className="race-results-correlation-row-label" />
        {rows.map((row) => (
          <span
            className="race-results-correlation-cell race-results-correlation-weight-label"
            key={`weight-${row.raceDate}`}
          >
            {formatMatrixNumber(row.weight)}
          </span>
        ))}
        <span className="race-results-correlation-row-label">増減</span>
        {rows.map((row) => (
          <span className="race-results-correlation-cell" key={`delta-${row.raceDate}`}>
            <DeltaBarCell delta={row.weightDelta} maxAbsDelta={maxAbsDelta} />
          </span>
        ))}
      </div>
    </div>
  );
};
