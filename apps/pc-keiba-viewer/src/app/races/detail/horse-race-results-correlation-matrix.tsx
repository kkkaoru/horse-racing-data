"use client";

// Run with bun (rendered by the race-detail 競走成績グラフ section).
// Race-aligned correlation matrix: one chronological column per race, the
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

// 着順 - 人気. A negative gap means the horse finished ahead of where the market
// ranked it (over-performance); a positive gap means it ran below expectation.
type MarketGapTone = "over" | "under" | "even";

interface MarketGapAppearance {
  className: string;
  label: string;
  tone: MarketGapTone;
}

interface RankCellProps {
  isUpcoming: boolean;
  rank: number | null;
}

interface MarketGapCellProps {
  finish: number | null;
  isUpcoming: boolean;
  popularity: number | null;
}

interface MatrixValueRowProps {
  cellClassName: string;
  keyPrefix: string;
  label: string;
  rows: HorseRaceCorrelationRow[];
  upcomingRaceDate: string | null;
  value: (row: HorseRaceCorrelationRow) => string;
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
const UPCOMING_CELL_CLASS = "race-results-correlation-upcoming";
const MARKET_GAP_OVER_CLASS = "race-results-correlation-gap-over";
const MARKET_GAP_UNDER_CLASS = "race-results-correlation-gap-under";
const MARKET_GAP_EVEN_CLASS = "race-results-correlation-gap-even";
const MARKET_GAP_EVEN_LABEL = "±0";
const EVEN_MARKET_GAP_APPEARANCE: MarketGapAppearance = {
  className: MARKET_GAP_EVEN_CLASS,
  label: MARKET_GAP_EVEN_LABEL,
  tone: "even",
};
const NULL_MARKET_GAP_APPEARANCE: MarketGapAppearance = {
  className: MARKET_GAP_EVEN_CLASS,
  label: EMPTY_VALUE_LABEL,
  tone: "even",
};

const findRankBucketAppearance = (rank: number | null): RankBucketAppearance => {
  if (rank === null) {
    return NULL_RANK_BUCKET;
  }
  return RANK_BUCKETS.find((bucket) => rank <= bucket.maxRank) ?? OVERFLOW_RANK_BUCKET;
};

export const getRankBucketColor = (rank: number | null): string =>
  findRankBucketAppearance(rank).backgroundColor;

// Resolve the 着順 vs 人気 gap into a tone + signed label. A horse finishing
// better than its popularity rank (smaller finish number) over-performed, which
// the matrix highlights as the headline comparison.
export const getMarketGapAppearance = (
  finish: number | null,
  popularity: number | null,
): MarketGapAppearance => {
  if (finish === null || popularity === null) {
    return NULL_MARKET_GAP_APPEARANCE;
  }
  const gap = finish - popularity;
  if (gap === 0) {
    return EVEN_MARKET_GAP_APPEARANCE;
  }
  return gap < 0
    ? { className: MARKET_GAP_OVER_CLASS, label: String(gap), tone: "over" }
    : { className: MARKET_GAP_UNDER_CLASS, label: `+${gap}`, tone: "under" };
};

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

// The synthetic upcoming/target race is the newest row whose finish AND
// popularity are still null while its weight is known (mirrors how the data
// layer appends it). Its raceDate keys the highlighted column.
const findUpcomingRaceDate = (rows: HorseRaceCorrelationRow[]): string | null => {
  const upcoming = rows.findLast(
    (row) => row.finish === null && row.popularity === null && row.weight !== null,
  );
  return upcoming === undefined ? null : upcoming.raceDate;
};

const toCellClassName = (baseClassName: string, isUpcoming: boolean): string =>
  isUpcoming ? `${baseClassName} ${UPCOMING_CELL_CLASS}` : baseClassName;

const RankCell = ({ isUpcoming, rank }: RankCellProps) => (
  <span className={toCellClassName("race-results-correlation-cell", isUpcoming)}>
    <span className="race-results-correlation-rank-badge" style={getRankBadgeStyle(rank)}>
      {formatMatrixNumber(rank)}
    </span>
  </span>
);

const MarketGapCell = ({ finish, isUpcoming, popularity }: MarketGapCellProps) => {
  const appearance = getMarketGapAppearance(finish, popularity);
  return (
    <span className={toCellClassName("race-results-correlation-cell", isUpcoming)}>
      <span
        className={`race-results-correlation-gap-badge ${appearance.className}`}
        data-tone={appearance.tone}
      >
        {appearance.label}
      </span>
    </span>
  );
};

const MatrixValueRow = ({
  cellClassName,
  keyPrefix,
  label,
  rows,
  upcomingRaceDate,
  value,
}: MatrixValueRowProps) => (
  <>
    <span className="race-results-correlation-row-label">{label}</span>
    {rows.map((row) => (
      <span
        className={toCellClassName(cellClassName, row.raceDate === upcomingRaceDate)}
        key={`${keyPrefix}-${row.raceDate}`}
      >
        {value(row)}
      </span>
    ))}
  </>
);

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
  const upcomingRaceDate = findUpcomingRaceDate(rows);
  return (
    <div className="race-results-correlation-matrix">
      <div
        className="race-results-correlation-matrix-grid"
        style={buildMatrixGridStyle(rows.length)}
      >
        <MatrixValueRow
          cellClassName="race-results-correlation-cell race-results-correlation-date"
          keyPrefix="date"
          label="日付"
          rows={rows}
          upcomingRaceDate={upcomingRaceDate}
          value={(row) => formatMatrixDate(row.raceDate)}
        />
        <span className="race-results-correlation-row-label">着順</span>
        {rows.map((row) => (
          <RankCell
            isUpcoming={row.raceDate === upcomingRaceDate}
            key={`finish-${row.raceDate}`}
            rank={row.finish}
          />
        ))}
        <span className="race-results-correlation-row-label">人気</span>
        {rows.map((row) => (
          <RankCell
            isUpcoming={row.raceDate === upcomingRaceDate}
            key={`popularity-${row.raceDate}`}
            rank={row.popularity}
          />
        ))}
        <span className="race-results-correlation-row-label">差</span>
        {rows.map((row) => (
          <MarketGapCell
            finish={row.finish}
            isUpcoming={row.raceDate === upcomingRaceDate}
            key={`gap-${row.raceDate}`}
            popularity={row.popularity}
          />
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
            className={toCellClassName(
              "race-results-correlation-cell race-results-correlation-weight-label",
              row.raceDate === upcomingRaceDate,
            )}
            key={`weight-${row.raceDate}`}
          >
            {formatMatrixNumber(row.weight)}
          </span>
        ))}
        <span className="race-results-correlation-row-label">増減</span>
        {rows.map((row) => (
          <span
            className={toCellClassName(
              "race-results-correlation-cell",
              row.raceDate === upcomingRaceDate,
            )}
            key={`delta-${row.raceDate}`}
          >
            <DeltaBarCell delta={row.weightDelta} maxAbsDelta={maxAbsDelta} />
          </span>
        ))}
        <MatrixValueRow
          cellClassName="race-results-correlation-cell race-results-correlation-futan-label"
          keyPrefix="futan"
          label="斤量"
          rows={rows}
          upcomingRaceDate={upcomingRaceDate}
          value={(row) => formatMatrixNumber(row.futan)}
        />
      </div>
    </div>
  );
};
