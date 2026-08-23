"use client";

// This file runs with bun.

import type { PointerEvent } from "react";
import { useRef, useState } from "react";

import {
  BAN_EI_ABILITY_HORSE_LINK_STROKE,
  buildDrawnBanEiAbilityChart,
  buildDrawnRaceTimeChart,
  formatRaceTimeChartTooltip,
  raceTimeChartEmptyMessage,
  raceTimeChartNote,
  RACE_TIME_REFERENCE_STROKE_OPACITY,
  RACE_TIME_REFERENCE_STROKE_WIDTH,
  raceTimeFinishStroke,
  type DrawnRaceTimeChart,
  type RaceTimeChartPoint,
} from "../../../lib/horse-race-time-charts";
import type { HorseRaceResult, RaceTimeStats } from "../../../lib/race-types";
import { isBanEiKeibajoCode } from "../../../lib/runner-format";
import {
  trainingChartFrameOrigin,
  trainingChartTooltipPosition,
} from "../../../lib/training-charts";

interface HorseRaceTimeChartProps {
  currentDistance: string | null | undefined;
  keibajoCode?: string | null;
  results: HorseRaceResult[];
  stats: RaceTimeStats | null;
}

interface RaceTimeChartLegendItem {
  label: string;
  stroke: string;
}

interface RaceTimeChartTooltipView {
  lines: string[];
  x: number;
  y: number;
}

interface RaceTimeChartPlotViewProps {
  chart: DrawnRaceTimeChart;
  hoverId: string | null;
  hoverUmaban: string | null;
  keyPrefix: string;
  onLeavePoint: () => void;
  onMovePoint: (event: PointerEvent<SVGCircleElement>, point: RaceTimeChartPoint) => void;
}

const AXIS_STROKE: string = "#8a9a90";
const GRID_STROKE: string = "#d8e0da";
const GRID_STROKE_OPACITY: number = 0.35;
const GRID_STROKE_WIDTH: number = 0.6;
const TICK_INSET: number = 6;
const LABEL_GAP: number = 18;
const TITLE_GAP: number = 36;
const Y_AXIS_TITLE_X: number = 16;
const POINT_RADIUS: number = 4.4;
const FIRST_POINT_RADIUS: number = 5.4;
const OTHER_POINT_RADIUS: number = 3.2;
const ACTIVE_POINT_RADIUS: number = 6.2;
const BAN_EI_ACTIVE_RADIUS_BOOST: number = 1.4;
const DIMMED_POINT_FILL_OPACITY: number = 0.1;
const BASE_POINT_FILL_OPACITY: number = 0.2;
const DISTANCE_POINT_FILL_SPAN: number = 0.2;
const OPACITY_DECIMAL_PLACES: number = 100;
const POINT_STROKE_OPACITY: number = 0.42;
const DIMMED_POINT_STROKE_OPACITY: number = 0.1;
const ACTIVE_POINT_STROKE_OPACITY: number = 0.7;
const UMABAN_OPACITY: number = 0.92;
const DIMMED_UMABAN_OPACITY: number = 0.26;
const ACTIVE_UMABAN_OPACITY: number = 1;
const UMABAN_LABEL_DX: number = 9;
const UMABAN_LABEL_DY: number = 4;
const HORSE_LINK_OPACITY: number = 0.32;
const HORSE_LINK_ACTIVE_OPACITY: number = 0.55;
const HORSE_LINK_DIMMED_OPACITY: number = 0.08;
const HORSE_LINK_WIDTH: number = 1.05;
const HORSE_LINK_ACTIVE_WIDTH: number = 1.6;

const FINISH_LEGEND: RaceTimeChartLegendItem[] = [
  { label: "1着", stroke: raceTimeFinishStroke(1) },
  { label: "2着", stroke: raceTimeFinishStroke(2) },
  { label: "3着", stroke: raceTimeFinishStroke(3) },
  { label: "4-5着", stroke: raceTimeFinishStroke(4) },
  { label: "着外", stroke: raceTimeFinishStroke(6) },
];

const REFERENCE_LEGEND: RaceTimeChartLegendItem[] = [
  { label: "最速", stroke: "#be123c" },
  { label: "平均", stroke: "#166534" },
  { label: "中央値", stroke: "#4338ca" },
];

const raceTimePointRadius = (
  active: boolean,
  finishRank: number | null,
  encodedRadius: number | null,
): number => {
  if (encodedRadius !== null) {
    if (active) {
      return encodedRadius + BAN_EI_ACTIVE_RADIUS_BOOST;
    }
    return encodedRadius;
  }
  if (active) {
    return ACTIVE_POINT_RADIUS;
  }
  if (finishRank === 1) {
    return FIRST_POINT_RADIUS;
  }
  if (finishRank !== null && finishRank <= 3) {
    return POINT_RADIUS;
  }
  return OTHER_POINT_RADIUS;
};

const roundOpacity = (value: number): number =>
  Math.round(value * OPACITY_DECIMAL_PLACES) / OPACITY_DECIMAL_PLACES;

const raceTimePointFillOpacity = (dimmed: boolean, distanceWeight: number): number => {
  if (dimmed) {
    return DIMMED_POINT_FILL_OPACITY;
  }
  return roundOpacity(BASE_POINT_FILL_OPACITY + DISTANCE_POINT_FILL_SPAN * distanceWeight);
};

const raceTimePointStrokeOpacity = (active: boolean, dimmed: boolean): number => {
  if (active) {
    return ACTIVE_POINT_STROKE_OPACITY;
  }
  if (dimmed) {
    return DIMMED_POINT_STROKE_OPACITY;
  }
  return POINT_STROKE_OPACITY;
};

const raceTimeUmabanOpacity = (active: boolean, dimmed: boolean): number => {
  if (active) {
    return ACTIVE_UMABAN_OPACITY;
  }
  if (dimmed) {
    return DIMMED_UMABAN_OPACITY;
  }
  return UMABAN_OPACITY;
};

const horseLinkOpacity = (umaban: string, hoverUmaban: string | null): number => {
  if (hoverUmaban === null) {
    return HORSE_LINK_OPACITY;
  }
  if (hoverUmaban === umaban) {
    return HORSE_LINK_ACTIVE_OPACITY;
  }
  return HORSE_LINK_DIMMED_OPACITY;
};

const RaceTimeChartPlotContents = ({
  chart,
  hoverId,
  hoverUmaban,
  keyPrefix,
  onLeavePoint,
  onMovePoint,
}: RaceTimeChartPlotViewProps) => (
  <g>
    {chart.yTicks.map((tick) => (
      <g key={`${keyPrefix}y-${tick.label}-${tick.y}`}>
        <line
          stroke={GRID_STROKE}
          strokeOpacity={GRID_STROKE_OPACITY}
          strokeWidth={GRID_STROKE_WIDTH}
          x1={chart.plotLeft}
          x2={chart.plotRight}
          y1={tick.y}
          y2={tick.y}
        />
        <text
          className="training-chart-axis"
          textAnchor="end"
          x={chart.plotLeft - TICK_INSET}
          y={tick.y + 4}
        >
          {tick.label}
        </text>
      </g>
    ))}
    {chart.xTicks.map((tick) => (
      <g key={`${keyPrefix}x-${tick.label}-${tick.x}`}>
        <line
          stroke={GRID_STROKE}
          strokeOpacity={GRID_STROKE_OPACITY}
          strokeWidth={GRID_STROKE_WIDTH}
          x1={tick.x}
          x2={tick.x}
          y1={chart.plotTop}
          y2={chart.plotBottom}
        />
        <text
          className="training-chart-axis"
          textAnchor="middle"
          x={tick.x}
          y={chart.plotBottom + LABEL_GAP}
        >
          {tick.label}
        </text>
      </g>
    ))}
    {chart.references.map((line) => (
      <line
        data-reference={line.kind}
        key={`${keyPrefix}${line.kind}`}
        stroke={line.stroke}
        strokeDasharray={line.strokeDasharray}
        strokeOpacity={RACE_TIME_REFERENCE_STROKE_OPACITY}
        strokeWidth={RACE_TIME_REFERENCE_STROKE_WIDTH}
        x1={line.x1}
        x2={line.x2}
        y1={line.y1}
        y2={line.y2}
      />
    ))}
    <line
      stroke={AXIS_STROKE}
      x1={chart.plotLeft}
      x2={chart.plotLeft}
      y1={chart.plotTop}
      y2={chart.plotBottom}
    />
    <line
      stroke={AXIS_STROKE}
      x1={chart.plotLeft}
      x2={chart.plotRight}
      y1={chart.plotBottom}
      y2={chart.plotBottom}
    />
    <text
      className="training-chart-axis"
      textAnchor="middle"
      transform={`rotate(-90 ${Y_AXIS_TITLE_X} ${(chart.plotTop + chart.plotBottom) / 2})`}
      x={Y_AXIS_TITLE_X}
      y={(chart.plotTop + chart.plotBottom) / 2}
    >
      {chart.yAxisTitle}
    </text>
    <text
      className="training-chart-axis"
      textAnchor="middle"
      x={(chart.plotLeft + chart.plotRight) / 2}
      y={chart.plotBottom + TITLE_GAP}
    >
      {chart.xAxisTitle}
    </text>
    {chart.horseLinks.map((link) => (
      <path
        d={link.path}
        data-horse-link={link.umaban}
        fill="none"
        key={`${keyPrefix}link-${link.umaban}`}
        stroke={BAN_EI_ABILITY_HORSE_LINK_STROKE}
        strokeOpacity={horseLinkOpacity(link.umaban, hoverUmaban)}
        strokeWidth={hoverUmaban === link.umaban ? HORSE_LINK_ACTIVE_WIDTH : HORSE_LINK_WIDTH}
      />
    ))}
    {chart.points.map((point) => {
      const active = hoverId === point.id || hoverUmaban === point.umaban;
      const dimmed =
        (hoverId !== null && hoverId !== point.id) ||
        (hoverUmaban !== null && hoverUmaban !== point.umaban);
      const showLabel = point.isLatest || active;
      return (
        <g key={`${keyPrefix}${point.id}`}>
          <circle
            className="training-chart-point"
            cx={point.x}
            cy={point.y}
            data-finish={point.finishRank === null ? "" : String(point.finishRank)}
            data-horse={point.horseName}
            data-race-time-point={point.id}
            data-umaban={point.umaban}
            fill={point.stroke}
            fillOpacity={raceTimePointFillOpacity(dimmed, point.distanceWeight)}
            r={raceTimePointRadius(active, point.finishRank, point.radius)}
            stroke={point.stroke}
            strokeOpacity={raceTimePointStrokeOpacity(active, dimmed)}
            strokeWidth={active ? 2.2 : 1.4}
            onPointerEnter={(event) => {
              onMovePoint(event, point);
            }}
            onPointerLeave={onLeavePoint}
            onPointerMove={(event) => {
              onMovePoint(event, point);
            }}
          />
          {showLabel ? (
            <text
              className="training-chart-umaban"
              data-umaban-label={point.umaban}
              fill={point.stroke}
              fontSize={active ? 14 : 11}
              fontWeight={active ? 800 : 700}
              opacity={raceTimeUmabanOpacity(active, dimmed)}
              x={point.x + UMABAN_LABEL_DX}
              y={point.y + UMABAN_LABEL_DY}
            >
              {point.umaban}
            </text>
          ) : null}
        </g>
      );
    })}
  </g>
);

const RaceTimeChartPlotView = ({
  chart,
  hoverId,
  hoverUmaban,
  keyPrefix,
  onLeavePoint,
  onMovePoint,
}: RaceTimeChartPlotViewProps) => (
  <svg className="training-chart-svg" viewBox={`0 0 ${chart.width} ${chart.height}`}>
    <RaceTimeChartPlotContents
      chart={chart}
      hoverId={hoverId}
      hoverUmaban={hoverUmaban}
      keyPrefix={keyPrefix}
      onLeavePoint={onLeavePoint}
      onMovePoint={onMovePoint}
    />
  </svg>
);

export const HorseRaceTimeChart = ({
  currentDistance,
  keibajoCode,
  results,
  stats,
}: HorseRaceTimeChartProps) => {
  const isBanEi = isBanEiKeibajoCode(keibajoCode);
  const banEiDrawn = isBanEi
    ? buildDrawnBanEiAbilityChart({ currentDistance, keibajoCode, results, stats })
    : null;
  const timeDrawn = isBanEi
    ? null
    : buildDrawnRaceTimeChart({ currentDistance, keibajoCode, results, stats });
  const frameRef = useRef<HTMLDivElement | null>(null);
  const [hoverId, setHoverId] = useState<string | null>(null);
  const [hoverUmaban, setHoverUmaban] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<RaceTimeChartTooltipView | null>(null);
  if (banEiDrawn === null && timeDrawn === null) {
    return <p className="empty-state">{raceTimeChartEmptyMessage(isBanEi)}</p>;
  }
  const hideTooltip = () => {
    setHoverId(null);
    setHoverUmaban(null);
    setTooltip(null);
  };
  const moveTooltip = (event: PointerEvent<SVGCircleElement>, point: RaceTimeChartPoint) => {
    const origin = trainingChartFrameOrigin(frameRef.current);
    const location = trainingChartTooltipPosition({
      clientX: event.clientX,
      clientY: event.clientY,
      frameLeft: origin.left,
      frameTop: origin.top,
    });
    setHoverId(isBanEi ? null : point.id);
    setHoverUmaban(isBanEi ? point.umaban : null);
    setTooltip({
      lines: formatRaceTimeChartTooltip(point),
      x: location.x,
      y: location.y,
    });
  };
  return (
    <figure aria-label="競走成績タイム散布図" className="training-chart">
      <p className="training-chart-note">{raceTimeChartNote(isBanEi)}</p>
      <ul className="training-chart-legend">
        {FINISH_LEGEND.map((item) => (
          <li key={item.label}>
            <span className="training-chart-swatch" style={{ background: item.stroke }} />
            {item.label}
          </li>
        ))}
      </ul>
      <ul className="training-chart-legend">
        {REFERENCE_LEGEND.map((item) => (
          <li key={item.label}>
            <span className="training-chart-swatch" style={{ background: item.stroke }} />
            {item.label}
          </li>
        ))}
      </ul>
      <div className="training-chart-frame" ref={frameRef}>
        {banEiDrawn === null ? null : (
          <RaceTimeChartPlotView
            chart={banEiDrawn}
            hoverId={null}
            hoverUmaban={hoverUmaban}
            keyPrefix="banei-"
            onLeavePoint={hideTooltip}
            onMovePoint={moveTooltip}
          />
        )}
        {timeDrawn === null ? null : (
          <RaceTimeChartPlotView
            chart={timeDrawn}
            hoverId={hoverId}
            hoverUmaban={null}
            keyPrefix=""
            onLeavePoint={hideTooltip}
            onMovePoint={moveTooltip}
          />
        )}
        {tooltip === null ? null : (
          <div
            className="training-chart-tooltip"
            role="tooltip"
            style={{ left: `${tooltip.x}px`, top: `${tooltip.y}px` }}
          >
            {tooltip.lines.map((line) => (
              <span className="training-chart-tooltip-line" key={line}>
                {line}
              </span>
            ))}
          </div>
        )}
      </div>
    </figure>
  );
};
