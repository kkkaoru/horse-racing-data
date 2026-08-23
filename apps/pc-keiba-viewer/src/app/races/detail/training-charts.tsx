"use client";

// This file runs with bun.

import type { PointerEvent } from "react";
import { useRef, useState } from "react";

import type { Training } from "../../../lib/race-types";
import {
  buildDrawnTrainingChart,
  buildDrawnTrainingTrendChart,
  formatTrainingChartTooltip,
  TRAINING_CHART_NOTE,
  TRAINING_TREND_CHART_NOTE,
  TRAINING_TREND_LABEL_X,
  TRAINING_TREND_LINE_STROKE,
  trainingChartFrameOrigin,
  trainingChartTooltipPosition,
  type DrawnTrainingChart,
  type DrawnTrainingTrendChart,
  type TrainingScatterPoint,
  type TrainingTrendLane,
  type TrainingTrendPoint,
} from "../../../lib/training-charts";

interface TrainingTimesChartProps {
  showAllWorkouts: boolean;
  trainings: Training[];
  onShowAllWorkouts: (showAllWorkouts: boolean) => void;
}

interface TrainingScatterScopeToggleProps {
  showAllWorkouts: boolean;
  onShowAllWorkouts: (showAllWorkouts: boolean) => void;
}

interface TrainingChartLegendItem {
  label: string;
  stroke: string;
}

interface TrainingChartTooltipView {
  lines: string[];
  x: number;
  y: number;
}

interface TrainingChartPointMarkProps {
  active: boolean;
  dimmed: boolean;
  point: TrainingScatterPoint;
  showLabel: boolean;
  onLeave: () => void;
  onMove: (event: PointerEvent<SVGCircleElement>, point: TrainingScatterPoint) => void;
}

interface TrainingChartPlotViewProps {
  chart: DrawnTrainingChart;
  hoverUmaban: string | null;
  onLeavePoint: () => void;
  onMovePoint: (event: PointerEvent<SVGCircleElement>, point: TrainingScatterPoint) => void;
}

interface TrainingTrendPlotViewProps {
  chart: DrawnTrainingTrendChart;
  hoverUmaban: string | null;
  onLeavePoint: () => void;
  onMovePoint: (event: PointerEvent<SVGCircleElement>, point: TrainingTrendPoint) => void;
}

interface TrainingTrendLaneViewProps {
  dimmed: boolean;
  hoverUmaban: string | null;
  lane: TrainingTrendLane;
  plotLeft: number;
  plotRight: number;
  onLeavePoint: () => void;
  onMovePoint: (event: PointerEvent<SVGCircleElement>, point: TrainingTrendPoint) => void;
}

interface TrainingMarkEmphasis {
  active: boolean;
  dimmed: boolean;
  isLatest: boolean;
}

interface TrainingPointMarkVisual {
  fillOpacity: number;
  fontSize: number;
  fontWeight: number;
  labelOpacity: number;
  radius: number;
  strokeOpacity: number;
  strokeWidth: number;
}

const AXIS_STROKE: string = "#8a9a90";
const GRID_STROKE: string = "#d8e0da";
const EVEN_LINE_DASH: string = "4 4";
const TICK_INSET: number = 6;
const LABEL_GAP: number = 18;
const TITLE_GAP: number = 36;
const POINT_RADIUS: number = 4.6;
const OLDER_POINT_RADIUS: number = 2.7;
const ACTIVE_POINT_RADIUS: number = 6.2;
const POINT_STROKE_WIDTH: number = 1.8;
const OLDER_POINT_STROKE_WIDTH: number = 0.7;
const ACTIVE_POINT_STROKE_WIDTH: number = 2.4;
const POINT_FILL_OPACITY: number = 0.5;
const OLDER_POINT_FILL_OPACITY: number = 0.28;
const ACTIVE_POINT_FILL_OPACITY: number = 0.85;
const DIMMED_POINT_FILL_OPACITY: number = 0.1;
const POINT_STROKE_OPACITY: number = 0.95;
const OLDER_POINT_STROKE_OPACITY: number = 0.5;
const ACTIVE_POINT_STROKE_OPACITY: number = 1;
const DIMMED_POINT_STROKE_OPACITY: number = 0.14;
const UMABAN_FONT_SIZE: number = 13;
const OLDER_UMABAN_FONT_SIZE: number = 10;
const ACTIVE_UMABAN_FONT_SIZE: number = 15;
const UMABAN_FONT_WEIGHT: number = 700;
const OLDER_UMABAN_FONT_WEIGHT: number = 500;
const ACTIVE_UMABAN_FONT_WEIGHT: number = 800;
const UMABAN_OPACITY: number = 0.95;
const OLDER_UMABAN_OPACITY: number = 0.55;
const ACTIVE_UMABAN_OPACITY: number = 1;
const DIMMED_UMABAN_OPACITY: number = 0.16;
const SERIES_STROKE: string = TRAINING_TREND_LINE_STROKE;
const TREND_LINE_WIDTH: number = 1.05;
const TREND_LINE_OPACITY: number = 0.32;
const TREND_LINE_ACTIVE_WIDTH: number = 1.6;
const TREND_LINE_ACTIVE_OPACITY: number = 0.5;
const TREND_LINE_DIMMED_OPACITY: number = 0.08;
const TREND_UMABAN_FILL: string = "#4a5850";
const Y_AXIS_TITLE_X: number = 16;
const UMABAN_LABEL_DX: number = 11;
const UMABAN_LABEL_DY: number = 4;
const TRAINING_SCATTER_SCOPE_RADIO_NAME: string = "training-scatter-scope";

const TRAINING_CHART_LEGEND: TrainingChartLegendItem[] = [
  { label: "◎ / S / 1", stroke: "#b45309" },
  { label: "○ / A / 2", stroke: "#166534" },
  { label: "▲ / B / 3", stroke: "#c2410c" },
  { label: "△ / C / 4", stroke: "#355f9f" },
  { label: "記号なし", stroke: "#64748b" },
];

const TrainingScatterScopeToggle = ({
  onShowAllWorkouts,
  showAllWorkouts,
}: TrainingScatterScopeToggleProps) => (
  <fieldset aria-label="調教グラフの点" className="win-rate-heatmap-view-toggle">
    <label className="running-style-bucket-toggle-label">
      <input
        checked={!showAllWorkouts}
        name={TRAINING_SCATTER_SCOPE_RADIO_NAME}
        type="radio"
        value="latest"
        onChange={() => {
          onShowAllWorkouts(false);
        }}
      />
      最新1本
    </label>
    <label className="running-style-bucket-toggle-label">
      <input
        checked={showAllWorkouts}
        name={TRAINING_SCATTER_SCOPE_RADIO_NAME}
        type="radio"
        value="all"
        onChange={() => {
          onShowAllWorkouts(true);
        }}
      />
      推移
    </label>
  </fieldset>
);

const trainingPointMarkVisual = ({
  active,
  dimmed,
  isLatest,
}: TrainingMarkEmphasis): TrainingPointMarkVisual => {
  if (active) {
    return {
      fillOpacity: ACTIVE_POINT_FILL_OPACITY,
      fontSize: ACTIVE_UMABAN_FONT_SIZE,
      fontWeight: ACTIVE_UMABAN_FONT_WEIGHT,
      labelOpacity: ACTIVE_UMABAN_OPACITY,
      radius: ACTIVE_POINT_RADIUS,
      strokeOpacity: ACTIVE_POINT_STROKE_OPACITY,
      strokeWidth: ACTIVE_POINT_STROKE_WIDTH,
    };
  }
  if (dimmed) {
    if (isLatest) {
      return {
        fillOpacity: DIMMED_POINT_FILL_OPACITY,
        fontSize: UMABAN_FONT_SIZE,
        fontWeight: UMABAN_FONT_WEIGHT,
        labelOpacity: DIMMED_UMABAN_OPACITY,
        radius: POINT_RADIUS,
        strokeOpacity: DIMMED_POINT_STROKE_OPACITY,
        strokeWidth: POINT_STROKE_WIDTH,
      };
    }
    return {
      fillOpacity: DIMMED_POINT_FILL_OPACITY,
      fontSize: OLDER_UMABAN_FONT_SIZE,
      fontWeight: OLDER_UMABAN_FONT_WEIGHT,
      labelOpacity: DIMMED_UMABAN_OPACITY,
      radius: OLDER_POINT_RADIUS,
      strokeOpacity: DIMMED_POINT_STROKE_OPACITY,
      strokeWidth: OLDER_POINT_STROKE_WIDTH,
    };
  }
  if (isLatest) {
    return {
      fillOpacity: POINT_FILL_OPACITY,
      fontSize: UMABAN_FONT_SIZE,
      fontWeight: UMABAN_FONT_WEIGHT,
      labelOpacity: UMABAN_OPACITY,
      radius: POINT_RADIUS,
      strokeOpacity: POINT_STROKE_OPACITY,
      strokeWidth: POINT_STROKE_WIDTH,
    };
  }
  return {
    fillOpacity: OLDER_POINT_FILL_OPACITY,
    fontSize: OLDER_UMABAN_FONT_SIZE,
    fontWeight: OLDER_UMABAN_FONT_WEIGHT,
    labelOpacity: OLDER_UMABAN_OPACITY,
    radius: OLDER_POINT_RADIUS,
    strokeOpacity: OLDER_POINT_STROKE_OPACITY,
    strokeWidth: OLDER_POINT_STROKE_WIDTH,
  };
};

const TrainingChartPointMark = ({
  active,
  dimmed,
  onLeave,
  onMove,
  point,
  showLabel,
}: TrainingChartPointMarkProps) => {
  const visual = trainingPointMarkVisual({
    active,
    dimmed,
    isLatest: point.isLatest,
  });
  return (
    <g>
      <circle
        className="training-chart-point"
        cx={point.x}
        cy={point.y}
        data-course-facet={point.courseFacet}
        data-even-furlongs={point.evenPaceFurlongs}
        data-horse={point.horseName}
        data-latest={point.isLatest ? "1" : "0"}
        data-training-point={point.id}
        data-umaban={point.umaban}
        fill={point.stroke}
        fillOpacity={visual.fillOpacity}
        r={visual.radius}
        stroke={point.stroke}
        strokeOpacity={visual.strokeOpacity}
        strokeWidth={visual.strokeWidth}
        onPointerEnter={(event) => {
          onMove(event, point);
        }}
        onPointerLeave={onLeave}
        onPointerMove={(event) => {
          onMove(event, point);
        }}
      />
      {showLabel ? (
        <text
          className="training-chart-umaban"
          data-umaban-label={point.umaban}
          fill={point.stroke}
          fontSize={visual.fontSize}
          fontWeight={visual.fontWeight}
          opacity={visual.labelOpacity}
          x={point.x + UMABAN_LABEL_DX}
          y={point.y + UMABAN_LABEL_DY}
        >
          {point.umaban}
        </text>
      ) : null}
    </g>
  );
};

const TrainingChartPlotView = ({
  chart,
  hoverUmaban,
  onLeavePoint,
  onMovePoint,
}: TrainingChartPlotViewProps) => (
  <svg className="training-chart-svg" viewBox={`0 0 ${chart.width} ${chart.height}`}>
    {chart.yTicks.map((tick) => (
      <g key={`y-${tick.label}-${tick.y}`}>
        <line
          stroke={GRID_STROKE}
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
      <g key={`x-${tick.label}-${tick.x}`}>
        <line
          stroke={GRID_STROKE}
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
    <line
      className="training-chart-even"
      stroke={AXIS_STROKE}
      strokeDasharray={EVEN_LINE_DASH}
      x1={chart.evenX}
      x2={chart.evenX}
      y1={chart.plotTop}
      y2={chart.plotBottom}
    />
    <line
      className="training-chart-even"
      stroke={AXIS_STROKE}
      strokeDasharray={EVEN_LINE_DASH}
      x1={chart.plotLeft}
      x2={chart.plotRight}
      y1={chart.evenY}
      y2={chart.evenY}
    />
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
    <text className="training-chart-even-label" x={chart.evenLabelX} y={chart.evenLabelY}>
      {chart.evenLabel}
    </text>
    <text
      className="training-chart-axis"
      textAnchor="middle"
      x={(chart.plotLeft + chart.plotRight) / 2}
      y={chart.plotBottom + TITLE_GAP}
    >
      {chart.xAxisTitle}
    </text>
    {chart.points.map((point) => (
      <TrainingChartPointMark
        active={hoverUmaban === point.umaban}
        dimmed={hoverUmaban !== null && hoverUmaban !== point.umaban}
        key={point.id}
        point={point}
        showLabel={point.isLatest}
        onLeave={onLeavePoint}
        onMove={onMovePoint}
      />
    ))}
  </svg>
);

const trendLineVisual = (active: boolean, dimmed: boolean): { opacity: number; width: number } => {
  if (active) {
    return { opacity: TREND_LINE_ACTIVE_OPACITY, width: TREND_LINE_ACTIVE_WIDTH };
  }
  if (dimmed) {
    return { opacity: TREND_LINE_DIMMED_OPACITY, width: TREND_LINE_WIDTH };
  }
  return { opacity: TREND_LINE_OPACITY, width: TREND_LINE_WIDTH };
};

const TrainingTrendLaneView = ({
  dimmed,
  hoverUmaban,
  lane,
  onLeavePoint,
  onMovePoint,
  plotLeft,
  plotRight,
}: TrainingTrendLaneViewProps) => {
  const active = hoverUmaban === lane.umaban;
  const line = trendLineVisual(active, dimmed);
  return (
    <g data-trend-lane={lane.umaban}>
      <line
        className="training-chart-even"
        stroke={AXIS_STROKE}
        strokeDasharray={EVEN_LINE_DASH}
        x1={plotLeft}
        x2={plotRight}
        y1={lane.evenY}
        y2={lane.evenY}
      />
      {lane.path.length === 0 ? null : (
        <path
          d={lane.path}
          data-trend-path={lane.umaban}
          fill="none"
          opacity={line.opacity}
          stroke={SERIES_STROKE}
          strokeWidth={line.width}
        />
      )}
      {lane.points.map((point) => (
        <TrainingChartPointMark
          active={active}
          dimmed={dimmed}
          key={point.id}
          point={point}
          showLabel={false}
          onLeave={onLeavePoint}
          onMove={onMovePoint}
        />
      ))}
      <text
        className="training-chart-umaban"
        data-trend-umaban={lane.umaban}
        fill={TREND_UMABAN_FILL}
        fontSize={12}
        fontWeight={700}
        opacity={dimmed ? DIMMED_UMABAN_OPACITY : UMABAN_OPACITY}
        textAnchor="middle"
        x={TRAINING_TREND_LABEL_X}
        y={lane.labelY + 4}
      >
        {lane.umaban}
      </text>
    </g>
  );
};

const TrainingTrendPlotView = ({
  chart,
  hoverUmaban,
  onLeavePoint,
  onMovePoint,
}: TrainingTrendPlotViewProps) => (
  <svg className="training-chart-svg" viewBox={`0 0 ${chart.width} ${chart.height}`}>
    {chart.xTicks.map((tick) => (
      <g key={`x-${tick.label}-${tick.x}`}>
        <line
          stroke={GRID_STROKE}
          x1={tick.x}
          x2={tick.x}
          y1={chart.lanes[0] === undefined ? 0 : chart.lanes[0].plotTop}
          y2={tick.y - 12}
        />
        <text className="training-chart-axis" textAnchor="middle" x={tick.x} y={tick.y}>
          {tick.label}
        </text>
      </g>
    ))}
    <text
      className="training-chart-axis"
      textAnchor="middle"
      transform={`rotate(-90 ${Y_AXIS_TITLE_X} ${chart.height / 2})`}
      x={Y_AXIS_TITLE_X}
      y={chart.height / 2}
    >
      {chart.yAxisTitle}
    </text>
    {chart.lanes.map((lane) => (
      <TrainingTrendLaneView
        dimmed={hoverUmaban !== null && hoverUmaban !== lane.umaban}
        hoverUmaban={hoverUmaban}
        key={lane.umaban}
        lane={lane}
        plotLeft={chart.plotLeft}
        plotRight={chart.plotRight}
        onLeavePoint={onLeavePoint}
        onMovePoint={onMovePoint}
      />
    ))}
    <text
      className="training-chart-axis"
      textAnchor="middle"
      x={(chart.plotLeft + chart.plotRight) / 2}
      y={chart.height - 2}
    >
      {chart.xAxisTitle}
    </text>
  </svg>
);

export const TrainingTimesChart = ({
  onShowAllWorkouts,
  showAllWorkouts,
  trainings,
}: TrainingTimesChartProps) => {
  const scatter = showAllWorkouts ? null : buildDrawnTrainingChart({ trainings });
  const trend = showAllWorkouts ? buildDrawnTrainingTrendChart(trainings) : null;
  const frameRef = useRef<HTMLDivElement | null>(null);
  const [hoverUmaban, setHoverUmaban] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<TrainingChartTooltipView | null>(null);
  if (scatter === null && trend === null) {
    return <p className="empty-state">調教グラフに使えるタイムがありません。</p>;
  }
  const hideTooltip = () => {
    setHoverUmaban(null);
    setTooltip(null);
  };
  const moveTooltip = (event: PointerEvent<SVGCircleElement>, point: TrainingScatterPoint) => {
    const origin = trainingChartFrameOrigin(frameRef.current);
    const location = trainingChartTooltipPosition({
      clientX: event.clientX,
      clientY: event.clientY,
      frameLeft: origin.left,
      frameTop: origin.top,
    });
    setHoverUmaban(point.umaban);
    setTooltip({
      lines: formatTrainingChartTooltip(point),
      x: location.x,
      y: location.y,
    });
  };
  return (
    <figure
      aria-label={showAllWorkouts ? "調教追い切り推移" : "調教追い切り散布図"}
      className="training-chart"
    >
      <TrainingScatterScopeToggle
        showAllWorkouts={showAllWorkouts}
        onShowAllWorkouts={onShowAllWorkouts}
      />
      <p className="training-chart-note">
        {showAllWorkouts ? TRAINING_TREND_CHART_NOTE : TRAINING_CHART_NOTE}
      </p>
      <ul className="training-chart-legend">
        {TRAINING_CHART_LEGEND.map((item) => (
          <li key={item.label}>
            <span className="training-chart-swatch" style={{ background: item.stroke }} />
            {item.label}
          </li>
        ))}
      </ul>
      <div className="training-chart-frame" ref={frameRef}>
        {scatter === null ? null : (
          <TrainingChartPlotView
            chart={scatter}
            hoverUmaban={hoverUmaban}
            onLeavePoint={hideTooltip}
            onMovePoint={moveTooltip}
          />
        )}
        {trend === null ? null : (
          <TrainingTrendPlotView
            chart={trend}
            hoverUmaban={hoverUmaban}
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
