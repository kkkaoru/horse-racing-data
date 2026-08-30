"use client";

// This file runs with bun.

import type { PointerEvent } from "react";
import { useRef, useState } from "react";

import {
  buildClockAnalysisRows,
  clockParFromRaceTimeStats,
  buildScatterView,
  formatClockPointTooltip,
  formatClockReferenceTooltip,
  type ClockReferenceKind,
  type ClockReferenceLine,
  type ClockScatterPoint,
  type ClockScatterView,
} from "../../../lib/horse-race-clock-analysis";
import { raceTimeChartEmptyMessage } from "../../../lib/horse-race-time-charts";
import type { HorseRaceResult, RaceTimeStats } from "../../../lib/race-types";
import {
  trainingChartFrameOrigin,
  trainingChartTooltipPosition,
} from "../../../lib/training-charts";

interface ClockGalleryRunner {
  kettoTorokuBango: string | null;
  wakuban: string | null;
}

interface HorseRaceClockGalleryProps {
  currentDistance: string | null | undefined;
  results: HorseRaceResult[];
  runners: ClockGalleryRunner[];
  stats: RaceTimeStats | null;
}

interface ClockTooltipView {
  lines: string[];
  x: number;
  y: number;
}

interface ScatterPlotProps {
  hoverReferenceKind: ClockReferenceKind | null;
  hoverUmaban: string | null;
  onLeave: () => void;
  onMovePoint: (event: PointerEvent<SVGElement>, point: ClockScatterPoint) => void;
  onMoveReference: (event: PointerEvent<SVGElement>, line: ClockReferenceLine) => void;
  view: ClockScatterView;
}

interface ReferenceLegendMarkProps {
  stroke: string;
  strokeDasharray: string;
}

interface ClockReferenceLegendProps {
  lines: ClockReferenceLine[];
}

const AXIS_STROKE: string = "#8a9a90";
const GRID_STROKE: string = "#d8e0da";
const GRID_STROKE_OPACITY: number = 0.18;
const GRID_STROKE_WIDTH: number = 0.6;
const TICK_INSET: number = 6;
const LABEL_GAP: number = 18;
const Y_AXIS_TITLE_X: number = 16;
const ACTIVE_POINT_RADIUS: number = 7;
const BASE_FILL_OPACITY: number = 0.28;
const DIMMED_FILL_OPACITY: number = 0.08;
const POINT_STROKE_OPACITY: number = 0.55;
const DIMMED_STROKE_OPACITY: number = 0.12;
const ACTIVE_STROKE_OPACITY: number = 0.9;
const UMABAN_OPACITY: number = 0.92;
const DIMMED_UMABAN_OPACITY: number = 0.22;
const ACTIVE_UMABAN_OPACITY: number = 1;
const UMABAN_LABEL_DX: number = 8;
const UMABAN_LABEL_DY: number = 4;
const REFERENCE_STROKE_OPACITY: number = 0.22;
const REFERENCE_HOVER_STROKE_OPACITY: number = 0.7;
const REFERENCE_STROKE_WIDTH: number = 1.8;
const REFERENCE_HIT_STROKE_WIDTH: number = 16;
const LEGEND_MARK_STROKE_WIDTH: number = 2.4;

const pointRadius = (active: boolean, base: number): number =>
  active ? ACTIVE_POINT_RADIUS : base;

const fillOpacity = (dimmed: boolean): number => (dimmed ? DIMMED_FILL_OPACITY : BASE_FILL_OPACITY);

const strokeOpacity = (active: boolean, dimmed: boolean): number => {
  if (active) {
    return ACTIVE_STROKE_OPACITY;
  }
  return dimmed ? DIMMED_STROKE_OPACITY : POINT_STROKE_OPACITY;
};

const umabanOpacity = (active: boolean, dimmed: boolean): number => {
  if (active) {
    return ACTIVE_UMABAN_OPACITY;
  }
  return dimmed ? DIMMED_UMABAN_OPACITY : UMABAN_OPACITY;
};

const isTimeReference = (line: ClockReferenceLine): boolean => line.y1 === line.y2;

const isKohanReference = (line: ClockReferenceLine): boolean => line.x1 === line.x2;

const referenceStrokeOpacity = (active: boolean): number =>
  active ? REFERENCE_HOVER_STROKE_OPACITY : REFERENCE_STROKE_OPACITY;

const ReferenceLegendMark = ({ stroke, strokeDasharray }: ReferenceLegendMarkProps) => (
  <svg aria-hidden="true" className="training-chart-legend-mark" viewBox="0 0 24 24">
    <line
      stroke={stroke}
      strokeDasharray={strokeDasharray}
      strokeWidth={LEGEND_MARK_STROKE_WIDTH}
      x1={2}
      x2={22}
      y1={12}
      y2={12}
    />
  </svg>
);

const ClockReferenceLegend = ({ lines }: ClockReferenceLegendProps) =>
  lines.length === 0 ? null : (
    <ul className="training-chart-legend">
      {lines.map((line) => (
        <li key={line.kind}>
          <ReferenceLegendMark stroke={line.stroke} strokeDasharray={line.strokeDasharray} />
          {line.label}
        </li>
      ))}
    </ul>
  );

const ScatterPlot = ({
  hoverReferenceKind,
  hoverUmaban,
  onLeave,
  onMovePoint,
  onMoveReference,
  view,
}: ScatterPlotProps) => (
  <svg className="training-chart-svg" viewBox={`0 0 ${view.width} ${view.height}`}>
    <line
      stroke={AXIS_STROKE}
      x1={view.plotLeft}
      x2={view.plotRight}
      y1={view.plotBottom}
      y2={view.plotBottom}
    />
    <line
      stroke={AXIS_STROKE}
      x1={view.plotLeft}
      x2={view.plotLeft}
      y1={view.plotTop}
      y2={view.plotBottom}
    />
    {view.xTicks.map((tick) => (
      <g key={`x-${tick.label}-${tick.x}`}>
        <line
          stroke={GRID_STROKE}
          strokeOpacity={GRID_STROKE_OPACITY}
          strokeWidth={GRID_STROKE_WIDTH}
          x1={tick.x}
          x2={tick.x}
          y1={view.plotTop}
          y2={view.plotBottom}
        />
        <text
          fill={AXIS_STROKE}
          fontSize={10}
          textAnchor="middle"
          x={tick.x}
          y={tick.y + LABEL_GAP}
        >
          {tick.label}
        </text>
      </g>
    ))}
    {view.yTicks.map((tick) => (
      <g key={`y-${tick.label}-${tick.y}`}>
        <line
          stroke={GRID_STROKE}
          strokeOpacity={GRID_STROKE_OPACITY}
          strokeWidth={GRID_STROKE_WIDTH}
          x1={view.plotLeft}
          x2={view.plotRight}
          y1={tick.y}
          y2={tick.y}
        />
        <text
          fill={AXIS_STROKE}
          fontSize={10}
          textAnchor="end"
          x={tick.x - TICK_INSET}
          y={tick.y + 3}
        >
          {tick.label}
        </text>
      </g>
    ))}
    <text
      fill={AXIS_STROKE}
      fontSize={11}
      textAnchor="middle"
      x={(view.plotLeft + view.plotRight) / 2}
      y={view.height - 8}
    >
      {view.xAxisTitle}
    </text>
    <text
      fill={AXIS_STROKE}
      fontSize={11}
      textAnchor="middle"
      transform={`rotate(-90 ${Y_AXIS_TITLE_X} ${(view.plotTop + view.plotBottom) / 2})`}
      x={Y_AXIS_TITLE_X}
      y={(view.plotTop + view.plotBottom) / 2}
    >
      {view.yAxisTitle}
    </text>
    {view.references.map((line) => (
      <g key={line.kind}>
        <line
          data-reference={line.kind}
          stroke={line.stroke}
          strokeDasharray={line.strokeDasharray}
          strokeOpacity={referenceStrokeOpacity(hoverReferenceKind === line.kind)}
          strokeWidth={REFERENCE_STROKE_WIDTH}
          x1={line.x1}
          x2={line.x2}
          y1={line.y1}
          y2={line.y2}
        />
        <line
          className="training-chart-reference-hit"
          data-reference-hit={line.kind}
          pointerEvents="stroke"
          stroke={line.stroke}
          strokeOpacity={0}
          strokeWidth={REFERENCE_HIT_STROKE_WIDTH}
          x1={line.x1}
          x2={line.x2}
          y1={line.y1}
          y2={line.y2}
          onPointerEnter={(event) => {
            onMoveReference(event, line);
          }}
          onPointerLeave={onLeave}
          onPointerMove={(event) => {
            onMoveReference(event, line);
          }}
        />
      </g>
    ))}
    {view.points.map((point) => {
      const active = hoverUmaban === point.umaban;
      const dimmed = hoverUmaban !== null && hoverUmaban !== point.umaban;
      return (
        <g key={point.id}>
          <circle
            className="training-chart-point"
            cx={point.x}
            cy={point.y}
            data-clock-point={point.id}
            data-horse={point.horseName}
            data-umaban={point.umaban}
            fill={point.fill}
            fillOpacity={fillOpacity(dimmed)}
            r={pointRadius(active, point.r)}
            stroke={point.stroke}
            strokeOpacity={strokeOpacity(active, dimmed)}
            strokeWidth={active ? 2.2 : 1.4}
            onPointerEnter={(event) => {
              onMovePoint(event, point);
            }}
            onPointerLeave={onLeave}
            onPointerMove={(event) => {
              onMovePoint(event, point);
            }}
          />
          <text
            className="training-chart-umaban"
            data-umaban-label={point.umaban}
            fill={point.stroke}
            fontSize={active ? 14 : 11}
            fontWeight={active ? 800 : 700}
            opacity={umabanOpacity(active, dimmed)}
            x={point.x + UMABAN_LABEL_DX}
            y={point.y + UMABAN_LABEL_DY}
          >
            {point.umaban}
          </text>
        </g>
      );
    })}
  </svg>
);

export const HorseRaceClockGallery = ({
  currentDistance,
  results,
  runners,
  stats,
}: HorseRaceClockGalleryProps) => {
  const rows = buildClockAnalysisRows({ currentDistance, results, runners });
  const scatter = buildScatterView(rows, clockParFromRaceTimeStats(stats));
  const frameRef = useRef<HTMLDivElement | null>(null);
  const [hoverReferenceKind, setHoverReferenceKind] = useState<ClockReferenceKind | null>(null);
  const [hoverUmaban, setHoverUmaban] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<ClockTooltipView | null>(null);
  if (scatter === null) {
    return <p className="empty-state">{raceTimeChartEmptyMessage(false)}</p>;
  }
  const hideTooltip = () => {
    setHoverReferenceKind(null);
    setHoverUmaban(null);
    setTooltip(null);
  };
  const tooltipLocation = (event: PointerEvent<SVGElement>) => {
    const origin = trainingChartFrameOrigin(frameRef.current);
    return trainingChartTooltipPosition({
      clientX: event.clientX,
      clientY: event.clientY,
      frameLeft: origin.left,
      frameTop: origin.top,
    });
  };
  const movePointTooltip = (event: PointerEvent<SVGElement>, point: ClockScatterPoint) => {
    const location = tooltipLocation(event);
    setHoverReferenceKind(null);
    setHoverUmaban(point.umaban);
    setTooltip({
      lines: formatClockPointTooltip(point),
      x: location.x,
      y: location.y,
    });
  };
  const moveReferenceTooltip = (event: PointerEvent<SVGElement>, line: ClockReferenceLine) => {
    const location = tooltipLocation(event);
    setHoverReferenceKind(line.kind);
    setHoverUmaban(null);
    setTooltip({
      lines: formatClockReferenceTooltip(line),
      x: location.x,
      y: location.y,
    });
  };
  return (
    <figure aria-label="競走成績タイム散布図" className="training-chart race-clock-gallery">
      <h3>{scatter.title}</h3>
      {scatter.references.length === 0 ? null : (
        <div className="race-clock-legend-groups">
          <ClockReferenceLegend lines={scatter.references.filter(isTimeReference)} />
          <ClockReferenceLegend lines={scatter.references.filter(isKohanReference)} />
        </div>
      )}
      <div className="training-chart-frame" ref={frameRef}>
        <ScatterPlot
          hoverReferenceKind={hoverReferenceKind}
          hoverUmaban={hoverUmaban}
          onLeave={hideTooltip}
          onMovePoint={movePointTooltip}
          onMoveReference={moveReferenceTooltip}
          view={scatter}
        />
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
