"use client";

// This file runs with bun.

import type { PointerEvent } from "react";
import { useRef, useState } from "react";

import {
  BAN_EI_ABILITY_HORSE_LINK_STROKE,
  BAN_EI_FINISH_FIRST_RADIUS,
  BAN_EI_FINISH_OTHER_RADIUS,
  BAN_EI_FINISH_PLACE_RADIUS,
  BAN_EI_FINISH_SECOND_RADIUS,
  BAN_EI_FINISH_THIRD_RADIUS,
  BAN_EI_SCHEDULED_GUIDE_STROKE,
  BAN_EI_WEIGHT_LINK_STROKE,
  buildDrawnBanEiAbilityChart,
  formatBanEiFinishMarkLabel,
  formatRaceTimeChartTooltip,
  raceTimeChartEmptyMessage,
  raceTimeChartNote,
  RACE_TIME_REFERENCE_STROKE_OPACITY,
  RACE_TIME_REFERENCE_STROKE_WIDTH,
  raceTimeFinishStroke,
  type DrawnRaceTimeChart,
  type RaceTimeChartPoint,
} from "../../../lib/horse-race-time-charts";
import type { HorseRaceResult, RaceTimeStats, Runner } from "../../../lib/race-types";
import { isBanEiKeibajoCode } from "../../../lib/runner-format";
import {
  trainingChartFrameOrigin,
  trainingChartTooltipPosition,
} from "../../../lib/training-charts";
import { HorseRaceClockGallery } from "./horse-race-clock-charts";

interface HorseRaceTimeChartProps {
  currentDistance: string | null | undefined;
  keibajoCode?: string | null;
  results: HorseRaceResult[];
  runners: Runner[];
  stats: RaceTimeStats | null;
}

interface RaceTimeChartLegendItem {
  label: string;
  radius: number | null;
  shape: "circle" | "diamond" | "swatch";
  stroke: string;
}

interface LegendSwatchProps {
  item: RaceTimeChartLegendItem;
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
  onMovePoint: (event: PointerEvent<SVGElement>, point: RaceTimeChartPoint) => void;
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
const UMABAN_OUTSIDE_GAP: number = 5;
const HORSE_LINK_OPACITY: number = 0.32;
const HORSE_LINK_ACTIVE_OPACITY: number = 0.55;
const HORSE_LINK_DIMMED_OPACITY: number = 0.08;
const HORSE_LINK_WIDTH: number = 1.05;
const HORSE_LINK_ACTIVE_WIDTH: number = 1.6;
const FINISH_LABEL_DY: number = 3.5;
const FINISH_LABEL_FONT_SIZE: number = 10;
const ACTIVE_FINISH_LABEL_FONT_SIZE: number = 12;
const LEGEND_MARK_CENTER: number = 12;
const LEGEND_MARK_FILL_OPACITY: number = 0.4;
const LEGEND_MARK_STROKE_WIDTH: number = 1.4;
const LEGEND_DIAMOND_POINTS: string = "12,4 20,12 12,20 4,12";
const WEIGHT_LINK_OPACITY: number = 0.38;
const WEIGHT_LINK_ACTIVE_OPACITY: number = 0.62;
const WEIGHT_LINK_DIMMED_OPACITY: number = 0.08;
const WEIGHT_LINK_WIDTH: number = 1.15;
const WEIGHT_LINK_ACTIVE_WIDTH: number = 1.7;
const SCHEDULED_GUIDE_OPACITY: number = 0.38;
const SCHEDULED_GUIDE_WIDTH: number = 1;
const SCHEDULED_GUIDE_DASH: string = "4 4";
const SCHEDULED_GUIDE_LABEL_DY: number = 12;
const SCHEDULED_MARK_FILL_OPACITY: number = 0.08;
const SCHEDULED_MARK_STROKE_WIDTH: number = 1.5;
const SCHEDULED_MARK_ACTIVE_STROKE_WIDTH: number = 2.1;

const BAN_EI_FINISH_LEGEND: RaceTimeChartLegendItem[] = [
  {
    label: "1着",
    radius: BAN_EI_FINISH_FIRST_RADIUS,
    shape: "circle",
    stroke: raceTimeFinishStroke(1),
  },
  {
    label: "2着",
    radius: BAN_EI_FINISH_SECOND_RADIUS,
    shape: "circle",
    stroke: raceTimeFinishStroke(2),
  },
  {
    label: "3着",
    radius: BAN_EI_FINISH_THIRD_RADIUS,
    shape: "circle",
    stroke: raceTimeFinishStroke(3),
  },
  {
    label: "4-5着",
    radius: BAN_EI_FINISH_PLACE_RADIUS,
    shape: "circle",
    stroke: raceTimeFinishStroke(4),
  },
  {
    label: "着外",
    radius: BAN_EI_FINISH_OTHER_RADIUS,
    shape: "circle",
    stroke: raceTimeFinishStroke(6),
  },
];

const REFERENCE_LEGEND: RaceTimeChartLegendItem[] = [
  { label: "最速", radius: null, shape: "swatch", stroke: "#be123c" },
  { label: "平均", radius: null, shape: "swatch", stroke: "#166534" },
  { label: "中央値", radius: null, shape: "swatch", stroke: "#4338ca" },
];

const WEIGHT_ROLE_LEGEND: RaceTimeChartLegendItem[] = [
  { label: "過去斤量", radius: 5.6, shape: "circle", stroke: "#64748b" },
  { label: "予定斤量", radius: null, shape: "diamond", stroke: "#64748b" },
];

const LegendSwatch = ({ item }: LegendSwatchProps) => {
  if (item.shape === "swatch") {
    return <span className="training-chart-swatch" style={{ background: item.stroke }} />;
  }
  if (item.shape === "diamond") {
    return (
      <svg aria-hidden="true" className="training-chart-legend-mark" viewBox="0 0 24 24">
        <polygon
          fill="none"
          points={LEGEND_DIAMOND_POINTS}
          stroke={item.stroke}
          strokeWidth={LEGEND_MARK_STROKE_WIDTH}
        />
      </svg>
    );
  }
  return (
    <svg aria-hidden="true" className="training-chart-legend-mark" viewBox="0 0 24 24">
      <circle
        cx={LEGEND_MARK_CENTER}
        cy={LEGEND_MARK_CENTER}
        fill={item.stroke}
        fillOpacity={LEGEND_MARK_FILL_OPACITY}
        r={item.radius === null ? LEGEND_MARK_CENTER : item.radius}
        stroke={item.stroke}
        strokeWidth={LEGEND_MARK_STROKE_WIDTH}
      />
    </svg>
  );
};

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

const weightLinkOpacity = (umaban: string, hoverUmaban: string | null): number => {
  if (hoverUmaban === null) {
    return WEIGHT_LINK_OPACITY;
  }
  if (hoverUmaban === umaban) {
    return WEIGHT_LINK_ACTIVE_OPACITY;
  }
  return WEIGHT_LINK_DIMMED_OPACITY;
};

const scheduledMarkOpacity = (umaban: string, hoverUmaban: string | null): number => {
  if (hoverUmaban === null) {
    return POINT_STROKE_OPACITY;
  }
  if (hoverUmaban === umaban) {
    return ACTIVE_POINT_STROKE_OPACITY;
  }
  return DIMMED_POINT_STROKE_OPACITY;
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
    {chart.scheduledGuides.map((guide) => (
      <g key={`${keyPrefix}guide-${guide.label}`}>
        <line
          data-scheduled-guide={guide.label}
          stroke={BAN_EI_SCHEDULED_GUIDE_STROKE}
          strokeDasharray={SCHEDULED_GUIDE_DASH}
          strokeOpacity={SCHEDULED_GUIDE_OPACITY}
          strokeWidth={SCHEDULED_GUIDE_WIDTH}
          x1={guide.x}
          x2={guide.x}
          y1={chart.plotTop}
          y2={chart.plotBottom}
        />
        <text
          className="training-chart-axis"
          textAnchor="middle"
          x={guide.x}
          y={chart.plotTop + SCHEDULED_GUIDE_LABEL_DY}
        >
          {guide.label}
        </text>
      </g>
    ))}
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
    {chart.weightLinks.map((link) => (
      <line
        data-weight-link={link.umaban}
        key={`${keyPrefix}weight-${link.umaban}-${link.x1}-${link.y}`}
        stroke={BAN_EI_WEIGHT_LINK_STROKE}
        strokeOpacity={weightLinkOpacity(link.umaban, hoverUmaban)}
        strokeWidth={hoverUmaban === link.umaban ? WEIGHT_LINK_ACTIVE_WIDTH : WEIGHT_LINK_WIDTH}
        x1={link.x1}
        x2={link.x2}
        y1={link.y}
        y2={link.y}
      />
    ))}
    {chart.points.flatMap((point) => {
      const mark = chart.scheduledMarks.find((item) => item.id === point.id);
      if (mark === undefined) {
        return [];
      }
      const active = hoverUmaban === mark.umaban;
      return [
        <polygon
          className="training-chart-point"
          data-scheduled-weight={mark.umaban}
          fill={mark.stroke}
          fillOpacity={SCHEDULED_MARK_FILL_OPACITY}
          key={`${keyPrefix}scheduled-${mark.id}`}
          points={mark.points}
          stroke={mark.stroke}
          strokeOpacity={scheduledMarkOpacity(mark.umaban, hoverUmaban)}
          strokeWidth={active ? SCHEDULED_MARK_ACTIVE_STROKE_WIDTH : SCHEDULED_MARK_STROKE_WIDTH}
          onPointerEnter={(event) => {
            onMovePoint(event, point);
          }}
          onPointerLeave={onLeavePoint}
          onPointerMove={(event) => {
            onMovePoint(event, point);
          }}
        />,
      ];
    })}
    {chart.points.map((point) => {
      const active = hoverId === point.id || hoverUmaban === point.umaban;
      const dimmed =
        (hoverId !== null && hoverId !== point.id) ||
        (hoverUmaban !== null && hoverUmaban !== point.umaban);
      const showLabel = point.isLatest || active;
      const finishLabel = formatBanEiFinishMarkLabel(point.finishRank);
      const pointRadius = raceTimePointRadius(active, point.finishRank, point.radius);
      const umabanDx = point.radius === null ? UMABAN_LABEL_DX : pointRadius + UMABAN_OUTSIDE_GAP;
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
            r={pointRadius}
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
          {point.radius === null ? null : (
            <text
              className="training-chart-umaban"
              data-finish-label={finishLabel}
              fill={point.stroke}
              fontSize={active ? ACTIVE_FINISH_LABEL_FONT_SIZE : FINISH_LABEL_FONT_SIZE}
              fontWeight={800}
              opacity={raceTimeUmabanOpacity(active, dimmed)}
              textAnchor="middle"
              x={point.x}
              y={point.y + FINISH_LABEL_DY}
            >
              {finishLabel}
            </text>
          )}
          {showLabel ? (
            <text
              className="training-chart-umaban"
              data-umaban-label={point.umaban}
              fill={point.stroke}
              fontSize={active ? 14 : 11}
              fontWeight={active ? 800 : 700}
              opacity={raceTimeUmabanOpacity(active, dimmed)}
              x={point.x + umabanDx}
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
  runners,
  stats,
}: HorseRaceTimeChartProps) => {
  const isBanEi = isBanEiKeibajoCode(keibajoCode);
  const banEiDrawn = isBanEi
    ? buildDrawnBanEiAbilityChart({ currentDistance, keibajoCode, results, runners, stats })
    : null;
  const frameRef = useRef<HTMLDivElement | null>(null);
  const [hoverUmaban, setHoverUmaban] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<RaceTimeChartTooltipView | null>(null);
  if (!isBanEi) {
    return (
      <HorseRaceClockGallery
        currentDistance={currentDistance}
        results={results}
        runners={runners}
        stats={stats}
      />
    );
  }
  if (banEiDrawn === null) {
    return <p className="empty-state">{raceTimeChartEmptyMessage(true)}</p>;
  }
  const hideTooltip = () => {
    setHoverUmaban(null);
    setTooltip(null);
  };
  const moveTooltip = (event: PointerEvent<SVGElement>, point: RaceTimeChartPoint) => {
    const origin = trainingChartFrameOrigin(frameRef.current);
    const location = trainingChartTooltipPosition({
      clientX: event.clientX,
      clientY: event.clientY,
      frameLeft: origin.left,
      frameTop: origin.top,
    });
    setHoverUmaban(point.umaban);
    setTooltip({
      lines: formatRaceTimeChartTooltip(point),
      x: location.x,
      y: location.y,
    });
  };
  return (
    <figure aria-label="競走成績タイム散布図" className="training-chart">
      <p className="training-chart-note">{raceTimeChartNote(true)}</p>
      <ul className="training-chart-legend">
        {BAN_EI_FINISH_LEGEND.map((item) => (
          <li key={item.label}>
            <LegendSwatch item={item} />
            {item.label}
          </li>
        ))}
      </ul>
      <ul className="training-chart-legend">
        {WEIGHT_ROLE_LEGEND.map((item) => (
          <li key={item.label}>
            <LegendSwatch item={item} />
            {item.label}
          </li>
        ))}
      </ul>
      <ul className="training-chart-legend">
        {REFERENCE_LEGEND.map((item) => (
          <li key={item.label}>
            <LegendSwatch item={item} />
            {item.label}
          </li>
        ))}
      </ul>
      <div className="training-chart-frame" ref={frameRef}>
        <RaceTimeChartPlotView
          chart={banEiDrawn}
          hoverId={null}
          hoverUmaban={hoverUmaban}
          keyPrefix="banei-"
          onLeavePoint={hideTooltip}
          onMovePoint={moveTooltip}
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
