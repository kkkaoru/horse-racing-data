"use client";

// This file runs with bun.

import { axisBottom, axisLeft, pointer, scaleBand, scaleLog, select } from "d3";
import type { ClientPointEvent, ScaleBand, ScaleLogarithmic, Selection } from "d3";
import { useEffect, useRef, useState } from "react";

import {
  CONDITION_FINISH_CHART_NOTE,
  CONDITION_PAYOUT_CHART_NOTE,
  buildFinishOddsDistributionView,
  finishBoxForGroup,
  finishGroupLabels,
  finishOddsLogTicks,
  finishOddsValues,
  finishTooltipContent,
  formatAnalysisOdds,
  formatAnalysisYen,
  hasFinishOddsChartData,
  hasPayoutChartData,
  paddedLogDomain,
  payoutBeeJitter,
  payoutBoxForBetType,
  payoutCategoryLabels,
  payoutLogTicks,
  payoutTooltipContent,
  payoutYenValues,
  type ChartTooltipContent,
  type FinishBeePoint,
  type FinishHorsePoint,
  type FinishOddsDistributionView,
  type PayoutBeePoint,
  type PayoutBoxPlot,
  type PayoutDistributionView,
  type PayoutSummaryRange,
} from "../../../lib/condition-analysis-charts";

interface PayoutTrendChartProps {
  view: PayoutDistributionView;
}

interface FinishPopularityChartProps {
  points: FinishHorsePoint[];
}

interface ChartTooltipView extends ChartTooltipContent {
  x: number;
  y: number;
}

interface PayoutScales {
  labels: string[];
  ticks: number[];
  xScale: ScaleBand<string>;
  yScale: ScaleLogarithmic<number, number>;
}

interface FinishOddsScales {
  labels: string[];
  ticks: number[];
  xScale: ScaleBand<string>;
  yScale: ScaleLogarithmic<number, number>;
}

interface DrawPayoutChartParams {
  frame: HTMLDivElement;
  onHideTooltip: () => void;
  onShowTooltip: (tooltip: ChartTooltipView) => void;
  svg: SVGSVGElement;
  view: PayoutDistributionView;
}

interface DrawFinishOddsChartParams {
  frame: HTMLDivElement;
  onHideTooltip: () => void;
  onShowTooltip: (tooltip: ChartTooltipView) => void;
  svg: SVGSVGElement;
  view: FinishOddsDistributionView;
}

interface BindMarkTooltipParams<Datum> {
  frame: HTMLDivElement;
  marks: Selection<SVGCircleElement, Datum, SVGGElement, undefined>;
  onHideTooltip: () => void;
  onShowTooltip: (tooltip: ChartTooltipView) => void;
  toTooltip: (datum: Datum) => ChartTooltipContent;
}

interface PointerLocationParams {
  event: ClientPointEvent;
  frame: HTMLDivElement;
}

interface BandCenterParams {
  fallback: number;
  label: string;
  scale: ScaleBand<string>;
}

interface ScaledValueParams {
  fallback: number;
  scale: ScaleLogarithmic<number, number>;
  value: number;
}

interface DrawHorizontalGridParams {
  fallback: number;
  layer: Selection<SVGGElement, undefined, null, undefined>;
  ticks: number[];
  yScale: ScaleLogarithmic<number, number>;
}

interface DrawFinishBeesParams {
  bees: FinishBeePoint[];
  layer: Selection<SVGGElement, undefined, null, undefined>;
  scales: FinishOddsScales;
}

const VIEW_WIDTH: number = 720;
const VIEW_HEIGHT: number = 380;
const PLOT_LEFT: number = 72;
const PLOT_RIGHT: number = 704;
const PLOT_TOP: number = 20;
const PLOT_BOTTOM: number = 332;
const BAND_PADDING: number = 0.28;
const BOX_HALF_WIDTH: number = 18;
const RANGE_CAP_WIDTH: number = 10;
const BEE_RADIUS: number = 3.2;
const OUTLIER_RADIUS: number = 4;
const TOOLTIP_OFFSET: number = 12;
const MIN_BOX_HEIGHT: number = 1;
const TICK_SIZE_OUTER: number = 0;

const svgRoot = (svg: SVGSVGElement): Selection<SVGSVGElement, undefined, null, undefined> =>
  select(svg);

const appendLayer = (
  root: Selection<SVGSVGElement, undefined, null, undefined>,
  className: string,
): Selection<SVGGElement, undefined, null, undefined> => root.append("g").attr("class", className);

const bandCenter = ({ fallback, label, scale }: BandCenterParams): number => {
  const start = scale(label);
  return start === undefined ? fallback : start + scale.bandwidth() / 2;
};

const scaledValue = ({ fallback, scale, value }: ScaledValueParams): number => {
  const next = scale(value);
  return Number.isFinite(next) ? next : fallback;
};

const tooltipLocation = ({ event, frame }: PointerLocationParams): { x: number; y: number } => {
  const point = pointer(event, frame);
  return {
    x: point[0] + TOOLTIP_OFFSET,
    y: point[1] + TOOLTIP_OFFSET,
  };
};

const bindMarkTooltip = <Datum,>({
  frame,
  marks,
  onHideTooltip,
  onShowTooltip,
  toTooltip,
}: BindMarkTooltipParams<Datum>): void => {
  const show = (event: ClientPointEvent, datum: Datum): void => {
    const location = tooltipLocation({ event, frame });
    const content = toTooltip(datum);
    onShowTooltip({
      lines: content.lines,
      meta: content.meta,
      title: content.title,
      x: location.x,
      y: location.y,
    });
  };
  marks.on("pointerover mouseover pointermove mousemove", show);
  marks.on("pointerleave mouseleave", onHideTooltip);
};

const createPayoutScales = (view: PayoutDistributionView): PayoutScales | null => {
  const values = payoutYenValues(view).filter((yen) => yen > 0);
  const labels = payoutCategoryLabels(view);
  if (values.length === 0 || labels.length === 0) {
    return null;
  }
  const domain = paddedLogDomain(Math.min(...values), Math.max(...values));
  return {
    labels,
    ticks: payoutLogTicks(domain),
    xScale: scaleBand().domain(labels).range([PLOT_LEFT, PLOT_RIGHT]).padding(BAND_PADDING),
    yScale: scaleLog().domain([domain[0], domain[1]]).range([PLOT_BOTTOM, PLOT_TOP]),
  };
};

const createFinishOddsScales = (view: FinishOddsDistributionView): FinishOddsScales | null => {
  const values = finishOddsValues(view).filter((odds) => odds > 0);
  const labels = finishGroupLabels(view);
  if (values.length === 0 || labels.length === 0) {
    return null;
  }
  const domain = paddedLogDomain(Math.min(...values), Math.max(...values));
  return {
    labels,
    ticks: finishOddsLogTicks(domain),
    xScale: scaleBand().domain(labels).range([PLOT_LEFT, PLOT_RIGHT]).padding(BAND_PADDING),
    yScale: scaleLog().domain([domain[0], domain[1]]).range([PLOT_BOTTOM, PLOT_TOP]),
  };
};

const drawHorizontalGrid = ({ fallback, layer, ticks, yScale }: DrawHorizontalGridParams): void => {
  layer
    .selectAll("line")
    .data(ticks)
    .join("line")
    .attr("class", "condition-analysis-chart-grid")
    .attr("x1", PLOT_LEFT)
    .attr("x2", PLOT_RIGHT)
    .attr("y1", (tick) => scaledValue({ fallback, scale: yScale, value: tick }))
    .attr("y2", (tick) => scaledValue({ fallback, scale: yScale, value: tick }));
};

const drawPayoutAxes = (
  root: Selection<SVGSVGElement, undefined, null, undefined>,
  scales: PayoutScales,
): void => {
  const yAxis = axisLeft(scales.yScale)
    .tickValues([...scales.ticks])
    .tickFormat((value) => formatAnalysisYen(Number(value)))
    .tickSizeOuter(TICK_SIZE_OUTER);
  const xAxis = axisBottom(scales.xScale).tickSizeOuter(TICK_SIZE_OUTER);
  yAxis(
    appendLayer(root, "condition-analysis-chart-y-axis").attr(
      "transform",
      `translate(${PLOT_LEFT},0)`,
    ),
  );
  xAxis(
    appendLayer(root, "condition-analysis-chart-x-axis").attr(
      "transform",
      `translate(0,${PLOT_BOTTOM})`,
    ),
  );
};

const drawFinishOddsAxes = (
  root: Selection<SVGSVGElement, undefined, null, undefined>,
  scales: FinishOddsScales,
): void => {
  const yAxis = axisLeft(scales.yScale)
    .tickValues([...scales.ticks])
    .tickFormat((value) => formatAnalysisOdds(Number(value)))
    .tickSizeOuter(TICK_SIZE_OUTER);
  const xAxis = axisBottom(scales.xScale).tickSizeOuter(TICK_SIZE_OUTER);
  yAxis(
    appendLayer(root, "condition-analysis-chart-y-axis").attr(
      "transform",
      `translate(${PLOT_LEFT},0)`,
    ),
  );
  xAxis(
    appendLayer(root, "condition-analysis-chart-x-axis").attr(
      "transform",
      `translate(0,${PLOT_BOTTOM})`,
    ),
  );
};

const drawPayoutRanges = (
  layer: Selection<SVGGElement, undefined, null, undefined>,
  ranges: PayoutSummaryRange[],
  scales: PayoutScales,
): void => {
  const groups = layer
    .selectAll("g")
    .data(ranges)
    .join("g")
    .attr("class", "condition-analysis-chart-range-group");
  groups
    .append("line")
    .attr("class", "condition-analysis-chart-range")
    .attr("x1", (range) =>
      bandCenter({ fallback: PLOT_LEFT, label: range.betType, scale: scales.xScale }),
    )
    .attr("x2", (range) =>
      bandCenter({ fallback: PLOT_LEFT, label: range.betType, scale: scales.xScale }),
    )
    .attr("y1", (range) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: range.max }),
    )
    .attr("y2", (range) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: range.min }),
    );
  groups
    .append("line")
    .attr("class", "condition-analysis-chart-median")
    .attr(
      "x1",
      (range) =>
        bandCenter({ fallback: PLOT_LEFT, label: range.betType, scale: scales.xScale }) -
        RANGE_CAP_WIDTH,
    )
    .attr(
      "x2",
      (range) =>
        bandCenter({ fallback: PLOT_LEFT, label: range.betType, scale: scales.xScale }) +
        RANGE_CAP_WIDTH,
    )
    .attr("y1", (range) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: range.median }),
    )
    .attr("y2", (range) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: range.median }),
    );
  groups
    .selectAll("circle")
    .data((range) =>
      range.average === null
        ? []
        : [
            {
              betType: range.betType,
              yen: range.average,
            },
          ],
    )
    .join("circle")
    .attr("class", "condition-analysis-chart-average")
    .attr("cx", (range) =>
      bandCenter({ fallback: PLOT_LEFT, label: range.betType, scale: scales.xScale }),
    )
    .attr("cy", (range) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: range.yen }),
    )
    .attr("r", BEE_RADIUS);
};

const drawPayoutBoxes = (
  layer: Selection<SVGGElement, undefined, null, undefined>,
  boxes: PayoutBoxPlot[],
  scales: PayoutScales,
): void => {
  const groups = layer
    .selectAll("g")
    .data(boxes)
    .join("g")
    .attr("class", "condition-analysis-chart-box-group");
  groups
    .append("line")
    .attr("class", "condition-analysis-chart-whisker")
    .attr("x1", (box) =>
      bandCenter({ fallback: PLOT_LEFT, label: box.betType, scale: scales.xScale }),
    )
    .attr("x2", (box) =>
      bandCenter({ fallback: PLOT_LEFT, label: box.betType, scale: scales.xScale }),
    )
    .attr("y1", (box) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: box.whiskerHigh }),
    )
    .attr("y2", (box) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: box.whiskerLow }),
    );
  groups
    .append("rect")
    .attr("class", "condition-analysis-chart-box")
    .attr("data-chart-shape", "box")
    .attr(
      "x",
      (box) =>
        bandCenter({ fallback: PLOT_LEFT, label: box.betType, scale: scales.xScale }) -
        BOX_HALF_WIDTH,
    )
    .attr("width", BOX_HALF_WIDTH * 2)
    .attr("y", (box) => {
      const top = scaledValue({ fallback: PLOT_TOP, scale: scales.yScale, value: box.q3 });
      const bottom = scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: box.q1 });
      return Math.min(top, bottom);
    })
    .attr("height", (box) => {
      const top = scaledValue({ fallback: PLOT_TOP, scale: scales.yScale, value: box.q3 });
      const bottom = scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: box.q1 });
      return Math.max(Math.abs(bottom - top), MIN_BOX_HEIGHT);
    });
  groups
    .append("line")
    .attr("class", "condition-analysis-chart-median")
    .attr(
      "x1",
      (box) =>
        bandCenter({ fallback: PLOT_LEFT, label: box.betType, scale: scales.xScale }) -
        BOX_HALF_WIDTH,
    )
    .attr(
      "x2",
      (box) =>
        bandCenter({ fallback: PLOT_LEFT, label: box.betType, scale: scales.xScale }) +
        BOX_HALF_WIDTH,
    )
    .attr("y1", (box) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: box.median }),
    )
    .attr("y2", (box) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: box.median }),
    );
};

const drawPayoutBees = (
  layer: Selection<SVGGElement, undefined, null, undefined>,
  bees: PayoutBeePoint[],
  scales: PayoutScales,
): Selection<SVGCircleElement, PayoutBeePoint, SVGGElement, undefined> =>
  layer
    .selectAll<SVGCircleElement, PayoutBeePoint>("circle")
    .data(bees)
    .join("circle")
    .attr("class", (bee) =>
      bee.isOutlier
        ? "condition-analysis-payout-bee condition-analysis-payout-bee-outlier"
        : "condition-analysis-payout-bee",
    )
    .attr("data-chart-point", "payout-bee")
    .attr("data-outlier", (bee) => (bee.isOutlier ? "true" : "false"))
    .attr(
      "cx",
      (bee) =>
        bandCenter({ fallback: PLOT_LEFT, label: bee.betType, scale: scales.xScale }) +
        payoutBeeJitter(bee.index),
    )
    .attr("cy", (bee) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: bee.yen }),
    )
    .attr("r", (bee) => (bee.isOutlier ? OUTLIER_RADIUS : BEE_RADIUS));

const finishBeeClassName = (bee: FinishBeePoint): string => {
  if (bee.isOutlier) {
    return "condition-analysis-finish-bee condition-analysis-finish-bee-outlier";
  }
  if (bee.finishGroup === "1着") {
    return "condition-analysis-finish-bee condition-analysis-finish-bee-win";
  }
  if (bee.finishGroup === "2着") {
    return "condition-analysis-finish-bee condition-analysis-finish-bee-place";
  }
  if (bee.finishGroup === "3着") {
    return "condition-analysis-finish-bee condition-analysis-finish-bee-show";
  }
  return "condition-analysis-finish-bee condition-analysis-finish-bee-out";
};

const drawFinishBees = ({
  bees,
  layer,
  scales,
}: DrawFinishBeesParams): Selection<SVGCircleElement, FinishBeePoint, SVGGElement, undefined> =>
  layer
    .selectAll<SVGCircleElement, FinishBeePoint>("circle")
    .data(bees)
    .join("circle")
    .attr("class", finishBeeClassName)
    .attr("data-chart-point", "finish-bee")
    .attr("data-finish-group", (bee) => bee.finishGroup)
    .attr("data-horse", (bee) => bee.horseName)
    .attr("data-outlier", (bee) => (bee.isOutlier ? "true" : "false"))
    .attr(
      "cx",
      (bee) =>
        bandCenter({ fallback: PLOT_LEFT, label: bee.finishGroup, scale: scales.xScale }) +
        payoutBeeJitter(bee.index),
    )
    .attr("cy", (bee) =>
      scaledValue({ fallback: PLOT_BOTTOM, scale: scales.yScale, value: bee.odds }),
    )
    .attr("r", (bee) => (bee.isOutlier ? OUTLIER_RADIUS : BEE_RADIUS));

const drawPayoutChart = ({
  frame,
  onHideTooltip,
  onShowTooltip,
  svg,
  view,
}: DrawPayoutChartParams): void => {
  const scales = createPayoutScales(view);
  const root = svgRoot(svg);
  root.selectAll("*").remove();
  if (scales === null) {
    return;
  }
  drawHorizontalGrid({
    fallback: PLOT_BOTTOM,
    layer: appendLayer(root, "condition-analysis-chart-y-grid"),
    ticks: scales.ticks,
    yScale: scales.yScale,
  });
  drawPayoutAxes(root, scales);
  drawPayoutRanges(appendLayer(root, "condition-analysis-chart-ranges"), view.ranges, scales);
  drawPayoutBoxes(appendLayer(root, "condition-analysis-chart-boxes"), view.boxes, scales);
  const bees = drawPayoutBees(
    appendLayer(root, "condition-analysis-chart-bees"),
    view.bees,
    scales,
  );
  bindMarkTooltip({
    frame,
    marks: bees,
    onHideTooltip,
    onShowTooltip,
    toTooltip: (bee) =>
      payoutTooltipContent({
        bee,
        box: payoutBoxForBetType(view.boxes, bee.betType),
      }),
  });
  root.on("pointerleave mouseleave", onHideTooltip);
};

const drawFinishOddsChart = ({
  frame,
  onHideTooltip,
  onShowTooltip,
  svg,
  view,
}: DrawFinishOddsChartParams): void => {
  const scales = createFinishOddsScales(view);
  const root = svgRoot(svg);
  root.selectAll("*").remove();
  if (scales === null) {
    return;
  }
  drawHorizontalGrid({
    fallback: PLOT_BOTTOM,
    layer: appendLayer(root, "condition-analysis-chart-y-grid"),
    ticks: scales.ticks,
    yScale: scales.yScale,
  });
  drawFinishOddsAxes(root, scales);
  drawPayoutBoxes(appendLayer(root, "condition-analysis-chart-boxes"), view.boxes, scales);
  const bees = drawFinishBees({
    bees: view.bees,
    layer: appendLayer(root, "condition-analysis-chart-bees"),
    scales,
  });
  bindMarkTooltip({
    frame,
    marks: bees,
    onHideTooltip,
    onShowTooltip,
    toTooltip: (bee) =>
      finishTooltipContent({
        bee,
        box: finishBoxForGroup(view.boxes, bee.finishGroup),
      }),
  });
  root.on("pointerleave mouseleave", onHideTooltip);
};

const ChartTooltip = ({ tooltip }: { tooltip: ChartTooltipView | null }) =>
  tooltip === null ? null : (
    <div
      className="condition-analysis-chart-tooltip"
      role="tooltip"
      style={{ left: `${tooltip.x}px`, top: `${tooltip.y}px` }}
    >
      <p className="condition-analysis-chart-tooltip-title">{tooltip.title}</p>
      {tooltip.lines.map((line) => (
        <p key={`${tooltip.title}-${line}`}>{line}</p>
      ))}
      {tooltip.meta === null ? null : (
        <p className="condition-analysis-chart-tooltip-meta">{tooltip.meta}</p>
      )}
    </div>
  );

export const PayoutTrendChart = ({ view }: PayoutTrendChartProps) => {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const frameRef = useRef<HTMLDivElement | null>(null);
  const [tooltip, setTooltip] = useState<ChartTooltipView | null>(null);
  const canDraw = hasPayoutChartData(view);
  useEffect(() => {
    const svgNode = svgRef.current;
    const frameNode = frameRef.current;
    if (canDraw && svgNode !== null && frameNode !== null) {
      drawPayoutChart({
        frame: frameNode,
        onHideTooltip: () => {
          setTooltip(null);
        },
        onShowTooltip: (nextTooltip) => {
          setTooltip(nextTooltip);
        },
        svg: svgNode,
        view,
      });
    }
    return () => {
      if (svgNode !== null) {
        select(svgNode).selectAll("*").remove();
        select(svgNode).on("pointerleave mouseleave", null);
      }
    };
  }, [canDraw, view]);
  if (!canDraw) {
    return <p className="empty-state">払い戻しのグラフに使えるデータがありません。</p>;
  }
  return (
    <figure aria-label="払い戻し傾向グラフ" className="condition-analysis-chart">
      <p className="condition-analysis-chart-note">{CONDITION_PAYOUT_CHART_NOTE}</p>
      <div className="condition-analysis-chart-frame" ref={frameRef}>
        <svg
          aria-label="払い戻し箱ひげ図"
          className="condition-analysis-chart-svg"
          ref={svgRef}
          viewBox={`0 0 ${VIEW_WIDTH} ${VIEW_HEIGHT}`}
        />
        <ChartTooltip tooltip={tooltip} />
      </div>
    </figure>
  );
};

export const FinishPopularityChart = ({ points }: FinishPopularityChartProps) => {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const frameRef = useRef<HTMLDivElement | null>(null);
  const [tooltip, setTooltip] = useState<ChartTooltipView | null>(null);
  const view = buildFinishOddsDistributionView(points);
  const canDraw = hasFinishOddsChartData(view);
  useEffect(() => {
    const svgNode = svgRef.current;
    const frameNode = frameRef.current;
    if (canDraw && svgNode !== null && frameNode !== null) {
      drawFinishOddsChart({
        frame: frameNode,
        onHideTooltip: () => {
          setTooltip(null);
        },
        onShowTooltip: (nextTooltip) => {
          setTooltip(nextTooltip);
        },
        svg: svgNode,
        view,
      });
    }
    return () => {
      if (svgNode !== null) {
        select(svgNode).selectAll("*").remove();
        select(svgNode).on("pointerleave mouseleave", null);
      }
    };
  }, [canDraw, view]);
  if (!canDraw) {
    return <p className="empty-state">着順別のグラフに使えるデータがありません。</p>;
  }
  return (
    <figure aria-label="着順別人気オッズグラフ" className="condition-analysis-chart">
      <p className="condition-analysis-chart-note">{CONDITION_FINISH_CHART_NOTE}</p>
      <div className="condition-analysis-chart-frame" ref={frameRef}>
        <svg
          aria-label="着順別単勝オッズ箱ひげ図"
          className="condition-analysis-chart-svg"
          ref={svgRef}
          viewBox={`0 0 ${VIEW_WIDTH} ${VIEW_HEIGHT}`}
        />
        <ChartTooltip tooltip={tooltip} />
      </div>
    </figure>
  );
};
