"use client";

import { memo, useEffect, useMemo, useState, useSyncExternalStore } from "react";

import { indexLiveHorseWeightKg } from "../../../lib/horse-weight-class";
import { useHorseWeightStream } from "../../../lib/horse-weight-stream-client";
import type {
  BloodlineStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  Runner,
  SimilarRaceStatsRow,
  WeightClassStatsRow,
} from "../../../lib/race-types";
import {
  buildWinRateHeatmapColorScaleGradient,
  buildWinRateHeatmapDisplay,
  DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS,
  DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  formatWinRateHeatmapColorScaleAriaLabel,
  formatWinRateHeatmapColorScaleCaption,
  formatWinRateHeatmapColorScaleTick,
  getWinRateHeatmapColorScaleTracks,
  WIN_RATE_HEATMAP_COLOR_SCALE_TICKS,
  WIN_RATE_HEATMAP_VIEW_MODES,
  type WinRateHeatmapDisplaySwatch,
  type WinRateHeatmapRateMetric,
  type WinRateHeatmapViewMode,
} from "../../../lib/win-rate-heatmap";
import { FrameNumberBadge } from "./frame-number-badge";
import { useRealtimeRacePayload, type RealtimeRaceRequest } from "./realtime-client";

interface WinRateHeatmapSectionProps {
  bloodlineRows: BloodlineStatsRow[];
  carriedWeightClassStats?: readonly WeightClassStatsRow[];
  frameStats: FrameStatsRow[];
  horseResults: HorseRaceResult[];
  keibajoCode: string;
  realtimeRequest: RealtimeRaceRequest;
  runners: Runner[];
  similarRows: SimilarRaceStatsRow[];
  weightClassStats?: readonly WeightClassStatsRow[];
}

interface WinRateHeatmapSwatchProps {
  frameNumber: string;
  isLastRow: boolean;
  isOpen: boolean;
  onToggle: () => void;
  swatch: WinRateHeatmapDisplaySwatch;
}

interface WinRateHeatmapColorScaleProps {
  metrics: readonly WinRateHeatmapRateMetric[];
}

const WIN_RATE_HEATMAP_VIEW_RADIO_NAME = "win-rate-heatmap-view";
const HEATMAP_MOBILE_TOOLTIP_QUERY = "(max-width: 720px)";

const heatmapSwatchClassName = (input: { isLastRow: boolean; isOpen: boolean }): string => {
  if (input.isLastRow && input.isOpen) {
    return "win-rate-heatmap-swatch win-rate-heatmap-tooltip-above tooltip-open";
  }
  if (input.isLastRow) {
    return "win-rate-heatmap-swatch win-rate-heatmap-tooltip-above";
  }
  if (input.isOpen) {
    return "win-rate-heatmap-swatch tooltip-open";
  }
  return "win-rate-heatmap-swatch";
};

const subscribeHeatmapMobileTooltip = (onStoreChange: () => void): (() => void) => {
  if (typeof window === "undefined" || !window.matchMedia) {
    return () => {};
  }
  const mediaQuery = window.matchMedia(HEATMAP_MOBILE_TOOLTIP_QUERY);
  if (mediaQuery.addEventListener) {
    mediaQuery.addEventListener("change", onStoreChange);
  } else {
    mediaQuery.addListener(onStoreChange);
  }
  return () => {
    if (mediaQuery.removeEventListener) {
      mediaQuery.removeEventListener("change", onStoreChange);
    } else {
      mediaQuery.removeListener(onStoreChange);
    }
  };
};

const getHeatmapMobileTooltipSnapshot = (): boolean =>
  typeof window !== "undefined" &&
  Boolean(window.matchMedia) &&
  window.matchMedia(HEATMAP_MOBILE_TOOLTIP_QUERY).matches;

const getHeatmapMobileTooltipServerSnapshot = (): boolean => false;

const WinRateHeatmapColorScale = ({ metrics }: WinRateHeatmapColorScaleProps) => (
  <div className="win-rate-heatmap-color-scale-slot">
    <figure
      aria-label={formatWinRateHeatmapColorScaleAriaLabel(metrics)}
      className="win-rate-heatmap-color-scale"
    >
      <figcaption className="win-rate-heatmap-color-scale-heading">
        {formatWinRateHeatmapColorScaleCaption(metrics)}
      </figcaption>
      <div className="win-rate-heatmap-color-scale-tracks">
        {getWinRateHeatmapColorScaleTracks(metrics).map((metric) => (
          <div
            className="win-rate-heatmap-color-scale-track win-rate-heatmap-color-scale-track-solo"
            key={metric.key}
          >
            <div
              className="win-rate-heatmap-color-scale-bar"
              style={{ backgroundImage: buildWinRateHeatmapColorScaleGradient(metric.hue) }}
            />
          </div>
        ))}
      </div>
      <div className="win-rate-heatmap-color-scale-ticks">
        {WIN_RATE_HEATMAP_COLOR_SCALE_TICKS.map((rate) => (
          <span key={rate}>{formatWinRateHeatmapColorScaleTick(rate)}</span>
        ))}
      </div>
    </figure>
  </div>
);

const heatmapSwatchAriaLabel = (input: {
  frameNumber: string;
  swatch: WinRateHeatmapDisplaySwatch;
}): string => {
  const nameLabel =
    input.swatch.columnKey === "frame"
      ? `${input.swatch.columnLabel} ${input.frameNumber}`
      : `${input.swatch.columnLabel} ${input.swatch.name}`;
  return input.swatch.startsLabel === null ? nameLabel : `${nameLabel} ${input.swatch.startsLabel}`;
};

const WinRateHeatmapSwatch = ({
  frameNumber,
  isLastRow,
  isOpen,
  onToggle,
  swatch,
}: WinRateHeatmapSwatchProps) => (
  <td
    className={heatmapSwatchClassName({ isLastRow, isOpen })}
    style={{ backgroundColor: swatch.background }}
  >
    <button
      aria-label={heatmapSwatchAriaLabel({ frameNumber, swatch })}
      aria-expanded={isOpen}
      className="win-rate-heatmap-swatch-button"
      type="button"
      onClick={onToggle}
    >
      <span className="win-rate-heatmap-swatch-value">{swatch.valueLabel}</span>
      <span className="win-rate-heatmap-tooltip" role="tooltip">
        {swatch.columnKey === "frame" ? <FrameNumberBadge value={frameNumber} /> : swatch.name}
        {swatch.startsLabel === null ? null : (
          <span className="win-rate-heatmap-tooltip-starts">{swatch.startsLabel}</span>
        )}
      </span>
    </button>
  </td>
);

export const WinRateHeatmapSection = memo(function WinRateHeatmapSection({
  bloodlineRows,
  carriedWeightClassStats,
  frameStats,
  horseResults,
  keibajoCode,
  realtimeRequest,
  runners,
  similarRows,
  weightClassStats,
}: WinRateHeatmapSectionProps) {
  const [openTooltipKey, setOpenTooltipKey] = useState<string | null>(null);
  const [showStarts, setShowStarts] = useState(DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS);
  const [viewMode, setViewMode] = useState<WinRateHeatmapViewMode>(
    DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  );
  const isMobileTooltip = useSyncExternalStore(
    subscribeHeatmapMobileTooltip,
    getHeatmapMobileTooltipSnapshot,
    getHeatmapMobileTooltipServerSnapshot,
  );
  useEffect(() => {
    if (isMobileTooltip) {
      return undefined;
    }
    setOpenTooltipKey(null);
    return undefined;
  }, [isMobileTooltip]);
  useEffect(() => {
    if (!isMobileTooltip || openTooltipKey === null) {
      return undefined;
    }
    const closeTooltipOnOutsidePointer = (event: PointerEvent): void => {
      const target = event.target;
      if (target instanceof Element && target.closest(".win-rate-heatmap-swatch-button") !== null) {
        return;
      }
      setOpenTooltipKey(null);
    };
    document.addEventListener("pointerdown", closeTooltipOnOutsidePointer);
    return () => {
      document.removeEventListener("pointerdown", closeTooltipOnOutsidePointer);
    };
  }, [isMobileTooltip, openTooltipKey]);
  const { payload: realtimePayload } = useRealtimeRacePayload(realtimeRequest, null);
  const horseWeightSnapshot = useHorseWeightStream({
    day: realtimeRequest.day,
    initial: realtimePayload?.horseWeights ?? null,
    keibajoCode: realtimeRequest.keibajoCode,
    month: realtimeRequest.month,
    raceNumber: realtimeRequest.raceNumber,
    source: realtimeRequest.source,
    year: realtimeRequest.year,
  });
  const liveWeightKgByHorse = useMemo(
    () =>
      indexLiveHorseWeightKg(
        horseWeightSnapshot?.horses ?? realtimePayload?.horseWeights?.horses ?? [],
      ),
    [horseWeightSnapshot, realtimePayload],
  );
  const display = buildWinRateHeatmapDisplay({
    bloodlineRows,
    carriedWeightClassStats,
    frameStats,
    horseResults,
    keibajoCode,
    liveWeightKgByHorse,
    runners,
    showStarts,
    similarRows,
    viewMode,
    weightClassStats,
  });
  if (display.empty) {
    return <p className="empty-state">勝率ヒートマップを表示する出走馬がありません。</p>;
  }
  return (
    <div className="win-rate-heatmap-wrap">
      <fieldset aria-label="表示指標" className="win-rate-heatmap-view-toggle">
        {WIN_RATE_HEATMAP_VIEW_MODES.map((mode) => (
          <label className="running-style-bucket-toggle-label" key={mode.key}>
            <input
              checked={viewMode === mode.key}
              name={WIN_RATE_HEATMAP_VIEW_RADIO_NAME}
              type="radio"
              value={mode.key}
              onChange={() => {
                setViewMode(mode.key);
              }}
            />
            {mode.label}
          </label>
        ))}
        <label className="running-style-bucket-toggle-label">
          <input
            checked={showStarts}
            type="checkbox"
            onChange={() => {
              setShowStarts((current) => !current);
            }}
          />
          レース数
        </label>
      </fieldset>
      <WinRateHeatmapColorScale metrics={display.visibleRateMetrics} />
      <div className="stats-table-wrap win-rate-heatmap-table-wrap">
        <table className="stats-table win-rate-heatmap-table">
          <colgroup>
            <col className="win-rate-heatmap-col-number" />
          </colgroup>
          <thead>
            <tr>
              <th className="win-rate-heatmap-number" rowSpan={2} scope="col">
                番
              </th>
              {display.visibleColumns.map((column) => (
                <th key={column.key} colSpan={display.entityColSpan} scope="colgroup">
                  {column.label}
                </th>
              ))}
            </tr>
            <tr>
              {display.visibleColumns.map((column) =>
                display.visibleRateMetrics.map((metric) => (
                  <th
                    className="win-rate-heatmap-rate-heading"
                    key={`${column.key}-${metric.key}`}
                    scope="col"
                  >
                    {metric.shortLabel}
                  </th>
                )),
              )}
            </tr>
          </thead>
          <tbody>
            {display.rows.map((row, rowIndex) => (
              <tr key={row.horseNumber}>
                <th className="win-rate-heatmap-number" scope="row" title={row.horseName}>
                  {row.horseNumber}
                </th>
                {row.swatches.map((swatch) => {
                  const tooltipKey = `${row.horseNumber}-${swatch.columnKey}-${swatch.metricKey}`;
                  return (
                    <WinRateHeatmapSwatch
                      frameNumber={row.frameNumber}
                      isLastRow={rowIndex === display.rows.length - 1}
                      isOpen={isMobileTooltip && openTooltipKey === tooltipKey}
                      key={tooltipKey}
                      swatch={swatch}
                      onToggle={() => {
                        if (!isMobileTooltip) {
                          return;
                        }
                        setOpenTooltipKey((current) =>
                          current === tooltipKey ? null : tooltipKey,
                        );
                      }}
                    />
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
});
