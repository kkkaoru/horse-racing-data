"use client";

import {
  memo,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";

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
  loadHeatmapShowStartsForCurrentUser,
  persistHeatmapShowStartsForCurrentUser,
} from "../../../lib/user-preferences-indexeddb";
import {
  buildWinRateHeatmapColorScaleGradient,
  buildWinRateHeatmapDisplay,
  DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS,
  DEFAULT_WIN_RATE_HEATMAP_VIEW_MODE,
  formatWinRateHeatmapColorScaleAriaLabel,
  formatWinRateHeatmapColorScaleCaption,
  formatWinRateHeatmapColorScaleTick,
  getWinRateHeatmapColorScaleTracks,
  type WinRateHeatmapColorScales,
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
  isLastColumn: boolean;
  isOpen: boolean;
  onToggle: () => void;
  swatch: WinRateHeatmapDisplaySwatch;
  tooltipAbove: boolean;
  viewMode: WinRateHeatmapViewMode;
}

interface WinRateHeatmapColorScaleProps {
  metrics: readonly WinRateHeatmapRateMetric[];
  scales: WinRateHeatmapColorScales;
}

const WIN_RATE_HEATMAP_VIEW_RADIO_NAME = "win-rate-heatmap-view";
const HEATMAP_MOBILE_TOOLTIP_QUERY = "(max-width: 720px)";
const HEATMAP_TOOLTIP_ABOVE_ROW_COUNT: number = 2;

const heatmapSwatchClassName = (input: {
  isLastColumn: boolean;
  isOpen: boolean;
  tooltipAbove: boolean;
}): string => {
  const classNames = ["win-rate-heatmap-swatch"];
  if (input.tooltipAbove) {
    classNames.push("win-rate-heatmap-tooltip-above");
  }
  if (input.isLastColumn) {
    classNames.push("win-rate-heatmap-tooltip-left");
  }
  if (input.isOpen) {
    classNames.push("tooltip-open");
  }
  return classNames.join(" ");
};

const heatmapTableClassName = (viewMode: WinRateHeatmapViewMode): string =>
  viewMode === "all"
    ? "stats-table win-rate-heatmap-table win-rate-heatmap-table-combined"
    : "stats-table win-rate-heatmap-table";

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

const isHeatmapTooltipAboveRow = (rowIndex: number, rowCount: number): boolean =>
  rowIndex >= rowCount - HEATMAP_TOOLTIP_ABOVE_ROW_COUNT;

const WinRateHeatmapColorScale = ({ metrics, scales }: WinRateHeatmapColorScaleProps) => {
  const tracks = getWinRateHeatmapColorScaleTracks(metrics, scales);
  const firstTrack = tracks[0];
  const sharedScale =
    tracks.length === 1 && firstTrack !== undefined ? scales[firstTrack.key] : null;
  return (
    <div className="win-rate-heatmap-color-scale-slot">
      <figure
        aria-label={formatWinRateHeatmapColorScaleAriaLabel(metrics, scales)}
        className={
          tracks.length > 1
            ? "win-rate-heatmap-color-scale win-rate-heatmap-color-scale-stacked"
            : "win-rate-heatmap-color-scale"
        }
      >
        <figcaption className="win-rate-heatmap-color-scale-heading">
          {formatWinRateHeatmapColorScaleCaption(metrics)}
        </figcaption>
        <div className="win-rate-heatmap-color-scale-tracks">
          {tracks.map((metric) => {
            const scale = scales[metric.key];
            return (
              <div
                className={
                  tracks.length === 1
                    ? "win-rate-heatmap-color-scale-track win-rate-heatmap-color-scale-track-solo"
                    : "win-rate-heatmap-color-scale-track"
                }
                key={metric.key}
              >
                {tracks.length === 1 ? null : (
                  <span className="win-rate-heatmap-color-scale-track-label">
                    {metric.shortLabel}
                  </span>
                )}
                <div className="win-rate-heatmap-color-scale-track-body">
                  <div
                    className="win-rate-heatmap-color-scale-bar"
                    style={{
                      backgroundImage: buildWinRateHeatmapColorScaleGradient({
                        hue: metric.hue,
                        maxRate: scale.maxRate,
                        minRate: scale.minRate,
                        ticks: scale.ticks,
                      }),
                    }}
                  />
                  {tracks.length === 1 ? null : (
                    <div className="win-rate-heatmap-color-scale-ticks">
                      {scale.ticks.map((rate) => (
                        <span key={rate}>
                          {formatWinRateHeatmapColorScaleTick(rate, scale.maxRate)}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
        {sharedScale === null ? null : (
          <div className="win-rate-heatmap-color-scale-ticks">
            {sharedScale.ticks.map((rate) => (
              <span key={rate}>
                {formatWinRateHeatmapColorScaleTick(rate, sharedScale.maxRate)}
              </span>
            ))}
          </div>
        )}
      </figure>
    </div>
  );
};

const heatmapSwatchValueClassName = (isZero: boolean): string =>
  isZero
    ? "win-rate-heatmap-swatch-value win-rate-heatmap-swatch-zero"
    : "win-rate-heatmap-swatch-value";

const heatmapSwatchStartsClassName = (isZero: boolean): string =>
  isZero
    ? "win-rate-heatmap-swatch-starts win-rate-heatmap-swatch-zero"
    : "win-rate-heatmap-swatch-starts";

// Combined 勝率+連対率+複勝率 stacks three rates in a narrow cell.
// Never append "%" there: it overflows 4-digit starts and dense rates.
// Single-metric modes keep the suffix so the unit stays visible on desktop.
const heatmapSwatchValueSuffix = (
  viewMode: WinRateHeatmapViewMode,
  valueLabel: string,
): string | null => (viewMode === "all" || valueLabel === "-" ? null : "%");

const heatmapSwatchNameLabel = (input: {
  frameNumber: string;
  swatch: WinRateHeatmapDisplaySwatch;
}): string => {
  if (input.swatch.columnKey === "jockeyFrame") {
    return `${input.swatch.columnLabel} ${input.frameNumber} ${input.swatch.name}`;
  }
  if (input.swatch.columnKey === "frame") {
    return `${input.swatch.columnLabel} ${input.frameNumber}`;
  }
  return `${input.swatch.columnLabel} ${input.swatch.name}`;
};

const heatmapSwatchAriaLabel = (input: {
  frameNumber: string;
  swatch: WinRateHeatmapDisplaySwatch;
}): string => {
  const nameLabel = heatmapSwatchNameLabel(input);
  return input.swatch.startsLabel === null ? nameLabel : `${nameLabel} ${input.swatch.startsLabel}`;
};

const heatmapTooltipNameContent = (input: {
  frameNumber: string;
  swatch: WinRateHeatmapDisplaySwatch;
}): ReactNode => {
  if (input.swatch.columnKey === "frame") {
    return <FrameNumberBadge value={input.frameNumber} />;
  }
  if (input.swatch.columnKey === "jockeyFrame") {
    return (
      <>
        <FrameNumberBadge value={input.frameNumber} />
        {input.swatch.name}
      </>
    );
  }
  return input.swatch.name;
};

const WinRateHeatmapSwatch = ({
  frameNumber,
  isLastColumn,
  isOpen,
  onToggle,
  swatch,
  tooltipAbove,
  viewMode,
}: WinRateHeatmapSwatchProps) => {
  const valueSuffix = heatmapSwatchValueSuffix(viewMode, swatch.valueLabel);
  return (
    <td
      className={heatmapSwatchClassName({ isLastColumn, isOpen, tooltipAbove })}
      style={{ backgroundColor: swatch.background, color: swatch.foreground }}
    >
      <button
        aria-label={heatmapSwatchAriaLabel({ frameNumber, swatch })}
        aria-expanded={isOpen}
        className="win-rate-heatmap-swatch-button"
        type="button"
        onClick={onToggle}
      >
        <span className={heatmapSwatchValueClassName(swatch.isZeroValue)}>
          {swatch.valueLabel}
          {valueSuffix === null ? null : (
            <span className="win-rate-heatmap-swatch-value-suffix">{valueSuffix}</span>
          )}
        </span>
        {swatch.graphStartsLabel === null ? null : (
          <span className={heatmapSwatchStartsClassName(swatch.isZeroGraphStarts)}>
            {swatch.graphStartsLabel}
          </span>
        )}
        <span className="win-rate-heatmap-tooltip" role="tooltip">
          {heatmapTooltipNameContent({ frameNumber, swatch })}
          {swatch.startsLabel === null ? null : (
            <span className="win-rate-heatmap-tooltip-starts">{swatch.startsLabel}</span>
          )}
        </span>
      </button>
    </td>
  );
};

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
  const hasEditedShowStarts = useRef(false);
  useEffect(() => {
    const loadState = { cancelled: false };
    void loadHeatmapShowStartsForCurrentUser()
      .then((stored) => {
        if (loadState.cancelled || hasEditedShowStarts.current) {
          return undefined;
        }
        setShowStarts(stored);
        return undefined;
      })
      .catch(() => undefined);
    return () => {
      loadState.cancelled = true;
    };
  }, []);
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
  const lastVisibleColumnKey =
    display.visibleColumns[display.visibleColumns.length - 1]?.key ?? null;
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
              const nextShowStarts = !showStarts;
              hasEditedShowStarts.current = true;
              setShowStarts(nextShowStarts);
              void persistHeatmapShowStartsForCurrentUser(nextShowStarts).catch(() => undefined);
            }}
          />
          レース数
        </label>
      </fieldset>
      <WinRateHeatmapColorScale metrics={display.visibleRateMetrics} scales={display.colorScales} />
      <div className="stats-table-wrap win-rate-heatmap-table-wrap">
        <table className={heatmapTableClassName(display.viewMode)}>
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
                      isLastColumn={swatch.columnKey === lastVisibleColumnKey}
                      isOpen={isMobileTooltip && openTooltipKey === tooltipKey}
                      tooltipAbove={isHeatmapTooltipAboveRow(rowIndex, display.rows.length)}
                      key={tooltipKey}
                      swatch={swatch}
                      viewMode={display.viewMode}
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
