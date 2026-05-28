"use client";

import type {
  RealtimeOddsTrend,
  RealtimeOddsType,
  RealtimeRacePayload,
} from "horse-racing-realtime/types";
import type { CSSProperties } from "react";
import { useMemo, useState, useSyncExternalStore } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { cleanText } from "../../../lib/format";
import type { Runner } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import { getFrameColor, PlainHorseNumberBadge } from "./frame-number-badge";
import { useRealtimeRacePayload } from "./realtime-client";

interface RealtimeRaceSectionProps {
  apiBaseUrl: string;
  day: string;
  initialPayload: RealtimeRacePayload | null;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  runners: Runner[];
  source: string;
  year: string;
}

const FALLBACK_TREND_COLOR = "#6f8378";
const LOW_ODDS_TICK_MAX = 10;
const LOW_ODDS_TICK_STEP = 0.5;
const MID_ODDS_TICK_MAX = 20;
const MID_ODDS_TICK_STEP = 1;
const HIGH_ODDS_TICK_STEP = 5;
const MOBILE_TOOLTIP_QUERY = "(max-width: 760px)";
const NON_TANSHO_TREND_LIMIT = 20;
const ODDS_TYPE_ORDER: RealtimeOddsType[] = [
  "tansho",
  "fukusho",
  "wakuren",
  "umaren",
  "wide",
  "umatan",
  "3renpuku",
  "3rentan",
];
const ODDS_TYPE_LABELS: Record<RealtimeOddsType, string> = {
  "3renpuku": "3連複",
  "3rentan": "3連単",
  fukusho: "複勝",
  tansho: "単勝",
  umaren: "馬連",
  umatan: "馬単",
  wakuren: "枠連",
  wide: "ワイド",
};
const COMBINATION_TREND_COLORS = [
  "#2f6f4e",
  "#8f4f24",
  "#355f9f",
  "#8d3d71",
  "#5f6f2f",
  "#a23d3d",
  "#2f7f82",
  "#6b55a3",
  "#a0802f",
  "#3f5d66",
];

const formatFetchedAt = (value: string): string => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("ja-JP", {
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    month: "2-digit",
    second: "2-digit",
  }).format(date);
};

const subscribeMobileTooltip = (onStoreChange: () => void): (() => void) => {
  if (typeof window === "undefined" || !window.matchMedia) {
    return () => {};
  }

  const mediaQuery = window.matchMedia(MOBILE_TOOLTIP_QUERY);
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

const getMobileTooltipSnapshot = (): boolean =>
  typeof window !== "undefined" &&
  Boolean(window.matchMedia) &&
  window.matchMedia(MOBILE_TOOLTIP_QUERY).matches;

const getMobileTooltipServerSnapshot = (): boolean => false;

const runnerNameByNumber = (runners: Runner[]): Map<string, string> =>
  new Map(
    runners.map((runner) => [
      formatRunnerNumber(runner.umaban),
      cleanText(runner.bamei, formatRunnerNumber(runner.umaban)),
    ]),
  );

const frameNumberByHorseNumber = (runners: Runner[]): Map<string, string> =>
  new Map(
    runners.map((runner) => [formatRunnerNumber(runner.umaban), cleanText(runner.wakuban, "")]),
  );

type OddsTrendRow = {
  fetchedAt: string;
  timeLabel: string;
} & Record<string, number | string | null>;

export type OddsTrendHoverEntry = {
  color?: string;
  dataKey?: string | number;
  name?: string | number;
  value?: number | string | null;
};

type OddsTrendHoverState = {
  activeLabel?: string | number;
  activePayload?: OddsTrendHoverEntry[];
  isTooltipActive?: boolean;
};

type OddsTrendTooltipProps = {
  active?: boolean;
  label?: string | number;
  payload?: OddsTrendHoverEntry[];
};

type OddsTrendLegendStyle = CSSProperties & {
  "--series-color": string;
};

const buildOddsTrendRows = (history: RealtimeOddsTrend[]): OddsTrendRow[] => {
  const rows = new Map<string, OddsTrendRow>();
  for (const trend of history) {
    for (const point of trend.points) {
      const row =
        rows.get(point.fetchedAt) ??
        ({
          fetchedAt: point.fetchedAt,
          timeLabel: formatFetchedAt(point.fetchedAt),
        } satisfies OddsTrendRow);
      row[trend.combination] = point.odds;
      rows.set(point.fetchedAt, row);
    }
  }
  return Array.from(rows.values()).toSorted(
    (left, right) => new Date(left.fetchedAt).getTime() - new Date(right.fetchedAt).getTime(),
  );
};

const getTooltipSortValue = (entry: OddsTrendHoverEntry): number => {
  const key = entry.dataKey ?? entry.name;
  const value = Number(String(key).match(/\d+/)?.[0] ?? Number.MAX_SAFE_INTEGER);
  return Number.isFinite(value) ? value : Number.MAX_SAFE_INTEGER;
};

const buildOddsYAxisTicks = (maxOdds: number): number[] => {
  const yAxisMax = Math.max(
    MID_ODDS_TICK_MAX,
    Math.ceil(maxOdds / HIGH_ODDS_TICK_STEP) * HIGH_ODDS_TICK_STEP,
  );
  const lowTicks = Array.from(
    { length: Math.floor(LOW_ODDS_TICK_MAX / LOW_ODDS_TICK_STEP) + 1 },
    (_, index) => Number((index * LOW_ODDS_TICK_STEP).toFixed(2)),
  );
  const midTicks = Array.from(
    { length: Math.floor((MID_ODDS_TICK_MAX - LOW_ODDS_TICK_MAX) / MID_ODDS_TICK_STEP) },
    (_, index) => LOW_ODDS_TICK_MAX + (index + 1) * MID_ODDS_TICK_STEP,
  );
  const highTicks: number[] = [];
  for (
    let value = MID_ODDS_TICK_MAX + HIGH_ODDS_TICK_STEP;
    value <= yAxisMax;
    value += HIGH_ODDS_TICK_STEP
  ) {
    highTicks.push(value);
  }
  return [...lowTicks, ...midTicks, ...highTicks];
};

const getLegendStyle = (color: string): OddsTrendLegendStyle => ({ "--series-color": color });

const toCombinationTrends = (
  trends: NonNullable<RealtimeRacePayload["odds"]>["horseTrends"],
): RealtimeOddsTrend[] =>
  trends.map((trend) => ({
    combination: trend.horseNumber,
    points: trend.points.map((point) => ({
      combination: trend.horseNumber,
      fetchedAt: point.fetchedAt,
      odds: point.odds,
      rank: point.popularity,
    })),
  }));

const hasTrendPoints = (trend: RealtimeOddsTrend): boolean =>
  trend.points.some((point) => typeof point.odds === "number");

const getLatestTrendOdds = (trend: RealtimeOddsTrend): number | null => {
  let latestTime = Number.NEGATIVE_INFINITY;
  let latestOdds: number | null = null;
  for (const point of trend.points) {
    if (typeof point.odds !== "number") {
      continue;
    }
    const time = new Date(point.fetchedAt).getTime();
    if (Number.isFinite(time) && time >= latestTime) {
      latestTime = time;
      latestOdds = point.odds;
    }
  }
  return latestOdds;
};

const compareTrendsByLatestOddsAsc = (
  left: RealtimeOddsTrend,
  right: RealtimeOddsTrend,
): number => {
  const leftOdds = getLatestTrendOdds(left) ?? Number.POSITIVE_INFINITY;
  const rightOdds = getLatestTrendOdds(right) ?? Number.POSITIVE_INFINITY;
  if (leftOdds !== rightOdds) {
    return leftOdds - rightOdds;
  }
  return left.combination.localeCompare(right.combination, "ja-JP", {
    numeric: true,
    sensitivity: "base",
  });
};

const compareTrendsByLatestOddsDesc = (
  left: RealtimeOddsTrend,
  right: RealtimeOddsTrend,
): number => {
  const leftOdds = getLatestTrendOdds(left) ?? Number.NEGATIVE_INFINITY;
  const rightOdds = getLatestTrendOdds(right) ?? Number.NEGATIVE_INFINITY;
  if (leftOdds !== rightOdds) {
    return rightOdds - leftOdds;
  }
  return left.combination.localeCompare(right.combination, "ja-JP", {
    numeric: true,
    sensitivity: "base",
  });
};

// Non-tansho trends keep "top N most-bet" (lowest odds) trimming, then we
// reorder the rendered list so the highest odds appear first to match the
// tooltip ordering the user expects.
export const getDisplayTrends = (
  oddsType: RealtimeOddsType,
  history: RealtimeOddsTrend[],
): RealtimeOddsTrend[] =>
  oddsType === "tansho" || oddsType === "fukusho"
    ? history.filter(hasTrendPoints).toSorted(compareTrendsByLatestOddsDesc)
    : history
        .filter(hasTrendPoints)
        .toSorted(compareTrendsByLatestOddsAsc)
        .slice(0, NON_TANSHO_TREND_LIMIT)
        .toSorted(compareTrendsByLatestOddsDesc);

const formatLegendOdds = (value: number | null): string =>
  value === null ? "--" : value >= 100 ? value.toFixed(0) : value.toFixed(1);

export const sortOddsTrendEntries = (entries: OddsTrendHoverEntry[]): OddsTrendHoverEntry[] =>
  entries
    .filter((entry) => typeof entry.value === "number")
    .toSorted((left, right) => {
      const leftOdds = Number(left.value);
      const rightOdds = Number(right.value);
      if (leftOdds !== rightOdds) {
        return rightOdds - leftOdds;
      }
      return getTooltipSortValue(left) - getTooltipSortValue(right);
    });

function OddsTrendTooltip({ active, label, payload }: OddsTrendTooltipProps) {
  const entries = sortOddsTrendEntries(payload ?? []);

  if (!active || entries.length === 0) {
    return null;
  }

  return (
    <div className="odds-trend-tooltip">
      <div className="odds-trend-tooltip-label">取得時刻 {label}</div>
      <div className="odds-trend-tooltip-grid">
        {entries.map((entry) => (
          <div className="odds-trend-tooltip-item" key={String(entry.dataKey ?? entry.name)}>
            <span
              className="odds-trend-tooltip-marker"
              style={{ backgroundColor: entry.color ?? "#6f8378" }}
            />
            <span className="odds-trend-tooltip-name">{entry.name}</span>
            <strong>{Number(entry.value).toFixed(1)}</strong>
          </div>
        ))}
      </div>
    </div>
  );
}

export function RealtimeRaceSection(props: RealtimeRaceSectionProps) {
  const { error, payload } = useRealtimeRacePayload(props, props.initialPayload);
  const names = useMemo(() => runnerNameByNumber(props.runners), [props.runners]);
  const frames = useMemo(() => frameNumberByHorseNumber(props.runners), [props.runners]);
  const [activeTrend, setActiveTrend] = useState<OddsTrendHoverState | null>(null);
  const [activeOddsType, setActiveOddsType] = useState<RealtimeOddsType>("tansho");
  const isMobileTooltip = useSyncExternalStore(
    subscribeMobileTooltip,
    getMobileTooltipSnapshot,
    getMobileTooltipServerSnapshot,
  );

  if (props.source !== "nar" && props.source !== "jra") {
    return null;
  }

  const trendsByType = useMemo(() => {
    const typedTrends = payload?.odds?.trendsByType ?? {};
    return {
      ...typedTrends,
      tansho:
        typedTrends.tansho && typedTrends.tansho.length > 0
          ? typedTrends.tansho
          : toCombinationTrends(payload?.odds?.horseTrends ?? []),
    } satisfies Partial<Record<RealtimeOddsType, RealtimeOddsTrend[]>>;
  }, [payload]);
  const availableOddsTypes = useMemo(
    () =>
      ODDS_TYPE_ORDER.filter((oddsType) =>
        (trendsByType[oddsType] ?? []).some((trend) => hasTrendPoints(trend)),
      ),
    [trendsByType],
  );
  const displayOddsType = availableOddsTypes.includes(activeOddsType)
    ? activeOddsType
    : (availableOddsTypes[0] ?? "tansho");
  const rawHistory = useMemo(
    () => trendsByType[displayOddsType] ?? [],
    [displayOddsType, trendsByType],
  );
  const history = useMemo(
    () => getDisplayTrends(displayOddsType, rawHistory),
    [displayOddsType, rawHistory],
  );
  const trendRows = useMemo(() => buildOddsTrendRows(history), [history]);
  const allTrendPoints = history.flatMap((trend) => trend.points);
  const oddsValues = allTrendPoints
    .map((point) => point.odds)
    .filter((value): value is number => typeof value === "number");
  const maxOdds = oddsValues.length > 0 ? Math.max(...oddsValues) : 1;
  const minOdds = oddsValues.length > 0 ? Math.min(...oddsValues) : 0;
  const oddsDomainPadding = Math.max((maxOdds - minOdds) * 0.08, 0.5);
  const oddsYAxisMax = Math.max(
    MID_ODDS_TICK_MAX,
    Math.ceil((maxOdds + oddsDomainPadding) / HIGH_ODDS_TICK_STEP) * HIGH_ODDS_TICK_STEP,
  );
  const oddsYAxisTicks = useMemo(
    () => buildOddsYAxisTicks(maxOdds + oddsDomainPadding),
    [maxOdds, oddsDomainPadding],
  );
  const isHorseNumberOddsType = displayOddsType === "tansho" || displayOddsType === "fukusho";
  const getSeriesName = (combination: string): string =>
    isHorseNumberOddsType ? `${combination} ${names.get(combination) ?? ""}`.trim() : combination;
  const isWhiteFrameSeries = (combination: string): boolean =>
    isHorseNumberOddsType && frames.get(combination) === "1";
  const getSeriesColor = (combination: string, index: number): string =>
    (isHorseNumberOddsType ? getFrameColor(frames.get(combination)) : null) ??
    COMBINATION_TREND_COLORS[index % COMBINATION_TREND_COLORS.length] ??
    FALLBACK_TREND_COLOR;
  const getSeriesStrokeWidth = (combination: string): number =>
    isWhiteFrameSeries(combination) ? 3.2 : 2.4;
  const activeTrendEntries = sortOddsTrendEntries(activeTrend?.activePayload ?? []);
  const chartTopMargin = Math.min(260, Math.max(96, Math.ceil(history.length / 4) * 26 + 40));

  return (
    <section className="realtime-section">
      <div className="section-heading compact">
        <h2>リアルタイムデータ</h2>
        <span>
          {payload?.odds ? `最終取得 ${formatFetchedAt(payload.odds.fetchedAt)}` : "取得待ち"}
        </span>
      </div>
      {error ? <p className="empty-state">リアルタイムデータを取得できません: {error}</p> : null}

      <div className="realtime-panel odds-trend-panel">
        <div className="section-heading compact">
          <h3>オッズ推移</h3>
          <span>
            {ODDS_TYPE_LABELS[displayOddsType]} / 横軸 時間 / 縦軸 オッズ
            {isHorseNumberOddsType ? "" : " / 最新上位を表示"}
          </span>
        </div>
        {availableOddsTypes.length > 1 ? (
          <div className="odds-trend-tabs" role="tablist" aria-label="馬券種類">
            {availableOddsTypes.map((oddsType) => (
              <button
                aria-selected={oddsType === displayOddsType}
                className="odds-trend-tab"
                key={oddsType}
                onClick={() => {
                  setActiveOddsType(oddsType);
                  setActiveTrend(null);
                }}
                role="tab"
                type="button"
              >
                {ODDS_TYPE_LABELS[oddsType]}
              </button>
            ))}
          </div>
        ) : null}
        {history.length === 0 || trendRows.length === 0 ? (
          <p className="empty-state">オッズ推移はまだありません。</p>
        ) : (
          <div
            className="odds-trend-chart"
            role="img"
            aria-label={`${ODDS_TYPE_LABELS[displayOddsType]}のオッズ推移`}
          >
            <div className="odds-trend-hover-panel" aria-live="polite">
              {activeTrend && activeTrendEntries.length > 0 ? (
                <>
                  <span className="odds-trend-hover-time">取得時刻 {activeTrend.activeLabel}</span>
                  <span className="odds-trend-hover-values">
                    {activeTrendEntries.map((entry) => (
                      <span className="odds-trend-hover-value" key={String(entry.name)}>
                        <span>{entry.name}</span>
                        <strong>{Number(entry.value).toFixed(1)}</strong>
                      </span>
                    ))}
                  </span>
                </>
              ) : (
                <span className="odds-trend-hover-placeholder">グラフにカーソルを合わせて表示</span>
              )}
            </div>
            <div className="odds-trend-plot">
              <ResponsiveContainer
                height="100%"
                initialDimension={{ height: 1, width: 1 }}
                width="100%"
              >
                <LineChart
                  data={trendRows}
                  margin={{ bottom: 42, left: 4, right: 18, top: chartTopMargin }}
                  onMouseLeave={() => setActiveTrend(null)}
                  onMouseMove={(state: OddsTrendHoverState) => {
                    if (state.isTooltipActive) {
                      setActiveTrend(state);
                      return;
                    }
                    setActiveTrend(null);
                  }}
                >
                  <CartesianGrid stroke="#d8e0da" strokeDasharray="3 3" />
                  <XAxis
                    dataKey="timeLabel"
                    interval="preserveStartEnd"
                    minTickGap={20}
                    tick={{ fill: "#5a6a60", fontSize: 11 }}
                    tickMargin={10}
                  />
                  <YAxis
                    allowDecimals
                    domain={[0, oddsYAxisMax]}
                    scale="sqrt"
                    tick={{ fill: "#5a6a60", fontSize: 11 }}
                    tickFormatter={(value) => Number(value).toFixed(2)}
                    ticks={oddsYAxisTicks}
                    width={62}
                  />
                  <Tooltip
                    allowEscapeViewBox={{ x: true, y: true }}
                    content={<OddsTrendTooltip />}
                    cursor={{ stroke: "#6f8378", strokeDasharray: "4 4", strokeWidth: 1 }}
                    offset={18}
                    position={isMobileTooltip ? { x: 12, y: 12 } : undefined}
                    reverseDirection={isMobileTooltip ? undefined : { x: true, y: false }}
                    wrapperStyle={{ maxWidth: "calc(100vw - 32px)", zIndex: 5 }}
                  />
                  {history.map((trend, index) => (
                    <Line
                      connectNulls
                      dataKey={trend.combination}
                      dot={{ r: 2 }}
                      key={trend.combination}
                      name={getSeriesName(trend.combination)}
                      stroke={getSeriesColor(trend.combination, index)}
                      strokeWidth={getSeriesStrokeWidth(trend.combination)}
                      type="monotone"
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div className="odds-trend-legend" aria-label="オッズ推移の線の説明">
              {history.map((trend, index) => (
                <span
                  className="odds-trend-legend-item"
                  key={trend.combination}
                  style={getLegendStyle(getSeriesColor(trend.combination, index))}
                >
                  <span
                    className="odds-trend-legend-marker"
                    style={{
                      backgroundColor: getSeriesColor(trend.combination, index),
                    }}
                  />
                  {isHorseNumberOddsType ? (
                    <PlainHorseNumberBadge horseNumber={trend.combination} />
                  ) : (
                    <span className="odds-trend-combination-badge">{trend.combination}</span>
                  )}
                  <span className="odds-trend-legend-name">
                    {isHorseNumberOddsType
                      ? (names.get(trend.combination) ?? trend.combination)
                      : ODDS_TYPE_LABELS[displayOddsType]}
                  </span>
                  <span className="odds-trend-legend-odds" aria-label="現在オッズ">
                    {formatLegendOdds(getLatestTrendOdds(trend))}
                  </span>
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
