"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";
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

type OddsTrendHoverEntry = {
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

const buildOddsTrendRows = (
  history: NonNullable<RealtimeRacePayload["odds"]>["horseTrends"],
): OddsTrendRow[] => {
  const rows = new Map<string, OddsTrendRow>();
  for (const trend of history) {
    for (const point of trend.points) {
      const row =
        rows.get(point.fetchedAt) ??
        ({
          fetchedAt: point.fetchedAt,
          timeLabel: formatFetchedAt(point.fetchedAt),
        } satisfies OddsTrendRow);
      row[trend.horseNumber] = point.odds;
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

function OddsTrendTooltip({ active, label, payload }: OddsTrendTooltipProps) {
  const entries = (payload ?? [])
    .filter((entry) => typeof entry.value === "number")
    .toSorted((left, right) => getTooltipSortValue(left) - getTooltipSortValue(right));

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
  const isMobileTooltip = useSyncExternalStore(
    subscribeMobileTooltip,
    getMobileTooltipSnapshot,
    getMobileTooltipServerSnapshot,
  );

  if (props.source !== "nar") {
    return null;
  }

  const history = useMemo(() => payload?.odds?.horseTrends ?? [], [payload]);
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
  const getSeriesName = (horseNumber: string): string =>
    `${horseNumber} ${names.get(horseNumber) ?? ""}`.trim();
  const getSeriesColor = (horseNumber: string): string =>
    getFrameColor(frames.get(horseNumber)) ?? FALLBACK_TREND_COLOR;
  const activeTrendEntries =
    activeTrend?.activePayload?.filter((entry) => typeof entry.value === "number") ?? [];
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
          <span>馬番号別 / 横軸 時間 / 縦軸 単勝オッズ</span>
        </div>
        {history.length === 0 || trendRows.length === 0 ? (
          <p className="empty-state">人気推移はまだありません。</p>
        ) : (
          <div className="odds-trend-chart" role="img" aria-label="馬番号ごとの単勝オッズ推移">
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
              <ResponsiveContainer height="100%" width="100%">
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
                  {history.map((trend) => (
                    <Line
                      connectNulls
                      dataKey={trend.horseNumber}
                      dot={{ r: 2 }}
                      key={trend.horseNumber}
                      name={getSeriesName(trend.horseNumber)}
                      stroke={getSeriesColor(trend.horseNumber)}
                      strokeWidth={2.4}
                      type="monotone"
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div className="odds-trend-legend" aria-label="オッズ推移の線の説明">
              {history.map((trend) => (
                <span
                  className="odds-trend-legend-item"
                  key={trend.horseNumber}
                  style={getLegendStyle(getSeriesColor(trend.horseNumber))}
                >
                  <span
                    className="odds-trend-legend-marker"
                    style={{ backgroundColor: getSeriesColor(trend.horseNumber) }}
                  />
                  <PlainHorseNumberBadge horseNumber={trend.horseNumber} />
                  <span className="odds-trend-legend-name">
                    {names.get(trend.horseNumber) ?? trend.horseNumber}
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
