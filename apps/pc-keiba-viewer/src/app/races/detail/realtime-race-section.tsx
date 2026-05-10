"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import { useMemo, useState } from "react";
import {
  CartesianGrid,
  Legend,
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

const TREND_COLORS = [
  "#0f766e",
  "#b45309",
  "#2563eb",
  "#be123c",
  "#6d28d9",
  "#15803d",
  "#c2410c",
  "#0369a1",
  "#a21caf",
  "#4d7c0f",
  "#7c2d12",
  "#4338ca",
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

const runnerNameByNumber = (runners: Runner[]): Map<string, string> =>
  new Map(
    runners.map((runner) => [
      formatRunnerNumber(runner.umaban),
      cleanText(runner.bamei, formatRunnerNumber(runner.umaban)),
    ]),
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
  const [activeTrend, setActiveTrend] = useState<OddsTrendHoverState | null>(null);

  if (props.source !== "nar") {
    return null;
  }

  const history = useMemo(() => payload?.odds?.horseTrends ?? [], [payload]);
  const trendRows = useMemo(() => buildOddsTrendRows(history), [history]);
  const allTrendPoints = history.flatMap((trend) => trend.points);
  const oddsValues = allTrendPoints
    .map((point) => point.odds)
    .filter((value): value is number => typeof value === "number");
  const minOdds = oddsValues.length > 0 ? Math.min(...oddsValues) : 0;
  const maxOdds = oddsValues.length > 0 ? Math.max(...oddsValues) : 1;
  const oddsDomainPadding = Math.max((maxOdds - minOdds) * 0.08, 0.5);
  const getSeriesName = (horseNumber: string): string =>
    `${horseNumber} ${names.get(horseNumber) ?? ""}`.trim();
  const activeTrendEntries =
    activeTrend?.activePayload?.filter((entry) => typeof entry.value === "number") ?? [];
  const chartTopMargin = Math.min(260, Math.max(110, Math.ceil(history.length / 3) * 30 + 46));
  const chartHeight = Math.max(460, chartTopMargin + 320);

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
            <ResponsiveContainer height={chartHeight} width="100%">
              <LineChart
                data={trendRows}
                margin={{ bottom: 30, left: 8, right: 24, top: chartTopMargin }}
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
                  domain={[Math.max(0, minOdds - oddsDomainPadding), maxOdds + oddsDomainPadding]}
                  tick={{ fill: "#5a6a60", fontSize: 11 }}
                  tickCount={9}
                  tickFormatter={(value) => Number(value).toFixed(1)}
                  width={48}
                />
                <Tooltip
                  allowEscapeViewBox={{ x: false, y: true }}
                  content={<OddsTrendTooltip />}
                  cursor={{ stroke: "#6f8378", strokeDasharray: "4 4", strokeWidth: 1 }}
                  offset={18}
                  position={{ y: 8 }}
                  reverseDirection={{ x: true, y: false }}
                  wrapperStyle={{ zIndex: 5 }}
                />
                <Legend wrapperStyle={{ fontSize: 12, paddingTop: 10 }} />
                {history.map((trend, index) => (
                  <Line
                    connectNulls
                    dataKey={trend.horseNumber}
                    dot={{ r: 2 }}
                    key={trend.horseNumber}
                    name={getSeriesName(trend.horseNumber)}
                    stroke={TREND_COLORS[index % TREND_COLORS.length]}
                    strokeWidth={2}
                    type="monotone"
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}
      </div>
    </section>
  );
}
