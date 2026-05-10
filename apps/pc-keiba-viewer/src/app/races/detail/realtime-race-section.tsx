"use client";

import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import type { CSSProperties } from "react";
import { useEffect, useMemo, useState } from "react";

import { cleanText } from "../../../lib/format";
import type { Runner } from "../../../lib/race-types";
import { formatHorseWeight, formatRunnerNumber } from "../../../lib/runner-format";

type TrendStyle = CSSProperties & {
  "--trend-index": number;
};

interface RealtimeRaceSectionProps {
  apiBaseUrl: string;
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  runners: Runner[];
  source: string;
  year: string;
}

const POLL_INTERVAL_MS = 30_000;
const GRAPH_WIDTH = 720;
const GRAPH_HEIGHT = 180;
const GRAPH_PADDING_X = 36;
const GRAPH_PADDING_Y = 22;

const buildRealtimeUrl = ({
  apiBaseUrl,
  day,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: Omit<RealtimeRaceSectionProps, "runners">): string | null => {
  if (!apiBaseUrl || source !== "nar") {
    return null;
  }
  return `${apiBaseUrl.replace(/\/$/u, "")}/api/nar/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/realtime`;
};

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

const formatOdds = (value: number | undefined): string =>
  typeof value === "number" ? value.toFixed(1) : "-";

const isRealtimeRacePayload = (value: unknown): value is RealtimeRacePayload =>
  typeof value === "object" &&
  value !== null &&
  "raceKey" in value &&
  typeof value.raceKey === "string";

const getTrendStyle = (index: number): TrendStyle => ({
  "--trend-index": index,
});

const runnerNameByNumber = (runners: Runner[]): Map<string, string> =>
  new Map(
    runners.map((runner) => [
      formatRunnerNumber(runner.umaban),
      cleanText(runner.bamei, formatRunnerNumber(runner.umaban)),
    ]),
  );

const buildTrendPath = (
  points: { fetchedAt: string; popularity: number | null }[],
  minTime: number,
  maxTime: number,
  maxPopularity: number,
): string => {
  const drawableWidth = GRAPH_WIDTH - GRAPH_PADDING_X * 2;
  const drawableHeight = GRAPH_HEIGHT - GRAPH_PADDING_Y * 2;
  const timeRange = Math.max(maxTime - minTime, 1);
  return points
    .filter((point) => point.popularity !== null)
    .map((point, index) => {
      const time = new Date(point.fetchedAt).getTime();
      const x = GRAPH_PADDING_X + ((time - minTime) / timeRange) * drawableWidth;
      const y =
        GRAPH_PADDING_Y +
        ((Number(point.popularity) - 1) / Math.max(maxPopularity - 1, 1)) * drawableHeight;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
};

export function RealtimeRaceSection(props: RealtimeRaceSectionProps) {
  const realtimeUrl = buildRealtimeUrl(props);
  const [payload, setPayload] = useState<RealtimeRacePayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const names = useMemo(() => runnerNameByNumber(props.runners), [props.runners]);

  useEffect(() => {
    if (!realtimeUrl) {
      return undefined;
    }
    let cancelled = false;
    const load = async () => {
      try {
        const response = await fetch(realtimeUrl, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`realtime api ${response.status}`);
        }
        const data: unknown = await response.json();
        if (!isRealtimeRacePayload(data)) {
          throw new Error("invalid realtime payload");
        }
        if (!cancelled) {
          setPayload(data);
          setError(null);
        }
      } catch (caught) {
        if (!cancelled) {
          setError(caught instanceof Error ? caught.message : String(caught));
        }
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [realtimeUrl]);

  if (props.source !== "nar") {
    return null;
  }

  const tansho = payload?.odds?.latest.tansho ?? [];
  const history = payload?.odds?.horseTrends ?? [];
  const latestWeight = payload?.horseWeights;
  const allTrendPoints = history.flatMap((trend) => trend.points);
  const times = allTrendPoints
    .map((point) => new Date(point.fetchedAt).getTime())
    .filter((time) => Number.isFinite(time));
  const popularityValues = allTrendPoints
    .map((point) => point.popularity)
    .filter((value): value is number => typeof value === "number");
  const minTime = times.length > 0 ? Math.min(...times) : Date.now();
  const maxTime = times.length > 0 ? Math.max(...times) : Date.now();
  const maxPopularity = Math.max(...popularityValues, props.runners.length, 1);

  return (
    <section className="realtime-section">
      <div className="section-heading compact">
        <h2>リアルタイムデータ</h2>
        <span>
          {payload?.odds ? `最終取得 ${formatFetchedAt(payload.odds.fetchedAt)}` : "取得待ち"}
        </span>
      </div>
      {error ? <p className="empty-state">リアルタイムデータを取得できません: {error}</p> : null}
      <div className="realtime-grid">
        <div className="realtime-panel">
          <div className="section-heading compact">
            <h3>単勝オッズ</h3>
            <span>{tansho.length} 件</span>
          </div>
          {tansho.length === 0 ? (
            <p className="empty-state">単勝オッズはまだありません。</p>
          ) : (
            <div className="realtime-table-wrap">
              <table className="realtime-table">
                <thead>
                  <tr>
                    <th>馬番号</th>
                    <th>馬名</th>
                    <th>単勝</th>
                    <th>人気</th>
                  </tr>
                </thead>
                <tbody>
                  {tansho.map((row) => (
                    <tr key={row.combination}>
                      <td>{row.combination}</td>
                      <td>{names.get(row.combination) ?? "-"}</td>
                      <td>{formatOdds(row.odds)}</td>
                      <td>{row.rank ?? "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div className="realtime-panel">
          <div className="section-heading compact">
            <h3>馬体重</h3>
            <span>{latestWeight ? formatFetchedAt(latestWeight.fetchedAt) : "取得待ち"}</span>
          </div>
          {!latestWeight || latestWeight.horses.length === 0 ? (
            <p className="empty-state">馬体重はまだありません。</p>
          ) : (
            <div className="realtime-table-wrap">
              <table className="realtime-table">
                <thead>
                  <tr>
                    <th>馬番号</th>
                    <th>馬名</th>
                    <th>馬体重</th>
                  </tr>
                </thead>
                <tbody>
                  {latestWeight.horses.map((horse) => (
                    <tr key={horse.horseNumber}>
                      <td>{horse.horseNumber}</td>
                      <td>{names.get(horse.horseNumber) ?? horse.horseName ?? "-"}</td>
                      <td>
                        {formatHorseWeight(
                          horse.weight === null ? null : String(horse.weight),
                          horse.changeSign,
                          horse.changeAmount === null ? null : String(horse.changeAmount),
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      <div className="realtime-panel odds-trend-panel">
        <div className="section-heading compact">
          <h3>人気推移</h3>
          <span>馬番号別</span>
        </div>
        {history.length === 0 ? (
          <p className="empty-state">人気推移はまだありません。</p>
        ) : (
          <div className="odds-trend-chart" role="img" aria-label="馬番号ごとの人気推移">
            <svg viewBox={`0 0 ${GRAPH_WIDTH} ${GRAPH_HEIGHT}`} preserveAspectRatio="none">
              <line
                x1={GRAPH_PADDING_X}
                x2={GRAPH_PADDING_X}
                y1={GRAPH_PADDING_Y}
                y2={GRAPH_HEIGHT - GRAPH_PADDING_Y}
              />
              <line
                x1={GRAPH_PADDING_X}
                x2={GRAPH_WIDTH - GRAPH_PADDING_X}
                y1={GRAPH_HEIGHT - GRAPH_PADDING_Y}
                y2={GRAPH_HEIGHT - GRAPH_PADDING_Y}
              />
              {history.map((trend, index) => (
                <path
                  d={buildTrendPath(trend.points, minTime, maxTime, maxPopularity)}
                  key={trend.horseNumber}
                  style={getTrendStyle(index)}
                />
              ))}
            </svg>
            <div className="odds-trend-legend">
              {history.map((trend, index) => (
                <span key={trend.horseNumber} style={getTrendStyle(index)}>
                  {trend.horseNumber} {names.get(trend.horseNumber) ?? ""}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
