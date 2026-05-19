"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import { formatKeibajo, formatRaceNumber } from "../../../lib/format";
import type {
  RaceTrendDetail,
  RaceTrendPayload,
  RaceTrendRunningStyle,
  RaceTrendRunningStyleRow,
} from "../../../lib/race-types";
import { useRealtimeRaceSelector } from "./realtime-client";

const RACE_TREND_RETRY_OPTIONS = {
  baseDelayMs: 300,
  maxAttempts: 4,
  maxDelayMs: 4000,
} as const;

const TREND_TARGET_KEYS = ["runningStyle", "frame", "jockey"] as const;

type TrendTargetKey = (typeof TREND_TARGET_KEYS)[number];
type TrendTargets = Record<TrendTargetKey, boolean>;

interface RaceTrendSectionProps {
  day: string;
  defaultEndDate: string;
  defaultStartDate: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  source: RaceSource;
  year: string;
}

const DEFAULT_TREND_TARGETS: TrendTargets = {
  runningStyle: true,
  frame: true,
  jockey: true,
};

const TREND_TARGET_LABELS: Record<TrendTargetKey, string> = {
  runningStyle: "脚質",
  frame: "枠",
  jockey: "騎手",
};

const RUNNING_STYLE_LABELS: Record<RaceTrendRunningStyle, string> = {
  nige: "逃げ",
  senkou: "先行",
  sashi: "差し",
  oikomi: "追込",
};

const formatRate = (value: number): string => `${value.toFixed(1)}%`;

const formatMedian = (value: number | null | undefined): string => {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "-";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(1);
};

const formatTrendWinOdds = (value: number | null | undefined): string =>
  typeof value === "number" && Number.isFinite(value) ? value.toFixed(1) : "-";

const formatRunningStyle = (value: RaceTrendRunningStyle | null): string =>
  value ? RUNNING_STYLE_LABELS[value] : "-";

const normalizeHorseNumber = (value: string | null | undefined): string => {
  const parsed = Number(value ?? "");
  return Number.isFinite(parsed) && parsed > 0 ? String(parsed) : "";
};

const countDistinctTrendRaces = (rows: RaceTrendRunningStyleRow[]): number =>
  new Set(
    rows.flatMap((row) =>
      row.details.map((detail) =>
        [detail.source, detail.date, detail.keibajoCode, detail.raceNumber].join(":"),
      ),
    ),
  ).size;

const isRaceTrendPayload = (value: unknown): value is RaceTrendPayload =>
  typeof value === "object" &&
  value !== null &&
  "runningStyleRows" in value &&
  Array.isArray((value as { runningStyleRows?: unknown }).runningStyleRows);

const sortRowsByShowRate = (rows: RaceTrendRunningStyleRow[]): RaceTrendRunningStyleRow[] =>
  rows.toSorted(
    (a, b) =>
      b.showRate - a.showRate ||
      b.quinellaRate - a.quinellaRate ||
      b.winRate - a.winRate ||
      b.starts - a.starts ||
      (a.targetHorseNumbers[0] ?? "").localeCompare(b.targetHorseNumbers[0] ?? "", "ja", {
        numeric: true,
      }) ||
      (a.frameNumber ?? "").localeCompare(b.frameNumber ?? "", "ja", { numeric: true }) ||
      (a.jockeyName ?? "").localeCompare(b.jockeyName ?? "", "ja"),
  );

const sortDetailsByLatestRace = (details: RaceTrendDetail[]): RaceTrendDetail[] =>
  details.toSorted((a, b) => {
    const dateOrder = b.date.localeCompare(a.date);
    if (dateOrder !== 0) {
      return dateOrder;
    }
    const raceOrder = b.raceNumber.localeCompare(a.raceNumber, "ja", { numeric: true });
    if (raceOrder !== 0) {
      return raceOrder;
    }
    return (a.horseNumber ?? "").localeCompare(b.horseNumber ?? "", "ja", { numeric: true });
  });

const getApiPath = ({
  day,
  defaultEndDate,
  defaultStartDate,
  jockeySameVenue,
  keibajoCode,
  month,
  raceNumber,
  source,
  trendEnd,
  trendStart,
  trendTargets,
  year,
}: RaceTrendSectionProps & {
  jockeySameVenue: boolean;
  trendEnd: string;
  trendStart: string;
  trendTargets: TrendTargets;
}): string => {
  const params = new URLSearchParams({
    source,
    jockeyStart: trendStart || defaultStartDate,
    jockeyEnd: trendEnd || defaultEndDate,
    frameStart: trendStart || defaultStartDate,
    frameEnd: trendEnd || defaultEndDate,
    includeRealtimeResults: "false",
    jockeySameVenue: String(jockeySameVenue),
    runningStyleIgnoreRunningStyle: String(!trendTargets.runningStyle),
    runningStyleIgnoreFrame: String(!trendTargets.frame),
    runningStyleIgnoreJockey: String(!trendTargets.jockey),
  });
  return `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/trends?${params.toString()}`;
};

const TrendHeaderLabel = ({ children, secondLine }: { children: string; secondLine?: string }) => (
  <span className={secondLine ? "race-trend-header-label two-line" : "race-trend-header-label"}>
    <span>{children}</span>
    {secondLine ? <span>{secondLine}</span> : null}
  </span>
);

function RaceTrendTable({
  isLoading,
  rows,
  trendTargets,
}: {
  isLoading: boolean;
  rows: RaceTrendRunningStyleRow[];
  trendTargets: TrendTargets;
}) {
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const realtimePayload = useRealtimeRaceSelector((state) => state.payload);
  const realtimeOddsByHorse = useMemo(
    () =>
      new Map(
        (realtimePayload?.odds?.latest.tansho ?? []).map((row) => [
          normalizeHorseNumber(row.combination),
          { popularity: row.rank ?? null, winOdds: row.odds ?? null },
        ]),
      ),
    [realtimePayload],
  );
  const sortedRows = useMemo(() => sortRowsByShowRate(rows), [rows]);
  const raceCount = useMemo(() => countDistinctTrendRaces(rows), [rows]);
  const colSpan =
    8 +
    (trendTargets.frame ? 1 : 0) +
    (trendTargets.runningStyle ? 1 : 0) +
    (trendTargets.jockey ? 1 : 0);

  useEffect(() => {
    setExpandedKey(null);
  }, [rows]);

  return (
    <div className="race-trend-table-panel">
      <div className="race-trend-subheading">
        <h3>脚質・枠・騎手ごとの勝率</h3>
        <span>集計 {raceCount}レース / 複勝率順</span>
      </div>
      <div className="stats-table-wrap">
        <table className="stats-table race-trend-table aggregate">
          <colgroup>
            <col className="race-trend-col-horse-number" />
            {trendTargets.frame ? <col className="race-trend-col-frame" /> : null}
            {trendTargets.runningStyle ? <col className="race-trend-col-running-style" /> : null}
            {trendTargets.jockey ? <col className="race-trend-col-jockey" /> : null}
            <col className="race-trend-col-rate" />
            <col className="race-trend-col-rate" />
            <col className="race-trend-col-rate" />
            <col className="race-trend-col-market" />
            <col className="race-trend-col-market" />
            <col className="race-trend-col-count" />
            <col className="race-trend-col-median" />
          </colgroup>
          <thead>
            <tr>
              <th>
                <TrendHeaderLabel>馬番</TrendHeaderLabel>
              </th>
              {trendTargets.frame ? (
                <th>
                  <TrendHeaderLabel>枠</TrendHeaderLabel>
                </th>
              ) : null}
              {trendTargets.runningStyle ? (
                <th>
                  <TrendHeaderLabel>脚質</TrendHeaderLabel>
                </th>
              ) : null}
              {trendTargets.jockey ? (
                <th>
                  <TrendHeaderLabel>騎手</TrendHeaderLabel>
                </th>
              ) : null}
              <th>
                <TrendHeaderLabel>複勝率</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>連対率</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>勝率</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>人気</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>単勝</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>出走回数</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel secondLine="中央値">着順</TrendHeaderLabel>
              </th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 5 }, (_, index) => (
                <tr className="race-trend-skeleton-row" key={`race-trend-skeleton-${index}`}>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-count" />
                  </td>
                  {trendTargets.frame ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-count" />
                    </td>
                  ) : null}
                  {trendTargets.runningStyle ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-count" />
                    </td>
                  ) : null}
                  {trendTargets.jockey ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-name" />
                    </td>
                  ) : null}
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-rate" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-rate" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-rate" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-count" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-rate" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-count" />
                  </td>
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-rate" />
                  </td>
                </tr>
              ))
            ) : sortedRows.length > 0 ? (
              sortedRows.map((row) => {
                const isExpanded = expandedKey === row.key;
                return (
                  <RowFragment
                    isExpanded={isExpanded}
                    key={row.key}
                    row={row}
                    realtimeOdds={realtimeOddsByHorse.get(
                      normalizeHorseNumber(row.targetHorseNumbers[0]),
                    )}
                    trendTargets={trendTargets}
                    onToggle={() => setExpandedKey(isExpanded ? null : row.key)}
                  />
                );
              })
            ) : (
              <tr>
                <td className="race-trend-empty-cell" colSpan={colSpan}>
                  該当する集計成績はありません
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function RowFragment({
  isExpanded,
  realtimeOdds,
  row,
  trendTargets,
  onToggle,
}: {
  isExpanded: boolean;
  realtimeOdds?: { popularity: number | null; winOdds: number | null };
  row: RaceTrendRunningStyleRow;
  trendTargets: TrendTargets;
  onToggle: () => void;
}) {
  const colSpan =
    8 +
    (trendTargets.frame ? 1 : 0) +
    (trendTargets.runningStyle ? 1 : 0) +
    (trendTargets.jockey ? 1 : 0);
  const detailRows = useMemo(() => sortDetailsByLatestRace(row.details), [row.details]);

  return (
    <>
      <tr className={isExpanded ? "stats-row-expanded" : undefined}>
        <td className="race-trend-horse-number-cell">
          <button
            aria-expanded={isExpanded}
            className="stats-detail-toggle race-trend-detail-toggle"
            onClick={onToggle}
            title={
              trendTargets.runningStyle ? `${formatRunningStyle(row.runningStyle)}の詳細` : "詳細"
            }
            type="button"
          >
            <span>{row.targetHorseNumbers.join(",") || "-"}</span>
          </button>
        </td>
        {trendTargets.frame ? <td>{row.frameNumber ?? "-"}</td> : null}
        {trendTargets.runningStyle ? <td>{formatRunningStyle(row.runningStyle)}</td> : null}
        {trendTargets.jockey ? <td className="stats-name-cell">{row.jockeyName ?? "-"}</td> : null}
        <td>{formatRate(row.showRate)}</td>
        <td>{formatRate(row.quinellaRate)}</td>
        <td>{formatRate(row.winRate)}</td>
        <td>{formatMedian(realtimeOdds?.popularity)}</td>
        <td>{formatTrendWinOdds(realtimeOdds?.winOdds)}</td>
        <td>{row.starts}</td>
        <td>{formatMedian(row.finishPositionMedian)}</td>
      </tr>
      {isExpanded ? (
        <tr className="stats-detail-row">
          <td colSpan={colSpan}>
            <div className="stats-detail-panel">
              <table className="stats-detail-table race-trend-detail-table aggregate">
                <colgroup>
                  <col className="race-trend-detail-col-date" />
                  <col className="race-trend-detail-col-venue" />
                  <col className="race-trend-detail-col-race-number" />
                  <col className="race-trend-detail-col-horse-number" />
                  <col className="race-trend-detail-col-frame" />
                  <col className="race-trend-detail-col-running-style" />
                  <col className="race-trend-detail-col-jockey" />
                  <col className="race-trend-detail-col-finish" />
                  <col className="race-trend-detail-col-popularity" />
                  <col className="race-trend-detail-col-odds" />
                  <col className="race-trend-detail-col-horse-name" />
                  <col className="race-trend-detail-col-race-name" />
                </colgroup>
                <thead>
                  <tr>
                    <th>日付</th>
                    <th>場</th>
                    <th>R</th>
                    <th>馬番</th>
                    <th>枠</th>
                    <th>脚質</th>
                    <th>騎手</th>
                    <th>着順</th>
                    <th>人気</th>
                    <th>単勝</th>
                    <th>馬名</th>
                    <th>レース名</th>
                  </tr>
                </thead>
                <tbody>
                  {detailRows.map((detail) => (
                    <tr
                      key={`${detail.source}:${detail.date}:${detail.keibajoCode}:${detail.raceNumber}:${detail.horseNumber}:${row.key}`}
                    >
                      <td>{detail.date}</td>
                      <td>{formatKeibajo(detail.keibajoCode)}</td>
                      <td>{formatRaceNumber(detail.raceNumber)}</td>
                      <td>{detail.horseNumber ?? "-"}</td>
                      <td>{detail.frameNumber ?? "-"}</td>
                      <td>{formatRunningStyle(detail.runningStyle)}</td>
                      <td>{detail.jockeyName ?? "-"}</td>
                      <td>{detail.finishPosition}</td>
                      <td>{formatMedian(detail.popularity)}</td>
                      <td>{formatTrendWinOdds(detail.winOdds)}</td>
                      <td className="race-trend-detail-horse-name">{detail.horseName ?? "-"}</td>
                      <td className="race-trend-detail-race-name">{detail.raceName ?? "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </td>
        </tr>
      ) : null}
    </>
  );
}

export function RaceTrendSection({
  day,
  defaultEndDate,
  defaultStartDate,
  keibajoCode,
  month,
  raceNumber,
  source,
  year,
}: RaceTrendSectionProps) {
  const [jockeySameVenue, setJockeySameVenue] = useState(true);
  const [trendStart, setTrendStart] = useState(defaultStartDate);
  const [trendEnd, setTrendEnd] = useState(defaultEndDate);
  const [trendTargets, setTrendTargets] = useState<TrendTargets>(DEFAULT_TREND_TARGETS);
  const [rows, setRows] = useState<RaceTrendRunningStyleRow[]>([]);
  const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");

  const toggleTrendTarget = useCallback((key: TrendTargetKey) => {
    setTrendTargets((current) => ({
      ...current,
      [key]: !current[key],
    }));
  }, []);

  const fetchTrendRows = useCallback(
    async (signal?: AbortSignal) => {
      setStatus("loading");
      try {
        const response = await fetchWithRetry(
          getApiPath({
            day,
            defaultEndDate,
            defaultStartDate,
            jockeySameVenue,
            keibajoCode,
            month,
            raceNumber,
            source,
            trendEnd,
            trendStart,
            trendTargets,
            year,
          }),
          { cache: "no-store", signal },
          RACE_TREND_RETRY_OPTIONS,
        );
        if (!response.ok) {
          throw new Error(`race trend api ${response.status}`);
        }
        const body: unknown = await response.json();
        if (!isRaceTrendPayload(body)) {
          throw new Error("invalid race trend payload");
        }
        setRows(body.runningStyleRows);
        setStatus("idle");
      } catch {
        if (signal?.aborted) {
          return;
        }
        setRows([]);
        setStatus("error");
      }
    },
    [
      day,
      defaultEndDate,
      defaultStartDate,
      jockeySameVenue,
      keibajoCode,
      month,
      raceNumber,
      source,
      trendEnd,
      trendStart,
      trendTargets,
      year,
    ],
  );

  useEffect(() => {
    const controller = new AbortController();
    void fetchTrendRows(controller.signal);
    return () => {
      controller.abort();
    };
  }, [fetchTrendRows]);

  return (
    <section className="race-trend-section">
      <div className="section-heading compact">
        <h2>レース傾向</h2>
        <span>
          {formatKeibajo(keibajoCode)} {source === "jra" ? "中央競馬" : "地方競馬"}
        </span>
      </div>

      <div className="race-trend-card">
        <div className="race-trend-controls">
          <label>
            <span>開始日</span>
            <input
              type="date"
              value={trendStart}
              onChange={(event) => setTrendStart(event.target.value)}
            />
          </label>
          <label>
            <span>終了日</span>
            <input
              type="date"
              value={trendEnd}
              onChange={(event) => setTrendEnd(event.target.value)}
            />
          </label>
          <label className="race-trend-checkbox">
            <input
              checked={jockeySameVenue}
              onChange={(event) => setJockeySameVenue(event.target.checked)}
              type="checkbox"
            />
            <span>同じ競馬場のみ</span>
          </label>
        </div>

        <div className="combined-score-targets race-trend-targets" aria-label="集計条件">
          <fieldset>
            <legend>集計条件</legend>
            {TREND_TARGET_KEYS.map((key) => (
              <label key={key}>
                <input
                  checked={trendTargets[key]}
                  type="checkbox"
                  onChange={() => toggleTrendTarget(key)}
                />
                <span>{TREND_TARGET_LABELS[key]}</span>
              </label>
            ))}
          </fieldset>
        </div>

        <RaceTrendTable isLoading={status === "loading"} rows={rows} trendTargets={trendTargets} />
      </div>

      {status === "error" ? (
        <p className="race-trend-error">レース傾向を取得できませんでした。</p>
      ) : null}
    </section>
  );
}
