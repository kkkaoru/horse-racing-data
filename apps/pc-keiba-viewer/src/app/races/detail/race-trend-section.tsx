"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import { formatKeibajo, formatRaceNumber } from "../../../lib/format";
import type {
  RaceTrendPayload,
  RaceTrendRateRow,
  RaceTrendRunningStyle,
  RaceTrendRunningStyleRow,
} from "../../../lib/race-types";

const RACE_TREND_RETRY_OPTIONS = {
  baseDelayMs: 300,
  maxAttempts: 4,
  maxDelayMs: 4000,
} as const;

type SortKey = "showRate" | "quinellaRate" | "winRate";
type TrendTableKind = "frame" | "jockey";

const RUNNING_STYLE_LABELS: Record<RaceTrendRunningStyle, string> = {
  nige: "逃げ",
  senkou: "先行",
  sashi: "差し",
  oikomi: "追い込み",
};

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

const SORT_LABELS: Record<SortKey, string> = {
  showRate: "複勝率",
  quinellaRate: "連対率",
  winRate: "勝率",
};

const formatRate = (value: number): string => `${value.toFixed(1)}%`;

const formatMedian = (value: number | null | undefined): string => {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "-";
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(1);
};

const formatTrendPopularity = (value: number | null | undefined): string =>
  typeof value === "number" && Number.isFinite(value) ? String(value) : "-";

const formatTrendWinOdds = (value: number | null | undefined): string =>
  typeof value === "number" && Number.isFinite(value) ? value.toFixed(1) : "-";

const countDistinctTrendRaces = (rows: RaceTrendRateRow[]): number =>
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
  "jockeyRows" in value &&
  "frameRows" in value &&
  "runningStyleRows" in value;

const sortRows = (rows: RaceTrendRateRow[], sortKey: SortKey): RaceTrendRateRow[] =>
  rows.toSorted((a, b) => {
    const selectedOrder = b[sortKey] - a[sortKey];
    if (selectedOrder !== 0) {
      return selectedOrder;
    }
    const showOrder = b.showRate - a.showRate;
    if (showOrder !== 0) {
      return showOrder;
    }
    return b.starts - a.starts || a.label.localeCompare(b.label, "ja", { numeric: true });
  });

const getApiPath = ({
  day,
  frameEnd,
  frameStart,
  jockeyEnd,
  jockeySameVenue,
  jockeyStart,
  keibajoCode,
  month,
  raceNumber,
  runningStyleIgnoreFrame = false,
  runningStyleIgnoreJockey = false,
  source,
  year,
}: RaceTrendSectionProps & {
  frameEnd: string;
  frameStart: string;
  jockeyEnd: string;
  jockeySameVenue: boolean;
  jockeyStart: string;
  runningStyleIgnoreFrame?: boolean;
  runningStyleIgnoreJockey?: boolean;
}): string => {
  const params = new URLSearchParams({
    source,
    jockeyStart,
    jockeyEnd,
    frameStart,
    frameEnd,
    jockeySameVenue: String(jockeySameVenue),
    runningStyleIgnoreFrame: String(runningStyleIgnoreFrame),
    runningStyleIgnoreJockey: String(runningStyleIgnoreJockey),
  });
  return `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/trends?${params.toString()}`;
};

const TrendHeaderLabel = ({ children, secondLine }: { children: string; secondLine?: string }) => (
  <span className={secondLine ? "race-trend-header-label two-line" : "race-trend-header-label"}>
    <span>{children}</span>
    {secondLine ? <span>{secondLine}</span> : null}
  </span>
);

function TrendTable({
  emptyLabel,
  isLoading,
  kind,
  labelColumn,
  rows,
  showStarts,
  showTargetHorseNumber,
  sortKey,
  title,
  onSortChange,
}: {
  emptyLabel: string;
  isLoading: boolean;
  kind: TrendTableKind;
  labelColumn: string;
  rows: RaceTrendRateRow[];
  showStarts?: boolean;
  showTargetHorseNumber?: boolean;
  sortKey: SortKey;
  title: string;
  onSortChange: (sortKey: SortKey) => void;
}) {
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const sortedRows = useMemo(() => sortRows(rows, sortKey), [rows, sortKey]);
  const showTargetBeforeLabel = showTargetHorseNumber && kind === "jockey";
  const showTargetAfterLabel = showTargetHorseNumber && kind === "frame";
  const showFinishPositionMedian = kind === "frame" || kind === "jockey";
  const showTargetMarket = kind === "jockey";
  const colSpan =
    (showStarts ? 5 : 4) +
    (showTargetHorseNumber ? 1 : 0) +
    (showFinishPositionMedian ? 1 : 0) +
    (showTargetMarket ? 2 : 0);
  const raceCount = useMemo(() => countDistinctTrendRaces(rows), [rows]);

  return (
    <div className="race-trend-table-panel">
      <div className="race-trend-subheading">
        <h3>{title}</h3>
        <span>
          {SORT_LABELS[sortKey]}順{kind === "frame" ? ` / 集計 ${raceCount}レース` : ""}
        </span>
      </div>
      <div className="stats-table-wrap">
        <table className={`stats-table race-trend-table ${kind}`}>
          <thead>
            <tr>
              {showTargetBeforeLabel ? (
                <th>
                  <TrendHeaderLabel>馬番</TrendHeaderLabel>
                </th>
              ) : null}
              <th>
                <TrendHeaderLabel>{labelColumn}</TrendHeaderLabel>
              </th>
              {showTargetAfterLabel ? (
                <th>
                  <TrendHeaderLabel>馬番</TrendHeaderLabel>
                </th>
              ) : null}
              {showTargetMarket ? (
                <>
                  <th>
                    <TrendHeaderLabel>人気</TrendHeaderLabel>
                  </th>
                  <th>
                    <TrendHeaderLabel>単勝</TrendHeaderLabel>
                  </th>
                </>
              ) : null}
              {(["showRate", "quinellaRate", "winRate"] as const).map((key) => (
                <th key={key}>
                  <button
                    aria-pressed={sortKey === key}
                    className="race-trend-sort-button"
                    onClick={() => onSortChange(key)}
                    type="button"
                  >
                    <TrendHeaderLabel>{SORT_LABELS[key]}</TrendHeaderLabel>
                  </button>
                </th>
              ))}
              {showStarts ? (
                <th>
                  <TrendHeaderLabel>出走回数</TrendHeaderLabel>
                </th>
              ) : null}
              {showFinishPositionMedian ? (
                <th>
                  <TrendHeaderLabel secondLine="中央値">着順</TrendHeaderLabel>
                </th>
              ) : null}
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              Array.from({ length: 5 }, (_, index) => (
                <tr className="race-trend-skeleton-row" key={`race-trend-skeleton-${index}`}>
                  {showTargetBeforeLabel ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-count" />
                    </td>
                  ) : null}
                  <td>
                    <span className="race-trend-skeleton race-trend-skeleton-name" />
                  </td>
                  {showTargetAfterLabel ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-count" />
                    </td>
                  ) : null}
                  {showTargetMarket ? (
                    <>
                      <td>
                        <span className="race-trend-skeleton race-trend-skeleton-count" />
                      </td>
                      <td>
                        <span className="race-trend-skeleton race-trend-skeleton-rate" />
                      </td>
                    </>
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
                  {showStarts ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-count" />
                    </td>
                  ) : null}
                  {showFinishPositionMedian ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-rate" />
                    </td>
                  ) : null}
                </tr>
              ))
            ) : sortedRows.length > 0 ? (
              sortedRows.map((row) => {
                const isExpanded = expandedKey === row.key;
                return (
                  <FragmentRow
                    isExpanded={isExpanded}
                    key={row.key}
                    kind={kind}
                    labelColumn={labelColumn}
                    row={row}
                    showStarts={showStarts}
                    showTargetHorseNumber={showTargetHorseNumber}
                    onToggle={() => setExpandedKey(isExpanded ? null : row.key)}
                  />
                );
              })
            ) : (
              <tr>
                <td className="race-trend-empty-cell" colSpan={colSpan}>
                  {emptyLabel}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function FragmentRow({
  isExpanded,
  kind,
  labelColumn,
  row,
  showStarts,
  showTargetHorseNumber,
  onToggle,
}: {
  isExpanded: boolean;
  kind: TrendTableKind;
  labelColumn: string;
  row: RaceTrendRateRow;
  showStarts?: boolean;
  showTargetHorseNumber?: boolean;
  onToggle: () => void;
}) {
  const showTargetBeforeLabel = showTargetHorseNumber && kind === "jockey";
  const showTargetAfterLabel = showTargetHorseNumber && kind === "frame";
  const showFinishPositionMedian = kind === "frame" || kind === "jockey";
  const showTargetMarket = kind === "jockey";
  const colSpan =
    (showStarts ? 5 : 4) +
    (showTargetHorseNumber ? 1 : 0) +
    (showFinishPositionMedian ? 1 : 0) +
    (showTargetMarket ? 2 : 0);

  return (
    <>
      <tr className={isExpanded ? "stats-row-expanded" : undefined}>
        {showTargetBeforeLabel ? (
          <td className="race-trend-horse-number-cell">{row.targetHorseNumber ?? "-"}</td>
        ) : null}
        <td className="stats-name-cell">
          <button
            aria-expanded={isExpanded}
            className="stats-detail-toggle"
            onClick={onToggle}
            type="button"
          >
            <span>{row.label}</span>
          </button>
        </td>
        {showTargetAfterLabel ? (
          <td className="race-trend-horse-number-cell">{row.targetHorseNumber ?? "-"}</td>
        ) : null}
        {showTargetMarket ? (
          <>
            <td>{formatTrendPopularity(row.targetPopularity)}</td>
            <td>{formatTrendWinOdds(row.targetWinOdds)}</td>
          </>
        ) : null}
        <td>{formatRate(row.showRate)}</td>
        <td>{formatRate(row.quinellaRate)}</td>
        <td>{formatRate(row.winRate)}</td>
        {showStarts ? <td>{row.starts}</td> : null}
        {showFinishPositionMedian ? <td>{formatMedian(row.finishPositionMedian)}</td> : null}
      </tr>
      {isExpanded ? (
        <tr className="stats-detail-row">
          <td colSpan={colSpan}>
            <div className="stats-detail-panel">
              <table className={`stats-detail-table race-trend-detail-table ${kind}`}>
                <colgroup>
                  <col className="race-trend-detail-col-date" />
                  <col className="race-trend-detail-col-venue" />
                  <col className="race-trend-detail-col-race-number" />
                  <col className="race-trend-detail-col-race-name" />
                  <col className="race-trend-detail-col-finish" />
                  <col className="race-trend-detail-col-popularity" />
                  <col className="race-trend-detail-col-odds" />
                  <col className="race-trend-detail-col-horse-number" />
                  <col className="race-trend-detail-col-frame" />
                  <col className="race-trend-detail-col-jockey" />
                </colgroup>
                <thead>
                  <tr>
                    <th>日付</th>
                    <th>場</th>
                    <th>R</th>
                    <th>レース名</th>
                    <th>着順</th>
                    <th>人気</th>
                    <th>単勝</th>
                    <th>馬番</th>
                    <th>枠</th>
                    <th>騎手</th>
                  </tr>
                </thead>
                <tbody>
                  {row.details.map((detail) => (
                    <tr
                      key={`${detail.date}:${detail.keibajoCode}:${detail.raceNumber}:${detail.horseNumber}:${labelColumn}`}
                    >
                      <td>{detail.date}</td>
                      <td>{formatKeibajo(detail.keibajoCode)}</td>
                      <td>{formatRaceNumber(detail.raceNumber)}</td>
                      <td className="race-trend-detail-race-name">{detail.raceName ?? "-"}</td>
                      <td>{detail.finishPosition}</td>
                      <td>{formatTrendPopularity(detail.popularity)}</td>
                      <td>{formatTrendWinOdds(detail.winOdds)}</td>
                      <td>{detail.horseNumber ?? "-"}</td>
                      <td>{detail.frameNumber ? detail.frameNumber : "-"}</td>
                      <td>{detail.jockeyName ?? "-"}</td>
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

function RunningStyleTrendTable({
  ignoreFrame,
  ignoreJockey,
  isLoading,
  rows,
}: {
  ignoreFrame: boolean;
  ignoreJockey: boolean;
  isLoading: boolean;
  rows: RaceTrendRunningStyleRow[];
}) {
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const colSpan = 6 + (ignoreFrame ? 0 : 1) + (ignoreJockey ? 0 : 1);
  return (
    <div className="race-trend-table-panel">
      <div className="race-trend-subheading">
        <h3>脚質・枠・騎手ごとの着順傾向</h3>
        <span>着順中央値順</span>
      </div>
      <div className="stats-table-wrap">
        <table className="stats-table race-trend-table running-style">
          <thead>
            <tr>
              <th>
                <TrendHeaderLabel>該当馬番</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>脚質</TrendHeaderLabel>
              </th>
              {ignoreFrame ? null : (
                <th>
                  <TrendHeaderLabel>枠</TrendHeaderLabel>
                </th>
              )}
              {ignoreJockey ? null : (
                <th>
                  <TrendHeaderLabel>騎手</TrendHeaderLabel>
                </th>
              )}
              <th>
                <TrendHeaderLabel>着順</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>人気</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel>単勝</TrendHeaderLabel>
              </th>
              <th>
                <TrendHeaderLabel secondLine="中央値">着順</TrendHeaderLabel>
              </th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              <tr className="race-trend-skeleton-row">
                <td colSpan={colSpan}>
                  <span className="race-trend-skeleton race-trend-skeleton-name" />
                </td>
              </tr>
            ) : rows.length > 0 ? (
              rows.map((row) => {
                const isExpanded = expandedKey === row.key;
                return (
                  <RunningStyleTrendRow
                    ignoreFrame={ignoreFrame}
                    ignoreJockey={ignoreJockey}
                    isExpanded={isExpanded}
                    key={row.key}
                    row={row}
                    onToggle={() => setExpandedKey(isExpanded ? null : row.key)}
                  />
                );
              })
            ) : (
              <tr>
                <td className="race-trend-empty-cell" colSpan={colSpan}>
                  該当する脚質成績はありません
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function RunningStyleTrendRow({
  ignoreFrame,
  ignoreJockey,
  isExpanded,
  row,
  onToggle,
}: {
  ignoreFrame: boolean;
  ignoreJockey: boolean;
  isExpanded: boolean;
  row: RaceTrendRunningStyleRow;
  onToggle: () => void;
}) {
  const colSpan = 6 + (ignoreFrame ? 0 : 1) + (ignoreJockey ? 0 : 1);
  return (
    <>
      <tr className={isExpanded ? "stats-row-expanded" : undefined}>
        <td className="race-trend-horse-number-cell">
          <button
            aria-expanded={isExpanded}
            className="stats-detail-toggle"
            onClick={onToggle}
            type="button"
          >
            <span>{row.targetHorseNumbers.join(",") || "-"}</span>
          </button>
        </td>
        <td>{RUNNING_STYLE_LABELS[row.runningStyle]}</td>
        {ignoreFrame ? null : <td>{row.frameNumber ?? "-"}</td>}
        {ignoreJockey ? null : <td>{row.jockeyName ?? "-"}</td>}
        <td>{formatMedian(row.finishPositionAverage)}</td>
        <td>{formatTrendPopularity(row.popularityMedian)}</td>
        <td>{formatTrendWinOdds(row.winOddsMedian)}</td>
        <td>{formatMedian(row.finishPositionMedian)}</td>
      </tr>
      {isExpanded ? (
        <tr className="stats-detail-row">
          <td colSpan={colSpan}>
            <div className="stats-detail-panel">
              <table className="stats-detail-table race-trend-detail-table running-style">
                <colgroup>
                  <col className="race-trend-detail-col-date" />
                  <col className="race-trend-detail-col-venue" />
                  <col className="race-trend-detail-col-race-number" />
                  <col className="race-trend-detail-col-style" />
                  <col className="race-trend-detail-col-finish" />
                  <col className="race-trend-detail-col-popularity" />
                  <col className="race-trend-detail-col-odds" />
                  <col className="race-trend-detail-col-horse-number" />
                  <col className="race-trend-detail-col-frame" />
                  <col className="race-trend-detail-col-jockey" />
                </colgroup>
                <thead>
                  <tr>
                    <th>日付</th>
                    <th>場</th>
                    <th>R</th>
                    <th>脚質</th>
                    <th>着順</th>
                    <th>人気</th>
                    <th>単勝</th>
                    <th>馬番</th>
                    <th>枠</th>
                    <th>騎手</th>
                  </tr>
                </thead>
                <tbody>
                  {row.details.map((detail) => (
                    <tr
                      key={[
                        detail.source,
                        detail.date,
                        detail.keibajoCode,
                        detail.raceNumber,
                        detail.horseNumber,
                        detail.frameNumber,
                        detail.jockeyName,
                        detail.finishPosition,
                        detail.popularity,
                        detail.winOdds,
                      ].join(":")}
                    >
                      <td>{detail.date}</td>
                      <td>{formatKeibajo(detail.keibajoCode)}</td>
                      <td>{formatRaceNumber(detail.raceNumber)}</td>
                      <td>
                        {detail.runningStyle ? RUNNING_STYLE_LABELS[detail.runningStyle] : "-"}
                      </td>
                      <td>{detail.finishPosition}</td>
                      <td>{formatTrendPopularity(detail.popularity)}</td>
                      <td>{formatTrendWinOdds(detail.winOdds)}</td>
                      <td>{detail.horseNumber ?? "-"}</td>
                      <td>{detail.frameNumber ? detail.frameNumber : "-"}</td>
                      <td>{detail.jockeyName ?? "-"}</td>
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
  const [runningStyleIgnoreFrame, setRunningStyleIgnoreFrame] = useState(false);
  const [runningStyleIgnoreJockey, setRunningStyleIgnoreJockey] = useState(false);
  const [jockeyStart, setJockeyStart] = useState(defaultStartDate);
  const [jockeyEnd, setJockeyEnd] = useState(defaultEndDate);
  const [frameStart, setFrameStart] = useState(defaultStartDate);
  const [frameEnd, setFrameEnd] = useState(defaultEndDate);
  const [jockeySortKey, setJockeySortKey] = useState<SortKey>("showRate");
  const [frameSortKey, setFrameSortKey] = useState<SortKey>("showRate");
  const [jockeyRows, setJockeyRows] = useState<RaceTrendRateRow[]>([]);
  const [frameRows, setFrameRows] = useState<RaceTrendRateRow[]>([]);
  const [runningStyleRows, setRunningStyleRows] = useState<RaceTrendRunningStyleRow[]>([]);
  const [jockeyStatus, setJockeyStatus] = useState<"idle" | "loading" | "error">("idle");
  const [frameStatus, setFrameStatus] = useState<"idle" | "loading" | "error">("idle");

  const fetchJockeyRows = useCallback(async () => {
    setJockeyStatus("loading");
    try {
      const response = await fetchWithRetry(
        getApiPath({
          day,
          defaultEndDate,
          defaultStartDate,
          frameEnd: defaultEndDate,
          frameStart: defaultStartDate,
          jockeyEnd,
          jockeySameVenue,
          jockeyStart,
          keibajoCode,
          month,
          raceNumber,
          runningStyleIgnoreFrame,
          runningStyleIgnoreJockey,
          source,
          year,
        }),
        { cache: "no-store" },
        RACE_TREND_RETRY_OPTIONS,
      );
      if (!response.ok) {
        throw new Error(`race trend api ${response.status}`);
      }
      const body: unknown = await response.json();
      if (!isRaceTrendPayload(body)) {
        throw new Error("invalid race trend payload");
      }
      setJockeyRows(body.jockeyRows);
      setRunningStyleRows(body.runningStyleRows);
      setJockeyStatus("idle");
    } catch {
      setJockeyRows([]);
      setRunningStyleRows([]);
      setJockeyStatus("error");
    }
  }, [
    day,
    defaultEndDate,
    defaultStartDate,
    jockeyEnd,
    jockeySameVenue,
    jockeyStart,
    keibajoCode,
    month,
    raceNumber,
    runningStyleIgnoreFrame,
    runningStyleIgnoreJockey,
    source,
    year,
  ]);

  useEffect(() => {
    void fetchJockeyRows();
  }, [fetchJockeyRows]);

  const fetchFrameRows = useCallback(async () => {
    setFrameStatus("loading");
    try {
      const response = await fetchWithRetry(
        getApiPath({
          day,
          defaultEndDate,
          defaultStartDate,
          frameEnd,
          frameStart,
          jockeyEnd: defaultEndDate,
          jockeySameVenue: true,
          jockeyStart: defaultStartDate,
          keibajoCode,
          month,
          raceNumber,
          source,
          year,
        }),
        { cache: "no-store" },
        RACE_TREND_RETRY_OPTIONS,
      );
      if (!response.ok) {
        throw new Error(`race trend api ${response.status}`);
      }
      const body: unknown = await response.json();
      if (!isRaceTrendPayload(body)) {
        throw new Error("invalid race trend payload");
      }
      setFrameRows(body.frameRows);
      setFrameStatus("idle");
    } catch {
      setFrameRows([]);
      setFrameStatus("error");
    }
  }, [
    day,
    defaultEndDate,
    defaultStartDate,
    frameEnd,
    frameStart,
    keibajoCode,
    month,
    raceNumber,
    source,
    year,
  ]);

  useEffect(() => {
    void fetchFrameRows();
  }, [fetchFrameRows]);

  return (
    <section className="race-trend-section">
      <div className="section-heading compact">
        <h2>レース傾向</h2>
        <span>
          {formatKeibajo(keibajoCode)} {source === "jra" ? "中央競馬" : "地方競馬"}
        </span>
      </div>

      <div className="race-trend-grid">
        <div className="race-trend-card">
          <div className="race-trend-controls">
            <label>
              <span>開始日</span>
              <input
                type="date"
                value={jockeyStart}
                onChange={(event) => setJockeyStart(event.target.value)}
              />
            </label>
            <label>
              <span>終了日</span>
              <input
                type="date"
                value={jockeyEnd}
                onChange={(event) => setJockeyEnd(event.target.value)}
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
            <label className="race-trend-checkbox">
              <input
                checked={runningStyleIgnoreFrame}
                onChange={(event) => setRunningStyleIgnoreFrame(event.target.checked)}
                type="checkbox"
              />
              <span>枠を解除</span>
            </label>
            <label className="race-trend-checkbox">
              <input
                checked={runningStyleIgnoreJockey}
                onChange={(event) => setRunningStyleIgnoreJockey(event.target.checked)}
                type="checkbox"
              />
              <span>騎手を解除</span>
            </label>
          </div>
          <TrendTable
            emptyLabel="該当する騎手成績はありません"
            isLoading={jockeyStatus === "loading"}
            kind="jockey"
            labelColumn="騎手名"
            rows={jockeyRows}
            showStarts
            showTargetHorseNumber
            sortKey={jockeySortKey}
            title="騎手ごとの勝率"
            onSortChange={setJockeySortKey}
          />
          <RunningStyleTrendTable
            ignoreFrame={runningStyleIgnoreFrame}
            ignoreJockey={runningStyleIgnoreJockey}
            isLoading={jockeyStatus === "loading"}
            rows={runningStyleRows}
          />
        </div>

        <div className="race-trend-card">
          <div className="race-trend-controls">
            <label>
              <span>開始日</span>
              <input
                type="date"
                value={frameStart}
                onChange={(event) => setFrameStart(event.target.value)}
              />
            </label>
            <label>
              <span>終了日</span>
              <input
                type="date"
                value={frameEnd}
                onChange={(event) => setFrameEnd(event.target.value)}
              />
            </label>
          </div>
          <TrendTable
            emptyLabel="該当する枠成績はありません"
            isLoading={frameStatus === "loading"}
            kind="frame"
            labelColumn="枠番"
            rows={frameRows}
            showTargetHorseNumber
            sortKey={frameSortKey}
            title="枠ごとの勝率"
            onSortChange={setFrameSortKey}
          />
        </div>
      </div>

      {jockeyStatus === "error" || frameStatus === "error" ? (
        <p className="race-trend-error">レース傾向を取得できませんでした。</p>
      ) : null}
    </section>
  );
}
