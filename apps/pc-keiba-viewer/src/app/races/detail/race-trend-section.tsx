"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import { formatKeibajo } from "../../../lib/format";
import type { RaceTrendPayload, RaceTrendRateRow } from "../../../lib/race-types";

type SortKey = "showRate" | "quinellaRate" | "winRate";
type TrendTableKind = "frame" | "jockey";

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

const isRaceTrendPayload = (value: unknown): value is RaceTrendPayload =>
  typeof value === "object" && value !== null && "jockeyRows" in value && "frameRows" in value;

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
  source,
  year,
}: RaceTrendSectionProps & {
  frameEnd: string;
  frameStart: string;
  jockeyEnd: string;
  jockeySameVenue: boolean;
  jockeyStart: string;
}): string => {
  const params = new URLSearchParams({
    source,
    jockeyStart,
    jockeyEnd,
    frameStart,
    frameEnd,
    jockeySameVenue: String(jockeySameVenue),
  });
  return `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/trends?${params.toString()}`;
};

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

  return (
    <div className="race-trend-table-panel">
      <div className="race-trend-subheading">
        <h3>{title}</h3>
        <span>{SORT_LABELS[sortKey]}順</span>
      </div>
      <div className="stats-table-wrap">
        <table className={`stats-table race-trend-table ${kind}`}>
          <thead>
            <tr>
              {showTargetBeforeLabel ? <th>馬番</th> : null}
              <th>{labelColumn}</th>
              {showTargetAfterLabel ? <th>馬番</th> : null}
              {showStarts ? <th>出走回数</th> : null}
              {(["showRate", "quinellaRate", "winRate"] as const).map((key) => (
                <th key={key}>
                  <button
                    aria-pressed={sortKey === key}
                    className="race-trend-sort-button"
                    onClick={() => onSortChange(key)}
                    type="button"
                  >
                    {SORT_LABELS[key]}
                  </button>
                </th>
              ))}
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
                  {showStarts ? (
                    <td>
                      <span className="race-trend-skeleton race-trend-skeleton-count" />
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
                <td
                  className="race-trend-empty-cell"
                  colSpan={(showStarts ? 5 : 4) + (showTargetHorseNumber ? 1 : 0)}
                >
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
        {showStarts ? <td>{row.starts}</td> : null}
        <td>{formatRate(row.showRate)}</td>
        <td>{formatRate(row.quinellaRate)}</td>
        <td>{formatRate(row.winRate)}</td>
      </tr>
      {isExpanded ? (
        <tr className="stats-detail-row">
          <td colSpan={(showStarts ? 5 : 4) + (showTargetHorseNumber ? 1 : 0)}>
            <div className="stats-detail-panel">
              <table className={`stats-detail-table race-trend-detail-table ${kind}`}>
                <colgroup>
                  <col className="race-trend-detail-col-finish" />
                  <col className="race-trend-detail-col-primary" />
                  {kind === "frame" ? <col className="race-trend-detail-col-horse-number" /> : null}
                  <col className="race-trend-detail-col-date" />
                  <col className="race-trend-detail-col-race-name" />
                  <col className="race-trend-detail-col-race-number" />
                  {kind === "jockey" ? (
                    <col className="race-trend-detail-col-horse-number" />
                  ) : null}
                  <col className="race-trend-detail-col-secondary" />
                </colgroup>
                <thead>
                  <tr>
                    <th>着順</th>
                    <th>{kind === "jockey" ? "騎手名" : "枠番"}</th>
                    {kind === "frame" ? <th>馬番</th> : null}
                    <th>日付</th>
                    <th>レース名</th>
                    <th>レースナンバー</th>
                    {kind === "jockey" ? <th>馬番</th> : null}
                    <th>{kind === "jockey" ? "枠番" : "騎手名"}</th>
                  </tr>
                </thead>
                <tbody>
                  {row.details.map((detail) => (
                    <tr
                      key={`${detail.date}:${detail.keibajoCode}:${detail.raceNumber}:${detail.horseNumber}:${labelColumn}`}
                    >
                      <td>{detail.finishPosition}</td>
                      <td>
                        {kind === "jockey"
                          ? (detail.jockeyName ?? "-")
                          : detail.frameNumber
                            ? detail.frameNumber
                            : "-"}
                      </td>
                      {kind === "frame" ? <td>{detail.horseNumber ?? "-"}</td> : null}
                      <td>{detail.date}</td>
                      <td>{detail.raceName ?? "-"}</td>
                      <td>{detail.raceNumber}</td>
                      {kind === "jockey" ? <td>{detail.horseNumber ?? "-"}</td> : null}
                      <td>
                        {kind === "jockey"
                          ? detail.frameNumber
                            ? detail.frameNumber
                            : "-"
                          : (detail.jockeyName ?? "-")}
                      </td>
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
  const [jockeyStart, setJockeyStart] = useState(defaultStartDate);
  const [jockeyEnd, setJockeyEnd] = useState(defaultEndDate);
  const [frameStart, setFrameStart] = useState(defaultStartDate);
  const [frameEnd, setFrameEnd] = useState(defaultEndDate);
  const [jockeySortKey, setJockeySortKey] = useState<SortKey>("showRate");
  const [frameSortKey, setFrameSortKey] = useState<SortKey>("showRate");
  const [jockeyRows, setJockeyRows] = useState<RaceTrendRateRow[]>([]);
  const [frameRows, setFrameRows] = useState<RaceTrendRateRow[]>([]);
  const [jockeyStatus, setJockeyStatus] = useState<"idle" | "loading" | "error">("idle");
  const [frameStatus, setFrameStatus] = useState<"idle" | "loading" | "error">("idle");

  const fetchJockeyRows = useCallback(async () => {
    setJockeyStatus("loading");
    try {
      const response = await fetch(
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
          source,
          year,
        }),
        { cache: "no-store" },
      );
      if (!response.ok) {
        throw new Error(`race trend api ${response.status}`);
      }
      const body: unknown = await response.json();
      if (!isRaceTrendPayload(body)) {
        throw new Error("invalid race trend payload");
      }
      setJockeyRows(body.jockeyRows);
      setJockeyStatus("idle");
    } catch {
      setJockeyRows([]);
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
    source,
    year,
  ]);

  useEffect(() => {
    void fetchJockeyRows();
  }, [fetchJockeyRows]);

  const fetchFrameRows = useCallback(async () => {
    setFrameStatus("loading");
    try {
      const response = await fetch(
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
