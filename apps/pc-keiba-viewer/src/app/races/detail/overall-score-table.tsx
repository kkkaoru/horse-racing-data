"use client";

import { Fragment, memo, useCallback, useMemo, useState } from "react";

import type { OverallScoreRow } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

interface OverallScoreTableProps {
  realtimeRequest: RealtimeRaceRequest;
  rows: OverallScoreRow[];
}

interface OverallScoreTableRowProps {
  isExpanded: boolean;
  onToggle: (horseNumber: string) => void;
  realtimeOdds: number | null | undefined;
  realtimePopularity: number | null | undefined;
  row: OverallScoreRow;
}

const formatRealtimePopularity = (value: number | null | undefined): string =>
  value === null || value === undefined ? "-" : `${value}`;

const formatRealtimeOdds = (value: number | null | undefined): string =>
  value === null || value === undefined ? "-" : value.toFixed(1);

const OverallScoreTableRow = memo(function OverallScoreTableRow({
  isExpanded,
  onToggle,
  realtimeOdds,
  realtimePopularity,
  row,
}: OverallScoreTableRowProps) {
  return (
    <Fragment>
      <tr className={isExpanded ? "stats-row-expanded" : undefined}>
        <td>{formatRunnerNumber(row.horseNumber)}</td>
        <td className="stats-name-cell">{row.horseName || "-"}</td>
        <td className="stats-name-cell">{row.jockeyName || "-"}</td>
        <td className="stats-score-cell">
          <span className="time-score-actions">
            <span>{row.score.toFixed(2)}</span>
            <button
              aria-expanded={isExpanded}
              aria-label={`${row.horseName || row.horseNumber}の総合スコア詳細`}
              className="stats-detail-toggle"
              type="button"
              onClick={() => {
                onToggle(row.horseNumber);
              }}
            />
          </span>
        </td>
        <td>{formatRealtimePopularity(realtimePopularity)}</td>
        <td>{formatRealtimeOdds(realtimeOdds)}</td>
      </tr>
      {isExpanded ? (
        <tr className="stats-detail-row">
          <td colSpan={6}>
            <div className="stats-detail-panel">
              <table className="stats-detail-table correlation-detail-table overall-score-detail-table">
                <thead>
                  <tr>
                    <th>項目</th>
                    <th>スコア</th>
                    <th>重み</th>
                    <th>理由</th>
                  </tr>
                </thead>
                <tbody>
                  {row.details.map((detail) => (
                    <tr key={detail.label}>
                      <td>{detail.label}</td>
                      <td>{detail.score.toFixed(2)}</td>
                      <td>{detail.weight.toFixed(2)}</td>
                      <td className="stats-name-cell">{detail.reason}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </td>
        </tr>
      ) : null}
    </Fragment>
  );
});

export function OverallScoreTable({ realtimeRequest, rows }: OverallScoreTableProps) {
  const [expandedHorseNumber, setExpandedHorseNumber] = useState<string | null>(null);
  const { payload } = useRealtimeRacePayload(realtimeRequest, null);
  const sortedRows = useMemo(
    () =>
      rows.toSorted(
        (left, right) =>
          right.score - left.score || Number(left.horseNumber) - Number(right.horseNumber),
      ),
    [rows],
  );
  const realtimeOddsByHorse = useMemo(
    () =>
      new Map(
        (payload?.odds?.latest.tansho ?? []).map((row) => [
          row.combination,
          { odds: row.odds ?? null, popularity: row.rank ?? null },
        ]),
      ),
    [payload],
  );
  const toggleExpandedHorse = useCallback((horseNumber: string) => {
    setExpandedHorseNumber((current) => (current === horseNumber ? null : horseNumber));
  }, []);

  return (
    <div className="stats-table-wrap">
      <table className="stats-table analysis-table overall-score-table">
        <thead>
          <tr>
            <th>馬番</th>
            <th>馬名</th>
            <th>騎手名</th>
            <th>総合スコア</th>
            <th>人気</th>
            <th>単勝</th>
          </tr>
        </thead>
        <tbody>
          {sortedRows.length > 0 ? (
            sortedRows.map((row) => {
              const realtimeOdds = realtimeOddsByHorse.get(row.horseNumber);
              return (
                <OverallScoreTableRow
                  isExpanded={expandedHorseNumber === row.horseNumber}
                  key={row.horseNumber}
                  realtimeOdds={realtimeOdds?.odds}
                  realtimePopularity={realtimeOdds?.popularity}
                  row={row}
                  onToggle={toggleExpandedHorse}
                />
              );
            })
          ) : (
            <tr>
              <td colSpan={6}>総合スコアを表示できるデータがありません。</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
