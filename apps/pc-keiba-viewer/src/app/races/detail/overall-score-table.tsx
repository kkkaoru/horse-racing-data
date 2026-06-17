"use client";

import { Fragment, memo, useCallback, useMemo, useState } from "react";

import type { OverallScoreRow } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

interface OverallScoreTableProps {
  expandAll: boolean;
  realtimeRequest: RealtimeRaceRequest;
  rows: OverallScoreRow[];
}

interface OverallScoreTableRowProps {
  entryStatus: string;
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
  entryStatus,
  isExpanded,
  onToggle,
  realtimeOdds,
  realtimePopularity,
  row,
}: OverallScoreTableRowProps) {
  const isScratched = entryStatus !== "";

  return (
    <Fragment>
      <tr
        className={[
          isExpanded ? "stats-row-expanded" : "",
          isScratched ? "stats-row-scratched" : "",
        ]
          .filter(Boolean)
          .join(" ")}
        data-entry-status={entryStatus || undefined}
      >
        <td>{formatRunnerNumber(row.horseNumber)}</td>
        <td className="stats-name-cell">
          {row.horseName || "-"}
          {entryStatus ? <span className="runner-status-badge">{entryStatus}</span> : null}
        </td>
        <td className="stats-name-cell">{row.jockeyName || "-"}</td>
        <td className="stats-score-cell">
          {isScratched ? (
            "対象外"
          ) : (
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
          )}
        </td>
        <td>{isScratched ? "-" : formatRealtimePopularity(realtimePopularity)}</td>
        <td>{isScratched ? "-" : formatRealtimeOdds(realtimeOdds)}</td>
      </tr>
      {isExpanded && !isScratched ? (
        <tr className="stats-detail-row">
          <td colSpan={6} aria-label="総合スコア詳細内訳">
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

export function OverallScoreTable({ expandAll, realtimeRequest, rows }: OverallScoreTableProps) {
  const [expandedHorseNumber, setExpandedHorseNumber] = useState<string | null>(null);
  const { payload } = useRealtimeRacePayload(realtimeRequest, null);
  const realtimeOddsByHorse = useMemo(
    () =>
      new Map(
        (payload?.odds?.latest.tansho ?? []).map((row) => [
          formatRunnerNumber(row.combination),
          { odds: row.odds ?? null, popularity: row.rank ?? null },
        ]),
      ),
    [payload],
  );
  const entryStatusByHorse = useMemo(
    () =>
      new Map(
        (payload?.raceEntries?.horses ?? []).map((horse) => [
          formatRunnerNumber(horse.horseNumber),
          horse.status ?? "",
        ]),
      ),
    [payload],
  );
  const sortedRows = useMemo(
    () =>
      rows.toSorted((left, right) => {
        const leftStatus = entryStatusByHorse.get(formatRunnerNumber(left.horseNumber)) ?? "";
        const rightStatus = entryStatusByHorse.get(formatRunnerNumber(right.horseNumber)) ?? "";
        if (leftStatus !== "" || rightStatus !== "") {
          return leftStatus === rightStatus ? 0 : leftStatus ? 1 : -1;
        }
        return right.score - left.score || Number(left.horseNumber) - Number(right.horseNumber);
      }),
    [entryStatusByHorse, rows],
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
              const horseNumber = formatRunnerNumber(row.horseNumber);
              const realtimeOdds = realtimeOddsByHorse.get(horseNumber);
              return (
                <OverallScoreTableRow
                  entryStatus={entryStatusByHorse.get(horseNumber) ?? ""}
                  isExpanded={expandAll || expandedHorseNumber === row.horseNumber}
                  key={row.horseNumber}
                  realtimeOdds={realtimeOdds?.odds ?? row.storedOdds}
                  realtimePopularity={realtimeOdds?.popularity ?? row.storedPopularity}
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
