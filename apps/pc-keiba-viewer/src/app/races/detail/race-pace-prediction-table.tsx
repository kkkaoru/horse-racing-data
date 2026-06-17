"use client";

import { Fragment, useEffect, useState } from "react";

import { RACE_PACE_PREDICTION_RESULTS_EVENT } from "../../../lib/race-pace-prediction";
import type { RacePacePredictionRow } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";

interface RacePacePredictionTableProps {
  rows: RacePacePredictionRow[];
}

const formatValue = (value: number | null): string => (value === null ? "-" : value.toFixed(1));

type CornerKey = "corner1" | "corner2" | "corner3" | "corner4";

const CORNERS: Array<{ key: CornerKey; label: string }> = [
  { key: "corner1", label: "1コーナー" },
  { key: "corner2", label: "2コーナー" },
  { key: "corner3", label: "3コーナー" },
  { key: "corner4", label: "4コーナー" },
];

const getCornerOrderRows = (
  rows: RacePacePredictionRow[],
  cornerKey: CornerKey,
): RacePacePredictionRow[] =>
  rows
    .filter((row) => row[cornerKey] !== null)
    .toSorted(
      (left, right) =>
        (left[cornerKey] ?? Number.POSITIVE_INFINITY) -
          (right[cornerKey] ?? Number.POSITIVE_INFINITY) ||
        Number(left.horseNumber) - Number(right.horseNumber),
    );

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isRacePacePredictionRow = (value: unknown): value is RacePacePredictionRow => {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.horseNumber === "string" &&
    typeof value.horseName === "string" &&
    typeof value.predictedCorners === "string" &&
    typeof value.confidence === "number" &&
    Array.isArray(value.details)
  );
};

const getRowsFromResultsUpdateEvent = (event: Event): RacePacePredictionRow[] | null => {
  if (!(event instanceof CustomEvent) || !isRecord(event.detail)) {
    return null;
  }
  const eventRows = event.detail.rows;
  if (!Array.isArray(eventRows) || !eventRows.every(isRacePacePredictionRow)) {
    return null;
  }
  return eventRows;
};

export function RacePacePredictionTable({ rows }: RacePacePredictionTableProps) {
  const [expandedHorseNumber, setExpandedHorseNumber] = useState<string | null>(null);
  const [displayRows, setDisplayRows] = useState<RacePacePredictionRow[]>(rows);
  const cornerOrderRows = CORNERS.map((corner) => ({
    key: corner.key,
    label: corner.label,
    passingOrder: getCornerOrderRows(displayRows, corner.key)
      .map((row) => formatRunnerNumber(row.horseNumber))
      .join("-"),
  }));

  useEffect(() => {
    setDisplayRows(rows);
  }, [rows]);

  useEffect(() => {
    const handleResultsUpdate = (event: Event) => {
      const nextRows = getRowsFromResultsUpdateEvent(event);
      if (nextRows === null) {
        return;
      }
      setDisplayRows(nextRows);
      setExpandedHorseNumber(null);
    };

    window.addEventListener(RACE_PACE_PREDICTION_RESULTS_EVENT, handleResultsUpdate);
    return () => {
      window.removeEventListener(RACE_PACE_PREDICTION_RESULTS_EVENT, handleResultsUpdate);
    };
  }, []);

  if (displayRows.length === 0) {
    return <p className="empty-state">レース展開予測を表示できるデータがありません。</p>;
  }

  return (
    <>
      <div className="stats-table-wrap race-pace-corner-table-wrap">
        <table className="race-pace-corner-table">
          <thead>
            <tr>
              <th>コーナー</th>
              <th>通過順</th>
            </tr>
          </thead>
          <tbody>
            {cornerOrderRows.map((corner) => (
              <tr key={corner.key}>
                <th>{corner.label}</th>
                <td>{corner.passingOrder || "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <details className="race-pace-horse-details">
        <summary>馬ごとの予測</summary>
        <div className="stats-table-wrap">
          <table className="stats-table analysis-table race-pace-prediction-table">
            <thead>
              <tr>
                <th>馬番</th>
                <th>馬名</th>
                <th>コーナー通過予測</th>
                <th>1C予測値</th>
                <th>2C予測値</th>
                <th>3C予測値</th>
                <th>4C予測値</th>
                <th>信頼度</th>
              </tr>
            </thead>
            <tbody>
              {displayRows.map((row) => {
                const isExpanded = expandedHorseNumber === row.horseNumber;
                return (
                  <Fragment key={row.horseNumber}>
                    <tr className={isExpanded ? "stats-row-expanded" : undefined}>
                      <td>{formatRunnerNumber(row.horseNumber)}</td>
                      <td className="stats-name-cell">{row.horseName || "-"}</td>
                      <td className="stats-score-cell">
                        <span className="time-score-actions">
                          <span>{row.predictedCorners}</span>
                          <button
                            aria-expanded={isExpanded}
                            aria-label={`${row.horseName || row.horseNumber}のレース展開予測詳細`}
                            className="stats-detail-toggle"
                            type="button"
                            onClick={() => {
                              setExpandedHorseNumber((current) =>
                                current === row.horseNumber ? null : row.horseNumber,
                              );
                            }}
                          />
                        </span>
                      </td>
                      <td>{formatValue(row.corner1)}</td>
                      <td>{formatValue(row.corner2)}</td>
                      <td>{formatValue(row.corner3)}</td>
                      <td>{formatValue(row.corner4)}</td>
                      <td>{row.confidence.toFixed(2)}</td>
                    </tr>
                    {isExpanded ? (
                      <tr className="stats-detail-row">
                        <td colSpan={8} aria-label="ペース予測詳細内訳">
                          <div className="stats-detail-panel">
                            <table className="stats-detail-table correlation-detail-table">
                              <thead>
                                <tr>
                                  <th>項目</th>
                                  <th>平均通過順</th>
                                  <th>重み</th>
                                  <th>理由</th>
                                </tr>
                              </thead>
                              <tbody>
                                {row.details.map((detail) => (
                                  <tr key={detail.label}>
                                    <td>{detail.label}</td>
                                    <td>{formatValue(detail.value)}</td>
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
              })}
            </tbody>
          </table>
        </div>
      </details>
    </>
  );
}
