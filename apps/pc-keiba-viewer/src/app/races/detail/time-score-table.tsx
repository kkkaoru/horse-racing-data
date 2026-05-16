"use client";

import { Fragment, useState } from "react";

import type { TimeScoreRow } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";

interface TimeScoreTableProps {
  rows: TimeScoreRow[];
}

const formatValue = (value: number | null): string => (value === null ? "-" : value.toFixed(1));

export function TimeScoreTable({ rows }: TimeScoreTableProps) {
  const [expandedHorseNumber, setExpandedHorseNumber] = useState<string | null>(null);

  return (
    <div className="stats-table-wrap time-score-table-wrap">
      <table className="stats-table analysis-table time-score-table">
        <thead>
          <tr>
            <th>馬番</th>
            <th>馬名</th>
            <th>スコア</th>
          </tr>
        </thead>
        <tbody>
          {rows.length > 0 ? (
            rows.map((row) => {
              const isExpanded = expandedHorseNumber === row.horseNumber;
              return (
                <Fragment key={row.horseNumber}>
                  <tr className={isExpanded ? "stats-row-expanded" : undefined}>
                    <td>{formatRunnerNumber(row.horseNumber)}</td>
                    <td className="stats-name-cell">{row.horseName || "-"}</td>
                    <td className="stats-score-cell">
                      <span className="time-score-actions">
                        <span>{row.score.toFixed(2)}</span>
                        <button
                          aria-expanded={isExpanded}
                          aria-label={`${row.horseName || row.horseNumber}のタイムスコア詳細`}
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
                  </tr>
                  {isExpanded ? (
                    <tr className="stats-detail-row">
                      <td colSpan={3}>
                        <div className="stats-detail-panel">
                          <table className="stats-detail-table correlation-detail-table">
                            <thead>
                              <tr>
                                <th>項目</th>
                                <th>現在値</th>
                                <th>対象平均</th>
                                <th>スコア</th>
                                <th>重み</th>
                                <th>理由</th>
                              </tr>
                            </thead>
                            <tbody>
                              {row.details.map((detail) => (
                                <tr key={detail.label}>
                                  <td>{detail.label}</td>
                                  <td>{formatValue(detail.value)}</td>
                                  <td>{formatValue(detail.target)}</td>
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
            })
          ) : (
            <tr>
              <td colSpan={3}>タイムスコアを表示できるデータがありません。</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
