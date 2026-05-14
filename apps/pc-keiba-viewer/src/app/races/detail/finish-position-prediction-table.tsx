"use client";

import { Fragment, memo, useCallback, useEffect, useMemo, useState } from "react";

import { RACE_FINISH_PREDICTION_RESULTS_EVENT } from "../../../lib/finish-position-prediction";
import type { FinishPredictionRow } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

interface FinishPositionPredictionTableProps {
  realtimeRequest: RealtimeRaceRequest;
  rows: FinishPredictionRow[];
}

interface FinishPredictionTableRowProps {
  isExpanded: boolean;
  onToggle: (horseNumber: string) => void;
  realtimeOdds: number | null | undefined;
  realtimePopularity: number | null | undefined;
  row: FinishPredictionRow;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isNullableNumber = (value: unknown): value is number | null =>
  value === null || typeof value === "number";

const isFinishPredictionRow = (value: unknown): value is FinishPredictionRow => {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.horseNumber === "string" &&
    typeof value.horseName === "string" &&
    typeof value.predictedRank === "number" &&
    typeof value.score === "number" &&
    typeof value.confidence === "number" &&
    isNullableNumber(value.storedOdds) &&
    isNullableNumber(value.storedPopularity) &&
    Array.isArray(value.details)
  );
};

const getRowsFromResultsUpdateEvent = (event: Event): FinishPredictionRow[] | null => {
  if (!(event instanceof CustomEvent) || !isRecord(event.detail)) {
    return null;
  }
  const eventRows = event.detail.rows;
  if (!Array.isArray(eventRows) || !eventRows.every(isFinishPredictionRow)) {
    return null;
  }
  return eventRows;
};

const formatPopularity = (value: number | null): string => (value === null ? "-" : `${value}`);

const formatOdds = (value: number | null): string => (value === null ? "-" : value.toFixed(1));

const FinishPredictionTableRow = memo(function FinishPredictionTableRow({
  isExpanded,
  onToggle,
  realtimeOdds,
  realtimePopularity,
  row,
}: FinishPredictionTableRowProps) {
  const displayedPopularity = realtimePopularity ?? row.storedPopularity;
  const displayedOdds = realtimeOdds ?? row.storedOdds;

  return (
    <Fragment>
      <tr className={isExpanded ? "stats-row-expanded" : undefined}>
        <td>{formatRunnerNumber(row.horseNumber)}</td>
        <td className="stats-name-cell">{row.horseName || "-"}</td>
        <td>{row.predictedRank}</td>
        <td>{formatPopularity(displayedPopularity)}</td>
        <td>{formatOdds(displayedOdds)}</td>
        <td className="stats-score-cell">
          <span className="time-score-actions">
            <span>{row.score.toFixed(2)}</span>
            <button
              aria-expanded={isExpanded}
              aria-label={`${row.horseName || row.horseNumber}の着順予測詳細`}
              className="stats-detail-toggle"
              type="button"
              onClick={() => {
                onToggle(row.horseNumber);
              }}
            />
          </span>
        </td>
        <td>{row.winProbability.toFixed(2)}</td>
        <td>{row.showProbability.toFixed(2)}</td>
        <td>{row.confidence.toFixed(2)}</td>
      </tr>
      {isExpanded ? (
        <tr className="stats-detail-row">
          <td colSpan={9}>
            <div className="stats-detail-panel">
              <table className="stats-detail-table correlation-detail-table overall-score-detail-table">
                <thead>
                  <tr>
                    <th>項目</th>
                    <th>値</th>
                    <th>重み</th>
                    <th>理由</th>
                  </tr>
                </thead>
                <tbody>
                  {row.details.map((detail) => (
                    <tr key={detail.label}>
                      <td>{detail.label}</td>
                      <td>{detail.value === null ? "-" : detail.value.toFixed(2)}</td>
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

export function FinishPositionPredictionTable({
  realtimeRequest,
  rows,
}: FinishPositionPredictionTableProps) {
  const [expandedHorseNumber, setExpandedHorseNumber] = useState<string | null>(null);
  const [displayRows, setDisplayRows] = useState<FinishPredictionRow[]>(rows);
  const { payload } = useRealtimeRacePayload(realtimeRequest, null);
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

    window.addEventListener(RACE_FINISH_PREDICTION_RESULTS_EVENT, handleResultsUpdate);
    return () => {
      window.removeEventListener(RACE_FINISH_PREDICTION_RESULTS_EVENT, handleResultsUpdate);
    };
  }, []);

  if (displayRows.length === 0) {
    return <p className="empty-state">着順予測を表示できるデータがありません。</p>;
  }

  return (
    <div className="stats-table-wrap">
      <table className="stats-table analysis-table finish-prediction-table">
        <colgroup>
          <col className="finish-prediction-col-number" />
          <col className="finish-prediction-col-horse" />
          <col className="finish-prediction-col-rank" />
          <col className="finish-prediction-col-popularity" />
          <col className="finish-prediction-col-odds" />
          <col className="finish-prediction-col-score" />
          <col className="finish-prediction-col-rate" />
          <col className="finish-prediction-col-rate" />
          <col className="finish-prediction-col-rate" />
        </colgroup>
        <thead>
          <tr>
            <th>馬番</th>
            <th>馬名</th>
            <th>予想着順</th>
            <th>人気</th>
            <th>単勝</th>
            <th>スコア</th>
            <th>勝率</th>
            <th>3着内率</th>
            <th>信頼度</th>
          </tr>
        </thead>
        <tbody>
          {displayRows.map((row) => {
            const realtimeOdds = realtimeOddsByHorse.get(row.horseNumber);
            return (
              <FinishPredictionTableRow
                isExpanded={expandedHorseNumber === row.horseNumber}
                key={row.horseNumber}
                realtimeOdds={realtimeOdds?.odds}
                realtimePopularity={realtimeOdds?.popularity}
                row={row}
                onToggle={toggleExpandedHorse}
              />
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
