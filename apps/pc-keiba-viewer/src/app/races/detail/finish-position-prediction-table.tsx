"use client";

import { Fragment, memo, useCallback, useEffect, useMemo, useState } from "react";

import { RACE_FINISH_PREDICTION_RESULTS_EVENT } from "../../../lib/finish-position-prediction";
import type { FinishPredictionEvaluationMetrics } from "../../../lib/finish-position-prediction-evaluation";
import { getPreferredJockeyName } from "../../../lib/jockey-name";
import type { FinishPredictionRow } from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

interface FinishPositionPredictionTableProps {
  evaluation: FinishPredictionEvaluationMetrics;
  realtimeRequest: RealtimeRaceRequest;
  rows: FinishPredictionRow[];
}

interface FinishPredictionTableRowProps {
  entryStatus: string;
  isExpanded: boolean;
  jockeyName: string;
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
    typeof value.jockeyName === "string" &&
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

const formatDate = (value: string): string =>
  value.length === 8 ? `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}` : value;

const formatPercent = (value: number): string => `${value.toFixed(2)}%`;

const formatRaceCount = (value: number): string => value.toLocaleString("ja-JP");

type EvaluationMetric = {
  description: string;
  key: string;
  label: string;
  value: number;
};

function FinishPredictionEvaluationPanel({
  evaluation,
}: {
  evaluation: FinishPredictionEvaluationMetrics;
}) {
  const [activeTooltipKey, setActiveTooltipKey] = useState<string | null>(null);
  const metrics: EvaluationMetric[] = [
    {
      description: "予想1位の馬が、実際に1着だったレースの割合です。",
      key: "place1Accuracy",
      label: "1着一致",
      value: evaluation.place1Accuracy,
    },
    {
      description: "予想2位の馬が、実際に2着だったレースの割合です。",
      key: "place2Accuracy",
      label: "2着一致",
      value: evaluation.place2Accuracy,
    },
    {
      description: "予想3位の馬が、実際に3着だったレースの割合です。",
      key: "place3Accuracy",
      label: "3着一致",
      value: evaluation.place3Accuracy,
    },
    {
      description: "実際の勝ち馬が、予想上位3頭の中に入っていたレースの割合です。",
      key: "top3WinnerCapture",
      label: "勝ち馬Top3",
      value: evaluation.top3WinnerCapture,
    },
    {
      description: "実際の勝ち馬が、予想上位5頭の中に入っていたレースの割合です。",
      key: "top5WinnerCapture",
      label: "勝ち馬Top5",
      value: evaluation.top5WinnerCapture,
    },
    {
      description: "予想上位3頭のうち、実際の1着から3着に入った頭数の平均割合です。",
      key: "top3PlaceRelation",
      label: "1-3着重なり",
      value: evaluation.top3PlaceRelation,
    },
    {
      description: "予想上位3頭に、実際の1着から3着の3頭が順不同で全て入った割合です。",
      key: "top3BoxAccuracy",
      label: "1-3着ボックス",
      value: evaluation.top3BoxAccuracy,
    },
    {
      description: "予想1位から3位が、実際の1着から3着と順番まで完全一致した割合です。",
      key: "top3ExactOrderAccuracy",
      label: "1-3着順序一致",
      value: evaluation.top3ExactOrderAccuracy,
    },
  ];

  return (
    <div className="finish-prediction-evaluation-panel" aria-label="着順予測の検証結果">
      <div className="finish-prediction-evaluation-summary">
        <span>{evaluation.categoryLabel}の検証精度</span>
        <strong>
          {formatDate(evaluation.fromDate)} - {formatDate(evaluation.toDate)}
        </strong>
        <small>{formatRaceCount(evaluation.raceCount)}レースで検証</small>
      </div>
      <div className="analysis-metric-grid finish-prediction-metric-grid">
        {metrics.map((metric) => {
          const tooltipId = `finish-prediction-metric-${metric.key}`;
          const isTooltipOpen = activeTooltipKey === metric.key;
          return (
            <button
              aria-describedby={tooltipId}
              aria-expanded={isTooltipOpen}
              className={`finish-prediction-metric-card${isTooltipOpen ? " tooltip-open" : ""}`}
              key={metric.key}
              type="button"
              onClick={() => {
                setActiveTooltipKey((current) => (current === metric.key ? null : metric.key));
              }}
            >
              <span>{metric.label}</span>
              <strong>{formatPercent(metric.value)}</strong>
              <small className="finish-prediction-metric-tooltip" id={tooltipId} role="tooltip">
                {metric.description}
              </small>
            </button>
          );
        })}
      </div>
    </div>
  );
}

const FinishPredictionTableRow = memo(function FinishPredictionTableRow({
  entryStatus,
  isExpanded,
  jockeyName,
  onToggle,
  realtimeOdds,
  realtimePopularity,
  row,
}: FinishPredictionTableRowProps) {
  const displayedPopularity = realtimePopularity ?? row.storedPopularity;
  const displayedOdds = realtimeOdds ?? row.storedOdds;
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
        <td className="stats-name-cell">{jockeyName || "-"}</td>
        <td>{isScratched ? "対象外" : row.predictedRank}</td>
        <td>{isScratched ? "-" : formatPopularity(displayedPopularity)}</td>
        <td>{isScratched ? "-" : formatOdds(displayedOdds)}</td>
        <td className="stats-score-cell">
          {isScratched ? (
            "対象外"
          ) : (
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
          )}
        </td>
        <td>{isScratched ? "-" : row.winProbability.toFixed(2)}</td>
        <td>{isScratched ? "-" : row.showProbability.toFixed(2)}</td>
        <td>{isScratched ? "-" : row.confidence.toFixed(2)}</td>
      </tr>
      {isExpanded && !isScratched ? (
        <tr className="stats-detail-row">
          <td colSpan={10}>
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
  evaluation,
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
  const realtimeJockeyByHorse = useMemo(
    () =>
      new Map(
        (payload?.raceEntries?.horses ?? []).map((horse) => [
          formatRunnerNumber(horse.horseNumber),
          horse.jockeyName ?? "",
        ]),
      ),
    [payload],
  );
  const sortedDisplayRows = useMemo(
    () =>
      displayRows.toSorted((left, right) => {
        const leftStatus = entryStatusByHorse.get(formatRunnerNumber(left.horseNumber)) ?? "";
        const rightStatus = entryStatusByHorse.get(formatRunnerNumber(right.horseNumber)) ?? "";
        if (leftStatus !== "" || rightStatus !== "") {
          return leftStatus === rightStatus ? 0 : leftStatus ? 1 : -1;
        }
        return 0;
      }),
    [displayRows, entryStatusByHorse],
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
    return (
      <>
        <FinishPredictionEvaluationPanel evaluation={evaluation} />
        <p className="empty-state">着順予測を表示できるデータがありません。</p>
      </>
    );
  }

  return (
    <>
      <FinishPredictionEvaluationPanel evaluation={evaluation} />
      <div className="stats-table-wrap">
        <table className="stats-table analysis-table finish-prediction-table">
          <colgroup>
            <col className="finish-prediction-col-number" />
            <col className="finish-prediction-col-horse" />
            <col className="finish-prediction-col-jockey" />
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
              <th>騎手名</th>
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
            {sortedDisplayRows.map((row) => {
              const horseNumber = formatRunnerNumber(row.horseNumber);
              const realtimeOdds = realtimeOddsByHorse.get(horseNumber);
              return (
                <FinishPredictionTableRow
                  entryStatus={entryStatusByHorse.get(horseNumber) ?? ""}
                  isExpanded={expandedHorseNumber === row.horseNumber}
                  jockeyName={getPreferredJockeyName(
                    row.jockeyName,
                    realtimeJockeyByHorse.get(horseNumber),
                  )}
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
    </>
  );
}
