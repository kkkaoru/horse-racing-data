"use client";

import {
  Fragment,
  memo,
  useCallback,
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
} from "react";

import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import type {
  CorrectionToggles,
  FinishPredictionBuildInputs,
} from "../../../lib/finish-position-prediction";
import {
  buildFinishPredictionMarketOverrides,
  buildFinishPredictionRowsFromInputs,
} from "../../../lib/finish-position-prediction";
import type { FinishPredictionEvaluationMetrics } from "../../../lib/finish-position-prediction-evaluation";
import { getPreferredJockeyName } from "../../../lib/jockey-name";
import {
  isPaddockState,
  normalizePaddockHorseScore,
  type PaddockOfficialRank,
  type PaddockState,
} from "../../../lib/paddock";
import type {
  BloodlineStatsRow,
  ConditionCorrelationRow,
  FinishPredictionRow,
  Runner,
  SimilarRaceStatsRow,
  TimeScoreRow,
} from "../../../lib/race-types";
import { formatRunnerNumber } from "../../../lib/runner-format";
import { buildCombinedScoreRows, type CombinedScoreRow } from "./bloodline-similar-combined-table";
import { MobileCollapsibleSection } from "./mobile-collapsible-section";
import type { RealtimeRaceRequest } from "./realtime-client";
import { useRealtimeRacePayload } from "./realtime-client";

const FINISH_PREDICTION_EVALUATION_TITLE = "着順予測精度";

const CORRECTION_TOGGLES_STORAGE_KEY = "pc-keiba:correction-toggles";
const CORRECTION_TOGGLES_CHANGE_EVENT = "pc-keiba:correction-toggles-change";

type CorrectionFeatureKey =
  | "horse"
  | "jockey"
  | "odds"
  | "popularity"
  | "recent"
  | "sameDayJockey"
  | "similarity"
  | "trainer";

interface CorrectionFeatureConfig {
  key: CorrectionFeatureKey;
  label: string;
}

const ALL_CORRECTION_FEATURE_KEYS: CorrectionFeatureKey[] = [
  "horse",
  "jockey",
  "odds",
  "popularity",
  "recent",
  "sameDayJockey",
  "similarity",
  "trainer",
];

const CORRECTION_FEATURES: CorrectionFeatureConfig[] = [
  { key: "horse", label: "競走成績" },
  { key: "recent", label: "近走" },
  { key: "jockey", label: "騎手" },
  { key: "trainer", label: "調教師" },
  { key: "popularity", label: "人気" },
  { key: "odds", label: "単勝" },
  { key: "sameDayJockey", label: "同日同場の騎手勝利" },
  { key: "similarity", label: "類似レース" },
];

export const buildAllOnToggles = (): Record<CorrectionFeatureKey, boolean> =>
  Object.fromEntries(ALL_CORRECTION_FEATURE_KEYS.map((k) => [k, true])) as Record<
    CorrectionFeatureKey,
    boolean
  >;

export const buildAllOffToggles = (): Record<CorrectionFeatureKey, boolean> =>
  Object.fromEntries(ALL_CORRECTION_FEATURE_KEYS.map((k) => [k, false])) as Record<
    CorrectionFeatureKey,
    boolean
  >;

export const buildTogglesFromStored = (
  stored: Record<string, unknown>,
): Record<CorrectionFeatureKey, boolean> =>
  Object.fromEntries(
    ALL_CORRECTION_FEATURE_KEYS.map((k) => [k, stored[k] !== false]),
  ) as Record<CorrectionFeatureKey, boolean>;

// Module-level cache: useSyncExternalStore requires getSnapshot to return the same
// reference when the underlying value has not changed, to avoid infinite re-render loops.
const ALL_ON_TOGGLES_CACHE: Record<CorrectionFeatureKey, boolean> = buildAllOnToggles();

interface CorrectionTogglesCache {
  key: string | null;
  toggles: Record<CorrectionFeatureKey, boolean>;
}

const correctionTogglesCache: CorrectionTogglesCache = {
  key: null,
  toggles: ALL_ON_TOGGLES_CACHE,
};

const subscribeCorrectionToggles = (onStoreChange: () => void): (() => void) => {
  if (typeof window === "undefined") {
    return () => {};
  }
  window.addEventListener(CORRECTION_TOGGLES_CHANGE_EVENT, onStoreChange);
  return () => {
    window.removeEventListener(CORRECTION_TOGGLES_CHANGE_EVENT, onStoreChange);
  };
};

export const getCorrectionTogglesSnapshot = (): Record<CorrectionFeatureKey, boolean> => {
  if (typeof window === "undefined" || typeof window.localStorage === "undefined") {
    return ALL_ON_TOGGLES_CACHE;
  }
  const stored = window.localStorage.getItem(CORRECTION_TOGGLES_STORAGE_KEY);
  if (stored === null) {
    return ALL_ON_TOGGLES_CACHE;
  }
  // Return cached reference if the raw stored string has not changed
  if (stored === correctionTogglesCache.key) {
    return correctionTogglesCache.toggles;
  }
  try {
    const parsed: unknown = JSON.parse(stored);
    if (typeof parsed !== "object" || parsed === null) {
      return ALL_ON_TOGGLES_CACHE;
    }
    const next = buildTogglesFromStored(parsed as Record<string, unknown>);
    correctionTogglesCache.key = stored;
    correctionTogglesCache.toggles = next;
    return next;
  } catch {
    return ALL_ON_TOGGLES_CACHE;
  }
};

const getCorrectionTogglesServerSnapshot = (): Record<CorrectionFeatureKey, boolean> =>
  ALL_ON_TOGGLES_CACHE;

interface CorrectionMasterCheckboxProps {
  rawToggles: Record<CorrectionFeatureKey, boolean>;
}

interface FinishPositionPredictionTableProps {
  combinedScoreData?: FinishPredictionCombinedScoreData | null;
  combinedScoreLoading?: boolean;
  evaluation: FinishPredictionEvaluationMetrics;
  inputs: FinishPredictionBuildInputs;
  realtimeRequest: RealtimeRaceRequest;
}

interface FinishPredictionCombinedScoreData {
  bloodlineRows: BloodlineStatsRow[];
  correlationRows: ConditionCorrelationRow[];
  rows: SimilarRaceStatsRow[];
  runners: Runner[];
  timeRows: TimeScoreRow[];
}

interface FinishPredictionTableRowProps {
  combinedScore: CombinedScoreRow | null;
  combinedScoreLoading: boolean;
  entryStatus: string;
  isExpanded: boolean;
  jockeyName: string;
  onToggle: (horseNumber: string) => void;
  paddockScore: number | null;
  realtimeOdds: number | null | undefined;
  realtimePopularity: number | null | undefined;
  row: FinishPredictionRow;
}

const formatPopularity = (value: number | null): string => (value === null ? "-" : `${value}`);

const formatOdds = (value: number | null): string => (value === null ? "-" : value.toFixed(1));

const formatDate = (value: string): string =>
  value.length === 8 ? `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}` : value;

const formatPercent = (value: number): string => `${value.toFixed(2)}%`;

const formatRaceCount = (value: number): string => value.toLocaleString("ja-JP");

const formatScore = (value: number): string => value.toFixed(2);

const normalizeScoreRange = (value: number, min: number, max: number): number => {
  if (max <= min) {
    return 1;
  }
  return (value - min) / (max - min);
};

const normalizeRankRange = (rank: PaddockOfficialRank | null, min: number, max: number): number => {
  if (rank === null) {
    return 0;
  }
  if (max <= min) {
    return 1;
  }
  return (max - rank) / (max - min);
};

const buildPaddockScoreByHorse = (
  rows: FinishPredictionRow[],
  state: PaddockState | null,
): Map<string, number> => {
  if (state === null) {
    return new Map();
  }
  const scoredRows = rows
    .map((row) => {
      const horseNumber = formatRunnerNumber(row.horseNumber);
      const scores = state.horses[horseNumber]
        ? normalizePaddockHorseScore(state.horses[horseNumber], {
            horseName: row.horseName,
            horseNumber,
          })
        : null;
      return { horseNumber, scores };
    })
    .filter(
      (row): row is { horseNumber: string; scores: NonNullable<typeof row.scores> } =>
        row.scores !== null,
    );
  if (scoredRows.length === 0) {
    return new Map();
  }
  if (scoredRows.length === 1) {
    return new Map([[scoredRows[0]?.horseNumber ?? "", 1]]);
  }
  const totals = scoredRows.map((row) => row.scores.total);
  const ranks = scoredRows
    .map((row) => row.scores.officialRank)
    .filter((rank): rank is PaddockOfficialRank => rank !== null);
  const minTotal = Math.min(...totals);
  const maxTotal = Math.max(...totals);
  const minRank = ranks.length > 0 ? Math.min(...ranks) : 1;
  const maxRank = ranks.length > 0 ? Math.max(...ranks) : 1;
  const rawScores = scoredRows.map((row) => {
    const totalScore = normalizeScoreRange(row.scores.total, minTotal, maxTotal);
    const rankScore = normalizeRankRange(row.scores.officialRank, minRank, maxRank);
    return (totalScore + rankScore) / 2;
  });
  const minRaw = Math.min(...rawScores);
  const maxRaw = Math.max(...rawScores);
  return new Map(
    scoredRows.map((row, index) => [
      row.horseNumber,
      normalizeScoreRange(rawScores[index] ?? 0, minRaw, maxRaw),
    ]),
  );
};

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
      description:
        "予想で上位3頭に選んだ馬が、実際の1着から3着に何頭入ったかを見ます。3頭中2頭が入れば約66.7%、3頭全て入れば100%として、検証レース全体で平均した値です。",
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
                <span>変数名: {metric.key}</span>
              </small>
            </button>
          );
        })}
      </div>
    </div>
  );
}

export function WrappedFinishPredictionEvaluation({
  evaluation,
}: {
  evaluation: FinishPredictionEvaluationMetrics;
}) {
  return (
    <MobileCollapsibleSection title={FINISH_PREDICTION_EVALUATION_TITLE}>
      <FinishPredictionEvaluationPanel evaluation={evaluation} />
    </MobileCollapsibleSection>
  );
}

const FinishPredictionTableRow = memo(function FinishPredictionTableRow({
  combinedScore,
  combinedScoreLoading,
  entryStatus,
  isExpanded,
  jockeyName,
  onToggle,
  paddockScore,
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
        <td>
          {isScratched ? (
            formatRunnerNumber(row.horseNumber)
          ) : (
            <span className="time-score-actions">
              <span>{formatRunnerNumber(row.horseNumber)}</span>
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
        <td className="stats-name-cell">
          {row.horseName || "-"}
          {entryStatus ? <span className="runner-status-badge">{entryStatus}</span> : null}
        </td>
        <td className="stats-name-cell">{jockeyName || "-"}</td>
        <td>{isScratched ? "対象外" : row.predictedRank}</td>
        <td>{isScratched ? "-" : formatPopularity(displayedPopularity)}</td>
        <td>{isScratched ? "-" : formatOdds(displayedOdds)}</td>
        <td className="stats-score-cell">{isScratched ? "-" : formatScore(row.score)}</td>
        <td className="stats-score-cell">
          {isScratched
            ? "-"
            : combinedScore
              ? formatScore(combinedScore.score)
              : combinedScoreLoading
                ? ""
                : "-"}
        </td>
        <td className="stats-score-cell">
          {isScratched ? "-" : paddockScore === null ? "-" : formatScore(paddockScore)}
        </td>
        <td>{isScratched ? "-" : row.winProbability.toFixed(2)}</td>
        <td>{isScratched ? "-" : row.showProbability.toFixed(2)}</td>
        <td>{isScratched ? "-" : row.confidence.toFixed(2)}</td>
      </tr>
      {isExpanded && !isScratched ? (
        <tr className="stats-detail-row">
          <td colSpan={12}>
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

export function CorrectionMasterCheckbox({ rawToggles }: CorrectionMasterCheckboxProps) {
  // Compute whether all features are on, all off, or mixed
  const allOn = ALL_CORRECTION_FEATURE_KEYS.every((k) => rawToggles[k]);
  const allOff = ALL_CORRECTION_FEATURE_KEYS.every((k) => !rawToggles[k]);
  const mixed = !allOn && !allOff;

  return (
    <label htmlFor="correction-checkbox-all">
      <input
        checked={allOn}
        id="correction-checkbox-all"
        onChange={() => {}}
        ref={(el: HTMLInputElement | null) => {
          if (el !== null) {
            el.indeterminate = mixed;
          }
        }}
        type="checkbox"
        onClick={() => {
          const next = allOn ? buildAllOffToggles() : buildAllOnToggles();
          window.localStorage.setItem(CORRECTION_TOGGLES_STORAGE_KEY, JSON.stringify(next));
          window.dispatchEvent(new Event(CORRECTION_TOGGLES_CHANGE_EVENT));
        }}
      />
      <span>すべて</span>
    </label>
  );
}

export function FinishPositionPredictionTable({
  combinedScoreData = null,
  combinedScoreLoading = false,
  evaluation,
  inputs,
  realtimeRequest,
}: FinishPositionPredictionTableProps) {
  const [expandedHorseNumber, setExpandedHorseNumber] = useState<string | null>(null);
  const rawToggles = useSyncExternalStore(
    subscribeCorrectionToggles,
    getCorrectionTogglesSnapshot,
    getCorrectionTogglesServerSnapshot,
  );
  const correctionToggles: CorrectionToggles = useMemo(
    () => ({
      horseEnabled: rawToggles.horse,
      jockeyEnabled: rawToggles.jockey,
      oddsEnabled: rawToggles.odds,
      popularityEnabled: rawToggles.popularity,
      recentEnabled: rawToggles.recent,
      sameDayJockeyEnabled: rawToggles.sameDayJockey,
      similarityEnabled: rawToggles.similarity,
      trainerEnabled: rawToggles.trainer,
    }),
    [
      rawToggles.horse,
      rawToggles.jockey,
      rawToggles.odds,
      rawToggles.popularity,
      rawToggles.recent,
      rawToggles.sameDayJockey,
      rawToggles.similarity,
      rawToggles.trainer,
    ],
  );
  const [displayRows, setDisplayRows] = useState<FinishPredictionRow[]>(() =>
    buildFinishPredictionRowsFromInputs({ ...inputs, correctionToggles }),
  );
  const [paddockState, setPaddockState] = useState<PaddockState | null>(null);
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
  const realtimeScoreValues = useMemo(
    () =>
      new Map(
        (payload?.odds?.latest.tansho ?? []).map((row) => [
          formatRunnerNumber(row.combination),
          { odds: row.odds ?? null, popularity: row.rank ?? null },
        ]),
      ),
    [payload],
  );
  const combinedScoreByHorse = useMemo(() => {
    if (combinedScoreData === null) {
      return new Map<string, CombinedScoreRow>();
    }
    return new Map(
      buildCombinedScoreRows({
        bloodlineRows: combinedScoreData.bloodlineRows,
        correlationRows: combinedScoreData.correlationRows,
        realtimeValues: realtimeScoreValues,
        rows: combinedScoreData.rows,
        runners: combinedScoreData.runners,
        timeRows: combinedScoreData.timeRows,
      }).map((row) => [formatRunnerNumber(row.horseNumber), row]),
    );
  }, [combinedScoreData, realtimeScoreValues]);
  const paddockScoreByHorse = useMemo(
    () => buildPaddockScoreByHorse(displayRows, paddockState),
    [displayRows, paddockState],
  );
  const sortedDisplayRows = useMemo(
    () =>
      displayRows.toSorted((left, right) => {
        const leftStatus = entryStatusByHorse.get(formatRunnerNumber(left.horseNumber)) ?? "";
        const rightStatus = entryStatusByHorse.get(formatRunnerNumber(right.horseNumber)) ?? "";
        if (leftStatus !== "" || rightStatus !== "") {
          return leftStatus === rightStatus ? 0 : leftStatus ? 1 : -1;
        }
        return (
          left.predictedRank - right.predictedRank ||
          Number(left.horseNumber) - Number(right.horseNumber)
        );
      }),
    [displayRows, entryStatusByHorse],
  );
  const toggleExpandedHorse = useCallback((horseNumber: string) => {
    setExpandedHorseNumber((current) => (current === horseNumber ? null : horseNumber));
  }, []);

  useEffect(() => {
    const tanshoRows = payload?.odds?.latest.tansho ?? [];
    const anyOddsRelatedOn = correctionToggles.oddsEnabled || correctionToggles.popularityEnabled;
    const marketOverrides =
      anyOddsRelatedOn && tanshoRows.length > 0
        ? buildFinishPredictionMarketOverrides(tanshoRows)
        : undefined;
    setDisplayRows(
      buildFinishPredictionRowsFromInputs({ ...inputs, correctionToggles }, marketOverrides),
    );
    setExpandedHorseNumber(null);
  }, [inputs, correctionToggles, payload?.odds?.latest.tansho]);

  useEffect(() => {
    let isActive = true;
    const load = async () => {
      try {
        const response = await fetchWithRetry(
          `/api/races/${realtimeRequest.year}/${realtimeRequest.month}/${realtimeRequest.day}/${realtimeRequest.keibajoCode}/${realtimeRequest.raceNumber}/paddock`,
          { cache: "no-store" },
        );
        if (!response.ok) {
          throw new Error(`paddock api ${response.status}`);
        }
        const paddockPayload: unknown = await response.json();
        if (isActive && isPaddockState(paddockPayload)) {
          setPaddockState(paddockPayload);
        }
      } catch {
        if (isActive) {
          setPaddockState(null);
        }
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), 15000);
    return () => {
      isActive = false;
      window.clearInterval(timer);
    };
  }, [
    realtimeRequest.day,
    realtimeRequest.keibajoCode,
    realtimeRequest.month,
    realtimeRequest.raceNumber,
    realtimeRequest.year,
  ]);

  if (displayRows.length === 0) {
    return (
      <>
        <WrappedFinishPredictionEvaluation evaluation={evaluation} />
        <p className="empty-state">着順予測を表示できるデータがありません。</p>
      </>
    );
  }

  return (
    <>
      <WrappedFinishPredictionEvaluation evaluation={evaluation} />
      <div className="finish-prediction-odds-toggle">
        <CorrectionMasterCheckbox rawToggles={rawToggles} />
        <span className="correction-toggle-separator" aria-hidden="true" />
        {CORRECTION_FEATURES.map((feature) => (
          <label htmlFor={`correction-checkbox-${feature.key}`} key={feature.key}>
            <input
              checked={rawToggles[feature.key]}
              id={`correction-checkbox-${feature.key}`}
              onChange={(event) => {
                const next = { ...rawToggles, [feature.key]: event.target.checked };
                window.localStorage.setItem(
                  CORRECTION_TOGGLES_STORAGE_KEY,
                  JSON.stringify(next),
                );
                window.dispatchEvent(new Event(CORRECTION_TOGGLES_CHANGE_EVENT));
              }}
              type="checkbox"
            />
            <span>{feature.label}</span>
          </label>
        ))}
        <span className="finish-prediction-odds-toggle-hint">
          オフ: その補正を無効化 / オン: 最新データで予想を補正
        </span>
      </div>
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
            <col className="finish-prediction-col-score" />
            <col className="finish-prediction-col-score" />
            <col className="finish-prediction-col-rate" />
            <col className="finish-prediction-col-rate" />
            <col className="finish-prediction-col-rate" />
          </colgroup>
          <thead>
            <tr>
              <th>
                <span className="finish-prediction-header-label">馬番</span>
              </th>
              <th>
                <span className="finish-prediction-header-label">馬名</span>
              </th>
              <th>
                <span className="finish-prediction-header-label">騎手名</span>
              </th>
              <th>
                <span className="finish-prediction-header-label">
                  <span>予想</span>
                  <span>着順</span>
                </span>
              </th>
              <th>
                <span className="finish-prediction-header-label">人気</span>
              </th>
              <th>
                <span className="finish-prediction-header-label">単勝</span>
              </th>
              <th>
                <span className="finish-prediction-header-label">
                  <span>着順予測</span>
                  <span>スコア</span>
                </span>
              </th>
              <th>
                <span className="finish-prediction-header-label">
                  <span>総合評価</span>
                  <span>スコア</span>
                </span>
              </th>
              <th>
                <span className="finish-prediction-header-label">
                  <span>パドック</span>
                  <span>スコア</span>
                </span>
              </th>
              <th>
                <span className="finish-prediction-header-label">勝率</span>
              </th>
              <th>
                <span className="finish-prediction-header-label">
                  <span>3着内</span>
                  <span>率</span>
                </span>
              </th>
              <th>
                <span className="finish-prediction-header-label">信頼度</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {sortedDisplayRows.map((row) => {
              const horseNumber = formatRunnerNumber(row.horseNumber);
              const realtimeOdds = realtimeOddsByHorse.get(horseNumber);
              return (
                <FinishPredictionTableRow
                  combinedScore={combinedScoreByHorse.get(horseNumber) ?? null}
                  combinedScoreLoading={combinedScoreLoading}
                  entryStatus={entryStatusByHorse.get(horseNumber) ?? ""}
                  isExpanded={expandedHorseNumber === row.horseNumber}
                  jockeyName={getPreferredJockeyName(
                    row.jockeyName,
                    realtimeJockeyByHorse.get(horseNumber),
                  )}
                  key={row.horseNumber}
                  paddockScore={paddockScoreByHorse.get(horseNumber) ?? null}
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
