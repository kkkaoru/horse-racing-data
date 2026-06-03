// Run with: rendered by the race detail page (Next.js client component)

"use client";

import { useRouter, useSearchParams } from "next/navigation";
import type { ReactElement, ReactNode } from "react";
import { useMemo, useState } from "react";

import type {
  RaceRunningStyleRow,
  RunningStyleLabel,
} from "../../../db/corner-running-style-parsers";
import { formatKeibajo, getTrackSurfaceLabel, getTrackTurnLabel } from "../../../lib/format";
import { getAgeLabel, getConditionLabel, getGradeLabel } from "../../../lib/race-classification";
import { formatRunnerNumber } from "../../../lib/runner-format";
import type {
  RaceRowForRunningStyleBucketFilter,
  RunningStyleBucketMetrics,
  RunningStyleBucketScope,
  RunningStyleClass,
  RunningStyleDimensionFlags,
} from "../../../lib/running-style-prediction-dimensions";
import { RUNNING_STYLE_PREDICTION_PARAM_NAMES } from "../../../lib/running-style-prediction-dimensions";
import { useRealtimeRaceSelector } from "./realtime-client";

const STYLE_TAB_VALUES = ["nige", "senkou", "sashi", "oikomi"] as const;
type StyleTab = (typeof STYLE_TAB_VALUES)[number];

type DimensionKey = keyof RunningStyleDimensionFlags;

type ClassIndex = 0 | 1 | 2 | 3;

const STYLE_TAB_LABELS: Record<StyleTab, string> = {
  nige: "逃げ",
  senkou: "先行",
  sashi: "差し",
  oikomi: "追い込み",
};

const RUNNING_STYLE_DISPLAY: Record<RunningStyleLabel, string> = {
  nige: "逃げ",
  senkou: "先行",
  sashi: "差し",
  oikomi: "追い込み",
};

const RUNNING_STYLE_CLASS_LABELS: Record<RunningStyleClass, string> = {
  nige: "逃げ",
  senkou: "先行",
  sashi: "差し",
  oikomi: "追い込み",
};

const DEFAULT_TAB: StyleTab = "nige";
const PERCENT_DECIMALS = 2;
const SEARCH_PARAM_KEY = "style";
const MISSING_NAME_PLACEHOLDER = "馬名不明";
const MISSING_JOCKEY_PLACEHOLDER = "騎手不明";
const NIGE_RANK_TOP = 1;
const SENKOU_RIVAL_DECIMALS = 0;
const MIN_SUPPORT_FOR_F1 = 5;
const PANEL_PERCENT_DECIMALS = 1;
const CARD_PERCENT_DECIMALS = 2;
const SCOPE_LABEL_SEPARATOR = " / ";
const PANEL_HEADLINE_SUFFIX = " の脚質精度";
const SCOPE_NOTICE_PREFIX = "該当条件のデータが無いため";
const SCOPE_NOTICE_SUFFIX = "で集計しています";
const LOG_LOSS_DECIMALS = 3;
const F1_DECIMALS = 3;
const HEATMAP_HUE = 210;
const HEATMAP_MAX_LIGHTNESS = 95;
const HEATMAP_MIN_LIGHTNESS = 35;
const HEATMAP_LIGHTNESS_RANGE = HEATMAP_MAX_LIGHTNESS - HEATMAP_MIN_LIGHTNESS;
const RACE_NAME_GRADE_CODES = new Set<string>(["A", "F"]);

interface RunnerDisplayInfo {
  bamei: string | null;
  jockey: string | null;
}

interface RunningStyleSectionProps {
  rows: RaceRunningStyleRow[];
  modelMacroF1: number | null;
  modelVersion: string | null;
  runnersByUmaban: Record<number, RunnerDisplayInfo>;
  bucketEvaluation?: RunningStyleBucketMetrics | null;
  bucketScope?: RunningStyleBucketScope | null;
  dimensionFlags?: RunningStyleDimensionFlags | null;
  bucketRace?: RaceRowForRunningStyleBucketFilter | null;
  bucketSource?: "jra" | "nar" | null;
  bucketGradeCode?: string | null;
}

interface RunningStyleBucketEvaluationPanelProps {
  evaluation: RunningStyleBucketMetrics;
  scopeLabel: string;
  isFallback: boolean;
}

interface RunningStyleBucketMetricCard {
  key: string;
  label: string;
  value: number | null;
}

interface BucketScopeLabelInput {
  scope: RunningStyleBucketScope;
  race: RaceRowForRunningStyleBucketFilter;
  source: "jra" | "nar";
  gradeCode: string | null;
}

interface RunningStyleDimensionTogglesProps {
  flags: RunningStyleDimensionFlags;
  race: RaceRowForRunningStyleBucketFilter;
  source: "jra" | "nar";
  gradeCode: string | null;
}

interface RowPercentInput {
  count: number;
  rowTotal: number;
}

interface HeatmapCellInput {
  count: number;
  rowTotal: number;
  total: number;
}

const formatPercent = (value: number): string => `${(value * 100).toFixed(PERCENT_DECIMALS)}%`;

const probabilityForTab = (row: RaceRunningStyleRow, tab: StyleTab): number => {
  if (tab === "nige") return row.p_nige;
  if (tab === "senkou") return row.p_senkou;
  if (tab === "sashi") return row.p_sashi;
  return row.p_oikomi;
};

const isStyleTab = (value: string): value is StyleTab => {
  for (const candidate of STYLE_TAB_VALUES) {
    if (candidate === value) return true;
  }
  return false;
};

const resolveCurrentTab = (raw: string | null): StyleTab => {
  if (raw === null) return DEFAULT_TAB;
  return isStyleTab(raw) ? raw : DEFAULT_TAB;
};

const sortRowsByTab = (
  rows: RaceRunningStyleRow[],
  tab: StyleTab,
  entryStatusByHorse: ReadonlyMap<string, string>,
): RaceRunningStyleRow[] =>
  rows.toSorted((left, right) => {
    const leftStatus = entryStatusByHorse.get(formatRunnerNumber(String(left.horseNumber))) ?? "";
    const rightStatus = entryStatusByHorse.get(formatRunnerNumber(String(right.horseNumber))) ?? "";
    if (leftStatus !== "" || rightStatus !== "") {
      return leftStatus === rightStatus ? 0 : leftStatus ? 1 : -1;
    }
    return (
      probabilityForTab(right, tab) - probabilityForTab(left, tab) ||
      left.horseNumber - right.horseNumber
    );
  });

const resolveBamei = (
  row: RaceRunningStyleRow,
  runners: Record<number, RunnerDisplayInfo>,
): string => row.bamei ?? runners[row.horseNumber]?.bamei ?? MISSING_NAME_PLACEHOLDER;

const resolveJockey = (
  row: RaceRunningStyleRow,
  runners: Record<number, RunnerDisplayInfo>,
): string => runners[row.horseNumber]?.jockey ?? MISSING_JOCKEY_PLACEHOLDER;

const compareByNigeDesc = (left: RaceRunningStyleRow, right: RaceRunningStyleRow): number =>
  right.p_nige - left.p_nige || left.horseNumber - right.horseNumber;

const buildNigeRankByHorseNumber = (rows: RaceRunningStyleRow[]): ReadonlyMap<number, number> =>
  new Map(rows.toSorted(compareByNigeDesc).map((row, index) => [row.horseNumber, index + 1]));

const formatSenkouRivalPercent = (probability: number): string =>
  `${(probability * 100).toFixed(SENKOU_RIVAL_DECIMALS)}%`;

interface RowLabelResolverInput {
  row: RaceRunningStyleRow;
  nigeRankByHorseNumber: ReadonlyMap<number, number>;
}

const resolveDisplayedStyleLabel = ({
  row,
  nigeRankByHorseNumber,
}: RowLabelResolverInput): string => {
  if (row.predictedLabel !== "nige") return RUNNING_STYLE_DISPLAY[row.predictedLabel];
  const rank = nigeRankByHorseNumber.get(row.horseNumber) ?? NIGE_RANK_TOP;
  if (rank === NIGE_RANK_TOP) return RUNNING_STYLE_DISPLAY.nige;
  return `先行?(${formatSenkouRivalPercent(row.p_nige)})`;
};

interface AllRowsTableProps {
  entryStatusByHorse: ReadonlyMap<string, string>;
  rows: RaceRunningStyleRow[];
  runnersByUmaban: Record<number, RunnerDisplayInfo>;
  nigeRankByHorseNumber: ReadonlyMap<number, number>;
}

const AllRowsTable = ({
  entryStatusByHorse,
  rows,
  runnersByUmaban,
  nigeRankByHorseNumber,
}: AllRowsTableProps) => (
  <div className="runner-table-wrap">
    <table className="runner-table">
      <thead>
        <tr>
          <th scope="col">馬番</th>
          <th scope="col">馬名</th>
          <th scope="col">騎手名</th>
          <th scope="col">脚質</th>
          <th scope="col">逃げ</th>
          <th scope="col">先行</th>
          <th scope="col">差し</th>
          <th scope="col">追い込み</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => {
          const entryStatus =
            entryStatusByHorse.get(formatRunnerNumber(String(row.horseNumber))) ?? "";
          const isScratched = entryStatus !== "";
          const displayedLabel = resolveDisplayedStyleLabel({ row, nigeRankByHorseNumber });
          return (
            <tr
              className={isScratched ? "stats-row-scratched" : undefined}
              data-entry-status={entryStatus || undefined}
              key={`${row.raceKey}-${row.horseNumber}`}
            >
              <td>{row.horseNumber}</td>
              <td className="stats-name-cell">
                {resolveBamei(row, runnersByUmaban)}
                {entryStatus ? <span className="runner-status-badge">{entryStatus}</span> : null}
              </td>
              <td className="stats-name-cell">
                {isScratched ? "-" : resolveJockey(row, runnersByUmaban)}
              </td>
              <td>{isScratched ? "-" : displayedLabel}</td>
              <td>{isScratched ? "-" : formatPercent(row.p_nige)}</td>
              <td>{isScratched ? "-" : formatPercent(row.p_senkou)}</td>
              <td>{isScratched ? "-" : formatPercent(row.p_sashi)}</td>
              <td>{isScratched ? "-" : formatPercent(row.p_oikomi)}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  </div>
);

interface TabButtonsProps {
  currentTab: StyleTab;
  onSelect: (tab: StyleTab) => void;
}

const TabButtons = ({ currentTab, onSelect }: TabButtonsProps) => (
  <div className="running-style-tabs" role="tablist" aria-label="脚質ソート">
    {STYLE_TAB_VALUES.map((tab) => (
      <button
        type="button"
        key={tab}
        role="tab"
        aria-selected={currentTab === tab}
        className={currentTab === tab ? "running-style-tab active" : "running-style-tab"}
        onClick={() => {
          onSelect(tab);
        }}
      >
        {STYLE_TAB_LABELS[tab]}
      </button>
    ))}
  </div>
);

interface MetricsBadgeProps {
  modelMacroF1: number | null;
  modelVersion: string | null;
}

const MetricsBadge = ({ modelMacroF1, modelVersion }: MetricsBadgeProps) => {
  if (modelVersion === null) return null;
  return (
    <p className="running-style-metrics">
      モデル: {modelVersion}
      {modelMacroF1 !== null ? `（macro-F1: ${modelMacroF1.toFixed(3)}）` : null}
    </p>
  );
};

const formatPanelPercent = (value: number): string =>
  `${(value * 100).toFixed(PANEL_PERCENT_DECIMALS)}%`;

const formatF1Value = (value: number | null): string =>
  value === null ? "-" : value.toFixed(F1_DECIMALS);

const formatLogLossValue = (value: number | null): string =>
  value === null ? "-" : value.toFixed(LOG_LOSS_DECIMALS);

const formatRowPercent = ({ count, rowTotal }: RowPercentInput): string =>
  rowTotal === 0 ? "0.0%" : `${((count / rowTotal) * 100).toFixed(PANEL_PERCENT_DECIMALS)}%`;

const formatAccuracyCI = (evaluation: RunningStyleBucketMetrics): string => {
  const halfRange = (evaluation.accuracyCI.upper - evaluation.accuracyCI.lower) / 2;
  return `±${(halfRange * 100).toFixed(PANEL_PERCENT_DECIMALS)}%`;
};

const formatCardPercent = (value: number | null): string =>
  value === null ? "-" : `${(value * 100).toFixed(CARD_PERCENT_DECIMALS)}%`;

const formatCardF1 = (value: number | null): string =>
  value === null ? "-" : value.toFixed(F1_DECIMALS);

const buildBucketMetricCards = (
  evaluation: RunningStyleBucketMetrics,
): readonly RunningStyleBucketMetricCard[] => [
  { key: "accuracy", label: "正解率", value: evaluation.accuracy },
  { key: "top2Accuracy", label: "Top2正解率", value: evaluation.top2Accuracy },
  { key: "macroF1", label: "macro-F1", value: evaluation.macroF1 },
  { key: "weightedF1", label: "weighted-F1", value: evaluation.weightedF1 },
  { key: "nigeRecall", label: "逃げ的中率", value: evaluation.perClass.nige.recall },
  { key: "senkouRecall", label: "先行的中率", value: evaluation.perClass.senkou.recall },
  { key: "sashiRecall", label: "差し的中率", value: evaluation.perClass.sashi.recall },
  { key: "oikomiRecall", label: "追込的中率", value: evaluation.perClass.oikomi.recall },
];

const F1_CARD_KEYS = new Set<string>(["macroF1", "weightedF1"]);

const NIGE_RECALL_CARD_KEY = "nigeRecall";

const DETAILS_SUMMARY_LABEL = "詳細指標を表示";

const NON_NIGE_DETAILS_HEADING = "脚質の逃げ的中率以外の精度";

const PER_CLASS_HEADING = "クラス別 metric";

const PER_CLASS_LOG_LOSS_HEADING = "クラス別 log loss";

const CONFUSION_MATRIX_HEADING = "confusion matrix (actual × predicted)";

const formatBucketMetricCard = (card: RunningStyleBucketMetricCard): string =>
  F1_CARD_KEYS.has(card.key) ? formatCardF1(card.value) : formatCardPercent(card.value);

const isNigeRecallCard = (card: RunningStyleBucketMetricCard): boolean =>
  card.key === NIGE_RECALL_CARD_KEY;

const findNigeRecallCard = (
  cards: readonly RunningStyleBucketMetricCard[],
): RunningStyleBucketMetricCard | null => cards.find(isNigeRecallCard) ?? null;

const filterNonNigeCards = (
  cards: readonly RunningStyleBucketMetricCard[],
): readonly RunningStyleBucketMetricCard[] => cards.filter((card) => !isNigeRecallCard(card));

const heatmapBackground = ({ count, rowTotal, total }: HeatmapCellInput): string => {
  if (total === 0 || rowTotal === 0) {
    return `hsl(${HEATMAP_HUE}, 30%, ${HEATMAP_MAX_LIGHTNESS}%)`;
  }
  const ratio = count / rowTotal;
  const lightness = HEATMAP_MAX_LIGHTNESS - HEATMAP_LIGHTNESS_RANGE * ratio;
  return `hsl(${HEATMAP_HUE}, 70%, ${lightness}%)`;
};

const sumConfusionMatrixRow = (row: readonly [number, number, number, number]): number =>
  row[0] + row[1] + row[2] + row[3];

const sumConfusionMatrixTotal = (cm: RunningStyleBucketMetrics["confusionMatrix"]): number =>
  sumConfusionMatrixRow(cm[0]) +
  sumConfusionMatrixRow(cm[1]) +
  sumConfusionMatrixRow(cm[2]) +
  sumConfusionMatrixRow(cm[3]);

const renderPerClassRow = (
  className: RunningStyleClass,
  evaluation: RunningStyleBucketMetrics,
): ReactElement => {
  const metric = evaluation.perClass[className];
  const supportTooSmall = metric.support < MIN_SUPPORT_FOR_F1;
  return (
    <tr key={`per-class-${className}`}>
      <th scope="row">{RUNNING_STYLE_CLASS_LABELS[className]}</th>
      <td>{supportTooSmall ? "n too small" : formatF1Value(metric.precision)}</td>
      <td>{supportTooSmall ? "n too small" : formatF1Value(metric.recall)}</td>
      <td>{supportTooSmall ? "n too small" : formatF1Value(metric.f1)}</td>
      <td>{metric.support}</td>
    </tr>
  );
};

const renderPerClassLogLossRow = (
  className: RunningStyleClass,
  evaluation: RunningStyleBucketMetrics,
): ReactElement => (
  <tr key={`per-class-logloss-${className}`}>
    <th scope="row">{RUNNING_STYLE_CLASS_LABELS[className]}</th>
    <td>{formatLogLossValue(evaluation.perClassLogLoss[className])}</td>
  </tr>
);

const renderHeatmapCell = (
  actualIndex: ClassIndex,
  predictedIndex: ClassIndex,
  evaluation: RunningStyleBucketMetrics,
  total: number,
): ReactElement => {
  const row = evaluation.confusionMatrix[actualIndex];
  const count = row[predictedIndex];
  const rowTotal = sumConfusionMatrixRow(row);
  const background = heatmapBackground({ count, rowTotal, total });
  return (
    <td
      key={`cm-${actualIndex}-${predictedIndex}`}
      className="running-style-bucket-heatmap-cell"
      style={{ backgroundColor: background }}
    >
      <span className="running-style-bucket-heatmap-count">{count}</span>
      <span className="running-style-bucket-heatmap-percent">
        ({formatRowPercent({ count, rowTotal })})
      </span>
    </td>
  );
};

const HEATMAP_ROW_LABELS: Record<ClassIndex, string> = {
  0: RUNNING_STYLE_CLASS_LABELS.nige,
  1: RUNNING_STYLE_CLASS_LABELS.senkou,
  2: RUNNING_STYLE_CLASS_LABELS.sashi,
  3: RUNNING_STYLE_CLASS_LABELS.oikomi,
};

const renderHeatmapRow = (
  actualIndex: ClassIndex,
  evaluation: RunningStyleBucketMetrics,
  total: number,
): ReactElement => (
  <tr key={`cm-row-${actualIndex}`}>
    <th scope="row">{HEATMAP_ROW_LABELS[actualIndex]}</th>
    {renderHeatmapCell(actualIndex, 0, evaluation, total)}
    {renderHeatmapCell(actualIndex, 1, evaluation, total)}
    {renderHeatmapCell(actualIndex, 2, evaluation, total)}
    {renderHeatmapCell(actualIndex, 3, evaluation, total)}
  </tr>
);

interface MetricCardProps {
  card: RunningStyleBucketMetricCard;
  accuracyCI: string | null;
}

const renderMetricCard = ({ card, accuracyCI }: MetricCardProps): ReactElement => (
  <div className="running-style-bucket-metric-card" key={card.key}>
    <span>{card.label}</span>
    <strong>{formatBucketMetricCard(card)}</strong>
    {card.key === "accuracy" && accuracyCI !== null ? (
      <small className="running-style-bucket-metric-ci">{accuracyCI}</small>
    ) : null}
  </div>
);

interface NigeRecallSectionProps {
  card: RunningStyleBucketMetricCard | null;
}

const renderNigeRecallSection = ({ card }: NigeRecallSectionProps): ReactElement | null => {
  if (card === null) return null;
  return (
    <div className="running-style-bucket-nige-recall">
      <span>{card.label}</span>
      <strong>{formatBucketMetricCard(card)}</strong>
    </div>
  );
};

interface NonNigeGridProps {
  cards: readonly RunningStyleBucketMetricCard[];
  accuracyCI: string;
}

const renderNonNigeMetricGrid = ({ cards, accuracyCI }: NonNigeGridProps): ReactElement => (
  <section className="running-style-stats-card">
    <h3>{NON_NIGE_DETAILS_HEADING}</h3>
    <div className="analysis-metric-grid running-style-bucket-metric-grid">
      {cards.map((card) => renderMetricCard({ accuracyCI, card }))}
    </div>
  </section>
);

const renderPerClassMetricSection = (evaluation: RunningStyleBucketMetrics): ReactElement => (
  <section className="running-style-bucket-per-class running-style-stats-card">
    <h3>{PER_CLASS_HEADING}</h3>
    <table className="running-style-bucket-per-class-table">
      <thead>
        <tr>
          <th scope="col">クラス</th>
          <th scope="col">precision</th>
          <th scope="col">recall</th>
          <th scope="col">F1</th>
          <th scope="col">support</th>
        </tr>
      </thead>
      <tbody>
        {renderPerClassRow("nige", evaluation)}
        {renderPerClassRow("senkou", evaluation)}
        {renderPerClassRow("sashi", evaluation)}
        {renderPerClassRow("oikomi", evaluation)}
      </tbody>
    </table>
  </section>
);

const renderPerClassLogLossSection = (evaluation: RunningStyleBucketMetrics): ReactElement => (
  <section className="running-style-bucket-per-class-logloss running-style-stats-card">
    <h3>{PER_CLASS_LOG_LOSS_HEADING}</h3>
    <table className="running-style-bucket-per-class-logloss-table">
      <thead>
        <tr>
          <th scope="col">クラス</th>
          <th scope="col">log loss</th>
        </tr>
      </thead>
      <tbody>
        {renderPerClassLogLossRow("nige", evaluation)}
        {renderPerClassLogLossRow("senkou", evaluation)}
        {renderPerClassLogLossRow("sashi", evaluation)}
        {renderPerClassLogLossRow("oikomi", evaluation)}
      </tbody>
    </table>
  </section>
);

interface ConfusionMatrixSectionProps {
  evaluation: RunningStyleBucketMetrics;
  total: number;
}

const renderConfusionMatrixSection = ({
  evaluation,
  total,
}: ConfusionMatrixSectionProps): ReactElement => (
  <section className="running-style-bucket-confusion-matrix running-style-stats-card">
    <h3>{CONFUSION_MATRIX_HEADING}</h3>
    <table className="running-style-bucket-heatmap-table">
      <thead>
        <tr>
          <th scope="col" aria-label="actual class">
            actual ＼ predicted
          </th>
          <th scope="col">{RUNNING_STYLE_CLASS_LABELS.nige}</th>
          <th scope="col">{RUNNING_STYLE_CLASS_LABELS.senkou}</th>
          <th scope="col">{RUNNING_STYLE_CLASS_LABELS.sashi}</th>
          <th scope="col">{RUNNING_STYLE_CLASS_LABELS.oikomi}</th>
        </tr>
      </thead>
      <tbody>
        {renderHeatmapRow(0, evaluation, total)}
        {renderHeatmapRow(1, evaluation, total)}
        {renderHeatmapRow(2, evaluation, total)}
        {renderHeatmapRow(3, evaluation, total)}
      </tbody>
    </table>
  </section>
);

function RunningStyleBucketEvaluationPanel({
  evaluation,
  scopeLabel,
  isFallback,
}: RunningStyleBucketEvaluationPanelProps): ReactElement {
  const total = sumConfusionMatrixTotal(evaluation.confusionMatrix);
  const metricCards = buildBucketMetricCards(evaluation);
  const nigeCard = findNigeRecallCard(metricCards);
  const nonNigeCards = filterNonNigeCards(metricCards);
  const accuracyCI = formatAccuracyCI(evaluation);
  return (
    <div className="running-style-bucket-evaluation-panel" aria-label="脚質予測の検証結果">
      <div className="running-style-bucket-evaluation-summary">
        <span>{buildPanelHeadline(scopeLabel)}</span>
        <strong>
          {formatPanelPercent(evaluation.accuracy)} {accuracyCI}
        </strong>
        <small>
          {evaluation.raceCount.toLocaleString("ja-JP")}レース /{" "}
          {evaluation.predictionCount.toLocaleString("ja-JP")}予測で検証
        </small>
        {evaluation.smallSampleWarning ? (
          <span className="running-style-bucket-small-sample-badge">
            (n={evaluation.predictionCount}, small sample)
          </span>
        ) : null}
      </div>
      {isFallback ? (
        <small className="running-style-bucket-scope-notice">{buildScopeNotice(scopeLabel)}</small>
      ) : null}
      {renderNigeRecallSection({ card: nigeCard })}
      <details className="running-style-bucket-details">
        <summary>{DETAILS_SUMMARY_LABEL}</summary>
        <div className="running-style-bucket-details-body">
          {renderNonNigeMetricGrid({ accuracyCI, cards: nonNigeCards })}
          {renderPerClassMetricSection(evaluation)}
          {renderPerClassLogLossSection(evaluation)}
          {renderConfusionMatrixSection({ evaluation, total })}
        </div>
      </details>
    </div>
  );
}

const resolveKeibajoLabel = (race: RaceRowForRunningStyleBucketFilter): string =>
  formatKeibajo(race.keibajoCode);

const resolveDistanceLabel = (race: RaceRowForRunningStyleBucketFilter): string => `${race.kyori}m`;

const resolveAgeLabel = (race: RaceRowForRunningStyleBucketFilter): string =>
  getAgeLabel(race.kyosoShubetsuCode);

const resolveJraConditionLabel = (race: RaceRowForRunningStyleBucketFilter): string =>
  getConditionLabel(race.kyosoJokenCode);

const resolveNarConditionLabel = (race: RaceRowForRunningStyleBucketFilter): string => {
  const trimmed = race.kyosoJokenMeisho?.trim() ?? "";
  return trimmed === "" ? "条件" : trimmed;
};

const resolveTrackLabel = (race: RaceRowForRunningStyleBucketFilter): string =>
  `${getTrackSurfaceLabel(race.trackCode)}${getTrackTurnLabel(race.trackCode)}`;

const resolveGradeLabel = (
  race: RaceRowForRunningStyleBucketFilter,
  source: "jra" | "nar",
): string => getGradeLabel(race.gradeCode, source);

const resolveRaceNameLabel = (race: RaceRowForRunningStyleBucketFilter): string => {
  const trimmed = race.kyosomeiHondai?.trim() ?? "";
  return trimmed;
};

const isGradeShown = (gradeCode: string | null): boolean => gradeCode !== null && gradeCode !== "";

const isRaceNameShown = (gradeCode: string | null): boolean =>
  gradeCode !== null && RACE_NAME_GRADE_CODES.has(gradeCode);

const CATEGORY_SCOPE_LABELS: Record<"jra" | "nar", string> = {
  jra: "JRA 全体",
  nar: "NAR 全体",
};

interface DimensionToggleEntry {
  key: DimensionKey;
  label: string;
}

const buildToggleEntries = (
  race: RaceRowForRunningStyleBucketFilter,
  source: "jra" | "nar",
  gradeCode: string | null,
): readonly DimensionToggleEntry[] => {
  const conditionLabel =
    source === "nar" ? resolveNarConditionLabel(race) : resolveJraConditionLabel(race);
  const raceNameLabel = resolveRaceNameLabel(race);
  const entries: DimensionToggleEntry[] = [
    { key: "keibajo", label: resolveKeibajoLabel(race) },
    { key: "distance", label: resolveDistanceLabel(race) },
    { key: "kyosoShubetsu", label: resolveAgeLabel(race) },
  ];
  if (source === "jra") {
    entries.push({ key: "kyosoJoken", label: resolveJraConditionLabel(race) });
    entries.push({ key: "track", label: resolveTrackLabel(race) });
  }
  if (source === "nar") {
    entries.push({ key: "condition", label: conditionLabel });
  }
  if (isGradeShown(gradeCode)) {
    entries.push({ key: "grade", label: resolveGradeLabel(race, source) });
  }
  if (isRaceNameShown(gradeCode) && raceNameLabel !== "") {
    entries.push({ key: "raceName", label: raceNameLabel });
  }
  return entries;
};

const buildExactScopeLabel = (input: BucketScopeLabelInput): string => {
  const entries = buildToggleEntries(input.race, input.source, input.gradeCode);
  const activeLabels = entries
    .filter((entry) => input.scope.flags[entry.key])
    .map((entry) => entry.label);
  return activeLabels.length === 0
    ? CATEGORY_SCOPE_LABELS[input.source]
    : activeLabels.join(SCOPE_LABEL_SEPARATOR);
};

const buildBucketScopeLabel = (input: BucketScopeLabelInput): string => {
  if (input.scope.level === "category") {
    return CATEGORY_SCOPE_LABELS[input.source];
  }
  if (input.scope.level === "keibajo") {
    return `${resolveKeibajoLabel(input.race)}（全レース）`;
  }
  return buildExactScopeLabel(input);
};

const buildPanelHeadline = (scopeLabel: string): string => `${scopeLabel}${PANEL_HEADLINE_SUFFIX}`;

const buildScopeNotice = (scopeLabel: string): string =>
  `${SCOPE_NOTICE_PREFIX}${scopeLabel}${SCOPE_NOTICE_SUFFIX}`;

function RunningStyleDimensionToggles({
  flags,
  race,
  source,
  gradeCode,
}: RunningStyleDimensionTogglesProps): ReactElement {
  const router = useRouter();
  const searchParams = useSearchParams();
  const entries = buildToggleEntries(race, source, gradeCode);

  const toggle = (key: DimensionKey): void => {
    const next = new URLSearchParams(searchParams?.toString() ?? "");
    const paramName = RUNNING_STYLE_PREDICTION_PARAM_NAMES[key];
    const enabled = !flags[key];
    next.set(paramName, enabled ? "1" : "0");
    router.replace(`?${next.toString()}`, { scroll: false });
  };

  const renderToggle = (entry: DimensionToggleEntry): ReactNode => (
    <label key={entry.key} className="running-style-bucket-toggle-label">
      <input
        type="checkbox"
        checked={flags[entry.key]}
        onChange={() => {
          toggle(entry.key);
        }}
      />
      {entry.label}
    </label>
  );

  return (
    <div className="running-style-bucket-controls">
      <div className="running-style-bucket-toggles-caption">
        <h3 className="running-style-bucket-toggles-heading">精度の集計条件</h3>
        <p className="running-style-bucket-toggles-help">
          チェックした条件に一致する過去レースで脚質予測の的中精度を集計します。一致するデータが無い場合は条件を緩めて表示します。
        </p>
      </div>
      <div className="running-style-bucket-toggles" aria-label="脚質予測 bucket 条件">
        {entries.map((entry) => renderToggle(entry))}
      </div>
    </div>
  );
}

interface RenderBucketSubSectionProps {
  bucketEvaluation: RunningStyleBucketMetrics | null | undefined;
  bucketScope: RunningStyleBucketScope | null | undefined;
  dimensionFlags: RunningStyleDimensionFlags | null | undefined;
  bucketRace: RaceRowForRunningStyleBucketFilter | null | undefined;
  bucketSource: "jra" | "nar" | null | undefined;
  bucketGradeCode: string | null | undefined;
}

interface BucketPanelInput {
  evaluation: RunningStyleBucketMetrics;
  scope: RunningStyleBucketScope;
  race: RaceRowForRunningStyleBucketFilter;
  source: "jra" | "nar";
  gradeCode: string | null;
}

const renderBucketEvaluationPanel = (input: BucketPanelInput): ReactElement => {
  const scopeLabel = buildBucketScopeLabel({
    gradeCode: input.gradeCode,
    race: input.race,
    scope: input.scope,
    source: input.source,
  });
  return (
    <RunningStyleBucketEvaluationPanel
      evaluation={input.evaluation}
      isFallback={input.scope.level !== "exact"}
      scopeLabel={scopeLabel}
    />
  );
};

const renderBucketSubSection = (props: RenderBucketSubSectionProps): ReactNode => {
  const flags = props.dimensionFlags ?? null;
  const race = props.bucketRace ?? null;
  const source = props.bucketSource ?? null;
  const evaluation = props.bucketEvaluation ?? null;
  const scope = props.bucketScope ?? null;
  const gradeCode = props.bucketGradeCode ?? null;
  const showToggles = flags !== null && race !== null && source !== null;
  const showPanel = evaluation !== null && scope !== null && race !== null && source !== null;
  if (!showToggles && !showPanel) {
    return null;
  }
  return (
    <>
      {showToggles ? (
        <RunningStyleDimensionToggles
          flags={flags}
          race={race}
          source={source}
          gradeCode={gradeCode}
        />
      ) : null}
      {showPanel
        ? renderBucketEvaluationPanel({ evaluation, gradeCode, race, scope, source })
        : null}
    </>
  );
};

export const RunningStyleSection = ({
  rows,
  modelMacroF1,
  modelVersion,
  runnersByUmaban,
  bucketEvaluation,
  bucketScope,
  dimensionFlags,
  bucketRace,
  bucketSource,
  bucketGradeCode,
}: RunningStyleSectionProps) => {
  const router = useRouter();
  const searchParams = useSearchParams();
  const payload = useRealtimeRaceSelector((state) => state.payload);
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
  const initialTab = resolveCurrentTab(searchParams?.get(SEARCH_PARAM_KEY) ?? null);
  const [currentTab, setCurrentTab] = useState<StyleTab>(initialTab);
  const visibleRows = useMemo(
    () => sortRowsByTab(rows, currentTab, entryStatusByHorse),
    [currentTab, entryStatusByHorse, rows],
  );
  const nigeRankByHorseNumber = useMemo(() => buildNigeRankByHorseNumber(rows), [rows]);

  const handleSelect = (tab: StyleTab): void => {
    setCurrentTab(tab);
    const params = new URLSearchParams(searchParams?.toString() ?? "");
    if (tab === DEFAULT_TAB) {
      params.delete(SEARCH_PARAM_KEY);
    } else {
      params.set(SEARCH_PARAM_KEY, tab);
    }
    const query = params.toString();
    router.replace(query === "" ? "?" : `?${query}`, { scroll: false });
  };

  const bucketSubSection = renderBucketSubSection({
    bucketEvaluation,
    bucketGradeCode,
    bucketRace,
    bucketScope,
    bucketSource,
    dimensionFlags,
  });

  if (rows.length === 0) {
    return (
      <section className="running-style-section" aria-label="脚質予測">
        <h2>脚質予測</h2>
        {bucketSubSection}
        <p className="running-style-empty">このレースの脚質予測データはまだありません。</p>
      </section>
    );
  }

  return (
    <section className="running-style-section" aria-label="脚質予測">
      <h2>脚質予測</h2>
      {bucketSubSection}
      <TabButtons currentTab={currentTab} onSelect={handleSelect} />
      <AllRowsTable
        entryStatusByHorse={entryStatusByHorse}
        rows={visibleRows}
        runnersByUmaban={runnersByUmaban}
        nigeRankByHorseNumber={nigeRankByHorseNumber}
      />
      <MetricsBadge modelMacroF1={modelMacroF1} modelVersion={modelVersion} />
    </section>
  );
};

export { STYLE_TAB_VALUES };
export type { StyleTab, RunnerDisplayInfo };
