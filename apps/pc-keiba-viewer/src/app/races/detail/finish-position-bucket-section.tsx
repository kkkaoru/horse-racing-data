// Run with: rendered by the race detail finish-prediction section (Next.js server component)

import type { ReactElement } from "react";

import type {
  FinishPositionBucketMetrics,
  FinishPositionBucketScope,
} from "../../../lib/finish-prediction-dimensions";
import { formatKeibajo, getTrackSurfaceLabel, getTrackTurnLabel } from "../../../lib/format";
import { getAgeLabel, getConditionLabel, getGradeLabel } from "../../../lib/race-classification";

export interface FinishPositionBucketRace {
  source: "jra" | "nar";
  keibajoCode: string;
  kyori: number;
  kyosoShubetsuCode: string;
  kyosoJokenCode: string | null;
  kyosoJokenMeisho: string | null;
  trackCode: string | null;
  gradeCode: string | null;
  kyosomeiHondai: string | null;
}

export interface FinishPositionBucketEvaluationPanelProps {
  evaluation: FinishPositionBucketMetrics | null;
  scope: FinishPositionBucketScope | null;
  race: FinishPositionBucketRace | null;
  source: "jra" | "nar" | null;
  gradeCode: string | null;
  modelVersion: string | null;
}

interface FinishPositionBucketMetricCard {
  key: string;
  label: string;
  value: number;
}

interface ResolvedPanelInput {
  evaluation: FinishPositionBucketMetrics;
  scope: FinishPositionBucketScope;
  race: FinishPositionBucketRace;
  source: "jra" | "nar";
  gradeCode: string | null;
  modelVersion: string | null;
}

const PERCENT_DECIMALS = 1;
const NDCG_DECIMALS = 3;
const SCOPE_LABEL_SEPARATOR = " / ";
const PANEL_HEADLINE_SUFFIX = " の着順予測精度";
const SCOPE_NOTICE_PREFIX = "該当条件のデータが無いため";
const SCOPE_NOTICE_SUFFIX = "で集計しています";
const ALL_MISS_NOTICE = "該当する分類の精度データがまだ蓄積されていません";
const RACE_NAME_GRADE_CODES = new Set<string>(["A", "F"]);
const NDCG_CARD_KEY = "ndcg";

const CATEGORY_SCOPE_LABELS: Record<"jra" | "nar", string> = {
  jra: "JRA 全体",
  nar: "NAR 全体",
};

const formatPercent = (value: number): string => `${(value * 100).toFixed(PERCENT_DECIMALS)}%`;

const formatNdcg = (value: number): string => value.toFixed(NDCG_DECIMALS);

const formatTop1CI = (evaluation: FinishPositionBucketMetrics): string => {
  const halfRange = (evaluation.top1AccuracyCI.upper - evaluation.top1AccuracyCI.lower) / 2;
  return `±${(halfRange * 100).toFixed(PERCENT_DECIMALS)}%`;
};

const buildMetricCards = (
  evaluation: FinishPositionBucketMetrics,
): readonly FinishPositionBucketMetricCard[] => [
  { key: "top1", label: "Top1正解率", value: evaluation.top1Accuracy },
  { key: "place1", label: "1着的中率", value: evaluation.place1Accuracy },
  { key: "place2", label: "2着的中率", value: evaluation.place2Accuracy },
  { key: "place3", label: "3着的中率", value: evaluation.place3Accuracy },
  { key: "top3Box", label: "三連複的中率", value: evaluation.top3BoxAccuracy },
  { key: "top3Exact", label: "三連単的中率", value: evaluation.top3ExactAccuracy },
  { key: NDCG_CARD_KEY, label: "NDCG@3", value: evaluation.ndcgAt3Avg },
  { key: "pairScore", label: "ペアスコア", value: evaluation.pairScoreAvg },
  { key: "top3WinnerCapture", label: "上位3頭で勝馬捕捉", value: evaluation.top3WinnerCaptureRate },
  { key: "top5WinnerCapture", label: "上位5頭で勝馬捕捉", value: evaluation.top5WinnerCaptureRate },
];

const formatMetricCard = (card: FinishPositionBucketMetricCard): string =>
  card.key === NDCG_CARD_KEY ? formatNdcg(card.value) : formatPercent(card.value);

const resolveKeibajoLabel = (race: FinishPositionBucketRace): string =>
  formatKeibajo(race.keibajoCode);

const resolveDistanceLabel = (race: FinishPositionBucketRace): string => `${race.kyori}m`;

const resolveAgeLabel = (race: FinishPositionBucketRace): string =>
  getAgeLabel(race.kyosoShubetsuCode);

const resolveJraConditionLabel = (race: FinishPositionBucketRace): string =>
  getConditionLabel(race.kyosoJokenCode);

const resolveNarConditionLabel = (race: FinishPositionBucketRace): string => {
  const trimmed = race.kyosoJokenMeisho?.trim() ?? "";
  return trimmed === "" ? "条件" : trimmed;
};

const resolveTrackLabel = (race: FinishPositionBucketRace): string =>
  `${getTrackSurfaceLabel(race.trackCode)}${getTrackTurnLabel(race.trackCode)}`;

const resolveGradeLabel = (race: FinishPositionBucketRace, source: "jra" | "nar"): string =>
  getGradeLabel(race.gradeCode, source);

const resolveRaceNameLabel = (race: FinishPositionBucketRace): string =>
  race.kyosomeiHondai?.trim() ?? "";

const isGradeShown = (gradeCode: string | null): boolean => gradeCode !== null && gradeCode !== "";

const isRaceNameShown = (gradeCode: string | null): boolean =>
  gradeCode !== null && RACE_NAME_GRADE_CODES.has(gradeCode);

interface ScopeLabelInput {
  scope: FinishPositionBucketScope;
  race: FinishPositionBucketRace;
  source: "jra" | "nar";
  gradeCode: string | null;
}

const buildActiveDimensionLabels = (input: ScopeLabelInput): readonly string[] => {
  const flags = input.scope.flags;
  const conditionLabel =
    input.source === "nar"
      ? resolveNarConditionLabel(input.race)
      : resolveJraConditionLabel(input.race);
  const raceNameLabel = resolveRaceNameLabel(input.race);
  return [
    flags.keibajo ? resolveKeibajoLabel(input.race) : null,
    flags.distance ? resolveDistanceLabel(input.race) : null,
    flags.kyosoShubetsu ? resolveAgeLabel(input.race) : null,
    flags.kyosoJoken && input.source === "jra" ? resolveJraConditionLabel(input.race) : null,
    flags.condition && input.source === "nar" ? conditionLabel : null,
    flags.track ? resolveTrackLabel(input.race) : null,
    flags.grade && isGradeShown(input.gradeCode)
      ? resolveGradeLabel(input.race, input.source)
      : null,
    flags.raceName && isRaceNameShown(input.gradeCode) && raceNameLabel !== ""
      ? raceNameLabel
      : null,
  ].flatMap((label) => (label === null ? [] : [label]));
};

const buildExactScopeLabel = (input: ScopeLabelInput): string => {
  const activeLabels = buildActiveDimensionLabels(input);
  return activeLabels.length === 0
    ? CATEGORY_SCOPE_LABELS[input.source]
    : activeLabels.join(SCOPE_LABEL_SEPARATOR);
};

const buildScopeLabel = (input: ScopeLabelInput): string => {
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

const renderResolvedPanel = (input: ResolvedPanelInput): ReactElement => {
  const { evaluation } = input;
  const scopeLabel = buildScopeLabel({
    gradeCode: input.gradeCode,
    race: input.race,
    scope: input.scope,
    source: input.source,
  });
  const isFallback = input.scope.level !== "exact";
  const metricCards = buildMetricCards(evaluation);
  return (
    <div className="finish-position-bucket-evaluation-panel" aria-label="着順予測の検証結果">
      <div className="finish-position-bucket-evaluation-summary">
        <span>{buildPanelHeadline(scopeLabel)}</span>
        <strong>
          {formatPercent(evaluation.top1Accuracy)} {formatTop1CI(evaluation)}
        </strong>
        <small>
          {evaluation.raceCount.toLocaleString("ja-JP")}レース /{" "}
          {evaluation.predictionCount.toLocaleString("ja-JP")}予測で検証
        </small>
        {evaluation.smallSampleWarning ? (
          <span className="finish-position-bucket-small-sample-badge">
            (n={evaluation.raceCount}, small sample)
          </span>
        ) : null}
      </div>
      {isFallback ? (
        <small className="finish-position-bucket-scope-notice">
          {buildScopeNotice(scopeLabel)}
        </small>
      ) : null}
      {input.modelVersion !== null ? (
        <p className="finish-position-bucket-model-version">モデル: {input.modelVersion}</p>
      ) : null}
      <div className="analysis-metric-grid finish-position-bucket-metric-grid">
        {metricCards.map((card) => (
          <div className="finish-position-bucket-metric-card" key={card.key}>
            <span>{card.label}</span>
            <strong>{formatMetricCard(card)}</strong>
            {card.key === "top1" ? (
              <small className="finish-position-bucket-metric-ci">{formatTop1CI(evaluation)}</small>
            ) : null}
          </div>
        ))}
      </div>
    </div>
  );
};

export function FinishPositionBucketEvaluationPanel({
  evaluation,
  scope,
  race,
  source,
  gradeCode,
  modelVersion,
}: FinishPositionBucketEvaluationPanelProps): ReactElement {
  if (evaluation === null || scope === null || race === null || source === null) {
    return (
      <div className="finish-position-bucket-evaluation-panel" aria-label="着順予測の検証結果">
        <p className="finish-position-bucket-empty-notice">{ALL_MISS_NOTICE}</p>
      </div>
    );
  }
  return renderResolvedPanel({ evaluation, gradeCode, modelVersion, race, scope, source });
}
