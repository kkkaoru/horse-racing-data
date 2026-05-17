"use client";

import { useSearchParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import type { FinishPredictionEvaluationMetrics } from "../../../lib/finish-position-prediction-evaluation";
import type {
  AbilityTest,
  BloodlineStatsRow,
  ConditionCorrelationRow,
  FinishPredictionRow,
  FinishPositionStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  OverallScoreRow,
  PayoutStatsRow,
  RacePacePredictionRow,
  RaceTimeStats,
  Runner,
  SimilarRaceStatsRow,
  StableComment,
  SimilarRaceStatsSettings,
  TimeScoreRow,
  Training,
} from "../../../lib/race-types";
import { AbilityTestTable } from "./ability-test-table";
import { BloodlineSimilarCombinedTable } from "./bloodline-similar-combined-table";
import { BloodlineStatsTable } from "./bloodline-stats-table";
import { FinishPositionPredictionTable } from "./finish-position-prediction-table";
import { HorseRaceResultsTable } from "./horse-race-results-table";
import { OverallScoreTable } from "./overall-score-table";
import { RaceConditionAnalysisSection } from "./race-condition-analysis-section";
import { RacePacePredictionTable } from "./race-pace-prediction-table";
import { SimilarRaceStatsTable } from "./similar-race-stats-table";
import { TrainingTable } from "./training-table";

type DetailSection =
  | "ability"
  | "bloodline"
  | "condition"
  | "finish-prediction"
  | "overall-score"
  | "pace-prediction"
  | "results"
  | "similar"
  | "time-score"
  | "training";

interface LazyDetailSectionsProps {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  realtimeApiBaseUrl: string;
  source: RaceSource;
  year: string;
}

type ConditionLabels = {
  age: string | null;
  class: string | null;
  distance: string | null;
  frame: string;
  monthWindow: string;
  raceNumber: string;
  raceSubtitle: string | null;
  raceTitle: string | null;
  runnerCount?: string | null;
  sex: string | null;
  surface: string | null;
  turn: string | null;
  venue: string | null;
  weight: string | null;
};

type ResultsPayload = {
  classConditionName: string | null;
  currentDistance: string | null;
  currentKeibajoCode: string;
  currentRaceDate: string;
  currentTrackCode: string | null;
  defaultIncludeClass: boolean;
  results: HorseRaceResult[];
  runners: Runner[];
  source: RaceSource;
  sourceScope: RaceSource | "all";
  type: "results";
};

type TrainingPayload = {
  sourceLabel: string;
  stableComments: StableComment[];
  trainings: Training[];
  type: "training";
};

type AbilityPayload = {
  abilityTests: AbilityTest[];
  type: "ability";
};

type ConditionPayload = {
  conditionLabels: ConditionLabels & { runnerCount: string | null };
  finishPositionStats: FinishPositionStatsRow[];
  frameStats: FrameStatsRow[];
  payoutStats: PayoutStatsRow[];
  raceTimeStats: RaceTimeStats;
  runners: Runner[];
  settings: SimilarRaceStatsSettings;
  source: RaceSource;
  type: "condition";
};

type BloodlinePayload = {
  conditionLabels: ConditionLabels;
  rows: BloodlineStatsRow[];
  runners: Runner[];
  settings: SimilarRaceStatsSettings;
  source: RaceSource;
  type: "bloodline";
};

type SimilarPayload = {
  bloodlineRows: BloodlineStatsRow[];
  bloodlineSettings: SimilarRaceStatsSettings;
  conditionLabels: ConditionLabels;
  rows: SimilarRaceStatsRow[];
  runners: Runner[];
  settings: SimilarRaceStatsSettings;
  source: RaceSource;
  type: "similar";
};

type TimeScorePayload = {
  bloodlineRows: BloodlineStatsRow[];
  bloodlineSettings: SimilarRaceStatsSettings;
  conditionLabels: ConditionLabels;
  correlationRows: ConditionCorrelationRow[];
  rows: TimeScoreRow[];
  runners: Runner[];
  settings: SimilarRaceStatsSettings;
  similarRows: SimilarRaceStatsRow[];
  source: RaceSource;
  type: "time-score";
};

type OverallScorePayload = {
  rows: OverallScoreRow[];
  type: "overall-score";
};

type FinishPredictionPayload = {
  evaluation: FinishPredictionEvaluationMetrics;
  rows: FinishPredictionRow[];
  type: "finish-prediction";
};

type RacePacePredictionPayload = {
  rows: RacePacePredictionRow[];
  supported?: boolean;
  type: "pace-prediction";
};

type SectionPayload =
  | AbilityPayload
  | BloodlinePayload
  | ConditionPayload
  | FinishPredictionPayload
  | OverallScorePayload
  | RacePacePredictionPayload
  | ResultsPayload
  | SimilarPayload
  | TimeScorePayload
  | TrainingPayload;

type SectionState =
  | { error: string; payload: null; status: "error" }
  | { error: null; payload: SectionPayload | null; status: "loading" }
  | { error: null; payload: SectionPayload; status: "ready" };

const SECTION_TITLES: Record<DetailSection, string> = {
  ability: "能力検査",
  bloodline: "血統成績",
  condition: "同条件レース分析",
  "finish-prediction": "着順予測",
  "overall-score": "総合スコア",
  "pace-prediction": "レース展開予測",
  results: "競走成績",
  similar: "同条件成績",
  "time-score": "総合評価スコア",
  training: "調教・追い切り",
};

const GENERIC_STATS_QUERY_KEYS = new Set([
  "statsAge",
  "statsClass",
  "statsDistance",
  "statsFrame",
  "statsMonthWindow",
  "statsNarOnly",
  "statsRaceMonth",
  "statsRaceName",
  "statsRaceNumber",
  "statsRaceSubtitle",
  "statsRaceTitle",
  "statsRunnerCount",
  "statsSex",
  "statsSourceScope",
  "statsSurface",
  "statsTrack",
  "statsTurn",
  "statsVenue",
  "statsWeight",
  "statsYears",
]);

const shouldIncludeSectionQueryParam = (section: DetailSection, name: string): boolean => {
  if (section === "bloodline") {
    return name.startsWith("bloodlineStats") || GENERIC_STATS_QUERY_KEYS.has(name);
  }
  if (section === "condition") {
    return (
      name.startsWith("analysisStats") ||
      name === "similarStatsVenue" ||
      GENERIC_STATS_QUERY_KEYS.has(name)
    );
  }
  if (section === "overall-score" || section === "finish-prediction") {
    return (
      name.startsWith("analysisStats") ||
      name.startsWith("bloodlineStats") ||
      name === "similarStatsVenue" ||
      GENERIC_STATS_QUERY_KEYS.has(name)
    );
  }
  if (section === "pace-prediction") {
    return (
      name === "resultsSourceScope" ||
      name.startsWith("analysisStats") ||
      name.startsWith("bloodlineStats") ||
      name.startsWith("similarStats") ||
      name === "similarStatsVenue" ||
      GENERIC_STATS_QUERY_KEYS.has(name)
    );
  }
  if (section === "results") {
    return name === "resultsSourceScope";
  }
  if (section === "time-score") {
    return (
      name.startsWith("analysisStats") ||
      name.startsWith("bloodlineStats") ||
      name.startsWith("similarStats") ||
      name === "similarStatsVenue" ||
      GENERIC_STATS_QUERY_KEYS.has(name)
    );
  }
  if (section === "similar") {
    return (
      name.startsWith("similarStats") ||
      name.startsWith("bloodlineStats") ||
      GENERIC_STATS_QUERY_KEYS.has(name)
    );
  }
  return false;
};

const getSectionSearchParams = (
  section: DetailSection,
  searchParams: URLSearchParams,
): URLSearchParams => {
  const next = new URLSearchParams();
  searchParams.forEach((value, name) => {
    if (shouldIncludeSectionQueryParam(section, name)) {
      next.append(name, value);
    }
  });
  return next;
};

const SectionSkeleton = ({ title }: { title: string }) => (
  <section className="detail-loading-section lazy-detail-section" aria-busy="true">
    <div className="section-heading compact">
      <h2>{title}</h2>
    </div>
    <div className="detail-section-skeleton">
      <span />
      <span />
      <span />
      <span />
    </div>
  </section>
);

const SectionError = ({ error, title }: { error: string; title: string }) => (
  <section className="detail-loading-section lazy-detail-section">
    <div className="section-heading compact">
      <h2>{title}</h2>
    </div>
    <p className="empty-state">データを取得できませんでした: {error}</p>
  </section>
);

const STABLE_COMMENT_LABELS = {
  comment: process.env.NEXT_PUBLIC_PREMIUM_RACE_COMMENT_LABEL_TEXT ?? "コメント",
  evaluation: process.env.NEXT_PUBLIC_PREMIUM_RACE_COMMENT_LABEL_EVALUATION ?? "評価",
  horseName: process.env.NEXT_PUBLIC_PREMIUM_RACE_COMMENT_LABEL_HORSE_NAME ?? "馬名",
  horseNumber: process.env.NEXT_PUBLIC_PREMIUM_RACE_COMMENT_LABEL_HORSE_NUMBER ?? "馬番",
};

const getStableEvaluationLabel = (grade: number | null): string => {
  if (grade === 1) {
    return "とてもよい";
  }
  if (grade === 2) {
    return "よい";
  }
  if (grade === 3) {
    return "まあまあ";
  }
  return "-";
};

const StableCommentsTable = ({ rows }: { rows: StableComment[] }) => {
  if (rows.length === 0) {
    return null;
  }
  const sortedRows = rows.toSorted((left, right) => {
    const leftGrade = left.evaluationGrade ?? 99;
    const rightGrade = right.evaluationGrade ?? 99;
    if (leftGrade !== rightGrade) {
      return leftGrade - rightGrade;
    }
    return Number(left.horseNumber) - Number(right.horseNumber);
  });
  return (
    <section className="detail-subsection">
      <div className="section-heading compact">
        <h3>厩舎コメント</h3>
      </div>
      <div className="stable-comment-table-wrap">
        <table className="stable-comment-table">
          <thead>
            <tr>
              <th>{STABLE_COMMENT_LABELS.horseNumber}</th>
              <th>{STABLE_COMMENT_LABELS.horseName}</th>
              <th>{STABLE_COMMENT_LABELS.evaluation}</th>
              <th>{STABLE_COMMENT_LABELS.comment}</th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => (
              <tr key={row.horseNumber}>
                <td>{row.horseNumber}</td>
                <td>{row.horseName ?? "-"}</td>
                <td>
                  <span className={`stable-comment-grade grade-${row.evaluationGrade ?? "none"}`}>
                    {getStableEvaluationLabel(row.evaluationGrade)}
                  </span>
                </td>
                <td className="stable-comment-text-cell">{row.commentText}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
};

const getSectionUrl = (
  section: DetailSection,
  { day, keibajoCode, month, raceNumber, year }: LazyDetailSectionsProps,
  searchParams: URLSearchParams,
): string => {
  const query = getSectionSearchParams(section, searchParams).toString();
  const base = `/api/races/${year}/${month}/${day}/${keibajoCode}/${raceNumber}/sections/${section}`;
  return query ? `${base}?${query}` : base;
};

const isObjectPayload = (payload: unknown): payload is { type?: unknown } =>
  typeof payload === "object" && payload !== null;

const isSectionPayload = (payload: unknown, section: DetailSection): payload is SectionPayload =>
  isObjectPayload(payload) && payload.type === section;

const useSectionPayload = (
  section: DetailSection,
  props: LazyDetailSectionsProps,
  searchParams: URLSearchParams,
): SectionState => {
  const [state, setState] = useState<SectionState>({
    error: null,
    payload: null,
    status: "loading",
  });
  const url = useMemo(
    () => getSectionUrl(section, props, searchParams),
    [props, searchParams, section],
  );

  useEffect(() => {
    let isActive = true;
    setState((current) => ({ error: null, payload: current.payload, status: "loading" }));

    fetchWithRetry(url)
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(`${response.status} ${response.statusText}`.trim());
        }
        const payload: unknown = await response.json();
        if (!isSectionPayload(payload, section)) {
          throw new Error("Invalid section payload");
        }
        return payload;
      })
      .then((payload) => {
        if (!isActive) {
          return undefined;
        }
        setState({ error: null, payload, status: "ready" });
        return undefined;
      })
      .catch((error: unknown) => {
        if (!isActive) {
          return;
        }
        if (error instanceof DOMException && error.name === "AbortError") {
          return;
        }
        setState({
          error: error instanceof Error ? error.message : "unknown error",
          payload: null,
          status: "error",
        });
      });

    return () => {
      isActive = false;
    };
  }, [section, url]);

  return state;
};

function LazyResultsSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("results", props, searchParams);
  if (state.status === "loading" && state.payload === null) {
    return <SectionSkeleton title={SECTION_TITLES.results} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES.results} />;
  }
  const payload = state.payload;
  if (!payload || payload.type !== "results") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES.results} />;
  }
  return (
    <section
      aria-busy={state.status === "loading"}
      className="race-results-section lazy-detail-section"
    >
      <div className="section-heading compact">
        <h2>競走成績</h2>
      </div>
      <HorseRaceResultsTable
        classConditionName={payload.classConditionName}
        currentDistance={payload.currentDistance}
        currentKeibajoCode={payload.currentKeibajoCode}
        currentRaceDate={payload.currentRaceDate}
        currentTrackCode={payload.currentTrackCode}
        defaultIncludeClass={payload.defaultIncludeClass}
        results={payload.results}
        runners={payload.runners}
        source={payload.source}
        sourceScope={payload.sourceScope}
      />
    </section>
  );
}

export function LazyOverallScoreSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("overall-score", props, searchParams);
  if (state.status === "loading" && state.payload === null) {
    return <SectionSkeleton title={SECTION_TITLES["overall-score"]} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES["overall-score"]} />;
  }
  const payload = state.payload;
  if (!payload || payload.type !== "overall-score") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES["overall-score"]} />;
  }
  return (
    <section
      aria-busy={state.status === "loading"}
      className="similar-stats-section lazy-detail-section"
    >
      <div className="section-heading compact">
        <h2>総合スコア</h2>
      </div>
      <OverallScoreTable
        realtimeRequest={{
          apiBaseUrl: props.realtimeApiBaseUrl,
          day: props.day,
          keibajoCode: props.keibajoCode,
          month: props.month,
          raceNumber: props.raceNumber,
          source: props.source,
          year: props.year,
        }}
        rows={payload.rows}
      />
    </section>
  );
}

export function LazyFinishPredictionSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("finish-prediction", props, searchParams);
  const scoreState = useSectionPayload("time-score", props, searchParams);
  if (state.status === "loading" && state.payload === null) {
    return <SectionSkeleton title={SECTION_TITLES["finish-prediction"]} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES["finish-prediction"]} />;
  }
  const payload = state.payload;
  if (!payload || payload.type !== "finish-prediction") {
    return (
      <SectionError error="Invalid section payload" title={SECTION_TITLES["finish-prediction"]} />
    );
  }
  return (
    <section
      aria-busy={state.status === "loading"}
      className="similar-stats-section lazy-detail-section"
    >
      <div className="section-heading compact">
        <h2>着順予測</h2>
      </div>
      <FinishPositionPredictionTable
        combinedScoreData={
          scoreState.payload?.type === "time-score"
            ? {
                bloodlineRows: scoreState.payload.bloodlineRows,
                correlationRows: scoreState.payload.correlationRows,
                rows: scoreState.payload.similarRows,
                runners: scoreState.payload.runners,
                timeRows: scoreState.payload.rows,
              }
            : null
        }
        combinedScoreLoading={scoreState.status === "loading"}
        evaluation={payload.evaluation}
        realtimeRequest={{
          apiBaseUrl: props.realtimeApiBaseUrl,
          day: props.day,
          keibajoCode: props.keibajoCode,
          month: props.month,
          raceNumber: props.raceNumber,
          source: props.source,
          year: props.year,
        }}
        rows={payload.rows}
      />
    </section>
  );
}

export function LazyRacePacePredictionSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("pace-prediction", props, searchParams);
  if (state.status === "loading" && state.payload === null) {
    return <SectionSkeleton title={SECTION_TITLES["pace-prediction"]} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES["pace-prediction"]} />;
  }
  const payload = state.payload;
  if (!payload || payload.type !== "pace-prediction") {
    return (
      <SectionError error="Invalid section payload" title={SECTION_TITLES["pace-prediction"]} />
    );
  }
  if (payload.supported === false) {
    return null;
  }
  return (
    <section
      aria-busy={state.status === "loading"}
      className="similar-stats-section lazy-detail-section"
    >
      <div className="section-heading compact">
        <h2>レース展開予測</h2>
      </div>
      <RacePacePredictionTable rows={payload.rows} />
    </section>
  );
}

function LazyTrainingSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("training", props, searchParams);
  if (state.status === "loading" && state.payload === null) {
    return <SectionSkeleton title={SECTION_TITLES.training} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES.training} />;
  }
  const payload = state.payload;
  if (!payload || payload.type !== "training") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES.training} />;
  }
  if (payload.trainings.length === 0) {
    return null;
  }
  return (
    <section className="training-section lazy-detail-section">
      <div className="section-heading compact">
        <h2>調教・追い切り</h2>
      </div>
      <TrainingTable sourceLabel={payload.sourceLabel} trainings={payload.trainings} />
      <StableCommentsTable rows={payload.stableComments} />
    </section>
  );
}

function LazyAbilitySection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("ability", props, searchParams);
  if (state.status === "loading" && state.payload === null) {
    return <SectionSkeleton title={SECTION_TITLES.ability} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES.ability} />;
  }
  const payload = state.payload;
  if (!payload || payload.type !== "ability") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES.ability} />;
  }
  if (payload.abilityTests.length === 0) {
    return null;
  }
  return (
    <section className="ability-test-section lazy-detail-section">
      <div className="section-heading compact">
        <h2>能力検査</h2>
      </div>
      <AbilityTestTable abilityTests={payload.abilityTests} />
    </section>
  );
}

function LazyConditionSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("condition", props, searchParams);
  if (state.status === "loading" && state.payload === null) {
    return <SectionSkeleton title={SECTION_TITLES.condition} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES.condition} />;
  }
  const payload = state.payload;
  if (!payload || payload.type !== "condition") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES.condition} />;
  }
  return (
    <section
      aria-busy={state.status === "loading"}
      className="similar-stats-section lazy-detail-section"
    >
      <div className="section-heading compact">
        <h2>同条件レース分析</h2>
      </div>
      <RaceConditionAnalysisSection
        conditionLabels={payload.conditionLabels}
        finishPositionStats={payload.finishPositionStats}
        frameStats={payload.frameStats}
        payoutStats={payload.payoutStats}
        raceTimeStats={payload.raceTimeStats}
        runners={payload.runners}
        settings={payload.settings}
        source={payload.source}
      />
    </section>
  );
}

function LazyTimeScoreSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const [showBloodline, setShowBloodline] = useState(false);
  const [showSimilar, setShowSimilar] = useState(false);
  const state = useSectionPayload("time-score", props, searchParams);
  if (state.status === "loading" && state.payload === null) {
    return <SectionSkeleton title={SECTION_TITLES["time-score"]} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES["time-score"]} />;
  }
  const payload = state.payload;
  if (!payload || payload.type !== "time-score") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES["time-score"]} />;
  }
  return (
    <section
      aria-busy={state.status === "loading"}
      className="similar-stats-section lazy-detail-section"
    >
      <div className="section-heading compact">
        <h2>総合評価スコア</h2>
      </div>
      <div className="stats-category-list">
        <BloodlineSimilarCombinedTable
          bloodlineRows={payload.bloodlineRows}
          correlationRows={payload.correlationRows}
          realtimeRequest={{
            apiBaseUrl: props.realtimeApiBaseUrl,
            day: props.day,
            keibajoCode: props.keibajoCode,
            month: props.month,
            raceNumber: props.raceNumber,
            source: props.source,
            year: props.year,
          }}
          rows={payload.similarRows}
          runners={payload.runners}
          timeRows={payload.rows}
        />
        <div className="stats-section-toggle-wrap">
          <button
            aria-expanded={showBloodline}
            className="stats-control-button stats-section-toggle"
            type="button"
            onClick={() => {
              setShowBloodline((current) => !current);
            }}
          >
            {showBloodline ? "血統成績を閉じる" : "血統成績を表示"}
          </button>
          <button
            aria-expanded={showSimilar}
            className="stats-control-button stats-section-toggle"
            type="button"
            onClick={() => {
              setShowSimilar((current) => !current);
            }}
          >
            {showSimilar ? "同条件成績を閉じる" : "同条件成績を表示"}
          </button>
        </div>
        {showBloodline ? (
          <section className="stats-category-section">
            <div className="section-heading compact">
              <h3>血統成績</h3>
            </div>
            <BloodlineStatsTable
              conditionLabels={payload.conditionLabels}
              rows={payload.bloodlineRows}
              runners={payload.runners}
              settings={payload.bloodlineSettings}
              source={payload.source}
            />
          </section>
        ) : null}
        {showSimilar ? (
          <section className="stats-category-section">
            <div className="section-heading compact">
              <h3>同条件成績</h3>
            </div>
            <SimilarRaceStatsTable
              conditionLabels={payload.conditionLabels}
              rows={payload.similarRows}
              runners={payload.runners}
              settings={payload.settings}
              source={payload.source}
            />
          </section>
        ) : null}
      </div>
    </section>
  );
}

export function LazyDetailSections(props: LazyDetailSectionsProps) {
  return (
    <>
      <LazyTimeScoreSection {...props} />
      <LazyResultsSection {...props} />
      <LazyTrainingSection {...props} />
      {props.source === "nar" ? <LazyAbilitySection {...props} /> : null}
      <LazyConditionSection {...props} />
    </>
  );
}
