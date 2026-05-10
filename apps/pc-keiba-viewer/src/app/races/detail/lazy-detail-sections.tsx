"use client";

import { useSearchParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";

import type { RaceSource } from "../../../lib/codes";
import type {
  AbilityTest,
  BloodlineStatsRow,
  FinishPositionStatsRow,
  FrameStatsRow,
  HorseRaceResult,
  PayoutStatsRow,
  RaceTimeStats,
  Runner,
  SimilarRaceStatsRow,
  SimilarRaceStatsSettings,
  Training,
} from "../../../lib/race-types";
import { AbilityTestTable } from "./ability-test-table";
import { BloodlineStatsTable } from "./bloodline-stats-table";
import { HorseRaceResultsTable } from "./horse-race-results-table";
import { RaceConditionAnalysisSection } from "./race-condition-analysis-section";
import { SimilarRaceStatsTable } from "./similar-race-stats-table";
import { TrainingTable } from "./training-table";

type DetailSection = "ability" | "bloodline" | "condition" | "results" | "similar" | "training";

interface LazyDetailSectionsProps {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
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
  defaultIncludeClass: boolean;
  results: HorseRaceResult[];
  runners: Runner[];
  source: RaceSource;
  type: "results";
};

type TrainingPayload = {
  sourceLabel: string;
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
  settings: SimilarRaceStatsSettings;
  type: "condition";
};

type BloodlinePayload = {
  conditionLabels: ConditionLabels;
  rows: BloodlineStatsRow[];
  runners: Runner[];
  settings: SimilarRaceStatsSettings;
  type: "bloodline";
};

type SimilarPayload = {
  conditionLabels: ConditionLabels;
  rows: SimilarRaceStatsRow[];
  settings: SimilarRaceStatsSettings;
  type: "similar";
};

type SectionPayload =
  | AbilityPayload
  | BloodlinePayload
  | ConditionPayload
  | ResultsPayload
  | SimilarPayload
  | TrainingPayload;

type SectionState =
  | { error: string; payload: null; status: "error" }
  | { error: null; payload: null; status: "loading" }
  | { error: null; payload: SectionPayload; status: "ready" };

const SECTION_TITLES: Record<DetailSection, string> = {
  ability: "能力検査",
  bloodline: "血統成績",
  condition: "同条件レース分析",
  results: "競走成績",
  similar: "同条件成績",
  training: "調教・追い切り",
};

const SectionSkeleton = ({ title }: { title: string }) => (
  <section className="detail-loading-section lazy-detail-section" aria-busy="true">
    <div className="section-heading compact">
      <h2>{title}</h2>
      <span>読み込み中</span>
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
      <span>取得失敗</span>
    </div>
    <p className="empty-state">データを取得できませんでした: {error}</p>
  </section>
);

const getSectionUrl = (
  section: DetailSection,
  { day, keibajoCode, month, raceNumber, year }: LazyDetailSectionsProps,
  searchParams: URLSearchParams,
): string => {
  const query = searchParams.toString();
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
    const controller = new AbortController();
    setState({ error: null, payload: null, status: "loading" });

    fetch(url, { signal: controller.signal })
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
        setState({ error: null, payload, status: "ready" });
        return undefined;
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) {
          return;
        }
        setState({
          error: error instanceof Error ? error.message : "unknown error",
          payload: null,
          status: "error",
        });
      });

    return () => {
      controller.abort();
    };
  }, [section, url]);

  return state;
};

function LazyResultsSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("results", props, searchParams);
  if (state.status === "loading") {
    return <SectionSkeleton title={SECTION_TITLES.results} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES.results} />;
  }
  const payload = state.payload;
  if (payload.type !== "results") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES.results} />;
  }
  return (
    <section className="race-results-section lazy-detail-section">
      <div className="section-heading compact">
        <h2>競走成績</h2>
        <span>{payload.results.length} 件</span>
      </div>
      <HorseRaceResultsTable
        classConditionName={payload.classConditionName}
        currentDistance={payload.currentDistance}
        currentKeibajoCode={payload.currentKeibajoCode}
        currentRaceDate={payload.currentRaceDate}
        defaultIncludeClass={payload.defaultIncludeClass}
        results={payload.results}
        runners={payload.runners}
        source={payload.source}
      />
    </section>
  );
}

function LazyTrainingSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("training", props, searchParams);
  if (state.status === "loading") {
    return <SectionSkeleton title={SECTION_TITLES.training} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES.training} />;
  }
  const payload = state.payload;
  if (payload.type !== "training") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES.training} />;
  }
  return (
    <section className="training-section lazy-detail-section">
      <div className="section-heading compact">
        <h2>調教・追い切り</h2>
        <span>{payload.trainings.length} 件</span>
      </div>
      <TrainingTable sourceLabel={payload.sourceLabel} trainings={payload.trainings} />
    </section>
  );
}

function LazyAbilitySection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("ability", props, searchParams);
  if (state.status === "loading") {
    return <SectionSkeleton title={SECTION_TITLES.ability} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES.ability} />;
  }
  const payload = state.payload;
  if (payload.type !== "ability") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES.ability} />;
  }
  return (
    <section className="ability-test-section lazy-detail-section">
      <div className="section-heading compact">
        <h2>能力検査</h2>
        <span>{payload.abilityTests.length} 件</span>
      </div>
      <AbilityTestTable abilityTests={payload.abilityTests} />
    </section>
  );
}

function LazyConditionSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("condition", props, searchParams);
  if (state.status === "loading") {
    return <SectionSkeleton title={SECTION_TITLES.condition} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES.condition} />;
  }
  const payload = state.payload;
  if (payload.type !== "condition") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES.condition} />;
  }
  return (
    <section className="similar-stats-section lazy-detail-section">
      <div className="section-heading compact">
        <h2>同条件レース分析</h2>
        <span>
          {payload.settings.years === null ? "全期間" : `過去${payload.settings.years}年`}
        </span>
      </div>
      <RaceConditionAnalysisSection
        conditionLabels={payload.conditionLabels}
        finishPositionStats={payload.finishPositionStats}
        frameStats={payload.frameStats}
        payoutStats={payload.payoutStats}
        raceTimeStats={payload.raceTimeStats}
        settings={payload.settings}
      />
    </section>
  );
}

function LazyBloodlineSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("bloodline", props, searchParams);
  if (state.status === "loading") {
    return <SectionSkeleton title={SECTION_TITLES.bloodline} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES.bloodline} />;
  }
  const payload = state.payload;
  if (payload.type !== "bloodline") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES.bloodline} />;
  }
  return (
    <section className="similar-stats-section lazy-detail-section">
      <div className="section-heading compact">
        <h2>血統成績</h2>
        <span>
          {payload.settings.years === null ? "全期間" : `過去${payload.settings.years}年`}
        </span>
      </div>
      <BloodlineStatsTable
        conditionLabels={payload.conditionLabels}
        rows={payload.rows}
        runners={payload.runners}
        settings={payload.settings}
      />
    </section>
  );
}

function LazySimilarSection(props: LazyDetailSectionsProps) {
  const searchParams = useSearchParams();
  const state = useSectionPayload("similar", props, searchParams);
  if (state.status === "loading") {
    return <SectionSkeleton title={SECTION_TITLES.similar} />;
  }
  if (state.status === "error") {
    return <SectionError error={state.error} title={SECTION_TITLES.similar} />;
  }
  const payload = state.payload;
  if (payload.type !== "similar") {
    return <SectionError error="Invalid section payload" title={SECTION_TITLES.similar} />;
  }
  return (
    <section className="similar-stats-section lazy-detail-section">
      <div className="section-heading compact">
        <h2>同条件成績</h2>
        <span>
          {payload.settings.years === null ? "全期間" : `過去${payload.settings.years}年`}
        </span>
      </div>
      <SimilarRaceStatsTable
        conditionLabels={payload.conditionLabels}
        rows={payload.rows}
        settings={payload.settings}
      />
    </section>
  );
}

export function LazyDetailSections(props: LazyDetailSectionsProps) {
  return (
    <>
      <LazyResultsSection {...props} />
      <LazyTrainingSection {...props} />
      {props.source === "nar" ? <LazyAbilitySection {...props} /> : null}
      <LazyConditionSection {...props} />
      <LazyBloodlineSection {...props} />
      <LazySimilarSection {...props} />
    </>
  );
}
